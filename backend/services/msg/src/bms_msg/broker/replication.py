"""ISR 半同步复制核心（任务 02-4）。

**职责边界**：本模块只做「复制机制」——副本集解析、follower 拉取、ISR 收缩 / 重入、
HW 推进、`acks=all` 提交等待与 epoch 校验；**不做**选举 / epoch 递增 / 旧 leader 写拒绝 /
未提交尾部截断（02-5）、**不做**真实跨实例网络传输与分区分配（02-8）。

**三条硬口径**：

1. **HW 只由 ISR 收敛推进**：leader 取 `min(本节点 LEO, 各 ISR follower 上报 LEO)`；
   消费者只能读到 `offset < HW`（夹取在 `DeliveryService`，不在存储层）。
2. **动态位点不逐条落记录**：记录头只写 `epoch`；`hw` / `replicas` 保持预留字段的缺省值，
   副本位点由本模块的运行时状态维护（详见详细设计 §8.1）。
3. **单实例零变化**：复制关闭时 `HW = LEO`、`acks=all` 与 `acks=1` 等价、ISR 恒为 `[本节点]`；
   既有 02 域端口与语义不改。

**传输是协议不是实现**：`ReplicaTransport` 只有 `fetch` 一个方法；进程内实现用于用例与同进程
多节点，02-8 的真实网络实现只需替换这一层。
"""

from __future__ import annotations

import asyncio
import os
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker.log import MessageHeaderView, MessageStore, SegmentMeta, StoredMessage
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.config import (
    ACKS_PUBLISH_ALL,
    DEFAULT_REPLICATION_COMMIT_TIMEOUT_MS,
    DEFAULT_REPLICATION_EPOCH_REFRESH_MS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_REPLICATION_FETCH_BATCH,
    DEFAULT_REPLICATION_FETCH_INTERVAL_MS,
    DEFAULT_REPLICATION_LAG_RECORDS,
    DEFAULT_REPLICATION_LAG_TIME_MS,
    DEFAULT_REPLICATION_LEADER_LEASE_MS,
    DEFAULT_REPLICATION_MIN_INSYNC,
    DEFAULT_REPLICATION_NODE_ID,
    DEFAULT_REPLICATION_SCAN_INTERVAL_MS,
    DEFAULT_REPLICATION_SWITCH_GRACE_MS,
    DEFAULT_REPLICATION_TOPOLOGY_TTL_MS,
    MS_PER_SECOND,
    REPLICATION_COMMIT_TIMEOUT_MS_ENV,
    REPLICATION_ENABLED_ENV,
    REPLICATION_EPOCH_REFRESH_MS_ENV,
    REPLICATION_FACTOR_ENV,
    REPLICATION_FETCH_BACKOFF_CAP_MS,
    REPLICATION_FETCH_BATCH_ENV,
    REPLICATION_FETCH_INTERVAL_MS_ENV,
    REPLICATION_LAG_RECORDS_ENV,
    REPLICATION_LAG_TIME_MS_ENV,
    REPLICATION_LEADER_LEASE_MS_ENV,
    REPLICATION_METRICS_PARTITION_LIMIT,
    REPLICATION_MIN_INSYNC_ENV,
    REPLICATION_NODE_ID_ENV,
    REPLICATION_NODES_ENV,
    REPLICATION_SCAN_INTERVAL_MS_ENV,
    REPLICATION_SWITCH_GRACE_MS_ENV,
    REPLICATION_TOPOLOGY_TTL_MS_ENV,
)
from bms_msg.core.trace import TraceContext
from bms_msg.errors import MsgInsufficientReplicasError, MsgNotLeaderError

__all__ = [
    "SWITCH_STATE_IDLE",
    "SWITCH_STATE_SWITCHING",
    "FollowerProgress",
    "InProcessReplicaTransport",
    "PartitionReplication",
    "PartitionTopology",
    "ReplicaConfig",
    "ReplicaFetchRequest",
    "ReplicaFetchResult",
    "ReplicaKey",
    "ReplicaRole",
    "ReplicaTopologyLoader",
    "ReplicaTransport",
    "ReplicaUnavailableError",
    "ReplicatedMessageStore",
    "ReplicationCoordinator",
    "ReplicationDivergedError",
    "ReplicationMetricRow",
    "ReplicationMetrics",
    "ReplicationMetricsSnapshot",
    "ReplicationSnapshot",
    "ReplicationView",
    "StaleLeaderError",
    "SwitchApplyCommand",
    "SwitchApplyResult",
]

_LOGGER = get_logger("bms_msg.replication")

_FAILURE_LOG_WINDOW_MS = 60_000
"""拉取失败日志的抑制窗口（毫秒）：同一分区 60s 内只记一条。"""

ReplicaKey = tuple[int, int]
"""分区键：`(topic_id, partition_no)`。"""

SWITCH_STATE_IDLE = "idle"
"""切换状态：空闲（leader 在租约内可写）。"""

SWITCH_STATE_SWITCHING = "switching"
"""切换状态：切换窗口（拒绝写入，等旧租约过期后由目标节点激活）。"""


class ReplicaRole(StrEnum):
    """本节点在某分区上的角色。"""

    DISABLED = "disabled"
    """复制关闭：单实例行为，HW 恒等于 LEO。"""

    LEADER = "leader"
    """leader：接受写入、维护 ISR、推进 HW。"""

    FOLLOWER = "follower"
    """follower：只从 leader 拉取，不承担写入与 ISR 扫描。"""


class ReplicationError(RuntimeError):
    """复制内部错误基类（不是客户端契约码：由传输层 / 装配层处置）。"""


class ReplicaUnavailableError(ReplicationError):
    """目标副本不可达（节点未登记 / leader 未就绪）：follower 退避重试。"""

    def __init__(self, message: str, *, reason: str = "unavailable") -> None:
        """初始化。

        Args:
            message: 错误说明。
            reason: 内部端点错误原因（`not_leader` / `switching` / `unavailable`；网络层据此映射）。
        """
        super().__init__(message)
        self.reason = reason


class StaleLeaderError(ReplicationError):
    """epoch 校验失败：对端 leader 任期低于本地已知任期（旧 leader 的数据一律拒绝）。"""


class ReplicationDivergedError(ReplicationError):
    """副本位点不连续 / 起点不匹配：停止拉取；可修复时按 `leader_leo` 截断本地发散尾部（02-5）。"""

    def __init__(self, message: str, *, leader_leo: int | None = None) -> None:
        """初始化。

        Args:
            message: 错误说明。
            leader_leo: 新 leader 的权威末端（可空；给出时调用方可据此截断本地尾部）。
        """
        super().__init__(message)
        self.leader_leo = leader_leo


@dataclass(frozen=True)
class ReplicaConfig(BaseValueObject):
    """复制配置（环境变量 > 常量缺省；关闭时只保留开关与节点标识）。"""

    enabled: bool = False
    """复制总开关：关闭时单实例行为与 8B_02 完全一致。"""

    node_id: str = DEFAULT_REPLICATION_NODE_ID
    """本节点标识（开启时必须在 `nodes` 内）。"""

    nodes: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """集群节点标识列表（插入序；首个为缺省 leader）。"""

    replication_factor: int = DEFAULT_REPLICATION_FACTOR
    """配置兜底的副本数（`1 <= 值 <= len(nodes)`）。"""

    min_insync_replicas: int = DEFAULT_REPLICATION_MIN_INSYNC
    """全局最小同步副本数（`1 <= 值 <= replication_factor`）。"""

    lag_time_max_ms: int = DEFAULT_REPLICATION_LAG_TIME_MS
    """ISR 时间阈值（毫秒）：距最近一次成功拉取超过该值即踢出。"""

    lag_records_max: int = DEFAULT_REPLICATION_LAG_RECORDS
    """ISR 条数阈值：`leader_leo - follower_leo` 超过该值即踢出。"""

    fetch_interval_ms: int = DEFAULT_REPLICATION_FETCH_INTERVAL_MS
    """follower 拉取周期（毫秒）。"""

    fetch_batch: int = DEFAULT_REPLICATION_FETCH_BATCH
    """单次拉取上限（条）。"""

    commit_timeout_ms: int = DEFAULT_REPLICATION_COMMIT_TIMEOUT_MS
    """`acks=all` 提交等待超时（毫秒）。"""

    scan_interval_ms: int = DEFAULT_REPLICATION_SCAN_INTERVAL_MS
    """leader ISR 扫描周期（毫秒）。"""

    topology_ttl_ms: int = DEFAULT_REPLICATION_TOPOLOGY_TTL_MS
    """分区拓扑缓存 TTL（毫秒）。"""

    epoch_refresh_ms: int = DEFAULT_REPLICATION_EPOCH_REFRESH_MS
    """已挂载分区的 epoch / leader / switch_state 强制刷新周期（毫秒；02-5）。"""

    leader_lease_ms: int = DEFAULT_REPLICATION_LEADER_LEASE_MS
    """leader 写权限租约（毫秒；超时未确认即拒写；02-5）。"""

    switch_grace_ms: int = DEFAULT_REPLICATION_SWITCH_GRACE_MS
    """切换停写宽限（毫秒；须 ≥ 租约；02-5）。"""

    @classmethod
    def from_env(cls) -> ReplicaConfig:
        """从环境变量构造配置并校验。

        Returns:
            ReplicaConfig: 已校验的配置。

        Raises:
            ValueError: 开启复制但节点 / 因子 / 阈值非法。
        """
        enabled = _env_bool(REPLICATION_ENABLED_ENV, False)
        node_id = os.environ.get(REPLICATION_NODE_ID_ENV, DEFAULT_REPLICATION_NODE_ID) or DEFAULT_REPLICATION_NODE_ID
        raw_nodes = os.environ.get(REPLICATION_NODES_ENV, "")
        nodes: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
            item.strip() for item in raw_nodes.split(",") if item.strip()
        )
        config = cls(
            enabled=enabled,
            node_id=node_id,
            nodes=nodes,
            replication_factor=_env_int(REPLICATION_FACTOR_ENV, DEFAULT_REPLICATION_FACTOR),
            min_insync_replicas=_env_int(REPLICATION_MIN_INSYNC_ENV, DEFAULT_REPLICATION_MIN_INSYNC),
            lag_time_max_ms=_env_int(REPLICATION_LAG_TIME_MS_ENV, DEFAULT_REPLICATION_LAG_TIME_MS),
            lag_records_max=_env_int(REPLICATION_LAG_RECORDS_ENV, DEFAULT_REPLICATION_LAG_RECORDS),
            fetch_interval_ms=_env_int(REPLICATION_FETCH_INTERVAL_MS_ENV, DEFAULT_REPLICATION_FETCH_INTERVAL_MS),
            fetch_batch=_env_int(REPLICATION_FETCH_BATCH_ENV, DEFAULT_REPLICATION_FETCH_BATCH),
            commit_timeout_ms=_env_int(REPLICATION_COMMIT_TIMEOUT_MS_ENV, DEFAULT_REPLICATION_COMMIT_TIMEOUT_MS),
            scan_interval_ms=_env_int(REPLICATION_SCAN_INTERVAL_MS_ENV, DEFAULT_REPLICATION_SCAN_INTERVAL_MS),
            topology_ttl_ms=_env_int(REPLICATION_TOPOLOGY_TTL_MS_ENV, DEFAULT_REPLICATION_TOPOLOGY_TTL_MS),
            epoch_refresh_ms=_env_int(REPLICATION_EPOCH_REFRESH_MS_ENV, DEFAULT_REPLICATION_EPOCH_REFRESH_MS),
            leader_lease_ms=_env_int(REPLICATION_LEADER_LEASE_MS_ENV, DEFAULT_REPLICATION_LEADER_LEASE_MS),
            switch_grace_ms=_env_int(REPLICATION_SWITCH_GRACE_MS_ENV, DEFAULT_REPLICATION_SWITCH_GRACE_MS),
        )
        config.validate()
        return config

    def validate(self) -> None:
        """校验配置组合（关闭时只校验数值基本合法性，不强制节点列表）。

        Raises:
            ValueError: 任一约束不满足。
        """
        for name, value in (
            ("replication_factor", self.replication_factor),
            ("min_insync_replicas", self.min_insync_replicas),
            ("fetch_interval_ms", self.fetch_interval_ms),
            ("fetch_batch", self.fetch_batch),
            ("commit_timeout_ms", self.commit_timeout_ms),
            ("scan_interval_ms", self.scan_interval_ms),
            ("topology_ttl_ms", self.topology_ttl_ms),
            ("epoch_refresh_ms", self.epoch_refresh_ms),
            ("leader_lease_ms", self.leader_lease_ms),
            ("switch_grace_ms", self.switch_grace_ms),
        ):
            if value <= 0:
                raise ValueError(f"复制配置非法：{name} 必须为正（当前 {value}）")
        if self.switch_grace_ms < self.leader_lease_ms:
            raise ValueError(
                f"复制配置非法：switch_grace_ms={self.switch_grace_ms} 小于 leader_lease_ms={self.leader_lease_ms}"
                "（宽限不足会与旧 leader 写窗口重叠）"
            )
        if self.lag_time_max_ms < 0 or self.lag_records_max < 0:
            raise ValueError("复制配置非法：ISR 阈值不得为负")
        if self.lag_time_max_ms == 0 and self.lag_records_max == 0:
            raise ValueError("复制配置非法：ISR 时间阈值与条数阈值不能同时为 0（等同没有判据）")
        if not self.enabled:
            return
        if not self.nodes:
            raise ValueError(f"复制已开启但 {REPLICATION_NODES_ENV} 为空")
        if self.node_id not in self.nodes:
            raise ValueError(f"复制已开启但本节点 {self.node_id!r} 不在 {REPLICATION_NODES_ENV} 列表内")
        if self.replication_factor > len(self.nodes):
            raise ValueError(f"复制配置非法：replication_factor={self.replication_factor} 大于节点数 {len(self.nodes)}")
        if self.min_insync_replicas > self.replication_factor:
            raise ValueError(
                f"复制配置非法：min_insync_replicas={self.min_insync_replicas} "
                f"大于 replication_factor={self.replication_factor}"
            )


@dataclass(frozen=True)
class PartitionTopology(BaseValueObject):
    """分区拓扑快照（来自 `msg_partition`；行缺失 / `replicas` 为空时由配置兜底）。"""

    replicas: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """副本节点标识（插入序；首个为缺省 leader）。"""

    min_insync_replicas: int = 0
    """分区级最小同步副本数（0 = 继承全局）。"""

    epoch: int = 0
    """分区 leader 任期（02-5 递增；本任务只读与校验）。"""

    leader: str = ""
    """当前 leader（02-5；空串表示按 `replicas` 首个推导）。"""

    switch_state: str = SWITCH_STATE_IDLE
    """切换状态（`idle` / `switching`；02-5）。"""


@dataclass(frozen=True)
class FollowerProgress(BaseValueObject):
    """一个 follower 的拉取进度（ISR 判据的唯一输入）。"""

    leo: int
    """最近一次拉取上报的位点（follower 的下一条待拉 offset）。"""

    last_fetch_ms: float
    """最近一次成功拉取的时刻（单调毫秒）。"""

    in_sync: bool
    """当前是否在 ISR。"""


@dataclass(frozen=True)
class SwitchApplyCommand(BaseValueObject):
    """目标节点激活命令（`LeaderControlTransport.apply_switch` 的载荷；02-5）。"""

    topic_id: int
    partition_no: int
    from_leader: str
    to_leader: str
    to_epoch: int
    watermark: int | None
    """切换水位（旧 leader 真实 HW；None = 未知，走保留）。"""

    truncate: bool
    """是否按水位截断（旧 leader 可达时为 True）。"""

    grace_ms: int
    """停写宽限（毫秒；目标节点从观察到 switching 起计时）。"""


@dataclass(frozen=True)
class SwitchApplyResult(BaseValueObject):
    """目标节点激活结果（02-5）。"""

    to_leo: int
    """激活后的 LEO。"""

    truncated_to: int | None
    """截断后的 LEO；None = 未截断。"""

    retained_tail: int
    """保留的未提交条数（0 = 未保留）。"""

    already_applied: bool
    """是否幂等重复激活。"""


@dataclass(frozen=True)
class ReplicationSnapshot(BaseValueObject):
    """分区复制运行态快照（管理面只读投影）。"""

    leader: str
    """当前 leader 节点标识。"""

    epoch: int
    """当前 leader 任期。"""

    leo: int
    """本节点已落盘末端。"""

    hw: int
    """高水位（消费者可见上限，不含）。"""

    isr: ConcurrentStableFrozenList[str]
    """当前同步副本集合（含 leader，按副本集顺序）。"""

    replicas: ConcurrentStableFrozenList[str]
    """分区副本集（含 leader）。"""

    switch_state: str = SWITCH_STATE_IDLE
    """切换状态（`idle` / `switching`；02-5）。"""

    writable: bool = False
    """本节点当前是否可写（租约内 + idle + 本节点为 leader；02-5）。"""

    last_known_hw: int = 0
    """最后已知的 leader HW（follower 取最近一次拉取结果；02-5 旧 leader 不可达时用于验证）。"""

    last_known_isr: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """最后已知的 leader ISR（02-5 旧 leader 不可达时用于「已提交不丢」验证）。"""

    last_known_at: float = 0.0
    """最后已知运行态的采集时刻（单调毫秒；02-5 判是否新鲜）。"""

    start_offset: int = 0
    """本节点日志可读起点（02-8 迁移预检判能否追平；缺省 0 = 兼容既有构造）。"""


@dataclass(frozen=True)
class ReplicaFetchRequest(BaseValueObject):
    """follower → leader 的拉取请求。"""

    leader: str
    follower: str
    topic_id: int
    partition_no: int
    from_offset: int
    """follower 当前 LEO（下一次要拉的 offset）。"""

    max_messages: int
    epoch: int
    """follower 已知的 leader 任期（leader 据此识别自己是否过期）。"""


@dataclass(frozen=True)
class ReplicaFetchResult(BaseValueObject):
    """leader → follower 的拉取结果。"""

    leader_epoch: int
    from_offset: int
    records: ConcurrentStableList[StoredMessage]
    leader_leo: int
    high_watermark: int
    leader_isr: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """leader 侧当前 ISR（follower 保存为 `last_known_isr`；02-5）。"""


class ReplicaTransport(BaseFrameworkObject, ABC):
    """副本拉取传输端口（02-8 用网络实现替换；本任务只提供进程内实现）。"""

    @abstractmethod
    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """从 leader 拉取一批记录。

        Args:
            request: 拉取请求（含 follower 位点与已知 epoch）。

        Returns:
            ReplicaFetchResult: 记录与 leader 运行态。

        Raises:
            ReplicaUnavailableError: 目标 leader 不可达。
            StaleLeaderError: leader 任期低于 follower 已知任期。
        """


class ReplicationView(BaseFrameworkObject, ABC):
    """复制运行态的只读 + 提交等待视图（`DeliveryService` 的唯一依赖）。"""

    @property
    @abstractmethod
    def enabled(self) -> bool:
        """复制是否开启。

        Returns:
            bool: True 表示需要按 HW 夹取与 `acks=all` 等待。
        """

    @abstractmethod
    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        """`acks=all` 写入前的 ISR 预检。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            acks: 发布确认档位（`"1"` / `"all"`）。

        Raises:
            MsgInsufficientReplicasError: ISR 不足或本节点不是 leader（350032）。
        """

    @abstractmethod
    async def wait_committed(self, *, topic_id: int, partition_no: int, offset: int) -> None:
        """等待某条记录被 ISR 提交（`HW > offset`）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 本批最后一条记录的 offset。

        Raises:
            MsgInsufficientReplicasError: ISR 不足或提交等待超时（350032）。
        """

    @abstractmethod
    async def visible_limit(self, *, topic_id: int, partition_no: int) -> int | None:
        """取消费可见上限（不含）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int | None: 可见上限；None 表示复制关闭、不夹取。
        """

    @abstractmethod
    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """取分区复制运行态快照（管理面）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照；无法挂载时返回 None。
        """


@dataclass(frozen=True)
class ReplicationMetricRow(BaseValueObject):
    """一个分区的复制指标行。"""

    topic_id: int
    partition_no: int
    isr_size: int
    lag_records: int


@dataclass(frozen=True)
class ReplicationMetricsSnapshot(BaseValueObject):
    """复制指标快照（Collector 的唯一输入）。"""

    shrink_total: int
    rows: ConcurrentStableFrozenList[ReplicationMetricRow] = field(
        default_factory=lambda: ConcurrentStableFrozenList[ReplicationMetricRow]()
    )


class ReplicationMetrics(BaseFrameworkObject):
    """复制指标采集点（进程内；Prometheus 出口由 06-3 的 Collector 负责）。"""

    def __init__(self, *, partition_limit: int = REPLICATION_METRICS_PARTITION_LIMIT) -> None:
        """初始化。

        Args:
            partition_limit: 分区基数上限（超出不输出，防指标爆炸）。
        """
        self._partition_limit = partition_limit
        self._shrink_total = 0
        self._rows: ConcurrentStableDict[ReplicaKey, tuple[int, int]] = ConcurrentStableDict()
        self._warned = False

    @property
    def shrink_total(self) -> int:
        """ISR 收缩累计次数。

        Returns:
            int: 累计值。
        """
        return self._shrink_total

    def record_shrink(self) -> None:
        """记一次 ISR 收缩。"""
        self._shrink_total += 1

    def update(self, *, topic_id: int, partition_no: int, isr_size: int, lag_records: int) -> None:
        """更新一个分区的 ISR 大小与副本延迟。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            isr_size: 当前 ISR 大小（含 leader）。
            lag_records: 副本延迟条数（无 follower / 关闭时为 0）。
        """
        key: ReplicaKey = (int(topic_id), int(partition_no))
        if key not in self._rows and len(self._rows) >= self._partition_limit:
            if not self._warned:
                _LOGGER.warning(
                    "msg_replication_metrics_truncated",
                    limit=self._partition_limit,
                    detail="复制指标分区基数达到上限，超出部分不再输出",
                )
                self._warned = True
            return
        self._rows.set(key, (int(isr_size), max(int(lag_records), 0)))

    def forget(self, *, topic_id: int, partition_no: int) -> None:
        """删除一个分区的指标行（分区卸载 / 主题清理）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
        """
        self._rows.pop((int(topic_id), int(partition_no)), None)

    def snapshot(self) -> ReplicationMetricsSnapshot:
        """取指标快照。

        Returns:
            ReplicationMetricsSnapshot: 收缩计数与分区行（插入序）。
        """
        rows: ConcurrentStableList[ReplicationMetricRow] = ConcurrentStableList()
        for (topic_id, partition_no), values in self._rows.items():
            isr_size, lag_records = values
            rows.add(
                ReplicationMetricRow(
                    topic_id=topic_id,
                    partition_no=partition_no,
                    isr_size=isr_size,
                    lag_records=lag_records,
                )
            )
        return ReplicationMetricsSnapshot(
            shrink_total=self._shrink_total,
            rows=ConcurrentStableFrozenList(rows),
        )


class ReplicaTopologyLoader(BaseFrameworkObject, ABC):
    """分区拓扑加载端口（生产实现走平台库；用例注入固定快照）。"""

    @abstractmethod
    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology | None:
        """加载分区拓扑。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过缓存（切换刷新循环用；02-5）。

        Returns:
            PartitionTopology | None: 拓扑；行缺失 / 读取失败返回 None（走配置兜底）。
        """


class PartitionReplication:
    """单个分区的复制运行态（leader / follower 共用同一结构）。"""

    def __init__(
        self,
        *,
        topic_id: int,
        partition_no: int,
        leader: str,
        epoch: int,
        role: ReplicaRole,
        replicas: ConcurrentStableFrozenList[str],
        min_insync_replicas: int,
        local_leo: int,
        hw: int,
        switch_state: str = SWITCH_STATE_IDLE,
        last_known_hw: int = 0,
        last_known_isr: ConcurrentStableFrozenList[str] | None = None,
        leadership_confirmed_at: float = 0.0,
    ) -> None:
        """初始化。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            leader: leader 节点标识。
            epoch: leader 任期。
            role: 本节点角色。
            replicas: 副本集（含 leader）。
            min_insync_replicas: 生效的最小同步副本数。
            local_leo: 本节点已落盘末端。
            hw: 初始高水位。
            switch_state: 已观察到的切换状态（02-5）。
            last_known_hw: 最后已知 leader HW（02-5）。
            last_known_isr: 最后已知 leader ISR（02-5）。
            leadership_confirmed_at: 最近一次确认 leader 身份的单调毫秒（02-5 租约起点）。
        """
        self.topic_id = int(topic_id)
        self.partition_no = int(partition_no)
        self.leader = leader
        self.epoch = int(epoch)
        self.role = role
        self.replicas = replicas
        self.min_insync_replicas = int(min_insync_replicas)
        self.local_leo = int(local_leo)
        self.leader_leo = int(local_leo) if role is ReplicaRole.LEADER else 0
        self.hw = int(hw)
        self.followers: ConcurrentStableDict[str, FollowerProgress] = ConcurrentStableDict()
        self.isr: ConcurrentStableSet[str] = ConcurrentStableSet()
        self.condition = asyncio.Condition()
        self.last_reported_leader_hw = hw
        self.last_reported_leader_leo = 0
        self.switch_state = switch_state
        self.last_known_hw = int(last_known_hw)
        self.last_known_isr: ConcurrentStableSet[str] = ConcurrentStableSet(
            last_known_isr if last_known_isr is not None else ()
        )
        self.last_known_at = float(leadership_confirmed_at)
        self.leadership_confirmed_at = float(leadership_confirmed_at)
        self.switch_observed_at: float | None = None
        self.op_lock = asyncio.Lock()
        self.lease_logged_at: float | None = None

    @property
    def key(self) -> ReplicaKey:
        """分区键。

        Returns:
            ReplicaKey: `(topic_id, partition_no)`。
        """
        return (self.topic_id, self.partition_no)

    @property
    def writable(self) -> bool:
        """本节点当前是否可写（仅按角色与切换状态；租约由协调器叠加判定）。

        Returns:
            bool: 复制关闭或 leader 且 idle 时为 True。
        """
        if self.role is ReplicaRole.DISABLED:
            return True
        return self.role is ReplicaRole.LEADER and self.switch_state == SWITCH_STATE_IDLE

    def in_lease(self, *, now: float, lease_ms: int) -> bool:
        """是否仍在 leader 身份租约内。

        Args:
            now: 当前单调毫秒。
            lease_ms: 租约时长（毫秒）。

        Returns:
            bool: 未超租约为 True。
        """
        return now - self.leadership_confirmed_at <= lease_ms

    def isr_snapshot(self) -> ConcurrentStableFrozenList[str]:
        """按副本集顺序输出当前 ISR（管理面与指标共用）。

        Returns:
            ConcurrentStableFrozenList[str]: ISR 节点标识（插入序）。
        """
        return ConcurrentStableFrozenList(member for member in self.replicas if member in self.isr)

    def lag_records(self) -> int:
        """当前副本延迟条数（leader 视角；无 follower / 非 leader 返回 0）。

        Returns:
            int: `leader_leo - min(ISR follower LEO)`，负值夹为 0。
        """
        if self.role is not ReplicaRole.LEADER:
            return 0
        progress: ConcurrentStableList[int] = ConcurrentStableList(
            self.followers[member].leo for member in self.isr if member in self.followers
        )
        if not progress:
            return 0
        return max(self.leader_leo - min(progress), 0)


class InProcessReplicaTransport(ReplicaTransport):
    """进程内传输：节点标识 → 协调器（用例与同进程多节点）。

    真实网络实现（02-8）只需实现 `fetch`，本类与协调器判定逻辑可以原样复用。
    """

    def __init__(self) -> None:
        """初始化（无节点）。"""
        self._nodes: ConcurrentStableDict[str, ReplicationCoordinator] = ConcurrentStableDict()

    def register(self, node_id: str, coordinator: ReplicationCoordinator) -> None:
        """登记节点（重复登记即替换，便于用例重置）。

        Args:
            node_id: 节点标识。
            coordinator: 该节点的复制协调器。
        """
        self._nodes.set(node_id, coordinator)

    def unregister(self, node_id: str) -> None:
        """移除节点。

        Args:
            node_id: 节点标识。
        """
        self._nodes.pop(node_id, None)

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """向目标 leader 拉取一批记录。

        Args:
            request: 拉取请求。

        Returns:
            ReplicaFetchResult: leader 返回的记录与运行态。

        Raises:
            ReplicaUnavailableError: 目标节点未登记。
        """
        coordinator = self._nodes.get(request.leader)
        if coordinator is None:
            raise ReplicaUnavailableError(f"副本节点不可达：{request.leader}")
        return await coordinator.serve_fetch(request)


class ReplicationCoordinator(ReplicationView):
    """复制协调器（进程级；每分区一份运行态）。"""

    def __init__(
        self,
        *,
        config: ReplicaConfig,
        store: MessageStore,
        transport: ReplicaTransport | None = None,
        topology_loader: ReplicaTopologyLoader | None = None,
        waiter: DeliveryWaiter | None = None,
        metrics: ReplicationMetrics | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """初始化（不挂载分区、不启动后台任务）。

        Args:
            config: 复制配置（已校验）。
            store: 消息存储（必须是**未经复制 / 唤醒包装**的实现，避免自递归）。
            transport: 副本拉取传输（None 表示单机 / 仅 leader）。
            topology_loader: 分区拓扑加载器（None 表示纯配置兜底）。
            waiter: 分区唤醒器（HW 推进时唤醒长轮询）。
            metrics: 复制指标采集点。
            clock: 单调毫秒时钟（用例注入以确定性驱动 ISR 判据）。
        """
        self._config = config
        self._store = store
        self._transport = transport
        self._topology_loader = topology_loader
        self._waiter = waiter
        self._metrics = metrics if metrics is not None else ReplicationMetrics()
        self._clock = clock if clock is not None else _monotonic_ms
        self._partitions: ConcurrentStableDict[ReplicaKey, PartitionReplication] = ConcurrentStableDict()
        self._attach_lock = asyncio.Lock()
        self._tasks: ConcurrentStableList[asyncio.Task[None]] = ConcurrentStableList()
        self._fetch_backoff: ConcurrentStableDict[ReplicaKey, float] = ConcurrentStableDict()
        self._fetch_backoff_delay: ConcurrentStableDict[ReplicaKey, float] = ConcurrentStableDict()
        self._fetch_failure_logged_at: ConcurrentStableDict[ReplicaKey, float] = ConcurrentStableDict()
        self._stopped = False

    @property
    def enabled(self) -> bool:
        """复制是否开启。

        Returns:
            bool: 配置开关值。
        """
        return self._config.enabled

    @property
    def config(self) -> ReplicaConfig:
        """复制配置（排障与用例断言）。

        Returns:
            ReplicaConfig: 配置。
        """
        return self._config

    async def attach_partition(self, *, topic_id: int, partition_no: int) -> PartitionReplication:
        """惰性挂载分区（幂等；首次访问时解析拓扑并初始化运行态）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            PartitionReplication: 分区运行态。
        """
        key: ReplicaKey = (int(topic_id), int(partition_no))
        existing = self._partitions.get(key)
        if existing is not None:
            return existing
        async with self._attach_lock:
            existing = self._partitions.get(key)
            if existing is not None:
                return existing
            state = await self._build_state(topic_id=int(topic_id), partition_no=int(partition_no))
            self._partitions.set(key, state)
            self._metrics.update(
                topic_id=state.topic_id,
                partition_no=state.partition_no,
                isr_size=len(state.isr),
                lag_records=state.lag_records(),
            )
            return state

    async def _build_state(self, *, topic_id: int, partition_no: int) -> PartitionReplication:
        """构造分区运行态（拓扑解析 + 初始 HW）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            PartitionReplication: 新运行态。
        """
        topology = await self._load_topology(topic_id=topic_id, partition_no=partition_no)
        replicas = topology.replicas if topology is not None and topology.replicas else None
        epoch = topology.epoch if topology is not None else 0
        partition_min = topology.min_insync_replicas if topology is not None else 0
        switch_state = topology.switch_state if topology is not None else SWITCH_STATE_IDLE
        if not self._config.enabled:
            chosen: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList((self._config.node_id,))
            role = ReplicaRole.DISABLED
            leader = self._config.node_id
            min_insync = 1
            switch_state = SWITCH_STATE_IDLE
        else:
            chosen = replicas if replicas is not None else self._derive_replicas(topic_id, partition_no)
            leader = topology.leader if topology is not None and topology.leader else chosen[0]
            role = ReplicaRole.LEADER if leader == self._config.node_id else ReplicaRole.FOLLOWER
            min_insync = self._effective_min_insync(partition_min, len(chosen))
        leo = await self._store.leo(topic_id=topic_id, partition_no=partition_no)
        start = await self._store.start_offset(topic_id=topic_id, partition_no=partition_no)
        hw = leo if role is ReplicaRole.DISABLED else start
        state = PartitionReplication(
            topic_id=topic_id,
            partition_no=partition_no,
            leader=leader,
            epoch=epoch,
            role=role,
            replicas=chosen,
            min_insync_replicas=min_insync,
            local_leo=leo,
            hw=hw,
            switch_state=switch_state,
            leadership_confirmed_at=self._clock(),
        )
        if role is not ReplicaRole.DISABLED:
            state.isr.add(leader)
            for member in chosen:
                if member == leader:
                    continue
                state.followers.set(
                    member,
                    FollowerProgress(leo=start, last_fetch_ms=self._clock(), in_sync=True),
                )
                state.isr.add(member)
            if role is ReplicaRole.LEADER:
                state.leader_leo = leo
            else:
                # follower 只保留 leader 下发的 HW（初始为 start，首次拉取后更新）。
                state.hw = min(leo, start)
        else:
            # 关闭复制时 ISR 恒为「本节点」：管理面据此区分「没有复制」与「取不到值」。
            state.isr.add(self._config.node_id)
        return state

    def _effective_min_insync(self, partition_value: int, replica_count: int) -> int:
        """解析生效的最小同步副本数（分区值 > 0 且不超过副本集长度才生效）。

        Args:
            partition_value: 分区列值（0 = 继承全局）。
            replica_count: 副本集长度。

        Returns:
            int: 生效值（1 ~ replica_count）。
        """
        candidate = partition_value if 0 < partition_value <= replica_count else self._config.min_insync_replicas
        if candidate > replica_count:
            _LOGGER.warning(
                "msg_replication_min_insync_clamped",
                configured=candidate,
                replicas=replica_count,
                detail="min_insync_replicas 大于副本集长度，按副本集长度生效",
            )
        return max(1, min(candidate, replica_count))

    def _derive_replicas(self, topic_id: int, partition_no: int) -> ConcurrentStableFrozenList[str]:
        """按配置节点列表为分区推导副本集（确定性轮转）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableFrozenList[str]: 副本节点（插入序，首个为 leader）。
        """
        nodes = self._config.nodes
        if not nodes:  # pragma: no cover - 开启复制时 validate 已保证节点非空
            return ConcurrentStableFrozenList((self._config.node_id,))
        count = min(self._config.replication_factor, len(nodes))
        start = (int(topic_id) * 31 + int(partition_no)) % len(nodes)
        chosen: ConcurrentStableList[str] = ConcurrentStableList()
        for index in range(count):
            chosen.add(nodes[(start + index) % len(nodes)])
        return ConcurrentStableFrozenList(chosen)

    async def _load_topology(
        self, *, topic_id: int, partition_no: int, force: bool = False
    ) -> PartitionTopology | None:
        """加载分区拓扑（加载器失败不抛：走配置兜底并告警）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过加载器缓存（02-5 刷新循环）。

        Returns:
            PartitionTopology | None: 拓扑；缺失 / 失败返回 None。
        """
        if self._topology_loader is None:
            return None
        try:
            return await self._topology_loader.load(topic_id=topic_id, partition_no=partition_no, force=force)
        except Exception as exc:
            _LOGGER.warning(
                "msg_replication_topology_failed",
                topic_id=topic_id,
                partition=partition_no,
                reason=type(exc).__name__,
            )
            return None

    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        """写入前的 leader / 租约预检（`acks=all` 额外判 ISR）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            acks: 发布确认档位（`"1"` / `"all"`）。

        Raises:
            MsgNotLeaderError: 非 leader / 切换中 / 租约过期（350033）。
            MsgInsufficientReplicasError: `acks=all` 且 ISR 不足（350032）。
        """
        if not self._config.enabled:
            return
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        self._ensure_leadership(state)
        if acks != ACKS_PUBLISH_ALL:
            return
        if len(state.isr) < state.min_insync_replicas:
            raise MsgInsufficientReplicasError(
                reason="insufficient_isr",
                isr=len(state.isr),
                required=state.min_insync_replicas,
                topic_id=topic_id,
                partition=partition_no,
            )

    async def ensure_write_allowed(self, *, topic_id: int, partition_no: int) -> None:
        """所有写入（含 `acks=1` / 延迟转投 / 重放）落盘前的 leader fencing。

        复制关闭时直接返回（单实例零变化）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Raises:
            MsgNotLeaderError: 非 leader / 切换中 / 租约过期（350033）。
        """
        if not self._config.enabled:
            return
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        self._ensure_leadership(state)

    def _ensure_leadership(self, state: PartitionReplication) -> None:
        """判本节点是否仍是可写 leader（顺序：角色 → 切换状态 → 租约）。

        Args:
            state: 分区运行态。

        Raises:
            MsgNotLeaderError: 任一条件不满足（350033，`data.reason` 区分原因）。
        """
        if state.role is not ReplicaRole.LEADER:
            raise MsgNotLeaderError(
                reason="not_leader",
                topic_id=state.topic_id,
                partition=state.partition_no,
                leader=state.leader,
                epoch=state.epoch,
            )
        if state.switch_state != SWITCH_STATE_IDLE:
            raise MsgNotLeaderError(
                reason="switching",
                topic_id=state.topic_id,
                partition=state.partition_no,
                leader=state.leader,
                epoch=state.epoch,
            )
        if self._topology_loader is None:
            # 无元数据权威（单实例 / 用例直连）：没有可确认的 leader 身份，不施加租约。
            return
        now = self._clock()
        if state.in_lease(now=now, lease_ms=self._config.leader_lease_ms):
            return
        self._log_lease_expired(state, now=now)
        raise MsgNotLeaderError(
            reason="lease_expired",
            topic_id=state.topic_id,
            partition=state.partition_no,
            leader=state.leader,
            epoch=state.epoch,
        )

    def _log_lease_expired(self, state: PartitionReplication, *, now: float) -> None:
        """记录租约过期（同分区 60s 窗口抑制，避免每次写入刷一条）。

        Args:
            state: 分区运行态。
            now: 当前单调毫秒。
        """
        last = state.lease_logged_at
        if last is not None and now - last < _FAILURE_LOG_WINDOW_MS:
            return
        state.lease_logged_at = now
        _LOGGER.warning(
            "msg_replication_lease_expired",
            topic_id=state.topic_id,
            partition=state.partition_no,
            last_confirmed_ms=int(now - state.leadership_confirmed_at),
        )

    async def wait_committed(self, *, topic_id: int, partition_no: int, offset: int) -> None:
        """等待某条记录被 ISR 提交。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 本批最后一条记录的 offset。

        Raises:
            MsgInsufficientReplicasError: ISR 不足或提交等待超时（350032）。
        """
        if not self._config.enabled:
            return
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        if state.role is not ReplicaRole.LEADER:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._config.commit_timeout_ms / MS_PER_SECOND
        async with state.condition:
            while state.hw <= offset:
                if len(state.isr) < state.min_insync_replicas:
                    raise MsgInsufficientReplicasError(
                        reason="insufficient_isr",
                        isr=len(state.isr),
                        required=state.min_insync_replicas,
                        topic_id=topic_id,
                        partition=partition_no,
                    )
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise MsgInsufficientReplicasError(
                        reason="commit_timeout",
                        isr=len(state.isr),
                        required=state.min_insync_replicas,
                        topic_id=topic_id,
                        partition=partition_no,
                        timeout_ms=self._config.commit_timeout_ms,
                    )
                try:
                    await asyncio.wait_for(state.condition.wait(), remaining)
                except TimeoutError:
                    continue

    async def visible_limit(self, *, topic_id: int, partition_no: int) -> int | None:
        """取消费可见上限（不含）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int | None: 可见上限；复制关闭返回 None。
        """
        if not self._config.enabled:
            return None
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        return min(state.hw, state.local_leo)

    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """取分区复制运行态快照。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照；挂载失败返回 None。
        """
        try:
            state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        except Exception as exc:
            _LOGGER.warning(
                "msg_replication_snapshot_failed",
                topic_id=topic_id,
                partition=partition_no,
                reason=type(exc).__name__,
            )
            return None
        return ReplicationSnapshot(
            leader=state.leader,
            epoch=state.epoch,
            leo=state.local_leo,
            hw=state.hw,
            isr=state.isr_snapshot(),
            replicas=state.replicas,
            switch_state=state.switch_state,
            writable=state.writable
            and (
                self._topology_loader is None
                or state.in_lease(now=self._clock(), lease_ms=self._config.leader_lease_ms)
            ),
            last_known_hw=state.last_known_hw if state.role is not ReplicaRole.LEADER else state.hw,
            last_known_isr=ConcurrentStableFrozenList(
                state.isr_snapshot()
                if state.role is ReplicaRole.LEADER
                else (member for member in state.replicas if member in state.last_known_isr)
            ),
            last_known_at=state.last_known_at,
            start_offset=await self._store.start_offset(topic_id=state.topic_id, partition_no=state.partition_no),
        )

    async def on_append(self, *, topic_id: int, partition_no: int, offset: int) -> None:
        """本节点写入一条记录后推进 LEO / HW（由 `ReplicatedMessageStore` 调用）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新记录的 offset。
        """
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        next_leo = int(offset) + 1
        state.local_leo = max(state.local_leo, next_leo)
        if state.role is ReplicaRole.DISABLED:
            # 复制关闭：HW 恒等于 LEO（管理面据此区分「没有复制」与「取不到值」）。
            state.hw = state.local_leo
            self._metrics.update(
                topic_id=state.topic_id,
                partition_no=state.partition_no,
                isr_size=len(state.isr),
                lag_records=state.lag_records(),
            )
            return
        if state.role is not ReplicaRole.LEADER:
            self._metrics.update(
                topic_id=state.topic_id,
                partition_no=state.partition_no,
                isr_size=len(state.isr),
                lag_records=state.lag_records(),
            )
            return
        state.leader_leo = max(state.leader_leo, next_leo)
        await self._advance_hw(state)

    async def serve_fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """leader 侧服务一次 follower 拉取（记录 follower 进度并推进 HW）。

        Args:
            request: 拉取请求。

        Returns:
            ReplicaFetchResult: 记录与 leader 运行态。

        Raises:
            ReplicaUnavailableError: 本节点不是该分区 leader / follower 不在副本集内。
            StaleLeaderError: 请求方 epoch 高于本节点（本节点是旧 leader）。
            ReplicationDivergedError: follower 位点早于日志起点（需要 02-5 重同步）。
        """
        state = await self.attach_partition(topic_id=request.topic_id, partition_no=request.partition_no)
        if state.role is not ReplicaRole.LEADER:
            raise ReplicaUnavailableError(f"本节点不是分区 leader：{state.key}", reason="not_leader")
        if state.switch_state != SWITCH_STATE_IDLE:
            raise ReplicaUnavailableError(f"分区处于切换窗口，暂停副本拉取：{state.key}", reason="switching")
        if request.epoch > state.epoch:
            raise StaleLeaderError(f"leader 任期过期：本地 {state.epoch} < 请求方 {request.epoch}（分区 {state.key}）")
        if request.follower not in state.followers:
            raise ReplicaUnavailableError(f"follower 不在副本集内：{request.follower}")
        start = await self._store.start_offset(topic_id=state.topic_id, partition_no=state.partition_no)
        if request.from_offset < start:
            raise ReplicationDivergedError(
                f"follower 位点早于日志起点：{request.from_offset} < {start}（分区 {state.key}）"
            )
        if request.from_offset > state.leader_leo:
            raise ReplicationDivergedError(
                f"follower 位点超过 leader LEO：{request.from_offset} > {state.leader_leo}（分区 {state.key}）",
                leader_leo=state.leader_leo,
            )
        self._record_follower_progress(state, follower=request.follower, leo=request.from_offset)
        await self._advance_hw(state)
        available = max(state.leader_leo - request.from_offset, 0)
        records: ConcurrentStableList[StoredMessage] = await self._store.fetch(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            from_offset=request.from_offset,
            max_messages=min(request.max_messages, available),
        )
        return ReplicaFetchResult(
            leader_epoch=state.epoch,
            from_offset=request.from_offset,
            records=records,
            leader_leo=state.leader_leo,
            high_watermark=state.hw,
            leader_isr=state.isr_snapshot(),
        )

    def _record_follower_progress(self, state: PartitionReplication, *, follower: str, leo: int) -> None:
        """记录 follower 上报的位点并按需重入 ISR。

        Args:
            state: 分区运行态。
            follower: follower 节点标识。
            leo: follower 上报的下一条待拉 offset。
        """
        progress = state.followers.get(follower)
        if progress is None:  # pragma: no cover - serve_fetch 已先判在副本集内
            return
        state.followers.set(
            follower,
            FollowerProgress(leo=int(leo), last_fetch_ms=self._clock(), in_sync=progress.in_sync),
        )
        if not progress.in_sync and int(leo) >= state.leader_leo:
            state.followers.set(
                follower,
                FollowerProgress(leo=int(leo), last_fetch_ms=self._clock(), in_sync=True),
            )
            state.isr.add(follower)
            _LOGGER.info(
                "msg_replication_isr_expand",
                topic_id=state.topic_id,
                partition=state.partition_no,
                replica=follower,
                leo=int(leo),
                isr_size=len(state.isr),
            )

    async def _advance_hw(self, state: PartitionReplication) -> None:
        """按 ISR 收敛重算 HW，并在推进时唤醒等待者。

        Args:
            state: 分区运行态。
        """
        computed = self._compute_hw(state)
        if computed <= state.hw:
            self._update_metrics(state)
            return
        # 走到这里必有推进（上方已判 `computed > state.hw`）：唤醒提交等待与长轮询。
        async with state.condition:
            state.hw = max(state.hw, computed)
            state.condition.notify_all()
        if self._waiter is not None:
            self._waiter.notify((state.topic_id, state.partition_no))
        self._update_metrics(state)

    def _compute_hw(self, state: PartitionReplication) -> int:
        """计算当前可提交位点（不修改状态）。

        **只由 leader 调用**（`on_append` / `serve_fetch` / ISR 扫描）：HW 是 leader 的提交判定，
        follower 的 HW 由拉取结果直接写入；复制关闭时 HW 恒等于 LEO，不经过本函数。

        Args:
            state: 分区运行态。

        Returns:
            int: 计算出的 HW（`min(leader LEO, 各 ISR follower LEO)`）。
        """
        positions: ConcurrentStableList[int] = ConcurrentStableList((state.leader_leo,))
        for member in state.isr:
            progress = state.followers.get(member)
            if progress is not None:
                positions.add(progress.leo)
        return min(positions)

    def _update_metrics(self, state: PartitionReplication) -> None:
        """把分区运行态投影到指标采集点。

        Args:
            state: 分区运行态。
        """
        self._metrics.update(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            isr_size=len(state.isr) if state.role is not ReplicaRole.DISABLED else 1,
            lag_records=state.lag_records(),
        )

    async def pull_once(self, *, topic_id: int, partition_no: int) -> int:
        """follower 拉取一轮（用例可直接驱动，不依赖后台循环）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 本轮追加的条数。

        Raises:
            ReplicaUnavailableError: 传输未装配 / leader 不可达。
            StaleLeaderError: leader 任期低于本地已知任期。
            ReplicationDivergedError: 记录 offset 不连续且无法按 `leader_leo` 修复。
        """
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        if state.role is not ReplicaRole.FOLLOWER:
            return 0
        if state.switch_state != SWITCH_STATE_IDLE:
            # 切换窗口不追加：目标节点尚未截断，并发追加会制造新的未提交尾部。
            return 0
        if self._transport is None:
            raise ReplicaUnavailableError("未装配副本传输，无法拉取")
        async with state.op_lock:
            if state.role is not ReplicaRole.FOLLOWER or state.switch_state != SWITCH_STATE_IDLE:
                return 0  # pragma: no cover - 拿到锁后角色已变的并发窗口
            await self._truncate_diverged_tail(state)
            request = ReplicaFetchRequest(
                leader=state.leader,
                follower=self._config.node_id,
                topic_id=state.topic_id,
                partition_no=state.partition_no,
                from_offset=state.local_leo,
                max_messages=self._config.fetch_batch,
                epoch=state.epoch,
            )
            try:
                result = await self._transport.fetch(request)
            except ReplicationDivergedError as exc:
                if exc.leader_leo is None or not await self._truncate_to_leader(state, leader_leo=exc.leader_leo):
                    raise
                return 0
            if result.leader_epoch < state.epoch:
                raise StaleLeaderError(
                    f"leader 任期过期：本地 {state.epoch} > 对端 {result.leader_epoch}（分区 {state.key}）"
                )
            if result.leader_epoch > state.epoch:
                state.epoch = result.leader_epoch
            if result.from_offset != state.local_leo:
                raise ReplicationDivergedError(
                    f"拉取起点不一致：本地 {state.local_leo} != 响应 {result.from_offset}（分区 {state.key}）"
                )
            expected = state.local_leo
            for message in result.records:
                if message.offset != expected:
                    raise ReplicationDivergedError(
                        f"记录 offset 不连续：期望 {expected}，实际 {message.offset}（分区 {state.key}）"
                    )
                appended = await self._store.append(
                    topic_id=message.topic_id,
                    partition_no=message.partition_no,
                    message_id=message.message_id,
                    log_id=message.log_id,
                    key=message.key,
                    payload=message.payload,
                    headers=message.headers,
                    trace=message.trace,
                    published_at=message.published_at,
                    content_type=message.content_type,
                    compression=message.compression,
                    uncompressed_len=message.uncompressed_len,
                    epoch=message.epoch,
                )
                if appended.offset != expected:  # pragma: no cover - 存储实现违约（本地 LEO 与期望不一致）
                    raise ReplicationDivergedError(
                        f"本地追加 offset 不一致：期望 {expected}，实际 {appended.offset}（分区 {state.key}）"
                    )
                expected += 1
            state.local_leo = expected
            visible = min(result.high_watermark, state.local_leo)
            if visible > state.hw:
                async with state.condition:
                    state.hw = max(state.hw, visible)
                    state.condition.notify_all()
                if self._waiter is not None:
                    self._waiter.notify((state.topic_id, state.partition_no))
            state.last_reported_leader_hw = result.high_watermark
            state.last_reported_leader_leo = result.leader_leo
            state.last_known_hw = result.high_watermark
            state.last_known_isr = ConcurrentStableSet(result.leader_isr)
            state.last_known_at = self._clock()
            self._update_metrics(state)
            return len(result.records)

    async def _truncate_diverged_tail(self, state: PartitionReplication) -> None:
        """拉取前按最后已知 leader LEO 截断本地发散尾部（旧 leader 降级后的多余记录）。

        Args:
            state: 分区运行态。

        Raises:
            ReplicationDivergedError: 最后已知 leader 末端早于本地日志起点（无法修复，需全量重同步）。
        """
        leader_leo = state.last_reported_leader_leo
        if leader_leo <= 0 or state.local_leo <= leader_leo:
            return
        if not await self._truncate_to_leader(state, leader_leo=leader_leo):
            raise ReplicationDivergedError(
                f"本地 LEO {state.local_leo} 超过 leader LEO {leader_leo} 且无法截断（分区 {state.key}）",
                leader_leo=leader_leo,
            )

    async def _truncate_to_leader(self, state: PartitionReplication, *, leader_leo: int) -> bool:
        """把本地日志截断到 leader 末端（并发安全由 `op_lock` 保证，调用方已持有）。

        Args:
            state: 分区运行态。
            leader_leo: leader 的权威末端。

        Returns:
            bool: 已截断 True；leader 末端早于本地起点（无法修复）False。
        """
        start = await self._store.start_offset(topic_id=state.topic_id, partition_no=state.partition_no)
        if leader_leo < start:
            return False
        from_leo = state.local_leo
        if from_leo <= leader_leo:
            return True
        await self._store.truncate_to(topic_id=state.topic_id, partition_no=state.partition_no, offset=leader_leo)
        state.local_leo = leader_leo
        state.hw = min(state.hw, leader_leo)
        state.last_reported_leader_leo = leader_leo
        _LOGGER.warning(
            "msg_replication_diverged_truncated",
            topic_id=state.topic_id,
            partition=state.partition_no,
            local_leo=from_leo,
            leader_leo=leader_leo,
        )
        return True

    async def apply_switch(self, command: SwitchApplyCommand) -> SwitchApplyResult:
        """目标节点激活新 leader（截断 / 保留未提交尾部，然后恢复可写）。

        Args:
            command: 切换命令（水位 / 是否截断 / 宽限）。

        Returns:
            SwitchApplyResult: 激活后的 LEO、截断位点与保留条数。

        Raises:
            ReplicaUnavailableError: 本节点不是命令目标（装配错误）。
        """
        state = await self.attach_partition(topic_id=command.topic_id, partition_no=command.partition_no)
        if command.to_leader != self._config.node_id:
            raise ReplicaUnavailableError(f"切换目标不是本节点：{command.to_leader}")
        async with state.op_lock:
            already = (
                state.role is ReplicaRole.LEADER
                and state.epoch == command.to_epoch
                and state.switch_state == SWITCH_STATE_IDLE
            )
            before = await self._store.leo(topic_id=state.topic_id, partition_no=state.partition_no)
            truncated_to: int | None = None
            retained = 0
            if command.truncate and command.watermark is not None and before > command.watermark:
                await self._store.truncate_to(
                    topic_id=state.topic_id, partition_no=state.partition_no, offset=command.watermark
                )
                truncated_to = command.watermark
                _LOGGER.warning(
                    "msg_leader_switch_truncated",
                    topic_id=state.topic_id,
                    partition=state.partition_no,
                    from_leo=before,
                    to_offset=command.watermark,
                    records=before - command.watermark,
                    watermark=command.watermark,
                    old_leader=command.from_leader,
                )
            elif command.watermark is not None:
                retained = max(before - command.watermark, 0)
            state.leader = command.to_leader
            state.epoch = max(state.epoch, int(command.to_epoch))
            state.role = ReplicaRole.LEADER
            state.switch_state = SWITCH_STATE_IDLE
            state.switch_observed_at = None
            state.isr = ConcurrentStableSet((self._config.node_id,))
            state.local_leo = await self._store.leo(topic_id=state.topic_id, partition_no=state.partition_no)
            state.leader_leo = state.local_leo
            state.hw = min(state.hw, state.local_leo)
            # 升主必须按副本集重建 follower 进度表：构造期只登记「非 leader」副本，
            # 原 leader 不在表里会让它的拉取被永久拒绝（同 `_apply_leader_change`）。
            previous: ConcurrentStableDict[str, FollowerProgress] = ConcurrentStableDict(state.followers)
            rebuilt: ConcurrentStableDict[str, FollowerProgress] = ConcurrentStableDict()
            for member in state.replicas:
                if member == self._config.node_id:
                    continue
                progress = previous.get(member)
                rebuilt.set(
                    member,
                    FollowerProgress(
                        leo=progress.leo if progress is not None else state.local_leo,
                        last_fetch_ms=self._clock(),
                        in_sync=False,
                    ),
                )
            state.followers = rebuilt
            state.leadership_confirmed_at = self._clock()
            state.last_known_hw = max(state.last_known_hw, state.hw)
            state.last_known_isr = ConcurrentStableSet((self._config.node_id,))
            state.last_known_at = state.leadership_confirmed_at
            self._update_metrics(state)
            return SwitchApplyResult(
                to_leo=state.local_leo,
                truncated_to=truncated_to,
                retained_tail=retained,
                already_applied=already,
            )

    async def demote(self, *, topic_id: int, partition_no: int, to_leader: str, to_epoch: int) -> None:
        """旧 leader 降级（控制通道可达时立即停写；不可达时靠租约）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader 节点标识。
            to_epoch: 新 leader 任期。
        """
        if not self._config.enabled:
            return
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        async with state.op_lock:
            state.leader = to_leader
            state.epoch = max(state.epoch, int(to_epoch))
            if state.role is ReplicaRole.LEADER:
                state.role = ReplicaRole.FOLLOWER
                state.leader_leo = 0
            state.switch_state = SWITCH_STATE_SWITCHING
            state.switch_observed_at = self._clock()
            state.isr = ConcurrentStableSet((to_leader,))
        async with state.condition:
            state.condition.notify_all()
        _LOGGER.info(
            "msg_replication_demoted",
            topic_id=state.topic_id,
            partition=state.partition_no,
            to_leader=to_leader,
            to_epoch=to_epoch,
        )

    async def refresh_once(self) -> None:
        """强制重载已挂载分区的拓扑（epoch / leader / switch_state），并收敛本节点角色。

        只对已挂载分区查库；加载失败不更新确认时间（超租约后自动拒写）。
        """
        if not self._config.enabled or self._topology_loader is None:
            return
        keys = ConcurrentStableList(self._partitions.keys())
        for key in keys:
            state = self._partitions.get(key)
            if state is None:  # pragma: no cover - 键快照与读取之间的并发卸载窗口
                continue
            topology = await self._load_topology(topic_id=key[0], partition_no=key[1], force=True)
            if topology is None:
                continue
            await self._observe_topology(state, topology)

    async def _observe_topology(self, state: PartitionReplication, topology: PartitionTopology) -> None:
        """把一次拓扑加载结果合入运行态（切换状态、epoch、leader 与租约确认）。

        Args:
            state: 分区运行态。
            topology: 最新拓扑。
        """
        now = self._clock()
        if topology.switch_state == SWITCH_STATE_SWITCHING and state.switch_state != SWITCH_STATE_SWITCHING:
            state.switch_observed_at = now
        elif topology.switch_state == SWITCH_STATE_IDLE:
            state.switch_observed_at = None
        state.switch_state = topology.switch_state
        if topology.epoch > state.epoch:
            state.epoch = topology.epoch
        new_leader = topology.leader or (topology.replicas[0] if topology.replicas else state.leader)
        if new_leader != state.leader:
            await self._apply_leader_change(state, new_leader=new_leader)
        if state.role is ReplicaRole.LEADER and state.switch_state == SWITCH_STATE_IDLE:
            state.leadership_confirmed_at = now
        if state.role is ReplicaRole.FOLLOWER and topology.switch_state == SWITCH_STATE_IDLE:
            state.last_known_at = max(state.last_known_at, now)

    async def _apply_leader_change(self, state: PartitionReplication, *, new_leader: str) -> None:
        """按拓扑变更切换本节点角色（leader ↔ follower）。

        **升主必须重建 followers 表**：构造运行态时只登记「非 leader」副本，升主后原 leader
        不在表中，`serve_fetch` 会以「follower 不在副本集内」永久拒绝其拉取（ISR 卡死、
        `acks=all` 提交超时）。这里按副本集重建进度表（本节点除外），并把 ISR 收敛为自身，
        由后续拉取 / 扫描按真实 LEO 重新扩张。

        Args:
            state: 分区运行态。
            new_leader: 新的 leader 节点标识。
        """
        was_leader = state.role is ReplicaRole.LEADER
        state.leader = new_leader
        state.role = ReplicaRole.LEADER if new_leader == self._config.node_id else ReplicaRole.FOLLOWER
        state.isr = ConcurrentStableSet((new_leader,))
        if state.role is ReplicaRole.LEADER:
            state.leader_leo = state.local_leo
            state.hw = min(state.hw, state.local_leo)
            start = await self._store.start_offset(topic_id=state.topic_id, partition_no=state.partition_no)
            previous: ConcurrentStableDict[str, FollowerProgress] = ConcurrentStableDict(state.followers)
            current: ConcurrentStableDict[str, FollowerProgress] = ConcurrentStableDict()
            for member in state.replicas:
                if member == state.leader:
                    continue
                progress = previous.get(member)
                current.set(
                    member,
                    FollowerProgress(
                        leo=progress.leo if progress is not None else start,
                        last_fetch_ms=self._clock(),
                        in_sync=False,
                    ),
                )
            state.followers = current
            state.isr = ConcurrentStableSet((self._config.node_id,))
        else:
            state.leader_leo = 0
        if was_leader and state.role is ReplicaRole.FOLLOWER:
            _LOGGER.info(
                "msg_replication_demoted_by_refresh",
                topic_id=state.topic_id,
                partition=state.partition_no,
                to_leader=new_leader,
                epoch=state.epoch,
            )
        elif not was_leader and state.role is ReplicaRole.LEADER:
            _LOGGER.info(
                "msg_replication_promoted_by_refresh",
                topic_id=state.topic_id,
                partition=state.partition_no,
                epoch=state.epoch,
            )
        self._update_metrics(state)

    async def ensure_started(self) -> None:
        """启动 follower 拉取、leader ISR 扫描与 epoch 刷新循环（幂等；关闭时不启动）。"""
        if not self._config.enabled or self._stopped or self._tasks:
            return
        self._tasks.add(asyncio.create_task(self._pull_loop()))
        self._tasks.add(asyncio.create_task(self._scan_loop()))
        if self._topology_loader is not None:
            self._tasks.add(asyncio.create_task(self._refresh_loop()))

    async def aclose(self) -> None:
        """停止后台循环（幂等；不写盘）。"""
        self._stopped = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

    async def scan_isr_once(self) -> None:
        """扫描一轮 ISR（用例可直接驱动，不依赖后台循环）。"""
        keys = ConcurrentStableList(self._partitions.keys())
        for key in keys:
            state = self._partitions.get(key)
            if state is None or state.role is not ReplicaRole.LEADER:
                continue
            if state.switch_state != SWITCH_STATE_IDLE:
                continue
            await self._scan_partition(state)

    async def _pull_loop(self) -> None:
        """follower 周期拉取（异常隔离，指数退避封顶；由 `aclose` 取消）。"""
        interval = self._config.fetch_interval_ms / MS_PER_SECOND
        while True:
            await asyncio.sleep(interval)
            keys = ConcurrentStableList(self._partitions.keys())
            for key in keys:
                state = self._partitions.get(key)
                if state is None or state.role is not ReplicaRole.FOLLOWER:
                    continue
                backoff_until = self._fetch_backoff.get(key, 0.0)
                if self._clock() < backoff_until:
                    continue
                try:
                    await self.pull_once(topic_id=key[0], partition_no=key[1])
                    self._fetch_backoff.pop(key, None)
                    self._fetch_backoff_delay.pop(key, None)
                except Exception as exc:
                    self._on_fetch_failure(key, exc)

    def _on_fetch_failure(self, key: ReplicaKey, exc: Exception) -> None:
        """记录拉取失败并按指数退避重试（日志 60s 窗口抑制）。

        Args:
            key: 分区键。
            exc: 异常。
        """
        now = self._clock()
        previous = self._fetch_backoff_delay.get(key)
        base = float(self._config.fetch_interval_ms)
        next_backoff = base if previous is None else min(previous * 2, float(REPLICATION_FETCH_BACKOFF_CAP_MS))
        self._fetch_backoff_delay.set(key, next_backoff)
        self._fetch_backoff.set(key, now + next_backoff)
        last_log = self._fetch_failure_logged_at.get(key)
        if last_log is not None and now - last_log < _FAILURE_LOG_WINDOW_MS:
            return
        self._fetch_failure_logged_at.set(key, now)
        _LOGGER.warning(
            "msg_replication_fetch_failed",
            topic_id=key[0],
            partition=key[1],
            reason=type(exc).__name__,
            detail=str(exc),
            backoff_ms=next_backoff,
        )

    async def _scan_loop(self) -> None:
        """leader 周期扫描 ISR（收缩 / 重入；由 `aclose` 取消）。"""
        interval = self._config.scan_interval_ms / MS_PER_SECOND
        while True:
            await asyncio.sleep(interval)
            await self.scan_isr_once()

    async def _refresh_loop(self) -> None:
        """周期强制重载拓扑（epoch / leader / switch_state；由 `aclose` 取消）。"""
        interval = self._config.epoch_refresh_ms / MS_PER_SECOND
        while True:
            await asyncio.sleep(interval)
            try:
                await self.refresh_once()
            except Exception as exc:  # pragma: no cover - 加载器已自行兜底，这里只防循环中断
                _LOGGER.warning("msg_replication_refresh_failed", reason=type(exc).__name__)

    async def _scan_partition(self, state: PartitionReplication) -> None:
        """扫描一个 leader 分区的 ISR（踢出 / 重入 + 指标）。

        Args:
            state: 分区运行态。
        """
        now = self._clock()
        changed = False
        for member in ConcurrentStableList(state.replicas):
            if member == state.leader:
                continue
            progress = state.followers.get(member)
            if progress is None:  # pragma: no cover - 构造时已为每个副本建进度
                continue
            lag_ms = now - progress.last_fetch_ms
            lag_records = state.leader_leo - progress.leo
            if progress.in_sync and (
                (self._config.lag_time_max_ms > 0 and lag_ms > self._config.lag_time_max_ms)
                or (self._config.lag_records_max > 0 and lag_records > self._config.lag_records_max)
            ):
                state.isr.discard(member)
                state.followers.set(
                    member,
                    FollowerProgress(leo=progress.leo, last_fetch_ms=progress.last_fetch_ms, in_sync=False),
                )
                self._metrics.record_shrink()
                changed = True
                _LOGGER.warning(
                    "msg_replication_isr_shrink",
                    topic_id=state.topic_id,
                    partition=state.partition_no,
                    replica=member,
                    lag_ms=int(lag_ms),
                    lag_records=int(lag_records),
                    isr_size=len(state.isr),
                )
            elif not progress.in_sync and (
                progress.leo >= state.leader_leo
                and (self._config.lag_time_max_ms == 0 or lag_ms <= self._config.lag_time_max_ms)
            ):
                state.isr.add(member)
                state.followers.set(
                    member,
                    FollowerProgress(leo=progress.leo, last_fetch_ms=progress.last_fetch_ms, in_sync=True),
                )
                changed = True
                _LOGGER.info(
                    "msg_replication_isr_expand",
                    topic_id=state.topic_id,
                    partition=state.partition_no,
                    replica=member,
                    leo=progress.leo,
                    isr_size=len(state.isr),
                )
        if changed:
            await self._advance_hw(state)
        else:
            self._update_metrics(state)


class ReplicatedMessageStore(MessageStore):
    """消息存储装饰器：`append` 成功后推进复制运行态（LEO / HW）。

    **包装顺序**：本类在 `NotifyingMessageStore` 内层——先更新复制定位，再唤醒长轮询；
    反过来会出现「等待者被唤醒但 HW 还是旧的」，放大空转。

    **为什么是装饰器**：写入方有三处（普通发布、延迟转投、重放），逐处加必然漏一处；
    包装器把「写入即推进 LEO / HW」收敛成单点。
    """

    def __init__(self, inner: MessageStore, *, replication: ReplicationCoordinator) -> None:
        """初始化。

        Args:
            inner: 被装饰的存储实现（文件段实现）。
            replication: 复制协调器（`append` 后推进其运行态）。
        """
        self._inner = inner
        self._replication = replication

    @property
    def inner(self) -> MessageStore:
        """被包装的存储实现。

        Returns:
            MessageStore: 内层存储。
        """
        return self._inner

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace: TraceContext,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
        epoch: int = 0,
    ) -> StoredMessage:
        """追加一条消息并推进复制运行态（参数与返回值原样透传）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识。
            log_id: 记录标识。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace: 链路字段（06-4）。
            published_at: 发布时间（UTC）。
            content_type: 消息体内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数。
            epoch: leader 任期（02-4）。

        Returns:
            StoredMessage: 含分配到的 `offset` 的完整记录。

        Raises:
            MsgNotLeaderError: 本节点非 leader / 切换中 / 租约过期（350033）。
        """
        await self._replication.ensure_write_allowed(topic_id=topic_id, partition_no=partition_no)
        message = await self._inner.append(
            topic_id=topic_id,
            partition_no=partition_no,
            message_id=message_id,
            log_id=log_id,
            key=key,
            payload=payload,
            headers=headers,
            trace=trace,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
            epoch=epoch,
        )
        await self._replication.on_append(topic_id=topic_id, partition_no=partition_no, offset=message.offset)
        return message

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """取一段消息（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的消息。
        """
        return await self._inner.fetch(
            topic_id=topic_id, partition_no=partition_no, from_offset=from_offset, max_messages=max_messages
        )

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取日志末端位点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO。
        """
        return await self._inner.leo(topic_id=topic_id, partition_no=partition_no)

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取日志起点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。
        """
        return await self._inner.start_offset(topic_id=topic_id, partition_no=partition_no)

    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """取段元数据（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据。
        """
        return await self._inner.segments(topic_id=topic_id, partition_no=partition_no)

    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """前移可读区间起点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新的起点。

        Returns:
            int: 前移后的起点。
        """
        return await self._inner.truncate_before(topic_id=topic_id, partition_no=partition_no, offset=offset)

    async def truncate_to(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """截掉末端记录（原样委托；切换截断由协调器显式驱动）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 保留到的位置（不含）。

        Returns:
            int: 截断后的 LEO。
        """
        return await self._inner.truncate_to(topic_id=topic_id, partition_no=partition_no, offset=offset)

    async def find_by_log_id(self, *, log_id: str) -> MessageHeaderView | None:
        """按记录身份取头视图（原样委托）。

        Args:
            log_id: 记录标识。

        Returns:
            MessageHeaderView | None: 头视图。
        """
        return await self._inner.find_by_log_id(log_id=log_id)

    async def scan_headers(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[MessageHeaderView]:
        """取只读头视图（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[MessageHeaderView]: 头视图。
        """
        return await self._inner.scan_headers(
            topic_id=topic_id, partition_no=partition_no, from_offset=from_offset, max_messages=max_messages
        )

    async def drop_topic(self, *, topic_id: int) -> int:
        """删除主题数据（原样委托）。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 受影响的分区数。
        """
        return await self._inner.drop_topic(topic_id=topic_id)


def _env_bool(name: str, default: bool) -> bool:
    """读取布尔环境变量。

    Args:
        name: 变量名。
        default: 缺省值。

    Returns:
        bool: 解析结果（`1/true/yes/on` 为真，空值取缺省）。
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    """读取整数环境变量。

    Args:
        name: 变量名。
        default: 缺省值。

    Returns:
        int: 解析结果。

    Raises:
        ValueError: 取值不是整数。
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 取值非法：{raw!r}（须为整数）") from exc


def _monotonic_ms() -> float:
    """单调毫秒时钟。

    Returns:
        float: 单调递增毫秒数。
    """
    return time.monotonic() * MS_PER_SECOND
