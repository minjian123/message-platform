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
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_REPLICATION_FETCH_BATCH,
    DEFAULT_REPLICATION_FETCH_INTERVAL_MS,
    DEFAULT_REPLICATION_LAG_RECORDS,
    DEFAULT_REPLICATION_LAG_TIME_MS,
    DEFAULT_REPLICATION_MIN_INSYNC,
    DEFAULT_REPLICATION_NODE_ID,
    DEFAULT_REPLICATION_SCAN_INTERVAL_MS,
    DEFAULT_REPLICATION_TOPOLOGY_TTL_MS,
    MS_PER_SECOND,
    REPLICATION_COMMIT_TIMEOUT_MS_ENV,
    REPLICATION_ENABLED_ENV,
    REPLICATION_FACTOR_ENV,
    REPLICATION_FETCH_BACKOFF_CAP_MS,
    REPLICATION_FETCH_BATCH_ENV,
    REPLICATION_FETCH_INTERVAL_MS_ENV,
    REPLICATION_LAG_RECORDS_ENV,
    REPLICATION_LAG_TIME_MS_ENV,
    REPLICATION_METRICS_PARTITION_LIMIT,
    REPLICATION_MIN_INSYNC_ENV,
    REPLICATION_NODE_ID_ENV,
    REPLICATION_NODES_ENV,
    REPLICATION_SCAN_INTERVAL_MS_ENV,
    REPLICATION_TOPOLOGY_TTL_MS_ENV,
)
from bms_msg.core.trace import TraceContext
from bms_msg.errors import MsgInsufficientReplicasError

__all__ = [
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
]

_LOGGER = get_logger("bms_msg.replication")

_FAILURE_LOG_WINDOW_MS = 60_000
"""拉取失败日志的抑制窗口（毫秒）：同一分区 60s 内只记一条。"""

ReplicaKey = tuple[int, int]
"""分区键：`(topic_id, partition_no)`。"""


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


class StaleLeaderError(ReplicationError):
    """epoch 校验失败：对端 leader 任期低于本地已知任期（旧 leader 的数据一律拒绝）。"""


class ReplicationDivergedError(ReplicationError):
    """副本位点不连续 / 起点不匹配：停止拉取，尾部截断与重同步归 02-5。"""


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
        ):
            if value <= 0:
                raise ValueError(f"复制配置非法：{name} 必须为正（当前 {value}）")
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
    async def load(self, *, topic_id: int, partition_no: int) -> PartitionTopology | None:
        """加载分区拓扑。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

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

    @property
    def key(self) -> ReplicaKey:
        """分区键。

        Returns:
            ReplicaKey: `(topic_id, partition_no)`。
        """
        return (self.topic_id, self.partition_no)

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
        if not self._config.enabled:
            chosen: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList((self._config.node_id,))
            role = ReplicaRole.DISABLED
            leader = self._config.node_id
            min_insync = 1
        else:
            chosen = replicas if replicas is not None else self._derive_replicas(topic_id, partition_no)
            if self._config.node_id in chosen:
                role = ReplicaRole.LEADER if chosen[0] == self._config.node_id else ReplicaRole.FOLLOWER
            else:
                role = ReplicaRole.FOLLOWER
            leader = chosen[0]
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

    async def _load_topology(self, *, topic_id: int, partition_no: int) -> PartitionTopology | None:
        """加载分区拓扑（加载器失败不抛：走配置兜底并告警）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            PartitionTopology | None: 拓扑；缺失 / 失败返回 None。
        """
        if self._topology_loader is None:
            return None
        try:
            return await self._topology_loader.load(topic_id=topic_id, partition_no=partition_no)
        except Exception as exc:
            _LOGGER.warning(
                "msg_replication_topology_failed",
                topic_id=topic_id,
                partition=partition_no,
                reason=type(exc).__name__,
            )
            return None

    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        """`acks=all` 写入前的 ISR 预检。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            acks: 发布确认档位。

        Raises:
            MsgInsufficientReplicasError: 本节点非 leader 或 ISR 不足（350032）。
        """
        if not self._config.enabled or acks != ACKS_PUBLISH_ALL:
            return
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        if state.role is not ReplicaRole.LEADER:
            raise MsgInsufficientReplicasError(
                reason="not_leader",
                isr=len(state.isr),
                required=state.min_insync_replicas,
                topic_id=topic_id,
                partition=partition_no,
            )
        if len(state.isr) < state.min_insync_replicas:
            raise MsgInsufficientReplicasError(
                reason="insufficient_isr",
                isr=len(state.isr),
                required=state.min_insync_replicas,
                topic_id=topic_id,
                partition=partition_no,
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
            raise ReplicaUnavailableError(f"本节点不是分区 leader：{state.key}")
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
                f"follower 位点超过 leader LEO：{request.from_offset} > {state.leader_leo}（分区 {state.key}）"
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
            ReplicationDivergedError: 记录 offset 不连续。
        """
        state = await self.attach_partition(topic_id=topic_id, partition_no=partition_no)
        if state.role is not ReplicaRole.FOLLOWER:
            return 0
        if self._transport is None:
            raise ReplicaUnavailableError("未装配副本传输，无法拉取")
        request = ReplicaFetchRequest(
            leader=state.leader,
            follower=self._config.node_id,
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            from_offset=state.local_leo,
            max_messages=self._config.fetch_batch,
            epoch=state.epoch,
        )
        result = await self._transport.fetch(request)
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
        self._update_metrics(state)
        return len(result.records)

    async def ensure_started(self) -> None:
        """启动 follower 拉取循环与 leader ISR 扫描（幂等；关闭时不启动）。"""
        if not self._config.enabled or self._stopped or self._tasks:
            return
        self._tasks.add(asyncio.create_task(self._pull_loop()))
        self._tasks.add(asyncio.create_task(self._scan_loop()))

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
        """
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
