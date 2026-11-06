"""延迟消息区（01-5 详细设计 §4、§8.2、§8.3、§8.5）：记录 / 端口 / 内存实现 / 发布侧服务 / 只读视图。

三条结构性口径：

1. **延迟期间对日志完全不存在**：延迟消息不进 `MessageStore`，故 `LEO`、位点、积压、停等、死信
   一律不受影响（01-2 / 01-3 / 01-4 的不变量一行都不用改）；到期由推进器 `append`，此刻才分配 offset。
2. **端口只有「存 / 数 / 列 / 删」四类语义**：时间轮是索引实现细节，不在端口里；8B_02 换 `.delay`
   文件段实现时用 `list_all()` 重建、端口签名不变。
3. **记录身份 = `log_id`**（发布时分配、到期转投沿用）：延迟区唯一键，也是 B 段恢复期的去重判据；
   `message_id` 是消息身份，重放会合法地产生同一 `message_id` 的多条记录，不能用它判记录。

**为什么记录里存 `namespace` / `topic` 展示名**：延迟区不保证主题一直存在（可被停用 / 删除）；转投时
仍要能报出「哪条延迟消息没能投出去」，而不是只剩两个雪花主键（详细设计 §4.2）。
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.objects import BaseValueObject
from bms_core.core.roots import BaseFrameworkObject
from bms_core.msgclient.payload import (
    COMPRESSION_NONE,
    DEFAULT_CONTENT_TYPE,
    normalize_compression,
    normalize_content_type,
)

from bms_msg.broker.delivery import resolve_payload
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.watermark import StorageGate
from bms_msg.core.config import (
    DELAY_LEVEL_MS,
    DELAY_MAX_SPAN_MS,
    DELAY_METRICS_KEY_LIMIT,
    DELAY_QUOTA_GLOBAL,
    DELAY_QUOTA_PER_TENANT,
)
from bms_msg.core.message_id import new_message_id
from bms_msg.errors import MsgDelayQuotaExceededError
from bms_msg.schemas.message import PublishRequest
from bms_msg.services.topic import MAX_LIST_SIZE
from bms_msg.tenant.context import CurrentTenant

if TYPE_CHECKING:
    from bms_msg.broker.scheduler import DelayWheel

__all__ = [
    "DELAY_SCOPE_GLOBAL",
    "DELAY_SCOPE_TENANT",
    "DelayInspector",
    "DelayMetrics",
    "DelayRecord",
    "DelayService",
    "DelayStore",
    "DelayedOrder",
    "DelayedRow",
    "InMemoryDelayStore",
    "delay_level_ms",
    "ensure_delay_capacity",
    "ensure_delay_span",
    "with_attempt",
]

DELAY_SCOPE_TENANT = "tenant"
"""容量拒绝范围：该租户的延迟区已满。"""

DELAY_SCOPE_GLOBAL = "global"
"""容量拒绝范围：延迟区全局已满。"""

_OTHER_KEY = "__other__"
"""指标分键溢出后的归并键（与 02-6 / 03-5 同口径）。"""

_TOTAL_KEY = "__total__"
"""指标合计键（读快照时求和，不单独维护）。"""


def _utcnow() -> datetime:
    """当前 UTC 时间（发布时刻与剩余时间的判据；用例注入可推进时钟）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


def delay_level_ms(level: str) -> int:
    """档位名 → 毫秒（档位表是唯一来源；未知档位即 `10001`）。

    契约层（`DelayLevel` 枚举）已拒绝非法值，本函数是服务层的**防御性二次校验**：档位表只增不删、
    存量记录按档位名映射，任何绕过契约的入口（帧协议 / 内部调用）也必须拿到同一判定。

    Args:
        level: 档位名（`1s` / `5s` / `10s` / `30s` / `1m` / `5m` / `10m` / `30m` / `60m`）。

    Returns:
        int: 延迟毫秒数。

    Raises:
        ParamError: 档位不在表内（10001）。
    """
    milliseconds = DELAY_LEVEL_MS.get(level)
    if milliseconds is None:
        raise ParamError(f"延迟档位非法（须为 {', '.join(DELAY_LEVEL_MS)}）：{level}")
    return milliseconds


def ensure_delay_span(record: DelayRecord) -> None:
    """校验延迟跨度不超上限（`10001`）。

    延迟区不是定时任务系统：超长跨度应由业务用「短延迟 + 状态机」表达（详细设计 §5.1）。
    判定放在端口层（`DelayStore.enqueue` 调用本函数），使所有实现与所有入口共用同一上限。

    Args:
        record: 待入队的延迟记录。

    Raises:
        ParamError: 跨度超过 `DELAY_MAX_SPAN_MS`（10001）。
    """
    span = record.deliver_at - record.published_at
    if span > timedelta(milliseconds=DELAY_MAX_SPAN_MS):
        raise ParamError(f"延迟跨度超出上限：{span.total_seconds():.0f}s > {DELAY_MAX_SPAN_MS // 1000}s")


def ensure_delay_capacity(
    *,
    known: bool,
    topic: str,
    tenant_count: int,
    total: int,
    quota: int,
    global_quota: int,
    metrics: DelayMetrics | None = None,
) -> None:
    """容量判定（每租户 + 全局）：两级共用一处口径，两种实现都调用。

    **为什么落在存储实现侧**（而不是发布服务）：端口不新增方法（任务 §2.9）就只能在 `enqueue()` 内判；
    且「判容量」与「写入」在同一原子步里，不会出现「判过之后被并发入队挤爆」。

    **为什么 `known` 直接放行**：覆盖写（同 `log_id`，含转投失败后的 `attempts + 1`）不增总量——
    重试被容量拒绝会把「不丢」变成「延迟区满了就再也投不出去」。

    Args:
        known: 该 `log_id` 是否已在延迟区（覆盖写为 True）。
        topic: 主题名（文案用）。
        tenant_count: 该租户当前条数。
        total: 延迟区总条数。
        quota: 每租户软上限。
        global_quota: 全局软上限。
        metrics: 拒绝计数采集器（缺省不采集）。

    Raises:
        MsgDelayQuotaExceededError: 该租户或全局已满（350015）。
    """
    if known:
        return
    if tenant_count >= quota:
        if metrics is not None:
            metrics.record_rejection(scope=DELAY_SCOPE_TENANT)
        raise MsgDelayQuotaExceededError(
            f"延迟区已满（每租户上限 {quota} 条），请稍后重试或缩短延迟：{topic}",
            data=_quota_payload(scope=DELAY_SCOPE_TENANT, count=tenant_count, limit=quota, tenant_limit=quota),
        )
    if total >= global_quota:
        if metrics is not None:
            metrics.record_rejection(scope=DELAY_SCOPE_GLOBAL)
        raise MsgDelayQuotaExceededError(
            f"延迟区已满（全局上限 {global_quota} 条），请稍后重试或缩短延迟：{topic}",
            data=_quota_payload(scope=DELAY_SCOPE_GLOBAL, count=total, limit=global_quota, tenant_limit=quota),
        )


def _quota_payload(*, scope: str, count: int, limit: int, tenant_limit: int) -> dict[str, object]:
    """`350015` 的 `data` 段（JSON 的 `data` 与帧的 `detail` 共用一份结构）。

    **用普通 dict 而不是 `ConcurrentStableDict`**：`ApiResponse.data` 走 pydantic 序列化，
    自定义 Mapping 会抛 `PydanticSerializationError`（响应退化成 500，客户端连业务码都拿不到）；
    本结构只读、无并发写入需求，普通 dict 已足够。

    Args:
        scope: 拒绝范围（`tenant` / `global`）。
        count: 当前条数。
        limit: 本次触发的上限。
        tenant_limit: 每租户软上限（全局满时也给出，便于判断「是不是只有我们一家」）。

    Returns:
        dict[str, object]: 四个字段。
    """
    return {"scope": scope, "limit": limit, "count": count, "tenant_limit": tenant_limit}


class DelayMetrics(BaseFrameworkObject):
    """延迟区指标采集器（进程级单例；02-7 只做采集点，出口归 06-3）。

    与 03-5 `BackpressureMetrics` / 02-6 `StorageMetrics` 同构：**纯内存、不抛异常**，
    `snapshot()` 只做拷贝。分键有界（租户数上限），溢出部分在快照里折进 `__other__`。
    """

    def __init__(self, *, tenant_key_limit: int = DELAY_METRICS_KEY_LIMIT) -> None:
        """初始化（全零）。

        Args:
            tenant_key_limit: 按租户分键的上限（越界租户只在 `__other__` 里体现）。
        """
        self._tenant_key_limit = tenant_key_limit
        self._tenant_keys: set[str] = set()
        self._pending: dict[str, int] = {}
        self._total = 0
        self._segments = 0
        self._rejected: dict[str, int] = {}

    def record_pending(self, *, tenant_id: int, count: int, total: int) -> None:
        """覆盖记录某租户的待投条数与总数（enqueue / remove 后调用）。

        Args:
            tenant_id: 租户主键。
            count: 该租户当前条数。
            total: 延迟区总条数。
        """
        self._total = total
        key = self._tenant_key(tenant_id)
        if key is None:
            return
        if count == 0:
            self._pending.pop(key, None)
            return
        self._pending[key] = count

    def record_total(self, *, total: int) -> None:
        """只更新延迟区总条数（重建结束时无逐租户变化，或租户已全部清空时使用）。

        Args:
            total: 延迟区总条数。
        """
        self._total = total

    def record_segments(self, *, count: int) -> None:
        """覆盖记录 `.delay` 段数（轮转 / 删段 / 重建结束调用）。

        Args:
            count: 当前段数。
        """
        self._segments = count

    def record_rejection(self, *, scope: str) -> None:
        """记一次 `350015` 拒绝（按 scope 分键）。

        Args:
            scope: `tenant` / `global`。
        """
        self._rejected[scope] = self._rejected.get(scope, 0) + 1

    def snapshot(self) -> ConcurrentStableDict[str, Any]:
        """导出当前指标快照（06-3 消费；用例断言）。

        Returns:
            ConcurrentStableDict[str, Any]: 三项指标（待投 / 段数 / 拒绝计数）。
        """
        pending: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        exposed = sum(self._pending.values())
        if self._total - exposed > 0:
            pending.set(_OTHER_KEY, self._total - exposed)
        for key, value in self._pending.items():
            pending.set(key, value)
        pending.set(_TOTAL_KEY, self._total)
        rejected: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        rejected.set(_TOTAL_KEY, sum(self._rejected.values()))
        for scope, count in self._rejected.items():
            rejected.set(scope, count)
        snapshot: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
        snapshot.set("msg_delay_pending", pending)
        snapshot.set("msg_delay_segments", self._segments)
        snapshot.set("msg_delay_rejected_total", rejected)
        return snapshot

    def _tenant_key(self, tenant_id: int) -> str | None:
        """把租户主键归一成有界分键。

        Args:
            tenant_id: 租户主键。

        Returns:
            str | None: 分键；超出分键上限返回 None（该租户只在 `__other__` 里体现）。
        """
        key = str(int(tenant_id))
        if key in self._tenant_keys:
            return key
        if len(self._tenant_keys) >= self._tenant_key_limit:
            return None
        self._tenant_keys.add(key)
        return key


@dataclass(frozen=True)
class DelayRecord(BaseValueObject):
    """延迟区中的一条记录（不可变快照；重试时整条替换，`attempts` 递增）。

    到期 `append` 时除 `offset` 外逐字段搬到 `StoredMessage`：`message_id` / `log_id` / `key` /
    `payload` / `headers` / `trace_id` 原样沿用，`published_at` 取 `deliver_at`（保留期自到期起算）。
    """

    log_id: str
    """记录标识（发布时分配，到期转投沿用；延迟区内唯一键、B 段去重判据）。"""

    message_id: str
    """消息标识（重投 / 转投 / 重放都不变）。"""

    tenant_id: int
    """租户主键（配额按租户计数）。"""

    topic_id: int
    """主题主键（转投按分区）。"""

    namespace: str
    """命名空间（展示名；视图不必回查主题表即可展示）。"""

    topic: str
    """主题名（展示名；主题删除后仍可排障）。"""

    partition_no: int
    """**发布时锁定的目标分区**（扩分区后同 key 不漂移）。"""

    delay_level: str
    """档位名（视图按档位分布统计；存名不存毫秒，避免档位调整后语义漂移）。"""

    key: str | None
    """分区键（可空；原样带入日志）。"""

    payload: bytes
    """业务字节（延迟区不解析，到期原样 `append`）。"""

    headers: ConcurrentStableDict[str, str]
    """结构化头（trace / 业务标记）。"""

    trace_id: str | None
    """链路标识（排障）。"""

    published_at: datetime
    """原始发布时刻（UTC；用于跨度校验与排障，**不等于**日志记录的 `published_at`）。"""

    deliver_at: datetime
    """到期时刻（UTC；判定 `deliver_at <= now` 才转投）。"""

    attempts: int = 0
    """转投失败次数（只计数与告警，**不作为丢弃依据**）。"""

    content_type: str = DEFAULT_CONTENT_TYPE
    """消息体内容类型（03-3：到期转投时原样带入日志记录）。"""

    compression: str = COMPRESSION_NONE
    """压缩算法（03-3：`none` 表示未压缩；延迟区不解析也不解压）。"""

    uncompressed_len: int | None = None
    """解压后字节数（生产者可选声明；随记录带入日志）。"""


@dataclass(frozen=True)
class DelayedOrder(BaseValueObject):
    """一次延迟发布的结果（尚无 offset：此刻消息未进入 offset 空间）。"""

    message_id: str
    """消息标识。"""

    log_id: str
    """记录标识（审计可凭它在发布成功这一刻就钉住记录）。"""

    partition_no: int
    """发布时锁定的分区号。"""

    deliver_at: datetime
    """到期时刻（UTC）。"""


@dataclass(frozen=True)
class DelayedRow(BaseValueObject):
    """待投列表行（延迟记录 + 剩余时间）。"""

    record: DelayRecord
    """延迟记录快照。"""

    remaining_ms: int
    """剩余时间（毫秒；已到期未转投为 0）。"""


class DelayStore(ABC):
    """延迟区存储端口（存 / 数 / 列 / 删；02-7 换 `.delay` 文件段实现，签名不变）。

    **实现义务**：`enqueue` 必须先调 `ensure_delay_span()`（跨度上限 `10001` 对所有实现一致）与
    `ensure_delay_capacity()`（每租户 + 全局软上限，`350015`），并按 `log_id` 幂等覆盖
    （重试更新 `attempts` 走同一入口）；`quota` / `global_quota` 是本实现承载的两级软上限。
    """

    quota: int
    """每租户软上限（条）；发布侧据此判 `350015`。"""

    global_quota: int
    """全局软上限（条）；由 `enqueue()` 判定（端口不新增方法）。"""

    @abstractmethod
    async def enqueue(self, record: DelayRecord) -> None:
        """写入 / 覆盖一条延迟记录（同 `log_id` 即更新）。

        Args:
            record: 延迟记录。

        Raises:
            ParamError: 延迟跨度超出上限（10001）。
        """

    @abstractmethod
    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """取某租户在延迟区中的记录数（配额判据）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 记录条数。
        """

    @abstractmethod
    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[DelayRecord]:
        """列出某租户的延迟记录（只读视图；按到期时刻升序）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录（`deliver_at` 升序，同刻按 `log_id` 升序）。
        """

    @abstractmethod
    async def list_all(self) -> ConcurrentStableList[DelayRecord]:
        """列出全部延迟记录（B 段启动重建时间轮的入口）。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录（`deliver_at` 升序，同刻按 `log_id` 升序）。
        """

    @abstractmethod
    async def remove(self, *, log_id: str) -> None:
        """按记录标识删除（转投成功后调用；不存在即无操作）。

        Args:
            log_id: 记录标识。
        """


class InMemoryDelayStore(DelayStore):
    """内存延迟区（轻量替身；生产装配走 `broker/delay_file.py::FileDelayStore`）。

    计数与过滤都是全量扫描：条数有软上限（每租户 10 万），进程内遍历可接受（01-5 详细设计 §12 风险 5）。
    容量判定与文件实现共用 `ensure_delay_capacity()`——契约用例对两实现参数化，口径必须一致。
    """

    def __init__(self, *, quota: int = DELAY_QUOTA_PER_TENANT, global_quota: int = DELAY_QUOTA_GLOBAL) -> None:
        """初始化。

        Args:
            quota: 每租户软上限（条）；用例注入小值即可验证 `350015`，不必造十万条。
            global_quota: 全局软上限（条）；用例注入小值即可验证全局拒绝。
        """
        self.quota = quota
        self.global_quota = global_quota
        self._records: dict[str, DelayRecord] = {}

    async def enqueue(self, record: DelayRecord) -> None:
        """写入 / 覆盖一条延迟记录（先校验跨度与容量）。

        Args:
            record: 延迟记录。

        Raises:
            ParamError: 延迟跨度超出上限（10001）。
            MsgDelayQuotaExceededError: 该租户或全局延迟区已满（350015）。
        """
        ensure_delay_span(record)
        ensure_delay_capacity(
            known=record.log_id in self._records,
            topic=record.topic,
            tenant_count=await self.count_for_tenant(tenant_id=record.tenant_id),
            total=len(self._records),
            quota=self.quota,
            global_quota=self.global_quota,
        )
        self._records[record.log_id] = record

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """取某租户的记录数。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 记录条数。
        """
        return sum(1 for record in self._records.values() if record.tenant_id == tenant_id)

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[DelayRecord]:
        """列出某租户的记录（到期时刻升序）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录。
        """
        matched = [record for record in self._records.values() if record.tenant_id == tenant_id]
        return ConcurrentStableList(sorted(matched, key=lambda record: (record.deliver_at, record.log_id)))

    async def list_all(self) -> ConcurrentStableList[DelayRecord]:
        """列出全部记录（到期时刻升序）。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录。
        """
        return ConcurrentStableList(
            sorted(self._records.values(), key=lambda record: (record.deliver_at, record.log_id))
        )

    async def remove(self, *, log_id: str) -> None:
        """按记录标识删除（不存在即无操作）。

        Args:
            log_id: 记录标识。
        """
        self._records.pop(log_id, None)


class DelayService:
    """延迟发布服务（请求级：主题仓储 + 进程级延迟区 / 时间轮）。

    判定顺序（01-5 详细设计 §8.3，02-6 / 02-7 各插一步）：命名空间（`350010`）→ 主题（`350001`）→
    分区锁定（`350009`）→ **存储水位**（`350022`，02-6）→ 档位映射（`10001`）→ 生成 ID →
    **容量**（`350015`，02-7 起由存储实现的 `enqueue()` 判：每租户 + 全局两级、覆盖写放行）→ 入队 + 装填。
    容量排在「无效请求全部拒绝完之后」：否则「档位非法」会被「延迟区已满」盖住，排障方向就错了。
    水位排在容量**之前**：盘满时容量已无意义（系统性故障优先），且延迟消息不得「先收进延迟区再丢」。
    """

    def __init__(
        self,
        *,
        store: DelayStore,
        router: TopicRouter,
        wheel: DelayWheel,
        now: Callable[[], datetime] = _utcnow,
        gate: StorageGate | None = None,
    ) -> None:
        """初始化。

        Args:
            store: 延迟区存储（进程级）。
            router: 主题路由（请求级，绑定平台库主题仓储）。
            wheel: 时间轮（与推进器共用同一个实例：装填即入索引）。
            now: 时钟（缺省 UTC 当前时间；用例注入固定时钟）。
            gate: 存储水位闸门（02-6；None 表示不判）。
        """
        self._store = store
        self._router = router
        self._wheel = wheel
        self._now = now
        self._gate = gate

    async def publish(self, *, tenant: CurrentTenant, request: PublishRequest) -> DelayedOrder:
        """发布一条延迟消息（入延迟区 + 装填时间轮，**不分配 offset**）。

        Args:
            tenant: 当前租户（数据面经 `frame_tenant()` 解析）。
            request: 发布请求（调用方保证 `delay_level` 非空：数据面按该字段分派到本服务）。

        Returns:
            DelayedOrder: `message_id` / `log_id` / 锁定分区 / 到期时刻。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 主题分区数非正（350009）。
            MsgStorageWatermarkExceededError: 存储水位超拒绝阈值或数据卷不可用（350022 / 507，02-6）。
            ParamError: 档位非法 / 载荷非合法 base64（10001）。
            MsgDelayQuotaExceededError: 该租户或全局延迟区已满（350015，由存储侧判定）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.route(topic, request.key)
        level = cast("str", request.delay_level)
        delay_ms = delay_level_ms(level)
        if self._gate is not None:
            # 02-6：延迟发布同样是「外部生产」，盘满时直接拒——不能先收进延迟区再丢（任务 §2.3）。
            # 排在档位映射之后：非法档位属契约层（`10001`），不能被系统性故障码盖住；
            # 排在容量判定之前：盘满时算配额已无意义。
            self._gate.ensure_writable()
        published_at = self._now()
        record = DelayRecord(
            log_id=new_message_id(),
            message_id=new_message_id(),
            tenant_id=int(tenant.tenant_id),
            topic_id=int(topic.id),
            namespace=topic.namespace,
            topic=topic.name,
            partition_no=partition_no,
            delay_level=level,
            key=request.key,
            payload=resolve_payload(request),
            headers=ConcurrentStableDict(request.headers.items()),
            trace_id=request.trace_id,
            published_at=published_at,
            deliver_at=published_at + timedelta(milliseconds=delay_ms),
            content_type=normalize_content_type(request.content_type),
            compression=normalize_compression(request.compression),
            uncompressed_len=request.uncompressed_len,
        )
        await self._store.enqueue(record)
        self._wheel.place(record)
        return DelayedOrder(
            message_id=record.message_id,
            log_id=record.log_id,
            partition_no=record.partition_no,
            deliver_at=record.deliver_at,
        )


class DelayInspector:
    """延迟区只读投影（请求级：主题仓储 + 进程级延迟区）。

    **只读**：不改期、不取消、不触发转投（`ensure_started` 只由发布路径调用）。
    过滤值给定时与发布链路**同码同判据**：命名空间 `350010`、主题 `350001`、分区越界 `350009`、
    档位非法 `10001`、分页越界 `10001`——排障时两侧结论一致。分区过滤需与主题同时给定才校验归属
    （单独给分区号时没有主题可判）。
    """

    def __init__(self, *, store: DelayStore, router: TopicRouter, now: Callable[[], datetime] = _utcnow) -> None:
        """初始化。

        Args:
            store: 延迟区存储（进程级）。
            router: 主题路由（请求级；过滤值校验与主题归属判定）。
            now: 时钟（剩余时间判据）。
        """
        self._store = store
        self._router = router
        self._now = now

    async def list_delayed(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str | None,
        topic: str | None,
        partition: int | None,
        level: str | None,
        page: int,
        size: int,
    ) -> tuple[ConcurrentStableList[DelayedRow], int]:
        """分页列出延迟中消息（到期时刻升序，最急的在前）。

        Args:
            tenant: 当前租户（按租户隔离）。
            namespace: 命名空间过滤；None 不限制（给定时同样校验合法性）。
            topic: 主题名过滤；None 不限制（与命名空间同时给定时校验存在性）。
            partition: 分区号过滤；None 不限制（与主题同时给定时校验越界）。
            level: 档位过滤；None 不限制。
            page: 页码（从 1 起）。
            size: 每页条数（1~100）。

        Returns:
            tuple[ConcurrentStableList[DelayedRow], int]: 当页行 + 过滤后总数。

        Raises:
            MsgNamespaceInvalidError: 命名空间过滤值非法（350010）。
            MsgTopicNotFoundError: 主题过滤值不存在或未启用（350001）。
            MsgPartitionCountInvalidError: 分区过滤值越界（350009）。
            ParamError: 档位非法 / 分页参数越界（10001）。
        """
        if namespace is not None:
            tenant.ensure_namespace(namespace)
        if topic is not None and namespace is not None:
            resolved = await self._router.resolve(namespace=namespace, name=topic)
            if partition is not None:
                self._router.partition(resolved, partition)
        if level is not None:
            delay_level_ms(level)
        if page < 1:
            raise ParamError(f"页码须从 1 起：{page}")
        if not 1 <= size <= MAX_LIST_SIZE:
            raise ParamError(f"每页条数须在 1~{MAX_LIST_SIZE}：{size}")
        now = self._now()
        records = await self._store.list_for_tenant(tenant_id=int(tenant.tenant_id))
        rows: ConcurrentStableList[DelayedRow] = ConcurrentStableList()
        for record in records:
            if namespace is not None and record.namespace != namespace:
                continue
            if topic is not None and record.topic != topic:
                continue
            if partition is not None and record.partition_no != partition:
                continue
            if level is not None and record.delay_level != level:
                continue
            rows.add(DelayedRow(record=record, remaining_ms=_remaining_ms(record=record, now=now)))
        total = len(rows)
        return ConcurrentStableList(list(rows)[(page - 1) * size : page * size]), total


def _remaining_ms(*, record: DelayRecord, now: datetime) -> int:
    """剩余时间（毫秒；已到期未转投为 0，不返回负数——视图不承担转投职责）。

    Args:
        record: 延迟记录。
        now: 当前时刻。

    Returns:
        int: 剩余毫秒数（>= 0）。
    """
    return max(0, int((record.deliver_at - now).total_seconds() * 1000))


def with_attempt(record: DelayRecord) -> DelayRecord:
    """返回 `attempts + 1` 的新记录（转投失败时整条替换：只计数，不丢弃）。

    Args:
        record: 原记录。

    Returns:
        DelayRecord: 计数加一后的记录。
    """
    return replace(record, attempts=record.attempts + 1)
