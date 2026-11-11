"""管理面只读投影（06-1-1）：分区详情 / 消息检索 / 概览聚合 / 延迟汇总。

**只读**：不写日志、不写位点、不触发转投；所有取数经 `MessageStore` / `OffsetStore` /
`DelayStore` 端口与 06-3 采样快照，不在请求内全量扫描指标事实源。

**租户边界**：`log_id` 索引是全局进程内 LRU，命中后必须比对 `topic_id` 与当前主题，
否则会成为跨租户直读入口。
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE
from bms_msg.acl.service import TopicAclService
from bms_msg.broker.delay import DelayRecord, DelayStore
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageHeaderView, MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replication import ReplicationView
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.watermark import StorageMetrics
from bms_msg.core.config import DELAY_QUOTA_PER_TENANT
from bms_msg.errors import MsgGroupNotFoundError
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.repositories.topic import PartitionRepository
from bms_msg.schemas.message import DelayLevel
from bms_msg.services.topic import MAX_LIST_SIZE
from bms_msg.tenant.context import CurrentTenant

__all__ = [
    "DelaySummaryInspector",
    "DelaySummaryView",
    "LevelCount",
    "MessageInspector",
    "OverviewInspector",
    "OverviewView",
    "PartitionDetailView",
    "PartitionInspector",
    "PartitionView",
]

_SCAN_BATCH = 500
"""消息检索单次批读上限（避免一个请求把整段日志读进内存）。"""

_NEAR_QUOTA_RATIO = 0.8
"""延迟区软上限占比达到该值即给「接近上限」提示。"""

_PAIR_SEPARATOR = "\x1f"
"""06-3 快照键内分隔符（与 `metrics/lag.py::topic_key` 一致）。"""


@dataclass(frozen=True)
class PartitionView(BaseValueObject):
    """单分区只读运行态（HW / ISR 来自复制协调器；复制关闭时 `hw=LEO`、`isr=[本节点]`）。"""

    partition_no: int
    leader: str | None
    leo: int
    committed: int | None
    lag: int | None
    owner: str | None
    hw: int | None = None
    isr: ConcurrentStableList[str] = field(default_factory=lambda: ConcurrentStableList[str]())


@dataclass(frozen=True)
class PartitionDetailView(BaseValueObject):
    """分区详情投影（分区总数 + 明细）。"""

    partition_count: int
    items: ConcurrentStableList[PartitionView]


@dataclass(frozen=True)
class OverviewView(BaseValueObject):
    """概览四张数字卡 + 快照可用性。"""

    messages_in_per_second: float
    consumer_lag: int
    dlq_total: int
    disk_used_ratio: float
    snapshot_available: bool


@dataclass(frozen=True)
class LevelCount(BaseValueObject):
    """一个延迟档位的分布。"""

    level: str
    count: int
    min_remaining_ms: int


@dataclass(frozen=True)
class DelaySummaryView(BaseValueObject):
    """延迟区汇总（与待投列表同一份当前租户快照）。"""

    total: int
    levels: ConcurrentStableList[LevelCount]
    stale_count: int
    quota_used_ratio: float
    hint: str


def _remaining_ms(*, record: DelayRecord, now: datetime) -> int:
    """剩余毫秒数（与 `DelayInspector` 同口径：已到期未转投为 0，不返回负数）。

    Args:
        record: 延迟记录。
        now: 服务端当前时刻。

    Returns:
        int: 剩余毫秒数。
    """
    return max(0, int((record.deliver_at - now).total_seconds() * 1000))


def _as_utc(value: datetime) -> datetime:
    """把时间统一到 UTC 可比较形态（输入可能是不带时区的 ISO 串）。

    Args:
        value: 待归一的时间。

    Returns:
        datetime: UTC 时间。
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _matches(
    view: MessageHeaderView,
    *,
    offset_from: int | None,
    offset_to: int | None,
    key: str | None,
    time_from: datetime | None,
    time_to: datetime | None,
    log_id: str | None,
) -> bool:
    """跨类相与过滤（判据与 `ReplayWindow.matches()` 同口径：区间左闭右开、key 精确相等）。

    只读头视图不是 `StoredMessage`，故在投影内实现同一组判据，避免为复用而伪造载荷对象。

    Args:
        view: 只读头视图。
        offset_from: 起始 offset（含）。
        offset_to: 结束 offset（不含）。
        key: 分区键精确值。
        time_from: 发布时间下界（含）。
        time_to: 发布时间上界（不含）。
        log_id: 记录身份精确值。

    Returns:
        bool: 是否全部命中。
    """
    if offset_from is not None and view.offset < offset_from:
        return False
    if offset_to is not None and view.offset >= offset_to:
        return False
    if key is not None and view.key != key:
        return False
    published = _as_utc(view.published_at)
    if time_from is not None and published < _as_utc(time_from):
        return False
    if time_to is not None and published >= _as_utc(time_to):
        return False
    return not (log_id is not None and view.log_id != log_id)


class PartitionInspector(BaseFrameworkObject):
    """分区详情只读投影（实时 LEO + 已提交位点 + 积压）。"""

    def __init__(
        self,
        *,
        router: TopicRouter,
        messages: MessageStore,
        offsets: OffsetStore,
        groups: GroupStore,
        partitions: PartitionRepository,
        replication: ReplicationView | None = None,
    ) -> None:
        """初始化。

        Args:
            router: 主题解析。
            messages: 消息存储（实时 LEO）。
            offsets: 位点存储（已提交位点）。
            groups: 组存储（分区持有者）。
            partitions: 分区元数据仓储（leader）。
            replication: 复制运行态视图（02-4；None 表示复制组件未装配，hw / isr 为空）。
        """
        self._router = router
        self._messages = messages
        self._offsets = offsets
        self._groups = groups
        self._partitions = partitions
        self._replication = replication

    async def describe(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str,
        topic: str,
        partition: int | None,
        group: str | None,
    ) -> PartitionDetailView:
        """取分区明细（按分区号升序）。

        Args:
            tenant: 当前租户。
            namespace: 命名空间。
            topic: 主题名。
            partition: 分区号过滤（None 为全部）。
            group: 消费组（None 时 `committed` / `lag` / `owner` 为 null）。

        Returns:
            PartitionDetailView: 分区总数与明细。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记（350010）。
            MsgTopicNotFoundError: 主题不存在（350001）。
            MsgGroupNotFoundError: 组不存在（350013）。
        """
        tenant.ensure_namespace(namespace)
        resolved = await self._router.resolve(namespace=namespace, name=topic)
        topic_id = int(resolved.id)
        if partition is not None:
            self._router.partition(resolved, partition)
        record = None
        if group is not None:
            record = await self._groups.load(tenant_id=int(tenant.tenant_id), topic_id=topic_id, group=group)
            if record is None:
                raise MsgGroupNotFoundError(f"消费组不存在：{group}")
        rows = await self._partitions.list_by_topic(topic_id)
        leaders: ConcurrentStableDict[int, str] = ConcurrentStableDict()
        for row in rows:
            leaders.set(int(row.partition_no), str(row.leader))
        result: ConcurrentStableList[PartitionView] = ConcurrentStableList()
        for number in range(int(resolved.partition_count)):
            if partition is not None and number != partition:
                continue
            leo = await self._messages.leo(topic_id=topic_id, partition_no=number)
            committed: int | None = None
            lag: int | None = None
            owner: str | None = None
            if record is not None:
                committed = await self._offsets.get(group=group or "", topic_id=topic_id, partition_no=number)
                lag = leo - committed
                owner = record.owner_of(number)
            snapshot = (
                None
                if self._replication is None
                else await self._replication.snapshot(topic_id=topic_id, partition_no=number)
            )
            result.add(
                PartitionView(
                    partition_no=number,
                    leader=snapshot.leader if snapshot is not None else leaders.get(number),
                    leo=leo,
                    committed=committed,
                    lag=lag,
                    owner=owner,
                    hw=snapshot.hw if snapshot is not None else None,
                    isr=ConcurrentStableList(snapshot.isr) if snapshot is not None else ConcurrentStableList(),
                )
            )
        return PartitionDetailView(partition_count=int(resolved.partition_count), items=result)


class MessageInspector(BaseFrameworkObject):
    """消息检索只读投影（`log_id` 精确命中走索引，其余条件仍相与）。"""

    def __init__(self, *, router: TopicRouter, messages: MessageStore) -> None:
        """初始化。

        Args:
            router: 主题解析。
            messages: 消息存储（只读头视图）。
        """
        self._router = router
        self._messages = messages

    async def search(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str,
        topic: str,
        partition: int | None,
        offset_from: int | None,
        offset_to: int | None,
        key: str | None,
        time_from: datetime | None,
        time_to: datetime | None,
        log_id: str | None,
        page: int,
        size: int,
    ) -> tuple[ConcurrentStableList[MessageHeaderView], int]:
        """按组合条件检索消息头（分页在合并后的有序序列上切）。

        Args:
            tenant: 当前租户。
            namespace: 命名空间。
            topic: 主题名。
            partition: 分区号过滤（None 为全部）。
            offset_from: 起始 offset（含）。
            offset_to: 结束 offset（不含）。
            key: 分区键精确值。
            time_from: 发布时间下界（含）。
            time_to: 发布时间上界（不含）。
            log_id: 记录身份精确值。
            page: 页码（从 1 起）。
            size: 每页条数。

        Returns:
            tuple[ConcurrentStableList[MessageHeaderView], int]: 当页头视图与过滤后总数。

        Raises:
            ParamError: 条件区间倒置或分页越界（10001）。
            MsgNamespaceInvalidError: 命名空间未登记（350010）。
            MsgTopicNotFoundError: 主题不存在（350001）。
        """
        tenant.ensure_namespace(namespace)
        resolved = await self._router.resolve(namespace=namespace, name=topic)
        topic_id = int(resolved.id)
        if partition is not None:
            self._router.partition(resolved, partition)
        self._ensure_conditions(offset_from, offset_to, time_from, time_to, page, size)
        if log_id is not None:
            found = await self._messages.find_by_log_id(log_id=log_id)
            if found is None or found.topic_id != topic_id:
                return ConcurrentStableList(), 0
            if partition is not None and found.partition_no != partition:
                return ConcurrentStableList(), 0
            if not _matches(
                found,
                offset_from=offset_from,
                offset_to=offset_to,
                key=key,
                time_from=time_from,
                time_to=time_to,
                log_id=log_id,
            ):
                return ConcurrentStableList(), 0
            return ConcurrentStableList([found]), 1
        collected: ConcurrentStableList[MessageHeaderView] = ConcurrentStableList()
        numbers = range(partition, partition + 1) if partition is not None else range(int(resolved.partition_count))
        for number in numbers:
            await self._scan_partition(
                collected=collected,
                topic_id=topic_id,
                number=number,
                offset_from=offset_from,
                offset_to=offset_to,
                key=key,
                time_from=time_from,
                time_to=time_to,
            )
        total = len(collected)
        return ConcurrentStableList(list(collected)[(page - 1) * size : page * size]), total

    async def _scan_partition(
        self,
        *,
        collected: ConcurrentStableList[MessageHeaderView],
        topic_id: int,
        number: int,
        offset_from: int | None,
        offset_to: int | None,
        key: str | None,
        time_from: datetime | None,
        time_to: datetime | None,
    ) -> None:
        """扫描单个分区的可读区间并追加命中记录。

        Args:
            collected: 命中集合（原地追加）。
            topic_id: 主题主键。
            number: 分区号。
            offset_from: 起始 offset（含）。
            offset_to: 结束 offset（不含）。
            key: 分区键精确值。
            time_from: 发布时间下界（含）。
            time_to: 发布时间上界（不含）。
        """
        start = await self._messages.start_offset(topic_id=topic_id, partition_no=number)
        leo = await self._messages.leo(topic_id=topic_id, partition_no=number)
        begin = max(start, offset_from) if offset_from is not None else start
        end = min(leo, offset_to) if offset_to is not None else leo
        cursor = begin
        while cursor < end:
            batch = await self._messages.scan_headers(
                topic_id=topic_id,
                partition_no=number,
                from_offset=cursor,
                max_messages=min(_SCAN_BATCH, end - cursor),
            )
            if not batch:
                break
            for view in batch:
                if view.offset >= end:
                    break
                if _matches(
                    view,
                    offset_from=offset_from,
                    offset_to=offset_to,
                    key=key,
                    time_from=time_from,
                    time_to=time_to,
                    log_id=None,
                ):
                    collected.add(view)
            cursor = batch[-1].offset + 1

    @staticmethod
    def _ensure_conditions(
        offset_from: int | None,
        offset_to: int | None,
        time_from: datetime | None,
        time_to: datetime | None,
        page: int,
        size: int,
    ) -> None:
        """条件区间与分页合法性（10001）。

        Args:
            offset_from: 起始 offset。
            offset_to: 结束 offset。
            time_from: 发布时间下界。
            time_to: 发布时间上界。
            page: 页码。
            size: 每页条数。

        Raises:
            ParamError: 任一条件非法。
        """
        if offset_from is not None and offset_from < 0:
            raise ParamError(f"起始 offset 不得为负：{offset_from}")
        if offset_to is not None and offset_to < 0:
            raise ParamError(f"结束 offset 不得为负：{offset_to}")
        if offset_from is not None and offset_to is not None and offset_from > offset_to:
            raise ParamError(f"offset 区间倒置：{offset_from} > {offset_to}")
        if time_from is not None and time_to is not None and _as_utc(time_from) > _as_utc(time_to):
            raise ParamError("发布时间区间倒置")
        if page < 1:
            raise ParamError(f"页码须从 1 起：{page}")
        if not 1 <= size <= MAX_LIST_SIZE:
            raise ParamError(f"每页条数须在 1~{MAX_LIST_SIZE}：{size}")


class OverviewInspector(BaseFrameworkObject):
    """概览聚合只读投影（只读 06-3 快照与存储水位，不回源扫描）。"""

    def __init__(self, *, snapshots: MetricsSnapshotStore, storage: StorageMetrics) -> None:
        """初始化。

        Args:
            snapshots: 06-3 采样快照。
            storage: 存储水位采集器。
        """
        self._snapshots = snapshots
        self._storage = storage

    def read(self, *, tenant: CurrentTenant) -> OverviewView:
        """取当前租户的四张数字卡。

        Args:
            tenant: 当前租户（快照键前缀按租户编码匹配）。

        Returns:
            OverviewView: 四值与快照可用性。
        """
        snapshot = self._snapshots.snapshot
        prefix = f"{tenant.code}{_PAIR_SEPARATOR}"
        consumer_lag = sum(value for key, value in snapshot.lag.items() if key.startswith(prefix))
        dlq_total = sum(value for key, value in snapshot.dlq.items() if key.startswith(prefix))
        raw = self._storage.snapshot().get("msg_disk_used_ratio")
        ratios = cast("ConcurrentStableDict[str, float] | None", raw)
        disk_used_ratio = 0.0
        if ratios is not None:
            for _volume, value in ratios.items():
                disk_used_ratio = max(disk_used_ratio, float(value))
        available = snapshot.window_seconds > 0
        rate = snapshot.messages_in_window / snapshot.window_seconds if available else 0.0
        return OverviewView(
            messages_in_per_second=rate,
            consumer_lag=consumer_lag,
            dlq_total=dlq_total,
            disk_used_ratio=disk_used_ratio,
            snapshot_available=available,
        )


class DelaySummaryInspector(BaseFrameworkObject):
    """延迟汇总只读投影（与 `/admin/delayed` 同一份当前租户快照）。"""

    def __init__(
        self,
        *,
        router: TopicRouter,
        store: DelayStore,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """初始化。

        Args:
            router: 主题解析。
            store: 延迟区存储。
            now: 服务端时钟（用例注入固定时刻）。
        """
        self._router = router
        self._store = store
        self._now = now or (lambda: datetime.now(UTC))

    async def summarize(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str | None,
        topic: str | None,
        partition: int | None,
        acl: TopicAclService,
    ) -> DelaySummaryView:
        """按档位汇总当前租户待投记录，并给结构化堆积提示。

        Args:
            tenant: 当前租户。
            namespace: 命名空间过滤。
            topic: 主题过滤。
            partition: 分区号过滤。
            acl: 主题授权守卫（逐记录 describe 过滤，与 `/admin/delayed` 同判据）。

        Returns:
            DelaySummaryView: 汇总结果。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记（350010）。
            MsgTopicNotFoundError: 主题不存在（350001）。
        """
        if namespace is not None:
            tenant.ensure_namespace(namespace)
        if topic is not None and namespace is not None:
            resolved = await self._router.resolve(namespace=namespace, name=topic)
            if partition is not None:
                self._router.partition(resolved, partition)
        now = self._now()
        records = await self._store.list_for_tenant(tenant_id=int(tenant.tenant_id))
        counts: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        minimums: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for level in DelayLevel:
            counts.set(str(level), 0)
        total = 0
        stale = 0
        for record in records:
            if namespace is not None and record.namespace != namespace:
                continue
            if topic is not None and record.topic != topic:
                continue
            if partition is not None and record.partition_no != partition:
                continue
            allowed = await acl.filter_allowed(
                tenant_id=int(tenant.tenant_id),
                namespace=record.namespace,
                topics=ConcurrentStableList([record.topic]),
                action=ACL_ACTION_DESCRIBE,
            )
            if not allowed:
                continue
            remaining = _remaining_ms(record=record, now=now)
            level = record.delay_level
            counts.set(level, counts.get(level, 0) + 1)
            current_min = minimums.get(level)
            if current_min is None or remaining < current_min:
                minimums.set(level, remaining)
            total += 1
            if remaining == 0:
                stale += 1
        levels: ConcurrentStableList[LevelCount] = ConcurrentStableList()
        for level in DelayLevel:
            key = str(level)
            levels.add(
                LevelCount(
                    level=key,
                    count=counts.get(key, 0),
                    min_remaining_ms=minimums.get(key, 0),
                )
            )
        quota_count = await self._store.count_for_tenant(tenant_id=int(tenant.tenant_id))
        ratio = min(max(quota_count / DELAY_QUOTA_PER_TENANT, 0.0), 1.0)
        hint = "stale" if stale > 0 else ("near_quota" if ratio >= _NEAR_QUOTA_RATIO else "normal")
        return DelaySummaryView(
            total=total,
            levels=levels,
            stale_count=stale,
            quota_used_ratio=ratio,
            hint=hint,
        )
