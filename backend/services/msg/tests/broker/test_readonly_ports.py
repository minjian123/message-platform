"""只读存储端口、速率快照与依赖降级（msg 任务 06-1-1；Kiwi 2462）。

覆盖：`MessageStore` 三个实现的 `find_by_log_id` / `scan_headers` 一致性、
`FileMessageStore` LRU 未命中时的段头兜底、`LagSnapshot` 速率字段、
采样器差分与降级、概览投影空水位分支、依赖惰性装配。
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.tenant import TenantLookup
from fastapi import Request

import bms_msg.broker.admin_view as admin_view_module
from bms_msg.acl.service import TopicAclService
from bms_msg.broker.admin_view import DelaySummaryInspector, MessageInspector, OverviewInspector
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayRecord, DelayStore
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import (
    InMemoryMessageStore,
    MessageHeaderView,
    MessageStore,
    message_header_view,
)
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore
from bms_msg.broker.watermark import StorageMetrics, StorageWatermark
from bms_msg.metrics import get_traffic_metrics, reset_traffic_metrics
from bms_msg.metrics.lag import LagSnapshot, MetricsSnapshotStore
from bms_msg.metrics.sampler import MetricsSampler
from bms_msg.schemas.message import DelayLevel
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2462)

_TENANT_ID = 7001
_NAMESPACE = "msg"
_TOPIC_NAME = "audit.operation"
_TOPIC_ID = 1
_NOW = datetime(2026, 11, 9, 9, 0, tzinfo=UTC)


async def _append(store: MessageStore, *, log_id: str, offset_partition: int = 0) -> None:
    """追加一条记录到内存 / 文件实现。

    Args:
        store: 消息存储。
        log_id: 记录标识。
        offset_partition: 分区号。
    """
    await store.append(
        topic_id=_TOPIC_ID,
        partition_no=offset_partition,
        message_id=f"message-{log_id}",
        log_id=log_id,
        key="alpha",
        payload=b"payload",
        headers=ConcurrentStableDict({"trace": "t1"}),
        trace_id="t1",
        published_at=_NOW,
    )


def _file_store(root: Path) -> FileMessageStore:
    """构造已绑定主题目录的文件实现。

    Args:
        root: 临时数据根。

    Returns:
        FileMessageStore: 文件实现。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=_TENANT_ID, namespace=_NAMESPACE, name=_TOPIC_NAME, topic_id=_TOPIC_ID)
    return FileMessageStore(directory=directory, fsync_mode="os", clock=lambda: 0.0)


async def test_in_memory_header_views() -> None:
    """内存实现：命中 / 未命中、批读摘要、结构上不含正文。"""
    store = InMemoryMessageStore()
    await _append(store, log_id="l1")
    await _append(store, log_id="l2")
    found = await store.find_by_log_id(log_id="l2")
    assert found is not None and found.log_id == "l2"
    assert found.payload_len == len(b"payload")
    assert (
        found.payload_sha256
        == message_header_view(
            (await store.fetch(topic_id=_TOPIC_ID, partition_no=0, from_offset=0, max_messages=1))[0]
        ).payload_sha256
    )
    assert await store.find_by_log_id(log_id="missing") is None
    views = await store.scan_headers(topic_id=_TOPIC_ID, partition_no=0, from_offset=0, max_messages=10)
    assert len(views) == 2
    assert not hasattr(views[0], "payload")
    assert views[0].headers.get("trace") == "t1"


async def test_notifying_store_forwards_readonly_views() -> None:
    """通知包装器原样转发两个新只读方法。"""
    inner = InMemoryMessageStore()
    await _append(inner, log_id="l1")
    store = NotifyingMessageStore(inner, DeliveryWaiter())
    assert (await store.find_by_log_id(log_id="l1")) is not None
    assert await store.find_by_log_id(log_id="missing") is None
    assert len(await store.scan_headers(topic_id=_TOPIC_ID, partition_no=0, from_offset=0, max_messages=5)) == 1


async def test_file_store_header_views_and_lru_fallback(tmp_path: Path) -> None:
    """文件实现：LRU 命中 / 清空 LRU 后段头兜底 / 未命中 / 批读 / 既有坐标入口不回归。"""
    store = _file_store(tmp_path)
    await _append(store, log_id="l1")
    await _append(store, log_id="l2")
    hit = await store.find_by_log_id(log_id="l1")
    assert hit is not None and hit.payload_len == len(b"payload")
    assert not hasattr(hit, "payload")
    assert await store.locate_by_log_id(log_id="l1") is not None
    store.__dict__["_log_ids"].clear()
    fallback = await store.find_by_log_id(log_id="l2")
    assert fallback is not None and fallback.log_id == "l2"
    assert await store.find_by_log_id(log_id="missing") is None
    assert await store.locate_by_log_id(log_id="missing") is None
    views = await store.scan_headers(topic_id=_TOPIC_ID, partition_no=0, from_offset=0, max_messages=10)
    assert [view.log_id for view in views] == ["l1", "l2"]


def test_lag_snapshot_rate_fields() -> None:
    """速率字段默认值、`replace` 未给维度沿用、`reset` 归零。"""
    assert (LagSnapshot().messages_in_window, LagSnapshot().window_seconds) == (0, 0.0)
    store = MetricsSnapshotStore()
    store.replace(messages_in_window=12, window_seconds=4.0)
    assert (store.snapshot.messages_in_window, store.snapshot.window_seconds) == (12, 4.0)
    store.replace(replay_failed=3)
    assert (store.snapshot.messages_in_window, store.snapshot.window_seconds) == (12, 4.0)
    assert store.snapshot.replay_failed == 3
    store.reset()
    assert store.snapshot.window_seconds == 0.0


def test_sampler_ingress_rate_and_failure() -> None:
    """首轮无差分写 0、次轮按周期差分、无增量仍可用、采集异常被吞掉。"""
    reset_traffic_metrics()
    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("GroupStore", object()),
        messages=cast("MessageStore", object()),
        offsets=cast("OffsetStore", object()),
        dead_letters=cast("DeadLetterQueue", object()),
        uow_factory=cast("UowFactory", object()),
        watermark=cast("StorageWatermark", object()),
        tenant_lookup=cast("TenantLookup", object()),
    )
    sample_ingress = MetricsSampler.__dict__["_sample_ingress"]
    sample_ingress(sampler)
    assert (snapshots.snapshot.messages_in_window, snapshots.snapshot.window_seconds) == (0, 0.0)
    traffic = get_traffic_metrics()
    for _ in range(10):
        traffic.record_publish(tenant="demo", topic="audit.operation", size=7, duration_ms=1.0)
    sample_ingress(sampler)
    assert snapshots.snapshot.messages_in_window == 10
    assert snapshots.snapshot.window_seconds > 0
    sample_ingress(sampler)
    assert snapshots.snapshot.messages_in_window == 0
    assert snapshots.snapshot.window_seconds > 0
    reset_traffic_metrics()


def test_overview_inspector_without_ratio_snapshot() -> None:
    """水位快照缺 `msg_disk_used_ratio` 时磁盘值取 0，不抛错。"""

    class _EmptyStorage:
        """空水位快照替身。"""

        def snapshot(self) -> ConcurrentStableDict[str, object]:
            """返回空快照。

            Returns:
                ConcurrentStableDict[str, object]: 空快照。
            """
            return ConcurrentStableDict()

    tenant = cast("CurrentTenant", SimpleNamespace(code="demo"))
    view = OverviewInspector(snapshots=MetricsSnapshotStore(), storage=cast("StorageMetrics", _EmptyStorage())).read(
        tenant=tenant
    )
    assert view.disk_used_ratio == 0.0
    assert view.snapshot_available is False


def test_deps_create_missing_state_lazily() -> None:
    """最小 app 未装配快照 / 水位时惰性建空实例，重复取同一实例。"""
    from bms_msg.api.deps import get_metrics_snapshots, get_storage_metrics

    request = cast("Request", SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace())))
    snapshots = get_metrics_snapshots(request)
    storage = get_storage_metrics(request)
    assert get_metrics_snapshots(request) is snapshots
    assert get_storage_metrics(request) is storage
    assert snapshots.snapshot.window_seconds == 0.0


def _header(
    *,
    log_id: str = "l1",
    topic_id: int = _TOPIC_ID,
    partition_no: int = 0,
    offset: int = 0,
    key: str | None = "alpha",
    published_at: datetime = _NOW,
) -> MessageHeaderView:
    """构造只读头视图（测试内部用）。

    Args:
        log_id: 记录标识。
        topic_id: 主题主键。
        partition_no: 分区号。
        offset: 分区内序号。
        key: 分区键。
        published_at: 发布时间。

    Returns:
        MessageHeaderView: 头视图。
    """
    return MessageHeaderView(
        message_id="m1",
        log_id=log_id,
        topic_id=topic_id,
        partition_no=partition_no,
        offset=offset,
        key=key,
        headers=ConcurrentStableDict(),
        trace_id=None,
        published_at=published_at,
        content_type="application/msgpack",
        payload_len=7,
        payload_sha256="0" * 64,
    )


def _tenant() -> CurrentTenant:
    """构造当前租户替身。

    Returns:
        CurrentTenant: 租户替身。
    """

    def _no_op(namespace: object) -> None:
        """命名空间校验替身。

        Args:
            namespace: 命名空间。
        """
        del namespace

    return cast(
        "CurrentTenant",
        SimpleNamespace(tenant_id="7001", code="demo", ensure_namespace=_no_op),
    )


class _Router:
    """主题解析替身（只给分区总数）。"""

    async def resolve(self, *, namespace: str, name: str) -> object:
        """解析主题。

        Args:
            namespace: 命名空间。
            name: 主题名。

        Returns:
            object: 主题快照替身。
        """
        del namespace, name
        return SimpleNamespace(id=_TOPIC_ID, partition_count=2)

    def partition(self, topic: object, partition_no: int) -> int:
        """分区号校验替身。

        Args:
            topic: 主题快照。
            partition_no: 分区号。

        Returns:
            int: 分区号。
        """
        del topic
        return partition_no


class _Headers:
    """消息存储替身（只实现消息检索用到的四个只读方法）。"""

    def __init__(self) -> None:
        """初始化可注入命中与批次。"""
        self.forced: MessageHeaderView | None = None
        self.batch: ConcurrentStableList[MessageHeaderView] = ConcurrentStableList()

    async def find_by_log_id(self, *, log_id: str) -> MessageHeaderView | None:
        """返回注入的命中。

        Args:
            log_id: 记录标识。

        Returns:
            MessageHeaderView | None: 命中视图。
        """
        del log_id
        return self.forced

    async def scan_headers(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[MessageHeaderView]:
        """返回注入的批次。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 上限。

        Returns:
            ConcurrentStableList[MessageHeaderView]: 头视图批次。
        """
        del topic_id, partition_no, from_offset, max_messages
        return self.batch

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """可读起点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 0。
        """
        del topic_id, partition_no
        return 0

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """日志末端。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 2。
        """
        del topic_id, partition_no
        return 2


async def _search(inspector: MessageInspector, **overrides: object) -> tuple[object, int]:
    """以缺省条件调用消息检索。

    Args:
        inspector: 只读投影。
        **overrides: 条件覆盖。

    Returns:
        tuple[object, int]: 命中集合与总数。
    """
    params: dict[str, object] = {
        "tenant": _tenant(),
        "namespace": _NAMESPACE,
        "topic": _TOPIC_NAME,
        "partition": None,
        "offset_from": None,
        "offset_to": None,
        "key": None,
        "time_from": None,
        "time_to": None,
        "log_id": None,
        "page": 1,
        "size": 20,
    }
    params.update(overrides)
    return await inspector.search(**params)  # type: ignore[arg-type]


def _matches_fn() -> object:
    """取私有过滤函数（避开 pyright 私有使用告警）。

    Returns:
        object: `_matches` 函数对象。
    """
    return admin_view_module.__dict__["_matches"]


def _as_utc_fn() -> object:
    """取私有时间归一函数。

    Returns:
        object: `_as_utc` 函数对象。
    """
    return admin_view_module.__dict__["_as_utc"]


def test_matches_and_as_utc_branches() -> None:
    """过滤函数的每个失败分支与时间归一的无时区分支。"""
    matches = cast("Callable[..., bool]", _matches_fn())
    as_utc = cast("Callable[..., datetime]", _as_utc_fn())
    kwargs = {
        "offset_from": None,
        "offset_to": None,
        "key": None,
        "time_from": None,
        "time_to": None,
        "log_id": None,
    }
    assert matches(_header(), **kwargs)
    assert not matches(_header(offset=0), **{**kwargs, "offset_from": 1})
    assert not matches(_header(offset=1), **{**kwargs, "offset_to": 1})
    assert not matches(_header(key="beta"), **{**kwargs, "key": "alpha"})
    assert not matches(_header(published_at=_NOW), **{**kwargs, "time_from": _NOW + timedelta(minutes=1)})
    assert not matches(_header(published_at=_NOW), **{**kwargs, "time_to": _NOW})
    assert not matches(_header(log_id="l1"), **{**kwargs, "log_id": "other"})
    naive = datetime(2026, 11, 9, 9, 0)
    assert as_utc(naive).tzinfo is UTC
    assert as_utc(_NOW).tzinfo is UTC


async def test_message_inspector_log_id_mismatch_branches() -> None:
    """`log_id` 命中但分区或其余条件不吻合时返回空页（不短路过滤）。"""
    headers = _Headers()
    inspector = MessageInspector(router=cast("TopicRouter", _Router()), messages=cast("MessageStore", headers))
    headers.forced = _header(partition_no=1)
    rows, total = await _search(inspector, log_id="l1", partition=0)
    assert (len(cast("ConcurrentStableList[MessageHeaderView]", rows)), total) == (0, 0)
    headers.forced = _header(key="beta")
    rows, total = await _search(inspector, log_id="l1", key="alpha")
    assert (len(cast("ConcurrentStableList[MessageHeaderView]", rows)), total) == (0, 0)
    headers.forced = _header()
    rows, total = await _search(inspector, log_id="l1")
    assert (len(cast("ConcurrentStableList[MessageHeaderView]", rows)), total) == (1, 1)


async def test_message_inspector_scan_break_branches() -> None:
    """空批次终止、批次越界终止、正常批次收集与分页。"""
    headers = _Headers()
    inspector = MessageInspector(router=cast("TopicRouter", _Router()), messages=cast("MessageStore", headers))
    rows, total = await _search(inspector)
    assert (len(cast("ConcurrentStableList[MessageHeaderView]", rows)), total) == (0, 0)
    headers.batch = ConcurrentStableList[MessageHeaderView]([_header(offset=5)])
    rows, total = await _search(inspector)
    assert (len(cast("ConcurrentStableList[MessageHeaderView]", rows)), total) == (0, 0)
    headers.batch = ConcurrentStableList[MessageHeaderView]([_header(offset=0), _header(offset=1, log_id="l2")])
    rows, total = await _search(inspector, size=1)
    assert total == 4 and len(cast("ConcurrentStableList[MessageHeaderView]", rows)) == 1


def test_ensure_conditions_negative_offset_to() -> None:
    """负的结束 offset 给 10001（与起始 offset 同判据）。"""
    from bms_msg.broker.admin_view import MessageInspector

    ensure = MessageInspector.__dict__["_ensure_conditions"].__func__
    with pytest.raises(ParamError):
        ensure(-1, None, None, None, 1, 20)
    with pytest.raises(ParamError):
        ensure(None, -1, None, None, 1, 20)


def _delay_record(
    *,
    log_id: str,
    namespace: str = _NAMESPACE,
    topic: str = _TOPIC_NAME,
    partition_no: int = 0,
    level: str = "1m",
    deliver_at: datetime = _NOW + timedelta(minutes=1),
) -> DelayRecord:
    """构造延迟记录。

    Args:
        log_id: 记录标识。
        namespace: 命名空间。
        topic: 主题名。
        partition_no: 分区号。
        level: 档位。
        deliver_at: 到期时刻。

    Returns:
        DelayRecord: 延迟记录。
    """
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=_TENANT_ID,
        topic_id=_TOPIC_ID,
        namespace=namespace,
        topic=topic,
        partition_no=partition_no,
        delay_level=level,
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict(),
        trace_id=None,
        published_at=_NOW,
        deliver_at=deliver_at,
        attempts=0,
    )


class _DelayStore:
    """延迟区替身。"""

    def __init__(self, records: ConcurrentStableList[DelayRecord]) -> None:
        """初始化。

        Args:
            records: 当前租户记录。
        """
        self.records = records
        self.quota_count = len(records)

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[DelayRecord]:
        """取当前租户快照。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录。
        """
        del tenant_id
        return self.records

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """取配额计数。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 计数。
        """
        del tenant_id
        return self.quota_count


class _Acl:
    """主题授权替身。"""

    def __init__(self, *, allow: bool = True) -> None:
        """初始化。

        Args:
            allow: 是否放行。
        """
        self.allow = allow

    async def filter_allowed(
        self, *, tenant_id: int, namespace: str, topics: ConcurrentStableList[str], action: str
    ) -> ConcurrentStableList[str]:
        """按开关放行。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            topics: 候选主题。
            action: 动作。

        Returns:
            ConcurrentStableList[str]: 允许的主题。
        """
        del tenant_id, namespace, action
        return topics if self.allow else ConcurrentStableList()


async def test_delay_summary_filter_acl_and_min_remaining_branches() -> None:
    """过滤 continue、ACL 拒绝 continue、同档最短剩余时间更新、接近上限提示。"""
    records = ConcurrentStableList(
        [
            _delay_record(log_id="a", deliver_at=_NOW + timedelta(minutes=5)),
            _delay_record(log_id="b", deliver_at=_NOW + timedelta(minutes=1)),
            _delay_record(log_id="f", deliver_at=_NOW + timedelta(minutes=10)),
            _delay_record(log_id="c", namespace="other"),
            _delay_record(log_id="d", topic="other.topic"),
            _delay_record(log_id="e", partition_no=1),
        ]
    )
    store = _DelayStore(records)
    inspector = DelaySummaryInspector(
        router=cast("TopicRouter", _Router()),
        store=cast("DelayStore", store),
        now=lambda: _NOW,
    )
    acl = cast("TopicAclService", _Acl())
    summary = await inspector.summarize(
        tenant=_tenant(),
        namespace=_NAMESPACE,
        topic=_TOPIC_NAME,
        partition=0,
        acl=acl,
    )
    assert summary.total == 3
    bucket = next(item for item in summary.levels if item.level == str(DelayLevel.MINUTE_1))
    assert bucket.count == 3 and bucket.min_remaining_ms == 60_000
    denied = await inspector.summarize(
        tenant=_tenant(), namespace=None, topic=None, partition=None, acl=cast("TopicAclService", _Acl(allow=False))
    )
    assert denied.total == 0
    store.quota_count = 90_000
    near = await inspector.summarize(tenant=_tenant(), namespace=_NAMESPACE, topic=_TOPIC_NAME, partition=0, acl=acl)
    assert near.hint == "near_quota" and near.quota_used_ratio == 0.9


async def test_sampler_ingress_exception_is_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    """入口速率采集失败只记 warning，不中断整轮采样。"""
    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("GroupStore", object()),
        messages=cast("MessageStore", object()),
        offsets=cast("OffsetStore", object()),
        dead_letters=cast("DeadLetterQueue", object()),
        uow_factory=cast("UowFactory", object()),
        watermark=cast("StorageWatermark", object()),
        tenant_lookup=cast("TenantLookup", object()),
    )

    def _boom(self: MetricsSampler) -> None:
        """模拟采集异常。

        Args:
            self: 采样器。

        Raises:
            RuntimeError: 恒定抛出。
        """
        del self
        raise RuntimeError("boom")

    monkeypatch.setattr(MetricsSampler, "_sample_ingress", _boom)
    await sampler.sample_once()
    assert snapshots.snapshot.window_seconds == 0.0
