"""指标采样器测试（06-3-1 / Kiwi 2460）。"""

import asyncio
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from pytest import mark

from bms_msg.metrics.lag import MetricsSnapshotStore, topic_key
from bms_msg.metrics.sampler import MetricsSampler


class _Record:
    """组记录替身。"""

    def __init__(self, tenant_id: int, topic_id: int, group: str) -> None:
        """初始化。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。
        """
        self.tenant_id = tenant_id
        self.topic_id = topic_id
        self.group = group


class _Topic:
    """主题替身。"""

    def __init__(self, tenant_id: int, name: str, partition_count: int = 2) -> None:
        """初始化。

        Args:
            tenant_id: 租户主键。
            name: 主题名。
            partition_count: 分区数。
        """
        self.tenant_id = tenant_id
        self.name = name
        self.partition_count = partition_count


class _Letter:
    """死信替身。"""

    def __init__(self, topic_id: int) -> None:
        """初始化。

        Args:
            topic_id: 主题主键。
        """
        self.topic_id = topic_id


class _Groups:
    """组存储替身。"""

    def __init__(self, records: ConcurrentStableList[_Record], *, fail: bool = False) -> None:
        """初始化。

        Args:
            records: 组记录。
            fail: 是否抛错（模拟采样失败）。
        """
        self._records = records
        self._fail = fail

    async def list_all(self) -> ConcurrentStableList[_Record]:
        """返回全部组记录。

        Returns:
            ConcurrentStableList[_Record]: 组记录。

        Raises:
            RuntimeError: 当 `fail=True`。
        """
        if self._fail:
            raise RuntimeError("db down")
        return self._records


class _Messages:
    """消息存储替身。"""

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区末端位点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 末端位点。
        """
        return 10


class _Offsets:
    """位点存储替身。"""

    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点。

        Args:
            group: 组名。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """
        return 7


class _DeadLetters:
    """死信队列替身。"""

    def __init__(self, letters: ConcurrentStableList[_Letter], *, fail: bool = False) -> None:
        """初始化。

        Args:
            letters: 死信记录。
            fail: 是否抛错。
        """
        self._letters = letters
        self._fail = fail

    async def list_all(self) -> ConcurrentStableList[_Letter]:
        """返回全部死信。

        Returns:
            ConcurrentStableList[_Letter]: 死信记录。

        Raises:
            RuntimeError: 当 `fail=True`。
        """
        if self._fail:
            raise RuntimeError("dlq down")
        return self._letters


class _Watermark:
    """存储水位替身。"""

    def __init__(self, *, fail: bool = False) -> None:
        """初始化。

        Args:
            fail: 是否抛错。
        """
        self.refreshed = 0
        self._fail = fail

    def force_refresh(self) -> None:
        """记录一次刷新。

        Raises:
            RuntimeError: 当 `fail=True`。
        """
        if self._fail:
            raise RuntimeError("storage down")
        self.refreshed += 1


class _Tenant:
    """租户上下文替身。"""

    def __init__(self, code: str) -> None:
        """初始化。

        Args:
            code: 租户编码。
        """
        self.code = code


class _TenantLookup:
    """租户源替身。"""

    def __init__(self, *, fail: bool = False) -> None:
        """初始化。

        Args:
            fail: 是否抛错。
        """
        self._fail = fail

    async def by_id(self, tenant_id: str, *, readonly: bool = False) -> _Tenant:
        """按主键取租户。

        Args:
            tenant_id: 租户主键。
            readonly: 只读通道。

        Returns:
            _Tenant: 租户上下文。

        Raises:
            RuntimeError: 当 `fail=True`。
        """
        if self._fail:
            raise RuntimeError("tenant source down")
        return _Tenant(f"code-{tenant_id}")


class _Result:
    """SQL 结果替身。"""

    def all(self) -> list[tuple[str, int]]:
        """返回状态计数行。

        Returns:
            list[tuple[str, int]]: 状态 → 计数。
        """
        return [("running", 2), ("failed", 1)]

    def scalar_one(self) -> int:
        """返回累计重放条数。

        Returns:
            int: 条数。
        """
        return 12


class _Session:
    """数据库会话替身。"""

    def __init__(self, topic: _Topic | None) -> None:
        """初始化。

        Args:
            topic: 主题替身（`get` 返回；None 模拟主题已删）。
        """
        self._topic = topic

    async def get(self, model: object, key: object) -> _Topic | None:
        """按主键取主题。

        Args:
            model: 模型类。
            key: 主键。

        Returns:
            _Topic | None: 主题替身。
        """
        return self._topic

    async def execute(self, statement: object) -> _Result:
        """执行查询。

        Args:
            statement: 查询语句。

        Returns:
            _Result: 固定结果。
        """
        return _Result()


class _Uow:
    """工作单元替身。"""

    def __init__(self, session: _Session) -> None:
        """初始化。

        Args:
            session: 会话替身。
        """
        self.session = session


class _UowContext:
    """工作单元上下文替身。"""

    def __init__(self, uow: _Uow) -> None:
        """初始化。

        Args:
            uow: 工作单元替身。
        """
        self._uow = uow

    async def __aenter__(self) -> _Uow:
        """进入上下文。

        Returns:
            _Uow: 工作单元。
        """
        return self._uow

    async def __aexit__(self, *args: object) -> bool:
        """退出上下文。

        Args:
            *args: 异常信息。

        Returns:
            bool: 不吞异常。
        """
        return False


def _build(
    *,
    groups_fail: bool = False,
    letters: int = 2,
    dlq_fail: bool = False,
    uow_fail: bool = False,
    watermark_fail: bool = False,
    topic_none: bool = False,
    lookup_fail: bool = False,
) -> tuple[MetricsSampler, MetricsSnapshotStore, _Watermark]:
    """构造采样器与可观测依赖。

    Args:
        groups_fail: 组存储是否抛错。
        letters: 死信条数。
        dlq_fail: 死信队列是否抛错。
        uow_fail: 工作单元是否抛错。
        watermark_fail: 存储水位是否抛错。
        topic_none: 主题反查是否返回 None。
        lookup_fail: 租户源是否抛错。

    Returns:
        tuple[MetricsSampler, MetricsSnapshotStore, _Watermark]: 采样器 / 快照 / 水位替身。
    """
    snapshots = MetricsSnapshotStore()
    watermark = _Watermark(fail=watermark_fail)
    dead_letters: ConcurrentStableList[_Letter] = ConcurrentStableList()
    for _ in range(letters):
        dead_letters.add(_Letter(100))
    topic = None if topic_none else _Topic(1, "orders")

    def _uow_factory() -> _UowContext:
        if uow_fail:
            raise RuntimeError("db down")
        return _UowContext(_Uow(_Session(topic)))

    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", _Groups(ConcurrentStableList([_Record(1, 100, "g1")]), fail=groups_fail)),
        messages=cast("Any", _Messages()),
        offsets=cast("Any", _Offsets()),
        dead_letters=cast("Any", _DeadLetters(dead_letters, fail=dlq_fail)),
        uow_factory=cast("Any", _uow_factory),
        watermark=cast("Any", watermark),
        tenant_lookup=cast("Any", _TenantLookup(fail=lookup_fail)),
    )
    return sampler, snapshots, watermark


@mark.kiwi_id(2460)
async def test_sample_once_writes_lag_dlq_replay() -> None:
    """一轮采样写入积压 / 死信存量与增长 / 重放状态，并刷新存储水位。"""
    sampler, snapshots, watermark = _build()
    await sampler.sample_once()

    snapshot = snapshots.snapshot
    key = topic_key(tenant="code-1", topic="orders")
    assert snapshot.lag[key] == 6
    assert snapshot.dlq[key] == 2
    assert snapshot.dlq_growth[key] == 2
    assert snapshot.replay["running"] == 2
    assert snapshot.replay_failed == 1
    assert snapshot.replay_replayed == 12
    assert watermark.refreshed == 1


@mark.kiwi_id(2460)
async def test_dlq_growth_accumulates_only_positive_delta() -> None:
    """死信增长只累加正增量（存量下降不产生负值）。"""
    first, snapshots, _ = _build(letters=2)
    await first.sample_once()
    key = topic_key(tenant="code-1", topic="orders")
    assert snapshots.snapshot.dlq_growth[key] == 2

    empty = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", _Groups(ConcurrentStableList([_Record(1, 100, "g1")]))),
        messages=cast("Any", _Messages()),
        offsets=cast("Any", _Offsets()),
        dead_letters=cast("Any", _DeadLetters(ConcurrentStableList([_Letter(100), _Letter(100)]))),
        uow_factory=cast("Any", lambda: _UowContext(_Uow(_Session(_Topic(1, "orders"))))),
        watermark=cast("Any", _Watermark()),
        tenant_lookup=cast("Any", _TenantLookup()),
    )
    await empty.sample_once()
    assert snapshots.snapshot.dlq_growth[key] == 2


@mark.kiwi_id(2460)
async def test_sample_failure_keeps_previous_dimension() -> None:
    """单维度失败保留上一轮快照，其它维度照常更新。"""
    sampler, snapshots, _ = _build()
    await sampler.sample_once()
    key = topic_key(tenant="code-1", topic="orders")
    assert snapshots.snapshot.lag[key] == 6

    failing, failing_snapshots, failing_watermark = _build(groups_fail=True)
    await failing.sample_once()
    assert not failing_snapshots.snapshot.lag
    assert failing_snapshots.snapshot.dlq[key] == 2
    assert failing_watermark.refreshed == 1


@mark.kiwi_id(2460)
async def test_start_and_stop_are_idempotent() -> None:
    """启动幂等，停止后可重复调用。"""
    sampler, _, _ = _build()
    sampler.ensure_started()
    sampler.ensure_started()
    await asyncio.sleep(0)
    assert sampler.running
    await sampler.stop()
    await sampler.stop()
    assert not sampler.running


@mark.kiwi_id(2460)
async def test_dimension_failures_are_isolated() -> None:
    """死信 / 存储两个维度失败时互不影响，积压仍正常采样。"""
    sampler, snapshots, watermark = _build(dlq_fail=True, watermark_fail=True)
    await sampler.sample_once()

    key = topic_key(tenant="code-1", topic="orders")
    assert snapshots.snapshot.lag[key] == 6
    assert not snapshots.snapshot.dlq
    assert snapshots.snapshot.replay["running"] == 2
    assert watermark.refreshed == 0


@mark.kiwi_id(2460)
async def test_replay_failure_keeps_others_when_db_unavailable() -> None:
    """数据库不可用时三个 DB 维度保留上次快照（不抛出、不清零）。"""
    sampler, snapshots, _ = _build(uow_fail=True)
    await sampler.sample_once()
    assert not snapshots.snapshot.lag
    assert not snapshots.snapshot.dlq
    assert not snapshots.snapshot.replay


@mark.kiwi_id(2460)
async def test_missing_topic_is_skipped() -> None:
    """主题已删时积压与死信都跳过，不中断整轮。"""
    sampler, snapshots, _ = _build(topic_none=True)
    await sampler.sample_once()
    assert not snapshots.snapshot.lag
    assert not snapshots.snapshot.dlq


@mark.kiwi_id(2460)
async def test_tenant_lookup_failure_falls_back_to_id() -> None:
    """租户源不可用时用主键字符串作标签，不中断采样。"""
    sampler, snapshots, _ = _build(lookup_fail=True)
    await sampler.sample_once()
    assert snapshots.snapshot.lag[topic_key(tenant="1", topic="orders")] == 6


@mark.kiwi_id(2460)
async def test_sampler_snapshot_store_reset() -> None:
    """快照存储 `reset()` 清空三维。"""
    snapshots = MetricsSnapshotStore()
    snapshots.replace(
        lag=ConcurrentStableDict({"a": 1}),
        dlq=ConcurrentStableDict({"a": 1}),
        replay=ConcurrentStableDict({"running": 1}),
    )
    snapshots.reset()
    assert not snapshots.snapshot.lag
    assert not snapshots.snapshot.dlq
    assert not snapshots.snapshot.replay
