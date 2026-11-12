"""迁移 / 切主 / 传输指标出口（02-8）：Collector 标签与采样器写入。"""

from __future__ import annotations

from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.election import LeaderSwitchMetrics
from bms_msg.broker.network import TransportErrorMetrics
from bms_msg.metrics.collector import MsgMetricsCollector
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.metrics.sampler import MetricsSampler
from bms_msg.metrics.traffic import TrafficMetrics


class _Source:
    """空快照源。"""

    def snapshot(self) -> ConcurrentStableDict[str, Any]:
        """返回空快照。"""
        return ConcurrentStableDict()


def _collector(snapshots: MetricsSnapshotStore) -> MsgMetricsCollector:
    """构造带 02-8 计数器的收集器。"""
    switch = LeaderSwitchMetrics()
    switch.record("truncated")
    switch.record("degraded")
    transport = TransportErrorMetrics()
    transport.record(operation="fetch", reason="timeout")
    transport.record(operation="apply", reason="auth")
    return MsgMetricsCollector(
        service="msg",
        traffic=TrafficMetrics(),
        snapshots=snapshots,
        storage=_Source(),
        delay=_Source(),
        backpressure=_Source(),
        lifecycle=_Source(),
        purge=_Source(),
        leader_switch=switch,
        transport_errors=transport,
    )


def _families(collector: MsgMetricsCollector) -> ConcurrentStableDict[str, Any]:
    """按名字索引指标家族。"""
    result: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    for family in collector.collect():
        result.set(family.name, family)
    return result


def test_collector_exports_switch_and_transport_counters() -> None:
    """收集器：切主按 result、传输错误按 operation/reason 输出。"""
    families = _families(_collector(MetricsSnapshotStore()))

    switch = families["msg_leader_switch"]
    results = {tuple(sample.labels["result"] for _ in [0]): sample.value for sample in switch.samples}
    assert results[("truncated",)] == 1.0
    assert results[("degraded",)] == 1.0

    transport = families["msg_replication_transport_errors"]
    pairs = {(sample.labels["operation"], sample.labels["reason"]): sample.value for sample in transport.samples}
    assert pairs[("fetch", "timeout")] == 1.0
    assert pairs[("apply", "auth")] == 1.0


def test_collector_exports_migration_gauges_from_snapshot() -> None:
    """收集器：迁移状态计数与剩余条数来自采样快照。"""
    snapshots = MetricsSnapshotStore()
    snapshots.replace(
        migration=ConcurrentStableDict({"catching_up": 2, "succeeded": 1}),
        migration_remaining=30,
    )

    families = _families(_collector(snapshots))

    tasks = families["msg_partition_migration_tasks"]
    by_status = {sample.labels["status"]: sample.value for sample in tasks.samples}
    assert by_status["catching_up"] == 2.0
    assert by_status["succeeded"] == 1.0
    remaining = families["msg_partition_migration_remaining_records"]
    assert remaining.samples[0].value == 30.0


async def test_sampler_writes_migration_snapshot() -> None:
    """采样器：从台账读状态计数与剩余条数并写快照。"""

    class FakeLedger:
        async def count_by_status(self) -> ConcurrentStableDict[str, int]:
            return ConcurrentStableDict({"planned": 1})

        async def sum_remaining(self) -> int:
            return 7

    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", object()),
        messages=cast("Any", object()),
        offsets=cast("Any", object()),
        dead_letters=cast("Any", object()),
        uow_factory=cast("Any", object()),
        watermark=cast("Any", object()),
        tenant_lookup=cast("Any", object()),
        migration=cast("Any", FakeLedger()),
    )

    await sampler._sample_migration()  # pyright: ignore[reportPrivateUsage]

    assert snapshots.snapshot.migration["planned"] == 1
    assert snapshots.snapshot.migration_remaining == 7


async def test_sampler_skips_migration_without_ledger() -> None:
    """采样器：未装配迁移台账时迁移维度空转。"""
    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", object()),
        messages=cast("Any", object()),
        offsets=cast("Any", object()),
        dead_letters=cast("Any", object()),
        uow_factory=cast("Any", object()),
        watermark=cast("Any", object()),
        tenant_lookup=cast("Any", object()),
    )
    await sampler._sample_migration()  # pyright: ignore[reportPrivateUsage]
    assert snapshots.snapshot.migration == {}


async def test_sampler_keeps_previous_on_migration_failure() -> None:
    """采样器：迁移维度失败只告警、不抛出。"""

    class BrokenLedger:
        async def count_by_status(self) -> ConcurrentStableDict[str, int]:
            raise RuntimeError("db down")

        async def sum_remaining(self) -> int:
            return 0

    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", object()),
        messages=cast("Any", object()),
        offsets=cast("Any", object()),
        dead_letters=cast("Any", object()),
        uow_factory=cast("Any", object()),
        watermark=cast("Any", object()),
        tenant_lookup=cast("Any", object()),
        migration=cast("Any", BrokenLedger()),
    )
    await sampler.sample_once()
    assert snapshots.snapshot.migration == {}
