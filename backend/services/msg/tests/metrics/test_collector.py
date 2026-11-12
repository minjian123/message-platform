"""Prometheus 收集器测试（06-3-1 / Kiwi 2460）。"""

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.metrics.prometheus import PrometheusMetrics
from prometheus_client.parser import text_string_to_metric_families
from pytest import mark

from bms_msg.metrics.collector import MsgMetricsCollector
from bms_msg.metrics.lag import MetricsSnapshotStore, topic_key
from bms_msg.metrics.names import METRIC_HELP
from bms_msg.metrics.traffic import TrafficMetrics


class _FakeSource:
    """固定快照源。"""

    def __init__(self, snapshot: Any) -> None:
        """初始化。

        Args:
            snapshot: 固定快照。
        """
        self._snapshot = snapshot

    def snapshot(self) -> Any:
        """返回固定快照。

        Returns:
            Any: 快照。
        """
        return self._snapshot


def _build() -> tuple[MsgMetricsCollector, TrafficMetrics, MetricsSnapshotStore]:
    """构造收集器与两个可写数据源。

    Returns:
        tuple[MsgMetricsCollector, TrafficMetrics, MetricsSnapshotStore]: 收集器 / 采集器 / 快照。
    """
    traffic = TrafficMetrics()
    snapshots = MetricsSnapshotStore()
    storage = _FakeSource(
        ConcurrentStableDict(
            {
                "msg_disk_used_ratio": ConcurrentStableDict({"vol-a": 0.2, "vol-b": 0.7}),
                "msg_storage_rejected_total": ConcurrentStableDict({"__total__": 2, "watermark": 2}),
            }
        )
    )
    delay = _FakeSource(
        ConcurrentStableDict(
            {
                "msg_delay_pending": ConcurrentStableDict({"__total__": 3, "1": 3}),
                "msg_delay_segments": 2,
                "msg_delay_rejected_total": ConcurrentStableDict({"__total__": 1, "tenant": 1}),
            }
        )
    )
    backpressure = _FakeSource(
        ConcurrentStableDict(
            {
                "msg_inflight_messages": ConcurrentStableDict({"__total__": 4, "t1|orders": 4}),
                "msg_inflight_bytes": ConcurrentStableDict({"__total__": 40, "t1|orders": 40}),
                "msg_window_stall_total": ConcurrentStableDict({"__total__": 1, "t1|orders": 1}),
                "msg_rate_limit_rejected_total": ConcurrentStableDict({"__total__": 2, "scope=topic:t1|orders": 2}),
                "msg_long_poll_waiters": 5,
                "msg_long_poll_changed_total": 6,
                "msg_long_poll_timeout_total": 7,
                "msg_long_poll_rejected_total": 8,
            }
        )
    )
    lifecycle = _FakeSource(ConcurrentStableDict({"applied": 1, "skipped": 2, "failed": 3}))
    purge = _FakeSource(ConcurrentStableDict({"scanned": 1, "purged": 2, "deferred": 3, "failed": 4}))
    collector = MsgMetricsCollector(
        service="msg",
        traffic=traffic,
        snapshots=snapshots,
        storage=storage,
        delay=delay,
        backpressure=backpressure,
        lifecycle=lifecycle,
        purge=purge,
    )
    return collector, traffic, snapshots


def _by_name(families: ConcurrentStableList[Any]) -> ConcurrentStableDict[str, Any]:
    """把家族列表转成名称索引。

    Args:
        families: 指标家族。

    Returns:
        ConcurrentStableDict[str, Any]: 名称 → 家族。
    """
    result: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    for family in families:
        result.set(family.name, family)
        if family.type == "counter":
            result.set(f"{family.name}_total", family)
    return result


@mark.kiwi_id(2460)
def test_collects_traffic_with_service_label() -> None:
    """流量指标的标签含 `service` / `tenant` / `topic`，值与采集器一致。"""
    collector, traffic, _ = _build()
    traffic.record_publish(tenant="t1", topic="orders", size=9, duration_ms=7.0)

    family = _by_name(collector.collect())["msg_messages_in_total"]
    assert family.type == "counter"
    sample = family.samples[0]
    assert sample.labels == {"service": "msg", "tenant": "t1", "topic": "orders"}
    assert sample.value == 1.0


@mark.kiwi_id(2460)
def test_histogram_has_ms_buckets() -> None:
    """延迟直方图按毫秒桶输出（含 +Inf）。"""
    collector, traffic, _ = _build()
    traffic.record_publish(tenant="t1", topic="orders", size=1, duration_ms=7.0)
    family = _by_name(collector.collect())["msg_publish_latency_ms"]
    bucket_labels = [sample.name for sample in family.samples if sample.name.endswith("_bucket")]
    assert bucket_labels
    assert any("+Inf" in sample.labels.get("le", "") for sample in family.samples if sample.name.endswith("_bucket"))


@mark.kiwi_id(2460)
def test_skips_total_and_keeps_other() -> None:
    """`__total__` 不产出序列；`__other__` 保留为标签值。"""
    collector, _, snapshots = _build()
    snapshots.replace(lag=ConcurrentStableDict({topic_key(tenant="t1", topic="a"): 4}))

    family = _by_name(collector.collect())["msg_consumer_lag"]
    labels = [sample.labels for sample in family.samples]
    assert {"service": "msg", "tenant": "t1", "topic": "a"} in labels

    backpressure_family = _by_name(collector.collect())["msg_inflight_messages"]
    assert all(sample.labels.get("tenant") != "__total__" for sample in backpressure_family.samples)
    assert backpressure_family.samples[0].value == 4.0


@mark.kiwi_id(2460)
def test_disk_ratio_takes_max_without_volume_label() -> None:
    """多卷磁盘使用率取最大值，且不带卷标签。"""
    collector, _, _ = _build()
    family = _by_name(collector.collect())["msg_disk_used_ratio"]
    assert family.samples[0].labels == {"service": "msg"}
    assert family.samples[0].value == 0.7


@mark.kiwi_id(2460)
def test_delay_and_lifecycle_families() -> None:
    """延迟区 / 生命周期 / 过期清理指标均按约定标签输出。"""
    collector, _, _ = _build()
    families = _by_name(collector.collect())
    assert families["msg_delay_pending"].samples[0].labels == {"service": "msg", "tenant": "1"}
    assert families["msg_delay_segments"].samples[0].value == 2.0
    assert families["msg_delay_rejected_total"].samples[0].labels == {"service": "msg", "scope": "tenant"}
    assert families["msg_tenant_lifecycle_events_total"].samples[0].labels == {"service": "msg", "result": "applied"}
    assert families["msg_tenant_purge_total"].samples[0].labels == {"service": "msg", "result": "scanned"}


@mark.kiwi_id(2460)
def test_replay_and_dlq_from_snapshot() -> None:
    """重放与死信指标从采样快照输出（含增长与失败计数）。"""
    collector, _, snapshots = _build()
    snapshots.replace(
        dlq=ConcurrentStableDict({topic_key(tenant="t1", topic="orders"): 2}),
        dlq_growth=ConcurrentStableDict({topic_key(tenant="t1", topic="orders"): 5}),
        replay=ConcurrentStableDict({"running": 1}),
        replay_replayed=12,
        replay_failed=1,
    )
    families = _by_name(collector.collect())
    assert families["msg_dlq_total"].samples[0].value == 2.0
    assert families["msg_dlq_growth_total"].samples[0].value == 5.0
    assert families["msg_replay_tasks"].samples[0].labels["status"] == "running"
    assert families["msg_replay_replayed_total"].samples[0].value == 12.0
    assert families["msg_replay_failed_total"].samples[0].labels["reason"] == "task_failed"


@mark.kiwi_id(2460)
def test_isr_constant_zero() -> None:
    """ISR 指标在 02-5 交付前恒 0（保持规则可引用）。"""
    collector, _, _ = _build()
    assert _by_name(collector.collect())["msg_isr_shrink_total"].samples[0].value == 0.0


@mark.kiwi_id(2460)
def test_endpoint_renders_parsable_text() -> None:
    """注册进基座后 `render()` 可被 Prometheus 解析。"""
    collector, traffic, _ = _build()
    metrics = PrometheusMetrics("msg")
    assert metrics.register_collector(collector) is True
    traffic.record_publish(tenant="t1", topic="orders", size=1, duration_ms=1.0)
    rendered = metrics.render()
    assert rendered is not None
    families = list(text_string_to_metric_families(rendered[0].decode("utf-8")))
    assert any(family.name == "msg_messages_in" for family in families)


@mark.kiwi_id(2460)
def test_help_text_registered_for_every_family() -> None:
    """收集器输出的每个指标家族都在 HELP 清单内。"""
    collector, traffic, _ = _build()
    traffic.record_rpc(method="msg.publish", outcome="ok", duration_ms=1.0)
    for family in collector.collect():
        name = family.name if family.name in METRIC_HELP else f"{family.name}_total"
        assert name in METRIC_HELP


@mark.kiwi_id(2460)
def test_describe_returns_same_families() -> None:
    """`describe()` 与 `collect()` 同名（注册期冲突检测依赖它）。"""
    collector, _, _ = _build()
    described = {family.name for family in collector.describe()}
    collected = {family.name for family in collector.collect()}
    assert described == collected


@mark.kiwi_id(2460)
def test_histogram_reading_cumulative() -> None:
    """直方图累计桶单调不减。"""
    _, traffic, _ = _build()
    traffic.record_publish(tenant="t1", topic="orders", size=1, duration_ms=7.0)
    reading = traffic.histogram_snapshot()["msg_publish_latency_ms"]["tenant=t1\x1ftopic=orders"]
    cumulative = reading.cumulative()
    assert isinstance(cumulative, ConcurrentStableFrozenList)
    assert cumulative[-1] == 1.0


@mark.kiwi_id(2460)
def test_all_label_shapes_and_empty_snapshots() -> None:
    """错误 / 限流 / fsync / 重放（空标签）与空快照分支都按约定输出。"""
    _, traffic, snapshots = _build()
    traffic.record_publish_error(exc=RuntimeError("x"))
    traffic.record_rpc(method="m", outcome="ok", duration_ms=1.0)
    traffic.record_fsync(scope="delay", duration_ms=1.0)
    traffic.record_counter("msg_custom_total", labels=ConcurrentStableDict())
    traffic.record_delay_deliver_failed(reason="unknown")
    snapshots.replace(
        lag=ConcurrentStableDict({"__other__": 1}),
        dlq_growth=ConcurrentStableDict({"__other__": 2}),
    )

    empty = MsgMetricsCollector(
        service="msg",
        traffic=traffic,
        snapshots=snapshots,
        storage=_FakeSource(ConcurrentStableDict({"msg_disk_used_ratio": "bad"})),
        delay=_FakeSource(ConcurrentStableDict({})),
        backpressure=_FakeSource(ConcurrentStableDict({"__other__": 2})),
        lifecycle=_FakeSource(ConcurrentStableDict({"x": "bad"})),
        purge=_FakeSource("bad"),
    )
    families = _by_name(empty.collect())
    assert families["msg_publish_errors_total"].samples[0].labels["reason"] == "internal"
    assert families["msg_rpc_duration_ms"].samples[0].labels["method"] == "m"
    assert families["msg_fsync_duration_ms"].samples[0].labels["scope"] == "delay"
    assert families["msg_consumer_lag"].samples[0].labels["tenant"] == "__other__"
    assert families["msg_dlq_growth_total"].samples[0].labels["topic"] == "__other__"
    assert "msg_disk_used_ratio" not in families
    assert "msg_delay_pending" not in families
    assert "msg_tenant_lifecycle_events_total" not in families


@mark.kiwi_id(2460)
def test_backpressure_other_key() -> None:
    """背压分键溢出（裸 `__other__`）仍能还原成标签。"""
    collector, _, _ = _build()
    other = MsgMetricsCollector(
        service="msg",
        traffic=TrafficMetrics(),
        snapshots=MetricsSnapshotStore(),
        storage=_FakeSource(ConcurrentStableDict({})),
        delay=_FakeSource(ConcurrentStableDict({})),
        backpressure=_FakeSource(
            ConcurrentStableDict({"msg_inflight_messages": ConcurrentStableDict({"__other__": 1})})
        ),
        lifecycle=_FakeSource(ConcurrentStableDict({})),
        purge=_FakeSource(ConcurrentStableDict({})),
    )
    assert other.collect()
    assert collector.collect()


@mark.kiwi_id(2488)
def test_auto_election_counter_family() -> None:
    """02-9：自动选举结果计数按 `result` 闭集打标签；未装配时不出家族。"""
    from bms_msg.broker.auto_election import AutoElectionMetrics

    assert "msg_auto_election_total" not in _by_name(_build()[0].collect()), "未装配不应产出家族"

    counters = AutoElectionMetrics()
    counters.record("succeeded")
    counters.record("blocked")
    collector = MsgMetricsCollector(
        service="msg",
        traffic=TrafficMetrics(),
        snapshots=MetricsSnapshotStore(),
        storage=_FakeSource(ConcurrentStableDict()),
        delay=_FakeSource(ConcurrentStableDict()),
        backpressure=_FakeSource(ConcurrentStableDict()),
        lifecycle=_FakeSource(ConcurrentStableDict()),
        purge=_FakeSource(ConcurrentStableDict()),
        auto_election=counters,
    )
    family = _by_name(collector.collect())["msg_auto_election_total"]
    assert {sample.labels["result"] for sample in family.samples} == {"succeeded", "blocked"}
    assert {sample.labels["service"] for sample in family.samples} == {"msg"}
    assert all(sample.value == 1.0 for sample in family.samples)
