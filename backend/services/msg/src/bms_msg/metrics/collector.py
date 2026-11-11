"""Prometheus 快照收集器（06-3-1）：把 msg 各采集器快照转成 `MetricFamily`。

**只读**：`collect()` 只做内存遍历与格式转换，不做 I/O、不查数据库；异步事实由采样器先写入
`MetricsSnapshotStore`。**单出口**：本收集器注册进基座 `PrometheusMetrics` 的注册表，
根路径 `/metrics` 由基座提供。
**service 标签**：基座写入式指标由实现统一注入；自定义收集器不受该机制覆盖，本类在每条指标上
显式带 `service`，保证全栈归因口径一致。
"""

from __future__ import annotations

from typing import Any, Protocol, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from prometheus_client.metrics_core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    Metric,
)

from bms_msg.broker.replication import ReplicationMetrics, ReplicationMetricsSnapshot
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.metrics.names import LATENCY_BUCKETS_MS, METRIC_HELP
from bms_msg.metrics.traffic import PAIR_SEPARATOR, TrafficMetrics

__all__ = ["MsgMetricsCollector", "SnapshotSource"]

_OTHER = "__other__"
_TOTAL = "__total__"
_DEFAULT_HELP = "msg 消息平台指标。"


class SnapshotSource(Protocol):
    """快照源的最小接口（既有采集器结构上满足）。"""

    def snapshot(self) -> ConcurrentStableDict[str, Any]:
        """导出当前指标快照。

        Returns:
            ConcurrentStableDict[str, Any]: 指标名 → 值 / 分键字典。
        """
        ...


class MsgMetricsCollector:
    """msg 指标收集器（注册进基座注册表；`/metrics` 拉取时调用）。"""

    def __init__(
        self,
        *,
        service: str,
        traffic: TrafficMetrics,
        snapshots: MetricsSnapshotStore,
        storage: SnapshotSource,
        delay: SnapshotSource,
        backpressure: SnapshotSource,
        lifecycle: SnapshotSource,
        purge: SnapshotSource,
        replication: ReplicationMetrics | None = None,
    ) -> None:
        """初始化。

        Args:
            service: 服务身份（统一注入 `service` 标签）。
            traffic: 事件点采集器。
            snapshots: 异步指标采样快照。
            storage: 存储水位采集器。
            delay: 延迟区采集器。
            backpressure: 背压采集器。
            lifecycle: 租户生命周期计数。
            purge: 租户到期清理计数。
            replication: 复制指标采集点（02-4；None 表示复制组件未装配）。
        """
        self._service = service
        self._traffic = traffic
        self._snapshots = snapshots
        self._storage = storage
        self._delay = delay
        self._backpressure = backpressure
        self._lifecycle = lifecycle
        self._purge = purge
        self._replication = replication

    def describe(self) -> ConcurrentStableList[Metric]:
        """产出指标名集合（注册期冲突检测；只读、无副作用）。

        Returns:
            ConcurrentStableList[Metric]: 与 `collect()` 同名的指标家族。
        """
        return self.collect()

    def collect(self) -> ConcurrentStableList[Metric]:
        """产出当前指标快照。

        Returns:
            list[Metric]: Prometheus 指标家族（空数据不产出空家族）。
        """
        families: ConcurrentStableList[Metric] = ConcurrentStableList()
        self._collect_traffic(families)
        self._collect_lag(families)
        self._collect_dlq(families)
        self._collect_replay(families)
        self._collect_storage(families)
        self._collect_delay(families)
        self._collect_backpressure(families)
        self._collect_counters(families, name="msg_tenant_lifecycle_events_total", source=self._lifecycle)
        self._collect_counters(families, name="msg_tenant_purge_total", source=self._purge)
        families.add(self._isr_family())
        self._collect_replication(families)
        return families

    def _collect_traffic(self, families: ConcurrentStableList[Metric]) -> None:
        """事件点计数器与直方图。"""
        for name, series in self._traffic.counter_snapshot().items():
            family = CounterMetricFamily(name, _help(name), labels=["service", *_business_labels(name)])
            for key, value in series.items():
                labels = self._decode_key(key)
                family.add_metric([self._service, *labels], float(value))
            if family.samples:  # pragma: no cover - 空 series 不可达（record_* 创建即写键）
                families.add(family)
        for name, series in self._traffic.histogram_snapshot().items():
            labels = ["service", *_business_labels(name)]
            family = HistogramMetricFamily(name, _help(name), labels=labels)
            for key, reading in series.items():
                decoded = self._decode_key(key)
                buckets: ConcurrentStableList[tuple[str, float]] = ConcurrentStableList()
                cumulative = 0.0
                raw = list(reading.buckets)
                for index, bound in enumerate(LATENCY_BUCKETS_MS):
                    cumulative += float(raw[index]) if index < len(raw) else 0.0
                    buckets.add((str(bound), cumulative))
                cumulative += float(raw[len(LATENCY_BUCKETS_MS)]) if len(raw) > len(LATENCY_BUCKETS_MS) else 0.0
                buckets.add(("+Inf", cumulative))
                family.add_metric([self._service, *decoded], list(buckets), reading.sum_ms)
            if family.samples:  # pragma: no cover - 空 series 不可达（record_histogram 创建即观测）
                families.add(family)

    def _collect_lag(self, families: ConcurrentStableList[Metric]) -> None:
        """消费积压（按租户 / 主题求和）。"""
        snapshot = self._snapshots.snapshot.lag
        if not snapshot:
            return
        family = GaugeMetricFamily("msg_consumer_lag", _help("msg_consumer_lag"), labels=["service", "tenant", "topic"])
        for key, value in snapshot.items():
            tenant, topic = _split_topic_key(key)
            family.add_metric([self._service, tenant, topic], float(value))
        families.add(family)

    def _collect_dlq(self, families: ConcurrentStableList[Metric]) -> None:
        """死信存量与累计增长。"""
        snapshot = self._snapshots.snapshot
        if snapshot.dlq:
            family = GaugeMetricFamily("msg_dlq_total", _help("msg_dlq_total"), labels=["service", "tenant", "topic"])
            for key, value in snapshot.dlq.items():
                tenant, topic = _split_topic_key(key)
                family.add_metric([self._service, tenant, topic], float(value))
            families.add(family)
        if snapshot.dlq_growth:
            family = CounterMetricFamily(
                "msg_dlq_growth_total", _help("msg_dlq_growth_total"), labels=["service", "tenant", "topic"]
            )
            for key, value in snapshot.dlq_growth.items():
                tenant, topic = _split_topic_key(key)
                family.add_metric([self._service, tenant, topic], float(value))
            families.add(family)

    def _collect_replay(self, families: ConcurrentStableList[Metric]) -> None:
        """重放任务数（按状态）与累计写入 / 失败。"""
        snapshot = self._snapshots.snapshot
        if snapshot.replay:
            family = GaugeMetricFamily("msg_replay_tasks", _help("msg_replay_tasks"), labels=["service", "status"])
            for status, value in snapshot.replay.items():
                family.add_metric([self._service, status], float(value))
            families.add(family)
        replayed = CounterMetricFamily(
            "msg_replay_replayed_total", _help("msg_replay_replayed_total"), labels=["service"]
        )
        replayed.add_metric([self._service], float(snapshot.replay_replayed))
        families.add(replayed)
        failed = CounterMetricFamily(
            "msg_replay_failed_total", _help("msg_replay_failed_total"), labels=["service", "reason"]
        )
        failed.add_metric([self._service, "task_failed"], float(snapshot.replay_failed))
        families.add(failed)

    def _collect_storage(self, families: ConcurrentStableList[Metric]) -> None:
        """磁盘水位（多卷取最大值）与存储拒绝。"""
        snapshot = self._storage.snapshot()
        used = _as_values(snapshot.get("msg_disk_used_ratio"))
        ratios = [float(value) for value in used.values() if isinstance(value, int | float)]
        if ratios:
            family = GaugeMetricFamily("msg_disk_used_ratio", _help("msg_disk_used_ratio"), labels=["service"])
            family.add_metric([self._service], max(ratios))
            families.add(family)
        rejected = _as_values(snapshot.get("msg_storage_rejected_total"))
        family = CounterMetricFamily(
            "msg_storage_rejected_total", _help("msg_storage_rejected_total"), labels=["service", "reason"]
        )
        for reason, value in rejected.items():
            if reason == _TOTAL or not isinstance(value, int):
                continue
            family.add_metric([self._service, str(reason)], float(value))
        if family.samples:
            families.add(family)

    def _collect_delay(self, families: ConcurrentStableList[Metric]) -> None:
        """延迟区：待投 / 段数 / 拒绝。"""
        snapshot = self._delay.snapshot()
        pending = _as_values(snapshot.get("msg_delay_pending"))
        family = GaugeMetricFamily("msg_delay_pending", _help("msg_delay_pending"), labels=["service", "tenant"])
        for tenant, value in pending.items():
            if tenant == _TOTAL or not isinstance(value, int):
                continue
            family.add_metric([self._service, str(tenant)], float(value))
        if family.samples:
            families.add(family)
        segments = snapshot.get("msg_delay_segments")
        if isinstance(segments, int):
            segments_family = GaugeMetricFamily("msg_delay_segments", _help("msg_delay_segments"), labels=["service"])
            segments_family.add_metric([self._service], float(segments))
            families.add(segments_family)
        rejected = _as_values(snapshot.get("msg_delay_rejected_total"))
        rejected_family = CounterMetricFamily(
            "msg_delay_rejected_total", _help("msg_delay_rejected_total"), labels=["service", "scope"]
        )
        for scope, value in rejected.items():
            if scope == _TOTAL or not isinstance(value, int):
                continue
            rejected_family.add_metric([self._service, str(scope)], float(value))
        if rejected_family.samples:
            families.add(rejected_family)

    def _collect_backpressure(self, families: ConcurrentStableList[Metric]) -> None:
        """背压：inflight / 暂停 / 限流 / 长轮询。"""
        snapshot = self._backpressure.snapshot()
        for name in ("msg_inflight_messages", "msg_inflight_bytes", "msg_window_stall_total"):
            values = _as_values(snapshot.get(name))
            if not values:
                continue
            family_type = GaugeMetricFamily if name.startswith("msg_inflight") else CounterMetricFamily
            family = family_type(name, _help(name), labels=["service", "tenant", "topic"])
            for key, value in values.items():
                if key == _TOTAL or not isinstance(value, int):
                    continue
                tenant, topic = _split_backpressure_key(str(key))
                family.add_metric([self._service, tenant, topic], float(value))
            if family.samples:
                families.add(family)
        rate = _as_values(snapshot.get("msg_rate_limit_rejected_total"))
        family = CounterMetricFamily(
            "msg_rate_limit_rejected_total",
            _help("msg_rate_limit_rejected_total"),
            labels=["service", "scope", "tenant", "topic"],
        )
        for key, value in rate.items():
            if key == _TOTAL or not isinstance(value, int):
                continue
            scope, tenant, topic = _split_rate_key(str(key))
            family.add_metric([self._service, scope, tenant, topic], float(value))
        if family.samples:
            families.add(family)
        waiters = snapshot.get("msg_long_poll_waiters")
        if isinstance(waiters, int):
            family = GaugeMetricFamily("msg_long_poll_waiters", _help("msg_long_poll_waiters"), labels=["service"])
            family.add_metric([self._service], float(waiters))
            families.add(family)
        for outcome in ("changed", "timeout", "rejected"):
            name = f"msg_long_poll_{outcome}_total"
            value = snapshot.get(name)
            if not isinstance(value, int):
                continue
            family = CounterMetricFamily(name, _help(name), labels=["service"])
            family.add_metric([self._service], float(value))
            families.add(family)

    def _collect_counters(self, families: ConcurrentStableList[Metric], *, name: str, source: SnapshotSource) -> None:
        """把「结果 → 计数」类采集器投影成 Counter（生命周期 / 过期清理）。"""
        values = source.snapshot()
        if not hasattr(values, "items"):
            return
        family = CounterMetricFamily(name, _help(name), labels=["service", "result"])
        for result, value in values.items():
            if not isinstance(value, int):
                continue
            family.add_metric([self._service, str(result)], float(value))
        if family.samples:
            families.add(family)

    def _isr_family(self) -> Metric:
        """ISR 收缩计数（02-4 起读真实计数；未装配复制时为 0）。"""
        family = CounterMetricFamily("msg_isr_shrink_total", _help("msg_isr_shrink_total"), labels=["service"])
        total = 0 if self._replication is None else self._replication.shrink_total
        family.add_metric([self._service], float(total))
        return family

    def _collect_replication(self, families: ConcurrentStableList[Metric]) -> None:
        """ISR 大小与副本延迟（02-4；按 `topic_id` + `partition` 分键）。

        Args:
            families: 指标家族收集列表（原地追加）。
        """
        if self._replication is None:
            return
        snapshot: ReplicationMetricsSnapshot = self._replication.snapshot()
        if not snapshot.rows:  # pragma: no cover - 无分区时无可输出行
            return
        isr = GaugeMetricFamily("msg_isr_size", _help("msg_isr_size"), labels=["service", "topic_id", "partition"])
        lag = GaugeMetricFamily(
            "msg_replication_lag_records",
            _help("msg_replication_lag_records"),
            labels=["service", "topic_id", "partition"],
        )
        for row in snapshot.rows:
            labels = [self._service, str(row.topic_id), str(row.partition_no)]
            isr.add_metric(labels, float(row.isr_size))
            lag.add_metric(labels, float(row.lag_records))
        families.add(isr)
        families.add(lag)

    def _decode_key(self, key: str) -> ConcurrentStableList[str]:
        """把事件点采集器的内部键还原成业务标签值列表。

        Args:
            key: `键=值` 以 `\\x1f` 连接的内部键。

        Returns:
            ConcurrentStableList[str]: 按指标标签顺序的值。
        """
        labels: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        if not key:
            return ConcurrentStableList()
        for pair in key.split(PAIR_SEPARATOR):
            name, _, value = pair.partition("=")
            labels.set(name, value)
        return self._ordered_values(labels)

    @staticmethod
    def _ordered_values(labels: ConcurrentStableDict[str, str]) -> ConcurrentStableList[str]:
        """按指标约定顺序取值（tenant → topic → reason → scope → method → outcome → result）。

        Args:
            labels: 解析后的标签集。

        Returns:
            ConcurrentStableList[str]: 已存在的业务标签值（顺序固定）。
        """
        order = ("tenant", "topic", "reason", "scope", "method", "outcome", "result")
        values: ConcurrentStableList[str] = ConcurrentStableList()
        for key in order:
            if key in labels:
                values.add(labels[key])
        return values


def _business_labels(name: str) -> ConcurrentStableList[str]:
    """返回指标的业务标签顺序（与 `TrafficMetrics` 埋点口径一致）。

    Args:
        name: 指标名。

    Returns:
        ConcurrentStableList[str]: 业务标签名（不含 `service`）。
    """
    if name in (
        "msg_messages_in_total",
        "msg_bytes_in_total",
        "msg_messages_out_total",
        "msg_publish_latency_ms",
        "msg_consume_latency_ms",
        "msg_window_stall_total",
        "msg_dlq_growth_total",
    ):
        return ConcurrentStableList(("tenant", "topic"))
    if name in ("msg_publish_errors_total", "msg_consume_errors_total", "msg_delay_deliver_failed_total"):
        return ConcurrentStableList(("reason",))
    if name == "msg_rpc_calls_total":
        return ConcurrentStableList(("method", "outcome"))
    if name == "msg_rpc_duration_ms":
        return ConcurrentStableList(("method",))
    if name == "msg_fsync_duration_ms":
        return ConcurrentStableList(("scope",))
    return ConcurrentStableList()


def _as_values(value: Any) -> ConcurrentStableDict[str, Any]:
    """把采集器快照中的值收敛成标签映射（非映射 / 缺失一律当空）。

    Args:
        value: 快照值（通常是分键字典）。

    Returns:
        ConcurrentStableDict[str, Any]: 标签键 → 值；非映射返回空。
    """
    if isinstance(value, ConcurrentStableDict):
        return cast("ConcurrentStableDict[str, Any]", value)
    return ConcurrentStableDict[str, Any]()


def _help(name: str) -> str:
    """取指标 HELP 文案。

    Args:
        name: 指标名。

    Returns:
        str: 文案（未登记名回退通用文案）。
    """
    return METRIC_HELP.get(name, _DEFAULT_HELP)


def _split_topic_key(key: str) -> tuple[str, str]:
    """拆 `tenant\\x1ftopic` 快照键。

    Args:
        key: 快照键。

    Returns:
        tuple[str, str]: `(tenant, topic)`。
    """
    if key == _OTHER:
        return _OTHER, _OTHER
    tenant, _, topic = key.partition(PAIR_SEPARATOR)
    return tenant, topic


def _split_backpressure_key(key: str) -> tuple[str, str]:
    """拆背压采集器的 `tenant|topic` 键。

    Args:
        key: 内部键。

    Returns:
        tuple[str, str]: `(tenant, topic)`。
    """
    if key == _OTHER:
        return _OTHER, _OTHER
    tenant, _, topic = key.partition("|")
    return tenant, topic


def _split_rate_key(key: str) -> tuple[str, str, str]:
    """拆限流采集器的 `scope=x:tenant|topic` 键。

    Args:
        key: 内部键。

    Returns:
        tuple[str, str, str]: `(scope, tenant, topic)`。
    """
    body = key.removeprefix("scope=")
    scope, _, rest = body.partition(":")
    tenant, _, topic = rest.partition("|")
    return scope, tenant, topic
