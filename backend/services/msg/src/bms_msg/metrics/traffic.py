"""事件点指标采集器（06-3-1）：流量 / 延迟 / 错误 / RPC / 刷盘 / 重放 / 死信增长。

**只收集、不暴露**：与 03-5 / 02-6 / 02-7 的采集器同构——纯内存、不抛异常、`snapshot()` 只做拷贝；
Prometheus 文本由 `MsgMetricsCollector` 转换（出口只保留一处）。

**分键有界**：`(tenant, topic)` 复用 `METRICS_TOPIC_KEY_LIMIT`，超出归 `__other__`，防高基数打爆内存。
**标签键编码**：内部用不可见分隔符拼键（`\\x1f` / `\\x1e`），标签值由租户编码 / 主题名 / 闭集枚举构成，
不含控制字符；Collector 按同一分隔符还原。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.metrics.base import MetricLabels

from bms_msg.core.config import METRICS_TOPIC_KEY_LIMIT
from bms_msg.metrics.names import LATENCY_BUCKETS_MS

__all__ = [
    "LABEL_SEPARATOR",
    "PAIR_SEPARATOR",
    "HistogramReading",
    "TrafficMetrics",
    "decode_labels",
    "get_traffic_metrics",
    "reason_of",
    "reset_traffic_metrics",
]

PAIR_SEPARATOR = "\x1f"
"""标签对内 `键=值` 与其它标签对之间的分隔符。"""

LABEL_SEPARATOR = "\x1e"
"""标签对之间的分隔符（`键=值` 为最小单元）。"""

_OTHER_LABEL = "__other__"
"""分键溢出后的归并值。"""


@dataclass(frozen=True)
class HistogramReading(BaseValueObject):
    """直方图快照（Collector 读取；累计桶 + 总和 + 样本数）。"""

    buckets: ConcurrentStableFrozenList[int] = field(default_factory=lambda: ConcurrentStableFrozenList())
    """原始桶计数（长度 = 桶边界数 + 1，最后一项为 `+Inf`）。"""

    sum_ms: float = 0.0
    """观测值总和（毫秒）。"""

    count: int = 0
    """观测次数。"""

    def cumulative(self) -> ConcurrentStableFrozenList[float]:
        """把原始桶计数转成累计桶（Prometheus `le` 语义）。

        Returns:
            ConcurrentStableFrozenList[float]: 与桶边界等长的累计计数（不含 `+Inf`）。
        """
        result: ConcurrentStableList[float] = ConcurrentStableList()
        total = 0
        for index in range(len(LATENCY_BUCKETS_MS)):
            total += self.buckets[index] if index < len(self.buckets) else 0
            result.add(float(total))
        return ConcurrentStableFrozenList(result)


class _HistogramSeries(BaseFrameworkObject):
    """单个标签组合的直方图累加器（可变；只在采集器内部使用）。"""

    def __init__(self) -> None:
        """初始化（全零）。"""
        self.buckets: ConcurrentStableDict[int, int] = ConcurrentStableDict()
        self.sum_ms: float = 0.0
        self.count: int = 0

    def observe(self, value_ms: float) -> None:
        """记录一次观测（落到对应桶；超出上限落 `+Inf`）。

        Args:
            value_ms: 观测值（毫秒）。
        """
        index = len(LATENCY_BUCKETS_MS)
        for position, bound in enumerate(LATENCY_BUCKETS_MS):
            if value_ms <= bound:
                index = position
                break
        self.buckets[index] = self.buckets.get(index, 0) + 1
        self.sum_ms += value_ms
        self.count += 1

    def reading(self) -> HistogramReading:
        """导出快照。

        Returns:
            HistogramReading: 累计桶（长度 = 边界数 + 1）、总和、样本数。
        """
        buckets: ConcurrentStableList[int] = ConcurrentStableList()
        for index in range(len(LATENCY_BUCKETS_MS) + 1):
            buckets.add(self.buckets.get(index, 0))
        return HistogramReading(
            buckets=ConcurrentStableFrozenList(buckets),
            sum_ms=self.sum_ms,
            count=self.count,
        )


class TrafficMetrics(BaseFrameworkObject):
    """事件点采集器（进程级单例；判据读它、断言读它、06-3 出口也读它）。"""

    def __init__(self, *, topic_key_limit: int = METRICS_TOPIC_KEY_LIMIT) -> None:
        """初始化（全零）。

        Args:
            topic_key_limit: `(tenant, topic)` 分键上限（超出归 `__other__`）。
        """
        self._topic_key_limit = topic_key_limit
        self._topic_keys: ConcurrentStableSet[str] = ConcurrentStableSet()
        self._counters: ConcurrentStableDict[str, ConcurrentStableDict[str, int]] = ConcurrentStableDict()
        self._histograms: ConcurrentStableDict[str, ConcurrentStableDict[str, _HistogramSeries]] = (
            ConcurrentStableDict()
        )

    def record_publish(self, *, tenant: str, topic: str, size: int, duration_ms: float) -> None:
        """记录一次成功发布（立即入日志或进入延迟区）。

        Args:
            tenant: 租户编码。
            topic: 主题名。
            size: 载荷字节数。
            duration_ms: 发布处理耗时（毫秒）。
        """
        labels = self._topic_labels(tenant=tenant, topic=topic)
        self.record_counter("msg_messages_in_total", labels=labels)
        self.record_counter("msg_bytes_in_total", labels=labels, value=float(max(size, 0)))
        self.record_histogram("msg_publish_latency_ms", labels=labels, value_ms=duration_ms)

    def record_publish_error(self, *, exc: BaseException) -> None:
        """记录一次发布失败（按原因归类）。

        Args:
            exc: 抛出的异常（`BizError` 或其子类；未知异常归 `internal`）。
        """
        self.record_counter("msg_publish_errors_total", labels=self._reason_labels(exc))

    def record_consume(self, *, tenant: str, topic: str, count: int, duration_ms: float) -> None:
        """记录一次消费读取（返回条数可为 0）。

        Args:
            tenant: 租户编码。
            topic: 主题名。
            count: 本次返回条数。
            duration_ms: 批读取耗时（毫秒；不含长轮询等待）。
        """
        labels = self._topic_labels(tenant=tenant, topic=topic)
        if count > 0:
            self.record_counter("msg_messages_out_total", labels=labels, value=float(count))
        self.record_histogram("msg_consume_latency_ms", labels=labels, value_ms=duration_ms)

    def record_consume_error(self, *, exc: BaseException) -> None:
        """记录一次消费失败（按原因归类）。

        Args:
            exc: 抛出的异常。
        """
        self.record_counter("msg_consume_errors_total", labels=self._reason_labels(exc))

    def record_delay_deliver_failed(self, *, reason: str) -> None:
        """记录一次延迟到期转投失败。

        Args:
            reason: `tenant_unavailable` / `storage` / `unknown`。
        """
        self.record_counter("msg_delay_deliver_failed_total", labels=self._labels(("reason", reason)))

    def record_rpc(self, *, method: str, outcome: str, duration_ms: float) -> None:
        """记录一次远程调用。

        Args:
            method: 注册表内的固定方法名（禁止用户输入）。
            outcome: 结果（`ok` / `business_error` / `deadline_exceeded` / `cancelled` / `error`）。
            duration_ms: 服务端处理耗时（毫秒）。
        """
        self.record_counter("msg_rpc_calls_total", labels=self._labels(("method", method), ("outcome", outcome)))
        self.record_histogram("msg_rpc_duration_ms", labels=self._labels(("method", method)), value_ms=duration_ms)

    def record_fsync(self, *, scope: str, duration_ms: float) -> None:
        """记录一次刷盘。

        Args:
            scope: `log` / `delay`。
            duration_ms: 刷盘耗时（毫秒）。
        """
        self.record_histogram("msg_fsync_duration_ms", labels=self._labels(("scope", scope)), value_ms=duration_ms)

    def record_counter(self, name: str, *, labels: MetricLabels, value: float = 1.0) -> None:
        """累加一个计数器。

        Args:
            name: 指标名。
            labels: 标签集。
            value: 增量（默认 1）。
        """
        series = self._counters.get(name)
        if series is None:
            series = ConcurrentStableDict[str, int]()
            self._counters.set(name, series)
        key = _encode(labels)
        series.set(key, series.get(key, 0) + int(value))

    def record_histogram(self, name: str, *, labels: MetricLabels, value_ms: float) -> None:
        """记录一次直方图观测。

        Args:
            name: 指标名。
            labels: 标签集。
            value_ms: 观测值（毫秒）。
        """
        series_by_labels = self._histograms.get(name)
        if series_by_labels is None:
            series_by_labels = ConcurrentStableDict[str, _HistogramSeries]()
            self._histograms.set(name, series_by_labels)
        key = _encode(labels)
        series = series_by_labels.get(key)
        if series is None:
            series = _HistogramSeries()
            series_by_labels.set(key, series)
        series.observe(value_ms)

    def counter_snapshot(self) -> ConcurrentStableDict[str, ConcurrentStableDict[str, int]]:
        """导出计数器快照（Collector 消费）。

        Returns:
            ConcurrentStableDict[str, ConcurrentStableDict[str, int]]: 指标名 → 标签键 → 值。
        """
        snapshot: ConcurrentStableDict[str, ConcurrentStableDict[str, int]] = ConcurrentStableDict()
        for name, series in self._counters.items():
            copied: ConcurrentStableDict[str, int] = ConcurrentStableDict()
            for key, value in series.items():
                copied.set(key, value)
            snapshot.set(name, copied)
        return snapshot

    def histogram_snapshot(self) -> ConcurrentStableDict[str, ConcurrentStableDict[str, HistogramReading]]:
        """导出直方图快照（Collector 消费）。

        Returns:
            ConcurrentStableDict[str, ConcurrentStableDict[str, HistogramReading]]: 指标名 → 标签键 → 读数。
        """
        snapshot: ConcurrentStableDict[str, ConcurrentStableDict[str, HistogramReading]] = ConcurrentStableDict()
        for name, series_by_labels in self._histograms.items():
            copied: ConcurrentStableDict[str, HistogramReading] = ConcurrentStableDict()
            for key, series in series_by_labels.items():
                copied.set(key, series.reading())
            snapshot.set(name, copied)
        return snapshot

    def reset(self) -> None:
        """清空全部计数（测试隔离用）。"""
        self._topic_keys.clear()
        self._counters.clear()
        self._histograms.clear()

    def _topic_labels(self, *, tenant: str, topic: str) -> MetricLabels:
        """构造 `(tenant, topic)` 标签集（分键有界）。

        Args:
            tenant: 租户编码。
            topic: 主题名。

        Returns:
            MetricLabels: `tenant` + `topic`。
        """
        key = f"{tenant}{PAIR_SEPARATOR}{topic}"
        if key not in self._topic_keys:
            if len(self._topic_keys) >= self._topic_key_limit:
                return self._labels(("tenant", tenant), ("topic", _OTHER_LABEL))
            self._topic_keys.add(key)
        return self._labels(("tenant", tenant), ("topic", topic))

    def _reason_labels(self, exc: BaseException) -> MetricLabels:
        """按错误原因构造标签集（标签只放 `reason`）。

        Args:
            exc: 抛出的异常。

        Returns:
            MetricLabels: `reason`。
        """
        return self._labels(("reason", reason_of(exc)))

    @staticmethod
    def _labels(*pairs: tuple[str, str]) -> MetricLabels:
        """把键值对转成标签映射。

        Args:
            *pairs: 标签键值对（调用方均为闭集或受控值）。

        Returns:
            MetricLabels: 标签映射（插入序）。
        """
        labels: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        for key, value in pairs:
            labels.set(key, value)
        return labels


_DEFAULT_TRAFFIC = TrafficMetrics()
"""进程级采集器：数据面 / 存储层共用一份，避免逐层透传漏计。"""


def get_traffic_metrics() -> TrafficMetrics:
    """取进程级采集器。

    Returns:
        TrafficMetrics: 采集器单例。
    """
    return _DEFAULT_TRAFFIC


def reset_traffic_metrics() -> None:
    """清空进程级采集器（测试隔离用；生产路径不调用）。"""
    _DEFAULT_TRAFFIC.reset()


def reason_of(exc: BaseException) -> str:
    """把异常归类为闭集原因（发布 / 消费错误指标用）。

    Args:
        exc: 抛出的异常。

    Returns:
        str: `PUBLISH_REASONS` 之一。
    """
    code = getattr(exc, "code", None)
    data = getattr(exc, "data", None)
    data_reason = _data_reason(data)
    if code == 350022:
        if data_reason == "tenant_quota":
            return "quota"
        if data_reason == "sampling_failed":
            return "unavailable"
        return "watermark"
    if code in (350015, 350025, 350026):
        return "quota"
    if code == 350005:
        return "rate_limit"
    if code == 350027:
        return "acl"
    if code in (350001, 350002, 350003, 350006, 350009, 350010, 350018, 350019, 10001):
        return "param"
    if code in (350011, 350012, 80001, 80002):
        return "unavailable"
    if code is None:
        return "internal"
    return "internal"


def _data_reason(data: Any) -> str:
    """从异常 `data` 中取 `reason` 字段（无则空串）。

    Args:
        data: 异常携带的结构化数据。

    Returns:
        str: `reason` 值；取不到为空串。
    """
    if data is None or not hasattr(data, "get"):
        return ""
    value = data.get("reason")  # type: ignore[union-attr]
    return value if isinstance(value, str) else ""


def _encode(labels: MetricLabels) -> str:
    """把标签集编码成内部键（按标签名排序，保证同集同键）。

    Args:
        labels: 标签集。

    Returns:
        str: `键=值` 以 `\\x1f` 连接。
    """
    return PAIR_SEPARATOR.join(f"{key}={labels[key]}" for key in sorted(labels))


def decode_labels(key: str) -> ConcurrentStableDict[str, str]:
    """把内部键还原成标签集（Collector 用）。

    Args:
        key: `_encode` 产出的键。

    Returns:
        ConcurrentStableDict[str, str]: 标签集。
    """
    labels: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    if not key:
        return labels
    for pair in key.split(PAIR_SEPARATOR):
        name, _, value = pair.partition("=")
        labels.set(name, value)
    return labels
