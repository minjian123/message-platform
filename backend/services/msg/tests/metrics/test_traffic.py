"""事件点采集器测试（06-3-1 / Kiwi 2460）。"""

from bms_core.core.concurrent import ConcurrentStableDict
from pytest import mark

from bms_msg.metrics.traffic import (
    PAIR_SEPARATOR,
    TrafficMetrics,
    decode_labels,
    get_traffic_metrics,
    reason_of,
    reset_traffic_metrics,
)


class _BizLike(Exception):
    """带业务码与结构化数据的异常（模拟 `BizError`）。"""

    def __init__(self, code: int, data: object | None = None) -> None:
        """初始化。

        Args:
            code: 业务错误码。
            data: 结构化数据。
        """
        super().__init__("boom")
        self.code = code
        self.data = data


@mark.kiwi_id(2460)
def test_publish_and_consume_counters() -> None:
    """发布 / 消费成功计数、字节数与直方图桶落点。"""
    metrics = TrafficMetrics()
    metrics.record_publish(tenant="t1", topic="orders", size=12, duration_ms=7.0)
    metrics.record_consume(tenant="t1", topic="orders", count=3, duration_ms=20.0)

    counters = metrics.counter_snapshot()
    assert counters["msg_messages_in_total"][f"tenant=t1{PAIR_SEPARATOR}topic=orders"] == 1
    assert counters["msg_bytes_in_total"][f"tenant=t1{PAIR_SEPARATOR}topic=orders"] == 12
    assert counters["msg_messages_out_total"][f"tenant=t1{PAIR_SEPARATOR}topic=orders"] == 3

    histograms = metrics.histogram_snapshot()
    publish = histograms["msg_publish_latency_ms"][f"tenant=t1{PAIR_SEPARATOR}topic=orders"]
    assert publish.buckets[1] == 1
    assert publish.sum_ms == 7.0
    consume = histograms["msg_consume_latency_ms"][f"tenant=t1{PAIR_SEPARATOR}topic=orders"]
    assert consume.buckets[2] == 1


@mark.kiwi_id(2460)
def test_empty_consume_does_not_count_out() -> None:
    """空批（长轮询超时）不计出流量，但仍记录延迟。"""
    metrics = TrafficMetrics()
    metrics.record_consume(tenant="t1", topic="orders", count=0, duration_ms=15000.0)
    assert "msg_messages_out_total" not in metrics.counter_snapshot()
    assert metrics.histogram_snapshot()["msg_consume_latency_ms"]


@mark.kiwi_id(2460)
def test_topic_key_limit_merges_other() -> None:
    """超过分键上限的主题归入 `__other__`。"""
    metrics = TrafficMetrics(topic_key_limit=1)
    metrics.record_publish(tenant="t1", topic="a", size=1, duration_ms=1.0)
    metrics.record_publish(tenant="t1", topic="b", size=1, duration_ms=1.0)
    keys = metrics.counter_snapshot()["msg_messages_in_total"]
    assert f"tenant=t1{PAIR_SEPARATOR}topic=__other__" in keys


@mark.kiwi_id(2460)
def test_reason_mapping() -> None:
    """错误原因按业务码归类为闭集七类。"""
    assert reason_of(_BizLike(350022, ConcurrentStableDict({"reason": "watermark"}))) == "watermark"
    assert reason_of(_BizLike(350022, ConcurrentStableDict({"reason": "tenant_quota"}))) == "quota"
    assert reason_of(_BizLike(350022, ConcurrentStableDict({"reason": "sampling_failed"}))) == "unavailable"
    assert reason_of(_BizLike(350025)) == "quota"
    assert reason_of(_BizLike(350005)) == "rate_limit"
    assert reason_of(_BizLike(350027)) == "acl"
    assert reason_of(_BizLike(350009)) == "param"
    assert reason_of(_BizLike(350011)) == "unavailable"
    assert reason_of(_BizLike(999999)) == "internal"
    assert reason_of(RuntimeError("x")) == "internal"


@mark.kiwi_id(2460)
def test_rpc_and_fsync_records() -> None:
    """RPC / fsync / 延迟转投失败埋点结构与标签正确。"""
    metrics = TrafficMetrics()
    metrics.record_rpc(method="msg.publish", outcome="ok", duration_ms=3.0)
    metrics.record_fsync(scope="log", duration_ms=2.0)
    metrics.record_delay_deliver_failed(reason="storage")

    counters = metrics.counter_snapshot()
    assert counters["msg_rpc_calls_total"][f"method=msg.publish{PAIR_SEPARATOR}outcome=ok"] == 1
    assert counters["msg_delay_deliver_failed_total"]["reason=storage"] == 1
    assert metrics.histogram_snapshot()["msg_fsync_duration_ms"]["scope=log"]


@mark.kiwi_id(2460)
def test_histogram_overflow_bucket() -> None:
    """超过最大桶的观测落 `+Inf` 桶。"""
    metrics = TrafficMetrics()
    metrics.record_fsync(scope="log", duration_ms=99_000.0)
    reading = metrics.histogram_snapshot()["msg_fsync_duration_ms"]["scope=log"]
    assert reading.buckets[-1] == 1


@mark.kiwi_id(2460)
def test_decode_labels_round_trip() -> None:
    """内部键可还原成标签集。"""
    labels = decode_labels(f"tenant=t1{PAIR_SEPARATOR}topic=a=b")
    assert labels["tenant"] == "t1"
    assert labels["topic"] == "a=b"


@mark.kiwi_id(2460)
def test_reset_clears_all() -> None:
    """`reset()` 清空全部计数与分键。"""
    metrics = TrafficMetrics()
    metrics.record_publish(tenant="t1", topic="a", size=1, duration_ms=1.0)
    metrics.reset()
    assert not metrics.counter_snapshot()
    assert not metrics.histogram_snapshot()


@mark.kiwi_id(2460)
def test_empty_labels_and_counter() -> None:
    """空标签键可正确编码 / 解码，计数器可直接累加。"""
    metrics = TrafficMetrics()
    metrics.record_counter("msg_custom_total", labels=ConcurrentStableDict())
    assert metrics.counter_snapshot()["msg_custom_total"][""] == 1
    assert decode_labels("") == ConcurrentStableDict()


@mark.kiwi_id(2460)
def test_process_singleton_reset() -> None:
    """进程级单例可被测试隔离重置。"""
    metrics = get_traffic_metrics()
    metrics.record_publish(tenant="t1", topic="a", size=1, duration_ms=1.0)
    reset_traffic_metrics()
    assert not metrics.counter_snapshot()
