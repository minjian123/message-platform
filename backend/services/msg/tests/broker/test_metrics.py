"""背压指标 collector（任务 03-5；Kiwi 2425）。

口径要点：gauge 是**覆盖**不是累加（`__total__` 读时求和，避免两份账漂）；分键有界，
溢出归 `__other__`；等待者数量直接读唤醒器，不重复记一份。
"""

import asyncio

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.backpressure import InflightUsage
from bms_msg.broker.metrics import OTHER_KEY, TOTAL_KEY, BackpressureMetrics
from bms_msg.broker.waiter import DeliveryWaiter, WaitOutcome

pytestmark = pytest.mark.kiwi_id(2425)


def test_inflight_gauge_overwrites_instead_of_accumulating() -> None:
    """同一键重复写入是覆盖；合计为分键求和。"""
    metrics = BackpressureMetrics()

    metrics.observe_inflight(tenant="demo", topic="audit", usage=InflightUsage(messages=3, bytes=30))
    metrics.observe_inflight(tenant="demo", topic="audit", usage=InflightUsage(messages=1, bytes=10))
    metrics.observe_inflight(tenant="demo", topic="other", usage=InflightUsage(messages=2, bytes=20))

    snapshot = metrics.snapshot()
    messages = snapshot["msg_inflight_messages"]
    assert messages[TOTAL_KEY] == 3
    assert messages["demo|audit"] == 1
    assert snapshot["msg_inflight_bytes"][TOTAL_KEY] == 30


def test_window_stall_and_rate_reject_counters() -> None:
    """传输层暂停按主题累加；限流拒绝按触发级别分键。"""
    metrics = BackpressureMetrics()

    metrics.record_window_stall(tenant="demo", topic="audit")
    metrics.record_window_stall(tenant="demo", topic="audit")
    metrics.record_rate_rejected(scope="tenant", tenant="demo", topic="audit")
    metrics.record_rate_rejected(scope="topic", tenant="demo", topic="audit")

    snapshot = metrics.snapshot()
    assert snapshot["msg_window_stall_total"][TOTAL_KEY] == 2
    assert snapshot["msg_window_stall_total"]["demo|audit"] == 2
    rejected = snapshot["msg_rate_limit_rejected_total"]
    assert rejected[TOTAL_KEY] == 2
    assert rejected["scope=tenant:demo|audit"] == 1
    assert rejected["scope=topic:demo|audit"] == 1


def test_wait_outcomes_are_counted_per_kind() -> None:
    """长轮询三种结果各有一个计数器。"""
    metrics = BackpressureMetrics()

    metrics.record_wait(WaitOutcome.CHANGED)
    metrics.record_wait(WaitOutcome.TIMEOUT)
    metrics.record_wait(WaitOutcome.REJECTED)
    metrics.record_wait(WaitOutcome.REJECTED)

    snapshot = metrics.snapshot()
    assert snapshot["msg_long_poll_changed_total"] == 1
    assert snapshot["msg_long_poll_timeout_total"] == 1
    assert snapshot["msg_long_poll_rejected_total"] == 2
    assert snapshot["msg_long_poll_waiters"] == 0


async def test_waiters_gauge_reads_live_value_from_waiter() -> None:
    """等待者数量实时取自唤醒器（不另存一份）。"""
    waiter = DeliveryWaiter()
    metrics = BackpressureMetrics(waiter=waiter)
    task = asyncio.create_task(waiter.wait((1, 0), version=0, timeout_ms=5_000))
    await asyncio.sleep(0)

    assert metrics.snapshot()["msg_long_poll_waiters"] == 1

    waiter.notify((1, 0))
    assert await task is WaitOutcome.CHANGED
    assert metrics.snapshot()["msg_long_poll_waiters"] == 0


def test_topic_keys_overflow_into_other_bucket() -> None:
    """分键超过上限后归入 `__other__`，已知键不受影响。"""
    metrics = BackpressureMetrics(topic_key_limit=1)

    metrics.observe_inflight(tenant="demo", topic="audit", usage=InflightUsage(messages=1, bytes=1))
    metrics.observe_inflight(tenant="demo", topic="other", usage=InflightUsage(messages=2, bytes=2))
    metrics.observe_inflight(tenant="demo", topic="audit", usage=InflightUsage(messages=5, bytes=5))

    snapshot = metrics.snapshot()
    messages = snapshot["msg_inflight_messages"]
    assert messages["demo|audit"] == 5
    assert messages[OTHER_KEY] == 2
    assert messages[TOTAL_KEY] == 7


def test_snapshot_returns_stable_dicts() -> None:
    """快照结构可安全传出（`ConcurrentStableDict`，06-3 直接消费）。"""
    metrics = BackpressureMetrics()
    metrics.record_wait(WaitOutcome.TIMEOUT)

    snapshot = metrics.snapshot()

    assert isinstance(snapshot, ConcurrentStableDict)
    assert isinstance(snapshot["msg_inflight_messages"], ConcurrentStableDict)
