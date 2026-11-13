"""F3 延迟转投退避（02-6-1 / Kiwi 2498）。

覆盖：1/2/5/10/30/60s 封顶 + 半抖动、时间轮不早投、`next_attempt_at` 落盘与旧记录兼容、
`_throttled` 退避变化即落盘、视图剩余时间取退避时刻。
"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

from datetime import timedelta
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore, _remaining_ms, with_attempt
from bms_msg.broker.delay_file import FileDelayStore, _DelayEntry, _put_header, _to_record
from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.record import (
    RECORD_FORMAT_VERSION,
    DelayStamp,
    RecordHeader,
    _decode_delay,
    decode_record,
    encode_record,
)
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.core.trace import EMPTY_TRACE

pytestmark = pytest.mark.kiwi_id(2498)


class _FailingMessages:
    """转投替身：可切换失败 / 成功（只实现调度器用到的 append）。"""

    def __init__(self) -> None:
        self._inner = InMemoryMessageStore()
        self.fail = True

    async def append(self, **kwargs: Any) -> StoredMessage:
        if self.fail:
            raise OSError(28, "No space left on device")
        return await self._inner.append(**kwargs)


def _record(clock: Any) -> DelayRecord:
    return DelayRecord(
        log_id="l1",
        message_id="m1",
        tenant_id=1,
        topic_id=1,
        namespace="msg",
        topic="t",
        partition_no=0,
        delay_level="1s",
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=clock(),
        deliver_at=clock() + timedelta(seconds=1),
    )


def _scheduler(store: InMemoryDelayStore, messages: _FailingMessages, wheel: DelayWheel, clock: Any) -> DelayScheduler:
    return DelayScheduler(
        store=store,
        messages=cast("Any", messages),
        wheel=wheel,
        now=clock,
        jitter=lambda low, high: low,
    )


async def test_backoff_sequence_and_no_early_retry(
    delay_store: InMemoryDelayStore, delay_wheel: DelayWheel, clock: Any, advance: Any
) -> None:
    """连续失败按 0.5/1/2.5/5/15/30s（半抖动下界）退避，未到点不重试。"""
    messages = _FailingMessages()
    scheduler = _scheduler(delay_store, messages, delay_wheel, clock)
    record = _record(clock)
    await delay_store.enqueue(record)
    delay_wheel.place(record)
    advance(1_000)

    expected = [500, 1_000, 2_500, 5_000, 15_000, 30_000]
    for index, delta_ms in enumerate(expected, start=1):
        assert await scheduler.run_once() == 0
        kept = (await delay_store.list_all())[0]
        assert kept.attempts == index
        assert kept.next_attempt_at == clock() + timedelta(milliseconds=delta_ms)
        assert await scheduler.run_once() == 0
        advance(delta_ms - 1)
        assert await scheduler.run_once() == 0
        advance(1)


async def test_backoff_then_success(
    delay_store: InMemoryDelayStore, delay_wheel: DelayWheel, clock: Any, advance: Any
) -> None:
    """失败后到点且故障恢复：转投成功、记录删除。"""
    messages = _FailingMessages()
    scheduler = _scheduler(delay_store, messages, delay_wheel, clock)
    record = _record(clock)
    await delay_store.enqueue(record)
    delay_wheel.place(record)
    advance(1_000)

    assert await scheduler.run_once() == 0
    messages.fail = False
    advance(500)
    assert await scheduler.run_once() == 1
    assert list(await delay_store.list_all()) == []


def test_next_attempt_at_persists_without_version_bump(clock: Any) -> None:
    """新键随记录落盘；解码回读一致，格式版本不变。"""
    record = with_attempt(_record(clock), next_attempt_at=clock() + timedelta(seconds=5))
    header = _put_header(record)
    blob = encode_record(header=header, payload=record.payload)
    decoded = decode_record(blob)
    stamp = cast("DelayStamp", decoded.header.delay)
    assert stamp.next_attempt_at_us is not None
    restored = _to_record(decoded.header, decoded.payload, stamp=stamp)
    assert restored.next_attempt_at == record.next_attempt_at
    assert decoded.header.format_version == header.format_version


def test_old_record_without_next_attempt_at_decodes_none() -> None:
    """旧记录不写新键：解码缺省 None，字节与 02-7 一致。"""
    stamp = DelayStamp(kind="put", deliver_at_us=1_000, delay_level="1s", attempts=2)
    blob = encode_record(
        header=RecordHeader(
            format_version=RECORD_FORMAT_VERSION,
            topic_id=1,
            partition_no=0,
            tenant_id=1,
            message_id="m1",
            log_id="l1",
            key=None,
            trace=EMPTY_TRACE,
            published_at_us=1,
            headers=(),
            content_type="application/msgpack",
            compression="none",
            uncompressed_len=None,
            delay=stamp,
        ),
        payload=b"x",
    )
    decoded = decode_record(blob)
    assert decoded.header.delay is not None
    assert decoded.header.delay.next_attempt_at_us is None


def test_decode_delay_ignores_bool_next_attempt_at() -> None:
    """布尔不是合法整数（避免 `True` 被当 1 微秒读成有效退避）。"""
    stamp = _decode_delay({"kind": "put", "deliver_at": 1_000, "attempts": 1, "next_attempt_at": True})
    assert stamp is not None
    assert stamp.next_attempt_at_us is None


def test_throttle_flushes_when_backoff_changes(clock: Any) -> None:
    """`next_attempt_at` 变化即落盘；仅 attempts 变化按原窗口节流。"""
    base = _record(clock)
    entry = _DelayEntry(record=base, seq=0, flushed_attempts=0, flushed_at_ms=0.0)

    class _Store:
        def _clock(self) -> float:
            return 1.0

    store = _Store()
    changed = with_attempt(base, next_attempt_at=clock() + timedelta(seconds=1))
    assert FileDelayStore._throttled(cast("Any", store), entry, changed) is False
    only_attempts = with_attempt(base)
    assert FileDelayStore._throttled(cast("Any", store), entry, only_attempts) is True


def test_remaining_ms_uses_next_attempt_at(clock: Any) -> None:
    """退避期剩余时间取 `next_attempt_at`，不显示 0。"""
    record = with_attempt(_record(clock), next_attempt_at=clock() + timedelta(seconds=5))
    assert _remaining_ms(record=record, now=clock()) == 5_000
