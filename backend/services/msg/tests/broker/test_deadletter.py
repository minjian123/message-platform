"""死信队列端口与内存实现（msg 任务 01-2；Kiwi 2414）。

覆盖：转入后可列（插入序）、同定位三元组重复转入幂等、`contains` 判定。
"""

from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.deadletter import (
    DEAD_LETTER_EXHAUSTED,
    DeadLetter,
    DeadLetterReason,
    InMemoryDeadLetterQueue,
)

pytestmark = pytest.mark.kiwi_id(2414)

_FAILED_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""固定转入时间。"""


def _letter(offset: int = 0, *, message_id: str = "m1") -> DeadLetter:
    """构造一条死信记录。

    Args:
        offset: 分区内序号。
        message_id: 消息 ID。

    Returns:
        DeadLetter: 死信记录。
    """
    return DeadLetter(
        message_id=message_id,
        topic_id=1,
        partition_no=0,
        offset=offset,
        attempts=5,
        failed_at=_FAILED_AT,
        reason=DEAD_LETTER_EXHAUSTED,
    )


async def test_add_and_list(dead_letters: InMemoryDeadLetterQueue) -> None:
    """转入的两条按插入序列出，字段原样保留。"""
    await dead_letters.add(_letter(0))
    await dead_letters.add(_letter(1, message_id="m2"))
    listed = await dead_letters.list_all()
    assert [(item.offset, item.message_id, item.attempts, item.reason) for item in listed] == [
        (0, "m1", 5, DEAD_LETTER_EXHAUSTED),
        (1, "m2", 5, DEAD_LETTER_EXHAUSTED),
    ]


async def test_duplicate_add_is_idempotent(dead_letters: InMemoryDeadLetterQueue) -> None:
    """同定位三元组重复转入不产生第二条。"""
    await dead_letters.add(_letter(0))
    await dead_letters.add(_letter(0, message_id="m1-again"))
    assert len(await dead_letters.list_all()) == 1


async def test_contains_scopes_by_partition_and_offset(dead_letters: InMemoryDeadLetterQueue) -> None:
    """`contains` 按 (topic, partition, offset) 判定。"""
    await dead_letters.add(_letter(2))
    assert await dead_letters.contains(topic_id=1, partition_no=0, offset=2)
    assert not await dead_letters.contains(topic_id=1, partition_no=0, offset=3)
    assert not await dead_letters.contains(topic_id=1, partition_no=1, offset=2)


def test_reason_enum_values() -> None:
    """原因三值：两值由消费端上报，`delivery_exhausted` 由 broker 独占。

    `StrEnum` 与字符串相等，因此既有比较、日志与 06 域展示都不需要改。
    """
    assert str(DeadLetterReason.DELIVERY_EXHAUSTED) == "delivery_exhausted"
    assert str(DeadLetterReason.SCHEMA_INVALID) == "schema_invalid"
    assert str(DeadLetterReason.PROCESSING_FAILED) == "processing_failed"
    assert DEAD_LETTER_EXHAUSTED == "delivery_exhausted"
    assert DEAD_LETTER_EXHAUSTED is DeadLetterReason.DELIVERY_EXHAUSTED


async def test_detail_is_preserved(dead_letters: InMemoryDeadLetterQueue) -> None:
    """消费端上报的字段级详情原样保存（06 域据此展示「哪个字段错了」）。"""
    letter = DeadLetter(
        message_id="m1",
        topic_id=1,
        partition_no=0,
        offset=0,
        attempts=1,
        failed_at=_FAILED_AT,
        reason=DeadLetterReason.SCHEMA_INVALID,
        detail=ConcurrentStableDict({"field": "order_id", "expected": "str", "actual": "int"}),
    )
    await dead_letters.add(letter)

    listed = await dead_letters.list_all()
    assert listed[0].detail["field"] == "order_id"
    assert str(listed[0].reason) == "schema_invalid"


def test_detail_defaults_to_empty() -> None:
    """不传详情：默认空映射（不是 None，展示侧不必判空）。"""
    letter = _letter(0)

    assert letter.detail == {}
    assert list(letter.detail.items()) == []
