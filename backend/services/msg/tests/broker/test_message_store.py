"""消息存储端口与内存实现（msg 任务 01-2；Kiwi 2414）。

覆盖：append 分配连续 offset（唯一分配方）、fetch 起点 / 上限 / 不足、leo 递增、跨分区隔离、
start_offset 恒 0（保留期清理归 01-6 / 8B_02）。
"""

from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.log import InMemoryMessageStore, StoredMessage

pytestmark = pytest.mark.kiwi_id(2414)

_PUBLISHED_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""固定发布时间（断言记录字段时用）。"""


async def _append(
    store: InMemoryMessageStore, *, offset_hint: str = "m1", topic_id: int = 1, partition_no: int = 0
) -> StoredMessage:
    """追加一条消息。

    Args:
        store: 消息存储。
        offset_hint: `message_id` 取值。
        topic_id: 主题主键。
        partition_no: 分区号。

    Returns:
        StoredMessage: 追加后的记录。
    """
    return await store.append(
        topic_id=topic_id,
        partition_no=partition_no,
        message_id=offset_hint,
        log_id=f"log-{offset_hint}",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace_id="trace-1",
        published_at=_PUBLISHED_AT,
    )


async def test_append_assigns_contiguous_offsets(message_store: InMemoryMessageStore) -> None:
    """append 分配 0 起连续 offset，并原样保留消息字段。"""
    first = await _append(message_store, offset_hint="m1")
    second = await _append(message_store, offset_hint="m2")
    assert (first.offset, second.offset) == (0, 1)
    assert (first.message_id, first.log_id, first.key, first.payload, first.trace_id) == (
        "m1",
        "log-m1",
        "order-1",
        b"payload",
        "trace-1",
    )
    assert dict(first.headers.items()) == {"source": "test"}
    assert first.published_at == _PUBLISHED_AT


async def test_leo_tracks_appends(message_store: InMemoryMessageStore) -> None:
    """LEO 随追加递增（空分区为 0）。"""
    assert await message_store.leo(topic_id=1, partition_no=0) == 0
    await _append(message_store)
    await _append(message_store, offset_hint="m2")
    assert await message_store.leo(topic_id=1, partition_no=0) == 2


async def test_fetch_respects_from_offset_and_limit(message_store: InMemoryMessageStore) -> None:
    """fetch 从 `from_offset` 起取至多 N 条（不足即返回实际条数）。"""
    for index in range(3):
        await _append(message_store, offset_hint=f"m{index}")
    assert [
        item.offset for item in await message_store.fetch(topic_id=1, partition_no=0, from_offset=1, max_messages=1)
    ] == [1]
    assert [
        item.offset for item in await message_store.fetch(topic_id=1, partition_no=0, from_offset=1, max_messages=10)
    ] == [1, 2]
    assert list(await message_store.fetch(topic_id=1, partition_no=0, from_offset=5, max_messages=10)) == []


async def test_fetch_clamps_negative_from_offset(message_store: InMemoryMessageStore) -> None:
    """负数起点按 0 处理（不抛切片异常）。"""
    await _append(message_store)
    assert [
        item.offset for item in await message_store.fetch(topic_id=1, partition_no=0, from_offset=-5, max_messages=10)
    ] == [0]


async def test_partitions_are_isolated(message_store: InMemoryMessageStore) -> None:
    """不同主题 / 分区的日志互不影响。"""
    await _append(message_store, topic_id=1, partition_no=0)
    await _append(message_store, topic_id=1, partition_no=1)
    await _append(message_store, topic_id=2, partition_no=0)
    assert await message_store.leo(topic_id=1, partition_no=0) == 1
    assert await message_store.leo(topic_id=1, partition_no=1) == 1
    assert await message_store.leo(topic_id=2, partition_no=0) == 1


async def test_start_offset_is_zero_in_memory(message_store: InMemoryMessageStore) -> None:
    """内存实现不做保留期清理，起点恒 0（保留期归 01-6 / 8B_02）。"""
    assert await message_store.start_offset(topic_id=1, partition_no=0) == 0
