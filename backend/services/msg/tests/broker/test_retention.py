"""保留期清理（msg 任务 01-6；Kiwi 2418）。

覆盖（详细设计 §10.1 用例 R1~R7）：

- **R1 时间维度整段删**：段内全部过期 → 该段整段删除、起点前移到下一段 `base_offset`；
- **R2 段内有一条未过期**：整段保留（按段而非按条——真实段删除就是这个语义）；
- **R3 容量维度补删**：`retention_bytes` 小值 → 从最老段删到达标；
- **R4 容量不删空**：只剩最后一段仍超上限 → 保留（宁可超配额，也不删刚写入的数据）；
- **R5 幂等**：连跑两次，第二次 `deleted_segments = 0`、起点不变；
- **R6 空分区**：不报错、起点 0、`leo` 0；
- **R7 前移后读一致性**：`fetch` 早于起点按起点返回、`leo` 不变。

段替身用 `InMemoryMessageStore(segment_records=2)`：每 2 条一段，用例只造几条就能验段边界，
与「整段删」语义同构（真实段粒度归 8B_02）。
"""

from datetime import UTC, datetime, timedelta

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.retention import RetentionService
from bms_msg.core.trace import EMPTY_TRACE

pytestmark = pytest.mark.kiwi_id(2418)

TOPIC_ID = 7
"""用例主题主键（存储层不校验存在性，取固定值即可）。"""

PARTITION = 0
"""用例分区号。"""

SEGMENT_RECORDS = 2
"""段替身条数（每 2 条一段，用例可精确断言段边界）。"""

PAYLOAD = b"0123456789"
"""固定载荷（10 字节，容量维度判据可算）。"""

_RETENTION_MS = 5_000


def _at(offset_seconds: int) -> datetime:
    """基准时刻 + 秒数（固定起点，用例不依赖真实时间）。

    Args:
        offset_seconds: 相对基准的秒数。

    Returns:
        datetime: 带时区的时刻。
    """
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=offset_seconds)


class StepClock:
    """可设定时刻的时钟（返回到期判据需要的「现在」）。"""

    def __init__(self, now: datetime) -> None:
        """初始化。

        Args:
            now: 当前时刻。
        """
        self._now = now

    def __call__(self) -> datetime:
        """当前时刻。

        Returns:
            datetime: 固定时刻。
        """
        return self._now


async def _append(store: InMemoryMessageStore, *, at: datetime, count: int = 1) -> None:
    """向用例分区追加若干记录（同一发布时间）。

    Args:
        store: 消息存储。
        at: 发布时间。
        count: 条数。
    """
    for index in range(count):
        await store.append(
            topic_id=TOPIC_ID,
            partition_no=PARTITION,
            message_id=f"m-{at.timestamp()}-{index}",
            log_id=f"l-{at.timestamp()}-{index}",
            key="order-1",
            payload=PAYLOAD,
            headers=ConcurrentStableDict({"source": "test"}),
            trace=EMPTY_TRACE,
            published_at=at,
        )


def _service(store: InMemoryMessageStore, *, now: datetime) -> RetentionService:
    """组装清理服务（存储 + 固定时钟）。

    Args:
        store: 消息存储。
        now: 当前时刻。

    Returns:
        RetentionService: 清理服务。
    """
    return RetentionService(store=store, now=StepClock(now))


async def test_time_dimension_drops_whole_segments() -> None:
    """R1：两个段全部过期 → 两段都删、起点前移到 LEO。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=4)

    outcome = await _service(store, now=base + timedelta(milliseconds=_RETENTION_MS + 1)).apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=0
    )

    assert (outcome.deleted_segments, outcome.start_offset, outcome.leo) == (2, 4, 4)
    assert await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == 4
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 4


async def test_segment_with_one_live_record_is_kept() -> None:
    """R2：段内有一条未过期 → 整段保留（按段而非按条）。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=1)
    await _append(store, at=base + timedelta(seconds=10), count=1)

    outcome = await _service(store, now=base + timedelta(seconds=10, milliseconds=1)).apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=0
    )

    assert (outcome.deleted_segments, outcome.start_offset) == (0, 0)


async def test_capacity_drops_oldest_segments() -> None:
    """R3：容量超上限 → 从最老段删到达标即停（一条记录约 27 字节、一段约 54 字节）。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=6)

    outcome = await _service(store, now=base).apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=120
    )

    assert (outcome.deleted_segments, outcome.start_offset) == (1, 2)


async def test_capacity_keeps_last_segment() -> None:
    """R4：容量维度不删空（只剩最后一段仍超上限 → 保留）。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=6)

    outcome = await _service(store, now=base).apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=1
    )

    assert (outcome.deleted_segments, outcome.start_offset) == (2, 4)
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 6


async def test_apply_is_idempotent() -> None:
    """R5：连跑两次，第二次无段可删且起点不变。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=4)
    service = _service(store, now=base + timedelta(milliseconds=_RETENTION_MS + 1))

    first = await service.apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=0
    )
    second = await service.apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=0
    )

    assert first.deleted_segments == 2
    assert (second.deleted_segments, second.start_offset) == (0, 4)


async def test_empty_partition_is_noop() -> None:
    """R6：空分区不报错、起点与 LEO 均为 0。"""
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)

    outcome = await _service(store, now=_at(0)).apply(
        topic_id=TOPIC_ID, partition_no=PARTITION, retention_ms=_RETENTION_MS, retention_bytes=0
    )

    assert (outcome.deleted_segments, outcome.start_offset, outcome.leo) == (0, 0, 0)


async def test_fetch_and_leo_after_truncate() -> None:
    """R7：起点前移后 `fetch` 按起点返回、`leo` 不变、越界截断拒绝。"""
    base = _at(0)
    store = InMemoryMessageStore(segment_records=SEGMENT_RECORDS)
    await _append(store, at=base, count=6)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=2)

    head = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    tail = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=5, max_messages=10)

    assert [message.offset for message in head] == [2, 3, 4, 5]
    assert [message.offset for message in tail] == [5]
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 6
    assert await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1) == 2
    with pytest.raises(ParamError):
        await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=99)
