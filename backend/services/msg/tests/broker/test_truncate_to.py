"""未提交尾部截断（msg 任务 02-5；Kiwi 2485）。

覆盖详细设计 §6.2 / §12.2 的第 22~23 项：

- 内存实现：可读区间与 LEO 正确、起点截断后空；
- 文件实现：段删除 / 段内截断 / 索引裁剪 / 恢复点持久化 / `log_id` LRU 清理 / 起点校验。
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.segment import LOG_SUFFIX
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2485)

TOPIC_ID = 31
TENANT_ID = 9_031
NAMESPACE = "msg"
TOPIC_NAME = "audit.truncate"
PARTITION = 0
BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)


async def _append(store: InMemoryMessageStore | FileMessageStore, *, index: int) -> object:
    """追加一条消息。

    Args:
        store: 存储实现。
        index: 序号。

    Returns:
        object: `StoredMessage`。
    """
    payload = f"payload-{index}".encode()
    return await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key=None,
        payload=payload,
        headers=ConcurrentStableDict({"i": str(index)}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=BASE_AT,
        content_type="application/json",
        compression="none",
        uncompressed_len=len(payload),
    )


def _file_store(root: Path, *, segment_max_bytes: int = 140) -> FileMessageStore:
    """构造文件实现（缺省小段阈值，保证多段）。

    Args:
        root: 数据根。
        segment_max_bytes: 段滚动字节阈值。

    Returns:
        FileMessageStore: 文件实现。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return FileMessageStore(
        directory=directory, fsync_mode="os", clock=lambda: 0.0, segment_max_bytes=segment_max_bytes
    )


def _logs(root: Path) -> list[Path]:
    """取分区下全部段文件（按名升序）。

    Args:
        root: 数据根。

    Returns:
        list[Path]: 段文件。
    """
    folder = root / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)
    return sorted(folder.glob(f"*{LOG_SUFFIX}"))


async def test_inmemory_truncate_to_moves_leo_and_visible_range() -> None:
    """内存实现：截断后 LEO / 可读区间正确，重复截断幂等。"""
    store = InMemoryMessageStore()
    for index in range(4):
        await _append(store, index=index)
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=2) == 2
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    batch = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [message.offset for message in batch] == [0, 1]
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=2) == 2
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0) == 0
    assert await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10) == []


async def test_inmemory_truncate_to_keeps_records_before_offset() -> None:
    """内存实现：offset 之前的记录内容不变。"""
    store = InMemoryMessageStore()
    for index in range(3):
        await _append(store, index=index)
    await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    batch = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [message.log_id for message in batch] == ["l-0"]


async def test_file_truncate_to_drops_later_segments_and_clears_index(tmp_path: Path) -> None:
    """文件实现：删除后续段、裁剪索引、清理越界 `log_id` 缓存。"""
    store = _file_store(tmp_path)
    for index in range(5):
        await _append(store, index=index)
    assert len(_logs(tmp_path)) >= 2
    assert await store.find_by_log_id(log_id="l-4") is not None

    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=3) == 3
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    batch = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [message.offset for message in batch] == [0, 1, 2]
    assert await store.find_by_log_id(log_id="l-4") is None
    assert await store.find_by_log_id(log_id="l-2") is not None


async def test_file_truncate_to_at_segment_base_removes_whole_segments(tmp_path: Path) -> None:
    """文件实现：offset 正好落在段首时整段删除。"""
    store = _file_store(tmp_path)
    for index in range(5):
        await _append(store, index=index)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    assert len(segments) >= 2
    second_base = segments[1].base_offset
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=second_base) == second_base
    remaining = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    assert all(segment.base_offset < second_base for segment in remaining)
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == second_base


async def test_file_truncate_to_persists_recovery_point(tmp_path: Path) -> None:
    """文件实现：恢复点随截断重写，重开进程不会读回尾部。"""
    store = _file_store(tmp_path)
    for index in range(4):
        await _append(store, index=index)
    await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=2)
    await store.aclose()

    reopened = _file_store(tmp_path)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    batch = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [message.offset for message in batch] == [0, 1]
    await reopened.aclose()


async def test_file_truncate_to_noop_at_leo(tmp_path: Path) -> None:
    """文件实现：offset >= LEO 时原样返回（no-op）。"""
    store = _file_store(tmp_path)
    await _append(store, index=0)
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1) == 1
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=9) == 1


async def test_inmemory_truncate_to_before_start_raises() -> None:
    """内存实现：offset < 起点即 `10001`（起点为 0 时负数越界）。"""
    store = InMemoryMessageStore()
    await _append(store, index=0)
    with pytest.raises(ParamError):
        await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=-1)


async def test_notifying_store_truncate_to_delegates() -> None:
    """唤醒包装的 `truncate_to` 纯委托（切换截断不触发长轮询唤醒）。"""
    inner = InMemoryMessageStore()
    for index in range(3):
        await _append(inner, index=index)
    wrapped = NotifyingMessageStore(inner, DeliveryWaiter())
    assert await wrapped.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1) == 1
    assert await inner.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1


async def test_file_truncate_to_inside_one_segment_updates_index(tmp_path: Path) -> None:
    """文件实现：单段内截断时按记录边界截字节并裁剪索引（覆盖 `_record_position`）。"""
    store = _file_store(tmp_path, segment_max_bytes=8 * 1024 * 1024)
    for index in range(5):
        await _append(store, index=index)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    assert len(segments) == 1
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=3) == 3
    batch = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [message.offset for message in batch] == [0, 1, 2]
    assert await store.find_by_log_id(log_id="l-4") is None
    assert await store.find_by_log_id(log_id="l-1") is not None


async def test_file_truncate_to_everything_creates_empty_segment(tmp_path: Path) -> None:
    """文件实现：截到起点后保留一个空段（段枚举与后续写入仍可用）。"""
    store = _file_store(tmp_path, segment_max_bytes=8 * 1024 * 1024)
    for index in range(3):
        await _append(store, index=index)
    assert await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0) == 0
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 0
    assert len(_logs(tmp_path)) == 1
    assert await _append(store, index=9) is not None


async def test_file_truncate_to_before_start_raises(tmp_path: Path) -> None:
    """文件实现：offset < 起点即 `10001`（调用方不得越过保留起点）。"""
    store = _file_store(tmp_path)
    await _append(store, index=0)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    with pytest.raises(ParamError):
        await store.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0)
