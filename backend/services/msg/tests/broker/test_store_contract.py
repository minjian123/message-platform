"""消息存储端口契约（msg 任务 02-1；Kiwi 2427）。

同一组断言对**内存实现**与**文件实现**参数化——端口签名不变的前提是两者行为一致，
契约漂移必须在这里当场暴露，而不是等某个用例单独踩到。

覆盖（详细设计 §13.1 用例 C1~C8、C10）：字段回读 / LEO 递增 / 拉取夹取 / 跨段连续 / 分区隔离 /
段元数据 / 起点前移的幂等与越界 / 前移后读一致 / 删光后接着编号。

**非段边界的前移行为不进本套件**：契约要求调用方传段边界（`RetentionService` 就是这么做的），
违约行为（内存切片 vs 文件向下取整）不作契约；文件实现的下取整由 `test_file_store.py` 专门覆盖。
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.log import InMemoryMessageStore, MessageStore
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2427)

TOPIC_ID = 11
"""用例主题主键（存储层不校验存在性，取固定值即可）。"""

TENANT_ID = 9_001
"""用例租户主键（文件实现的目录第一层）。"""

NAMESPACE = "orders"
"""用例命名空间。"""

TOPIC_NAME = "orders.events"
"""用例主题名。"""

PARTITION = 0
"""用例分区号。"""

SECOND_PARTITION = 1
"""隔离用例的第二个分区。"""

PUBLISHED_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""固定发布时间（微秒精度回读的基准）。"""

PAYLOAD = b"payload"
"""固定载荷。"""


def _file_store(root: Path) -> FileMessageStore:
    """构造文件实现（小段阈值 + 不刷盘，便于用例造多段）。

    Args:
        root: 数据根（用例临时目录）。

    Returns:
        FileMessageStore: 已绑定主题目录的文件实现。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return FileMessageStore(directory=directory, segment_max_bytes=200, fsync_mode="os", clock=lambda: 0.0)


@pytest.fixture(params=["memory", "file"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> MessageStore:
    """按参数产出两种实现（内存：每 2 条一段；文件：按 200 字节切段）。

    Args:
        request: pytest 参数（`memory` / `file`）。
        tmp_path: 用例临时目录（文件实现的数据根）。

    Returns:
        MessageStore: 消息存储实现。
    """
    if request.param == "memory":
        return InMemoryMessageStore(segment_records=2)
    return _file_store(tmp_path)


async def _append(
    store: MessageStore,
    *,
    index: int = 0,
    payload: bytes = PAYLOAD,
    key: str | None = "order-1",
    partition_no: int = PARTITION,
    at: datetime = PUBLISHED_AT,
) -> object:
    """追加一条消息。

    Args:
        store: 消息存储。
        index: 消息 / 记录标识序号。
        payload: 载荷。
        key: 分区键。
        partition_no: 分区号。
        at: 发布时间。

    Returns:
        object: `StoredMessage`（类型由实现决定，断言按属性读取）。
    """
    return await store.append(
        topic_id=TOPIC_ID,
        partition_no=partition_no,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key=key,
        payload=payload,
        headers=ConcurrentStableDict({"source": "contract"}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=at,
        content_type="application/json",
        compression="none",
        uncompressed_len=len(payload),
    )


async def test_append_assigns_contiguous_offsets_and_keeps_fields(store: MessageStore) -> None:
    """C1：连续 offset；`message_id` / `log_id` / 键 / 载荷 / 头 / 时间 / 三字段原样回读。"""
    first = await _append(store, index=0, key="order-1", at=PUBLISHED_AT)
    second = await _append(store, index=1, key=None, at=PUBLISHED_AT + timedelta(microseconds=123))

    assert (first.offset, second.offset) == (0, 1)  # type: ignore[attr-defined]
    assert (first.message_id, first.log_id) == ("m-0", "l-0")  # type: ignore[attr-defined]
    assert (first.key, first.payload, first.trace_id) == ("order-1", PAYLOAD, "trace-1")  # type: ignore[attr-defined]
    assert dict(first.headers.items()) == {"source": "contract"}  # type: ignore[attr-defined]
    assert first.published_at == PUBLISHED_AT  # type: ignore[attr-defined]
    assert (first.content_type, first.compression, first.uncompressed_len) == ("application/json", "none", len(PAYLOAD))  # type: ignore[attr-defined]
    assert second.key is None  # type: ignore[attr-defined]
    assert second.published_at.microsecond == 123  # type: ignore[attr-defined]


async def test_leo_tracks_appends(store: MessageStore) -> None:
    """C2：LEO 随追加递增，空分区为 0。"""
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 0
    await _append(store, index=0)
    await _append(store, index=1)
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_fetch_respects_start_limit_and_negative(store: MessageStore) -> None:
    """C3：`fetch` 的起点 / 上限 / 不足 / 负数夹取。"""
    for index in range(3):
        await _append(store, index=index)

    one = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=1, max_messages=1)
    many = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=1, max_messages=10)
    negative = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=-5, max_messages=10)
    beyond = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=9, max_messages=10)

    assert [item.offset for item in one] == [1]
    assert [item.offset for item in many] == [1, 2]
    assert [item.offset for item in negative] == [0, 1, 2]
    assert list(beyond) == []


async def test_fetch_is_continuous_across_segments(store: MessageStore) -> None:
    """C4：跨段读取连续（不因段边界丢条或乱序）。"""
    for index in range(5):
        await _append(store, index=index, payload=b"x" * 90)

    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)

    assert [item.offset for item in fetched] == [0, 1, 2, 3, 4]
    assert [item.message_id for item in fetched] == [f"m-{index}" for index in range(5)]
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    assert len(segments) >= 2


async def test_partitions_are_isolated(store: MessageStore) -> None:
    """C5：不同分区 / 主题互不影响。"""
    await _append(store, index=0, partition_no=PARTITION)
    await _append(store, index=1, partition_no=SECOND_PARTITION)

    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert await store.leo(topic_id=TOPIC_ID, partition_no=SECOND_PARTITION) == 1


async def test_segments_are_empty_for_unused_partition(store: MessageStore) -> None:
    """C6：空分区的 `segments()` 为空列表，段元数据按 offset 升序。"""
    assert list(await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)) == []

    for index in range(4):
        await _append(store, index=index, payload=b"y" * 90)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)

    assert [segment.base_offset for segment in segments] == sorted(segment.base_offset for segment in segments)
    assert segments[0].base_offset == 0
    assert segments[-1].end_offset == 4


async def test_truncate_before_is_idempotent_and_rejects_beyond_leo(store: MessageStore) -> None:
    """C7：`truncate_before` 幂等、越界 `ParamError`、返回新起点。"""
    for index in range(4):
        await _append(store, index=index, payload=b"z" * 90)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    boundary = segments[1].base_offset

    assert await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=boundary) == boundary
    assert await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0) == boundary
    with pytest.raises(ParamError):
        await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=99)


async def test_fetch_and_leo_after_truncate(store: MessageStore) -> None:
    """C8：起点前移后 `fetch` 从起点返回、`leo` 不变。"""
    for index in range(4):
        await _append(store, index=index, payload=b"q" * 90)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    boundary = segments[1].base_offset
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=boundary)

    head = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)

    assert [item.offset for item in head] == list(range(boundary, 4))
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 4
    assert await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == boundary


async def test_truncate_all_keeps_leo_and_continues_numbering(store: MessageStore) -> None:
    """C10：删光后 `start == leo`，新记录接着编号（offset 不重排）。"""
    for index in range(4):
        await _append(store, index=index, payload=b"v" * 90)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    started = await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo)

    assert (started, await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION)) == (leo, leo)
    assert list(await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)) == []
    appended = await _append(store, index=9, payload=b"v" * 90)
    assert appended.offset == leo  # type: ignore[attr-defined]
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == leo + 1
