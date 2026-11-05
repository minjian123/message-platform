"""文件段消息存储专项（msg 任务 02-1；Kiwi 2427）。

覆盖（详细设计 §13.1 用例 F1~F19）：记录字节布局与 CRC、损坏判定、段轮转（大小 / 时间）、
稀疏索引与时间索引、段元数据与真实文件一致、整段删除、删光后留空段、重启后 LEO / 起点不变、
尾部半条记录截断、索引体积比、读取原语坐标、`log_id` 定位（命中 / 扫段 / 淘汰后仍可定位）、
未登记分区 fail-closed、目录布局、格式版本拒绝、段删除后 `log_id` 不误报。

段阈值在这些用例里注入小值（200 字节 / 1 秒），不靠造大数据碰运气。
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.file_store import (
    FileMessageStore,
    FileSlice,
    PartitionDirectory,
    PartitionNotBoundError,
)
from bms_msg.broker.record import (
    CorruptedRecordError,
    RecordHeader,
    UnsupportedRecordVersionError,
    decode_record,
    encode_record,
    read_frame,
)
from bms_msg.broker.segment import INDEX_SUFFIX, LOG_SUFFIX, TIME_SUFFIX

pytestmark = pytest.mark.kiwi_id(2427)

TOPIC_ID = 21
"""用例主题主键。"""

TENANT_ID = 9_002
"""用例租户主键。"""

NAMESPACE = "audit"
"""用例命名空间。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

PARTITION = 0
"""用例分区号。"""

BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间。"""


def _directory(root: Path) -> PartitionDirectory:
    """构造并绑定一个主题目录登记表。

    Args:
        root: 数据根。

    Returns:
        PartitionDirectory: 已登记用例主题的登记表。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return directory


def _store(root: Path, **kwargs: object) -> FileMessageStore:
    """构造文件实现（缺省不刷盘、时钟固定，便于确定性断言）。

    Args:
        root: 数据根。
        **kwargs: 覆盖项（段大小 / 轮转时长 / 索引上限等）。

    Returns:
        FileMessageStore: 文件实现。
    """
    options: dict[str, object] = {"fsync_mode": "os", "clock": lambda: 0.0}
    options.update(kwargs)
    return FileMessageStore(directory=_directory(root), **options)  # type: ignore[arg-type]


async def _append(
    store: FileMessageStore,
    *,
    index: int = 0,
    payload: bytes = b"payload",
    at: datetime = BASE_AT,
    partition_no: int = PARTITION,
) -> object:
    """追加一条消息。

    Args:
        store: 文件实现。
        index: 消息 / 记录标识序号。
        payload: 载荷。
        at: 发布时间。
        partition_no: 分区号。

    Returns:
        object: `StoredMessage`。
    """
    return await store.append(
        topic_id=TOPIC_ID,
        partition_no=partition_no,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key="order-1",
        payload=payload,
        headers=ConcurrentStableDict({"source": "file"}),
        trace_id="trace-1",
        published_at=at,
        content_type="application/json",
        compression="none",
        uncompressed_len=len(payload),
    )


def _only_log(root: Path) -> Path:
    """取用例分区里唯一的（或首个）`.log` 文件。

    Args:
        root: 数据根。

    Returns:
        Path: 段文件路径。
    """
    folder = root / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)
    logs = sorted(folder.glob(f"*{LOG_SUFFIX}"))
    assert logs
    return logs[0]


async def test_record_layout_and_header_fields(tmp_path: Path) -> None:
    """F1：记录布局与头字段（含 03-3 三字段与 02-4 预留字段，默认值可读）。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"hello")

    blob = _only_log(tmp_path).read_bytes()
    frame = read_frame(blob[:12])
    parsed = decode_record(blob)

    assert frame.length == len(blob) - 4
    assert parsed.total_bytes == len(blob)
    assert parsed.payload == b"hello"
    header = parsed.header
    assert (header.topic_id, header.partition_no, header.tenant_id) == (TOPIC_ID, PARTITION, TENANT_ID)
    assert (header.message_id, header.log_id, header.key) == ("m-0", "l-0", "order-1")
    assert dict(header.headers) == {"source": "file"}
    assert (header.content_type, header.compression) == ("application/json", "none")
    assert header.uncompressed_len == 5
    assert (header.epoch, header.hw, header.replicas) == (0, 0, ())


async def test_corrupted_record_raises(tmp_path: Path) -> None:
    """F2：改一个字节 → CRC 不符，`read_bytes` 抛 `CorruptedRecordError`。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"abcdef")
    path = _only_log(tmp_path)
    blob = bytearray(path.read_bytes())
    blob[-1] ^= 0xFF
    path.write_bytes(bytes(blob))

    with pytest.raises(CorruptedRecordError):
        await store.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0)


async def test_rolls_by_size(tmp_path: Path) -> None:
    """F3：段超过字节阈值即滚动；段名为 20 位零填充的 base offset。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(4):
        await _append(store, index=index, payload=b"x" * 150)

    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    bases = [segment.base_offset for segment in segments]
    assert len(segments) >= 2
    folder = tmp_path / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)
    for base in bases:
        assert (folder / f"{base:020d}{LOG_SUFFIX}").exists()


async def test_rolls_by_time(tmp_path: Path) -> None:
    """F4：按段首记录的发布时间滚动（记录时间，不是墙钟）。"""
    store = _store(tmp_path, segment_roll_ms=1_000)
    await _append(store, index=0, payload=b"x" * 10, at=BASE_AT)
    await _append(store, index=1, payload=b"x" * 10, at=BASE_AT + timedelta(seconds=2))

    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)

    assert [segment.base_offset for segment in segments] == [0, 1]


async def test_sparse_index_entries(tmp_path: Path) -> None:
    """F5：稀疏 offset 索引按 4096 字节间隔建；二分定位能读到目标 offset。"""
    store = _store(tmp_path)
    for index in range(5):
        await _append(store, index=index, payload=b"y" * 5_000)

    folder = _only_log(tmp_path).parent
    index_entries = len((folder / f"{0:020d}{INDEX_SUFFIX}").read_bytes()) // 8
    assert index_entries == 5  # 每条记录都跨过 4096 字节间隔
    located = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=3)
    assert located is not None
    blob = located.path.read_bytes()[located.position : located.position + located.length]
    assert decode_record(blob).header.message_id == "m-3"


async def test_time_index_tracks_max_published(tmp_path: Path) -> None:
    """F6：段内最大发布时间取时间索引末条（保留判定的时间输入）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"z" * 10, at=BASE_AT + timedelta(seconds=index))

    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)

    assert segments[-1].max_published_at == BASE_AT + timedelta(seconds=2)


async def test_segment_size_matches_file(tmp_path: Path) -> None:
    """F7：`SegmentMeta.size_bytes` 取真实文件大小。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"w" * 30)

    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)

    assert segments[0].size_bytes == _only_log(tmp_path).stat().st_size


async def test_delete_whole_segment_keeps_others_readable(tmp_path: Path) -> None:
    """F8：整段删除不影响其他段；起点前移到下一段 base。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"q" * 150)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    assert len(segments) == 3
    removed = segments[0].base_offset
    remained = segments[1].base_offset

    started = await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=remained)

    assert started == remained
    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.offset for item in fetched] == [remained, remained + 1]
    folder = _only_log(tmp_path).parent
    assert not (folder / f"{removed:020d}{LOG_SUFFIX}").exists()
    assert not (folder / f"{removed:020d}{INDEX_SUFFIX}").exists()
    assert not (folder / f"{removed:020d}{TIME_SUFFIX}").exists()


async def test_delete_all_keeps_empty_segment(tmp_path: Path) -> None:
    """F9：删光后留一个 base = LEO 的空段（承载 LEO，重启不重排 offset）。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"a" * 30)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo)

    assert list(await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)) == []
    assert await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == leo
    empty = _only_log(tmp_path)
    assert empty.name == f"{leo:020d}{LOG_SUFFIX}"
    assert empty.stat().st_size == 0


async def test_restart_keeps_leo_and_start(tmp_path: Path) -> None:
    """F10：重启（新实例指向同目录）后段列表、LEO、起点不变。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"b" * 40)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    start = await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION)

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == leo
    assert await reopened.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == start
    fetched = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.message_id for item in fetched] == ["m-1", "m-2"]


async def test_restart_truncates_partial_tail(tmp_path: Path) -> None:
    """F11：尾部半条记录 → 重启截断到上一条完整记录，后续追加从截断点继续。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"c" * 40)
    path = _only_log(tmp_path)
    complete = path.read_bytes()
    with path.open("ab") as handle:
        handle.write(b"\x00\x00\x01\x00")  # 声明 length=256，但后面没有字节

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert path.stat().st_size == len(complete)
    appended = await _append(reopened, index=9, payload=b"c" * 40)
    assert appended.offset == 2  # type: ignore[attr-defined]


async def test_index_is_much_smaller_than_data(tmp_path: Path) -> None:
    """F12：索引 + 时间索引总字节显著小于数据字节（给出实测比值）。"""
    store = _store(tmp_path)
    for index in range(100):
        await _append(store, index=index, payload=b"d" * 200, at=BASE_AT + timedelta(seconds=index))

    folder = _only_log(tmp_path).parent
    data_bytes = sum(path.stat().st_size for path in folder.glob(f"*{LOG_SUFFIX}"))
    index_bytes = sum(path.stat().st_size for path in folder.glob(f"*{INDEX_SUFFIX}"))
    time_bytes = sum(path.stat().st_size for path in folder.glob(f"*{TIME_SUFFIX}"))

    assert data_bytes / (index_bytes + time_bytes) >= 10


async def test_read_primitives_return_usable_coordinates(tmp_path: Path) -> None:
    """F13：`locate` / `read_range` 的坐标可直接 open + seek + read 出同一条记录。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"e" * 40)

    one = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    batch = await store.read_range(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=3)

    assert one is not None
    assert [item.position for item in batch] == sorted(item.position for item in batch)
    slices: list[FileSlice] = list(batch)
    assert len(slices) == 3
    with slices[1].path.open("rb") as handle:
        handle.seek(slices[1].position)
        parsed = decode_record(handle.read(slices[1].length))
    assert parsed.header.message_id == "m-1"


async def test_read_bytes_matches_fetch(tmp_path: Path) -> None:
    """F14：`read_bytes` 与 `fetch` 对同一 offset 返回等值记录。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"f" * 20)

    one = await store.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=1, max_messages=1)

    assert one is not None
    assert (one.message_id, one.payload, one.offset) == (fetched[0].message_id, fetched[0].payload, fetched[0].offset)


async def test_log_id_lookup_hits_cache_and_scans(tmp_path: Path) -> None:
    """F15：`log_id` 命中 LRU / 缓存为空时扫段 / 上限淘汰后仍能扫段命中。"""
    store = _store(tmp_path, log_id_limit=1)
    await _append(store, index=0, payload=b"g" * 10)
    await _append(store, index=1, payload=b"g" * 10)

    hits = await store.locate_by_log_id(log_id="l-1")
    evicted = await store.locate_by_log_id(log_id="l-0")
    missing = await store.locate_by_log_id(log_id="l-404")

    assert hits is not None
    assert evicted is not None  # 已被 LRU 淘汰，靠扫段命中
    assert missing is None


async def test_unbound_topic_fails_closed(tmp_path: Path) -> None:
    """F16：未登记且盘上无目录 → 六个端口方法与四个原语全部抛 `PartitionNotBoundError`。"""
    unbound = FileMessageStore(directory=PartitionDirectory(root=tmp_path), fsync_mode="os")
    with pytest.raises(PartitionNotBoundError):
        await _append(unbound, index=0)
    with pytest.raises(PartitionNotBoundError):
        await unbound.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=1)
    with pytest.raises(PartitionNotBoundError):
        await unbound.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    with pytest.raises(PartitionNotBoundError):
        await unbound.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION)
    with pytest.raises(PartitionNotBoundError):
        await unbound.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    with pytest.raises(PartitionNotBoundError):
        await unbound.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0)
    with pytest.raises(PartitionNotBoundError):
        await unbound.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0)
    with pytest.raises(PartitionNotBoundError):
        await unbound.read_range(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=1)
    with pytest.raises(PartitionNotBoundError):
        await unbound.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0)


async def test_directory_layout(tmp_path: Path) -> None:
    """F17：目录布局 `data/{tenant}/{namespace}/{topic}.{topic_id}/{partition}`。"""
    store = _store(tmp_path)
    await _append(store, index=0)

    expected = tmp_path / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)
    assert expected.is_dir()
    assert list(expected.glob(f"*{LOG_SUFFIX}"))


async def test_unsupported_format_version_is_rejected(tmp_path: Path) -> None:
    """F18：记录版本高于实现支持 → 加载失败（不截断、不当垃圾数据读）。"""
    folder = tmp_path / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)
    folder.mkdir(parents=True)
    header = RecordHeader(
        format_version=99,
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        tenant_id=TENANT_ID,
        message_id="m-x",
        log_id="l-x",
        key=None,
        trace_id=None,
        published_at_us=0,
        headers=(),
        content_type="application/json",
        compression="none",
        uncompressed_len=None,
    )
    (folder / f"{0:020d}{LOG_SUFFIX}").write_bytes(encode_record(header=header, payload=b"x"))

    store = _store(tmp_path)
    with pytest.raises(UnsupportedRecordVersionError):
        await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)


async def test_deleted_segment_does_not_report_log_id(tmp_path: Path) -> None:
    """F19：段被删后 `log_id` 定位返回 None（不误报坐标）。"""
    store = _store(tmp_path, segment_max_bytes=200)
    await _append(store, index=0, payload=b"h" * 150)
    await _append(store, index=1, payload=b"h" * 150)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=segments[1].base_offset)

    assert await store.locate_by_log_id(log_id="l-0") is None
    assert await store.locate_by_log_id(log_id="l-1") is not None


async def test_truncate_floors_to_segment_boundary(tmp_path: Path) -> None:
    """F20：非段边界的起点向下取整到段边界（真实段删除是整段语义，不能删半段）。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(5):
        await _append(store, index=index, payload=b"i" * 90)
    segments = await store.segments(topic_id=TOPIC_ID, partition_no=PARTITION)
    bases = [segment.base_offset for segment in segments]
    target = bases[1] + 1
    assert target < 5

    started = await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=target)

    assert started == max(base for base in bases if base <= target)
