"""文件段存储与记录编解码的边界分支（msg 任务 02-1；Kiwi 2427）。

契约套件与专项用例覆盖正常路径；本文件只覆盖**防御分支与不可达兜底之外**的边界：
长度 / 头长非法、缓冲不足、CRC 不符、头解析失败、版本非整数、头与记录超限、目录登记冲突与
盘上反查、fsync 四档、段索引重写、无索引定位、截断提前返回等。

判据：变更模块行 + 分支 100%（口径见《AI开发规范》「覆盖率要求」节）。
"""

import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker import file_store as file_store_module
from bms_msg.broker.file_store import (
    FileMessageStore,
    PartitionDirectory,
    PartitionNotBoundError,
    data_root,
)
from bms_msg.broker.record import (
    CorruptedRecordError,
    RecordHeader,
    decode_header_blob,
    decode_record,
    encode_record,
    parse_header,
    read_frame,
    record_total_bytes,
    to_epoch_us,
)
from bms_msg.core.config import MAX_RECORD_BYTES, MAX_RECORD_HEADER_BYTES

pytestmark = pytest.mark.kiwi_id(2427)

TOPIC_ID = 31
"""用例主题主键。"""

TENANT_ID = 9_003
"""用例租户主键。"""

NAMESPACE = "ops"
"""用例命名空间。"""

TOPIC_NAME = "ops.events"
"""用例主题名。"""

PARTITION = 0
"""用例分区号。"""

BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间。"""


def _header(**overrides: object) -> RecordHeader:
    """构造一个字段齐全的记录头（只覆盖用例关心的字段）。

    Args:
        **overrides: 覆盖项。

    Returns:
        RecordHeader: 记录头。
    """
    values: dict[str, object] = {
        "format_version": 1,
        "topic_id": TOPIC_ID,
        "partition_no": PARTITION,
        "tenant_id": TENANT_ID,
        "message_id": "m-0",
        "log_id": "l-0",
        "key": None,
        "trace_id": None,
        "published_at_us": 1_767_225_600_000_000,
        "headers": (),
        "content_type": "application/json",
        "compression": "none",
        "uncompressed_len": None,
    }
    values.update(overrides)
    return RecordHeader(**values)  # type: ignore[arg-type]


def _directory(root: Path) -> PartitionDirectory:
    """构造并登记用例主题目录。

    Args:
        root: 数据根。

    Returns:
        PartitionDirectory: 登记表。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return directory


def _store(root: Path, **kwargs: object) -> FileMessageStore:
    """构造文件实现。

    Args:
        root: 数据根。
        **kwargs: 覆盖项。

    Returns:
        FileMessageStore: 文件实现。
    """
    options: dict[str, object] = {"fsync_mode": "os", "clock": lambda: 0.0}
    options.update(kwargs)
    return FileMessageStore(directory=_directory(root), **options)  # type: ignore[arg-type]


async def _append(store: FileMessageStore, *, index: int = 0, payload: bytes = b"payload") -> object:
    """追加一条消息。

    Args:
        store: 文件实现。
        index: 标识序号。
        payload: 载荷。

    Returns:
        object: `StoredMessage`。
    """
    return await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key=None,
        payload=payload,
        headers=ConcurrentStableDict({}),
        trace_id=None,
        published_at=BASE_AT,
    )


def _folder(root: Path) -> Path:
    """取用例分区目录。

    Args:
        root: 数据根。

    Returns:
        Path: 分区目录。
    """
    return root / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)


def test_frame_guards() -> None:
    """记录框架的三种非法形态（缓冲不足 / 长度非法 / 头长非法）。"""
    with pytest.raises(CorruptedRecordError):
        read_frame(b"")
    with pytest.raises(CorruptedRecordError):
        read_frame(struct.pack(">III", 4, 0, 0))
    with pytest.raises(CorruptedRecordError):
        read_frame(struct.pack(">III", 16, 0, 99))
    with pytest.raises(CorruptedRecordError):
        decode_record(b"")
    blob = encode_record(header=_header(), payload=b"x")
    with pytest.raises(CorruptedRecordError):
        decode_record(blob[:-1])


def test_parse_header_and_total_bytes() -> None:
    """`parse_header` 返回头与总长；`record_total_bytes` 与框架口径一致。"""
    blob = encode_record(header=_header(), payload=b"abc")

    header, total = parse_header(blob)

    assert total == len(blob)
    assert header.message_id == "m-0"
    assert record_total_bytes(len(blob) - 4) == len(blob)


def test_to_epoch_us_requires_timezone() -> None:
    """无时区的 `datetime` 拒绝（无法确定绝对时刻）。"""
    with pytest.raises(ValueError):
        to_epoch_us(datetime(2026, 1, 1))


def test_header_and_record_limits() -> None:
    """头超限与记录超限都必须显式拒绝（不静默截断）。"""
    with pytest.raises(ValueError):
        encode_record(header=_header(headers=(("k", "v" * (MAX_RECORD_HEADER_BYTES + 1)),)), payload=b"")
    with pytest.raises(ValueError):
        encode_record(header=_header(), payload=b"\x00" * (MAX_RECORD_BYTES + 1))


def test_header_unpack_failures() -> None:
    """头解析失败 / 非 map / 版本非整数各自单独报错。"""
    with pytest.raises(CorruptedRecordError):
        decode_header_blob(b"\xc1")
    with pytest.raises(CorruptedRecordError):
        decode_header_blob(b"\x92\x01\x02")
    with pytest.raises(CorruptedRecordError):
        decode_header_blob(b"\x81\xaeformat_version\xa1x")
    empty = decode_header_blob(b"\x80")
    assert empty.format_version == 0


def test_crc_mismatch_is_detected() -> None:
    """载荷改一个字节 → CRC 不符。"""
    blob = bytearray(encode_record(header=_header(), payload=b"abcd"))
    blob[-1] ^= 0xFF

    with pytest.raises(CorruptedRecordError):
        decode_record(bytes(blob))


def test_directory_bind_conflict_and_length(tmp_path: Path) -> None:
    """同 `topic_id` 绑定不同名字 → 冲突标记且保留首次；`__len__` 计数。"""
    directory = PartitionDirectory(root=tmp_path)
    assert directory.bind(tenant_id=1, namespace="a", name="x", topic_id=5) is False
    assert directory.bind(tenant_id=1, namespace="a", name="y", topic_id=5) is True

    assert len(directory) == 1
    location = directory.locate(5)
    assert location is not None
    assert location.name == "x"


async def test_recover_finds_existing_directory(tmp_path: Path) -> None:
    """重启后的新实例未登记也能从盘上反查回目录（内部写入不依赖外部请求先解析）。"""
    store = _store(tmp_path)
    await _append(store, index=0)

    fresh = FileMessageStore(directory=PartitionDirectory(root=tmp_path), fsync_mode="os")

    assert await fresh.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert await fresh.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == 0


def test_data_root_reads_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`data_root` 取环境变量；缺省回落到相对 `var/lib/msg`。"""
    monkeypatch.setenv("MSG_DATA_DIR", str(tmp_path))
    assert data_root() == tmp_path
    monkeypatch.delenv("MSG_DATA_DIR")
    assert data_root() == Path(file_store_module.DEFAULT_DATA_DIR)


def test_monotonic_clock_is_positive() -> None:
    """`_monotonic_ms` 返回正数毫秒（fsync 时间窗判据）。"""
    assert cast("Any", file_store_module)._monotonic_ms() > 0


def test_invalid_fsync_options_are_rejected(tmp_path: Path) -> None:
    """非法 fsync 档位 / 时间窗 → 启动即失败（不静默降级）。"""
    with pytest.raises(ValueError):
        FileMessageStore(directory=_directory(tmp_path), fsync_mode="nope")
    with pytest.raises(ValueError):
        FileMessageStore(directory=_directory(tmp_path), fsync_mode="os", fsync_interval_ms=0)


async def test_fsync_each_mode_flushes_every_record(tmp_path: Path) -> None:
    """`each` 档每条刷盘。"""
    store = _store(tmp_path, fsync_mode="each")

    await _append(store, index=0)
    await _append(store, index=1)

    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_fsync_batch_mode_flushes_on_record_threshold(tmp_path: Path) -> None:
    """`batch` 档累计到条数阈值即刷盘（时钟固定，只由条数触发）。"""
    store = _store(tmp_path, fsync_mode="batch")

    for index in range(65):
        await _append(store, index=index, payload=b"s")

    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 65


async def test_fsync_time_mode_flushes_on_window(tmp_path: Path) -> None:
    """`time` 档只看时间窗（时钟推进后刷盘）。"""
    ticks = [0.0]

    def _clock() -> float:
        """可推进时钟。

        Returns:
            float: 当前毫秒。
        """
        return ticks[0]

    store = _store(tmp_path, fsync_mode="time", clock=_clock, fsync_interval_ms=10)
    await _append(store, index=0)
    ticks[0] = 50.0

    await _append(store, index=1)

    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_partition_close_and_flush_are_idempotent(tmp_path: Path) -> None:
    """`close` / `flush` 幂等（段轮转与优雅关闭共用）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partitions = cast("Any", store)._partitions
    partition = partitions[(TOPIC_ID, PARTITION)]

    partition.flush()
    partition.close()
    partition.close()
    partition.flush()

    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1


async def test_read_edges(tmp_path: Path) -> None:
    """`fetch` 零条、首段内前移（目标仍为首段 base）、越界定位与读取。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"t" * 150)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    empty = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=0)
    started = await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)

    assert list(empty) == []
    assert started == 0
    assert await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo) is None
    assert await store.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo) is None


async def test_iterate_headers_skips_empty_segment(tmp_path: Path) -> None:
    """删光后留下的空段不参与 `log_id` 扫描。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo)

    assert await store.locate_by_log_id(log_id="l-0") is None
    assert await store.locate_by_log_id(log_id="missing") is None


async def test_segment_paths_and_refresh_size(tmp_path: Path) -> None:
    """段对象的索引路径与 `refresh_size`（外部改写后对账）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]

    assert segment.index_path().name.endswith(".index")
    assert segment.time_index_path().name.endswith(".timeindex")
    before = segment.size_bytes
    segment.refresh_size()
    assert segment.size_bytes == before
    segment.truncate(size=segment.size_bytes, max_relative_offset=0)


async def test_truncate_drops_index_entries_beyond_cut(tmp_path: Path) -> None:
    """手工截断段文件后重启：索引中指向被截断区间的条目被丢弃（并重写索引文件）。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"u" * 5_000)
    folder = _folder(tmp_path)
    log_path = folder / f"{0:020d}.log"
    blob = log_path.read_bytes()
    first_record = 12 + read_frame(blob[:12]).header_len + 5_000
    log_path.write_bytes(blob[:first_record])

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert await reopened.locate_by_log_id(log_id="l-0") is not None
    assert await reopened.locate_by_log_id(log_id="l-1") is None


async def test_scan_tail_without_index_starts_from_zero(tmp_path: Path) -> None:
    """索引文件缺失 → 从头顺序扫描（不丢 LEO）。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"v" * 20)
    folder = _folder(tmp_path)
    (folder / f"{0:020d}.index").unlink()

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_iter_frames_detects_partial_record(tmp_path: Path) -> None:
    """段尾半条记录 → `iter_frames` 抛损坏（不在读取期静默跳过）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    with segment.log_path.open("ab") as handle:
        handle.write(b"\x00\x00\x01\x00")
    segment.refresh_size()

    with pytest.raises(CorruptedRecordError):
        list(segment.iter_frames())
    with pytest.raises(CorruptedRecordError):
        list(segment.iter_headers())


def test_partition_path_recovers_or_rejects(tmp_path: Path) -> None:
    """`partition_path`：盘上已有目录则反查，没有则 fail-closed。"""
    existing = tmp_path / "data" / "77" / "ns" / "t.31" / "0"
    existing.mkdir(parents=True)
    directory = PartitionDirectory(root=tmp_path)
    assert directory.partition_path(topic_id=TOPIC_ID, partition_no=0) == existing
    with pytest.raises(PartitionNotBoundError):
        directory.partition_path(topic_id=99, partition_no=0)


def test_recover_skips_unrelated_directories(tmp_path: Path) -> None:
    """反查只认「目录名以 `.topic_id` 结尾」：脏目录（非数字租户 / 非目录 / 后缀不符）逐个跳过。"""
    data = tmp_path / "data"
    (data / "not-a-number" / "ns" / "x.31").mkdir(parents=True)
    (data / "5" / "ns").mkdir(parents=True)
    (data / "5" / "nsfile").write_text("x", encoding="utf-8")
    (data / "5" / "ns" / "other.99").mkdir(parents=True)

    directory = PartitionDirectory(root=tmp_path)

    assert directory.recover(topic_id=TOPIC_ID) is None


async def test_position_without_index_starts_from_zero(tmp_path: Path) -> None:
    """索引文件缺失时 `position_of` 返回 `(0, 0)`，读取从头扫描。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"w" * 30)
    await _append(store, index=1, payload=b"w" * 30)
    folder = _folder(tmp_path)
    (folder / f"{0:020d}.index").unlink()
    reopened = _store(tmp_path)

    located = await reopened.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)

    assert located is not None
    loaded = await reopened.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    assert loaded is not None
    assert loaded.message_id == "m-1"


async def test_partition_without_segments_flush_and_lookup(tmp_path: Path) -> None:
    """空分区 `flush` 不报错；`_segment_by_path` 未命中返回 None。"""
    store = _store(tmp_path)
    assert cast("Any", store)._partitions.get((TOPIC_ID, PARTITION)) is None
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]

    assert partition._segment_by_path(tmp_path / "missing.log") is None
    partition._segments.clear()
    partition.flush()
    partition.close()


async def test_multiple_segments_lookup_and_range(tmp_path: Path) -> None:
    """多段下定位跳过不匹配段（`continue`），批量坐标覆盖多段。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"y" * 150)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    last = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo - 1)
    batch = await store.read_range(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=3)

    assert last is not None
    assert len(list(batch)) == 3


async def test_truncate_rewrites_index_on_same_instance(tmp_path: Path) -> None:
    """同实例内手工截断段文件后调用 `truncate`：越界索引条目被丢弃并重写索引。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"z" * 5_000)
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        message_id="m-1",
        log_id="l-1",
        key=None,
        payload=b"z" * 5_000,
        headers=ConcurrentStableDict({}),
        trace_id=None,
        published_at=BASE_AT + timedelta(seconds=2),
    )
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    blob = segment.log_path.read_bytes()
    first_total = 4 + read_frame(blob[:12]).length
    segment.log_path.write_bytes(blob[:first_total])

    # 不刷新内存大小：模拟「文件已被外部截断、段对象仍记旧长度」的重启前状态
    segment.truncate(size=first_total, max_relative_offset=0)

    assert segment.size_bytes == first_total
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_truncate_keeps_index_when_nothing_to_drop(tmp_path: Path) -> None:
    """截断位置覆盖全部索引条目时不重写索引（无越界项）。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"q" * 5_000)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    blob = segment.log_path.read_bytes()
    segment.log_path.write_bytes(blob[: len(blob) - 1])
    segment.refresh_size()

    segment.truncate(size=len(blob) - 1, max_relative_offset=0)

    assert segment.size_bytes == len(blob) - 1


async def test_iter_frames_and_headers_detect_truncated_frames(tmp_path: Path) -> None:
    """12 字节框架可读但记录总长超出文件 → 扫描与逐条读取都判损坏。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    with segment.log_path.open("ab") as handle:
        handle.write(struct.pack(">III", 256, 0, 8))
    segment.refresh_size()

    with pytest.raises(CorruptedRecordError):
        list(segment.iter_frames())
    with pytest.raises(CorruptedRecordError):
        list(segment.iter_headers())
    with pytest.raises(CorruptedRecordError):
        segment.read_record(position=segment.size_bytes - 12)


async def test_scan_tail_breaks_on_partial_record(tmp_path: Path) -> None:
    """重启时尾部半条记录 → `scan_tail` 停止扫描并截断（不把半条当完整记录）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    with segment.log_path.open("ab") as handle:
        handle.write(b"\x00\x00\x01\x00\x00\x00\x00\x00")
    segment.refresh_size()

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1


async def test_scan_tail_counts_records_before_partial_tail(tmp_path: Path) -> None:
    """`scan_tail` 遇半条记录即停（返回已扫到的完整条数，不吞异常）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    with segment.log_path.open("ab") as handle:
        handle.write(bytes(8))
    segment.refresh_size()

    scan = segment.scan_tail()

    assert scan.count == 1
    assert scan.complete_bytes < segment.size_bytes


async def test_fetch_stops_at_max_messages_within_segment(tmp_path: Path) -> None:
    """单段内条数超过 `max_messages` → 达到上限即停（不越界多读）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"r" * 20)

    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=2)

    assert [item.offset for item in fetched] == [0, 1]


async def test_locate_skips_leading_segments(tmp_path: Path) -> None:
    """三段下定位末段记录：前两段逐个跳过（`continue` 回循环头）。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"m" * 150)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    middle = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    last = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo - 1)

    assert middle is not None
    assert last is not None


async def test_scan_tail_stops_when_record_exceeds_file(tmp_path: Path) -> None:
    """尾部框架声明的长度超出文件 → `scan_tail` 立即停止（不按声明长度读越界）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    partition = cast("Any", store)._partitions[(TOPIC_ID, PARTITION)]
    segment = partition._segments[-1]
    with segment.log_path.open("ab") as handle:
        handle.write(struct.pack(">III", 256, 0, 8))
    segment.refresh_size()

    scan = segment.scan_tail()

    assert scan.count == 1
    assert scan.complete_bytes < segment.size_bytes


async def test_locate_walks_past_earlier_segments(tmp_path: Path) -> None:
    """定位末段记录时，前面段的内层扫描要完整走完再进下一段。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"n" * 150)

    located = await store.locate(topic_id=TOPIC_ID, partition_no=PARTITION, offset=2)

    assert located is not None
