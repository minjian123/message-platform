"""崩溃恢复专项（msg 任务 02-2；Kiwi 2428）。

覆盖（详细设计 §11.1 用例 R1~R21 + 补充边界）：恢复点写入 / 回读 / 缺失 / CRC 不符 / 结构非法 /
版本过高、尾部半条与 CRC 不符的截断、坏记录之后仍有好记录时的取证与计数、索引残条修复、
索引补建、非末段 size / CRC 对账、恢复点记了已删段、位点对齐（注入 / 未注入 / 未越界）、
优雅关闭、恢复点写失败 fail-open、不扫全量、取证文件保留上限、空段承载 LEO、
恢复点锚点不可信时退回索引、两锚点皆缺时从段首扫。

用例一律注入小段阈值（200 字节）与 `fsync_mode="each"`，不靠造大数据碰运气。
"""

import json
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker import file_store as file_store_module
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.lifecycle import StorageCloser
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.recovery import (
    REASON_ABSENT,
    REASON_CRC_MISMATCH,
    REASON_MISMATCH,
    REASON_OK,
    REASON_UNPARSABLE,
    UnsupportedRecoveryPointVersionError,
    read_recovery_point,
)
from bms_msg.broker.segment import INDEX_SUFFIX, LOG_SUFFIX, TIME_SUFFIX
from bms_msg.core.config import CORRUPT_TAIL_INFIX, RECOVERY_POINT_NAME
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2428)

TOPIC_ID = 31
"""用例主题主键。"""

TENANT_ID = 9_003
"""用例租户主键。"""

NAMESPACE = "audit"
"""用例命名空间。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

PARTITION = 0
"""用例分区号。"""

BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间。"""

KEY = (TOPIC_ID, PARTITION)
"""分区键（内部字典用）。"""


class _Probe:
    """恢复读取量探针（实现 `RecoveryProbe` 协议）。"""

    def __init__(self) -> None:
        """初始化（计数从 0 起）。"""
        self.total = 0

    def on_bytes_read(self, *, amount: int) -> None:
        """累计读取字节数。

        Args:
            amount: 本次读取的字节数。
        """
        self.total += amount


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
        **kwargs: 覆盖项（fsync 档位 / 段大小 / 对齐端口 / 探针等）。

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
) -> Any:
    """追加一条消息。

    Args:
        store: 文件实现。
        index: 消息 / 记录标识序号。
        payload: 载荷。
        at: 发布时间。
        partition_no: 分区号。

    Returns:
        Any: `StoredMessage`。
    """
    return await store.append(
        topic_id=TOPIC_ID,
        partition_no=partition_no,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key="order-1",
        payload=payload,
        headers=ConcurrentStableDict({"source": "recovery"}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=at,
        content_type="application/json",
        compression="none",
        uncompressed_len=len(payload),
    )


def _folder(root: Path) -> Path:
    """取用例分区目录。

    Args:
        root: 数据根。

    Returns:
        Path: 分区目录。
    """
    return root / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)


def _point_path(root: Path) -> Path:
    """取恢复点路径。

    Args:
        root: 数据根。

    Returns:
        Path: 恢复点文件路径。
    """
    return _folder(root) / RECOVERY_POINT_NAME


def _partition_of(store: FileMessageStore) -> Any:
    """取分区日志（访问内部状态做恢复报告断言）。

    Args:
        store: 文件实现。

    Returns:
        Any: `_PartitionLog`。
    """
    return cast("Any", store)._partitions[KEY]


async def _report_of(store: FileMessageStore) -> Any:
    """取恢复报告（先触发一次懒加载）。

    Args:
        store: 文件实现。

    Returns:
        Any: `RecoveryReport`。
    """
    await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    return _partition_of(store).report


def _write_point(path: Path, payload: dict[str, Any]) -> None:
    """按恢复点的文件格式写一份手工恢复点（CRC 自洽，便于造异常场景）。

    Args:
        path: 恢复点路径。
        payload: payload 对象。
    """
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    document = {"payload": payload, "crc": zlib.crc32(body)}
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


def _payload(*, leo: int, segments: list[dict[str, int]], version: int = 1) -> dict[str, Any]:
    """构造一份恢复点 payload。

    Args:
        leo: LEO。
        segments: 段快照列表。
        version: 格式版本。

    Returns:
        dict[str, Any]: payload。
    """
    return {
        "format_version": version,
        "topic_id": TOPIC_ID,
        "partition_no": PARTITION,
        "leo": leo,
        "epoch": 0,
        "updated_at_us": 0,
        "segments": segments,
    }


def _frame_total(blob: bytes, position: int) -> int:
    """读某位置记录的框架并算总长。

    Args:
        blob: 段文件字节。
        position: 记录起点。

    Returns:
        int: 记录总长（含 4 字节 length）。
    """
    length = int.from_bytes(blob[position : position + 4], "big")
    return 4 + length


def _header_len(blob: bytes, position: int) -> int:
    """读某位置记录的头长。

    Args:
        blob: 段文件字节。
        position: 记录起点。

    Returns:
        int: 头字节数。
    """
    return int.from_bytes(blob[position + 8 : position + 12], "big")


def _corrupt_payload(path: Path, *, position: int, blob: bytes) -> None:
    """改坏一条记录的载荷区（长度不变，只让 CRC 不符）。

    Args:
        path: 段文件路径。
        position: 记录起点。
        blob: 改前的段文件字节。
    """
    data = bytearray(blob)
    target = position + 12 + _header_len(blob, position)
    assert target < position + _frame_total(blob, position)
    data[target] ^= 0xFF
    path.write_bytes(bytes(data))


# ---------------------------------------------------------------------------
# 恢复点：写入 / 读取 / 三类异常
# ---------------------------------------------------------------------------


async def test_recovery_point_written_and_readable(tmp_path: Path) -> None:
    """R1：each 档每条刷盘后写出恢复点，字段齐全、CRC 自洽、无 `.tmp` 残留。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"a" * 40)

    path = _point_path(tmp_path)
    readout = read_recovery_point(path, topic_id=TOPIC_ID, partition_no=PARTITION)

    assert path.exists()
    assert not (path.parent / f"{path.name}.tmp").exists()
    assert readout.reason == REASON_OK
    assert readout.point is not None
    assert readout.point.leo == 1
    assert len(readout.point.segments) == 1
    assert readout.point.segments[0].last_offset == 0
    assert readout.point.segments[0].size == path.parent.joinpath(f"{0:020d}{LOG_SUFFIX}").stat().st_size


async def test_missing_recovery_point_falls_back(tmp_path: Path) -> None:
    """R2：无恢复点（02-1 目录）→ 退回推导，不报错。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"b" * 20)

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    assert (await _report_of(reopened)).recovery_point == REASON_ABSENT


async def test_recovery_point_crc_mismatch_falls_back(tmp_path: Path) -> None:
    """R3：恢复点 payload 被改（CRC 不符）→ 退回推导。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"c" * 10)
    path = _point_path(tmp_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["payload"]["leo"] = 99
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert (await _report_of(reopened)).recovery_point == REASON_CRC_MISMATCH


async def test_recovery_point_unparsable_falls_back(tmp_path: Path) -> None:
    """R4：恢复点不是合法 JSON → 退回推导（不阻塞启动）。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"d" * 10)
    _point_path(tmp_path).write_text("{not json", encoding="utf-8")

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert (await _report_of(reopened)).recovery_point == REASON_UNPARSABLE


async def test_recovery_point_wrong_partition_is_unusable(tmp_path: Path) -> None:
    """R4b：恢复点的 `topic_id` 与所在分区不符 → 不可用（文件被挪错目录）。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"d" * 10)
    payload = _payload(leo=1, segments=[{"base": 0, "size": 1, "last_offset": 0, "last_position": 0, "last_crc": 0}])
    payload["topic_id"] = TOPIC_ID + 1
    _write_point(_point_path(tmp_path), payload)

    reopened = _store(tmp_path)

    assert (await _report_of(reopened)).recovery_point == REASON_MISMATCH


async def test_unsupported_recovery_point_version_is_rejected(tmp_path: Path) -> None:
    """R5：恢复点版本高于实现 → 拒绝加载（不截断、不退回推导）。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"e" * 10)
    _write_point(_point_path(tmp_path), _payload(leo=1, segments=[], version=99))

    with pytest.raises(UnsupportedRecoveryPointVersionError):
        await _store(tmp_path).leo(topic_id=TOPIC_ID, partition_no=PARTITION)


# ---------------------------------------------------------------------------
# 尾部扫描与截断
# ---------------------------------------------------------------------------


async def test_partial_tail_is_truncated_and_isolated(tmp_path: Path) -> None:
    """R6：尾部半条记录 → 截断到上一条完整记录 + 生成取证文件。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"f" * 40)
    log_path = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    complete = log_path.read_bytes()
    with log_path.open("ab") as handle:
        handle.write(b"\x00\x00\x01\x00")  # 声明 length=256，后面没有字节

    reopened = _store(tmp_path)

    report = await _report_of(reopened)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert log_path.stat().st_size == len(complete)
    assert report.truncated_bytes == len(complete)
    assert report.isolated is not None
    assert Path(report.isolated).stat().st_size == 4
    assert CORRUPT_TAIL_INFIX in Path(report.isolated).name


async def test_crc_mismatch_tail_is_truncated(tmp_path: Path) -> None:
    """R7：尾部记录长度完整但 CRC 不符 → 照样截断（02-1 只判 length 的盲区）。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"g" * 40)
    log_path = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    blob = log_path.read_bytes()
    second = _frame_total(blob, 0)
    _corrupt_payload(log_path, position=second, blob=blob)

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert (await _report_of(reopened)).truncated_bytes == second


async def test_good_records_after_bad_one_are_counted_not_rescued(tmp_path: Path) -> None:
    """R8：坏记录之后仍有好记录 → 只截断到坏记录前，尾随记录数进报告（不静默救）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"h" * 40)
    log_path = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    blob = log_path.read_bytes()
    first = _frame_total(blob, 0)
    second = _frame_total(blob, first)
    _corrupt_payload(log_path, position=first, blob=blob)

    reopened = _store(tmp_path)

    report = await _report_of(reopened)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert report.truncated_bytes == first
    assert report.trailing_records == 1  # 第三条仍可读出框架，但只计数不恢复
    assert log_path.stat().st_size == first
    assert second == first  # 三条记录等长，坏记录起点即第二条起点


# ---------------------------------------------------------------------------
# 索引：残条修复与补建
# ---------------------------------------------------------------------------


async def test_index_tail_residue_is_repaired(tmp_path: Path) -> None:
    """R9：索引尾部半条条目 → 加载时截到整数倍，此后追加不错位。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"i" * 5_000)
    index_path = _folder(tmp_path) / f"{0:020d}{INDEX_SUFFIX}"
    aligned = index_path.stat().st_size
    assert aligned % 8 == 0
    with index_path.open("ab") as handle:
        handle.write(b"\x00\x00\x00\x00")  # 写一半的条目

    reopened = _store(tmp_path)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    assert index_path.stat().st_size == aligned
    assert (await _report_of(reopened)).index_repair_bytes == 4

    await _append(reopened, index=3, payload=b"i" * 5_000)
    fetched = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=3, max_messages=1)
    assert fetched[0].message_id == "m-3"


async def test_missing_indexes_are_rebuilt_from_tail_scan(tmp_path: Path) -> None:
    """R10：`.index` / `.timeindex` 缺失 → 尾部扫描补建（条数进报告）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"j" * 5_000, at=BASE_AT + timedelta(seconds=index * 2))
    folder = _folder(tmp_path)
    (folder / f"{0:020d}{INDEX_SUFFIX}").unlink()
    (folder / f"{0:020d}{TIME_SUFFIX}").unlink()

    reopened = _store(tmp_path)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3

    assert (await _report_of(reopened)).index_rebuilt >= 3
    assert (folder / f"{0:020d}{INDEX_SUFFIX}").stat().st_size >= 8
    assert (folder / f"{0:020d}{TIME_SUFFIX}").stat().st_size >= 12


# ---------------------------------------------------------------------------
# 非末段对账
# ---------------------------------------------------------------------------


async def test_non_tail_segment_size_mismatch_is_marked(tmp_path: Path) -> None:
    """R11：非末段文件变长 → 标记 `size_mismatch`，其他段仍可读、启动不受阻。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"k" * 150)
    first_log = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    with first_log.open("ab") as handle:
        handle.write(b"\x00" * 10)

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == (0,)
    fetched = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=1, max_messages=10)
    assert [item.message_id for item in fetched] == ["m-1", "m-2"]


async def test_non_tail_segment_crc_mismatch_is_marked(tmp_path: Path) -> None:
    """R12：非末段段尾记录被改写（size 不变）→ 标记 `crc_mismatch`。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"l" * 150)
    first_log = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    blob = first_log.read_bytes()
    _corrupt_payload(first_log, position=0, blob=blob)

    reopened = _store(tmp_path)
    await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    partition = _partition_of(reopened)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == (0,)
    assert partition._corrupted[0] == "crc_mismatch"


async def test_intact_non_tail_segments_are_not_marked(tmp_path: Path) -> None:
    """R13：非末段全部一致 → 无标记。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(3):
        await _append(store, index=index, payload=b"m" * 150)

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == ()
    assert (await _report_of(reopened)).corrupted_segments == ()


async def test_empty_stamp_matches_empty_segment(tmp_path: Path) -> None:
    """R13b：恢复点里某非末段 size=0 且文件确为 0 字节 → 通过（不误报）。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"n" * 150)
    folder = _folder(tmp_path)
    (folder / f"{0:020d}{LOG_SUFFIX}").write_bytes(b"")
    _write_point(
        folder / RECOVERY_POINT_NAME,
        _payload(
            leo=2,
            segments=[
                {"base": 0, "size": 0, "last_offset": -1, "last_position": 0, "last_crc": 0},
                {"base": 1, "size": 0, "last_offset": 0, "last_position": 0, "last_crc": 0},
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == ()


async def test_deleted_segment_in_point_is_ignored(tmp_path: Path) -> None:
    """R14：恢复点记了已删段（保留期清理后恢复点落后）→ 跳过，不报损坏。"""
    store = _store(tmp_path, fsync_mode="each")
    for index in range(2):
        await _append(store, index=index, payload=b"o" * 20)
    _write_point(
        _point_path(tmp_path),
        _payload(
            leo=2,
            segments=[
                {"base": 999, "size": 10, "last_offset": 998, "last_position": 0, "last_crc": 1},
                {"base": 0, "size": 1, "last_offset": 0, "last_position": 0, "last_crc": 1},
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == ()


# ---------------------------------------------------------------------------
# 位点对齐
# ---------------------------------------------------------------------------


async def _truncated_store(root: Path, **kwargs: object) -> FileMessageStore:
    """造一个「尾部有半条记录」的分区（恢复时必然截断 → 触发位点对齐）。

    Args:
        root: 数据根。
        **kwargs: 传给构造函数的覆盖项。

    Returns:
        FileMessageStore: 已加载（已截断）的实例。
    """
    store = _store(root)
    for index in range(2):
        await _append(store, index=index, payload=b"p" * 20)
    log_path = _folder(root) / f"{0:020d}{LOG_SUFFIX}"
    with log_path.open("ab") as handle:
        handle.write(b"\x00\x00\x01\x00")
    return _store(root, **kwargs)


async def test_offset_alignment_clamps_to_new_leo(tmp_path: Path) -> None:
    """R15：已提交位点超过截断后的 LEO → 夹到新 LEO（不归零）。"""
    offsets = InMemoryOffsetStore()
    await offsets.commit(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION, offset=5)
    await offsets.commit(group="g-2", topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)

    reopened = await _truncated_store(tmp_path, realigner=offsets)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert (await offsets.get(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION)) == 2
    assert (await offsets.get(group="g-2", topic_id=TOPIC_ID, partition_no=PARTITION)) == 1


async def test_offset_alignment_without_realigner_is_skipped(tmp_path: Path) -> None:
    """R16：未注入对齐端口 → 跳过且不阻断访问（位点保持原值）。"""
    offsets = InMemoryOffsetStore()
    await offsets.commit(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION, offset=5)

    reopened = await _truncated_store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert (await offsets.get(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION)) == 5


async def test_offset_alignment_within_leo_is_untouched(tmp_path: Path) -> None:
    """R17：位点未越界 → 不发生夹取。"""
    offsets = InMemoryOffsetStore()
    await offsets.commit(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)

    reopened = await _truncated_store(tmp_path, realigner=offsets)
    await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    assert (await offsets.get(group="g-1", topic_id=TOPIC_ID, partition_no=PARTITION)) == 1
    assert cast("Any", reopened)._pending_alignment == {}


# ---------------------------------------------------------------------------
# 优雅关闭 / fail-open / 不扫全量 / 取证保留
# ---------------------------------------------------------------------------


async def test_graceful_close_flushes_and_writes_point(tmp_path: Path) -> None:
    """R18：`aclose` 后恢复点存在且 LEO 一致；重开 LEO / 起点不变。"""
    store = _store(tmp_path)
    for index in range(2):
        await _append(store, index=index, payload=b"q" * 20)

    await store.aclose()

    path = _point_path(tmp_path)
    readout = read_recovery_point(path, topic_id=TOPIC_ID, partition_no=PARTITION)
    assert readout.reason == REASON_OK
    assert readout.point is not None
    assert readout.point.leo == 2
    reopened = _store(tmp_path)
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert await reopened.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == 0


async def test_recovery_point_write_failure_is_fail_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """R19：恢复点写失败 → 只告警，写入路径不失败。"""

    def _boom(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("disk full")

    monkeypatch.setattr("bms_msg.broker.file_store.write_recovery_point", _boom)
    store = _store(tmp_path, fsync_mode="each")

    message = await _append(store, index=0, payload=b"r" * 10)

    assert message.offset == 0
    assert not _point_path(tmp_path).exists()


async def test_recovery_does_not_scan_whole_segment(tmp_path: Path) -> None:
    """R20：恢复点可用时扫描起点在文件尾 → 恢复不读段数据（O(尾部) 的机器判据）。"""
    store = _store(tmp_path, fsync_mode="each")
    for index in range(3):
        await _append(store, index=index, payload=b"s" * 5_000)
    probe = _Probe()

    reopened = _store(tmp_path, probe=probe)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    assert probe.total <= 64


async def test_corrupt_tails_are_pruned_to_limit(tmp_path: Path) -> None:
    """R21：取证副本超过保留份数 → 删最早（按名字里的时间戳排序）。"""
    store = _store(tmp_path)
    await _append(store, index=0, payload=b"t" * 10)
    segment = _partition_of(store)._segments[-1]
    folder = _folder(tmp_path)
    for index in range(10):
        (folder / f"{0:020d}{CORRUPT_TAIL_INFIX}20260101T0000{index:02d}").write_bytes(b"x")

    removed = segment.prune_corrupt_tails(limit=8)

    remaining = sorted(path.name for path in folder.glob(f"*{CORRUPT_TAIL_INFIX}*"))
    assert removed == 2
    assert len(remaining) == 8
    assert f"{0:020d}{CORRUPT_TAIL_INFIX}20260101T000000" not in remaining


# ---------------------------------------------------------------------------
# 锚点与边界
# ---------------------------------------------------------------------------


async def test_empty_segment_carries_leo_after_recovery(tmp_path: Path) -> None:
    """R22：删光段后的空段承载 LEO；带恢复点重启后 LEO / 起点不变。"""
    store = _store(tmp_path, fsync_mode="each")
    for index in range(2):
        await _append(store, index=index, payload=b"u" * 20)
    leo = await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=leo)
    await store.aclose()

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == leo
    assert await reopened.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION) == leo


async def test_untrusted_point_anchor_falls_back_to_index(tmp_path: Path) -> None:
    """R23：恢复点声明的 size 大于文件（不可信）→ 退回索引锚点，LEO 仍正确。"""
    store = _store(tmp_path, fsync_mode="each")
    for index in range(2):
        await _append(store, index=index, payload=b"v" * 20)
    log_path = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    real_size = log_path.stat().st_size
    _write_point(
        _point_path(tmp_path),
        _payload(
            leo=2,
            segments=[
                {
                    "base": 0,
                    "size": real_size + 1_000,
                    "last_offset": 1,
                    "last_position": 0,
                    "last_crc": 0,
                }
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2


async def test_no_anchor_scans_from_segment_start(tmp_path: Path) -> None:
    """R24：恢复点与索引皆缺 → 从段首扫描，LEO 仍正确。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"w" * 20)
    folder = _folder(tmp_path)
    (folder / f"{0:020d}{INDEX_SUFFIX}").unlink()

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    assert (await _report_of(reopened)).recovery_point == REASON_ABSENT


async def test_stamp_position_out_of_range_is_marked(tmp_path: Path) -> None:
    """R25：恢复点记的末条位置越界（段被截短且 size 恰好对上）→ 标记 `crc_mismatch`。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"x" * 150)
    folder = _folder(tmp_path)
    first_log = folder / f"{0:020d}{LOG_SUFFIX}"
    size = first_log.stat().st_size
    _write_point(
        folder / RECOVERY_POINT_NAME,
        _payload(
            leo=2,
            segments=[
                {"base": 0, "size": size, "last_offset": 0, "last_position": size + 100, "last_crc": 1},
                {"base": 1, "size": 0, "last_offset": 0, "last_position": 0, "last_crc": 0},
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == (0,)


# ---------------------------------------------------------------------------
# 补充边界（覆盖率收口：结构校验 / 扫描分支 / 关闭钩子 / 持久化防御）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        "[1,2,3]",
        '{"payload": {"format_version": 1}, "crc": "x"}',
        '{"payload": 1, "crc": 1}',
    ],
)
async def test_recovery_point_raw_structure_is_unusable(tmp_path: Path, raw: str) -> None:
    """R26：顶层非对象 / payload 非对象 / crc 非整数 → 一律不可用（退回推导）。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"y" * 10)
    _point_path(tmp_path).write_text(raw, encoding="utf-8")

    readout = read_recovery_point(_point_path(tmp_path), topic_id=TOPIC_ID, partition_no=PARTITION)

    assert readout.point is None
    assert readout.reason == REASON_UNPARSABLE


@pytest.mark.parametrize(
    "mutate",
    [
        {"format_version": "1"},
        {"leo": "x"},
        {"segments": {"base": 0}},
        {"segments": [1]},
        {"segments": [{"base": "x", "size": 0, "last_offset": -1, "last_position": 0, "last_crc": 0}]},
    ],
)
async def test_recovery_point_payload_fields_are_validated(tmp_path: Path, mutate: dict[str, Any]) -> None:
    """R27：payload 字段类型不符 → 不可用（不按缺省值猜着读）。"""
    store = _store(tmp_path, fsync_mode="each")
    await _append(store, index=0, payload=b"z" * 10)
    payload = _payload(leo=1, segments=[])
    payload.update(mutate)
    _write_point(_point_path(tmp_path), payload)

    readout = read_recovery_point(_point_path(tmp_path), topic_id=TOPIC_ID, partition_no=PARTITION)

    assert readout.point is None
    assert readout.reason == REASON_UNPARSABLE


async def test_second_segment_stamp_is_scanned_after_reload(tmp_path: Path) -> None:
    """R28：重开后段对象无写入缓存 → 末段 stamp 走扫描，恢复点仍完整。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"a" * 150)
    reopened = _store(tmp_path, fsync_mode="each")
    await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION)

    await reopened.aclose()

    readout = read_recovery_point(_point_path(tmp_path), topic_id=TOPIC_ID, partition_no=PARTITION)
    assert readout.point is not None
    assert len(readout.point.segments) == 2
    assert readout.point.segments[-1].last_offset == 1


async def test_fetch_without_index_scans_from_segment_start(tmp_path: Path) -> None:
    """R29：非末段索引缺失（不会被补建）→ 该段定位退化为段首扫描，读路径仍正确。"""
    store = _store(tmp_path, segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"b" * 150)
    (_folder(tmp_path) / f"{0:020d}{INDEX_SUFFIX}").unlink()

    reopened = _store(tmp_path)
    fetched = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=2)

    assert [item.offset for item in fetched] == [0, 1]


def _corrupt_second_and_replace_third(root: Path, *, tail: bytes) -> None:
    """把第三条记录替换成给定字节，并把第二条记录的载荷改坏。

    Args:
        root: 数据根。
        tail: 第三条记录位置上的替换字节。
    """
    log_path = _folder(root) / f"{0:020d}{LOG_SUFFIX}"
    blob = log_path.read_bytes()
    first = _frame_total(blob, 0)
    second = _frame_total(blob, first)
    data = bytearray(blob[: first + second])
    target = first + 12 + _header_len(blob, first)
    data[target] ^= 0xFF
    log_path.write_bytes(bytes(data) + tail)


async def test_trailing_scan_stops_on_illegal_frame(tmp_path: Path) -> None:
    """R30：坏记录之后是非法框架（length=0）→ 尾随计数立即停止（计 0）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"c" * 40)
    _corrupt_second_and_replace_third(tmp_path, tail=b"\x00" * 12)

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert (await _report_of(reopened)).trailing_records == 0


async def test_trailing_scan_stops_on_overlong_frame(tmp_path: Path) -> None:
    """R31：坏记录之后框架声明超出文件 → 尾随计数停止（不按声明长度读越界）。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"d" * 40)
    _corrupt_second_and_replace_third(tmp_path, tail=(1000).to_bytes(4, "big") + b"\x00" * 8)

    reopened = _store(tmp_path)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert (await _report_of(reopened)).trailing_records == 0


async def test_probe_counts_reads_without_recovery_point(tmp_path: Path) -> None:
    """R32：无恢复点时从索引锚点扫尾部，探针统计到真实读取量。"""
    store = _store(tmp_path)
    for index in range(3):
        await _append(store, index=index, payload=b"e" * 20)
    probe = _Probe()

    reopened = _store(tmp_path, probe=probe)

    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3
    assert probe.total > 0


async def test_stamp_total_beyond_file_is_marked(tmp_path: Path) -> None:
    """R33：非末段被截短且恢复点 size 恰好对上 → 段尾记录读不全，标记损坏。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"f" * 150)
    first_log = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    blob = first_log.read_bytes()
    truncated = len(blob) - 10
    first_log.write_bytes(blob[:truncated])
    _write_point(
        _point_path(tmp_path),
        _payload(
            leo=2,
            segments=[
                {"base": 0, "size": truncated, "last_offset": 0, "last_position": 0, "last_crc": 1},
                {"base": 1, "size": 0, "last_offset": 0, "last_position": 0, "last_crc": 0},
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == (0,)


def _orphan_partition(root: Path) -> Any:
    """构造一个指向不存在目录的分区日志（持久化防御分支用）。

    Args:
        root: 数据根。

    Returns:
        Any: `_PartitionLog`。
    """
    partition_type = cast("Any", file_store_module)._PartitionLog
    return partition_type(
        directory=root / "missing",
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        fsync_mode="os",
        fsync_interval_ms=200,
        fsync_batch_records=64,
        segment_max_bytes=200,
        segment_roll_ms=86_400_000,
        index_interval=4_096,
        time_index_interval_ms=1_000,
        clock=lambda: 0.0,
    )


async def test_persist_point_skips_missing_directory(tmp_path: Path) -> None:
    """R34：分区目录不存在时写恢复点直接跳过（不建目录、不抛错）。"""
    partition = _orphan_partition(tmp_path)

    partition.persist_recovery_point()

    assert not (tmp_path / "missing" / RECOVERY_POINT_NAME).exists()


class _FakeStore:
    """关闭钩子的替身（只记调用次数）。"""

    def __init__(self) -> None:
        """初始化。"""
        self.closed = 0

    async def aclose(self) -> None:
        """记录一次关闭。"""
        self.closed += 1


async def test_storage_closer_delegates_to_store() -> None:
    """R35：关闭钩子把 `aclose` 转给存储（幂等，可重复调用）。"""
    fake = _FakeStore()
    closer = StorageCloser(store=fake)

    await closer.aclose()
    await closer.aclose()

    assert fake.closed == 2


async def test_stamp_frame_unreadable_is_marked(tmp_path: Path) -> None:
    """R36：恢复点记的末条位置读到非法框架 → 判损坏（不抛给启动流程）。"""
    store = _store(tmp_path, fsync_mode="each", segment_max_bytes=200)
    for index in range(2):
        await _append(store, index=index, payload=b"g" * 150)
    first_log = _folder(tmp_path) / f"{0:020d}{LOG_SUFFIX}"
    with first_log.open("ab") as handle:
        handle.write(b"\x00" * 12)  # length=0：非法框架
    size = first_log.stat().st_size
    _write_point(
        _point_path(tmp_path),
        _payload(
            leo=2,
            segments=[
                {"base": 0, "size": size, "last_offset": 0, "last_position": size - 12, "last_crc": 1},
                {"base": 1, "size": 0, "last_offset": 0, "last_position": 0, "last_crc": 0},
            ],
        ),
    )

    reopened = _store(tmp_path)

    assert await reopened.corrupted_segments(topic_id=TOPIC_ID, partition_no=PARTITION) == (0,)
