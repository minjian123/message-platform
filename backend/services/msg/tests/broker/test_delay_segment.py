"""`.delay` 段（msg 任务 02-7；Kiwi 2432）。

覆盖（详细设计 §4、§12 用例 D1~D5）：

- **D1 命名互不误读**：`.log` 枚举不把 `.delay` / `.delay.corrupt-*` 当数据段；反向亦然；
- **D2 记录头延迟子结构**：`put` / `clear` 两形态逐字段往返；`.log` 记录不写 `delay` 键（体积与 02-1 一致）；
- **D3 兼容性**：未知键被忽略；版本高于实现即拒绝（不做「猜着读」）；
- **D4 追加与扫描**：顺序扫出全部记录、位置与条数正确；CRC / 长度异常停在 `complete_bytes`；
- **D5 截断与隔离**：末段取证截断、非末段整段改名、副本保留份数上限。
"""

from collections.abc import Callable
from pathlib import Path
from typing import cast

import msgpack
import pytest

from bms_msg.broker.delay_segment import (
    DELAY_CORRUPT_INFIX,
    DelaySegment,
    list_delay_segment_seqs,
)
from bms_msg.broker.record import (
    DELAY_KIND_CLEAR,
    DELAY_KIND_PUT,
    RECORD_FORMAT_VERSION,
    DelayStamp,
    RecordHeader,
    UnsupportedRecordVersionError,
    decode_header_blob,
    decode_record,
    encode_record,
    read_frame,
)
from bms_msg.broker.segment import list_segment_bases
from bms_msg.core.config import CORRUPT_TAIL_KEEP, RECORD_FRAME_BYTES
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2432)

_PACK = cast("Callable[..., bytes]", msgpack.packb)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
"""编码入口（msgpack 无类型存根，按 `broker/record.py` 同法收窄）。"""

_UNPACK = cast("Callable[..., object]", msgpack.unpackb)  # pyright: ignore[reportUnknownMemberType]
"""解码入口（同上）。"""

TOPIC_ID = 11
"""用例主题主键（段层不校验归属，取固定值即可）。"""

TIMESTAMP = "20260101T000000000000"
"""取证文件名里的时间戳（由调用方给，用例可确定性断言）。"""


def _header(*, log_id: str = "log-1", delay: DelayStamp | None = None) -> RecordHeader:
    """构造一条记录头（缺省为普通 `.log` 记录）。

    Args:
        log_id: 记录标识。
        delay: 延迟元数据（None 表示普通记录，不写 `delay` 键）。

    Returns:
        RecordHeader: 记录头。
    """
    return RecordHeader(
        format_version=RECORD_FORMAT_VERSION,
        topic_id=TOPIC_ID,
        partition_no=0,
        tenant_id=9001,
        message_id="msg-1",
        log_id=log_id,
        key="order-1",
        trace=TraceContext(trace_id="trace-1"),
        published_at_us=1_767_225_600_000_000,
        headers=(("source", "test"),),
        content_type="application/json",
        compression="none",
        uncompressed_len=3,
        delay=delay,
    )


def _put(*, deliver_at_us: int = 1_767_225_605_000_000, attempts: int = 0) -> DelayStamp:
    """构造 `put` 形态的延迟元数据。

    Args:
        deliver_at_us: 到期时刻（epoch 微秒）。
        attempts: 转投失败次数。

    Returns:
        DelayStamp: 延迟元数据。
    """
    return DelayStamp(
        kind=DELAY_KIND_PUT,
        deliver_at_us=deliver_at_us,
        delay_level="5s",
        attempts=attempts,
        namespace="msg",
        topic="audit.operation",
    )


def test_delay_segments_are_not_mistaken_for_log_segments(tmp_path: Path) -> None:
    """D1：`.log` 枚举与 `.delay` 枚举互不误读（含隔离副本）。"""
    for name in (
        "00000000000000000000.log",
        "00000000000000000000.delay",
        "00000000000000000000.delay.corrupt-20260101T000000",
        "recovery-point.json",
    ):
        (tmp_path / name).write_bytes(b"")

    assert list_segment_bases(tmp_path) == [0]
    assert list_delay_segment_seqs(tmp_path) == [0]
    assert list_delay_segment_seqs(tmp_path / "missing") == []


def test_delay_stamp_roundtrip_and_plain_record_has_no_key() -> None:
    """D2：`put` / `clear` 逐字段往返；普通记录不写 `delay` 键（零体积开销）。"""
    plain = encode_record(header=_header(), payload=b"abc")
    frame = read_frame(plain)
    raw = _UNPACK(plain[RECORD_FRAME_BYTES : RECORD_FRAME_BYTES + frame.header_len], raw=False)
    assert isinstance(raw, dict)
    assert "delay" not in raw
    assert decode_record(plain).header.delay is None

    stamped = encode_record(header=_header(delay=_put(attempts=2)), payload=b"abc")
    stamp = decode_record(stamped).header.delay
    assert stamp == _put(attempts=2)
    assert len(stamped) > len(plain)

    cleared = encode_record(header=_header(delay=DelayStamp(kind=DELAY_KIND_CLEAR)), payload=b"")
    tombstone = decode_record(cleared).header.delay
    assert tombstone is not None
    assert (tombstone.kind, tombstone.deliver_at_us, tombstone.attempts) == (DELAY_KIND_CLEAR, 0, 0)


def test_unknown_keys_ignored_and_future_version_rejected() -> None:
    """D3：新增键被忽略（只增不删）；版本高于实现即拒绝（不猜着读）。"""
    header = decode_header_blob(
        _PACK({"format_version": RECORD_FORMAT_VERSION, "log_id": "l-1", "future": 1}, use_bin_type=True)
    )
    assert header.log_id == "l-1"
    assert header.delay is None

    with pytest.raises(UnsupportedRecordVersionError):
        decode_header_blob(_PACK({"format_version": RECORD_FORMAT_VERSION + 1}, use_bin_type=True))


def test_scan_reads_in_order_and_stops_at_bad_record(tmp_path: Path) -> None:
    """D4：顺序扫出全部记录；长度越界与 CRC 不符都停在坏记录起点。"""
    segment = DelaySegment(partition_dir=tmp_path, seq=0)
    assert (segment.size_bytes, segment.record_count) == (0, 0)
    first_blob = encode_record(header=_header(log_id="a", delay=_put()), payload=b"one")
    second_blob = encode_record(header=_header(log_id="b", delay=_put()), payload=b"two")
    first = segment.append(first_blob)
    second = segment.append(second_blob)
    assert (first, second) == (0, len(first_blob))

    scan = segment.scan()
    assert [entry.header.log_id for entry in scan.entries] == ["a", "b"]
    assert [entry.position for entry in scan.entries] == [first, second]
    assert scan.complete_bytes == segment.size_bytes
    assert scan.corrupted_position is None
    assert segment.record_count == 2

    # 半条记录（声明长度越过文件尾）
    segment.append(b"\x00\x00\x00\xff\x00\x00\x00\x00\x00\x00\x00\x02")
    broken = segment.scan(verify_crc=True)
    assert broken.corrupted_position == scan.complete_bytes
    assert [entry.header.log_id for entry in broken.entries] == ["a", "b"]

    # CRC 不符（框架合法、校验和错）——换一个干净段验证，避免与上一条坏记录混在一起
    other = DelaySegment(partition_dir=tmp_path, seq=1)
    blob = bytearray(encode_record(header=_header(log_id="c", delay=_put()), payload=b"three"))
    blob[4:8] = b"\x00\x00\x00\x00"
    other.append(bytes(blob))
    crc_bad = other.scan(verify_crc=True)
    assert crc_bad.entries == ()
    assert crc_bad.corrupted_position == 0
    # 不校验 CRC 时只按结构读，能读出这条记录（结构级判定与内容级判定的区别）
    assert [entry.header.log_id for entry in other.scan(verify_crc=False).entries] == ["c"]


def test_tail_truncated_and_middle_segment_isolated(tmp_path: Path) -> None:
    """D5：末段取证截断、非末段整段隔离、隔离副本按份数上限清扫。"""
    good = encode_record(header=_header(log_id="a", delay=_put()), payload=b"one")
    segment = DelaySegment(partition_dir=tmp_path, seq=0)
    segment.append(good)
    segment.append(b"\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x02")

    scan = segment.scan()
    assert scan.corrupted_position == len(good)
    copy = segment.isolate_tail(keep_bytes=scan.complete_bytes, timestamp=TIMESTAMP)
    segment.truncate(size=scan.complete_bytes)

    assert copy.name == f"00000000000000000000{DELAY_CORRUPT_INFIX}{TIMESTAMP}"
    assert segment.size_bytes == len(good)
    assert segment.scan().corrupted_position is None

    isolated = DelaySegment(partition_dir=tmp_path, seq=1)
    isolated.append(good)
    target = isolated.isolate(timestamp=TIMESTAMP)
    assert not isolated.path.exists()
    assert target.name == f"00000000000000000001{DELAY_CORRUPT_INFIX}{TIMESTAMP}"
    assert (isolated.size_bytes, isolated.record_count) == (0, 0)

    for index in range(CORRUPT_TAIL_KEEP + 3):
        (tmp_path / f"{segment.seq:020d}{DELAY_CORRUPT_INFIX}2025010{index}T000000000000").write_bytes(b"x")
    removed = segment.prune_corrupts(limit=CORRUPT_TAIL_KEEP)
    assert removed == 1 + (CORRUPT_TAIL_KEEP + 3) - CORRUPT_TAIL_KEEP
    prefix = f"{segment.seq:020d}{DELAY_CORRUPT_INFIX}"
    assert len([item for item in tmp_path.iterdir() if item.name.startswith(prefix)]) == CORRUPT_TAIL_KEEP


def test_rotation_thresholds_and_delete(tmp_path: Path) -> None:
    """段轮转的两个阈值（字节 / 条数）与整段删除（空段永不滚）。"""
    by_bytes = DelaySegment(partition_dir=tmp_path, seq=3, max_bytes=10, max_records=100)
    assert by_bytes.needs_roll(record_bytes=1) is False, "空段不滚"
    by_bytes.append(encode_record(header=_header(delay=_put()), payload=b"abc"))
    assert by_bytes.needs_roll(record_bytes=1_000) is True

    by_records = DelaySegment(partition_dir=tmp_path, seq=4, max_bytes=10_000_000, max_records=1)
    assert by_records.needs_roll(record_bytes=1) is False
    by_records.append(encode_record(header=_header(delay=_put()), payload=b"abc"))
    assert by_records.needs_roll(record_bytes=1) is True

    by_bytes.fsync()
    by_bytes.close()
    by_bytes.delete()
    assert not by_bytes.path.exists()
    assert by_bytes.size_bytes == 0


def test_scan_of_tail_only_record_counts_as_complete(tmp_path: Path) -> None:
    """单条完整记录：`complete_bytes` 等于文件大小（重建时不该误判为截断）。"""
    segment = DelaySegment(partition_dir=tmp_path, seq=0)
    blob = encode_record(header=_header(delay=_put()), payload=b"x")
    segment.append(blob)
    scan = segment.scan(verify_crc=True)
    assert scan.complete_bytes == len(blob) == segment.size_bytes
    assert scan.corrupted_position is None


class _ByteCounter:
    """恢复读量探针替身（统计 `on_bytes_read`）。"""

    def __init__(self) -> None:
        """初始化（零字节）。"""
        self.bytes = 0

    def on_bytes_read(self, *, amount: int) -> None:
        """累加读取字节数。

        Args:
            amount: 本次读取字节数。
        """
        self.bytes += amount


def test_truncate_is_noop_when_not_shrinking_and_probe_counts(tmp_path: Path) -> None:
    """`truncate` 放大即无操作；扫描读量经探针上报（恢复读量可观测）。"""
    probe = _ByteCounter()
    segment = DelaySegment(partition_dir=tmp_path, seq=0, probe=probe)
    blob = encode_record(header=_header(delay=_put()), payload=b"abc")
    segment.append(blob)

    segment.truncate(size=len(blob) + 10)
    assert segment.size_bytes == len(blob)

    assert segment.scan(verify_crc=True).complete_bytes == len(blob)
    assert probe.bytes >= len(blob)
