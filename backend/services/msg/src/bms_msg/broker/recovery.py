"""恢复点与恢复报告（02-2 详细设计 §4 / §5）：每分区一个文件，记 LEO 与段清单。

四条结构性口径：

1. **只作锚点、不作唯一真相**：恢复点记的是「已 fsync 的 LEO 与当时各段末尾」，崩溃后从它起扫
   文件尾部，接受一切 CRC 完整的新记录——恢复点落后是常态（`os` 档不刷盘），不能因为它落后就
   把「已落盘的好数据」判成垃圾。
2. **原子替换**：临时文件写完 fsync 再 `replace`。半截文件永不生效，故「文件存在」就意味着可解析；
   残留的 `.tmp` 由下次写入覆盖，不参与读取。
3. **两类异常分开**：版本高于实现 → 拒绝加载（按旧格式理解未来数据会截掉新记录）；CRC / 结构不符
   → 退回推导（一个可再生的索引文件坏了，不等于数据坏了）。
4. **不做目录 fsync**：Windows 无对应语义，做了就是不可测的平台分支；恢复点落后只让下次启动多扫
   一点，不影响正确性。
"""

import json
import os
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Protocol, cast

from bms_core.core.objects import BaseValueObject

from bms_msg.core.config import RECOVERY_POINT_FORMAT_VERSION, RECOVERY_POINT_TEMP_SUFFIX

__all__ = [
    "REASON_ABSENT",
    "REASON_CRC_MISMATCH",
    "REASON_MISMATCH",
    "REASON_OK",
    "REASON_UNPARSABLE",
    "RecoveryLedger",
    "RecoveryPoint",
    "RecoveryPointReadout",
    "RecoveryProbe",
    "RecoveryReport",
    "RecoverySummary",
    "SegmentStamp",
    "UnsupportedRecoveryPointVersionError",
    "read_recovery_point",
    "write_recovery_point",
]

REASON_OK = "ok"
"""恢复点可用。"""

REASON_ABSENT = "absent"
"""恢复点文件不存在（02-1 的数据目录即此态，向前兼容）。"""

REASON_UNPARSABLE = "unparsable"
"""JSON 解析失败 / 结构不符 / 字段类型不对。"""

REASON_CRC_MISMATCH = "crc_mismatch"
"""外层 CRC 与 payload 不符（文件被改写或写坏）。"""

REASON_MISMATCH = "mismatch"
"""`topic_id` / `partition_no` 与所在分区不符（文件被挪错目录）。"""

_WRITE_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0)
"""写临时文件的打开标志。

**必须带 `O_BINARY`**：Windows 上 `os.open` 默认文本模式，会把 JSON 里的 `\\n`（`ensure_ascii=False`
时中文不转义，但结构里仍有转义与控制字符）翻译成 `\\r\\n`，CRC 与实际字节立刻对不上——与 02-1 段文件
同一坑（实施记录 02-1 §5.1）。
"""


class UnsupportedRecoveryPointVersionError(RuntimeError):
    """恢复点格式版本高于实现支持（拒绝加载，不退回推导）。

    与「不可用」分开的原因：不可用可以退回推导（数据仍在段文件里），版本过高不能——按旧格式理解
    未来数据会把新版本记录当垃圾截掉，与架构 08 §5「数据格式不回滚」相悖。
    """


class RecoveryProbe(Protocol):
    """恢复读取量的观测探针（「不扫全量」的机器判据）。"""

    def on_bytes_read(self, *, amount: int) -> None:
        """上报一次恢复期读取的字节数。

        Args:
            amount: 本次读取的字节数。
        """


@dataclass(frozen=True)
class SegmentStamp(BaseValueObject):
    """一个段在恢复点里的快照（非末段对账的唯一依据）。"""

    base: int
    """段首 offset。"""

    size: int
    """段文件字节数（写恢复点那一刻）。"""

    last_offset: int
    """段内末条记录的绝对 offset（空段为 `base - 1`）。"""

    last_position: int
    """段内末条记录的起始字节偏移（对账读它，O(1)）。"""

    last_crc: int
    """段内末条记录的 CRC32（空段为 0）。"""

    def to_payload(self) -> dict[str, int]:
        """转成可 JSON 序列化的对象。

        Returns:
            dict[str, int]: 五个整数字段。
        """
        return {
            "base": self.base,
            "size": self.size,
            "last_offset": self.last_offset,
            "last_position": self.last_position,
            "last_crc": self.last_crc,
        }


@dataclass(frozen=True)
class RecoveryPoint(BaseValueObject):
    """一个分区的恢复点（LEO + 段清单）。"""

    topic_id: int
    """主题主键（目录归属自检）。"""

    partition_no: int
    """分区号（目录归属自检）。"""

    leo: int
    """已 fsync 的 LEO。"""

    segments: tuple[SegmentStamp, ...]
    """段清单（按 base 升序）。"""

    epoch: int = 0
    """leader 任期（02-4 / 02-5 预留，本期恒 0）。"""

    updated_at_us: int = 0
    """写入时刻（epoch 微秒；排障用，不作判据）。"""

    def to_payload(self) -> dict[str, Any]:
        """转成可 JSON 序列化的 payload（键名固定，只增不删）。

        Returns:
            dict[str, Any]: payload 对象。
        """
        return {
            "format_version": RECOVERY_POINT_FORMAT_VERSION,
            "topic_id": self.topic_id,
            "partition_no": self.partition_no,
            "leo": self.leo,
            "epoch": self.epoch,
            "updated_at_us": self.updated_at_us,
            "segments": [stamp.to_payload() for stamp in self.segments],
        }


@dataclass(frozen=True)
class RecoveryPointReadout(BaseValueObject):
    """读取结果：可用时给 `point`，不可用时给 `reason`（两者互斥）。"""

    point: RecoveryPoint | None
    """解析出的恢复点（不可用为 None）。"""

    reason: str
    """`ok` / `absent` / `unparsable` / `crc_mismatch` / `mismatch`。"""


@dataclass(frozen=True)
class RecoveryReport(BaseValueObject):
    """一次分区恢复的结果（只进日志与 `corrupted_segments()`，不改任何状态）。"""

    segments: int = 0
    """段数。"""

    start_offset: int = 0
    """恢复后的可读起点。"""

    leo: int = 0
    """恢复后的 LEO。"""

    scanned_bytes: int = 0
    """本次恢复读取的字节数（探针口径一致：实际 read 的字节）。"""

    truncated_from_bytes: int = 0
    """截断前的段大小（未截断即与 `truncated_bytes` 相同）。"""

    truncated_bytes: int = 0
    """截断后保留的字节数（未截断即段大小）。"""

    trailing_records: int = 0
    """坏记录之后「疑似完整」的记录数（只用 length 试探，不校验 CRC）。"""

    index_rebuilt: int = 0
    """补建的索引条目数（offset 索引 + 时间索引合计）。"""

    index_repair_bytes: int = 0
    """残条修复丢弃的字节数。"""

    corrupted_segments: tuple[int, ...] = ()
    """非末段对账失败的段 base 列表。"""

    recovery_point: str = REASON_ABSENT
    """恢复点的读取结论（见 `REASON_*`）。"""

    isolated: str | None = None
    """截断取证文件路径（未截断为 None）。"""

    elapsed_ms: int = 0
    """恢复耗时（毫秒）。"""

    needs_alignment: bool = False
    """是否需要位点对齐（发生截断，或恢复点声明的 LEO 大于推导 LEO）。"""


def read_recovery_point(path: Path, *, topic_id: int, partition_no: int) -> RecoveryPointReadout:
    """读恢复点（缺失 / 损坏都退回推导，版本过高抛错）。

    Args:
        path: 恢复点文件路径。
        topic_id: 期望的主题主键。
        partition_no: 期望的分区号。

    Returns:
        RecoveryPointReadout: 可用时给 `point`，否则给 `reason`。

    Raises:
        UnsupportedRecoveryPointVersionError: 文件里的版本高于实现支持。
    """
    if not path.exists():
        return RecoveryPointReadout(point=None, reason=REASON_ABSENT)
    try:
        raw = json.loads(path.read_bytes())
    except OSError, ValueError:
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    outer = _as_mapping(raw)
    if outer is None:
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    payload = _as_mapping(outer.get("payload"))
    crc = outer.get("crc")
    if payload is None or not isinstance(crc, int):
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    if zlib.crc32(_payload_bytes(payload)) != crc:
        return RecoveryPointReadout(point=None, reason=REASON_CRC_MISMATCH)
    return _decode_payload(payload, topic_id=topic_id, partition_no=partition_no)


def write_recovery_point(path: Path, point: RecoveryPoint) -> None:
    """原子写恢复点（临时文件 + fsync + rename）。

    Args:
        path: 恢复点文件路径。
        point: 恢复点。

    Raises:
        OSError: 磁盘满 / 权限不足等（调用方按 fail-open 处理，不阻断写入路径）。
    """
    payload = point.to_payload()
    document = {"payload": payload, "crc": zlib.crc32(_payload_bytes(payload))}
    raw = _payload_bytes(document)
    temporary = path.with_name(f"{path.name}{RECOVERY_POINT_TEMP_SUFFIX}")
    descriptor = os.open(temporary, _WRITE_FLAGS, 0o644)
    try:
        written = 0
        while written < len(raw):
            written += os.write(descriptor, raw[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(path)


def _payload_bytes(payload: dict[str, Any]) -> bytes:
    """把 JSON 对象规范化成字节（排序 + 紧凑，保证 CRC 可复算）。

    Args:
        payload: JSON 对象。

    Returns:
        bytes: 规范化 UTF-8 字节。
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _as_mapping(value: object) -> dict[str, Any] | None:
    """把 JSON 值收窄成字符串键的字典（不是字典即返回 None）。

    Args:
        value: 任意 JSON 值。

    Returns:
        dict[str, Any] | None: 字典或 None。
    """
    if not isinstance(value, dict):
        return None
    return cast("dict[str, Any]", value)


def _decode_payload(payload: dict[str, Any], *, topic_id: int, partition_no: int) -> RecoveryPointReadout:
    """把 payload 解码成 `RecoveryPoint`（字段缺失 / 类型不符即不可用）。

    Args:
        payload: 已通过 CRC 校验的 payload。
        topic_id: 期望的主题主键。
        partition_no: 期望的分区号。

    Returns:
        RecoveryPointReadout: 解码结果。

    Raises:
        UnsupportedRecoveryPointVersionError: 版本高于实现支持。
    """
    version = payload.get("format_version")
    if not isinstance(version, int):
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    if version > RECOVERY_POINT_FORMAT_VERSION:
        raise UnsupportedRecoveryPointVersionError(
            f"恢复点格式版本不受支持：{version} > {RECOVERY_POINT_FORMAT_VERSION}"
        )
    numbers = {name: payload.get(name) for name in ("topic_id", "partition_no", "leo", "epoch", "updated_at_us")}
    if not all(isinstance(value, int) for value in numbers.values()):
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    if numbers["topic_id"] != topic_id or numbers["partition_no"] != partition_no:
        return RecoveryPointReadout(point=None, reason=REASON_MISMATCH)
    stamps = _decode_stamps(payload.get("segments"))
    if stamps is None:
        return RecoveryPointReadout(point=None, reason=REASON_UNPARSABLE)
    return RecoveryPointReadout(
        point=RecoveryPoint(
            topic_id=int(cast("int", numbers["topic_id"])),
            partition_no=int(cast("int", numbers["partition_no"])),
            leo=int(cast("int", numbers["leo"])),
            segments=stamps,
            epoch=int(cast("int", numbers["epoch"])),
            updated_at_us=int(cast("int", numbers["updated_at_us"])),
        ),
        reason=REASON_OK,
    )


def _decode_stamps(value: object) -> tuple[SegmentStamp, ...] | None:
    """把 `segments` 数组解码成段快照列表（任一项不合法即整体不可用）。

    Args:
        value: JSON 值。

    Returns:
        tuple[SegmentStamp, ...] | None: 段快照；不合法返回 None。
    """
    if not isinstance(value, list):
        return None
    stamps: list[SegmentStamp] = []
    for item in cast("list[object]", value):
        entry = _as_mapping(item)
        if entry is None:
            return None
        fields = [entry.get(name) for name in ("base", "size", "last_offset", "last_position", "last_crc")]
        if not all(isinstance(field, int) for field in fields):
            return None
        base, size, last_offset, last_position, last_crc = (int(cast("int", field)) for field in fields)
        stamps.append(
            SegmentStamp(
                base=base,
                size=size,
                last_offset=last_offset,
                last_position=last_position,
                last_crc=last_crc,
            )
        )
    return tuple(stamps)


@dataclass(frozen=True)
class RecoverySummary(BaseValueObject):
    """一个分区的恢复结论摘要（台账条目；只留判据字段，不留字节细节）。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    truncated_bytes: int
    """被截掉的字节数（`0` 表示未发生截断；只作判据，不记绝对值）。"""

    trailing_records: int
    """坏记录之后「疑似完整」的记录数（排障参考，不作判据）。"""

    corrupted_segments: tuple[int, ...]
    """非末段对账失败的段 base 列表（空表示各段与恢复点一致）。"""

    @property
    def degraded(self) -> bool:
        """该分区是否发生过「数据有损」（截断或非末段损坏）。

        Returns:
            bool: True 表示恢复期发现过损坏。
        """
        return self.truncated_bytes > 0 or self.corrupted_segments != ()


class RecoveryLedger:
    """进程级恢复台账（02-6 就绪检查项的只读数据源）。

    **为什么要台账**：`RecoveryReport` 是「单分区、最近一次」的结论且分区懒加载，
    启动期根本不存在可查状态；`/readyz` 要判「有无数据损坏」只能靠一张按分区累积的表。

    只记「最近一次恢复」的分区结论（同分区重复打开以最新一次为准）：覆盖写入，条数上界＝分区数。
    """

    def __init__(self) -> None:
        """初始化（空台账）。"""
        self._entries: dict[tuple[int, int], RecoverySummary] = {}

    def record(self, *, topic_id: int, partition_no: int, report: RecoveryReport) -> None:
        """登记一个分区的恢复结论（覆盖同分区旧值）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            report: 该分区的恢复报告。
        """
        truncated = report.truncated_from_bytes - report.truncated_bytes
        self._entries[(topic_id, partition_no)] = RecoverySummary(
            topic_id=topic_id,
            partition_no=partition_no,
            truncated_bytes=truncated,
            trailing_records=report.trailing_records,
            corrupted_segments=report.corrupted_segments,
        )

    def entries(self) -> tuple[RecoverySummary, ...]:
        """取全部条目（插入序，稳定）。

        Returns:
            tuple[RecoverySummary, ...]: 台账快照。
        """
        return tuple(self._entries.values())

    def degraded(self) -> tuple[RecoverySummary, ...]:
        """取发生过数据损坏的条目（空表示全部完好或尚未打开任何分区）。

        Returns:
            tuple[RecoverySummary, ...]: 损坏条目。
        """
        return tuple(item for item in self._entries.values() if item.degraded)

    def __len__(self) -> int:
        """当前条目数（排障与用例断言用）。

        Returns:
            int: 条目数。
        """
        return len(self._entries)
