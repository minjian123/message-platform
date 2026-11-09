"""段文件与索引（02-1 详细设计 §6）：`.log` / `.index` / `.timeindex` 三件套。

四条结构性口径：

1. **段名即 base offset**：20 位零填充，字典序等于 offset 序——段名二分与目录排序用的是同一份序，
   不需要额外元数据文件来记「有哪些段」。
2. **索引是稀疏的、且与 `append` 同源**：offset 索引每越过 `INDEX_INTERVAL_BYTES` 记一条；时间索引每越过
   `TIME_INDEX_INTERVAL_MS` 记一条（且时间戳单调不减）。写入顺序固定「先日志、后索引」——反过来的话
   索引会指向尚未落盘的记录。
3. **时间索引末条 = 段内最大发布时间的来源**：条目只在「时间创新高且越过间隔」时追加，故末条时间
   恒不小于真实最大值（时间回退的记录不写条目），保留判定因此不会提前删段（误差上界见详细设计 §6.5）。
4. **尾部扫描只读头、不读载荷**：顺序扫描拿到 `length` 就跳到下一条，`payload` 一个字节都不进内存
   （零拷贝与启动成本的前提）；只有真正的读取路径才 `read_record` 解整条。
"""

import contextlib
import os
import re
import struct
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from bms_core.core.objects import BaseValueObject

from bms_msg.broker.record import (
    CorruptedRecordError,
    ParsedRecord,
    RecordHeader,
    decode_header_blob,
    decode_record,
    read_frame,
)
from bms_msg.broker.recovery import RecoveryProbe
from bms_msg.core.config import (
    CORRUPT_TAIL_INFIX,
    INDEX_INTERVAL_BYTES,
    RECORD_FRAME_BYTES,
    TIME_INDEX_INTERVAL_MS,
)
from bms_msg.metrics import get_traffic_metrics

__all__ = [
    "INDEX_SUFFIX",
    "LOG_SUFFIX",
    "TIME_SUFFIX",
    "SegmentFile",
    "TailScan",
    "list_segment_bases",
]

LOG_SUFFIX = ".log"
"""日志段后缀。"""

INDEX_SUFFIX = ".index"
"""稀疏 offset 索引后缀（条目 8 字节：相对 offset + 段内位置）。"""

TIME_SUFFIX = ".timeindex"
"""时间索引后缀（条目 12 字节：微秒时间戳 + 相对 offset）。"""

_OPEN_MODE = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
"""追加写的打开标志。

**必须带 `O_BINARY`**：Windows 上 `os.open` 默认是文本模式，会把 `0x0A` 翻译成 `0x0D 0x0A`——
记录头里的 msgpack 字节很容易撞上 `0x0A`，落盘后偏移全错（实测每条多 1 字节、后续记录读成垃圾）。
"""

_SEGMENT_PATTERN = re.compile(r"^(\d{20})\.log$")
"""段文件名形态（20 位零填充的 base offset）。"""

_INDEX_ENTRY = struct.Struct(">II")
"""offset 索引条目：`relative_offset` / `position`（段大小 64 MiB，两者都在 u32 内）。"""

_TIME_ENTRY = struct.Struct(">qI")
"""时间索引条目：`timestamp_us` / `relative_offset`。"""


@dataclass(frozen=True)
class TailScan(BaseValueObject):
    """末段尾部扫描结果（崩溃恢复的唯一输入）。"""

    from_rel: int
    """扫描起点在段内的相对 offset（锚点；无锚点且无索引为 0）。"""

    from_position: int
    """扫描起点在段文件内的字节偏移。"""

    count: int
    """从起点起扫到的完整记录条数。"""

    complete_bytes: int
    """完整区间的字节长度（小于文件大小即需要截断到这个位置）。"""

    entries: tuple[tuple[int, int, RecordHeader], ...] = ()
    """扫到的「字节偏移 / 相对 offset / 头」（补建索引用；尾部有界）。"""

    last_crc: int = 0
    """最后一条完整记录的 CRC32（写恢复点、算段 stamp 用）。"""

    last_position: int = 0
    """最后一条完整记录的起始字节偏移。"""

    corrupted_position: int | None = None
    """第一条坏记录的起点；None 表示扫到文件尾无坏记录。"""

    trailing_records: int = 0
    """坏记录之后「疑似完整」的记录数（只用 `length` 试探，不校验 CRC）。"""


def list_segment_bases(directory: Path) -> list[int]:
    """列出一个分区目录下的段 base offset（升序；目录不存在返回空）。

    Args:
        directory: 分区目录。

    Returns:
        list[int]: 段 base offset 升序列表。
    """
    if not directory.is_dir():
        return []
    bases: list[int] = []
    for entry in directory.iterdir():
        matched = _SEGMENT_PATTERN.match(entry.name)
        if matched is not None:
            bases.append(int(matched.group(1)))
    return sorted(bases)


class SegmentFile:
    """一个段（三个文件）：只追加、可顺序扫描、可按 offset 定位、可整段删除。"""

    def __init__(
        self,
        *,
        partition_dir: Path,
        base_offset: int,
        index_interval: int = INDEX_INTERVAL_BYTES,
        time_interval_ms: int = TIME_INDEX_INTERVAL_MS,
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化（不读文件内容，索引与大小按需加载）。

        Args:
            partition_dir: 分区目录（段文件所在目录）。
            base_offset: 段首 offset（同时是文件名）。
            index_interval: 稀疏 offset 索引的字节间隔。
            time_interval_ms: 时间索引的毫秒间隔。
            probe: 恢复读取量探针（缺省不统计，生产零开销）。
        """
        self._partition_dir = partition_dir
        self._base_offset = base_offset
        self._index_interval = index_interval
        self._time_interval_us = time_interval_ms * 1_000
        self._log_path = partition_dir / f"{base_offset:020d}{LOG_SUFFIX}"
        self._index_path = partition_dir / f"{base_offset:020d}{INDEX_SUFFIX}"
        self._time_path = partition_dir / f"{base_offset:020d}{TIME_SUFFIX}"
        self._size = self._log_path.stat().st_size if self._log_path.exists() else 0
        self._index: list[tuple[int, int]] | None = None
        self._time_index: list[tuple[int, int]] | None = None
        self._write_fd: int | None = None
        self._probe = probe
        self._last_stamp: tuple[int, int, int] | None = None

    @property
    def base_offset(self) -> int:
        """段首 offset（含）。

        Returns:
            int: base offset。
        """
        return self._base_offset

    @property
    def log_path(self) -> Path:
        """`.log` 文件路径。

        Returns:
            Path: 段文件。
        """
        return self._log_path

    @property
    def size_bytes(self) -> int:
        """段内字节合计（真实文件大小；写入时内存累加，不必每次 stat）。

        Returns:
            int: 字节数。
        """
        return self._size

    def index_path(self) -> Path:
        """`.index` 文件路径。

        Returns:
            Path: 稀疏 offset 索引。
        """
        return self._index_path

    def time_index_path(self) -> Path:
        """`.timeindex` 文件路径。

        Returns:
            Path: 时间索引。
        """
        return self._time_path

    def ensure_indexes(self) -> None:
        """加载索引到内存（幂等；段内无索引文件即视为空索引）。"""
        if self._index is not None and self._time_index is not None:
            return
        self._index = _read_entries(self._index_path, _INDEX_ENTRY)
        self._time_index = _read_entries(self._time_path, _TIME_ENTRY)

    def max_published_us(self) -> int:
        """段内最大发布时间的取值来源：时间索引末条时间戳。

        Returns:
            int: epoch 微秒；无记录（空段）返回 0。
        """
        entries = self.time_index_entries()
        return entries[-1][0] if entries else 0

    def time_index_entries(self) -> list[tuple[int, int]]:
        """时间索引条目（`(timestamp_us, relative_offset)` 升序）。

        Returns:
            list[tuple[int, int]]: 条目列表。
        """
        self.ensure_indexes()
        assert self._time_index is not None  # ensure_indexes 后必非空
        return self._time_index

    def iter_frames(self, *, from_position: int = 0, limit: int | None = None) -> Iterator[tuple[int, int, int]]:
        """从给定偏移起顺序产出「字节偏移 / 记录总长 / 头长」（只读框架，不读头与载荷）。

        Args:
            from_position: 起始字节偏移。
            limit: 最多产出条数（None 表示到段尾）。

        Yields:
            tuple[int, int, int]: 偏移 / 总长 / 头长。

        Raises:
            CorruptedRecordError: 遇到不完整或非法记录。
        """
        size = self._size
        position = from_position
        produced = 0
        with self._log_path.open("rb") as handle:
            while position < size and (limit is None or produced < limit):
                handle.seek(position)
                frame_bytes = handle.read(RECORD_FRAME_BYTES)
                frame = read_frame(frame_bytes)
                total = 4 + frame.length
                if position + total > size:
                    raise CorruptedRecordError(f"记录不完整：{position}+{total} > {size}")
                yield position, total, frame.header_len
                position += total
                produced += 1

    def position_of(self, *, offset: int) -> tuple[int, int]:
        """段内相对 offset 定位到「字节偏移 + 相对 offset」（二分最近索引项）。

        Args:
            offset: 绝对 offset（须在本段范围内）。

        Returns:
            tuple[int, int]: 索引项给出的「字节偏移 / 相对 offset」；无索引项时为 `(0, 0)`。
        """
        self.ensure_indexes()
        assert self._index is not None  # ensure_indexes 后必非空
        relative = offset - self._base_offset
        index = self._index
        if not index:
            return 0, 0
        position = 0
        located = 0
        low = 0
        high = len(index)
        while low < high:  # 手写二分：找最后一个 relative <= 目标的条目
            middle = (low + high) // 2
            if index[middle][0] <= relative:
                position = index[middle][1]
                located = index[middle][0]
                low = middle + 1
            else:
                high = middle
        return position, located

    def append_record(self, *, offset: int, published_at_us: int, blob: bytes) -> int:
        """追加一条已编码记录，并按间隔补索引条目。

        Args:
            offset: 本条记录的绝对 offset（由 `append` 唯一分配）。
            published_at_us: 本条记录的发布时间（epoch 微秒；时间索引判据）。
            blob: 已编码的完整记录字节。

        Returns:
            int: 本条记录在段文件内的字节偏移（读取原语用）。
        """
        position = self._size
        self._write(blob)
        self._size += len(blob)
        relative = offset - self._base_offset
        self._last_stamp = (position, relative, read_frame(blob[:RECORD_FRAME_BYTES]).crc)
        self.ensure_indexes()
        assert self._index is not None and self._time_index is not None  # ensure_indexes 后必非空
        if not self._index or position - self._index[-1][1] >= self._index_interval:
            _append_entry(self._index_path, _INDEX_ENTRY, (relative, position))
            self._index.append((relative, position))
        if not self._time_index or published_at_us - self._time_index[-1][0] >= self._time_interval_us:
            _append_entry(self._time_path, _TIME_ENTRY, (published_at_us, relative))
            self._time_index.append((published_at_us, relative))
        return position

    def scan_tail(self, *, verify_crc: bool = False, anchor: tuple[int, int] | None = None) -> TailScan:
        """从锚点起顺序扫描到段尾，给出完整区间（崩溃恢复的唯一扫描入口）。

        Args:
            verify_crc: 是否校验 CRC（02-1 的结构级判定传 False；崩溃恢复传 True）。
            anchor: 锚点 `(字节偏移, 相对 offset)`；None 表示用最后一条索引项。

        Returns:
            TailScan: 起点、完整条数、完整区间、逐条坐标与坏记录信息。

        Raises:
            UnsupportedRecordVersionError: 记录版本高于实现支持（冒泡拒绝加载，不截断）。
        """
        if anchor is None:
            self.ensure_indexes()
            assert self._index is not None  # ensure_indexes 后必非空
            if self._index:
                from_position, from_rel = self._index[-1][1], self._index[-1][0]
            else:
                from_position, from_rel = 0, 0
        else:
            from_position, from_rel = anchor
        size = self._size
        start_position = min(max(from_position, 0), size)
        position = start_position
        current_rel = from_rel
        entries: list[tuple[int, int, RecordHeader]] = []
        last_crc = 0
        last_position = 0
        corrupted: int | None = None
        with self._log_path.open("rb") as handle:
            while position < size:
                handle.seek(position)
                frame_bytes = self._emit_read(handle, RECORD_FRAME_BYTES)
                try:
                    frame = read_frame(frame_bytes)
                    total = 4 + frame.length
                    if position + total > size:
                        raise CorruptedRecordError(f"记录不完整：{position}+{total} > {size}")
                    header = self._read_header(
                        handle,
                        head=frame_bytes,
                        header_len=frame.header_len,
                        total=total,
                        verify_crc=verify_crc,
                    )
                except CorruptedRecordError:
                    corrupted = position
                    break
                entries.append((position, current_rel, header))
                last_crc = frame.crc
                last_position = position
                current_rel += 1
                position += total
            trailing = 0 if corrupted is None else self._count_trailing_records(handle, position=corrupted, size=size)
        return TailScan(
            from_rel=from_rel,
            from_position=start_position,
            count=len(entries),
            complete_bytes=position,
            entries=tuple(entries),
            last_crc=last_crc,
            last_position=last_position,
            corrupted_position=corrupted,
            trailing_records=trailing,
        )

    def index_anchor(self) -> tuple[int, int] | None:
        """最后一条索引项作为扫描锚点。

        Returns:
            tuple[int, int] | None: `(字节偏移, 相对 offset)`；无索引条目返回 None。
        """
        self.ensure_indexes()
        assert self._index is not None  # ensure_indexes 后必非空
        if not self._index:
            return None
        relative, position = self._index[-1]
        return position, relative

    def repair_index_tail(self) -> int:
        """把两个索引文件的尾部残条截掉（崩溃可能写了一半条目）。

        **为什么必须修**：`_read_entries` 只读整数条（丢弃残条），但 `_append_entry` 用 `O_APPEND`
        从物理尾追加——残条留在文件里，后续条目会整体错位，索引从此全段读偏。

        Returns:
            int: 丢弃的字节数（两文件合计；无残条为 0）。
        """
        dropped = 0
        for path, entry_size in ((self._index_path, _INDEX_ENTRY.size), (self._time_path, _TIME_ENTRY.size)):
            if not path.exists():
                continue
            actual = path.stat().st_size
            remainder = actual % entry_size
            if remainder == 0:
                continue
            with path.open("r+b") as handle:
                handle.truncate(actual - remainder)
            dropped += remainder
        if dropped:
            self._index = None
            self._time_index = None
        return dropped

    def backfill_index(self, entries: tuple[tuple[int, int, RecordHeader], ...]) -> int:
        """按写入路径的同一判据补建尾部区间内缺失的索引条目。

        Args:
            entries: 扫描到的「字节偏移 / 相对 offset / 头」（须按 relative 升序）。

        Returns:
            int: 补建的条目数（offset 索引 + 时间索引合计）。
        """
        self.ensure_indexes()
        assert self._index is not None and self._time_index is not None  # ensure_indexes 后必非空
        rebuilt = 0
        for position, relative, header in entries:
            index_tail = self._index[-1] if self._index else None
            if index_tail is None or (relative > index_tail[0] and position - index_tail[1] >= self._index_interval):
                _append_entry(self._index_path, _INDEX_ENTRY, (relative, position))
                self._index.append((relative, position))
                rebuilt += 1
            time_tail = self._time_index[-1] if self._time_index else None
            if time_tail is None or (
                header.published_at_us > time_tail[0]
                and header.published_at_us - time_tail[0] >= self._time_interval_us
            ):
                _append_entry(self._time_path, _TIME_ENTRY, (header.published_at_us, relative))
                self._time_index.append((header.published_at_us, relative))
                rebuilt += 1
        return rebuilt

    def last_record_stamp(self) -> tuple[int, int, int] | None:
        """段内末条记录的「字节偏移 / 相对 offset / CRC」（写恢复点用）。

        Returns:
            tuple[int, int, int] | None: 三元组；空段返回 None。
        """
        if self._last_stamp is not None:
            return self._last_stamp
        if self._size == 0:
            return None
        self.ensure_indexes()
        assert self._index is not None  # ensure_indexes 后必非空
        relative, position = self._index[-1] if self._index else (0, 0)
        last: tuple[int, int, int] | None = None
        size = self._size
        with self._log_path.open("rb") as handle:
            while position < size:
                handle.seek(position)
                frame_bytes = self._emit_read(handle, RECORD_FRAME_BYTES)
                try:
                    frame = read_frame(frame_bytes)
                except CorruptedRecordError:  # pragma: no cover - 恢复已把末段修到完整，写入期不会读到坏框架
                    break
                total = 4 + frame.length
                if position + total > size:  # pragma: no cover - 同上：恢复后段尾必然完整
                    break
                last = (position, relative, frame.crc)
                relative += 1
                position += total
        return last

    def verify_stamp(self, *, position: int, expected_crc: int) -> tuple[bool, int]:
        """校验非末段段尾那条记录（读整条并算 CRC）。

        Args:
            position: 该记录的起始字节偏移。
            expected_crc: 恢复点里记的 CRC32。

        Returns:
            tuple[bool, int]: 是否一致 / 实际读到的 CRC（读不到为 0）。
        """
        if position < 0 or position + RECORD_FRAME_BYTES > self._size:
            return False, 0
        with self._log_path.open("rb") as handle:
            handle.seek(position)
            frame_bytes = self._emit_read(handle, RECORD_FRAME_BYTES)
            try:
                frame = read_frame(frame_bytes)
            except CorruptedRecordError:
                # 段尾被外部改写：对账要「判损坏」而不是让恢复流程崩掉
                return False, 0
            total = 4 + frame.length
            if position + total > self._size:
                return False, frame.crc
            body = self._emit_read(handle, total - RECORD_FRAME_BYTES)
        try:
            decode_record(frame_bytes + body)
        except CorruptedRecordError:
            return False, frame.crc
        return frame.crc == expected_crc, frame.crc

    def isolate_tail(self, *, keep_bytes: int, timestamp: str) -> Path:
        """把 `[keep_bytes, 段尾)` 的字节另存为取证文件（截断前调用）。

        Args:
            keep_bytes: 保留区间的字节长度（坏记录之前）。
            timestamp: 取证文件后缀里的时间戳（由调用方给，便于确定性用例）。

        Returns:
            Path: 取证文件路径。
        """
        target = self._partition_dir / f"{self._base_offset:020d}{CORRUPT_TAIL_INFIX}{timestamp}"
        with self._log_path.open("rb") as source, target.open("wb") as sink:
            source.seek(keep_bytes)
            remaining = self._size - keep_bytes
            while remaining > 0:
                chunk = source.read(min(remaining, 1_048_576))
                if not chunk:  # pragma: no cover - seek 与 size 一致，读不满只可能是并发截断
                    break
                sink.write(chunk)
                remaining -= len(chunk)
        return target

    def prune_corrupt_tails(self, *, limit: int) -> int:
        """按时间删最早的取证副本，只保留最近 `limit` 份。

        Args:
            limit: 保留份数。

        Returns:
            int: 删除的份数。
        """
        prefix = f"{self._base_offset:020d}{CORRUPT_TAIL_INFIX}"
        candidates = sorted(
            (path for path in self._partition_dir.iterdir() if path.name.startswith(prefix)),
            key=lambda path: path.name,
        )
        removed = 0
        for path in candidates[: max(len(candidates) - limit, 0)]:
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    def _read_header(
        self, handle: IO[bytes], *, head: bytes, header_len: int, total: int, verify_crc: bool
    ) -> RecordHeader:
        """读一条记录的头（需要时连带读载荷并校验 CRC）。

        Args:
            handle: 段文件句柄（已读完 12 字节框架）。
            head: 已读出的 12 字节框架（拼整条时复用，避免重读与重新编码）。
            header_len: 头字节数。
            total: 记录总长。
            verify_crc: 是否校验 CRC。

        Returns:
            RecordHeader: 记录头。

        Raises:
            CorruptedRecordError: 记录体不完整 / CRC 不符 / 头无法解析。
        """
        rest = self._emit_read(handle, total - RECORD_FRAME_BYTES)
        if len(rest) != total - RECORD_FRAME_BYTES:
            # 循环已保证 position + total ≤ size；同一临界区内不会并发截断
            raise CorruptedRecordError("记录体不完整")  # pragma: no cover - 不可达（见上）
        if verify_crc:
            # 拼回整条再解：CRC 覆盖「头长 + 头 + 载荷」，少一段就校不出来
            return decode_record(head + rest).header
        header_blob = rest[:header_len]
        if len(header_blob) != header_len:  # pragma: no cover - 框架已保证 total ≥ 12 + header_len
            raise CorruptedRecordError("记录头不完整")
        return decode_header_blob(header_blob)

    def _count_trailing_records(self, handle: IO[bytes], *, position: int, size: int) -> int:
        """数「坏记录之后」还能读出的完整框架数（只用 length 试探，不校验 CRC）。

        Args:
            handle: 段文件句柄。
            position: 坏记录的起点。
            size: 段文件大小。

        Returns:
            int: 疑似完整的记录数（坏记录本身不计）。
        """
        handle.seek(position)
        frame_bytes = self._emit_read(handle, RECORD_FRAME_BYTES)
        try:
            frame = read_frame(frame_bytes)
        except CorruptedRecordError:
            # 坏到框架都读不出（长度字段被写坏）：无法定位后继，计 0
            return 0
        cursor = position + 4 + frame.length
        if cursor > size:
            # 坏记录声明的长度超出文件：其后什么都没有
            return 0
        count = 0
        while cursor + RECORD_FRAME_BYTES <= size:
            handle.seek(cursor)
            try:
                following = read_frame(self._emit_read(handle, RECORD_FRAME_BYTES))
            except CorruptedRecordError:
                break
            total = 4 + following.length
            if cursor + total > size:
                break
            count += 1
            cursor += total
        return count

    def _emit_read(self, handle: IO[bytes], amount: int) -> bytes:
        """读一段字节并上报探针（恢复期读取量的唯一出口）。

        Args:
            handle: 文件句柄。
            amount: 期望读取的字节数。

        Returns:
            bytes: 实际读到的字节（可能不足）。
        """
        blob = handle.read(amount)
        if self._probe is not None:
            self._probe.on_bytes_read(amount=len(blob))
        return blob

    def iter_headers(
        self, *, from_position: int = 0, limit: int | None = None
    ) -> Iterator[tuple[int, int, RecordHeader]]:
        """从给定偏移起顺序产出「字节偏移 / 记录总长 / 头」（不读载荷）。

        Args:
            from_position: 起始字节偏移。
            limit: 最多产出条数（None 表示到段尾）。

        Yields:
            tuple[int, int, RecordHeader]: 偏移 / 总长 / 头。

        Raises:
            CorruptedRecordError: 扫描途中遇到不完整或非法记录。
        """
        size = self._size
        position = from_position
        produced = 0
        with self._log_path.open("rb") as handle:
            while position < size and (limit is None or produced < limit):
                handle.seek(position)
                frame_bytes = handle.read(RECORD_FRAME_BYTES)
                frame = read_frame(frame_bytes)
                total = 4 + frame.length
                if position + total > size:
                    raise CorruptedRecordError(f"记录不完整：{position}+{total} > {size}")
                header_blob = handle.read(frame.header_len)
                if len(header_blob) != frame.header_len:  # pragma: no cover - read_frame 已保证 total ≥ 12+header_len
                    raise CorruptedRecordError(f"记录头不完整：偏移 {position}")
                yield position, total, decode_header_blob(header_blob)
                position += total
                produced += 1

    def read_record(self, *, position: int) -> ParsedRecord:
        """读一条完整记录并校验 CRC（读取路径与读取原语用）。

        Args:
            position: 记录起始的字节偏移。

        Returns:
            ParsedRecord: 头 / 载荷 / 总长。

        Raises:
            CorruptedRecordError: 记录不完整 / CRC 不符 / 头非法。
        """
        with self._log_path.open("rb") as handle:
            handle.seek(position)
            head = handle.read(RECORD_FRAME_BYTES)
            frame = read_frame(head)
            total = 4 + frame.length
            rest = handle.read(total - RECORD_FRAME_BYTES)
            if len(rest) != total - RECORD_FRAME_BYTES:
                raise CorruptedRecordError(f"记录不完整：偏移 {position}")
            return decode_record(head + rest)

    def truncate(self, *, size: int, max_relative_offset: int) -> None:
        """把段文件截断到给定字节数（结构级恢复用），并丢弃越界的索引条目。

        Args:
            size: 保留的字节数（须不超过当前大小）。
            max_relative_offset: 截断后仍存在记录的最大相对 offset（含）；更晚的索引条目全部丢弃。
        """
        if size >= self._size:
            return
        self.close()
        with self._log_path.open("r+b") as handle:
            handle.truncate(size)
        self._size = size
        self._last_stamp = None
        self.ensure_indexes()
        assert self._index is not None and self._time_index is not None  # ensure_indexes 后必非空
        kept_index = [entry for entry in self._index if entry[0] <= max_relative_offset]
        kept_time = [entry for entry in self._time_index if entry[1] <= max_relative_offset]
        if len(kept_index) != len(self._index):
            _rewrite_entries(self._index_path, _INDEX_ENTRY, kept_index)
            self._index = kept_index
        if len(kept_time) != len(self._time_index):
            _rewrite_entries(self._time_path, _TIME_ENTRY, kept_time)
            self._time_index = kept_time

    def fsync(self) -> None:
        """把当前写句柄的内容刷到磁盘（无写句柄即无操作）。"""
        if self._write_fd is not None:
            started = time.perf_counter()
            os.fsync(self._write_fd)
            with contextlib.suppress(Exception):
                get_traffic_metrics().record_fsync(
                    scope="log",
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                )

    def close(self) -> None:
        """关闭写句柄（幂等；读路径不持句柄）。"""
        if self._write_fd is not None:
            os.close(self._write_fd)
            self._write_fd = None

    def delete(self) -> None:
        """整段删除（三个文件一起删；幂等）。"""
        self.close()
        for path in (self._log_path, self._index_path, self._time_path):
            path.unlink(missing_ok=True)

    def refresh_size(self) -> None:
        """按文件实际大小刷新内存中的段大小（外部改写后对账用）。"""
        self._size = self._log_path.stat().st_size if self._log_path.exists() else 0
        self._last_stamp = None

    def _write(self, blob: bytes) -> None:
        """追加写 `.log`（持写句柄；短写循环补齐）。

        Args:
            blob: 完整记录字节。
        """
        if self._write_fd is None:
            self._write_fd = os.open(self._log_path, _OPEN_MODE, 0o644)
        written = 0
        while written < len(blob):
            written += os.write(self._write_fd, blob[written:])


def _read_entries(path: Path, layout: struct.Struct) -> list[tuple[int, int]]:
    """读一个定长条目索引文件（尾部残条忽略——写入崩溃只丢最后一条索引，读取会从更早的项扫起）。

    Args:
        path: 索引文件路径。
        layout: 条目布局（两个字段）。

    Returns:
        list[tuple[int, int]]: 条目列表（按写入顺序）。
    """
    if not path.exists():
        return []
    blob = path.read_bytes()
    entry_size = layout.size
    count = len(blob) // entry_size
    entries: list[tuple[int, int]] = []
    for index in range(count):
        first, second = layout.unpack_from(blob, index * entry_size)
        entries.append((first, second))
    return entries


def _append_entry(path: Path, layout: struct.Struct, entry: tuple[int, int]) -> None:
    """追加一条索引条目（打开 → 写 → 关闭：索引写入频率低，不值得持句柄）。

    Args:
        path: 索引文件路径。
        layout: 条目布局。
        entry: 两个字段的值。
    """
    descriptor = os.open(path, _OPEN_MODE, 0o644)
    try:
        os.write(descriptor, layout.pack(*entry))
    finally:
        os.close(descriptor)


def _rewrite_entries(path: Path, layout: struct.Struct, entries: list[tuple[int, int]]) -> None:
    """重写索引文件（截断后丢弃越界条目；先写临时文件再替换，避免半截文件）。

    Args:
        path: 索引文件路径。
        layout: 条目布局。
        entries: 保留的条目。
    """
    temporary = path.with_name(f"{path.name}.tmp")
    with temporary.open("wb") as handle:
        for entry in entries:
            handle.write(layout.pack(*entry))
    temporary.replace(path)
