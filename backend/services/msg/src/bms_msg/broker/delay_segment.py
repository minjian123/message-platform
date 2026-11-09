"""延迟段（02-7 详细设计 §4）：`.delay` 的追加 / 扫描 / 截断 / 隔离 / 清扫。

四条结构性口径：

1. **后缀独立、序号独立**：文件名 `{seq:020d}.delay`，`seq` 是**延迟段自己的**序号（0 起、只增不复用），
   与 offset 无关——延迟记录不占 offset，用 offset 命名会造出一个「没有任何记录占用的 base offset」，
   且保留期删掉同号 `.log` 段后该 `.delay` 仍在，排障必然误判。20 位零填充让字典序 = 序号序，
   「重建按段序号判写入先后」才成立（后写胜出的判据）。
2. **不带索引**：延迟区重建靠全量扫（写入顺序与到期顺序无关，只扫末段会丢记录），稀疏 offset 索引
   与时间索引在这里没有消费者。因此本模块**不碰** `segment.py`——`.log` 段与其三种索引一行不改。
3. **`_SEGMENT_PATTERN` 反向也安全**：本模块的枚举只认 `^(\\d{20})\\.delay$`，不会把
   `.delay.corrupt-*`（隔离副本）与 `.log` 当延迟段；反之 `list_segment_bases()` 只认 `.log`，
   也不会把 `.delay` 当数据段。
4. **损坏只能整段处置**：记录是变长的，一条坏记录之后无法可靠重同步（强行试探会把垃圾当记录读出）。
   故末段截断（崩溃常态）、非末段整段隔离（改名留证 + 告警），与 02-2 对 `.log` 的处置同构。
"""

import contextlib
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from bms_core.core.objects import BaseValueObject

from bms_msg.broker.record import (
    CorruptedRecordError,
    RecordHeader,
    decode_header_blob,
    decode_record,
    read_frame,
)
from bms_msg.broker.recovery import RecoveryProbe
from bms_msg.core.config import (
    DELAY_SEGMENT_MAX_BYTES,
    DELAY_SEGMENT_MAX_RECORDS,
    RECORD_FRAME_BYTES,
)
from bms_msg.metrics import get_traffic_metrics

__all__ = [
    "DELAY_CORRUPT_INFIX",
    "DELAY_SUFFIX",
    "DelayEntry",
    "DelayScan",
    "DelaySegment",
    "list_delay_segment_seqs",
]

DELAY_SUFFIX = ".delay"
"""延迟段后缀。"""

DELAY_CORRUPT_INFIX = ".delay.corrupt-"
"""隔离副本的中缀：`{seq:020d}.delay.corrupt-{ts}`。

刻意不以 `.delay` 结尾（后面还有时间戳）：段枚举只认 20 位零填充 + `.delay`，副本不得参与重建。
"""

_OPEN_MODE = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0)
"""追加写的打开标志。

**必须带 `O_BINARY`**：Windows 上 `os.open` 默认文本模式，会把 `0x0A` 译成 `0x0D 0x0A`——
记录头是 msgpack，撞上 `0x0A` 的概率很高，落盘后长度对不上（02-1 已踩过，见 02-1 实施记录 §5.1）。
"""

_DELAY_PATTERN = re.compile(r"^(\d{20})\.delay$")
"""延迟段文件名形态（20 位零填充的段序号）。"""


@dataclass(frozen=True)
class DelayEntry(BaseValueObject):
    """段内一条记录（扫描产物）。"""

    position: int
    """段内字节偏移。"""

    header: RecordHeader
    """记录头（含 `delay` 子结构）。"""

    payload: bytes
    """业务字节（`clear` 记录为空）。"""


@dataclass(frozen=True)
class DelayScan(BaseValueObject):
    """一次段扫描的结果（重建的唯一输入）。"""

    entries: tuple[DelayEntry, ...] = ()
    """扫到的完整记录（按写入顺序）。"""

    complete_bytes: int = 0
    """完整区间的字节长度（小于文件大小即末段需截断到这个位置）。"""

    corrupted_position: int | None = None
    """第一条坏记录的起点；None 表示扫到段尾无坏记录。"""

    last_crc: int = 0
    """最后一条完整记录的 CRC32（排障用）。"""


def list_delay_segment_seqs(directory: Path) -> list[int]:
    """列出一个分区目录下的延迟段序号（升序；目录不存在返回空）。

    Args:
        directory: 分区目录。

    Returns:
        list[int]: 段序号升序列表。
    """
    if not directory.is_dir():
        return []
    seqs: list[int] = []
    for entry in directory.iterdir():
        matched = _DELAY_PATTERN.match(entry.name)
        if matched is not None:
            seqs.append(int(matched.group(1)))
    return sorted(seqs)


class DelaySegment:
    """一个 `.delay` 段（只追加、可顺序扫描、可截断、可整段隔离）。"""

    def __init__(
        self,
        *,
        partition_dir: Path,
        seq: int,
        max_bytes: int = DELAY_SEGMENT_MAX_BYTES,
        max_records: int = DELAY_SEGMENT_MAX_RECORDS,
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化（不读文件内容：大小与条数按需由扫描或追加维护）。

        Args:
            partition_dir: 分区目录（段文件所在目录）。
            seq: 段序号（只增不复用）。
            max_bytes: 段滚动字节阈值。
            max_records: 段滚动条数阈值。
            probe: 恢复读取量探针（缺省不统计，生产零开销）。
        """
        self._partition_dir = partition_dir
        self._seq = seq
        self._max_bytes = max_bytes
        self._max_records = max_records
        self._path = partition_dir / f"{seq:020d}{DELAY_SUFFIX}"
        self._size = self._path.stat().st_size if self._path.exists() else 0
        self._count = 0
        self._write_fd: int | None = None
        self._probe = probe

    @property
    def seq(self) -> int:
        """段序号。

        Returns:
            int: 段序号。
        """
        return self._seq

    @property
    def path(self) -> Path:
        """段文件路径。

        Returns:
            Path: `.delay` 文件。
        """
        return self._path

    @property
    def size_bytes(self) -> int:
        """段内字节数（写入时内存累加，不每次 stat）。

        Returns:
            int: 字节数。
        """
        return self._size

    @property
    def record_count(self) -> int:
        """段内记录条数（追加累加；重建时由扫描结果回填）。

        Returns:
            int: 条数。
        """
        return self._count

    def set_record_count(self, count: int) -> None:
        """回填记录条数（重建扫描后调用：本类不缓存扫描结果）。

        Args:
            count: 段内完整记录条数。
        """
        self._count = count

    def needs_roll(self, *, record_bytes: int) -> bool:
        """判定本条写入前是否需要先滚动到新段（空段永不滚）。

        Args:
            record_bytes: 本条记录字节数。

        Returns:
            bool: True 表示需要滚动。
        """
        if self._size == 0:
            return False
        if self._size + record_bytes > self._max_bytes:
            return True
        return self._count + 1 > self._max_records

    def append(self, blob: bytes) -> int:
        """追加一条已编码记录，返回段内字节偏移。

        Args:
            blob: 完整记录字节。

        Returns:
            int: 本条记录在段内的起始偏移。
        """
        position = self._size
        self._write(blob)
        self._size += len(blob)
        self._count += 1
        return position

    def scan(self, *, verify_crc: bool = True) -> DelayScan:
        """从段首顺序扫描到段尾，给出全部完整记录与损坏位置。

        Args:
            verify_crc: 是否校验 CRC（重建传 True；只补结构信息时传 False）。

        Returns:
            DelayScan: 记录、完整区间、坏记录起点。

        Raises:
            UnsupportedRecordVersionError: 记录版本高于实现支持（冒泡拒绝加载，不截断）。
        """
        entries: list[DelayEntry] = []
        position = 0
        last_crc = 0
        corrupted: int | None = None
        size = self._size
        with self._path.open("rb") as handle:
            while position < size:
                handle.seek(position)
                head = self._emit_read(handle, RECORD_FRAME_BYTES)
                try:
                    frame = read_frame(head)
                    total = 4 + frame.length
                    if position + total > size:
                        raise CorruptedRecordError(f"记录不完整：{position}+{total} > {size}")
                    body = self._emit_read(handle, total - RECORD_FRAME_BYTES)
                    if len(body) != total - RECORD_FRAME_BYTES:  # pragma: no cover - 上一行已判过界
                        raise CorruptedRecordError("记录体不完整")
                    header, payload = _parse(head + body, header_len=frame.header_len, verify_crc=verify_crc)
                except CorruptedRecordError:
                    corrupted = position
                    break
                entries.append(DelayEntry(position=position, header=header, payload=payload))
                last_crc = frame.crc
                position += total
        return DelayScan(
            entries=tuple(entries),
            complete_bytes=position,
            corrupted_position=corrupted,
            last_crc=last_crc,
        )

    def truncate(self, *, size: int) -> None:
        """把段文件截断到给定字节数（末段崩后恢复用；结构级操作，不做取证）。

        Args:
            size: 保留的字节数（须不超过当前大小）。
        """
        if size >= self._size:
            return
        self.close()
        with self._path.open("r+b") as handle:
            handle.truncate(size)
        self._size = size

    def isolate_tail(self, *, keep_bytes: int, timestamp: str) -> Path:
        """把 `[keep_bytes, 段尾)` 的字节另存为取证文件（末段截断前调用）。

        **为什么末段也要留证**：截断丢掉的是「崩溃时写了一半的记录」与坏记录之后的疑似数据，
        留一份副本才能在事后判断「是写一半还是介质坏」——与 02-2 对 `.log` 的处置同口径。

        Args:
            keep_bytes: 保留区间的字节长度（坏记录之前）。
            timestamp: 取证文件后缀里的时间戳（由调用方给，便于确定性用例）。

        Returns:
            Path: 取证文件路径。
        """
        target = self._partition_dir / f"{self._seq:020d}{DELAY_CORRUPT_INFIX}{timestamp}"
        with self._path.open("rb") as source, target.open("wb") as sink:
            source.seek(keep_bytes)
            remaining = self._size - keep_bytes
            while remaining > 0:
                chunk = source.read(min(remaining, 1_048_576))
                if not chunk:  # pragma: no cover - seek 与 size 一致，读不满只可能是并发截断
                    break
                sink.write(chunk)
                remaining -= len(chunk)
        return target

    def isolate(self, *, timestamp: str) -> Path:
        """把整段改名隔离（非末段损坏用），返回副本路径。

        **为什么整段而不是逐条跳过**：变长记录一旦错位就无法可靠重同步，跳过会把垃圾当记录读出；
        整段隔离至少保证「读出来的都是校验过的」。

        Args:
            timestamp: 副本文件名里的时间戳（由调用方给，便于确定性用例）。

        Returns:
            Path: 隔离副本路径。
        """
        self.close()
        target = self._partition_dir / f"{self._seq:020d}{DELAY_CORRUPT_INFIX}{timestamp}"
        self._path.replace(target)
        self._size = 0
        self._count = 0
        return target

    def prune_corrupts(self, *, limit: int) -> int:
        """按文件名（含时间戳）删最早的隔离副本，只保留最近 `limit` 份。

        Args:
            limit: 保留份数。

        Returns:
            int: 删除的份数。
        """
        prefix = f"{self._seq:020d}{DELAY_CORRUPT_INFIX}"
        candidates = sorted(path for path in self._partition_dir.iterdir() if path.name.startswith(prefix))
        removed = 0
        for path in candidates[: max(len(candidates) - limit, 0)]:
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    def fsync(self) -> None:
        """把当前写句柄的内容刷到磁盘（无写句柄即无操作）。"""
        if self._write_fd is not None:
            started = time.perf_counter()
            os.fsync(self._write_fd)
            with contextlib.suppress(Exception):
                get_traffic_metrics().record_fsync(
                    scope="delay",
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                )

    def close(self) -> None:
        """关闭写句柄（幂等；读路径不持句柄）。"""
        if self._write_fd is not None:
            os.close(self._write_fd)
            self._write_fd = None

    def delete(self) -> None:
        """整段删除（幂等：段内有效条目归零后由存储层调用）。"""
        self.close()
        self._path.unlink(missing_ok=True)
        self._size = 0
        self._count = 0

    def _write(self, blob: bytes) -> None:
        """追加写 `.delay`（持写句柄；短写循环补齐）。

        Args:
            blob: 完整记录字节。
        """
        if self._write_fd is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._write_fd = os.open(self._path, _OPEN_MODE, 0o644)
        written = 0
        while written < len(blob):
            written += os.write(self._write_fd, blob[written:])

    def _emit_read(self, handle: IO[bytes], amount: int) -> bytes:
        """读一段字节并上报探针（恢复读量的唯一出口）。

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


def _parse(blob: bytes, *, header_len: int, verify_crc: bool) -> tuple[RecordHeader, bytes]:
    """解析一条完整记录（可选校验 CRC）。

    Args:
        blob: 从记录起点开始的完整字节。
        header_len: 头字节数。
        verify_crc: 是否校验 CRC。

    Returns:
        tuple[RecordHeader, bytes]: 头 / 载荷。

    Raises:
        CorruptedRecordError: CRC 不符或头无法解析。
    """
    if verify_crc:
        parsed = decode_record(blob)
        return parsed.header, parsed.payload
    return decode_header_blob(blob[RECORD_FRAME_BYTES : RECORD_FRAME_BYTES + header_len]), blob[
        RECORD_FRAME_BYTES + header_len :
    ]
