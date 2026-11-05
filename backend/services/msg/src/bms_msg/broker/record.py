"""记录编解码（02-1 详细设计 §5）：`[length][crc][header_len][header][payload]`。

四条结构性口径：

1. **头体分离**：头是 MessagePack map，载荷是原始字节——broker 只解头做路由，**永不解析载荷**
   （零拷贝前提）。因此本模块只提供「解析头」与「解析整条」两种入口，前者给顺序扫描用。
2. **`length` 不含自身**：`length = 4 + 4 + header_len + len(payload)`，记录总长 = `4 + length`；
   这样顺序扫描拿到的下一位置就是 `当前位置 + 4 + length`，不必再减。
3. **CRC 覆盖「头长字段 + 头 + 载荷」**：只校头或只校载荷都会漏检——头长被改会让解析器读到错位的
   「头」，比载荷损坏更危险（解析出的是合法但错误的字段）。
4. **版本只增不删**：解码一律按键取缺省，新增字段不需要迁移旧记录；读到高于实现支持的
   `format_version` 时**拒绝解析**（抛错），不做「猜着读」——那会把新格式当垃圾数据。
"""

import struct
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast

import msgpack
from bms_core.core.objects import BaseValueObject

from bms_msg.core.config import (
    MAX_RECORD_BYTES,
    MAX_RECORD_HEADER_BYTES,
    RECORD_FORMAT_VERSION,
    RECORD_FRAME_BYTES,
)

__all__ = [
    "CorruptedRecordError",
    "ParsedRecord",
    "RecordFrame",
    "RecordHeader",
    "UnsupportedRecordVersionError",
    "decode_header_blob",
    "decode_record",
    "from_epoch_us",
    "parse_header",
    "read_frame",
    "record_total_bytes",
    "to_epoch_us",
]

_PACK = cast("Callable[..., bytes]", msgpack.packb)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
"""编码入口（msgpack 无类型存根，按既有 `protocol/codec.py` 同法收窄）。"""

_UNPACK = cast("Callable[..., object]", msgpack.unpackb)  # pyright: ignore[reportUnknownMemberType]
"""解码入口（同上）。"""

_UNPACK_ERRORS = (msgpack.exceptions.UnpackException, ValueError, TypeError)
"""解码可能抛出的异常（格式错误 / 非字节入参）。"""

_FRAME: Final = struct.Struct(">III")
"""记录框架：`length` / `crc` / `header_len`，各 4 字节大端。"""

_U32: Final = struct.Struct(">I")
"""单个 4 字节大端无符号整数（CRC 计算需要单独的头长字节）。"""

_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
"""纪元时刻（微秒整数与 `datetime` 互转的基准）。"""

_MIN_LENGTH: Final = 8
"""`length` 最小值（`crc` + `header_len` 两个字段）。"""


@dataclass(frozen=True)
class RecordFrame(BaseValueObject):
    """记录框架三元组（`length` / `crc` / `header_len`）。"""

    length: int
    """`length` 字段（不含自身 4 字节）。"""

    crc: int
    """CRC32（覆盖头长字段 + 头 + 载荷）。"""

    header_len: int
    """头字节数。"""


def read_frame(buf: bytes) -> RecordFrame:
    """解析并校验 12 字节框架（顺序扫描与读取路径共用一处判据）。

    Args:
        buf: 至少 12 字节的缓冲（从记录起点开始）。

    Returns:
        RecordFrame: 框架三元组。

    Raises:
        CorruptedRecordError: 缓冲不足 / 长度非法 / 头长非法。
    """
    if len(buf) < RECORD_FRAME_BYTES:
        raise CorruptedRecordError(f"记录框架不足 {RECORD_FRAME_BYTES} 字节（实际 {len(buf)} 字节）")
    length, crc, header_len = _FRAME.unpack_from(buf, 0)
    if length < _MIN_LENGTH or length > MAX_RECORD_BYTES:
        raise CorruptedRecordError(f"记录长度非法：{length}")
    if header_len > length - _MIN_LENGTH:
        raise CorruptedRecordError(f"记录头长非法：{header_len} > {length - _MIN_LENGTH}")
    return RecordFrame(length=length, crc=crc, header_len=header_len)


def decode_header_blob(blob: bytes) -> RecordHeader:
    """解码头字节（顺序扫描只读头、不读载荷时使用）。

    Args:
        blob: MessagePack 头字节。

    Returns:
        RecordHeader: 记录头。

    Raises:
        CorruptedRecordError: 头不是合法 map / 版本不受支持 / 字段类型不符。
    """
    return _unpack_header(blob)


class UnsupportedRecordVersionError(RuntimeError):
    """记录格式版本高于实现支持（拒绝加载，不做「猜着读」）。

    与 `CorruptedRecordError` 分开的原因：损坏可以截断（尾部），版本不支持不能截断——
    截掉等于把新版本写的数据当垃圾丢掉，必须先升级实现。
    """


class CorruptedRecordError(RuntimeError):
    """记录损坏（长度非法 / CRC 不符 / 头无法解析）。

    程序性错误：读取期不静默跳过（跳过等于丢消息且无痕迹），由 02-2 的启动恢复消除。
    """


@dataclass(frozen=True)
class RecordHeader(BaseValueObject):
    """一条记录的完整头（写入与读取的唯一字段口径）。"""

    format_version: int
    """记录格式版本（写入写当前值；读取高于实现即拒绝）。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    tenant_id: int
    """租户主键（按租户统计与复制比对用）。"""

    message_id: str
    """消息身份（重投 / 转投 / 重放都不变）。"""

    log_id: str
    """记录身份（发布分配、延迟转投沿用、重放新分配）。"""

    key: str | None
    """分区键（可空）。"""

    trace_id: str | None
    """链路标识（可空）。"""

    published_at_us: int
    """发布时间（epoch 微秒，UTC）：用整数而非 `datetime`，避免 MessagePack 时间扩展的类型歧义。"""

    headers: tuple[tuple[str, str], ...]
    """结构化头（有序键值对；tuple 而非 dict，保证值语义可哈希且不可变）。"""

    content_type: str
    """消息体内容类型（03-3：三出口回显，必须随记录落盘）。"""

    compression: str
    """压缩算法（`none` 表示未压缩）。"""

    uncompressed_len: int | None
    """解压后字节数（生产者可选声明，broker 不校验）。"""

    epoch: int = 0
    """leader 任期（02-4 / 02-5 预留，本期恒 0）。"""

    hw: int = 0
    """高水位（复制预留，本期恒 0）。"""

    replicas: tuple[int, ...] = ()
    """副本位点（复制预留，本期恒空）。"""


@dataclass(frozen=True)
class ParsedRecord(BaseValueObject):
    """一条解析后的记录（头 + 载荷 + 记录总长）。"""

    header: RecordHeader
    """解析出的头。"""

    payload: bytes
    """业务字节（原样，不解压）。"""

    total_bytes: int
    """记录总长（含 4 字节 `length` 字段）。"""


def to_epoch_us(value: datetime) -> int:
    """带时区的时刻 → epoch 微秒（整数运算，不用 float 乘法以免丢 1 微秒）。

    Args:
        value: 时刻（必须带时区）。

    Returns:
        int: epoch 微秒。

    Raises:
        ValueError: 未带时区（无法确定绝对时刻）。
    """
    if value.tzinfo is None:
        raise ValueError("发布时间必须带时区（UTC）")
    delta = value.astimezone(UTC) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_epoch_us(value: int) -> datetime:
    """epoch 微秒 → 带 UTC 时区的时刻（`to_epoch_us` 的逆运算，逐微秒精确）。

    Args:
        value: epoch 微秒。

    Returns:
        datetime: UTC 时刻。
    """
    return _EPOCH + timedelta(microseconds=value)


def record_total_bytes(length: int) -> int:
    """由 `length` 字段算记录总长（`length` 不含自身 4 字节）。

    Args:
        length: `length` 字段值。

    Returns:
        int: 记录总长。
    """
    return 4 + length


def _pack_header(header: RecordHeader) -> bytes:
    """把头编码为 MessagePack（键名固定，只增不删）。

    Args:
        header: 记录头。

    Returns:
        bytes: 头字节。

    Raises:
        ValueError: 头超过 `MAX_RECORD_HEADER_BYTES`。
    """
    raw: dict[str, Any] = {
        "format_version": header.format_version,
        "topic_id": header.topic_id,
        "partition_no": header.partition_no,
        "tenant_id": header.tenant_id,
        "message_id": header.message_id,
        "log_id": header.log_id,
        "key": header.key,
        "trace_id": header.trace_id,
        "published_at": header.published_at_us,
        "headers": dict(header.headers),
        "content_type": header.content_type,
        "compression": header.compression,
        "uncompressed_len": header.uncompressed_len,
        "epoch": header.epoch,
        "hw": header.hw,
        "replicas": list(header.replicas),
    }
    packed = _PACK(raw, use_bin_type=True)
    if len(packed) > MAX_RECORD_HEADER_BYTES:
        raise ValueError(f"记录头超过上限 {MAX_RECORD_HEADER_BYTES} 字节（实际 {len(packed)} 字节）")
    return packed


def _unpack_header(blob: bytes) -> RecordHeader:
    """把 MessagePack 头解码为 `RecordHeader`（按键取缺省，只增不删）。

    Args:
        blob: 头字节。

    Returns:
        RecordHeader: 记录头。

    Raises:
        CorruptedRecordError: 头不是合法 map / 版本高于实现支持 / 必填字段类型不符。
    """
    try:
        raw = _UNPACK(blob, raw=False, strict_map_key=False)
    except _UNPACK_ERRORS as exc:  # 解析失败即数据损坏
        raise CorruptedRecordError(f"记录头无法解析：{exc}") from exc
    if not isinstance(raw, dict):
        raise CorruptedRecordError(f"记录头须为 map，实际 {type(raw).__name__}")
    data = cast("dict[str, Any]", raw)
    version = data.get("format_version", 0)
    if not isinstance(version, int):
        raise CorruptedRecordError(f"记录格式版本非法：{version!r}")
    if version > RECORD_FORMAT_VERSION:
        raise UnsupportedRecordVersionError(f"记录格式版本不受支持：{version} > {RECORD_FORMAT_VERSION}")
    headers_raw = data.get("headers")
    headers_map: dict[str, Any] = cast("dict[str, Any]", headers_raw) if isinstance(headers_raw, dict) else {}
    replicas_raw = data.get("replicas")
    replicas_list: list[Any] = cast("list[Any]", replicas_raw) if isinstance(replicas_raw, list) else []
    return RecordHeader(
        format_version=version,
        topic_id=int(data.get("topic_id", 0)),
        partition_no=int(data.get("partition_no", 0)),
        tenant_id=int(data.get("tenant_id", 0)),
        message_id=str(data.get("message_id", "")),
        log_id=str(data.get("log_id", "")),
        key=cast("str | None", data.get("key")),
        trace_id=cast("str | None", data.get("trace_id")),
        published_at_us=int(data.get("published_at", 0)),
        headers=tuple((str(name), str(value)) for name, value in headers_map.items()),
        content_type=str(data.get("content_type", "")),
        compression=str(data.get("compression", "")),
        uncompressed_len=cast("int | None", data.get("uncompressed_len")),
        epoch=int(data.get("epoch", 0)),
        hw=int(data.get("hw", 0)),
        replicas=tuple(int(item) for item in replicas_list),
    )


def _split(buf: bytes, *, verify_crc: bool) -> tuple[RecordHeader, bytes, int]:
    """解析一条记录的公共部分（可选校验 CRC）。

    Args:
        buf: 至少覆盖整条记录的字节缓冲（从记录起点开始）。
        verify_crc: 是否校验 CRC（顺序扫描尾部时关掉，读取路径打开）。

    Returns:
        tuple[RecordHeader, bytes, int]: 头 / 载荷 / 记录总长。

    Raises:
        CorruptedRecordError: 长度非法 / 缓冲不足 / CRC 不符 / 头无法解析。
    """
    if len(buf) < RECORD_FRAME_BYTES:
        raise CorruptedRecordError(f"记录框架不足 {RECORD_FRAME_BYTES} 字节（实际 {len(buf)} 字节）")
    frame = read_frame(buf)
    total = 4 + frame.length
    if len(buf) < total:
        raise CorruptedRecordError(f"记录不完整：需要 {total} 字节，只有 {len(buf)} 字节")
    header_end = RECORD_FRAME_BYTES + frame.header_len
    header_blob = buf[RECORD_FRAME_BYTES:header_end]
    payload = buf[header_end:total]
    if verify_crc:
        actual = zlib.crc32(buf[8:RECORD_FRAME_BYTES])
        actual = zlib.crc32(header_blob, actual)
        actual = zlib.crc32(payload, actual)
        if actual & 0xFFFFFFFF != frame.crc:
            raise CorruptedRecordError(f"记录 CRC 不符：期望 {frame.crc:#010x}，实际 {actual & 0xFFFFFFFF:#010x}")
    return _unpack_header(header_blob), payload, total


def parse_header(buf: bytes) -> tuple[RecordHeader, int]:
    """只解析头（不校验 CRC、不复制载荷），供顺序扫描与尾部判定使用。

    Args:
        buf: 从记录起点开始的字节缓冲（至少覆盖整条记录）。

    Returns:
        tuple[RecordHeader, int]: 头 / 记录总长。

    Raises:
        CorruptedRecordError: 长度非法或头无法解析。
    """
    header, _payload, total = _split(buf, verify_crc=False)
    return header, total


def decode_record(buf: bytes) -> ParsedRecord:
    """完整解析一条记录（校验 CRC），供读取路径与 `read_bytes` 使用。

    Args:
        buf: 从记录起点开始的字节缓冲（至少覆盖整条记录）。

    Returns:
        ParsedRecord: 头 / 载荷 / 记录总长。

    Raises:
        CorruptedRecordError: 长度非法 / CRC 不符 / 头无法解析。
    """
    header, payload, total = _split(buf, verify_crc=True)
    return ParsedRecord(header=header, payload=payload, total_bytes=total)


def encode_record(*, header: RecordHeader, payload: bytes) -> bytes:
    """编码一条记录（框架 + 头 + 载荷），并校验长度上限。

    Args:
        header: 记录头。
        payload: 业务字节（原样写入，不解压）。

    Returns:
        bytes: 完整记录字节。

    Raises:
        ValueError: 头或记录总长超过上限。
    """
    head = _pack_header(header)
    length = 8 + len(head) + len(payload)
    if length > MAX_RECORD_BYTES:
        raise ValueError(f"记录总长超过上限 {MAX_RECORD_BYTES} 字节（实际 {length + 4} 字节）")
    crc = zlib.crc32(_U32.pack(len(head)))
    crc = zlib.crc32(head, crc)
    crc = zlib.crc32(payload, crc)
    return _FRAME.pack(length, crc & 0xFFFFFFFF, len(head)) + head + payload
