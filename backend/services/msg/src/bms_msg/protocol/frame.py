"""帧线格式：定宽头、类型 / 标志位、编解码与结构校验（任务 03-2）。

**布局（15B 定宽头 + 变长头 + 变长体，全字段大端）**：

```text
magic(2) | ver.major(1) ver.minor(1) | type(1) | flags(2) | header_len(4) | payload_len(4)
```

- `magic`：协议识别，非本协议流量一次比较即拒；
- `ver`：主版本决定兼容性（不等即 `350019`），次版本只增可选字段（任高任低都接受）；
- `header_len` / `payload_len`：自描述长度，一条 stream 上可连续拼多帧（03-5 批量推送不用改布局）；
- 上限：header ≤ 64 KiB、payload ≤ 1 MiB；**先判长度再读内容**，不按声明长度预分配。

边界：本模块**不碰**业务语义（字段含义由各端点的 Pydantic 模型定义），也不碰 payload 的编码
（`content_type` / 压缩归 03-3）——它只回答「这一串字节是不是一帧、帧头说了什么」。
"""

from __future__ import annotations

import struct
from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum, IntFlag
from typing import Any

from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.errors import MsgFrameInvalidError, MsgPayloadTooLargeError, MsgVersionUnsupportedError
from bms_msg.protocol.codec import decode_header, encode_header

__all__ = [
    "FRAME_CONTENT_TYPE",
    "FRAME_HEADER_BYTES",
    "FRAME_MAGIC",
    "MAX_FRAME_BYTES",
    "MAX_FRAME_HEADER_BYTES",
    "MAX_FRAME_PAYLOAD_BYTES",
    "PROTOCOL_MAJOR",
    "PROTOCOL_MINOR",
    "PROTOCOL_VERSION",
    "Frame",
    "FrameBatch",
    "FrameFlags",
    "FrameType",
    "decode_frame",
    "decode_frames",
    "encode_frame",
]

FRAME_MAGIC = b"MS"
"""协议标识（ASCII `MS`，与产品名 msg 一致，抓包可读）。"""

PROTOCOL_MAJOR = 1
"""当前主版本（不等即不兼容：`350019` + 结束该 stream）。"""

PROTOCOL_MINOR = 0
"""当前次版本（只增可选字段；服务端接受任意次版本）。"""

PROTOCOL_VERSION = f"{PROTOCOL_MAJOR}.{PROTOCOL_MINOR}"
"""文本版本（日志 / 响应帧 / 连接元数据统一用它，避免各处自拼）。"""

FRAME_HEADER_BYTES = 15
"""定宽帧头字节数（2 + 1 + 1 + 1 + 2 + 4 + 4）。"""

FRAME_CONTENT_TYPE = "application/x-msg-frame"
"""帧形态的媒体类型（`Content-Type` 协商的唯一键：缺省 / JSON 一律走 JSON 路径）。"""

MAX_FRAME_HEADER_BYTES = 64 * 1024
"""header 上限（64 KiB）：header 只放路由与元数据，不该承载业务大字段。"""

MAX_FRAME_PAYLOAD_BYTES = 1024 * 1024
"""单帧 payload 上限（1 MiB，与「单条消息 1 MiB」同值，超限 `350006`）。"""

MAX_FRAME_BYTES = FRAME_HEADER_BYTES + MAX_FRAME_HEADER_BYTES + MAX_FRAME_PAYLOAD_BYTES
"""整帧上限（由前两者派生，不单独判）。"""

_HEADER = struct.Struct(">2sBBBHII")
"""定宽头结构：magic / major / minor / type / flags / header_len / payload_len（大端）。"""

_UINT32_MAX = 0xFFFFFFFF
"""长度字段上限（4B 无符号；超出无法编码）。"""


class FrameType(IntEnum):
    """帧类型编号（分组区间：数据面 1~15 / 控制面 16~31 / admin 32~47 / rpc 48~63）。

    分组区间而非连续编号：`admin` / `rpc` 的「预留」必须是可扩展的一段，否则后续新增预留类型
    只能插空，容易出现两个任务抢同一个号。
    """

    PUBLISH = 1
    """发布一条消息（payload = 消息体原始字节）。"""

    CONSUME = 2
    """拉取一批消息（无 payload）。"""

    ACK = 3
    """确认一条消息（无 payload）。"""

    SUBSCRIBE = 4
    """订阅长驻流（只登记编号：帧式推送归 03-5）。"""

    NACK = 5
    """拒绝一条消息并给出原因（03-3：转死信 + 位点前移；payload 必须为空）。"""

    ERROR = 16
    """错误帧（响应帧，携带 `code` / `layer` / `retryable`）。"""

    HEARTBEAT = 17
    """心跳（只登记编号：端点与超时判定归 03-4）。"""


class FrameFlags(IntFlag):
    """标志位（16 位；未定义位一律忽略，为后续留位且不破坏兼容）。"""

    COMPRESSED = 1 << 0
    """payload 已压缩（算法由 header 的 `compression` 给；03-3 启用，broker 只透传不解压）。"""

    END_OF_STREAM = 1 << 1
    """本帧是该 stream 的最后一帧（单帧请求恒为最后帧，本任务不置位）。"""

    NEED_RESPONSE = 1 << 2
    """请求方等一个响应帧（未知帧类型据此决定是否回 `error`）。"""

    BATCH = 1 << 3
    """payload 为多条消息的批（03-5）。"""

    RESPONSE = 1 << 4
    """本帧是响应帧（与请求同 `type`）。"""


@dataclass(frozen=True)
class Frame:
    """一帧解码结果（`payload` 是对原缓冲的切片视图，不复制）。"""

    frame_type: int
    """帧类型编号（保留原始整数：未知类型也要能表达）。"""

    flags: FrameFlags
    """标志位。"""

    version: tuple[int, int]
    """协议版本 `(major, minor)`。"""

    header: Mapping[str, Any]
    """MessagePack 头（key 必为 str；未知键由调用方忽略）。"""

    payload: memoryview
    """消息体原始字节视图（长度 = `payload_len`）。"""


@dataclass(frozen=True)
class FrameBatch:
    """一次解码的结果（`consumed` 为完整帧占用的字节数，剩余即不完整尾包）。"""

    frames: ConcurrentStableList[Frame]
    """完整帧（按出现顺序）。"""

    consumed: int
    """已消费字节数（流式解码时从该位置继续）。"""


def _check_lengths(header_len: int, payload_len: int) -> None:
    """校验两个长度字段（在任何读取之前调用）。

    Args:
        header_len: header 声明长度。
        payload_len: payload 声明长度。

    Raises:
        MsgPayloadTooLargeError: 任一长度超过上限（`350006`）。
    """
    if header_len > MAX_FRAME_HEADER_BYTES:
        raise MsgPayloadTooLargeError(f"帧头超过上限 {MAX_FRAME_HEADER_BYTES} 字节（声明 {header_len}）")
    if payload_len > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"消息体超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（声明 {payload_len}）")


def encode_frame(
    *,
    frame_type: int,
    header: Mapping[str, Any],
    flags: FrameFlags | int = 0,
    payload: bytes | memoryview = b"",
    version: tuple[int, int] = (PROTOCOL_MAJOR, PROTOCOL_MINOR),
) -> bytes:
    """编码一帧（只校验结构上限，不校验业务字段）。

    Args:
        frame_type: 帧类型编号。
        header: 头（MessagePack Map；键须为字符串）。
        flags: 标志位。
        payload: 消息体原始字节。
        version: 协议版本 `(major, minor)`。

    Returns:
        bytes: 完整帧字节。

    Raises:
        MsgPayloadTooLargeError: header / payload 超过上限（`350006`）。
        MsgFrameInvalidError: 版本或长度无法编码（`350018`）。
    """
    header_bytes = encode_header(header)
    payload_bytes = bytes(payload)
    _check_lengths(len(header_bytes), len(payload_bytes))
    major, minor = version
    if not (0 <= major <= 0xFF and 0 <= minor <= 0xFF):
        raise MsgFrameInvalidError(f"协议版本超出单字节范围：{major}.{minor}")
    if len(header_bytes) > _UINT32_MAX or len(payload_bytes) > _UINT32_MAX:  # pragma: no cover - 上限已在上方拦住
        raise MsgFrameInvalidError("长度字段超出 uint32 范围")
    prefix = _HEADER.pack(FRAME_MAGIC, major, minor, frame_type, int(flags), len(header_bytes), len(payload_bytes))
    return prefix + header_bytes + payload_bytes


def decode_frames(data: bytes | memoryview) -> FrameBatch:
    """解码缓冲中的完整帧（不完整尾包保留给调用方）。

    逐帧滑走：只有长度字段校验通过且整帧都在缓冲内才解 header——因此「声明 4 GB 但只发 10 B」
    不会触发任何按声明长度的分配。

    Args:
        data: 待解码缓冲。

    Returns:
        FrameBatch: 完整帧与已消费字节数。

    Raises:
        MsgFrameInvalidError: magic 不符 / header 非法（`350018`）。
        MsgVersionUnsupportedError: 主版本不支持（`350019`）。
        MsgPayloadTooLargeError: header / payload 超限（`350006`）。
    """
    view = memoryview(data)
    frames: ConcurrentStableList[Frame] = ConcurrentStableList()
    offset = 0
    total = len(view)
    while total - offset >= FRAME_HEADER_BYTES:
        magic, major, minor, frame_type, flags, header_len, payload_len = _HEADER.unpack_from(view, offset)
        if magic != FRAME_MAGIC:
            raise MsgFrameInvalidError(f"magic 不符（期望 {FRAME_MAGIC!r}，实际 {magic!r}）")
        if major != PROTOCOL_MAJOR:
            raise MsgVersionUnsupportedError(f"协议主版本不支持：{major}.{minor}（服务端支持 {PROTOCOL_VERSION}）")
        _check_lengths(header_len, payload_len)
        end = offset + FRAME_HEADER_BYTES + header_len + payload_len
        if end > total:
            break
        header_start = offset + FRAME_HEADER_BYTES
        header = decode_header(view[header_start : header_start + header_len])
        frames.add(
            Frame(
                frame_type=frame_type,
                flags=FrameFlags(flags),
                version=(major, minor),
                header=header,
                payload=view[header_start + header_len : end],
            )
        )
        offset = end
    return FrameBatch(frames=frames, consumed=offset)


def decode_frame(data: bytes | memoryview) -> Frame:
    """解码「恰好一帧」（请求体形态；尾部多余字节同样拒绝）。

    Args:
        data: 请求体字节。

    Returns:
        Frame: 解码结果。

    Raises:
        MsgFrameInvalidError: 长度不足 / 多余字节 / 结构非法（`350018`）。
        MsgVersionUnsupportedError: 主版本不支持（`350019`）。
        MsgPayloadTooLargeError: header / payload 超限（`350006`）。
    """
    batch = decode_frames(data)
    if len(batch.frames) != 1 or batch.consumed != len(data):
        raise MsgFrameInvalidError(
            f"请求体须恰好是一帧（完整帧 {len(batch.frames)} 个，已消费 {batch.consumed} / {len(data)} 字节）"
        )
    return batch.frames[0]
