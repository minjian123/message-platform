"""协议层包出口（任务 03-2）：帧线格式、header 编解码与帧契约。

分层对应架构 05 §2：`frame` 是「消息层」（帧结构），`codec` 是「编码层」（header 的 MessagePack），
`contract` 是对外承诺的机器可读描述。序列化语义（payload 的 `content_type` / 压缩）归 03-3，
本包不定义 payload 内容。
"""

from __future__ import annotations

from bms_msg.protocol.codec import decode_header, encode_header
from bms_msg.protocol.contract import FRAME_CONTRACT_KEY, frame_contract
from bms_msg.protocol.frame import (
    FRAME_CONTENT_TYPE,
    FRAME_HEADER_BYTES,
    FRAME_MAGIC,
    MAX_FRAME_BYTES,
    MAX_FRAME_HEADER_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
    PROTOCOL_MAJOR,
    PROTOCOL_MINOR,
    PROTOCOL_VERSION,
    Frame,
    FrameBatch,
    FrameFlags,
    FrameType,
    decode_frame,
    decode_frames,
    encode_frame,
)

__all__ = [
    "FRAME_CONTENT_TYPE",
    "FRAME_CONTRACT_KEY",
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
    "decode_header",
    "encode_frame",
    "encode_header",
    "frame_contract",
]
