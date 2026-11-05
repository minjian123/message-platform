"""协议层包出口（任务 03-2 建包，03-3 扩 payload 口径）：帧线格式、header 编解码、帧契约与消息体口径。

分层对应架构 05 §2：`frame` 是「消息层」（帧结构），`codec` 是「编码层」（header 的 MessagePack），
`contract` 是对外承诺的机器可读描述，`payload` 是消息体的内容类型与压缩口径，`validation` 是
消费端的校验器端口（broker 不调用）。
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
from bms_msg.protocol.payload import (
    DEFAULT_CONTENT_TYPE,
    KNOWN_COMPRESSIONS,
    KNOWN_CONTENT_TYPES,
    ensure_compression_flag,
    normalize_compression,
    normalize_content_type,
)
from bms_msg.protocol.validation import PayloadValidationError, PayloadValidator, ValidatorRegistry

__all__ = [
    "DEFAULT_CONTENT_TYPE",
    "FRAME_CONTENT_TYPE",
    "FRAME_CONTRACT_KEY",
    "FRAME_HEADER_BYTES",
    "FRAME_MAGIC",
    "KNOWN_COMPRESSIONS",
    "KNOWN_CONTENT_TYPES",
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
    "PayloadValidationError",
    "PayloadValidator",
    "ValidatorRegistry",
    "decode_frame",
    "decode_frames",
    "decode_header",
    "encode_frame",
    "encode_header",
    "ensure_compression_flag",
    "frame_contract",
    "normalize_compression",
    "normalize_content_type",
]
