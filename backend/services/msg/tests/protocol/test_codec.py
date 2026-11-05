"""header 编解码（任务 03-2；Kiwi 2421）：Map 强约束 / 中文与二进制 / 非法形态。

header 必须解成 `Map[str, Any]`：键不是字符串就分不清「格式坏了」与「键类型非法」，
错误只能含糊成一句话；本文件把三类形态固化成断言。
"""

from __future__ import annotations

from typing import cast

import msgpack
import pytest

from bms_msg.errors import MsgFrameInvalidError
from bms_msg.protocol.codec import decode_header, encode_header

pytestmark = pytest.mark.kiwi_id(2421)


def _pack(value: object) -> bytes:
    """编码任意值为 MessagePack 字节（test 侧只用于造非法输入）。

    Args:
        value: 待编码值。

    Returns:
        bytes: 编码结果。
    """
    return cast("bytes", msgpack.packb(value, use_bin_type=True))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]


def test_roundtrip_keeps_values() -> None:
    """往返保真：中文不转义、嵌套结构不变、二进制单独编码。"""
    header = {"request_id": "r1", "topic": "审计.操作", "headers": {"k": "v"}, "blob": b"\x00\xff"}

    decoded = decode_header(encode_header(header))

    assert dict(decoded.items()) == header


def test_empty_header_allowed() -> None:
    """空 Map 合法（结构上允许，业务必填由模型判）。"""
    assert dict(decode_header(encode_header({})).items()) == {}


def test_non_map_rejected() -> None:
    """头是数组 / 字符串都不合法（必须是 Map）。"""
    for value in (["a"], "text", 42):
        with pytest.raises(MsgFrameInvalidError):
            decode_header(_pack(value))


def test_non_str_key_rejected() -> None:
    """键必须为字符串。"""
    with pytest.raises(MsgFrameInvalidError):
        decode_header(_pack({1: "a"}))


def test_invalid_bytes_rejected() -> None:
    """不是合法 MessagePack 即拒（含截断与多余字节）。"""
    with pytest.raises(MsgFrameInvalidError):
        decode_header(b"\xc1")

    with pytest.raises(MsgFrameInvalidError):
        decode_header(_pack({"a": 1}) + b"\x00")


def test_encode_rejects_non_str_key() -> None:
    """编码侧同样拒绝非字符串键。"""
    with pytest.raises(MsgFrameInvalidError):
        encode_header({1: "a"})  # type: ignore[dict-item]
