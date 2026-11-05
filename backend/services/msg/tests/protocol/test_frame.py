"""帧线格式（任务 03-2；Kiwi 2421）：定宽头 / 长度校验 / 版本判定 / 多帧与尾包。

口径见 03-2 详细设计 §4.1~§4.7；本文件只验结构，不碰业务字段（业务字段由各端点模型负责）。
"""

from __future__ import annotations

import struct

import pytest

from bms_msg.errors import MsgFrameInvalidError, MsgPayloadTooLargeError, MsgVersionUnsupportedError
from bms_msg.protocol.frame import (
    FRAME_HEADER_BYTES,
    FRAME_MAGIC,
    MAX_FRAME_HEADER_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
    FrameFlags,
    FrameType,
    decode_frame,
    decode_frames,
    encode_frame,
)

pytestmark = pytest.mark.kiwi_id(2421)

_HEADER = struct.Struct(">2sBBBHII")


def _raw(
    *,
    magic: bytes = FRAME_MAGIC,
    major: int = 1,
    minor: int = 0,
    frame_type: int = 1,
    flags: int = 0,
    header: bytes = b"\x80",
    header_len: int | None = None,
    payload: bytes = b"",
    payload_len: int | None = None,
) -> bytes:
    """手工拼一帧（绕开编码器，构造非法 / 超限等边界）。

    Args:
        magic: 协议标识。
        major: 主版本。
        minor: 次版本。
        frame_type: 类型编号。
        flags: 标志位。
        header: 头字节。
        header_len: 声明头长（None 取实际长度）。
        payload: 体字节。
        payload_len: 声明体长（None 取实际长度）。

    Returns:
        bytes: 完整帧字节。
    """
    declared_header = len(header) if header_len is None else header_len
    declared_payload = len(payload) if payload_len is None else payload_len
    prefix = _HEADER.pack(magic, major, minor, frame_type, flags, declared_header, declared_payload)
    return prefix + header + payload


def test_frame_header_is_15_bytes_big_endian() -> None:
    """定宽头 15B 且全字段大端：逐字节核对 magic / 版本 / 类型 / 标志 / 两个长度。"""
    header = {"request_id": "r1"}
    raw = encode_frame(frame_type=int(FrameType.PUBLISH), flags=FrameFlags.NEED_RESPONSE, header=header, payload=b"abc")

    magic, major, minor, frame_type, flags, header_len, payload_len = _HEADER.unpack_from(raw, 0)

    assert magic == b"MS"
    assert (major, minor) == (1, 0)
    assert frame_type == 1
    assert flags == int(FrameFlags.NEED_RESPONSE)
    assert payload_len == 3
    assert len(raw) == FRAME_HEADER_BYTES + header_len + payload_len


def test_roundtrip_keeps_header_and_payload() -> None:
    """编解码往返：头键值一致、payload 是原字节切片。"""
    raw = encode_frame(
        frame_type=int(FrameType.PUBLISH),
        header={"request_id": "r1", "namespace": "msg", "topic": "audit.operation"},
        payload=b"hello",
    )

    frame = decode_frame(raw)

    assert frame.frame_type == int(FrameType.PUBLISH)
    assert dict(frame.header.items()) == {"request_id": "r1", "namespace": "msg", "topic": "audit.operation"}
    assert bytes(frame.payload) == b"hello"
    assert frame.version == (1, 0)


def test_decode_frames_supports_concatenation() -> None:
    """一条缓冲里拼多帧时逐帧滑走（03-5 批量推送的布局前提）。"""
    first = encode_frame(frame_type=int(FrameType.PUBLISH), header={"request_id": "a"}, payload=b"a")
    second = encode_frame(frame_type=int(FrameType.ACK), header={"request_id": "b"})

    batch = decode_frames(first + second)

    assert [frame.frame_type for frame in batch.frames] == [int(FrameType.PUBLISH), int(FrameType.ACK)]
    assert batch.consumed == len(first) + len(second)


def test_decode_frames_keeps_incomplete_tail() -> None:
    """不完整尾包不消费（流式解码从 `consumed` 继续拼）。"""
    whole = encode_frame(frame_type=int(FrameType.PUBLISH), header={"request_id": "a"}, payload=b"abcdef")
    partial = whole[:-2]

    batch = decode_frames(partial)

    assert batch.frames == []
    assert batch.consumed == 0


def test_short_body_rejected() -> None:
    """不足 15B 的请求体即「不是一帧」。"""
    with pytest.raises(MsgFrameInvalidError):
        decode_frame(b"MS\x01\x00")


def test_magic_mismatch_rejected() -> None:
    """magic 不符：非本协议流量一次比较即拒。"""
    with pytest.raises(MsgFrameInvalidError):
        decode_frame(_raw(magic=b"XX"))


def test_length_mismatch_rejected() -> None:
    """声明长度与实送字节不一致（截断）即拒，不留歧义。"""
    with pytest.raises(MsgFrameInvalidError):
        decode_frame(_raw(payload=b"12345678", payload_len=16))


def test_trailing_bytes_rejected() -> None:
    """单帧入口不接受尾部多余字节（拼帧请走 `decode_frames`）。"""
    whole = encode_frame(frame_type=int(FrameType.ACK), header={"request_id": "a"})

    with pytest.raises(MsgFrameInvalidError):
        decode_frame(whole + b"\x00")


def test_header_overflow_rejected() -> None:
    """header 声明超过 64 KiB 即 `350006`（先判长度，不按声明值分配）。"""
    with pytest.raises(MsgPayloadTooLargeError) as excinfo:
        decode_frame(_raw(header_len=MAX_FRAME_HEADER_BYTES + 1))

    assert excinfo.value.code == 350006


def test_payload_overflow_rejected() -> None:
    """payload 声明超过 1 MiB 即 `350006`。"""
    with pytest.raises(MsgPayloadTooLargeError) as excinfo:
        decode_frame(_raw(payload_len=MAX_FRAME_PAYLOAD_BYTES + 1))

    assert excinfo.value.code == 350006


def test_encode_rejects_oversize_payload() -> None:
    """编码侧同样守住上限（不把超限请求发出去）。"""
    with pytest.raises(MsgPayloadTooLargeError):
        encode_frame(
            frame_type=int(FrameType.PUBLISH),
            header={"request_id": "a"},
            payload=b"\x00" * (MAX_FRAME_PAYLOAD_BYTES + 1),
        )


def test_major_version_unsupported() -> None:
    """主版本不等即 `350019`（服务端只认同一主版本）。"""
    with pytest.raises(MsgVersionUnsupportedError) as excinfo:
        decode_frame(_raw(major=2))

    assert excinfo.value.code == 350019


@pytest.mark.parametrize("minor", [0, 9, 255])
def test_minor_version_any_accepted(minor: int) -> None:
    """次版本任高任低都接受（只增可选字段）。

    Args:
        minor: 次版本号。
    """
    frame = decode_frame(_raw(minor=minor, header=b"\x80"))

    assert frame.version == (1, minor)


def test_unknown_flag_bits_ignored() -> None:
    """未定义标志位不报错（为后续留位）。"""
    frame = decode_frame(_raw(flags=0xFFFF))

    assert frame.flags & FrameFlags.NEED_RESPONSE


def test_encode_rejects_bad_version() -> None:
    """版本超出单字节无法编码。"""
    with pytest.raises(MsgFrameInvalidError):
        encode_frame(frame_type=int(FrameType.ACK), header={"request_id": "a"}, version=(256, 0))


def test_encode_rejects_non_str_header_key() -> None:
    """头键必须是字符串（数字键跨语言表示不一致）。"""
    with pytest.raises(MsgFrameInvalidError):
        encode_frame(frame_type=int(FrameType.ACK), header={1: "a"})  # type: ignore[dict-item]
