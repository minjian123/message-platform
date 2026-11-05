"""帧结果与错误帧装配（任务 03-2；Kiwi 2421）：头体分离 / 分段长度 / 码→层→重试。

`consume` 响应是「元数据进 header + 消息体进 payload」的分段布局：本文件按 `length` 逐段切回，
确认客户端拿到的字节与发布时一致（不 base64、不重排）。
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, classify, error_frame, result_frame
from bms_msg.protocol.frame import FrameFlags, FrameType, decode_frame
from bms_msg.schemas.message import AckResult, ConsumeResult, MessageItem, PublishResult

pytestmark = pytest.mark.kiwi_id(2421)

_NOW = datetime(2026, 11, 5, 8, 0, tzinfo=UTC)


def _item(*, payload: bytes, offset: int = 0) -> MessageItem:
    """构造一条投递消息（载荷按接口约定用 base64）。

    Args:
        payload: 原始消息体。
        offset: 分区内序号。

    Returns:
        MessageItem: 投递消息。
    """
    return MessageItem(
        message_id=f"m{offset}",
        topic="audit.operation",
        partition=0,
        offset=offset,
        key="k",
        payload=base64.b64encode(payload).decode("ascii"),
        headers=ConcurrentStableDict({"trace": "t1"}),
        trace_id="t1",
        delivery_count=1,
        published_at=_NOW,
    )


def _result(frame_type: int, result: object, request_id: str = "r1"):
    """编码并解码结果帧（返回解码后的帧，供逐字段断言）。

    Args:
        frame_type: 帧类型。
        result: 结果模型。
        request_id: 请求标识。

    Returns:
        解码后的帧。
    """
    return decode_frame(result_frame(frame_type=frame_type, request_id=request_id, result=result))


def test_publish_result_frame_carries_fields_in_header() -> None:
    """publish 结果：字段进 header、payload 为空、response 位已置。"""
    result = PublishResult(
        message_id="m1", log_id="l1", topic="audit.operation", partition=2, offset=17, deliver_at=None
    )

    frame = _result(int(FrameType.PUBLISH), result)

    assert frame.frame_type == int(FrameType.PUBLISH)
    assert frame.flags & FrameFlags.RESPONSE
    assert frame.header["request_id"] == "r1"
    assert frame.header["protocol_version"] == "1.0"
    assert (frame.header["partition"], frame.header["offset"]) == (2, 17)
    assert frame.header["deliver_at"] is None
    assert bytes(frame.payload) == b""


def test_delay_publish_result_frame_has_null_offset() -> None:
    """延迟发布：尚未进入 offset 空间，`offset` 为 null、`deliver_at` 为 ISO 文本。"""
    result = PublishResult(message_id="m1", log_id="l1", topic="t", partition=0, offset=None, deliver_at=_NOW)

    frame = _result(int(FrameType.PUBLISH), result)

    assert frame.header["offset"] is None
    assert frame.header["deliver_at"] == _NOW.isoformat()


def test_consume_result_frame_splits_payload_by_length() -> None:
    """consume 结果：元数据进 header，消息体按序拼接进 payload，按 `length` 可逐段切回。"""
    result = ConsumeResult(
        messages=ConcurrentStableList([_item(payload=b"abc", offset=0), _item(payload=b"hello", offset=1)])
    )

    frame = _result(int(FrameType.CONSUME), result)

    metas = frame.header["messages"]
    assert frame.header["count"] == 2
    assert [item["length"] for item in metas] == [3, 5]
    body = bytes(frame.payload)
    lengths = [item["length"] for item in metas]
    first = body[: lengths[0]]
    second = body[lengths[0] : lengths[0] + lengths[1]]
    assert (first, second) == (b"abc", b"hello")
    assert metas[0]["headers"] == {"trace": "t1"}
    assert metas[0]["published_at"] == _NOW.isoformat()


def test_consume_result_frame_without_messages() -> None:
    """空批次：count 为 0、payload 为空（客户端按 count 判定，不必解析 payload）。"""
    frame = _result(int(FrameType.CONSUME), ConsumeResult(messages=ConcurrentStableList()))

    assert frame.header["count"] == 0
    assert bytes(frame.payload) == b""


def test_ack_result_frame() -> None:
    """ack 结果：`committed_offset` 进 header、payload 为空。"""
    frame = _result(int(FrameType.ACK), AckResult(committed_offset=3))

    assert frame.header["committed_offset"] == 3
    assert bytes(frame.payload) == b""


def test_result_frame_rejects_mismatched_type() -> None:
    """帧类型与结果类型不匹配属装配错误（不是协议错误，直接抛 TypeError）。"""
    with pytest.raises(TypeError):
        result_frame(frame_type=int(FrameType.PUBLISH), request_id="r1", result=AckResult(committed_offset=1))


def test_error_frame_fields() -> None:
    """错误帧：字段齐全、response 位已置、payload 为空。"""
    frame = decode_frame(
        error_frame(code=350001, message="主题不存在", request_id="r1", detail={"k": "v"}, retry_after_ms=30)
    )

    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.flags & FrameFlags.RESPONSE
    assert frame.header["code"] == 350001
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is False
    assert frame.header["detail"] == {"k": "v"}
    assert frame.header["retry_after_ms"] == 30
    assert bytes(frame.payload) == b""


def test_error_frame_without_request_id() -> None:
    """解码前失败时没有 request_id（仍要能回错误帧，字段为 null）。"""
    frame = decode_frame(error_frame(code=350019, message="版本不支持"))

    assert frame.header["request_id"] is None


@pytest.mark.parametrize(
    ("code", "layer", "retryable"),
    [
        (350018, "protocol", False),
        (10001, "protocol", False),
        (350019, "transport", False),
        (350006, "transport", False),
        (350001, "business", False),
        (350015, "business", True),
        (350005, "business", True),
        (10006, "system", True),
        (10007, "system", True),
        (99999, "system", False),
    ],
)
def test_classify_table(code: int, layer: str, retryable: bool) -> None:
    """码 → 层 → 重试 的映射逐条固定（客户端据此决定「断开 / 改参 / 重试 / 放弃」）。

    Args:
        code: 错误码。
        layer: 期望层。
        retryable: 期望可重试性。
    """
    assert classify(code) == (layer, retryable)


def test_frame_media_type_constant() -> None:
    """响应媒体类型与请求协商键一致（客户端只认一个值）。"""
    assert FRAME_MEDIA_TYPE == "application/x-msg-frame"
