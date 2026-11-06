"""帧结果装配：成功结果帧与错误帧（任务 03-2）。

两条原则：

1. **码 → 层 → 重试**：错误帧不只给码，还给 `layer` 与 `retryable`——客户端据此决定「断开 / 改参 /
   重试 / 放弃」，不必维护一张隐式的码表。判定只在 `classify()` 一处，新增码必须在此登记。
2. **头体分离落到响应上**：`consume` 的元数据进 header、消息体按序进 payload，`length` 是切分表，
   客户端按切片取即可（不做整包重排，也不 base64）。
"""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.frame import FRAME_CONTENT_TYPE, PROTOCOL_VERSION, FrameFlags, FrameType, encode_frame

from bms_msg.errors import ErrorCategory
from bms_msg.schemas.message import AckResult, ConsumeResult, MessageItem, NackResult, PublishResult
from bms_msg.schemas.transport import ConnectionInfo, StreamOpen

__all__ = [
    "FRAME_MEDIA_TYPE",
    "classify",
    "error_frame",
    "message_frame",
    "result_frame",
    "stream_open_frame",
]

_RETRYABLE_BUSINESS = frozenset({350005, 350015, 350020, 350021})
"""业务层里「等一会儿再试有意义」的码：限流 / 背压、延迟区已满、连接数 / 单连接流数超限。

`350004`（RPC 预算耗尽）**故意不在其中**：超时是「结果未知」，不得直接重试（需求 04-2 §3.5）；
是否重试由 04-4 按幂等键判定。
"""

_RETRYABLE_SYSTEM = frozenset({10006, 10007, 350022})
"""系统层里可退避重试的码：数据库繁忙、下游服务不可用、**存储水位拒绝**（清理 / 扩容后即可恢复）。"""

_BUSINESS_CODES = frozenset(
    {
        350001,
        350002,
        350003,
        350004,
        350005,
        350006,
        350007,
        350008,
        350009,
        350010,
        350011,
        350012,
        350013,
        350014,
        350015,
        350016,
        350017,
        350020,
        350021,
        350023,
        350024,
    }
)
"""business 层的产品码（其余产品码兜底进 system，宁可少重试也不放大故障）。

`350024`（RPC 流空闲超时）由 04-3-1 加入：它是「这条流没进展」的业务判定，不是传输故障，
故同时不在 `_RETRYABLE_BUSINESS` 里（重开流才有意义，原地重试只会再空等一个阈值）。
"""


def classify(code: int) -> tuple[ErrorCategory, str, bool]:
    """按码判定类别 / 层 / 可重试性（错误帧的 `category` / `layer` / `retryable` 唯一出处）。

    **04-4**：`layer` 与 `retryable` 的既有判定值一字不变，只新增 `category` 一维——已交付的
    03-2 / 04-2 语义不回退。类别与层正交：同一 `transient` 既可能是 business（`350005`），
    也可能是 system（`350022`）。

    Args:
        code: 产品码 / 平台码。

    Returns:
        tuple[ErrorCategory, str, bool]: `(category, layer, retryable)`。
    """
    if code == 350018 or code == 10001:  # 帧格式非法（协议层） / 契约层参数非法
        return ErrorCategory.PARAM_ERROR, "protocol", False
    if code == 350019 or code == 350006:  # 版本不支持 / 帧或消息体超限：连接不可继续按本协议使用
        return ErrorCategory.PARAM_ERROR, "transport", False
    if code == 350004:  # 预算耗尽：结果未知，不是失败
        return ErrorCategory.UNKNOWN, "business", False
    if code in _BUSINESS_CODES:
        layer, retryable = "business", code in _RETRYABLE_BUSINESS
    else:
        layer, retryable = "system", code in _RETRYABLE_SYSTEM
    if retryable:
        return ErrorCategory.TRANSIENT, layer, True
    if layer == "business":
        return ErrorCategory.BUSINESS_REJECT, layer, False
    return ErrorCategory.SERVER_BUG, layer, False


def _result_header(request_id: str | None) -> ConcurrentStableDict[str, Any]:
    """响应帧的公共头（`request_id` 回显 + 版本可见）。

    Args:
        request_id: 请求标识（可空：解码前失败时无此值）。

    Returns:
        ConcurrentStableDict[str, Any]: 头（含公共字段）。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    return header


def _iso(value: datetime | None) -> str | None:
    """时间转 ISO 文本（帧头里不放 datetime，跨语言解析成本高）。

    Args:
        value: 时间或空。

    Returns:
        str | None: ISO 文本；空值原样返回 None。
    """
    return value.isoformat() if value is not None else None


def error_frame(
    *,
    code: int,
    message: str,
    request_id: str | None = None,
    detail: object | None = None,
    retry_after_ms: int | None = None,
) -> bytes:
    """编码错误帧（`type=error` + response 位，payload 为空）。

    Args:
        code: 产品码 / 平台码。
        message: 可读提示。
        request_id: 请求标识（可空）。
        detail: 随附数据（对应 JSON 的 `data`）。
        retry_after_ms: 建议重试间隔（毫秒）。

    Returns:
        bytes: 完整错误帧。
    """
    category, layer, retryable = classify(code)
    header = _result_header(request_id)
    header.set("code", code)
    header.set("message", message)
    header.set("retryable", retryable)
    header.set("layer", layer)
    header.set("category", str(category))
    header.set("detail", detail)
    header.set("retry_after_ms", retry_after_ms)
    return encode_frame(
        frame_type=int(FrameType.ERROR),
        flags=FrameFlags.RESPONSE,
        header=header,
    )


def result_frame(*, frame_type: int, request_id: str | None, result: object) -> bytes:
    """编码成功结果帧（同 type + response 位）。

    Args:
        frame_type: 请求帧类型（响应沿用）。
        request_id: 请求标识。
        result: 业务结果模型（`PublishResult` / `ConsumeResult` / `AckResult`）。

    Returns:
        bytes: 完整结果帧。

    Raises:
        TypeError: 结果类型与帧类型不匹配（装配错误，不属协议错误）。
    """
    header = _result_header(request_id)
    payload = b""
    if frame_type == int(FrameType.PUBLISH) and isinstance(result, PublishResult):
        header.set("message_id", result.message_id)
        header.set("log_id", result.log_id)
        header.set("topic", result.topic)
        header.set("partition", result.partition)
        header.set("offset", result.offset)
        header.set("deliver_at", _iso(result.deliver_at))
    elif frame_type == int(FrameType.CONSUME) and isinstance(result, ConsumeResult):
        header.set("count", len(result.messages))
        header.set("messages", _consume_meta(result))
        header.set("paused", result.paused)
        header.set("inflight_messages", result.inflight_messages)
        header.set("inflight_bytes", result.inflight_bytes)
        payload = _consume_payload(result)
    elif frame_type in (int(FrameType.ACK), int(FrameType.NACK)) and isinstance(result, (AckResult, NackResult)):
        # 两种「确认类」结果同构（都只有一个 committed_offset），共用一支装配。
        header.set("committed_offset", result.committed_offset)
    elif frame_type == int(FrameType.HEARTBEAT) and isinstance(result, ConnectionInfo):
        # 心跳响应与 `GET /msg/connection` 同模型：整份自报按字段平铺进 header（时间已 JSON 化成 ISO）。
        for name, value in result.model_dump(mode="json").items():
            header.set(name, value)
        header.set("request_id", request_id)
        header.set("protocol_version", PROTOCOL_VERSION)
    else:
        raise TypeError(f"帧类型 {frame_type} 与结果类型 {type(result).__name__} 不匹配")
    return encode_frame(
        frame_type=frame_type,
        flags=FrameFlags.RESPONSE,
        header=header,
        payload=payload,
    )


def _item_bytes(item: MessageItem) -> bytes:
    """取消息体原始字节（优先私有通道，回落 base64 字段）。

    运行期两条入口都由 `message_item()` 统一设置原始字节；回落分支只服务「非本装配构造」的模型
    （测试直接构造、或将来内部转发反序列化），不是第二条运行路径（03-3 详细设计 §4.7）。

    Args:
        item: 投递明细。

    Returns:
        bytes: 消息体字节。
    """
    raw = item.raw_payload
    if raw is not None:
        return raw
    return base64.b64decode(item.payload)


def _message_meta(item: MessageItem) -> ConcurrentStableDict[str, Any]:
    """构造单条消息的元数据（consume 响应头与 `message` 帧共用一处，字段不分叉）。

    Args:
        item: 投递明细。

    Returns:
        ConcurrentStableDict[str, Any]: 元数据（含 `length` 与内容类型三字段）。
    """
    meta: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    meta.set("message_id", item.message_id)
    meta.set("topic", item.topic)
    meta.set("partition", item.partition)
    meta.set("offset", item.offset)
    meta.set("key", item.key)
    meta.set("trace_id", item.trace_id)
    meta.set("delivery_count", item.delivery_count)
    meta.set("published_at", _iso(item.published_at))
    meta.set("headers", dict(item.headers.items()))
    meta.set("content_type", item.content_type)
    meta.set("compression", item.compression)
    meta.set("uncompressed_len", item.uncompressed_len)
    meta.set("length", len(_item_bytes(item)))
    return meta


def _consume_meta(result: ConsumeResult) -> ConcurrentStableList[ConcurrentStableDict[str, Any]]:
    """构造 consume 响应头的消息元数据（含切分用的 `length` 与内容类型标记）。

    Args:
        result: 拉取结果。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, Any]]: 逐条元数据。
    """
    metas: ConcurrentStableList[ConcurrentStableDict[str, Any]] = ConcurrentStableList()
    for item in result.messages:
        metas.add(_message_meta(item))
    return metas


def _consume_payload(result: ConsumeResult) -> bytes:
    """把各消息体按序拼接为 payload（顺序与 header.messages 一一对应）。

    Args:
        result: 拉取结果。

    Returns:
        bytes: 拼接后的消息体。
    """
    return b"".join(_item_bytes(item) for item in result.messages)


def stream_open_frame(*, request_id: str, opened: StreamOpen) -> bytes:
    """编码帧流的首帧（`SUBSCRIBE` + `RESPONSE`，等价 SSE 的 `open` 事件）。

    Args:
        request_id: 请求标识（回显）。
        opened: 流参数回显。

    Returns:
        bytes: 完整帧字节。
    """
    header = _result_header(request_id)
    header.set("stream_id", opened.stream_id)
    header.set("max_seconds", opened.max_seconds)
    header.set("max_wait_ms", opened.max_wait_ms)
    header.set("poll_interval_ms", opened.poll_interval_ms)
    header.set("prefetch", opened.prefetch)
    header.set("inflight_messages", opened.inflight_messages)
    header.set("inflight_bytes", opened.inflight_bytes)
    return encode_frame(frame_type=int(FrameType.SUBSCRIBE), flags=FrameFlags.RESPONSE, header=header)


def message_frame(*, request_id: str, item: MessageItem) -> bytes:
    """编码一条推送消息帧（`MESSAGE`，payload 为消息体原始字节，不 base64）。

    Args:
        request_id: 请求标识（回显；客户端据此把多路流对齐）。
        item: 投递明细。

    Returns:
        bytes: 完整帧字节。
    """
    header = _result_header(request_id)
    for name, value in _message_meta(item).items():
        header.set(name, value)
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    return encode_frame(
        frame_type=int(FrameType.MESSAGE),
        header=header,
        payload=_item_bytes(item),
    )


FRAME_MEDIA_TYPE = FRAME_CONTENT_TYPE
"""响应帧媒体类型（与请求一致，供路由层引用同一常量）。"""
