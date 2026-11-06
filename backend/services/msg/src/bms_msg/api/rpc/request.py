"""RPC 请求帧解析（任务 04-1 / 04-3-1）：一元闸门与流式端点共用的同一份判据。

**为什么单列一个模块**：一元帧形态走路由级闸门（`api/rpc/gate.py`），流式走独立端点
（`api/rpc/stream.py`）——两个入口但**同一套请求帧语义**（`type=48` 且不置响应位、`request_id` 必填、
头里的 `service` / `method` 与路径一致、不支持压缩、租户只能与请求身份一致）。判据留两份必然漂移：
典型形态是「新增一种压缩协商只改了闸门」，流式路径就会静默接受压缩帧、把压缩字节直接交给处理器。

**边界**：只做「这一帧能不能当 RPC 请求用」，不解析 payload 语义、不碰业务、不碰限流与预算
（那是 `rpc/service.py` 的事）。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.errors import MsgFrameInvalidError
from bms_msg.protocol.frame import Frame, FrameFlags, FrameType
from bms_msg.protocol.payload import COMPRESSION_NONE, normalize_compression

__all__ = [
    "MAX_HEADER_TEXT",
    "ensure_request_frame",
    "ensure_uncompressed",
    "optional_text",
    "request_id_of",
    "string_map",
]

MAX_HEADER_TEXT = 128
"""帧头里 `caller` / `trace_id` 的长度上限（与 JSON 体字段同尺；超限即契约层 `10001`）。"""


def request_id_of(header: Mapping[str, Any]) -> str | None:
    """取 `request_id`（缺失或非字符串时返回 None，由必填校验给出 `350018`）。

    Args:
        header: 帧头。

    Returns:
        str | None: 请求标识。
    """
    value = header.get("request_id")
    return value if isinstance(value, str) and value else None


def optional_text(header: Mapping[str, Any], key: str) -> str | None:
    """取一个可选的字符串头（形态非法即契约层 `10001`，不静默忽略）。

    Args:
        header: 帧头。
        key: 键名。

    Returns:
        str | None: 头值；缺失为 None。

    Raises:
        ParamError: 值不是字符串或超长（`10001`）。
    """
    raw = header.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ParamError(f"frame header 的 {key} 必须是字符串")
    if len(raw) > MAX_HEADER_TEXT:
        raise ParamError(f"frame header 的 {key} 超过 {MAX_HEADER_TEXT} 字符")
    return raw or None


def string_map(header: Mapping[str, Any]) -> ConcurrentStableDict[str, str]:
    """取帧头里的 `headers`（结构化头；缺省为空）。

    Args:
        header: 帧头。

    Returns:
        ConcurrentStableDict[str, str]: 结构化头。

    Raises:
        ParamError: 不是「字符串 → 字符串」映射（`10001`）。
    """
    raw = header.get("headers")
    result: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    if raw is None:
        return result
    if not isinstance(raw, Mapping):
        raise ParamError("frame header 的 headers 必须是 map<str, str>")
    for key, value in cast("Mapping[Any, Any]", raw).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ParamError("frame header 的 headers 必须是 map<str, str>")
        result.set(key, value)
    return result


def ensure_request_frame(frame: Frame, service: str, method: str) -> None:
    """校验帧类型、响应位、路径一致性。

    Args:
        frame: 解码结果。
        service: 路径里的服务标识。
        method: 路径里的方法名。

    Raises:
        MsgFrameInvalidError: 类型不符 / 置了 response 位 / 头里的 service 或 method 与路径不一致
            （`350018`）。
    """
    if frame.frame_type != int(FrameType.RPC):
        raise MsgFrameInvalidError(f"该端点只接受 RPC 帧（{int(FrameType.RPC)}），收到 {frame.frame_type}")
    if frame.flags & FrameFlags.RESPONSE:
        raise MsgFrameInvalidError("请求帧不得置 response 位")
    declared_service = frame.header.get("service")
    if declared_service is not None and declared_service != service:
        raise MsgFrameInvalidError(f"frame header 的 service（{declared_service!r}）与路径（{service!r}）不一致")
    declared_method = frame.header.get("method")
    if declared_method is not None and declared_method != method:
        raise MsgFrameInvalidError(f"frame header 的 method（{declared_method!r}）与路径（{method!r}）不一致")


def ensure_uncompressed(frame: Frame) -> None:
    """校验 payload 未被压缩（RPC 的 payload 直接交给 handler，没人解压）。

    Args:
        frame: 解码结果。

    Raises:
        MsgFrameInvalidError: `flags.compressed` 置位或 `header.compression` 非 `none`（`350018`）。
        ParamError: `compression` 取值不在白名单（`10001`）。
    """
    if frame.flags & FrameFlags.COMPRESSED:
        raise MsgFrameInvalidError("RPC 帧不支持压缩（payload 直接交给处理器，broker 不解压）")
    declared = normalize_compression(cast("str | None", frame.header.get("compression")))
    if declared != COMPRESSION_NONE:
        raise MsgFrameInvalidError(f"RPC 帧不支持压缩（收到 compression={declared}）")
