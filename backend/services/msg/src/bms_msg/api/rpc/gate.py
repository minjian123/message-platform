"""RPC 帧闸门（任务 04-1）：同一路径上的 `application/x-msg-frame` 形态。

**为什么是「依赖闸门」而不是第二条路由**（与 03-2 同一结论）：同路径第二条 `include_in_schema=False`
路由会被基座 `hidden_route_violations()` 判隐藏路由（它按**路径集合**判定，业务路径不得进白名单）；
第二条可见路由会让 `app.openapi()` 的 `paths` 后者覆盖前者、把 JSON 请求体 schema 抹掉。

**与数据面闸门的关系**：不复用 `build_frame_gate()`——它的必填键与字段白名单取自数据面请求模型
（`namespace` / `topic` / `group` …），RPC 的请求模型是「路径参数 + 参数段」，套进去会要求客户端
填一堆无关字段。复用的只是**判据**：`ensure_frame_tenant`（租户一致性，唯一一份）与 `error_frame`
（码 → 层 → 重试，唯一一份）。

**依赖先于端点参数校验执行**：`service` / `method` 在这里取到的是未校验的原始路径段，所以形态
校验必须在本模块自己做一遍（`ensure_rpc_name`，与 JSON 路径同码 `10001`），不能指望端点那层先拦。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from fastapi import Request, Response

from bms_msg.api.deps import DataPlaneTenantDep, RateLimiterDep, RpcRegistryDep
from bms_msg.api.frame.gate import FrameHandled, ensure_frame_tenant
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame
from bms_msg.api.rpc.caller import resolve_caller, resolve_trace_id
from bms_msg.api.rpc.responses import rpc_result_frame
from bms_msg.broker.backpressure import RateLimiter
from bms_msg.errors import (
    MsgError,
    MsgFrameInvalidError,
    MsgVersionUnsupportedError,
)
from bms_msg.protocol.frame import (
    Frame,
    FrameFlags,
    FrameType,
    decode_frame,
)
from bms_msg.protocol.payload import COMPRESSION_NONE, normalize_compression
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import RpcRegistry
from bms_msg.rpc.service import invoke_rpc
from bms_msg.tenant.context import CurrentTenant

__all__ = ["build_rpc_frame_gate"]

_LOGGER = get_logger("bms_msg.rpc")

_MAX_HEADER_TEXT = 128
"""帧头里 `caller` / `trace_id` 的长度上限（与 JSON 体字段同尺；超限即契约层 `10001`）。"""


def _frame_response(body: bytes) -> Response:
    """包装一个帧响应（HTTP 200；错误也在帧里）。

    Args:
        body: 帧字节。

    Returns:
        Response: 响应对象。
    """
    return Response(content=body, media_type=FRAME_MEDIA_TYPE, status_code=200)


def _request_id(header: Mapping[str, Any]) -> str | None:
    """取 `request_id`（缺失或非字符串时返回 None，由必填校验给出 `350018`）。

    Args:
        header: 帧头。

    Returns:
        str | None: 请求标识。
    """
    value = header.get("request_id")
    return value if isinstance(value, str) and value else None


def _optional_text(header: Mapping[str, Any], key: str) -> str | None:
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
    if len(raw) > _MAX_HEADER_TEXT:
        raise ParamError(f"frame header 的 {key} 超过 {_MAX_HEADER_TEXT} 字符")
    return raw or None


def _string_map(header: Mapping[str, Any]) -> ConcurrentStableDict[str, str]:
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


def _ensure_request_frame(frame: Frame, service: str, method: str) -> None:
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


def _ensure_uncompressed(frame: Frame) -> None:
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


async def _handle_rpc_frame(
    *,
    request: Request,
    service: str,
    method: str,
    tenant: CurrentTenant,
    registry: RpcRegistry,
    limiter: RateLimiter,
) -> Response:
    """处理一个 RPC 帧请求（解码 → 校验 → 调核心 → 编码）。

    Args:
        request: 当前请求。
        service: 路径里的服务标识。
        method: 路径里的方法名。
        tenant: 当前租户。
        registry: 方法注册表。
        limiter: 限流器。

    Returns:
        Response: 帧响应（成功或错误帧，均为 200）。

    Raises:
        MsgError: 帧不可解析（magic 不符 / 长度越界）——由基座统一渲染为 4xx JSON（03-2 契约）。
    """
    body = await request.body()
    try:
        frame = decode_frame(body)
    except MsgVersionUnsupportedError as exc:
        # 版本不支持要给客户端一个可解析的帧（它已经按帧在说话），与数据面同口径。
        return _frame_response(error_frame(code=exc.code, message=str(exc)))
    request_id = _request_id(frame.header)
    try:
        ensure_rpc_name(service, method)
        _ensure_request_frame(frame, service, method)
        if request_id is None:
            raise MsgFrameInvalidError("缺少必填 frame header 字段：request_id")
        _ensure_uncompressed(frame)
        ensure_frame_tenant(frame.header, tenant)
        headers = _string_map(frame.header)
        caller = resolve_caller(request, _optional_text(frame.header, "caller"))
        trace_id = resolve_trace_id(request, _optional_text(frame.header, "trace_id"))
    except ParamError as exc:
        # 形态非法（路径参数 / 头值类型）：契约层，与 JSON 路径同码。
        return _frame_response(error_frame(code=10001, message=str(exc), request_id=request_id))
    except MsgError as exc:
        return _frame_response(error_frame(code=exc.code, message=exc.message or str(exc), request_id=request_id))
    try:
        reply = await invoke_rpc(
            registry=registry,
            limiter=limiter,
            tenant=tenant.code,
            service=service,
            method=method,
            payload=bytes(frame.payload),
            headers=headers,
            trace_id=trace_id,
            caller=caller,
        )
    except MsgError as exc:
        _LOGGER.info(
            "msg_rpc",
            service=service,
            method=method,
            request_id=request_id,
            outcome="business_error",
            code=exc.code,
        )
        return _frame_response(
            error_frame(
                code=exc.code,
                message=exc.message or str(exc),
                request_id=request_id,
                detail=exc.data,
                retry_after_ms=getattr(exc, "retry_after_ms", None),
            )
        )
    _LOGGER.info("msg_rpc", service=service, method=method, request_id=request_id, outcome="ok")
    return _frame_response(rpc_result_frame(request_id=request_id, service=service, method=method, reply=reply))


def build_rpc_frame_gate() -> Callable[..., Awaitable[None]]:
    """构造 RPC 帧闸门依赖（非帧请求直接放行，JSON 路径零改动）。

    Returns:
        Callable[..., Awaitable[None]]: 可直接 `Depends(...)` 的闸门函数。
    """

    async def gate(
        request: Request,
        tenant: DataPlaneTenantDep,
        registry: RpcRegistryDep,
        limiter: RateLimiterDep,
    ) -> None:
        """帧形态则整单处理并短路端点。

        **路径参数从 `request.path_params` 取，不声明成依赖参数**：依赖里声明同名 `str` 会与端点的
        `Annotated[str, Path(...)]` 抢同一份参数定义，公开契约里的 `pattern` / `minLength` 会丢
        （实测：声明成依赖后端点的约束不再出现在 `msg.json`）。

        Args:
            request: 当前请求。
            tenant: 当前租户。
            registry: 方法注册表。
            limiter: 限流器。

        Raises:
            FrameHandled: 帧请求已处理完毕（携带响应）。
            MsgError: 帧不可解析（4xx JSON）。
        """
        if not is_frame_request(request):
            return
        response = await _handle_rpc_frame(
            request=request,
            service=str(request.path_params.get("service", "")),
            method=str(request.path_params.get("method", "")),
            tenant=tenant,
            registry=registry,
            limiter=limiter,
        )
        raise FrameHandled(response)

    return gate
