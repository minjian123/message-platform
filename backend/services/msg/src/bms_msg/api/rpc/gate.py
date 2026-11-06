"""RPC 帧闸门（任务 04-1）：同一路径上的 `application/x-msg-frame` 形态。

**为什么是「依赖闸门」而不是第二条路由**（与 03-2 同一结论）：同路径第二条 `include_in_schema=False`
路由会被基座 `hidden_route_violations()` 判隐藏路由（它按**路径集合**判定，业务路径不得进白名单）；
第二条可见路由会让 `app.openapi()` 的 `paths` 后者覆盖前者、把 JSON 请求体 schema 抹掉。

**与数据面闸门的关系**：不复用 `build_frame_gate()`——它的必填键与字段白名单取自数据面请求模型
（`namespace` / `topic` / `group` …），RPC 的请求模型是「路径参数 + 参数段」，套进去会要求客户端
填一堆无关字段。复用的只是**判据**：`ensure_frame_tenant`（租户一致性，唯一一份）与 `error_frame`
（码 → 层 → 重试，唯一一份）。

**请求帧判定搬到了 `api/rpc/request.py`**（04-3-1）：流式端点（`api/rpc/stream.py`）要的是同一套
「这一帧能不能当 RPC 请求用」的判定，两份必然漂移；本模块只留「怎么处理已通过判定的请求」。

**依赖先于端点参数校验执行**：`service` / `method` 在这里取到的是未校验的原始路径段，所以形态
校验必须在本模块自己做一遍（`ensure_rpc_name`，与 JSON 路径同码 `10001`），不能指望端点那层先拦。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from bms_core.core.exceptions import ParamError
from fastapi import Request, Response

from bms_msg.api.deps import DataPlaneTenantDep, IdempotencyStoreDep, RateLimiterDep, RpcRegistryDep
from bms_msg.api.frame.gate import FrameHandled, ensure_frame_tenant
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame
from bms_msg.api.rpc.caller import resolve_caller, resolve_trace_id
from bms_msg.api.rpc.idempotency import idempotency_context
from bms_msg.api.rpc.request import (
    ensure_request_frame,
    ensure_uncompressed,
    optional_budget,
    optional_text,
    request_id_of,
    string_map,
)
from bms_msg.api.rpc.responses import rpc_result_frame
from bms_msg.broker.backpressure import RateLimiter
from bms_msg.errors import (
    MsgError,
    MsgFrameInvalidError,
    MsgVersionUnsupportedError,
)
from bms_msg.protocol.frame import decode_frame
from bms_msg.rpc.cancel import ensure_not_cancelled
from bms_msg.rpc.deadline import Deadline
from bms_msg.rpc.idempotency import InProcessIdempotencyStore
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import RpcRegistry
from bms_msg.rpc.service import invoke_rpc
from bms_msg.tenant.context import CurrentTenant

__all__ = ["build_rpc_frame_gate"]


def _frame_response(body: bytes) -> Response:
    """包装一个帧响应（HTTP 200；错误也在帧里）。

    Args:
        body: 帧字节。

    Returns:
        Response: 响应对象。
    """
    return Response(content=body, media_type=FRAME_MEDIA_TYPE, status_code=200)


async def _handle_rpc_frame(
    *,
    request: Request,
    service: str,
    method: str,
    tenant: CurrentTenant,
    registry: RpcRegistry,
    limiter: RateLimiter,
    store: InProcessIdempotencyStore,
) -> Response:
    """处理一个 RPC 帧请求（解码 → 校验 → 调核心 → 编码）。

    Args:
        request: 当前请求。
        service: 路径里的服务标识。
        method: 路径里的方法名。
        tenant: 当前租户。
        registry: 方法注册表。
        limiter: 限流器。
        store: 幂等去重存储。

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
    request_id = request_id_of(frame.header)
    try:
        deadline = Deadline.start(frame.header.get("deadline_ms"))
    except ParamError as exc:
        # 预算值非法：契约层，与 JSON 路径同码同形态（错误帧里的 10001）。
        return _frame_response(error_frame(code=10001, message=str(exc), request_id=request_id))
    try:
        ensure_rpc_name(service, method)
        ensure_request_frame(frame, service, method)
        if request_id is None:
            raise MsgFrameInvalidError("缺少必填 frame header 字段：request_id")
        ensure_uncompressed(frame)
        ensure_frame_tenant(frame.header, tenant)
        headers = string_map(frame.header)
        caller = resolve_caller(request, optional_text(frame.header, "caller"))
        trace_id = resolve_trace_id(request, optional_text(frame.header, "trace_id"))
        idempotency_key = optional_text(frame.header, "idempotency_key")
        retry_budget = optional_budget(frame.header, "retry_budget")
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
            deadline=deadline,
            trace_id=trace_id,
            caller=caller,
            request_id=request_id,
            disconnected=request.is_disconnected,
            idempotency=idempotency_context(
                store=store,
                registry=registry,
                tenant=tenant.code,
                service=service,
                method=method,
                key=idempotency_key,
            ),
            retry_budget=retry_budget,
            idempotency_key=idempotency_key,
        )
    except MsgError as exc:
        return _frame_response(
            error_frame(
                code=exc.code,
                message=exc.message or str(exc),
                request_id=request_id,
                detail=exc.data,
                retry_after_ms=getattr(exc, "retry_after_ms", None),
            )
        )
    # 判定必须紧贴「写响应」：handler 若吞掉取消并正常返回，这里把响应丢掉（详细设计 §5.4）。
    ensure_not_cancelled()
    return _frame_response(
        rpc_result_frame(
            request_id=request_id,
            service=service,
            method=method,
            reply=reply,
            used_ms=deadline.spent_ms(),
            remaining_ms=deadline.remaining_ms(),
        )
    )


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
        store: IdempotencyStoreDep,
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
            store: 幂等去重存储。

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
            store=store,
        )
        raise FrameHandled(response)

    return gate
