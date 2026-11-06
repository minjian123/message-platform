"""RPC 服务端流端点（任务 04-3-1）：`POST /rpc/{service}/{method}/stream`，只接帧形态。

**为什么是独立路由而不是挂进一元 `router`**：一元路由的路由级依赖里有 `build_rpc_frame_gate()`，
它对**任何**带帧媒体类型的请求都整单处理并 `FrameHandled` 短路——挂进去等于流式请求被一元闸门吃掉。
改闸门「跳过 `/stream`」是把语义判断塞进一个与语义无关的依赖里，且下次再加一条形态还要再改一次闸门；
独立路由把「哪种形态」写在路由表里，一眼可核对（详细设计 §4.1）。

**端点只做三件事**：解请求帧 → 建流（`StreamingResponse`）→ 编码每批结果。业务语义（攒批 / 背压 /
三条判据）在 `rpc/stream.py`，请求帧判据在 `api/rpc/request.py`（与一元闸门同一份）。

**建流前的失败也回帧**（HTTP 200 + 单帧 `error`）：客户端已经在按帧说话（`Content-Type` 是帧），
回 JSON 等于要求它维护两套解析——03-2 已把「版本不支持给一个可解析的帧」写成口径，这里延续同一条。
只有「不是帧请求」例外：那说明它根本不是按帧在说话，用基座统一错误响应（JSON `10001`）最省事。

**长驻流名额在生成器里占、`finally` 里还**（与 03-5 的订阅流同一本账）：若在端点里先占、响应却
从未被迭代（客户端连上即断），`finally` 不会执行，名额就永久泄漏一个；把占用放进生成器第一行，
「占到了就一定有对应的归还」在同一个代码块里成立。判定时点仍在预检之前（名额是连接级资源）。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import BizError, ParamError
from bms_core.permission.base import require_permission
from fastapi import Depends, Path, Request, Response
from fastapi.responses import StreamingResponse

from bms_msg.api.deps import (
    BackpressureTuningDep,
    ConnectionRegistryDep,
    ConnectionTuningDep,
    DataPlaneTenantDep,
    RateLimiterDep,
    RpcRegistryDep,
)
from bms_msg.api.frame.gate import ensure_frame_tenant
from bms_msg.api.frame.guard import CONNECTION_GUARD
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame
from bms_msg.api.rpc.caller import resolve_caller, resolve_trace_id
from bms_msg.api.rpc.request import (
    ensure_request_frame,
    ensure_uncompressed,
    optional_text,
    request_id_of,
    string_map,
)
from bms_msg.api.rpc.responses import rpc_stream_frame
from bms_msg.api.rpc.routes import RPC_PERMISSION
from bms_msg.broker.backpressure import BackpressureTuning, RateLimiter
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.errors import (
    MsgError,
    MsgErrorCode,
    MsgFrameInvalidError,
    MsgVersionUnsupportedError,
)
from bms_msg.protocol.frame import FrameFlags, decode_frame
from bms_msg.rpc.deadline import Deadline
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import RpcRegistry
from bms_msg.rpc.stream import open_rpc_stream

__all__ = ["STREAM_ERROR_CODES", "stream_router"]

STREAM_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
        int(MsgErrorCode.RATE_LIMITED),
        int(MsgErrorCode.PAYLOAD_TOO_LARGE),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
        int(MsgErrorCode.STREAM_LIMIT_EXCEEDED),
        int(MsgErrorCode.RPC_METHOD_NOT_FOUND),
        int(MsgErrorCode.RPC_STREAM_IDLE),
    ]
)
"""流端点声明的错误码（`x-error-codes`）：与一元同码 + `350021`（长驻流名额）+ `350024`（空闲超时）。

协议层通用码（`350018` / `350019` / `10001`）不在此列：它们由请求帧判据统一给出，与一元口径一致。
"""

_STREAM_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
"""长驻流的公共响应头：禁缓存、禁代理缓冲（否则帧流会被网关攒批）。"""

stream_router = BaseRouter(
    key="msg_rpc_stream",
    prefix="/rpc",
    tags=["msg-rpc"],
    dependencies=[
        Depends(CONNECTION_GUARD),
        Depends(require_permission(RPC_PERMISSION)),
    ],
)
"""RPC 流式路由（挂空前缀后即 `/rpc/{service}/{method}/stream`）。

**没有帧闸门**：本路由的每个端点都是「只接帧」的形态，不需要「是不是帧请求」的二次判定；
守卫与权限与一元同一实例（同一条连接、同一个 `msg:rpc`）。
"""


def _single_frame(payload: bytes) -> Response:
    """把一帧包装成响应（建流前的失败：HTTP 200 + 单帧 `error`）。

    Args:
        payload: 帧字节。

    Returns:
        Response: 帧响应（媒体类型与流响应一致，客户端同一套解析）。
    """
    return Response(content=payload, media_type=FRAME_MEDIA_TYPE, status_code=200, headers=_STREAM_HEADERS)


async def _stream_body(
    *,
    request_id: str,
    service: str,
    method: str,
    payload: bytes,
    headers: ConcurrentStableDict[str, str],
    tenant: str,
    deadline: Deadline,
    caller: str | None,
    trace_id: str | None,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tuning: BackpressureTuning,
    connections: ConnectionRegistry,
    connection_key: str,
    max_streams: int,
    request: Request,
) -> AsyncGenerator[bytes]:
    """把服务层的「批」编码成帧字节流（占用名额 → 逐批编码 → `finally` 归还）。

    Args:
        request_id: 请求标识（逐帧回显）。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头。
        tenant: 租户编码。
        deadline: 整条流的预算视图。
        caller: 调用方服务标识（透传给处理器）。
        trace_id: 链路标识（透传给处理器与日志）。
        registry: 方法注册表。
        limiter: 限流器。
        tuning: inflight 双阈值。
        connections: 连接登记表（长驻流名额）。
        connection_key: 所属连接的派生键（名额归还与此同键）。
        max_streams: 单连接长驻流上限（取自连接参数）。
        request: 当前请求（断开探测来源）。

    Yields:
        bytes: 连续帧字节（一批可能多帧；客户端按 `header_len` / `payload_len` 切分）。
    """
    opened = False
    seq = 0
    try:
        # 名额先占（预检之前）：连接级资源不必等租户 / 方法解析；`finally` 保证一一对应。
        connections.open_stream(connection_key, limit=max_streams)
        opened = True
        async for item in open_rpc_stream(
            registry=registry,
            limiter=limiter,
            tenant=tenant,
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            deadline=deadline,
            tuning=tuning,
            trace_id=trace_id,
            caller=caller,
            request_id=request_id,
            disconnected=request.is_disconnected,
        ):
            last_index = len(item.replies) - 1
            # **一批一帧不改语义**：这里把整批编成一段字节一次写出（`yield` 一次 = 一次 `send`），
            # 攒批才真的落到线上（一帧一次 send 会让「批」只剩服务层内部概念，且多出 N 次调度）。
            batch = b"".join(
                rpc_stream_frame(
                    request_id=request_id,
                    service=service,
                    method=method,
                    reply=reply,
                    seq=seq + index,
                    used_ms=deadline.spent_ms(),
                    remaining_ms=deadline.remaining_ms(),
                    end_of_stream=item.terminated and index == last_index,
                )
                for index, reply in enumerate(item.replies)
            )
            seq += len(item.replies)
            yield batch
    except BizError as exc:
        # 流内错误一律以 ERROR 帧收尾（码 → 层 → 重试由 `error_frame` 唯一判定）；
        # 取消不在此列（`CancelledError` 不是 `BizError`），取消后不写任何收尾帧。
        yield error_frame(
            code=exc.code,
            message=exc.message or str(exc),
            request_id=request_id,
            detail=exc.data,
            retry_after_ms=getattr(exc, "retry_after_ms", None),
        )
    finally:
        if opened:
            connections.close_stream(connection_key)


@stream_router.post(
    "/{service}/{method}/stream",
    openapi_extra={"x-error-codes": list(STREAM_ERROR_CODES)},
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "帧流（48+response 逐帧、末帧另置 end_of_stream；ERROR 帧 16 收尾；EOF 即结束）",
            "content": {FRAME_MEDIA_TYPE: {}},
        }
    },
)
async def open_stream(
    service: Annotated[str, Path(description="服务标识（与注册表登记的 service 同值）")],
    method: Annotated[str, Path(description="流式方法名（与注册表登记的 method 同值）")],
    request: Request,
    tenant: DataPlaneTenantDep,
    registry: RpcRegistryDep,
    limiter: RateLimiterDep,
    connections: ConnectionRegistryDep,
    connection_tuning: ConnectionTuningDep,
    tuning: BackpressureTuningDep,
) -> Response:
    """服务端流式调用（一次请求、多次响应；只接 `application/x-msg-frame`）。

    需要 `msg:rpc` 权限（与一元同一权限码）。**只接帧形态**：JSON 请求按契约层拒绝（`10001`，
    基座统一错误响应），因为流式的响应是连续帧、没有 JSON 形态。

    建流前的失败（帧不可解析 / 类型不符 / 置了响应位或结束位 / 缺 `request_id` / 压缩 / 租户不一致 /
    路径参数形态 / 预算已耗尽 / 配额 / 参数超限 / 方法未注册或非流式 / 长驻流名额满）一律返回
    **HTTP 200 + 单帧 `error`**；对应码为 `350018` / `350019` / `350012` / `10001` / `350004` /
    `350005` / `350006` / `350023` / `350021`。推流中的错误以 `ERROR` 帧收尾（`BizError` 原码、
    `350004` 预算、`350006` 单帧超限、`350024` 空闲超时）；正常结束由末帧 `end_of_stream` 表达；
    客户端取消不写收尾帧。

    **路径参数不在契约里声明 `pattern` / `minLength`**（2026-11-06 确认）：FastAPI 的参数校验发生在
    端点之前，非法形态会先被渲染成 JSON 错误、永远到不了「单帧 `error`」这条口径；形态判据改在端点内
    用 `ensure_rpc_name`（与一元同一函数、同一码），契约里只保留可读描述。
    """
    if not is_frame_request(request):
        raise ParamError("RPC 流式端点只接受帧形态（Content-Type: application/x-msg-frame）")
    body = await request.body()
    try:
        frame = decode_frame(body)
    except MsgVersionUnsupportedError as exc:
        # 版本不支持要给客户端一个可解析的帧（它已经按帧在说话），与一元闸门同口径。
        return _single_frame(error_frame(code=exc.code, message=str(exc)))
    request_id = request_id_of(frame.header)
    try:
        deadline = Deadline.start(frame.header.get("deadline_ms"))
    except ParamError as exc:
        return _single_frame(error_frame(code=10001, message=str(exc), request_id=request_id))
    try:
        ensure_rpc_name(service, method)
        ensure_request_frame(frame, service, method)
        if frame.flags & FrameFlags.END_OF_STREAM:
            raise MsgFrameInvalidError("请求帧不得置 end_of_stream 位")
        if request_id is None:
            raise MsgFrameInvalidError("缺少必填 frame header 字段：request_id")
        ensure_uncompressed(frame)
        ensure_frame_tenant(frame.header, tenant)
        headers = string_map(frame.header)
        caller = resolve_caller(request, optional_text(frame.header, "caller"))
        trace_id = resolve_trace_id(request, optional_text(frame.header, "trace_id"))
    except ParamError as exc:
        return _single_frame(error_frame(code=10001, message=str(exc), request_id=request_id))
    except MsgError as exc:
        return _single_frame(error_frame(code=exc.code, message=exc.message or str(exc), request_id=request_id))
    return StreamingResponse(
        _stream_body(
            request_id=request_id,
            service=service,
            method=method,
            payload=bytes(frame.payload),
            headers=headers,
            tenant=tenant.code,
            deadline=deadline,
            caller=caller,
            trace_id=trace_id,
            registry=registry,
            limiter=limiter,
            tuning=tuning,
            connections=connections,
            connection_key=cast("str", request.state.connection_key),
            max_streams=connection_tuning.max_streams_per_connection,
            request=request,
        ),
        media_type=FRAME_MEDIA_TYPE,
        headers=_STREAM_HEADERS,
    )
