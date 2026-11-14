"""RPC 一元调用端点（任务 04-1）：`POST /rpc/{service}/{method}`，双形态同一实现。

**挂根路径**（`prefix=""` 聚合），与 `/msg/*` 并列、**不进** `/api/v1`：RPC 是内网直连的数据面入口
（需求 04-1 §2.1、架构 05 §3），不需要网关前缀；结果帧也不能进 `/openapi.json`（生成式客户端会
按 JSON 解析帧端点）。

**双形态怎么共存**：帧形态是**路由级依赖**（`build_rpc_frame_gate()`），它在端点之前执行、自己
处理完整个帧请求并用 `FrameHandled` 短路。JSON 路径完全不受影响，签名里也不需要任何「是否帧」分支。

**只做「取参数 → 调核心 → 编码」**：业务语义（形态 / 限流 / 上限 / 路由 / 执行）全在
`rpc/service.py::invoke_rpc`，与帧闸门共用同一份——两形态不会分叉。

**流式端点不在这条路由**（04-3-1）：`api/rpc/stream.py` 的 `stream_router` 另起一条，前缀同为
`/rpc` 但**不挂**本路由的帧闸门——闸门会把任何帧请求整单处理并短路，挂进去等于流式请求被吃掉。
"""

from __future__ import annotations

import base64
import contextlib
import time
from functools import wraps
from typing import Annotated, Any

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import BizError, ParamError
from bms_core.msgclient.deadline import Deadline
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path, Request, Response

from bms_msg.api.deps import DataPlaneTenantDep, IdempotencyStoreDep, RateLimiterDep, RpcRegistryDep
from bms_msg.api.frame.guard import CONNECTION_GUARD
from bms_msg.api.rpc.caller import (
    CALLER_HEADER,
    TRACE_HEADER,
    resolve_caller,
    resolve_trace_id,
)
from bms_msg.api.rpc.gate import build_rpc_frame_gate
from bms_msg.api.rpc.idempotency import idempotency_context
from bms_msg.core.config import (
    RPC_METHOD_MAX_LENGTH,
    RPC_METHOD_PATTERN,
    RPC_REMAINING_MS_HEADER,
    RPC_SERVICE_MAX_LENGTH,
    RPC_SERVICE_PATTERN,
    RPC_USED_MS_HEADER,
)
from bms_msg.errors import MsgErrorCode
from bms_msg.metrics import get_traffic_metrics
from bms_msg.rpc.cancel import ensure_not_cancelled
from bms_msg.rpc.service import invoke_rpc
from bms_msg.schemas.rpc import RpcRequest, RpcResult

__all__ = ["RPC_ERROR_CODES", "RPC_PERMISSION", "router"]

RPC_PERMISSION = "msg:rpc"
"""RPC 端点权限码（05 域落服务令牌前只是声明式占位；无 provider 时恒放行）。"""


def _track_rpc(func: Any) -> Any:
    """RPC 埋点装饰器（06-3-1）：记录调用次数与处理耗时。

    指标异常必须吞掉（绝不因采集失败影响调用）；`method` 取注册表内固定方法名，
    禁止把用户输入拼进标签。

    Args:
        func: RPC 端点实现。

    Returns:
        Any: 包装后的端点。
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        method = str(kwargs.get("method", ""))
        try:
            response = await func(*args, **kwargs)
        except BizError as exc:
            with contextlib.suppress(Exception):
                get_traffic_metrics().record_rpc(
                    method=method,
                    outcome=_rpc_outcome(exc.code),
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                )
            raise
        with contextlib.suppress(Exception):
            get_traffic_metrics().record_rpc(
                method=method,
                outcome="ok",
                duration_ms=(time.perf_counter() - started) * 1000.0,
            )
        return response

    return wrapper


def _rpc_outcome(code: int) -> str:
    """把 RPC 错误码归类为结果标签（闭集）。

    Args:
        code: 业务错误码。

    Returns:
        str: `deadline_exceeded` / `handler_timeout` / `rate_limited` / `method_not_found` / `business_error`。
    """
    if code == int(MsgErrorCode.RPC_DEADLINE_EXCEEDED):
        return "deadline_exceeded"
    if code == int(MsgErrorCode.RPC_HANDLER_TIMEOUT):
        return "handler_timeout"
    if code == int(MsgErrorCode.RATE_LIMITED):
        return "rate_limited"
    if code == int(MsgErrorCode.RPC_METHOD_NOT_FOUND):
        return "method_not_found"
    return "business_error"


RPC_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
        int(MsgErrorCode.RPC_HANDLER_TIMEOUT),
        int(MsgErrorCode.RATE_LIMITED),
        int(MsgErrorCode.PAYLOAD_TOO_LARGE),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
        int(MsgErrorCode.RPC_METHOD_NOT_FOUND),
    ]
)
"""RPC 端点声明的错误码（`x-error-codes`；只列本端点可达的业务码，协议层通用码由闸门统一处理）。

`350004` 由任务 04-2 启用（预算耗尽 / 504），与 `350023`（方法未注册）同为 business 层、都不重试，
但一个表示「没时间」、一个表示「没这个方法」。
"""

router = BaseRouter(
    key="msg_rpc",
    prefix="/rpc",
    tags=["msg-rpc"],
    dependencies=[
        Depends(CONNECTION_GUARD),
        Depends(require_permission(RPC_PERMISSION)),
        Depends(build_rpc_frame_gate()),
    ],
)
"""RPC 聚合路由（挂空前缀后即 `/rpc/{service}/{method}`）。

三个路由级依赖各管一件事：连接登记与上限（与数据面同一实例，RPC 也计入 `max_connections`）、
权限占位、帧形态闸门（非帧请求直接放行）。
"""


def _decode_payload(text: str) -> bytes:
    """把 JSON 形态的 base64 参数还原成原始字节（空串 = 无参数）。

    Args:
        text: base64 文本。

    Returns:
        bytes: 参数原始字节。

    Raises:
        ParamError: 不是合法 base64（`10001`；`binascii.Error` 是 `ValueError` 子类，一并在此拦下）。
    """
    if not text:
        return b""
    try:
        return base64.b64decode(text, validate=True)
    except ValueError as exc:
        raise ParamError("payload 不是合法的 base64 编码") from exc


@router.post(
    "/{service}/{method}",
    openapi_extra={"x-error-codes": list(RPC_ERROR_CODES)},
)
@_track_rpc
async def invoke(
    service: Annotated[
        str,
        Path(
            min_length=1,
            max_length=RPC_SERVICE_MAX_LENGTH,
            pattern=RPC_SERVICE_PATTERN.pattern,
            description="服务标识（与注册表登记的 service 同值）",
        ),
    ],
    method: Annotated[
        str,
        Path(
            min_length=1,
            max_length=RPC_METHOD_MAX_LENGTH,
            pattern=RPC_METHOD_PATTERN.pattern,
            description="方法名（与注册表登记的 method 同值）",
        ),
    ],
    req: RpcRequest,
    request: Request,
    response: Response,
    tenant: DataPlaneTenantDep,
    registry: RpcRegistryDep,
    limiter: RateLimiterDep,
    store: IdempotencyStoreDep,
) -> ApiResponse[RpcResult]:
    """一元调用（JSON 形态）。

    需要 `msg:rpc` 权限（当前无 provider 时恒放行）。`service` / `method` 形态非法由契约层拒绝
    （`10001`）；`payload` 非合法 base64 同码。未注册方法返回 `350023`（404，`layer=business`、
    `retryable=false`），不是 500。参数 / 结果超过 1 MiB 返回 `350006`。
    调用方标识与链路标识走「HTTP 头优先」，两者都会回写进响应头。

    **预算**（04-2）：`deadline_ms` 缺省 30000、区间 0~600000（越界 `10001`）；耗尽返回 `350004`
    （504，`data.outcome=unknown`）。`used_ms` / `remaining_ms` 随响应头回传，供上游核对透传是否生效。
    取消（`RST_STREAM` / 连接断开）由 `ensure_not_cancelled()` 在写响应前复核，取消后不写响应。
    """
    deadline = Deadline.start(req.deadline_ms)
    caller = resolve_caller(request, req.caller)
    trace_id = resolve_trace_id(request, req.trace_id)
    reply = await invoke_rpc(
        registry=registry,
        limiter=limiter,
        tenant=tenant.code,
        service=service,
        method=method,
        payload=_decode_payload(req.payload),
        headers=req.headers,
        deadline=deadline,
        trace_id=trace_id,
        caller=caller,
        disconnected=request.is_disconnected,
        idempotency=idempotency_context(
            store=store,
            registry=registry,
            tenant=tenant.code,
            service=service,
            method=method,
            key=req.idempotency_key,
        ),
        retry_budget=req.retry_budget,
        idempotency_key=req.idempotency_key,
    )
    # 判定必须紧贴「写响应」：handler 若吞掉取消并正常返回，这里把响应丢掉（详细设计 §5.4）。
    ensure_not_cancelled()
    if caller is not None:
        response.headers[CALLER_HEADER] = caller
    if trace_id is not None:
        response.headers[TRACE_HEADER] = trace_id
    response.headers[RPC_USED_MS_HEADER] = str(deadline.spent_ms())
    response.headers[RPC_REMAINING_MS_HEADER] = str(deadline.remaining_ms())
    return ApiResponse.ok(
        RpcResult(
            payload=base64.b64encode(reply.payload).decode("ascii"),
            headers=reply.headers,
        )
    )
