"""RPC 调用核心（任务 04-1）：限流 → 参数上限 → 路由 → 执行 → 结果上限。

**业务语义只写一遍**：JSON 端点（`api/rpc/routes.py`）与帧闸门（`api/rpc/gate.py`）都只做
「取参数 → 调本函数 → 编码结果」。两者差别只在承载（异常 → 统一错误响应 / 异常 → ERROR 帧），
所以任何新增判定都只改这里，不会出现「JSON 路径有、帧路径没有」的静默分叉。

**判定顺序**（详细设计 §7.2）：

1. 形态（`service` / `method` 正则）——API 层已用 `Path(pattern=...)` 声明，这里再判一次：
   帧形态的路径参数在依赖里拿，**依赖先于端点参数校验执行**，不能指望端点先拦；
2. 限流（`(tenant, service)`）——打满时连注册表都不必查（与 03-4「连接类判定先于租户解析」同取向）；
3. 参数上限——超大参数是客户端问题，不该因方法名顺手就先查表；
4. 路由——未注册 `350023`；
5. 执行；
6. 结果上限——在编码之前判，否则帧编码在端点里抛错会变成 500。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.backpressure import RateLimiter
from bms_msg.errors import (
    MsgPayloadTooLargeError,
    MsgRateLimitedError,
    MsgRpcMethodNotFoundError,
)
from bms_msg.protocol.frame import MAX_FRAME_PAYLOAD_BYTES
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import RpcCall, RpcHandler, RpcRegistry, RpcReply

__all__ = ["invoke_rpc"]


def _handler_of(registry: RpcRegistry, service: str, method: str) -> RpcHandler:
    """取处理器（未注册转 `350023`，`data` 为普通 dict）。

    Args:
        registry: 方法注册表。
        service: 服务标识。
        method: 方法名。

    Returns:
        RpcHandler: 处理器。

    Raises:
        MsgRpcMethodNotFoundError: 未注册（`350023`）。
    """
    handler = registry.resolve(service, method)
    if handler is None:
        raise MsgRpcMethodNotFoundError(
            f"RPC 方法未注册：{service}.{method}",
            # 普通 dict：自定义 Mapping 走 pydantic 序列化会 500（02-7 在 350015 上验过）。
            data={"service": service, "method": method},
        )
    return handler


async def invoke_rpc(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    headers: ConcurrentStableDict[str, str],
    trace_id: str | None = None,
    caller: str | None = None,
) -> RpcReply:
    """执行一次一元调用（两种形态共用）。

    Args:
        registry: 方法注册表。
        limiter: 限流器（RPC 维度独立于生产两级）。
        tenant: 租户编码（已由数据面解析链判定）。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头。
        trace_id: 链路标识。
        caller: 调用方服务标识。

    Returns:
        RpcReply: 结果（payload 已判过上限）。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数或结果超过 1 MiB（`350006`）。
        MsgRpcMethodNotFoundError: 方法未注册（`350023`）。
        MsgError: 处理器抛出的业务错误（原样向上）。
    """
    ensure_rpc_name(service, method)
    verdict = limiter.check_rpc(tenant=tenant, service=service)
    if not verdict.allowed:
        raise MsgRateLimitedError(f"RPC 调用速率超出配额（{service}）", retry_after_ms=verdict.retry_after_ms)
    if len(payload) > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"RPC 参数超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(payload)}）")
    handler = _handler_of(registry, service, method)
    reply = await handler(
        RpcCall(
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            tenant=tenant,
            trace_id=trace_id,
            caller=caller,
        )
    )
    if len(reply.payload) > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"RPC 结果超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(reply.payload)}）")
    return reply
