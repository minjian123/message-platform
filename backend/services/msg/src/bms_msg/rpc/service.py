"""RPC 调用核心（任务 04-1 / 04-2）：预算预检 → 限流 → 参数上限 → 路由 → 执行（预算硬兜底）→ 结果上限。

**业务语义只写一遍**：JSON 端点（`api/rpc/routes.py`）与帧闸门（`api/rpc/gate.py`）都只做
「取参数 → 调本函数 → 编码结果」。两者差别只在承载（异常 → 统一错误响应 / 异常 → ERROR 帧），
所以任何新增判定都只改这里，不会出现「JSON 路径有、帧路径没有」的静默分叉。

**判定顺序**（04-1 详细设计 §7.2 + 04-2 详细设计 §5.2）：

1. 形态（`service` / `method` 正则）——API 层已用 `Path(pattern=...)` 声明，这里再判一次：
   帧形态的路径参数在依赖里拿，**依赖先于端点参数校验执行**，不能指望端点先拦；
2. **预算预检**——`remaining <= 0` 直接 `350004`，**不进 handler、不消耗配额**：耗尽的请求做任何事
   都是浪费，而且若排在限流之后，客户端刷 `deadline_ms=0` 就能把 `(tenant, service)` 桶打穿；
3. 限流（`(tenant, service)`）——打满时连注册表都不必查（与 03-4「连接类判定先于租户解析」同取向）；
4. 参数上限——超大参数是客户端问题，不该因方法名顺手就先查表；
5. 路由——未注册 `350023`；
6. 执行——`asyncio.timeout()` 硬兜底（04-2 详细设计 §5.3）；
7. 结果上限——在编码之前判，否则帧编码在端点里抛错会变成 500。

**取消不进错误帧**：取消意味着 stream 已经没了，没有「回给谁」的问题；只落日志 + 让异常继续上抛，
资源由请求作用域与 handler 自己的 `finally` 收口（04-2 详细设计 §5.4）。

**前置判定与流式共用**（04-3-1）：上面 1~4 步收在 `_preflight()`，一元走 `prepare_handler()`（查一元表）、
流式走 `prepare_stream_handler()`（查流式表），两者判定顺序与错误码完全一致——流式不会出现
「JSON 路径有、帧路径没有」的静默分叉，也不会因为复制一份判定而漂移。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.logging import get_logger
from bms_core.msgclient.deadline import OUTCOME_UNKNOWN, Deadline
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES
from bms_core.msgclient.naming import ensure_rpc_name

from bms_msg.broker.backpressure import RateLimiter
from bms_msg.core.config import MS_PER_SECOND, RPC_CANCEL_POLL_MS
from bms_msg.errors import (
    MsgError,
    MsgErrorCode,
    MsgPayloadTooLargeError,
    MsgRateLimitedError,
    MsgRpcDeadlineExceededError,
    MsgRpcInProgressError,
    MsgRpcMethodNotFoundError,
)
from bms_msg.rpc.idempotency import IdempotencyContext
from bms_msg.rpc.registry import RpcCall, RpcHandler, RpcRegistry, RpcReply, RpcStreamHandler

__all__ = ["DisconnectProbe", "exhausted_error", "invoke_rpc", "prepare_handler", "prepare_stream_handler"]

_LOGGER = get_logger("bms_msg.rpc")

DisconnectProbe = Callable[[], Awaitable[bool]]
"""断开探测（端点的 `request.is_disconnected`；服务层不 import ASGI 类型）。"""


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


def _deadline_detail(deadline: Deadline) -> dict[str, object]:
    """构造超时随附数据（`outcome` + 已用 / 剩余毫秒）。

    Args:
        deadline: 预算视图。

    Returns:
        dict[str, object]: `{"outcome": "unknown", "used_ms": …, "remaining_ms": …}`。
    """
    return {
        "outcome": OUTCOME_UNKNOWN,
        "used_ms": deadline.spent_ms(),
        "remaining_ms": deadline.remaining_ms(),
    }


def exhausted_error(deadline: Deadline) -> MsgRpcDeadlineExceededError:
    """构造预算耗尽异常（预检 / 硬兜底 / 流式三处共用，文案与随附数据不分叉）。

    Args:
        deadline: 预算视图。

    Returns:
        MsgRpcDeadlineExceededError: 待抛出的 `350004`。
    """
    return MsgRpcDeadlineExceededError(
        f"RPC 预算耗尽（deadline_ms={deadline.budget_ms}，已用 {deadline.spent_ms()}）",
        data=_deadline_detail(deadline),
    )


def _preflight(
    *,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    deadline: Deadline,
) -> None:
    """调用前的四项判定（形态 → 预算 → 限流 → 参数上限），一元与流式共用一份。

    **顺序不能调**（04-1 §7.2 + 04-2 §5.2）：预算预检在限流之前（耗尽的不消耗配额，否则刷
    `deadline_ms=0` 能打穿 `(tenant, service)` 桶）；参数上限在路由之前（超大参数是客户端问题，
    不该因方法名顺手就先查表）。

    Args:
        limiter: 限流器（RPC 维度独立于生产两级）。
        tenant: 租户编码。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        deadline: 本跳预算视图。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRpcDeadlineExceededError: 预算已耗尽（`350004`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数超过 1 MiB（`350006`）。
    """
    ensure_rpc_name(service, method)
    if deadline.is_exhausted():
        raise exhausted_error(deadline)
    verdict = limiter.check_rpc(tenant=tenant, service=service)
    if not verdict.allowed:
        raise MsgRateLimitedError(f"RPC 调用速率超出配额（{service}）", retry_after_ms=verdict.retry_after_ms)
    if len(payload) > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"RPC 参数超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(payload)}）")


def prepare_handler(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    deadline: Deadline,
) -> RpcHandler:
    """一元调用的前置判定 + 路由（`invoke_rpc` 与流式端点共用同一份判定）。

    拆出来的理由（04-3-1）：流式端点要的是**同一套**判定（形态 / 预算 / 限流 / 参数上限）但另一张
    表的方法（流式表）；复制一份必然漂移，故判定收在 `_preflight()`，两个入口各自只做「查哪张表」。

    Args:
        registry: 方法注册表。
        limiter: 限流器。
        tenant: 租户编码。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        deadline: 本跳预算视图。

    Returns:
        RpcHandler: 已登记的一元处理器。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRpcDeadlineExceededError: 预算已耗尽（`350004`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数超过 1 MiB（`350006`）。
        MsgRpcMethodNotFoundError: 方法未注册（`350023`）。
    """
    _preflight(limiter=limiter, tenant=tenant, service=service, method=method, payload=payload, deadline=deadline)
    return _handler_of(registry, service, method)


def prepare_stream_handler(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    deadline: Deadline,
) -> RpcStreamHandler:
    """流式调用的前置判定 + 路由（判定与一元同一份，只换查表对象）。

    「一元方法当流式调」与「完全未注册」返回同一个码（`350023`）：对调用方而言都是「这个端点上没有
    可流式调用的方法」，区分开只会多一个客户端要处理的码，而排障信息已由 `data.service/method` 给出。

    Args:
        registry: 方法注册表。
        limiter: 限流器。
        tenant: 租户编码。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        deadline: 整条流的预算视图。

    Returns:
        RpcStreamHandler: 已登记的流式处理器。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRpcDeadlineExceededError: 预算已耗尽（`350004`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数超过 1 MiB（`350006`）。
        MsgRpcMethodNotFoundError: 未注册或不是流式方法（`350023`）。
    """
    _preflight(limiter=limiter, tenant=tenant, service=service, method=method, payload=payload, deadline=deadline)
    handler = registry.resolve_stream(service, method)
    if handler is None:
        raise MsgRpcMethodNotFoundError(
            f"RPC 流式方法未注册：{service}.{method}",
            # 普通 dict：自定义 Mapping 走 pydantic 序列化会 500（02-7 在 350015 上验过）。
            data={"service": service, "method": method},
        )
    return handler


def _log_outcome(
    outcome: str,
    *,
    service: str,
    method: str,
    request_id: str | None,
    deadline: Deadline,
    code: int | None = None,
) -> None:
    """记一条 `msg_rpc` 事件（四态唯一出口，两形态共用）。

    Args:
        outcome: `ok` / `business_error` / `deadline_exceeded` / `cancelled`。
        service: 服务标识。
        method: 方法名。
        request_id: 请求标识（帧形态有；JSON 形态无）。
        deadline: 预算视图（用于 `used_ms` / `remaining_ms`）。
        code: 产品码（`ok` / `cancelled` 时为 None）。
    """
    _LOGGER.info(
        "msg_rpc",
        service=service,
        method=method,
        request_id=request_id,
        outcome=outcome,
        code=code,
        deadline_ms=deadline.budget_ms,
        used_ms=deadline.spent_ms(),
        remaining_ms=deadline.remaining_ms(),
    )


async def _cancel_and_wait(task: asyncio.Task[RpcReply]) -> None:
    """取消 handler 任务并等它真正结束（保证 `finally` / 资源释放已经跑完）。

    **必须等**：只 `cancel()` 不等待，`350004` 会先返回，而 handler 的清理还排在事件循环里——
    调用方看到「已停止」但资源还没放，测试也会时灵时不灵。

    Args:
        task: handler 任务。
    """
    task.cancel()
    # handler 自身的异常在这里不重要（主错误由调用方给）：只关心它确实结束了。
    with suppress(asyncio.CancelledError, Exception):
        await task


async def _run_handler(
    handler: RpcHandler,
    call: RpcCall,
    deadline: Deadline,
    disconnected: DisconnectProbe | None,
) -> RpcReply:
    """在剩余预算内执行处理器（到点取消协程并转 `350004`），并按 `RPC_CANCEL_POLL_MS` 探测断开。

    `asyncio.timeout()` 而不是 `wait_for`：不额外包一层「等任务」的语义，取消传播更直接，且能查
    `expired()`——handler 内部自己抛 `TimeoutError`（调下游超时）时不会被误报成本跳预算耗尽。

    **为什么要在执行期探测断开**（实测，非推测）：hypercorn 收到 `RST_STREAM` 只把 `http.disconnect`
    塞进 app 的 receive 队列、**不取消任务**；一元 handler 不写流，所以永远等不到 `CancelledError`
    （03-1 验的是流式响应：写失败才取消生成器）。不探测就变成「客户端走了、服务端跑到预算耗尽」。

    Args:
        handler: 处理器。
        call: 调用入参。
        deadline: 预算视图。
        disconnected: 断开探测（端点的 `request.is_disconnected`）；None 表示不做断开探测。

    Returns:
        RpcReply: 处理结果。

    Raises:
        MsgRpcDeadlineExceededError: 预算到点（`350004`）。
        TimeoutError: handler 自己抛的超时（原样上抛，不吞）。
        asyncio.CancelledError: 请求被取消 / 客户端断开（原样上抛，不写响应）。
    """
    task = asyncio.ensure_future(handler(call))
    timeout = asyncio.timeout(deadline.remaining_ms() / MS_PER_SECOND)
    try:
        async with timeout:
            while True:
                done, _ = await asyncio.wait({task}, timeout=RPC_CANCEL_POLL_MS / MS_PER_SECOND)
                if done:
                    return task.result()
                if disconnected is not None and await disconnected():
                    await _cancel_and_wait(task)
                    raise asyncio.CancelledError
    except TimeoutError as exc:
        if not timeout.expired():
            raise
        await _cancel_and_wait(task)
        raise exhausted_error(deadline) from exc
    finally:
        if not task.done():
            task.cancel()


async def _invoke_once(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    headers: ConcurrentStableDict[str, str],
    deadline: Deadline,
    trace_id: str | None,
    caller: str | None,
    request_id: str | None,
    disconnected: DisconnectProbe | None,
    retry_budget: int | None,
    idempotency_key: str | None,
) -> RpcReply:
    """执行一次一元调用（不含幂等去重；`invoke_rpc` 是带去重的对外入口）。

    Args:
        registry: 方法注册表。
        limiter: 限流器（RPC 维度独立于生产两级）。
        tenant: 租户编码（已由数据面解析链判定）。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头。
        deadline: 本跳预算视图（**必填**：两形态都在收到请求时构造，不允许漏传退化成「无预算」）。
        trace_id: 链路标识。
        caller: 调用方服务标识。
        request_id: 请求标识（帧形态有；只进日志）。
        disconnected: 断开探测（端点传 `request.is_disconnected`；执行期按
            `RPC_CANCEL_POLL_MS` 轮询，命中即取消 handler）。None 表示不做取消探测（单元场景）。

    Returns:
        RpcReply: 结果（payload 已判过上限）。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRpcDeadlineExceededError: 预算已耗尽或执行中超时（`350004`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数或结果超过 1 MiB（`350006`）。
        MsgRpcMethodNotFoundError: 方法未注册（`350023`）。
        MsgError: 处理器抛出的业务错误（原样向上）。
        asyncio.CancelledError: 请求被取消（原样向上，不写响应）。
    """
    try:
        handler = prepare_handler(
            registry=registry,
            limiter=limiter,
            tenant=tenant,
            service=service,
            method=method,
            payload=payload,
            deadline=deadline,
        )
        reply = await _run_handler(
            handler,
            RpcCall(
                service=service,
                method=method,
                payload=payload,
                headers=headers,
                tenant=tenant,
                trace_id=trace_id,
                caller=caller,
                deadline_ms=deadline.downstream_ms(),
                retry_budget=retry_budget,
                idempotency_key=idempotency_key,
            ),
            deadline,
            disconnected,
        )
        if len(reply.payload) > MAX_FRAME_PAYLOAD_BYTES:
            raise MsgPayloadTooLargeError(
                f"RPC 结果超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(reply.payload)}）"
            )
    except MsgRpcDeadlineExceededError:
        _log_outcome(
            "deadline_exceeded",
            service=service,
            method=method,
            request_id=request_id,
            deadline=deadline,
            code=int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
        )
        raise
    except asyncio.CancelledError:
        # 取消不回错误帧（stream 已经没了），只留日志；异常继续上抛，交给请求作用域收口。
        _log_outcome("cancelled", service=service, method=method, request_id=request_id, deadline=deadline)
        raise
    except MsgError as exc:
        _log_outcome(
            "business_error",
            service=service,
            method=method,
            request_id=request_id,
            deadline=deadline,
            code=int(exc.code),
        )
        raise
    _log_outcome("ok", service=service, method=method, request_id=request_id, deadline=deadline)
    return reply


async def invoke_rpc(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    headers: ConcurrentStableDict[str, str],
    deadline: Deadline,
    trace_id: str | None = None,
    caller: str | None = None,
    request_id: str | None = None,
    disconnected: DisconnectProbe | None = None,
    idempotency: IdempotencyContext | None = None,
    retry_budget: int | None = None,
    idempotency_key: str | None = None,
) -> RpcReply:
    """执行一次一元调用（两种形态共用；带键且方法非幂等时按作用域键去重）。

    **去重只包「执行」不包「预检返回」**：`idempotency` 为 `None` 时整层是零开销透传，
    一元与流式既有调用链一行不改。

    Args:
        registry: 方法注册表。
        limiter: 限流器（RPC 维度独立于生产两级）。
        tenant: 租户编码（已由数据面解析链判定）。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头。
        deadline: 本跳预算视图。
        trace_id: 链路标识。
        caller: 调用方服务标识。
        request_id: 请求标识（帧形态有；只进日志）。
        disconnected: 断开探测；None 表示不做取消探测（单元场景）。
        idempotency: 幂等上下文（闸门仅在「带键 + 方法声明非幂等」时构造）。
        retry_budget: 调用链剩余重试预算（透传给 handler）。
        idempotency_key: 幂等键业务部分（透传给 handler）。

    Returns:
        RpcReply: 结果（首次结果或复用结果）。

    Raises:
        MsgRpcInProgressError: 重复请求且首次仍在执行（`10007`）。
        MsgError: 预检 / 执行期的业务错误（原样向上）。
        asyncio.CancelledError: 请求被取消（原样向上，并释放幂等键）。
    """
    if idempotency is None:
        return await _invoke_once(
            registry=registry,
            limiter=limiter,
            tenant=tenant,
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            deadline=deadline,
            trace_id=trace_id,
            caller=caller,
            request_id=request_id,
            disconnected=disconnected,
            retry_budget=retry_budget,
            idempotency_key=idempotency_key,
        )
    store, key, ttl_ms = idempotency.store, idempotency.key, idempotency.ttl_ms
    if not await store.begin(key, ttl_ms=ttl_ms):
        cached = await store.load(key)
        if cached is not None:
            return cached
        raise MsgRpcInProgressError("相同幂等键的请求仍在执行")
    try:
        reply = await _invoke_once(
            registry=registry,
            limiter=limiter,
            tenant=tenant,
            service=service,
            method=method,
            payload=payload,
            headers=headers,
            deadline=deadline,
            trace_id=trace_id,
            caller=caller,
            request_id=request_id,
            disconnected=disconnected,
            retry_budget=retry_budget,
            idempotency_key=idempotency_key,
        )
    except BaseException:
        # 取消 / GeneratorExit 也算：失败或被取消都释放键，让带键重试能真正重跑业务。
        await store.release(key)
        raise
    await store.save(key, reply, ttl_ms=ttl_ms)
    return reply
