"""RPC 服务端流核心（任务 04-3-1）：产出 → 攒批 → 背压 → 断开 / 预算 / 空闲三判据 → 收尾。

**为什么单列一个模块而不是塞进 `rpc/service.py`**：一元的「一进一出」与流式的「一边产一边发」是两种
推进方式，混在一个文件里最容易被后来者按一元思路改流式（比如给整条流套一个 `asyncio.timeout`——
长流必被掐断）。两者共用的是**前置判定**（`prepare_stream_handler`）与错误模型，推进逻辑各写一份。

**服务层不编码帧**（与 `rpc/service.py` 同一取向）：本模块只产 `RpcReply` 与异常，`seq` / 标志位 /
错误帧全在 `api/rpc/` 编码——帧是承载，不是语义。`StreamItem` 因此是「一批结果 + 是否收尾」，
调用方按批编码写出。

**为什么必须攒批**（04-3-1 决策 6 / 详细设计 §5.4）：`yield` 返回即代表「上一批已写出」，若一帧一
`yield`，在途恒 ≤1 帧，`InflightUsage` 双阈值永远不可达（等于装饰）。攒批后「已产出未写出」的帧与
字节才是真实在途量，阈值才真的能暂停生产。

**为什么攒批要留一帧**（本任务实现口径，2026-11-06 确认）：处理器只在**下一次 `anext` 返回
`StopAsyncIteration`** 时才暴露「上一项是末项」，所以「攒满即整批写出」会在恰好卡上界时丢掉末帧判定，
只能补一个空 payload 收尾帧——那会让客户端「N 次产出收到 N+1 个结果」，与任务 §3「帧序与 handler
产出序一致」相抵。留一帧的代价是在途恒 ≥1 帧（阈值下限为 1），换来的是「末帧必为真实产出帧」。
只有处理器一帧不产时才补一个空收尾帧（与详细设计 §6.1② 同形）。

边界：不认识 ASGI / FastAPI，也不认识 h2（帧就是字节，写由调用方做）。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import BizError
from bms_core.core.logging import get_logger
from bms_core.core.roots import BaseValueObject
from bms_core.msgclient.deadline import Deadline
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES

from bms_msg.broker.backpressure import BackpressureTuning, InflightUsage, RateLimiter
from bms_msg.core.config import (
    MS_PER_SECOND,
    RPC_CANCEL_POLL_MS,
    RPC_STREAM_COALESCE_BYTES,
    RPC_STREAM_COALESCE_FRAMES,
    RPC_STREAM_IDLE_MS,
)
from bms_msg.errors import MsgPayloadTooLargeError, MsgRpcDeadlineExceededError, MsgRpcStreamIdleError
from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply
from bms_msg.rpc.service import DisconnectProbe, exhausted_error, prepare_stream_handler

__all__ = [
    "OUTCOME_BUSINESS_ERROR",
    "OUTCOME_CANCELLED",
    "OUTCOME_DEADLINE_EXCEEDED",
    "OUTCOME_ERROR",
    "OUTCOME_IDLE",
    "OUTCOME_OK",
    "StreamItem",
    "open_rpc_stream",
]

_LOGGER = get_logger("bms_msg.rpc")

OUTCOME_OK = "ok"
"""正常结束（处理器迭代完毕，末帧置 `end_of_stream`）。"""

OUTCOME_BUSINESS_ERROR = "business_error"
"""流内业务错误（处理器抛 `BizError`，以 ERROR 帧 + EOF 收尾）。"""

OUTCOME_DEADLINE_EXCEEDED = "deadline_exceeded"
"""整条流预算耗尽（`350004`；`detail.outcome=unknown`，不得直接重试）。"""

OUTCOME_IDLE = "idle"
"""空闲超时（`350024`；与连接心跳分层，只说明这条流没进展）。"""

OUTCOME_CANCELLED = "cancelled"
"""客户端取消 / 断开（不写任何收尾帧，stream 已被 reset）。"""

OUTCOME_ERROR = "error"
"""未预期故障（非 `BizError`）：不写收尾帧，客户端按「无收尾帧即异常中断」判。

沿用 03-5 `_stream_core` 的 `reason="error"` 兜底口径：它是日志里的逃生值，不进五态契约。
"""


@dataclass(frozen=True)
class StreamItem(BaseValueObject):
    """一批已产出的结果（编码由调用方做：`seq` / 标志位 / 错误帧都在 `api/rpc/`）。"""

    replies: tuple[RpcReply, ...]
    """本批结果（顺序即产出顺序；至少 1 项）。"""

    terminated: bool
    """本批是否为「正常结束的末批」——调用方据此给本批末帧置 `flags.end_of_stream`。"""


def _outcome_of(exc: BizError) -> str:
    """按异常类型给 `msg_rpc_stream.outcome`（五态的唯一判定处）。

    Args:
        exc: 流内抛出的业务异常。

    Returns:
        str: `deadline_exceeded` / `idle` / `business_error`。
    """
    if isinstance(exc, MsgRpcDeadlineExceededError):
        return OUTCOME_DEADLINE_EXCEEDED
    if isinstance(exc, MsgRpcStreamIdleError):
        return OUTCOME_IDLE
    return OUTCOME_BUSINESS_ERROR


def _flush_due(pending: list[RpcReply], pending_bytes: int, tuning: BackpressureTuning) -> bool:
    """攒批是否该交出一批（顺序：上界 → 双阈值护栏；**至少 2 帧才交**，末帧要留着判收尾）。

    Args:
        pending: 当前未写出的结果。
        pending_bytes: 其 payload 字节合计。
        tuning: inflight 双阈值（护栏；日常由上界先到）。

    Returns:
        bool: 该交出一批（调用方写出 `pending[:-1]`、留下最后一帧）。
    """
    if len(pending) < 2:
        return False
    if len(pending) >= RPC_STREAM_COALESCE_FRAMES:
        return True
    if pending_bytes >= RPC_STREAM_COALESCE_BYTES:
        return True
    if not tuning.enabled:
        return False
    return InflightUsage(messages=len(pending), bytes=pending_bytes).exhausted(tuning.limit)


def _log_stream(
    *,
    outcome: str,
    service: str,
    method: str,
    request_id: str | None,
    frames: int,
    payload_bytes: int,
    duration_ms: int,
    stalled_ms: int,
) -> None:
    """记一条 `msg_rpc_stream` 事件（一条流一条日志；与一元 `msg_rpc` 靠 `request_id` 对账）。

    Args:
        outcome: 结束原因（五态 + `error` 兜底）。
        service: 服务标识。
        method: 方法名。
        request_id: 请求标识。
        frames: 已产出帧数。
        payload_bytes: 结果 payload 字节合计（不含帧头）。
        duration_ms: 整条流存活毫秒。
        stalled_ms: 两次写出之间的最长等待（回答「背压是否真的发生」）。
    """
    _LOGGER.info(
        "msg_rpc_stream",
        service=service,
        method=method,
        request_id=request_id,
        outcome=outcome,
        frames=frames,
        bytes=payload_bytes,
        seq_max=frames - 1 if frames else None,
        duration_ms=duration_ms,
        stalled_ms=stalled_ms,
    )


async def _cancel_task(task: asyncio.Task[RpcReply] | None) -> None:
    """取消「取下一项」的任务并等它真正结束（保证处理器的 `finally` 已跑完）。

    **必须等**：只 `cancel()` 不等待，收尾帧会先写出，而处理器的资源释放还排在事件循环里——
    调用方看到「已停止」但名额 / 连接还没放，用例也会时灵时不灵（与 04-2 `_cancel_and_wait` 同理）。
    另一个必须等的理由：随后 `_close_iterator()` 要 `aclose()` 同一个生成器，任务没结束时它会报
    `RuntimeError: asynchronous generator is already running`（实测）。

    Args:
        task: 在途的「取下一项」任务；None 表示当前没有在途任务。
    """
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
        await task


async def _close_iterator(stream: AsyncGenerator[RpcReply]) -> None:
    """关闭处理器迭代器（让它的 `finally` / 资源释放确定性发生）。

    `RuntimeError` 一并吞掉：取消路径下（外层任务已被取消）那次 `await task` 会立刻抛
    `CancelledError`，等到 `aclose()` 时生成器可能仍在运行——此时它的清理已由那次取消驱动过，
    关闭失败不构成资源问题，而把异常抛到 `StreamingResponse` 里会变成一条无意义的 500 日志。

    Args:
        stream: 处理器返回的异步生成器。
    """
    with suppress(RuntimeError, StopAsyncIteration, Exception):
        await stream.aclose()


async def open_rpc_stream(
    *,
    registry: RpcRegistry,
    limiter: RateLimiter,
    tenant: str,
    service: str,
    method: str,
    payload: bytes,
    headers: ConcurrentStableDict[str, str],
    deadline: Deadline,
    tuning: BackpressureTuning,
    trace_id: str | None = None,
    caller: str | None = None,
    request_id: str | None = None,
    disconnected: DisconnectProbe | None = None,
    now: Callable[[], float] = time.monotonic,
) -> AsyncGenerator[StreamItem]:
    """执行一条服务端流（产出 → 攒批 → 背压 → 三判据），逐批交出结果。

    判定顺序与 04-1 / 04-2 一致（详细设计 §5.3）：前置判定（形态 → 预算 → 限流 → 参数上限 → 路由）
    收在 `prepare_stream_handler()`；每次向处理器索取下一项之前判三条——断开（50ms 轮询）→ 空闲
    （距上次写出超 `RPC_STREAM_IDLE_MS`）→ 预算（`is_exhausted`）。

    Args:
        registry: 方法注册表。
        limiter: 限流器（`(tenant, service)` 桶；与一元同一本账）。
        tenant: 租户编码。
        service: 服务标识。
        method: 方法名。
        payload: 调用参数原始字节。
        headers: 结构化头（透传给处理器）。
        deadline: **整条流共享**的预算视图（不按帧重置，否则长流等于没有预算）。
        tuning: inflight 双阈值（护栏；用例注入小阈值即可覆盖暂停分支）。
        trace_id: 链路标识。
        caller: 调用方服务标识。
        request_id: 请求标识（只进日志）。
        disconnected: 断开探测（端点的 `request.is_disconnected`）；None 表示不探测。
        now: 单调时钟（用例可注入固定值，避免 sleep 猜时序）。

    Yields:
        StreamItem: 一批结果 + 是否收尾（`terminated=True` 表示正常结束）。

    Raises:
        ParamError: 路径参数形态非法（`10001`）。
        MsgRpcDeadlineExceededError: 预算已耗尽或推流中耗尽（`350004`）。
        MsgRateLimitedError: RPC 速率超配额（`350005`）。
        MsgPayloadTooLargeError: 参数或单帧结果超过 1 MiB（`350006`）。
        MsgRpcMethodNotFoundError: 未注册或不是流式方法（`350023`）。
        MsgRpcStreamIdleError: 空闲超时（`350024`）。
        BizError: 处理器抛出的业务错误（原码；调用方转 ERROR 帧）。
        asyncio.CancelledError: 客户端取消 / 断开（调用方不写收尾帧）。
    """
    started = now()
    last_emit = started
    stalled_ms = 0
    frames = 0
    payload_bytes = 0
    outcome = OUTCOME_OK
    pending: list[RpcReply] = []
    pending_bytes = 0
    task: asyncio.Task[RpcReply] | None = None
    stream: AsyncGenerator[RpcReply] | None = None
    poll_seconds = RPC_CANCEL_POLL_MS / MS_PER_SECOND
    try:
        # 前置判定放进 `try`：预检失败（`350023` / `350005` / `350006` / `350004`）也要落一条
        # `msg_rpc_stream`，否则「建流前的失败」在流式视角里是完全不可见的。
        handler = prepare_stream_handler(
            registry=registry,
            limiter=limiter,
            tenant=tenant,
            service=service,
            method=method,
            payload=payload,
            deadline=deadline,
        )
        # 处理器契约是 `AsyncIterator`（只承诺逐项产出），收尾要靠 `aclose()`——生成器实现都带它，
        # 这里在收口处显式声明「按异步生成器处理」：自定义迭代器不实现 `aclose` 属装配违约，
        # 由 `_cancel_task` 的取消路径兜底（不会因此漏放名额）。
        stream = cast(
            "AsyncGenerator[RpcReply]",
            handler(
                RpcCall(
                    service=service,
                    method=method,
                    payload=payload,
                    headers=headers,
                    tenant=tenant,
                    trace_id=trace_id,
                    caller=caller,
                    deadline_ms=deadline.downstream_ms(),
                )
            ),
        )
        while True:
            # 三条判据在**每次索取下一项之前**判：断连优先（stream 已没了，写什么都没意义），
            # 其次空闲（这条流没进展），最后预算（整条流没时间了）。
            if disconnected is not None and await disconnected():
                raise asyncio.CancelledError
            idle_ms = int((now() - last_emit) * MS_PER_SECOND)
            if idle_ms >= RPC_STREAM_IDLE_MS:
                raise MsgRpcStreamIdleError(
                    f"RPC 流空闲超时（{idle_ms}ms 无帧写出）",
                    data={"idle_ms": RPC_STREAM_IDLE_MS, "idle_elapsed_ms": idle_ms},
                )
            if deadline.is_exhausted():
                raise exhausted_error(deadline)
            if task is None:
                task = asyncio.ensure_future(stream.__anext__())
            done, _ = await asyncio.wait({task}, timeout=poll_seconds)
            if not done:
                if pending:
                    # 处理器停顿超过一个轮询周期（50ms）：把手上的帧写出去，再回到循环头重判三条。
                    # **这条分支是「攒批不吞时延」的保证**：只攒「连续到位」的那一串，处理器一停就交付，
                    # 否则单帧流要等到下一条产出（或流结束）才可见，长间隔流会整体晚一拍。
                    stalled_ms = max(stalled_ms, int((now() - last_emit) * MS_PER_SECOND))
                    yield StreamItem(tuple(pending), terminated=False)
                    last_emit = now()
                    pending = []
                    pending_bytes = 0
                continue
            try:
                reply = task.result()
            except StopAsyncIteration:
                task = None
                break  # 处理器迭代结束 = 正常结束
            task = None
            if len(reply.payload) > MAX_FRAME_PAYLOAD_BYTES:
                raise MsgPayloadTooLargeError(
                    f"RPC 流结果超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(reply.payload)}）"
                )
            pending.append(reply)
            pending_bytes += len(reply.payload)
            frames += 1
            payload_bytes += len(reply.payload)
            if _flush_due(pending, pending_bytes, tuning):
                # 留最后一帧：它可能正是末帧，标志位要在下一次 anext 之后才敢定。
                mark = now()
                stalled_ms = max(stalled_ms, int((mark - last_emit) * MS_PER_SECOND))
                yield StreamItem(tuple(pending[:-1]), terminated=False)
                last_emit = now()
                keep = pending[-1]
                pending = [keep]
                pending_bytes = len(keep.payload)
    except BizError as exc:
        outcome = _outcome_of(exc)
        if pending:
            # 已产出的结果先交付，再让调用方补 ERROR 帧：错误帧必须是最后一帧。
            yield StreamItem(tuple(pending), terminated=False)
            pending = []
            pending_bytes = 0
        raise
    except asyncio.CancelledError, GeneratorExit:
        outcome = OUTCOME_CANCELLED
        raise
    except Exception:
        # 非 BizError 的处理器故障：不写收尾帧（客户端按「无收尾帧即中断」判），
        # 日志给逃生值 outcome=error，避免在五态里硬凑一个语义。
        outcome = OUTCOME_ERROR
        raise
    else:
        if not pending:
            # 处理器一帧没产（空流）：补一个空 payload 收尾帧，客户端仍拿到确定的「正常结束」。
            pending.append(RpcReply())
        yield StreamItem(tuple(pending), terminated=True)
        pending = []
    finally:
        # 顺序不能反：先取消在途的「取下一项」，再关生成器。反过来会撞
        # `RuntimeError: aclose(): asynchronous generator is already running`（实测）。
        await _cancel_task(task)
        if stream is not None:
            await _close_iterator(stream)
        _log_stream(
            outcome=outcome,
            service=service,
            method=method,
            request_id=request_id,
            frames=frames,
            payload_bytes=payload_bytes,
            duration_ms=int((now() - started) * MS_PER_SECOND),
            stalled_ms=stalled_ms,
        )
