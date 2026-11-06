"""客户端流式消费（任务 04-3-2）：`RpcStream` 句柄——惰性打开、逐帧产出、可确定性取消。

**为什么是句柄而不是 `async generator`**：生成器在调用方 `break` 之后**不会被立刻 `aclose()`**
（语言层面不通知迭代器），取消时机取决于 GC，正是 04-2 / 04-7 花整轮修掉的那类问题。句柄把
「何时释放」交给调用方显式表达：`async with` 退出即发 `RST_STREAM(CANCEL)`，`aclose()` 幂等。

**惰性打开**：`RpcClient.stream()` 只组装请求，不碰网络；`__aenter__` / 首次 `__anext__` 才发。
「构造即发请求」会让调用方拿到句柄却没消费时，服务端白跑一条流（04-2 的病根）。

**职责边界**：

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| 传输层（`client/transport/`） | 字节：取连接、发请求、逐块读、发 `RST_STREAM` | 帧、序号、结束语义 |
| 本模块 | 帧：增量切帧、`seq` / `request_id` 校验、结束三态、取消与观测 | 连接、窗口、h2 状态机 |

**不套用一元的整段本地超时**：整条流的期限由服务端预算兜底（到点发 `350004`）；本模块唯一的计时器是
「单次 `read()` 连续 `RPC_STREAM_CLIENT_IDLE_MS`（90s）没有任何字节」。把「预算 + 余量」包到整条流上，
预算 30s 的流 30s 后必被本地掐断——哪怕服务端正正常推，长流等于不可用。

**`break` 不自动发 RST**（已知行为，用例固定）：`async for` 的 `break` 只是不再 `__anext__`，不会通知
任何人。因此 docstring 只给 `async with` 一种写法；后续 SDK 封装（8B_03）也必须强制上下文。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Self

from bms_core.core.logging import get_logger

from bms_msg.client.rpc import RpcCallError, RpcCallTimeoutError, error_of_frame, frame_headers
from bms_msg.client.transport import (
    RpcHttpRequest,
    RpcTransport,
    RpcTransportError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
    TransportStream,
)
from bms_msg.client.transport.stream import CLOSE_CANCELLED, CLOSE_ERROR, CLOSE_TIMEOUT
from bms_msg.core.config import MS_PER_SECOND, RPC_STREAM_CLIENT_IDLE_MS
from bms_msg.errors import MsgError
from bms_msg.protocol.frame import Frame, FrameFlags, FrameType, decode_frames
from bms_msg.rpc.registry import RpcReply

__all__ = ["RpcStream"]

_LOGGER = get_logger("bms_msg.rpc.client")
"""与服务端 `msg_rpc` 同一命名族：`msg_rpc_client_stream` 给「流正常跑完」，`msg_rpc_client_cancel`
给「我不看了」，两条靠 `request_id` / `stream_id` 对账。"""


class RpcStream:
    """一条流式调用的句柄（单消费者；`async for` + `async with` + `aclose()`）。"""

    def __init__(
        self,
        *,
        transport: RpcTransport,
        request: RpcHttpRequest,
        request_id: str,
        service: str,
        method: str,
        budget_ms: int,
    ) -> None:
        """初始化（**不发请求**；何时打开由 `__aenter__` / 首次 `__anext__` 决定）。

        Args:
            transport: 传输层（用例注入替身，生产是 `H2Transport`）。
            request: 已组装好的传输层请求（`target` 指向 `/stream`）。
            request_id: 本次请求标识（逐帧校验用）。
            service: 服务标识（日志用）。
            method: 方法名（日志用）。
            budget_ms: 整条流的预算（毫秒；只随请求发给服务端，不做本地倒计时）。
        """
        self._transport = transport
        self._request = request
        self._request_id = request_id
        self._service = service
        self._method = method
        self._budget_ms = budget_ms
        self._opener: AbstractAsyncContextManager[TransportStream] | None = None
        self._stream: TransportStream | None = None
        # 切帧缓冲挂在句柄上而不是 `__anext__` 的局部变量：一帧可能横跨两次 `read()`，局部变量
        # 会在「本批已产出一帧就 return」时把尾部的半帧丢掉（设计 §5.3 的伪代码是局部 buffer）。
        # **用 `bytes` 而不是 `bytearray`**：`decode_frames()` 交回的 `Frame.payload` 是缓冲的
        # 视图，视图还活着时 `del bytearray[...]` 会抛 `BufferError`（不能缩容有导出的缓冲）。
        # `bytes` 切片另开一块，旧缓冲被视图继续引用，互不影响。
        self._buffer = b""
        self._pending: deque[RpcReply] = deque()
        self._deferred: RpcCallError | None = None
        self._expected_seq = 0
        self._frames = 0
        self._bytes = 0
        self._ended = False
        self._failed = False
        self._closed = False
        self._started_at = 0.0

    # ------------------------------------------------------------------ 上下文与迭代协议

    async def __aenter__(self) -> Self:
        """进入上下文（**这里才发请求**）。

        Returns:
            Self: 句柄自身。

        Raises:
            RpcCallTimeoutError: 打开阶段超时。
            RpcCallError: 打开失败（传输层 / 协议层 / 服务端拒绝）。
        """
        await self._open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """退出上下文：流未结束则发 `RST_STREAM(CANCEL)`（**不抛流内异常**）。

        参数未使用：这里只做清理。清理路径上再抛异常会把调用方真正关心的异常盖掉——`aclose()` 的
        语义是「我不看了」，不是「我看错了」。

        Args:
            exc_type: 异常类型（未使用）。
            exc: 异常实例（未使用）。
            traceback: 栈（未使用）。
        """
        await self.aclose()

    def __aiter__(self) -> Self:
        """返回自身（单消费者，不支持并发 `__anext__`）。

        Returns:
            Self: 句柄自身。
        """
        return self

    async def __anext__(self) -> RpcReply:
        """产出下一帧结果。

        Returns:
            RpcReply: 本帧结果（``headers`` 为该帧的结果头）。

        Raises:
            StopAsyncIteration: 已收到末帧（`end_of_stream`）。
            RpcCallError: 流内 `ERROR` 帧 / 帧形态或序号不符 / 流异常中断。
            RpcCallTimeoutError: 单次读取空闲超时，或流内 `350004`。
        """
        if self._pending:
            return self._pending.popleft()
        if self._ended:
            raise StopAsyncIteration
        stream = await self._open()
        # 循环里不再判 `_ended`：末帧一定是**先入 `_pending` 再置位**的（`_result_reply` 的顺序），
        # 所以「pending 空且 ended」只可能发生在下一次 `__anext__` 的入口——那里已经判过。
        while True:
            self._slice_frames()
            if self._pending:
                return self._pending.popleft()
            if self._deferred is not None:
                raise self._deferred
            await self._fill(stream)

    # ------------------------------------------------------------------ 打开与读取

    async def _open(self) -> TransportStream:
        """打开流（幂等：已打开直接返回）。

        Returns:
            TransportStream: 可逐块读的传输流。

        Raises:
            RpcCallTimeoutError: 打开阶段超时。
            RpcCallError: 传输 / 协议层失败，或流端点返回非 200。
        """
        if self._stream is not None:
            return self._stream
        if self._closed:
            raise RpcCallError(0, f"RPC 流已关闭：{self._service}.{self._method}", layer="transport")
        opener = self._transport.open_stream(self._request)
        try:
            stream = await opener.__aenter__()
        except RpcTransportTimeoutError as exc:
            raise RpcCallTimeoutError(
                f"RPC 流打开超时（{self._budget_ms} ms）：{self._service}.{self._method}",
                budget_ms=self._budget_ms,
            ) from exc
        except RpcTransportProtocolError as exc:
            raise RpcCallError(0, f"RPC 流不可解析：{exc}", layer="protocol") from exc
        except RpcTransportError as exc:
            raise RpcCallError(0, f"RPC 流打开失败：{exc}", layer="transport") from exc
        self._opener = opener
        self._stream = stream
        self._started_at = time.monotonic()
        return stream

    async def _fill(self, stream: TransportStream) -> None:
        """读一块字节进缓冲；`None` 即「`END_STREAM` 但没收到收尾帧」。

        Args:
            stream: 已打开的传输流。

        Raises:
            RpcCallError: EOF 前未收到 `end_of_stream` / `ERROR` 帧（协议错）。
        """
        chunk = await self._read_chunk(stream)
        if chunk is None:
            raise self._protocol_error("RPC 流异常中断（EOF 前未收到 end_of_stream 或 ERROR 帧）")
        self._buffer += chunk

    async def _read_chunk(self, stream: TransportStream) -> bytes | None:
        """带空闲兜底地读一块。

        Args:
            stream: 已打开的传输流。

        Returns:
            bytes | None: 下一块字节或 None。

        Raises:
            RpcCallTimeoutError: 单次读取超过空闲上限（已发 `RST_STREAM(CANCEL)`）。
            RpcCallError: 传输层错误（对端 reset / 连接故障 / 帧不可解析）。
        """
        try:
            async with asyncio.timeout(RPC_STREAM_CLIENT_IDLE_MS / MS_PER_SECOND):
                return await stream.read()
        except TimeoutError as exc:
            self._abort(CLOSE_TIMEOUT)
            raise RpcCallTimeoutError(
                f"RPC 流空闲超时（{RPC_STREAM_CLIENT_IDLE_MS} ms 无数据）：{self._service}.{self._method}",
                budget_ms=self._budget_ms,
            ) from exc
        except asyncio.CancelledError:
            # 调用方取消：本流立即中止（否则服务端会一直推），异常原样穿透（04-7-2 口径）。
            self._abort(CLOSE_CANCELLED)
            raise
        except RpcTransportProtocolError as exc:
            self._abort(CLOSE_ERROR)
            raise RpcCallError(0, f"RPC 流不可解析：{exc}", layer="protocol") from exc
        except RpcTransportError as exc:
            self._abort(CLOSE_ERROR)
            raise RpcCallError(0, f"RPC 流中断：{exc}", layer="transport") from exc

    # ------------------------------------------------------------------ 切帧与校验

    def _slice_frames(self) -> None:
        """把缓冲里的完整帧切出来放进 `_pending`（不完整尾包留在 `_buffer` 等下一块）。

        Raises:
            RpcCallError: 帧结构非法（`decode_frames` 拒绝）。
        """
        if not self._buffer:
            return
        try:
            batch = decode_frames(self._buffer)
        except MsgError as exc:
            raise self._protocol_error(f"RPC 流帧不可解析：{exc}") from exc
        self._buffer = self._buffer[batch.consumed :]
        for frame in batch.frames:
            if frame.frame_type == int(FrameType.ERROR):
                # **错误帧是终止帧**：不再切后续字节，也不再 abort（服务端写完就收尾了）。
                # 已产出的结果留在 `_pending` 里先交付，错误等它们吐完再抛（`_deferred`）。
                self._failed = True
                self._deferred = error_of_frame(frame.header, budget_ms=self._budget_ms)
                return
            self._pending.append(self._result_reply(frame))

    def _result_reply(self, frame: Frame) -> RpcReply:
        """校验并转换一帧结果（`48` + response 位 + `request_id` / `seq` 一致）。

        Args:
            frame: 已解出的帧。

        Returns:
            RpcReply: 本帧结果。

        Raises:
            RpcCallError: 帧类型 / `request_id` / `seq` 不符（`code=0`，`layer=protocol`）。
        """
        if frame.frame_type != int(FrameType.RPC) or not frame.flags & FrameFlags.RESPONSE:
            raise self._protocol_error(f"流内帧类型不符（type={frame.frame_type}）")
        request_id = frame.header.get("request_id")
        if request_id != self._request_id:
            raise self._protocol_error(f"流内帧 request_id 不符（期望 {self._request_id}，实际 {request_id!r}）")
        seq = frame.header.get("seq")
        if not isinstance(seq, int) or seq != self._expected_seq:
            raise self._protocol_error(f"流内帧 seq 不符（期望 {self._expected_seq}，实际 {seq!r}）")
        payload = bytes(frame.payload)
        self._expected_seq += 1
        self._frames += 1
        self._bytes += len(payload)
        reply = RpcReply(payload=payload, headers=frame_headers(frame.header.get("headers")))
        if frame.flags & FrameFlags.END_OF_STREAM:
            # 末帧照常产出，下一次 `__anext__` 才 `StopAsyncIteration`（迭代协议要求「先给完再停」）。
            self._ended = True
            self._log_finished()
        return reply

    def _protocol_error(self, message: str) -> RpcCallError:
        """记一条中止并构造协议错（**唯一构造处**：既定的「`code=0` + `layer=protocol`」）。

        为什么协议错给 `code=0`：这类错误不是服务端判定的业务结果（它可能什么都没回），给一个产品码
        会诱导调用方按码重试。04-7-1 的异常梯度已确立同一口径。

        Args:
            message: 可读提示。

        Returns:
            RpcCallError: 待抛出的错误。
        """
        self._abort(CLOSE_ERROR)
        return RpcCallError(0, message, layer="protocol")

    # ------------------------------------------------------------------ 收尾与观测

    async def aclose(self) -> None:
        """关闭句柄（幂等；**流内异常不在这里抛**）。

        流未结束则发 `RST_STREAM(CANCEL)` 并记 `msg_rpc_client_cancel`；已正常结束 / 已因错误收口 /
        从未打开（惰性）则什么都不做。
        """
        if self._closed:
            return
        self._closed = True
        self._abort(CLOSE_CANCELLED)
        if self._opener is not None:
            await self._opener.__aexit__(None, None, None)

    def _abort(self, reason: str) -> bool:
        """中止本流并记一条取消事件（**唯一出口**，幂等）。

        已正常结束 / 已因错误收口 / 从未打开三种情况直接返回：前者没有可中止的东西，后者的 RST 早在
        各自的出口发过了——重复记日志会让「取消了几条流」这件事对不上账。

        Args:
            reason: `cancelled` / `timeout` / `error`。

        Returns:
            bool: 本次是否真的发出了 `RST_STREAM`。
        """
        if self._failed or self._ended or self._stream is None:
            return False
        self._failed = True
        sent = self._stream.abort(reason)
        _LOGGER.info(
            "msg_rpc_client_cancel",
            request_id=self._request_id,
            service=self._service,
            method=self._method,
            stream_id=self._stream.stream_id,
            reason=reason,
            rst_sent=sent,
            elapsed_ms=int((time.monotonic() - self._started_at) * MS_PER_SECOND),
            frames=self._frames,
        )
        return sent

    def _log_finished(self) -> None:
        """记一条 `msg_rpc_client_stream`（正常结束；与取消事件分开，靠 `stream_id` 对账）。"""
        _LOGGER.info(
            "msg_rpc_client_stream",
            request_id=self._request_id,
            service=self._service,
            method=self._method,
            stream_id=None if self._stream is None else self._stream.stream_id,
            frames=self._frames,
            bytes=self._bytes,
            seq_max=self._frames - 1 if self._frames else None,
            duration_ms=int((time.monotonic() - self._started_at) * MS_PER_SECOND),
        )
