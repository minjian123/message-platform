"""测试辅助：三个 `RpcTransport` 实现（任务 04-7-1 / 04-3-2）。

**为什么要自己写**：客户端用例原来靠 `httpx.ASGITransport` / `httpx.MockTransport` 注入，等于「测试走 httpx、
生产走 h2」——04-2 的「测试全绿、真机不发 `RST_STREAM`」正是这么来的。换成同一份 `RpcTransport` 契约后，
`RpcClient` 里不再有「测试才走的分支」，生产与测试只差「字节往哪送」。

- `AsgiRpcTransport`：进程内驱动真 app（真路由 → 真 handler → 真帧），替代 `ASGITransport`；
  流式面上**边产边读**——`send` 把每块推进队列、读侧逐块取，断开放到 `receive()` 里让服务端的
  50ms 断开探测命中（一次性收齐就测不出背压与取消，04-3-1 §6.2 的教训）。
- `ScriptedRpcTransport`：按脚本返响应 / 抛传输异常 / 逐块吐字节，替代 `MockTransport` 的错误分支。
  流式用例需要「一帧跨两块」「一次读含三帧」「读到最后挂住」这类确定性时序，只有脚本能精确摆出来。
- `ScriptedStream`：脚本流的流对象（`TransportStream` 实现）。

三者都**不算覆盖率分母**（`coverage_packages` 只含 `bms_msg` / `services` / `ops`），但契约必须与生产
一致：`abort()` 已收口返回 `False` 且不发帧、`read()` 结束回 `None`、中止后 `read()` 抛
`RpcTransportClosedError`——与 `H2Stream` 同形。
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from contextlib import AbstractAsyncContextManager, suppress
from typing import Any, cast
from urllib.parse import urlsplit

from fastapi import FastAPI

from bms_msg.client.transport import (
    RpcHttpRequest,
    RpcHttpResponse,
    RpcTransportClosedError,
    RpcTransportError,
    TransportStats,
    TransportStream,
)

__all__ = [
    "AsgiRpcTransport",
    "ScriptedRpcTransport",
    "ScriptedStream",
    "TransportHandler",
]

TransportHandler = Callable[[RpcHttpRequest], Awaitable[RpcHttpResponse]]
"""脚本传输的一元处理器类型（返回响应或抛传输异常）。"""

_ASGI_EXIT_GRACE_SECONDS = 2.0
"""等待应用自行响应断开的上限（秒）：服务端断开探测 50ms 一轮，2s 是「它根本没在探测」的判据。"""


def _scope_of(request: RpcHttpRequest) -> dict[str, Any]:
    """把传输层请求折成 ASGI `scope`。

    Args:
        request: 传输层入参。

    Returns:
        dict[str, Any]: ASGI scope。
    """
    parsed = urlsplit(request.origin)
    path, _, query = request.target.partition("?")
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": parsed.scheme,
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query.encode("utf-8"),
        "root_path": "",
        "headers": [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in request.headers],
        "client": ("127.0.0.1", 0),
        "server": (parsed.hostname or "test", parsed.port or 80),
    }


class AsgiRpcTransport:
    """在进程内驱动 ASGI 应用（不起服务、不碰 socket）。"""

    def __init__(self, app: FastAPI) -> None:
        """初始化。

        Args:
            app: 目标应用（测试里是 `build_frame_app(...)` 的产物）。
        """
        self._app = app
        self.closed = False

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """把传输层请求折成 ASGI 调用并收集响应。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 应用产出的状态码、头与体。
        """
        collected: MutableMapping[str, Any] = {"status": 0, "headers": (), "body": bytearray()}
        delivered = False
        finished = asyncio.Event()

        async def receive() -> MutableMapping[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": request.body, "more_body": False}
            # 请求体给完之后必须「挂住」而不是立刻回 `http.disconnect`：服务端 04-2 的断开探测
            # 会轮询 `request.is_disconnected()`，立刻回 disconnect 等于宣告「客户端跑了」，
            # 于是每个 handler 都被当成取消（真服务端只在真的断开时才会给这条消息）。
            await finished.wait()
            return {"type": "http.disconnect"}

        async def send_message(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.start":
                collected["status"] = message["status"]
                collected["headers"] = message.get("headers", ())
            elif message["type"] == "http.response.body":
                collected["body"].extend(message.get("body", b""))
                if not message.get("more_body", False):
                    finished.set()

        await self._app(_scope_of(request), receive, send_message)
        headers = tuple((name.decode("latin-1"), value.decode("latin-1")) for name, value in collected["headers"])
        return RpcHttpResponse(
            status=int(collected["status"]),
            headers=headers,
            body=bytes(collected["body"]),
            stream_id=0,
        )

    def open_stream(self, request: RpcHttpRequest) -> AbstractAsyncContextManager[TransportStream]:
        """打开一条进程内流（边产边读）。

        Args:
            request: 传输层入参。

        Returns:
            AbstractAsyncContextManager[TransportStream]: 进入后拿到可逐块读的 `_AsgiStream`。
        """
        return _AsgiStreamOpener(self._app, request)

    async def aclose(self) -> None:
        """标记关闭（无连接可释放）。"""
        self.closed = True

    def stats(self) -> TransportStats:
        """进程内传输没有连接概念。

        Returns:
            TransportStats: 恒零。
        """
        return TransportStats(connections=0, open_streams=0)


class _AsgiStream:
    """`TransportStream` 的进程内实现：`send` 推队列、读侧逐块取、`abort` 桥接成 `http.disconnect`。"""

    def __init__(self, request: RpcHttpRequest) -> None:
        """初始化（不做任何 IO）。

        Args:
            request: 传输层入参。
        """
        self._request = request
        self._queue: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
        self._body_sent = False
        self._disconnect = asyncio.Event()
        self._finished = False
        self._aborted = False
        self._task: asyncio.Task[None] | None = None

    @property
    def stream_id(self) -> int:
        """进程内流没有 h2 流号（恒 0，与 `send()` 的 `stream_id=0` 同口径）。

        Returns:
            int: 恒 0。
        """
        return 0

    async def _receive(self) -> dict[str, Any]:
        """给应用的 `receive`（请求体一次给完，之后只在断开时返回）。

        **断开必须同步返回**：Starlette 的 `is_disconnected()` 把 `receive` 放进「立刻取消」的作用域里
        探一次，凡是要 `await` 才能就绪的消息它都看不见（04-3-1 §6.2 实测）。

        Returns:
            dict[str, Any]: ASGI 消息。
        """
        if not self._body_sent:
            self._body_sent = True
            return {"type": "http.request", "body": self._request.body, "more_body": False}
        if self._disconnect.is_set():
            return {"type": "http.disconnect"}
        await self._disconnect.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message: MutableMapping[str, Any]) -> None:
        """把应用写出的每一条消息推进队列（不在这里阻塞，读侧决定节奏）。

        Args:
            message: ASGI 响应消息。
        """
        if message["type"] == "http.response.start":
            self._queue.put_nowait(("start", int(message["status"])))
        elif message["type"] == "http.response.body":
            body = bytes(message.get("body", b""))
            if body:
                self._queue.put_nowait(("chunk", body))
            if not message.get("more_body", False):
                self._queue.put_nowait(("end", None))

    async def open(self, app: FastAPI) -> None:
        """起应用任务并等到响应头（非 200 在此抛出，与 h2 实现同口径）。

        Args:
            app: 目标应用。

        Raises:
            RpcTransportClosedError: 响应头之前应用就出错 / 结束，或状态码非 200。
        """

        async def run() -> None:
            try:
                await app(_scope_of(self._request), self._receive, self._send)
            except BaseException as exc:  # 应用异常（含取消）经队列交给读侧，不在后台任务里吞掉
                self._queue.put_nowait(("error", exc))
            finally:
                self._queue.put_nowait(("end", None))

        self._task = asyncio.create_task(run())
        kind, value = await self._queue.get()
        if kind != "start":
            raise RpcTransportClosedError(f"ASGI 应用在响应头之前结束：{value!r}")
        status = int(cast("int", value))
        if status != 200:
            await self.shutdown()
            raise RpcTransportClosedError(f"RPC 流端点返回 HTTP {status}：{self._request.target}")

    async def read(self) -> bytes | None:
        """读下一块（`None` = 应用已结束响应体）。

        Returns:
            bytes | None: 下一块字节或 None。

        Raises:
            RpcTransportClosedError: 本流已被本地中止，或应用抛出了非传输层异常。
            RpcTransportError: 应用抛出的传输层异常（原样上抛）。
        """
        if self._aborted:
            raise RpcTransportClosedError("本流已被本地中止（ASGI 替身）")
        while True:
            kind, value = await self._queue.get()
            if kind == "chunk":
                return cast("bytes", value)
            if kind == "end":
                self._finished = True
                return None
            if isinstance(value, RpcTransportError):
                raise value
            raise RpcTransportClosedError(f"ASGI 应用异常：{value!r}")

    def abort(self, reason: str | None = None) -> bool:
        """中止本流（桥接成 `http.disconnect`，让服务端的断开探测真的命中）。

        Args:
            reason: 中止原因（替身只记录，不参与断言）。

        Returns:
            bool: 本次是否真的中止了（已收口 / 已中止返回 False）。
        """
        del reason
        if self._aborted or self._finished:
            return False
        self._aborted = True
        self._disconnect.set()
        return True

    async def shutdown(self) -> None:
        """置断开并等应用自行收尾（超时则由 `wait_for` 取消任务）。

        走「置断开」而不是直接 `cancel()`：服务端的取消路径就是 50ms `is_disconnected` 轮询，替身照这条
        路走，用例才验得到真实机制；`wait_for` 的超时是「它根本没在探测」的兜底。
        """
        self._disconnect.set()
        task = self._task
        if task is None:
            return
        with suppress(asyncio.CancelledError, TimeoutError, Exception):
            await asyncio.wait_for(task, timeout=_ASGI_EXIT_GRACE_SECONDS)

    async def __aexit__(self, *_: object) -> None:
        """退出：交给 `shutdown()`。"""
        await self.shutdown()


class _AsgiStreamOpener(AbstractAsyncContextManager[TransportStream]):
    """`AsgiRpcTransport.open_stream()` 的异步上下文管理器（进入时才起应用任务）。"""

    def __init__(self, app: FastAPI, request: RpcHttpRequest) -> None:
        """初始化。

        Args:
            app: 目标应用。
            request: 传输层入参。
        """
        self._app = app
        self._stream = _AsgiStream(request)

    async def __aenter__(self) -> TransportStream:
        """起应用任务并等到响应头。

        Returns:
            TransportStream: 可逐块读的流。

        Raises:
            RpcTransportClosedError: 应用在响应头前结束，或状态码非 200。
        """
        await self._stream.open(self._app)
        return self._stream

    async def __aexit__(self, *_: object) -> None:
        """退出：交给流自己置断开并收尾。"""
        await self._stream.__aexit__(None, None, None)


class ScriptedStream:
    """脚本流：按顺序交出块，可选在读完后抛错或挂住（覆盖「空闲超时」这类没有服务端的路径）。"""

    def __init__(
        self,
        chunks: Sequence[bytes],
        *,
        error: Exception | None = None,
        hold: bool = False,
        aborts: list[str | None] | None = None,
    ) -> None:
        """初始化。

        Args:
            chunks: 依次交出的块。
            error: 块交完后抛出的异常（只抛一次；None 表示正常结束）。
            hold: 块交完后是否挂住不返回（模拟「服务端不发也不断」）。
            aborts: 中止记录（用例断言 `reason` 用；None 表示不记录）。
        """
        self._chunks = deque(chunks)
        self._error = error
        self._hold = hold
        self._held = asyncio.Event()
        self._finished = False
        self._aborted = False
        self.aborts: list[str | None] = [] if aborts is None else aborts

    @property
    def stream_id(self) -> int:
        """脚本流没有 h2 流号（恒 7，取非 0 值以便断言「日志里带的是它」）。

        Returns:
            int: 恒 7。
        """
        return 7

    async def read(self) -> bytes | None:
        """读下一块。

        Returns:
            bytes | None: 下一块字节或 None（正常结束）。

        Raises:
            RpcTransportError: 已中止，或脚本指定的错误。
        """
        if self._aborted:
            raise RpcTransportClosedError("脚本流已被本地中止")
        if self._chunks:
            return self._chunks.popleft()
        if self._error is not None:
            error, self._error = self._error, None
            raise error
        if self._hold:
            await self._held.wait()
            raise RpcTransportClosedError("脚本流已中止")
        self._finished = True
        return None

    def abort(self, reason: str | None = None) -> bool:
        """中止本流（已收口返回 False，与 `H2Stream` 同形）。

        Args:
            reason: 中止原因。

        Returns:
            bool: 本次是否发了帧。
        """
        if self._aborted or self._finished:
            return False
        self._aborted = True
        self.aborts.append(reason)
        self._held.set()
        return True


class _ScriptedStreamOpener(AbstractAsyncContextManager[TransportStream]):
    """`ScriptedRpcTransport.open_stream()` 的异步上下文管理器（进入时才「发请求」）。"""

    def __init__(self, transport: ScriptedRpcTransport, request: RpcHttpRequest) -> None:
        """初始化。

        Args:
            transport: 所属传输层（记录打开过的请求与中止原因）。
            request: 传输层入参。
        """
        self._transport = transport
        self._request = request

    async def __aenter__(self) -> TransportStream:
        """登记一次「打开」并交出脚本流。

        Returns:
            TransportStream: 脚本流。

        Raises:
            Exception: 脚本指定的打开异常（`transport.open_error`）。
        """
        self._transport.open_requests.append(self._request)
        if self._transport.open_error is not None:
            error = self._transport.open_error
            raise error
        chunks = self._transport.stream_chunks
        if callable(chunks):
            # 允许按请求现算块序列：用例要拿「本次请求的 request_id」去编帧，只能等请求发出后才知道。
            chunks = chunks(self._request)
        stream = ScriptedStream(
            chunks,
            error=self._transport.stream_error,
            hold=self._transport.stream_hold,
            aborts=self._transport.aborts,
        )
        self._transport.streams.append(stream)
        return stream

    async def __aexit__(self, *_: object) -> None:
        """退出（脚本流无资源可释放）。"""
        return None


class ScriptedRpcTransport:
    """按脚本应答 / 抛错的传输替身（覆盖错误映射、超时与逐块读取分支）。"""

    def __init__(
        self,
        handler: TransportHandler | None = None,
        *,
        stream_chunks: Sequence[bytes] | Callable[[RpcHttpRequest], Sequence[bytes]] = (),
        stream_error: Exception | None = None,
        stream_hold: bool = False,
        open_error: Exception | None = None,
    ) -> None:
        """初始化。

        Args:
            handler: 一元处理器（返回响应，或抛 `RpcTransportError` / 其它异常）；None 表示不走一元。
            stream_chunks: 流式脚本块（按顺序交出；也可给「按请求现算」的回调）。
            stream_error: 流式脚本块交完后抛出的异常。
            stream_hold: 流式脚本块交完后挂住不返回。
            open_error: `open_stream().__aenter__()` 抛出的异常（覆盖打开阶段的异常映射）。
        """
        self._handler = handler
        self.stream_chunks = stream_chunks if callable(stream_chunks) else tuple(stream_chunks)
        self.stream_error = stream_error
        self.stream_hold = stream_hold
        self.open_error = open_error
        self.closed = False
        self.requests: list[RpcHttpRequest] = []
        self.open_requests: list[RpcHttpRequest] = []
        self.aborts: list[str | None] = []
        self.streams: list[ScriptedStream] = []

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """记录请求并交给脚本。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 脚本给出的响应。

        Raises:
            RpcTransportError: 脚本抛出（原样上抛，供 `RpcClient` 映射）。
        """
        self.requests.append(request)
        if self._handler is None:  # pragma: no cover - 用例总会给一元处理器
            raise RpcTransportClosedError("脚本传输未配置一元处理器")
        return await self._handler(request)

    def open_stream(self, request: RpcHttpRequest) -> AbstractAsyncContextManager[TransportStream]:
        """按脚本打开一条流（进入时才登记「已发请求」，惰性打开可断言）。

        Args:
            request: 传输层入参。

        Returns:
            AbstractAsyncContextManager[TransportStream]: 进入后拿到脚本流。
        """
        return _ScriptedStreamOpener(self, request)

    async def aclose(self) -> None:
        """标记关闭。"""
        self.closed = True

    def stats(self) -> TransportStats:
        """脚本传输没有真实流。

        Returns:
            TransportStats: 恒零。
        """
        return TransportStats(connections=0, open_streams=0)
