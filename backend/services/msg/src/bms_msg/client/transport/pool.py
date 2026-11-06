"""h2 传输实现（任务 04-7-1 / 04-3-2）：按 origin 分池、TLS/ALPN、异常梯度、一元与流式两个出口。

**池键是 origin 而不是「实例列表」**：04-6 的 `InstanceProvider` 只负责挑地址，池只管「这个地址
我有一条连接」。两者解耦后，换实例 = 换 origin = 换个池位，实例摘除时也只关一条连接，不会误伤
同进程里其它实例的调用。

**每 origin 首期一条连接**：h2 单连接多流是常态，多连接属 04-6 的「最少在途」策略。冷启动用
「建立中的 future 表」去重——否则 10 个并发调用会各建一条连接，把 h2 的好处抵掉。

**异常一律转成 `base.py` 的四条梯度**：`RpcClient` 只认这四条，不认识 `OSError` / `h2` 的异常类型；
换实现（04-6 接服务目录后发现要换底层）时映射表不用重写。`open_stream()` 与 `send()` **同表**，
不新增第五条——流式只是「读法不同」，不是「错法不同」。

**流式不经 `asyncio.timeout` 包住全程**（04-3-2 §5.5）：`request.timeout_ms` 只约束「建连 + 发请求 +
等响应头」这一段，随后把流交给句柄层逐块读。把一元的「预算 + 余量」包到整条流上，预算 30s 的流
30s 后必被本地掐断，长流等于不可用。
"""

from __future__ import annotations

import asyncio
import ssl
import time
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit

from bms_core.core.logging import get_logger

from bms_msg.client.transport.base import (
    RpcHttpRequest,
    RpcHttpResponse,
    RpcTransportClosedError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
    RpcTransportUnsupportedError,
    TransportStats,
    TransportStream,
)
from bms_msg.client.transport.connection import H2ClientConnection
from bms_msg.client.transport.stream import CLOSE_ERROR, CLOSE_TIMEOUT, H2Stream, abort_reason_of
from bms_msg.core.config import MS_PER_SECOND

__all__ = ["H2Transport"]

_LOGGER = get_logger("bms_msg.rpc.client")
"""客户端侧日志门面（与服务端 `msg_rpc` 同一命名族：`msg_rpc` 给服务端视角，`msg_rpc_client_cancel`
给客户端视角，06-3 按事件名分流出指标）。"""

_IDLE_TIMEOUT_SECONDS = 30.0
"""空闲连接回收阈值（秒）：对端 30s 空闲即断，本端先回收，省一次「写进去才知道断了」。"""

_SUPPORTED_SCHEMES = ("http", "https")
"""支持的 scheme（分别走 h2c 与 TLS + ALPN `h2`）。"""


@dataclass(frozen=True)
class _Origin:
    """解析后的实例基址。"""

    base: str
    scheme: str
    host: str
    port: int
    authority: str


def _parse_origin(origin: str) -> _Origin:
    """解析并校验 origin。

    Args:
        origin: 实例基址（`scheme://host[:port]`）。

    Returns:
        _Origin: 规范化后的基址各部分。

    Raises:
        RpcTransportUnsupportedError: scheme 不受支持，或缺少主机。
    """
    parsed = urlsplit(origin)
    if parsed.scheme not in _SUPPORTED_SCHEMES:
        raise RpcTransportUnsupportedError(f"不支持的 scheme：{parsed.scheme!r}（仅 {'/'.join(_SUPPORTED_SCHEMES)}）")
    if parsed.hostname is None:
        raise RpcTransportUnsupportedError(f"origin 缺少主机：{origin!r}")
    port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
    authority = f"{parsed.hostname}:{port}"
    return _Origin(
        base=f"{parsed.scheme}://{authority}",
        scheme=parsed.scheme,
        host=parsed.hostname,
        port=port,
        authority=authority,
    )


def _log_cancel(*, request: RpcHttpRequest, stream: H2Stream, reason: str, started_at: float) -> None:
    """记一条 `msg_rpc_client_cancel`（客户端取消唯一出口）。

    **为什么不改服务端 `msg_rpc`**：服务端四态回答的是「我这边怎么结束的」，客户端这条回答的是
    「我为什么不等了、RST 发没发」——混成一条会让「一行日志一个视角」失效（04-7-2 决策 10）。
    两者靠 `request_id` 对账。

    Args:
        request: 传输层入参（取 `trace` 里的关联字段）。
        stream: 被中止的流（取 `stream_id` 与 `rst_sent`）。
        reason: `cancelled` / `timeout` / `error`。
        started_at: `send()` 进入时刻（`time.monotonic()`），用于算 `elapsed_ms`。
    """
    trace = request.trace
    _LOGGER.info(
        "msg_rpc_client_cancel",
        request_id=None if trace is None else trace.request_id,
        service=None if trace is None else trace.service,
        method=None if trace is None else trace.method,
        stream_id=stream.stream_id,
        reason=reason,
        rst_sent=stream.rst_sent,
        elapsed_ms=int((time.monotonic() - started_at) * MS_PER_SECOND),
    )


class _H2TransportStream:
    """`TransportStream` 的生产实现（把 `H2Stream` 的读 / 中止面收窄给句柄层）。

    **只转发，不加语义**：句柄层要的是「读到一块 / 发中止」，`close_reason` 与 `status` 都留在传输层
    （前者给日志、后者在打开阶段就判掉）。转发一层而不是直接把 `H2Stream` 递出去，是为了让句柄层
    依赖 `TransportStream` 协议——换底层（04-6）时 `client/stream.py` 一个字不用改。
    """

    __slots__ = ("_stream",)

    def __init__(self, stream: H2Stream) -> None:
        """初始化。

        Args:
            stream: 已发出请求的 h2 流。
        """
        self._stream = stream

    @property
    def stream_id(self) -> int:
        """h2 流号。

        Returns:
            int: 流号。
        """
        return self._stream.stream_id

    async def read(self) -> bytes | None:
        """读下一块响应体（`None` = 已收 `END_STREAM`）。

        Returns:
            bytes | None: 下一块字节或 None。

        Raises:
            RpcTransportError: 本流以错误收口（对端 reset / 连接故障 / 本地已中止）。
        """
        return await self._stream.read_chunk()

    def abort(self, reason: str | None = None) -> bool:
        """本地中止本流（幂等；返回本次是否真发了 `RST_STREAM`）。

        Args:
            reason: 中止原因（`cancelled` / `timeout` / `error`）。

        Returns:
            bool: 本次是否发了帧。
        """
        return self._stream.abort(reason)


class _StreamOpener(AbstractAsyncContextManager[TransportStream]):
    """把「进入时才执行」封起来的异步上下文管理器（`open_stream()` 的惰性打开载体）。

    为什么不是「`open_stream()` 直接 `await` 出一个流」：`RpcClient.stream()` 要能同步构造句柄
    （否则调用方得写 `async with await client.stream(...)`），而发请求本身就是异步的。把异步那半
    放进 `__aenter__`，两种诉求同时满足，且「建连失败 / 非 200」统一在进入时抛，调用方只有一处 `try`。

    载体只持一个「打开动作」而不是持有传输层对象：真正的打开逻辑留在 `H2Transport` 里（它才该碰
    连接池与关闭标志），这里只负责「晚一点调用它」。
    """

    __slots__ = ("_open",)

    def __init__(self, open_action: Callable[[], Awaitable[TransportStream]]) -> None:
        """初始化（不做任何 IO）。

        Args:
            open_action: 进入时执行的打开动作（返回已发出请求的流）。
        """
        self._open = open_action

    async def __aenter__(self) -> TransportStream:
        """执行打开动作。

        Returns:
            TransportStream: 已发出请求、可 `read()` / `abort()` 的流。
        """
        return await self._open()

    async def __aexit__(self, *_: object) -> None:
        """退出上下文（连接由池持有，中止由 `TransportStream.abort()` 负责，这里无事可做）。"""
        return None


class H2Transport:
    """按 origin 分池的 h2 传输（`RpcTransport` 的生产实现）。"""

    def __init__(
        self,
        *,
        ssl_context: ssl.SSLContext | None = None,
        idle_timeout_s: float = _IDLE_TIMEOUT_SECONDS,
    ) -> None:
        """初始化。

        Args:
            ssl_context: `https` 用的上下文（缺省 `create_default_context()`；用例注入信任自签证书的上下文）。
            idle_timeout_s: 空闲连接回收阈值（秒）。
        """
        self._ssl_context = ssl_context
        self._idle_timeout_s = idle_timeout_s
        self._connections: dict[str, H2ClientConnection] = {}
        self._connecting: dict[str, asyncio.Task[H2ClientConnection]] = {}
        self._closed = False

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """发送一次一元调用并读满响应（超时 / 取消即中止本流，同连接其它流不受影响）。

        **三条「中止」出口在这里归一**（`reason` 是唯一判定处）：

        | 触发 | `reason` | 对外异常 |
        | --- | --- | --- |
        | 调用方 `task.cancel()` | `cancelled` | 原样 `CancelledError`（不包装、不吞） |
        | `timeout_ms` 到点 | `timeout` | `RpcTransportTimeoutError` |
        | 其它异常 | `error` | 原样上抛 |

        三条出口都走同一段 `finally`：`stream.abort(reason)` 发 `RST_STREAM(CANCEL)`，再按结果记
        `msg_rpc_client_cancel`（`rst_sent` 直接回答「RST 发了没有」）。已收口的流 `abort()` 不发帧，
        所以「正常返回」也走这段而不产生副作用，也不会产生日志（`reason` 为 None）。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 响应（状态码原样给调用方）。

        Raises:
            RpcTransportUnsupportedError: origin 的 scheme 不受支持。
            RpcTransportTimeoutError: 连接 / 等头 / 读体超出 `request.timeout_ms`。
            RpcTransportClosedError: 连接不可用 / 对端 reset / GOAWAY 拒收。
            RpcTransportProtocolError: TLS 未协商到 `h2`、帧不可解析。
            asyncio.CancelledError: 调用方取消（本流中止后原样上抛）。
        """
        if self._closed:
            raise RpcTransportClosedError("传输层已关闭")
        origin = _parse_origin(request.origin)
        # 流一注册就收下：请求体发送中被取消时 `send_request` 会抛，返回值拿不到，只能经 `on_open` 持有。
        opened: list[H2Stream] = []
        reason: str | None = None
        started_at = time.monotonic()
        try:
            async with asyncio.timeout(request.timeout_ms / MS_PER_SECOND):
                connection = await self._connection_for(origin)
                stream = await connection.send_request(request, on_open=opened.append)
                return await stream.wait_response()
        except TimeoutError as exc:
            reason = CLOSE_TIMEOUT
            raise RpcTransportTimeoutError(
                f"RPC 传输超时（{request.timeout_ms} ms）：{origin.base}{request.target}"
            ) from exc
        except BaseException as exc:
            # `CancelledError` 落这一支：原样 `raise` 让它继续往上走，只在这里记下原因。
            reason = abort_reason_of(exc)
            raise
        finally:
            if opened:
                opened[0].abort(reason)
                if reason is not None:
                    _log_cancel(request=request, stream=opened[0], reason=reason, started_at=started_at)

    def open_stream(self, request: RpcHttpRequest) -> AbstractAsyncContextManager[TransportStream]:
        """打开一条流式请求（进入上下文时才取连接、发请求、等响应头）。

        **与 `send()` 同源**：取连接仍是 `_connection_for()`（含空闲回收与建连去重），异常映射仍是同
        四条梯度，不新增第五条。区别只在「读完的方式」——`send()` 读满，本方法把流交出去逐块读。

        Args:
            request: 传输层入参（`timeout_ms` 只约束打开阶段）。

        Returns:
            AbstractAsyncContextManager[TransportStream]: 进入后拿到可 `read()` / `abort()` 的流。
        """
        return _StreamOpener(lambda: self._open_stream(request))

    async def _open_stream(self, request: RpcHttpRequest) -> TransportStream:
        """取连接 → 发请求 → 等响应头（`open_stream().__aenter__()` 的真正执行体）。

        Args:
            request: 传输层入参（`timeout_ms` 只约束这一段）。

        Returns:
            TransportStream: 已发出请求的流。

        Raises:
            RpcTransportClosedError: 传输层已关闭 / 连接不可用 / 流端点返回非 200。
            RpcTransportTimeoutError: 建连、发请求或等响应头超出 `request.timeout_ms`。
            RpcTransportProtocolError: TLS 未协商到 `h2`。
            RpcTransportUnsupportedError: origin 的 scheme 不受支持。
            asyncio.CancelledError: 打开期间被取消（本流中止后原样上抛）。
        """
        if self._closed:
            raise RpcTransportClosedError("传输层已关闭")
        origin = _parse_origin(request.origin)
        # 与 `send()` 同一手法：流一注册就收下（发请求体时被取消会让 `send_request` 抛，返回值拿不到）。
        opened: list[H2Stream] = []
        try:
            async with asyncio.timeout(request.timeout_ms / MS_PER_SECOND):
                connection = await self._connection_for(origin)
                stream = await connection.send_request(request, on_open=opened.append)
                status = await stream.wait_headers()
        except BaseException as exc:
            # **两个出口归一**（与 `send()` 同取向）：先中止本流，再按时限 / 其它分流。
            # `CancelledError` 落「其它」这一支，原样上抛（取消不包装、不吞）。
            reason = CLOSE_TIMEOUT if isinstance(exc, TimeoutError) else abort_reason_of(exc)
            for candidate in opened:
                candidate.abort(reason)
            if isinstance(exc, TimeoutError):
                raise RpcTransportTimeoutError(
                    f"RPC 流打开超时（{request.timeout_ms} ms）：{origin.base}{request.target}"
                ) from exc
            raise
        if status != 200:
            # **状态码在传输层判掉**：流端点对帧请求恒回 200（建流前失败也是 200 + 单帧 error），
            # 走到这里说明是路由 / 权限 / 网关层的问题。此时响应体不是帧流，交给句柄层只会得到
            # 「无收尾帧」这种误导性的协议错，不如在打开阶段就说清状态码。
            opened[0].abort(CLOSE_ERROR)
            raise RpcTransportClosedError(f"RPC 流端点返回 HTTP {status}：{request.target}")
        return _H2TransportStream(stream)

    async def aclose(self) -> None:
        """关闭全部连接与建立中的任务（幂等）。"""
        self._closed = True
        for task in list(self._connecting.values()):
            task.cancel()
        self._connecting.clear()
        for base, connection in list(self._connections.items()):
            await connection.aclose()
            del self._connections[base]

    def stats(self) -> TransportStats:
        """取当前快照。

        Returns:
            TransportStats: 连接数与在途流数。
        """
        return TransportStats(
            connections=len(self._connections),
            open_streams=sum(connection.open_streams for connection in self._connections.values()),
        )

    # ------------------------------------------------------------------ 连接管理

    async def _connection_for(self, origin: _Origin) -> H2ClientConnection:
        """取该 origin 的可用连接（无则建，不可用 / 空闲超时则重建）。

        Args:
            origin: 解析后的基址。

        Returns:
            H2ClientConnection: 可用连接。
        """
        existing = self._connections.get(origin.base)
        if existing is not None and existing.is_available and not existing.is_idle_for(self._idle_timeout_s):
            return existing
        if existing is not None:
            await self._drop(origin.base)
        pending = self._connecting.get(origin.base)
        if pending is None:
            pending = asyncio.create_task(self._connect(origin))
            self._connecting[origin.base] = pending
            pending.add_done_callback(lambda _task, key=origin.base: self._connecting.pop(key, None))
        return await pending

    async def _drop(self, base: str) -> None:
        """摘掉并关闭一条连接。

        Args:
            base: 规范化基址。
        """
        connection = self._connections.pop(base)
        await connection.aclose()

    async def _connect(self, origin: _Origin) -> H2ClientConnection:
        """建连（含 TLS / ALPN 校验）并握手。

        Args:
            origin: 解析后的基址。

        Returns:
            H2ClientConnection: 已握手的连接。

        Raises:
            RpcTransportClosedError: TCP / TLS 建连失败。
            RpcTransportProtocolError: ALPN 未协商到 `h2`。
        """
        try:
            if origin.scheme == "https":
                context = self._ssl_context if self._ssl_context is not None else ssl.create_default_context()
                reader, writer = await asyncio.open_connection(
                    origin.host,
                    origin.port,
                    ssl=context,
                    server_hostname=origin.host,
                )
                ssl_object = cast("ssl.SSLObject", writer.get_extra_info("ssl_object"))
                negotiated = ssl_object.selected_alpn_protocol()
                if negotiated != "h2":
                    writer.close()
                    with suppress(OSError):
                        await writer.wait_closed()
                    raise RpcTransportProtocolError(f"TLS 未协商到 h2（实际 {negotiated!r}）：{origin.base}")
            else:
                reader, writer = await asyncio.open_connection(origin.host, origin.port)
        except OSError as exc:
            raise RpcTransportClosedError(f"连接 {origin.base} 失败：{exc}") from exc
        connection = H2ClientConnection(
            origin=origin.base,
            authority=origin.authority,
            scheme=origin.scheme,
            reader=reader,
            writer=writer,
        )
        await connection.start()
        self._connections[origin.base] = connection
        return connection
