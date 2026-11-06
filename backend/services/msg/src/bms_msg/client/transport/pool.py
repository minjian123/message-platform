"""h2 传输实现（任务 04-7-1）：按 origin 分池、TLS/ALPN、异常梯度。

**池键是 origin 而不是「实例列表」**：04-6 的 `InstanceProvider` 只负责挑地址，池只管「这个地址
我有一条连接」。两者解耦后，换实例 = 换 origin = 换个池位，实例摘除时也只关一条连接，不会误伤
同进程里其它实例的调用。

**每 origin 首期一条连接**：h2 单连接多流是常态，多连接属 04-6 的「最少在途」策略。冷启动用
「建立中的 future 表」去重——否则 10 个并发调用会各建一条连接，把 h2 的好处抵掉。

**异常一律转成 `base.py` 的四条梯度**：`RpcClient` 只认这四条，不认识 `OSError` / `h2` 的异常类型；
换实现（04-6 接服务目录后发现要换底层）时映射表不用重写。
"""

from __future__ import annotations

import asyncio
import ssl
from contextlib import suppress
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlsplit

from bms_msg.client.transport.base import (
    RpcHttpRequest,
    RpcHttpResponse,
    RpcTransportClosedError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
    RpcTransportUnsupportedError,
    TransportStats,
)
from bms_msg.client.transport.connection import H2ClientConnection
from bms_msg.client.transport.stream import H2Stream
from bms_msg.core.config import MS_PER_SECOND

__all__ = ["H2Transport"]

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
        """发送一次一元调用并读满响应（超时即中止本流，同连接其它流不受影响）。

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
        stream: H2Stream | None = None
        try:
            async with asyncio.timeout(request.timeout_ms / MS_PER_SECOND):
                connection = await self._connection_for(origin)
                stream = await connection.send_request(request)
                return await stream.wait_response()
        except TimeoutError as exc:
            raise RpcTransportTimeoutError(
                f"RPC 传输超时（{request.timeout_ms} ms）：{origin.base}{request.target}"
            ) from exc
        finally:
            if stream is not None:
                stream.abort()

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
