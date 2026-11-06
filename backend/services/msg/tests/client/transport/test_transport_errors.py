"""H2 传输层 · 异常梯度（任务 04-7-1；Kiwi 2435）：超时 / 断连 / 协议错 / ALPN / scheme / 收口护栏。

判据：四类异常各由真实场景触发（不 mock），`RpcClient` 只认这四类，不认识 `OSError` / `h2` 的类型；
以及「未握手的连接也能安全关闭」「关闭后的连接拒绝新流」两条护栏。
"""

import asyncio
import socket
from collections.abc import Callable

import pytest

from bms_msg.client.transport import (
    H2Transport,
    RpcHttpRequest,
    RpcTransportClosedError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
    RpcTransportUnsupportedError,
)
from bms_msg.client.transport.connection import H2ClientConnection
from tests_support.h2_server import H2TestServer, client_ssl_context, server_tls_context

pytestmark = pytest.mark.kiwi_id(2435)

HEADERS: tuple[tuple[str, str], ...] = (("content-type", "application/x-msg-frame"),)
"""请求头。"""


def _request(origin: str, target: str = "/ok", *, body: bytes = b"", timeout_ms: int = 3000) -> RpcHttpRequest:
    """构造传输层请求。

    Args:
        origin: 实例基址。
        target: 请求路径。
        body: 请求体。
        timeout_ms: 本地超时。

    Returns:
        RpcHttpRequest: 请求。
    """
    return RpcHttpRequest(origin=origin, target=target, headers=HEADERS, body=body, timeout_ms=timeout_ms)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    """等到条件成立（网络往返是异步的，断言前必须等）。

    Args:
        predicate: 条件。
        timeout: 超时（秒）。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("等待超时")
        await asyncio.sleep(0.01)


def _free_port() -> int:
    """取一个本机空闲端口（关闭后不监听，用来制造连接被拒）。

    Returns:
        int: 端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_unsupported_scheme_is_rejected_locally() -> None:
    """`ftp://` 与缺主机的 origin 本地即拒（不发网络）。"""
    transport = H2Transport()
    try:
        with pytest.raises(RpcTransportUnsupportedError):
            await transport.send(_request("ftp://127.0.0.1:9"))
        with pytest.raises(RpcTransportUnsupportedError):
            await transport.send(_request("http://"))
    finally:
        await transport.aclose()


async def test_connection_refused_maps_to_closed_error() -> None:
    """连不上实例 → `RpcTransportClosedError`（transport 层，不重试）。

    超时给 5s：Windows 对本机已关闭端口要重试约 2s 才回 `ConnectionRefused`，给 1s 会先命中外层超时。
    """
    transport = H2Transport()
    try:
        with pytest.raises(RpcTransportClosedError):
            await transport.send(_request(f"http://127.0.0.1:{_free_port()}", timeout_ms=5000))
    finally:
        await transport.aclose()


async def test_local_timeout_aborts_stream() -> None:
    """等响应头超时 → `RpcTransportTimeoutError`，且该流被中止（服务端收到 RST）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportTimeoutError):
                await transport.send(_request(server.url, "/slow", timeout_ms=100))
            await _wait_until(lambda: server.reset_recorded(1))
            assert ("reset", 1, 8) in server.frames, "超时也应中止本流，否则服务端白跑到预算耗尽"
            assert transport.stats().open_streams == 0
        finally:
            await transport.aclose()


async def test_peer_close_maps_to_closed_error() -> None:
    """服务端直接关连接 → 在途流以 `RpcTransportClosedError` 结束。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url, "/close"))
        finally:
            await transport.aclose()


async def test_garbage_bytes_map_to_closed_error() -> None:
    """对端发非 h2 字节 → 协议异常被包成 `RpcTransportClosedError`（不是裸 `h2` 异常）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url, "/boom"))
        finally:
            await transport.aclose()


async def test_tls_with_alpn_roundtrip() -> None:
    """`https` + ALPN `h2` 正常往返（客户端信任测试自签证书）。"""
    async with H2TestServer(tls=server_tls_context()) as server:
        transport = H2Transport(ssl_context=client_ssl_context())
        try:
            response = await transport.send(_request(server.url))
        finally:
            await transport.aclose()

        assert response.body == b"ok"


async def test_tls_without_alpn_is_protocol_error() -> None:
    """服务端不声明 ALPN → 协商不到 `h2`，明确报协议错（不静默降级到 HTTP/1.1）。"""
    async with H2TestServer(tls=server_tls_context(alpn=False)) as server:
        transport = H2Transport(ssl_context=client_ssl_context())
        try:
            with pytest.raises(RpcTransportProtocolError):
                await transport.send(_request(server.url))
        finally:
            await transport.aclose()


async def test_tls_with_default_context_rejects_self_signed() -> None:
    """缺省上下文（系统信任库）不认自签证书 → `RpcTransportClosedError`。"""
    async with H2TestServer(tls=server_tls_context()) as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url))
        finally:
            await transport.aclose()


async def test_unstarted_connection_can_be_closed() -> None:
    """未握手 / 已关闭的连接：`aclose()` 幂等、新流明确拒绝（护栏）。"""
    async with H2TestServer() as server:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        connection = H2ClientConnection(
            origin=server.url,
            authority=f"127.0.0.1:{server.port}",
            scheme="http",
            reader=reader,
            writer=writer,
        )
        await connection.aclose()
        await connection.aclose()

        assert connection.is_available is False
        with pytest.raises(RpcTransportClosedError):
            await connection.send_request(_request(server.url))


async def test_aclose_during_connect_cancels_pending() -> None:
    """建连途中关闭传输：挂起的调用以 `CancelledError` 结束，不留下无人管的连接任务。"""
    transport = H2Transport()
    task = asyncio.create_task(transport.send(_request("http://192.0.2.1:80", timeout_ms=30_000)))
    await asyncio.sleep(0.05)
    await transport.aclose()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert transport.stats().connections == 0
