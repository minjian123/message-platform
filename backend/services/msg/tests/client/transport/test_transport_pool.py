"""H2 传输层 · 连接池（任务 04-7-1；Kiwi 2435）：复用 / 并发建连 / 并发流上限 / 空闲回收 / 收口。

判据：

1. **复用**：同一 origin 的连续两次调用只建一条连接（`server.connections == 1`）；
2. **冷启动去重**：并发 5 个调用仍只建一条连接（建立中的 future 表）；
3. **并发流上限**：对端 `SETTINGS_MAX_CONCURRENT_STREAMS` 生效——超限的调用排队等待，不报错也不硬发；
4. **空闲回收**：空闲超阈值后下一次调用重建连接；
5. **收口**：`aclose()` 后在途流归零、连接数归零、再发请求明确报错（幂等）。
"""

import asyncio
from collections.abc import Callable

import pytest

from bms_msg.client.transport import H2Transport, RpcHttpRequest, RpcTransportClosedError, TransportStats
from tests_support.h2_server import H2TestServer

pytestmark = pytest.mark.kiwi_id(2435)

HEADERS: tuple[tuple[str, str], ...] = (("content-type", "application/x-msg-frame"),)
"""请求头（真实调用只带这两个里的一到两个，用例不模拟额外头）。"""


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
    """等到条件成立（失败即断言超时，不用固定 sleep 猜时序）。

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


async def test_reuses_one_connection() -> None:
    """同一 origin 的两次调用复用一条连接，在途流收口后归零。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            first = await transport.send(_request(server.url))
            second = await transport.send(_request(server.url, "/echo", body=b"abc"))
        finally:
            await transport.aclose()

        assert (first.status, first.body) == (200, b"ok")
        assert second.body == b"abc"
        assert server.connections == 1
        assert first.stream_id > 0


async def test_concurrent_cold_start_builds_one_connection() -> None:
    """并发冷启动只建一条连接（建立中的 future 去重）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            responses = await asyncio.gather(*(transport.send(_request(server.url)) for _ in range(5)))
        finally:
            await transport.aclose()

        assert [response.status for response in responses] == [200] * 5
        assert server.connections == 1


async def test_respects_remote_concurrency_limit() -> None:
    """对端把并发流上限下到 1 时，第二个调用排队而不是硬发。"""
    async with H2TestServer(max_concurrent_streams=1) as server:
        transport = H2Transport()
        try:
            held = asyncio.create_task(transport.send(_request(server.url, "/gated")))
            await _wait_until(lambda: transport.stats().open_streams == 1)
            # 让客户端先把对端 SETTINGS 读完（上限 1 生效）再发第二个请求。
            await asyncio.sleep(0.05)

            second = asyncio.create_task(transport.send(_request(server.url, "/ok")))
            await asyncio.sleep(0.05)
            assert not second.done(), "超过对端并发上限时应排队"

            server.release.set()
            assert (await held).body == b"gated"
            assert (await second).status == 200
        finally:
            await transport.aclose()


async def test_idle_connection_is_rebuilt() -> None:
    """空闲超过阈值后下一次调用重建连接（对端 30s 空闲即断，本端先回收）。"""
    async with H2TestServer() as server:
        transport = H2Transport(idle_timeout_s=0.0)
        try:
            await transport.send(_request(server.url))
            await asyncio.sleep(0.01)
            await transport.send(_request(server.url))
        finally:
            await transport.aclose()

        assert server.connections == 2


async def test_stats_and_aclose_are_idempotent() -> None:
    """`stats()` 反映连接数，`aclose()` 幂等且之后拒绝新请求。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        await transport.send(_request(server.url))
        assert transport.stats() == TransportStats(connections=1, open_streams=0)

        await transport.aclose()
        await transport.aclose()
        assert transport.stats() == TransportStats(connections=0, open_streams=0)
        with pytest.raises(RpcTransportClosedError):
            await transport.send(_request(server.url))


async def test_settings_without_concurrency_limit() -> None:
    """对端 SETTINGS 不含并发流上限时照常工作（上限维持缺省）。"""
    async with H2TestServer(settings_without_max=True) as server:
        transport = H2Transport()
        try:
            first = await transport.send(_request(server.url))
            second = await transport.send(_request(server.url))
        finally:
            await transport.aclose()

        assert (first.status, second.status) == (200, 200)
        assert server.connections == 1
