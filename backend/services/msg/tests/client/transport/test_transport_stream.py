"""H2 传输层 · 流生命周期（任务 04-7-1；Kiwi 2435）：复位 / 对端 reset / GOAWAY / 大体积 / 状态码。

判据：

1. **中止只影响本流**：取消一条流后 `RST_STREAM(CANCEL)` 到达服务端，同连接另一条流照常 200；
   被中止流的迟到响应（服务端还没察觉）不炸、不串到别的流；
2. **对端 reset**：服务端 `RST_STREAM` → 该流以 `RpcTransportClosedError` 结束；
3. **GOAWAY**：`last_stream_id` 之后的流立刻失败；之内的照常完成；
4. **流控**：> 64 KiB 的请求体走窗口更新，响应体能完整读回；
5. **状态码**：非 2xx 照常返回给调用方判（传输层不解释业务）。
"""

import asyncio
from collections.abc import Callable

import pytest
from bms_core.msgclient.transport import H2Transport, RpcHttpRequest, RpcTransportClosedError

from tests_support.h2_server import H2TestServer

pytestmark = pytest.mark.kiwi_id(2435)

HEADERS: tuple[tuple[str, str], ...] = (("content-type", "application/x-msg-frame"),)
"""请求头。"""

BIG_BYTES = 200_000
"""大体积用例的字节数（> 对端初始 64 KiB 窗口，必然触发 `WINDOW_UPDATE` 等待）。"""


def _request(origin: str, target: str = "/ok", *, body: bytes = b"", timeout_ms: int = 5000) -> RpcHttpRequest:
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
    """等到条件成立。

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


async def test_abort_sends_reset_and_spares_other_streams() -> None:
    """取消一条流：服务端收到 `RST_STREAM(CANCEL)`，同连接另一条流不受影响。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            held = asyncio.create_task(transport.send(_request(server.url, "/gated")))
            await _wait_until(lambda: transport.stats().open_streams == 1)
            other = await transport.send(_request(server.url, "/ok"))
            assert other.status == 200

            held.cancel()
            with pytest.raises(asyncio.CancelledError):
                await held
            await _wait_until(lambda: server.reset_recorded(1))
            assert ("reset", 1, 8) in server.frames, "取消应发 RST_STREAM(CANCEL=8)"

            # 服务端随后才把被中止流的响应写出来（它还来不及处理 RST）：客户端必须丢弃，不串流。
            server.release.set()
            await _wait_until(lambda: transport.stats().open_streams == 0)
            assert transport.stats().connections == 1
        finally:
            await transport.aclose()


async def test_peer_reset_surfaces_as_closed_error() -> None:
    """服务端 `RST_STREAM` → 该流以 `RpcTransportClosedError` 结束。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url, "/reset"))
        finally:
            await transport.aclose()


async def test_goaway_rejects_streams_beyond_last_stream_id() -> None:
    """`GOAWAY.last_stream_id` 之后的流立刻失败（未被对端受理）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url, "/goaway/0"))
        finally:
            await transport.aclose()


async def test_goaway_lets_earlier_streams_finish() -> None:
    """`last_stream_id` 覆盖到的流照常完成，连接进入 drain（不可再承接新流）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            response = await transport.send(_request(server.url, "/goaway/1000"))
            assert response.body == b"bye"
            assert transport.stats().open_streams == 0
        finally:
            await transport.aclose()


async def test_large_body_roundtrip_uses_window_updates() -> None:
    """200 KB 请求体（超过对端初始窗口）能发完，且响应体能完整读回。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        payload = b"x" * BIG_BYTES
        try:
            response = await transport.send(_request(server.url, "/echo", body=payload, timeout_ms=10_000))
        finally:
            await transport.aclose()

        assert response.status == 200
        assert len(response.body) == BIG_BYTES


async def test_non_2xx_is_returned_to_caller() -> None:
    """非 2xx 不算传输错误：状态码原样返回，由 `RpcClient` 映射。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            response = await transport.send(_request(server.url, "/status/429"))
        finally:
            await transport.aclose()

        assert response.status == 429


async def test_cancel_during_body_send_aborts_stream() -> None:
    """请求体还没发完就被取消：本流被中止（RST 到达服务端），不留下半截流。"""
    async with H2TestServer(pause_acks=True) as server:
        transport = H2Transport()
        try:
            task = asyncio.create_task(
                transport.send(_request(server.url, "/echo", body=b"x" * BIG_BYTES, timeout_ms=5000))
            )
            await _wait_until(lambda: ("headers", 1, 0) in server.frames)
            await asyncio.sleep(0.05)  # 让发送侧卡在窗口上（服务端不回 WINDOW_UPDATE）
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await _wait_until(lambda: server.reset_recorded(1))
            assert transport.stats().open_streams == 0
        finally:
            await transport.aclose()


async def test_partial_frame_read_is_tolerated() -> None:
    """读到半帧（`receive_data` 返回空事件列表）不当作错误，后续字节补齐后照常完成。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            response = await transport.send(_request(server.url, "/drip"))
        finally:
            await transport.aclose()

        assert response.body == b"hello"
