"""H2 传输层 · 逐块读与流中止（任务 04-3-2；Kiwi 2438）：真 TCP + 裸 h2 服务端。

判据（对应 04-3-2 详细设计 §6.1 第一个文件）：

1. **逐块读**：服务端分三块写出，`read()` 三次各拿一块，第四次返回 `None`；`stats().open_streams`
   在途为 1、收尾归零。
2. **读中中止**：`abort()` 发 `RST_STREAM(CANCEL)`；后续 `read()` 以 `RpcTransportClosedError` 结束
   （不是「悄悄结束」——那会让句柄层把故障误判成「对方没写收尾帧」）；同连接第二条流照常 200。
3. **已完成流中止**：返回 `False` 且不发帧（对已关闭流发 `RST_STREAM` 属协议违规，会放大成连接事故）。
4. **打开阶段失败**：建连失败与端点非 200 都在 `__aenter__` 抛 `RpcTransportClosedError`。

用裸 h2 服务端而不是 hypercorn：本文件断言的是**字节级事实**（分了几次读、RST 有没有发、状态码），
这些在 hypercorn 后面全看不见；真 hypercorn 的端到端在 `tests/client/test_rpc_stream_h2c.py`。
"""

import asyncio
import socket
from collections.abc import Callable

import pytest
from bms_core.msgclient.transport import (
    H2Transport,
    RpcHttpRequest,
    RpcTransportClosedError,
    RpcTransportTimeoutError,
)
from bms_core.msgclient.transport.stream import CLOSE_CANCELLED

from tests_support.h2_server import H2TestServer

pytestmark = pytest.mark.kiwi_id(2438)

HEADERS: tuple[tuple[str, str], ...] = (("content-type", "application/x-msg-frame"),)
"""请求头。"""


def _request(origin: str, target: str, *, timeout_ms: int = 5000) -> RpcHttpRequest:
    """构造传输层请求。

    Args:
        origin: 实例基址。
        target: 请求路径。
        timeout_ms: 打开阶段的本地超时。

    Returns:
        RpcHttpRequest: 请求。
    """
    return RpcHttpRequest(origin=origin, target=target, headers=HEADERS, body=b"", timeout_ms=timeout_ms)


def _closed_port() -> int:
    """取一个「刚释放、必然没人监听」的端口。

    Returns:
        int: 端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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


async def test_stream_reads_chunk_by_chunk_until_end() -> None:
    """分块响应逐块 `read()`，最后返回 `None`；在途数在打开时为 1、收尾后归零。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            async with transport.open_stream(_request(server.url, "/chunks")) as stream:
                assert transport.stats().open_streams == 1, "打开后应有一条在途流"
                chunks = [await stream.read(), await stream.read(), await stream.read()]
                assert chunks == [b"one", b"two", b"three"], f"应逐块返回，实际 {chunks!r}"
                assert await stream.read() is None, "收尾后再读应为 None"
            await _wait_until(lambda: transport.stats().open_streams == 0)
        finally:
            await transport.aclose()


async def test_abort_during_read_then_other_stream_ok() -> None:
    """读中 `abort()`：本流以 `RpcTransportClosedError` 结束，同连接另一条流照常 200。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            opener = transport.open_stream(_request(server.url, "/hold"))
            stream = await opener.__aenter__()
            assert stream.stream_id == 1
            assert stream.abort(CLOSE_CANCELLED) is True, "未收口的流应真的发出 RST_STREAM"
            with pytest.raises(RpcTransportClosedError):
                await stream.read()
            await _wait_until(lambda: server.reset_recorded(1))
            assert ("reset", 1, 8) in server.frames, "中止应发 RST_STREAM(CANCEL=8)"

            other = await transport.send(_request(server.url, "/ok"))
            assert other.status == 200, "中止一条流不该影响同连接其它流"
            await opener.__aexit__(None, None, None)
        finally:
            await transport.aclose()


async def test_abort_after_end_returns_false_and_sends_nothing() -> None:
    """已完成流再 `abort()`：返回 `False`，且服务端收不到任何 `RST_STREAM`。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            async with transport.open_stream(_request(server.url, "/chunks")) as stream:
                while await stream.read() is not None:
                    pass
                assert stream.abort(CLOSE_CANCELLED) is False, "已收口的流没有可中止的东西"
            await _wait_until(lambda: transport.stats().open_streams == 0)
            assert not any(kind == "reset" for kind, _, _ in server.frames), "已收口的流不该再发 RST_STREAM"
        finally:
            await transport.aclose()


async def test_open_failure_raises_closed_error() -> None:
    """建连失败（端口无人监听）在 `__aenter__` 抛 `RpcTransportClosedError`。"""
    transport = H2Transport()
    try:
        with pytest.raises(RpcTransportClosedError):
            async with transport.open_stream(_request(f"http://127.0.0.1:{_closed_port()}", "/chunks")):
                pytest.fail("建连失败却拿到了流")
    finally:
        await transport.aclose()


async def test_open_rejects_non_200() -> None:
    """流端点返回非 200：打开阶段即失败（响应体不是帧流，交给句柄层只会得到误导性的协议错）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError, match="HTTP 403"):
                async with transport.open_stream(_request(server.url, "/status/403")) as stream:
                    pytest.fail(f"非 200 却拿到了流：{stream!r}")
            await _wait_until(lambda: transport.stats().open_streams == 0)
        finally:
            await transport.aclose()


async def test_open_after_close_raises_closed_error() -> None:
    """传输层已关闭：`open_stream` 进入即失败，不去建连。"""
    transport = H2Transport()
    await transport.aclose()

    with pytest.raises(RpcTransportClosedError, match="已关闭"):
        async with transport.open_stream(_request(f"http://127.0.0.1:{_closed_port()}", "/chunks")):
            pytest.fail("传输层已关闭却拿到了流")


async def test_open_timeout_aborts_stream() -> None:
    """服务端不回响应头（`/slow`）→ 打开阶段超时，且本流被 `RST_STREAM` 中止。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportTimeoutError):
                async with transport.open_stream(_request(server.url, "/slow", timeout_ms=50)):
                    pytest.fail("服务端没回响应头却拿到了流")
            await _wait_until(lambda: server.reset_recorded(1))
            assert ("reset", 1, 8) in server.frames, "打开超时也要把本流中止掉，不留半截"
            await _wait_until(lambda: transport.stats().open_streams == 0)
        finally:
            await transport.aclose()


async def test_open_peer_reset_raises_closed_error() -> None:
    """响应头之前对端 `RST_STREAM` → 打开阶段报「对端中止」，而不是「HTTP 0」。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with pytest.raises(RpcTransportClosedError, match="对端中止"):
                async with transport.open_stream(_request(server.url, "/reset")):
                    pytest.fail("对端 reset 却拿到了流")
        finally:
            await transport.aclose()
