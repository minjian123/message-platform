"""内部复制服务生命周期（02-8）：启动 / 存活 / 请求 / 关闭 / 绑定失败。"""

from __future__ import annotations

import asyncio
import socket

import httpx
import pytest
from fastapi import FastAPI

from bms_msg.broker.internal_server import InternalReplicationServer, InternalServerError


def _free_port() -> int:
    """取一个空闲端口。

    Returns:
        int: 端口。
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def _app() -> FastAPI:
    """最小内部应用。"""
    app = FastAPI()

    @app.get("/ping")
    async def ping() -> dict[str, str]:
        return {"status": "ok"}

    return app


def test_server_rejects_non_positive_port() -> None:
    """端口：非正数即 ValueError（不静默）。"""
    with pytest.raises(ValueError, match="内部端口"):
        InternalReplicationServer(_app(), host="127.0.0.1", port=0)


async def test_server_start_serve_and_stop() -> None:
    """生命周期：启动后可请求、`alive` 为真；关闭后幂等。"""
    port = _free_port()
    server = InternalReplicationServer(_app(), host="127.0.0.1", port=port)

    await server.start()
    try:
        assert server.alive is True
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
            response = await client.get("/ping")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    finally:
        await server.aclose()
    assert server.alive is False
    await server.aclose()


async def test_server_bind_failure_is_fatal() -> None:
    """绑定：端口被占用即 InternalServerError（bind_failed）。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = int(sock.getsockname()[1])
    server = InternalReplicationServer(_app(), host="127.0.0.1", port=port)
    try:
        with pytest.raises(InternalServerError) as exc:
            await server.start()
        assert exc.value.reason == "bind_failed"
    finally:
        sock.close()
        await server.aclose()


def test_server_error_message_and_attributes() -> None:
    """错误对象：`reason` / `detail` 可读。"""
    error = InternalServerError("ready_timeout", "超时")
    assert error.reason == "ready_timeout"
    assert str(error) == "超时"


async def test_server_properties_and_idempotent_start() -> None:
    """属性可读；重复 start 不重复监听。"""
    port = _free_port()
    server = InternalReplicationServer(_app(), host="127.0.0.1", port=port)
    assert server.host == "127.0.0.1"
    assert server.port == port
    await server.start()
    try:
        await server.start()
        assert server.alive is True
    finally:
        await server.aclose()


async def test_wait_ready_detects_failed_task() -> None:
    """就绪等待：监听任务提前失败即抛 serve_failed。"""

    async def _boom() -> None:
        raise RuntimeError("bind failed")

    server = InternalReplicationServer(_app(), host="127.0.0.1", port=_free_port())
    task = asyncio.create_task(_boom())
    await asyncio.sleep(0)
    server._task = task  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(InternalServerError) as exc:
        await server._wait_ready()  # pyright: ignore[reportPrivateUsage]
    assert exc.value.reason == "serve_failed"


async def test_wait_ready_timeout() -> None:
    """就绪等待：端口始终不可连即超时并取消监听任务。"""
    server = InternalReplicationServer(_app(), host="127.0.0.1", port=_free_port(), ready_timeout_ms=1)
    sleeper = asyncio.create_task(asyncio.sleep(30))
    server._task = sleeper  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(InternalServerError) as exc:
        await server._wait_ready()  # pyright: ignore[reportPrivateUsage]
    assert exc.value.reason == "ready_timeout"


async def test_server_close_timeout_and_closer() -> None:
    """关闭：任务不结束即超时取消；关闭句柄转调服务。"""
    server = InternalReplicationServer(_app(), host="127.0.0.1", port=_free_port(), shutdown_timeout_ms=1)
    server._task = asyncio.create_task(asyncio.sleep(30))  # pyright: ignore[reportPrivateUsage]
    server._started = True  # pyright: ignore[reportPrivateUsage]
    await server.aclose()
    assert server.alive is False

    from bms_msg.broker.internal_server import InternalServerCloser

    await InternalServerCloser(server=server).aclose()
