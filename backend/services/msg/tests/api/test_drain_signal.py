"""真 h2c 下的连接退休与重连（任务 03-4；Kiwi 2424）。

覆盖 03-1 留给本任务的遗留：**服务端连接退休（GOAWAY）边界的客户端重连**。

事实依据（hypercorn 0.18 源码）：单连接请求数达到 `keep_alive_max_requests` 后连接退休——先对新流回
`RST_STREAM` + `SETTINGS MAX_CONCURRENT_STREAMS=0`，全 idle 后 `close_connection()` 发 GOAWAY；
**空闲回收（`keep_alive_timeout`）不发 GOAWAY**，直接关 TCP。因此客户端必须把「连接消失」当普通断线，
用退避重连接续——本用例验证这条链路真的能接上（换连接即换派生连接键）。
"""

from __future__ import annotations

import asyncio
import importlib
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.plugin import default_plugin_registry
from bms_core.core.run import serve_service
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import AsyncClient

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2424)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    register_error_code_records(ConcurrentStableList(load_product_manifest(_MANIFEST).error_codes))
    yield
    reset_error_code_records()


def _free_port() -> int:
    """取一个本机空闲端口（关闭 socket 后由 hypercorn 重新绑定）。

    Returns:
        int: 端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _serve(
    app: FastAPI, *, max_requests: int, keep_alive_seconds: int = 5
) -> AsyncIterator[tuple[int, asyncio.Task[None]]]:
    """在空闲端口起 hypercorn（连接退休 / 空闲回收阈值收小，便于用例触发）。

    Args:
        app: 应用实例。
        max_requests: 单连接请求数退休阈值。
        keep_alive_seconds: 空闲连接回收阈值（秒）。

    Yields:
        tuple[int, asyncio.Task[None]]: 端口与伺服任务。
    """
    port = _free_port()
    settings = Settings()
    settings.server.host = "127.0.0.1"
    settings.server.port = port
    settings.server.graceful_timeout_seconds = 1
    settings.server.keep_alive_max_requests = max_requests
    settings.server.keep_alive_timeout_seconds = keep_alive_seconds
    task = asyncio.create_task(serve_service(app, settings))
    probe = AsyncClient(base_url=f"http://127.0.0.1:{port}")
    try:
        for _ in range(200):
            try:
                if (await probe.get("/msg/connection", headers=DEMO_HEADERS)).status_code == 200:
                    break
            except Exception:  # 端口尚未监听
                await asyncio.sleep(0.05)
        else:  # pragma: no cover - 仅在启动失败时命中
            pytest.fail("服务未就绪")
        yield port, task
    finally:
        await probe.aclose()
        task.cancel()
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


async def _connection_key(client: AsyncClient) -> str:
    """读一次连接自报并取派生连接键。

    Args:
        client: 客户端。

    Returns:
        str: 派生连接键。
    """
    response = await client.get("/msg/connection", headers=DEMO_HEADERS)
    body = cast("dict[str, Any]", response.json())
    return cast("str", body["data"]["connection_key"])


async def test_retirement_forces_new_connection_key(db_session: Any, tenant_source: object) -> None:
    """连接退休后重连续得上：请求仍成功，且派生连接键变成新的（证明换了 TCP 连接）。

    `keep_alive_max_requests = 2` 时，第 3 个请求必然在新连接上完成；两个连接键都进登记表。
    """
    app = build_frame_app(session=db_session, tenant_source=tenant_source)
    async for port, _task in _serve(app, max_requests=2):
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as client:
            keys: list[str] = []
            for _ in range(3):
                with suppress(Exception):
                    keys.append(await _connection_key(client))
            keys.append(await _connection_key(client))
        assert len(set(keys)) >= 2, f"连接退休后应换新连接（实际键：{keys}）"
        assert app.state.connection_registry.count() >= 2


async def test_idle_recycle_closes_without_goaway(db_session: Any, tenant_source: object) -> None:
    """空闲回收直接关连接（不发 GOAWAY）：复用旧连接会读错误，客户端重连后拿到新连接键。

    这正是「不能把 GOAWAY 当唯一断线信号」的实证——空闲回收没有 GOAWAY，客户端必须先撞一次错误
    再退避重连（`next_backoff_ms` 就是为这条路径准备的）。
    """
    app = build_frame_app(session=db_session, tenant_source=tenant_source)
    async for port, _task in _serve(app, max_requests=1_000, keep_alive_seconds=1):
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as client:
            first = await _connection_key(client)
            await asyncio.sleep(1.6)
            reused_error = False
            try:
                await _connection_key(client)
            except Exception:  # 旧连接已被服务器关闭：复用即读错误
                reused_error = True
            second = await _connection_key(client)
        assert reused_error, "空闲回收不发 GOAWAY，复用旧连接应当报错（而不是静默成功）"
        assert first != second, f"重连后应换新连接（实际键：{first} / {second}）"


async def test_draining_visible_over_real_h2c(db_session: Any, tenant_source: object) -> None:
    """摘流标记经真 h2c 连接可见：客户端据此换实例重连（服务端不主动发 GOAWAY）。"""
    app = build_frame_app(session=db_session, tenant_source=tenant_source)
    async for port, _task in _serve(app, max_requests=1_000):
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as client:
            app.state.draining = True
            response = await client.get("/msg/connection", headers=DEMO_HEADERS)
        body = cast("dict[str, Any]", response.json())
        assert response.status_code == 200
        assert body["data"]["http_version"] == "HTTP/2"
        assert body["data"]["draining"] is True
        assert cast("int", body["data"]["retry_after_ms"]) > 0
