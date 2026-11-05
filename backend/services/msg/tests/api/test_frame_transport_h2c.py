"""真 h2c 传输（任务 03-1；Kiwi 2420）：协议协商 / 同连接多 stream / 取消释放。

为什么必须真起服务器：ASGI 直调没有连接概念（`scope["http_version"]` 恒 `"1.1"`），多路复用、流式响应、
`RST_STREAM` 取消都验不了。做法沿用 bms `test_http2_runtime.py`：`serve_service` 起在空闲端口，
`httpx.AsyncClient(http2=True, http1=False)` 走 **h2c prior knowledge**（只给 `http2=True` 仍协商 HTTP/1.1）。

关键事实（bms 02_04 实测）：明文下 hypercorn 不发 `Upgrade: h2c`，客户端必须直发 HTTP/2 preface。
"""

import asyncio
import importlib
import json
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.plugin import default_plugin_registry
from bms_core.core.run import serve_service
from bms_core.models.base import Base
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from bms_msg.api.deps import get_stream_tuning
from bms_msg.broker.transport import StreamTuning
from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app

pytestmark = [pytest.mark.kiwi_id(2420), pytest.mark.own_db]
"""`own_db`：本模块自建临时库（并发用例需要多连接，内存库 `StaticPool` 只有一个连接）。"""

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
FAST = StreamTuning(poll_interval_ms=10, max_seconds=30, keepalive_ms=100)
"""毫秒级流参数：并发用例不必等秒级，也不至于 60s 才结束。"""


def _free_port() -> int:
    """取一个本机空闲端口（关闭 socket 后由 hypercorn 重新绑定）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。"""
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明。"""
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """临时文件库引擎（多连接：并发用例必需）。

    Args:
        tmp_path: pytest 临时目录。

    Yields:
        AsyncEngine: 已建元数据两表的引擎。
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'h2c.db'}"
    created = create_async_engine(url)
    async with created.begin() as connection:
        for table in ("msg_topic", "msg_partition"):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
async def api_app(engine: AsyncEngine, tenant_source: object) -> AsyncIterator[FastAPI]:
    """最小应用（每个请求 / 每轮拉取新开会话）+ 毫秒级流参数。

    Args:
        engine: 临时文件库引擎。
        tenant_source: 租户源替身。

    Yields:
        FastAPI: 应用实例。
    """
    app = build_frame_app(engine=engine, tenant_source=tenant_source)
    app.dependency_overrides[get_stream_tuning] = lambda: FAST
    yield app


async def _serve(app: FastAPI) -> AsyncIterator[tuple[AsyncClient, int]]:
    """在空闲端口起 hypercorn，产出就绪的 HTTP/1.1 客户端与端口。

    Args:
        app: 应用实例。

    Yields:
        tuple[AsyncClient, int]: 客户端与端口。
    """
    port = _free_port()
    settings = Settings()
    settings.server.host = "127.0.0.1"
    settings.server.port = port
    # 停机宽限压到 1s：用例收尾时若还有活跃流，缺省 30s 的优雅停机会让 teardown 挂住
    settings.server.graceful_timeout_seconds = 1
    task = asyncio.create_task(serve_service(app, settings))
    client = AsyncClient(base_url=f"http://127.0.0.1:{port}")
    try:
        for _ in range(200):
            try:
                response = await client.get("/msg/connection", headers=DEMO_HEADERS)
                if response.status_code == 200:
                    break
            except Exception:  # 端口尚未监听
                await asyncio.sleep(0.05)
        else:  # pragma: no cover - 仅在启动失败时命中
            pytest.fail("服务未就绪")
        yield client, port
    finally:
        await client.aclose()
        task.cancel()
        # 停机可能仍有活跃流：给宽限一点时间，超时也照常退出（不让 teardown 挂住用例）
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


async def _create_topic_and_join(client: AsyncClient) -> None:
    """建主题（单分区）并加入消费组。

    Args:
        client: 客户端。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200


def _subscribe_params() -> dict[str, str | int]:
    """长驻流查询参数（单分区、成员 c1）。"""
    return {"namespace": NAMESPACE, "topic": TOPIC_NAME, "group": GROUP, "consumer_id": CONSUMER, "partition": 0}


def _publish_body() -> dict[str, object]:
    """发布请求体。"""
    return {"namespace": NAMESPACE, "topic": TOPIC_NAME, "key": "k", "payload": "cGF5bG9hZA=="}


async def _read_event(lines: AsyncIterator[str]) -> tuple[str, dict[str, object] | None]:
    """从 SSE 行流读一条事件（跳过注释行与空行）。

    Args:
        lines: `response.aiter_lines()` 产出的异步行迭代器。

    Returns:
        tuple[str, dict[str, object] | None]: 事件名与负载。
    """
    name = ""
    data: dict[str, object] | None = None
    async for line in lines:
        if line.startswith("event: "):
            name = line[len("event: ") :]
        elif line.startswith("data: "):
            data = cast("dict[str, object]", json.loads(line[len("data: ") :]))
        elif line.strip() == "" and name:
            return name, data
    raise AssertionError("SSE 流在事件结束前关闭")


async def _wait_active(app: FastAPI, expected: int, *, timeout: float = 5.0) -> None:
    """等待流登记表到达期望活跃数（取消是异步传播的）。

    Args:
        app: 应用实例。
        expected: 期望活跃流数。
        timeout: 超时秒数。
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if app.state.stream_registry.active() == expected:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"活跃流数未在 {timeout}s 内变为 {expected}（当前 {app.state.stream_registry.active()}）")


async def test_h2c_negotiation_reports_protocol_version(api_app: FastAPI) -> None:
    """同一实例：HTTP/1.1 客户端回 `HTTP/1.1`，h2c prior knowledge 回 `HTTP/2`（元数据即判据）。"""
    async for http1, port in _serve(api_app):
        legacy = await http1.get("/msg/connection", headers=DEMO_HEADERS)
        assert legacy.status_code == 200
        assert legacy.json()["data"]["http_version"] == "HTTP/1.1"

        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as h2:
            negotiated = await h2.get("/msg/connection", headers=DEMO_HEADERS)

        assert negotiated.json()["data"]["http_version"] == "HTTP/2"


async def test_single_connection_multiplexes_stream_and_data_plane(api_app: FastAPI) -> None:
    """同一条 h2c 连接：长驻流开着的同时 publish / consume / ack 都能完成（无队头阻塞）。

    顺序：先开流（读到 `open`）→ 并发发布 + 等流的 `message` → 同连接 consume（队头在途→空批）+ ack
    流里拿到的 offset。三步都在同一条连接上，任一步被流阻塞都会超时。
    """
    async for http1, port in _serve(api_app):
        await _create_topic_and_join(http1)

        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as h2:
            async with h2.stream("GET", "/msg/subscribe", params=_subscribe_params(), headers=DEMO_HEADERS) as stream:
                assert stream.status_code == 200
                assert stream.headers["content-type"].startswith("text/event-stream")
                lines = stream.aiter_lines()
                name, opened = await _read_event(lines)
                assert name == "open"
                assert opened is not None
                assert api_app.state.stream_registry.active() == 1, "流建立后应登记为活跃"

                published = await asyncio.wait_for(
                    h2.post("/msg/publish", json=_publish_body(), headers=DEMO_HEADERS), timeout=10
                )
                assert published.status_code == 200
                assert published.json()["data"]["offset"] == 0

                name, message = await asyncio.wait_for(_read_event(lines), timeout=10)
                while name == "comment":
                    name, message = await asyncio.wait_for(_read_event(lines), timeout=10)
                assert name == "message"
                assert message is not None
                assert message["offset"] == 0, "流必须把发布的消息推出来（同连接复用）"

                # 同连接确认：在流仍开着的时候提交位点
                acked = await asyncio.wait_for(
                    h2.post(
                        "/msg/ack",
                        json={
                            "group": GROUP,
                            "consumer_id": CONSUMER,
                            "namespace": NAMESPACE,
                            "topic": TOPIC_NAME,
                            "partition": 0,
                            "offset": message["offset"],
                        },
                        headers=DEMO_HEADERS,
                    ),
                    timeout=10,
                )
                assert acked.status_code == 200

                # 同连接再拉一批（队头已确认、后续为空 —— 立即返回即证明未被流阻塞）
                pulled = await asyncio.wait_for(
                    h2.post(
                        "/msg/consume",
                        json={
                            "group": GROUP,
                            "consumer_id": CONSUMER,
                            "namespace": NAMESPACE,
                            "topic": TOPIC_NAME,
                            "partition": 0,
                            "max_wait_ms": 0,
                        },
                        headers=DEMO_HEADERS,
                    ),
                    timeout=10,
                )
                assert pulled.status_code == 200

            # 流上下文退出后，同一条连接必须仍能正常请求（流没有把连接占死）
            after = await h2.post("/msg/publish", json=_publish_body(), headers=DEMO_HEADERS)
            assert after.status_code == 200


async def test_client_disconnect_releases_registry(api_app: FastAPI) -> None:
    """客户端断开后流被释放：活跃数归零，服务端不再为空流耗资源。

    **造法说明**：`httpx` 在 `stream()` 上下文提前退出时**不会**立即发 `RST_STREAM`（httpcore 只在连接
    关闭时重置未完成的流），单靠退出上下文测不出释放。这里直接关闭客户端连接——正是「消费者进程消失 /
    网络中断」的真实形态，hypercorn 收到 `StreamReset` → 应用收 `http.disconnect` → 生成器被取消 →
    `finally` 注销登记。
    """
    async for http1, port in _serve(api_app):
        await _create_topic_and_join(http1)

        dying = AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False)
        async with dying.stream("GET", "/msg/subscribe", params=_subscribe_params(), headers=DEMO_HEADERS) as stream:
            assert (await _read_event(stream.aiter_lines()))[0] == "open"
            assert api_app.state.stream_registry.active() == 1
            await dying.aclose()  # 客户端整条连接消失

        await _wait_active(api_app, 0)

        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as fresh:
            assert (await fresh.get("/msg/connection", headers=DEMO_HEADERS)).json()["data"]["http_version"] == "HTTP/2"
