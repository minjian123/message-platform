"""传输层背压的真机取证（任务 03-5；Kiwi 2425）：窗口耗尽自动暂停、不忙等、恢复后继续。

这是**机制判据**，不是性能判据（容量归 8B_12）：

1. 客户端开流后**停止读取**，服务端发送受 h2 窗口约束——进程 CPU 不应升高（协程挂起 ≠ 忙等）；
2. `inflight` 上限仍然生效：暂停期间在途条数不会无限增长；
3. 恢复读取后消息继续到达（窗口更新即继续），并记录到 `window_stall` 计数；
4. 另一条**连接**上的消费者不受影响（应用层隔离；同连接受连接级窗口共享约束，属协议语义）。

为什么必须真起服务器：ASGI 直调没有窗口概念，`send` 不会阻塞，「自动暂停」无从观测。
"""

import asyncio
import base64
import importlib
import json
import socket
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from pathlib import Path

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

from bms_msg.api.deps import get_backpressure_tuning, get_stream_tuning
from bms_msg.broker.backpressure import BackpressureTuning
from bms_msg.broker.transport import StreamTuning
from tests_support.frame_app import build_frame_app

pytestmark = [pytest.mark.kiwi_id(2425), pytest.mark.own_db]
"""`own_db`：并发用例要多连接（内存库 `StaticPool` 只有一个连接）。"""

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"

NAMESPACE = "msg"
TOPIC_A = "audit.window"
TOPIC_B = "audit.window.other"
GROUP_A = "audit"
GROUP_B = "audit-other"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
PAYLOAD_BYTES = 8 * 1024
"""单条载荷 8 KiB：100 条即 800 KiB，足以吃满 h2 缺省的 64 KiB 连接窗口。"""
FAST = StreamTuning(poll_interval_ms=10, max_seconds=2, keepalive_ms=1_000, max_wait_ms=1_000, prefetch=0)


def _free_port() -> int:
    """取一个本机空闲端口。

    Returns:
        int: 端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
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
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """临时文件库引擎（多连接）。

    Args:
        tmp_path: pytest 临时目录。

    Yields:
        AsyncEngine: 已建两表的引擎。
    """
    created = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'window.db'}")
    async with created.begin() as connection:
        for table in ("msg_topic", "msg_partition"):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
async def api_app(engine: AsyncEngine, tenant_source: object) -> AsyncIterator[FastAPI]:
    """最小应用（每请求新开会话）+ 毫秒级流参数 + 收窄 inflight 条数阈值。

    Args:
        engine: 临时文件库引擎。
        tenant_source: 租户源替身。

    Yields:
        FastAPI: 应用实例。
    """
    app = build_frame_app(engine=engine, tenant_source=tenant_source)
    app.dependency_overrides[get_stream_tuning] = lambda: FAST
    app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=100, inflight_bytes=64 * 1024 * 1024
    )
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
        else:  # pragma: no cover - 仅启动失败时命中
            pytest.fail("服务未就绪")
        yield client, port
    finally:
        await client.aclose()
        task.cancel()
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


async def _prepare(client: AsyncClient, *, topic: str, group: str, count: int) -> None:
    """建主题、加入组并发布 `count` 条 8 KiB 消息。

    Args:
        client: HTTP/1.1 客户端。
        topic: 主题名。
        group: 组名。
        count: 发布条数。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": topic, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200, created.text
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": group, "namespace": NAMESPACE, "topic": topic, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200, joined.text
    payload = base64.b64encode(b"z" * PAYLOAD_BYTES).decode("ascii")
    for _ in range(count):
        published = await client.post(
            "/api/v1/msg/publish",
            json={"namespace": NAMESPACE, "topic": topic, "payload": payload},
            headers=DEMO_HEADERS,
        )
        assert published.status_code == 200, published.text


def _params(topic: str, group: str, *, max_messages: int) -> dict[str, str | int]:
    """构造订阅查询参数。

    Args:
        topic: 主题名。
        group: 组名。
        max_messages: 单批条数。

    Returns:
        dict[str, str | int]: 查询参数。
    """
    return {
        "namespace": NAMESPACE,
        "topic": topic,
        "group": group,
        "consumer_id": CONSUMER,
        "partition": 0,
        "max_messages": max_messages,
        "max_seconds": 2,
    }


async def _read_event(lines: AsyncIterator[str]) -> tuple[str, dict[str, object] | None]:
    """读一条 SSE 事件（跨行拼接收到空行为止）。

    Args:
        lines: 行迭代器。

    Returns:
        tuple[str, dict[str, object] | None]: 事件名与 JSON 载荷（注释行为 `("comment", None)`）。
    """
    name = ""
    data = ""
    async for line in lines:
        if line == "":
            if not name and not data:
                continue
            return name, (json.loads(data) if data else None)
        if line.startswith(":"):
            return "comment", None
        if line.startswith("event: "):
            name = line[len("event: ") :]
        elif line.startswith("data: "):
            data = line[len("data: ") :]
    return name, (json.loads(data) if data else None)


async def test_window_exhaustion_pauses_without_busy_wait(api_app: FastAPI) -> None:
    """停读 → 推送停住且不烧 CPU；恢复读取 → 继续推完并记 `window_stall`。"""
    async for http1, port in _serve(api_app):
        await _prepare(http1, topic=TOPIC_A, group=GROUP_A, count=120)

        async with (
            AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as h2,
            h2.stream(
                "GET", "/msg/subscribe", params=_params(TOPIC_A, GROUP_A, max_messages=100), headers=DEMO_HEADERS
            ) as stream,
        ):
            assert stream.status_code == 200
            lines = stream.aiter_lines()
            name, opened = await _read_event(lines)
            assert (name, opened is not None) == ("open", True)

            # 只读到 open 就停手：h2 窗口很快耗在尚未读走的消息体上
            cpu_before = time.process_time()
            await asyncio.sleep(0.6)
            cpu_delta = time.process_time() - cpu_before

            assert cpu_delta < 0.2, f"暂停期间不应忙等（CPU 增量 {cpu_delta:.3f}s）"
            inflight = api_app.state.backpressure_metrics.snapshot()["msg_inflight_messages"]
            assert inflight["__total__"] <= 100, "在途条数受 inflight 阈值约束，不随停读无限增长"

            received = 0
            while True:
                name, _payload = await asyncio.wait_for(_read_event(lines), timeout=10)
                if name == "close":
                    break
                if name == "message":
                    received += 1

            assert received >= 20, f"恢复读取后应继续推送（实收 {received}）"
            stalls = api_app.state.backpressure_metrics.snapshot()["msg_window_stall_total"]
            assert stalls["__total__"] >= 1, "两次推进间隔超阈值应记一次传输层暂停"


async def test_other_connection_is_unaffected(api_app: FastAPI) -> None:
    """一条连接停读不影响另一条连接上的消费者（应用层隔离）。

    同**一条**连接上的多条流仍受 h2 连接级窗口共享约束——那是协议语义，已在契约里写明。
    """
    async for http1, port in _serve(api_app):
        await _prepare(http1, topic=TOPIC_A, group=GROUP_A, count=120)
        await _prepare(http1, topic=TOPIC_B, group=GROUP_B, count=3)

        slow = AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False)
        fast = AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False)
        try:
            async with slow.stream(
                "GET", "/msg/subscribe", params=_params(TOPIC_A, GROUP_A, max_messages=100), headers=DEMO_HEADERS
            ) as stalled:
                lines = stalled.aiter_lines()
                assert (await _read_event(lines))[0] == "open"

                async with fast.stream(
                    "GET", "/msg/subscribe", params=_params(TOPIC_B, GROUP_B, max_messages=10), headers=DEMO_HEADERS
                ) as healthy:
                    healthy_lines = healthy.aiter_lines()
                    assert (await _read_event(healthy_lines))[0] == "open"
                    received = 0
                    while True:
                        name, _payload = await asyncio.wait_for(_read_event(healthy_lines), timeout=10)
                        if name == "close":
                            break
                        if name == "message":
                            received += 1
                    assert received == 3, "另一条连接应正常收完，不受慢消费者牵连"
        finally:
            await slow.aclose()
            await fast.aclose()
