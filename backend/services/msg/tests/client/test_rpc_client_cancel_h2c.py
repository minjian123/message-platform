"""真 h2c 单 stream 取消（任务 04-7-2；Kiwi 2436）：真 hypercorn + **生产 `RpcClient`**。

与 04-2 的 `tests/api/test_rpc_cancel_h2c.py` 的分工（两文件并存，各验一条失效路径）：

| 用例 | 触发 | 验的是 |
| --- | --- | --- |
| `test_rpc_cancel_h2c.py`（04-2） | 连接关闭 | 断开兜底（客户端连 `RST_STREAM` 都发不出的年代留下的路径，仍然有效） |
| 本文件（04-7-2） | 单 stream `RST_STREAM(CANCEL)` | 取消本身就能让服务端停下，且**连接不被牺牲** |

**为什么用生产 `RpcClient` 而不是手搓 h2 客户端**：验收的是「客户端取消」，不是「h2 库能发帧」——
手搓客户端证明的是 spike 已经证明过的事，反而绕过了 `RpcClient` → `H2Transport` 这段真正要交付的接线。

判据（阈值口径见详细设计 §3）：

1. 取消后服务端 handler 在 **≤150ms**（一个 50ms 轮询周期 + 一帧往返 + 调度余量）内被取消，且未跑完；
   实测值随用例打印，`≤100ms` 只作观测口径——服务端 50ms 轮询决定了下界，100ms 硬阈值会做成 flaky 断言。
2. 同连接后续调用照常 `200`，连接数不因取消增长（`stats().connections == 1`）。
3. `aclose()` 后在途流归零、连接归零。
"""

import asyncio
import socket
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.config import Settings
from bms_core.core.run import serve_service
from bms_core.models.base import Base
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.msgclient.transport import H2Transport
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from bms_msg.rpc.registry import RpcCall, RpcReply
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import DEMO_HEADERS
from tests_support.msg_fixtures import msg_declared_codes, msg_plugins

pytestmark = [pytest.mark.kiwi_id(2436), pytest.mark.own_db]
"""`own_db`：本模块自建临时库（连接 / 请求各开会话，内存库单连接不够用）。"""

__all__ = ["msg_declared_codes", "msg_plugins"]

INTERRUPT_BUDGET_MS = 150.0
"""硬断言：一个 50ms 轮询周期 + 一帧往返 + 调度余量。"""

INTERRUPT_OBSERVED_MS = 100.0
"""观测口径（只打印不硬断言）：需求 04-7 原文的「100ms」。"""

SLOW_HANDLER_SECONDS = 10.0
"""慢 handler 的睡眠时长：远大于预算，取消若不生效用例必失败而不是「宽限内跑完」。"""


def _free_port() -> int:
    """取一个本机空闲端口（关闭 socket 后由 hypercorn 重新绑定）。

    Returns:
        int: 端口号。
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """临时文件库引擎（多连接）。

    Args:
        tmp_path: pytest 临时目录。

    Yields:
        AsyncEngine: 已建元数据两表的引擎。
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'cancel-client.db'}"
    created = create_async_engine(url)
    async with created.begin() as connection:
        for table in ("msg_topic", "msg_partition"):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
async def app(engine: AsyncEngine, tenant_source: object) -> AsyncIterator[FastAPI]:
    """最小应用（每请求新开会话）。

    Args:
        engine: 临时文件库引擎。
        tenant_source: 租户源替身。

    Yields:
        FastAPI: 应用实例。
    """
    yield build_frame_app(engine=engine, tenant_source=tenant_source)


async def _serve(app: FastAPI) -> AsyncIterator[int]:
    """在空闲端口起 hypercorn，产出就绪后的端口。

    Args:
        app: 应用实例。

    Yields:
        int: 监听端口。
    """
    port = _free_port()
    settings = Settings()
    settings.server.host = "127.0.0.1"
    settings.server.port = port
    # 停机宽限压到 1s：被取消的 handler 已收尾，但缺省 30s 仍会让 teardown 白等
    settings.server.graceful_timeout_seconds = 1
    task = asyncio.create_task(serve_service(app, settings))
    # 就绪探测用 HTTP/1.1（只探端口是否已服务），被测调用一律走生产 h2 客户端
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
        yield port
    finally:
        await probe.aclose()
        task.cancel()
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


async def test_cancel_interrupts_server_handler(app: FastAPI) -> None:
    """取消在途调用：服务端 handler ≤150ms 内被取消且未跑完；同连接后续调用照常成功。"""
    started = asyncio.Event()
    marks: ConcurrentStableDict[str, float] = ConcurrentStableDict()

    async def slow(call: RpcCall) -> RpcReply:
        """长睡 handler：被取消时记下时刻，跑完则记 `finished_at`。

        Args:
            call: 调用入参（未使用）。

        Returns:
            RpcReply: 只有没被取消才会走到。
        """
        del call
        marks.set("started_at", time.monotonic())
        started.set()
        try:
            await asyncio.sleep(SLOW_HANDLER_SECONDS)
        except asyncio.CancelledError:
            marks.set("cancelled_at", time.monotonic())
            raise
        marks.set("finished_at", time.monotonic())
        return RpcReply(payload=b"late")

    registry = app.state.rpc_registry
    registry.register("cancel", "slow", slow)

    async for port in _serve(app):
        transport = H2Transport()
        client = RpcClient(
            StaticInstanceSelector([f"http://127.0.0.1:{port}"]),
            tenant="demo",
            transport=transport,
        )
        try:
            task = asyncio.create_task(client.call("cancel", "slow", deadline_ms=60_000))
            await asyncio.wait_for(started.wait(), timeout=10)
            assert transport.stats().open_streams == 1, "调用应在途（一条流）"

            cancelled_from = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            for _ in range(200):
                if marks.get("cancelled_at") is not None:
                    break
                await asyncio.sleep(0.005)

            interrupted_ms = (marks.get("cancelled_at", cancelled_from) - cancelled_from) * 1000
            print(
                f"[04-7-2] 取消 → 服务端 handler 中断实测 {interrupted_ms:.1f} ms"
                f"（硬断言 ≤{INTERRUPT_BUDGET_MS:.0f}ms，观测口径 ≤{INTERRUPT_OBSERVED_MS:.0f}ms）",
                flush=True,
            )
            assert "cancelled_at" in marks, "服务端 handler 未被取消（取消信号没到）"
            assert interrupted_ms <= INTERRUPT_BUDGET_MS
            assert "finished_at" not in marks, "handler 跑完了：取消没生效（假绿）"

            # 取消只作用于本流：同连接再调一次照常成功，且连接没被牺牲。
            reply = await client.call("demo", "echo", b"hello")
            assert reply.payload == b"hello"
            stats = transport.stats()
            assert stats.connections == 1, f"取消不该让连接数变化，实际 {stats.connections}"
            assert stats.open_streams == 0
        finally:
            await client.aclose()

        assert transport.stats().open_streams == 0
        assert transport.stats().connections == 0
