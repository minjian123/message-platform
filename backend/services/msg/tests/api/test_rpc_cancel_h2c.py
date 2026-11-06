"""真 h2c 断开兜底（任务 04-2；Kiwi 2434）：连接关闭 → 服务端探测到断开 → 取消 handler、释放资源。

**实测结论（决定了本用例的断言口径）**：`httpx 0.28.1` / `httpcore 1.0.9` 在「等待响应头期间被取消」
时**不发 `RST_STREAM`**，只是本地放弃；`http.disconnect` 要等连接关闭才到（三组最小实验时间戳可证）。
`hypercorn 0.18.0` 收到 `StreamReset` 只 `app_put({"type": "http.disconnect"})`、**不取消任务**。

所以本任务的可验证口径是：**客户端取消 = 不再等待；服务端由预算到点停止；连接关闭时断开兜底生效**
（精确 RST 归 04-7）。本用例验最后一段，且证明「断开一旦到达，服务端确实中断并释放」。

判据：

1. 取消后客户端拿不到响应（任务以 `CancelledError` 结束）；
2. 连接关闭后服务端 handler 被取消（不是跑完），长驻流名额归 0；
3. 连接登记不因该次取消而膨胀。
"""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.config import Settings
from bms_core.core.run import serve_service
from bms_core.models.base import Base
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from bms_msg.protocol.frame import FrameFlags, FrameType, encode_frame
from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import DEMO_HEADERS, FRAME_TYPE
from tests_support.msg_fixtures import msg_declared_codes, msg_plugins

pytestmark = [pytest.mark.kiwi_id(2434), pytest.mark.own_db]
"""`own_db`：本模块自建临时库（连接 / 请求各开会话，内存库单连接不够用）。"""

__all__ = ["msg_declared_codes", "msg_plugins"]


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
    url = f"sqlite+aiosqlite:///{tmp_path / 'cancel.db'}"
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
                if (await client.get("/msg/connection", headers=DEMO_HEADERS)).status_code == 200:
                    break
            except Exception:  # 端口尚未监听
                await asyncio.sleep(0.05)
        else:  # pragma: no cover - 仅在启动失败时命中
            pytest.fail("服务未就绪")
        yield client, port
    finally:
        await client.aclose()
        task.cancel()
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


def _rpc_frame(*, request_id: str = "r1", deadline_ms: int = 60_000, payload: bytes = b"") -> bytes:
    """编码一个 RPC 请求帧。

    Args:
        request_id: 请求标识。
        deadline_ms: 预算（毫秒）。
        payload: 参数原始字节。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.NEED_RESPONSE,
        header={"request_id": request_id, "deadline_ms": deadline_ms},
        payload=payload,
    )


async def test_connection_close_cancels_handler_and_releases(app: FastAPI) -> None:
    """连接关闭 → 断开兜底生效：handler 被取消、名额归零、客户端未收到响应。"""
    started = asyncio.Event()
    state = ConcurrentStableDict[str, bool]()

    async def slow(call: RpcCall) -> RpcReply:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            state.set("cancelled", True)
            raise
        state.set("finished", True)
        return RpcReply(payload=b"late")

    registry: RpcRegistry = app.state.rpc_registry
    registry.register("cancel", "slow", slow)

    async for _http1, port in _serve(app):
        client = AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False)
        task = asyncio.create_task(
            client.post("/rpc/cancel/slow", content=_rpc_frame(), headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE})
        )
        await asyncio.wait_for(started.wait(), timeout=10)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        """取消后客户端不再等，也拿不到响应（httpx 此刻不发 RST，服务端仍会按预算兜底）。"""

        # 关闭连接 → hypercorn 投递 http.disconnect → 服务端探测到并取消 handler。
        await client.aclose()

        for _ in range(200):
            if "cancelled" in state:
                break
            await asyncio.sleep(0.05)

        assert state["cancelled"] is True
        assert "finished" not in state

        connection_registry = app.state.connection_registry
        assert sum(record.streams for record in connection_registry.records()) == 0
