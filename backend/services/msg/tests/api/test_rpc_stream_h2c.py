"""真机 h2c 帧流（任务 04-3-1；Kiwi 2437）：连续推送、EOF 收尾、窗口阻断、同连接复用。

为什么必须真起服务器：ASGI 直调没有连接概念——帧流「逐块到达」「窗口耗尽后写出被挡住」「同一条连接
上再发一元调用」这三件事都验不了。做法沿用 03-1 / 04-2：`serve_service` 起在空闲端口，
`httpx.AsyncClient(http2=True, http1=False)` 走 **h2c prior knowledge**（只给 `http2=True` 仍协商 HTTP/1.1）。

**取消的端到端验收归 04-3-2**：客户端侧 `RpcStream.aclose()` → `RST_STREAM` 那条路径要生产 `RpcClient`
参与，本文件只用 `httpx` 读帧（它不发 RST），故只验「服务端不被慢消费者拖死」与「收尾干净」。
"""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path

import pytest
from bms_core.core.config import Settings
from bms_core.core.run import serve_service
from bms_core.models.base import Base
from bms_core.msgclient.frame import FrameFlags, FrameType, encode_frame
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from bms_msg.rpc.registry import RpcCall, RpcReply
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import DEMO_HEADERS, FRAME_TYPE
from tests_support.msg_fixtures import msg_declared_codes, msg_plugins
from tests_support.rpc_stream import read_stream

pytestmark = [pytest.mark.kiwi_id(2437), pytest.mark.own_db]
"""`own_db`：本模块自建临时库（连接 / 请求各开会话，内存库单连接不够用）。"""

__all__ = ["msg_declared_codes", "msg_plugins"]

FRAME_HEADERS = {**DEMO_HEADERS, "Content-Type": FRAME_TYPE}


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
    url = f"sqlite+aiosqlite:///{tmp_path / 'stream-h2c.db'}"
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
    """在空闲端口起 hypercorn，产出端口号（停机宽限压到 1s，避免收尾挂住）。

    Args:
        app: 应用实例。

    Yields:
        int: 端口号。
    """
    port = _free_port()
    settings = Settings()
    settings.server.host = "127.0.0.1"
    settings.server.port = port
    settings.server.graceful_timeout_seconds = 1
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
        yield port
    finally:
        await probe.aclose()
        task.cancel()
        with suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, 10)


def _rpc_body(payload: bytes = b"", *, request_id: str = "r1", deadline_ms: int = 60_000) -> bytes:
    """编码一个 RPC 请求帧。

    Args:
        payload: 参数原始字节。
        request_id: 请求标识。
        deadline_ms: 预算（毫秒）。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.NEED_RESPONSE,
        header={"request_id": request_id, "deadline_ms": deadline_ms},
        payload=payload,
    )


async def test_stream_frames_over_h2c(app: FastAPI) -> None:
    """20 帧连续到达（`seq` 连续、末帧 `end_of_stream`），EOF 后无残留，同连接一元调用仍 200。"""
    async for port in _serve(app):
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as client:
            buffer = b""
            async with client.stream(
                "POST", "/rpc/demo/ticker/stream", content=_rpc_body(b"20"), headers=FRAME_HEADERS
            ) as response:
                assert response.status_code == 200
                assert response.headers["content-type"].startswith(FRAME_TYPE)
                async for chunk in response.aiter_bytes():
                    buffer += chunk

            entries = read_stream(buffer)
            assert [entry.seq for entry in entries] == list(range(20))
            assert [entry.end for entry in entries[:-1]] == [False] * 19
            assert entries[-1].end is True
            assert bytes(entries[-1].frame.payload) == b"19"
            assert entries[0].frame.header["request_id"] == "r1"

            # 同一条连接上再发一元调用：流式不该污染一元面。
            unary = await client.post("/rpc/demo/echo", content=_rpc_body(b"after"), headers=FRAME_HEADERS)
            assert unary.status_code == 200
            assert bytes(read_stream(unary.content)[0].frame.payload) == b"after"


async def test_slow_reader_stalls_producer(app: FastAPI) -> None:
    """客户端不读 → 写出被窗口 / 缓冲挡住 → 处理器停止产出；恢复读取后帧序仍然连续。"""
    produced: list[int] = []
    total = 400
    chunk = b"y" * 1024

    async def bulk(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        for index in range(total):
            produced.append(index)
            yield RpcReply(payload=chunk)

    app.state.rpc_registry.register_stream("demo", "bulk", bulk)

    async for port in _serve(app):
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}", http2=True, http1=False) as client:
            async with client.stream(
                "POST", "/rpc/demo/bulk/stream", content=_rpc_body(), headers=FRAME_HEADERS
            ) as response:
                assert response.status_code == 200
                chunks = response.aiter_bytes()
                first = await chunks.__anext__()
                await asyncio.sleep(0.2)
                stalled = len(produced)
                rest = b"".join([piece async for piece in chunks])

            print(f"慢消费者实测：客户端停读 200ms 时已产出 {stalled}/{total} 帧")
            assert stalled < total, f"客户端不读却已产出 {stalled}/{total} 帧，背压没生效"
            entries = read_stream(first + rest)
            assert [entry.seq for entry in entries] == list(range(total))
            assert entries[-1].end is True
