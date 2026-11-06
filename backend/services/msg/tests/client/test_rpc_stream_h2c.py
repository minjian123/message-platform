"""真 h2c 客户端流式消费（任务 04-3-2；Kiwi 2438）：真 hypercorn + **生产 `RpcClient`**。

与 `tests/client/test_rpc_stream.py` 的分工：那个文件用脚本替身验「切帧 / 序号 / 三态」这类字节级判据；
本文件验**真连接上的行为**——帧真的逐条到达、慢消费不丢帧、`break` + `aclose()` 真的让服务端停下、
同连接的一元调用不受影响、空闲兜底真的发得出 `RST_STREAM`。详细设计 §9 风险 2 的处置就是这一分工：
关键路径一律在真机 h2c 验，ASGI 面只验协议与异常映射。

判据（阈值口径沿用 04-7-2）：

1. 连续 20 帧逐条到达，`seq` 与产出序一致（payload 即序号文本）。
2. **客户端不另设额度**：慢消费时服务端跑在消费前面（客户端不靠「少读」反向卡住服务端），而客户端
   按序收全、不丢帧。
3. `break` + `async with` 退出 → 服务端 **≤150ms** 停止产出（一个 50ms 轮询周期 + 一帧往返 + 调度余量），
   实测值随用例打印。
4. 取消只作用于本流：同客户端随后 `demo.echo` 仍 `200`，`stats().connections == 1`。
5. 空闲兜底：注入 20ms 阈值 + 只推 1 帧的处理器 → 本地超时后发 `RST_STREAM`，服务端流被释放。
"""

import asyncio
import socket
import time
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.config import Settings
from bms_core.core.run import serve_service
from bms_core.models.base import Base
from bms_core.msgclient.rpc import RpcCallTimeoutError, RpcClient, StaticInstanceProvider
from bms_core.msgclient.transport import H2Transport
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from structlog.testing import capture_logs

from bms_msg.rpc.registry import RpcCall, RpcReply
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import DEMO_HEADERS
from tests_support.msg_fixtures import msg_declared_codes, msg_plugins

pytestmark = [pytest.mark.kiwi_id(2438), pytest.mark.own_db]
"""`own_db`：本模块自建临时库（连接 / 请求各开会话，内存库单连接不够用）。"""

__all__ = ["msg_declared_codes", "msg_plugins"]

INTERRUPT_BUDGET_MS = 150.0
"""硬断言：一个 50ms 轮询周期 + 一帧往返 + 调度余量（与 04-7-2 同判据）。"""

TICKER_FRAMES = 20
"""连续推送用例的帧数。"""

SLOW_FRAMES = 30
"""慢消费用例的帧数（每帧客户端睡 10ms，够看出「服务端跑在前面」）。"""

IDLE_MS = 20
"""空闲兜底用例注入的客户端阈值（毫秒；生产 90s 无法实跑）。"""


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
    url = f"sqlite+aiosqlite:///{tmp_path / 'stream-client.db'}"
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
    # 停机宽限压到 1s：被取消的处理器已收尾，但缺省 30s 仍会让 teardown 白等
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


def _client(port: int, transport: H2Transport) -> RpcClient:
    """构造生产客户端（真 h2 传输）。

    Args:
        port: 服务端口。
        transport: 传输层（用例随后断言连接数）。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceProvider([f"http://127.0.0.1:{port}"]), tenant="demo", transport=transport)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
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


def _server_streams(app: FastAPI) -> int:
    """服务端当前占用的长驻流名额。

    Args:
        app: 应用实例。

    Returns:
        int: 名额合计。
    """
    return sum(record.streams for record in app.state.connection_registry.records())


async def test_stream_delivers_frames_in_order(app: FastAPI) -> None:
    """连续 20 帧逐条到达：`seq` 与产出序一致（payload 即序号文本），收尾不发 `RST_STREAM`。"""
    async for port in _serve(app):
        transport = H2Transport()
        client = _client(port, transport)
        try:
            async with client.stream("demo", "ticker", str(TICKER_FRAMES).encode("ascii")) as replies:
                payloads = [reply.payload async for reply in replies]

            assert payloads == [str(index).encode("ascii") for index in range(TICKER_FRAMES)]
            assert transport.stats().open_streams == 0, "收尾后在途流应归零"
        finally:
            await client.aclose()
        assert transport.stats().connections == 0


async def test_slow_consumer_does_not_throttle_server(app: FastAPI) -> None:
    """慢消费：客户端每帧睡 10ms，服务端仍跑在前面（**客户端不另设额度**），且一帧不丢。"""
    produced: list[float] = []

    async def pulser(call: RpcCall) -> AsyncIterator[RpcReply]:
        """按参数条数推帧，逐帧记录产出时刻。

        Args:
            call: 调用入参（ASCII 十进制条数）。

        Yields:
            RpcReply: 逐帧结果。
        """
        for _ in range(int(call.payload.decode("ascii"))):
            produced.append(time.monotonic())
            yield RpcReply(payload=b"x")

    app.state.rpc_registry.register_stream("demo", "pulser", pulser)

    async for port in _serve(app):
        transport = H2Transport()
        client = _client(port, transport)
        try:
            samples: list[int] = []
            async with client.stream("demo", "pulser", str(SLOW_FRAMES).encode("ascii")) as replies:
                async for _reply in replies:
                    samples.append(len(produced) - len(samples))
                    await asyncio.sleep(0.01)

            assert len(samples) == SLOW_FRAMES, "慢消费不该丢帧"
            print(
                f"[04-3-2] 慢消费：服务端已产出 {len(produced)} 帧时客户端只读了 {samples.index(max(samples)) + 1} 帧，"
                f"最大超前 {max(samples)} 帧（客户端不另设额度 ⇒ 不反向卡住服务端）",
                flush=True,
            )
            assert max(samples) > 0, "服务端应先于消费产出（否则说明客户端在反向限流）"
        finally:
            await client.aclose()


async def test_aclose_stops_server_producer(app: FastAPI) -> None:
    """`break` + `async with` 退出：服务端 ≤150ms 停止产出，同客户端的一元调用照常 `200`。"""
    marks: ConcurrentStableDict[str, float] = ConcurrentStableDict()
    produced = [0]

    async def endless(call: RpcCall) -> AsyncIterator[RpcReply]:
        """持续推帧（每帧 5ms），收尾时记时刻。

        **用 `finally` 而不是 `except CancelledError`**：服务端停推有两条路径——取消落在
        `await` 上时抛 `CancelledError`，落在 `yield` 上时是收口处的 `aclose()` 抛 `GeneratorExit`；
        只认前者会让「明明停了却没记时刻」变成偶发失败（实测在并行跑时撞到过）。

        Args:
            call: 调用入参（未使用）。

        Yields:
            RpcReply: 逐帧结果。
        """
        del call
        marks.set("started_at", time.monotonic())
        try:
            while True:
                produced[0] += 1
                yield RpcReply(payload=b"tick")
                await asyncio.sleep(0.005)
        finally:
            marks.set("stopped_at", time.monotonic())

    app.state.rpc_registry.register_stream("demo", "endless", endless)

    async for port in _serve(app):
        transport = H2Transport()
        client = _client(port, transport)
        try:
            received = 0
            async with client.stream("demo", "endless") as replies:
                async for _reply in replies:
                    received += 1
                    if received >= 3:
                        break
                closed_from = time.monotonic()

            for _ in range(300):
                if marks.get("stopped_at") is not None:
                    break
                await asyncio.sleep(0.005)

            interrupted_ms = (marks.get("stopped_at", closed_from) - closed_from) * 1000
            print(
                f"[04-3-2] aclose → 服务端生成器收尾实测 {interrupted_ms:.1f} ms"
                f"（硬断言 ≤{INTERRUPT_BUDGET_MS:.0f}ms）",
                flush=True,
            )
            assert "stopped_at" in marks, "服务端未停推（RST_STREAM 没到？）"
            assert interrupted_ms <= INTERRUPT_BUDGET_MS

            # 停推：再等一个可观测量级，产出计数不该再涨。
            frozen = produced[0]
            await asyncio.sleep(0.2)
            assert produced[0] == frozen, f"取消后仍在产出（{frozen} → {produced[0]}）"

            reply = await client.call("demo", "echo", b"hello")
            assert reply.payload == b"hello", "取消只作用于本流，同连接一元调用应照常"
            stats = transport.stats()
            assert stats.connections == 1, f"取消不该让连接数变化，实际 {stats.connections}"
            assert stats.open_streams == 0
            await _wait_until(lambda: _server_streams(app) == 0)
        finally:
            await client.aclose()


async def test_client_idle_aborts_stream(app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    """空闲兜底：服务端只推 1 帧再不出声 → 本地 20ms 后放弃、发 `RST_STREAM`，服务端流被释放。"""
    monkeypatch.setattr("bms_core.msgclient.stream.RPC_STREAM_CLIENT_IDLE_MS", IDLE_MS)

    async for port in _serve(app):
        transport = H2Transport()
        client = _client(port, transport)
        try:
            with capture_logs() as logs, pytest.raises(RpcCallTimeoutError) as excinfo:
                async with client.stream("demo", "idle") as replies:
                    assert (await replies.__anext__()).payload == b"idle"
                    await replies.__anext__()

            assert excinfo.value.code == 350004
            assert "空闲超时" in str(excinfo.value)
            cancelled = [event for event in logs if event.get("event") == "msg_rpc_client_cancel"]
            assert len(cancelled) == 1
            assert cancelled[0]["reason"] == "timeout"
            assert cancelled[0]["rst_sent"] is True, "空闲兜底必须真的发出 RST_STREAM"
            # RST 到达 + 50ms 断开探测命中后，服务端把名额还回来。
            await _wait_until(lambda: _server_streams(app) == 0)
            assert transport.stats().open_streams == 0
        finally:
            await client.aclose()
