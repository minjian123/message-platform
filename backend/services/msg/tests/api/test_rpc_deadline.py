"""RPC 预算与取消（任务 04-2；Kiwi 2434）：两形态的预算预检 / 硬兜底 / 逐跳扣减 / 取消不写响应。

判据：

1. **预检**：`deadline_ms=0` 不调用 handler、不消耗配额，直接 `350004`（504 / `data.outcome=unknown`）；
2. **硬兜底**：handler 跑超预算被取消协程、转 `350004`，返回时 handler 已结束且未完成；
3. **不误判**：handler 自己抛 `TimeoutError`（调下游超时）原样上抛，不被当成预算耗尽；
4. **缺省与区间**：缺失取 30000；越界 / 非整数 `10001`（两形态同码）；
5. **回传**：`used_ms` / `remaining_ms`（JSON 走响应头，帧走结果帧头）；
6. **逐跳**：A → B → C 预算严格递减、每跳减 50ms 返回余量；下游到点后不再长跑；
7. **取消**：handler 吞掉取消并正常返回时，写响应前复核丢弃响应（任务以 `CancelledError` 结束）。
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.backpressure import RateLimiter, RateLimitTuning
from bms_msg.client.rpc import RpcCallTimeoutError, RpcClient, StaticInstanceProvider
from bms_msg.core.config import (
    RPC_DEADLINE_MAX_MS,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_RETURN_RESERVE_MS,
)
from bms_msg.errors import MsgRpcDeadlineExceededError
from bms_msg.protocol.frame import FrameFlags, FrameType, decode_frame
from bms_msg.rpc.deadline import Deadline
from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply
from bms_msg.rpc.service import invoke_rpc
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import DEMO_HEADERS, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2434)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

BASE_URL = "http://test"
"""测试基址（ASGI 直连，host 不真正解析）。"""


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """JSON 客户端（ASGI 直连，不启服务）。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as session:
        yield session


def _registry(app: FastAPI) -> RpcRegistry:
    """取应用的方法注册表。

    Args:
        app: 应用实例。

    Returns:
        RpcRegistry: 注册表。
    """
    registry: RpcRegistry = app.state.rpc_registry
    return registry


def _error_code(response: Response) -> int:
    """从错误帧取码（同时断言确实是 ERROR 帧、HTTP 恒 200）。

    Args:
        response: 响应。

    Returns:
        int: 错误码。
    """
    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    code = frame.header.get("code")
    assert isinstance(code, int)
    return code


def _hop_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """构造一跳应用（共享内存会话：RPC 路径不触库，嵌套调用安全）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    return build_frame_app(session=db_session, tenant_source=tenant_source)


async def test_zero_budget_rejected_before_handler(client: AsyncClient, msg_app: FastAPI) -> None:
    """`deadline_ms=0`：不调 handler，直接 `350004`（504，结果未知）。"""
    called = ConcurrentStableList[str]()

    async def handler(call: RpcCall) -> RpcReply:
        called.add(call.method)
        return RpcReply(payload=b"x")

    _registry(msg_app).register("budget", "counted", handler)

    response = await client.post("/rpc/budget/counted", json={"deadline_ms": 0})

    assert response.status_code == 504
    body = response.json()
    assert body["code"] == 350004
    assert body["data"]["outcome"] == "unknown"
    assert body["data"]["remaining_ms"] == 0
    assert len(called) == 0
    """预检在 handler 之前，所以 handler 一次都没被调。"""


async def test_default_budget_is_reported(client: AsyncClient) -> None:
    """缺省预算 30000：响应头回传已用 / 剩余，两者合计不超过缺省。"""
    response = await client.post("/rpc/demo/echo", json={"payload": ""})

    assert response.status_code == 200
    used = int(response.headers["X-Rpc-Used-Ms"])
    remaining = int(response.headers["X-Rpc-Remaining-Ms"])
    assert used >= 0
    assert remaining + used <= RPC_DEFAULT_TIMEOUT_MS


@pytest.mark.parametrize("value", [-1, RPC_DEADLINE_MAX_MS + 1])
async def test_out_of_range_budget_is_param_error(client: AsyncClient, value: int) -> None:
    """区间外预算 → `10001`（契约层，改参可解）。"""
    response = await client.post("/rpc/demo/echo", json={"deadline_ms": value})

    assert response.json()["code"] == 10001


async def test_handler_aborted_when_budget_exhausted(client: AsyncClient, msg_app: FastAPI) -> None:
    """handler 跑超预算：协程被取消、转 `350004`，且返回时 handler 未完成。"""
    state = ConcurrentStableDict[str, bool]()

    async def slow(call: RpcCall) -> RpcReply:
        state.set("started", True)
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            state.set("cancelled", True)
            raise
        state.set("finished", True)
        return RpcReply(payload=b"late")

    _registry(msg_app).register("budget", "slow", slow)

    response = await client.post("/rpc/budget/slow", json={"deadline_ms": 60})

    assert response.status_code == 504
    assert response.json()["code"] == 350004
    assert state["started"] is True
    assert state["cancelled"] is True
    assert "finished" not in state


async def test_handler_swallowing_cancel_still_reports_deadline(client: AsyncClient, msg_app: FastAPI) -> None:
    """handler 吞掉取消并正常返回：仍然报 `350004`（预算到点是本跳的决定，不因 handler 不配合而丢）。"""

    async def stubborn(call: RpcCall) -> RpcReply:
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(5)
        return RpcReply(payload=b"late")

    _registry(msg_app).register("budget", "stubborn", stubborn)

    response = await client.post("/rpc/budget/stubborn", json={"deadline_ms": 60})

    assert response.status_code == 504
    assert response.json()["code"] == 350004


async def test_handler_own_timeout_is_not_misreported(msg_app: FastAPI) -> None:
    """handler 自己抛 `TimeoutError`（调下游超时）原样上抛，不伪装成预算耗尽。"""

    async def boom(call: RpcCall) -> RpcReply:
        raise TimeoutError("downstream slow")

    _registry(msg_app).register("budget", "boom", boom)

    with pytest.raises(TimeoutError):
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.rate_limiter,
            tenant="demo",
            service="budget",
            method="boom",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(5000),
        )


async def test_exhausted_requests_do_not_consume_quota(msg_app: FastAPI) -> None:
    """预算预检排在限流之前：刷 `deadline_ms=0` 打不穿配额桶。"""
    msg_app.state.rate_limiter = RateLimiter(
        RateLimitTuning(tenant_per_second=0, topic_per_second=0, rpc_per_second=1, burst_seconds=1)
    )
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as session:
        for _ in range(3):
            assert (await session.post("/rpc/demo/echo", json={"deadline_ms": 0})).json()["code"] == 350004
        allowed = await session.post("/rpc/demo/echo", json={})
    assert allowed.status_code == 200


async def test_frame_zero_budget_returns_error_frame(client: AsyncClient) -> None:
    """帧形态同样预检：`350004` 错误帧（business / 不重试）+ `detail.outcome`。"""
    body = frame_bytes(int(FrameType.RPC), header(extra={"deadline_ms": 0}), b"")
    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350004
    frame = decode_frame(response.content)
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is False
    detail = frame.header["detail"]
    assert isinstance(detail, dict)
    assert detail["outcome"] == "unknown"


async def test_frame_invalid_budget_is_param_error(client: AsyncClient) -> None:
    """帧头预算非整数 → 错误帧 `10001`（与 JSON 路径同码）。"""
    body = frame_bytes(int(FrameType.RPC), header(extra={"deadline_ms": "soon"}), b"")
    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_frame_result_reports_budget_usage(client: AsyncClient) -> None:
    """成功结果帧头回传 `used_ms` / `remaining_ms`。"""
    body = frame_bytes(int(FrameType.RPC), header(extra={"deadline_ms": 5000}), b"ok")
    response = await post_frame(client, "/rpc/demo/echo", body)

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.RPC)
    assert frame.flags & FrameFlags.RESPONSE
    used = frame.header["used_ms"]
    remaining = frame.header["remaining_ms"]
    assert isinstance(used, int)
    assert isinstance(remaining, int)
    assert used >= 0
    assert remaining + used <= 5000


async def _call_down(frames_app: FastAPI, service: str, method: str, call: RpcCall) -> RpcReply:
    """用 handler 拿到的下游预算继续调下一跳（下游超时按本跳结果未知上抛）。

    Args:
        frames_app: 下游应用。
        service: 服务标识。
        method: 方法名。
        call: 本跳的调用入参（`deadline_ms` 已是下游预算）。

    Returns:
        RpcReply: 下游结果。

    Raises:
        MsgRpcDeadlineExceededError: 下游返回 `350004`（超时逐跳回传，不当成失败）。
    """
    async with RpcClient(
        StaticInstanceProvider([BASE_URL]),
        tenant="demo",
        deadline_ms=call.deadline_ms,
        transport=ASGITransport(app=frames_app),
    ) as downstream:
        try:
            return await downstream.call(service, method)
        except RpcCallTimeoutError as exc:
            raise MsgRpcDeadlineExceededError(f"下游 {service}.{method} 预算耗尽") from exc


async def test_three_hop_budget_decreases(db_session: AsyncSession, tenant_source: object) -> None:
    """A → B → C：预算严格递减、每跳扣返回余量；下游拿到的是剩余预算而不是原始总超时。"""
    bottom = _hop_app(db_session, tenant_source)
    middle = _hop_app(db_session, tenant_source)
    top = _hop_app(db_session, tenant_source)
    seen = ConcurrentStableDict[str, RpcCall]()

    async def leaf(call: RpcCall) -> RpcReply:
        seen.set("leaf", call)
        return RpcReply(payload=b"c")

    async def second(call: RpcCall) -> RpcReply:
        seen.set("middle", call)
        return await _call_down(bottom, "hop", "c", call)

    async def first(call: RpcCall) -> RpcReply:
        seen.set("top", call)
        return await _call_down(middle, "hop", "b", call)

    _registry(bottom).register("hop", "c", leaf)
    _registry(middle).register("hop", "b", second)
    _registry(top).register("hop", "a", first)

    async with AsyncClient(transport=ASGITransport(app=top), base_url=BASE_URL, headers=DEMO_HEADERS) as client:
        response = await client.post("/rpc/hop/a", json={"deadline_ms": 5000})

    assert response.status_code == 200
    top_budget = seen["top"].deadline_ms
    middle_budget = seen["middle"].deadline_ms
    leaf_budget = seen["leaf"].deadline_ms
    assert top_budget is not None
    assert middle_budget is not None
    assert leaf_budget is not None
    assert top_budget <= 5000 - RPC_RETURN_RESERVE_MS
    assert middle_budget <= top_budget - RPC_RETURN_RESERVE_MS
    assert leaf_budget <= middle_budget - RPC_RETURN_RESERVE_MS
    assert leaf_budget > 0


async def test_downstream_stops_at_its_own_budget(db_session: AsyncSession, tenant_source: object) -> None:
    """上游给足预算时，下游仍按自己那跳的剩余预算到点即停（不长跑）。"""
    bottom = _hop_app(db_session, tenant_source)
    top = _hop_app(db_session, tenant_source)
    state = ConcurrentStableDict[str, bool]()

    async def sleepy(call: RpcCall) -> RpcReply:
        state.set("started", True)
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            state.set("cancelled", True)
            raise
        state.set("finished", True)
        return RpcReply(payload=b"late")

    async def relay(call: RpcCall) -> RpcReply:
        return await _call_down(bottom, "hop", "slow", call)

    _registry(bottom).register("hop", "slow", sleepy)
    _registry(top).register("hop", "relay", relay)

    async with AsyncClient(transport=ASGITransport(app=top), base_url=BASE_URL, headers=DEMO_HEADERS) as client:
        response = await client.post("/rpc/hop/relay", json={"deadline_ms": 300})

    assert response.status_code == 504
    assert response.json()["code"] == 350004
    assert state["started"] is True
    assert state["cancelled"] is True
    assert "finished" not in state


async def test_injected_disconnect_probe_cancels_handler(msg_app: FastAPI) -> None:
    """断开探测命中 → 取消 handler 并上抛取消。

    服务层与传输解耦（`disconnected` 是可注入的探测），所以「断开」这条分支能确定性地验，
    不必依赖真实 `RST_STREAM` 的到达时机（真机那半见 `test_rpc_cancel_h2c.py`）。
    """
    state = ConcurrentStableDict[str, bool]()

    async def slow(call: RpcCall) -> RpcReply:
        state.set("started", True)
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            state.set("cancelled", True)
            raise
        state.set("finished", True)
        return RpcReply(payload=b"late")

    async def always_disconnected() -> bool:
        return True

    _registry(msg_app).register("budget", "dropped", slow)

    with pytest.raises(asyncio.CancelledError):
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.rate_limiter,
            tenant="demo",
            service="budget",
            method="dropped",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(10_000),
            disconnected=always_disconnected,
        )

    assert state["started"] is True
    assert state["cancelled"] is True
    assert "finished" not in state


async def test_cancel_discards_response(msg_app: FastAPI) -> None:
    """handler 吞掉取消并正常返回时，写响应前复核把它丢掉（任务以取消结束，不写 200）。"""
    started = asyncio.Event()

    async def swallow(call: RpcCall) -> RpcReply:
        started.set()
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(5)
        return RpcReply(payload=b"late")

    _registry(msg_app).register("budget", "swallow", swallow)

    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as client:
        task = asyncio.create_task(client.post("/rpc/budget/swallow", json={"deadline_ms": 10000}))
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
