"""04-2-1 handler 自抛超时分类（Kiwi 2499）：`350034` / 504 / business / 不重试。

**为什么单独成文件**：`test_rpc_deadline.py` 带模块级 `pytestmark = kiwi_id(2434)`，把本子任务
用例放进去会被双重标注（违反「一条用例一个编号」），故独立成文件。

覆盖：
1. `invoke_rpc` 把 handler 自抛的 `TimeoutError` 转成 `MsgRpcHandlerTimeoutError`（进程内不吞）；
2. `data` 四字段：`outcome=unknown` / `source=handler` / `used_ms` / `remaining_ms`；
3. HTTP 形态：504 + `350034`；
4. 帧形态：error 帧 `code=350034`，`detail` 带四字段且 `layer=business` / `retryable=false`；
5. 预算到点仍是 `350004`，两码互斥。
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.deadline import Deadline
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES, FrameType, decode_frame
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.broker.backpressure import RateLimiter, RateLimitTuning
from bms_msg.errors import (
    MsgPayloadTooLargeError,
    MsgRateLimitedError,
    MsgRpcDeadlineExceededError,
    MsgRpcHandlerTimeoutError,
)
from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply
from bms_msg.rpc.service import invoke_rpc
from tests_support.frame_client import DEMO_HEADERS, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2499)

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


def _error_frame(response: Response) -> tuple[int, object]:
    """从错误帧取码与 `detail`（同时断言确实是 ERROR 帧、HTTP 恒 200）。

    Args:
        response: 响应。

    Returns:
        tuple[int, object]: 错误码与随附数据（`dict`）。
    """
    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    code = frame.header.get("code")
    assert isinstance(code, int)
    return code, frame.header.get("detail")


def _assert_handler_timeout_detail(detail: object) -> None:
    """断言 `350034` 的四字段齐备。

    Args:
        detail: error 帧 `detail` 或 JSON `data`。
    """
    assert isinstance(detail, dict)
    assert detail["outcome"] == "unknown"
    assert detail["source"] == "handler"
    assert isinstance(detail["used_ms"], int)
    assert isinstance(detail["remaining_ms"], int)


def _register_boom(app: FastAPI) -> None:
    """注册一个「等下游超时」的 handler。

    Args:
        app: 应用实例。
    """

    async def boom(call: RpcCall) -> RpcReply:
        del call
        raise TimeoutError("downstream slow")

    _registry(app).register("budget", "boom", boom)


async def test_invoke_rpc_converts_handler_timeout(msg_app: FastAPI) -> None:
    """进程内：handler 自抛 `TimeoutError` → `MsgRpcHandlerTimeoutError`（不再裸上抛）。"""
    _register_boom(msg_app)

    with pytest.raises(MsgRpcHandlerTimeoutError) as excinfo:
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.msg_rate_limiter,
            tenant="demo",
            service="budget",
            method="boom",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(5000),
        )

    error = excinfo.value
    assert error.code == 350034
    assert error.http_status == 504
    assert isinstance(error.__cause__, TimeoutError)
    _assert_handler_timeout_detail(error.data)


async def test_http_form_returns_350034(client: AsyncClient, msg_app: FastAPI) -> None:
    """HTTP 形态：504 + `350034` + `data` 四字段。"""
    _register_boom(msg_app)

    response = await client.post("/rpc/budget/boom", json={"deadline_ms": 5000})

    assert response.status_code == 504
    body = response.json()
    assert body["code"] == 350034
    _assert_handler_timeout_detail(body["data"])


async def test_frame_form_returns_350034_error_frame(client: AsyncClient, msg_app: FastAPI) -> None:
    """帧形态：error 帧 `code=350034`、`detail` 四字段、`layer=business` / `retryable=false`。"""
    _register_boom(msg_app)

    body = frame_bytes(int(FrameType.RPC), header(extra={"deadline_ms": 5000}), b"")
    response = await post_frame(client, "/rpc/budget/boom", body)

    code, detail = _error_frame(response)
    assert code == 350034
    _assert_handler_timeout_detail(detail)
    frame = decode_frame(response.content)
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is False


async def test_budget_exhaustion_stays_350004(msg_app: FastAPI) -> None:
    """预算到点仍是 `350004`，不产生 `350034`（两码互斥）。"""

    async def slow(call: RpcCall) -> RpcReply:
        del call
        await asyncio.sleep(5)
        return RpcReply(payload=b"late")

    _registry(msg_app).register("budget", "slow", slow)

    with pytest.raises(MsgRpcDeadlineExceededError) as excinfo:
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.msg_rate_limiter,
            tenant="demo",
            service="budget",
            method="slow",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(60),
        )

    assert excinfo.value.code == 350004


# ---- 覆盖率补口：变更模块 `bms_msg.rpc.service` 的既有分支（`_preflight` 与结果上限） ----


async def test_preflight_rejects_rate_limited_rpc(msg_app: FastAPI) -> None:
    """`_preflight`：RPC 维度桶耗尽 → `350005`，且不进 handler。"""
    msg_app.state.msg_rate_limiter = RateLimiter(
        RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1, rpc_per_second=1)
    )
    called = ConcurrentStableList[str]()

    async def handler(call: RpcCall) -> RpcReply:
        called.add(call.method)
        return RpcReply(payload=b"x")

    _registry(msg_app).register("budget", "quota", handler)

    async def invoke() -> None:
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.msg_rate_limiter,
            tenant="demo",
            service="budget",
            method="quota",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(5000),
        )

    await invoke()
    with pytest.raises(MsgRateLimitedError) as excinfo:
        await invoke()

    assert excinfo.value.code == 350005
    assert called.to_list() == ["quota"]


async def test_preflight_rejects_oversized_request_payload(msg_app: FastAPI) -> None:
    """`_preflight`：参数超 1 MiB → `350006`（不进 handler）。"""

    async def handler(call: RpcCall) -> RpcReply:
        del call
        return RpcReply(payload=b"ok")

    _registry(msg_app).register("budget", "big", handler)

    with pytest.raises(MsgPayloadTooLargeError) as excinfo:
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.msg_rate_limiter,
            tenant="demo",
            service="budget",
            method="big",
            payload=b"x" * (MAX_FRAME_PAYLOAD_BYTES + 1),
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(5000),
        )

    assert excinfo.value.code == 350006


async def test_oversized_reply_is_rejected(msg_app: FastAPI) -> None:
    """结果超 1 MiB → `350006`（handler 已执行，但结果不返回）。"""

    async def handler(call: RpcCall) -> RpcReply:
        del call
        return RpcReply(payload=b"x" * (MAX_FRAME_PAYLOAD_BYTES + 1))

    _registry(msg_app).register("budget", "bigreply", handler)

    with pytest.raises(MsgPayloadTooLargeError) as excinfo:
        await invoke_rpc(
            registry=_registry(msg_app),
            limiter=msg_app.state.msg_rate_limiter,
            tenant="demo",
            service="budget",
            method="bigreply",
            payload=b"",
            headers=ConcurrentStableDict(),
            deadline=Deadline.start(5000),
        )

    assert excinfo.value.code == 350006
