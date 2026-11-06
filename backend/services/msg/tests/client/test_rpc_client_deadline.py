"""RPC 客户端预算与超时（任务 04-2；Kiwi 2434）：`deadline_ms` 传播 / 别名 / 本地超时统一 `350004`。

判据：

1. **预算写进帧头**：`deadline_ms`（构造缺省 + 每次调用覆盖）落在 `header.deadline_ms`，逐跳可扣；
2. **别名**：`timeout_ms` 仍可用（内部等价）、两个都给即 `10001`、越界即 `10001`；
3. **本地超时**：`httpx` 超时 = 预算 + 200ms 冗余（守住「客户端超时 > 服务端预算」），抛
   `RpcCallTimeoutError(code=350004)`；
4. **服务端 350004**：错误帧按码映射成同一个异常类（一个语义一种异常）；
5. **取消**：取消调用方协程即以 `CancelledError` 结束（不新增句柄）。
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from httpx import MockTransport, ReadTimeout, Request, Response

from bms_msg.api.frame.responses import error_frame
from bms_msg.client.rpc import RpcCallError, RpcCallTimeoutError, RpcClient, StaticInstanceProvider
from bms_msg.core.config import (
    MS_PER_SECOND,
    RPC_CLIENT_TIMEOUT_SLACK_MS,
    RPC_DEADLINE_MAX_MS,
)
from bms_msg.errors import MsgErrorCode
from bms_msg.protocol.frame import FrameFlags, FrameType, decode_frame, encode_frame
from tests_support.msg_fixtures import msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2434)

__all__ = ["msg_declared_codes", "msg_plugins"]

BASE_URL = "http://test"
"""实例基址（MockTransport 不真正解析 host）。"""


def _client(handler: Any, **kwargs: Any) -> RpcClient:
    """构造走 MockTransport 的客户端。

    Args:
        handler: 请求处理器。
        **kwargs: 透传给 `RpcClient`（预算 / 别名）。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceProvider([BASE_URL]), transport=MockTransport(handler), **kwargs)


async def _read_deadline(request: Request) -> int:
    """从请求帧头取预算。

    Args:
        request: 请求。

    Returns:
        int: 帧头里的 `deadline_ms`。
    """
    frame = decode_frame(request.content)
    value = frame.header["deadline_ms"]
    assert isinstance(value, int)
    return value


def _result(content: bytes = b"", *, request_id: str = "r1") -> bytes:
    """构造一个成功结果帧。

    Args:
        content: 结果原始字节。
        request_id: 请求标识（回显）。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.RESPONSE,
        header={"request_id": request_id, "length": len(content), "used_ms": 1, "remaining_ms": 100},
        payload=content,
    )


@pytest.fixture
async def captured() -> AsyncIterator[ConcurrentStableDict[str, int]]:
    """捕获每次请求的预算与超时（`deadline_ms` 与 `extensions["timeout"]`）。

    Yields:
        ConcurrentStableDict[str, int]: 最近一次请求的 `deadline_ms` 与 `read_timeout_ms`。
    """
    seen: ConcurrentStableDict[str, int] = ConcurrentStableDict()
    yield seen


async def test_deadline_is_written_into_frame_header(captured: ConcurrentStableDict[str, int]) -> None:
    """每次调用的 `deadline_ms` 进帧头（下游据此继续扣减）。"""

    async def handler(request: Request) -> Response:
        captured.set("deadline", await _read_deadline(request))
        return Response(200, content=_result(b"ok"))

    client = _client(handler)
    try:
        await client.call("demo", "echo", deadline_ms=1234)
    finally:
        await client.aclose()

    assert captured["deadline"] == 1234


async def test_constructor_budget_is_default(captured: ConcurrentStableDict[str, int]) -> None:
    """构造期预算作为缺省；单次调用不传即用它。"""

    async def handler(request: Request) -> Response:
        captured.set("deadline", await _read_deadline(request))
        return Response(200, content=_result())

    client = _client(handler, deadline_ms=4321)
    try:
        await client.call("demo", "echo")
    finally:
        await client.aclose()

    assert captured["deadline"] == 4321


async def test_timeout_ms_alias_still_works(captured: ConcurrentStableDict[str, int]) -> None:
    """`timeout_ms` 是弃用别名：等价 `deadline_ms`（并记一条弃用日志）。"""

    async def handler(request: Request) -> Response:
        captured.set("deadline", await _read_deadline(request))
        return Response(200, content=_result())

    client = _client(handler)
    try:
        await client.call("demo", "echo", timeout_ms=2000)
    finally:
        await client.aclose()

    assert captured["deadline"] == 2000


async def test_constructor_alias_still_works(captured: ConcurrentStableDict[str, int]) -> None:
    """构造期别名同样生效（04-1 的调用方不改就能跑）。"""

    async def handler(request: Request) -> Response:
        captured.set("deadline", await _read_deadline(request))
        return Response(200, content=_result())

    client = _client(handler, timeout_ms=1500)
    try:
        await client.call("demo", "echo")
    finally:
        await client.aclose()

    assert captured["deadline"] == 1500


async def test_both_budgets_rejected() -> None:
    """两个都给 → `10001`（不猜优先序）。"""

    async def handler(request: Request) -> Response:  # pragma: no cover - 不会发请求
        del request
        return Response(200, content=_result())

    client = _client(handler)
    try:
        with pytest.raises(ParamError):
            await client.call("demo", "echo", deadline_ms=1000, timeout_ms=1000)
    finally:
        await client.aclose()


async def test_out_of_range_budget_rejected() -> None:
    """越界预算本地先拦（与服务端同一判据）。"""

    async def handler(request: Request) -> Response:  # pragma: no cover - 不会发请求
        del request
        return Response(200, content=_result())

    client = _client(handler)
    try:
        with pytest.raises(ParamError):
            await client.call("demo", "echo", deadline_ms=RPC_DEADLINE_MAX_MS + 1)
    finally:
        await client.aclose()


async def test_local_timeout_carries_350004() -> None:
    """本地兜底超时 = `350004` / business / 不重试 / `outcome=unknown`。"""

    async def handler(request: Request) -> Response:
        del request
        raise ReadTimeout("slow")

    client = _client(handler)
    try:
        with pytest.raises(RpcCallTimeoutError) as caught:
            await client.call("demo", "echo", deadline_ms=50)
    finally:
        await client.aclose()

    assert caught.value.code == int(MsgErrorCode.RPC_DEADLINE_EXCEEDED)
    assert caught.value.layer == "business"
    assert caught.value.retryable is False
    assert caught.value.outcome == "unknown"
    assert caught.value.budget_ms == 50


async def test_server_deadline_maps_to_timeout_error() -> None:
    """服务端 `350004` 错误帧 → 同一个异常类（一个语义一种异常）。"""

    async def handler(request: Request) -> Response:
        del request
        return Response(200, content=error_frame(code=350004, message="预算耗尽", request_id="r1"))

    client = _client(handler)
    try:
        with pytest.raises(RpcCallTimeoutError) as caught:
            await client.call("demo", "echo", deadline_ms=2000)
    finally:
        await client.aclose()

    assert caught.value.code == int(MsgErrorCode.RPC_DEADLINE_EXCEEDED)
    assert caught.value.budget_ms == 2000


async def test_other_error_codes_stay_generic() -> None:
    """非超时错误仍走 `RpcCallError`（不被超时分支吞掉）。"""

    async def handler(request: Request) -> Response:
        del request
        return Response(200, content=error_frame(code=350023, message="未注册", request_id="r1"))

    client = _client(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()

    assert not isinstance(caught.value, RpcCallTimeoutError)
    assert caught.value.code == 350023


async def test_local_timeout_adds_slack_over_budget(captured: ConcurrentStableDict[str, int]) -> None:
    """本地超时 = 预算 + 200ms：服务端先到点，调用方才有机会看到 `350004`。"""

    async def handler(request: Request) -> Response:
        timeout = cast("dict[str, object]", request.extensions["timeout"])
        read = timeout["read"]
        assert isinstance(read, float)
        captured.set("read_ms", int(read * MS_PER_SECOND))
        return Response(200, content=_result())

    client = _client(handler)
    try:
        await client.call("demo", "echo", deadline_ms=800)
    finally:
        await client.aclose()

    assert captured["read_ms"] == 800 + RPC_CLIENT_TIMEOUT_SLACK_MS


async def test_cancel_ends_with_cancelled_error() -> None:
    """取消调用方协程 → 以 `CancelledError` 结束（取消令牌即 `Task.cancel()`）。"""
    entered = asyncio.Event()

    async def handler(request: Request) -> Response:
        del request
        entered.set()
        await asyncio.sleep(5)
        return Response(200, content=_result())

    client = _client(handler)
    try:
        task = asyncio.create_task(client.call("demo", "echo", deadline_ms=10_000))
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await client.aclose()
