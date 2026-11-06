"""RPC 客户端预算与超时（任务 04-2 / 04-7-1；Kiwi 2434）：`deadline_ms` 传播 / 本地超时 / 取消。

判据：

1. **预算写进帧头**：`deadline_ms`（构造缺省 + 每次调用覆盖）落在 `header.deadline_ms`，逐跳可扣；
2. **本地超时**：传输层超时 = 预算 + 200ms 冗余（守住「客户端超时 > 服务端预算」），映射为
   `RpcCallTimeoutError(code=350004)`；
3. **服务端 350004**：错误帧按码映射成同一个异常类（一个语义一种异常）；
4. **`timeout_ms` 已删**（04-7-1）：构造与调用传它即 `TypeError`，不再有第二套预算语义；
5. **取消**：取消调用方协程即以 `CancelledError` 结束（不新增句柄）。
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame, encode_frame
from bms_core.msgclient.rpc import RpcCallError, RpcCallTimeoutError, RpcClient, StaticInstanceSelector
from bms_core.msgclient.transport import (
    RpcHttpRequest,
    RpcHttpResponse,
    RpcTransportTimeoutError,
)

from bms_msg.api.frame.responses import error_frame
from bms_msg.core.config import RPC_CLIENT_TIMEOUT_SLACK_MS, RPC_DEADLINE_MAX_MS
from bms_msg.errors import MsgErrorCode
from tests_support.rpc_transport import ScriptedRpcTransport

pytestmark = pytest.mark.kiwi_id(2434)

BASE_URL = "http://test"
"""实例基址（脚本传输不真正解析 host）。"""


def _client(handler: object, **kwargs: object) -> RpcClient:
    """构造走脚本传输的客户端。

    Args:
        handler: 传输层处理器。
        **kwargs: 透传给 `RpcClient`（预算）。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceSelector([BASE_URL]), transport=ScriptedRpcTransport(handler), **kwargs)  # type: ignore[arg-type]


def _response(status: int = 200, body: bytes = b"") -> RpcHttpResponse:
    """构造传输层响应。

    Args:
        status: 状态码。
        body: 响应体。

    Returns:
        RpcHttpResponse: 响应。
    """
    return RpcHttpResponse(status=status, headers=(), body=body, stream_id=1)


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
    """捕获每次请求的预算与本地超时。

    Yields:
        ConcurrentStableDict[str, int]: 最近一次请求的 `deadline_ms` 与 `timeout_ms`。
    """
    yield ConcurrentStableDict()


async def test_deadline_is_written_into_frame_header(captured: ConcurrentStableDict[str, int]) -> None:
    """每次调用的 `deadline_ms` 进帧头（下游据此继续扣减）。"""

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """截获帧头预算。

        Args:
            request: 传输层请求。

        Returns:
            RpcHttpResponse: 结果帧。
        """
        value = decode_frame(request.body).header["deadline_ms"]
        assert isinstance(value, int)
        captured.set("deadline", value)
        return _response(200, _result(b"ok"))

    client = _client(handler)
    try:
        await client.call("demo", "echo", deadline_ms=1234)
    finally:
        await client.aclose()

    assert captured["deadline"] == 1234


async def test_constructor_budget_is_default(captured: ConcurrentStableDict[str, int]) -> None:
    """构造期预算作为缺省；单次调用不传即用它。"""

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """截获帧头预算。

        Args:
            request: 传输层请求。

        Returns:
            RpcHttpResponse: 结果帧。
        """
        value = decode_frame(request.body).header["deadline_ms"]
        assert isinstance(value, int)
        captured.set("deadline", value)
        return _response(200, _result())

    client = _client(handler, deadline_ms=4321)
    try:
        await client.call("demo", "echo")
    finally:
        await client.aclose()

    assert captured["deadline"] == 4321


def test_timeout_ms_removed_from_constructor() -> None:
    """`timeout_ms` 已删：构造期传入即 `TypeError`（不留两套预算语义）。"""
    with pytest.raises(TypeError):
        RpcClient(StaticInstanceSelector([BASE_URL]), timeout_ms=1000)  # type: ignore[call-arg]


async def test_timeout_ms_removed_from_call() -> None:
    """`timeout_ms` 已删：调用期传入即 `TypeError`。"""

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:  # pragma: no cover - 不会发请求
        del request
        return _response(200, _result())

    client = _client(handler)
    try:
        with pytest.raises(TypeError):
            await client.call("demo", "echo", timeout_ms=1000)  # type: ignore[call-arg]
    finally:
        await client.aclose()


async def test_out_of_range_budget_rejected() -> None:
    """越界预算本地先拦（与服务端同一判据）。"""

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:  # pragma: no cover - 不会发请求
        del request
        return _response(200, _result())

    client = _client(handler)
    try:
        with pytest.raises(ParamError):
            await client.call("demo", "echo", deadline_ms=RPC_DEADLINE_MAX_MS + 1)
    finally:
        await client.aclose()


async def test_local_timeout_carries_350004() -> None:
    """本地兜底超时 = `350004` / business / 不重试 / `outcome=unknown`。"""

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """抛传输层超时。

        Args:
            request: 传输层请求（未使用）。

        Raises:
            RpcTransportTimeoutError: 恒定超时。
        """
        del request
        raise RpcTransportTimeoutError("slow")

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

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """回服务端预算耗尽错误帧。

        Args:
            request: 传输层请求（未使用）。

        Returns:
            RpcHttpResponse: 错误帧。
        """
        del request
        return _response(200, error_frame(code=350004, message="预算耗尽", request_id="r1"))

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

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """回未注册方法错误帧。

        Args:
            request: 传输层请求（未使用）。

        Returns:
            RpcHttpResponse: 错误帧。
        """
        del request
        return _response(200, error_frame(code=350023, message="未注册", request_id="r1"))

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

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """截获传输层预算与本地超时。

        Args:
            request: 传输层请求。

        Returns:
            RpcHttpResponse: 结果帧。
        """
        captured.set("timeout_ms", request.timeout_ms)
        captured.set("deadline", decode_frame(request.body).header["deadline_ms"])  # type: ignore[arg-type]
        return _response(200, _result())

    client = _client(handler)
    try:
        await client.call("demo", "echo", deadline_ms=800)
    finally:
        await client.aclose()

    assert captured["timeout_ms"] == 800 + RPC_CLIENT_TIMEOUT_SLACK_MS


async def test_cancel_ends_with_cancelled_error() -> None:
    """取消调用方协程 → 以 `CancelledError` 结束（取消令牌即 `Task.cancel()`）。"""
    entered = asyncio.Event()

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """挂起等取消。

        Args:
            request: 传输层请求（未使用）。

        Returns:
            RpcHttpResponse: 不会走到。
        """
        del request
        entered.set()
        await asyncio.sleep(5)
        return _response(200, _result())

    client = _client(handler)
    try:
        task = asyncio.create_task(client.call("demo", "echo", deadline_ms=10_000))
        await asyncio.wait_for(entered.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await client.aclose()
