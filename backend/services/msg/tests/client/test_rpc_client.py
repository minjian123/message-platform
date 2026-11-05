"""最小 RPC 客户端（任务 04-1；Kiwi 2433）：一元调用闭环 + 错误映射 + 实例轮询。

判据：

1. **闭环**：客户端发帧、服务端回帧，`demo.echo` 原样回字节、`demo.length` 回长度；
2. **错误映射**：错误帧 → `RpcCallError`（码 / `layer` / `retryable` 照搬服务端，不自行分类）；
   非 200（429 / 413 / 其它）→ 对应码；响应不是合法帧 → protocol 层；
3. **超时与连接失败**：`RpcCallTimeoutError` / transport 层 `RpcCallError`；
4. **实例来源**：`StaticInstanceProvider` 轮询、空列表拒建；客户端可作上下文管理器释放连接池。
"""

from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from fastapi import FastAPI
from httpx import ASGITransport, ConnectError, MockTransport, ReadTimeout, Request, Response

from bms_msg.client.rpc import (
    RpcCallError,
    RpcCallTimeoutError,
    RpcClient,
    StaticInstanceProvider,
)
from bms_msg.protocol.frame import (
    PROTOCOL_VERSION,
    FrameFlags,
    FrameType,
    decode_frame,
    encode_frame,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2433)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

BASE_URL = "http://test"
"""实例基址（ASGI 直连，host 不真正解析）。"""

Handler = Callable[[Request], Coroutine[Any, Any, Response]]
"""MockTransport 的异步处理器类型（`httpx.AsyncHandler` 的等价形状）。"""


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[RpcClient]:
    """指向最小应用的客户端（真实 ASGI 往返，不起服务）。

    Args:
        msg_app: 最小应用。

    Yields:
        RpcClient: 客户端。
    """
    async with RpcClient(
        StaticInstanceProvider([BASE_URL]),
        tenant="demo",
        transport=ASGITransport(app=msg_app),
    ) as opened:
        yield opened


def _client_with(handler: Handler) -> RpcClient:
    """构造走 MockTransport 的客户端（用于错误 / 超时分支）。

    Args:
        handler: 请求处理器。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceProvider([BASE_URL]), transport=MockTransport(handler))


def _result_frame(payload: bytes = b"", *, headers: object = None) -> bytes:
    """构造一个成功结果帧（可控 header 形态，用于异常分支）。

    Args:
        payload: 结果字节。
        headers: 结果头（原样写入，可为非法形态）。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.RESPONSE,
        header={
            "request_id": "r",
            "protocol_version": PROTOCOL_VERSION,
            "service": "demo",
            "method": "echo",
            "length": len(payload),
            "headers": {} if headers is None else headers,
        },
        payload=payload,
    )


def _error_frame(code: object = 350023, *, message: object = "bad") -> bytes:
    """构造一个错误帧（可控 code 形态，用于异常分支）。

    Args:
        code: 错误码（可为非法形态）。
        message: 提示。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.ERROR),
        flags=FrameFlags.RESPONSE,
        header={"request_id": "r", "code": code, "message": message, "retryable": False, "layer": "business"},
    )


async def test_call_echo_roundtrip(client: RpcClient) -> None:
    """真实往返：`demo.echo` 原样回字节，结果头来自处理器。"""
    reply = await client.call("demo", "echo", b"\x00raw")

    assert reply.payload == b"\x00raw"
    assert reply.headers["handler"] == "demo.echo"


async def test_call_length_with_headers(client: RpcClient) -> None:
    """带结构化头的调用同样可用（头透传给处理器）。"""
    reply = await client.call("demo", "length", b"1234", {"x-biz": "audit"})

    assert reply.payload == b"4"


async def test_error_frame_maps_to_rpc_call_error(client: RpcClient) -> None:
    """未注册方法 → `RpcCallError`，码与层照搬服务端。"""
    with pytest.raises(RpcCallError) as caught:
        await client.call("demo", "missing")

    assert caught.value.code == 350023
    assert caught.value.layer == "business"
    assert caught.value.retryable is False


async def test_invalid_name_rejected_locally(client: RpcClient) -> None:
    """本地先拦形态非法（省一次往返，与服务端同码）。"""
    with pytest.raises(ParamError):
        await client.call("bad!", "echo")


async def test_rate_limited_status_mapped() -> None:
    """429 → `350005` + `retryable=True`。"""

    async def handler(request: Request) -> Response:
        """恒回 429。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 429。
        """
        del request
        return Response(429)

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.code == 350005
    assert caught.value.retryable is True


async def test_payload_too_large_status_mapped() -> None:
    """413 → `350006`。"""

    async def handler(request: Request) -> Response:
        """恒回 413。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 413。
        """
        del request
        return Response(413)

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.code == 350006


async def test_other_status_mapped_to_transport() -> None:
    """其它非 200 → code 0 / transport 层（无法映射时不臆造产品码）。"""

    async def handler(request: Request) -> Response:
        """恒回 500。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 500。
        """
        del request
        return Response(500)

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.code == 0
    assert caught.value.layer == "transport"


async def test_timeout_raises_typed_error() -> None:
    """读超时 → `RpcCallTimeoutError`（不是「失败」，码化归 04-2）。"""

    async def handler(request: Request) -> Response:
        """抛读超时。

        Args:
            request: 请求（未使用）。

        Raises:
            ReadTimeout: 恒定超时。
        """
        del request
        raise ReadTimeout("slow")

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallTimeoutError):
            await client.call("demo", "echo", timeout_ms=1)
    finally:
        await client.aclose()


async def test_connection_error_mapped_to_transport() -> None:
    """连接失败 → transport 层 `RpcCallError`。"""

    async def handler(request: Request) -> Response:
        """抛连接失败。

        Args:
            request: 请求（未使用）。

        Raises:
            ConnectError: 恒定失败。
        """
        del request
        raise ConnectError("refused")

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.layer == "transport"


async def test_unparsable_response_is_protocol_error() -> None:
    """响应不是合法帧 → protocol 层（带解析得到的码）。"""

    async def handler(request: Request) -> Response:
        """回一段垃圾字节。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 非帧内容。
        """
        del request
        return Response(200, content=b"not-a-frame")

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.layer == "protocol"
    assert caught.value.code == 350018


async def test_wrong_response_type_is_protocol_error() -> None:
    """响应帧类型既不是 ERROR 也不是 RPC+response → protocol 层。"""

    async def handler(request: Request) -> Response:
        """回一个 publish 帧。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 类型不符的帧。
        """
        del request
        body = encode_frame(frame_type=int(FrameType.PUBLISH), flags=FrameFlags.RESPONSE, header={"request_id": "r"})
        return Response(200, content=body)

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.code == 0
    assert caught.value.layer == "protocol"


async def test_error_frame_without_code_yields_zero() -> None:
    """错误帧缺 `code` 时给 0（错误路径不再抛错）。"""

    async def handler(request: Request) -> Response:
        """回一个缺 code 的错误帧。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 错误帧。
        """
        del request
        return Response(200, content=_error_frame(None))

    client = _client_with(handler)
    try:
        with pytest.raises(RpcCallError) as caught:
            await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert caught.value.code == 0


async def test_non_map_result_headers_yield_empty() -> None:
    """结果帧 `headers` 不是映射 → 空头（不抛，结果本身有效）。"""

    async def handler(request: Request) -> Response:
        """回一个头形态非法的结果帧。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 结果帧。
        """
        del request
        return Response(200, content=_result_frame(b"ok", headers="not-a-map"))

    client = _client_with(handler)
    try:
        reply = await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert reply.payload == b"ok"
    assert dict(reply.headers.items()) == {}


async def test_non_string_header_entries_dropped() -> None:
    """结果帧头里非字符串的项被丢弃（不整包失败）。"""

    async def handler(request: Request) -> Response:
        """回一个混入非字符串项的结果帧。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 结果帧。
        """
        del request
        return Response(200, content=_result_frame(b"ok", headers={"k": 1, "good": "v"}))

    client = _client_with(handler)
    try:
        reply = await client.call("demo", "echo")
    finally:
        await client.aclose()
    assert dict(reply.headers.items()) == {"good": "v"}


async def test_request_frame_carries_trace_and_caller() -> None:
    """`trace_id` / `caller` 进请求帧头（服务端据此写日志与响应头）。"""
    captured: ConcurrentStableDict[str, object] = ConcurrentStableDict()

    async def handler(request: Request) -> Response:
        """截获请求帧头。

        Args:
            request: 请求。

        Returns:
            Response: 固定结果帧。
        """
        for key, value in decode_frame(request.content).header.items():
            captured.set(key, value)
        return Response(200, content=_result_frame(b"ok"))

    client = _client_with(handler)
    try:
        await client.call("demo", "echo", trace_id="t1", caller="identity")
    finally:
        await client.aclose()

    assert captured.get("trace_id") == "t1"
    assert captured.get("caller") == "identity"
    assert captured.get("service") == "demo"
    assert captured.get("method") == "echo"


async def test_request_without_optional_headers_stays_minimal() -> None:
    """不传可选头时请求帧里也不出现这些键（不为空值占位）。"""
    captured: ConcurrentStableDict[str, object] = ConcurrentStableDict()

    async def handler(request: Request) -> Response:
        """截获请求帧头。

        Args:
            request: 请求。

        Returns:
            Response: 固定结果帧。
        """
        for key, value in decode_frame(request.content).header.items():
            captured.set(key, value)
        return Response(200, content=_result_frame(b"ok"))

    client = _client_with(handler)
    try:
        await client.call("demo", "echo")
    finally:
        await client.aclose()

    assert "trace_id" not in captured
    assert "caller" not in captured
    assert "headers" not in captured


async def test_static_provider_round_robins() -> None:
    """静态实例来源按插入序轮询（确定性，易断言）。"""
    provider = StaticInstanceProvider(["http://a/", "http://b"])

    assert provider.next() == "http://a"
    assert provider.next() == "http://b"
    assert provider.next() == "http://a"


def test_static_provider_requires_endpoint() -> None:
    """空列表拒建（否则第一次调用才炸，定位成本高）。"""
    with pytest.raises(ValueError, match="至少需要一个实例地址"):
        StaticInstanceProvider([])


async def test_client_can_be_reused_and_closed() -> None:
    """同一客户端可连续调用；`aclose()` 可重复调用不炸。"""

    async def handler(request: Request) -> Response:
        """回固定结果帧。

        Args:
            request: 请求（未使用）。

        Returns:
            Response: 结果帧。
        """
        del request
        return Response(200, content=_result_frame(b"x"))

    client = _client_with(handler)
    assert (await client.call("demo", "echo")).payload == b"x"
    assert (await client.call("demo", "echo")).payload == b"x"
    await client.aclose()
