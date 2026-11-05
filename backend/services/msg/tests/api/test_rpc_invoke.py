"""RPC 一元调用端点（JSON 形态；任务 04-1；Kiwi 2433）。

判据：

1. **成功闭环**：`demo.echo` 原样回字节、`demo.length` 回长度字节，结果 base64 解码后逐字节可比；
2. **未注册方法**：`350023` / 404 / `layer=business` / `retryable=false`，`data` 带 service 与 method，
   不是 500；
3. **契约层**：`service` / `method` 形态非法、`payload` 非合法 base64 一律 `10001`；
4. **上限**：参数或结果超过 1 MiB → `350006`；
5. **调用方标识**：HTTP 头 `X-Caller-Service` 优先于 JSON 体 `caller`，冲突时以 HTTP 头为准并回写响应头。
"""

import base64
from collections.abc import AsyncIterator

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.protocol.frame import MAX_FRAME_PAYLOAD_BYTES
from bms_msg.rpc.registry import RpcCall, RpcReply
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2433)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""租户头（中间件据此解析租户）。"""

BASE_URL = "http://test"
"""测试基址。"""


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


def _b64(raw: bytes) -> str:
    """编码 base64（请求体 `payload` 字段的载体）。

    Args:
        raw: 原始字节。

    Returns:
        str: base64 文本。
    """
    return base64.b64encode(raw).decode("ascii")


async def _call(client: AsyncClient, path: str, body: dict[str, object]) -> Response:
    """发一次 JSON 调用。

    Args:
        client: 客户端。
        path: 端点路径。
        body: 请求体。

    Returns:
        Response: 响应。
    """
    return await client.post(path, json=body)


async def test_echo_returns_payload_verbatim(client: AsyncClient) -> None:
    """`demo.echo` 原样回字节（含非 UTF-8 字节）。"""
    response = await _call(client, "/rpc/demo/echo", {"payload": _b64(b"\x00\xffok")})

    assert response.status_code == 200
    data = response.json()["data"]
    assert base64.b64decode(data["payload"]) == b"\x00\xffok"
    assert data["headers"]["handler"] == "demo.echo"


async def test_length_returns_decimal_bytes(client: AsyncClient) -> None:
    """`demo.length` 回传十进制长度。"""
    response = await _call(client, "/rpc/demo/length", {"payload": _b64(b"12345")})

    assert response.status_code == 200
    assert base64.b64decode(response.json()["data"]["payload"]) == b"5"


async def test_empty_payload_is_allowed(client: AsyncClient) -> None:
    """缺省 `payload`（空串）等价于无参数。"""
    response = await _call(client, "/rpc/demo/length", {})

    assert response.status_code == 200
    assert base64.b64decode(response.json()["data"]["payload"]) == b"0"


async def test_unregistered_method_returns_350023(client: AsyncClient) -> None:
    """未注册方法：404 + `350023` + `data` 带 service / method（不是 500）。"""
    response = await _call(client, "/rpc/demo/missing", {"payload": ""})

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == 350023
    assert body["data"] == {"service": "demo", "method": "missing"}


async def test_invalid_service_name_is_param_error(client: AsyncClient) -> None:
    """`service` 形态非法 → `10001`（契约层，不是业务码）。"""
    response = await client.post("/rpc/bad!/echo", json={})

    # 契约层错误（`ParamError`）的 HTTP 状态由基座统一错误响应决定（本仓既有口径不断言状态码，
    # 只断言 `code`）——这里同样只判码，避免把「状态码口径」写成第二份事实。
    assert response.json()["code"] == 10001


async def test_invalid_method_name_is_param_error(client: AsyncClient) -> None:
    """`method` 形态非法 → `10001`。"""
    response = await client.post("/rpc/demo/bad!", json={})

    assert response.json()["code"] == 10001


async def test_invalid_base64_payload_is_param_error(client: AsyncClient) -> None:
    """`payload` 不是合法 base64 → `10001`。"""
    response = await _call(client, "/rpc/demo/echo", {"payload": "###"})

    assert response.json()["code"] == 10001


async def test_payload_over_limit_rejected(client: AsyncClient) -> None:
    """参数超过 1 MiB → `350006`（在路由表查询之前就拒，不必先确认方法存在）。"""
    oversized = base64.b64encode(b"x" * (MAX_FRAME_PAYLOAD_BYTES + 1)).decode("ascii")

    response = await _call(client, "/rpc/demo/echo", {"payload": oversized})

    assert response.status_code == 413
    assert response.json()["code"] == 350006


async def test_result_over_limit_rejected(msg_app: FastAPI, client: AsyncClient) -> None:
    """结果超过 1 MiB → `350006`（在编码之前判，不让帧编码抛错变 500）。"""

    async def _big(call: RpcCall) -> RpcReply:
        """返回超大结果。

        Args:
            call: 调用入参（未使用）。

        Returns:
            RpcReply: 超过上限的结果。
        """
        del call
        return RpcReply(payload=b"x" * (MAX_FRAME_PAYLOAD_BYTES + 1))

    msg_app.state.rpc_registry.register("demo", "big", _big)

    response = await _call(client, "/rpc/demo/big", {})

    assert response.status_code == 413
    assert response.json()["code"] == 350006


async def test_caller_header_wins_over_body(client: AsyncClient) -> None:
    """`X-Caller-Service` 优先于 JSON 体 `caller`，且回写进响应头。"""
    response = await client.post(
        "/rpc/demo/echo",
        json={"payload": "", "caller": "from-body", "trace_id": "t-body"},
        headers={"X-Caller-Service": "from-header", "X-Trace-Id": "t-header"},
    )

    assert response.status_code == 200
    assert response.headers["x-caller-service"] == "from-header"
    assert response.headers["x-trace-id"] == "t-header"


async def test_declared_caller_used_when_no_header(client: AsyncClient) -> None:
    """没有 HTTP 头时回落 JSON 体声明值。"""
    response = await client.post("/rpc/demo/echo", json={"payload": "", "caller": "from-body"})

    assert response.status_code == 200
    assert response.headers["x-caller-service"] == "from-body"


async def test_no_caller_leaves_header_absent(client: AsyncClient) -> None:
    """两边都不给时不写响应头（不臆造空值）。"""
    response = await _call(client, "/rpc/demo/echo", {"payload": ""})

    assert response.status_code == 200
    assert "x-caller-service" not in response.headers
    assert "x-trace-id" not in response.headers


async def test_blank_caller_header_treated_as_absent(client: AsyncClient) -> None:
    """空白 HTTP 头视为未给（回落到 JSON 体声明值）。"""
    response = await client.post(
        "/rpc/demo/echo",
        json={"payload": "", "caller": "from-body"},
        headers={"X-Caller-Service": "   "},
    )

    assert response.headers["x-caller-service"] == "from-body"


async def test_blank_caller_in_body_treated_as_absent(client: AsyncClient) -> None:
    """JSON 体里的空串同样视为未给（不写空响应头）。"""
    response = await client.post("/rpc/demo/echo", json={"payload": "", "caller": ""})

    assert "x-caller-service" not in response.headers


async def test_blank_trace_in_body_treated_as_absent(client: AsyncClient) -> None:
    """`trace_id` 为空串时不写响应头。"""
    response = await client.post("/rpc/demo/echo", json={"payload": "", "trace_id": ""})

    assert "x-trace-id" not in response.headers


async def test_request_headers_reach_handler(msg_app: FastAPI, client: AsyncClient) -> None:
    """请求的结构化头透传给处理器（处理器据此决定结果头）。"""
    seen: ConcurrentStableDict[str, str] = ConcurrentStableDict()

    async def _capture(call: RpcCall) -> RpcReply:
        """把收到的头带进结果。

        Args:
            call: 调用入参。

        Returns:
            RpcReply: 结果头 = 请求头。
        """
        for key, value in call.headers.items():
            seen.set(key, value)
        headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        headers.set("tenant", call.tenant)
        return RpcReply(headers=headers)

    msg_app.state.rpc_registry.register("demo", "capture", _capture)

    response = await client.post("/rpc/demo/capture", json={"payload": "", "headers": {"x-biz": "audit"}})

    assert response.status_code == 200
    assert dict(seen.items()) == {"x-biz": "audit"}
    assert response.json()["data"]["headers"]["tenant"] == "demo"
