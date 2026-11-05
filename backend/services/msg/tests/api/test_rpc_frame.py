"""RPC 帧形态（任务 04-1；Kiwi 2433）：`application/x-msg-frame` 与 JSON 同一路径、同一实现。

判据：

1. **成功**：`type=48` + `flags.response`，header 回显 `request_id` / `service` / `method` / `length`，
   payload 为结果原始字节（不 base64）；
2. **协议层**：类型不符、置 response 位、带压缩、缺 `request_id`、头里 service / method 与路径不一致
   一律 `350018`；头值类型错 `10001`；
3. **租户**：帧头 `tenant` 与解析结果不一致 → `350012`（与 `/msg/*` 同判据）；
4. **不可解析的帧**：magic 不符走 4xx JSON（03-2 契约）；主版本不支持回 `350019` 错误帧；
5. **业务错误**：未注册方法回 ERROR 帧（HTTP 恒 200），带 `layer=business` / `retryable=false`。
"""

import base64
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.protocol.frame import (
    Frame,
    FrameFlags,
    FrameType,
    decode_frame,
    encode_frame,
)
from tests_support.frame_client import DEMO_HEADERS, FRAME_TYPE, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2433)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

BASE_URL = "http://test"
"""测试基址。"""

RPC_TYPE = int(FrameType.RPC)
"""RPC 帧类型编号（48）。"""


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """帧客户端（ASGI 直连）。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as session:
        yield session


def _error_code(response: Response) -> int:
    """从错误帧取码（同时断言确实是 ERROR 帧、HTTP 200）。

    Args:
        response: 响应。

    Returns:
        int: 错误码。
    """
    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    return int(frame.header["code"])


def _result_frame(response: Response) -> Frame:
    """从成功响应取结果帧（断言 type 与 response 位）。

    Args:
        response: 响应。

    Returns:
        Frame: 结果帧。
    """
    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == RPC_TYPE
    assert frame.flags & FrameFlags.RESPONSE
    return frame


async def test_echo_frame_roundtrip(client: AsyncClient) -> None:
    """帧形态闭环：结果帧回显请求标识，payload 与入参逐字节相同。"""
    body = frame_bytes(RPC_TYPE, header(request_id="r1"), b"\x00raw")

    response = await post_frame(client, "/rpc/demo/echo", body)

    frame = _result_frame(response)
    assert frame.header["request_id"] == "r1"
    assert frame.header["service"] == "demo"
    assert frame.header["method"] == "echo"
    assert frame.header["length"] == 4
    assert bytes(frame.payload) == b"\x00raw"
    assert dict(frame.header["headers"].items()) == {"handler": "demo.echo"}


async def test_missing_request_id_is_frame_invalid(client: AsyncClient) -> None:
    """缺 `request_id` → `350018`（envelope 必填，与数据面同判据）。"""
    body = frame_bytes(RPC_TYPE, {"service": "demo"})

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_wrong_frame_type_rejected(client: AsyncClient) -> None:
    """把 publish 帧发到 RPC 端点 → `350018`。"""
    body = frame_bytes(int(FrameType.PUBLISH), header())

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_request_frame_must_not_set_response_flag(client: AsyncClient) -> None:
    """请求帧置 response 位 → `350018`。"""
    body = frame_bytes(RPC_TYPE, header(), b"", flags=int(FrameFlags.NEED_RESPONSE) | int(FrameFlags.RESPONSE))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_compressed_flag_rejected(client: AsyncClient) -> None:
    """`flags.compressed` 置位 → `350018`（RPC 的 payload 直接交给处理器，没人解压）。"""
    body = frame_bytes(RPC_TYPE, header(), b"xx", flags=int(FrameFlags.COMPRESSED))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_declared_compression_rejected(client: AsyncClient) -> None:
    """只声明 `header.compression` 也拒（与 flags 必须自洽的那条口径对齐）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"compression": "gzip"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_unknown_compression_is_param_error(client: AsyncClient) -> None:
    """`compression` 取值不在白名单 → `10001`（契约层，不是协议层）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"compression": "brotli"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_service_mismatch_rejected(client: AsyncClient) -> None:
    """帧头 `service` 与路径不一致 → `350018`（防串包）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"service": "other"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_method_mismatch_rejected(client: AsyncClient) -> None:
    """帧头 `method` 与路径不一致 → `350018`。"""
    body = frame_bytes(RPC_TYPE, header(extra={"method": "length"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350018


async def test_declared_service_and_method_accepted(client: AsyncClient) -> None:
    """帧头给出且与路径一致时放行（不因「多填了两个键」被拒）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"service": "demo", "method": "length"}), b"1234")

    response = await post_frame(client, "/rpc/demo/length", body)

    assert bytes(_result_frame(response).payload) == b"4"


async def test_tenant_mismatch_rejected(client: AsyncClient) -> None:
    """帧头 `tenant` 与解析结果不一致 → `350012`（帧不作为第二个鉴权入口）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"tenant": "other"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350012


async def test_bad_magic_returns_json(client: AsyncClient) -> None:
    """magic 不符 → 4xx JSON（帧已不可解析，回错误帧没有意义）。"""
    body = b"XX" + frame_bytes(RPC_TYPE, header())[2:]

    response = await client.post("/rpc/demo/echo", content=body, headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE})

    assert response.status_code >= 400
    assert response.json()["code"] == 350018


async def test_unsupported_version_returns_error_frame(client: AsyncClient) -> None:
    """主版本不支持 → `350019` 错误帧（客户端已在按帧说话，给它一个可解析的失败）。"""
    body = encode_frame(frame_type=RPC_TYPE, header=header(), flags=FrameFlags.NEED_RESPONSE, version=(2, 0))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 350019


async def test_unregistered_method_returns_error_frame(client: AsyncClient) -> None:
    """未注册方法 → ERROR 帧（HTTP 200）+ `350023` + `layer=business` / `retryable=false`。"""
    body = frame_bytes(RPC_TYPE, header(request_id="r9"))

    response = await post_frame(client, "/rpc/demo/missing", body)

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == 350023
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is False
    assert frame.header["request_id"] == "r9"
    assert dict(frame.header["detail"].items()) == {"service": "demo", "method": "missing"}


async def test_headers_must_be_string_map(client: AsyncClient) -> None:
    """帧头 `headers` 不是 map<str, str> → `10001`。"""
    body = frame_bytes(RPC_TYPE, header(extra={"headers": {"k": 1}}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_headers_must_be_mapping(client: AsyncClient) -> None:
    """帧头 `headers` 不是映射 → `10001`。"""
    body = frame_bytes(RPC_TYPE, header(extra={"headers": "not-a-map"}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_caller_must_be_string(client: AsyncClient) -> None:
    """帧头 `caller` 非字符串 → `10001`。"""
    body = frame_bytes(RPC_TYPE, header(extra={"caller": 42}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_caller_too_long_is_param_error(client: AsyncClient) -> None:
    """帧头 `caller` 超过长度上限 → `10001`。"""
    body = frame_bytes(RPC_TYPE, header(extra={"caller": "x" * 129}))

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert _error_code(response) == 10001


async def test_blank_caller_in_frame_header_treated_as_absent(client: AsyncClient) -> None:
    """帧头 `caller` 为空串视为未给（不当作非法值拒绝）。"""
    body = frame_bytes(RPC_TYPE, header(extra={"caller": ""}), b"x")

    response = await post_frame(client, "/rpc/demo/echo", body)

    assert bytes(_result_frame(response).payload) == b"x"


async def test_invalid_service_in_path_is_param_error(client: AsyncClient) -> None:
    """帧形态下路径 `service` 形态非法 → `10001`（依赖先于端点参数校验，闸门自己判）。"""
    body = frame_bytes(RPC_TYPE, header())

    response = await post_frame(client, "/rpc/bad!/echo", body)

    assert _error_code(response) == 10001


async def test_invalid_method_in_path_is_param_error(client: AsyncClient) -> None:
    """帧形态下路径 `method` 形态非法 → `10001`。"""
    body = frame_bytes(RPC_TYPE, header())

    response = await post_frame(client, "/rpc/demo/bad!", body)

    assert _error_code(response) == 10001


async def test_json_request_still_works_on_same_path(client: AsyncClient) -> None:
    """同一路径的 JSON 形态不被帧闸门影响（非帧请求直接放行）。"""
    response = await client.post("/rpc/demo/length", json={"payload": ""})

    assert response.status_code == 200
    assert base64.b64decode(response.json()["data"]["payload"]) == b"0"
