# pyright: reportPrivateUsage=false
"""B17 发布载荷与 headers 上限（02-6-1 / Kiwi 2498）。

覆盖：payload 1 MiB 契约边界（350006）、请求体 Content-Length / chunked 早闸、
headers 四重上限（10001）、帧路径与 JSON 路径共用同一模型。
"""

from __future__ import annotations

import base64
import json
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES
from pydantic import ValidationError
from starlette.types import Message, Receive, Scope, Send

from bms_msg.api.data.messages import DATA_PLANE_ROUTES
from bms_msg.api.publish_body_limit import PublishBodyLimitMiddleware, _BodyTooLarge
from bms_msg.core.config import (
    MAX_HEADER_COUNT,
    MAX_HEADER_KEY_LENGTH,
    MAX_HEADER_TOTAL_BYTES,
    MAX_HEADER_VALUE_LENGTH,
    PAYLOAD_BASE64_MAX_LENGTH,
)
from bms_msg.errors import MsgPayloadTooLargeError
from bms_msg.schemas.message import PublishRequest

pytestmark = pytest.mark.kiwi_id(2498)

NAMESPACE = "msg"


def _headers(**overrides: str) -> ConcurrentStableDict[str, str]:
    """构造 headers（测试内联）。"""
    payload: ConcurrentStableDict[str, str] = ConcurrentStableDict({"trace": "abc"})
    for key, value in overrides.items():
        payload.set(key, value)
    return payload


def _request(*, payload: str = "", headers: ConcurrentStableDict[str, str] | None = None) -> PublishRequest:
    """构造发布请求。"""
    return PublishRequest(namespace=NAMESPACE, topic="t", payload=payload, headers=headers or _headers())


def test_payload_exact_limit_is_accepted() -> None:
    """恰好 1 MiB 的 base64 载荷通过契约层。"""
    text = base64.b64encode(b"\x00" * MAX_FRAME_PAYLOAD_BYTES).decode("ascii")
    assert len(text) == PAYLOAD_BASE64_MAX_LENGTH
    request = _request(payload=text)
    assert len(request.payload) == PAYLOAD_BASE64_MAX_LENGTH


def test_payload_over_limit_raises_product_code() -> None:
    """超过 base64 上限抛 350006（保持既有 413 口径，不退化为 10001）。"""
    text = "A" * (PAYLOAD_BASE64_MAX_LENGTH + 4)
    with pytest.raises(MsgPayloadTooLargeError) as excinfo:
        _request(payload=text)
    assert excinfo.value.code == 350006


def test_headers_boundaries_pass() -> None:
    """四重上限的边界值通过（分别验证，不要求同时打满）。"""
    max_key = "k" * MAX_HEADER_KEY_LENGTH
    max_value = "v" * MAX_HEADER_VALUE_LENGTH
    _request(headers=_headers(**{max_key: max_value}))
    pairs: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for index in range(MAX_HEADER_COUNT):
        pairs.set(f"k{index}", "v")
    _request(headers=pairs)


def test_headers_count_over_limit_raises_validation() -> None:
    """对数超过 32 → 契约校验失败（10001）。"""
    pairs: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    for index in range(MAX_HEADER_COUNT + 1):
        pairs.set(f"k{index}", "v")
    with pytest.raises(ValidationError):
        _request(headers=pairs)


def test_headers_key_and_value_over_limit_raise_validation() -> None:
    """单键 / 单值超限 → 契约校验失败。"""
    with pytest.raises(ValidationError):
        _request(headers=_headers(**{"k" * (MAX_HEADER_KEY_LENGTH + 1): "v"}))
    with pytest.raises(ValidationError):
        _request(headers=_headers(v="x" * (MAX_HEADER_VALUE_LENGTH + 1)))


def test_headers_total_bytes_over_limit_raises_validation() -> None:
    """键值总字节超过 8 KiB → 契约校验失败。"""
    pairs: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    chunk = "x" * 1024
    for index in range(MAX_HEADER_TOTAL_BYTES // (len(chunk) + 8) + 1):
        pairs.set(f"k{index}", chunk)
    with pytest.raises(ValidationError):
        _request(headers=pairs)


def test_frame_path_uses_same_request_model() -> None:
    """帧路径的请求模型就是 PublishRequest（同一校验器天然同源）。"""
    publish = next(route for route in DATA_PLANE_ROUTES if route.path == "/publish")
    assert publish.request_model is PublishRequest


def _scope(*, headers: list[tuple[bytes, bytes]]) -> Scope:
    return cast(
        "Scope",
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/msg/publish",
            "headers": headers,
        },
    )


def _messages(messages: list[Message]) -> Send:
    async def _send(message: Message) -> None:
        messages.append(message)

    return _send


async def test_content_length_over_limit_rejected_before_downstream() -> None:
    """Content-Length 超限：下游未被调用，直接 413 / 350006。"""
    called = False

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called = True

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=8)
    messages: list[Message] = []

    async def _receive() -> Message:
        return {"type": "http.request", "body": b""}

    await middleware(
        _scope(headers=[(b"content-type", b"application/json"), (b"content-length", b"9")]),
        _receive,
        _messages(messages),
    )
    assert called is False
    assert messages[0]["type"] == "http.response.start"
    assert messages[0]["status"] == 413
    body = json.loads(messages[1]["body"])
    assert body["code"] == 350006


async def test_chunked_body_over_limit_rejected() -> None:
    """无 Content-Length：累计读取超限同样 413 / 350006。"""
    payloads = [b"12345", b"67890"]
    index = 0

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal index
        while index < len(payloads):
            await receive()

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=8)
    messages: list[Message] = []

    async def _receive() -> Message:
        nonlocal index
        body = payloads[index]
        index += 1
        return {"type": "http.request", "body": body}

    await middleware(
        _scope(headers=[(b"content-type", b"application/json")]),
        _receive,
        _messages(messages),
    )
    assert messages[0]["status"] == 413
    assert json.loads(messages[1]["body"])["code"] == 350006


async def test_non_publish_request_passes_through() -> None:
    """非发布路径直通，不做体大小判定。"""
    called = False

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called = True

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=1)
    scope = _scope(headers=[(b"content-type", b"application/json"), (b"content-length", b"999")])
    scope["path"] = "/api/v1/msg/consume"
    await middleware(scope, cast("Any", object()), _messages([]))
    assert called is True


async def test_non_http_and_non_post_pass_through() -> None:
    """非 HTTP 作用域 / 非 POST 直通。"""
    called = 0

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called += 1

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=1)
    websocket = _scope(headers=[(b"content-type", b"application/json")])
    websocket["type"] = "websocket"
    await middleware(websocket, cast("Any", object()), _messages([]))
    get_scope = _scope(headers=[(b"content-type", b"application/json")])
    get_scope["method"] = "GET"
    await middleware(get_scope, cast("Any", object()), _messages([]))
    assert called == 2


async def test_invalid_content_length_falls_back_to_receive() -> None:
    """非法 Content-Length 按缺失处理，交给累计读取判。"""
    called = False

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        nonlocal called
        called = True

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=8)
    await middleware(
        _scope(headers=[(b"content-type", b"application/json"), (b"content-length", b"oops")]),
        cast("Any", object()),
        _messages([]),
    )
    assert called is True


async def test_downstream_response_is_tracked() -> None:
    """下游正常发送响应：包装 send 记录已开始并透传。"""

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=8)
    messages: list[Message] = []
    await middleware(
        _scope(headers=[(b"content-type", b"application/json"), (b"content-length", b"0")]),
        cast("Any", object()),
        _messages(messages),
    )
    assert [message["type"] for message in messages] == ["http.response.start", "http.response.body"]


async def test_body_too_large_after_response_started_reraises() -> None:
    """响应已开始后才发现超限：不再改写响应，原异常上抛。"""

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await receive()

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=1)
    with pytest.raises(_BodyTooLarge):
        await middleware(
            _scope(headers=[(b"content-type", b"application/json")]),
            cast("Any", _receive_body(b"12345")),
            _messages([]),
        )


async def test_receive_non_request_message_passes_through() -> None:
    """非 `http.request`（如 disconnect）不累计字节，原样交给下游。"""
    seen: list[Message] = []

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(await receive())

    middleware = PublishBodyLimitMiddleware(_app, max_body_bytes=1)

    async def _receive() -> Message:
        return {"type": "http.disconnect"}

    await middleware(
        _scope(headers=[(b"content-type", b"application/json")]),
        _receive,
        _messages([]),
    )
    assert seen == [{"type": "http.disconnect"}]


def _receive_body(body: bytes) -> Receive:
    async def _receive() -> Message:
        return {"type": "http.request", "body": body}

    return _receive
