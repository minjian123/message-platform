"""03-5-1 帧订阅超限错误帧（Kiwi 2500）：建立期失败统一单帧 error。

覆盖：
1. 单连接长驻流名额已满（`max_streams_per_connection=0`）→ 单帧 error `350021`，
   `detail.max_streams` / `detail.active_streams` 可见；
2. 超限失败**不占名额**：放开上限后可成功订阅并拿到 `open` 帧；
3. 预检失败（主题不存在）仍为单帧 error（回归，`350001`）；
4. 请求帧头非法仍为单帧 error（回归，`10001`）。
"""

from collections.abc import AsyncIterator
from dataclasses import replace

import pytest
from bms_core.msgclient.frame import FrameType, decode_frame
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_connection_tuning
from bms_msg.broker.connection import ConnectionTuning
from tests_support.frame_client import DEMO_HEADERS, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2500)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
TOPIC = "audit.subscribe.limit"
GROUP = "audit"
CONSUMER = "c1"
BASE_URL = "http://test"

BASE_TUNING = ConnectionTuning(
    heartbeat_interval_ms=100,
    heartbeat_timeout_ms=300,
    idle_timeout_ms=1_000,
    max_connections=8,
    max_streams_per_connection=1,
    evict_scan_limit=8,
    retry_after_ms=10,
)


def _use_max_streams(app: FastAPI, limit: int) -> None:
    """覆盖连接参数里的「单连接长驻流上限」。

    Args:
        app: 应用实例。
        limit: 上限（0 表示一开即拒）。
    """
    tuning = replace(BASE_TUNING, max_streams_per_connection=limit)
    app.dependency_overrides[get_connection_tuning] = lambda: tuning


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """ASGI 客户端（默认上限收到 1）。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    _use_max_streams(msg_app, 1)
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as session:
        yield session


async def _prepare(client: AsyncClient, *, topic: str = TOPIC) -> None:
    """建主题并加入消费组（帧订阅预检的前置）。

    Args:
        client: 客户端。
        topic: 主题名。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": topic, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200, created.text
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": topic, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200, joined.text


def _subscribe_header(**overrides: object) -> dict[str, object]:
    """构造订阅帧头（必填项齐备）。

    Args:
        overrides: 覆盖项。

    Returns:
        dict[str, object]: 帧头。
    """
    extra: dict[str, object] = {
        "namespace": NAMESPACE,
        "topic": TOPIC,
        "group": GROUP,
        "consumer_id": CONSUMER,
        "partition": 0,
        "max_seconds": 1,
    }
    extra.update(overrides)
    return header(extra=extra)


async def _post_subscribe(client: AsyncClient, request_header: dict[str, object]) -> Response:
    """发一次帧订阅请求。

    Args:
        client: 客户端。
        request_header: 帧头。

    Returns:
        Response: 响应（失败时只有一帧）。
    """
    return await post_frame(client, "/msg/subscribe", frame_bytes(int(FrameType.SUBSCRIBE), request_header))


def _error_frame(response: Response) -> tuple[int, object]:
    """取单帧 error 的码与 detail。

    Args:
        response: 响应。

    Returns:
        tuple[int, object]: 错误码与随附数据。
    """
    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    code = frame.header.get("code")
    assert isinstance(code, int)
    return code, frame.header.get("detail")


async def test_stream_limit_returns_error_frame(client: AsyncClient, msg_app: FastAPI) -> None:
    """名额已满 → 单帧 error `350021`，`detail` 带上限与实际用量。"""
    _use_max_streams(msg_app, 0)

    response = await _post_subscribe(client, _subscribe_header())

    code, detail = _error_frame(response)
    assert code == 350021
    assert isinstance(detail, dict)
    assert detail["max_streams"] == 0
    assert detail["active_streams"] == 0


async def test_limit_failure_does_not_consume_slot(client: AsyncClient, msg_app: FastAPI) -> None:
    """先撞一次上限，再放开上限：能成功订阅并拿到 `open` 帧（失败未占名额）。"""
    await _prepare(client)
    _use_max_streams(msg_app, 0)
    rejected = await _post_subscribe(client, _subscribe_header())
    assert _error_frame(rejected)[0] == 350021

    _use_max_streams(msg_app, 1)
    accepted = await _post_subscribe(client, _subscribe_header())

    assert accepted.status_code == 200
    frame = decode_frame(accepted.content)
    assert frame.frame_type == int(FrameType.SUBSCRIBE)


async def test_preflight_failure_stays_single_error_frame(client: AsyncClient) -> None:
    """预检失败（主题不存在）仍为单帧 error（回归，`350001`）。"""
    response = await _post_subscribe(client, _subscribe_header(topic="audit.not.exists"))

    code, _detail = _error_frame(response)
    assert code == 350001


async def test_invalid_frame_header_stays_single_error_frame(client: AsyncClient) -> None:
    """请求帧头缺必填 → 单帧 error（回归，`350018` 帧格式非法）。"""
    body = frame_bytes(int(FrameType.SUBSCRIBE), header(extra={"namespace": NAMESPACE}))
    response = await post_frame(client, "/msg/subscribe", body)

    code, _detail = _error_frame(response)
    assert code == 350018
