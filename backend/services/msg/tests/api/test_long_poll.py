"""长轮询（任务 03-5；Kiwi 2425）：`/msg/consume` 的 `max_wait_ms`。

四条判据都落在这里：

1. **有消息立即返回**（不因配置了等待而变慢）；
2. **无消息挂起，发布即唤醒**（事件驱动，不等固定间隔）；
3. **到期返空批**（200 + 空数组，不是错误、不是永久挂起）；
4. **`max_wait_ms=0`** 保持 03-5 之前的立即返回语义；等待者超上限立即返空。

另覆盖一个易漏分支：挂起期间成员被撤销 → 唤醒后立即报 `350014`（不能静默继续等）。
"""

import asyncio
import base64
import time
from collections.abc import AsyncIterator
from typing import cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_waiter
from bms_msg.broker.waiter import DeliveryWaiter
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2425)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
TOPIC_NAME = "audit.longpoll"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}


async def _create_topic(client: AsyncClient) -> None:
    """建一个单分区主题（用例前置）。

    Args:
        client: 客户端。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200, created.text


async def _join(client: AsyncClient) -> None:
    """成员加入组（长轮询仍受成员制约束）。

    Args:
        client: 客户端。
    """
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200, joined.text


async def _publish(client: AsyncClient, payload: bytes = b"x") -> None:
    """发布一条消息（载荷按 base64 传输）。

    Args:
        client: 客户端。
        payload: 消息体。
    """
    published = await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": base64.b64encode(payload).decode("ascii")},
        headers=DEMO_HEADERS,
    )
    assert published.status_code == 200, published.text


async def _consume_response(client: AsyncClient, *, max_wait_ms: int, max_messages: int = 10) -> Response:
    """发一次拉取请求并返回原始响应（错误分支要自己断言状态码）。

    Args:
        client: 客户端。
        max_wait_ms: 长轮询挂起上限（毫秒）。
        max_messages: 本次上限条数。

    Returns:
        Response: 原始响应。
    """
    return await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "max_messages": max_messages,
            "max_wait_ms": max_wait_ms,
        },
        headers=DEMO_HEADERS,
    )


async def _consume(client: AsyncClient, *, max_wait_ms: int, max_messages: int = 10) -> dict[str, object]:
    """发一次拉取请求并返回 `data` 段（仅用于期望 200 的用例）。

    Args:
        client: 客户端。
        max_wait_ms: 长轮询挂起上限（毫秒）。
        max_messages: 本次上限条数。

    Returns:
        dict[str, object]: 响应 `data`。
    """
    response = await _consume_response(client, max_wait_ms=max_wait_ms, max_messages=max_messages)
    assert response.status_code == 200, response.text
    return cast("dict[str, object]", response.json()["data"])


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """ASGI 客户端。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as opened:
        yield opened


async def test_returns_immediately_when_messages_exist(client: AsyncClient) -> None:
    """有消息时不因长轮询配置而变慢（一次拉取即返回）。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)

    started = time.monotonic()
    data = await _consume(client, max_wait_ms=5_000)
    elapsed = time.monotonic() - started

    assert len(cast("list[object]", data["messages"])) == 1
    assert data["paused"] is False
    assert elapsed < 1


async def test_wakes_up_when_message_arrives(client: AsyncClient) -> None:
    """拉空后挂起，发布即唤醒（不等 `max_wait_ms` 到期）。"""
    await _create_topic(client)
    await _join(client)

    started = time.monotonic()
    task = asyncio.create_task(_consume(client, max_wait_ms=5_000))
    await asyncio.sleep(0.05)
    await _publish(client)
    data = await asyncio.wait_for(task, timeout=3)
    elapsed = time.monotonic() - started

    assert len(cast("list[object]", data["messages"])) == 1
    assert elapsed < 2, "发布后应立即唤醒，而不是等满 5s 超时"


async def test_returns_empty_after_deadline(client: AsyncClient) -> None:
    """到期返空批（200 + 空数组），不是错误也不是永久挂起。"""
    await _create_topic(client)
    await _join(client)

    started = time.monotonic()
    data = await _consume(client, max_wait_ms=30)
    elapsed = time.monotonic() - started

    assert cast("list[object]", data["messages"]) == []
    assert data["paused"] is False
    assert elapsed >= 0.02


async def test_zero_wait_returns_immediately(client: AsyncClient) -> None:
    """`max_wait_ms=0` 保持「立即返回」语义（03-5 之前的客户端行为）。"""
    await _create_topic(client)
    await _join(client)

    started = time.monotonic()
    data = await _consume(client, max_wait_ms=0)

    assert cast("list[object]", data["messages"]) == []
    assert time.monotonic() - started < 0.5


async def test_rejected_when_waiters_exceed_limit(msg_app: FastAPI, client: AsyncClient) -> None:
    """挂起数到上限立即返空（不排队、不报错），并计入 `rejected` 计数。"""
    await _create_topic(client)
    await _join(client)
    waiter = DeliveryWaiter(max_waiters=1)
    msg_app.dependency_overrides[get_waiter] = lambda: waiter
    holder = asyncio.create_task(waiter.wait((404, 0), version=0, timeout_ms=5_000))
    await asyncio.sleep(0)

    started = time.monotonic()
    data = await _consume(client, max_wait_ms=2_000)
    elapsed = time.monotonic() - started

    assert cast("list[object]", data["messages"]) == []
    assert elapsed < 1
    snapshot = msg_app.state.backpressure_metrics.snapshot()
    assert snapshot["msg_long_poll_rejected_total"] == 1

    waiter.notify((404, 0))
    await holder


async def test_member_lost_during_wait(client: AsyncClient) -> None:
    """挂起期间成员被撤销：唤醒后立即报 `350014`（不静默继续等）。"""
    await _create_topic(client)
    await _join(client)

    task = asyncio.create_task(_consume_response(client, max_wait_ms=5_000))
    await asyncio.sleep(0.05)
    left = await client.post(
        "/api/v1/msg/leave",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert left.status_code == 200
    await _publish(client)

    response = await asyncio.wait_for(task, timeout=3)

    assert response.status_code == 409
    assert response.json()["code"] == 350014
