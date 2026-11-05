"""inflight 双阈值（任务 03-5；Kiwi 2425）：条数 / 字节、暂停与归还。

三条不可混的口径：

1. **暂停不是错误**：达阈值返空批 + `paused=true`，绝不返 350005（那是生产限流）；
2. **字节按 `len(payload)`**：broker 不解压，`uncompressed_len` 不参与；
3. **`acks=0` 不受阈值约束**：该档位不登记在途，用量恒不增长。

另覆盖归还的三个来源：`ack`、`nack`、以及易主时的 `drop_partition`（字节账一并清）。
"""

import asyncio
import base64
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bms_msg.api.deps import get_backpressure_tuning
from bms_msg.broker.backpressure import BackpressureTuning, InflightUsage
from bms_msg.broker.delivery import DeliveryState, InflightDelivery
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2425)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
TOPIC_NAME = "audit.inflight"
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
    """成员加入组。

    Args:
        client: 客户端。
    """
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200, joined.text


async def _publish(client: AsyncClient, payload: bytes = b"12345678") -> None:
    """发布一条消息（缺省 8 字节，便于算字节额度）。

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


async def _consume(client: AsyncClient, *, acks: int = 1, max_messages: int = 20) -> dict[str, object]:
    """发一次拉取（不等待，便于断言「暂停」而不是「挂起」）。

    Args:
        client: 客户端。
        acks: 投递档位。
        max_messages: 本次上限条数。

    Returns:
        dict[str, object]: 响应 `data`。
    """
    response = await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "max_messages": max_messages,
            "acks": acks,
            "max_wait_ms": 0,
        },
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200, response.text
    return cast("dict[str, object]", response.json()["data"])


def _offsets(data: dict[str, object]) -> list[int]:
    """取本批 offset 列表。

    Args:
        data: 拉取响应 `data`。

    Returns:
        list[int]: offset 升序。
    """
    return [cast("int", item["offset"]) for item in cast("list[dict[str, object]]", data["messages"])]


async def _ack(client: AsyncClient, offset: int) -> None:
    """确认一条消息。

    Args:
        client: 客户端。
        offset: 分区内序号。
    """
    acked = await client.post(
        "/api/v1/msg/ack",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "offset": offset,
        },
        headers=DEMO_HEADERS,
    )
    assert acked.status_code == 200, acked.text


async def _nack(client: AsyncClient, offset: int) -> None:
    """拒绝一条消息（转死信，额度同样归还）。

    Args:
        client: 客户端。
        offset: 分区内序号。
    """
    nacked = await client.post(
        "/api/v1/msg/nack",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "offset": offset,
            "reason": "processing_failed",
        },
        headers=DEMO_HEADERS,
    )
    assert nacked.status_code == 200, nacked.text


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


async def test_message_threshold_pauses_then_resumes(msg_app: FastAPI, client: AsyncClient) -> None:
    """条数到顶停批 + `paused`；ack 归还后继续（无需等超时、无需重启）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=2, inflight_bytes=1_048_576
    )
    await _create_topic(client)
    await _join(client)
    for _ in range(3):
        await _publish(client)

    first = await _consume(client)
    assert _offsets(first) == [0, 1]
    assert first["paused"] is True
    assert first["inflight_messages"] == 2
    assert first["inflight_bytes"] == 16

    paused = await _consume(client)
    assert _offsets(paused) == []
    assert paused["paused"] is True

    await _ack(client, 0)
    await _ack(client, 1)
    resumed = await _consume(client)

    assert _offsets(resumed) == [2]
    assert resumed["paused"] is False


async def test_byte_threshold_splits_batch(msg_app: FastAPI, client: AsyncClient) -> None:
    """字节额度不足即停批（两条 8 字节，额度 12 → 只投一条）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=100, inflight_bytes=12
    )
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    await _publish(client)

    first = await _consume(client)
    assert _offsets(first) == [0]
    assert first["inflight_bytes"] == 8

    blocked = await _consume(client)
    assert _offsets(blocked) == []

    await _ack(client, 0)
    resumed = await _consume(client)
    assert _offsets(resumed) == [1]


async def test_acks_zero_is_not_limited(msg_app: FastAPI, client: AsyncClient) -> None:
    """`acks=0` 不登记在途，阈值对它无效（行为与 03-5 之前一致）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=1, inflight_bytes=1
    )
    await _create_topic(client)
    await _join(client)
    for _ in range(3):
        await _publish(client)

    data = await _consume(client, acks=0)

    assert _offsets(data) == [0, 1, 2]
    assert data["paused"] is False
    assert data["inflight_messages"] == 0


async def test_nack_returns_budget(msg_app: FastAPI, client: AsyncClient) -> None:
    """`nack` 把消息转死信并归还额度（与 ack 同一条归还路径）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=1, inflight_bytes=1_048_576
    )
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    await _publish(client)

    first = await _consume(client)
    assert _offsets(first) == [0]
    await _nack(client, 0)
    resumed = await _consume(client)

    assert _offsets(resumed) == [1]


def test_state_ledger_tracks_bytes_and_releases_on_drop() -> None:
    """字节账：重复登记不虚增、`forget` 归还、易主清账（否则新持有者一上来就超限）。"""
    state = DeliveryState()
    key = ("audit", 1, 0)

    state.track(key, 0, _inflight(), size=8)
    state.track(key, 1, _inflight(), size=8)
    assert state.usage(key) == InflightUsage(messages=2, bytes=16)

    # 重投覆盖同 offset：字节不累加
    state.track(key, 1, _inflight(), size=8)
    assert state.usage(key) == InflightUsage(messages=2, bytes=16)
    assert state.usage(key).remaining(InflightUsage(messages=3, bytes=20)) == InflightUsage(messages=1, bytes=4)

    state.forget(key, 0)
    assert state.usage(key) == InflightUsage(messages=1, bytes=8)

    state.drop_partition(key)
    assert state.usage(key) == InflightUsage(messages=0, bytes=0)


def _inflight() -> InflightDelivery:
    """造一条在途记录（到期时刻与判定无关，用固定偏移即可）。

    Returns:
        InflightDelivery: 在途记录。
    """
    return InflightDelivery(attempts=1, deadline=datetime.now(UTC) + timedelta(seconds=30))


async def test_paused_flag_is_false_when_budget_plenty(msg_app: FastAPI, client: AsyncClient) -> None:
    """额度充足时 `paused` 恒 false（避免客户端误退避）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=100, inflight_bytes=1_048_576
    )
    await _create_topic(client)
    await _join(client)
    await _publish(client)

    data = await _consume(client)

    assert _offsets(data) == [0]
    assert data["paused"] is False


async def test_concurrent_consumes_do_not_double_spend(msg_app: FastAPI, client: AsyncClient) -> None:
    """两个并发拉取不会把同一条消息投两次（预算与在途登记同锁）。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=1, inflight_bytes=1_048_576
    )
    await _create_topic(client)
    await _join(client)
    await _publish(client)

    first, second = await asyncio.gather(_consume(client), _consume(client))
    total = _offsets(first) + _offsets(second)

    assert sorted(total) == [0], total
