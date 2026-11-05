"""生产限速（任务 03-5；Kiwi 2425）：两级令牌桶 + `350005` + `Retry-After`。

四条判据：

1. **默认关闭**：rate=0 时任意发布都放行（既有行为不变）；
2. **租户 / 主题两级**：任一打满即拒，`scope` 指明触发级别；
3. **拒绝形态**：JSON → `429` + `350005` + `Retry-After`；帧 → `error` 帧 + `retry_after_ms`；
4. **覆盖延迟发布**：`delay_level` 不能成为绕过限速的旁路。
"""

import base64
from collections.abc import AsyncIterator
from typing import cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_rate_limiter
from bms_msg.broker.backpressure import RateLimiter, RateLimitTuning
from bms_msg.protocol.frame import FrameFlags, FrameType, decode_frame
from tests_support.frame_client import frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2425)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
TOPIC_NAME = "audit.ratelimit"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
PAYLOAD = base64.b64encode(b"x").decode("ascii")


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


async def _publish(client: AsyncClient, *, delay_level: str | None = None) -> Response:
    """发一次 JSON 发布并返回响应。

    Args:
        client: 客户端。
        delay_level: 延迟档位（None 表示立即发布）。

    Returns:
        Response: 响应。
    """
    body: dict[str, object] = {"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": PAYLOAD}
    if delay_level is not None:
        body["delay_level"] = delay_level
    return await client.post("/api/v1/msg/publish", json=body, headers=DEMO_HEADERS)


def _limiter(*, tenant: int = 0, topic: int = 0) -> RateLimiter:
    """构造两级速率可控的限流器。

    Args:
        tenant: 租户级速率（条/秒）。
        topic: 主题级速率（条/秒）。

    Returns:
        RateLimiter: 限流器。
    """
    return RateLimiter(RateLimitTuning(tenant_per_second=tenant, topic_per_second=topic, burst_seconds=1))


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


async def test_default_is_unlimited(msg_app: FastAPI, client: AsyncClient) -> None:
    """默认关闭：连续发布全部放行，且不产生拒绝计数。"""
    await _create_topic(client)

    for _ in range(3):
        assert (await _publish(client)).status_code == 200

    assert cast("RateLimiter", msg_app.state.rate_limiter).tuning.enabled is False


async def test_tenant_scope_rejects_with_retry_after(msg_app: FastAPI, client: AsyncClient) -> None:
    """租户桶打满 → 429 + 350005 + `Retry-After`（秒，向上取整 ≥1）。"""
    limiter = _limiter(tenant=1)
    msg_app.dependency_overrides[get_rate_limiter] = lambda: limiter
    await _create_topic(client)

    assert (await _publish(client)).status_code == 200

    rejected = await _publish(client)
    assert rejected.status_code == 429
    assert rejected.json()["code"] == 350005
    assert int(rejected.headers["Retry-After"]) >= 1

    snapshot = msg_app.state.backpressure_metrics.snapshot()
    assert snapshot["msg_rate_limit_rejected_total"]["__total__"] == 1


async def test_topic_scope_rejects_independently(msg_app: FastAPI, client: AsyncClient) -> None:
    """主题桶可单独打满（租户额度仍充足），`scope=topic` 计入指标。"""
    limiter = _limiter(tenant=100, topic=1)
    msg_app.dependency_overrides[get_rate_limiter] = lambda: limiter
    await _create_topic(client)

    assert (await _publish(client)).status_code == 200
    rejected = await _publish(client)

    assert rejected.status_code == 429
    snapshot = msg_app.state.backpressure_metrics.snapshot()
    assert snapshot["msg_rate_limit_rejected_total"][f"scope=topic:demo|{TOPIC_NAME}"] == 1


async def test_delayed_publish_is_rate_limited_too(msg_app: FastAPI, client: AsyncClient) -> None:
    """延迟发布同样受限（否则「延迟档」就是绕过限速的旁路）。"""
    limiter = _limiter(tenant=1)
    msg_app.dependency_overrides[get_rate_limiter] = lambda: limiter
    await _create_topic(client)

    assert (await _publish(client, delay_level="1s")).status_code == 200
    rejected = await _publish(client, delay_level="1s")

    assert rejected.status_code == 429
    assert rejected.json()["code"] == 350005


async def test_frame_rejection_carries_retry_after_ms(msg_app: FastAPI, client: AsyncClient) -> None:
    """帧形态：`error` 帧（layer=business / retryable=true）+ `retry_after_ms`。"""
    limiter = _limiter(tenant=1)
    msg_app.dependency_overrides[get_rate_limiter] = lambda: limiter
    await _create_topic(client)
    request_header = header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME})
    body = frame_bytes(int(FrameType.PUBLISH), request_header, b"x", flags=int(FrameFlags.NEED_RESPONSE))

    first = await post_frame(client, "/msg/publish", body)
    assert first.status_code == 200
    assert decode_frame(first.content).header["offset"] == 0

    rejected = await post_frame(client, "/msg/publish", body)
    frame = decode_frame(rejected.content)

    assert rejected.status_code == 200, "帧可解析即协议内失败：HTTP 仍是 200"
    assert frame.header["code"] == 350005
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is True
    assert cast("int", frame.header["retry_after_ms"]) >= 1
