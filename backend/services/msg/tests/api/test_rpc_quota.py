"""RPC 配额（任务 04-1；Kiwi 2433）：`(tenant, service)` 独立令牌桶 + `350005`。

判据：

1. **缺省关闭**：`rpc_per_second = 0` 时不建桶、恒放行；
2. **独立维度**：RPC 桶与生产两级桶（tenant / topic）互不消耗——大消息发布打满 topic 桶不会连带
   拒 RPC，反之亦然（任务 §2.6「大消息发布不得挤占 RPC」在配额层的落点）；
3. **拒绝形态**：JSON → `429` + `350005` + `Retry-After`；帧 → `error` 帧 + `retry_after_ms`；
4. **桶上限**：RPC 桶计入 `buckets()`，并参与「先清满桶、再按插入序淘汰」。
"""

from collections.abc import AsyncIterator

import pytest
from bms_core.msgclient.frame import FrameType, decode_frame
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.broker.backpressure import RateLimiter, RateLimitTuning
from tests_support.frame_client import DEMO_HEADERS, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2433)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

RPC_TYPE = int(FrameType.RPC)
"""RPC 帧类型编号。"""

TOPIC_KEY = ("demo", "msg", "audit")
"""生产限流的主题键。"""

NOW = 1_000.0
"""固定时刻（令牌桶按 elapsed 补，注入固定值让断言确定）。"""


def _limiter(*, tenant: int = 0, topic: int = 0, rpc: int = 0, bucket_limit: int = 4_096) -> RateLimiter:
    """构造三级速率可控的限流器。

    Args:
        tenant: 租户级速率（条/秒）。
        topic: 主题级速率（条/秒）。
        rpc: RPC 级速率（次/秒）。
        bucket_limit: 桶数量上限。

    Returns:
        RateLimiter: 限流器。
    """
    return RateLimiter(
        RateLimitTuning(
            tenant_per_second=tenant,
            topic_per_second=topic,
            burst_seconds=1,
            rpc_per_second=rpc,
        ),
        bucket_limit=bucket_limit,
    )


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """ASGI 客户端。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(
        transport=ASGITransport(app=msg_app), base_url="http://test", headers=DEMO_HEADERS
    ) as opened:
        yield opened


def test_default_disabled_allows_everything() -> None:
    """缺省 0 = 不限：连调多次都放行且不建桶。"""
    limiter = _limiter()

    for _ in range(5):
        verdict = limiter.check_rpc(tenant="demo", service="demo", now=NOW)
        assert verdict.allowed is True
        assert verdict.scope == ""
    assert limiter.buckets() == 0


def test_rpc_scope_rejects_after_burst() -> None:
    """开启后突发用尽即拒，`scope` 指明 rpc 维度并给出建议重试毫秒。"""
    limiter = _limiter(rpc=1)

    assert limiter.check_rpc(tenant="demo", service="demo", now=NOW).allowed is True
    rejected = limiter.check_rpc(tenant="demo", service="demo", now=NOW)
    assert rejected.allowed is False
    assert rejected.scope == "rpc"
    assert rejected.retry_after_ms >= 1


def test_rpc_bucket_is_per_service() -> None:
    """不同服务各有独立桶（同一租户下互不影响）。"""
    limiter = _limiter(rpc=1)

    assert limiter.check_rpc(tenant="demo", service="demo", now=NOW).allowed is True
    assert limiter.check_rpc(tenant="demo", service="other", now=NOW).allowed is True


def test_topic_backpressure_does_not_affect_rpc() -> None:
    """生产 topic 桶打满不影响 RPC（反之亦然）。"""
    limiter = _limiter(topic=1, rpc=1)

    assert limiter.check(tenant="demo", topic=TOPIC_KEY, now=NOW).allowed is True
    assert limiter.check(tenant="demo", topic=TOPIC_KEY, now=NOW).allowed is False
    # RPC 桶没被动过：第一次调用仍放行。
    assert limiter.check_rpc(tenant="demo", service="demo", now=NOW).allowed is True


def test_rpc_backpressure_does_not_affect_production() -> None:
    """RPC 桶打满不影响生产两级桶。"""
    limiter = _limiter(topic=1, rpc=1)

    assert limiter.check_rpc(tenant="demo", service="demo", now=NOW).allowed is True
    assert limiter.check_rpc(tenant="demo", service="demo", now=NOW).allowed is False
    assert limiter.check(tenant="demo", topic=TOPIC_KEY, now=NOW).allowed is True


def test_enabled_covers_rpc_only() -> None:
    """只开 RPC 一级也算「已启用」（否则 `check()` 会短路成「全不限」）。"""
    assert RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1).enabled is False
    assert RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1, rpc_per_second=1).enabled is True


def test_rpc_buckets_counted_and_pruned_when_full() -> None:
    """RPC 桶计入 `buckets()`；达上限时先清「已满且无负债」的桶。"""
    limiter = _limiter(rpc=1, bucket_limit=2)

    limiter.check_rpc(tenant="t1", service="s", now=NOW)
    limiter.check_rpc(tenant="t2", service="s", now=NOW)
    assert limiter.buckets() == 2
    # 时间推进后前两个桶被补满 → 清理走「清满桶」分支，不相干地腾出名额。
    limiter.check_rpc(tenant="t3", service="s", now=NOW + 10)
    assert limiter.buckets() == 1


def test_rpc_buckets_evicted_when_no_full_bucket() -> None:
    """没有满桶时按插入序淘汰（RPC 桶也在淘汰范围内）。"""
    limiter = _limiter(rpc=1, bucket_limit=2)

    # 每次调用都消费 1 个令牌，桶不再「满」（容量 1、已用 1）——清理只能走淘汰分支。
    limiter.check_rpc(tenant="t1", service="s", now=NOW)
    limiter.check_rpc(tenant="t2", service="s", now=NOW)
    limiter.check_rpc(tenant="t3", service="s", now=NOW)

    assert limiter.buckets() == 2


async def test_endpoint_rejects_with_429_and_retry_after(msg_app: FastAPI, client: AsyncClient) -> None:
    """端点层：超限 → `429` + `350005` + `Retry-After`（JSON 形态）。"""
    # 直接换进程级单例（`get_rate_limiter` 读 `app.state`）：与生产装配同一入口，不必依赖
    # `dependency_overrides` 的匹配细节。
    msg_app.state.msg_rate_limiter = _limiter(rpc=1)

    first = await client.post("/rpc/demo/echo", json={"payload": ""})
    second = await client.post("/rpc/demo/echo", json={"payload": ""})

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["code"] == 350005
    assert "retry-after" in second.headers


async def test_frame_rejection_carries_retry_after_ms(msg_app: FastAPI, client: AsyncClient) -> None:
    """帧形态：超限 → ERROR 帧 + `350005` + `retry_after_ms`（HTTP 仍 200）。"""
    msg_app.state.msg_rate_limiter = _limiter(rpc=1)
    body = frame_bytes(RPC_TYPE, header(request_id="q1"), b"x")

    await post_frame(client, "/rpc/demo/echo", body)
    rejected: Response = await post_frame(client, "/rpc/demo/echo", body)

    assert rejected.status_code == 200
    frame = decode_frame(rejected.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == 350005
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is True
    assert isinstance(frame.header["retry_after_ms"], int)
