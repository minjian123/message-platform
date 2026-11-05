"""背压额度与两级令牌桶（任务 03-5；Kiwi 2425）。

要点：

- `InflightUsage.remaining` 逐项 `max(0, …)`，`exhausted` 任一维度到顶即算满；
- 令牌桶「判」与「扣」分离——两级先都判够再一起扣，避免主题桶拒绝时白扣租户桶；
- 桶数上限先清满桶、再按插入序淘汰，防高基数打爆内存。
"""

import pytest

from bms_msg.broker.backpressure import (
    BackpressureTuning,
    InflightUsage,
    RateLimiter,
    RateLimitTuning,
    RateVerdict,
    TokenBucket,
    default_backpressure_tuning,
    default_rate_limit_tuning,
    retry_after_ms,
)
from bms_msg.core.config import (
    INFLIGHT_MAX_BYTES,
    INFLIGHT_MAX_MESSAGES,
    RATE_LIMIT_RETRY_AFTER_MS,
)

pytestmark = pytest.mark.kiwi_id(2425)

TENANT = "demo"
TOPIC = ("demo", "msg", "audit.operation")


def test_inflight_usage_remaining_and_exhausted() -> None:
    """剩余额度逐项夹到 0；任一维度到顶即算「满」。"""
    limit = InflightUsage(messages=10, bytes=100)

    assert InflightUsage(messages=3, bytes=40).remaining(limit) == InflightUsage(messages=7, bytes=60)
    assert InflightUsage(messages=12, bytes=999).remaining(limit) == InflightUsage(messages=0, bytes=0)
    assert InflightUsage(messages=9, bytes=10).exhausted(limit) is False
    assert InflightUsage(messages=10, bytes=10).exhausted(limit) is True
    assert InflightUsage(messages=1, bytes=100).exhausted(limit) is True


def test_backpressure_tuning_defaults_and_switch() -> None:
    """缺省阈值与开关口径（全 0 即不限）。"""
    tuning = default_backpressure_tuning()

    assert tuning == BackpressureTuning(inflight_messages=INFLIGHT_MAX_MESSAGES, inflight_bytes=INFLIGHT_MAX_BYTES)
    assert tuning.limit == InflightUsage(messages=INFLIGHT_MAX_MESSAGES, bytes=INFLIGHT_MAX_BYTES)
    assert tuning.enabled is True
    assert BackpressureTuning(inflight_messages=0, inflight_bytes=0).enabled is False


def test_rate_limit_tuning_defaults_are_disabled() -> None:
    """缺省关闭（两级都为 0），避免改变既有行为。"""
    tuning = default_rate_limit_tuning()

    assert tuning.enabled is False
    assert RateLimitTuning(tenant_per_second=1, topic_per_second=0, burst_seconds=1).enabled is True
    assert RateLimitTuning(tenant_per_second=0, topic_per_second=1, burst_seconds=1).enabled is True


def test_token_bucket_refills_over_time() -> None:
    """起始满桶；扣完按速率补；`can` 给出还需等待的毫秒。"""
    bucket = TokenBucket(rate_per_second=10, burst_seconds=1)
    assert bucket.capacity == 10
    assert bucket.is_full(now=0.0) is True

    assert bucket.can(10, now=0.0) == 0
    bucket.consume(10, now=0.0)
    assert bucket.is_full(now=0.0) is False
    assert bucket.can(1, now=0.0) == 100
    assert bucket.can(5, now=0.5) == 0
    assert bucket.can(6, now=0.5) == 100
    assert bucket.can(10, now=1.0) == 0


def test_token_bucket_capacity_at_least_one_second() -> None:
    """`burst_seconds` 至少按 1 算（0 会造出永远放行的空桶）。"""
    bucket = TokenBucket(rate_per_second=3, burst_seconds=0)

    assert bucket.capacity == 3


def test_rate_limiter_disabled_allows_everything() -> None:
    """默认关闭：直接放行，不建桶。"""
    limiter = RateLimiter(default_rate_limit_tuning())

    verdict = limiter.check(tenant=TENANT, topic=TOPIC)

    assert verdict == RateVerdict(allowed=True, retry_after_ms=0, scope="")
    assert limiter.buckets() == 0
    assert limiter.tuning.enabled is False


def test_rate_limiter_rejects_at_tenant_scope() -> None:
    """租户桶打满 → `scope=tenant`，建议重试毫秒来自桶。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=1, topic_per_second=0, burst_seconds=1))

    assert limiter.check(tenant=TENANT, topic=TOPIC, now=0.0).allowed is True
    rejected = limiter.check(tenant=TENANT, topic=TOPIC, now=0.0)

    assert rejected.allowed is False
    assert rejected.scope == "tenant"
    assert rejected.retry_after_ms > 0
    assert limiter.buckets() == 1
    assert limiter.check(tenant=TENANT, topic=TOPIC, now=2.0).allowed is True


def test_rate_limiter_rejects_at_topic_scope() -> None:
    """主题桶打满 → `scope=topic`（租户额度仍有余）。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=100, topic_per_second=1, burst_seconds=1))

    assert limiter.check(tenant=TENANT, topic=TOPIC, now=0.0).allowed is True
    rejected = limiter.check(tenant=TENANT, topic=TOPIC, now=0.0)

    assert rejected.allowed is False
    assert rejected.scope == "topic"
    # 同租户的另一个主题不受影响（主题维度隔离）
    assert limiter.check(tenant=TENANT, topic=("demo", "msg", "other"), now=0.0).allowed is True
    assert limiter.buckets() == 3


def test_rate_limiter_prunes_buckets_at_limit() -> None:
    """桶数到上限时先清满桶，再按插入序淘汰（不无限增长）。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=5, topic_per_second=0, burst_seconds=1), bucket_limit=2)

    limiter.check(tenant="a", topic=TOPIC, now=0.0)
    limiter.check(tenant="b", topic=TOPIC, now=0.0)
    assert limiter.buckets() == 2

    # 两个桶都已扣过 1 个令牌（不满）→ 只能按插入序淘汰最旧的 a
    limiter.check(tenant="c", topic=TOPIC, now=0.0)
    assert limiter.buckets() == 2
    # 新桶是满桶（新租户不受旧账影响）
    assert limiter.check(tenant="c", topic=TOPIC, now=0.0).allowed is True

    # 都补满后再建桶：满桶先被清掉，桶数回落到 1
    limiter.check(tenant="d", topic=TOPIC, now=100.0)
    assert limiter.buckets() == 1


def test_retry_after_falls_back_to_constant() -> None:
    """桶未给出等待时回落常量，保证响应恒有值。"""
    assert retry_after_ms(RateVerdict(allowed=False, retry_after_ms=0, scope="tenant")) == RATE_LIMIT_RETRY_AFTER_MS
    assert retry_after_ms(RateVerdict(allowed=False, retry_after_ms=42, scope="topic")) == 42


def test_rate_limiter_topic_only_skips_tenant_bucket() -> None:
    """只开主题级：租户桶不建、不扣（两级独立开关）。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=1, burst_seconds=1))

    assert limiter.check(tenant=TENANT, topic=TOPIC, now=0.0).allowed is True
    rejected = limiter.check(tenant=TENANT, topic=TOPIC, now=0.0)

    assert rejected.scope == "topic"
    assert limiter.buckets() == 1


def test_rate_limiter_prunes_topic_buckets_at_limit() -> None:
    """主题桶同样受上限约束：满桶先清，剩余按插入序淘汰。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=5, burst_seconds=1), bucket_limit=2)
    limiter.check(tenant=TENANT, topic=("demo", "msg", "a"), now=0.0)
    limiter.check(tenant=TENANT, topic=("demo", "msg", "b"), now=0.0)
    limiter.check(tenant=TENANT, topic=("demo", "msg", "c"), now=0.0)

    assert limiter.buckets() == 2

    # 都补满后再建桶：满桶被清空，桶数回落到 1
    limiter.check(tenant=TENANT, topic=("demo", "msg", "d"), now=100.0)
    assert limiter.buckets() == 1
