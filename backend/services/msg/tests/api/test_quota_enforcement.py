"""05-2 配额执行点（Kiwi 2454）：建主题 / 首次 join / 速率 / 存储 / 未确认。"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.tenant import TenantContext
from bms_core.db.unit_of_work import DbUnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.data.messages import (
    _ensure_quota_rate,
    _ensure_storage_quota,
    _ensure_unacked_quota,
    _payload_bytes,
)
from bms_msg.broker.backpressure import RateLimiter, RateLimitTuning
from bms_msg.broker.coordinator import GroupCoordinator
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import (
    MsgGroupCountQuotaExceededError,
    MsgRateLimitedError,
    MsgStorageWatermarkExceededError,
    MsgTopicQuotaExceededError,
)
from bms_msg.models.quota import MsgQuota
from bms_msg.quota.cache import QuotaCache
from bms_msg.quota.service import QuotaService
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.message import PublishRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2454)

TENANT_ID = 7001
NAMESPACE = "msg"


def _quota(*, key: str, limit: int, namespace: str = "") -> QuotaService:
    cache = QuotaCache()
    cache.put(
        TENANT_ID,
        ConcurrentStableList(
            [
                MsgQuota(
                    tenant_id=TENANT_ID,
                    namespace=namespace,
                    quota_key=key,
                    limit_value=limit,
                    soft_limit=0,
                    unit="count",
                    period="none",
                    origin="configured",
                )
            ]
        ),
    )
    return QuotaService(cache=cache)


class _Metrics:
    def __init__(self) -> None:
        self.rejected: list[str] = []

    def record_rate_rejected(self, *, scope: str, tenant: str, topic: str) -> None:
        del tenant, topic
        self.rejected.append(scope)


class _Usage:
    def __init__(self, *, storage: int | None = None, unacked: int = 0) -> None:
        self._storage = storage
        self._unacked = unacked

    async def storage_bytes(self, tenant_id: int) -> int | None:
        del tenant_id
        return self._storage

    async def unacked_count(self, tenant_id: int) -> int:
        del tenant_id
        return self._unacked


def test_payload_bytes_from_raw_and_base64() -> None:
    """帧原始字节优先；JSON 按 base64 估算。"""
    request = PublishRequest(namespace=NAMESPACE, topic="t", payload="AAAA")
    assert _payload_bytes(request) == 3
    request.set_raw_payload(b"12345")
    assert _payload_bytes(request) == 5


async def test_topic_quota_rejects_second_create(
    db_session: AsyncSession, current_tenant: CurrentTenant, make_request: object
) -> None:
    """建主题：硬限 1 时第二个主题返回 350025。"""
    quota = _quota(key="topic_count", limit=1)
    service = TopicService(
        TopicRepository(cast("DbSession", db_session)),
        PartitionRepository(cast("DbSession", db_session)),
        DbUnitOfWork(cast("DbSession", db_session)),
        quota=quota,
    )
    make = cast("Any", make_request)
    await service.create_topic(tenant=current_tenant, request=make(name="a", partition_count=1))
    with pytest.raises(MsgTopicQuotaExceededError):
        await service.create_topic(tenant=current_tenant, request=make(name="b", partition_count=1))


async def test_group_quota_rejects_second_group(
    db_session: AsyncSession,
    current_tenant: CurrentTenant,
    topic: object,
    make_member: object,
) -> None:
    """首次 join：组数硬限 1 时第二个组返回 350026；已有组重连不受影响。"""
    quota = _quota(key="group_count", limit=1)
    coordinator = GroupCoordinator(
        groups=InMemoryGroupStore(),
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        delivery=DeliveryState(),
        quota=quota,
    )
    make = cast("Any", make_member)
    await coordinator.join(tenant=current_tenant, request=make(group="g1"))
    await coordinator.join(tenant=current_tenant, request=make(group="g1"))
    with pytest.raises(MsgGroupCountQuotaExceededError):
        await coordinator.join(tenant=current_tenant, request=make(group="g2"))


async def test_rate_quota_rejects_and_records_metric() -> None:
    """配额速率硬限 1：第二次发布返回 350005 并记指标。"""
    quota = _quota(key="produce_rate", limit=1, namespace=NAMESPACE)
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1))
    fake = _Metrics()
    metrics = cast("BackpressureMetrics", fake)
    tenant = CurrentTenant.from_context(
        TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=TENANT_ID)
    )
    await _ensure_quota_rate(
        quota=quota, limiter=limiter, metrics=metrics, tenant=tenant, namespace=NAMESPACE, topic="t"
    )
    with pytest.raises(MsgRateLimitedError):
        await _ensure_quota_rate(
            quota=quota, limiter=limiter, metrics=metrics, tenant=tenant, namespace=NAMESPACE, topic="t"
        )
    assert fake.rejected == ["tenant"]


async def test_rate_static_gate_rejects() -> None:
    """静态两级桶仍先生效（既有行为不回归）。"""
    quota = _quota(key="produce_rate", limit=0)
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=1, topic_per_second=0, burst_seconds=1))
    metrics = cast("BackpressureMetrics", _Metrics())
    tenant = CurrentTenant.from_context(
        TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=TENANT_ID)
    )
    await _ensure_quota_rate(
        quota=quota, limiter=limiter, metrics=metrics, tenant=tenant, namespace=NAMESPACE, topic="t"
    )
    with pytest.raises(MsgRateLimitedError):
        await _ensure_quota_rate(
            quota=quota, limiter=limiter, metrics=metrics, tenant=tenant, namespace=NAMESPACE, topic="t"
        )


async def test_storage_quota_branches() -> None:
    """存储配额：未超 / 超限 / 不可用 / 不限。"""
    usage = cast("object", _Usage(storage=8))
    with pytest.raises(MsgStorageWatermarkExceededError):
        await _ensure_storage_quota(
            quota=_quota(key="storage_bytes", limit=10),
            usage=usage,  # type: ignore[arg-type]
            tenant_id=TENANT_ID,
            namespace=NAMESPACE,
            incoming=5,
        )
    await _ensure_storage_quota(
        quota=_quota(key="storage_bytes", limit=10),
        usage=usage,  # type: ignore[arg-type]
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        incoming=1,
    )
    await _ensure_storage_quota(
        quota=_quota(key="storage_bytes", limit=10),
        usage=cast("object", _Usage(storage=None)),  # type: ignore[arg-type]
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        incoming=99,
    )
    await _ensure_storage_quota(
        quota=_quota(key="storage_bytes", limit=0),
        usage=usage,  # type: ignore[arg-type]
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        incoming=99,
    )


async def test_unacked_quota_branches() -> None:
    """未确认配额：到顶暂停 / 未到顶 / 不限。"""
    assert (
        await _ensure_unacked_quota(
            quota=_quota(key="unacked_count", limit=2),
            usage=cast("object", _Usage(unacked=2)),  # type: ignore[arg-type]
            tenant_id=TENANT_ID,
        )
        is True
    )
    assert (
        await _ensure_unacked_quota(
            quota=_quota(key="unacked_count", limit=2),
            usage=cast("object", _Usage(unacked=1)),  # type: ignore[arg-type]
            tenant_id=TENANT_ID,
        )
        is False
    )
    assert (
        await _ensure_unacked_quota(
            quota=_quota(key="unacked_count", limit=0),
            usage=cast("object", _Usage(unacked=9)),  # type: ignore[arg-type]
            tenant_id=TENANT_ID,
        )
        is False
    )


def test_rate_limiter_quota_bucket_rebuilds_on_rate_change() -> None:
    """配额速率变化即重建桶；0 表示不限。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1))
    assert limiter.check_quota(tenant="demo", namespace=NAMESPACE, rate_per_second=0).allowed is True
    assert limiter.check_quota(tenant="demo", namespace=NAMESPACE, rate_per_second=1).allowed is True
    assert limiter.check_quota(tenant="demo", namespace=NAMESPACE, rate_per_second=1).allowed is False
    # 速率调整后桶重建（容量按新速率给足），下一次放行
    assert limiter.check_quota(tenant="demo", namespace=NAMESPACE, rate_per_second=2).allowed is True
    assert limiter.buckets() >= 1


def test_rate_limiter_prunes_quota_buckets() -> None:
    """桶数到上限时优先清理已满的配额桶（防高基数打爆内存）。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1), bucket_limit=1)
    limiter._quota_bucket(("demo", "a"), 10, 0.0)
    limiter._quota_bucket(("demo", "b"), 10, 0.0)
    assert limiter.buckets() <= 1


def test_rate_limiter_evicts_oldest_quota_bucket() -> None:
    """非满配额桶到上限时按插入序淘汰最旧（`while` 分支）。"""
    limiter = RateLimiter(RateLimitTuning(tenant_per_second=0, topic_per_second=0, burst_seconds=1), bucket_limit=1)
    limiter.check_quota(tenant="demo", namespace="a", rate_per_second=10)
    limiter._quota_bucket(("demo", "b"), 10, 0.0)
    assert limiter.buckets() <= 1


async def test_topic_zero_limit_skips_quota(
    db_session: AsyncSession, current_tenant: CurrentTenant, make_request: object
) -> None:
    """主题数硬限 0（不限）：不查询用量、直接放行。"""
    service = TopicService(
        TopicRepository(cast("DbSession", db_session)),
        PartitionRepository(cast("DbSession", db_session)),
        DbUnitOfWork(cast("DbSession", db_session)),
        quota=_quota(key="topic_count", limit=0),
    )
    topic = await service.create_topic(
        tenant=current_tenant, request=cast("Any", make_request)(name="zero-quota", partition_count=1)
    )
    assert topic.name == "zero-quota"


async def test_topic_without_quota_service_creates(
    db_session: AsyncSession, current_tenant: CurrentTenant, make_request: object
) -> None:
    """未装配配额服务：建主题不判配额（既有单测兼容）。"""
    service = TopicService(
        TopicRepository(cast("DbSession", db_session)),
        PartitionRepository(cast("DbSession", db_session)),
        DbUnitOfWork(cast("DbSession", db_session)),
    )
    make = cast("Any", make_request)
    topic = await service.create_topic(tenant=current_tenant, request=make(name="no-quota", partition_count=1))
    assert topic.name == "no-quota"


async def test_group_without_quota_or_zero_limit_joins(
    db_session: AsyncSession, current_tenant: CurrentTenant, topic: object, make_member: object
) -> None:
    """未装配配额服务 / 硬限 0：首次 join 不判组数配额。"""
    make = cast("Any", make_member)
    first = GroupCoordinator(
        groups=InMemoryGroupStore(),
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        delivery=DeliveryState(),
    )
    await first.join(tenant=current_tenant, request=make(group="g1"))
    zero = GroupCoordinator(
        groups=InMemoryGroupStore(),
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        delivery=DeliveryState(),
        quota=_quota(key="group_count", limit=0),
    )
    await zero.join(tenant=current_tenant, request=make(group="g2"))
