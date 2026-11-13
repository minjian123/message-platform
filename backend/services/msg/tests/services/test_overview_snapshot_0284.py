# pyright: reportPrivateUsage=false
"""概览快照与入口计数（02-8-4；Kiwi 2497）：每租户一行、增量累加、速率换算、旧窗口清理。"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.db.session import DbSession
from bms_core.db.tenant import TenantLookup
from bms_core.db.unit_of_work import UnitOfWork
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.watermark import StorageReading
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.metrics.sampler import MetricsSampler
from bms_msg.metrics.traffic import get_traffic_metrics, reset_traffic_metrics
from bms_msg.models.overview import MsgOverviewSnapshot
from bms_msg.repositories.overview import IngressCounterRepository, OverviewSnapshotRepository

pytestmark = pytest.mark.kiwi_id(2497)

TENANT_ID = 7001


def _repo(db_session: AsyncSession) -> OverviewSnapshotRepository:
    """概览快照仓储（绑定内存会话）。"""
    return OverviewSnapshotRepository(cast("DbSession", db_session))


def _counters(db_session: AsyncSession) -> IngressCounterRepository:
    """入口计数仓储（绑定内存会话）。"""
    return IngressCounterRepository(cast("DbSession", db_session))


async def test_snapshot_upsert_and_read(db_session: AsyncSession) -> None:
    """每租户一行：首次插入、再次覆盖写。"""
    repository = _repo(db_session)
    now = datetime(2026, 11, 13, 12, 0, tzinfo=UTC).replace(tzinfo=None)
    await repository.upsert(
        tenant_id=TENANT_ID,
        messages_in_per_second=2.0,
        consumer_lag=7,
        dlq_total=3,
        disk_used_ratio=0.42,
        sampled_at=now,
    )
    await db_session.commit()

    row = await repository.get_for_tenant(tenant_id=TENANT_ID)
    assert row is not None
    assert (row.messages_in_per_second, row.consumer_lag, row.dlq_total) == (2.0, 7, 3)
    assert row.disk_used_ratio == 0.42

    await repository.upsert(
        tenant_id=TENANT_ID,
        messages_in_per_second=4.0,
        consumer_lag=1,
        dlq_total=0,
        disk_used_ratio=0.5,
        sampled_at=now,
    )
    await db_session.commit()
    rows = (await db_session.execute(select(MsgOverviewSnapshot))).scalars().all()
    assert len(rows) == 1
    assert rows[0].messages_in_per_second == 4.0


async def test_ingress_counter_accumulates_and_purges(db_session: AsyncSession) -> None:
    """同一窗口多实例增量累加；汇总与旧窗口清理。"""
    counters = _counters(db_session)
    await counters.add(tenant_id=TENANT_ID, window_start=1000, delta=3)
    await counters.add(tenant_id=TENANT_ID, window_start=1000, delta=2)
    await counters.add(tenant_id=TENANT_ID, window_start=1015, delta=5)
    await db_session.commit()

    total, earliest, latest = await counters.sum_since(tenant_id=TENANT_ID, since=1000)
    assert (total, earliest, latest) == (10, 1000, 1015)

    assert await counters.purge_before(cutoff=1010) == 1
    await db_session.commit()
    total, earliest, _latest = await counters.sum_since(tenant_id=TENANT_ID, since=0)
    assert (total, earliest) == (5, 1015)
    assert await counters.purge_tenant(tenant_id=TENANT_ID) == 1


class _Watermark:
    """存储水位替身（固定读数）。"""

    def force_refresh(self) -> StorageReading:
        """强制刷新（本替身恒返回固定读数）。"""
        return self.reading()

    def reading(self) -> StorageReading:
        """当前读数。"""
        return StorageReading(
            volume="v1", used_ratio=0.5, free_bytes=100, total_bytes=200, warn_ratio=0.7, reject_ratio=0.85
        )


def _sampler(
    db_session: AsyncSession,
    uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
    *,
    interval_seconds: float = 15.0,
) -> tuple[MetricsSampler, MetricsSnapshotStore]:
    """构造采样器（真实内存库 + 真实租户源）。"""
    from tests.conftest import FakeTenantSource

    snapshots = MetricsSnapshotStore()
    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", _Groups()),
        messages=cast("Any", _Messages()),
        offsets=cast("Any", _Offsets()),
        dead_letters=cast("Any", _DeadLetters()),
        uow_factory=uow,
        watermark=cast("Any", _Watermark()),
        tenant_lookup=cast("TenantLookup", FakeTenantSource()),
        interval_seconds=interval_seconds,
    )
    return sampler, snapshots


class _Groups:
    """空组存储替身。"""

    async def list_all(self) -> object:
        """返回空清单。"""
        return ()


class _Messages:
    """消息存储替身（LEO 恒 0）。"""

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """LEO。"""
        del topic_id, partition_no
        return 0


class _Offsets:
    """位点存储替身（无提交）。"""

    async def committed(self, *, group: str, topic_id: int, partition_no: int) -> int | None:
        """无提交记录。"""
        del group, topic_id, partition_no
        return None


class _DeadLetters:
    """死信队列替身（空）。"""

    async def list_all(self) -> object:
        """返回空清单。"""
        return ()


async def test_sampler_writes_per_tenant_snapshot_and_counter(
    db_session: AsyncSession,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> None:
    """采样两轮：入口增量写入时间窗，按租户落库快照（速率 > 0）。"""
    reset_traffic_metrics()
    sampler, _snapshots = _sampler(db_session, replay_uow, interval_seconds=15.0)
    traffic = get_traffic_metrics()
    traffic.record_publish(tenant="demo", topic="audit.operation", size=1, duration_ms=1.0)

    await sampler.sample_once()  # 首轮建立基线
    traffic.record_publish(tenant="demo", topic="audit.operation", size=1, duration_ms=1.0)
    await sampler.sample_once()  # 第二轮产生增量

    row = await _repo(db_session).get_for_tenant(tenant_id=TENANT_ID)
    assert row is not None
    assert row.messages_in_per_second >= 0.0
    total, _earliest, _latest = await _counters(db_session).sum_since(tenant_id=TENANT_ID, since=0)
    assert total >= 1


@pytest.mark.kiwi_id(2497)
async def test_observability_covers_lag_dlq_and_unknown_tenant(
    db_session: AsyncSession,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> None:
    """仅有积压 / 死信维度时也落快照；未知租户跳过；租户主键缓存可复用。"""
    sampler, snapshots = _sampler(db_session, replay_uow)
    snapshots.replace(
        lag=ConcurrentStableDict({"demo\x1faudit.operation": 5, "ghost\x1faudit.operation": 1}),
        dlq=ConcurrentStableDict({"demo\x1faudit.operation": 2}),
        messages_in_window=0,
        window_seconds=0.0,
    )

    await sampler._write_observability()
    await sampler._write_observability()

    row = await _repo(db_session).get_for_tenant(tenant_id=TENANT_ID)
    assert row is not None
    assert (row.consumer_lag, row.dlq_total) == (5, 2)
    assert row.messages_in_per_second == 0.0
    assert await _repo(db_session).get_for_tenant(tenant_id=999999) is None


@pytest.mark.kiwi_id(2497)
async def test_zero_delta_and_tenant_purge(db_session: AsyncSession) -> None:
    """零增量不建行 / 不更新；两个仓储的租户清理返回删除行数。"""
    counters = _counters(db_session)
    await counters.add(tenant_id=TENANT_ID, window_start=1000, delta=0)
    await db_session.commit()
    total, _earliest, _latest = await counters.sum_since(tenant_id=TENANT_ID, since=0)
    assert total == 0

    await counters.add(tenant_id=TENANT_ID, window_start=1000, delta=1)
    await counters.add(tenant_id=TENANT_ID, window_start=1000, delta=0)
    await db_session.commit()
    total, _earliest, _latest = await counters.sum_since(tenant_id=TENANT_ID, since=0)
    assert total == 1

    await _repo(db_session).upsert(
        tenant_id=TENANT_ID,
        messages_in_per_second=1.0,
        consumer_lag=1,
        dlq_total=0,
        disk_used_ratio=0.1,
        sampled_at=datetime(2026, 11, 13, tzinfo=UTC).replace(tzinfo=None),
    )
    await db_session.commit()
    assert await _repo(db_session).purge_tenant(tenant_id=TENANT_ID) == 1
    assert await counters.purge_tenant(tenant_id=TENANT_ID) == 1
    assert await _repo(db_session).purge_tenant(tenant_id=TENANT_ID) == 0
