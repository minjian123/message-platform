# pyright: reportPrivateUsage=false
"""05-5 到期清理（Kiwi 2453）：dry-run、逐主题保留期、孤儿目录、活跃重放延后与失败重试。"""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.events.base import EventEnvelope
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from bms_msg.models.partition import MsgPartition
from bms_msg.models.replay import TASK_CANCELLED, TASK_RUNNING, MsgReplayTask
from bms_msg.models.topic import MsgTopic
from bms_msg.tenant.ledger import DbTenantLifecycleLedger
from bms_msg.tenant.purge import (
    TenantPurgeMetrics,
    TenantPurgeResult,
    TenantPurgeService,
    _safe_rmtree,
    _utc_naive,
    _utc_now_naive,
    get_tenant_purge_metrics,
    reset_tenant_purge_metrics,
)
from bms_msg.tenant.runtime import TenantLifecycleRuntime

pytestmark = pytest.mark.kiwi_id(2453)

TENANT_ID = 7001
"""测试租户主键。"""

_TOPIC_ID = 9101
"""测试主题主键。"""

_NOW = datetime(2026, 11, 8, 12, 0, 0)
"""测试基准时刻（UTC naive）。"""


def _audit_events(service: TenantPurgeService) -> list[tuple[EventEnvelope, bool]]:
    """取审计替身记录（测试专用）。

    Args:
        service: 清扫服务。

    Returns:
        list[tuple[EventEnvelope, bool]]: 审计事件与是否强审计。
    """
    return cast("list[tuple[EventEnvelope, bool]]", cast("Any", service._runtime.audit_sink).events)


async def _seed_topic(
    engine: AsyncEngine,
    *,
    topic_id: int = _TOPIC_ID,
    tenant_id: int = TENANT_ID,
    retention_ms: int = 1_000,
    name: str = "audit.operation",
) -> None:
    """写入主题与一个分区。

    Args:
        engine: 内存库引擎。
        topic_id: 主题主键。
        tenant_id: 租户主键。
        retention_ms: 保留时长（毫秒）。
        name: 主题名。
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        session.add(
            MsgTopic(
                id=topic_id,
                tenant_id=tenant_id,
                namespace="msg",
                name=name,
                partition_count=1,
                retention_ms=retention_ms,
            )
        )
        session.add(MsgPartition(topic_id=topic_id, partition_no=0, leader="msg-0"))


async def _seed_replay(engine: AsyncEngine, *, status: str = TASK_RUNNING) -> None:
    """写入一条重放任务。

    Args:
        engine: 内存库引擎。
        status: 任务状态。
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        session.add(
            MsgReplayTask(
                tenant_id=TENANT_ID,
                namespace="msg",
                topic_id=_TOPIC_ID,
                topic="audit.operation",
                partitions=ConcurrentStableList([0]),
                status=status,
            )
        )


def _create_data_dir(
    root: Path, *, topic_id: int = _TOPIC_ID, tenant_id: int = TENANT_ID, name: str = "audit.operation"
) -> Path:
    """创建主题数据目录与一个段文件。

    Args:
        root: 数据根（`data` 的父目录）。
        topic_id: 主题主键。
        tenant_id: 租户主键。
        name: 主题名。

    Returns:
        Path: 主题目录。
    """
    topic_dir = root / "data" / str(tenant_id) / "msg" / f"{name}.{topic_id}"
    partition = topic_dir / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000000.log").write_bytes(b"x")
    return topic_dir


def _create_offset(root: Path, *, topic_id: int = _TOPIC_ID, tenant_id: int = TENANT_ID) -> Path:
    """创建一个位点文件（内容不参与删除判定）。

    Args:
        root: 数据根。
        topic_id: 主题主键。
        tenant_id: 租户主键。

    Returns:
        Path: 位点文件。
    """
    group_dir = root / "offsets" / str(tenant_id) / "audit-12345678"
    group_dir.mkdir(parents=True)
    path = group_dir / f"{topic_id}-0.offset"
    path.write_bytes(b"offset")
    return path


async def _deleted(ledger: DbTenantLifecycleLedger, *, purge_after: datetime) -> None:
    """写一条 05-5 存量删除排期（`purge_after` 非空才进周期清扫）。

    Args:
        ledger: 账本。
        purge_after: 到期点。
    """
    deleted_at = _NOW - timedelta(seconds=2) if purge_after <= _NOW else _NOW
    await ledger.mark_deleted(tenant_id=TENANT_ID, version=1, deleted_event_at=deleted_at, purge_after=purge_after)


@pytest.fixture
def service(ledger: DbTenantLifecycleLedger, runtime: TenantLifecycleRuntime, root: Path) -> TenantPurgeService:
    """清扫服务（固定时钟）。

    Args:
        ledger: 账本。
        runtime: 运行时。
        root: 数据根。

    Returns:
        TenantPurgeService: 清扫服务。
    """
    return TenantPurgeService(
        ledger=ledger,
        runtime=runtime,
        data_root=root / "data",
        offsets_root=root / "offsets",
        now=lambda: _NOW,
    )


async def test_plan_is_dry_run(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """dry-run 只读：不删文件、不改 DB、不取消任务。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    offset = _create_offset(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))

    plan = await service.plan(TENANT_ID, now=_NOW)
    assert plan.due_topic_ids == (_TOPIC_ID,)
    assert plan.next_purge_after is None
    assert topic_dir.exists()
    assert offset.exists()
    assert await ledger.count_topics(tenant_id=TENANT_ID) == 1


async def test_execute_deletes_due_topic(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """到期执行：删数据目录 / 位点 / DB 行，标记 purged_at 并写强审计。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    offset = _create_offset(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))

    result = await service.execute(await service.plan(TENANT_ID, now=_NOW))
    assert result.deleted_topics == 1
    assert result.purged is True
    assert not topic_dir.exists()
    assert not offset.exists()
    assert await ledger.count_topics(tenant_id=TENANT_ID) == 0
    state = await ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at == _NOW
    actions = [event.payload.get("action") for event, required in _audit_events(service) if required]
    assert "msg:tenant.purge" in actions
    assert actions[-1] == "msg:tenant.purge"


async def test_not_due_topic_retained(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """未到期主题不删，purge_after 保留到下一到期点。"""
    await _seed_topic(engine, retention_ms=30 * 24 * 3600 * 1000)
    topic_dir = _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW + timedelta(days=30))

    plan = await service.plan(TENANT_ID, now=_NOW)
    assert plan.due_topics == ()
    result = await service.execute(plan)
    assert result.purged is False
    assert topic_dir.exists()
    state = await ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is None


async def test_active_replay_defers_then_completes(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """有活跃重放任务时先取消并延后；下一轮再删。"""
    await _seed_topic(engine)
    await _seed_replay(engine)
    topic_dir = _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))

    first = await service.plan(TENANT_ID, now=_NOW)
    assert first.deferred_reason == "active_replay"
    result = await service.execute(first)
    assert result.deferred is True
    assert topic_dir.exists()
    assert await _replay_status(engine) == TASK_CANCELLED

    second = await service.plan(TENANT_ID, now=_NOW)
    assert second.deferred_reason is None
    result = await service.execute(second)
    assert result.purged is True
    assert not topic_dir.exists()


async def test_orphan_directory_is_cleaned(
    ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """DB 无主题行的孤儿目录按到期清理（目录兜底）。"""
    orphan = _create_data_dir(root, topic_id=9999)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    plan = await service.plan(TENANT_ID, now=_NOW)
    assert any(topic.orphan for topic in plan.due_topics)
    result = await service.execute(plan)
    assert result.purged is True
    assert not orphan.exists()


async def test_retention_boundary(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """保留期边界：前 1ms 不到期，后 1ms 到期。"""
    await _seed_topic(engine, retention_ms=1_000)
    _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW + timedelta(seconds=1))

    before = await service.plan(TENANT_ID, now=_NOW + timedelta(milliseconds=999))
    after = await service.plan(TENANT_ID, now=_NOW + timedelta(milliseconds=1_001))
    assert before.due_topics == ()
    assert len(after.due_topics) == 1


async def test_purge_audit_failure_records_and_retries(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """强审计失败：记录 failure_count，不标记 purged_at。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    service._runtime.audit_sink.fail = True  # type: ignore[attr-defined]

    with pytest.raises(Exception, match=r"msg:tenant\.purge"):
        await service.execute(await service.plan(TENANT_ID, now=_NOW))
    state = await ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None
    assert state.purged_at is None
    assert state.failure_count >= 1
    assert "msg:tenant.purge" in (state.last_error or "")


async def test_run_once_scans_due(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """周期入口按 due 列表扫描并执行。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    assert await service.run_once(now=_NOW) == 1
    assert await ledger.count_topics(tenant_id=TENANT_ID) == 0


def test_safe_rmtree_rejects_outside_root(root: Path) -> None:
    """路径安全：数据根之外拒绝删除。"""
    with pytest.raises(ValueError):
        _safe_rmtree(root.parent / "outside", root=root / "data")


def test_metrics_snapshot_and_reset() -> None:
    """清扫计数快照与清零。"""
    reset_tenant_purge_metrics()
    metrics = TenantPurgeMetrics(scanned=1, purged=2, deferred=3, failed=4)
    assert metrics.snapshot() == {"scanned": 1, "purged": 2, "deferred": 3, "failed": 4}
    metrics.scanned = 5
    global_metrics = get_tenant_purge_metrics()
    global_metrics.failed = 2
    reset_tenant_purge_metrics()
    assert global_metrics.snapshot() == {"scanned": 0, "purged": 0, "deferred": 0, "failed": 0}


async def test_service_lifecycle(service: TenantPurgeService) -> None:
    """清扫服务可启动 / 停止（幂等）。"""
    assert service.running is False
    service.ensure_started()
    assert service.running is True
    await service.stop()
    assert service.running is False
    await service.stop()


async def test_empty_plan_for_missing_or_purged(service: TenantPurgeService, ledger: DbTenantLifecycleLedger) -> None:
    """无删除行 / 已完成清理返回空计划。"""
    assert (await service.plan(9999, now=_NOW)).topics == ()
    await _deleted(ledger, purge_after=_NOW)
    await ledger.mark_purged(tenant_id=TENANT_ID, at=_NOW)
    assert (await service.plan(TENANT_ID, now=_NOW)).topics == ()


async def test_partial_purge_sets_next_deadline(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """只删到期主题，未到期主题保留并推进 purge_after。"""
    await _seed_topic(engine, topic_id=9101, retention_ms=1_000, name="audit.operation")
    await _seed_topic(engine, topic_id=9102, retention_ms=30 * 24 * 3600 * 1000, name="audit.other")
    due_dir = _create_data_dir(root, topic_id=9101, name="audit.operation")
    kept_dir = _create_data_dir(root, topic_id=9102, name="audit.other")
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))

    plan = await service.plan(TENANT_ID, now=_NOW)
    assert plan.due_topic_ids == (9101,)
    assert plan.next_purge_after is not None
    result = await service.execute(plan)
    assert result.purged is False
    assert not due_dir.exists()
    assert kept_dir.exists()
    state = await ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purge_after == plan.next_purge_after


async def test_execute_no_due_no_topics_finalizes(
    ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """无主题且无到期项时直接完成清理（幂等重试路径）。"""
    (root / "data" / str(TENANT_ID)).mkdir(parents=True)
    await _deleted(ledger, purge_after=_NOW)
    result = await service.execute(await service.plan(TENANT_ID, now=_NOW))
    assert result.purged is True
    assert not (root / "data" / str(TENANT_ID)).exists()


async def test_orphan_scan_skips_non_topic_dirs(
    ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """非 `{name}.{topic_id}` 目录不按孤儿主题处理。"""
    namespace = root / "data" / str(TENANT_ID) / "msg"
    namespace.mkdir(parents=True)
    (namespace / "not.a.topic").mkdir()
    await _deleted(ledger, purge_after=_NOW)
    plan = await service.plan(TENANT_ID, now=_NOW)
    assert plan.due_topics == ()


async def test_run_once_failure_metrics(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """单租户执行失败：run_once 记失败计数并继续。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    service._runtime.audit_sink.fail = True  # type: ignore[attr-defined]
    assert await service.run_once(now=_NOW) == 1
    assert service._metrics.snapshot()["failed"] == 1


async def test_run_once_deferred_metrics(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, root: Path, service: TenantPurgeService
) -> None:
    """活跃重放延后：run_once 记 deferred 计数。"""
    await _seed_topic(engine)
    await _seed_replay(engine)
    _create_data_dir(root)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    assert await service.run_once(now=_NOW) == 1
    assert service._metrics.snapshot()["deferred"] == 1


def test_utc_helpers() -> None:
    """时间工具：None 原样、naive 原样、aware 转 UTC naive。"""
    assert _utc_naive(None) is None
    naive = datetime(2026, 1, 1)
    assert _utc_naive(naive) == naive
    from datetime import UTC

    assert _utc_naive(datetime(2026, 1, 1, tzinfo=UTC)) == naive
    assert _utc_now_naive().tzinfo is None


def test_safe_rmtree_deletes_existing_and_skips_missing(root: Path) -> None:
    """安全删除：存在删除、缺失返回 False。"""
    target = root / "data" / "7001"
    target.mkdir(parents=True)
    assert _safe_rmtree(target, root=root / "data") is True
    assert _safe_rmtree(target, root=root / "data") is False


async def test_ensure_started_is_idempotent(service: TenantPurgeService) -> None:
    """重复 ensure_started 不建第二个任务。"""
    service.ensure_started()
    service.ensure_started()
    await service.stop()


async def test_run_once_returns_zero_when_locked(service: TenantPurgeService) -> None:
    """同实例已有清扫在跑时，本轮直接返回 0。"""
    async with service._lock:
        assert await service.run_once(now=_NOW) == 0


async def test_run_once_errors_branch(
    ledger: DbTenantLifecycleLedger, service: TenantPurgeService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """执行结果带 errors 时计入失败。"""
    await _deleted(ledger, purge_after=_NOW)

    async def _fake_execute(plan: object) -> TenantPurgeResult:
        del plan
        return TenantPurgeResult(tenant_id=TENANT_ID, errors=ConcurrentStableFrozenList(["boom"]))

    monkeypatch.setattr(service, "execute", _fake_execute)
    assert await service.run_once(now=_NOW) == 1
    assert service._metrics.snapshot()["failed"] == 1


async def test_record_failure_inner_exception(service: TenantPurgeService, monkeypatch: pytest.MonkeyPatch) -> None:
    """账本记录失败不掩盖原始异常。"""

    async def _boom(**kwargs: object) -> None:
        del kwargs
        raise RuntimeError("ledger down")

    monkeypatch.setattr(service._ledger, "record_purge_failure", _boom)
    await service._record_failure(TENANT_ID, RuntimeError("original"))


async def test_loop_body(service: TenantPurgeService) -> None:
    """清扫循环可跑一轮并随取消退出。"""
    service._interval_ms = 1
    task = asyncio.create_task(service._loop())
    await asyncio.sleep(0.01)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_loop_tick_exception_does_not_stop(service: TenantPurgeService, monkeypatch: pytest.MonkeyPatch) -> None:
    """单轮异常只记日志，循环继续（随后取消退出）。"""

    async def _boom() -> int:
        raise RuntimeError("tick")

    monkeypatch.setattr(service, "run_once", _boom)
    service._interval_ms = 1
    task = asyncio.create_task(service._loop())
    await asyncio.sleep(0.01)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_orphan_scan_missing_root(ledger: DbTenantLifecycleLedger, service: TenantPurgeService) -> None:
    """租户数据根不存在时孤儿扫描返回空。"""
    await _deleted(ledger, purge_after=_NOW)
    plan = await service.plan(TENANT_ID, now=_NOW)
    assert plan.due_topics == ()


async def test_execute_missing_data_dir(
    engine: AsyncEngine, ledger: DbTenantLifecycleLedger, service: TenantPurgeService
) -> None:
    """主题行存在但目录缺失：按成功处理，不报错。"""
    await _seed_topic(engine)
    await _deleted(ledger, purge_after=_NOW - timedelta(seconds=1))
    result = await service.execute(await service.plan(TENANT_ID, now=_NOW))
    assert result.deleted_data_dirs == 0
    assert result.purged is True


async def _replay_status(engine: AsyncEngine) -> str:
    """读测试重放任务状态。

    Args:
        engine: 内存库引擎。

    Returns:
        str: 任务状态。
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        row = (await session.execute(select(MsgReplayTask).where(MsgReplayTask.tenant_id == TENANT_ID))).scalar_one()
        return str(row.status)


async def _replay_count(engine: AsyncEngine) -> int:
    """统计测试重放任务行数。

    Args:
        engine: 内存库引擎。

    Returns:
        int: 行数。
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        rows = (await session.execute(select(MsgReplayTask).where(MsgReplayTask.tenant_id == TENANT_ID))).all()
        return len(rows)


async def test_purge_tenant_ignores_retention(engine: AsyncEngine, service: TenantPurgeService, root: Path) -> None:
    """`purge_tenant` 强制全量：保留期再长也当轮删除（平台已授权）。"""
    await _seed_topic(engine, retention_ms=30 * 24 * 3600 * 1000)
    topic_dir = _create_data_dir(root)
    offset = _create_offset(root)
    await service._ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)

    outcome = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    assert outcome.purged is True
    assert outcome.skipped is False
    assert outcome.deleted_topics == 1
    assert not topic_dir.exists()
    assert not offset.exists()
    assert await service._ledger.count_topics(tenant_id=TENANT_ID) == 0
    state = await service._ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at == _NOW and state.purge_after is None
    audit = [event for event, required in _audit_events(service) if required]
    actions = [event.payload.get("action") for event in audit]
    assert "msg:tenant.purge" in actions
    tenant_purge = [event for event in audit if event.payload.get("action") == "msg:tenant.purge"]
    assert json.loads(str(tenant_purge[0].payload.get("params"))).get("trigger") == "tenant.purged"


async def test_purge_tenant_is_idempotent(engine: AsyncEngine, service: TenantPurgeService, root: Path) -> None:
    """重复 purged：第二次按 `purged_at` 跳过，不再删、不再写审计。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    await service._ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)
    first = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)
    second = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW + timedelta(minutes=5))

    assert first.purged is True and first.skipped is False
    assert second.skipped is True and second.deleted_topics == 0
    assert len([event for event, _required in _audit_events(service)]) == 1
    state = await service._ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at == _NOW


async def test_purge_tenant_without_deleted_row_creates_ledger(
    engine: AsyncEngine, service: TenantPurgeService, root: Path
) -> None:
    """孤立 `tenant.purged`（无 deleted 行）：照常清理并补建账本行留 purged_at。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    assert await service._ledger.get_deleted(tenant_id=TENANT_ID) is None

    outcome = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    assert outcome.purged is True
    assert not topic_dir.exists()
    assert await service._ledger.count_topics(tenant_id=TENANT_ID) == 0
    state = await service._ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None
    assert state.purged_at == _NOW
    assert state.deleted_event_at is None
    assert state.applied_version == -1


async def test_purge_tenant_cancels_active_replay_and_deletes_same_round(
    engine: AsyncEngine, service: TenantPurgeService, root: Path
) -> None:
    """活跃重放任务：先取消、同轮删除（不 deferred，避免「已 applied 但数据滞留」）。"""
    await _seed_topic(engine)
    await _seed_replay(engine)
    topic_dir = _create_data_dir(root)
    await service._ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)

    outcome = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    assert outcome.purged is True
    assert not topic_dir.exists()
    # 非终态任务先取消、随后随主题行一并删除（同轮清理，不留 deferred）。
    assert await _replay_count(engine) == 0
    assert await service._ledger.count_topics(tenant_id=TENANT_ID) == 0


async def test_purge_tenant_cleans_orphan_directory(
    ledger: DbTenantLifecycleLedger, service: TenantPurgeService, root: Path
) -> None:
    """孤儿目录（DB 无主题行）在强制路径同样被清掉。"""
    orphan = _create_data_dir(root, topic_id=9999)
    await ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)

    outcome = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    assert outcome.purged is True
    assert not orphan.exists()


async def test_purge_tenant_audit_failure_keeps_purged_at_empty(
    engine: AsyncEngine, service: TenantPurgeService, root: Path
) -> None:
    """强审计失败：异常上抛、`purged_at` 不写（重投可重入补审计）。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    await service._ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)
    service._runtime.audit_sink.fail = True  # type: ignore[attr-defined]

    with pytest.raises(Exception, match=r"msg:tenant\.purge"):
        await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    state = await service._ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is None


async def test_purge_tenant_partial_counts_failure(
    service: TenantPurgeService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """未完全清理（计划外观测到残留）：计失败、`purged=False`，不谎报完成。"""
    await service._ledger.mark_deleted(tenant_id=TENANT_ID, version=3, deleted_event_at=_NOW)

    async def _fake_execute(plan: object) -> TenantPurgeResult:
        del plan
        return TenantPurgeResult(tenant_id=TENANT_ID, deleted_topics=1)

    monkeypatch.setattr(service, "execute", _fake_execute)
    outcome = await service.purge_tenant(tenant_id=TENANT_ID, at=_NOW)

    assert outcome.purged is False
    assert outcome.skipped is False
    assert service._metrics.snapshot()["failed"] == 1
