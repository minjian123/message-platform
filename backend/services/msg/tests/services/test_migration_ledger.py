"""迁移台账（02-8）：唯一性 / CAS / 进度 / 重启可读（内存 + 平台库两实现）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.services.migration_ledger import (
    DbMigrationLedger,
    InMemoryMigrationLedger,
    MigrationLedger,
    MigrationPlanItem,
    new_plan_id,
)

pytestmark = pytest.mark.kiwi_id(2486)

PLAN = "rb-20261112T000000Z-abcdef"


def _item(*, plan_id: str = PLAN, partition_no: int = 0) -> MigrationPlanItem:
    """构造计划项。

    pytestmark = pytest.mark.kiwi_id(2486)

        Args:
            plan_id: 计划标识。
            partition_no: 分区号。

        Returns:
            MigrationPlanItem: 计划项。
    """
    return MigrationPlanItem(
        plan_id=plan_id,
        tenant_id=7001,
        namespace="msg",
        topic="audit.operation",
        topic_id=9,
        partition_no=partition_no,
        action="expand",
        from_replicas=ConcurrentStableFrozenList(("msg-1",)),
        to_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        from_leader="msg-1",
        to_leader="msg-1",
    )


def test_new_plan_id_is_bounded_and_prefixed() -> None:
    """计划标识：前缀固定、长度在列宽内。"""
    value = new_plan_id()

    assert value.startswith("rb-")
    assert len(value) <= 64


@pytest.fixture(params=["memory", "db"])
def ledger_name(request: pytest.FixtureRequest) -> str:
    """两种实现各跑一遍。

    Args:
        request: pytest 参数。

    Returns:
        str: 实现名。
    """
    return str(request.param)


@pytest.fixture
async def ledger(ledger_name: str, db_session: AsyncSession) -> AsyncGenerator[MigrationLedger]:
    """台账实例（内存 / 平台库）。

    Args:
        ledger_name: 实现名。
        db_session: 会话。

    Yields:
        MigrationLedger: 台账。
    """
    if ledger_name == "memory":
        yield InMemoryMigrationLedger()
        return
    session = db_session

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(session)

    yield DbMigrationLedger(uow_factory=_uow)


async def test_create_is_idempotent(ledger: MigrationLedger) -> None:
    """计划项：同计划同分区重复创建只留一行。"""
    items = ConcurrentStableList([_item(), _item(partition_no=1)])

    assert await ledger.create(items) == 2
    assert await ledger.create(items) == 0
    rows = await ledger.load(PLAN)
    assert [row.partition_no for row in rows] == [0, 1]


async def test_state_machine_and_progress(ledger: MigrationLedger) -> None:
    """状态机：planned → running → catching_up → succeeded，CAS 拒绝越级。"""
    await ledger.create(ConcurrentStableList([_item()]))
    row = (await ledger.load(PLAN))[0]
    assert row.status == "planned"
    assert await ledger.mark_catching_up(item_id=row.id, result="noop", isr_verified=True, retained_tail=0) is False

    from datetime import UTC, datetime

    assert await ledger.mark_running(item_id=row.id, operator="tester", reason="drill", started_at=datetime.now(UTC))
    assert await ledger.mark_catching_up(item_id=row.id, result="noop", isr_verified=True, retained_tail=0)
    assert await ledger.update_step(item_id=row.id, step="expand")
    assert await ledger.update_progress(item_id=row.id, leo=10, hw=8, remaining=0)
    assert await ledger.mark_terminal(
        item_id=row.id,
        status="succeeded",
        result="noop",
        isr_verified=True,
        retained_tail=0,
        error="",
        finished_at=datetime.now(UTC),
    )
    final = (await ledger.load(PLAN))[0]
    assert final.status == "succeeded"
    assert final.progress_leo == 10
    assert final.version > 1
    assert await ledger.mark_running(item_id=row.id, operator="t", reason="r", started_at=datetime.now(UTC)) is False


async def test_active_and_counts(ledger: MigrationLedger) -> None:
    """未完成项与按状态汇总：`has_active` 命中、计数与剩余条数正确。"""
    from datetime import UTC, datetime

    await ledger.create(ConcurrentStableList([_item(), _item(partition_no=1)]))
    rows = await ledger.load(PLAN)
    assert await ledger.has_active(topic_id=9, partition_no=0) is True
    assert await ledger.has_active(topic_id=9, partition_no=0, exclude_plan_id=PLAN) is False

    await ledger.mark_running(item_id=rows[0].id, operator="t", reason="r", started_at=datetime.now(UTC))
    await ledger.mark_catching_up(item_id=rows[0].id, result="noop", isr_verified=True, retained_tail=0)
    await ledger.update_progress(item_id=rows[0].id, leo=1, hw=5, remaining=4)

    counts = await ledger.count_by_status()
    assert counts.get("catching_up") == 1
    assert counts.get("planned") == 1
    assert await ledger.sum_remaining() == 4


async def test_latest_plan_and_delete(ledger: MigrationLedger) -> None:
    """最近计划与清理：`latest_plan_id` 可取、删除后归零。"""
    await ledger.create(ConcurrentStableList([_item(plan_id="rb-a")]))
    await ledger.create(ConcurrentStableList([_item(plan_id="rb-b")]))

    latest = await ledger.latest_plan_id()
    assert latest in ("rb-a", "rb-b")
    assert await ledger.delete_plan("rb-b") == 1
    assert await ledger.load("rb-b") == []
    assert await ledger.load_one(plan_id="rb-b", topic_id=9, partition_no=0) is None


async def test_ledger_empty_create_and_inactive_skip() -> None:
    """台账：空计划不落库；`has_active` 跳过终态行。"""
    from datetime import UTC, datetime

    ledger = InMemoryMigrationLedger()
    assert await ledger.create(ConcurrentStableList()) == 0

    await ledger.create(ConcurrentStableList([_item()]))
    row = (await ledger.load(PLAN))[0]
    await ledger.mark_terminal(
        item_id=row.id,
        status="succeeded",
        result="noop",
        isr_verified=True,
        retained_tail=0,
        error="",
        finished_at=datetime.now(UTC),
    )
    assert await ledger.has_active(topic_id=9, partition_no=0) is False


async def test_db_ledger_empty_create_returns_zero(db_session: AsyncSession) -> None:
    """平台库台账：空计划直接返回 0（不签发会话写入）。"""
    from bms_core.core.concurrent import ConcurrentStableList

    session = db_session

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(session)

    ledger = DbMigrationLedger(uow_factory=_uow)
    assert await ledger.create(ConcurrentStableList()) == 0
