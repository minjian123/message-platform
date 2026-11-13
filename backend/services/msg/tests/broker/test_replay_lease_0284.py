# pyright: reportPrivateUsage=false
"""重放任务租约（02-8-4；Kiwi 2497）：多实例抢占 / 续约 / 让出 / 重启续跑。"""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.msgclient.payload import DEFAULT_CONTENT_TYPE
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner, ReplayService, UowFactory
from bms_msg.core.trace import TraceContext
from bms_msg.models.replay import TASK_COMPLETED, MsgReplayTask
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.tenant.context import CurrentTenant
from tests_support.clock import ManualClock

pytestmark = pytest.mark.kiwi_id(2497)

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
PAYLOAD = b"payload"
BASE = datetime(2026, 1, 1)


async def _seed(store: InMemoryMessageStore, *, topic: MsgTopic, count: int) -> None:
    """向分区 0 写入历史记录（重放范围来源）。"""
    for index in range(count):
        await store.append(
            topic_id=int(topic.id),
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"l-{index}",
            key="k",
            payload=PAYLOAD,
            headers=ConcurrentStableDict({"source": "test"}),
            trace=TraceContext(trace_id="t-1"),
            published_at=BASE,
            content_type=DEFAULT_CONTENT_TYPE,
        )


def _request(*, rate: int = 100) -> ReplayRequest:
    """建任务请求（单分区、指定限速）。"""
    return ReplayRequest.model_validate(
        {"namespace": NAMESPACE, "topic": TOPIC_NAME, "partitions": [0], "rate_per_second": rate}
    )


def _runner(
    *,
    store: InMemoryMessageStore,
    db_session: AsyncSession,
    auditor: ReplayAuditor,
    now: Callable[[], datetime],
    owner: str,
    lease_seconds: int = 30,
) -> ReplayRunner:
    """组装执行器（工作单元复用内存会话）。"""
    session = cast("DbSession", db_session)

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(session)

    factory: UowFactory = _uow
    return ReplayRunner(
        messages=store,
        uow_factory=factory,
        auditor=auditor,
        now=now,
        interval_ms=10,
        owner=owner,
        lease_seconds=lease_seconds,
    )


def _repository(db_session: AsyncSession) -> ReplayTaskRepository:
    """任务仓储（绑定内存会话）。"""
    return ReplayTaskRepository(cast("DbSession", db_session))


async def test_list_active_respects_lease_and_expiry(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
) -> None:
    """A 持租约时 B 取不到；租约过期后 B 可取到；本实例始终可取。"""
    await _seed(message_store, topic=topic, count=2)
    task = await replay_service.create(tenant=current_tenant, request=_request())
    repository = _repository(db_session)
    task_id = int(task.id)
    now = clock()

    # 02-8-4 回归：未给 owner / now 时不过滤租约（既有调用方兼容）
    assert [int(item.id) for item in await repository.list_active(limit=10)] == [task_id]
    assert [int(item.id) for item in await repository.list_active(limit=10, owner="A", now=now)] == [task_id]
    assert await repository.claim(task_id=task_id, owner="A", expires_at=now, now=now) is True
    assert await repository.list_active(limit=10, owner="B", now=now) == []
    assert [int(item.id) for item in await repository.list_active(limit=10, owner="A", now=now)] == [task_id]

    later = now.replace(year=now.year + 1)
    assert [int(item.id) for item in await repository.list_active(limit=10, owner="B", now=later)] == [task_id]


async def test_claim_is_exclusive_and_renewable(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
) -> None:
    """抢占排他；同一持有者可重复抢占（续约）并推进到期时间。"""
    await _seed(message_store, topic=topic, count=1)
    task = await replay_service.create(tenant=current_tenant, request=_request())
    repository = _repository(db_session)
    task_id = int(task.id)
    now = clock()

    assert await repository.claim(task_id=task_id, owner="A", expires_at=now, now=now) is True
    assert await repository.claim(task_id=task_id, owner="B", expires_at=now, now=now) is False
    renewed = now.replace(year=now.year + 1)
    assert await repository.claim(task_id=task_id, owner="A", expires_at=renewed, now=now) is True
    row = await repository.get(task_id)
    assert row is not None
    assert row.lease_owner == "A"
    assert row.lease_expires_at == renewed.replace(tzinfo=None)


async def test_update_owned_rejects_stolen_lease(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
) -> None:
    """租约被接管后旧持有者的进度写入被拒（不覆盖他人进度）。"""
    await _seed(message_store, topic=topic, count=2)
    task = await replay_service.create(tenant=current_tenant, request=_request())
    repository = _repository(db_session)
    task_id = int(task.id)
    now = clock()
    await repository.claim(task_id=task_id, owner="A", expires_at=now, now=now)
    later = now.replace(year=now.year + 1)
    await repository.claim(task_id=task_id, owner="B", expires_at=later, now=later)

    updated = await repository.update_owned(
        task_id=task_id,
        owner="A",
        expires_at=now,
        values=ConcurrentStableDict({"replayed": 99, "status": "running"}),
    )
    assert updated is None
    row = await repository.get(task_id)
    assert row is not None
    assert row.replayed == 0


async def test_runner_skips_task_held_by_another_instance(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
    replay_auditor: ReplayAuditor,
) -> None:
    """A 推进后租约未过期，B 本轮写入 0（不重复 append）。"""
    await _seed(message_store, topic=topic, count=4)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=1))
    runner_a = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="A")
    assert await runner_a.run_once() > 0
    lease_owner = await _lease_owner(db_session, task_id=int(task.id))
    assert lease_owner == "A"

    runner_b = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="B")
    assert await runner_b.run_once() == 0
    assert await _lease_owner(db_session, task_id=int(task.id)) == "A"


async def test_runner_takes_over_expired_lease_and_clears_on_completion(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
    replay_auditor: ReplayAuditor,
) -> None:
    """A 崩溃后租约过期，B 接管并把任务跑完；终态清空租约。"""
    await _seed(message_store, topic=topic, count=4)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=1))
    runner_a = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="A")
    assert await runner_a.run_once() > 0
    clock.advance(milliseconds=60_000)

    runner_b = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="B")
    for _ in range(8):
        await runner_b.run_once()
        clock.advance(milliseconds=1_000)
        row = await _task_row(db_session, task_id=int(task.id))
        if row is not None and row.status == TASK_COMPLETED:
            break
    row = await _task_row(db_session, task_id=int(task.id))
    assert row is not None
    assert row.status == TASK_COMPLETED
    assert row.replayed == 4
    assert row.lease_owner is None
    assert row.lease_expires_at is None


async def _task_row(db_session: AsyncSession, *, task_id: int) -> MsgReplayTask | None:
    """读任务行（先回滚未收尾的隐式事务，再在独立事务里读）。"""
    await db_session.rollback()
    uow = DbUnitOfWork(cast("DbSession", db_session))
    async with uow.begin():
        return await _repository(db_session).get(task_id)


async def _lease_owner(db_session: AsyncSession, *, task_id: int) -> str | None:
    """读任务当前租约持有者。"""
    row = await _task_row(db_session, task_id=task_id)
    return None if row is None else row.lease_owner


@pytest.mark.kiwi_id(2497)
async def test_advance_skips_vanished_task(
    message_store: InMemoryMessageStore,
    db_session: AsyncSession,
    clock: ManualClock,
    replay_auditor: ReplayAuditor,
) -> None:
    """任务被删 / 已终结：`_advance` 直接让出（不误写）。"""
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="A")
    assert await runner._advance(task_id=987654321, now=clock()) == (0, None)


@pytest.mark.kiwi_id(2497)
async def test_advance_gives_up_when_lease_stolen(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    clock: ManualClock,
    replay_auditor: ReplayAuditor,
) -> None:
    """租约在推进前被接管：`update_owned` 返回 None，本轮写入不生效。"""
    await _seed(message_store, topic=topic, count=2)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=100))
    repository = _repository(db_session)
    now = clock()
    await repository.claim(task_id=int(task.id), owner="A", expires_at=now, now=now)
    await db_session.commit()
    later = now.replace(year=now.year + 1)
    await repository.claim(task_id=int(task.id), owner="B", expires_at=later, now=later)
    await db_session.commit()

    runner_a = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock, owner="A")
    assert await runner_a._advance(task_id=int(task.id), now=later) == (0, None)
    row = await _task_row(db_session, task_id=int(task.id))
    assert row is not None
    assert row.replayed == 0
