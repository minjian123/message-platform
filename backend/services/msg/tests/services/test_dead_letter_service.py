"""死信服务用例（06-5-1 / 06-5-2）：状态迁移、惰性回写、租户隔离与列表过滤。"""

# pyright: reportPrivateUsage=false

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.deadletter import DatabaseDeadLetterQueue, DeadLetter, DeadLetterReason
from bms_msg.errors import MsgDeadLetterInvalidStateError, MsgDeadLetterNotFoundError
from bms_msg.models.dead_letter import MsgDeadLetter
from bms_msg.models.replay import TASK_COMPLETED, TASK_FAILED, MsgReplayTask
from bms_msg.repositories.dead_letter import DeadLetterRepository
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.services.dead_letter import DeadLetterService

TENANT_ID = 7001
"""测试租户主键（与 conftest 的 `demo` 同值）。"""

OTHER_TENANT_ID = 8002
"""另一租户主键（验证租户隔离）。"""


def _letter(*, offset: int = 10, tenant_id: int = TENANT_ID, status: str = "pending") -> DeadLetter:
    """构造一条死信。

    Args:
        offset: 分区内序号。
        tenant_id: 租户主键。
        status: 处置状态。

    Returns:
        DeadLetter: 死信值对象。
    """
    return DeadLetter(
        message_id=f"msg-{offset}",
        topic_id=9001,
        partition_no=0,
        offset=offset,
        attempts=5,
        failed_at=datetime(2026, 1, 1, tzinfo=UTC),
        reason=DeadLetterReason.PROCESSING_FAILED,
        detail=ConcurrentStableDict({"error": "boom"}),
        tenant_id=tenant_id,
        namespace="msg",
        topic="audit.operation",
        log_id=f"log-{offset}",
        trace_id="trace-1",
        status=status,
    )


def _task(*, status: str = TASK_COMPLETED) -> MsgReplayTask:
    """构造一条重放任务。

    Args:
        status: 任务状态。

    Returns:
        MsgReplayTask: 重放任务行。
    """
    return MsgReplayTask(
        tenant_id=TENANT_ID,
        namespace="msg",
        topic_id=9001,
        topic="audit.operation",
        status=status,
        total=1,
        replayed=1,
    )


@pytest.fixture
async def queue(replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]]) -> DatabaseDeadLetterQueue:
    """持久化队列（复用内存会话）。"""
    return DatabaseDeadLetterQueue(uow_factory=replay_uow)


@pytest.fixture
def repository(db_session: AsyncSession) -> DeadLetterRepository:
    """死信仓储（同会话）。"""
    return DeadLetterRepository(db_session)


@pytest.fixture
def service(db_session: AsyncSession) -> DeadLetterService:
    """死信服务（同会话仓储 + 工作单元）。"""
    return DeadLetterService(
        uow=DbUnitOfWork(db_session),
        dead_letters=DeadLetterRepository(db_session),
        replay_tasks=ReplayTaskRepository(db_session),
    )


@pytest.mark.kiwi_id(2467)
async def test_detail_rejects_other_tenant(
    service: DeadLetterService, queue: DatabaseDeadLetterQueue, repository: DeadLetterRepository
) -> None:
    """跨租户按「不存在」处理（350029），不泄露存在性。"""
    await queue.add(_letter(tenant_id=OTHER_TENANT_ID))
    other = (await repository.list_all())[0]
    with pytest.raises(MsgDeadLetterNotFoundError):
        await service.detail(item_id=int(other.id), tenant_id=TENANT_ID)


@pytest.mark.kiwi_id(2467)
async def test_ignore_then_archive_and_illegal_transitions(
    service: DeadLetterService, queue: DatabaseDeadLetterQueue
) -> None:
    """`pending → ignored → archived` 合法；重复忽略 / 归档返回 350030。"""
    await queue.add(_letter(offset=10))
    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    row = await service.detail(item_id=int(rows[0].id), tenant_id=TENANT_ID)

    await service.ignore(item_id=int(row.id), tenant_id=TENANT_ID, reason="上游已作废", actor_id=7001)
    row = await service.detail(item_id=int(rows[0].id), tenant_id=TENANT_ID)
    assert row.status == "ignored"
    assert row.disposition_reason == "上游已作废"
    row_id = int(rows[0].id)
    with pytest.raises(MsgDeadLetterInvalidStateError):
        await service.ignore(item_id=row_id, tenant_id=TENANT_ID, reason="再次忽略", actor_id=7001)

    await service.archive(item_id=row_id, tenant_id=TENANT_ID, reason="归档", actor_id=7001)
    row = await service.detail(item_id=row_id, tenant_id=TENANT_ID)
    assert row.status == "archived"
    with pytest.raises(MsgDeadLetterInvalidStateError):
        await service.archive(item_id=row_id, tenant_id=TENANT_ID, reason=None, actor_id=7001)


@pytest.mark.kiwi_id(2467)
async def test_begin_replay_sets_replaying_once(service: DeadLetterService, queue: DatabaseDeadLetterQueue) -> None:
    """`pending → replaying` 只允许一次（并发 / 重复处置返回 350030）。"""
    await queue.add(_letter(offset=10))
    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    row = await service.detail(item_id=int(rows[0].id), tenant_id=TENANT_ID)

    await service.mark_replaying(item_id=int(row.id), tenant_id=TENANT_ID, replay_task_id=123)
    row_id = int(rows[0].id)
    row = await service.detail(item_id=row_id, tenant_id=TENANT_ID)
    assert row.status == "replaying"
    assert row.replay_task_id == 123
    with pytest.raises(MsgDeadLetterInvalidStateError):
        await service.mark_replaying(item_id=row_id, tenant_id=TENANT_ID, replay_task_id=124)


@pytest.mark.kiwi_id(2467)
async def test_list_excludes_archived_by_default(service: DeadLetterService, queue: DatabaseDeadLetterQueue) -> None:
    """列表缺省排除 `archived`；显式筛选时只返回该状态。"""
    await queue.add(_letter(offset=10))
    await queue.add(_letter(offset=11, status="archived"))

    rows, total = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    assert total == 1
    assert [int(row.msg_offset) for row in rows] == [10]

    archived, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20, status="archived")
    assert [int(row.msg_offset) for row in archived] == [11]


@pytest.mark.kiwi_id(2467)
async def test_detail_lazily_resolves_replaying_by_task_state(
    service: DeadLetterService,
    queue: DatabaseDeadLetterQueue,
    repository: DeadLetterRepository,
    db_session: AsyncSession,
) -> None:
    """`replaying` + 任务 `completed` → 查询时回写 `resolved`。"""
    await queue.add(_letter(offset=10, status="replaying"))
    row = (await repository.list_all())[0]

    task = MsgReplayTask(
        tenant_id=TENANT_ID,
        namespace="msg",
        topic_id=9001,
        topic="audit.operation",
        status=TASK_COMPLETED,
        total=1,
        replayed=1,
    )
    db_session.add(task)
    await db_session.commit()
    row_id = int(row.id)
    row.replay_task_id = int(task.id)
    await db_session.commit()

    resolved = await service.detail(item_id=row_id, tenant_id=TENANT_ID)
    assert resolved.status == "resolved"


@pytest.mark.kiwi_id(2467)
async def test_repository_purge_empty_is_noop(repository: DeadLetterRepository) -> None:
    """仓储按主题清理：空集合直接返回 0。"""
    assert await repository.purge_topics(topic_ids=ConcurrentStableList()) == 0


@pytest.mark.kiwi_id(2467)
async def test_repository_page_can_include_archived(
    repository: DeadLetterRepository, queue: DatabaseDeadLetterQueue
) -> None:
    """仓储分页缺省包含 `archived`（排除策略由服务层显式传入）。"""
    await queue.add(_letter(offset=10))
    await queue.add(_letter(offset=11, status="archived"))

    rows, total = await repository.page_for_tenant(tenant_id=TENANT_ID, page=1, size=20)
    assert total == 2
    assert sorted((int(row.msg_offset), row.status) for row in rows) == [(10, "pending"), (11, "archived")]


@pytest.mark.kiwi_id(2467)
async def test_list_syncs_replaying_rows_and_tolerates_failure(
    service: DeadLetterService,
    queue: DatabaseDeadLetterQueue,
    repository: DeadLetterRepository,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """列表按任务终态回写 `replaying`；回写失败只告警、仍返回原行。"""
    await queue.add(_letter(offset=10, status="replaying"))
    task = _task()
    db_session.add(task)
    await db_session.commit()
    row = (await repository.list_all())[0]
    row.replay_task_id = int(task.id)
    await db_session.commit()

    rows, total = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    assert total == 1
    assert rows[0].status == "resolved"

    await db_session.rollback()
    await queue.add(_letter(offset=11, status="replaying"))
    second = (await repository.list_all())[-1]
    second_task = _task()
    db_session.add(second_task)
    await db_session.commit()
    second.replay_task_id = int(second_task.id)
    await db_session.commit()

    async def failing_sync(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("sync down")

    monkeypatch.setattr(type(service), "_sync_replaying_ids", failing_sync)
    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    assert any(row.status == "replaying" for row in rows)


@pytest.mark.kiwi_id(2467)
async def test_list_resolves_failed_replay_to_pending(
    service: DeadLetterService,
    queue: DatabaseDeadLetterQueue,
    repository: DeadLetterRepository,
    db_session: AsyncSession,
) -> None:
    """`replaying` + 任务失败 → 查询时收回 `pending`。"""
    await queue.add(_letter(offset=10, status="replaying"))
    task = _task(status=TASK_FAILED)
    db_session.add(task)
    await db_session.commit()
    row = (await repository.list_all())[0]
    row.replay_task_id = int(task.id)
    await db_session.commit()

    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    assert rows[0].status == "pending"


@pytest.mark.kiwi_id(2467)
async def test_detail_tolerates_sync_failure(
    service: DeadLetterService, queue: DatabaseDeadLetterQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """详情回写失败不阻塞查询（返回原 `replaying` 行）。"""
    await queue.add(_letter(offset=10, status="replaying"))

    async def failing_sync(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("sync down")

    monkeypatch.setattr(type(service), "_sync_replaying_ids", failing_sync)
    item_id = int((await service.list(tenant_id=TENANT_ID, page=1, size=20))[0][0].id)
    row = await service.detail(item_id=item_id, tenant_id=TENANT_ID)
    assert row.status == "replaying"


@pytest.mark.kiwi_id(2467)
async def test_detail_raises_when_row_vanishes_after_sync(
    service: DeadLetterService,
    queue: DatabaseDeadLetterQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回写期间行被删除（并发清理）→ 详情按不存在处理（350029）。"""
    await queue.add(_letter(offset=10, status="replaying"))
    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    item_id = int(rows[0].id)

    real = service._dead_letters.get_for_tenant
    calls = ConcurrentStableList([0])

    async def flaky_get_for_tenant(*, item_id: int, tenant_id: int) -> MsgDeadLetter | None:
        calls[0] += 1
        if calls[0] == 1:
            return await real(item_id=item_id, tenant_id=tenant_id)
        return None

    monkeypatch.setattr(service._dead_letters, "get_for_tenant", flaky_get_for_tenant)
    with pytest.raises(MsgDeadLetterNotFoundError):
        await service.detail(item_id=item_id, tenant_id=TENANT_ID)


@pytest.mark.kiwi_id(2467)
async def test_mark_replaying_rejects_lost_race(
    service: DeadLetterService, queue: DatabaseDeadLetterQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """并发下条件更新未命中 → 350030。"""
    await queue.add(_letter(offset=10))
    rows, _ = await service.list(tenant_id=TENANT_ID, page=1, size=20)
    item_id = int(rows[0].id)

    async def lost_race(*, item_id: int, tenant_id: int, replay_task_id: int) -> bool:
        del item_id, tenant_id, replay_task_id
        return False

    monkeypatch.setattr(service._dead_letters, "mark_replaying", lost_race)
    with pytest.raises(MsgDeadLetterInvalidStateError):
        await service.mark_replaying(item_id=item_id, tenant_id=TENANT_ID, replay_task_id=1)


@pytest.mark.kiwi_id(2467)
async def test_flush_is_noop_without_session(repository: DeadLetterRepository) -> None:
    """工作单元无会话时刷写为空操作。"""
    service = DeadLetterService(
        uow=cast("UnitOfWork", SimpleNamespace(session=None)),
        dead_letters=repository,
        replay_tasks=cast("ReplayTaskRepository", SimpleNamespace()),
    )
    await service._flush()
