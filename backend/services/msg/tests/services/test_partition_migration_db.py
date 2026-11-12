"""平台库迁移拓扑（02-8）：分区事实读取 + CAS 写分配。"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.partition_migration import DbMigrationTopology
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2486)

MakeRequest = Callable[..., TopicCreateRequest]


def _uow(session: AsyncSession) -> Callable[[], AbstractAsyncContextManager[UnitOfWork]]:
    """构造复用同一会话的工作单元工厂。

    pytestmark = pytest.mark.kiwi_id(2486)

        Args:
            session: 请求级会话。

        Returns:
            Callable[[], AbstractAsyncContextManager[UnitOfWork]]: 工厂。
    """

    @asynccontextmanager
    async def _open() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(session)

    return _open


async def test_db_topology_reads_and_writes_assignment(
    service: TopicService,
    db_session: AsyncSession,
    current_tenant: CurrentTenant,
    make_request: MakeRequest,
) -> None:
    """拓扑：按主题过滤读取、单分区读取、CAS 写成功与版本冲突。"""
    topic = await service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    topology = DbMigrationTopology(uow_factory=_uow(db_session))

    rows = await topology.list_partitions(topic_ids=ConcurrentStableFrozenList((int(topic.id),)))
    assert [row.partition_no for row in rows] == [0, 1]
    assert rows[0].tenant_id == 7001
    assert rows[0].namespace == "msg"

    one = await topology.read_partition(topic_id=int(topic.id), partition_no=1)
    assert one is not None
    assert await topology.read_partition(topic_id=int(topic.id), partition_no=9) is None

    written = await topology.write_assignment(
        topic_id=int(topic.id),
        partition_no=0,
        replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        leader="msg-1",
        expected_version=1,
    )
    assert written is True
    updated = await topology.read_partition(topic_id=int(topic.id), partition_no=0)
    assert updated is not None
    assert list(updated.replicas) == ["msg-1", "msg-2"]
    assert updated.version == 2

    conflict = await topology.write_assignment(
        topic_id=int(topic.id),
        partition_no=0,
        replicas=ConcurrentStableFrozenList(("msg-1",)),
        leader="msg-1",
        expected_version=1,
    )
    assert conflict is False


async def test_db_topology_list_without_filter(
    service: TopicService, db_session: AsyncSession, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """拓扑：不传主题过滤时返回全部（按主题 / 分区升序）。"""

    await service.create_topic(tenant=current_tenant, request=make_request(partition_count=1))
    topology = DbMigrationTopology(uow_factory=_uow(db_session))

    rows = await topology.list_partitions()

    assert len(rows) >= 1
    assert [row.partition_no for row in rows] == sorted(row.partition_no for row in rows)
