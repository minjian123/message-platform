"""消费组落库（01-4-1；Kiwi 2496）：两表往返 / 成员整替 / 删除 / 空组回收 / 清理。

装配：真实 `DatabaseGroupStore` + 内存库会话（工作单元工厂每次自开短事务）。
"""

# pyright: reportPrivateUsage=false

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.group import (
    GROUP_STATE_DEAD,
    GROUP_STATE_EMPTY,
    GROUP_STATE_STABLE,
    ConsumerGroup,
    GroupMember,
)
from bms_msg.broker.group_db import DatabaseGroupStore
from bms_msg.models.consumer_group import MsgConsumerGroup, MsgGroupMember
from bms_msg.repositories.consumer_group import ConsumerGroupRepository

pytestmark = pytest.mark.kiwi_id(2496)

TENANT_ID = 7001
TOPIC_ID = 5_550_001
NAMESPACE = "msg"
GROUP = "audit"
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)


def _uow_factory(session: AsyncSession):
    """构造「每次自开 UoW」的工厂（复用内存库会话）。"""

    @asynccontextmanager
    async def factory() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(cast("DbSession", session))

    return factory


def _store(session: AsyncSession, *, instance_id: str = "n1") -> DatabaseGroupStore:
    """构造 DB 组存储。"""
    return DatabaseGroupStore(uow_factory=_uow_factory(session), instance_id=instance_id)


def _record(*members: str, state: str = GROUP_STATE_STABLE) -> ConsumerGroup:
    """组快照（成员按给定顺序，分区连续切分）。"""
    return ConsumerGroup(
        tenant_id=TENANT_ID,
        topic_id=TOPIC_ID,
        group=GROUP,
        namespace=NAMESPACE,
        state=state,
        generation=len(members),
        members=ConcurrentStableList(
            GroupMember(
                consumer_id=name,
                joined_at=NOW,
                last_seen=NOW,
                partitions=ConcurrentStableList([index]),
            )
            for index, name in enumerate(members)
        ),
    )


async def test_save_and_load_round_trip(db_session: AsyncSession) -> None:
    """保存后能取回同快照：成员、分区、世代、命名空间一致，实例标识落库。"""
    store = _store(db_session)
    await store.save(_record("c1", "c2"))

    loaded = await store.load(tenant_id=TENANT_ID, topic_id=TOPIC_ID, group=GROUP)
    assert loaded is not None
    assert loaded.group == GROUP
    assert loaded.namespace == NAMESPACE
    assert loaded.generation == 2
    assert [(item.consumer_id, list(item.partitions)) for item in loaded.members] == [
        ("c1", [0]),
        ("c2", [1]),
    ]

    instance = (await db_session.execute(select(MsgGroupMember.instance_id))).scalars().all()
    assert set(instance) == {"n1"}


async def test_save_replaces_members(db_session: AsyncSession) -> None:
    """成员整替：再均衡后旧成员行不残留。"""
    store = _store(db_session)
    await store.save(_record("c1", "c2"))
    await store.save(_record("c2"))

    loaded = await store.load(tenant_id=TENANT_ID, topic_id=TOPIC_ID, group=GROUP)
    assert loaded is not None
    assert [item.consumer_id for item in loaded.members] == ["c2"]


async def test_list_and_delete(db_session: AsyncSession) -> None:
    """列表按租户 / 主题过滤；删除命中后不可再取，重复删除返回 False。"""
    store = _store(db_session)
    await store.save(_record("c1"))

    by_tenant = await store.list_for_tenant(tenant_id=TENANT_ID)
    assert [record.group for record in by_tenant] == [GROUP]
    assert await store.list_for_tenant(tenant_id=TENANT_ID, topic_id=TOPIC_ID + 1) == []

    assert await store.delete(tenant_id=TENANT_ID, topic_id=TOPIC_ID, group=GROUP) is True
    assert await store.load(tenant_id=TENANT_ID, topic_id=TOPIC_ID, group=GROUP) is None
    assert await store.delete(tenant_id=TENANT_ID, topic_id=TOPIC_ID, group=GROUP) is False


async def test_mark_idle_dead_only_empty_groups(db_session: AsyncSession) -> None:
    """空组超期置 `dead`；有成员的组不动；重复执行幂等。"""
    store = _store(db_session)
    await store.save(_record("c1", state=GROUP_STATE_EMPTY))
    await store.save(replace(_record(state=GROUP_STATE_EMPTY), group="other"))
    await store.save(_record("c1", state=GROUP_STATE_STABLE))

    changed = await store.mark_idle_dead(before=datetime.now(UTC) + timedelta(seconds=1))
    assert changed == 1

    groups = await store.list_for_tenant(tenant_id=TENANT_ID)
    states = {record.group: record.state for record in groups}
    assert states[GROUP] == GROUP_STATE_STABLE
    assert states["other"] == GROUP_STATE_DEAD
    assert await store.mark_idle_dead(before=datetime.now(UTC) + timedelta(seconds=1)) == 0


async def test_purge_tenant_and_topic(db_session: AsyncSession) -> None:
    """租户 / 主题清理物理删除组与成员行。"""
    store = _store(db_session)
    await store.save(_record("c1"))

    assert await store.purge_topic(topic_id=TOPIC_ID) == 1
    assert await store.list_all() == []

    await store.save(_record("c1"))
    assert await store.purge_tenant(tenant_id=TENANT_ID) == 1
    assert await store.list_for_tenant(tenant_id=TENANT_ID) == []


async def test_repository_get_by_id_and_count(db_session: AsyncSession) -> None:
    """仓储按主键取快照；组数统计；空清理返回 0。"""
    store = _store(db_session)
    await store.save(_record("c1"))

    repository = ConsumerGroupRepository(cast("DbSession", db_session))
    group_id = int((await db_session.execute(select(MsgConsumerGroup.id))).scalar_one())
    loaded = await repository.get_by_id(group_id)
    assert loaded is not None and loaded.group == GROUP
    assert await repository.get_by_id(group_id + 1) is None
    await db_session.rollback()
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 1
    assert await store.purge_tenant(tenant_id=TENANT_ID + 1) == 0
