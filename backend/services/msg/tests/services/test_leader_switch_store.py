"""切换元数据存储（msg 任务 02-5；Kiwi 2485）：`msg_partition` 两阶段 CAS。

用内存 SQLite + 真实主题服务建表数据，覆盖：
读分区 / 读主题分区、phase 1 CAS（成功 / 冲突 / 缺行）、phase 2 CAS（成功 / 已 idle）、
checkpoint 回落到截断位点 / 两者皆空不写。
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.replay import UowFactory
from bms_msg.models.partition import SWITCH_STATE_IDLE, SWITCH_STATE_SWITCHING, MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.leader_switch import DbLeaderSwitchStore
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2485)


def _uow_factory(session: AsyncSession) -> UowFactory:
    """把请求级会话包装成工作单元工厂。

    Args:
        session: 内存库会话。

    Returns:
        UowFactory: 工作单元工厂。
    """
    db_session = cast("DbSession", session)

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """产出绑定同一会话的工作单元。

        Yields:
            UnitOfWork: 工作单元。
        """
        yield DbUnitOfWork(db_session)

    return _uow


async def _topic_with_replicas(
    service: TopicService,
    tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    session: AsyncSession,
) -> int:
    """建一个两分区主题并写入副本集。

    Args:
        service: 主题服务。
        tenant: 当前租户。
        make_request: 创建请求构造器。
        session: 内存库会话。

    Returns:
        int: 主题主键。
    """
    request = make_request(name="audit.failover", partition_count=2)
    topic = await service.create_topic(tenant=tenant, request=request)
    rows = (await session.execute(select(MsgPartition).where(MsgPartition.topic_id == int(topic.id)))).scalars().all()
    for row in rows:
        await session.execute(
            update(MsgPartition).where(MsgPartition.id == row.id).values(replicas=["n1", "n2"], min_insync_replicas=2)
        )
    await session.commit()
    return int(topic.id)


async def test_load_partition_and_topic_partitions(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    db_session: AsyncSession,
) -> None:
    """读单个分区与主题全部分区（含主题身份 / 副本集 / 状态）。"""
    topic_id = await _topic_with_replicas(service, current_tenant, make_request, db_session)
    store = DbLeaderSwitchStore(uow_factory=_uow_factory(db_session))
    state = await store.load_partition(topic_id=topic_id, partition_no=0)
    assert state is not None
    assert state.topic_id == topic_id
    assert state.partition_no == 0
    assert state.namespace == "msg"
    assert state.topic == "audit.failover"
    assert state.tenant_id == int(current_tenant.tenant_id)
    assert list(state.replicas) == ["n1", "n2"]
    assert state.switch_state == SWITCH_STATE_IDLE
    assert state.last_switch_at is None, "从未切换的分区没有冷却判据时间"

    partitions = await store.load_topic_partitions(topic_id=topic_id)
    assert [item.partition_no for item in partitions] == [0, 1]

    assert await store.load_partition(topic_id=topic_id, partition_no=9) is None
    assert await store.load_partition(topic_id=topic_id + 1, partition_no=0) is None
    assert list(await store.load_topic_partitions(topic_id=topic_id + 1)) == []


async def test_begin_and_finish_switch_cas(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    db_session: AsyncSession,
) -> None:
    """phase 1 成功 → phase 2 收口；重复调用返回 False（幂等由服务判定）。"""
    topic_id = await _topic_with_replicas(service, current_tenant, make_request, db_session)
    store = DbLeaderSwitchStore(uow_factory=_uow_factory(db_session))

    assert await store.begin_switch(
        topic_id=topic_id,
        partition_no=0,
        expected_epoch=0,
        target="n2",
        from_leader="n1",
        from_epoch=0,
    )
    begun = await store.load_partition(topic_id=topic_id, partition_no=0)
    assert begun is not None
    assert begun.switch_state == SWITCH_STATE_SWITCHING
    assert begun.leader == "n2"
    assert begun.epoch == 1
    assert begun.switch_from_leader == "n1"
    assert begun.switch_from_epoch == 0

    # 已是 switching：同参数再执行 0 行（不是幂等成功，由服务续跑）。
    assert not await store.begin_switch(
        topic_id=topic_id,
        partition_no=0,
        expected_epoch=0,
        target="n2",
        from_leader="n1",
        from_epoch=0,
    )
    # 缺行：0 行。
    assert not await store.begin_switch(
        topic_id=topic_id,
        partition_no=9,
        expected_epoch=0,
        target="n2",
        from_leader="n1",
        from_epoch=0,
    )

    assert await store.finish_switch(
        topic_id=topic_id,
        partition_no=0,
        to_epoch=1,
        watermark=2,
        result="truncated",
        truncated_to=2,
        operator="system:drill",
    )
    finished = await store.load_partition(topic_id=topic_id, partition_no=0)
    assert finished is not None
    assert finished.switch_state == SWITCH_STATE_IDLE
    assert finished.epoch == 1
    row = await db_session.get(MsgPartition, (await _row_id(db_session, topic_id)))
    assert row is not None
    assert row.last_switch_by == "system:drill"
    assert row.last_switch_epoch == 1
    assert row.last_switch_result == "truncated"
    assert row.last_truncate_to == 2
    assert row.checkpoint_hw == 2
    assert row.last_switch_at is not None

    # 已 idle：phase 2 再执行 0 行。
    assert not await store.finish_switch(
        topic_id=topic_id,
        partition_no=0,
        to_epoch=1,
        watermark=2,
        result="truncated",
        truncated_to=2,
        operator="system:drill",
    )


async def test_finish_switch_checkpoint_fallbacks(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    db_session: AsyncSession,
) -> None:
    """checkpoint 回落：watermark 为空取 truncated_to；两者皆空不写 checkpoint。"""
    topic_id = await _topic_with_replicas(service, current_tenant, make_request, db_session)
    store = DbLeaderSwitchStore(uow_factory=_uow_factory(db_session))

    await store.begin_switch(
        topic_id=topic_id, partition_no=0, expected_epoch=0, target="n2", from_leader="n1", from_epoch=0
    )
    await store.finish_switch(
        topic_id=topic_id,
        partition_no=0,
        to_epoch=1,
        watermark=None,
        result="retained",
        truncated_to=3,
        operator="system",
    )
    row = await db_session.get(MsgPartition, await _row_id(db_session, topic_id))
    assert row is not None
    assert row.checkpoint_hw == 3
    assert row.last_truncate_to == 3

    await store.begin_switch(
        topic_id=topic_id, partition_no=1, expected_epoch=0, target="n2", from_leader="n1", from_epoch=0
    )
    await store.finish_switch(
        topic_id=topic_id,
        partition_no=1,
        to_epoch=1,
        watermark=None,
        result="retained",
        truncated_to=None,
        operator="system",
    )
    second = await db_session.get(MsgPartition, await _row_id(db_session, topic_id, partition_no=1))
    assert second is not None
    assert second.checkpoint_hw == 0
    assert second.last_truncate_to is None


async def test_state_serialises_empty_replicas(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    db_session: AsyncSession,
) -> None:
    """副本集为空：状态仍可读（协调器按配置兜底）。"""
    request = make_request(name="audit.empty")  # type: ignore[operator]
    topic = await service.create_topic(tenant=current_tenant, request=request)
    store = DbLeaderSwitchStore(uow_factory=_uow_factory(db_session))
    state = await store.load_partition(topic_id=int(topic.id), partition_no=0)
    assert state is not None
    assert list(state.replicas) == []
    assert state.replicas == ConcurrentStableFrozenList[str]()


async def test_load_partition_returns_none_when_topic_missing(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: Callable[..., TopicCreateRequest],
    db_session: AsyncSession,
) -> None:
    """分区行在但主题行缺失：返回 None（不伪造租户 / 主题身份）。"""
    topic_id = await _topic_with_replicas(service, current_tenant, make_request, db_session)
    await db_session.execute(delete(MsgTopic).where(MsgTopic.id == topic_id))
    await db_session.commit()
    store = DbLeaderSwitchStore(uow_factory=_uow_factory(db_session))
    assert await store.load_partition(topic_id=topic_id, partition_no=0) is None


async def _row_id(session: AsyncSession, topic_id: int, *, partition_no: int = 0) -> int:
    """取分区行主键。

    Args:
        session: 内存库会话。
        topic_id: 主题主键。
        partition_no: 分区号。

    Returns:
        int: 分区行主键。
    """
    rows = (await session.execute(select(MsgPartition).where(MsgPartition.topic_id == int(topic_id)))).scalars().all()
    for row in rows:
        if int(row.partition_no) == partition_no:
            return int(row.id)
    raise AssertionError("分区行不存在")
