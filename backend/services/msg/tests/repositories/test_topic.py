"""主题 / 分区仓储（msg 任务 01-1；Kiwi 2412）。

覆盖：按标识查重、分页列表与总数（命名空间 / 关键词过滤）、分区列表顺序、追加分区、租户作用域隔离。
"""

from typing import cast

import pytest
from bms_core.core.context import reset_tenant_context, set_tenant_context
from bms_core.db.session import DbSession
from bms_core.db.tenant import TenantContext
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.repositories.topic import PartitionRepository, TopicRepository

pytestmark = pytest.mark.kiwi_id(2412)


def _topics(session: AsyncSession) -> TopicRepository:
    """构造主题仓储（异步会话满足 `DbSession` 契约）。

    Args:
        session: 异步会话。

    Returns:
        TopicRepository: 主题仓储。
    """
    return TopicRepository(cast("DbSession", session))


def _partitions(session: AsyncSession) -> PartitionRepository:
    """构造分区仓储。

    Args:
        session: 异步会话。

    Returns:
        PartitionRepository: 分区仓储。
    """
    return PartitionRepository(cast("DbSession", session))


async def test_create_and_lookup_by_identity(db_session: AsyncSession) -> None:
    """创建后可按唯一键取回（`tenant_id` 由仓储按上下文注入）。"""
    topic = await _topics(db_session).create(
        tenant_id=7001, namespace="msg", name="audit.operation", partition_count=2, retention_ms=1000
    )
    found = await _topics(db_session).get_by_identity(namespace="msg", name="audit.operation")
    assert found is not None
    assert (found.id, found.tenant_id, found.partition_count) == (topic.id, 7001, 2)


async def test_list_and_count_with_filters(db_session: AsyncSession) -> None:
    """分页列表与总数：命名空间 / 关键词过滤同源，分页切片正确。"""
    repository = _topics(db_session)
    for index in range(5):
        await repository.create(
            tenant_id=7001,
            namespace="msg",
            name=f"audit.operation.{index}",
            partition_count=1,
            retention_ms=1000,
        )
    await repository.create(tenant_id=7001, namespace="msg", name="notify.sent", partition_count=1, retention_ms=1000)
    all_rows = await repository.list_topics(namespace=None, keyword=None, offset=0, limit=10)
    assert len(all_rows) == 6
    assert await repository.count_topics(namespace=None, keyword=None) == 6

    filtered = await repository.list_topics(namespace="msg", keyword="audit", offset=1, limit=2)
    assert [row.name for row in filtered] == ["audit.operation.3", "audit.operation.2"]
    assert await repository.count_topics(namespace="msg", keyword="audit") == 5
    assert await repository.count_topics(namespace="other", keyword=None) == 0


async def test_partitions_listed_in_partition_order(db_session: AsyncSession) -> None:
    """分区列表按分区号升序（详情 / 复制的稳定顺序）。"""
    topic = await _topics(db_session).create(
        tenant_id=7001, namespace="msg", name="ordered", partition_count=3, retention_ms=1000
    )
    partitions = await _partitions(db_session).add_partitions(
        topic_id=int(topic.id), start_no=0, count=3, leader="msg-0"
    )
    listed = await _partitions(db_session).list_by_topic(int(topic.id))
    assert [partition.partition_no for partition in partitions] == [0, 1, 2]
    assert [partition.partition_no for partition in listed] == [0, 1, 2]
    assert {partition.leader for partition in listed} == {"msg-0"}
    assert [list(partition.replicas) for partition in listed] == [[], [], []]


async def test_append_partitions_keeps_existing_numbers(db_session: AsyncSession) -> None:
    """追加分区：从现有分区数继续编号，不重排、不覆盖既有行。"""
    topic = await _topics(db_session).create(
        tenant_id=7001, namespace="msg", name="expand", partition_count=2, retention_ms=1000
    )
    partitions = _partitions(db_session)
    await partitions.add_partitions(topic_id=int(topic.id), start_no=0, count=2, leader="msg-0")
    await partitions.add_partitions(topic_id=int(topic.id), start_no=2, count=2, leader="msg-0")
    listed = await partitions.list_by_topic(int(topic.id))
    assert [partition.partition_no for partition in listed] == [0, 1, 2, 3]


async def test_tenant_scope_hides_other_tenant_rows(db_session: AsyncSession) -> None:
    """租户作用域：切换租户上下文后，同库的其他租户主题不可见（平台库同库多租户的隔离层）。"""
    await _topics(db_session).create(
        tenant_id=7001, namespace="msg", name="tenant.a", partition_count=1, retention_ms=1000
    )
    token = set_tenant_context(TenantContext(code="other", db_key="tenant_other", name="other", tenant_id=8002))
    try:
        assert await _topics(db_session).get_by_identity(namespace="msg", name="tenant.a") is None
        assert await _topics(db_session).count_topics(namespace=None, keyword=None) == 0
    finally:
        reset_tenant_context(token)
