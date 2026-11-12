"""分区分配枚举（msg 任务 02-10；Kiwi 2489）。

覆盖 `PartitionRepository.list_assignments()`：全量、按 `(topic_id, partition_no)` 升序、带副本集。
"""

from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.repositories.topic import PartitionRepository, TopicRepository

pytestmark = pytest.mark.kiwi_id(2489)


async def test_list_assignments_returns_all_partitions_in_order(db_session: AsyncSession) -> None:
    """全量枚举：跨主题返回全部行，按 `(topic_id, partition_no)` 升序且带副本集。"""
    topics = TopicRepository(cast("DbSession", db_session))
    partitions = PartitionRepository(cast("DbSession", db_session))
    first = await topics.create(tenant_id=7001, namespace="msg", name="assign.a", partition_count=2, retention_ms=1_000)
    second = await topics.create(
        tenant_id=7001, namespace="msg", name="assign.b", partition_count=1, retention_ms=1_000
    )
    await partitions.add_partitions(
        topic_id=int(second.id),
        start_no=0,
        count=1,
        leader="msg-1",
        replicas_by_no=ConcurrentStableDict({0: ConcurrentStableList(("msg-1", "msg-2"))}),
    )
    await partitions.add_partitions(topic_id=int(first.id), start_no=0, count=2, leader="msg-0")

    rows = await partitions.list_assignments()
    keys = [(int(row.topic_id), int(row.partition_no)) for row in rows]
    assert keys == sorted(keys)
    assert keys == [
        (int(first.id), 0),
        (int(first.id), 1),
        (int(second.id), 0),
    ]
    replicas = {int(row.topic_id): list(row.replicas) for row in rows if int(row.topic_id) == int(second.id)}
    assert replicas[int(second.id)] == ["msg-1", "msg-2"]
