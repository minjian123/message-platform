"""消费组存储端口与内存实现（msg 任务 01-4；Kiwi 2416）。

覆盖：未存过的组返回 None；快照往返（成员 / 分配保序）；`(tenant, topic, group)` 三维隔离；
`list_for_tenant` 的 `topic_id` 可选过滤。8B_02 换 DB 实现时，本文件即端口契约的可复用判据。
"""

from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.broker.group import (
    GROUP_STATE_EMPTY,
    GROUP_STATE_STABLE,
    ConsumerGroup,
    GroupMember,
    GroupStore,
    InMemoryGroupStore,
)

pytestmark = pytest.mark.kiwi_id(2416)

TENANT = 7001
"""用例租户主键。"""

NOW = datetime(2026, 1, 1, tzinfo=UTC)
"""用例固定时刻。"""


def _record(
    *,
    tenant_id: int = TENANT,
    topic_id: int = 101,
    group: str = "audit",
    state: str = GROUP_STATE_STABLE,
    generation: int = 1,
    members: ConcurrentStableList[GroupMember] | None = None,
) -> ConsumerGroup:
    """构造组记录。

    Args:
        tenant_id: 租户主键。
        topic_id: 主题主键。
        group: 组名。
        state: 组状态。
        generation: 世代号。
        members: 成员列表；None 表示空组。

    Returns:
        ConsumerGroup: 组记录。
    """
    return ConsumerGroup(
        tenant_id=tenant_id,
        topic_id=topic_id,
        group=group,
        state=state,
        generation=generation,
        members=ConcurrentStableList() if members is None else members,
    )


def _member(consumer_id: str, partitions: list[int]) -> GroupMember:
    """构造成员。

    Args:
        consumer_id: 成员标识。
        partitions: 持有的分区号。

    Returns:
        GroupMember: 成员。
    """
    return GroupMember(
        consumer_id=consumer_id,
        joined_at=NOW,
        last_seen=NOW,
        partitions=ConcurrentStableList(partitions),
    )


async def test_load_missing_returns_none(group_store: InMemoryGroupStore) -> None:
    """未存过的组：`load` 返回 None（未 join 过即组不存在）。"""
    assert await group_store.load(tenant_id=TENANT, topic_id=101, group="audit") is None


async def test_save_then_load_round_trip(group_store: InMemoryGroupStore) -> None:
    """快照往返：成员与分配原样取回（成员升序、分区升序由调用方保证）。"""
    record = _record(members=ConcurrentStableList([_member("c1", [0]), _member("c2", [1, 2])]))
    await group_store.save(record)
    loaded = await group_store.load(tenant_id=TENANT, topic_id=101, group="audit")
    assert loaded == record
    assert [member.consumer_id for member in loaded.members] == ["c1", "c2"]  # type: ignore[union-attr]
    assert [list(member.partitions) for member in loaded.members] == [[0], [1, 2]]  # type: ignore[union-attr]


async def test_groups_isolated_by_topic_and_tenant(group_store: InMemoryGroupStore) -> None:
    """三维隔离：同组名不同主题 / 不同租户互不可见。"""
    await group_store.save(_record())
    await group_store.save(_record(topic_id=202))
    await group_store.save(_record(tenant_id=8002))
    assert (await group_store.load(tenant_id=TENANT, topic_id=101, group="audit")) is not None
    assert (await group_store.load(tenant_id=TENANT, topic_id=202, group="audit")) is not None
    assert (await group_store.load(tenant_id=8002, topic_id=101, group="audit")) is not None
    assert (await group_store.load(tenant_id=TENANT, topic_id=303, group="audit")) is None
    assert (await group_store.load(tenant_id=TENANT, topic_id=101, group="other")) is None


async def test_list_for_tenant_filters_topic(group_store: InMemoryGroupStore) -> None:
    """列表：不带 `topic_id` 取本租户全部（组名升序），带了只取该主题。"""
    await group_store.save(_record(group="zeta"))
    await group_store.save(_record(group="alpha"))
    await group_store.save(_record(topic_id=202, group="beta"))
    await group_store.save(_record(tenant_id=8002, group="foreign"))
    everything = await group_store.list_for_tenant(tenant_id=TENANT)
    assert [record.group for record in everything] == ["alpha", "beta", "zeta"]
    narrowed = await group_store.list_for_tenant(tenant_id=TENANT, topic_id=202)
    assert [record.group for record in narrowed] == ["beta"]


async def test_save_overwrites_snapshot(group_store: InMemoryGroupStore) -> None:
    """覆盖语义：同键再存即整条替换（`preparing_rebalance` → `stable` 两步写法的基础）。"""
    await group_store.save(_record(state=GROUP_STATE_EMPTY, generation=0))
    await group_store.save(
        _record(state=GROUP_STATE_STABLE, generation=1, members=ConcurrentStableList([_member("c1", [0])]))
    )
    loaded = await group_store.load(tenant_id=TENANT, topic_id=101, group="audit")
    assert (loaded.state, loaded.generation) == (GROUP_STATE_STABLE, 1)  # type: ignore[union-attr]
    assert len(loaded.members) == 1  # type: ignore[union-attr]


async def test_purge_topic_removes_only_target_topic() -> None:
    """06-2：按主题清理只删该主题的组，其他主题与租户不受影响。"""
    store = InMemoryGroupStore()
    await store.save(_record(topic_id=1, group="a"))
    await store.save(_record(topic_id=2, group="b"))

    assert await store.purge_topic(topic_id=1) == 1
    assert [record.group for record in await store.list_all()] == ["b"]
    assert await store.purge_topic(topic_id=1) == 0


async def test_purge_topic_default_implementation_is_noop() -> None:
    """未知存储形态走基类默认实现：无组可清返回 0（清理任务据此继续后续步骤）。"""
    assert await GroupStore.purge_topic(InMemoryGroupStore(), topic_id=1) == 0


@pytest.mark.kiwi_id(2496)
async def test_default_delete_and_mark_idle_dead_are_noops() -> None:
    """01-4-1：未知存储形态走基类默认实现（删除 False、回收 0）。"""
    store = InMemoryGroupStore()
    assert await GroupStore.delete(store, tenant_id=1, topic_id=1, group="audit") is False
    assert await GroupStore.mark_idle_dead(store, before=datetime(2026, 1, 1, tzinfo=UTC)) == 0


@pytest.mark.kiwi_id(2496)
async def test_in_memory_delete_removes_group_and_touch() -> None:
    """01-4-1：内存实现删除命中返回 True，重复删除 False。"""
    store = InMemoryGroupStore()
    await store.save(_record())
    assert await store.delete(tenant_id=TENANT, topic_id=101, group="audit") is True
    assert await store.load(tenant_id=TENANT, topic_id=101, group="audit") is None
    assert await store.delete(tenant_id=TENANT, topic_id=101, group="audit") is False
