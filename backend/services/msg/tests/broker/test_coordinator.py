"""消费组协议编排（msg 任务 01-4；Kiwi 2416）。

覆盖：建组与首轮分配、成员变化重算、重连不抖、空闲成员、心跳刷新与超时清理、离开（有他人 / 最后一人 /
未注册幂等）、撤销清在途（位点不动）、扩分区触发重算、分区归属校验。

时钟一律用 `ManualClock` 推进（不 sleep）：会话超时 30s，用例以「29 999 + 1ms」精确踩边界。
（`services/msg/tests` 不是包，故常量与替身类型在本文件内定义，不从 `conftest` 导入。）
"""

from collections.abc import Callable
from datetime import datetime
from typing import Protocol, cast

import pytest
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.coordinator import GroupCoordinator, MemberState
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.group import GROUP_STATE_EMPTY, GROUP_STATE_STABLE
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import SESSION_TIMEOUT_MS
from bms_msg.errors import MsgGroupNotFoundError, MsgMemberInvalidError, MsgPartitionCountInvalidError
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.group import MemberRequest
from bms_msg.schemas.message import ConsumeRequest, PublishRequest
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2416)

GROUP = "audit"
"""测试消费组名。"""

NAMESPACE = "msg"
"""可用命名空间。"""

TOPIC = "audit.operation"
"""两分区主题名（缺省夹具）。"""

SINGLE = "audit.single"
"""单分区主题名（空闲成员与撤销用例用）。"""

MakeMember = Callable[..., MemberRequest]
"""成员请求构造器类型。"""

MakeTopic = Callable[..., TopicCreateRequest]
"""主题创建请求构造器类型。"""


class Clock(Protocol):
    """可推进时钟（`conftest.ManualClock` 的测试侧协议）。"""

    def __call__(self) -> datetime:
        """当前时间。

        Returns:
            datetime: 当前（可推进的）UTC 时间。
        """
        ...

    def advance(self, milliseconds: int) -> None:
        """推进时钟。

        Args:
            milliseconds: 推进的毫秒数。
        """
        ...


async def _create(
    service: TopicService, tenant: CurrentTenant, make_request: MakeTopic, *, name: str, partitions: int
) -> MsgTopic:
    """建一个指定分区数的主题。

    Args:
        service: 主题服务（同会话仓储）。
        tenant: 当前租户。
        make_request: 创建请求构造器。
        name: 主题名。
        partitions: 分区数。

    Returns:
        MsgTopic: 新建主题。
    """
    return await service.create_topic(tenant=tenant, request=make_request(name=name, partition_count=partitions))


async def _join(
    coordinator: GroupCoordinator,
    tenant: CurrentTenant,
    make_member: MakeMember,
    *,
    consumer_id: str,
    topic: str = TOPIC,
) -> MemberState:
    """加入组。

    Args:
        coordinator: 协调器。
        tenant: 当前租户。
        make_member: 成员请求构造器。
        consumer_id: 成员标识。
        topic: 主题名。

    Returns:
        MemberState: 组成员状态。
    """
    return await coordinator.join(tenant=tenant, request=make_member(consumer_id=consumer_id, topic=topic))


async def test_join_creates_group_and_assigns(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """首成员加入：建组 + 首轮分配（世代 1、状态 stable、分到全部分区）。"""
    del topic
    state = await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    assert (state.group.state, state.group.generation) == (GROUP_STATE_STABLE, 1)
    assert list(state.member.partitions) == [0, 1]
    assert len(state.group.members) == 1


async def test_second_member_triggers_rebalance(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """第二成员加入：世代 +1，两人分区不重叠且并集为全集（组内不重复分配）。"""
    del topic
    first = await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    second = await _join(coordinator, current_tenant, make_member, consumer_id="c2")
    assert first.group.generation == 1
    assert (second.group.generation, second.group.state) == (2, GROUP_STATE_STABLE)
    owned = {member.consumer_id: list(member.partitions) for member in second.group.members}
    assert owned == {"c1": [0], "c2": [1]}


async def test_rejoin_same_consumer_is_reconnect(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """同 `consumer_id` 再 join：重连（世代不变、分配不变、成员不重复）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    again = await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    assert (again.group.generation, len(again.group.members)) == (1, 1)
    assert list(again.member.partitions) == [0, 1]


async def test_more_members_than_partitions_keeps_idle(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    service: TopicService,
    make_request: MakeTopic,
) -> None:
    """成员数 > 分区数：多余成员分配为空、状态仍 stable、不报错（需求 01-4 内容 3）。"""
    await _create(service, current_tenant, make_request, name=SINGLE, partitions=1)
    await _join(coordinator, current_tenant, make_member, consumer_id="c1", topic=SINGLE)
    idle = await _join(coordinator, current_tenant, make_member, consumer_id="c2", topic=SINGLE)
    assert idle.group.state == GROUP_STATE_STABLE
    assert list(idle.member.partitions) == []
    assert [list(member.partitions) for member in idle.group.members] == [[0], []]


async def test_idle_member_join_keeps_owner(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    service: TopicService,
    make_request: MakeTopic,
) -> None:
    """空闲成员加入不改变分区归属（无易主分区时不撤销任何在途）。"""
    await _create(service, current_tenant, make_request, name=SINGLE, partitions=1)
    await _join(coordinator, current_tenant, make_member, consumer_id="c1", topic=SINGLE)
    after = await _join(coordinator, current_tenant, make_member, consumer_id="c2", topic=SINGLE)
    assert after.group.owner_of(0) == "c1"


async def test_heartbeat_refreshes_last_seen(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    clock: Clock,
    topic: MsgTopic,
) -> None:
    """心跳刷新存活：分段推进 40s（每段 20s）仍活跃，不因处理慢被误判死亡。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    request = make_member(consumer_id="c1")
    clock.advance(20_000)
    first = await coordinator.heartbeat(tenant=current_tenant, request=request)
    clock.advance(20_000)
    second = await coordinator.heartbeat(tenant=current_tenant, request=request)
    assert (first.group.generation, second.group.generation) == (1, 1)
    assert list(second.member.partitions) == [0, 1]


async def test_heartbeat_reports_member_timeout(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    clock: Clock,
    topic: MsgTopic,
) -> None:
    """超时清理：静默成员达会话超时即被清理，接管者世代 +1 并拿到全部分区。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    await _join(coordinator, current_tenant, make_member, consumer_id="c2")
    request = make_member(consumer_id="c1")
    clock.advance(SESSION_TIMEOUT_MS - 1)
    await coordinator.heartbeat(tenant=current_tenant, request=request)
    clock.advance(1)
    after = await coordinator.heartbeat(tenant=current_tenant, request=request)
    assert (after.group.generation, len(after.group.members)) == (3, 1)
    assert list(after.member.partitions) == [0, 1]


async def test_heartbeat_unknown_group_returns_350013(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """组不存在：心跳 `350013`（组只由 join 创建）。"""
    del topic
    with pytest.raises(MsgGroupNotFoundError):
        await coordinator.heartbeat(tenant=current_tenant, request=make_member(consumer_id="c1"))


async def test_heartbeat_unknown_member_returns_350014(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """成员未加入：心跳 `350014`（须重新 join）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    with pytest.raises(MsgMemberInvalidError):
        await coordinator.heartbeat(tenant=current_tenant, request=make_member(consumer_id="c2"))


async def test_heartbeat_after_timeout_returns_350014(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    clock: Clock,
    topic: MsgTopic,
) -> None:
    """自己超时后心跳：先被清理再判定，返回 `350014`（客户端须重新 join）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    clock.advance(SESSION_TIMEOUT_MS)
    with pytest.raises(MsgMemberInvalidError):
        await coordinator.heartbeat(tenant=current_tenant, request=make_member(consumer_id="c1"))


async def test_leave_rebalances_remaining_members(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """成员离开：世代 +1，剩下者接管全部分区。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    await _join(coordinator, current_tenant, make_member, consumer_id="c2")
    after = await coordinator.leave(tenant=current_tenant, request=make_member(consumer_id="c1"))
    assert (after.generation, len(after.members)) == (3, 1)
    assert list(after.members[0].partitions) == [0, 1]


async def test_leave_last_member_marks_empty(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """最后一名成员离开：组转 `empty`（记录与位点保留）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    after = await coordinator.leave(tenant=current_tenant, request=make_member(consumer_id="c1"))
    assert (after.state, after.generation, len(after.members)) == (GROUP_STATE_EMPTY, 2, 0)


async def test_leave_unknown_member_is_idempotent(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """未注册成员离开：幂等返回（世代不变、成员仍在），网络重试友好。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    after = await coordinator.leave(tenant=current_tenant, request=make_member(consumer_id="c9"))
    assert (after.generation, len(after.members)) == (1, 1)


async def test_rejoin_after_peer_timeout_rebalances(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    clock: Clock,
    topic: MsgTopic,
) -> None:
    """重连接手：同伴超时被清理时，重连的成员不递增世代只刷新存活；清理发生后同一 join 触发重算。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    await _join(coordinator, current_tenant, make_member, consumer_id="c2")
    clock.advance(SESSION_TIMEOUT_MS - 1)
    kept = await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    assert (kept.group.generation, len(kept.group.members)) == (2, 2)
    clock.advance(1)
    takeover = await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    assert (takeover.group.generation, list(takeover.member.partitions)) == (3, [0, 1])


async def test_leave_unknown_group_returns_350013(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """组不存在：离开 `350013`。"""
    del topic
    with pytest.raises(MsgGroupNotFoundError):
        await coordinator.leave(tenant=current_tenant, request=make_member(consumer_id="c1"))


async def test_expand_partitions_rebalances_on_heartbeat(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    service: TopicService,
    topic: MsgTopic,
    db_session: AsyncSession,
) -> None:
    """扩分区：心跳检测到分配未覆盖全部分区 → 重算（新分区不落空）。"""
    topic_id = int(topic.id)  # 回滚会让 ORM 属性过期，先把主键取到局部量
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    # 主题解析留下的隐式事务先结束，`expand_partitions()` 才能在干净事务上 begin。
    await db_session.rollback()
    await service.expand_partitions(tenant=current_tenant, topic_id=topic_id, partition_count=4)
    after = await coordinator.heartbeat(tenant=current_tenant, request=make_member(consumer_id="c1"))
    assert (after.group.generation, list(after.member.partitions)) == (2, [0, 1, 2, 3])


async def test_ensure_owner_accepts_assigned_partition(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """归属校验通过：成员持有时可消费该分区（无异常即通过）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    await coordinator.ensure_owner(
        tenant=current_tenant, namespace=NAMESPACE, topic=TOPIC, group=GROUP, consumer_id="c1", partition=1
    )


async def test_ensure_owner_rejects_foreign_partition(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """分区不在该成员分配内：`350014`（组内不重复分配的运行期约束）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    await _join(coordinator, current_tenant, make_member, consumer_id="c2")
    with pytest.raises(MsgMemberInvalidError):
        await coordinator.ensure_owner(
            tenant=current_tenant, namespace=NAMESPACE, topic=TOPIC, group=GROUP, consumer_id="c1", partition=1
        )


async def test_ensure_owner_rejects_expired_member(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    clock: Clock,
    topic: MsgTopic,
) -> None:
    """成员已超时（尚未被清理）：`350014`（死连接不能继续拉取）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    clock.advance(SESSION_TIMEOUT_MS)
    with pytest.raises(MsgMemberInvalidError):
        await coordinator.ensure_owner(
            tenant=current_tenant, namespace=NAMESPACE, topic=TOPIC, group=GROUP, consumer_id="c1", partition=0
        )


async def test_ensure_owner_unknown_group_returns_350013(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """组不存在：`350013`（拼错组名不再静默产生新位点）。"""
    del topic
    with pytest.raises(MsgGroupNotFoundError):
        await coordinator.ensure_owner(
            tenant=current_tenant, namespace=NAMESPACE, topic=TOPIC, group=GROUP, consumer_id="c1", partition=0
        )


async def test_ensure_owner_partition_out_of_range_returns_350009(
    coordinator: GroupCoordinator, current_tenant: CurrentTenant, make_member: MakeMember, topic: MsgTopic
) -> None:
    """分区号越界先于归属判定：`350009`（判定顺序：组 → 分区号 → 成员）。"""
    del topic
    await _join(coordinator, current_tenant, make_member, consumer_id="c1")
    with pytest.raises(MsgPartitionCountInvalidError):
        await coordinator.ensure_owner(
            tenant=current_tenant, namespace=NAMESPACE, topic=TOPIC, group=GROUP, consumer_id="c1", partition=9
        )


async def test_rebalance_drops_inflight_of_moved_partition(
    coordinator: GroupCoordinator,
    current_tenant: CurrentTenant,
    make_member: MakeMember,
    service: TopicService,
    make_request: MakeTopic,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: Clock,
    db_session: object,
) -> None:
    """分区易主：撤销清在途、位点不动 → 新 owner 立即可重投（计数从 1 重计，不必等可见性超时）。"""
    subject = await _create(service, current_tenant, make_request, name=SINGLE, partitions=1)
    delivery = DeliveryService(
        store=message_store,
        offsets=offset_store,
        dead_letters=dead_letters,
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        state=delivery_state,
        now=clock,
    )
    await delivery.publish(
        tenant=current_tenant,
        request=PublishRequest(namespace=NAMESPACE, topic=SINGLE, key=None, payload="", trace_id=None),
    )
    await _join(coordinator, current_tenant, make_member, consumer_id="c1", topic=SINGLE)
    first = await delivery.consume(
        tenant=current_tenant,
        request=ConsumeRequest(
            group=GROUP, consumer_id="c1", namespace=NAMESPACE, topic=SINGLE, partition=0, max_messages=1
        ),
    )
    assert [item.message.offset for item in first] == [0]

    await coordinator.leave(tenant=current_tenant, request=make_member(consumer_id="c1", topic=SINGLE))
    await _join(coordinator, current_tenant, make_member, consumer_id="c2", topic=SINGLE)
    second = await delivery.consume(
        tenant=current_tenant,
        request=ConsumeRequest(
            group=GROUP, consumer_id="c2", namespace=NAMESPACE, topic=SINGLE, partition=0, max_messages=1
        ),
    )
    assert [item.message.offset for item in second] == [0]
    assert [item.delivery_count for item in second] == [1]
    assert await offset_store.get(group=GROUP, topic_id=int(subject.id), partition_no=0) == 0
