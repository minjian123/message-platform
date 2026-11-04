"""顺序性（msg 任务 01-3；Kiwi 2415）。

覆盖（对应详细设计 §4、§9 验收映射）：

- **分区内停等**：未确认队头挡住其后的所有 offset；队头 ack 后才继续投；
- **同 key 有序**：一次拉取按 offset 升序返回，逐条 ack 后位点连续；
- **重投不越队头**：超时重投同一条消息（`message_id` / `offset` 不变、计数递增），其后 offset 不投；
- **死信例外**：转死信后其后消息可继续投；已死信 offset 后续跳过（含位点提交失败后的重试）；
- **acks=0**：按序返回、返回前推进位点，但受 `acks=1` 在途队头约束，不越过未确认队头；
- **位点不跳**：实例失效后从已提交位点续读，未 ack 重投、已 ack 不重投、offset 连续；
- **跨分区**：两分区各自独立推进（不承诺跨分区顺序）。

投递用例沿用 01-2 的口径：**单分区主题**（模块内覆盖 `topic` 夹具），分区选择本身由
`test_topic_router` 覆盖；时间用可推进时钟模拟可见性超时，不 sleep。
"""

import base64
from collections.abc import Callable
from datetime import datetime
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.deadletter import DeadLetter, InMemoryDeadLetterQueue
from bms_msg.broker.delivery import Delivery, DeliveryService, DeliveryState
from bms_msg.broker.log import InMemoryMessageStore, MessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore, OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import ACKS_FIRE_AND_FORGET, DEFAULT_VISIBILITY_TIMEOUT_MS, MAX_DELIVERY_ATTEMPTS
from bms_msg.core.routing import select_partition
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import AckRequest, ConsumeRequest, PublishRequest
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2415)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.ordering"
"""单分区用例主题名（顺序性断言依赖分区号恒 0）。"""

PARTITIONED_TOPIC_NAME = "audit.ordering.parts"
"""两分区用例主题名（验证跨分区独立推进）。"""

GROUP = "ordering"
"""用例消费组。"""

CONSUMER = "c1"
"""用例成员（01-4 起请求须携带成员标识；本层不校验成员，只满足契约）。"""

PAST_TIMEOUT_MS = DEFAULT_VISIBILITY_TIMEOUT_MS + 1
"""刚过可见性超时的推进量。"""

Advance = Callable[[int], None]
"""时钟推进器类型（夹具 `advance`）。"""

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器类型（夹具 `make_request`）。"""


@pytest.fixture
async def topic(service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest) -> MsgTopic:
    """单分区主题（覆盖同名夹具：顺序性用例只走分区 0）。

    Args:
        service: 主题服务。
        current_tenant: 当前租户。
        make_request: 创建请求构造器。

    Returns:
        MsgTopic: 新建主题（一分区）。
    """
    return await service.create_topic(tenant=current_tenant, request=make_request(name=TOPIC_NAME, partition_count=1))


@pytest.fixture
async def partitioned_topic(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> MsgTopic:
    """两分区主题（跨分区独立推进用例）。

    Args:
        service: 主题服务。
        current_tenant: 当前租户。
        make_request: 创建请求构造器。

    Returns:
        MsgTopic: 新建主题（两分区）。
    """
    return await service.create_topic(
        tenant=current_tenant, request=make_request(name=PARTITIONED_TOPIC_NAME, partition_count=2)
    )


def _publish(*, key: str | None = "order-1", payload: bytes = b"payload", topic: str = TOPIC_NAME) -> PublishRequest:
    """构造发布请求（载荷自动 base64）。

    Args:
        key: 分区键（None 走轮询）。
        payload: 原始载荷。
        topic: 主题名。

    Returns:
        PublishRequest: 发布请求。
    """
    return PublishRequest(
        namespace=NAMESPACE,
        topic=topic,
        key=key,
        payload=base64.b64encode(payload).decode("ascii"),
    )


def _consume(
    *,
    group: str = GROUP,
    partition: int = 0,
    max_messages: int = 10,
    acks: int = 1,
    topic: str = TOPIC_NAME,
) -> ConsumeRequest:
    """构造拉取请求。

    Args:
        group: 消费组。
        partition: 分区号。
        max_messages: 拉取条数上限。
        acks: 投递档位（1 / 0）。
        topic: 主题名。

    Returns:
        ConsumeRequest: 拉取请求。
    """
    return ConsumeRequest(
        group=group,
        consumer_id=CONSUMER,
        namespace=NAMESPACE,
        topic=topic,
        partition=partition,
        max_messages=max_messages,
        acks=acks,
    )


def _ack(offset: int, *, group: str = GROUP, partition: int = 0, topic: str = TOPIC_NAME) -> AckRequest:
    """构造确认请求。

    Args:
        offset: 待确认 offset。
        group: 消费组。
        partition: 分区号。
        topic: 主题名。

    Returns:
        AckRequest: 确认请求。
    """
    return AckRequest(
        group=group, consumer_id=CONSUMER, namespace=NAMESPACE, topic=topic, partition=partition, offset=offset
    )


async def _publish_many(
    delivery_service: DeliveryService,
    tenant: CurrentTenant,
    count: int,
    *,
    key: str = "order-1",
    topic: str = TOPIC_NAME,
) -> ConcurrentStableList[StoredMessage]:
    """发布 `count` 条同 key 消息（落同一分区，offset 连续）。

    Args:
        delivery_service: 投递服务。
        tenant: 当前租户。
        count: 条数。
        key: 分区键。
        topic: 主题名。

    Returns:
        ConcurrentStableList[StoredMessage]: 发布的记录。
    """
    published: ConcurrentStableList[StoredMessage] = ConcurrentStableList()
    for _ in range(count):
        published.add(await delivery_service.publish(tenant=tenant, request=_publish(key=key, topic=topic)))
    return published


def _offsets(deliveries: ConcurrentStableList[Delivery]) -> list[int]:
    """取投递批次的 offset 列表（按返回顺序）。

    Args:
        deliveries: 投递批次。

    Returns:
        list[int]: offset 列表。
    """
    return [item.message.offset for item in deliveries]


def _counts(deliveries: ConcurrentStableList[Delivery]) -> list[int]:
    """取投递批次的投递次数列表（按返回顺序）。

    Args:
        deliveries: 投递批次。

    Returns:
        list[int]: `delivery_count` 列表。
    """
    return [item.delivery_count for item in deliveries]


class _FailingOffsetStore(OffsetStore):
    """位点存储替身：可切换提交失败（验证死信时的提交失败与后续重试）。"""

    def __init__(self, inner: OffsetStore) -> None:
        """初始化（缺省不失败）。

        Args:
            inner: 真实实现。
        """
        self._inner = inner
        self.fail = False

    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """转调真实实现。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """
        return await self._inner.get(group=group, topic_id=topic_id, partition_no=partition_no)

    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（`fail` 为真时抛错，模拟存储故障）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点。

        Raises:
            RuntimeError: 模拟提交失败。
        """
        if self.fail:
            raise RuntimeError("位点存储不可用")
        await self._inner.commit(group=group, topic_id=topic_id, partition_no=partition_no, offset=offset)


def _build_service(
    *,
    db_session: AsyncSession,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    state: DeliveryState,
    now: Callable[[], datetime],
) -> DeliveryService:
    """按给定依赖构造投递服务（替身 / 新实例场景用）。

    Args:
        db_session: 请求级会话（主题仓储）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态（新实例即新的空状态，模拟进程重启 / 分区转移）。
        now: 时钟。

    Returns:
        DeliveryService: 投递服务。
    """
    return DeliveryService(
        store=store,
        offsets=offsets,
        dead_letters=dead_letters,
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        state=state,
        now=now,
    )


async def _exhaust_attempts(service: DeliveryService, tenant: CurrentTenant, advance: Advance) -> None:
    """把队头消息投满上限（每次投出后推进时钟，下一次拉取即可重投）。

    Args:
        service: 投递服务。
        tenant: 当前租户。
        advance: 时钟推进器。
    """
    for _ in range(MAX_DELIVERY_ATTEMPTS):
        await service.consume(tenant=tenant, request=_consume(max_messages=1))
        advance(PAST_TIMEOUT_MS)


async def test_same_key_delivers_in_offset_order(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    offset_store: InMemoryOffsetStore,
) -> None:
    """同 key 顺序：一次拉取按 offset 升序返回，逐条 ack 后位点连续推进。"""
    topic_id = int(topic.id)
    published = await _publish_many(delivery_service, current_tenant, 5)
    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=5))
    assert _offsets(delivered) == [0, 1, 2, 3, 4]
    assert [item.message.message_id for item in delivered] == [message.message_id for message in published]

    for offset in range(5):
        assert await delivery_service.ack(tenant=current_tenant, request=_ack(offset)) == offset + 1
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 5


async def test_unacked_head_blocks_following_offsets(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """停等：未确认队头存在时，下一次拉取为空；逐条 ack 后才继续投下一条。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 3)
    first = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert _offsets(first) == [0, 1]

    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))) == []
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1
    # 队头 1 仍在途（未过期）：位点已到 1，但后续 offset 一律不投。
    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))) == []
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(1)) == 2

    rest = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert _offsets(rest) == [2]


async def test_retry_stops_at_unacked_head(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
) -> None:
    """重投不越队头：队头超时后重投同一条（计数递增），其后 offset 仍不投。"""
    del topic
    published = await _publish_many(delivery_service, current_tenant, 3)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))

    advance(PAST_TIMEOUT_MS)
    again = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=3))
    assert _offsets(again) == [0, 1]
    assert _counts(again) == [2, 2]
    assert [item.message.message_id for item in again] == [
        published[0].message_id,
        published[1].message_id,
    ]


async def test_ack_after_retry_continues(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
) -> None:
    """重投后仍需逐条 ack：队头未确认时后续 offset 不投，ack 后才继续。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 3)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    advance(PAST_TIMEOUT_MS)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))

    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1
    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))) == []
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(1)) == 2

    rest = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert _offsets(rest) == [2]


async def test_dead_letter_allows_following_message(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
    dead_letters: InMemoryDeadLetterQueue,
    offset_store: InMemoryOffsetStore,
) -> None:
    """死信例外：毒消息转死信后位点前移，其后消息可继续投（不堵死分区）。"""
    topic_id = int(topic.id)
    await _publish_many(delivery_service, current_tenant, 2)
    counts: list[int] = []
    for _ in range(MAX_DELIVERY_ATTEMPTS):
        delivered = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=1))
        counts.append(delivered[0].delivery_count)
        advance(PAST_TIMEOUT_MS)
    assert counts == [1, 2, 3, 4, 5]

    after = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    assert _offsets(after) == [1]
    letters: ConcurrentStableList[DeadLetter] = await dead_letters.list_all()
    assert [(item.offset, item.attempts) for item in letters] == [(0, MAX_DELIVERY_ATTEMPTS)]
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 1


async def test_dead_letter_commit_failure_allows_later_ack(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
) -> None:
    """死信时位点提交失败：后续拉取跳过已死信 offset（不重投），ack 补齐缺口后位点成段越过。"""
    failing = _FailingOffsetStore(offset_store)
    service = _build_service(
        db_session=db_session,
        store=message_store,
        offsets=failing,
        dead_letters=dead_letters,
        state=delivery_state,
        now=clock,
    )
    await _publish_many(service, current_tenant, 2)
    await _exhaust_attempts(service, current_tenant, advance)
    failing.fail = True

    with pytest.raises(RuntimeError):
        await service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    failing.fail = False

    after = await service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    assert _offsets(after) == [1]
    # 跳过已死信 0 时补记了暂存；ack 1 后连续前缀直接越过 0 到 2。
    assert await service.ack(tenant=current_tenant, request=_ack(1)) == 2
    assert list(await service.consume(tenant=current_tenant, request=_consume(max_messages=1))) == []


async def test_acks_zero_skips_dead_letter_and_advances(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
) -> None:
    """`acks=0` 跳过已死信 offset 并推进位点（死信是终结，不算未确认缺口）。"""
    failing = _FailingOffsetStore(offset_store)
    service = _build_service(
        db_session=db_session,
        store=message_store,
        offsets=failing,
        dead_letters=dead_letters,
        state=delivery_state,
        now=clock,
    )
    await _publish_many(service, current_tenant, 2)
    await _exhaust_attempts(service, current_tenant, advance)
    failing.fail = True

    with pytest.raises(RuntimeError):
        await service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    failing.fail = False

    delivered = await service.consume(
        tenant=current_tenant, request=_consume(max_messages=1, acks=ACKS_FIRE_AND_FORGET)
    )
    assert _offsets(delivered) == [1]
    topic_id = int(topic.id)
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 2


async def test_consumer_failure_resumes_without_skipping(
    delivery_service: DeliveryService,
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """位点不跳：实例失效后从已提交位点续读，未 ack 重投、已 ack 不重投、offset 连续。"""
    topic_id = int(topic.id)
    await _publish_many(delivery_service, current_tenant, 3)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=3))
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1

    resumed_service = _build_service(
        db_session=db_session,
        store=message_store,
        offsets=offset_store,
        dead_letters=dead_letters,
        state=DeliveryState(),
        now=clock,
    )
    resumed = await resumed_service.consume(tenant=current_tenant, request=_consume(max_messages=3))
    assert _offsets(resumed) == [1, 2]
    assert _counts(resumed) == [1, 1]
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 1

    assert await resumed_service.ack(tenant=current_tenant, request=_ack(1)) == 2
    assert await resumed_service.ack(tenant=current_tenant, request=_ack(2)) == 3


async def test_partitions_advance_independently(
    delivery_service: DeliveryService,
    partitioned_topic: MsgTopic,
    current_tenant: CurrentTenant,
    offset_store: InMemoryOffsetStore,
) -> None:
    """跨分区：两分区各自从 0 起、独立推进（不承诺跨分区顺序）。"""
    topic_id = int(partitioned_topic.id)
    key_zero = next(key for key in (f"k{index}" for index in range(100)) if select_partition(key, 2) == 0)
    key_one = next(key for key in (f"k{index}" for index in range(100)) if select_partition(key, 2) == 1)
    await _publish_many(delivery_service, current_tenant, 2, key=key_zero, topic=PARTITIONED_TOPIC_NAME)
    await _publish_many(delivery_service, current_tenant, 2, key=key_one, topic=PARTITIONED_TOPIC_NAME)

    first = await delivery_service.consume(tenant=current_tenant, request=_consume(topic=PARTITIONED_TOPIC_NAME))
    second = await delivery_service.consume(
        tenant=current_tenant, request=_consume(partition=1, topic=PARTITIONED_TOPIC_NAME)
    )
    assert _offsets(first) == [0, 1]
    assert _offsets(second) == [0, 1]

    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0, topic=PARTITIONED_TOPIC_NAME)) == 1
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 1
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=1) == 0
