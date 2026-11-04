"""投递语义（msg 任务 01-2；Kiwi 2414）。

覆盖（对应详细设计 §6、§11 验收映射）：

- **至少一次**：首次投递 `delivery_count=1`；可见性超时（30s）内不重投；超时后重投且计数递增、
  `message_id` / `offset` / `payload` 不变；
- **acks 两档**：`acks=1` 由 ack 推进位点；`acks=0` 返回前直接前移、不跟踪在途、不重投，
  且位点不得越过未确认消息（缺口场景）；
- **位点规则**：提交值 = 连续已确认最大 offset + 1；乱序 ack 成段推进；重复 ack 幂等；
  提交失败位点不前移且可重试；越界 `350002`；未投递 / 已被保留期清理 `350003`；
- **毒消息**：重投 5 次后转死信、位点前移、死信后 ack `350007`；
- **发布**：分区选择复用 `core/routing`（同 key 同分区）、载荷 base64 解码、命名空间与主题校验。
"""

import base64
from collections.abc import Callable
from datetime import datetime
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.deadletter import DeadLetter, DeadLetterQueue, InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.log import InMemoryMessageStore, MessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore, OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import ACKS_FIRE_AND_FORGET, DEFAULT_VISIBILITY_TIMEOUT_MS, MAX_DELIVERY_ATTEMPTS
from bms_msg.errors import (
    MsgDeliveryExhaustedError,
    MsgErrorCode,
    MsgMessageNotFoundError,
    MsgNamespaceInvalidError,
    MsgOffsetInvalidError,
    MsgPartitionCountInvalidError,
    MsgTopicNotFoundError,
)
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import AckRequest, ConsumeRequest, PublishRequest
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2414)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.delivery"
"""用例主题名（本模块覆盖 `topic` 夹具，建**单分区**主题，分区号恒 0 便于断言 offset 序列）。"""

GROUP = "audit"
"""用例消费组。"""

PAST_TIMEOUT_MS = DEFAULT_VISIBILITY_TIMEOUT_MS + 1
"""刚过可见性超时的推进量。"""

Advance = Callable[[int], None]
"""时钟推进器类型（夹具 `advance`）。"""

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器类型（夹具 `make_request`）。"""


@pytest.fixture
async def topic(service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest) -> MsgTopic:
    """单分区主题（覆盖同名夹具：投递用例只走分区 0，分区选择由 `test_topic_router` 覆盖）。

    Args:
        service: 主题服务。
        current_tenant: 当前租户。
        make_request: 创建请求构造器。

    Returns:
        MsgTopic: 新建主题（一分区）。
    """
    return await service.create_topic(tenant=current_tenant, request=make_request(name=TOPIC_NAME, partition_count=1))


def _publish(
    key: str | None = "order-1", *, payload: bytes = b"payload", topic: str = TOPIC_NAME, namespace: str = NAMESPACE
) -> PublishRequest:
    """构造发布请求（载荷自动 base64）。

    Args:
        key: 分区键（None 走轮询）。
        payload: 原始载荷。
        topic: 主题名。
        namespace: 命名空间。

    Returns:
        PublishRequest: 发布请求。
    """
    return PublishRequest(
        namespace=namespace,
        topic=topic,
        key=key,
        payload=base64.b64encode(payload).decode("ascii"),
    )


def _consume(*, group: str = GROUP, partition: int = 0, max_messages: int = 10, acks: int = 1) -> ConsumeRequest:
    """构造拉取请求。

    Args:
        group: 消费组。
        partition: 分区号。
        max_messages: 拉取条数上限。
        acks: 投递档位（1 / 0）。

    Returns:
        ConsumeRequest: 拉取请求。
    """
    return ConsumeRequest(
        group=group,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition=partition,
        max_messages=max_messages,
        acks=acks,
    )


def _ack(offset: int, *, group: str = GROUP, partition: int = 0) -> AckRequest:
    """构造确认请求。

    Args:
        offset: 待确认 offset。
        group: 消费组。
        partition: 分区号。

    Returns:
        AckRequest: 确认请求。
    """
    return AckRequest(group=group, namespace=NAMESPACE, topic=TOPIC_NAME, partition=partition, offset=offset)


async def _publish_many(
    delivery_service: DeliveryService, tenant: CurrentTenant, count: int
) -> ConcurrentStableList[StoredMessage]:
    """发布 `count` 条同 key 消息（落同一分区）。

    Args:
        delivery_service: 投递服务。
        tenant: 当前租户。
        count: 条数。

    Returns:
        ConcurrentStableList[StoredMessage]: 发布的记录。
    """
    published: ConcurrentStableList[StoredMessage] = ConcurrentStableList()
    for _ in range(count):
        published.add(await delivery_service.publish(tenant=tenant, request=_publish()))
    return published


class _FailingOffsetStore(OffsetStore):
    """位点存储替身：可切换提交失败（验证「提交失败不得前移」）。"""

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


class _RetainedStore(MessageStore):
    """消息存储替身：把日志起点抬到指定值（模拟保留期清理，`start_offset > 0`）。"""

    def __init__(self, inner: MessageStore, *, start: int) -> None:
        """初始化。

        Args:
            inner: 真实实现。
            start: 伪装的日志起点。
        """
        self._inner = inner
        self._start = start

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
    ) -> StoredMessage:
        """转调真实实现（签名同端口）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 消息 ID。
            key: 分区键。
            payload: 载荷。
            headers: 结构化头。
            trace_id: 链路标识。
            published_at: 发布时间。

        Returns:
            StoredMessage: 追加后的记录。
        """
        return await self._inner.append(
            topic_id=topic_id,
            partition_no=partition_no,
            message_id=message_id,
            key=key,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            published_at=published_at,
        )

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 消息批次。
        """
        return await self._inner.fetch(
            topic_id=topic_id, partition_no=partition_no, from_offset=from_offset, max_messages=max_messages
        )

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO。
        """
        return await self._inner.leo(topic_id=topic_id, partition_no=partition_no)

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """返回伪装的日志起点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 伪装的起点。
        """
        del topic_id, partition_no
        return self._start


def _build_service(
    *,
    db_session: AsyncSession,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    now: Callable[[], datetime],
) -> DeliveryService:
    """按给定依赖构造投递服务（替身场景用，避免改动夹具）。

    Args:
        db_session: 请求级会话（主题仓储）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
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


async def test_publish_assigns_partition_and_offset(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """发布：同 key 恒同分区、offset 连续、`message_id` 为不重复的十进制字符串。"""
    del topic
    first = await delivery_service.publish(tenant=current_tenant, request=_publish())
    second = await delivery_service.publish(tenant=current_tenant, request=_publish())
    assert (first.offset, second.offset) == (0, 1)
    assert first.partition_no == second.partition_no
    assert first.message_id != second.message_id
    assert first.message_id.isdigit()
    assert first.payload == b"payload"


async def test_publish_accepts_empty_payload(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """空载荷合法（空消息体是有效事件）。"""
    del topic
    message = await delivery_service.publish(tenant=current_tenant, request=_publish(payload=b""))
    assert message.payload == b""


async def test_publish_rejects_invalid_base64(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """非法 base64：参数错误 `10001`（形态属契约，不是业务码）。"""
    del topic
    request = PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, payload="not-base64!!")
    with pytest.raises(ParamError):
        await delivery_service.publish(tenant=current_tenant, request=request)


async def test_publish_rejects_unregistered_namespace(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """命名空间未登记：`350010`。"""
    del topic
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        await delivery_service.publish(tenant=current_tenant, request=_publish(namespace="nope"))
    assert excinfo.value.code == MsgErrorCode.NAMESPACE_INVALID


async def test_publish_rejects_missing_topic(delivery_service: DeliveryService, current_tenant: CurrentTenant) -> None:
    """主题不存在（未建）：`350001`。"""
    with pytest.raises(MsgTopicNotFoundError):
        await delivery_service.publish(tenant=current_tenant, request=_publish())


async def test_consume_delivers_first_time(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """首次投递：`delivery_count=1`，偏移与载荷原样返回。"""
    del topic
    published = await delivery_service.publish(tenant=current_tenant, request=_publish())
    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert len(delivered) == 1
    item = delivered[0]
    assert (item.delivery_count, item.message.offset, item.message.message_id) == (
        1,
        published.offset,
        published.message_id,
    )
    assert item.message.payload == b"payload"


async def test_consume_returns_empty_without_messages(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """空分区（位点已到 LEO）返回空批次。"""
    del topic
    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume())) == []


async def test_consume_skips_inflight_until_timeout(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant, advance: Advance
) -> None:
    """可见性窗口内不重投；刚过超时重投且 `delivery_count` 递增、`message_id` 不变。"""
    del topic
    published = await delivery_service.publish(tenant=current_tenant, request=_publish())
    first = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert [item.delivery_count for item in first] == [1]

    advance(1)
    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume())) == []

    advance(PAST_TIMEOUT_MS)
    again = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert [(item.delivery_count, item.message.message_id) for item in again] == [(2, published.message_id)]
    assert again[0].message.offset == published.offset


async def test_ack_advances_committed_offset(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """确认后位点前移（提交值 = 已处理完的最后一条 + 1）。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 2)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(1)) == 2


async def test_ack_out_of_order_advances_when_gap_filled(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """乱序确认：缺口未补齐时位点不动，补齐后成段推进。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 3)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=3))
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(1)) == 0
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(2)) == 0
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 3


async def test_ack_prune_keeps_offsets_beyond_committed(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """成段推进后，超过新位点的已确认记录保留，等缺口补齐再一次并入。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 4)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=4))
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(1)) == 0
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(3)) == 0
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 2
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(2)) == 4


async def test_ack_is_idempotent(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """重复确认同一 offset：幂等返回已提交位点（不报错、不回退）。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 1)
    await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1
    assert await delivery_service.ack(tenant=current_tenant, request=_ack(0)) == 1


async def test_ack_commit_failure_keeps_offset_and_allows_retry(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """位点提交失败：异常冒泡、位点不前移、在途记录保留（可重试成功）。"""
    topic_id = int(topic.id)
    failing = _FailingOffsetStore(offset_store)
    service = _build_service(
        db_session=db_session,
        store=message_store,
        offsets=failing,
        dead_letters=dead_letters,
        state=delivery_state,
        now=clock,
    )
    await service.publish(tenant=current_tenant, request=_publish())
    await service.consume(tenant=current_tenant, request=_consume())

    failing.fail = True
    with pytest.raises(RuntimeError):
        await service.ack(tenant=current_tenant, request=_ack(0))
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 0

    failing.fail = False
    assert await service.ack(tenant=current_tenant, request=_ack(0)) == 1


async def test_ack_rejects_offset_beyond_leo(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """位点越界（`>= LEO`）：`350002`。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 1)
    await delivery_service.consume(tenant=current_tenant, request=_consume())
    with pytest.raises(MsgOffsetInvalidError) as excinfo:
        await delivery_service.ack(tenant=current_tenant, request=_ack(9))
    assert excinfo.value.code == MsgErrorCode.OFFSET_INVALID


async def test_ack_rejects_undelivered_offset(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """该 offset 不在投递中（未投递过）：`350003`。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 2)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    with pytest.raises(MsgMessageNotFoundError) as excinfo:
        await delivery_service.ack(tenant=current_tenant, request=_ack(1))
    assert excinfo.value.code == MsgErrorCode.MESSAGE_NOT_FOUND


async def test_consume_rejects_partition_out_of_range(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """分区号越界：`350009`。"""
    del topic
    with pytest.raises(MsgPartitionCountInvalidError):
        await delivery_service.consume(tenant=current_tenant, request=_consume(partition=2))


async def test_consume_reports_retention_gap(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """位点早于保留起点（消息已被清理）：`350003`。"""
    del topic
    service = _build_service(
        db_session=db_session,
        store=_RetainedStore(message_store, start=5),
        offsets=offset_store,
        dead_letters=dead_letters,
        state=delivery_state,
        now=clock,
    )
    with pytest.raises(MsgMessageNotFoundError) as excinfo:
        await service.consume(tenant=current_tenant, request=_consume())
    assert excinfo.value.code == MsgErrorCode.MESSAGE_NOT_FOUND


async def test_acks_zero_commits_before_return(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    offset_store: InMemoryOffsetStore,
) -> None:
    """`acks=0`：返回前位点直接前移，且不再重投（可能丢，属该档语义）。"""
    topic_id = int(topic.id)
    await _publish_many(delivery_service, current_tenant, 2)
    delivered = await delivery_service.consume(
        tenant=current_tenant, request=_consume(max_messages=2, acks=ACKS_FIRE_AND_FORGET)
    )
    assert [item.delivery_count for item in delivered] == [1, 1]
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 2
    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume())) == []


async def test_acks_zero_does_not_jump_unacked_gap(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    offset_store: InMemoryOffsetStore,
) -> None:
    """`acks=1` 的在途队头挡住扫描：`acks=0` 也不越过它、位点不前进（顺序性停等，不静默丢弃）。

    01-3 起该用例的语义由「不越过缺口后继续投」升级为「不越过未确认队头」：停等是分区扫描规则，
    与 `acks` 档位无关；`acks=0` 自己不登记在途（不阻塞后续），但仍受他人在途约束。
    """
    topic_id = int(topic.id)
    await _publish_many(delivery_service, current_tenant, 2)
    await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=1))
    delivered = await delivery_service.consume(
        tenant=current_tenant, request=_consume(max_messages=2, acks=ACKS_FIRE_AND_FORGET)
    )
    assert list(delivered) == []
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 0


async def test_dead_letter_after_max_attempts(
    delivery_service: DeliveryService,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
    dead_letters: InMemoryDeadLetterQueue,
    offset_store: InMemoryOffsetStore,
) -> None:
    """重投达上限：转死信、位点前移（不阻塞后续）、死信后 ack 报 `350007`。"""
    topic_id = int(topic.id)
    await _publish_many(delivery_service, current_tenant, 1)
    counts: list[int] = []
    for _ in range(MAX_DELIVERY_ATTEMPTS):
        delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
        counts.append(delivered[0].delivery_count)
        advance(PAST_TIMEOUT_MS)
    assert counts == [1, 2, 3, 4, 5]

    assert list(await delivery_service.consume(tenant=current_tenant, request=_consume())) == []
    letters: ConcurrentStableList[DeadLetter] = await dead_letters.list_all()
    assert [(item.offset, item.attempts) for item in letters] == [(0, MAX_DELIVERY_ATTEMPTS)]
    assert await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=0) == 1
    with pytest.raises(MsgDeliveryExhaustedError) as excinfo:
        await delivery_service.ack(tenant=current_tenant, request=_ack(0))
    assert excinfo.value.code == MsgErrorCode.DELIVERY_EXHAUSTED


async def test_consume_batches_up_to_max_messages(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """单次拉取不超过 `max_messages`，下次从位点续读。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 3)
    first = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert [item.message.offset for item in first] == [0, 1]
    await delivery_service.ack(tenant=current_tenant, request=_ack(0))
    await delivery_service.ack(tenant=current_tenant, request=_ack(1))
    second = await delivery_service.consume(tenant=current_tenant, request=_consume(max_messages=2))
    assert [item.message.offset for item in second] == [2]


async def test_delivery_count_isolated_per_group(
    delivery_service: DeliveryService, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """投递计数按组隔离：A 组重投不影响 B 组的首次投递。"""
    del topic
    await _publish_many(delivery_service, current_tenant, 1)
    await delivery_service.consume(tenant=current_tenant, request=_consume(group="audit"))
    other = await delivery_service.consume(tenant=current_tenant, request=_consume(group="notify"))
    assert [item.delivery_count for item in other] == [1]
