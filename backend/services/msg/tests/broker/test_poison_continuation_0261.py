"""F10 毒消息转死信后同分区续消费（02-6-1 / Kiwi 2498）。

先证明行为：毒消息重投到上限转死信后，同分区后续正常消息可消费；重建服务（复跑）不重复投递、
不重复入死信，已死信 offset 的 ack 返回 `350007`。
"""

from __future__ import annotations

import base64
from typing import Any, cast

import pytest
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import DEFAULT_VISIBILITY_TIMEOUT_MS, MAX_DELIVERY_ATTEMPTS
from bms_msg.core.routing import select_partition
from bms_msg.errors import MsgDeliveryExhaustedError
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import AckRequest, ConsumeRequest, PublishRequest
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2498)

NAMESPACE = "msg"
TOPIC = "audit.operation"
GROUP = "audit"
KEY = "poison-key"
PARTITION = select_partition(KEY, 2)


def _publish_request(payload: bytes) -> PublishRequest:
    return PublishRequest(
        namespace=NAMESPACE,
        topic=TOPIC,
        key=KEY,
        payload=base64.b64encode(payload).decode("ascii"),
    )


def _consume() -> ConsumeRequest:
    return ConsumeRequest(
        group=GROUP,
        consumer_id="c1",
        namespace=NAMESPACE,
        topic=TOPIC,
        partition=PARTITION,
        max_messages=1,
    )


def _ack(offset: int) -> AckRequest:
    return AckRequest(
        group=GROUP,
        consumer_id="c1",
        namespace=NAMESPACE,
        topic=TOPIC,
        partition=PARTITION,
        offset=offset,
    )


def _rebuild(
    *,
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    clock: Any,
) -> DeliveryService:
    return DeliveryService(
        store=message_store,
        offsets=offset_store,
        dead_letters=dead_letters,
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        state=DeliveryState(),
        now=clock,
    )


async def test_poison_dead_letter_then_same_partition_continues(
    delivery_service: DeliveryService,
    topic: object,
    current_tenant: CurrentTenant,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    db_session: AsyncSession,
    clock: Any,
    advance: Any,
) -> None:
    """毒消息转死信后，同分区正常消息可消费；复跑不重复。"""
    del topic
    await delivery_service.publish(tenant=current_tenant, request=_publish_request(b"poison"))
    await delivery_service.publish(tenant=current_tenant, request=_publish_request(b"good"))

    counts: list[int] = []
    for _ in range(MAX_DELIVERY_ATTEMPTS):
        delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
        assert [item.message.offset for item in delivered] == [0]
        counts.append(delivered[0].delivery_count)
        advance(DEFAULT_VISIBILITY_TIMEOUT_MS + 1)
    assert counts == [1, 2, 3, 4, 5]

    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    if not delivered:
        # 死信与后续新消息允许分两批（01-3 的「重试不与主链路同批」），但必须最终投出。
        delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert [(item.message.offset, item.delivery_count) for item in delivered] == [(1, 1)]

    await delivery_service.ack(tenant=current_tenant, request=_ack(1))
    with pytest.raises(MsgDeliveryExhaustedError) as excinfo:
        await delivery_service.ack(tenant=current_tenant, request=_ack(0))
    assert excinfo.value.code == 350007

    letters = await dead_letters.list_all()
    assert [(letter.offset, letter.attempts) for letter in letters] == [(0, MAX_DELIVERY_ATTEMPTS)]
    assert await offset_store.get(group=GROUP, topic_id=int(letters[0].topic_id), partition_no=PARTITION) == 2

    restarted = _rebuild(
        db_session=db_session,
        message_store=message_store,
        offset_store=offset_store,
        dead_letters=dead_letters,
        clock=clock,
    )
    assert list(await restarted.consume(tenant=current_tenant, request=_consume())) == []
    assert len(await dead_letters.list_all()) == 1
    with pytest.raises(MsgDeliveryExhaustedError):
        await restarted.ack(tenant=current_tenant, request=_ack(0))
