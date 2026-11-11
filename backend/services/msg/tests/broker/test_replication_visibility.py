"""复制可见性（msg 任务 02-4；Kiwi 2484）：消费者只能读到 `offset < HW`。

本文件只验「夹取落在投递服务」：用最小替身绕过主题仓储与数据库，
在 `DeliveryService.consume` 上直接观察 HW 前后可见条数。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableFrozenSet,
    ConcurrentStableList,
)
from bms_core.db.tenant import TenantContext

from bms_msg.broker.deadletter import DeadLetter, DeadLetterReason, InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replication import ReplicationSnapshot, ReplicationView
from bms_msg.broker.router import TopicRouter
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.schemas.message import ConsumeRequest
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2484)

TOPIC_ID = 9001
GROUP = "audit"
CONSUMER = "c1"
NAMESPACE = "msg"
TOPIC_NAME = "audit.visibility"


class _Topic:
    """主题快照替身。"""

    id = TOPIC_ID
    partition_count = 1


class _Router:
    """主题路由替身（只满足投递服务用到的两个方法）。"""

    async def resolve(self, *, namespace: str, name: str) -> _Topic:
        """返回固定主题。

        Args:
            namespace: 命名空间。
            name: 主题名。

        Returns:
            _Topic: 主题快照。
        """
        del namespace, name
        return _Topic()

    def partition(self, topic: _Topic, partition_no: int) -> int:
        """校验并返回分区号。

        Args:
            topic: 主题快照。
            partition_no: 分区号。

        Returns:
            int: 分区号。
        """
        del topic
        return partition_no

    def route(self, topic: _Topic, key: str | None) -> int:
        """按分区键选分区（替身恒 0）。

        Args:
            topic: 主题快照。
            key: 分区键。

        Returns:
            int: 分区号。
        """
        del topic, key
        return 0


class _View(ReplicationView):
    """可调可见上限的复制视图替身。"""

    def __init__(self, *, limit: int | None) -> None:
        self._limit = limit

    @property
    def enabled(self) -> bool:
        """复制是否开启。

        Returns:
            bool: 固定 True（本用例只验夹取路径）。
        """
        return True

    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        """不判（本用例不测 acks）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            acks: 发布档位。
        """
        del topic_id, partition_no, acks

    async def wait_committed(self, *, topic_id: int, partition_no: int, offset: int) -> None:
        """不等待（本用例不测 acks）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 位点。
        """
        del topic_id, partition_no, offset

    async def visible_limit(self, *, topic_id: int, partition_no: int) -> int | None:
        """返回固定可见上限。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int | None: 可见上限。
        """
        del topic_id, partition_no
        return self._limit

    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """返回固定快照（本用例不使用）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 固定快照。
        """
        del topic_id, partition_no
        return ReplicationSnapshot(
            leader="n1",
            epoch=0,
            leo=self._limit or 0,
            hw=self._limit or 0,
            isr=ConcurrentStableFrozenList(("n1",)),
            replicas=ConcurrentStableFrozenList(("n1",)),
        )


def _tenant() -> CurrentTenant:
    """构造最小租户上下文。

    Returns:
        CurrentTenant: 当前租户。
    """
    return CurrentTenant(
        tenant=TenantContext(code="demo", db_key="tenant_demo", name="演示租户", tenant_id=1),
        namespace_allowlist=ConcurrentStableFrozenSet((NAMESPACE,)),
    )


def _message(*, offset: int) -> StoredMessage:
    """构造一条已落盘记录。

    Args:
        offset: 分区内序号。

    Returns:
        StoredMessage: 记录。
    """
    return StoredMessage(
        message_id=f"m-{offset}",
        log_id=f"l-{offset}",
        topic_id=TOPIC_ID,
        partition_no=0,
        offset=offset,
        key=None,
        payload=f"p-{offset}".encode(),
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )


def _service(*, store: InMemoryMessageStore, limit: int | None) -> DeliveryService:
    """组装带复制视图的投递服务。

    Args:
        store: 内存消息存储。
        limit: 可见上限。

    Returns:
        DeliveryService: 投递服务。
    """
    return DeliveryService(
        store=store,
        offsets=InMemoryOffsetStore(),
        dead_letters=InMemoryDeadLetterQueue(),
        router=cast("TopicRouter", _Router()),
        state=DeliveryState(),
        replication=_View(limit=limit),
    )


class _AcksView(_View):
    """记录 acks 预检 / 等待调用的复制视图替身。"""

    def __init__(self) -> None:
        super().__init__(limit=None)
        self.ensure_calls: ConcurrentStableList[str] = ConcurrentStableList()
        self.wait_calls = 0

    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        del topic_id, partition_no
        self.ensure_calls.add(acks)

    async def wait_committed(self, *, topic_id: int, partition_no: int, offset: int) -> None:
        del topic_id, partition_no, offset
        self.wait_calls += 1


def _service_with_view(*, store: InMemoryMessageStore, view: ReplicationView) -> DeliveryService:
    """用给定复制视图组装投递服务。

    Args:
        store: 内存消息存储。
        view: 复制视图。

    Returns:
        DeliveryService: 投递服务。
    """
    return DeliveryService(
        store=store,
        offsets=InMemoryOffsetStore(),
        dead_letters=InMemoryDeadLetterQueue(),
        router=cast("TopicRouter", _Router()),
        state=DeliveryState(),
        replication=view,
    )


async def test_publish_acks_all_waits_and_acks_local_does_not() -> None:
    """发布档位：acks=1 只落盘；acks=all 走 ISR 预检并等待提交。"""
    from bms_msg.schemas.message import PublishRequest

    store = InMemoryMessageStore()
    view = _AcksView()
    service = _service_with_view(store=store, view=view)
    request = PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, acks="1")
    await service.publish(tenant=_tenant(), request=request)
    assert list(view.ensure_calls) == ["1"]
    assert view.wait_calls == 0

    request_all = PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, acks="all")
    await service.publish(tenant=_tenant(), request=request_all)
    assert list(view.ensure_calls) == ["1", "all"]
    assert view.wait_calls == 1


async def test_consume_does_not_read_uncommitted() -> None:
    """HW=1：只投 offset 0，offset 1 对消费不可见。"""
    store = InMemoryMessageStore()
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id="m-0",
        log_id="l-0",
        key=None,
        payload=b"p-0",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id="m-1",
        log_id="l-1",
        key=None,
        payload=b"p-1",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )
    service = _service(store=store, limit=1)
    delivered = await service.consume(
        tenant=_tenant(),
        request=ConsumeRequest(
            group=GROUP,
            consumer_id=CONSUMER,
            namespace=NAMESPACE,
            topic=TOPIC_NAME,
            partition=0,
            max_messages=10,
        ),
    )
    assert [item.message.offset for item in delivered] == [0]


async def test_consume_skips_dead_letter_and_stops_at_hw() -> None:
    """HW 之前有死信：跳过死信后循环到 HW 即停（不再拉未提交尾部）。"""
    store = InMemoryMessageStore()
    for offset in range(3):
        await store.append(
            topic_id=TOPIC_ID,
            partition_no=0,
            message_id=f"m-{offset}",
            log_id=f"l-{offset}",
            key=None,
            payload=f"p-{offset}".encode(),
            headers=ConcurrentStableDict(),
            trace=EMPTY_TRACE,
            published_at=datetime.now(UTC),
        )
    dead_letters = InMemoryDeadLetterQueue()
    await dead_letters.add(
        DeadLetter(
            message_id="m-1",
            topic_id=TOPIC_ID,
            partition_no=0,
            offset=1,
            attempts=5,
            failed_at=datetime.now(UTC),
            reason=DeadLetterReason.DELIVERY_EXHAUSTED,
        )
    )
    service = DeliveryService(
        store=store,
        offsets=InMemoryOffsetStore(),
        dead_letters=dead_letters,
        router=cast("TopicRouter", _Router()),
        state=DeliveryState(),
        replication=_View(limit=3),
    )
    delivered = await service.consume(
        tenant=_tenant(),
        request=ConsumeRequest(
            group=GROUP,
            consumer_id=CONSUMER,
            namespace=NAMESPACE,
            topic=TOPIC_NAME,
            partition=0,
            max_messages=10,
        ),
    )
    assert [item.message.offset for item in delivered] == [0, 2]


async def test_consume_reads_more_after_hw_advances() -> None:
    """HW 推进到 LEO：未提交尾部变为可见。"""
    store = InMemoryMessageStore()
    for offset in range(3):
        await store.append(
            topic_id=TOPIC_ID,
            partition_no=0,
            message_id=f"m-{offset}",
            log_id=f"l-{offset}",
            key=None,
            payload=f"p-{offset}".encode(),
            headers=ConcurrentStableDict(),
            trace=EMPTY_TRACE,
            published_at=datetime.now(UTC),
        )
    service = _service(store=store, limit=3)
    delivered = await service.consume(
        tenant=_tenant(),
        request=ConsumeRequest(
            group=GROUP,
            consumer_id=CONSUMER,
            namespace=NAMESPACE,
            topic=TOPIC_NAME,
            partition=0,
            max_messages=10,
        ),
    )
    assert [item.message.offset for item in delivered] == [0, 1, 2]


async def test_committed_at_hw_returns_empty_without_fetch() -> None:
    """已提交位点到达 HW：直接返空批（不读未提交记录）。"""
    store = InMemoryMessageStore()
    service = _service(store=store, limit=0)
    delivered = await service.consume(
        tenant=_tenant(),
        request=ConsumeRequest(
            group=GROUP,
            consumer_id=CONSUMER,
            namespace=NAMESPACE,
            topic=TOPIC_NAME,
            partition=0,
            max_messages=10,
        ),
    )
    assert list(delivered) == []


def test_message_helper_shape() -> None:
    """记录替身的 offset / payload 口径（防用例自身写错）。"""
    message = _message(offset=2)
    assert message.offset == 2
    assert message.payload == b"p-2"
