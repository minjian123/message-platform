"""长轮询等待器与「写入即唤醒」存储包装（任务 03-5；Kiwi 2425）。

覆盖三件事：

1. `DeliveryWaiter` 的三态返回（`changed` / `timeout` / `rejected`）与版本号语义——
   版本号存在的唯一理由是「拉取期间到达的消息不能让等待者睡满超时」；
2. 多等待者同键同醒、无等待者时 notify 只加版本；
3. `NotifyingMessageStore` 六个方法全部委托、只有 `append` 顺带唤醒。
"""

import asyncio
from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker.log import InMemoryMessageStore, MessageStore, StoredMessage
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore, WaitOutcome

pytestmark = pytest.mark.kiwi_id(2425)


def test_forget_topic_clears_versions_and_events() -> None:
    """按主题清等待器：版本与事件都移除，重复调用返回 0。"""
    waiter = DeliveryWaiter()
    waiter.notify(KEY)
    waiter.notify((8, 0))
    assert waiter.forget_topic(topic_id=7) == 1
    assert waiter.version(KEY) == 0
    assert waiter.version((8, 0)) == 1
    assert waiter.forget_topic(topic_id=7) == 0


KEY = (7, 0)
"""等待键（`(topic_id, partition_no)`）。"""


async def test_notify_bumps_version_and_wakes_all_waiters() -> None:
    """同键多个等待者同时被唤醒；版本号单调递增。"""
    waiter = DeliveryWaiter()
    assert waiter.version(KEY) == 0

    first = asyncio.create_task(waiter.wait(KEY, version=0, timeout_ms=5_000))
    second = asyncio.create_task(waiter.wait(KEY, version=0, timeout_ms=5_000))
    await asyncio.sleep(0)
    assert waiter.waiting() == 2

    waiter.notify(KEY)

    assert await first is WaitOutcome.CHANGED
    assert await second is WaitOutcome.CHANGED
    assert waiter.version(KEY) == 1
    assert waiter.waiting() == 0


async def test_wait_returns_changed_when_version_already_moved() -> None:
    """拉取期间已有变化：不再睡满超时，直接返回 `changed`。"""
    waiter = DeliveryWaiter()
    waiter.notify(KEY)

    outcome = await waiter.wait(KEY, version=0, timeout_ms=10_000)

    assert outcome is WaitOutcome.CHANGED
    assert waiter.waiting() == 0


async def test_wait_times_out_without_change() -> None:
    """无变化即超时（长轮询到期返空的判据）。"""
    waiter = DeliveryWaiter()

    outcome = await waiter.wait(KEY, version=0, timeout_ms=10)

    assert outcome is WaitOutcome.TIMEOUT


async def test_wait_rejects_when_limit_reached() -> None:
    """等待者到上限立即返回 `rejected`（不排队、不报错）。"""
    waiter = DeliveryWaiter(max_waiters=1)
    assert waiter.max_waiters == 1
    holder = asyncio.create_task(waiter.wait(KEY, version=0, timeout_ms=5_000))
    await asyncio.sleep(0)

    outcome = await waiter.wait(KEY, version=0, timeout_ms=5_000)

    assert outcome is WaitOutcome.REJECTED
    waiter.notify(KEY)
    assert await holder is WaitOutcome.CHANGED


async def test_notify_without_waiters_only_bumps_version() -> None:
    """无等待者时 notify 只加版本，不残留 Event（下次等待重建）。"""
    waiter = DeliveryWaiter()

    waiter.notify(KEY)
    waiter.notify(KEY)

    assert waiter.version(KEY) == 2
    assert waiter.waiting() == 0


async def test_second_wait_needs_a_new_round() -> None:
    """唤醒后 Event 被丢弃：同一版本号再等仍是「已变化」，不会假醒。"""
    waiter = DeliveryWaiter()
    first = asyncio.create_task(waiter.wait(KEY, version=0, timeout_ms=5_000))
    await asyncio.sleep(0)
    waiter.notify(KEY)
    assert await first is WaitOutcome.CHANGED

    # 版本已前进，用旧版本再调仍返回 changed（调用方正常会先拉取再更新版本）
    assert await waiter.wait(KEY, version=0, timeout_ms=10) is WaitOutcome.CHANGED
    # 用新版本等待则真正挂起并超时（证明上一轮的 Event 已被丢弃）
    assert await waiter.wait(KEY, version=1, timeout_ms=10) is WaitOutcome.TIMEOUT


async def _append(store: MessageStore, *, payload: bytes = b"x") -> StoredMessage:
    """向存储追加一条消息（用例共用）。

    Args:
        store: 目标存储。
        payload: 消息体。

    Returns:
        StoredMessage: 存储记录。
    """
    return await store.append(
        topic_id=KEY[0],
        partition_no=KEY[1],
        message_id="m1",
        log_id="l1",
        key=None,
        payload=payload,
        headers=ConcurrentStableDict(),
        trace_id=None,
        published_at=datetime.now(UTC),
    )


async def test_notifying_store_notifies_on_append_and_delegates() -> None:
    """`append` 唤醒该分区；其余五个方法原样委托（端口行为不变）。"""
    inner = InMemoryMessageStore()
    waiter = DeliveryWaiter()
    store = NotifyingMessageStore(inner, waiter)
    assert store.inner is inner

    message = await _append(store)
    assert message.offset == 0
    assert waiter.version(KEY) == 1
    assert message.content_type == DEFAULT_CONTENT_TYPE
    assert message.compression == COMPRESSION_NONE

    assert [
        item.offset for item in await store.fetch(topic_id=KEY[0], partition_no=KEY[1], from_offset=0, max_messages=10)
    ] == [0]
    assert await store.leo(topic_id=KEY[0], partition_no=KEY[1]) == 1
    assert await store.start_offset(topic_id=KEY[0], partition_no=KEY[1]) == 0
    assert len(await store.segments(topic_id=KEY[0], partition_no=KEY[1])) == 1
    assert await store.truncate_before(topic_id=KEY[0], partition_no=KEY[1], offset=1) == 1
    assert await store.start_offset(topic_id=KEY[0], partition_no=KEY[1]) == 1
    # 截断不唤醒（只有写入才唤醒）：版本仍是 1
    assert waiter.version(KEY) == 1


async def test_notifying_store_forwards_payload_marks() -> None:
    """内容类型 / 压缩标记同样透传（包装层不改语义）。"""
    inner = InMemoryMessageStore()
    store = NotifyingMessageStore(inner, DeliveryWaiter())

    message = await store.append(
        topic_id=KEY[0],
        partition_no=KEY[1],
        message_id="m2",
        log_id="l2",
        key="k",
        payload=b"compressed",
        headers=ConcurrentStableDict(),
        trace_id="t-1",
        published_at=datetime.now(UTC),
        content_type="application/json",
        compression="gzip",
        uncompressed_len=42,
    )

    assert message.content_type == "application/json"
    assert message.compression == "gzip"
    assert message.uncompressed_len == 42
