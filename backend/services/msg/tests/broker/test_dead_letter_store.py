"""死信持久化队列用例（06-5-1）：落库 / 幂等 / 重启恢复 / 清理。"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.broker.deadletter import DatabaseDeadLetterQueue, DeadLetter, DeadLetterReason

TENANT_ID = 7001
"""测试租户主键（与 conftest 的 `demo` 同值）。"""


def _letter(*, offset: int = 10, topic_id: int = 9001, status: str = "pending") -> DeadLetter:
    """构造一条死信（缺省同一主题 / 分区）。

    Args:
        offset: 分区内序号。
        topic_id: 主题主键。
        status: 处置状态。

    Returns:
        DeadLetter: 死信值对象。
    """
    return DeadLetter(
        message_id=f"msg-{offset}",
        topic_id=topic_id,
        partition_no=0,
        offset=offset,
        attempts=5,
        failed_at=datetime(2026, 1, 1, tzinfo=UTC),
        reason=DeadLetterReason.PROCESSING_FAILED,
        detail=ConcurrentStableDict({"error": "boom"}),
        tenant_id=TENANT_ID,
        namespace="msg",
        topic="audit.operation",
        log_id=f"log-{offset}",
        trace_id="trace-1",
        status=status,
    )


@pytest.fixture
def queue(replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]]) -> DatabaseDeadLetterQueue:
    """持久化死信队列（复用内存会话工作单元）。

    Args:
        replay_uow: 工作单元工厂夹具。

    Returns:
        DatabaseDeadLetterQueue: 空队列。
    """
    return DatabaseDeadLetterQueue(uow_factory=replay_uow)


@pytest.mark.kiwi_id(2467)
async def test_add_persists_and_lists(
    queue: DatabaseDeadLetterQueue,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> None:
    """`add` 落库并可按插入序列出；同三元组重复转入幂等（含新实例按库判重）。"""
    await queue.add(_letter(offset=10))
    await queue.add(_letter(offset=11))
    await queue.add(_letter(offset=10))

    restarted = DatabaseDeadLetterQueue(uow_factory=replay_uow)
    await restarted.add(_letter(offset=10))
    letters = await restarted.list_all()
    assert [letter.offset for letter in letters] == [10, 11]
    assert letters[0].status == "pending"
    assert letters[0].log_id == "log-10"
    assert letters[0].namespace == "msg"
    assert letters[0].topic == "audit.operation"
    assert letters[0].detail["error"] == "boom"


@pytest.mark.kiwi_id(2467)
async def test_contains_uses_memory_index_and_survives_reload(
    queue: DatabaseDeadLetterQueue,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> None:
    """`contains` 走内存索引；新队列启动加载后仍命中（重启不丢）。"""
    await queue.add(_letter(offset=10))
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is True
    assert await queue.contains(topic_id=9001, partition_no=0, offset=11) is False

    restarted = DatabaseDeadLetterQueue(uow_factory=replay_uow)
    assert await restarted.contains(topic_id=9001, partition_no=0, offset=10) is False
    await restarted.load()
    assert await restarted.contains(topic_id=9001, partition_no=0, offset=10) is True


@pytest.mark.kiwi_id(2467)
async def test_purge_topics_physically_deletes_and_is_idempotent(
    queue: DatabaseDeadLetterQueue,
) -> None:
    """按主题物理删除并同步索引；重复清理返回 0。"""
    await queue.add(_letter(offset=10, topic_id=9001))
    await queue.add(_letter(offset=11, topic_id=9001))
    await queue.add(_letter(offset=12, topic_id=9002))

    assert await queue.purge_topics(topic_ids=ConcurrentStableList([9001])) == 2
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is False
    assert await queue.contains(topic_id=9002, partition_no=0, offset=12) is True
    assert await queue.purge_topics(topic_ids=ConcurrentStableList([9001])) == 0
    assert await queue.purge_topics(topic_ids=ConcurrentStableList()) == 0


@pytest.mark.kiwi_id(2467)
async def test_add_without_tenant_is_rejected(queue: DatabaseDeadLetterQueue) -> None:
    """持久化实现要求租户主键（缺省值只在旧内存实现里成立）。"""
    letter = _letter(offset=10)
    missing = DeadLetter(
        message_id=letter.message_id,
        topic_id=letter.topic_id,
        partition_no=letter.partition_no,
        offset=letter.offset,
        attempts=letter.attempts,
        failed_at=letter.failed_at,
        reason=letter.reason,
        detail=letter.detail,
    )
    with pytest.raises(ValueError, match="tenant_id"):
        await queue.add(missing)


def test_queue_port_signature_is_unchanged() -> None:
    """持久化实现仍满足端口签名（`add` / `list_all` / `contains` / `purge_topics`）。"""
    from bms_msg.broker.deadletter import DeadLetterQueue

    assert issubclass(DatabaseDeadLetterQueue, DeadLetterQueue)
    assert isinstance(DeadLetterQueue, type)
