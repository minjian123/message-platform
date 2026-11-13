"""死信 DB 权威 + 本地短缓存（02-8-4；Kiwi 2497）。

覆盖：跨实例可见性（新队列实例无需预热）、TTL 过期后回源、`add` 立即生效、有界裁剪、清理后不可见。
"""

# pyright: reportPrivateUsage=false
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.broker.deadletter import DatabaseDeadLetterQueue, DeadLetter, DeadLetterReason

pytestmark = pytest.mark.kiwi_id(2497)

TENANT_ID = 7001


def _letter(*, offset: int = 10, topic_id: int = 9001) -> DeadLetter:
    """构造一条死信。"""
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
    )


class _Clock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


@pytest.fixture
def clock() -> _Clock:
    """单调时钟夹具。"""
    return _Clock()


@pytest.fixture
def queue(replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]], clock: _Clock) -> DatabaseDeadLetterQueue:
    """持久化死信队列（带可控时钟）。"""
    return DatabaseDeadLetterQueue(uow_factory=replay_uow, cache_seconds=5.0, clock=clock)


async def test_other_instance_sees_dead_letter_without_warmup(
    queue: DatabaseDeadLetterQueue,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> None:
    """A 转入后，B（新实例、未预热）查库即命中。"""
    await queue.add(_letter(offset=10))

    other = DatabaseDeadLetterQueue(uow_factory=replay_uow)
    assert await other.contains(topic_id=9001, partition_no=0, offset=10) is True
    assert await other.contains(topic_id=9001, partition_no=0, offset=11) is False


async def test_add_is_immediately_visible_on_same_instance(queue: DatabaseDeadLetterQueue) -> None:
    """本节点 `add` 后立即命中（写库成功即回填缓存）。"""
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is False
    await queue.add(_letter(offset=10))
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is True


async def test_cache_expires_and_requeries(
    queue: DatabaseDeadLetterQueue,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
    clock: _Clock,
) -> None:
    """负结果在 TTL 内复用；过期后回源能看到其他实例新转入的死信。"""
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is False
    other = DatabaseDeadLetterQueue(uow_factory=replay_uow)
    await other.add(_letter(offset=10))
    # TTL 内仍按旧缓存返回（跨实例最长 5 秒收敛）
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is False
    clock.value += 6.0
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is True


async def test_cache_is_bounded(
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]], clock: _Clock
) -> None:
    """缓存超上限时先剔过期项、再按插入序淘汰（不会无限增长）。"""
    queue = DatabaseDeadLetterQueue(uow_factory=replay_uow, cache_limit=2, clock=clock)
    for offset in range(2):
        await queue.contains(topic_id=9001, partition_no=0, offset=offset)
    # 让已有条目过期，再触发一次写入：应优先清掉过期项而不是淘汰新项
    clock.value += 6.0
    await queue.contains(topic_id=9001, partition_no=0, offset=99)
    assert queue.cache_size <= 2

    # 无过期项时按插入序淘汰最老（第二段裁剪）
    tight = DatabaseDeadLetterQueue(uow_factory=replay_uow, cache_limit=1, clock=clock)
    await tight.contains(topic_id=9001, partition_no=0, offset=0)
    await tight.contains(topic_id=9001, partition_no=0, offset=1)
    assert tight.cache_size == 1


async def test_prune_clears_all_expired_without_second_loop(
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]], clock: _Clock
) -> None:
    """全部过期时一次清空：第二段淘汰循环不进入（02-8-4 分支覆盖）。"""
    queue = DatabaseDeadLetterQueue(uow_factory=replay_uow, cache_limit=2, clock=clock)
    now = clock()
    for offset in range(3):
        queue._cache[(9001, 0, offset)] = (True, now)
    clock.value += 6.0
    queue._trim()
    assert queue.cache_size == 0


async def test_purge_topics_hides_entries(queue: DatabaseDeadLetterQueue) -> None:
    """按主题清理后 `contains` 为假（缓存同步剔除）。"""
    await queue.add(_letter(offset=10, topic_id=9001))
    await queue.add(_letter(offset=11, topic_id=9002))
    assert await queue.purge_topics(topic_ids=ConcurrentStableList([9001])) == 1
    assert await queue.contains(topic_id=9001, partition_no=0, offset=10) is False
    assert await queue.contains(topic_id=9002, partition_no=0, offset=11) is True
