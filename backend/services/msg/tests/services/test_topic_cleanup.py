# pyright: reportPrivateUsage=false
"""删主题清理编排（msg 任务 06-2-1；Kiwi 2465）。

覆盖：七步清理全生效与台账收口、重复执行幂等、单步失败留痕并从失败步骤续跑、连续失败告警分支、
影响预检五项计数与单维度容错、`kick` / 周期循环启停、未知步骤与台账缺失的防御分支。

装配：真实编排 + 内存端口的存储 / 组 / 延迟 / 死信 / 运行态，平台库仍是内存会话（台账与主题行）。
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.deadletter import DeadLetter, DeadLetterReason, InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import ConsumerGroup, InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.broker.watermark import StorageReading, StorageWatermark
from bms_msg.models.topic_cleanup import (
    CLEANUP_COMPLETED,
    CLEANUP_PENDING,
    CLEANUP_STEPS,
    STEP_DELAY,
    STEP_METADATA,
)
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.repositories.topic_cleanup import TopicCleanupRepository
from bms_msg.services.topic_cleanup import TopicCleanupService

pytestmark = pytest.mark.kiwi_id(2465)

TENANT_ID = 7001
"""测试租户主键（与夹具上下文同值）。"""

OTHER_TOPIC_ID = 4242
"""对照主题主键（验证清理不越界到别的主题）。"""


class _FlakyMessageStore(InMemoryMessageStore):
    """消息存储替身：`drop_topic` 可注入若干次失败（验证「失败留痕 + 下一轮续跑」）。"""

    def __init__(self) -> None:
        """初始化（默认不失败）。"""
        super().__init__()
        self.failures = 0

    async def drop_topic(self, *, topic_id: int) -> int:
        """按注入次数抛错，之后正常委托。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 受影响的分区数。

        Raises:
            OSError: 仍在注入的失败轮次内。
        """
        if self.failures > 0:
            self.failures -= 1
            raise OSError("disk busy")
        return await super().drop_topic(topic_id=topic_id)


class _BrokenMessageStore(InMemoryMessageStore):
    """消息存储替身：`leo` / `start_offset` 恒抛错（影响预检的消息维度容错）。"""

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """恒抛错。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 不会返回。

        Raises:
            OSError: 恒抛。
        """
        del topic_id, partition_no
        raise OSError("message store down")


class _SlowMessageStore(InMemoryMessageStore):
    """消息存储替身：`drop_topic` 先等一会儿（验证 `stop()` 取消在途清理任务）。"""

    async def drop_topic(self, *, topic_id: int) -> int:
        """等 50ms 再委托。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 受影响的分区数。
        """
        await asyncio.sleep(0.05)
        return await super().drop_topic(topic_id=topic_id)


class _BrokenDelayStore(InMemoryDelayStore):
    """延迟区替身：`list_for_tenant` 恒抛错（影响预检的延迟维度容错）。"""

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[DelayRecord]:
        """恒抛错。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[DelayRecord]: 不会返回。

        Raises:
            OSError: 恒抛。
        """
        del tenant_id
        raise OSError("delay store down")


class _BrokenDeadLetters(InMemoryDeadLetterQueue):
    """死信替身：`list_all` 恒抛错（影响预检的死信维度容错）。"""

    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """恒抛错。

        Returns:
            ConcurrentStableList[DeadLetter]: 不会返回。

        Raises:
            OSError: 恒抛。
        """
        raise OSError("dlq down")


class _BrokenWatermark(StorageWatermark):
    """水位替身：`force_refresh` 恒抛错（清理完成后的刷新失败不应回滚删除事实）。"""

    def force_refresh(self) -> StorageReading:
        """恒抛错。

        Returns:
            StorageReading: 不会返回。

        Raises:
            OSError: 恒抛。
        """
        raise OSError("watermark down")


class _BrokenGroupStore(InMemoryGroupStore):
    """组存储替身：`list_all` 恒抛错（影响预检的单维度容错）。"""

    async def list_all(self) -> ConcurrentStableList[ConsumerGroup]:
        """恒抛错。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 不会返回。

        Raises:
            OSError: 恒抛。
        """
        raise OSError("group store down")


def _uow_factory(session: DbSession) -> Callable[[], AbstractAsyncContextManager[UnitOfWork]]:
    """构造复用同一会话的工作单元工厂。

    Args:
        session: 会话。

    Returns:
        Callable[[], AbstractAsyncContextManager[UnitOfWork]]: 工厂。
    """

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """产出绑定会话的工作单元。

        Yields:
            UnitOfWork: 工作单元。
        """
        yield DbUnitOfWork(session)

    return _uow


def _build(
    db_session: AsyncSession,
    tmp_path: Path,
    *,
    messages: InMemoryMessageStore | None = None,
    groups: InMemoryGroupStore | None = None,
    delay: InMemoryDelayStore | None = None,
    dead_letters: InMemoryDeadLetterQueue | None = None,
    watermark: StorageWatermark | None = None,
    interval_seconds: float = 30.0,
) -> tuple[TopicCleanupService, InMemoryOffsetStore, InMemoryDelayStore, InMemoryDeadLetterQueue, RetentionThrottle]:
    """组装清理编排与可断言的端口。

    Args:
        db_session: 会话。
        tmp_path: 临时目录（目录登记与水位根）。
        messages: 消息存储替身（缺省为内存实现）。
        groups: 组存储替身（缺省为内存实现）。
        delay: 延迟区替身（缺省为内存实现）。
        dead_letters: 死信替身（缺省为内存实现）。
        watermark: 水位替身（缺省为真实实现，根临时目录）。
        interval_seconds: 周期扫描间隔（用例给毫秒级，避免长等）。

    Returns:
        tuple: （服务, 位点, 延迟区, 死信, 节流窗口）。
    """
    session = cast("DbSession", db_session)
    offsets = InMemoryOffsetStore()
    delay_store = delay or InMemoryDelayStore(quota=100, global_quota=1_000)
    dead = dead_letters or InMemoryDeadLetterQueue()
    throttle = RetentionThrottle()
    service = TopicCleanupService(
        messages=messages or InMemoryMessageStore(),
        offsets=offsets,
        groups=groups or InMemoryGroupStore(),
        delay=delay_store,
        wheel=DelayWheel(),
        dead_letters=dead,
        delivery=DeliveryState(),
        waiter=DeliveryWaiter(),
        throttle=throttle,
        directory=PartitionDirectory(root=tmp_path),
        cache=TopicCache(),
        watermark=watermark or StorageWatermark(root=tmp_path),
        uow_factory=_uow_factory(session),
        interval_seconds=interval_seconds,
    )
    return service, offsets, delay_store, dead, throttle


async def _wait_idle(service: TopicCleanupService, *, timeout: float = 5.0) -> None:
    """等待在本主题清理任务全部结束（覆盖率采集 / 并行下固定 sleep 不够）。

    Args:
        service: 清理服务。
        timeout: 最长等待秒数。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while service._running_topics:
        assert loop.time() < deadline, "清理任务未在 5s 内结束"
        await asyncio.sleep(0.02)


async def _seed_topic(db_session: AsyncSession, *, name: str = "audit.cleanup", partitions: int = 2) -> int:
    """建主题行 + 分区行 + 台账行（`pending`），返回主题主键。

    Args:
        db_session: 会话。
        name: 主题名。
        partitions: 分区数。

    Returns:
        int: 主题主键。
    """
    session = cast("DbSession", db_session)
    topics = TopicRepository(session)
    topic = await topics.create(
        tenant_id=TENANT_ID,
        namespace="msg",
        name=name,
        partition_count=partitions,
        retention_ms=60_000,
        retention_bytes=0,
        cleanup_policy="delete",
    )
    await PartitionRepository(session).add_partitions(
        topic_id=int(topic.id), start_no=0, count=partitions, leader="msg-0"
    )
    await TopicCleanupRepository(session).create(
        tenant_id=TENANT_ID,
        topic_id=int(topic.id),
        namespace="msg",
        topic=name,
    )
    await session.commit()
    return int(topic.id)


def _delay_record(*, topic_id: int, log_id: str, tenant_id: int = TENANT_ID) -> DelayRecord:
    """构造一条延迟记录。

    Args:
        topic_id: 主题主键。
        log_id: 记录标识。
        tenant_id: 租户主键。

    Returns:
        DelayRecord: 延迟记录。
    """
    now = datetime.now(UTC)
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=tenant_id,
        topic_id=topic_id,
        namespace="msg",
        topic="audit.cleanup",
        partition_no=0,
        delay_level="5m",
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict[str, str](),
        trace_id=None,
        published_at=now,
        deliver_at=now + timedelta(minutes=5),
    )


async def _seed_side_data(
    db_session: AsyncSession,
    *,
    topic_id: int,
    offsets: InMemoryOffsetStore,
    delay: InMemoryDelayStore,
    dead_letters: InMemoryDeadLetterQueue,
    groups: InMemoryGroupStore,
) -> None:
    """往各端口灌该主题（与对照主题）的数据。

    Args:
        db_session: 会话（消息存储单独注入，见调用方）。
        topic_id: 主题主键。
        offsets: 位点存储。
        delay: 延迟区。
        dead_letters: 死信队列。
        groups: 组存储。
    """
    del db_session
    await offsets.commit(group="audit-group", topic_id=topic_id, partition_no=0, offset=7)
    await delay.enqueue(_delay_record(topic_id=topic_id, log_id="log-1"))
    await delay.enqueue(_delay_record(topic_id=OTHER_TOPIC_ID, log_id="log-other"))
    await dead_letters.add(
        DeadLetter(
            message_id="m-1",
            topic_id=topic_id,
            partition_no=0,
            offset=1,
            attempts=3,
            failed_at=datetime.now(UTC),
            reason=DeadLetterReason.DELIVERY_EXHAUSTED,
        )
    )
    await dead_letters.add(
        DeadLetter(
            message_id="m-other",
            topic_id=OTHER_TOPIC_ID,
            partition_no=0,
            offset=1,
            attempts=3,
            failed_at=datetime.now(UTC),
            reason=DeadLetterReason.PROCESSING_FAILED,
        )
    )
    await groups.save(
        ConsumerGroup(
            tenant_id=TENANT_ID,
            topic_id=topic_id,
            group="audit-group",
            state="stable",
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    await groups.save(
        ConsumerGroup(
            tenant_id=TENANT_ID,
            topic_id=OTHER_TOPIC_ID,
            group="other-group",
            state="stable",
            generation=1,
            members=ConcurrentStableList(),
        )
    )


async def _append_messages(store: InMemoryMessageStore, *, topic_id: int, count: int = 2) -> None:
    """往分区 0 写若干条消息（验证段清理与消息总量）。

    Args:
        store: 消息存储。
        topic_id: 主题主键。
        count: 条数。
    """
    for index in range(count):
        await store.append(
            topic_id=topic_id,
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"log-msg-{index}",
            key=None,
            payload=b"x",
            headers=ConcurrentStableDict[str, str](),
            trace_id=None,
            published_at=datetime.now(UTC),
        )


async def test_run_once_cleans_every_resource_and_completes_ledger(db_session: AsyncSession, tmp_path: Path) -> None:
    """七步全跑完：段 / 延迟 / 位点 / 组 / 死信 / 元数据全清，台账 `completed` 且有完成时刻。"""
    messages = InMemoryMessageStore()
    groups = InMemoryGroupStore()
    service, offsets, delay, dead_letters, throttle = _build(db_session, tmp_path, messages=messages, groups=groups)
    topic_id = await _seed_topic(db_session)
    await _append_messages(messages, topic_id=topic_id)
    await _seed_side_data(
        db_session, topic_id=topic_id, offsets=offsets, delay=delay, dead_letters=dead_letters, groups=groups
    )

    advanced = await service.run_once()

    session = cast("DbSession", db_session)
    assert advanced == 1
    assert await messages.leo(topic_id=topic_id, partition_no=0) == 0
    remaining_delay = await delay.list_for_tenant(tenant_id=TENANT_ID)
    assert [record.log_id for record in remaining_delay] == ["log-other"]
    assert await offsets.get(group="audit-group", topic_id=topic_id, partition_no=0) == 0
    assert [record.group for record in await groups.list_for_tenant(tenant_id=TENANT_ID)] == ["other-group"]
    assert [letter.message_id for letter in await dead_letters.list_all()] == ["m-other"]
    throttle.forget(topic_id=topic_id, partition_no=0)
    assert await TopicRepository(session).get(topic_id) is None
    assert await PartitionRepository(session).list_by_topic(topic_id) == []
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    assert (ledger.status, ledger.attempts, ledger.last_error) == (CLEANUP_COMPLETED, 1, None)
    assert ledger.done_steps == list(CLEANUP_STEPS)
    assert ledger.finished_at is not None


async def test_run_once_is_idempotent_after_completion(db_session: AsyncSession, tmp_path: Path) -> None:
    """台账已 `completed`：再扫不再推进（返回 0，无副作用）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    topic_id = await _seed_topic(db_session)
    await service.run_once()
    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    first_finished = ledger.finished_at
    # 直接读会话会开启隐式事务；服务方法内部显式 `begin()` 前必须先收掉它。
    await session.rollback()
    assert await service.run_once() == 0
    db_session.expire_all()
    reloaded = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert reloaded is not None and reloaded.finished_at == first_finished


async def test_failure_records_error_and_resumes_from_failed_step(db_session: AsyncSession, tmp_path: Path) -> None:
    """单步失败：台账回 `pending` 并记错误；下一轮从失败步骤续跑并最终收口。"""
    messages = _FlakyMessageStore()
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path, messages=messages)
    topic_id = await _seed_topic(db_session)
    messages.failures = 1

    await service.run_once()

    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    assert ledger.status == CLEANUP_PENDING
    assert ledger.last_error is not None and "messages" in ledger.last_error and "OSError" in ledger.last_error
    assert ledger.done_steps == []

    await session.rollback()
    await service.run_once()
    db_session.expire_all()
    reloaded = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert reloaded is not None
    assert (reloaded.status, reloaded.attempts) == (CLEANUP_COMPLETED, 2)


async def test_repeated_failure_escalates_log(db_session: AsyncSession, tmp_path: Path) -> None:
    """连续失败达阈值：台账持续 `pending` 且尝试次数累加（告警分支由日志级别承担）。"""
    messages = _FlakyMessageStore()
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path, messages=messages)
    topic_id = await _seed_topic(db_session)
    messages.failures = 3
    for _round in range(3):
        await service.run_once()
    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    assert (ledger.status, ledger.attempts) == (CLEANUP_PENDING, 3)


async def test_fail_and_record_step_tolerate_missing_ledger(db_session: AsyncSession, tmp_path: Path) -> None:
    """台账不存在时 `_fail` / `_record_step` 直接返回（防御分支，不抛错）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    await service._fail(999999, error="x")
    await service._record_step(999999, done=ConcurrentStableList(["messages"]))


async def test_unknown_step_is_rejected(db_session: AsyncSession, tmp_path: Path) -> None:
    """未知步骤键：`ValueError`（登记错即失败，不静默跳过）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    with pytest.raises(ValueError, match="未知清理步骤"):
        await service._run_step(step="nope", topic_id=1, tenant_id=TENANT_ID)


async def test_impact_counts_five_dimensions(db_session: AsyncSession, tmp_path: Path) -> None:
    """影响预检：消息总量按分区求和、组 / 延迟 / 死信按主题过滤，对照主题不计入。"""
    messages = InMemoryMessageStore()
    groups = InMemoryGroupStore()
    service, offsets, delay, dead_letters, _throttle = _build(db_session, tmp_path, messages=messages, groups=groups)
    topic_id = await _seed_topic(db_session, partitions=2)
    await _append_messages(messages, topic_id=topic_id, count=3)
    await _seed_side_data(
        db_session, topic_id=topic_id, offsets=offsets, delay=delay, dead_letters=dead_letters, groups=groups
    )

    impact = await service.impact(tenant_id=TENANT_ID, topic_id=topic_id, partition_count=2)

    assert (impact.partition_count, impact.message_total) == (2, 3)
    assert (impact.group_count, impact.delayed_total, impact.dlq_total) == (1, 1, 1)


async def test_impact_tolerates_broken_dimension(db_session: AsyncSession, tmp_path: Path) -> None:
    """影响预检单维度失败：该维度记 0（不阻断后续删除）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path, groups=_BrokenGroupStore())
    topic_id = await _seed_topic(db_session)
    impact = await service.impact(tenant_id=TENANT_ID, topic_id=topic_id, partition_count=1)
    assert impact.group_count == 0


async def test_kick_runs_immediately_and_stop_cancels(db_session: AsyncSession, tmp_path: Path) -> None:
    """`kick` 立即起一轮且同主题不重复起；`ensure_started` / `stop` 幂等，`running` 随循环生命周期变化。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    topic_id = await _seed_topic(db_session)
    await db_session.rollback()

    service.kick(topic_id)
    first = service._running_topics.get(topic_id)
    service.kick(topic_id)
    assert service._running_topics.get(topic_id) is first
    await _wait_idle(service)

    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None and ledger.status == CLEANUP_COMPLETED

    await session.rollback()
    service.ensure_started()
    service.ensure_started()
    assert service.running is True
    await service.stop()
    assert service.running is False
    await service.stop()


async def test_guarded_swallows_round_error(db_session: AsyncSession, tmp_path: Path) -> None:
    """`_guarded` 吞掉本轮异常（后台任务不能把异常抛进事件循环）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    original = service._begin_round

    async def _boom(topic_id: int) -> None:
        """恒抛错。

        Args:
            topic_id: 主题主键。

        Raises:
            RuntimeError: 恒抛。
        """
        del topic_id
        raise RuntimeError("boom")

    service._begin_round = _boom  # type: ignore[method-assign]
    await service._guarded(1)
    service._begin_round = original  # type: ignore[method-assign]


async def test_begin_round_skips_completed_ledger(db_session: AsyncSession, tmp_path: Path) -> None:
    """已完成台账：`_begin_round` 返回 None（不重复推进）。"""
    service, _offsets, _delay, _dead_letters, _throttle = _build(db_session, tmp_path)
    topic_id = await _seed_topic(db_session)
    await service.run_once()
    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    # 直接读会话会开启隐式事务；服务方法显式 `begin()` 前必须先收掉它。
    await session.rollback()
    assert await service._begin_round(topic_id) is None


async def test_delay_and_metadata_counts(db_session: AsyncSession, tmp_path: Path) -> None:
    """删除计数：延迟按条、元数据按行（用于结构化日志的可观测数字）。"""
    service, _offsets, delay, _dead, _throttle = _build(db_session, tmp_path)
    topic_id = await _seed_topic(db_session, partitions=3)
    await delay.enqueue(_delay_record(topic_id=topic_id, log_id="log-a"))
    await delay.enqueue(_delay_record(topic_id=topic_id, log_id="log-b"))
    assert await service._drop_delay(topic_id=topic_id, tenant_id=TENANT_ID) == 2
    assert await service._drop_metadata(topic_id=topic_id) == 4


def test_step_order_is_frozen() -> None:
    """步骤顺序是契约（先断存储、再删元数据）：顺序被改会让元数据先消失而段目录变孤儿。"""
    assert CLEANUP_STEPS[0] == "messages"
    assert CLEANUP_STEPS[-1] == STEP_METADATA
    assert STEP_DELAY in CLEANUP_STEPS


async def test_impact_tolerates_broken_message_delay_and_dlq(db_session: AsyncSession, tmp_path: Path) -> None:
    """影响预检三处容错：消息 / 延迟 / 死信取数失败各记 0（不阻断删除受理）。"""
    topic_id = await _seed_topic(db_session)
    service, _offsets, _delay, _dead, _throttle = _build(
        db_session,
        tmp_path,
        messages=_BrokenMessageStore(),
        delay=_BrokenDelayStore(quota=10, global_quota=10),
        dead_letters=_BrokenDeadLetters(),
    )
    impact = await service.impact(tenant_id=TENANT_ID, topic_id=topic_id, partition_count=2)
    assert (impact.message_total, impact.delayed_total, impact.dlq_total) == (0, 0, 0)


async def test_advance_skips_unknown_ledger(db_session: AsyncSession, tmp_path: Path) -> None:
    """台账不存在：`_advance` 直接返回（周期扫描与 kick 的防御分支）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    await service._advance(topic_id=999999)


async def test_resume_skips_completed_steps(db_session: AsyncSession, tmp_path: Path) -> None:
    """续跑：台账已记的步骤不再执行（此处预置 messages 步骤，其段数据应保留）。"""
    messages = InMemoryMessageStore()
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path, messages=messages)
    topic_id = await _seed_topic(db_session)
    await _append_messages(messages, topic_id=topic_id, count=1)
    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None
    await TopicCleanupRepository(session).update(int(ledger.id), done_steps=["messages"])
    await session.commit()

    await service.run_once()

    assert await messages.leo(topic_id=topic_id, partition_no=0) == 1
    await session.rollback()
    reloaded = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert reloaded is not None and reloaded.status == CLEANUP_COMPLETED


async def test_complete_tolerates_missing_ledger_and_watermark_failure(
    db_session: AsyncSession, tmp_path: Path
) -> None:
    """收口容错：台账缺失直接返回；水位刷新失败只告警（删除事实已成立）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path)
    await service._complete(
        999999,
        namespace="msg",
        topic_name="nope",
        tenant_id=TENANT_ID,
        counts=ConcurrentStableDict(),
    )

    broken, _offsets2, _delay2, _dead2, _throttle2 = _build(
        db_session, tmp_path, watermark=_BrokenWatermark(root=tmp_path)
    )
    topic_id = await _seed_topic(db_session)
    await broken.run_once()
    session = cast("DbSession", db_session)
    ledger = await TopicCleanupRepository(session).get_by_topic(topic_id)
    assert ledger is not None and ledger.status == CLEANUP_COMPLETED


async def test_stop_cancels_inflight_topic_task(db_session: AsyncSession, tmp_path: Path) -> None:
    """`stop()` 取消在途清理任务（停机不留在途协程）。"""
    service, _offsets, _delay, _dead, _throttle = _build(
        db_session, tmp_path, messages=_SlowMessageStore(), interval_seconds=0.01
    )
    topic_id = await _seed_topic(db_session)
    await db_session.rollback()
    service.kick(topic_id)
    assert len(service._running_topics) == 1
    inflight = list(service._running_topics.values())
    await service.stop()
    for task in inflight:
        with suppress(asyncio.CancelledError):
            await task
    assert len(service._running_topics) == 0


async def test_loop_survives_round_failure(db_session: AsyncSession, tmp_path: Path) -> None:
    """扫描循环吞掉本轮异常并继续（`_loop` 的容错分支）。"""
    service, _offsets, _delay, _dead, _throttle = _build(db_session, tmp_path, interval_seconds=0.01)

    async def _boom() -> int:
        """恒抛错。

        Returns:
            int: 不会返回。

        Raises:
            RuntimeError: 恒抛。
        """
        raise RuntimeError("boom")

    service.run_once = _boom  # type: ignore[method-assign]
    service.ensure_started()
    await asyncio.sleep(0.03)
    await service.stop()
    assert service.running is False
