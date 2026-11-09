"""删主题的异步资源清理编排（06-2 需求第 2 条、详细设计 §7）。

**为什么是独立服务而不是塞进 `TopicService`**：清理要操作七个跨域端口（消息存储 / 延迟区 / 位点 /
消费组 / 死信 / 进程内运行态 / 元数据），且运行在**无请求上下文**的后台任务里；塞进主题服务会让
「请求级会话」与「后台自开会话」两套事务边界混在一个类里。

**三个不变量**：

1. **受理即执行、失败自愈**：受理时 `kick()` 起首轮，周期扫描兜底（进程退出 / 上一轮失败）；
2. **每步幂等且立即落库**：`done_steps` 逐步骤追加并提交，重试从失败步骤续跑，已完成步骤不重跑；
3. **不设失败终态**：可自愈故障（磁盘满 / 句柄占用）恢复后由下一轮收口；连续失败只升级日志级别。

**不得绕过端口**：清理只经 `MessageStore` / `DelayStore` / `OffsetPurger` / `GroupStore` /
`DeadLetterQueue` 动手，不直接改日志文件、不直接 UPDATE 消息记录（需求 06-2 第 8 条）。
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject
from bms_core.db.session import DbSession

from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetPurger
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.broker.watermark import StorageWatermark
from bms_msg.core.config import TOPIC_CLEANUP_FAILURE_ALERT_ATTEMPTS, TOPIC_CLEANUP_SCAN_INTERVAL_SECONDS
from bms_msg.models.topic_cleanup import (
    CLEANUP_COMPLETED,
    CLEANUP_PENDING,
    CLEANUP_RUNNING,
    CLEANUP_STEPS,
    STEP_DELAY,
    STEP_DLQ,
    STEP_GROUPS,
    STEP_MESSAGES,
    STEP_METADATA,
    STEP_OFFSETS,
    STEP_RUNTIME,
)
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.repositories.topic_cleanup import TopicCleanupRepository
from bms_msg.schemas.topic import DeleteImpact

__all__ = ["TopicCleanupService"]

_LOGGER = get_logger("bms_msg.topic_cleanup")

_MAX_ERROR_LEN = 512
"""`last_error` 列宽（超出截断，避免长栈把行撑爆）。"""


class TopicCleanupService(BaseFrameworkObject):
    """删除清理编排（进程级；`ensure_started()` 与 `stop()` 均幂等）。"""

    def __init__(
        self,
        *,
        messages: MessageStore,
        offsets: OffsetPurger,
        groups: GroupStore,
        delay: DelayStore,
        wheel: DelayWheel,
        dead_letters: DeadLetterQueue,
        delivery: DeliveryState,
        waiter: DeliveryWaiter,
        throttle: RetentionThrottle,
        directory: PartitionDirectory,
        cache: TopicCache,
        watermark: StorageWatermark,
        uow_factory: UowFactory,
        interval_seconds: float = TOPIC_CLEANUP_SCAN_INTERVAL_SECONDS,
    ) -> None:
        """初始化。

        Args:
            messages: 消息存储（删段）。
            offsets: 位点清理端口（按主题删）。
            groups: 组存储（按主题清）。
            delay: 延迟区存储（按主题清）。
            wheel: 延迟时间轮（清该主题的到期条目；与 05-5 租户清理同口径）。
            dead_letters: 死信队列（按主题清）。
            delivery: 投递状态（清在途 / 暂存确认）。
            waiter: 分区唤醒器（清版本表）。
            throttle: 保留清理节流窗口表（清窗口）。
            directory: 分区目录登记表（清登记）。
            cache: 主题快照缓存（清理完成失效）。
            watermark: 存储水位（清理后强制刷新）。
            uow_factory: 平台库工作单元工厂（后台自开会话）。
            interval_seconds: 周期扫描间隔（秒）。
        """
        self._messages = messages
        self._offsets = offsets
        self._groups = groups
        self._delay = delay
        self._wheel = wheel
        self._dead_letters = dead_letters
        self._delivery = delivery
        self._waiter = waiter
        self._throttle = throttle
        self._directory = directory
        self._cache = cache
        self._watermark = watermark
        self._uow_factory = uow_factory
        self._interval = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._running_topics: ConcurrentStableDict[int, asyncio.Task[None]] = ConcurrentStableDict()

    @property
    def running(self) -> bool:
        """扫描循环是否存活（测试与幂等判定用）。

        Returns:
            bool: 存活为 True。
        """
        return self._task is not None and not self._task.done()

    def ensure_started(self) -> None:
        """启动周期扫描（幂等；需在事件循环内调用）。"""
        if self.running:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止扫描循环与在途清理任务（幂等）。"""
        for task in list(self._running_topics.values()):
            task.cancel()
        self._running_topics.clear()
        if self._task is None:
            return
        task = self._task
        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    def kick(self, topic_id: int) -> None:
        """受理后立即起一轮（不等待完成；同一主题在途时不重复起）。

        Args:
            topic_id: 主题主键。
        """
        target = int(topic_id)
        existing = self._running_topics.get(target)
        if existing is not None and not existing.done():
            return
        task = asyncio.create_task(self._guarded(target))
        self._running_topics.set(target, task)
        task.add_done_callback(lambda _task: self._running_topics.pop(target, None))

    async def run_once(self) -> int:
        """扫一轮待办台账并逐个推进（返回推进的主题数）。

        Returns:
            int: 本轮推进的主题数。
        """
        rows = await self._active_rows()
        advanced = 0
        for row in rows:
            await self._advance(topic_id=row)
            advanced += 1
        return advanced

    async def impact(self, *, tenant_id: int, topic_id: int, partition_count: int) -> DeleteImpact:
        """算删主题的影响范围（二次确认弹窗的唯一数据源）。

        **单个维度取数失败即记 0 并告警**：预检是「给人看的估值」，不该因为延迟区读不出来就整单拒绝；
        真正的保护在后端的租户 / 状态判定与二次确认。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            partition_count: 主题分区数（已由调用方从主题行取到）。

        Returns:
            DeleteImpact: 五项影响。
        """
        message_total = 0
        for partition_no in range(int(partition_count)):
            try:
                leo = await self._messages.leo(topic_id=topic_id, partition_no=partition_no)
                start = await self._messages.start_offset(topic_id=topic_id, partition_no=partition_no)
                message_total += max(leo - start, 0)
            except Exception as exc:
                _LOGGER.warning(
                    "msg_topic_delete_impact_failed",
                    dimension="messages",
                    topic_id=int(topic_id),
                    partition_no=partition_no,
                    error=type(exc).__name__,
                )
        group_count = 0
        try:
            for record in await self._groups.list_all():
                if record.topic_id == int(topic_id):
                    group_count += 1
        except Exception as exc:
            _LOGGER.warning(
                "msg_topic_delete_impact_failed", dimension="groups", topic_id=int(topic_id), error=type(exc).__name__
            )
        delayed_total = 0
        try:
            for record in await self._delay.list_for_tenant(tenant_id=int(tenant_id)):
                if record.topic_id == int(topic_id):
                    delayed_total += 1
        except Exception as exc:
            _LOGGER.warning(
                "msg_topic_delete_impact_failed", dimension="delay", topic_id=int(topic_id), error=type(exc).__name__
            )
        dlq_total = 0
        try:
            for letter in await self._dead_letters.list_all():
                if letter.topic_id == int(topic_id):
                    dlq_total += 1
        except Exception as exc:
            _LOGGER.warning(
                "msg_topic_delete_impact_failed", dimension="dlq", topic_id=int(topic_id), error=type(exc).__name__
            )
        return DeleteImpact(
            partition_count=int(partition_count),
            message_total=message_total,
            group_count=group_count,
            delayed_total=delayed_total,
            dlq_total=dlq_total,
        )

    async def _guarded(self, topic_id: int) -> None:
        """推进单主题并吞掉异常（后台任务不能把异常抛到事件循环）。

        Args:
            topic_id: 主题主键。
        """
        try:
            await self._advance(topic_id=topic_id)
        except Exception as exc:  # 兜底：本轮失败留给下一轮扫描
            _LOGGER.warning("msg_topic_cleanup_round_failed", topic_id=topic_id, error=type(exc).__name__)

    async def _loop(self) -> None:
        """周期扫描（首轮立即执行，便于重启后马上续跑未完成台账）。"""
        while True:
            try:
                await self.run_once()
            except Exception as exc:
                _LOGGER.warning("msg_topic_cleanup_scan_failed", error=type(exc).__name__)
            await asyncio.sleep(self._interval)

    async def _active_rows(self) -> ConcurrentStableList[int]:
        """取待办台账对应的主题主键。

        Returns:
            ConcurrentStableList[int]: 主题主键（按受理时间升序）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = TopicCleanupRepository(cast("DbSession", uow.session))
            return ConcurrentStableList(int(row.topic_id) for row in await repository.list_active())

    async def _advance(self, *, topic_id: int) -> None:
        """推进一轮：置 running / 逐步骤执行 / 失败留痕。

        Args:
            topic_id: 主题主键。
        """
        snapshot = await self._begin_round(topic_id)
        if snapshot is None:
            return
        tenant_id, namespace, topic_name, done = snapshot
        counts: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for step in CLEANUP_STEPS:
            if step in done:
                continue
            try:
                removed = await self._run_step(step=step, topic_id=topic_id, tenant_id=tenant_id)
            except Exception as exc:
                await self._fail(topic_id, error=f"{step}: {type(exc).__name__}: {exc}")
                return
            counts.set(step, removed)
            done.add(step)
            await self._record_step(topic_id, done=done)
        await self._complete(topic_id, namespace=namespace, topic_name=topic_name, tenant_id=tenant_id, counts=counts)

    async def _begin_round(self, topic_id: int) -> tuple[int, str, str, ConcurrentStableList[str]] | None:
        """把台账置 running 并返回本轮快照（已完成 / 不存在返回 None）。

        **快照在事务内取**：提交后再读 ORM 属性会触发过期刷新（回滚分支直接失效，02-3 踩过）。

        Args:
            topic_id: 主题主键。

        Returns:
            tuple[int, str, str, ConcurrentStableList[str]] | None: （租户, 命名空间, 主题名, 已完成步骤）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = TopicCleanupRepository(cast("DbSession", uow.session))
            row = await repository.get_by_topic(topic_id)
            if row is None or row.status == CLEANUP_COMPLETED:
                return None
            values: ConcurrentStableDict[str, object] = ConcurrentStableDict(
                {
                    "status": CLEANUP_RUNNING,
                    "attempts": int(row.attempts) + 1,
                    "started_at": row.started_at or _now(),
                }
            )
            await repository.update(int(row.id), **dict(values))
            return (
                int(row.tenant_id),
                row.namespace,
                row.topic,
                ConcurrentStableList(str(item) for item in row.done_steps),
            )

    async def _record_step(self, topic_id: int, *, done: ConcurrentStableList[str]) -> None:
        """把「已完成某步」落库（**每步一提交**，重试从失败步骤续跑）。

        Args:
            topic_id: 主题主键。
            done: 当前已完成步骤集合。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = TopicCleanupRepository(cast("DbSession", uow.session))
            row = await repository.get_by_topic(topic_id)
            if row is None:
                return
            await repository.update(int(row.id), done_steps=list(done))

    async def _fail(self, topic_id: int, *, error: str) -> None:
        """记录本轮失败并把台账退回 `pending`（下一轮重试；不设失败终态）。

        Args:
            topic_id: 主题主键。
            error: 失败原因（截断到列宽）。
        """
        message = error[:_MAX_ERROR_LEN]
        async with self._uow_factory() as uow, uow.begin():
            repository = TopicCleanupRepository(cast("DbSession", uow.session))
            row = await repository.get_by_topic(topic_id)
            if row is None:
                return
            attempts = int(row.attempts)
            await repository.update(int(row.id), status=CLEANUP_PENDING, last_error=message)
        if attempts >= TOPIC_CLEANUP_FAILURE_ALERT_ATTEMPTS:
            _LOGGER.warning(
                "msg_topic_cleanup_failed",
                topic_id=int(topic_id),
                attempts=attempts,
                last_error=message,
            )
        else:
            _LOGGER.info("msg_topic_cleanup_retry", topic_id=int(topic_id), attempts=attempts, last_error=message)

    async def _complete(
        self,
        topic_id: int,
        *,
        namespace: str,
        topic_name: str,
        tenant_id: int,
        counts: ConcurrentStableDict[str, int],
    ) -> None:
        """收口：台账置 completed、失效缓存、刷新水位。

        Args:
            topic_id: 主题主键。
            namespace: 命名空间。
            topic_name: 主题名。
            tenant_id: 租户主键。
            counts: 各步骤删除计数（结构化日志用）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = TopicCleanupRepository(cast("DbSession", uow.session))
            row = await repository.get_by_topic(topic_id)
            if row is None:
                return
            await repository.update(int(row.id), status=CLEANUP_COMPLETED, finished_at=_now())
        self._cache.invalidate(tenant_id=tenant_id, namespace=namespace, name=topic_name)
        try:
            self._watermark.force_refresh()
        except Exception as exc:  # 水位刷新失败不影响「删除已完成」的事实
            _LOGGER.warning("msg_topic_cleanup_watermark_failed", topic_id=int(topic_id), error=type(exc).__name__)
        _LOGGER.info(
            "msg_topic_cleanup_completed",
            topic_id=int(topic_id),
            namespace=namespace,
            topic=topic_name,
            **dict(counts),
        )

    async def _run_step(self, *, step: str, topic_id: int, tenant_id: int) -> int:
        """执行单个清理步骤（返回删除计数）。

        Args:
            step: 步骤键。
            topic_id: 主题主键。
            tenant_id: 租户主键。

        Returns:
            int: 该步骤的删除计数（无法计数时为受影响对象数）。

        Raises:
            ValueError: 步骤键不在闭集内（登记错即失败，不静默跳过）。
        """
        if step == STEP_MESSAGES:
            return await self._messages.drop_topic(topic_id=topic_id)
        if step == STEP_DELAY:
            return await self._drop_delay(topic_id=topic_id, tenant_id=tenant_id)
        if step == STEP_OFFSETS:
            return await self._offsets.purge_topic(topic_id=topic_id)
        if step == STEP_GROUPS:
            return await self._groups.purge_topic(topic_id=topic_id)
        if step == STEP_DLQ:
            return await self._dead_letters.purge_topics(topic_ids=ConcurrentStableList([int(topic_id)]))
        if step == STEP_RUNTIME:
            return await self._drop_runtime(topic_id=topic_id)
        if step == STEP_METADATA:
            return await self._drop_metadata(topic_id=topic_id)
        raise ValueError(f"未知清理步骤：{step}")

    async def _drop_delay(self, *, topic_id: int, tenant_id: int) -> int:
        """清理该主题的延迟区记录与时间轮条目。

        Args:
            topic_id: 主题主键。
            tenant_id: 租户主键。

        Returns:
            int: 删除的记录数。
        """
        removed = 0
        for record in await self._delay.list_for_tenant(tenant_id=int(tenant_id)):
            if record.topic_id != int(topic_id):
                continue
            await self._delay.remove(log_id=record.log_id)
            self._wheel.forget(log_id=record.log_id)
            removed += 1
        return removed

    async def _drop_runtime(self, *, topic_id: int) -> int:
        """清理进程内运行态（在途 / 节流窗口 / 唤醒器 / 目录登记）。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 清理的分区数（取自分区行）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = PartitionRepository(cast("DbSession", uow.session))
            partitions = await repository.list_by_topic(topic_id)
            numbers = ConcurrentStableList(int(item.partition_no) for item in partitions)
        for partition_no in numbers:
            self._throttle.forget(topic_id=int(topic_id), partition_no=partition_no)
        self._delivery.drop_topic(topic_id=int(topic_id))
        self._waiter.forget_topic(topic_id=int(topic_id))
        self._directory.forget(int(topic_id))
        return len(numbers)

    async def _drop_metadata(self, *, topic_id: int) -> int:
        """物理删除分区行与主题行（清理的最后一步）。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的行数（分区行 + 主题行）。
        """
        async with self._uow_factory() as uow, uow.begin():
            partitions = PartitionRepository(cast("DbSession", uow.session))
            removed = await partitions.delete_by_topic(int(topic_id))
            topics = TopicRepository(cast("DbSession", uow.session))
            removed += 1 if await topics.delete(int(topic_id)) else 0
        return removed


def _now() -> datetime:
    """当前 UTC 时刻（台账时间列用）。

    Returns:
        datetime: 带时区的当前时刻。
    """
    return datetime.now(UTC)
