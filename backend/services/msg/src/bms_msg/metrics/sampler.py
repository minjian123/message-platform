"""异步指标采样器（06-3-1）：积压 / 死信存量 / 重放任务数 / 存储水位。

**为什么是后台任务**：这些事实源需要异步查询与遍历，不能在同步 `/metrics` 请求内做；
采样器按固定周期写入 `MetricsSnapshotStore` 与既有采集器，Collector 只读结果。

**失败语义**：单轮失败只记 warning、保留上一轮快照——不能把「采样失败」表现成「积压归零」，
否则 P1 积压告警会误恢复。停机关闭由 `BackgroundLoops` 调用 `stop()`。
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject
from bms_core.db.tenant import TenantLookup
from sqlalchemy import func, select

from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.watermark import StorageWatermark
from bms_msg.core.config import (
    INGRESS_COUNTER_RETAIN_SECONDS,
    METRICS_SAMPLE_FAILURE_LOG_WINDOW_SECONDS,
    METRICS_SAMPLE_INTERVAL_SECONDS,
)
from bms_msg.metrics.lag import MetricsSnapshotStore, topic_key
from bms_msg.metrics.traffic import decode_labels, get_traffic_metrics
from bms_msg.models.dead_letter import DLQ_ACTIVE_STATUSES
from bms_msg.models.replay import MsgReplayTask
from bms_msg.repositories.overview import IngressCounterRepository, OverviewSnapshotRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.services.migration_ledger import MigrationLedger

__all__ = ["MetricsSampler"]

_LOGGER = get_logger("bms_msg.metrics_sampler")


class MetricsSampler(BaseFrameworkObject):
    """周期采样器（进程级；`ensure_started()` 幂等，`stop()` 幂等）。"""

    def __init__(
        self,
        *,
        snapshots: MetricsSnapshotStore,
        groups: GroupStore,
        messages: MessageStore,
        offsets: OffsetStore,
        dead_letters: DeadLetterQueue,
        uow_factory: UowFactory,
        watermark: StorageWatermark,
        tenant_lookup: TenantLookup,
        migration: MigrationLedger | None = None,
        interval_seconds: float = METRICS_SAMPLE_INTERVAL_SECONDS,
    ) -> None:
        """初始化。

        Args:
            snapshots: 采样快照存储（Collector 读取）。
            groups: 组存储（遍历全部组）。
            messages: 消息存储（取 LEO）。
            offsets: 位点存储（取已提交位点）。
            dead_letters: 死信队列（取当前存量）。
            uow_factory: 平台库工作单元工厂（主题反查 / 重放任务状态统计）。
            watermark: 存储水位（强制刷新）。
            tenant_lookup: 租户源（主键 → 编码）。
            migration: 迁移台账（02-8；None 表示不采样迁移指标）。
            interval_seconds: 采样周期（默认 15s）。
        """
        self._snapshots = snapshots
        self._groups = groups
        self._messages = messages
        self._offsets = offsets
        self._dead_letters = dead_letters
        self._uow_factory = uow_factory
        self._watermark = watermark
        self._tenant_lookup = tenant_lookup
        self._migration = migration
        self._interval = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._tenant_codes: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        self._tenant_ids: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        self._last_messages_in: int | None = None
        self._last_sample_at: float | None = None
        self._last_messages_by_tenant: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        self._ingress_deltas: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        self._topic_keys: ConcurrentStableDict[int, str] = ConcurrentStableDict()
        self._sample_failure_logged: ConcurrentStableDict[str, float] = ConcurrentStableDict()

    @property
    def running(self) -> bool:
        """是否已启动（测试与幂等判定用）。

        Returns:
            bool: 后台循环存活为 True。
        """
        return self._task is not None and not self._task.done()

    def ensure_started(self) -> None:
        """启动采样循环（幂等；需在事件循环内调用）。"""
        if self.running:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止采样循环（幂等）。"""
        if self._task is None:
            return
        task = self._task
        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    async def sample_once(self) -> None:
        """执行一轮采样（各维度独立容错：一个维度失败不影响其它维度）。"""
        try:
            lag = await self._sample_lag()
            self._snapshots.replace(lag=lag)
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="lag", error=type(exc).__name__)
        try:
            dlq = await self._sample_dlq()
            previous = self._snapshots.snapshot.dlq
            growth = ConcurrentStableDict(self._snapshots.snapshot.dlq_growth)
            for key, value in dlq.items():
                delta = value - previous.get(key, 0)
                if delta > 0:
                    growth.set(key, growth.get(key, 0) + delta)
            self._snapshots.replace(dlq=dlq, dlq_growth=growth)
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="dlq", error=type(exc).__name__)
        try:
            replay, replayed, failed = await self._sample_replay()
            self._snapshots.replace(replay=replay, replay_replayed=replayed, replay_failed=failed)
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="replay", error=type(exc).__name__)
        try:
            self._watermark.force_refresh()
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="storage", error=type(exc).__name__)
        try:
            self._sample_ingress()
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="ingress", error=type(exc).__name__)
        try:
            await self._sample_migration()
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="migration", error=type(exc).__name__)
        try:
            await self._write_observability()
        except Exception as exc:
            _LOGGER.warning("msg_metrics_sample_failed", dimension="overview", error=type(exc).__name__)

    async def _sample_migration(self) -> None:
        """按状态汇总迁移台账并写快照（02-8；未装配台账时跳过）。"""
        if self._migration is None:
            return
        status = await self._migration.count_by_status()
        remaining = await self._migration.sum_remaining()
        self._snapshots.replace(migration=status, migration_remaining=remaining)

    def _sample_ingress(self) -> None:
        """差分累计入口消息数，写入本周期速率快照与**每租户增量**（06-1-1 / 02-8-4）。

        只读 `TrafficMetrics` 进程级单例的累计 Counter，不新增采集点、不请求内扫描；
        首轮没有上一轮读数时写 0 / 0.0，端点据此判 `snapshot_available=false`。
        每租户增量留给 `_write_observability` 写入 DB 时间窗（多实例求和即集群总量）。
        """
        counters = get_traffic_metrics().counter_snapshot().get("msg_messages_in_total")
        total = 0
        per_tenant: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        if counters is not None:
            for key, value in counters.items():
                total += value
                code = decode_labels(key).get("tenant")
                if code is not None:
                    per_tenant.set(code, per_tenant.get(code, 0) + value)
        now = time.monotonic()
        deltas: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        if self._last_messages_in is None or self._last_sample_at is None:
            self._snapshots.replace(messages_in_window=0, window_seconds=0.0)
        else:
            elapsed = max(now - self._last_sample_at, 1e-6)
            delta = max(total - self._last_messages_in, 0)
            self._snapshots.replace(messages_in_window=delta, window_seconds=elapsed)
            for code, value in per_tenant.items():
                deltas.set(code, max(value - self._last_messages_by_tenant.get(code, 0), 0))
        self._ingress_deltas = deltas
        self._last_messages_by_tenant = per_tenant
        self._last_messages_in = total
        self._last_sample_at = now

    async def _write_observability(self) -> None:
        """把本轮采样写成跨实例可读的 DB 观测数据（02-8-4）。

        - 每租户一行概览快照（速率 / 积压 / 死信 / 磁盘）；
        - 每租户本拍入口增量累加进当前时间窗；
        - 清理过旧的时间窗。

        失败由 `sample_once` 统一告警：**保留上一份快照**，不把采样失败表现成 0。
        """
        snapshot = self._snapshots.snapshot
        lag_by_tenant: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        dlq_by_tenant: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for key, value in snapshot.lag.items():
            code, _, _topic = key.partition("\x1f")
            lag_by_tenant.set(code, lag_by_tenant.get(code, 0) + value)
        for key, value in snapshot.dlq.items():
            code, _, _topic = key.partition("\x1f")
            dlq_by_tenant.set(code, dlq_by_tenant.get(code, 0) + value)
        disk_ratio = float(self._watermark.reading().used_ratio or 0.0)
        codes: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
        for code in lag_by_tenant:
            codes.set(code, True)
        for code in dlq_by_tenant:
            codes.set(code, True)
        for code in self._ingress_deltas:
            codes.set(code, True)
        if not codes:
            return
        now = time.time()
        interval = max(int(self._interval), 1)
        window_start = int(now) // interval * interval
        async with self._uow_factory() as uow:
            # 前面的读维度可能留下隐式事务；先释放再显式开短事务（写快照 + 计数）。
            await uow.rollback()
            async with uow.begin():
                session = uow.session
                if session is None:  # pragma: no cover - DbUnitOfWork 恒有会话
                    return
                counters = IngressCounterRepository(session)
                overview = OverviewSnapshotRepository(session)
                for code in codes:
                    tenant_id = await self._tenant_id(code)
                    if tenant_id is None:
                        continue
                    await counters.add(
                        tenant_id=tenant_id,
                        window_start=window_start,
                        delta=self._ingress_deltas.get(code, 0),
                    )
                    rate = await self._ingress_rate(counters, tenant_id=tenant_id, now=now, window=window_start)
                    await overview.upsert(
                        tenant_id=tenant_id,
                        messages_in_per_second=rate,
                        consumer_lag=lag_by_tenant.get(code, 0),
                        dlq_total=dlq_by_tenant.get(code, 0),
                        disk_used_ratio=disk_ratio,
                        sampled_at=datetime.now(UTC).replace(tzinfo=None),
                    )
                await counters.purge_before(cutoff=int(now) - INGRESS_COUNTER_RETAIN_SECONDS)
        self._ingress_deltas = ConcurrentStableDict()

    async def _ingress_rate(
        self, counters: IngressCounterRepository, *, tenant_id: int, now: float, window: int
    ) -> float:
        """按最近 4 个时间窗换算每秒入口速率。

        Args:
            counters: 入口计数仓储。
            tenant_id: 租户主键。
            now: 当前 epoch 秒。
            window: 当前窗口起点。

        Returns:
            float: 每秒条数（无数据为 0）。
        """
        interval = max(int(self._interval), 1)
        since = max(int(now) - 3 * interval, window - 3 * interval)
        total, earliest, latest = await counters.sum_since(tenant_id=tenant_id, since=since)
        if total <= 0 or latest <= 0:
            return 0.0
        span = max(latest - earliest + interval, interval)
        return total / span

    async def _tenant_id(self, code: str) -> int | None:
        """把租户编码翻成主键（缓存；查不到返回 None）。

        Args:
            code: 租户编码。

        Returns:
            int | None: 租户主键。
        """
        cached = self._tenant_ids.get(code)
        if cached is not None:
            return cached
        try:
            context = await self._tenant_lookup.by_code(code)
        except Exception:
            return None
        if context.tenant_id is None:  # pragma: no cover - 启用租户恒有主键
            return None
        tenant_id = int(context.tenant_id)
        self._tenant_ids.set(code, tenant_id)
        return tenant_id

    async def _loop(self) -> None:
        """循环：采样后等待周期（首轮立即执行）。"""
        while True:
            await self.sample_once()
            await asyncio.sleep(self._interval)

    async def _sample_lag(self) -> ConcurrentStableDict[str, int]:
        """按 `(tenant, topic)` 汇总积压（所有组、所有分区求和）。

        **按主题隔离**（02-6-1 / B21）：单个未绑定主题 / 单条坏记录只影响自己的快照键；
        失败键保留上一轮值（不归零），其余主题照常更新。主题已被删除（查不到行）时不恢复旧键，
        避免快照永久残留。

        Returns:
            ConcurrentStableDict[str, int]: 快照键 → 积压条数。
        """
        records = await self._groups.list_all()
        previous = self._snapshots.snapshot.lag
        async with self._uow_factory() as uow:
            result = ConcurrentStableDict[str, int]()
            session = uow.session
            if session is None:  # pragma: no cover - DbUnitOfWork 恒有会话
                return result
            topics = TopicRepository(session)
            values: ConcurrentStableDict[str, int] = ConcurrentStableDict()
            seen: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
            failed: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
            for record in records:
                topic_id = int(record.topic_id)
                key = self._topic_keys.get(topic_id)
                try:
                    topic = await topics.get_by_id_unscoped(topic_id)
                    if topic is None:
                        self._topic_keys.pop(topic_id, None)
                        continue
                    code = await self._tenant_code(topic.tenant_id)
                    key = topic_key(tenant=code, topic=topic.name)
                    self._topic_keys.set(topic_id, key)
                    committed_total = 0
                    leo_total = 0
                    for partition_no in range(int(topic.partition_count)):
                        committed_total += await self._offsets.get(
                            group=record.group,
                            topic_id=topic_id,
                            partition_no=partition_no,
                        )
                        leo_total += await self._messages.leo(topic_id=topic_id, partition_no=partition_no)
                    values.set(key, values.get(key, 0) + max(leo_total - committed_total, 0))
                    seen.set(key, True)
                    self._clear_sample_failure("lag", key)
                except Exception as exc:
                    if key is None:
                        self._log_sample_failure("lag", f"topic:{topic_id}", exc)
                        continue
                    seen.set(key, True)
                    failed.set(key, True)
                    self._log_sample_failure("lag", key, exc)
            for key in seen:
                if key in failed:
                    if key in previous:
                        result.set(key, previous[key])
                else:
                    result.set(key, values.get(key, 0))
            return result

    async def _sample_dlq(self) -> ConcurrentStableDict[str, int]:
        """按 `(tenant, topic)` 统计当前死信存量。

        与积压同一隔离口径：单条死信解析失败只影响该主题键，失败键保留上一轮值。

        Returns:
            ConcurrentStableDict[str, int]: 快照键 → 死信条数。
        """
        letters = await self._dead_letters.list_all()
        previous = self._snapshots.snapshot.dlq
        async with self._uow_factory() as uow:
            result = ConcurrentStableDict[str, int]()
            session = uow.session
            if session is None:  # pragma: no cover - DbUnitOfWork 恒有会话
                return result
            topics = TopicRepository(session)
            values: ConcurrentStableDict[str, int] = ConcurrentStableDict()
            seen: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
            failed: ConcurrentStableDict[str, bool] = ConcurrentStableDict()
            for letter in letters:
                if letter.status not in DLQ_ACTIVE_STATUSES:
                    continue
                topic_id = int(letter.topic_id)
                key = self._topic_keys.get(topic_id)
                try:
                    topic = await topics.get_by_id_unscoped(topic_id)
                    if topic is None:
                        self._topic_keys.pop(topic_id, None)
                        continue
                    code = await self._tenant_code(topic.tenant_id)
                    key = topic_key(tenant=code, topic=topic.name)
                    self._topic_keys.set(topic_id, key)
                    values.set(key, values.get(key, 0) + 1)
                    seen.set(key, True)
                    self._clear_sample_failure("dlq", key)
                except Exception as exc:
                    key = key or await self._letter_key(letter)
                    if key is None:
                        self._log_sample_failure("dlq", f"topic:{topic_id}", exc)
                        continue
                    seen.set(key, True)
                    failed.set(key, True)
                    self._log_sample_failure("dlq", key, exc)
            for key in seen:
                if key in failed:
                    if key in previous:
                        result.set(key, previous[key])
                else:
                    result.set(key, values.get(key, 0))
            return result

    async def _letter_key(self, letter: object) -> str | None:
        """死信行反查失败时的键回退（用行内快照字段，不再查主题表）。

        Args:
            letter: 死信行（含 `tenant_id` / `topic`）。

        Returns:
            str | None: 快照键；字段缺失时 None。
        """
        tenant_id = getattr(letter, "tenant_id", None)
        topic = getattr(letter, "topic", None)
        if tenant_id is None or not topic:
            return None
        code = await self._tenant_code(int(tenant_id))
        return topic_key(tenant=code, topic=str(topic))

    def _log_sample_failure(self, dimension: str, key: str, exc: Exception) -> None:
        """按 60s 窗口记一条采样失败日志（同维度同键只一条）。

        Args:
            dimension: 维度（`lag` / `dlq`）。
            key: 快照键或 `topic:{id}`。
            exc: 失败原因。
        """
        marker = f"{dimension}:{key}"
        now = time.monotonic()
        last = self._sample_failure_logged.get(marker)
        if last is not None and now - last < METRICS_SAMPLE_FAILURE_LOG_WINDOW_SECONDS:
            return
        self._sample_failure_logged.set(marker, now)
        _LOGGER.warning(
            "msg_metrics_sample_failed",
            dimension=dimension,
            key=key,
            error=type(exc).__name__,
        )

    def _clear_sample_failure(self, dimension: str, key: str) -> None:
        """成功采样后清失败标记并记一条恢复日志（无失败标记时空转）。

        Args:
            dimension: 维度（`lag` / `dlq`）。
            key: 快照键。
        """
        marker = f"{dimension}:{key}"
        if marker not in self._sample_failure_logged:
            return
        self._sample_failure_logged.pop(marker, None)
        _LOGGER.info("msg_metrics_sample_recovered", dimension=dimension, key=key)

    async def _sample_replay(self) -> tuple[ConcurrentStableDict[str, int], int, int]:
        """按状态统计重放任务数，并汇总累计重放条数与失败任务数。

        Returns:
            tuple[ConcurrentStableDict[str, int], int, int]: 状态计数 / 累计写入条数 / 失败任务数。
        """
        result: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        replayed = 0
        failed = 0
        async with self._uow_factory() as uow:
            session = uow.session
            if session is None:  # pragma: no cover - DbUnitOfWork 恒有会话
                return result, replayed, failed
            rows = await session.execute(select(MsgReplayTask.status, func.count()).group_by(MsgReplayTask.status))
            for status, count in rows.all():
                result.set(str(status), int(count))
                if str(status) == "failed":
                    failed = int(count)
            total = await session.execute(select(func.coalesce(func.sum(MsgReplayTask.replayed), 0)))
            replayed = int(total.scalar_one())
        return result, replayed, failed

    async def _tenant_code(self, tenant_id: int) -> str:
        """把租户主键翻成编码（缓存；失败回退主键字符串）。

        Args:
            tenant_id: 租户主键。

        Returns:
            str: 租户编码（查不到时为 `str(tenant_id)`）。
        """
        key = str(tenant_id)
        cached = self._tenant_codes.get(key)
        if cached is not None:
            return cached
        try:
            context = await self._tenant_lookup.by_id(key, readonly=True)
        except Exception:
            code = key
        else:
            code = context.code
        self._tenant_codes.set(key, code)
        return code
