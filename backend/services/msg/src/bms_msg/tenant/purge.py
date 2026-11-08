"""租户到期清理：计划（dry-run）、执行与后台清扫（05-5 详细设计 §8 / §9）。

- **按主题保留期**：每个主题 `deleted_event_at + retention_ms` 到期；`purge_after` 取最早到期点。
- **先关句柄、再删目录**：`FileMessageStore.close_tenant()` / `FileOffsetStore.purge_topic()` 依赖
  `PartitionDirectory` 定位租户，因此目录登记留到删位点之后。
- **失败不阻塞基座删除**：事件动作只做易失清理与排期；物理删除由本服务重试。清扫失败记
  `failure_count` / `last_error`，下轮继续；审计失败不标记 `purged_at`，下轮补审计。
- **不新增 HTTP 端点**：`plan()` 是进程内 dry-run，供测试与运维脚本调用；`run_once()` 由启动钩子
  周期驱动。
"""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject
from bms_core.db.session import DbSession

from bms_msg.core.config import (
    DEFAULT_RETENTION_MS,
    TENANT_PURGE_BATCH_LIMIT,
    TENANT_PURGE_SCAN_INTERVAL_MS,
)
from bms_msg.models.replay import TASK_PENDING, TASK_RUNNING
from bms_msg.quota.cache import QuotaCache
from bms_msg.repositories.quota import QuotaRepository
from bms_msg.repositories.tenant_lifecycle import TenantReplayRef, TenantTopicRef
from bms_msg.tenant.ledger import TenantLifecycleLedger
from bms_msg.tenant.runtime import TenantLifecycleRuntime
from bms_msg.usage.snapshot import StorageUsageSampler

__all__ = [
    "TenantPurgeMetrics",
    "TenantPurgePlan",
    "TenantPurgeResult",
    "TenantPurgeService",
    "TenantPurgeTopic",
    "get_tenant_purge_metrics",
    "reset_tenant_purge_metrics",
]

_LOGGER = get_logger("bms_msg.tenant_purge")


@dataclass
class TenantPurgeMetrics:
    """清扫进程内计数（外部指标出口归 06-3）。"""

    scanned: int = 0
    """本轮扫描的到期租户数。"""

    purged: int = 0
    """完成清理的租户数。"""

    deferred: int = 0
    """因活跃重放任务延后的租户数。"""

    failed: int = 0
    """清理失败的租户数。"""

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """返回当前计数快照。

        Returns:
            ConcurrentStableDict[str, int]: 四计数。
        """
        return ConcurrentStableDict(
            {
                "scanned": self.scanned,
                "purged": self.purged,
                "deferred": self.deferred,
                "failed": self.failed,
            }
        )


_PURGE_METRICS = TenantPurgeMetrics()
"""进程级清扫计数（测试经 `reset_tenant_purge_metrics()` 隔离）。"""


def reset_tenant_purge_metrics() -> None:
    """清零进程级清扫计数（测试隔离用）。"""
    _PURGE_METRICS.scanned = 0
    _PURGE_METRICS.purged = 0
    _PURGE_METRICS.deferred = 0
    _PURGE_METRICS.failed = 0


def get_tenant_purge_metrics() -> TenantPurgeMetrics:
    """取进程级清扫计数。

    Returns:
        TenantPurgeMetrics: 进程级计数容器。
    """
    return _PURGE_METRICS


@dataclass(frozen=True)
class TenantPurgeTopic(BaseValueObject):
    """一个主题的到期视图（计划与执行共用）。"""

    topic_id: int
    """主题主键。"""

    namespace: str
    """命名空间（孤儿目录为目录名）。"""

    name: str
    """主题名。"""

    partition_count: int
    """分区数（孤儿目录为 0）。"""

    retention_ms: int
    """保留时长（孤儿目录为 0）。"""

    deadline: datetime
    """到期时刻（UTC naive）。"""

    due: bool
    """本轮是否到期。"""

    data_path: Path
    """主题数据目录。"""

    orphan: bool = False
    """是否 DB 无对应主题行的孤儿目录。"""


@dataclass(frozen=True)
class TenantPurgePlan(BaseValueObject):
    """一次 dry-run / 清扫计划（不执行任何删除）。"""

    tenant_id: int
    """租户主键。"""

    as_of: datetime
    """计划时刻（UTC naive）。"""

    topics: ConcurrentStableFrozenList[TenantPurgeTopic]
    """全部主题（含未到期与孤儿）。"""

    next_purge_after: datetime | None
    """剩余未到期主题的最早到期点；None 表示无剩余。"""

    connections: int
    """当前连接数。"""

    streams: int
    """当前未取消长驻流数。"""

    delay_records: int
    """延迟区记录数。"""

    group_records: int
    """组记录数。"""

    dead_letters: int
    """死信数。"""

    replay_tasks: ConcurrentStableFrozenList[TenantReplayRef]
    """重放任务清单。"""

    data_root_exists: bool
    """租户数据目录是否存在。"""

    offsets_root_exists: bool
    """租户位点目录是否存在。"""

    deferred_reason: str | None = None
    """延后原因（`active_replay`）；None 表示可执行。"""

    @property
    def due_topics(self) -> ConcurrentStableList[TenantPurgeTopic]:
        """本轮到期主题（含孤儿目录）。

        Returns:
            tuple[TenantPurgeTopic, ...]: 到期主题。
        """
        return ConcurrentStableList(topic for topic in self.topics if topic.due)

    @property
    def due_topic_ids(self) -> ConcurrentStableList[int]:
        """本轮到期主题主键（去重、升序）。

        Returns:
            tuple[int, ...]: 主题主键。
        """
        return ConcurrentStableList(sorted({topic.topic_id for topic in self.due_topics}))

    def to_payload(self) -> ConcurrentStableDict[str, object]:
        """转审计 / 日志载荷（不包含 Path，保证 JSON 可序列化）。

        Returns:
            dict[str, object]: 载荷。
        """
        return ConcurrentStableDict(
            {
                "tenant_id": self.tenant_id,
                "as_of": self.as_of.isoformat(),
                "topics": len(self.topics),
                "due_topics": len(self.due_topics),
                "next_purge_after": None if self.next_purge_after is None else self.next_purge_after.isoformat(),
                "connections": self.connections,
                "streams": self.streams,
                "delay_records": self.delay_records,
                "group_records": self.group_records,
                "dead_letters": self.dead_letters,
                "replay_tasks": len(self.replay_tasks),
                "data_root_exists": self.data_root_exists,
                "offsets_root_exists": self.offsets_root_exists,
                "deferred_reason": self.deferred_reason,
            }
        )


def _empty_errors() -> ConcurrentStableFrozenList[str]:
    """构造空错误清单（值对象默认工厂）。

    Returns:
        ConcurrentStableFrozenList[str]: 空清单。
    """
    return ConcurrentStableFrozenList()


@dataclass(frozen=True)
class TenantPurgeResult(BaseValueObject):
    """一次执行的核对结果。"""

    tenant_id: int
    """租户主键。"""

    deleted_topics: int = 0
    """删除的主题数。"""

    deleted_data_dirs: int = 0
    """删除的数据目录数。"""

    deleted_offset_files: int = 0
    """删除的位点文件数。"""

    deleted_db_rows: int = 0
    """删除的 DB 行数。"""

    deferred: bool = False
    """是否因活跃重放任务延后。"""

    purged: bool = False
    """是否完成该租户全部清理。"""

    errors: ConcurrentStableFrozenList[str] = field(default_factory=_empty_errors)
    """失败原因（空表示成功）。"""

    def to_payload(self) -> ConcurrentStableDict[str, object]:
        """转审计 / 日志载荷。

        Returns:
            dict[str, object]: 载荷。
        """
        return ConcurrentStableDict(
            {
                "tenant_id": self.tenant_id,
                "deleted_topics": self.deleted_topics,
                "deleted_data_dirs": self.deleted_data_dirs,
                "deleted_offset_files": self.deleted_offset_files,
                "deleted_db_rows": self.deleted_db_rows,
                "deferred": self.deferred,
                "purged": self.purged,
                "errors": tuple(self.errors),
            }
        )


class TenantPurgeService:
    """到期清扫服务：计划、执行与周期驱动。"""

    def __init__(
        self,
        *,
        ledger: TenantLifecycleLedger,
        runtime: TenantLifecycleRuntime,
        data_root: Path,
        offsets_root: Path,
        interval_ms: int = TENANT_PURGE_SCAN_INTERVAL_MS,
        batch_limit: int = TENANT_PURGE_BATCH_LIMIT,
        now: Callable[[], datetime] | None = None,
        metrics: TenantPurgeMetrics | None = None,
        quota_session_scope: Callable[[], AbstractAsyncContextManager[DbSession]] | None = None,
        quota_cache: QuotaCache | None = None,
        storage_sampler: StorageUsageSampler | None = None,
    ) -> None:
        """初始化（不启动任务）。

        Args:
            ledger: 生命周期账本。
            runtime: 生命周期运行时（资源句柄）。
            data_root: 消息数据根。
            offsets_root: 位点数据根。
            interval_ms: 清扫周期（毫秒）。
            batch_limit: 单轮最多处理的到期租户数。
            now: 时钟（缺省 UTC naive；用例注入）。
            metrics: 清扫计数（缺省进程级）。
            quota_session_scope: 配额清理的平台库短会话入口（None = 不删配额行）。
            quota_cache: 配额快照缓存（清理后失效）。
            storage_sampler: 存储用量采样器（清理后失效）。
        """
        self._ledger = ledger
        self._runtime = runtime
        self._data_root = Path(data_root)
        self._offsets_root = Path(offsets_root)
        self._interval_ms = interval_ms
        self._batch_limit = batch_limit
        self._now = now if now is not None else _utc_now_naive
        self._metrics = metrics if metrics is not None else _PURGE_METRICS
        self._quota_session_scope = quota_session_scope
        self._quota_cache = quota_cache
        self._storage_sampler = storage_sampler
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        """清扫循环是否在跑。

        Returns:
            bool: 有活跃任务即 True。
        """
        return self._task is not None

    def ensure_started(self) -> None:
        """惰性启动清扫循环（幂等）。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止清扫循环（幂等）。"""
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def run_once(self, *, now: datetime | None = None) -> int:
        """扫描一轮到期待清理租户并执行（不 sleep，用例可精确驱动）。

        Args:
            now: 当前时刻（缺省取注入时钟）。

        Returns:
            int: 本轮扫描的到期租户数。
        """
        if self._lock.locked():
            return 0
        async with self._lock:
            current = _utc_naive(now) or self._now()
            due = await self._ledger.list_due_purges(now=current, limit=self._batch_limit)
            for item in due:
                self._metrics.scanned += 1
                try:
                    plan = await self.plan(item.tenant_id, now=current)
                    result = await self.execute(plan)
                except Exception as exc:  # 单租户失败不阻断其它租户
                    self._metrics.failed += 1
                    await self._record_failure(item.tenant_id, exc)
                    _LOGGER.warning(
                        "msg_tenant_purge_failed",
                        tenant_id=item.tenant_id,
                        error=repr(exc),
                    )
                    continue
                if result.deferred:
                    self._metrics.deferred += 1
                elif result.purged:
                    self._metrics.purged += 1
                else:
                    self._metrics.failed += 1
            return len(due)

    async def plan(self, tenant_id: int, *, now: datetime | None = None) -> TenantPurgePlan:
        """构建清理计划（纯读，dry-run）。

        Args:
            tenant_id: 租户主键。
            now: 计划时刻（缺省取注入时钟）。

        Returns:
            TenantPurgePlan: 计划。
        """
        current = _utc_naive(now) or self._now()
        state = await self._ledger.get_deleted(tenant_id=tenant_id)
        if state is None or state.purged_at is not None:
            return self._empty_plan(tenant_id=tenant_id, current=current)
        base_at = state.deleted_event_at or current
        topics = await self._ledger.list_topics(tenant_id=tenant_id)
        known = ConcurrentStableSet(topic.topic_id for topic in topics)
        planned = [self._topic_plan(topic=topic, base_at=base_at, current=current) for topic in topics]
        planned.extend(self._orphan_topics(tenant_id=tenant_id, known=known, current=current))
        remaining = [topic.deadline for topic in planned if not topic.due]
        replay_tasks = await self._ledger.list_replay_tasks(tenant_id=tenant_id)
        due_ids = {topic.topic_id for topic in planned if topic.due}
        active = [
            task for task in replay_tasks if task.topic_id in due_ids and task.status in (TASK_PENDING, TASK_RUNNING)
        ]
        return TenantPurgePlan(
            tenant_id=int(tenant_id),
            as_of=current,
            topics=ConcurrentStableFrozenList(planned),
            next_purge_after=min(remaining) if remaining else None,
            connections=sum(1 for record in self._runtime.connections.records() if record.tenant_id == int(tenant_id)),
            streams=sum(
                1
                for session in self._runtime.streams.sessions()
                if session.tenant_id == int(tenant_id) and not session.cancelled
            ),
            delay_records=await self._runtime.delay_store.count_for_tenant(tenant_id=int(tenant_id)),
            group_records=len(await self._runtime.groups.list_for_tenant(tenant_id=int(tenant_id))),
            dead_letters=sum(
                1
                for letter in await self._runtime.dead_letters.list_all()
                if letter.topic_id in ConcurrentStableSet(topic.topic_id for topic in planned)
            ),
            replay_tasks=ConcurrentStableFrozenList(replay_tasks),
            data_root_exists=(self._data_root / str(int(tenant_id))).is_dir(),
            offsets_root_exists=(self._offsets_root / str(int(tenant_id))).is_dir(),
            deferred_reason="active_replay" if active else None,
        )

    async def execute(self, plan: TenantPurgePlan) -> TenantPurgeResult:
        """执行清理计划（幂等；已删资源按成功处理）。

        Args:
            plan: 清理计划。

        Returns:
            TenantPurgeResult: 执行结果。
        """
        tenant_id = int(plan.tenant_id)
        if plan.deferred_reason is not None:
            cancelled = await self._ledger.cancel_active_replay_tasks(tenant_id=tenant_id, at=plan.as_of)
            _LOGGER.info(
                "msg_tenant_purge_deferred",
                tenant_id=tenant_id,
                reason=plan.deferred_reason,
                replay_tasks_cancelled=cancelled,
            )
            return TenantPurgeResult(tenant_id=tenant_id, deferred=True)

        due = plan.due_topics
        if not due:
            remaining = await self._ledger.count_topics(tenant_id=tenant_id)
            if remaining == 0:
                return await self._finalize(plan=plan, result=TenantPurgeResult(tenant_id=tenant_id))
            return TenantPurgeResult(tenant_id=tenant_id)

        due_ids = plan.due_topic_ids
        await self._runtime.message_store.close_tenant(tenant_id=tenant_id)
        await self._runtime.groups.purge_tenant(tenant_id=tenant_id)
        await self._runtime.dead_letters.purge_topics(topic_ids=due_ids)
        self._runtime.delay_store.drop_tenant(tenant_id=tenant_id)
        self._runtime.delay_wheel.forget_tenant(tenant_id=tenant_id)
        for topic in due:
            self._runtime.delivery_state.drop_topic(topic_id=topic.topic_id)
            self._runtime.waiter.forget_topic(topic_id=topic.topic_id)
            self._runtime.topic_cache.invalidate(tenant_id=tenant_id, namespace=topic.namespace, name=topic.name)
            for partition_no in range(topic.partition_count):
                self._runtime.retention_throttle.forget(topic_id=topic.topic_id, partition_no=partition_no)

        deleted_offset_files = 0
        for topic in due:
            deleted_offset_files += await self._runtime.offset_store.purge_topic(topic_id=topic.topic_id)
        deleted_data_dirs = 0
        for topic in due:
            if _safe_rmtree(topic.data_path, root=self._data_root):
                deleted_data_dirs += 1
        deleted_db_rows = await self._ledger.delete_topics(tenant_id=tenant_id, topic_ids=due_ids)
        for topic in due:
            self._runtime.directory.forget(topic.topic_id)
        result = TenantPurgeResult(
            tenant_id=tenant_id,
            deleted_topics=len(due),
            deleted_data_dirs=deleted_data_dirs,
            deleted_offset_files=deleted_offset_files,
            deleted_db_rows=deleted_db_rows,
        )
        remaining = await self._ledger.count_topics(tenant_id=tenant_id)
        if remaining == 0:
            return await self._finalize(plan=plan, result=result)
        if plan.next_purge_after is not None:
            await self._ledger.set_purge_after(tenant_id=tenant_id, at=plan.next_purge_after)
        # 计划快照后新增主题属并发防御；已删除租户不应再建主题，正常路径必走上面的分支。
        else:  # pragma: no cover - 计划外新增主题的防御分支
            await self._ledger.set_purge_after(tenant_id=tenant_id, at=plan.as_of + timedelta(days=1))
        await self._runtime.record_audit(
            action="msg:tenant.purge",
            path=f"/api/v1/msg/admin/tenants/{tenant_id}/purge",
            tenant_id=tenant_id,
            payload=ConcurrentStableDict({**plan.to_payload(), **result.to_payload(), "result": "applied"}),
            strong=True,
        )
        _LOGGER.info("msg_tenant_purge_completed", **result.to_payload())
        return result

    async def _finalize(self, *, plan: TenantPurgePlan, result: TenantPurgeResult) -> TenantPurgeResult:
        """删除租户根目录并标记完成（含强审计）。

        Args:
            plan: 清理计划。
            result: 当前结果。

        Returns:
            TenantPurgeResult: 完成结果。
        """
        tenant_id = int(plan.tenant_id)
        _safe_rmtree(self._data_root / str(tenant_id), root=self._data_root)
        _safe_rmtree(self._offsets_root / str(tenant_id), root=self._offsets_root)
        final = TenantPurgeResult(
            tenant_id=tenant_id,
            deleted_topics=result.deleted_topics,
            deleted_data_dirs=result.deleted_data_dirs,
            deleted_offset_files=result.deleted_offset_files,
            deleted_db_rows=result.deleted_db_rows,
            purged=True,
        )
        try:
            await self._runtime.record_audit(
                action="msg:tenant.purge",
                path=f"/api/v1/msg/admin/tenants/{tenant_id}/purge",
                tenant_id=tenant_id,
                payload=ConcurrentStableDict({**plan.to_payload(), **final.to_payload(), "result": "purged"}),
                strong=True,
            )
        except Exception as exc:
            await self._record_failure(tenant_id, exc)
            raise
        await self._purge_quota(tenant_id)
        await self._ledger.mark_purged(tenant_id=tenant_id, at=plan.as_of)
        _LOGGER.info("msg_tenant_purge_completed", **final.to_payload())
        return final

    async def _purge_quota(self, tenant_id: int) -> None:
        """删除租户配额行并失效相关缓存（05-2；失败上抛由调用方记失败）。

        Args:
            tenant_id: 租户主键。
        """
        if self._quota_session_scope is not None:
            async with self._quota_session_scope() as session:
                await QuotaRepository(session).delete_for_tenant(tenant_id=tenant_id)
                await session.commit()
        if self._quota_cache is not None:
            self._quota_cache.invalidate(tenant_id)
        if self._storage_sampler is not None:
            self._storage_sampler.invalidate(tenant_id)

    async def _record_failure(self, tenant_id: int, exc: Exception) -> None:
        """记录清扫失败（账本 + 日志）。

        Args:
            tenant_id: 租户主键。
            exc: 失败原因。
        """
        try:
            await self._ledger.record_purge_failure(tenant_id=tenant_id, error=repr(exc))
        except Exception as inner:  # 记录失败不掩盖原始异常
            _LOGGER.warning("msg_tenant_purge_failure_record_failed", tenant_id=tenant_id, error=repr(inner))

    async def _loop(self) -> None:
        """清扫循环：单轮异常不终止任务。"""
        while True:
            try:
                await self.run_once()
            except Exception:
                _LOGGER.exception("msg_tenant_purge_tick_failed")
            await asyncio.sleep(self._interval_ms / 1000)

    def _empty_plan(self, *, tenant_id: int, current: datetime) -> TenantPurgePlan:
        """构造空计划（无删除行 / 已完成）。

        Args:
            tenant_id: 租户主键。
            current: 当前时刻。

        Returns:
            TenantPurgePlan: 空计划。
        """
        return TenantPurgePlan(
            tenant_id=int(tenant_id),
            as_of=current,
            topics=ConcurrentStableFrozenList(),
            next_purge_after=None,
            connections=0,
            streams=0,
            delay_records=0,
            group_records=0,
            dead_letters=0,
            replay_tasks=ConcurrentStableFrozenList(),
            data_root_exists=(self._data_root / str(int(tenant_id))).is_dir(),
            offsets_root_exists=(self._offsets_root / str(int(tenant_id))).is_dir(),
        )

    def _topic_plan(self, *, topic: TenantTopicRef, base_at: datetime, current: datetime) -> TenantPurgeTopic:
        """把一个 DB 主题转成计划项。

        Args:
            topic: 主题最小视图。
            base_at: 删除事件生效时刻。
            current: 当前时刻。

        Returns:
            TenantPurgeTopic: 计划项。
        """
        retention = topic.retention_ms if topic.retention_ms > 0 else DEFAULT_RETENTION_MS
        deadline = base_at + timedelta(milliseconds=retention)
        return TenantPurgeTopic(
            topic_id=topic.topic_id,
            namespace=topic.namespace,
            name=topic.name,
            partition_count=topic.partition_count,
            retention_ms=retention,
            deadline=deadline,
            due=deadline <= current,
            data_path=(self._data_root / str(topic.tenant_id) / topic.namespace / f"{topic.name}.{topic.topic_id}"),
        )

    def _orphan_topics(
        self,
        *,
        tenant_id: int,
        known: ConcurrentStableSet[int],
        current: datetime,
    ) -> ConcurrentStableList[TenantPurgeTopic]:
        """扫描 DB 无对应行的孤儿主题目录（目录兜底）。

        Args:
            tenant_id: 租户主键。
            known: DB 已知主题主键。
            current: 当前时刻。

        Returns:
            list[TenantPurgeTopic]: 孤儿主题计划项。
        """
        tenant_root = self._data_root / str(int(tenant_id))
        if not tenant_root.is_dir():
            return ConcurrentStableList()
        orphaned: ConcurrentStableList[TenantPurgeTopic] = ConcurrentStableList()
        for namespace_dir in sorted(path for path in tenant_root.iterdir() if path.is_dir()):
            for topic_dir in sorted(path for path in namespace_dir.iterdir() if path.is_dir()):
                stem, _dot, suffix = topic_dir.name.rpartition(".")
                if not stem or not suffix.isdigit():
                    continue
                topic_id = int(suffix)
                if topic_id in known:
                    continue
                orphaned.add(
                    TenantPurgeTopic(
                        topic_id=topic_id,
                        namespace=namespace_dir.name,
                        name=stem,
                        partition_count=0,
                        retention_ms=0,
                        deadline=current,
                        due=True,
                        data_path=topic_dir,
                        orphan=True,
                    )
                )
        return orphaned


def _utc_now_naive() -> datetime:
    """当前 UTC naive 时间（与库列口径一致）。

    Returns:
        datetime: UTC naive 当前时间。
    """
    return datetime.now(UTC).replace(tzinfo=None)


def _utc_naive(value: datetime | None) -> datetime | None:
    """把时间规整为 UTC naive。

    Args:
        value: 时间（可能带时区）。

    Returns:
        datetime | None: UTC naive；None 原样返回。
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _safe_rmtree(path: Path, *, root: Path) -> bool:
    """安全删除目录（必须位于数据根内，且不能删数据根本身）。

    Args:
        path: 待删目录。
        root: 数据根。

    Returns:
        bool: 实际删除返回 True；不存在返回 False。

    Raises:
        ValueError: 路径越界（防误删）。
    """
    resolved_root = root.resolve()
    target = path.resolve()
    if target == resolved_root or not target.is_relative_to(resolved_root):
        raise ValueError(f"拒绝删除数据根之外的路径：{target}")
    if not target.exists():
        return False
    shutil.rmtree(target)
    return True
