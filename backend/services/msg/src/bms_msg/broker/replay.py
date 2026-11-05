"""消息重放（01-6 详细设计 §5~§7）：范围解析 / 任务化执行 / 限速 / 审计留痕。

四条不可动摇的口径：

1. **重放是新的物理写入**：保留原 `message_id`、分配**新的 `log_id`**、载荷 / 头 / 键 / 链路标识原样复制，
   `published_at` 取**写入时刻**（保留期从自己的写入开始算，避免「刚重放就被清掉」）。
2. **不破坏原顺序链**：只往分区**尾部** `append`，原记录与位点一个都不动；重放记录自然进入停等顺序链
   （队头未 ack 就排在后面），因此 01-3 的停等规则一行不用改。任务的扫描上界锁定为**建任务时刻的 `LEO`**，
   重放写入的新记录不在扫描区间内（不会自噬）。
3. **不互斥、不加锁**：offset 仍由 `append` 唯一分配，内存实现单事件循环内无 `await` 切换点；
   不去争 `DeliveryState.lock`（那是 `consume` / `ack` 的临界区，大任务会把消费饿死）。
4. **任务终态不可逆**：`completed` / `cancelled` / `failed` 不提供重开，重试即按范围重建任务；
   `failed` 不自动重试——重放是运维主动操作，失败就该显式停下来看（与延迟转投「必重试」刻意相反）。

**审计两条**：建任务一条、终态一条（`cancelled` 由请求侧记，`completed` / `failed` 由执行器记），
载荷带范围、结果与**首末 `log_id`**——能一次寻址到具体记录，又不因逐条 `log_id` 产生审计洪泛。
审计失败只告警、不影响重放结果（与基座 `AuditEventEmitter` 同口径）。
"""

import asyncio
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

from bms_core.audit.events import build_operation_event
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseDataContract, BaseValueObject
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.events.base import EventEnvelope

from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import (
    MS_PER_SECOND,
    REPLAY_BURST_SECONDS,
    REPLAY_ERROR_MSG_MAX_LENGTH,
    REPLAY_SCAN_BATCH,
    REPLAY_TASK_BATCH,
    REPLAY_TICK_MS,
)
from bms_msg.core.message_id import new_message_id
from bms_msg.errors import MsgReplayTaskInvalidStateError, MsgReplayTaskNotFoundError
from bms_msg.models.replay import (
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PAUSED,
    TASK_PENDING,
    TASK_RUNNING,
    MsgReplayTask,
)
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = [
    "REPLAY_ACTION",
    "REPLAY_PATH",
    "AuditSink",
    "ReplayAuditor",
    "ReplayRunner",
    "ReplayService",
    "ReplayWindow",
    "UowFactory",
]

_LOGGER = get_logger("bms_msg.broker.replay")

REPLAY_ACTION = "msg:replay"
"""审计动作（与权限码同名：`sys_operation_log.action` 可直接按权限码检索）。"""

REPLAY_PATH = "/api/v1/msg/admin/replay"
"""审计路径（创建与终态共用同一路径：一条重放操作的两次留痕据此归并）。"""

UowFactory = Callable[[], AbstractAsyncContextManager[UnitOfWork]]
"""工作单元工厂（执行器在无请求上下文时自开会话；用例注入内存库会话）。"""


class AuditSink(Protocol):
    """审计出口（最小协议）：基座 `AuditEventEmitter` 结构上即满足，用例可注入替身。"""

    async def emit(self, event: EventEnvelope) -> bool:
        """写一条审计事件。

        Args:
            event: 事件信封。

        Returns:
            bool: 写入成功 True（失败只告警）。
        """
        ...


def _utcnow() -> datetime:
    """当前 UTC 时间（限速令牌与终态时刻的判据；用例注入可推进时钟）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


def _utc_naive(value: datetime) -> datetime:
    """把时间规整为 UTC naive（与库列口径一致：`time_from` / `time_to` / `finished_at` 均为 UTC naive）。

    Args:
        value: 带时区或 naive 的时间（naive 按 UTC 解释）。

    Returns:
        datetime: UTC naive 时间。
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class ReplayWindow(BaseValueObject):
    """重放范围条件（已归一：offset 左闭右开、时间为 UTC naive、key 精确匹配）。"""

    offset_from: int | None
    """起始 offset（含）；None 不限。"""

    offset_to: int | None
    """结束 offset（不含）；None 不限。"""

    time_from: datetime | None
    """起始发布时间（含，UTC naive）；None 不限。"""

    time_to: datetime | None
    """结束发布时间（不含，UTC naive）；None 不限。"""

    key: str | None
    """分区键（精确匹配）；None 不过滤。"""

    @classmethod
    def of(cls, request: ReplayRequest) -> ReplayWindow:
        """由请求构造窗口（时间统一转 UTC naive，避免与记录的 aware 时间比较时报错）。

        Args:
            request: 建任务请求。

        Returns:
            ReplayWindow: 归一后的范围条件。
        """
        return cls(
            offset_from=request.offset_from,
            offset_to=request.offset_to,
            time_from=None if request.time_from is None else _utc_naive(request.time_from),
            time_to=None if request.time_to is None else _utc_naive(request.time_to),
            key=request.key,
        )

    @classmethod
    def of_task(cls, task: MsgReplayTask) -> ReplayWindow:
        """由任务行还原窗口（执行器推进时复述同一条件）。

        Args:
            task: 任务行。

        Returns:
            ReplayWindow: 范围条件。
        """
        return cls(
            offset_from=task.offset_from,
            offset_to=task.offset_to,
            time_from=task.time_from,
            time_to=task.time_to,
            key=task.filter_key,
        )

    def matches(self, message: StoredMessage) -> bool:
        """判断一条记录是否命中范围（offset 区间由扫描范围保证，此处只判时间与 key）。

        Args:
            message: 存储中的记录。

        Returns:
            bool: 命中 True。
        """
        published = _utc_naive(message.published_at)
        if self.time_from is not None and published < self.time_from:
            return False
        if self.time_to is not None and published >= self.time_to:
            return False
        return self.key is None or message.key == self.key


class ReplayService:
    """重放任务服务（请求级：平台库会话 + 进程级消息存储）。

    判定顺序（详细设计 §8.1）：命名空间（`350010`）→ 主题（`350001`）→ 分区号（`350009`）
    → 范围计数 → 落任务。任务查询 / 控制先取任务（`350016`）再判状态（`350017`）。
    """

    def __init__(
        self,
        *,
        tasks: ReplayTaskRepository,
        router: TopicRouter,
        store: MessageStore,
        uow: UnitOfWork,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        """初始化。

        Args:
            tasks: 任务仓储（同会话）。
            router: 主题路由（请求级，绑定平台库主题仓储）。
            store: 消息存储（范围计数与重放写入）。
            uow: 工作单元（事务边界）。
            now: 时钟（缺省 UTC 当前时间）。
        """
        self._tasks = tasks
        self._router = router
        self._store = store
        self._uow = uow
        self._now = now

    async def create(self, *, tenant: CurrentTenant, request: ReplayRequest) -> MsgReplayTask:
        """建重放任务（范围解析 + 精确计数 + 落库）。

        Args:
            tenant: 当前租户。
            request: 建任务请求。

        Returns:
            MsgReplayTask: 新建任务（`status = pending`，等待执行器拾取）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 分区号越界（350009）。
        """
        tenant.ensure_namespace(request.namespace)
        async with self._uow.begin():
            topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
            topic_id = int(topic.id)
            partitions = self._resolve_partitions(topic=topic, requested=request.partitions)
            window = ReplayWindow.of(request)
            cursors, total = await self._scan(topic_id=topic_id, partitions=partitions, window=window)
            task = await self._tasks.create(
                tenant_id=int(tenant.tenant_id),
                namespace=request.namespace,
                topic_id=topic_id,
                topic=topic.name,
                partitions=partitions,
                offset_from=window.offset_from,
                offset_to=window.offset_to,
                time_from=window.time_from,
                time_to=window.time_to,
                filter_key=window.key,
                rate_per_second=request.rate_per_second,
                status=TASK_PENDING,
                total=total,
                replayed=0,
                cursor_state=cursors,
            )
        return task

    async def get(self, *, tenant: CurrentTenant, task_id: int) -> MsgReplayTask:
        """取任务快照（进度查询）。

        Args:
            tenant: 当前租户（显式归属校验）。
            task_id: 任务 ID。

        Returns:
            MsgReplayTask: 任务行。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
        """
        async with self._uow.begin():
            return await self._load(tenant=tenant, task_id=task_id)

    async def pause(self, *, tenant: CurrentTenant, task_id: int) -> MsgReplayTask:
        """暂停任务（`pending` / `running` → `paused`，批边界生效）。

        Args:
            tenant: 当前租户（显式归属校验）。
            task_id: 任务 ID。

        Returns:
            MsgReplayTask: 迁移后的任务行。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
            MsgReplayTaskInvalidStateError: 已暂停或已终态（350017）。
        """
        return await self._transition(
            tenant=tenant, task_id=task_id, allowed=(TASK_PENDING, TASK_RUNNING), target=TASK_PAUSED
        )

    async def resume(self, *, tenant: CurrentTenant, task_id: int) -> MsgReplayTask:
        """恢复任务（`paused` → `running`）。

        Args:
            tenant: 当前租户（显式归属校验）。
            task_id: 任务 ID。

        Returns:
            MsgReplayTask: 迁移后的任务行。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
            MsgReplayTaskInvalidStateError: 未处于暂停（350017）。
        """
        return await self._transition(tenant=tenant, task_id=task_id, allowed=(TASK_PAUSED,), target=TASK_RUNNING)

    async def cancel(self, *, tenant: CurrentTenant, task_id: int) -> MsgReplayTask:
        """取消任务（非终态 → `cancelled`，批边界生效；已写入的记录不回滚）。

        Args:
            tenant: 当前租户（显式归属校验）。
            task_id: 任务 ID。

        Returns:
            MsgReplayTask: 迁移后的任务行（`finished_at` 已置）。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
            MsgReplayTaskInvalidStateError: 已是终态（350017）。
        """
        return await self._transition(
            tenant=tenant,
            task_id=task_id,
            allowed=(TASK_PENDING, TASK_RUNNING, TASK_PAUSED),
            target=TASK_CANCELLED,
        )

    async def _load(self, *, tenant: CurrentTenant, task_id: int) -> MsgReplayTask:
        """取任务行并显式校验归属，不满足即 `350016`（不让「别人的任务」泄露存在性）。

        Args:
            tenant: 当前租户。
            task_id: 任务 ID。

        Returns:
            MsgReplayTask: 任务行。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
        """
        task = await self._tasks.get(task_id)
        if task is None or int(task.tenant_id) != int(tenant.tenant_id):
            raise MsgReplayTaskNotFoundError(f"重放任务不存在：{task_id}")
        return task

    async def _transition(
        self, *, tenant: CurrentTenant, task_id: int, allowed: tuple[str, ...], target: str
    ) -> MsgReplayTask:
        """状态迁移（终态不可逆；取消时置 `finished_at`）。

        Args:
            tenant: 当前租户。
            task_id: 任务 ID。
            allowed: 允许的来源状态。
            target: 目标状态。

        Returns:
            MsgReplayTask: 迁移后的任务行。

        Raises:
            MsgReplayTaskNotFoundError: 不存在或不属于当前租户（350016）。
            MsgReplayTaskInvalidStateError: 来源状态不允许该迁移（350017）。
        """
        async with self._uow.begin():
            task = await self._load(tenant=tenant, task_id=task_id)
            if task.status not in allowed:
                raise MsgReplayTaskInvalidStateError(f"重放任务状态不允许该操作：{task.status} → {target}")
            values: ConcurrentStableDict[str, object] = ConcurrentStableDict({"status": target})
            if target == TASK_CANCELLED:
                values.set("finished_at", _utc_naive(self._now()))
            updated = await self._tasks.update(task_id, **dict(values.items()))
        return updated or task

    def _resolve_partitions(
        self, *, topic: MsgTopic, requested: ConcurrentStableList[int] | None
    ) -> ConcurrentStableList[int]:
        """解析分区集合（缺省全部分区；给定时逐个校验越界并去重排序）。

        Args:
            topic: 主题记录（`MsgTopic`）。
            requested: 请求给的分区号列表；None 表示全部分区。

        Returns:
            ConcurrentStableList[int]: 分区号（升序，去重）。

        Raises:
            MsgPartitionCountInvalidError: 分区号越界（350009）。
        """
        if requested is None:
            return ConcurrentStableList(range(int(topic.partition_count)))
        for partition_no in requested:
            self._router.partition(topic, partition_no)
        return ConcurrentStableList(sorted(set(requested)))

    async def _scan(
        self, *, topic_id: int, partitions: ConcurrentStableList[int], window: ReplayWindow
    ) -> tuple[ConcurrentStableList[ConcurrentStableDict[str, int]], int]:
        """按范围扫描各分区，产出游标与精确总量。

        条件区间先与本分区可读区间 `[start_offset, leo)` 取交（**裁剪而非报错**：历史已清理是正常状态）；
        裁剪后为空的分区游标 `next == end`，执行器直接跳过。

        Args:
            topic_id: 主题主键。
            partitions: 分区号列表。
            window: 范围条件。

        Returns:
            tuple[ConcurrentStableList[ConcurrentStableDict[str, int]], int]: 游标清单 + 命中总条数。
        """
        cursors: ConcurrentStableList[ConcurrentStableDict[str, int]] = ConcurrentStableList()
        total = 0
        for partition_no in partitions:
            start = await self._store.start_offset(topic_id=topic_id, partition_no=partition_no)
            leo = await self._store.leo(topic_id=topic_id, partition_no=partition_no)
            begin = start if window.offset_from is None else max(window.offset_from, start)
            end = leo if window.offset_to is None else min(window.offset_to, leo)
            end = max(end, begin)
            total += await self._count(
                topic_id=topic_id, partition_no=partition_no, begin=begin, end=end, window=window
            )
            cursors.add(ConcurrentStableDict({"partition": partition_no, "next": begin, "end": end}))
        return cursors, total

    async def _count(self, *, topic_id: int, partition_no: int, begin: int, end: int, window: ReplayWindow) -> int:
        """逐批扫描并统计命中条数（进度分母必须准确，否则「已投递 / 总量」失去意义）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            begin: 扫描起点（含）。
            end: 扫描终点（不含）。
            window: 范围条件。

        Returns:
            int: 命中条数。
        """
        matched = 0
        cursor = begin
        while cursor < end:
            batch = await self._store.fetch(
                topic_id=topic_id,
                partition_no=partition_no,
                from_offset=cursor,
                max_messages=min(REPLAY_SCAN_BATCH, end - cursor),
            )
            if not batch:
                break
            for message in batch:
                cursor = message.offset + 1
                if window.matches(message):
                    matched += 1
        return matched


@dataclass
class _RunState(BaseDataContract):
    """单轮推进的累计状态（预算 / 已写入 / 首末记录标识 / 失败原因）。"""

    budget: int
    """本轮剩余可写条数。"""

    written: int = 0
    """本轮已写入条数。"""

    first: str | None = None
    """首条成功写入的记录标识。"""

    last: str | None = None
    """末条成功写入的记录标识。"""

    error: str | None = None
    """失败原因（非空即本轮中止并把任务置 `failed`）。"""


class ReplayRunner:
    """重放执行器（进程级单例）：按限速推进任务、批边界响应暂停 / 取消、终态写审计。

    生命周期（详细设计 §6.4）：**惰性启动**——首个建任务请求触发 `ensure_started()`；`stop()` 幂等；
    `_loop` 里单轮异常只记日志，**不得让任务静默退出**（否则后续任务再也不会被推进）。
    """

    def __init__(
        self,
        *,
        messages: MessageStore,
        uow_factory: UowFactory,
        auditor: ReplayAuditor,
        now: Callable[[], datetime] = _utcnow,
        interval_ms: int = REPLAY_TICK_MS,
        batch_limit: int = REPLAY_TASK_BATCH,
    ) -> None:
        """初始化（不启动任务：由 `ensure_started()` 决定）。

        Args:
            messages: 消息存储（重放写入的唯一落点）。
            uow_factory: 工作单元工厂（无请求上下文时自开会话）。
            auditor: 审计记录器（终态留痕）。
            now: 时钟（缺省 UTC 当前时间；用例注入可推进时钟）。
            interval_ms: 推进周期（毫秒）。
            batch_limit: 单轮最多推进的任务数。
        """
        self._messages = messages
        self._uow_factory = uow_factory
        self._auditor = auditor
        self._now = now
        self._interval_ms = interval_ms
        self._batch_limit = batch_limit
        self._tokens: dict[int, float] = {}
        self._last: dict[int, datetime] = {}
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        """执行任务是否在跑（`ensure_started` 幂等与用例判据）。

        Returns:
            bool: 有活跃任务即 True。
        """
        return self._task is not None

    def ensure_started(self) -> None:
        """惰性启动推进任务（幂等：已有活跃任务即返回；重入安全）。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止推进任务（幂等：无任务即返回；不等待在途批次之外的东西）。"""
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def run_once(self) -> int:
        """单步推进（不 sleep，用例可精确驱动）。

        Returns:
            int: 本轮写入条数。
        """
        now = self._now()
        finished: ConcurrentStableList[MsgReplayTask] = ConcurrentStableList()
        written = 0
        for task_id in await self._active_ids():
            count, task = await self._advance(task_id=task_id, now=now)
            written += count
            if task is not None:
                finished.add(task)
        for task in finished:
            await self._auditor.record_finished(task=task)
        return written

    async def _active_ids(self) -> ConcurrentStableList[int]:
        """取待推进任务主键（`pending` / `running`）。

        Returns:
            ConcurrentStableList[int]: 任务主键（升序）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ReplayTaskRepository(cast("DbSession", uow.session))
            return ConcurrentStableList(int(task.id) for task in await repository.list_active(limit=self._batch_limit))

    async def _advance(self, *, task_id: int, now: datetime) -> tuple[int, MsgReplayTask | None]:
        """推进一步（重新读状态 → 按令牌额度写入 → 落库）。

        Args:
            task_id: 任务主键。
            now: 当前时刻（令牌与终态时刻判据）。

        Returns:
            tuple[int, MsgReplayTask | None]: 本轮写入条数 + 终态任务（非终态为 None）。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ReplayTaskRepository(cast("DbSession", uow.session))
            task = await repository.get(task_id)
            if task is None or task.status not in (TASK_PENDING, TASK_RUNNING):
                self._forget(task_id)
                return 0, None
            budget = self._budget(task_id=task_id, rate=int(task.rate_per_second), now=now)
            state = _RunState(budget=budget, first=task.log_id_first, last=task.log_id_last)
            cursors = ConcurrentStableList(ConcurrentStableDict(item) for item in task.cursor_state)
            window = ReplayWindow.of_task(task)
            if state.budget > 0:
                for cursor in cursors:
                    await self._advance_cursor(cursor=cursor, task=task, window=window, state=state)
                    if state.error is not None or state.budget <= 0:
                        break
            values = self._progress_values(task=task, cursors=cursors, state=state, now=now)
            updated = await repository.update(task_id, **dict(values.items()))
            finished = updated if updated is not None and updated.status in (TASK_COMPLETED, TASK_FAILED) else None
            if finished is not None:
                self._forget(task_id)
            return state.written, finished

    async def _advance_cursor(
        self, *, cursor: ConcurrentStableDict[str, int], task: MsgReplayTask, window: ReplayWindow, state: _RunState
    ) -> None:
        """推进单个分区游标（命中即写入，额度用完回退到下轮）。

        Args:
            cursor: 分区游标（`partition` / `next` / `end`，原地更新）。
            task: 任务行（取主题与分区）。
            window: 范围条件。
            state: 本轮累计状态（原地更新）。
        """
        partition_no = cursor["partition"]
        next_offset = cursor["next"]
        end = cursor["end"]
        while next_offset < end and state.budget > 0:
            batch = await self._messages.fetch(
                topic_id=int(task.topic_id),
                partition_no=partition_no,
                from_offset=next_offset,
                max_messages=min(REPLAY_SCAN_BATCH, end - next_offset),
            )
            if not batch:
                next_offset = end
                break
            for message in batch:
                if not window.matches(message):
                    next_offset = message.offset + 1
                    continue
                if state.budget <= 0:
                    break
                try:
                    stored = await self._append(task=task, partition_no=partition_no, message=message)
                except Exception as exc:  # 存储故障 / 主题被停用：任务转 failed，不静默重试
                    state.error = f"{type(exc).__name__}: {exc}"
                    break
                next_offset = message.offset + 1
                state.budget -= 1
                state.written += 1
                state.first = state.first or stored.log_id
                state.last = stored.log_id
            else:
                continue
            break
        cursor.set("next", next_offset)

    async def _append(self, *, task: MsgReplayTask, partition_no: int, message: StoredMessage) -> StoredMessage:
        """把一条历史记录重新写入分区尾部（新 `log_id`、原 `message_id`、载荷不变）。

        Args:
            task: 任务行（取主题主键）。
            partition_no: 目标分区。
            message: 原记录。

        Returns:
            StoredMessage: 新写入的记录（含新 offset 与 `log_id`）。
        """
        return await self._messages.append(
            topic_id=int(task.topic_id),
            partition_no=partition_no,
            message_id=message.message_id,
            log_id=new_message_id(),
            key=message.key,
            payload=message.payload,
            headers=message.headers,
            trace_id=message.trace_id,
            published_at=self._now(),
            content_type=message.content_type,
            compression=message.compression,
            uncompressed_len=message.uncompressed_len,
        )

    def _progress_values(
        self,
        *,
        task: MsgReplayTask,
        cursors: ConcurrentStableList[ConcurrentStableDict[str, int]],
        state: _RunState,
        now: datetime,
    ) -> ConcurrentStableDict[str, object]:
        """计算本轮要落库的字段（进度 / 游标 / 状态 / 终态时刻）。

        Args:
            task: 任务行。
            cursors: 推进后的游标。
            state: 本轮累计状态。
            now: 当前时刻。

        Returns:
            ConcurrentStableDict[str, object]: 待更新字段。
        """
        values: ConcurrentStableDict[str, object] = ConcurrentStableDict(
            {
                "cursor_state": cursors,
                "replayed": int(task.replayed) + state.written,
                "log_id_first": state.first,
                "log_id_last": state.last,
            }
        )
        if state.error is not None:
            values.set("status", TASK_FAILED)
            values.set("error_msg", state.error[:REPLAY_ERROR_MSG_MAX_LENGTH])
            values.set("finished_at", _utc_naive(now))
        elif all(cursor["next"] >= cursor["end"] for cursor in cursors):
            values.set("status", TASK_COMPLETED)
            values.set("finished_at", _utc_naive(now))
        else:
            values.set("status", TASK_RUNNING)
        return values

    def _budget(self, *, task_id: int, rate: int, now: datetime) -> int:
        """按经过时间累计令牌并取本轮额度（首次拾取给足 1 秒额度，让任务立刻动起来）。

        Args:
            task_id: 任务主键。
            rate: 速率（条/秒）。
            now: 当前时刻。

        Returns:
            int: 本轮可写条数。
        """
        cap = float(rate * REPLAY_BURST_SECONDS)
        last = self._last.get(task_id)
        tokens = cap if last is None else self._tokens.get(task_id, 0.0) + rate * max((now - last).total_seconds(), 0.0)
        tokens = min(tokens, cap)
        whole = int(tokens)
        self._tokens[task_id] = tokens - whole
        self._last[task_id] = now
        return whole

    def _forget(self, task_id: int) -> None:
        """清理已终结 / 不可推进任务的令牌状态（避免长跑进程里字典无限增长）。

        Args:
            task_id: 任务主键。
        """
        self._tokens.pop(task_id, None)
        self._last.pop(task_id, None)

    async def _loop(self) -> None:
        """推进循环：先等一个周期再推进（ticker 语义），单轮异常不终止任务。

        **不在启动瞬间插队执行**：`ensure_started()` 由建任务请求触发，若立刻推进就会在请求未结束时
        抢同一会话做读写；按周期等到下一拍，请求侧与执行器的事务边界互不重叠。
        """
        while True:
            await asyncio.sleep(self._interval_ms / MS_PER_SECOND)
            try:
                await self.run_once()
            except Exception:  # 台账写失败 / 存储抖动：只记日志，下一轮继续
                _LOGGER.exception("replay_tick_failed")


class ReplayAuditor:
    """重放审计记录器（两条：建任务 / 终态；失败只告警，不影响重放结果）。"""

    def __init__(self, *, sink: AuditSink) -> None:
        """初始化。

        Args:
            sink: 审计出口（生产为基座 `AuditEventEmitter`，用例注入替身）。
        """
        self._sink = sink

    async def record_created(
        self, *, task: MsgReplayTask, actor_id: int | None, ip: str | None, trace_id: str | None
    ) -> bool:
        """记录「建任务」。

        Args:
            task: 新建任务。
            actor_id: 操作人主键（请求上下文）。
            ip: 来源 IP。
            trace_id: 链路标识。

        Returns:
            bool: 写入成功 True（失败只告警）。
        """
        return await self._emit(
            task=task, actor_id=actor_id if actor_id is not None else task.created_by, ip=ip, trace_id=trace_id
        )

    async def record_cancelled(
        self, *, task: MsgReplayTask, actor_id: int | None, ip: str | None, trace_id: str | None
    ) -> bool:
        """记录「取消」（操作人有真实请求上下文）。

        Args:
            task: 取消后的任务。
            actor_id: 操作人主键。
            ip: 来源 IP。
            trace_id: 链路标识。

        Returns:
            bool: 写入成功 True（失败只告警）。
        """
        return await self._emit(
            task=task, actor_id=actor_id if actor_id is not None else task.created_by, ip=ip, trace_id=trace_id
        )

    async def record_finished(self, *, task: MsgReplayTask) -> bool:
        """记录「完成 / 失败」（执行器触发：无请求上下文，操作人取任务创建人）。

        Args:
            task: 终态任务。

        Returns:
            bool: 写入成功 True（失败只告警）。
        """
        return await self._emit(task=task, actor_id=task.created_by, ip=None, trace_id=None)

    async def _emit(self, *, task: MsgReplayTask, actor_id: int | None, ip: str | None, trace_id: str | None) -> bool:
        """组装并发射一条操作审计事件（复用 `sys.operation.logged`：审计链只认这两种事件类型）。

        Args:
            task: 任务行。
            actor_id: 操作人主键（可空）。
            ip: 来源 IP（执行器触发时为空）。
            trace_id: 链路标识（执行器触发时为空）。

        Returns:
            bool: 写入成功 True（异常一律吞掉并告警）。
        """
        event = build_operation_event(
            tenant_id=str(task.tenant_id),
            user_id=actor_id,
            username=None,
            module="msg",
            action=REPLAY_ACTION,
            method="POST",
            path=REPLAY_PATH,
            query=None,
            params=_scope_payload(task),
            ip=ip,
            user_agent=None,
            cost_time=None,
            status=200,
            error_msg=task.error_msg,
            trace_id=trace_id,
        )
        try:
            return await self._sink.emit(event)
        except Exception as exc:  # 审计出口实现异常不得影响重放结果（与基座同口径）
            _LOGGER.warning("replay_audit_failed", task_id=str(task.id), error=repr(exc))
            return False


def _scope_payload(task: MsgReplayTask) -> str:
    """构造审计载荷（范围 + 结果 + 首末 `log_id`；空字段不落，保持载荷紧凑）。

    Args:
        task: 任务行。

    Returns:
        str: JSON 串（落 `sys_operation_log.params`）。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "task_id": str(task.id),
            "namespace": task.namespace,
            "topic": task.topic,
            "partitions": list(task.partitions),
            "rate_per_second": int(task.rate_per_second),
            "status": task.status,
            "total": int(task.total),
            "replayed": int(task.replayed),
        }
    )
    optional: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "offset_from": task.offset_from,
            "offset_to": task.offset_to,
            "time_from": None if task.time_from is None else task.time_from.isoformat(),
            "time_to": None if task.time_to is None else task.time_to.isoformat(),
            "key": task.filter_key,
            "log_id_first": task.log_id_first,
            "log_id_last": task.log_id_last,
            "error_msg": task.error_msg,
        }
    )
    for name, value in optional.items():
        if value is not None:
            payload.set(name, value)
    return json.dumps(dict(payload.items()), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
