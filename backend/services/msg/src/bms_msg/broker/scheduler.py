"""到期驱动：分层分桶时间轮 + 推进器（01-5 详细设计 §5、§8.4）。

**为什么正确性不依赖「级联」**：桶只用于缩小候选集，真正的判据是逐条 `deliver_at <= now`。
装填放错层顶多多扫几条；取出后未到期的记录**重新装填**（剩余时间必然变小，通常落进更细的层），
因此不会漏投、不会早投——这也是把 L2 留着不级联、把过期记录单独暂存的前提。

**三层（槽位按绝对时间取模，不按「相对指针偏移」）**：

| 层 | 单元 | 桶数 | 覆盖 | 装填条件 |
| --- | --- | --- | --- | --- |
| L0 | 1s | 60 | 60s 内 | 剩余 < 60s |
| L1 | 60s | 60 | 1 小时内 | 剩余 < 3600s |
| L2 | 3600s | 24 | 24 小时内 | 其余 |

**时钟两条纪律**（§5.5）：回拨（`now <= last_scan`）本轮直接返回且指针不后退；前跳单轮最多推进
`DELAY_MAX_CATCHUP_SECONDS` 秒，其余留给后续轮次——不丢、不早投，只是补投变慢。

**为什么过期记录要单独暂存（`_overdue`）**：转投失败 / B 段启动补投的记录 `deliver_at` 已经过去，
按绝对槽位装填会落进「已扫过的秒位」，要等整轮（L0 60s / L1 1 小时）才再被看到；暂存后由下一次
推进首先取出，实现「失败保留 + 下一轮重试」。
"""

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Protocol

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger

from bms_msg.broker.delay import DelayRecord, DelayStore, with_attempt
from bms_msg.broker.log import MessageStore
from bms_msg.core.config import (
    DELAY_BATCH_LIMIT,
    DELAY_FAILURE_LOG_WINDOW_MS,
    DELAY_MAX_CATCHUP_SECONDS,
    DELAY_TICK_MS,
    MS_PER_SECOND,
)

__all__ = [
    "DELAY_LAYER_HOUR",
    "DELAY_LAYER_MINUTE",
    "DELAY_LAYER_SECOND",
    "DelayScheduler",
    "DelayWheel",
    "DurabilityPoint",
    "TenantAvailability",
]

_LOGGER = get_logger("bms_msg.broker.scheduler")

DELAY_LAYER_SECOND = "L0"
"""时间轮 L0：秒级桶（60 桶，覆盖 60s）。"""

DELAY_LAYER_MINUTE = "L1"
"""时间轮 L1：分钟级桶（60 桶，覆盖 1 小时）。"""

DELAY_LAYER_HOUR = "L2"
"""时间轮 L2：小时级桶（24 桶，覆盖 24 小时；首期最长档位 60m 故恒空，保留以免扩档位改结构）。"""

_SECOND_SLOTS = 60
"""L0 桶数（= 覆盖秒数）。"""

_MINUTE_SLOTS = 60
"""L1 桶数（= 覆盖分钟数）。"""

_HOUR_SLOTS = 24
"""L2 桶数（= 覆盖小时数）。"""

_SECONDS_PER_MINUTE = 60
"""一分钟秒数。"""

_SECONDS_PER_HOUR = 3_600
"""一小时秒数。"""


def _utcnow() -> datetime:
    """当前 UTC 时间（推进判据；用例注入可推进时钟）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


class DelayWheel:
    """分层分桶时间轮（纯内存索引：装填 O(1)，推进按秒位取桶）。

    只存记录引用、不缓存 payload；`place` 是唯一装填入口（发布 / 重试 / B 段重建共用），
    `take_due` 是唯一推进入口（`DelayScheduler.run_once` 每轮调用一次）。
    """

    def __init__(self, *, now: Callable[[], datetime] = _utcnow) -> None:
        """初始化（空轮；指针落在创建时刻，装填只可能落在其后的秒位）。

        Args:
            now: 时钟（层选择按「剩余时间」，故与推进器必须同源）。
        """
        self._now = now
        self._seconds: dict[int, ConcurrentStableList[DelayRecord]] = {}
        self._minutes: dict[int, ConcurrentStableList[DelayRecord]] = {}
        self._hours: dict[int, ConcurrentStableList[DelayRecord]] = {}
        self._overdue: ConcurrentStableList[DelayRecord] = ConcurrentStableList()
        self._last_scan: datetime = now()

    def layer_of(self, record: DelayRecord) -> str:
        """按剩余时间选层（纯函数，用例可直接断言装填结果）。

        Args:
            record: 延迟记录。

        Returns:
            str: `L0` / `L1` / `L2`。
        """
        remaining = record.deliver_at - self._now()
        if remaining < timedelta(seconds=_SECOND_SLOTS):
            return DELAY_LAYER_SECOND
        if remaining < timedelta(seconds=_MINUTE_SLOTS * _SECONDS_PER_MINUTE):
            return DELAY_LAYER_MINUTE
        return DELAY_LAYER_HOUR

    def place(self, record: DelayRecord) -> None:
        """把记录装进对应层的绝对时间槽（已到期的进待重试队列）。

        Args:
            record: 延迟记录。
        """
        if record.deliver_at <= self._now():
            self._overdue.add(record)
            return
        layer = self.layer_of(record)
        second = int(record.deliver_at.timestamp())
        if layer == DELAY_LAYER_SECOND:
            self._seconds.setdefault(second % _SECOND_SLOTS, ConcurrentStableList()).add(record)
        elif layer == DELAY_LAYER_MINUTE:
            slot = (second // _SECONDS_PER_MINUTE) % _MINUTE_SLOTS
            self._minutes.setdefault(slot, ConcurrentStableList()).add(record)
        else:
            slot = (second // _SECONDS_PER_HOUR) % _HOUR_SLOTS
            self._hours.setdefault(slot, ConcurrentStableList()).add(record)

    def take_due(self, now: datetime) -> ConcurrentStableList[DelayRecord]:
        """推进指针并取出本轮的候选记录（不判到期，由调用方逐条判）。

        窗口是「上次扫描时刻 → 本轮截止」的秒区间（含端点），单轮最多推进 60s：跨秒抖动不会漏位，
        长停摆 / 前跳时剩余秒位留给后续轮次。同刻重复调用（`now == last_scan`）仍会重扫当前秒位，
        使「被批上限挡下 / 重试失败」而重新装填到同秒的记录能在下一轮被取出。

        Args:
            now: 当前时刻。

        Returns:
            ConcurrentStableList[DelayRecord]: 候选记录（待重试队列在前，秒位 / 分位 / 时位依次在后）。
        """
        last = self._last_scan
        if now < last:
            # 回拨：不推进指针、不扫桶；记录留在桶里，时钟追上后照常到期（已到期者不早投）。
            return ConcurrentStableList()
        cursor = min(now, last + timedelta(seconds=DELAY_MAX_CATCHUP_SECONDS))
        self._last_scan = cursor
        collected = self._overdue
        self._overdue = ConcurrentStableList()
        for second in range(int(last.timestamp()), int(cursor.timestamp()) + 1):
            collected.extend(self._seconds.pop(second % _SECOND_SLOTS, ConcurrentStableList()))
            collected.extend(self._minutes.pop((second // _SECONDS_PER_MINUTE) % _MINUTE_SLOTS, ConcurrentStableList()))
            collected.extend(self._hours.pop((second // _SECONDS_PER_HOUR) % _HOUR_SLOTS, ConcurrentStableList()))
        return collected

    def clear(self) -> None:
        """清空全部桶并把指针落回当前时刻（B 段启动重建：`clear()` + `list_all()` 重装）。"""
        self._seconds.clear()
        self._minutes.clear()
        self._hours.clear()
        self._overdue = ConcurrentStableList()
        self._last_scan = self._now()

    def forget_tenant(self, *, tenant_id: int) -> int:
        """移除某租户在时间轮中的全部记录引用（租户删除时的易失资源清理）。

        `FileDelayStore.drop_tenant()` 已删 `.delay` 与内存索引；若不同时清时间轮，下一轮
        `take_due()` 仍会拿到旧引用并尝试转投，等于把已删除租户的延迟消息重新写回日志。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 移除的记录数。
        """
        target = int(tenant_id)
        removed = 0
        for layers in (self._seconds, self._minutes, self._hours):
            for slot in list(layers):
                kept = [record for record in layers[slot] if record.tenant_id != target]
                removed += len(layers[slot]) - len(kept)
                if kept:
                    layers[slot] = ConcurrentStableList(kept)
                else:
                    layers.pop(slot, None)
        kept_overdue = [record for record in self._overdue if record.tenant_id != target]
        removed += len(self._overdue) - len(kept_overdue)
        self._overdue = ConcurrentStableList(kept_overdue)
        return removed


class DurabilityPoint(Protocol):
    """转投的耐久点端口（02-7）：把某分区日志显式刷到磁盘。

    只要求一个方法，实现方是 `FileMessageStore.sync()`（结构满足）；缺省 `None` 表示不判
    （内存实现与用例路径）。
    """

    async def sync(self, *, topic_id: int, partition_no: int) -> None:
        """刷该分区日志并写恢复点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
        """
        ...


class TenantAvailability(Protocol):
    """租户可用性检查端口（延迟转投前的异步守卫；生产复用基座 `BaseTenantGuard`）。"""

    async def is_active(self, tenant_id: int) -> bool:
        """租户当前是否可收发。

        Args:
            tenant_id: 租户主键。

        Returns:
            bool: 可用 True；停用 / 不存在 / 租户源不可达按 False（fail-closed）。
        """
        ...


class DelayScheduler:
    """到期推进器（进程级单例）：取候选 → 转投 `append` → **显式刷盘** → 成功才清延迟记录。

    **偏序**（详细设计 §6.1 / 02-7 §8.1）：先写日志、**等它落盘**、后清记录。反向会把崩溃变成丢消息；
    只 append 不刷盘同样不行——`os` 档下页缓存里的一行还不是「已写入」，此时清掉 `.delay` 崩溃即两头皆空。
    本序最坏结果是重复转投（恢复期按 `log_id` 去重），而延迟是「出问题时的兜底」，丢一条等于让业务的
    重试彻底失联。

    **生命周期**（01-5 §6.5 / 02-7 §8.2）：启动钩子里先重建再 `ensure_started()`；首个延迟发布的
    惰性调用保留（幂等）。
    """

    def __init__(
        self,
        *,
        store: DelayStore,
        messages: MessageStore,
        wheel: DelayWheel,
        now: Callable[[], datetime] = _utcnow,
        interval_ms: int = DELAY_TICK_MS,
        batch_limit: int = DELAY_BATCH_LIMIT,
        failure_log_window_ms: int = DELAY_FAILURE_LOG_WINDOW_MS,
        clock: Callable[[], float] | None = None,
        durable: DurabilityPoint | None = None,
        availability: TenantAvailability | None = None,
    ) -> None:
        """初始化（不启动任务：由 `ensure_started()` 决定）。

        Args:
            store: 延迟区存储（进程级）。
            messages: 消息存储（转投的唯一写入方）。
            wheel: 时间轮（与发布侧共用同一实例）。
            now: 时钟（缺省 UTC 当前时间；用例注入可推进时钟）。
            interval_ms: 推进周期（毫秒；用例用 10ms 验证生命周期）。
            batch_limit: 单轮转投上限（条）。
            failure_log_window_ms: 同分区同原因的失败日志抑制窗口（毫秒；缺省一个 tick）。
            clock: 单调毫秒时钟（抑制窗口判据；用例注入以确定性控制窗口边界）。
            durable: 日志耐久点（02-7；None 表示不显式刷盘，用于内存实现与用例）。
            availability: 租户可用性检查（None 表示不判；生产传基座租户守卫适配器）。
        """
        self._store = store
        self._messages = messages
        self._wheel = wheel
        self._now = now
        self._interval_ms = interval_ms
        self._batch_limit = batch_limit
        self._failure_log_window_ms = failure_log_window_ms
        self._clock = clock if clock is not None else _monotonic_ms
        self._durable = durable
        self._availability = availability
        self._failure_logged: dict[tuple[int, int, str], float] = {}
        self._inactive_logged: ConcurrentStableDict[int, float] = ConcurrentStableDict()
        self._task: asyncio.Task[None] | None = None

    @property
    def wheel(self) -> DelayWheel:
        """时间轮（发布侧装填用：延迟发布与推进器必须共用同一实例）。

        Returns:
            DelayWheel: 时间轮。
        """
        return self._wheel

    @property
    def running(self) -> bool:
        """推进任务是否在跑（`ensure_started` 幂等与用例判据）。

        Returns:
            bool: 有活跃任务即 True。
        """
        return self._task is not None

    async def run_once(self) -> int:
        """单步推进（不 sleep，用例可精确驱动）：取候选 → 转投 → 清记录。

        Returns:
            int: 本轮成功转投的条数。
        """
        now = self._now()
        candidates = self._wheel.take_due(now)
        delivered = 0
        for record in candidates:
            if record.deliver_at > now:
                # 未到期（分桶只缩候选集）：重新装填，剩余时间更小，通常落进更细的层。
                self._wheel.place(record)
                continue
            if delivered >= self._batch_limit:
                # 本轮已达上限：留到下一轮（进待重试队列，下一轮首先取出）。
                self._wheel.place(record)
                continue
            if self._availability is not None and not await self._availability.is_active(record.tenant_id):
                # 停用 / 不存在 / 租户源不可达：记录保留，不转投；下一轮再判。
                self._wheel.place(record)
                self._log_inactive(record)
                continue
            if await self._transfer(record):
                delivered += 1
        return delivered

    def ensure_started(self) -> None:
        """惰性启动推进任务（幂等：已有活跃任务即返回；重入安全）。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止推进任务（幂等：无任务即返回；首期无落盘，B 段在此补刷盘）。"""
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _loop(self) -> None:
        """推进循环：单轮异常不终止任务（兜底通道不能因一次失败彻底停摆）。"""
        while True:
            try:
                await self.run_once()
            except Exception:  # 单轮异常只记日志，下一轮继续（任务不得静默退出）
                _LOGGER.exception("delay_tick_failed")
            await asyncio.sleep(self._interval_ms / MS_PER_SECOND)

    async def _transfer(self, record: DelayRecord) -> bool:
        """转投一条到期消息（先 `append` 并**落盘**、成功才删延迟记录；失败保留并计次）。

        Args:
            record: 到期记录。

        Returns:
            bool: 是否转投成功（失败已重新装填，下一轮重试）。
        """
        try:
            await self._messages.append(
                topic_id=record.topic_id,
                partition_no=record.partition_no,
                message_id=record.message_id,
                log_id=record.log_id,
                key=record.key,
                payload=record.payload,
                headers=record.headers,
                trace_id=record.trace_id,
                published_at=record.deliver_at,
                content_type=record.content_type,
                compression=record.compression,
                uncompressed_len=record.uncompressed_len,
            )
            if self._durable is not None:
                # 02-7：偏序的「已写入」= 已 fsync，不是「已 append」。刷盘失败与写失败同等对待
                # （记录保留、下一轮重试），否则 os 档下崩溃会两头皆空。
                await self._durable.sync(topic_id=record.topic_id, partition_no=record.partition_no)
            await self._store.remove(log_id=record.log_id)
        except Exception as exc:  # 主题停用 / 租户停用 / 存储故障：保留记录，下一轮重试
            return await self._defer(record, exc)
        return True

    async def _defer(self, record: DelayRecord, exc: Exception) -> bool:
        """转投失败处置：记录保留、`attempts + 1`、记日志、下一轮重试（不丢弃）。

        **日志带原因并按窗口抑制**（02-6）：没有 `reason` 时「磁盘满」与「主题停用」只能靠猜；
        而高水位期间每 tick 都会失败，不抑制会在几秒内刷出成千上万行同样的内容。
        抑制键是 `(主题, 分区, 原因)`——不同原因各记一条，不会被别的失败掩住。

        Args:
            record: 失败记录。
            exc: 失败原因（原样取其类名与 `errno`，不改抛）。

        Returns:
            bool: 恒 False（未投出）。
        """
        retried = with_attempt(record)
        self._log_failure(retried, exc)
        await self._store.enqueue(retried)
        self._wheel.place(retried)
        return False

    def _log_inactive(self, record: DelayRecord) -> None:
        """按窗口记一次「租户不可用、延迟转投暂停」日志（防每 tick 刷屏）。

        Args:
            record: 被暂停的延迟记录。
        """
        key = int(record.tenant_id)
        now_ms = self._clock()
        last = self._inactive_logged.get(key)
        if last is not None and now_ms - last < self._failure_log_window_ms:
            return
        self._inactive_logged[key] = now_ms
        _LOGGER.info(
            "delay_tenant_inactive",
            tenant_id=key,
            log_id=record.log_id,
            namespace=record.namespace,
            topic=record.topic,
        )

    def _log_failure(self, record: DelayRecord, exc: Exception) -> None:
        """按窗口记一次转投失败日志（同分区同原因在窗口内只记一条）。

        Args:
            record: 失败记录（已 `attempts + 1`）。
            exc: 失败原因。
        """
        reason = type(exc).__name__
        key = (record.topic_id, record.partition_no, reason)
        now_ms = self._clock()
        last = self._failure_logged.get(key)
        if last is not None and now_ms - last < self._failure_log_window_ms:
            return
        self._failure_logged[key] = now_ms
        _LOGGER.warning(
            "delay_deliver_failed",
            log_id=record.log_id,
            namespace=record.namespace,
            topic=record.topic,
            partition=record.partition_no,
            attempts=record.attempts,
            reason=reason,
            errno=getattr(exc, "errno", None),
        )


def _monotonic_ms() -> float:
    """当前单调时钟毫秒（日志抑制窗口判据，不受系统时钟回拨影响）。

    Returns:
        float: 单调时钟（毫秒）。
    """
    return time.monotonic() * 1_000
