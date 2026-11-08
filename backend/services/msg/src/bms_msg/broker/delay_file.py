"""`.delay` 文件段延迟区（02-7 详细设计 §5~§8）：`DelayStore` 端口的生产实现。

六条结构性口径：

1. **端口签名不变**：`enqueue` / `count_for_tenant` / `list_for_tenant` / `list_all` / `remove` 与
   01-5 完全一致；容量（每租户 + 全局）、去重、重建都是内部实现或**非端口**方法
   （`rebuild()` / `stale_count()` / `aclose()`），端口不新增方法（任务 §2.9）。
2. **追加 + 后写胜出**：同 `log_id` 再入队（含转投失败的 `attempts + 1`）追加新记录，重建按
   「段序号 + 段内位置」后者覆盖前者——不改写已有字节，CRC 与崩溃安全都不受影响。
3. **删除也是追加**：转投成功后追加 `clear` tombstone；段内**有效条目归零**即整段 unlink。
   不做 compaction（架构 09 D39），因此不会出现「重写活段」这类需要额外崩溃语义的动作。
4. **全量驻留**：索引与完整记录（含 payload）都常驻内存——`list_all()` 忠于端口语义（返回完整记录），
   转投零读盘，行为与 `InMemoryDelayStore` 一致（契约用例可对两实现参数化）。
   落盘解决的是「重启不丢」，不是「内存占用」；上界由两级条数上限兜住，容量结论归 8B_12。
5. **重建只扫全量**：`.delay` 的写入顺序与到期顺序无关，只扫末段会丢掉其它段里全部未到期记录。
   读量由全局上限封顶，且本来就要读 payload 装填时间轮，CRC 校验因此是顺手完成的。
6. **`attempts` 落盘节流**：转投失败每 tick 重试一次，逐次落盘会把「1 KiB 记录卡一天」放大到几十 MB；
   窗口内只更新内存，到点（或下一次同记录失败）才追加。崩溃可能少记几次计数，不影响投递。

**为什么 `.delay` 段不建索引**：延迟区重建靠全量扫，稀疏 offset 索引与时间索引在这里没有消费者；
`.log` 段（`segment.py`）一行不改，两种段的枚举正则互不匹配（`^(\\d{20})\\.log$` vs `^(\\d{20})\\.delay$`）。
"""

import asyncio
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Protocol

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.delay import (
    DelayMetrics,
    DelayRecord,
    DelayStore,
    ensure_delay_capacity,
    ensure_delay_span,
)
from bms_msg.broker.delay_segment import (
    DelayScan,
    DelaySegment,
    list_delay_segment_seqs,
)
from bms_msg.broker.file_store import PartitionDirectory, TopicLocation
from bms_msg.broker.record import (
    DELAY_KIND_CLEAR,
    DELAY_KIND_PUT,
    RECORD_FORMAT_VERSION,
    DelayStamp,
    RecordHeader,
    encode_record,
    from_epoch_us,
    to_epoch_us,
)
from bms_msg.broker.recovery import RecoveryLedger, RecoveryProbe
from bms_msg.core.config import (
    CORRUPT_TAIL_KEEP,
    DATA_ROOT_SUBDIR,
    DEFAULT_FSYNC_MODE,
    DELAY_ATTEMPT_FLUSH_ATTEMPTS,
    DELAY_ATTEMPT_FLUSH_MS,
    DELAY_SEGMENT_MAX_BYTES,
    DELAY_SEGMENT_MAX_RECORDS,
    DELAY_STUCK_TTL_MS,
    FSYNC_BATCH_RECORDS,
    FSYNC_INTERVAL_MS,
    FSYNC_INTERVAL_MS_ENV,
    FSYNC_MODE_BATCH,
    FSYNC_MODE_EACH,
    FSYNC_MODE_ENV,
    FSYNC_MODE_OS,
    FSYNC_MODE_TIME,
    MS_PER_SECOND,
)

__all__ = [
    "DelayDedupeSource",
    "DelayRecoveryReport",
    "FileDelayStore",
]

_LOGGER = get_logger("bms_msg.delay_file")
"""延迟区日志（只出结构化事件：段轮转 / 段隔离 / 截断 / 重建 / 关闭）。"""

_FSYNC_MODES: Final = frozenset({FSYNC_MODE_BATCH, FSYNC_MODE_TIME, FSYNC_MODE_EACH, FSYNC_MODE_OS})
"""合法的 fsync 档位（与 `file_store` 同一组常量；非法值即启动失败，不静默降级）。"""


class DelayDedupeSource(Protocol):
    """恢复期去重的数据源（`.log` 侧的 `log_id` 查询；由 `FileMessageStore` 结构满足）。"""

    async def log_ids_since(self, *, topic_id: int, partition_no: int, since_us: int, candidates: set[str]) -> set[str]:
        """在 `.log` 里找出时间下界之后、且命中候选集的 `log_id`。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            since_us: 发布时间下界（epoch 微秒；含该时刻之前的锚点项）。
            candidates: 关心的 `log_id` 集合（有界：来自 `.delay` 侧）。

        Returns:
            set[str]: 命中集合（可能是空集）。
        """
        ...


@dataclass(frozen=True)
class DelayRecoveryReport(BaseValueObject):
    """一次延迟区重建的结果（进日志、恢复台账与实施记录）。"""

    partitions: int = 0
    """涉及的分区数。"""

    segments: int = 0
    """`.delay` 段数（含被隔离的）。"""

    records: int = 0
    """重建后仍有效的待投记录数。"""

    expired: int = 0
    """其中已到期（进待重试队列）的条数。"""

    cleared: int = 0
    """tombstone 命中条数（正常清掉的记录）。"""

    deduped: int = 0
    """恢复期按 `log_id` 判定「已转投」而清掉的条数。"""

    unknown_kind: int = 0
    """未知 `kind` 而跳过的条数（不隔离，只告警）。"""

    truncated_bytes: int = 0
    """末段尾部被截掉的字节数合计（崩溃遗留半条记录）。"""

    isolated_segments: int = 0
    """非末段损坏被整段隔离的段数。"""

    lost_records: int = 0
    """随隔离段一起不可用的记录数（已扫出的有效条数；整段移出故计入丢失）。"""

    scanned_bytes: int = 0
    """本次重建读取的字节数（`RecoveryProbe` 口径一致）。"""

    elapsed_ms: int = 0
    """重建耗时（毫秒）。"""


@dataclass(frozen=True)
class _DelayEntry(BaseValueObject):
    """内存索引条目：记录 + 落盘位置 + `attempts` 节流状态。"""

    record: DelayRecord
    """完整记录（含 payload；全量驻留）。"""

    seq: int
    """该记录所在 `.delay` 段序号（有效计数与段回收用）。"""

    flushed_attempts: int
    """上次落盘时的 `attempts`（节流判据）。"""

    flushed_at_ms: float
    """上次落盘时刻（单调毫秒，节流判据）。"""


class _PartitionDelayLog:
    """单个分区的延迟段列表 + 活动段 + 写锁 + 每段有效条目数。"""

    def __init__(
        self,
        *,
        directory: Path,
        max_bytes: int,
        max_records: int,
        fsync_mode: str,
        fsync_interval_ms: int,
        fsync_batch_records: int,
        clock: Callable[[], float],
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化（不加载文件：段由 `rebuild()` 逐段 attach）。

        Args:
            directory: 分区目录。
            max_bytes: 段滚动字节阈值。
            max_records: 段滚动条数阈值。
            fsync_mode: fsync 档位（已校验）。
            fsync_interval_ms: 刷盘时间窗（毫秒）。
            fsync_batch_records: 按批档的条数阈值。
            clock: 单调毫秒时钟（用例注入以确定性驱动刷盘窗口）。
            probe: 恢复读取量探针（缺省不统计）。
        """
        self._directory = directory
        self._max_bytes = max_bytes
        self._max_records = max_records
        self._fsync_mode = fsync_mode
        self._fsync_interval_ms = fsync_interval_ms
        self._fsync_batch_records = fsync_batch_records
        self._clock = clock
        self._probe = probe
        self._segments: list[DelaySegment] = []
        self._effective: dict[int, int] = {}
        self._active: DelaySegment | None = None
        self._next_seq = 0
        self._unflushed = 0
        self._last_fsync_ms = clock()
        self.lock = asyncio.Lock()

    @property
    def segment_count(self) -> int:
        """当前段数。

        Returns:
            int: 段数。
        """
        return len(self._segments)

    def attach(self, seq: int) -> DelaySegment:
        """登记一个已存在的段（重建用；按 seq 升序，最大者为活动段）。

        Args:
            seq: 段序号。

        Returns:
            DelaySegment: 段对象。
        """
        segment = DelaySegment(
            partition_dir=self._directory,
            seq=seq,
            max_bytes=self._max_bytes,
            max_records=self._max_records,
            probe=self._probe,
        )
        self._segments.append(segment)
        self._segments.sort(key=lambda item: item.seq)
        self._next_seq = max(self._next_seq, seq + 1)
        self._active = self._segments[-1]
        return segment

    def prune_empty(self) -> int:
        """删掉「有效条目为 0 且非活动段」的段（段回收的唯一实现）。

        Returns:
            int: 删除的段数。
        """
        removed = 0
        kept: list[DelaySegment] = []
        active_seq = None if self._active is None else self._active.seq
        for segment in self._segments:
            if segment.seq != active_seq and self._effective.get(segment.seq, 0) == 0:
                segment.delete()
                self._effective.pop(segment.seq, None)
                removed += 1
                continue
            kept.append(segment)
        self._segments = kept
        return removed

    def recount(self, entries: dict[str, _DelayEntry]) -> None:
        """按当前存活条目重算每段有效数（重建 / 去重后调用，精确且不依赖增量）。

        Args:
            entries: 存活条目表。
        """
        counts: dict[int, int] = {}
        for entry in entries.values():
            counts[entry.seq] = counts.get(entry.seq, 0) + 1
        self._effective = counts

    def append(self, blob: bytes, *, record_bytes: int) -> DelaySegment:
        """追加一条记录（必要时先轮转 + 回收空段）。

        Args:
            blob: 完整记录字节。
            record_bytes: 本条记录字节数（轮转判据）。

        Returns:
            DelaySegment: 写入的段。
        """
        segment = self._active
        if segment is None:
            segment = self._open_new()
        elif segment.needs_roll(record_bytes=record_bytes):
            segment.fsync()
            segment.close()
            segment = self._open_new()
            self.prune_empty()
        segment.append(blob)
        self._effective[segment.seq] = self._effective.get(segment.seq, 0) + 1
        self._unflushed += 1
        self._maybe_fsync(segment)
        return segment

    def mark_cleared(self, seq: int) -> None:
        """把某段的有效条目数减一（tombstone 命中后）。

        Args:
            seq: 被清记录所在段序号。
        """
        remaining = self._effective.get(seq, 0) - 1
        if remaining > 0:
            self._effective[seq] = remaining
            return
        self._effective.pop(seq, None)

    def fsync_active(self) -> None:
        """刷当前活动段（关闭 / 重建结束 / 显式耐久点用）。"""
        if self._active is not None:
            self._active.fsync()
        self._unflushed = 0
        self._last_fsync_ms = self._clock()

    def close(self) -> None:
        """关闭全部写句柄（幂等）。"""
        for segment in self._segments:
            segment.close()

    def delete_all(self) -> int:
        """关闭并删除该分区的全部 `.delay` 段（租户删除时用）。

        Returns:
            int: 删除的段数。
        """
        removed = 0
        for segment in self._segments:
            segment.close()
            segment.delete()
            removed += 1
        self._segments = []
        self._effective = {}
        self._active = None
        return removed

    def _open_new(self) -> DelaySegment:
        """以 `max_seq + 1` 开新段（序号只增不复用）。

        Returns:
            DelaySegment: 新段。
        """
        segment = DelaySegment(
            partition_dir=self._directory,
            seq=self._next_seq,
            max_bytes=self._max_bytes,
            max_records=self._max_records,
            probe=self._probe,
        )
        self._next_seq += 1
        self._segments.append(segment)
        self._active = segment
        return segment

    def _maybe_fsync(self, segment: DelaySegment) -> None:
        """按 fsync 档位决定是否刷盘（与 `.log` 同一组档位与常量）。

        Args:
            segment: 刚写入的段。
        """
        if self._fsync_mode == FSYNC_MODE_OS:
            return
        if self._fsync_mode == FSYNC_MODE_EACH:
            segment.fsync()
            self._unflushed = 0
            self._last_fsync_ms = self._clock()
            return
        now = self._clock()
        reached_batch = self._fsync_mode == FSYNC_MODE_BATCH and self._unflushed >= self._fsync_batch_records
        reached_window = now - self._last_fsync_ms >= self._fsync_interval_ms
        if not reached_batch and not reached_window:
            return
        segment.fsync()
        self._unflushed = 0
        self._last_fsync_ms = now


class FileDelayStore(DelayStore):
    """`.delay` 文件段延迟区（生产实现；端口签名与内存实现完全一致）。"""

    def __init__(
        self,
        *,
        directory: PartitionDirectory,
        dedupe: DelayDedupeSource | None = None,
        ledger: RecoveryLedger | None = None,
        metrics: DelayMetrics | None = None,
        quota: int = 100_000,
        global_quota: int = 500_000,
        segment_max_bytes: int = DELAY_SEGMENT_MAX_BYTES,
        segment_max_records: int = DELAY_SEGMENT_MAX_RECORDS,
        fsync_mode: str | None = None,
        fsync_interval_ms: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
        probe: RecoveryProbe | None = None,
    ) -> None:
        """初始化（不扫盘、不建目录：重建由启动钩子调 `rebuild()`）。

        Args:
            directory: 主题目录登记表（分区路径与段枚举的唯一来源）。
            dedupe: 恢复期去重的 `.log` 查询源（None 表示不做去重，仅用于轻量用例）。
            ledger: 进程级恢复台账（None 表示不记账）。
            metrics: 指标采集器（None 表示不采集）。
            quota: 每租户软上限（条）。
            global_quota: 全局软上限（条）。
            segment_max_bytes: 段滚动字节阈值。
            segment_max_records: 段滚动条数阈值。
            fsync_mode: fsync 档位（None 取环境变量，缺省按批）。
            fsync_interval_ms: 刷盘时间窗（None 取环境变量，缺省模块常量）。
            clock: 单调毫秒时钟（节流与刷盘窗口判据；用例注入）。
            now: 墙钟（到期判定与 tombstone 时间戳；缺省取系统时钟）。
            probe: 恢复读取量探针（缺省不统计）。

        Raises:
            ValueError: fsync 档位或时间窗取值非法（启动即失败，不静默降级）。
        """
        mode = fsync_mode if fsync_mode is not None else os.environ.get(FSYNC_MODE_ENV, DEFAULT_FSYNC_MODE)
        if mode not in _FSYNC_MODES:
            raise ValueError(f"MSG_FSYNC_MODE 取值非法：{mode!r}（可选 {sorted(_FSYNC_MODES)}）")
        interval = (
            fsync_interval_ms
            if fsync_interval_ms is not None
            else int(os.environ.get(FSYNC_INTERVAL_MS_ENV, FSYNC_INTERVAL_MS))
        )
        if interval <= 0:
            raise ValueError(f"MSG_FSYNC_INTERVAL_MS 须为正数：{interval}")
        self.quota = quota
        self.global_quota = global_quota
        self._directory = directory
        self._dedupe = dedupe
        self._ledger = ledger
        self._metrics = metrics
        self._segment_max_bytes = segment_max_bytes
        self._segment_max_records = segment_max_records
        self._fsync_mode = mode
        self._fsync_interval_ms = interval
        self._clock = clock
        self._now = now if now is not None else _utcnow
        self._probe = probe
        self._entries: dict[str, _DelayEntry] = {}
        self._tenant_counts: dict[int, int] = {}
        self._logs: dict[tuple[int, int], _PartitionDelayLog] = {}

    async def enqueue(self, record: DelayRecord) -> None:
        """写入 / 覆盖一条延迟记录（先校验跨度与容量；同 `log_id` 后写胜出）。

        Args:
            record: 延迟记录。

        Raises:
            ParamError: 延迟跨度超出上限（10001）。
            MsgDelayQuotaExceededError: 该租户或全局延迟区已满（350015）。
        """
        ensure_delay_span(record)
        existing = self._entries.get(record.log_id)
        ensure_delay_capacity(
            known=existing is not None,
            topic=record.topic,
            tenant_count=self._tenant_counts.get(record.tenant_id, 0),
            total=len(self._entries),
            quota=self.quota,
            global_quota=self.global_quota,
            metrics=self._metrics,
        )
        log = self._partition(topic_id=record.topic_id, partition_no=record.partition_no)
        async with log.lock:
            if existing is not None and self._throttled(existing, record):
                # 只更新内存：落盘放大由 DELAY_ATTEMPT_FLUSH_* 控制（关闭时补写）
                self._entries[record.log_id] = replace(
                    existing,
                    record=record,
                    flushed_attempts=existing.flushed_attempts,
                )
                self._publish_pending(record.tenant_id)
                return
            blob = encode_record(header=_put_header(record), payload=record.payload)
            segment = log.append(blob, record_bytes=len(blob))
            if existing is not None:
                # 旧记录被本次覆盖（可能落在同一段）：有效计数先减，随后本次 append 已加一
                log.mark_cleared(existing.seq)
            self._entries[record.log_id] = _DelayEntry(
                record=record,
                seq=segment.seq,
                flushed_attempts=record.attempts,
                flushed_at_ms=self._clock(),
            )
            if existing is None:
                self._tenant_counts[record.tenant_id] = self._tenant_counts.get(record.tenant_id, 0) + 1
        self._publish_pending(record.tenant_id)
        self._publish_segments()

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """取某租户在延迟区中的记录数（配额判据，O(1)）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 记录条数。
        """
        return self._tenant_counts.get(int(tenant_id), 0)

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[DelayRecord]:
        """列出某租户的延迟记录（只读视图；按到期时刻升序）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录（`deliver_at` 升序，同刻按 `log_id` 升序）。
        """
        matched = [entry.record for entry in self._entries.values() if entry.record.tenant_id == int(tenant_id)]
        return ConcurrentStableList(sorted(matched, key=lambda item: (item.deliver_at, item.log_id)))

    async def list_all(self) -> ConcurrentStableList[DelayRecord]:
        """列出全部延迟记录（启动重建装填时间轮的入口）。

        Returns:
            ConcurrentStableList[DelayRecord]: 记录（`deliver_at` 升序，同刻按 `log_id` 升序）。
        """
        records = [entry.record for entry in self._entries.values()]
        return ConcurrentStableList(sorted(records, key=lambda item: (item.deliver_at, item.log_id)))

    async def remove(self, *, log_id: str) -> None:
        """按记录标识删除（转投成功后调用：追加 tombstone，不存在即无操作）。

        Args:
            log_id: 记录标识。
        """
        entry = self._entries.pop(log_id, None)
        if entry is None:
            return
        record = entry.record
        log = self._partition(topic_id=record.topic_id, partition_no=record.partition_no)
        async with log.lock:
            blob = encode_record(header=_clear_header(record, now=self._now()), payload=b"")
            log.append(blob, record_bytes=len(blob))
            log.mark_cleared(entry.seq)
            log.prune_empty()
        self._decrement_tenant(record.tenant_id)
        self._publish_pending(record.tenant_id)
        self._publish_segments()

    def drop_tenant(self, *, tenant_id: int) -> int:
        """清空某租户的延迟区（内存条目 + `.delay` 段；不动 `.log` 与位点）。

        租户删除后延迟消息不得再投；同时删盘上 `.delay`，避免进程重启时 `rebuild()` 把记录
        重新装进时间轮。调用方负责同时清 `DelayWheel`。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 清掉的延迟记录数。
        """
        target = int(tenant_id)
        removed = 0
        for log_id, entry in list(self._entries.items()):
            if entry.record.tenant_id != target:
                continue
            self._entries.pop(log_id, None)
            removed += 1
        for key, log in list(self._logs.items()):
            located = self._directory.locate(key[0])
            if located is None or int(located.tenant_id) != target:
                continue
            log.delete_all()
            self._logs.pop(key, None)
        self._tenant_counts.pop(target, None)
        self._publish_pending(target)
        self._publish_segments()
        return removed

    async def rebuild(self) -> DelayRecoveryReport:
        """全量扫 `.delay` 重建索引（启动钩子调用；幂等：先清空再重建）。

        Returns:
            DelayRecoveryReport: 重建结果（进日志、恢复台账与实施记录）。
        """
        started = time.monotonic()
        self._entries.clear()
        self._tenant_counts.clear()
        # 重建会换掉全部段对象：先把旧写句柄关掉（否则第二次重建会留下孤儿 fd，Windows 上还会锁住文件）
        for log in self._logs.values():
            log.close()
        self._logs.clear()
        counters = _RebuildCounters()
        recoveries = self._load_segments(counters)
        deduped = await self._dedupe_transferred()
        self._recount_segments()
        expired = sum(1 for entry in self._entries.values() if entry.record.deliver_at <= self._now())
        report = DelayRecoveryReport(
            partitions=len(self._logs),
            segments=counters.segments,
            records=len(self._entries),
            expired=expired,
            cleared=counters.cleared,
            deduped=deduped,
            unknown_kind=counters.unknown,
            truncated_bytes=sum(item.truncated_bytes for item in recoveries.values()),
            isolated_segments=counters.lost_segments,
            lost_records=sum(item.lost_records for item in recoveries.values()),
            scanned_bytes=counters.scanned_bytes,
            elapsed_ms=int((time.monotonic() - started) * 1_000),
        )
        self._record_ledger(recoveries)
        self._publish_pending_all()
        self._publish_segments()
        _LOGGER.info(
            "msg_delay_rebuilt",
            partitions=report.partitions,
            segments=report.segments,
            records=report.records,
            expired=report.expired,
            deduped=report.deduped,
            truncated_bytes=report.truncated_bytes,
            isolated_segments=report.isolated_segments,
            lost_records=report.lost_records,
            unknown_kind=report.unknown_kind,
            elapsed_ms=report.elapsed_ms,
        )
        return report

    def stale_count(self, *, now: datetime | None = None) -> int:
        """统计「已到期但超过告警线仍未投出」的条数（TTL 兜底的判据；O(n)，只由采集出口调用）。

        Args:
            now: 参照时刻（缺省取系统墙钟）。

        Returns:
            int: 超期条数（0 表示健康）。
        """
        moment = now if now is not None else self._now()
        return sum(
            1
            for entry in self._entries.values()
            if (moment - entry.record.deliver_at).total_seconds() * MS_PER_SECOND > DELAY_STUCK_TTL_MS
        )

    async def aclose(self) -> None:
        """优雅关闭：补写待落盘的 `attempts`、刷 `.delay`、关句柄（幂等）。"""
        flushed = 0
        for entry in list(self._entries.values()):
            if entry.record.attempts <= entry.flushed_attempts:
                continue
            record = entry.record
            log = self._partition(topic_id=record.topic_id, partition_no=record.partition_no)
            blob = encode_record(header=_put_header(record), payload=record.payload)
            segment = log.append(blob, record_bytes=len(blob))
            self._entries[record.log_id] = _DelayEntry(
                record=record,
                seq=segment.seq,
                flushed_attempts=record.attempts,
                flushed_at_ms=self._clock(),
            )
            log.mark_cleared(entry.seq)
            flushed += 1
        for log in self._logs.values():
            log.fsync_active()
            log.close()
        _LOGGER.info("msg_delay_store_closed", partitions=len(self._logs), attempts_flushed=flushed)

    def _load_segments(self, counters: _RebuildCounters) -> dict[tuple[int, int], _PartitionRecovery]:
        """枚举分区目录并逐段扫描（末段截断 / 非末段隔离）。

        Args:
            counters: 计数累加器（条数与读量）。

        Returns:
            dict[tuple[int, int], _PartitionRecovery]: 发生过损坏的分区及其恢复结论。
        """
        recoveries: dict[tuple[int, int], _PartitionRecovery] = {}
        for location, partition_no, seqs in self._discover():
            key = (location.topic_id, partition_no)
            directory = self._directory.path_of(location, partition_no)
            log = self._partition(topic_id=location.topic_id, partition_no=partition_no)
            for index, seq in enumerate(seqs):
                segment = log.attach(seq)
                scan = segment.scan(verify_crc=True)
                counters.segments += 1
                counters.scanned_bytes += segment.size_bytes
                if scan.corrupted_position is not None:
                    if index == len(seqs) - 1:
                        dropped = segment.size_bytes - scan.complete_bytes
                        _LOGGER.warning(
                            "msg_delay_tail_truncated",
                            partition_path=str(directory),
                            seq=seq,
                            dropped_bytes=dropped,
                        )
                        segment.isolate_tail(keep_bytes=scan.complete_bytes, timestamp=_timestamp())
                        segment.truncate(size=scan.complete_bytes)
                        previous = recoveries.get(key, _PartitionRecovery())
                        recoveries[key] = replace(previous, truncated_bytes=previous.truncated_bytes + dropped)
                    else:
                        target = segment.isolate(timestamp=_timestamp())
                        segment.prune_corrupts(limit=CORRUPT_TAIL_KEEP)
                        _LOGGER.warning(
                            "msg_delay_segment_isolated",
                            partition_path=str(directory),
                            seq=seq,
                            isolated=target.name,
                            valid_records=len(scan.entries),
                        )
                        previous = recoveries.get(key, _PartitionRecovery())
                        recoveries[key] = replace(
                            previous,
                            isolated_seqs=(*previous.isolated_seqs, seq),
                            lost_records=previous.lost_records + len(scan.entries),
                        )
                        counters.lost_segments += 1
                        continue
                segment.set_record_count(len(scan.entries))
                self._account(scan, seq=seq, counters=counters)
        return recoveries

    def _account(self, scan: DelayScan, *, seq: int, counters: _RebuildCounters) -> None:
        """把一段的扫描结果记入索引与计数（按写入顺序，后写胜出）。

        Args:
            scan: 段扫描结果。
            seq: 段序号。
            counters: 计数累加器。
        """
        for entry in scan.entries:
            stamp = entry.header.delay
            if stamp is None:
                # `.delay` 段里的普通记录：不该出现（写入路径只写带 delay 子结构的记录）
                counters.unknown += 1
                continue
            if stamp.kind == DELAY_KIND_CLEAR:
                existing = self._entries.pop(entry.header.log_id, None)
                if existing is not None:
                    self._decrement_tenant(existing.record.tenant_id)
                    counters.cleared += 1
                continue
            if stamp.kind != DELAY_KIND_PUT:
                # 未知 kind：跳过该条（该 log_id 保留上一次已知状态），不隔离整段
                counters.unknown += 1
                continue
            previous = self._entries.get(entry.header.log_id)
            if previous is not None:
                self._decrement_tenant(previous.record.tenant_id)
            record = _to_record(entry.header, entry.payload, stamp=stamp)
            self._entries[record.log_id] = _DelayEntry(
                record=record,
                seq=seq,
                flushed_attempts=record.attempts,
                flushed_at_ms=self._clock(),
            )
            self._tenant_counts[record.tenant_id] = self._tenant_counts.get(record.tenant_id, 0) + 1

    async def _dedupe_transferred(self) -> int:
        """把「已转投但没清掉」的 `.delay` 记录去掉（恢复期 `log_id` 比对）。

        Returns:
            int: 被清掉的条数。
        """
        if self._dedupe is None or not self._entries:
            return 0
        since_by_partition: dict[tuple[int, int], tuple[int, set[str]]] = {}
        for log_id, entry in self._entries.items():
            key = (entry.record.topic_id, entry.record.partition_no)
            since = to_epoch_us(entry.record.deliver_at)
            current = since_by_partition.get(key)
            if current is None:
                since_by_partition[key] = (since, {log_id})
                continue
            current[1].add(log_id)
            if since < current[0]:
                since_by_partition[key] = (since, current[1])
        cleared = 0
        for (topic_id, partition_no), (since_us, candidates) in since_by_partition.items():
            hits = await self._dedupe.log_ids_since(
                topic_id=topic_id, partition_no=partition_no, since_us=since_us, candidates=candidates
            )
            for log_id in hits:
                entry = self._entries.pop(log_id, None)
                if entry is None:  # pragma: no cover - 候选集来自同一张表，命中后必在
                    continue
                await self._append_clear(entry)
                self._decrement_tenant(entry.record.tenant_id)
                cleared += 1
        if cleared:
            _LOGGER.info("msg_delay_deduped", records=cleared)
        return cleared

    async def _append_clear(self, entry: _DelayEntry) -> None:
        """恢复期清一条已转投记录（追加 tombstone；写失败只告警，幂等可重做）。

        Args:
            entry: 待清条目。
        """
        record = entry.record
        log = self._partition(topic_id=record.topic_id, partition_no=record.partition_no)
        try:
            blob = encode_record(header=_clear_header(record, now=self._now()), payload=b"")
            log.append(blob, record_bytes=len(blob))
            log.mark_cleared(entry.seq)
        except OSError as exc:
            _LOGGER.warning(
                "msg_delay_clear_failed",
                log_id=record.log_id,
                topic_id=record.topic_id,
                partition=record.partition_no,
                error=str(exc),
            )

    def _recount_segments(self) -> None:
        """重建后按存活条目重算每段有效数并回收空段。"""
        for log in self._logs.values():
            log.recount(self._entries)
            log.prune_empty()

    def _discover(self) -> list[tuple[TopicLocation, int, list[int]]]:
        """枚举数据根下「有 `.delay` 段」的分区（同时把目录身份登记进 `PartitionDirectory`）。

        Returns:
            list[tuple[TopicLocation, int, list[int]]]: 目录身份 / 分区号 / 段序号列表。
        """
        root = self._directory.root / DATA_ROOT_SUBDIR
        if not root.is_dir():
            return []
        found: list[tuple[TopicLocation, int, list[int]]] = []
        for tenant_dir in sorted(root.iterdir()):
            if not tenant_dir.is_dir() or not tenant_dir.name.isdigit():
                continue
            for namespace_dir in sorted(tenant_dir.iterdir()):
                if not namespace_dir.is_dir():
                    continue
                for topic_dir in sorted(namespace_dir.iterdir()):
                    if not topic_dir.is_dir():
                        continue
                    parsed = _split_topic_dir(topic_dir.name)
                    if parsed is None:
                        continue
                    name, topic_id = parsed
                    location = TopicLocation(
                        tenant_id=int(tenant_dir.name),
                        namespace=namespace_dir.name,
                        name=name,
                        topic_id=topic_id,
                    )
                    self._directory.bind(
                        tenant_id=location.tenant_id,
                        namespace=location.namespace,
                        name=location.name,
                        topic_id=location.topic_id,
                    )
                    for partition_dir in sorted(topic_dir.iterdir()):
                        if not partition_dir.is_dir() or not partition_dir.name.isdigit():
                            continue
                        seqs = list_delay_segment_seqs(partition_dir)
                        if seqs:
                            found.append((location, int(partition_dir.name), seqs))
        return found

    def _record_ledger(self, recoveries: dict[tuple[int, int], _PartitionRecovery]) -> None:
        """把重建结论写进进程级恢复台账（`/readyz` 的 `msg_recovery` 数据源）。

        Args:
            recoveries: 发生过损坏的分区及其恢复结论。
        """
        if self._ledger is None:
            return
        for (topic_id, partition_no), item in recoveries.items():
            self._ledger.record_delay(
                topic_id=topic_id,
                partition_no=partition_no,
                truncated_bytes=item.truncated_bytes,
                isolated_segments=item.isolated_seqs,
                lost_records=item.lost_records,
            )

    def _partition(self, *, topic_id: int, partition_no: int) -> _PartitionDelayLog:
        """取分区延迟日志（懒建；目录不存在即视为空分区）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            _PartitionDelayLog: 分区延迟日志。
        """
        key = (int(topic_id), int(partition_no))
        existing = self._logs.get(key)
        if existing is not None:
            return existing
        directory = self._directory.partition_path(topic_id=key[0], partition_no=key[1])
        log = _PartitionDelayLog(
            directory=directory,
            max_bytes=self._segment_max_bytes,
            max_records=self._segment_max_records,
            fsync_mode=self._fsync_mode,
            fsync_interval_ms=self._fsync_interval_ms,
            fsync_batch_records=FSYNC_BATCH_RECORDS,
            clock=self._clock,
            probe=self._probe,
        )
        self._logs[key] = log
        return log

    def _throttled(self, existing: _DelayEntry, record: DelayRecord) -> bool:
        """判定这次覆盖写是否可只更新内存（`attempts` 节流）。

        Args:
            existing: 既有条目。
            record: 新记录。

        Returns:
            bool: True 表示本轮不落盘（只更新内存）。
        """
        if replace(record, attempts=0) != replace(existing.record, attempts=0):
            # 除 attempts 之外的字段变了（改期 / 换档）：立即落盘，节流只管「纯计数」
            return False
        if record.attempts <= existing.flushed_attempts:
            return False
        if record.attempts - existing.flushed_attempts >= DELAY_ATTEMPT_FLUSH_ATTEMPTS:
            return False
        return self._clock() - existing.flushed_at_ms < DELAY_ATTEMPT_FLUSH_MS

    def _decrement_tenant(self, tenant_id: int) -> None:
        """租户计数减一（归零即删键，避免键表无界）。

        Args:
            tenant_id: 租户主键。
        """
        remaining = self._tenant_counts.get(int(tenant_id), 0) - 1
        if remaining > 0:
            self._tenant_counts[int(tenant_id)] = remaining
            return
        self._tenant_counts.pop(int(tenant_id), None)

    def _publish_pending(self, tenant_id: int) -> None:
        """把某租户的待投条数与总数写进指标。

        Args:
            tenant_id: 租户主键。
        """
        if self._metrics is None:
            return
        self._metrics.record_pending(
            tenant_id=tenant_id,
            count=self._tenant_counts.get(int(tenant_id), 0),
            total=len(self._entries),
        )

    def _publish_pending_all(self) -> None:
        """把全部租户的待投条数与总数写进指标（重建结束调用）。"""
        if self._metrics is None:
            return
        for tenant_id in list(self._tenant_counts):
            self._metrics.record_pending(
                tenant_id=tenant_id,
                count=self._tenant_counts[tenant_id],
                total=len(self._entries),
            )
        self._metrics.record_total(total=len(self._entries))

    def _publish_segments(self) -> None:
        """把段数写进指标（轮转 / 删段 / 重建结束调用）。"""
        if self._metrics is None:
            return
        self._metrics.record_segments(count=sum(log.segment_count for log in self._logs.values()))


@dataclass(frozen=True)
class _PartitionRecovery(BaseValueObject):
    """一个分区的延迟区恢复结论（只记损坏；未损坏的分区不入表）。"""

    truncated_bytes: int = 0
    """末段尾部被截掉的字节数。"""

    isolated_seqs: tuple[int, ...] = ()
    """被整段隔离的段序号。"""

    lost_records: int = 0
    """随隔离段一起不可用的记录数。"""


@dataclass
class _RebuildCounters:
    """重建过程的计数器（避免一串可变局部变量）。"""

    segments: int = 0
    """扫过的段数。"""

    cleared: int = 0
    """tombstone 命中条数。"""

    unknown: int = 0
    """未知 kind / 非延迟记录条数。"""

    lost_segments: int = 0
    """被整段隔离的段数。"""

    scanned_bytes: int = 0
    """读取字节数。"""


def _split_topic_dir(name: str) -> tuple[str, int] | None:
    """把主题目录名拆成「主题名 + 主题主键」（`{topic}.{topic_id}`）。

    Args:
        name: 目录名。

    Returns:
        tuple[str, int] | None: 拆分结果；不符合形态返回 None。
    """
    base, _, suffix = name.rpartition(".")
    if not base or not suffix.isdigit():
        return None
    return base, int(suffix)


def _put_header(record: DelayRecord) -> RecordHeader:
    """延迟记录 → 记录头（`kind = put`）。

    Args:
        record: 延迟记录。

    Returns:
        RecordHeader: 记录头（带 `delay` 子结构）。
    """
    return RecordHeader(
        format_version=RECORD_FORMAT_VERSION,
        topic_id=record.topic_id,
        partition_no=record.partition_no,
        tenant_id=record.tenant_id,
        message_id=record.message_id,
        log_id=record.log_id,
        key=record.key,
        trace_id=record.trace_id,
        published_at_us=to_epoch_us(record.published_at),
        headers=tuple((str(name), str(value)) for name, value in record.headers.items()),
        content_type=record.content_type,
        compression=record.compression,
        uncompressed_len=record.uncompressed_len,
        delay=DelayStamp(
            kind=DELAY_KIND_PUT,
            deliver_at_us=to_epoch_us(record.deliver_at),
            delay_level=record.delay_level,
            attempts=record.attempts,
            namespace=record.namespace,
            topic=record.topic,
        ),
    )


def _clear_header(record: DelayRecord, *, now: datetime) -> RecordHeader:
    """tombstone 记录头（`kind = clear`；只带 `log_id` 与分区归属，无 payload）。

    Args:
        record: 被清的延迟记录（取其身份与分区归属）。
        now: 写入时刻（当作 `published_at`；本字段对 tombstone 无业务含义）。

    Returns:
        RecordHeader: 记录头。
    """
    return RecordHeader(
        format_version=RECORD_FORMAT_VERSION,
        topic_id=record.topic_id,
        partition_no=record.partition_no,
        tenant_id=record.tenant_id,
        message_id=record.message_id,
        log_id=record.log_id,
        key=None,
        trace_id=None,
        published_at_us=to_epoch_us(now),
        headers=(),
        content_type="",
        compression="",
        uncompressed_len=None,
        delay=DelayStamp(kind=DELAY_KIND_CLEAR),
    )


def _to_record(header: RecordHeader, payload: bytes, *, stamp: DelayStamp) -> DelayRecord:
    """记录头 + 载荷 → 延迟记录（字段口径的唯一出处）。

    Args:
        header: 记录头。
        payload: 业务字节。
        stamp: 延迟元数据。

    Returns:
        DelayRecord: 延迟记录。
    """
    return DelayRecord(
        log_id=header.log_id,
        message_id=header.message_id,
        tenant_id=header.tenant_id,
        topic_id=header.topic_id,
        namespace=stamp.namespace,
        topic=stamp.topic,
        partition_no=header.partition_no,
        delay_level=stamp.delay_level,
        key=header.key,
        payload=payload,
        headers=ConcurrentStableDict(dict(header.headers)),
        trace_id=header.trace_id,
        published_at=from_epoch_us(header.published_at_us),
        deliver_at=from_epoch_us(stamp.deliver_at_us),
        attempts=stamp.attempts,
        content_type=header.content_type,
        compression=header.compression,
        uncompressed_len=header.uncompressed_len,
    )


def _timestamp() -> str:
    """隔离 / 截断取证文件名里的时间戳（UTC，可字典序排序）。

    Returns:
        str: `%Y%m%dT%H%M%S%f` 形态的 UTC 时刻。
    """
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S%f")


def _utcnow() -> datetime:
    """当前 UTC 时间（到期判定与 tombstone 时间戳的缺省来源）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(tz=UTC)
