# pyright: reportPrivateUsage=false
"""`.delay` 文件延迟区（msg 任务 02-7；Kiwi 2432）。

覆盖（详细设计 §12 用例 D3 的 kind 分支、D6~D12、D20）：

- **D6 后写胜出**：同 `log_id` 再入队追加新记录，重建后按写入序取最后一条；
- **D7 两级上限**：租户满 / 全局满都返回 `350015`，`data` 四字段与 `scope` 正确；覆盖写不受限；
- **D8 隔离与排序**：跨分区互不影响，`list_all` / `list_for_tenant` 按 `(deliver_at, log_id)` 升序；
- **D9 删除**：`remove` 幂等、写 tombstone、段内有效条目归零后整段回收；
- **D10 轮转与序号**：8 MiB / 条数任一触发轮转，段序号只增不复用；
- **D11 attempts 节流**：窗口内不写、超窗口写、跨阈值写、关闭补写；
- **D12 端口义务**：跨度超上限 `10001`；
- **D20 指标与超期计数**：`snapshot()` 三项 + `stale_count()`。
"""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.broker.delay import (
    DELAY_SCOPE_GLOBAL,
    DELAY_SCOPE_TENANT,
    DelayMetrics,
    DelayRecord,
)
from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.delay_segment import DelaySegment, list_delay_segment_seqs
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.record import DELAY_KIND_PUT, RECORD_FORMAT_VERSION, DelayStamp, RecordHeader, encode_record
from bms_msg.core.config import (
    CORRUPT_TAIL_KEEP,
    DELAY_ATTEMPT_FLUSH_ATTEMPTS,
    DELAY_ATTEMPT_FLUSH_MS,
    DELAY_MAX_SPAN_MS,
    DELAY_STUCK_TTL_MS,
)
from bms_msg.core.trace import EMPTY_TRACE, TraceContext
from bms_msg.errors import MsgDelayQuotaExceededError

pytestmark = pytest.mark.kiwi_id(2432)

TENANT_ID = 9_001
"""用例租户主键（目录第一层）。"""

OTHER_TENANT = 9_002
"""第二个租户（全局上限用例）。"""

TOPIC_ID = 11
"""用例主题主键。"""

NAMESPACE = "orders"
"""用例命名空间。"""

TOPIC_NAME = "orders.events"
"""用例主题名。"""

Clock = Callable[[], float]


def _directory(tmp_path: Path) -> PartitionDirectory:
    """构造并绑定一个主题目录登记表。

    Args:
        tmp_path: 用例临时目录（数据根）。

    Returns:
        PartitionDirectory: 已绑定本用例主题的登记表。
    """
    directory = PartitionDirectory(root=tmp_path)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return directory


def _store(
    tmp_path: Path,
    *,
    now: Callable[[], datetime],
    quota: int = 1_000,
    global_quota: int = 2_000,
    fsync_mode: str = "os",
    tier_clock: Clock | None = None,
    metrics: DelayMetrics | None = None,
    max_records: int = 100_000,
    max_bytes: int = 8 * 1024 * 1024,
) -> FileDelayStore:
    """构造文件延迟区（小阈值 / 不刷盘，便于用例造多段）。

    Args:
        tmp_path: 数据根。
        now: 墙钟（到期判定）。
        quota: 每租户软上限。
        global_quota: 全局软上限。
        fsync_mode: fsync 档位。
        tier_clock: 单调毫秒时钟（节流窗口判据）。
        metrics: 指标采集器。
        max_records: 段条数阈值。
        max_bytes: 段字节阈值。

    Returns:
        FileDelayStore: 延迟区。
    """
    return FileDelayStore(
        directory=_directory(tmp_path),
        quota=quota,
        global_quota=global_quota,
        segment_max_bytes=max_bytes,
        segment_max_records=max_records,
        fsync_mode=fsync_mode,
        clock=tier_clock if tier_clock is not None else (lambda: 0.0),
        now=now,
        metrics=metrics,
    )


def _record(
    now: Callable[[], datetime],
    *,
    log_id: str = "l-1",
    span_ms: int = 60_000,
    attempts: int = 0,
    tenant_id: int = TENANT_ID,
    partition_no: int = 0,
    level: str = "1m",
) -> DelayRecord:
    """构造一条延迟记录。

    Args:
        now: 墙钟。
        log_id: 记录标识。
        span_ms: 延迟跨度（毫秒）。
        attempts: 转投失败次数。
        tenant_id: 租户主键。
        partition_no: 分区号。
        level: 档位名。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = now()
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=tenant_id,
        topic_id=TOPIC_ID,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=partition_no,
        delay_level=level,
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
        attempts=attempts,
    )


def _segment_sizes(tmp_path: Path) -> list[int]:
    """列出全部 `.delay` 段文件大小（升序按段序号）。

    Args:
        tmp_path: 数据根。

    Returns:
        list[int]: 段大小列表。
    """
    return sorted(path.stat().st_size for path in tmp_path.rglob("*.delay"))


def _partition_dir(tmp_path: Path) -> Path:
    """取本用例的分区目录。

    Args:
        tmp_path: 数据根。

    Returns:
        Path: 分区目录。
    """
    return next(path for path in tmp_path.rglob("*.delay")).parent


async def test_last_write_wins_after_rebuild(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D6：同 `log_id` 再入队追加新记录（不原地改），重建后取最后一条。"""
    ticks = [0.0]
    store = _store(tmp_path, now=clock, tier_clock=lambda: ticks[0], fsync_mode="each")
    first = _record(clock)
    await store.enqueue(first)
    # 跨过次数阈值 → 立即落盘（节流只管窗口内的纯计数）
    await store.enqueue(replace(first, attempts=DELAY_ATTEMPT_FLUSH_ATTEMPTS + 2))

    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 1
    assert [item.attempts for item in await store.list_all()] == [DELAY_ATTEMPT_FLUSH_ATTEMPTS + 2]

    reopened = _store(tmp_path, now=clock)
    report = await reopened.rebuild()
    assert report.records == 1
    assert [item.attempts for item in await reopened.list_all()] == [DELAY_ATTEMPT_FLUSH_ATTEMPTS + 2]


async def test_tenant_and_global_quota(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D7：租户满与全局满都返回 `350015`（`data.scope` 区分），覆盖写不受限。"""
    metrics = DelayMetrics()
    store = _store(tmp_path, now=clock, quota=2, global_quota=3, metrics=metrics)
    await store.enqueue(_record(clock, log_id="t-0"))
    await store.enqueue(_record(clock, log_id="t-1"))

    with pytest.raises(MsgDelayQuotaExceededError) as tenant_full:
        await store.enqueue(_record(clock, log_id="t-2"))
    assert tenant_full.value.code == 350015
    payload = tenant_full.value.data
    assert isinstance(payload, dict)
    assert payload == {"scope": DELAY_SCOPE_TENANT, "limit": 2, "count": 2, "tenant_limit": 2}
    assert "每租户上限 2 条" in str(tenant_full.value.message)

    # 另一个租户还有名额，但全局只剩 1 条
    await store.enqueue(_record(clock, log_id="o-0", tenant_id=OTHER_TENANT))
    with pytest.raises(MsgDelayQuotaExceededError) as global_full:
        await store.enqueue(_record(clock, log_id="o-1", tenant_id=OTHER_TENANT))
    assert global_full.value.data == {
        "scope": DELAY_SCOPE_GLOBAL,
        "limit": 3,
        "count": 3,
        "tenant_limit": 2,
    }
    assert "全局上限 3 条" in str(global_full.value.message)

    # 覆盖写不增总量：即便全局已满也放行（重试永不被容量拒绝）
    await store.enqueue(replace(_record(clock, log_id="t-0"), attempts=1))
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 2
    assert dict(metrics.snapshot()["msg_delay_rejected_total"]) == {"__total__": 2, "tenant": 1, "global": 1}


async def test_partitions_isolated_and_sorted(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D8：跨分区互不影响，两个视图都按 `(deliver_at, log_id)` 升序。"""
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="late", span_ms=600_000))
    await store.enqueue(_record(clock, log_id="same-a", span_ms=60_000))
    await store.enqueue(_record(clock, log_id="same-b", span_ms=60_000, partition_no=1))

    assert [item.log_id for item in await store.list_all()] == ["same-a", "same-b", "late"]
    assert [item.log_id for item in await store.list_for_tenant(tenant_id=TENANT_ID)] == [
        "same-a",
        "same-b",
        "late",
    ]
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 3
    assert await store.count_for_tenant(tenant_id=OTHER_TENANT) == 0
    assert [item.partition_no for item in await store.list_all()] == [0, 1, 0]


async def test_remove_appends_tombstone_and_reclaims_empty_segment(
    tmp_path: Path, clock: Callable[[], datetime]
) -> None:
    """D9：`remove` 幂等、追加 tombstone、段内有效条目归零即删段。"""
    store = _store(tmp_path, now=clock, max_records=1)
    for index in range(3):
        await store.enqueue(_record(clock, log_id=f"l-{index}"))
    directory = _partition_dir(tmp_path)
    assert list_delay_segment_seqs(directory) == [0, 1, 2]

    await store.remove(log_id="l-0")
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 2
    assert 0 not in list_delay_segment_seqs(directory), "段内有效条目归零 → 整段回收"
    assert [item.log_id for item in await store.list_all()] == ["l-1", "l-2"]

    await store.remove(log_id="missing")
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 2


async def test_rotation_keeps_sequence_monotonic(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D10：条数阈值触发轮转，段序号只增不复用（删掉空段也不回收序号）。"""
    store = _store(tmp_path, now=clock, max_records=1)
    for index in range(3):
        await store.enqueue(_record(clock, log_id=f"l-{index}"))
    directory = _partition_dir(tmp_path)
    assert list_delay_segment_seqs(directory) == [0, 1, 2]

    await store.remove(log_id="l-0")
    assert list_delay_segment_seqs(directory) == [1, 2, 3], "序号 0 已删但不复用"
    await store.enqueue(_record(clock, log_id="l-3"))
    assert list_delay_segment_seqs(directory) == [1, 2, 3, 4], "新段接着最大序号往后开"

    assert len(_segment_sizes(tmp_path)) == 4


async def test_attempt_flush_is_throttled(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D11：窗口内只更新内存；超窗口 / 跨阈值 / 关闭时补写。"""
    ticks = [0.0]
    store = _store(tmp_path, now=clock, tier_clock=lambda: ticks[0])
    record = _record(clock, log_id="l-1")
    await store.enqueue(record)
    after_first = _segment_sizes(tmp_path)[0]

    await store.enqueue(replace(record, attempts=1))
    assert _segment_sizes(tmp_path)[0] == after_first, "窗口内不落盘"
    assert [item.attempts for item in await store.list_all()] == [1], "内存立即更新"

    ticks[0] = DELAY_ATTEMPT_FLUSH_MS + 1
    await store.enqueue(replace(record, attempts=2))
    after_window = _segment_sizes(tmp_path)[0]
    assert after_window > after_first

    ticks[0] += 1
    await store.enqueue(replace(record, attempts=2 + DELAY_ATTEMPT_FLUSH_ATTEMPTS))
    after_step = _segment_sizes(tmp_path)[0]
    assert after_step > after_window, "跨越次数阈值即落盘"

    ticks[0] += 1
    await store.enqueue(replace(record, attempts=3 + DELAY_ATTEMPT_FLUSH_ATTEMPTS))
    assert _segment_sizes(tmp_path)[0] == after_step, "又回到窗口内"
    await store.aclose()
    assert _segment_sizes(tmp_path)[0] > after_step, "优雅关闭补写"


async def test_span_limit_enforced_by_port(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D12：跨度超上限由端口层拒绝（`10001`），边界值放行。"""
    store = _store(tmp_path, now=clock)
    with pytest.raises(ParamError) as rejected:
        await store.enqueue(_record(clock, log_id="over", span_ms=DELAY_MAX_SPAN_MS + 1_000))
    assert rejected.value.code == 10001

    await store.enqueue(_record(clock, log_id="exact", span_ms=DELAY_MAX_SPAN_MS))
    assert [item.log_id for item in await store.list_all()] == ["exact"]


async def test_unknown_kind_skipped_without_isolating(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D3（kind 分支）：未知 `kind` 跳过并计数，不隔离整段（将来加 kind 不误伤存量实现）。"""
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="l-1"))
    directory = _partition_dir(tmp_path)
    future = DelayStamp(kind="reschedule", deliver_at_us=1, delay_level="1m", namespace=NAMESPACE, topic=TOPIC_NAME)
    blob = encode_record(
        header=RecordHeader(
            format_version=RECORD_FORMAT_VERSION,
            topic_id=TOPIC_ID,
            partition_no=0,
            tenant_id=TENANT_ID,
            message_id="m-future",
            log_id="l-future",
            key=None,
            trace=EMPTY_TRACE,
            published_at_us=1,
            headers=(),
            content_type="",
            compression="",
            uncompressed_len=None,
            delay=future,
        ),
        payload=b"",
    )
    segment = DelaySegment(partition_dir=directory, seq=list_delay_segment_seqs(directory)[-1])
    segment.append(blob)

    report = await store.rebuild()
    assert report.unknown_kind == 1
    assert report.records == 1
    assert report.isolated_segments == 0
    assert [item.log_id for item in await store.list_all()] == ["l-1"]


async def test_metrics_and_stale_count(
    tmp_path: Path, clock: Callable[[], datetime], advance: Callable[[int], None]
) -> None:
    """D20：`snapshot()` 三项正确；`stale_count()` 按 TTL 统计超期未投。"""
    metrics = DelayMetrics()
    store = _store(tmp_path, now=clock, metrics=metrics)
    await store.enqueue(_record(clock, log_id="l-1"))
    await store.enqueue(_record(clock, log_id="o-1", tenant_id=OTHER_TENANT))
    await store.remove(log_id="o-1")

    snapshot = metrics.snapshot()
    pending = dict(snapshot["msg_delay_pending"])
    assert pending == {"9001": 1, "__total__": 1}
    assert snapshot["msg_delay_segments"] == 1
    assert dict(snapshot["msg_delay_rejected_total"]) == {"__total__": 0}
    assert store.stale_count() == 0

    advance(DELAY_STUCK_TTL_MS + 120_000)
    assert store.stale_count() == 1


async def test_metric_keys_are_bounded(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """指标分键上限：超出的租户折进 `__other__`（`__total__` 仍准确）。"""
    metrics = DelayMetrics(tenant_key_limit=1)
    store = _store(tmp_path, now=clock, metrics=metrics, quota=10, global_quota=10)
    await store.enqueue(_record(clock, log_id="a", tenant_id=1))
    await store.enqueue(_record(clock, log_id="b", tenant_id=2))

    pending = dict(metrics.snapshot()["msg_delay_pending"])
    assert pending == {"1": 1, "__other__": 1, "__total__": 2}

    await store.remove(log_id="a")
    pending = dict(metrics.snapshot()["msg_delay_pending"])
    assert pending == {"__other__": 1, "__total__": 1}


async def test_aclose_is_idempotent_and_closes_handles(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """关闭幂等：重复 `aclose()` 不报错、不再追加记录。"""
    store = _store(tmp_path, now=clock, fsync_mode="each")
    await store.enqueue(_record(clock, log_id="l-1"))
    await store.aclose()
    sizes = _segment_sizes(tmp_path)
    await store.aclose()
    assert _segment_sizes(tmp_path) == sizes


def test_corrupt_keep_constant_matches_log_segments() -> None:
    """取证副本保留份数与 `.log` 同口径（一个常量管两处，避免两套上限）。"""
    assert CORRUPT_TAIL_KEEP == 8
    assert DELAY_KIND_PUT == "put"


async def test_fsync_windows_and_invalid_configuration(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """按批档的刷盘窗口：窗口内不刷、超窗口刷一次；非法配置启动即失败。"""
    ticks = [0.0]
    store = _store(tmp_path, now=clock, fsync_mode="batch", tier_clock=lambda: ticks[0])
    await store.enqueue(_record(clock, log_id="l-1"))
    ticks[0] = 5_000.0
    await store.enqueue(_record(clock, log_id="l-2"))
    assert len(_segment_sizes(tmp_path)) == 1

    with pytest.raises(ValueError, match="MSG_FSYNC_MODE"):
        FileDelayStore(directory=_directory(tmp_path), fsync_mode="nope")
    with pytest.raises(ValueError, match="MSG_FSYNC_INTERVAL_MS"):
        FileDelayStore(directory=_directory(tmp_path), fsync_interval_ms=0)


async def test_rebuild_counts_tombstone_and_plain_records(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """重建时 tombstone 计 `cleared`、普通记录计 `unknown_kind`；指标随重建归零。"""
    metrics = DelayMetrics()
    store = _store(tmp_path, now=clock, metrics=metrics)
    await store.enqueue(_record(clock, log_id="l-1"))
    await store.enqueue(_record(clock, log_id="l-2"))
    await store.remove(log_id="l-1")

    directory = _partition_dir(tmp_path)
    plain = RecordHeader(
        format_version=RECORD_FORMAT_VERSION,
        topic_id=TOPIC_ID,
        partition_no=0,
        tenant_id=TENANT_ID,
        message_id="m-plain",
        log_id="l-plain",
        key=None,
        trace=EMPTY_TRACE,
        published_at_us=1,
        headers=(),
        content_type="",
        compression="",
        uncompressed_len=None,
    )
    segment = DelaySegment(partition_dir=directory, seq=list_delay_segment_seqs(directory)[-1])
    segment.append(encode_record(header=plain, payload=b""))

    report = await store.rebuild()
    assert (report.records, report.cleared, report.unknown_kind) == (1, 1, 1)
    assert dict(metrics.snapshot()["msg_delay_pending"]) == {"9001": 1, "__total__": 1}


async def test_throttle_bypasses_on_field_change_and_smaller_attempts(
    tmp_path: Path, clock: Callable[[], datetime]
) -> None:
    """节流只管「纯计数增长」：字段变化或计数回退都立即落盘。"""
    ticks = [0.0]
    store = _store(tmp_path, now=clock, tier_clock=lambda: ticks[0])
    record = _record(clock, log_id="l-1", attempts=5)
    await store.enqueue(record)
    after_first = _segment_sizes(tmp_path)[0]

    await store.enqueue(replace(record, deliver_at=record.deliver_at + timedelta(seconds=5), attempts=6))
    after_change = _segment_sizes(tmp_path)[0]
    assert after_change > after_first, "改期属实质变化，立即落盘"

    changed = replace(record, deliver_at=record.deliver_at + timedelta(seconds=5), attempts=6)
    await store.enqueue(replace(changed, attempts=3))
    assert _segment_sizes(tmp_path)[0] > after_change, "计数回退（重启后重算）也要落盘"


async def test_default_clock_is_used_when_not_injected(tmp_path: Path) -> None:
    """未注入墙钟时用系统时间（`stale_count` 的缺省路径）。"""
    store = FileDelayStore(directory=_directory(tmp_path), fsync_mode="os", clock=lambda: 0.0)
    assert store.stale_count() == 0


async def test_drop_tenant_removes_records_and_segments(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """租户删除：清内存条目 + 删 `.delay` 段；重建后不复活。"""
    store = _store(tmp_path, now=clock, fsync_mode="each")
    await store.enqueue(_record(clock, log_id="l-1", tenant_id=TENANT_ID))
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 1

    assert store.drop_tenant(tenant_id=TENANT_ID) == 1
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0
    partition_dir = tmp_path / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / "0"
    assert list_delay_segment_seqs(partition_dir) == []
    await store.rebuild()
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0


async def test_drop_tenant_skips_other_tenant_records(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """按租户删除只清目标租户的条目（同分区其他租户条目保留）。"""
    store = _store(tmp_path, now=clock, fsync_mode="each", quota=10, global_quota=10)
    await store.enqueue(_record(clock, log_id="a", tenant_id=TENANT_ID))
    await store.enqueue(_record(clock, log_id="b", tenant_id=OTHER_TENANT))

    assert store.drop_tenant(tenant_id=TENANT_ID) == 1
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0
    assert await store.count_for_tenant(tenant_id=OTHER_TENANT) == 1


async def test_drop_tenant_skips_log_without_directory(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """目录登记缺失时跳过段删除（只清内存条目，不误删未知目录）。"""
    store = _store(tmp_path, now=clock, fsync_mode="each")
    await store.enqueue(_record(clock, log_id="l-1", tenant_id=TENANT_ID))
    store._directory.forget(TOPIC_ID)

    assert store.drop_tenant(tenant_id=TENANT_ID) == 1
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0
