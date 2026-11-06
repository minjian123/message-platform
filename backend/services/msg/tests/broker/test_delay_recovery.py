"""延迟区启动重建与转投偏序（msg 任务 02-7；Kiwi 2432）。

覆盖（详细设计 §7、§8、§12 用例 D14~D18）：

- **D14 重建装填**：未到期按剩余时间落层、已到期进待重试队列（`take_due` 首轮即取出）；
- **D15 `log_id` 去重**：`.log` 里已有该记录的延迟条目被清掉，且不再追加第二条；
- **D16 损坏处置**：末段截断 / 非末段整段隔离，两者都写恢复台账（`msg_recovery` 据此降级）；
- **D17 转投偏序**：`append` → 显式 `sync` → `remove`；`append` 失败时记录保留、计数 +1、不刷盘；
- **D18 关闭**：`DelayCloser` 补写待落盘计数并刷段（幂等）。
"""

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker import delay_file as delay_file_module
from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore
from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.delay_segment import DelaySegment, list_delay_segment_seqs
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.lifecycle import DelayCloser
from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.record import (
    DELAY_KIND_CLEAR,
    RECORD_FORMAT_VERSION,
    DelayStamp,
    RecordHeader,
    encode_record,
    to_epoch_us,
)
from bms_msg.broker.recovery import RecoveryLedger
from bms_msg.broker.scheduler import DELAY_LAYER_SECOND, DelayScheduler, DelayWheel

pytestmark = pytest.mark.kiwi_id(2432)

TENANT_ID = 9_001
"""用例租户主键。"""

TOPIC_ID = 11
"""用例主题主键。"""

NAMESPACE = "orders"
"""用例命名空间。"""

TOPIC_NAME = "orders.events"
"""用例主题名。"""

GARBAGE = b"\x00\x00\x00\xff\x00\x00\x00\x00\x00\x00\x00\x02"
"""半条记录（声明长度越过文件尾）。"""


def _directory(tmp_path: Path) -> PartitionDirectory:
    """构造并绑定主题目录登记表。

    Args:
        tmp_path: 数据根。

    Returns:
        PartitionDirectory: 登记表。
    """
    directory = PartitionDirectory(root=tmp_path)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return directory


def _store(
    tmp_path: Path,
    *,
    now: Callable[[], datetime],
    dedupe: FileMessageStore | None = None,
    ledger: RecoveryLedger | None = None,
    fsync_mode: str = "os",
    tier_clock: Callable[[], float] | None = None,
    max_records: int = 100_000,
) -> FileDelayStore:
    """构造文件延迟区。

    Args:
        tmp_path: 数据根。
        now: 墙钟。
        dedupe: 去重数据源（`.log` 侧）。
        ledger: 恢复台账。
        fsync_mode: fsync 档位。
        tier_clock: 单调毫秒时钟。
        max_records: 段条数阈值。

    Returns:
        FileDelayStore: 延迟区。
    """
    return FileDelayStore(
        directory=_directory(tmp_path),
        dedupe=dedupe,
        ledger=ledger,
        fsync_mode=fsync_mode,
        clock=tier_clock if tier_clock is not None else (lambda: 0.0),
        now=now,
        segment_max_records=max_records,
    )


def _record(
    now: Callable[[], datetime],
    *,
    log_id: str = "l-1",
    span_ms: int = 60_000,
    attempts: int = 0,
) -> DelayRecord:
    """构造一条延迟记录。

    Args:
        now: 墙钟。
        log_id: 记录标识。
        span_ms: 延迟跨度（毫秒）。
        attempts: 转投失败次数。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = now()
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=TENANT_ID,
        topic_id=TOPIC_ID,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=0,
        delay_level="1m",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace_id="trace-1",
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
        attempts=attempts,
    )


def _partition_dir(tmp_path: Path) -> Path:
    """取本用例的分区目录（`data/{tenant}/{namespace}/{topic}.{topic_id}/{partition}`）。

    Args:
        tmp_path: 数据根。

    Returns:
        Path: 分区目录。
    """
    return tmp_path / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / "0"


def _segments(tmp_path: Path) -> list[Path]:
    """列出分区目录下的 `.delay` 段（按段序号升序）。

    Args:
        tmp_path: 数据根。

    Returns:
        list[Path]: 段文件。
    """
    directory = _partition_dir(tmp_path)
    return [directory / f"{seq:020d}.delay" for seq in list_delay_segment_seqs(directory)]


async def test_rebuild_places_records_by_remaining_time(
    tmp_path: Path, clock: Callable[[], datetime], advance: Callable[[int], None]
) -> None:
    """D14：未到期落对应层、已到期进待重试队列（首轮 `take_due` 即取出）。"""
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="soon", span_ms=5_000))
    await store.enqueue(_record(clock, log_id="later", span_ms=600_000))
    await store.enqueue(_record(clock, log_id="due", span_ms=1_000))
    advance(2_000)  # `due` 已到期，`soon` / `later` 未到期

    reopened = _store(tmp_path, now=clock)
    report = await reopened.rebuild()
    assert (report.records, report.expired, report.isolated_segments) == (3, 1, 0)

    wheel = DelayWheel(now=clock)
    for record in await reopened.list_all():
        wheel.place(record)
    due = wheel.take_due(clock())
    assert [item.log_id for item in due] == ["due"]

    remaining = {item.log_id: item for item in await reopened.list_all()}
    assert wheel.layer_of(remaining["soon"]) == DELAY_LAYER_SECOND
    assert wheel.layer_of(remaining["later"]) != DELAY_LAYER_SECOND


async def test_rebuild_clears_records_already_in_log(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D15：`.log` 里已有同 `log_id` 的记录 → 恢复期只清不追加（日志中只有一条）。"""
    directory = _directory(tmp_path)
    messages = FileMessageStore(directory=directory, fsync_mode="each", clock=lambda: 0.0)
    record = _record(clock)
    # 模拟「转投已 append 并落盘、但清 `.delay` 前崩溃」
    await messages.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id=record.message_id,
        log_id=record.log_id,
        key=record.key,
        payload=record.payload,
        headers=record.headers,
        trace_id=record.trace_id,
        published_at=record.deliver_at,
        content_type=DEFAULT_CONTENT_TYPE,
        compression=COMPRESSION_NONE,
        uncompressed_len=None,
    )

    store = _store(tmp_path, now=clock)
    await store.enqueue(record)

    reopened = _store(tmp_path, now=clock, dedupe=messages)
    report = await reopened.rebuild()
    assert (report.records, report.deduped) == (0, 1)
    assert list(await reopened.list_all()) == []

    fetched = await messages.fetch(topic_id=TOPIC_ID, partition_no=0, from_offset=0, max_messages=10)
    assert len(fetched) == 1, "去重只清延迟记录，不再追加第二条日志记录"


async def test_rebuild_isolates_middle_segment_and_records_ledger(
    tmp_path: Path, clock: Callable[[], datetime]
) -> None:
    """D16：非末段损坏 → 整段隔离 + 归口恢复台账（`msg_recovery` 据此降级）。"""
    ledger = RecoveryLedger()
    store = _store(tmp_path, now=clock, ledger=ledger, max_records=1)
    for index in range(3):
        await store.enqueue(_record(clock, log_id=f"l-{index}"))
    segments = _segments(tmp_path)
    assert len(segments) == 3
    with segments[0].open("ab") as handle:
        handle.write(GARBAGE)

    reopened = _store(tmp_path, now=clock, ledger=ledger, max_records=1)
    report = await reopened.rebuild()
    assert report.isolated_segments == 1
    assert report.lost_records == 1
    assert [item.log_id for item in await reopened.list_all()] == ["l-1", "l-2"]
    assert not segments[0].exists(), "整段被改名隔离"
    assert len([path for path in segments[0].parent.iterdir() if ".delay.corrupt-" in path.name]) == 1

    degraded = ledger.degraded()
    assert len(degraded) == 1
    assert degraded[0].delay_isolated_segments == (0,)
    assert degraded[0].delay_lost_records == 1


async def test_rebuild_truncates_corrupt_tail(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D16：末段尾部坏记录 → 截断并留取证副本（已写好的记录全部保留）。"""
    ledger = RecoveryLedger()
    store = _store(tmp_path, now=clock, ledger=ledger, fsync_mode="each")
    await store.enqueue(_record(clock, log_id="l-1"))
    segment = _segments(tmp_path)[0]
    complete_bytes = segment.stat().st_size
    with segment.open("ab") as handle:
        handle.write(GARBAGE)

    reopened = _store(tmp_path, now=clock, ledger=ledger)
    report = await reopened.rebuild()
    assert report.truncated_bytes == len(GARBAGE)
    assert report.lost_records == 0
    assert [item.log_id for item in await reopened.list_all()] == ["l-1"]
    assert segment.stat().st_size == complete_bytes, "截断回完整区间"
    assert len([path for path in segment.parent.iterdir() if ".delay.corrupt-" in path.name]) == 1
    assert ledger.degraded()[0].delay_truncated_bytes == len(GARBAGE)


class _Durable:
    """耐久点替身：记录 `sync` 调用（`DelayScheduler.durable`）。"""

    def __init__(self) -> None:
        """初始化（无调用）。"""
        self.calls: list[tuple[int, int]] = []

    async def sync(self, *, topic_id: int, partition_no: int) -> None:
        """记录一次刷盘。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
        """
        self.calls.append((topic_id, partition_no))


class _GuardedStore(InMemoryDelayStore):
    """删除前断言「日志已刷盘」的延迟区替身（偏序的机器判据）。"""

    def __init__(self, durable: _Durable) -> None:
        """初始化。

        Args:
            durable: 耐久点替身。
        """
        super().__init__()
        self._durable = durable

    async def remove(self, *, log_id: str) -> None:
        """先断言已刷盘，再删除。

        Args:
            log_id: 记录标识。
        """
        assert self._durable.calls, "必须先 append 并 fsync 才能清延迟记录"
        await super().remove(log_id=log_id)


class _FailingMessages(InMemoryMessageStore):
    """转投必失败的存储替身（模拟盘满 / 主题停用）。"""

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
    ) -> StoredMessage:
        """恒抛 `OSError`（ENOSPC）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 消息标识。
            log_id: 记录标识。
            key: 分区键。
            payload: 载荷。
            headers: 结构化头。
            trace_id: 链路标识。
            published_at: 发布时间。
            content_type: 内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数。

        Raises:
            OSError: 恒抛（磁盘满）。
        """
        del topic_id, partition_no, message_id, log_id, key, payload, headers, trace_id, published_at
        del content_type, compression, uncompressed_len
        raise OSError(28, "No space left on device")


async def test_transfer_syncs_before_clearing(clock: Callable[[], datetime], advance: Callable[[int], None]) -> None:
    """D17：转投顺序为 `append` → `sync` → `remove`（`sync` 收到正确的分区坐标）。"""
    durable = _Durable()
    store = _GuardedStore(durable)
    wheel = DelayWheel(now=clock)
    messages = InMemoryMessageStore()
    record = _record(clock, span_ms=1_000)
    await store.enqueue(record)
    wheel.place(record)
    advance(1_000)

    scheduler = DelayScheduler(store=store, messages=messages, wheel=wheel, now=clock, interval_ms=10, durable=durable)
    assert await scheduler.run_once() == 1

    assert durable.calls == [(TOPIC_ID, 0)]
    assert list(await store.list_all()) == []
    assert await messages.leo(topic_id=TOPIC_ID, partition_no=0) == 1


async def test_transfer_failure_keeps_record_without_sync(
    clock: Callable[[], datetime], advance: Callable[[int], None]
) -> None:
    """D17：`append` 失败 → 记录保留、`attempts + 1`、不刷盘、不删记录。"""
    durable = _Durable()
    store = _GuardedStore(durable)
    wheel = DelayWheel(now=clock)
    record = _record(clock, span_ms=1_000)
    await store.enqueue(record)
    wheel.place(record)
    advance(1_000)

    scheduler = DelayScheduler(
        store=store,
        messages=_FailingMessages(),
        wheel=wheel,
        now=clock,
        interval_ms=10,
        durable=durable,
    )
    assert await scheduler.run_once() == 0

    assert durable.calls == []
    records = list(await store.list_all())
    assert [item.attempts for item in records] == [1]


async def test_delay_closer_flushes_pending_attempts(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D18：`DelayCloser` 补写窗口内未落盘的 `attempts`，且重复调用幂等。"""
    ticks = [0.0]
    store = _store(tmp_path, now=clock, fsync_mode="each", tier_clock=lambda: ticks[0])
    record = _record(clock)
    await store.enqueue(record)
    await store.enqueue(_replace_attempts(record, 3))
    size_before = _segments(tmp_path)[0].stat().st_size

    closer = DelayCloser(store=store)
    await closer.aclose()
    size_after = _segments(tmp_path)[0].stat().st_size
    assert size_after > size_before, "关闭时补写一次"

    await closer.aclose()
    assert _segments(tmp_path)[0].stat().st_size == size_after, "幂等：不再重复追加"


async def test_rebuild_is_idempotent(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """重建可重复执行（启动钩子幂等）：结果一致、不重复计数。"""
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="l-1"))
    first = await store.rebuild()
    second = await store.rebuild()

    assert (first.records, second.records) == (1, 1)
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 1
    assert list_delay_segment_seqs(_partition_dir(tmp_path)) == [0]


async def test_rebuild_without_dedupe_source_keeps_records(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """未注入去重源时不做比对（轻量用例路径），记录原样保留。"""
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="l-1"))
    report = await store.rebuild()
    assert (report.records, report.deduped) == (1, 0)


def _replace_attempts(record: DelayRecord, attempts: int) -> DelayRecord:
    """替换记录的 `attempts`（只是纯计数变化 → 会走节流）。

    Args:
        record: 原记录。
        attempts: 新的失败次数。

    Returns:
        DelayRecord: 新记录。
    """
    return replace(record, attempts=attempts)


async def test_log_ids_since_anchors_by_time(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D15 配套：`.log` 侧按时间锚点扫描（跨段 / 空候选集 / 无更早索引项三种情形）。"""
    messages = FileMessageStore(
        directory=_directory(tmp_path), segment_max_bytes=200, fsync_mode="os", clock=lambda: 0.0
    )
    base = clock()
    moments = [base, base + timedelta(seconds=10), base + timedelta(seconds=20)]
    for index, moment in enumerate(moments):
        await messages.append(
            topic_id=TOPIC_ID,
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"l-{index}",
            key=None,
            payload=b"x" * 90,
            headers=ConcurrentStableDict(),
            trace_id=None,
            published_at=moment,
            content_type=DEFAULT_CONTENT_TYPE,
            compression=COMPRESSION_NONE,
            uncompressed_len=None,
        )
    assert len(await messages.segments(topic_id=TOPIC_ID, partition_no=0)) >= 2

    assert await messages.log_ids_since(topic_id=TOPIC_ID, partition_no=0, since_us=0, candidates=set()) == set()
    # 下界落在第 1 与第 2 条之间：第 1 段整段早于下界被跳过，锚点取「最后一条 ≤ 下界」的索引项
    between = to_epoch_us(moments[0]) + 1
    assert await messages.log_ids_since(
        topic_id=TOPIC_ID, partition_no=0, since_us=between, candidates={"l-0", "l-1", "l-2"}
    ) == {"l-1", "l-2"}
    # 下界早于全部记录：锚点为段首（无更早时间索引项）
    assert await messages.log_ids_since(topic_id=TOPIC_ID, partition_no=0, since_us=0, candidates={"l-0", "l-2"}) == {
        "l-0",
        "l-2",
    }


async def test_store_sync_writes_recovery_point(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D17 配套：`FileMessageStore.sync()` 刷盘并落恢复点（转投的耐久点）。"""
    del clock
    messages = FileMessageStore(directory=_directory(tmp_path), fsync_mode="os", clock=lambda: 0.0)
    await messages.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id="m-1",
        log_id="l-1",
        key=None,
        payload=b"payload",
        headers=ConcurrentStableDict(),
        trace_id=None,
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_type=DEFAULT_CONTENT_TYPE,
        compression=COMPRESSION_NONE,
        uncompressed_len=None,
    )
    point = _partition_dir(tmp_path) / "recovery-point.json"
    point.unlink(missing_ok=True)
    await messages.sync(topic_id=TOPIC_ID, partition_no=0)
    assert point.exists()


async def test_rebuild_dedupes_multiple_records_in_one_partition(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """D15：同分区多条待投记录时，去重下界取其中最早 `deliver_at`（一次扫描判定全部）。"""
    messages = FileMessageStore(directory=_directory(tmp_path), fsync_mode="each", clock=lambda: 0.0)
    store = _store(tmp_path, now=clock)
    first = _record(clock, log_id="l-1", span_ms=1_000)
    second = _record(clock, log_id="l-2", span_ms=600_000)
    third = _record(clock, log_id="l-3", span_ms=1_200_000)
    await store.enqueue(second)  # 先入长跨度，后入短跨度 → 遍历时下界会被替换（更早者胜出）
    await store.enqueue(first)
    await store.enqueue(third)
    await messages.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id=first.message_id,
        log_id=first.log_id,
        key=first.key,
        payload=first.payload,
        headers=first.headers,
        trace_id=first.trace_id,
        published_at=first.deliver_at,
        content_type=DEFAULT_CONTENT_TYPE,
        compression=COMPRESSION_NONE,
        uncompressed_len=None,
    )

    reopened = _store(tmp_path, now=clock, dedupe=messages)
    report = await reopened.rebuild()
    assert (report.records, report.deduped) == (2, 1)
    assert [item.log_id for item in await reopened.list_all()] == ["l-2", "l-3"]


async def test_rebuild_skips_unrelated_entries_under_data_root(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """重建的目录遍历对「非租户 / 非目录 / 不可解析主题名 / 非分区」一律跳过，不影响真实记录。"""
    root = tmp_path / "data"
    (root / "not-a-tenant").mkdir(parents=True)
    (root / str(TENANT_ID)).mkdir()
    (root / str(TENANT_ID) / "loose.txt").write_text("x", encoding="utf-8")
    (root / str(TENANT_ID) / NAMESPACE).mkdir()
    (root / str(TENANT_ID) / NAMESPACE / "loose.txt").write_text("x", encoding="utf-8")
    (root / str(TENANT_ID) / NAMESPACE / "nodot").mkdir()
    topic = root / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}"
    topic.mkdir(parents=True)
    (topic / "notes.txt").write_text("x", encoding="utf-8")
    (topic / "0").mkdir()
    (topic / "1").mkdir()  # 无 `.delay` 段的分区目录

    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock))

    reopened = _store(tmp_path, now=clock)
    report = await reopened.rebuild()
    assert report.records == 1
    assert report.isolated_segments == 0


async def test_rebuild_without_data_root_is_empty(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """数据根不存在（全新部署）→ 重建为空，不抛错。"""
    store = FileDelayStore(
        directory=PartitionDirectory(root=tmp_path / "missing"),
        fsync_mode="os",
        clock=lambda: 0.0,
        now=clock,
    )
    report = await store.rebuild()
    assert (report.partitions, report.records) == (0, 0)


async def test_rebuild_survives_clear_write_failure(
    tmp_path: Path, clock: Callable[[], datetime], monkeypatch: pytest.MonkeyPatch
) -> None:
    """恢复期补写 tombstone 失败只告警（下次重建会重做，幂等），记录仍被清掉。"""
    messages = FileMessageStore(directory=_directory(tmp_path), fsync_mode="each", clock=lambda: 0.0)
    store = _store(tmp_path, now=clock)
    record = _record(clock)
    await store.enqueue(record)
    await messages.append(
        topic_id=TOPIC_ID,
        partition_no=0,
        message_id=record.message_id,
        log_id=record.log_id,
        key=record.key,
        payload=record.payload,
        headers=record.headers,
        trace_id=record.trace_id,
        published_at=record.deliver_at,
        content_type=DEFAULT_CONTENT_TYPE,
        compression=COMPRESSION_NONE,
        uncompressed_len=None,
    )

    reopened = _store(tmp_path, now=clock, dedupe=messages)
    # 重建期间唯一的 `append` 就是「补写 tombstone」，把分区写入口整体换成失败实现即可
    monkeypatch.setattr(delay_file_module._PartitionDelayLog, "append", _fail_append)  # pyright: ignore[reportPrivateUsage]
    report = await reopened.rebuild()

    assert (report.records, report.deduped) == (0, 1)
    assert list(await reopened.list_all()) == []


def _fail_append(self: object, blob: bytes, *, record_bytes: int) -> object:
    """替身：任何写入都抛 `OSError(ENOSPC)`。

    Args:
        self: 分区日志（未使用）。
        blob: 记录字节。
        record_bytes: 记录字节数。

    Raises:
        OSError: 恒抛（磁盘满）。
    """
    del self, blob, record_bytes
    raise OSError(28, "No space left on device")


async def test_rebuild_tolerates_tombstone_without_put(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """重建遇到「只有 tombstone、没有对应 put」时按无操作处理（put 可能随隔离段一起丢失）。"""
    directory = _partition_dir(tmp_path)
    directory.mkdir(parents=True)
    DelaySegment(partition_dir=directory, seq=0).append(_clear_only_blob())

    store = _store(tmp_path, now=clock)
    report = await store.rebuild()
    assert (report.records, report.cleared) == (0, 0), "孤儿 tombstone 不计数、不报错"
    assert list(await store.list_all()) == []


async def test_dedupe_without_hits_is_noop(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """去重源可用但没有命中：不清任何记录（`cleared == 0` 分支）。"""
    messages = FileMessageStore(directory=_directory(tmp_path), fsync_mode="each", clock=lambda: 0.0)
    store = _store(tmp_path, now=clock)
    await store.enqueue(_record(clock, log_id="l-1"))

    reopened = _store(tmp_path, now=clock, dedupe=messages)
    report = await reopened.rebuild()
    assert (report.records, report.deduped) == (1, 0)


async def test_aclose_after_failed_first_append(tmp_path: Path, clock: Callable[[], datetime]) -> None:
    """首个 append 就失败（分区尚无段）后优雅关闭：不报错、不刷不存在的段。"""
    store = _store(tmp_path, now=clock)
    log = store._partition(topic_id=TOPIC_ID, partition_no=0)  # pyright: ignore[reportPrivateUsage] - 造「有分区无段」状态
    log.fsync_active()
    await store.aclose()
    assert list_delay_segment_seqs(_partition_dir(tmp_path)) == []


def _clear_only_blob() -> bytes:
    """构造一条只有 tombstone 的记录字节（无对应 put）。

    Returns:
        bytes: 完整记录字节。
    """
    header = RecordHeader(
        format_version=RECORD_FORMAT_VERSION,
        topic_id=TOPIC_ID,
        partition_no=0,
        tenant_id=TENANT_ID,
        message_id="m-orphan",
        log_id="l-orphan",
        key=None,
        trace_id=None,
        published_at_us=1,
        headers=(),
        content_type="",
        compression="",
        uncompressed_len=None,
        delay=DelayStamp(kind=DELAY_KIND_CLEAR),
    )
    return encode_record(header=header, payload=b"")
