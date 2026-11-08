"""消费位点文件实现与三个写侧协议（任务 02-3；Kiwi 2429）。

覆盖三条结构性口径的行为面：
1. **单文件双槽**：格式自洽、`seq` 递增、主槽坏了用另一槽回退、两槽全坏 fail-closed；
2. **内存为准 + 刷盘**：`commit` 零 IO、后台刷 / 脏阈值提前刷 / 关闭强制刷、单飞与版本比对；
3. **越界不夹取**：负数与 `>LEO` 一律 `10001`，`==LEO` 合法。

另覆盖 `reset` / `list_group` / `purge_*` 三协议、装配接线与关闭偏序（先日志后位点）。
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.core.capability import BaseAsyncResource
from bms_core.core.exceptions import ParamError
from bms_core.core.resources import ResourceManager
from bms_core.events.base import EventEnvelope

from bms_msg.api.deps import (
    get_offset_enumerator,
    get_offset_purger,
    get_offset_resetter,
    get_offset_store,
)
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.lifecycle import BackgroundLoops, OffsetFlusher, OffsetFlushLoop, StorageCloser
from bms_msg.broker.log import PartitionNotBoundError
from bms_msg.broker.offset import (
    OFFSET_REASON_ABSENT,
    OFFSET_REASON_CRC,
    OFFSET_REASON_HEADER,
    OFFSET_REASON_IDENTITY,
    OFFSET_REASON_OK,
    OFFSET_REASON_PARTIAL,
    OFFSET_REASON_TRUNCATED,
    OFFSET_REASON_VERSION,
    OFFSET_RESET_ACTION,
    OFFSET_RESET_PATH,
    FileOffsetStore,
    InMemoryOffsetStore,
    OffsetAuditError,
    OffsetSlot,
    OffsetUnavailableError,
    decode_offset_file,
    encode_offset_file,
    group_directory_name,
    parse_offset_filename,
)
from bms_msg.core.config import (
    OFFSET_FILE_MAGIC,
    OFFSET_FILE_SUFFIX,
    OFFSET_HEADER_FIXED_BYTES,
    OFFSET_SLOT_BYTES,
    OFFSETS_SUBDIR,
)

pytestmark = pytest.mark.kiwi_id(2429)

TENANT_ID = 9_001
TOPIC_ID = 41
OTHER_TOPIC_ID = 42
NAMESPACE = "billing"
TOPIC_NAME = "orders"
GROUP = "audit"


class _AuditStub:
    """审计出口替身（记录事件；可配置「拒绝」或「抛异常」）。"""

    def __init__(self, *, accepted: bool = True, error: Exception | None = None) -> None:
        """初始化。

        Args:
            accepted: `emit` 的返回值（False 表示被拒）。
            error: 非 None 时 `emit` 抛出该异常。
        """
        self.events: list[EventEnvelope] = []
        self.accepted = accepted
        self.error = error

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录并回传配置的结果。

        Args:
            event: 事件信封。

        Returns:
            bool: 配置的返回值。

        Raises:
            Exception: 配置了 `error` 时抛出。
        """
        if self.error is not None:
            raise self.error
        self.events.append(event)
        return self.accepted


class _CeilingStub:
    """LEO 只读端口替身（固定值，可改）。"""

    def __init__(self, leo: int) -> None:
        """初始化。

        Args:
            leo: 固定的 LEO。
        """
        self.leo_value = leo

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """返回固定 LEO。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 固定 LEO。
        """
        return self.leo_value


class _LoopStub:
    """后台循环替身（`BackgroundLoops` 只调 `stop`）。"""

    def __init__(self, calls: list[str]) -> None:
        """初始化。

        Args:
            calls: 共享的调用顺序记录。
        """
        self._calls = calls

    async def stop(self) -> None:
        """记录一次停止。"""
        self._calls.append("loops")


class _NoopCloseable:
    """消息存储替身（`StorageCloser` 只调 `aclose`）。"""

    def __init__(self, calls: list[str]) -> None:
        """初始化。

        Args:
            calls: 共享的调用顺序记录。
        """
        self._calls = calls

    async def aclose(self) -> None:
        """记录一次日志刷盘。"""
        self._calls.append("log")


class _OrderStore:
    """位点存储替身（记录「停循环」与「刷位点」的先后）。"""

    def __init__(self, calls: list[str]) -> None:
        """初始化。

        Args:
            calls: 共享的调用顺序记录。
        """
        self._calls = calls

    def ensure_started(self) -> None:
        """无操作。"""

    async def stop(self) -> None:
        """记录一次停循环。"""
        self._calls.append("stop")

    async def run_once(self) -> int:
        """不刷任何文件。

        Returns:
            int: 恒 0。
        """
        return 0

    async def aclose(self) -> None:
        """记录一次刷位点。"""
        self._calls.append("offset")


def _directory(root: Path, *, topic_id: int = TOPIC_ID) -> PartitionDirectory:
    """构造并绑定一个主题目录登记表。

    Args:
        root: 数据根。
        topic_id: 主题主键。

    Returns:
        PartitionDirectory: 已登记用例主题的登记表。
    """
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=topic_id)
    return directory


def _store(root: Path, **kwargs: Any) -> FileOffsetStore:
    """构造文件位点存储（缺省注入审计替身与固定时钟）。

    Args:
        root: 数据根。
        **kwargs: 覆盖项（`audit` / `ceiling` / `directory` / `dirty_limit` / `flush_interval_ms`）。

    Returns:
        FileOffsetStore: 位点存储。
    """
    audit = cast("_AuditStub", kwargs.pop("audit", _AuditStub()))
    ceiling = cast("_CeilingStub | None", kwargs.pop("ceiling", None))
    directory = cast("PartitionDirectory", kwargs.pop("directory", _directory(root)))
    store = FileOffsetStore(
        directory=directory,
        root=root,
        audit=audit,
        now_us=lambda: 1_700_000_000_000_000,
        **kwargs,
    )
    if ceiling is not None:
        store.bind_ceiling(ceiling)
    return store


def _path(root: Path, *, group: str = GROUP, topic_id: int = TOPIC_ID, partition_no: int = 0) -> Path:
    """算用例里的位点文件路径。

    Args:
        root: 数据根。
        group: 消费组。
        topic_id: 主题主键。
        partition_no: 分区号。

    Returns:
        Path: 位点文件路径。
    """
    return (
        root
        / OFFSETS_SUBDIR
        / str(TENANT_ID)
        / group_directory_name(group)
        / f"{topic_id}-{partition_no}{OFFSET_FILE_SUFFIX}"
    )


async def _commit(store: FileOffsetStore, offset: int, *, group: str = GROUP, partition_no: int = 0) -> None:
    """提交一次位点并落盘（多数用例只关心落盘后的文件）。

    Args:
        store: 位点存储。
        offset: 目标位点。
        group: 消费组。
        partition_no: 分区号。
    """
    await store.commit(group=group, topic_id=TOPIC_ID, partition_no=partition_no, offset=offset)
    await store.flush_once()


# ---------------------------------------------------------------------------
# O1 - O8：文件格式与读取
# ---------------------------------------------------------------------------


async def test_file_layout_and_identity(tmp_path: Path) -> None:
    """O1：文件字节布局与身份字段自洽，且 Windows 下没被写成 CRLF。"""
    store = _store(tmp_path)
    await _commit(store, 7)

    raw = _path(tmp_path).read_bytes()

    assert raw[:4] == OFFSET_FILE_MAGIC
    assert b"\r\n" not in raw  # O_BINARY：文本模式会把 0x0A 翻成 0x0D0A
    readout = decode_offset_file(raw, group=GROUP, topic_id=TOPIC_ID, partition_no=0)
    assert readout.reason == OFFSET_REASON_OK
    assert (readout.group, readout.topic_id, readout.partition_no) == (GROUP, TOPIC_ID, 0)
    assert readout.persisted is not None
    assert readout.persisted.offset == 7
    assert len(raw) == OFFSET_HEADER_FIXED_BYTES + len(GROUP.encode()) + 2 * OFFSET_SLOT_BYTES


async def test_dual_slot_seq_increases(tmp_path: Path) -> None:
    """O2：两次提交后两槽 `seq` 递增，最新值在 `seq` 大的槽、上一版在另一槽。"""
    store = _store(tmp_path)
    await _commit(store, 3)
    await _commit(store, 5)

    readout = decode_offset_file(_path(tmp_path).read_bytes())

    assert readout.reason == OFFSET_REASON_OK
    assert readout.persisted is not None and readout.previous is not None
    assert (readout.previous.offset, readout.persisted.offset) == (3, 5)
    assert readout.persisted.seq == 2


async def test_restart_resumes_from_persisted_offset(tmp_path: Path) -> None:
    """O3：换一个实例重读文件，位点与落盘值一致（不依赖进程内缓存）。"""
    await _commit(_store(tmp_path), 11)

    reopened = _store(tmp_path)

    assert await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 11


async def test_broken_newest_slot_falls_back_to_previous(tmp_path: Path) -> None:
    """O4：最新槽 CRC 坏 → 用另一槽（上一版）回退，不归零。"""
    store = _store(tmp_path)
    await _commit(store, 3)
    await _commit(store, 5)
    path = _path(tmp_path)
    raw = bytearray(path.read_bytes())
    header_len = OFFSET_HEADER_FIXED_BYTES + len(GROUP.encode())
    raw[header_len + 20] ^= 0xFF  # 打坏第一槽（新值）的 CRC
    path.write_bytes(bytes(raw))

    reopened = _store(tmp_path)

    assert await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 3
    assert decode_offset_file(bytes(raw)).reason == OFFSET_REASON_PARTIAL


async def test_both_slots_broken_is_fail_closed(tmp_path: Path) -> None:
    """O5：两槽均坏 → 抛错（不归零、不跳 LEO），而不是静默从头消费。"""
    await _commit(_store(tmp_path), 9)
    path = _path(tmp_path)
    raw = bytearray(path.read_bytes())
    header_len = OFFSET_HEADER_FIXED_BYTES + len(GROUP.encode())
    for index in range(2):
        raw[header_len + index * OFFSET_SLOT_BYTES] ^= 0xFF
    path.write_bytes(bytes(raw))

    reopened = _store(tmp_path)

    assert decode_offset_file(bytes(raw)).reason == OFFSET_REASON_CRC
    with pytest.raises(OffsetUnavailableError):
        await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0)


async def test_future_format_version_is_rejected(tmp_path: Path) -> None:
    """O6：格式版本高于实现 → 拒绝加载（不按旧格式猜）。"""
    await _commit(_store(tmp_path), 4)
    path = _path(tmp_path)
    raw = bytearray(path.read_bytes())
    raw[4:6] = (99).to_bytes(2, "big")
    path.write_bytes(bytes(raw))

    reopened = _store(tmp_path)

    assert decode_offset_file(bytes(raw)).reason == OFFSET_REASON_VERSION
    with pytest.raises(OffsetUnavailableError):
        await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0)


async def test_absent_file_reads_as_zero(tmp_path: Path) -> None:
    """O7：文件不存在 → 0（从未提交），且不建目录、不写盘。"""
    store = _store(tmp_path)

    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 0
    assert not _path(tmp_path).exists()
    assert not (tmp_path / OFFSETS_SUBDIR).exists()


async def test_identity_mismatch_is_unavailable(tmp_path: Path) -> None:
    """O8：文件被挪到别的组目录（身份不符）→ 不可用，不误读别人的位点。"""
    await _commit(_store(tmp_path), 6, group="audit")
    original = _path(tmp_path, group="audit")
    moved = _path(tmp_path, group="notify")
    moved.parent.mkdir(parents=True, exist_ok=True)
    moved.write_bytes(original.read_bytes())

    reopened = _store(tmp_path)

    assert decode_offset_file(moved.read_bytes(), group="notify").reason == OFFSET_REASON_IDENTITY
    with pytest.raises(OffsetUnavailableError):
        await reopened.get(group="notify", topic_id=TOPIC_ID, partition_no=0)


# ---------------------------------------------------------------------------
# O9 - O15：提交语义
# ---------------------------------------------------------------------------


async def test_negative_offset_is_rejected(tmp_path: Path) -> None:
    """O9：负位点直接拒绝（`10001`），不写盘。"""
    store = _store(tmp_path)

    with pytest.raises(ParamError):
        await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=-1)
    assert not _path(tmp_path).exists()


async def test_offset_over_leo_is_rejected(tmp_path: Path) -> None:
    """O10：`>LEO` 拒绝、`==LEO` 合法（等于「已消费到末尾」）。"""
    store = _store(tmp_path, ceiling=_CeilingStub(5))

    with pytest.raises(ParamError):
        await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=6)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=5)
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 5


async def test_unbound_ceiling_skips_upper_bound(tmp_path: Path) -> None:
    """O11：未绑 LEO（装配缺陷）→ 跳过上界判定、不阻断写入。"""
    store = _store(tmp_path)

    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=10_000)

    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 10_000


async def test_commit_only_moves_forward(tmp_path: Path) -> None:
    """O12：小值提交不生效、同值提交幂等（且不产生新的落盘）。"""
    store = _store(tmp_path)
    await _commit(store, 5)

    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=2)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=5)

    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 5
    assert await store.flush_once() == 0  # 无脏：幂等提交不标脏


async def test_offsets_are_isolated_by_group_topic_partition(tmp_path: Path) -> None:
    """O13：位点按 `(group, topic, partition)` 三维隔离，各自独立文件。"""
    store = _store(tmp_path)
    await _commit(store, 4, group="audit", partition_no=0)
    await _commit(store, 8, group="audit", partition_no=1)
    await _commit(store, 12, group="notify", partition_no=0)

    assert await store.get(group="audit", topic_id=TOPIC_ID, partition_no=0) == 4
    assert await store.get(group="audit", topic_id=TOPIC_ID, partition_no=1) == 8
    assert await store.get(group="notify", topic_id=TOPIC_ID, partition_no=0) == 12
    assert len(list((tmp_path / OFFSETS_SUBDIR / str(TENANT_ID)).rglob(f"*{OFFSET_FILE_SUFFIX}"))) == 3


async def test_group_directory_is_case_sensitive_safe(tmp_path: Path) -> None:
    """O14：仅大小写不同的两个组落到不同目录（Windows 大小写不敏感也不串组）。"""
    assert group_directory_name("Audit") != group_directory_name("audit")

    store = _store(tmp_path)
    await _commit(store, 3, group="Audit")
    await _commit(store, 9, group="audit")

    assert await store.get(group="Audit", topic_id=TOPIC_ID, partition_no=0) == 3
    assert await store.get(group="audit", topic_id=TOPIC_ID, partition_no=0) == 9


async def test_group_directory_name_edges(tmp_path: Path) -> None:
    """O15：组名 `.` / 128 字符 / 含 `-` 都得到安全且唯一的目录名。"""
    assert group_directory_name(".").startswith(".-")
    assert group_directory_name("..").startswith("..-")
    long_name = "g" * 128
    trimmed = group_directory_name(long_name)
    assert len(trimmed) <= 64 + 1 + 8
    assert trimmed != group_directory_name("g" * 127)
    assert group_directory_name("a-b") != group_directory_name("a") + "-" + group_directory_name("b")
    store = _store(tmp_path)
    await _commit(store, 2, group=long_name)
    assert await store.get(group=long_name, topic_id=TOPIC_ID, partition_no=0) == 2


# ---------------------------------------------------------------------------
# O16 - O20：刷盘
# ---------------------------------------------------------------------------


async def test_background_flush_writes_dirty_entries(tmp_path: Path) -> None:
    """O16：`run_once` 把脏条目落盘，落盘后再刷为 0（脏已清）。"""
    store = _store(tmp_path)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=6)

    assert await store.run_once() == 1
    assert _path(tmp_path).exists()
    assert await store.run_once() == 0


async def test_dirty_limit_wakes_the_flush_loop(tmp_path: Path) -> None:
    """O17：脏条目达到阈值即唤醒后台刷盘（不必等窗口）。"""
    store = _store(tmp_path, dirty_limit=2)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=1, offset=1)

    assert cast("Any", store)._wakeup.is_set() is True  # 私有状态是「是否被唤醒」的唯一判据


async def test_close_flushes_unconditionally(tmp_path: Path) -> None:
    """O18：`aclose` 无条件落盘（不受窗口影响），并清空进程内缓存。"""
    store = _store(tmp_path, flush_interval_ms=60_000)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=8)

    await store.aclose()

    reopened = _store(tmp_path)
    assert await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 8


async def test_flush_failure_keeps_dirty_and_does_not_block_commit(tmp_path: Path) -> None:
    """O19：写盘失败保留脏标记并告警，`commit` 不因此报错（fail-open）。"""
    store = _store(tmp_path)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=3)
    path = _path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.mkdir()  # 让 os.open(目标，O_TRUNC) 失败：目录不可当文件写

    assert await store.flush_once() == 0
    assert cast("Any", store)._entries[GROUP, TOPIC_ID, 0].dirty is True
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=4)
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 4


async def test_version_mismatch_keeps_entry_dirty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """O20：快照取完后内存又前进 → 不清脏（下一轮补刷），且已落盘值不受影响。"""
    store = _store(tmp_path)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=5)
    original = cast("Any", store)._write

    def _write_then_bump(snapshot: Any) -> None:
        original(snapshot)
        snapshot.entry.current = 6
        snapshot.entry.version += 1

    monkeypatch.setattr(store, "_write", _write_then_bump)

    assert await store.flush_once() == 1

    entry = cast("Any", store)._entries[GROUP, TOPIC_ID, 0]
    assert entry.dirty is True
    assert entry.persisted == 5


async def test_flush_loop_starts_and_stops(tmp_path: Path) -> None:
    """O16 补充：刷盘循环能起、能停，单轮异常不终止循环。"""
    store = _store(tmp_path, flush_interval_ms=1)
    calls: list[int] = []

    async def _run_once() -> int:
        calls.append(1)
        raise RuntimeError("boom")

    store.ensure_started()
    assert store.running is True
    store.ensure_started()  # 幂等
    monkeypatch_target = cast("Any", store)
    monkeypatch_target.run_once = _run_once
    await asyncio.sleep(0.05)
    await store.stop()
    await store.stop()  # 幂等

    assert store.running is False
    assert calls


# ---------------------------------------------------------------------------
# O21 - O23：位点对齐
# ---------------------------------------------------------------------------


async def test_realign_clamps_and_persists_immediately(tmp_path: Path) -> None:
    """O21：超 `ceiling` 的位点被夹到 `ceiling`、立即落盘、返回调整清单。"""
    store = _store(tmp_path)
    await _commit(store, 9, group="audit")
    await _commit(store, 9, group="notify")

    adjusted = await store.realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=4)

    assert sorted((item.group, item.previous, item.current) for item in adjusted) == [
        ("audit", 9, 4),
        ("notify", 9, 4),
    ]
    reopened = _store(tmp_path)
    assert await reopened.get(group="audit", topic_id=TOPIC_ID, partition_no=0) == 4


async def test_realign_leaves_in_range_offsets_alone(tmp_path: Path) -> None:
    """O22：未越界不做任何调整（清单为空、不触发刷盘）。"""
    store = _store(tmp_path)
    await _commit(store, 3)

    adjusted = await store.realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=9)

    assert len(adjusted) == 0
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 3


async def test_realign_without_tenant_returns_empty(tmp_path: Path) -> None:
    """O23：主题目录未登记（盘上没有该分区）→ 空清单、不报错。"""
    store = _store(tmp_path, directory=PartitionDirectory(root=tmp_path))

    assert len(await store.realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=1)) == 0


# ---------------------------------------------------------------------------
# O24 - O25：装配与关闭偏序
# ---------------------------------------------------------------------------


async def test_dependency_ports_resolve_to_file_store(tmp_path: Path) -> None:
    """O24：四个依赖出口都取到同一个文件实现实例。"""
    store = _store(tmp_path)
    request = cast("Any", SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(offset_store=store))))

    assert get_offset_store(request) is store
    assert get_offset_resetter(request) is store
    assert get_offset_enumerator(request) is store
    assert get_offset_purger(request) is store


async def test_close_order_is_stop_then_log_then_offset() -> None:
    """O25：释放序 = 停后台循环 → 停位点刷盘 → 刷日志 → 刷位点（先日志后位点）。"""
    calls: list[str] = []
    offsets = _OrderStore(calls)
    resources = ResourceManager()
    resources.register(OffsetFlusher(store=offsets))
    resources.register(StorageCloser(store=_NoopCloseable(calls)))
    resources.register(OffsetFlushLoop(store=offsets))
    resources.register(BackgroundLoops(scheduler=cast("Any", _LoopStub(calls)), runner=cast("Any", _LoopStub(calls))))

    await resources.aclose()

    assert calls == ["loops", "loops", "stop", "log", "offset"]


def test_flush_handles_are_async_resources() -> None:
    """O25 补充：两个句柄都是 `BaseAsyncResource`（能登记进资源管理器）。"""
    offsets = _OrderStore([])

    assert isinstance(OffsetFlushLoop(store=offsets), BaseAsyncResource)
    assert isinstance(OffsetFlusher(store=offsets), BaseAsyncResource)


# ---------------------------------------------------------------------------
# O26 - O29：位点重置
# ---------------------------------------------------------------------------


async def test_reset_moves_backward_and_forward_and_persists(tmp_path: Path) -> None:
    """O26：重置可倒退可前跳，且立即落盘（重启后可见）。"""
    store = _store(tmp_path, ceiling=_CeilingStub(20))
    await _commit(store, 10)

    assert await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=3) == 3
    reopened = _store(tmp_path)
    assert await reopened.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 3

    assert await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=15) == 15
    assert await _store(tmp_path).get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 15


async def test_reset_rejects_out_of_range(tmp_path: Path) -> None:
    """O27：重置越界（负 / `>LEO`）→ `10001` 且值不变。"""
    store = _store(tmp_path, ceiling=_CeilingStub(5))
    await _commit(store, 3)

    with pytest.raises(ParamError):
        await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=-1)
    with pytest.raises(ParamError):
        await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=6)
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 3


async def test_reset_fails_closed_when_audit_fails(tmp_path: Path) -> None:
    """O28：审计写入失败 → 重置被拒且值不变（没留痕就不该生效）。"""
    store = _store(tmp_path, audit=_AuditStub(error=RuntimeError("audit down")))
    await _commit(store, 7)

    with pytest.raises(OffsetAuditError):
        await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=2)
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 7

    rejected = _store(tmp_path, audit=_AuditStub(accepted=False))
    with pytest.raises(OffsetAuditError):
        await rejected.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)


async def test_reset_audit_payload(tmp_path: Path) -> None:
    """O29：审计事件带 action / path / 操作人 / 前后值。"""
    audit = _AuditStub()
    store = _store(tmp_path, audit=audit, ceiling=_CeilingStub(50))
    await _commit(store, 10)

    await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=4, actor_id=77, ip="10.0.0.1")

    assert len(audit.events) == 1
    event = audit.events[0]
    payload = cast("Any", event.payload)
    assert payload["action"] == OFFSET_RESET_ACTION
    assert payload["path"] == OFFSET_RESET_PATH
    assert payload["user_id"] == "77"
    assert payload["ip"] == "10.0.0.1"
    assert json.loads(payload["params"])["current"] == 4
    assert json.loads(payload["params"])["previous"] == 10


# ---------------------------------------------------------------------------
# O30 - O34：枚举、清理、租户未登记、内存替身
# ---------------------------------------------------------------------------


async def test_list_group_enumerates_offsets(tmp_path: Path) -> None:
    """O30：按组枚举全部位点，按 `(topic_id, partition)` 升序，值为当前位点。"""
    store = _store(tmp_path)
    await _commit(store, 1, partition_no=1)
    await _commit(store, 2, partition_no=0)
    await _commit(store, 3, group="notify")
    directory = _directory(tmp_path, topic_id=OTHER_TOPIC_ID)
    other = FileOffsetStore(directory=directory, root=tmp_path, audit=_AuditStub(), now_us=lambda: 1)
    await other.commit(group=GROUP, topic_id=OTHER_TOPIC_ID, partition_no=0, offset=5)
    await other.flush_once()

    entries = await store.list_group(tenant_id=TENANT_ID, group=GROUP)

    assert [(item.topic_id, item.partition_no, item.offset) for item in entries] == [
        (TOPIC_ID, 0, 2),
        (TOPIC_ID, 1, 1),
        (OTHER_TOPIC_ID, 0, 5),
    ]
    assert await store.list_group(tenant_id=TENANT_ID, group="missing") == []


async def test_list_group_skips_unavailable_files(tmp_path: Path) -> None:
    """O30 补充：不可用的位点文件不进入视图（跳过并告警，不打断整组枚举）。"""
    store = _store(tmp_path)
    await _commit(store, 5, partition_no=0)
    await _commit(store, 6, partition_no=1)
    path = _path(tmp_path, partition_no=1)
    raw = bytearray(path.read_bytes())
    header_len = OFFSET_HEADER_FIXED_BYTES + len(GROUP.encode())
    for index in range(2):
        raw[header_len + index * OFFSET_SLOT_BYTES] ^= 0xFF
    path.write_bytes(bytes(raw))

    entries = await _store(tmp_path).list_group(tenant_id=TENANT_ID, group=GROUP)

    assert [(item.partition_no, item.offset) for item in entries] == [(0, 5)]


async def test_purge_group_removes_files_and_cache(tmp_path: Path) -> None:
    """O31：删组清空文件与缓存，空目录一并删；不存在的组返回 0。"""
    store = _store(tmp_path)
    await _commit(store, 1, partition_no=0)
    await _commit(store, 2, partition_no=1)

    assert await store.purge_group(tenant_id=TENANT_ID, group=GROUP) == 2
    assert not _path(tmp_path).exists()
    assert not _path(tmp_path, partition_no=1).exists()
    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 0
    assert await store.purge_group(tenant_id=TENANT_ID, group=GROUP) == 0


async def test_purge_topic_removes_across_groups(tmp_path: Path) -> None:
    """O32：按主题跨组删除同名位点文件，其他主题不受影响。"""
    store = _store(tmp_path)
    await _commit(store, 1, group="audit", partition_no=0)
    await _commit(store, 2, group="notify", partition_no=1)
    directory = _directory(tmp_path, topic_id=OTHER_TOPIC_ID)
    other = FileOffsetStore(directory=directory, root=tmp_path, audit=_AuditStub(), now_us=lambda: 1)
    await other.commit(group="audit", topic_id=OTHER_TOPIC_ID, partition_no=0, offset=3)
    await other.flush_once()

    assert await store.purge_topic(topic_id=TOPIC_ID) == 2
    assert not _path(tmp_path, group="audit").exists()
    assert not _path(tmp_path, group="notify", partition_no=1).exists()
    assert _path(tmp_path, group="audit", topic_id=OTHER_TOPIC_ID).exists()


async def test_unresolved_tenant_reads_zero_and_commit_fails(tmp_path: Path) -> None:
    """O33：主题目录未登记 → `get` 返回 0、`commit` 抛 `PartitionNotBoundError`（写侧不静默丢）。"""
    store = _store(tmp_path, directory=PartitionDirectory(root=tmp_path))

    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 0
    with pytest.raises(PartitionNotBoundError):
        await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)
    with pytest.raises(PartitionNotBoundError):
        await store.reset(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)


async def test_in_memory_store_covers_write_side_protocols() -> None:
    """O34：内存替身同样满足三协议（端点用例可直接用内存库）。"""
    store = InMemoryOffsetStore()
    await store.commit(group="audit", topic_id=1, partition_no=0, offset=4)
    await store.commit(group="audit", topic_id=1, partition_no=1, offset=6)
    await store.commit(group="notify", topic_id=2, partition_no=0, offset=8)

    assert await store.reset(group="audit", topic_id=1, partition_no=0, offset=2) == 2
    with pytest.raises(ParamError):
        await store.reset(group="audit", topic_id=1, partition_no=0, offset=-3)
    assert [
        (item.topic_id, item.partition_no, item.offset) for item in await store.list_group(tenant_id=1, group="audit")
    ] == [
        (1, 0, 2),
        (1, 1, 6),
    ]
    assert await store.purge_group(tenant_id=1, group="audit") == 2
    assert await store.purge_topic(topic_id=2) == 1
    assert await store.purge_group(tenant_id=1, group="audit") == 0


# ---------------------------------------------------------------------------
# 编解码边界（覆盖率用）
# ---------------------------------------------------------------------------


def test_encode_rejects_oversized_group() -> None:
    """组名超过 128 字节 → 写前失败（写进去就读不出来了）。"""
    with pytest.raises(ValueError):
        encode_offset_file(
            group="g" * 129,
            topic_id=1,
            partition_no=0,
            slots=(OffsetSlot(seq=1, offset=1, updated_at_us=1),),
        )


def test_decode_rejects_short_and_foreign_bytes() -> None:
    """长度不足 / 魔数不符 / 组名长度非法 / 槽不完整 → 各自给对应结论。"""
    assert decode_offset_file(b"MO").reason == OFFSET_REASON_TRUNCATED
    assert decode_offset_file(b"XXXX" + b"\x00" * 40).reason == OFFSET_REASON_HEADER
    zero_group = bytearray(OFFSET_FILE_MAGIC + (1).to_bytes(2, "big") + (0).to_bytes(1, "big"))
    zero_group += (1).to_bytes(8, "big") + (0).to_bytes(4, "big")
    assert decode_offset_file(bytes(zero_group)).reason == OFFSET_REASON_HEADER
    header = OFFSET_FILE_MAGIC + (1).to_bytes(2, "big") + len(GROUP).to_bytes(1, "big") + GROUP.encode()
    header += (TOPIC_ID).to_bytes(8, "big") + (0).to_bytes(4, "big")
    assert decode_offset_file(header).reason == OFFSET_REASON_TRUNCATED


def test_parse_offset_filename_edges() -> None:
    """文件名解析：合法 / 后缀不符 / 无分隔 / 非数字一律给出确定结论。"""
    assert parse_offset_filename("41-0.offset") == (41, 0)
    assert parse_offset_filename("41-0.index") is None
    assert parse_offset_filename("410.offset") is None
    assert parse_offset_filename("a-0.offset") is None
    assert parse_offset_filename("41-b.offset") is None


async def test_absent_reason_constant_is_used() -> None:
    """`absent` 是「文件缺失」的语义标记（与损坏区分），由读取路径产出 info 而非不可用。"""
    assert OFFSET_REASON_ABSENT == "absent"
    assert OFFSET_REASON_ABSENT not in {OFFSET_REASON_CRC, OFFSET_REASON_HEADER, OFFSET_REASON_IDENTITY}


# ---------------------------------------------------------------------------
# 补充边界：构造校验、身份校验、扫描路径与单飞（覆盖率用）
# ---------------------------------------------------------------------------


def test_constructor_rejects_bad_tuning(tmp_path: Path) -> None:
    """刷盘窗口 / 脏阈值非正数 → 启动即失败（不静默降级）。"""
    with pytest.raises(ValueError):
        FileOffsetStore(directory=_directory(tmp_path), root=tmp_path, audit=_AuditStub(), flush_interval_ms=0)
    with pytest.raises(ValueError):
        FileOffsetStore(directory=_directory(tmp_path), root=tmp_path, audit=_AuditStub(), dirty_limit=0)


def test_decode_checks_group_topic_and_partition_identity() -> None:
    """三个身份字段各自不符都判 `identity`（挪错目录 / 改名都不会误读）。"""
    raw = encode_offset_file(
        group=GROUP,
        topic_id=TOPIC_ID,
        partition_no=0,
        slots=(
            OffsetSlot(seq=1, offset=5, updated_at_us=1),
            OffsetSlot(seq=0, offset=0, updated_at_us=1),
        ),
    )

    assert decode_offset_file(raw, group="other").reason == OFFSET_REASON_IDENTITY
    assert decode_offset_file(raw, topic_id=OTHER_TOPIC_ID).reason == OFFSET_REASON_IDENTITY
    assert decode_offset_file(raw, partition_no=1).reason == OFFSET_REASON_IDENTITY


def test_decode_rejects_oversized_group_length() -> None:
    """头里声明组名 129 字节 → 结构非法（`header`），不给越界读取的机会。"""
    raw = OFFSET_FILE_MAGIC + (1).to_bytes(2, "big") + (129).to_bytes(1, "big") + b"g" * 129
    raw += (TOPIC_ID).to_bytes(8, "big") + (0).to_bytes(4, "big") + b"\x00" * (2 * OFFSET_SLOT_BYTES)

    assert decode_offset_file(raw).reason == OFFSET_REASON_HEADER


async def test_default_clock_is_used(tmp_path: Path) -> None:
    """不注入 `now_us` 时走真实时钟（`updated_at_us` 有值）。"""
    store = FileOffsetStore(directory=_directory(tmp_path), root=tmp_path, audit=_AuditStub())
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=3)
    await store.flush_once()

    readout = decode_offset_file(_path(tmp_path).read_bytes())

    assert readout.persisted is not None
    assert readout.persisted.updated_at_us > 0


async def test_unbound_ceiling_warns_only_once(tmp_path: Path) -> None:
    """未绑 LEO 只告警一次（第二次提交不再刷同一条告警）。"""
    store = _store(tmp_path)

    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=2)

    assert await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0) == 2


async def test_realign_skips_when_no_offsets_root(tmp_path: Path) -> None:
    """主题已登记但还没有任何位点文件 → 空清单（不建目录）。"""
    store = _store(tmp_path)

    assert len(await store.realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=3)) == 0
    assert not (tmp_path / OFFSETS_SUBDIR).exists()


async def test_realign_skips_groups_without_that_partition(tmp_path: Path) -> None:
    """组目录存在但没有该分区的位点文件 → 跳过该组（不是所有组都订了同一分区）。"""
    store = _store(tmp_path)
    await _commit(store, 9, partition_no=1)

    assert len(await store.realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=3)) == 0


async def test_realign_skips_unavailable_files(tmp_path: Path) -> None:
    """位点文件不可用 → 跳过（不夹取、不报错），其余组照常对齐。"""
    store = _store(tmp_path)
    await _commit(store, 9, group="audit")
    await _commit(store, 9, group="notify")
    broken = _path(tmp_path, group="notify")
    raw = bytearray(broken.read_bytes())
    header_len = OFFSET_HEADER_FIXED_BYTES + len(GROUP.encode())
    for index in range(2):
        raw[header_len + index * OFFSET_SLOT_BYTES] ^= 0xFF
    broken.write_bytes(bytes(raw))

    adjusted = await _store(tmp_path).realign_partition(topic_id=TOPIC_ID, partition_no=0, ceiling=4)

    assert [(item.group, item.current) for item in adjusted] == [("audit", 4)]


async def test_list_group_skips_foreign_and_unreadable_files(tmp_path: Path) -> None:
    """组目录里的非法文件名与读不出身份的文件都跳过（不打断整组枚举）。"""
    store = _store(tmp_path)
    await _commit(store, 5)
    directory = _path(tmp_path).parent
    (directory / "notes.offset").write_bytes(b"whatever")
    (directory / f"{OTHER_TOPIC_ID}-0{OFFSET_FILE_SUFFIX}").write_bytes(b"")

    entries = await store.list_group(tenant_id=TENANT_ID, group=GROUP)

    assert [(item.topic_id, item.partition_no) for item in entries] == [(TOPIC_ID, 0)]


async def test_purge_group_skips_foreign_files(tmp_path: Path) -> None:
    """删组只删/只清缓存合法位点文件；非位点文件不受影响（目录因此不删）。"""
    store = _store(tmp_path)
    await _commit(store, 5)
    directory = _path(tmp_path).parent
    foreign = directory / "notes.offset"
    foreign.write_bytes(b"x")
    keep = directory / "keep.txt"
    keep.write_bytes(b"x")

    assert await store.purge_group(tenant_id=TENANT_ID, group=GROUP) == 2
    assert not foreign.exists()
    assert keep.exists()
    assert directory.is_dir()


async def test_purge_topic_without_tenant_or_root(tmp_path: Path) -> None:
    """主题未登记 / 位点根不存在 → 都返回 0（不报错）。"""
    unbound = _store(tmp_path, directory=PartitionDirectory(root=tmp_path))
    assert await unbound.purge_topic(topic_id=TOPIC_ID) == 0

    empty = _store(tmp_path)
    assert await empty.purge_topic(topic_id=TOPIC_ID) == 0


async def test_purge_topic_skips_foreign_files(tmp_path: Path) -> None:
    """按主题删只看 `{topic_id}-*` 的合法位点文件，别的文件不受影响。"""
    store = _store(tmp_path)
    await _commit(store, 5)
    directory = _path(tmp_path).parent
    foreign = directory / f"{TOPIC_ID}-x{OFFSET_FILE_SUFFIX}"
    foreign.write_bytes(b"x")

    assert await store.purge_topic(topic_id=TOPIC_ID) == 1
    assert foreign.exists()


async def test_flush_is_single_flight(tmp_path: Path) -> None:
    """刷盘进行中再次触发 → 直接返回 0（不并发写同一文件）。"""
    store = _store(tmp_path)
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=3)
    cast("Any", store)._flushing = True  # 构造「已在飞」这一瞬间

    try:
        assert await store.flush_once() == 0
    finally:
        cast("Any", store)._flushing = False
    assert await store.flush_once() == 1


async def test_entry_for_path_handles_unreadable_and_garbage(tmp_path: Path) -> None:
    """路径读不出（目录冒充文件）不让扫描路径崩；合法文件照常进视图。"""
    store = _store(tmp_path)
    await _commit(store, 5)
    directory = _path(tmp_path).parent
    (directory / f"{TOPIC_ID}-9{OFFSET_FILE_SUFFIX}").mkdir()

    entries = await store.list_group(tenant_id=TENANT_ID, group=GROUP)

    assert [(item.partition_no, item.offset) for item in entries] == [(0, 5)]


async def test_load_reports_read_failure_as_unavailable(tmp_path: Path) -> None:
    """读位点文件遇到非「不存在」的错误（如路径是目录）→ 标不可用并 fail-closed。"""
    store = _store(tmp_path)
    path = _path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.mkdir()

    with pytest.raises(OffsetUnavailableError):
        await store.get(group=GROUP, topic_id=TOPIC_ID, partition_no=0)


async def test_flush_reports_oversized_group_without_writing(tmp_path: Path) -> None:
    """组名超长（写进去就读不出来）→ 该条不落盘并告警，内存值仍可读。"""
    store = _store(tmp_path)
    await store.commit(group="g" * 129, topic_id=TOPIC_ID, partition_no=0, offset=1)

    assert await store.flush_once() == 0
    assert await store.get(group="g" * 129, topic_id=TOPIC_ID, partition_no=0) == 1


async def test_purge_tenant_removes_all_offsets(tmp_path: Path) -> None:
    """租户删除：删全部位点文件与缓存；重复调用返回 0。"""
    store = _store(tmp_path)
    await _commit(store, 5, group="audit")
    assert await store.purge_tenant(tenant_id=TENANT_ID) >= 1
    assert list(await store.list_group(tenant_id=TENANT_ID, group="audit")) == []
    assert await store.purge_tenant(tenant_id=TENANT_ID) == 0


async def test_in_memory_purge_tenant_is_noop() -> None:
    """内存替身没有租户维度，`purge_tenant` 恒返回 0。"""
    store = InMemoryOffsetStore()
    await store.commit(group=GROUP, topic_id=TOPIC_ID, partition_no=0, offset=1)
    assert await store.purge_tenant(tenant_id=TENANT_ID) == 0


async def test_purge_tenant_skips_foreign_files(tmp_path: Path) -> None:
    """按租户删只清合法位点文件；非位点文件保留（目录因此不删）。"""
    store = _store(tmp_path)
    await _commit(store, 5)
    foreign = _path(tmp_path).parent / "foreign.offset"
    foreign.write_text("x", encoding="utf-8")

    assert await store.purge_tenant(tenant_id=TENANT_ID) == 1
    assert foreign.exists()
