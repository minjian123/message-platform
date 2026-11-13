# pyright: reportPrivateUsage=false
"""`log_id` 分区持久化索引（02-8-4；Kiwi 2497）：重启 / 卸载后仍可定位，缺失可重建，碰撞回读校验。"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker import file_store as file_store_module
from bms_msg.broker.file_store import (
    LOG_INDEX_NAME,
    FileMessageStore,
    PartitionDirectory,
    append_log_index,
    iter_log_index,
    lookup_log_index_file,
)
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2497)

TOPIC_ID = 21
TENANT_ID = 9_002
NAMESPACE = "audit"
TOPIC_NAME = "audit.operation"
PARTITION = 0
BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _store(root: Path) -> FileMessageStore:
    """构造文件实现（新实例 = 模拟重启）。"""
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return FileMessageStore(directory=directory, fsync_mode="os", clock=lambda: 0.0)


async def _append(store: FileMessageStore, *, index: int) -> None:
    """追加一条消息。"""
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key="k",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "file"}),
        trace=TraceContext(trace_id="t-1"),
        published_at=BASE_AT,
        content_type="application/json",
        compression="none",
        uncompressed_len=7,
    )


def _partition_dir(root: Path) -> Path:
    """用例分区目录。"""
    return root / "data" / str(TENANT_ID) / NAMESPACE / f"{TOPIC_NAME}.{TOPIC_ID}" / str(PARTITION)


async def test_log_id_resolvable_from_new_store_instance(tmp_path: Path) -> None:
    """重启（新 store）后 LRU 为空，仍能通过分区索引定位冷记录。"""
    first = _store(tmp_path)
    for index in range(3):
        await _append(first, index=index)
    assert (await first.find_by_log_id(log_id="l-1")) is not None

    restarted = _store(tmp_path)
    view = await restarted.find_by_log_id(log_id="l-1")
    assert view is not None
    assert view.offset == 1
    assert await restarted.find_by_log_id(log_id="missing") is None


async def test_index_file_is_written_per_partition(tmp_path: Path) -> None:
    """每条写入追加一条索引，索引文件落在分区目录。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    await _append(store, index=1)

    index_path = _partition_dir(tmp_path) / LOG_INDEX_NAME
    assert index_path.is_file()
    assert lookup_log_index_file(index_path, log_id="l-0") == [0]
    assert lookup_log_index_file(index_path, log_id="l-1") == [1]


async def test_missing_index_is_rebuilt_from_segments(tmp_path: Path) -> None:
    """索引文件被删后，已加载分区按段头重建并仍可定位。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    await _append(store, index=1)
    index_path = _partition_dir(tmp_path) / LOG_INDEX_NAME
    index_path.unlink()

    # 先让分区进入已加载态（随后查找走“已加载分区 → 重建”路径）
    restarted = _store(tmp_path)
    assert await restarted.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    view = await restarted.find_by_log_id(log_id="l-1")
    assert view is not None
    assert view.offset == 1
    assert index_path.is_file()


async def test_hash_collision_is_verified_by_reading_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """哈希碰撞时按候选逐个回读比对，不返回错误记录。"""
    store = _store(tmp_path)

    def _constant_hash(_log_id: str) -> int:
        """恒定哈希（强制碰撞）。"""
        return 1

    monkeypatch.setattr(file_store_module, "log_id_hash", _constant_hash)
    await _append(store, index=0)
    await _append(store, index=1)

    restarted = _store(tmp_path)
    view = await restarted.find_by_log_id(log_id="l-0")
    assert view is not None
    assert view.offset == 0
    # 02-8-4 分支覆盖：第一个候选（offset 0）不匹配，继续回读下一个候选
    view = await restarted.find_by_log_id(log_id="l-1")
    assert view is not None
    assert view.offset == 1


async def test_disk_index_collision_skips_non_matching_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """未加载分区的磁盘索引扫描：哈希碰撞时跳过不匹配候选，命中正确 offset。"""
    store = _store(tmp_path)

    def _constant_hash(_log_id: str) -> int:
        """恒定哈希（强制碰撞）。"""
        return 1

    monkeypatch.setattr(file_store_module, "log_id_hash", _constant_hash)
    await _append(store, index=0)
    await _append(store, index=1)

    restarted = _store(tmp_path)
    view = await restarted.find_by_log_id(log_id="l-1")
    assert view is not None
    assert view.offset == 1


def test_index_append_and_read_tolerate_io_errors(tmp_path: Path) -> None:
    """索引追加失败只告警；读取目录路径按空索引处理。"""
    append_log_index(tmp_path / "missing" / "log_id.idx", log_id="l-0", offset=0)
    assert iter_log_index(tmp_path) == []


async def test_rebuild_tolerates_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """索引文件写失败只告警，内存索引仍可用于本次查找。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    (_partition_dir(tmp_path) / LOG_INDEX_NAME).unlink()
    restarted = _store(tmp_path)
    assert await restarted.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1

    def _fail_write(self: Path, data: bytes) -> int:
        """恒抛 OSError。"""
        del self, data
        raise OSError("disk full")

    monkeypatch.setattr(Path, "write_bytes", _fail_write)
    view = await restarted.find_by_log_id(log_id="l-0")
    assert view is not None
    assert view.offset == 0


async def test_rebuild_skips_empty_segments(tmp_path: Path) -> None:
    """段被整段清空后重建索引：空段（ends <= base）跳过，不产出条目。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    await store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION, offset=1)
    (_partition_dir(tmp_path) / LOG_INDEX_NAME).unlink()

    restarted = _store(tmp_path)
    assert await restarted.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 1
    assert await restarted.find_by_log_id(log_id="l-0") is None


async def test_collision_candidates_are_verified_on_loaded_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """已加载分区上的哈希碰撞：同哈希候选逐个回读比对。"""

    def _constant_hash(_log_id: str) -> int:
        """恒定哈希（强制碰撞）。"""
        return 1

    monkeypatch.setattr(file_store_module, "log_id_hash", _constant_hash)
    store = _store(tmp_path)
    await _append(store, index=0)
    await _append(store, index=1)
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2

    restarted = _store(tmp_path)
    assert await restarted.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    view = await restarted.find_by_log_id(log_id="l-0")
    assert view is not None
    assert view.offset == 0


async def test_disk_scan_skips_missing_dirs_and_absent_index(tmp_path: Path) -> None:
    """磁盘扫描跳过：未落盘主题目录 / 非分区目录 / 无索引的分区目录。"""
    store = _store(tmp_path)
    await _append(store, index=0)

    topic_dir = _partition_dir(tmp_path).parent
    (topic_dir / "README").write_text("x", encoding="utf-8")
    (topic_dir / "1").mkdir()

    restarted = _store(tmp_path)
    # 再绑定一个没有磁盘目录的主题（扫描时跳过）
    restarted._directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name="audit.other", topic_id=TOPIC_ID + 1)
    assert await restarted.find_by_log_id(log_id="missing") is None


async def test_disk_scan_respects_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """单次全局查找的分区扫描数有上限（超出记 warning 并停止）。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    monkeypatch.setattr(file_store_module, "LOG_ID_INDEX_SCAN_LIMIT", 0)

    restarted = _store(tmp_path)
    assert await restarted.find_by_log_id(log_id="missing") is None
