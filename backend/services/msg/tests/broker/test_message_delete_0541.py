"""删消息（墓碑标记）存储语义（05-4-1；Kiwi 2495）。

覆盖：删除后 `fetch` / `scan_headers` / `find_by_log_id` / `read_bytes` 不可见，
LEO 与位点不变；重复删除与不存在返回 False；重启后删除集合重建仍不可见；内存实现同口径。
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2495)

TOPIC_ID = 41
TENANT_ID = 9_041
NAMESPACE = "audit"
TOPIC_NAME = "audit.delete"
PARTITION = 0
BASE_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _store(root: Path) -> FileMessageStore:
    """构造文件实现（已绑定用例主题）。"""
    directory = PartitionDirectory(root=root)
    directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
    return FileMessageStore(directory=directory, fsync_mode="os", clock=lambda: 0.0)


async def _append(store: InMemoryMessageStore | FileMessageStore, *, index: int) -> None:
    """追加一条消息（`log_id = l-<index>`）。"""
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION,
        message_id=f"m-{index}",
        log_id=f"l-{index}",
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict[str, str](),
        trace=TraceContext(),
        published_at=BASE_AT,
        content_type="application/json",
        compression="none",
        uncompressed_len=1,
    )


async def test_file_store_delete_hides_record_and_keeps_leo(tmp_path: Path) -> None:
    """删除后读数不可见、LEO 不变；重复 / 不存在返回 False。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    await _append(store, index=1)

    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True
    # 墓碑占一个 offset（既有记录 offset 不变，LEO +1）
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3

    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.log_id for item in fetched] == ["l-1"]
    headers = await store.scan_headers(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.log_id for item in headers] == ["l-1"]
    assert await store.find_by_log_id(log_id="l-0") is None
    assert await store.find_by_log_id(log_id="l-1") is not None
    assert await store.read_bytes(topic_id=TOPIC_ID, partition_no=PARTITION, offset=0) is None

    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is False
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-404") is False


async def test_delete_survives_restart(tmp_path: Path) -> None:
    """重启（重新构造存储）后删除标记由段扫描重建，记录仍不可见。"""
    store = _store(tmp_path)
    await _append(store, index=0)
    await _append(store, index=1)
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True
    await store.sync(topic_id=TOPIC_ID, partition_no=PARTITION)

    reopened = _store(tmp_path)
    fetched = await reopened.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.log_id for item in fetched] == ["l-1"]
    assert await reopened.find_by_log_id(log_id="l-0") is None
    assert await reopened.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 3


async def test_in_memory_store_delete_matches_contract() -> None:
    """内存实现与文件实现同口径：删除后不可见、LEO 不变、重复删除 False。"""
    store = InMemoryMessageStore()
    await _append(store, index=0)
    await _append(store, index=1)

    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True
    fetched = await store.fetch(topic_id=TOPIC_ID, partition_no=PARTITION, from_offset=0, max_messages=10)
    assert [item.log_id for item in fetched] == ["l-1"]
    assert await store.find_by_log_id(log_id="l-0") is None
    # 内存实现不追加墓碑记录，LEO 不变（端口契约只保证既有 offset 不变）
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION) == 2
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is False


async def test_delete_missing_and_repeat(tmp_path: Path) -> None:
    """不存在 / 跨分区 / 重复删除均返回 False（含墓碑后再删）。"""
    store = _store(tmp_path)
    await _append(store, index=0)

    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-404") is False
    assert await store.delete(topic_id=TOPIC_ID, partition_no=1, log_id="l-0") is False
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True
    # 目标已被墓碑命中：解析仍命中坐标，但读路径返回 None，删除返回 False
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is False


async def test_delete_delegates_through_wrappers() -> None:
    """`NotifyingMessageStore` / `ReplicatedMessageStore` 的删除原样委托。"""
    from types import SimpleNamespace
    from typing import cast

    from bms_msg.broker.replication import ReplicatedMessageStore
    from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore

    inner = InMemoryMessageStore()
    await _append(inner, index=0)

    notifying = NotifyingMessageStore(inner, DeliveryWaiter())
    assert await notifying.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True

    replicated = ReplicatedMessageStore(
        InMemoryMessageStore(),
        replication=cast("object", SimpleNamespace()),  # type: ignore[arg-type]
    )
    await _append(cast("InMemoryMessageStore", replicated.inner), index=0)
    assert await replicated.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-0") is True


async def test_delete_on_unbound_topic_returns_false(tmp_path: Path) -> None:
    """主题目录未登记：删除返回 False（不建目录、不抛错）。"""
    store = FileMessageStore(directory=PartitionDirectory(root=tmp_path), fsync_mode="os")
    assert await store.delete(topic_id=999, partition_no=0, log_id="l-0") is False


async def test_in_memory_delete_missing_returns_false() -> None:
    """内存实现：不存在的记录删除返回 False。"""
    store = InMemoryMessageStore()
    assert await store.delete(topic_id=TOPIC_ID, partition_no=PARTITION, log_id="l-404") is False
