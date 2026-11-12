"""分区分配枚举源（msg 任务 02-10；Kiwi 2489）。

覆盖 `DbReplicaTopologyLoader.list_assignments()`：仓储行 → 分配事实（只读、不走缓存）。
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.replay import UowFactory
from bms_msg.services import replication_topology
from bms_msg.services.replication_topology import DbReplicaTopologyLoader, PartitionDirectoryBinder

pytestmark = pytest.mark.kiwi_id(2489)

TOPIC_ID = 7001


class _Row:
    """分区行替身（只带枚举需要的三列）。"""

    def __init__(self, *, topic_id: int, partition_no: int, replicas: ConcurrentStableList[str]) -> None:
        self.topic_id = topic_id
        self.partition_no = partition_no
        self.replicas = ConcurrentStableList(replicas)


class _Uow(AbstractAsyncContextManager[object]):
    """工作单元替身（会话不被使用：仓储已被替换）。"""

    session = object()

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _Topic:
    """主题行替身（只带身份事实）。"""

    def __init__(self, *, topic_id: int, tenant_id: int, namespace: str, name: str) -> None:
        self.id = topic_id
        self.tenant_id = tenant_id
        self.namespace = namespace
        self.name = name


class _TopicRepo:
    """主题仓储替身：只实现活跃主题枚举。"""

    rows: ConcurrentStableList[_Topic] = ConcurrentStableList()

    def __init__(self, session: object) -> None:
        del session

    async def list_active_unscoped(self) -> ConcurrentStableList[_Topic]:
        """返回固定活跃主题。

        Returns:
            ConcurrentStableList[_Topic]: 活跃主题。
        """
        return ConcurrentStableList(type(self).rows)


class _Repo:
    """仓储替身：只实现 `list_assignments`。"""

    rows: ConcurrentStableList[_Row] = ConcurrentStableList()
    calls = 0

    def __init__(self, session: object) -> None:
        del session

    async def list_assignments(self) -> ConcurrentStableList[_Row]:
        """返回固定行。

        Returns:
            ConcurrentStableList[_Row]: 分区行。
        """
        type(self).calls += 1
        return ConcurrentStableList(type(self).rows)


def _uow_factory() -> UowFactory:
    """构造工作单元工厂替身。

    Returns:
        UowFactory: 零参工厂。
    """

    def _factory() -> _Uow:
        return _Uow()

    return cast("UowFactory", _factory)


async def test_partition_directory_binder_delegates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """目录登记适配：登记后目录表能按 `topic_id` 反查到三段名（挂载前补登记的实际效果）。"""
    monkeypatch.setattr(replication_topology, "PartitionRepository", _Repo)
    _Repo.rows = ConcurrentStableList()
    directory = PartitionDirectory(root=tmp_path)
    binder = PartitionDirectoryBinder(directory)
    binder.bind(tenant_id=7001, namespace="msg", name="drill.replica", topic_id=TOPIC_ID)
    located = directory.locate(TOPIC_ID)
    assert located is not None
    assert (located.tenant_id, located.namespace, located.name) == (7001, "msg", "drill.replica")


async def test_list_assignments_maps_rows_to_assignments(monkeypatch: pytest.MonkeyPatch) -> None:
    """枚举：行被映射为分配事实（带主题身份），非活跃主题的分区被过滤，且直接读库。"""
    monkeypatch.setattr(replication_topology, "PartitionRepository", _Repo)
    monkeypatch.setattr(replication_topology, "TopicRepository", _TopicRepo)
    _Repo.calls = 0
    _Repo.rows = ConcurrentStableList(
        (
            _Row(topic_id=TOPIC_ID, partition_no=0, replicas=ConcurrentStableList(("n1", "n2"))),
            _Row(topic_id=TOPIC_ID, partition_no=1, replicas=ConcurrentStableList()),
            _Row(topic_id=TOPIC_ID + 1, partition_no=0, replicas=ConcurrentStableList(("n1",))),
        )
    )
    _TopicRepo.rows = ConcurrentStableList(
        (_Topic(topic_id=TOPIC_ID, tenant_id=7001, namespace="msg", name="drill.replica"),)
    )
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=5_000)
    assignments = await loader.list_assignments()
    assert [(item.topic_id, item.partition_no) for item in assignments] == [(TOPIC_ID, 0), (TOPIC_ID, 1)]
    assert list(assignments[0].replicas) == ["n1", "n2"]
    assert list(assignments[1].replicas) == []
    assert (assignments[0].tenant_id, assignments[0].namespace, assignments[0].name) == (7001, "msg", "drill.replica")
    assert _Repo.calls == 1
    # 不进拓扑缓存：同一方法再调一次仍会读库。
    await loader.list_assignments()
    assert _Repo.calls == 2
