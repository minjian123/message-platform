"""分区复制拓扑加载（msg 任务 02-4；Kiwi 2484）：`msg_partition` → 副本集 / min / epoch。

用替身仓储覆盖缓存命中、TTL 过期、行缺失与显式失效；不连数据库。
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.broker.replay import UowFactory
from bms_msg.services import replication_topology
from bms_msg.services.replication_topology import DbReplicaTopologyLoader

pytestmark = pytest.mark.kiwi_id(2484)

TOPIC_ID = 7001
PARTITION_NO = 0


class _Clock:
    """可推进的单调毫秒时钟。"""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class _Row:
    """分区行替身。"""

    def __init__(
        self,
        *,
        partition_no: int,
        replicas: ConcurrentStableList[str],
        min_insync: int,
        epoch: int,
        leader: str = "",
        switch_state: str = "idle",
    ) -> None:
        self.partition_no = partition_no
        self.replicas = ConcurrentStableList(replicas)
        self.min_insync_replicas = min_insync
        self.epoch = epoch
        self.leader = leader
        self.switch_state = switch_state


class _Uow(AbstractAsyncContextManager[object]):
    """工作单元替身（会话不被使用：仓储已被替换）。"""

    session = object()

    async def __aenter__(self) -> object:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class _Repo:
    """仓储替身（记录调用次数）。"""

    calls = 0
    rows: ConcurrentStableList[_Row] = ConcurrentStableList()

    def __init__(self, session: object) -> None:
        del session

    async def list_by_topic(self, topic_id: int) -> ConcurrentStableList[_Row]:
        del topic_id
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


@pytest.fixture
def _patched_repo(monkeypatch: pytest.MonkeyPatch) -> type[_Repo]:
    """替换模块内仓储为替身并重置计数。

    Args:
        monkeypatch: pytest 补丁器。

    Returns:
        type[_Repo]: 替身仓储类。
    """
    monkeypatch.setattr(replication_topology, "PartitionRepository", _Repo)
    _Repo.calls = 0
    _Repo.rows = ConcurrentStableList(
        (
            _Row(
                partition_no=0,
                replicas=ConcurrentStableList(("n2", "n1")),
                min_insync=2,
                epoch=3,
                leader="n2",
                switch_state="switching",
            ),
        )
    )
    return _Repo


async def test_load_returns_topology_and_caches(_patched_repo: type[_Repo]) -> None:
    """首次查询后按主题缓存；TTL 内的第二次命中不查库。"""
    clock = _Clock()
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=1_000, clock=clock)
    topology = await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert topology is not None
    assert list(topology.replicas) == ["n2", "n1"]
    assert topology.min_insync_replicas == 2
    assert topology.epoch == 3
    assert topology.leader == "n2"
    assert topology.switch_state == "switching"
    assert _patched_repo.calls == 1
    await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert _patched_repo.calls == 1


async def test_ttl_expiry_and_forget_reload(_patched_repo: type[_Repo]) -> None:
    """TTL 过期、`force=True` 与显式失效都会重新查库。"""
    clock = _Clock()
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=100, clock=clock)
    await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    clock.value = 1_000
    await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert _patched_repo.calls == 2
    await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO, force=True)
    assert _patched_repo.calls == 3
    loader.forget(topic_id=TOPIC_ID)
    await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert _patched_repo.calls == 4


async def test_missing_partition_returns_none(_patched_repo: type[_Repo]) -> None:
    """行不存在返回 None（协调器走配置兜底）。"""
    _patched_repo.rows = ConcurrentStableList()
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=1_000, clock=_Clock())
    assert await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO) is None


async def test_other_partition_no_returns_none(_patched_repo: type[_Repo]) -> None:
    """主题有分区行但目标分区号不在其中：返回 None（跳过不匹配的行）。"""
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=1_000, clock=_Clock())
    assert await loader.load(topic_id=TOPIC_ID, partition_no=9) is None
    assert _patched_repo.calls == 1


async def test_default_clock_is_used(_patched_repo: type[_Repo]) -> None:
    """不注入时钟时用单调毫秒缺省实现（缓存仍生效）。"""
    loader = DbReplicaTopologyLoader(uow_factory=_uow_factory(), ttl_ms=1_000)
    assert await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO) is not None
    assert await loader.load(topic_id=TOPIC_ID, partition_no=PARTITION_NO) is not None
    assert _patched_repo.calls == 1
