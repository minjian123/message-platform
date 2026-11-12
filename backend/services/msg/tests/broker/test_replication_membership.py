# pyright: reportPrivateUsage=false
"""副本集变更收敛（msg 任务 02-8-3；Kiwi 2490）。

覆盖详细设计 §8 的 G1 ~ G8：

- G1 缩容：leader 的 `followers` / ISR 剔除被移出成员，指标行同步；被移出节点卸载；
- G2 扩容：新成员按日志起点建进度且 `in_sync=False`，暂不进 ISR；
- G3 本节点被移出：卸载后退出挂载表与探活集合、已挂载数减一、不拉取；
- G4 幂等与中间态：同一副本集重复刷新无动作；先扩后缩各收敛一次；
- G5 脏 leader：leader 不在新集合时保留现 leader 并告警，整轮不收敛；
- G6 交集成员保进度：成员新增时原位点与新鲜度不重置；
- G7 HW 重算：剔除落后成员后 HW 推进并唤醒等待者；
- G8 单实例零变化：复制关闭时刷新不触达收敛路径。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableSet

from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.replication import (
    FollowerProgress,
    PartitionTopology,
    ReplicaConfig,
    ReplicaRole,
    ReplicationCoordinator,
    ReplicationMetrics,
    ReplicaTopologyLoader,
)
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.trace import EMPTY_TRACE

pytestmark = pytest.mark.kiwi_id(2490)

TOPIC_ID = 9201
PARTITION_NO = 0
NODES3 = ("n1", "n2", "n3")
NODES2 = ("n1", "n2")
KEY = (TOPIC_ID, PARTITION_NO)


def _config(
    *,
    node_id: str = "n1",
    nodes: Iterable[str] = NODES3,
    enabled: bool = True,
    min_insync: int = 2,
) -> ReplicaConfig:
    """构造复制配置。

    Args:
        node_id: 本节点标识。
        nodes: 集群节点。
        enabled: 是否开启复制。
        min_insync: 最小同步副本数（全局）。

    Returns:
        ReplicaConfig: 配置。
    """
    members = ConcurrentStableFrozenList(nodes)
    return ReplicaConfig(
        enabled=enabled,
        node_id=node_id,
        nodes=members,
        replication_factor=len(members),
        min_insync_replicas=min_insync,
        lag_time_max_ms=10_000,
        lag_records_max=10_000,
        fetch_interval_ms=200,
        fetch_batch=100,
        commit_timeout_ms=100,
        scan_interval_ms=1_000,
        topology_ttl_ms=1_000,
        mount_sweep_ms=30_000,
        mount_batch=64,
        idle_fetch_interval_ms=1_000,
        pull_concurrency=8,
    )


class _MutableTopology(ReplicaTopologyLoader):
    """可变拓扑加载器：用例中途改副本集，模拟分配落库后的读回。"""

    def __init__(
        self,
        replicas: Iterable[str],
        *,
        leader: str = "",
        min_insync: int = 2,
        epoch: int = 0,
    ) -> None:
        """初始化。

        Args:
            replicas: 初始副本集。
            leader: 显式 leader（空表示按副本集首个推导）。
            min_insync: 分区级最小同步副本数（0 表示继承全局）。
            epoch: leader 任期。
        """
        self.replicas = ConcurrentStableFrozenList(replicas)
        self.leader = leader
        self.min_insync = min_insync
        self.epoch = epoch

    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
        """返回当前副本集。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过缓存（本替身忽略）。

        Returns:
            PartitionTopology: 当前拓扑。
        """
        del topic_id, partition_no, force
        return PartitionTopology(
            replicas=self.replicas,
            min_insync_replicas=self.min_insync,
            leader=self.leader,
            epoch=self.epoch,
        )

    def set_replicas(self, replicas: Iterable[str], *, leader: str = "") -> None:
        """改副本集（模拟迁移写库）。

        Args:
            replicas: 新副本集。
            leader: 显式 leader（空表示按副本集首个推导）。
        """
        self.replicas = ConcurrentStableFrozenList(replicas)
        self.leader = leader


def _coordinator(
    topology: _MutableTopology,
    *,
    node_id: str = "n1",
    nodes: Iterable[str] = NODES3,
    enabled: bool = True,
    waiter: DeliveryWaiter | None = None,
    metrics: ReplicationMetrics | None = None,
) -> ReplicationCoordinator:
    """构造协调器（内存存储 + 可变拓扑）。

    Args:
        topology: 拓扑加载器。
        node_id: 本节点标识。
        nodes: 集群节点。
        enabled: 是否开启复制。
        waiter: 唤醒器（缺省不装配）。
        metrics: 指标采集点（缺省新建）。

    Returns:
        ReplicationCoordinator: 协调器。
    """
    return ReplicationCoordinator(
        config=_config(node_id=node_id, nodes=nodes, enabled=enabled),
        store=InMemoryMessageStore(),
        topology_loader=topology,
        waiter=waiter,
        metrics=metrics,
        clock=lambda: 1_000.0,
    )


async def test_shrink_prunes_followers_and_isr() -> None:
    """G1 缩容：leader 的 followers / ISR 同步剔除被移出成员，指标行随之收缩。"""
    topology = _MutableTopology(NODES3)
    leader = _coordinator(topology)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert list(state.replicas) == list(NODES3)
    assert list(state.followers.keys()) == ["n2", "n3"]
    assert list(state.isr) == list(NODES3)

    topology.set_replicas(NODES2)
    await leader.refresh_once()

    assert list(state.replicas) == list(NODES2)
    assert list(state.followers.keys()) == ["n2"]
    assert list(state.isr) == list(NODES2)
    rows = leader._metrics.snapshot().rows
    assert [(row.topic_id, row.partition_no, row.isr_size) for row in rows] == [(TOPIC_ID, PARTITION_NO, 2)]


async def test_expand_registers_new_member_out_of_isr() -> None:
    """G2 扩容：新成员按日志起点建进度且未同步，暂不进 ISR。"""
    topology = _MutableTopology(NODES2)
    leader = _coordinator(topology, nodes=NODES2)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert list(state.isr) == list(NODES2)

    topology.set_replicas(NODES3)
    await leader.refresh_once()

    assert list(state.replicas) == list(NODES3)
    assert list(state.followers.keys()) == ["n2", "n3"]
    assert state.followers["n3"].in_sync is False
    assert state.followers["n3"].leo == 0
    assert "n3" not in state.isr
    assert list(state.isr) == list(NODES2)


async def test_detach_when_node_removed_from_replicas() -> None:
    """G3 本节点被移出：卸载并退出挂载表、已挂载数减一、指标行删除、不再拉取。"""
    topology = _MutableTopology(NODES2)
    follower = _coordinator(topology, node_id="n2", nodes=NODES2)
    state = await follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.hosted is True
    assert list(follower.mounted_partition_keys()) == [KEY]
    assert follower._metrics.mounted_partitions == 1

    topology.set_replicas(("n1",))
    await follower.refresh_once()

    assert state.hosted is False
    assert list(follower.mounted_partition_keys()) == []
    assert follower._metrics.mounted_partitions == 0
    assert list(follower._metrics.snapshot().rows) == []
    assert await follower.pull_round() == 0


async def test_follower_side_convergence_updates_replicas() -> None:
    """G2 follower 视角：成员新增时本节点仍承载，只更新集合与进度，不做 leader 侧 HW 重算。"""
    topology = _MutableTopology(NODES2)
    follower = _coordinator(topology, node_id="n2", nodes=NODES2)
    state = await follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.role is ReplicaRole.FOLLOWER

    topology.set_replicas(NODES3)
    await follower.refresh_once()

    assert list(state.replicas) == list(NODES3)
    assert "n3" in state.followers
    assert state.hosted is True
    assert list(follower.mounted_partition_keys()) == [KEY]


def test_unrecord_mount_never_underflows() -> None:
    """卸载计数自减有下限：未挂载过也不会变负（防指标出现负值）。"""
    metrics = ReplicationMetrics()
    metrics.unrecord_mount()
    assert metrics.mounted_partitions == 0
    metrics.record_mount()
    metrics.unrecord_mount()
    assert metrics.mounted_partitions == 0


async def test_detach_is_idempotent_and_unhosted_state_keeps_count() -> None:
    """G3 卸载幂等：重复卸载返回 False；非承载态卸载不减已挂载数。"""
    topology = _MutableTopology(NODES2)
    follower = _coordinator(topology, node_id="n2", nodes=NODES2)
    await follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert await follower.detach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO, reason="test") is True
    assert await follower.detach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO, reason="test") is False

    # 非承载态（本节点不在副本集内）挂载不计数，卸载也不减计数。
    other = _coordinator(_MutableTopology(NODES2), node_id="n9", nodes=NODES2)
    state = await other.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.hosted is False
    assert other._metrics.mounted_partitions == 0
    other._update_metrics(state)
    assert list(other._metrics.snapshot().rows) == []
    assert await other.detach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO, reason="test") is True
    assert other._metrics.mounted_partitions == 0


async def test_membership_convergence_is_idempotent() -> None:
    """G4 幂等：同一副本集重复刷新不改动进度对象与 ISR。"""
    topology = _MutableTopology(NODES3)
    leader = _coordinator(topology)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    progress = state.followers["n2"]
    isr = list(state.isr)

    await leader.refresh_once()
    await leader.refresh_once()

    assert state.followers["n2"] is progress
    assert list(state.isr) == isr


async def test_expand_then_shrink_converges_twice() -> None:
    """G4 中间态：先扩后缩各收敛一次，中间态不误卸。"""
    topology = _MutableTopology(NODES2)
    leader = _coordinator(topology, nodes=NODES2)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)

    topology.set_replicas(NODES3)
    await leader.refresh_once()
    assert list(state.replicas) == list(NODES3)
    assert list(state.isr) == list(NODES2)

    topology.set_replicas(NODES2)
    await leader.refresh_once()
    assert list(state.replicas) == list(NODES2)
    assert list(state.followers.keys()) == ["n2"]
    assert list(state.isr) == list(NODES2)


async def test_leader_outside_replicas_is_ignored_with_warning() -> None:
    """G5 脏 leader：leader 不在新集合时保留现状、整轮不收敛（不改主、不换集合）。"""
    topology = _MutableTopology(NODES3)
    leader = _coordinator(topology)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)

    topology.set_replicas(NODES2, leader="n9")
    await leader.refresh_once()

    assert state.leader == "n1"
    assert state.role is ReplicaRole.LEADER
    assert list(state.replicas) == list(NODES3)
    assert list(state.followers.keys()) == ["n2", "n3"]


async def test_intersection_member_keeps_progress() -> None:
    """G6 交集成员：新增成员不重置原成员位点与新鲜度。"""
    topology = _MutableTopology(NODES2)
    leader = _coordinator(topology, nodes=NODES2)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    progress = FollowerProgress(leo=7, last_fetch_ms=123.0, in_sync=True)
    state.followers.set("n2", progress)

    topology.set_replicas(NODES3)
    await leader.refresh_once()

    assert state.followers["n2"] is progress
    assert state.followers["n2"].leo == 7


async def test_shrink_advances_hw_and_notifies_waiter() -> None:
    """G7 HW 重算：剔除落后成员后 HW 推进并唤醒长轮询。"""
    topology = _MutableTopology(NODES3)
    waiter = DeliveryWaiter()
    leader = _coordinator(topology, waiter=waiter)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    state.leader_leo = 2
    state.local_leo = 2
    state.followers.set("n2", FollowerProgress(leo=2, last_fetch_ms=0.0, in_sync=True))
    state.followers.set("n3", FollowerProgress(leo=1, last_fetch_ms=0.0, in_sync=True))
    state.isr = ConcurrentStableSet(NODES3)
    assert state.hw == 0

    topology.set_replicas(NODES2)
    await leader.refresh_once()

    assert state.hw == 2
    assert waiter.version(KEY) > 0


async def test_empty_replicas_keeps_runtime_membership() -> None:
    """历史行无副本集：不据此收敛（运行态保持，交给轮转推导兜底）。"""
    topology = _MutableTopology(NODES3)
    leader = _coordinator(topology)
    state = await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)

    topology.set_replicas(())
    await leader.refresh_once()

    assert list(state.replicas) == list(NODES3)
    assert list(state.isr) == list(NODES3)


async def test_disabled_replication_never_converges() -> None:
    """G8 单实例零变化：复制关闭时刷新不进入收敛路径。"""
    topology = _MutableTopology(NODES3)
    solo = _coordinator(topology, enabled=False)
    state = await solo.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.role is ReplicaRole.DISABLED

    topology.set_replicas(NODES2)
    await solo.refresh_once()

    assert list(state.replicas) == ["n1"]
    assert state.hosted is True
    assert solo._metrics.mounted_partitions == 1


async def test_unload_keeps_local_data() -> None:
    """G3 数据处置：卸载只动运行态与指标，本地已落盘记录与 LEO 不变（不删数据）。"""
    store = InMemoryMessageStore()
    await store.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        message_id="m-1",
        log_id="l-1",
        key=None,
        payload=b"p-1",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )
    topology = _MutableTopology(NODES2)
    follower = ReplicationCoordinator(
        config=_config(node_id="n2", nodes=NODES2),
        store=store,
        topology_loader=topology,
        metrics=ReplicationMetrics(),
        clock=lambda: 1_000.0,
    )
    await follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 1

    topology.set_replicas(("n1",))
    await follower.refresh_once()

    assert follower._partitions.get(KEY) is None
    assert await store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 1
    assert await store.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
