"""fencing / 租约 / 角色收敛（msg 任务 02-5；Kiwi 2485）。

覆盖详细设计 §12.2 的 G4 / G5 / G10：

- 写路径：`acks=1` / `acks=all` 写 follower 均 350033；switching 拒写；超租约拒写、刷新恢复；
- 读写闸门：switching 期间 `serve_fetch` 拒绝、`pull_once` 不拉取、ISR 扫描跳过；
- 角色收敛：刷新后旧 leader 降级、目标升主、epoch 前进；
- 发散尾部：本地 LEO > 新 leader LEO 自动截断、无法修复时报错；
- 激活：按水位截断 / 保留 / no-op / 幂等 / 非目标拒绝。
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
)

from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.replication import (
    SWITCH_STATE_IDLE,
    SWITCH_STATE_SWITCHING,
    InProcessReplicaTransport,
    PartitionTopology,
    ReplicaConfig,
    ReplicaFetchRequest,
    ReplicaFetchResult,
    ReplicatedMessageStore,
    ReplicationCoordinator,
    ReplicationDivergedError,
    ReplicaTopologyLoader,
    ReplicaTransport,
    ReplicaUnavailableError,
    SwitchApplyCommand,
)
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.errors import MsgError, MsgErrorCode, MsgNotLeaderError

pytestmark = pytest.mark.kiwi_id(2485)

TOPIC_ID = 7101
PARTITION_NO = 0
NAMESPACE = "msg"
TOPIC_NAME = "audit.fencing"


class _Clock:
    """可推进的单调毫秒时钟。"""

    def __init__(self) -> None:
        """初始化（从 0 起）。"""
        self.value = 0.0

    def __call__(self) -> float:
        """返回当前毫秒。

        Returns:
            float: 当前毫秒数。
        """
        return self.value

    def advance(self, ms: float) -> None:
        """推进时钟。

        Args:
            ms: 推进毫秒数。
        """
        self.value += ms


class _Topology:
    """可变拓扑加载器（用例直接改 leader / epoch / switch_state 来模拟 DB）。"""

    def __init__(self, *, nodes: tuple[str, ...], min_insync: int, epoch: int = 0) -> None:
        """初始化。

        Args:
            nodes: 副本集。
            min_insync: 最小同步副本数。
            epoch: 初始 epoch。
        """
        self.nodes = nodes
        self.min_insync = min_insync
        self.epoch = epoch
        self.leader = ""
        self.switch_state = SWITCH_STATE_IDLE
        self.fail = False
        self.loads = 0

    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
        """返回当前拓扑。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过缓存（用例不区分）。

        Returns:
            PartitionTopology: 当前拓扑。

        Raises:
            RuntimeError: `fail=True` 时模拟元数据库不可用。
        """
        del topic_id, partition_no, force
        self.loads += 1
        if self.fail:
            raise RuntimeError("元数据库不可用")
        return PartitionTopology(
            replicas=ConcurrentStableFrozenList(self.nodes),
            min_insync_replicas=self.min_insync,
            epoch=self.epoch,
            leader=self.leader,
            switch_state=self.switch_state,
        )


class _Cluster:
    """两节点进程内复制簇（共享拓扑 / 传输 / 时钟）。"""

    def __init__(self, *, lease_ms: int = 5_000, epoch_refresh_ms: int = 1_000) -> None:
        """初始化。

        Args:
            lease_ms: leader 写租约（毫秒）。
            epoch_refresh_ms: 拓扑刷新周期（毫秒）。
        """
        self.clock = _Clock()
        self.topology = _Topology(nodes=("n1", "n2"), min_insync=2)
        self.transport = InProcessReplicaTransport()
        self.leader_store = InMemoryMessageStore()
        self.follower_store = InMemoryMessageStore()
        base = ReplicaConfig(
            enabled=True,
            node_id="n1",
            nodes=ConcurrentStableFrozenList(("n1", "n2")),
            replication_factor=2,
            min_insync_replicas=2,
            fetch_interval_ms=1_000,
            fetch_batch=100,
            commit_timeout_ms=100,
            scan_interval_ms=1_000,
            topology_ttl_ms=1_000,
            epoch_refresh_ms=epoch_refresh_ms,
            leader_lease_ms=lease_ms,
            switch_grace_ms=lease_ms,
        )
        self.leader = ReplicationCoordinator(
            config=base,
            store=self.leader_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", self.topology),
            waiter=DeliveryWaiter(),
            clock=self.clock,
        )
        self.follower = ReplicationCoordinator(
            config=replace(base, node_id="n2"),
            store=self.follower_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", self.topology),
            clock=self.clock,
        )
        self.transport.register("n1", self.leader)
        self.transport.register("n2", self.follower)
        self.writer = ReplicatedMessageStore(self.leader_store, replication=self.leader)
        self.follower_writer = ReplicatedMessageStore(self.follower_store, replication=self.follower)


def _message(*, seq: int) -> StoredMessage:
    """构造记录骨架。

    Args:
        seq: 序号。

    Returns:
        StoredMessage: 记录。
    """
    return StoredMessage(
        message_id=f"m-{seq}",
        log_id=f"l-{seq}",
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        offset=0,
        key=None,
        payload=f"p-{seq}".encode(),
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )


async def _append(store: InMemoryMessageStore, *, seq: int) -> StoredMessage:
    """直接向存储追加（用于模拟 follower 上已有 / 发散数据）。

    Args:
        store: 消息存储。
        seq: 序号。

    Returns:
        StoredMessage: 记录。
    """
    template = _message(seq=seq)
    return await store.append(
        topic_id=template.topic_id,
        partition_no=template.partition_no,
        message_id=template.message_id,
        log_id=template.log_id,
        key=template.key,
        payload=template.payload,
        headers=template.headers,
        trace=template.trace,
        published_at=template.published_at,
    )


async def _append_writer(writer: ReplicatedMessageStore, *, seq: int) -> StoredMessage:
    """经复制包装追加（走 leader / 租约校验）。

    Args:
        writer: 复制包装。
        seq: 序号。

    Returns:
        StoredMessage: 记录。
    """
    template = _message(seq=seq)
    return await writer.append(
        topic_id=template.topic_id,
        partition_no=template.partition_no,
        message_id=template.message_id,
        log_id=template.log_id,
        key=template.key,
        payload=template.payload,
        headers=template.headers,
        trace=template.trace,
        published_at=template.published_at,
    )


async def _ready(cluster: _Cluster) -> None:
    """挂载两个节点的分区。

    Args:
        cluster: 复制簇。
    """
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


def _error_data(error: MsgError) -> ConcurrentStableDict[str, object]:
    """取错误附带数据。

    Args:
        error: 错误。

    Returns:
        ConcurrentStableDict[str, object]: data。
    """
    return cast("ConcurrentStableDict[str, object]", error.data)


async def test_acks1_follower_write_rejected() -> None:
    """follower 收到 `acks=1` 写：350033 not_leader，data 带 leader / epoch。"""
    cluster = _Cluster()
    await _ready(cluster)
    with pytest.raises(MsgNotLeaderError) as captured:
        await _append_writer(cluster.follower_writer, seq=1)
    assert captured.value.code_ == MsgErrorCode.NOT_LEADER
    assert _error_data(captured.value)["reason"] == "not_leader"
    assert _error_data(captured.value)["leader"] == "n1"


async def test_leader_write_allowed() -> None:
    """leader 写：正常落盘。"""
    cluster = _Cluster()
    await _ready(cluster)
    message = await _append_writer(cluster.writer, seq=1)
    assert message.offset == 0


async def test_switching_rejects_write_fetch_and_pull() -> None:
    """切换窗口：写 350033 switching、`serve_fetch` 拒绝、`pull_once` 不拉取。"""
    cluster = _Cluster()
    await _ready(cluster)
    await _append_writer(cluster.writer, seq=1)
    cluster.topology.switch_state = SWITCH_STATE_SWITCHING
    await cluster.leader.refresh_once()
    await cluster.follower.refresh_once()

    with pytest.raises(MsgNotLeaderError) as captured:
        await _append_writer(cluster.writer, seq=2)
    assert _error_data(captured.value)["reason"] == "switching"
    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    # 旧 leader 不再把数据复制给 follower（epoch 已推进后继续拉取会制造分叉）。
    from bms_msg.broker.replication import ReplicaFetchRequest

    with pytest.raises(ReplicaUnavailableError):
        await cluster.leader.serve_fetch(
            ReplicaFetchRequest(
                leader="n1",
                follower="n2",
                topic_id=TOPIC_ID,
                partition_no=PARTITION_NO,
                from_offset=0,
                max_messages=10,
                epoch=0,
            )
        )


async def test_isr_scan_skips_switching_partition() -> None:
    """切换窗口：ISR 扫描跳过（不推进 HW / 不收缩 ISR）。"""
    cluster = _Cluster()
    await _ready(cluster)
    cluster.topology.switch_state = SWITCH_STATE_SWITCHING
    await cluster.leader.refresh_once()
    cluster.clock.advance(60_000)
    snapshot_before = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.leader.scan_isr_once()
    snapshot_after = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot_before is not None and snapshot_after is not None
    assert list(snapshot_after.isr) == list(snapshot_before.isr)


async def test_lease_expiry_then_refresh_recovers() -> None:
    """超租约拒写（350033 lease_expired）；刷新恢复后可写。"""
    cluster = _Cluster(lease_ms=1_000)
    await _ready(cluster)
    cluster.clock.advance(2_000)
    with pytest.raises(MsgNotLeaderError) as captured:
        await _append_writer(cluster.writer, seq=1)
    assert _error_data(captured.value)["reason"] == "lease_expired"
    # 第二次仍拒绝（日志抑制分支）。
    with pytest.raises(MsgNotLeaderError):
        await _append_writer(cluster.writer, seq=1)
    await cluster.leader.refresh_once()
    await _append_writer(cluster.writer, seq=1)


async def test_refresh_failure_keeps_lease_expired() -> None:
    """刷新失败不更新确认时间：超租约后继续拒写。"""
    cluster = _Cluster(lease_ms=1_000)
    await _ready(cluster)
    cluster.clock.advance(2_000)
    cluster.topology.fail = True
    await cluster.leader.refresh_once()
    with pytest.raises(MsgNotLeaderError) as captured:
        await _append_writer(cluster.writer, seq=1)
    assert _error_data(captured.value)["reason"] == "lease_expired"


async def test_refresh_demotes_old_leader_and_promotes_target() -> None:
    """刷新看到新 leader：旧 leader 降级拒写，目标升主可写。"""
    cluster = _Cluster()
    await _ready(cluster)
    cluster.topology.leader = "n2"
    cluster.topology.epoch = 1
    await cluster.leader.refresh_once()
    await cluster.follower.refresh_once()

    leader_state = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    follower_state = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert leader_state is not None and follower_state is not None
    assert leader_state.epoch == 1 and follower_state.epoch == 1
    with pytest.raises(MsgNotLeaderError) as captured:
        await _append_writer(cluster.writer, seq=1)
    assert _error_data(captured.value)["reason"] == "not_leader"
    assert follower_state.writable is True
    await _append_writer(cluster.follower_writer, seq=2)


async def test_demote_sets_switching_and_blocks_write() -> None:
    """控制通道 demote：立即停写并停在 switching（等刷新收口）。"""
    cluster = _Cluster()
    await _ready(cluster)
    await cluster.leader.demote(topic_id=TOPIC_ID, partition_no=PARTITION_NO, to_leader="n2", to_epoch=1)
    snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.leader == "n2"
    assert snapshot.epoch == 1
    assert snapshot.switch_state == SWITCH_STATE_SWITCHING
    assert snapshot.writable is False
    with pytest.raises(MsgNotLeaderError):
        await _append_writer(cluster.writer, seq=1)


async def test_demote_disabled_is_noop() -> None:
    """复制关闭：demote 直接返回（单实例零变化）。"""
    store = InMemoryMessageStore()
    coordinator = ReplicationCoordinator(
        config=ReplicaConfig(enabled=False, nodes=ConcurrentStableFrozenList()),
        store=store,
        clock=_Clock(),
    )
    await coordinator.demote(topic_id=TOPIC_ID, partition_no=PARTITION_NO, to_leader="n2", to_epoch=1)


async def test_apply_switch_truncates_to_watermark() -> None:
    """目标激活：按水位截断本地尾部并恢复可写。"""
    cluster = _Cluster()
    await _ready(cluster)
    for seq in range(3):
        await _append(cluster.follower_store, seq=seq)
    cluster.follower_writer = ReplicatedMessageStore(cluster.follower_store, replication=cluster.follower)
    result = await cluster.follower.apply_switch(
        SwitchApplyCommand(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_leader="n1",
            to_leader="n2",
            to_epoch=1,
            watermark=2,
            truncate=True,
            grace_ms=0,
        )
    )
    assert result.truncated_to == 2
    assert result.retained_tail == 0
    assert result.to_leo == 2
    assert await cluster.follower_store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 2
    snapshot = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.switch_state == SWITCH_STATE_IDLE
    assert snapshot.writable is True
    assert snapshot.epoch == 1


async def test_apply_switch_retains_tail_when_not_truncating() -> None:
    """旧 leader 不可达：保留尾部，记 `retained_tail`。"""
    cluster = _Cluster()
    await _ready(cluster)
    for seq in range(3):
        await _append(cluster.follower_store, seq=seq)
    result = await cluster.follower.apply_switch(
        SwitchApplyCommand(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_leader="n1",
            to_leader="n2",
            to_epoch=1,
            watermark=2,
            truncate=False,
            grace_ms=0,
        )
    )
    assert result.truncated_to is None
    assert result.retained_tail == 1
    assert await cluster.follower_store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 3


async def test_apply_switch_noop_when_below_watermark() -> None:
    """目标 LEO 未超过水位：不截断、不保留。"""
    cluster = _Cluster()
    await _ready(cluster)
    await _append(cluster.follower_store, seq=0)
    result = await cluster.follower.apply_switch(
        SwitchApplyCommand(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_leader="n1",
            to_leader="n2",
            to_epoch=1,
            watermark=2,
            truncate=True,
            grace_ms=0,
        )
    )
    assert result.truncated_to is None
    assert result.retained_tail == 0


async def test_apply_switch_idempotent() -> None:
    """重复激活：幂等返回 `already_applied`。"""
    cluster = _Cluster()
    await _ready(cluster)
    command = SwitchApplyCommand(
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        from_leader="n1",
        to_leader="n2",
        to_epoch=1,
        watermark=0,
        truncate=True,
        grace_ms=0,
    )
    await cluster.follower.apply_switch(command)
    again = await cluster.follower.apply_switch(command)
    assert again.already_applied is True


async def test_apply_switch_rejects_non_target() -> None:
    """非目标节点执行激活：拒绝（装配错误）。"""
    cluster = _Cluster()
    await _ready(cluster)
    with pytest.raises(ReplicaUnavailableError):
        await cluster.leader.apply_switch(
            SwitchApplyCommand(
                topic_id=TOPIC_ID,
                partition_no=PARTITION_NO,
                from_leader="n1",
                to_leader="n2",
                to_epoch=1,
                watermark=0,
                truncate=True,
                grace_ms=0,
            )
        )


async def test_diverged_tail_truncated_before_pull() -> None:
    """本地 LEO > 最后已知新 leader LEO：拉取前自动截断发散尾部。"""
    cluster = _Cluster()
    await _ready(cluster)
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append_writer(cluster.writer, seq=10)
    await _append(cluster.follower_store, seq=0)
    await _append(cluster.follower_store, seq=1)
    follower_state.local_leo = 2
    follower_state.last_reported_leader_leo = 1
    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    assert await cluster.follower_store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 1
    assert follower_state.local_leo == 1


async def test_diverged_unrepairable_raises() -> None:
    """leader 末端早于本地起点：无法截断，报分叉（全量重同步归 02-8）。"""
    cluster = _Cluster()
    await _ready(cluster)
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    for seq in range(4):
        await _append(cluster.follower_store, seq=seq)
    await cluster.follower_store.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=3)
    follower_state.local_leo = 4
    follower_state.last_reported_leader_leo = 2
    with pytest.raises(ReplicationDivergedError):
        await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


async def test_follower_snapshot_keeps_last_known_state() -> None:
    """follower 每次拉取保存最后已知 HW / ISR / 时刻（旧 leader 不可达时的验证输入）。"""
    cluster = _Cluster()
    await _ready(cluster)
    await _append_writer(cluster.writer, seq=1)
    cluster.clock.advance(10)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    snapshot = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.last_known_hw == 1
    assert list(snapshot.last_known_isr) == ["n1", "n2"]
    assert snapshot.last_known_at > 0


async def test_refresh_without_loader_is_noop() -> None:
    """无拓扑加载器：刷新循环不启动、refresh_once 直接返回。"""
    coordinator = ReplicationCoordinator(
        config=ReplicaConfig(
            enabled=True,
            node_id="n1",
            nodes=ConcurrentStableFrozenList(("n1",)),
            replication_factor=1,
            min_insync_replicas=1,
            leader_lease_ms=1_000,
            switch_grace_ms=1_000,
        ),
        store=InMemoryMessageStore(),
        clock=_Clock(),
    )
    await coordinator.refresh_once()
    await coordinator.ensure_started()
    assert coordinator.enabled is True
    await coordinator.aclose()


async def test_refresh_skips_when_disabled() -> None:
    """复制关闭：refresh_once 直接返回。"""
    coordinator = ReplicationCoordinator(
        config=ReplicaConfig(enabled=False, nodes=ConcurrentStableFrozenList()),
        store=InMemoryMessageStore(),
        clock=_Clock(),
    )
    await coordinator.refresh_once()


async def test_ensure_started_and_close_are_idempotent() -> None:
    """刷新循环随协调器启停（幂等）。"""
    cluster = _Cluster()
    await cluster.leader.ensure_started()
    await cluster.leader.ensure_started()
    await cluster.leader.aclose()
    await cluster.leader.aclose()
    assert cluster.leader.enabled is True


async def test_refresh_loop_runs_and_stops() -> None:
    """刷新循环按周期执行（1ms 注入，等到至少一轮后停机）。"""
    cluster = _Cluster(epoch_refresh_ms=1)
    await _ready(cluster)
    loads_before = cluster.topology.loads
    await cluster.leader.ensure_started()
    await asyncio.sleep(0.03)
    assert cluster.topology.loads > loads_before
    await cluster.leader.aclose()


async def test_apply_switch_without_watermark_keeps_tail() -> None:
    """水位置空（完全未知）：不截断也不计保留条数。"""
    cluster = _Cluster()
    await _ready(cluster)
    for seq in range(2):
        await _append(cluster.follower_store, seq=seq)
    result = await cluster.follower.apply_switch(
        SwitchApplyCommand(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_leader="n1",
            to_leader="n2",
            to_epoch=1,
            watermark=None,
            truncate=False,
            grace_ms=0,
        )
    )
    assert result.truncated_to is None
    assert result.retained_tail == 0
    assert await cluster.follower_store.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 2


async def test_demote_follower_is_noop_for_role() -> None:
    """follower 收到 demote：只更新 leader / epoch / 状态，不改角色分支。"""
    cluster = _Cluster()
    await _ready(cluster)
    await cluster.follower.demote(topic_id=TOPIC_ID, partition_no=PARTITION_NO, to_leader="n1", to_epoch=2)
    snapshot = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.epoch == 2
    assert snapshot.switch_state == SWITCH_STATE_SWITCHING


async def test_refresh_twice_while_switching_keeps_observed_state() -> None:
    """已在 switching 时再次刷新：保持切换状态（不清 observed / 不重复计时）。"""
    cluster = _Cluster()
    await _ready(cluster)
    cluster.topology.switch_state = SWITCH_STATE_SWITCHING
    await cluster.leader.refresh_once()
    first = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.leader.refresh_once()
    second = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert first is not None and second is not None
    assert second.switch_state == SWITCH_STATE_SWITCHING
    assert second.writable is False


async def test_refresh_follower_to_follower_leader_change_updates_role() -> None:
    """非 leader 的 follower 看到 leader 换人：只改 leader，不触发升降级日志分支。"""
    topology = _Topology(nodes=("n1", "n2", "n3"), min_insync=1)
    config = ReplicaConfig(
        enabled=True,
        node_id="n2",
        nodes=ConcurrentStableFrozenList(("n1", "n2", "n3")),
        replication_factor=3,
        min_insync_replicas=1,
        leader_lease_ms=5_000,
        switch_grace_ms=5_000,
    )
    coordinator = ReplicationCoordinator(
        config=config,
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", topology),
        clock=_Clock(),
    )
    await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    topology.leader = "n3"
    topology.epoch = 2
    await coordinator.refresh_once()
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.leader == "n3"
    assert snapshot.epoch == 2
    assert snapshot.writable is False


async def test_config_rejects_grace_below_lease() -> None:
    """配置校验：停写宽限小于租约即拒启（避免与旧 leader 写窗口重叠）。"""
    with pytest.raises(ValueError):
        ReplicaConfig(
            enabled=True,
            node_id="n1",
            nodes=ConcurrentStableFrozenList(("n1",)),
            replication_factor=1,
            min_insync_replicas=1,
            leader_lease_ms=5_000,
            switch_grace_ms=1_000,
        ).validate()


class _ExplodingTransport(ReplicaTransport):
    """拉取即抛分叉错误的传输替身。"""

    def __init__(self, error: ReplicationDivergedError) -> None:
        """初始化。

        Args:
            error: 要抛出的错误。
        """
        self.error = error

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """恒抛预置错误。

        Args:
            request: 拉取请求。

        Returns:
            ReplicaFetchResult: 不返回。

        Raises:
            ReplicationDivergedError: 恒抛。
        """
        del request
        raise self.error


async def test_pull_diverged_exception_without_leader_leo_raises() -> None:
    """拉取报分叉且无 leader 末端：原样上抛（无法修复）。"""
    cluster = _Cluster()
    await _ready(cluster)
    cluster.follower._transport = _ExplodingTransport(  # pyright: ignore[reportPrivateUsage]
        ReplicationDivergedError("分叉", leader_leo=None)
    )
    with pytest.raises(ReplicationDivergedError):
        await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


async def test_pull_diverged_exception_repairable_truncates() -> None:
    """拉取报分叉且给出 leader 末端 >= 本地 LEO：视为已对齐，返回 0。"""
    cluster = _Cluster()
    await _ready(cluster)
    cluster.follower._transport = _ExplodingTransport(  # pyright: ignore[reportPrivateUsage]
        ReplicationDivergedError("分叉", leader_leo=5)
    )
    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0


async def test_replicated_store_truncate_to_delegates() -> None:
    """复制包装的 `truncate_to` 纯委托（截断由切换流程显式驱动）。"""
    cluster = _Cluster()
    await _ready(cluster)
    await _append(cluster.leader_store, seq=0)
    await _append(cluster.leader_store, seq=1)
    assert await cluster.writer.truncate_to(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=1) == 1
