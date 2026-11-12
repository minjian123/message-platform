"""副本与 ISR 复制（msg 任务 02-4；Kiwi 2484）。

覆盖详细设计 §14 的 G1~G6：

- 复制关闭：`HW = LEO`、`acks=all` 与 `acks=1` 等价、ISR 恒为 `[本节点]`；
- 两节点追赶：HW 只在 follower 上报 LEO 后推进，未提交部分不可见；
- `acks=all`：等待提交、ISR 不足拒绝、提交超时 `350032`；
- ISR：时间 / 条数阈值踢出、追上重入、收缩计数；
- epoch：旧 leader 数据被拒、任期前进被接受、leader 侧拒绝更高任期请求；
- 配置校验与指标基数上限。
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableFrozenSet,
    ConcurrentStableList,
)
from bms_core.db.tenant import TenantContext

from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replication import (
    InProcessReplicaTransport,
    PartitionTopology,
    ReplicaConfig,
    ReplicaFetchRequest,
    ReplicaFetchResult,
    ReplicatedMessageStore,
    ReplicationCoordinator,
    ReplicationDivergedError,
    ReplicationMetrics,
    ReplicationSnapshot,
    ReplicationView,
    ReplicaTopologyLoader,
    ReplicaTransport,
    ReplicaUnavailableError,
    StaleLeaderError,
)
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.errors import MsgError, MsgErrorCode, MsgInsufficientReplicasError, MsgNotLeaderError

pytestmark = pytest.mark.kiwi_id(2484)

TOPIC_ID = 7001
PARTITION_NO = 0
"""用例分区键（不依赖数据库 / 主题路由）。"""

NAMESPACE = "msg"
"""契约用例的命名空间。"""

TOPIC_NAME = "audit.replication"
"""契约用例的主题名。"""


class _Clock:
    """可推进的单调毫秒时钟（ISR 时间判据用）。"""

    def __init__(self) -> None:
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


class _TickClock(_Clock):
    """每次读取自增的时钟（让退避窗口既过期、又落在日志抑制窗口内）。"""

    def __init__(self, *, step: float = 10.0) -> None:
        super().__init__()
        self._step = step

    def __call__(self) -> float:
        """返回自增后的毫秒。

        Returns:
            float: 自增后的毫秒数。
        """
        self.value += self._step
        return self.value


def _config(
    *,
    node_id: str = "n1",
    nodes: tuple[str, ...] = ("n1", "n2"),
    enabled: bool = True,
    factor: int = 2,
    min_insync: int = 2,
    lag_time_ms: int = 10_000,
    lag_records: int = 10_000,
    commit_timeout_ms: int = 100,
) -> ReplicaConfig:
    """构造复制配置。

    Args:
        node_id: 本节点标识。
        nodes: 集群节点。
        enabled: 是否开启复制。
        factor: 副本数。
        min_insync: 最小同步副本数。
        lag_time_ms: ISR 时间阈值。
        lag_records: ISR 条数阈值。
        commit_timeout_ms: 提交等待超时。

    Returns:
        ReplicaConfig: 配置。
    """
    return ReplicaConfig(
        enabled=enabled,
        node_id=node_id,
        nodes=ConcurrentStableFrozenList(nodes),
        replication_factor=factor,
        min_insync_replicas=min_insync,
        lag_time_max_ms=lag_time_ms,
        lag_records_max=lag_records,
        fetch_interval_ms=1_000,
        fetch_batch=100,
        commit_timeout_ms=commit_timeout_ms,
        scan_interval_ms=1_000,
        topology_ttl_ms=1_000,
    )


class _Topology:
    """固定拓扑加载器（用例：显式副本集，避免依赖轮转推导）。"""

    def __init__(self, *, nodes: tuple[str, ...], min_insync: int, epoch: int = 0) -> None:
        self.nodes = nodes
        self.min_insync = min_insync
        self.epoch = epoch

    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
        """返回固定拓扑。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            PartitionTopology: 固定副本集。
        """
        return PartitionTopology(
            replicas=ConcurrentStableFrozenList(self.nodes),
            min_insync_replicas=self.min_insync,
            epoch=self.epoch,
        )


class _Cluster:
    """两节点进程内复制簇（leader + follower + 传输）。"""

    def __init__(
        self,
        *,
        nodes: tuple[str, ...] = ("n1", "n2"),
        factor: int = 2,
        min_insync: int = 2,
        lag_time_ms: int = 10_000,
        lag_records: int = 10_000,
        commit_timeout_ms: int = 100,
        clock: _Clock | None = None,
    ) -> None:
        self.clock = clock if clock is not None else _Clock()
        self.transport = InProcessReplicaTransport()
        self.leader_store = InMemoryMessageStore()
        self.follower_store = InMemoryMessageStore()
        self.waiter = DeliveryWaiter()
        self.metrics = ReplicationMetrics()
        base = _config(
            node_id=nodes[0],
            nodes=nodes,
            factor=factor,
            min_insync=min_insync,
            lag_time_ms=lag_time_ms,
            lag_records=lag_records,
            commit_timeout_ms=commit_timeout_ms,
        )
        topology = _Topology(nodes=nodes, min_insync=min_insync)
        self.leader = ReplicationCoordinator(
            config=base,
            store=self.leader_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", topology),
            waiter=self.waiter,
            metrics=self.metrics,
            clock=self.clock,
        )
        self.follower = ReplicationCoordinator(
            config=replace(base, node_id=nodes[1]),
            store=self.follower_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", topology),
            metrics=ReplicationMetrics(),
            clock=self.clock,
        )
        self.transport.register(nodes[0], self.leader)
        self.transport.register(nodes[1], self.follower)
        self.writer = ReplicatedMessageStore(self.leader_store, replication=self.leader)


def _error_data(error: MsgError) -> ConcurrentStableDict[str, object]:
    """取错误附带数据（`MsgError.data` 在基类上是可选对象，测试里统一收窄）。

    Args:
        error: 复制错误。

    Returns:
        ConcurrentStableDict[str, object]: 附带数据。
    """
    return cast("ConcurrentStableDict[str, object]", error.data)


def _message(*, seq: int) -> StoredMessage:
    """构造一条待写入记录（固定分区、可重复调用）。

    Args:
        seq: 序号（用于 ID 去重）。

    Returns:
        StoredMessage: 记录骨架。
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


async def _append(writer: ReplicatedMessageStore, *, seq: int, epoch: int = 0) -> StoredMessage:
    """经复制包装写入一条记录。

    Args:
        writer: 复制包装后的存储。
        seq: 序号。
        epoch: leader 任期。

    Returns:
        StoredMessage: 含分配 offset 的记录。
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
        epoch=epoch,
    )


async def test_disabled_hw_equals_leo_and_acks_all_not_gated() -> None:
    """复制关闭：HW = LEO、ISR = [本节点]、acks=all 不预检也不等待。"""
    store = InMemoryMessageStore()
    coordinator = ReplicationCoordinator(config=_config(enabled=False, nodes=()), store=store)
    writer = ReplicatedMessageStore(store, replication=coordinator)
    await _append(writer, seq=1)
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.hw == snapshot.leo == 1
    assert list(snapshot.isr) == ["n1"]
    await coordinator.ensure_writable(topic_id=TOPIC_ID, partition_no=PARTITION_NO, acks="all")
    await coordinator.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0)
    assert await coordinator.visible_limit(topic_id=TOPIC_ID, partition_no=PARTITION_NO) is None


async def test_single_replica_acks_all_succeeds_without_follower() -> None:
    """单副本：ISR 只有 leader，acks=all 立即提交。"""
    store = InMemoryMessageStore()
    coordinator = ReplicationCoordinator(
        config=_config(nodes=("n1",), factor=1, min_insync=1),
        store=store,
    )
    writer = ReplicatedMessageStore(store, replication=coordinator)
    await _append(writer, seq=1)
    await coordinator.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0)
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.hw == 1
    assert list(snapshot.isr) == ["n1"]


async def test_hw_advances_only_after_follower_reports_leo() -> None:
    """两节点：follower 未上报前 HW 不动，上报并追平后才推进。"""
    cluster = _Cluster()
    await _append(cluster.writer, seq=1)
    leader_snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert leader_snapshot is not None and leader_snapshot.hw == 0
    assert await cluster.follower.visible_limit(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0

    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 1
    leader_snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert leader_snapshot is not None and leader_snapshot.hw == 0

    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    leader_snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    follower_snapshot = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert leader_snapshot is not None and leader_snapshot.hw == 1
    assert follower_snapshot is not None and follower_snapshot.hw == 1


async def test_acks_all_waits_until_commit() -> None:
    """acks=all：follower 拉取前挂起，提交后返回。"""
    cluster = _Cluster()
    await _append(cluster.writer, seq=1)
    waiter = asyncio.create_task(cluster.leader.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await asyncio.wait_for(waiter, timeout=1)
    # ISR 充足时 acks=all 的预检直接通过（不抛 350032）。
    await cluster.leader.ensure_writable(topic_id=TOPIC_ID, partition_no=PARTITION_NO, acks="all")


async def test_insufficient_isr_rejects_acks_all_but_allows_local() -> None:
    """ISR 不足：acks=all 返回 350032，acks=1 不受影响。"""
    clock = _Clock()
    cluster = _Cluster(clock=clock, lag_time_ms=100)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    clock.advance(1_000)
    await cluster.leader.scan_isr_once()
    with pytest.raises(MsgInsufficientReplicasError) as captured:
        await cluster.leader.ensure_writable(topic_id=TOPIC_ID, partition_no=PARTITION_NO, acks="all")
    assert _error_data(captured.value)["reason"] == "insufficient_isr"
    await cluster.leader.ensure_writable(topic_id=TOPIC_ID, partition_no=PARTITION_NO, acks="1")


async def test_commit_timeout_returns_350032() -> None:
    """提交等待超时：返回 350032（reason=commit_timeout）。"""
    cluster = _Cluster(commit_timeout_ms=5)
    await _append(cluster.writer, seq=1)
    with pytest.raises(MsgInsufficientReplicasError) as captured:
        await cluster.leader.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0)
    assert _error_data(captured.value)["reason"] == "commit_timeout"
    assert _error_data(captured.value)["timeout_ms"] == 5


async def test_isr_shrinks_by_time_and_counts() -> None:
    """时间阈值：超过 lag_time 未拉取即踢出，收缩计数 +1。"""
    clock = _Clock()
    cluster = _Cluster(clock=clock, lag_time_ms=100)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    clock.advance(1_000)
    await cluster.leader.scan_isr_once()
    snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert list(snapshot.isr) == ["n1"]
    assert cluster.metrics.shrink_total == 1


async def test_isr_shrinks_by_records() -> None:
    """条数阈值：leader LEO 与 follower LEO 差值超限即踢出。"""
    cluster = _Cluster(lag_records=1)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append(cluster.writer, seq=1)
    await _append(cluster.writer, seq=2)
    await cluster.leader.scan_isr_once()
    snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert list(snapshot.isr) == ["n1"]


async def test_isr_rejoins_after_catch_up() -> None:
    """重入：踢出后 follower 追上 leader LEO，扫描后回到 ISR 并推进 HW。"""
    cluster = _Cluster(lag_records=1)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append(cluster.writer, seq=1)
    await _append(cluster.writer, seq=2)
    await cluster.leader.scan_isr_once()
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.leader.scan_isr_once()
    snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert list(snapshot.isr) == ["n1", "n2"]
    assert snapshot.hw == 2


async def test_epoch_rejects_stale_leader() -> None:
    """follower 本地任期更高：拒绝低任期 leader 的批次。"""
    cluster = _Cluster()
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    leader_state = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    leader_state.epoch = 1
    follower_state.epoch = 2
    await _append(cluster.writer, seq=1)
    with pytest.raises(StaleLeaderError):
        await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


async def test_epoch_advances_on_newer_leader() -> None:
    """leader 任期更高：follower 接受并更新本地任期。"""
    cluster = _Cluster()
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    leader_state = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    leader_state.epoch = 2
    follower_state.epoch = 1
    await _append(cluster.writer, seq=1, epoch=2)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert follower_state.epoch == 2
    stored = await cluster.follower_store.fetch(
        topic_id=TOPIC_ID, partition_no=PARTITION_NO, from_offset=0, max_messages=10
    )
    assert stored[0].epoch == 2


async def test_leader_rejects_newer_epoch_request() -> None:
    """leader 侧：请求方任期更高即判定本节点是旧 leader，不返回数据。"""
    cluster = _Cluster()
    state = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    state.epoch = 1
    request = ReplicaFetchRequest(
        leader="n1",
        follower="n2",
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        from_offset=0,
        max_messages=10,
        epoch=2,
    )
    with pytest.raises(StaleLeaderError):
        await cluster.leader.serve_fetch(request)


async def test_follower_diverged_when_ahead_of_leader() -> None:
    """follower 位点超过 leader LEO：02-5 按 leader 末端截断本地发散尾部后重新拉取。"""
    cluster = _Cluster()
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    follower_state.local_leo = 5
    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    assert follower_state.local_leo == 0


async def test_unregistered_transport_node_raises_unavailable() -> None:
    """传输未登记 leader：抛 ReplicaUnavailableError（follower 侧退避）。"""
    config = _config(node_id="n2", nodes=("n1", "n2"))
    coordinator = ReplicationCoordinator(
        config=config,
        store=InMemoryMessageStore(),
        transport=InProcessReplicaTransport(),
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=2)),
    )
    await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    with pytest.raises(ReplicaUnavailableError):
        await coordinator.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


async def test_hw_advance_notifies_waiter() -> None:
    """HW 推进唤醒长轮询（版本号变化）。"""
    cluster = _Cluster()
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    before = cluster.waiter.version((TOPIC_ID, PARTITION_NO))
    await _append(cluster.writer, seq=1)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert cluster.waiter.version((TOPIC_ID, PARTITION_NO)) > before


def test_publish_request_acks_contract() -> None:
    """发布 `acks` 契约：缺省 `"1"`、接受 `"all"`、非法值由契约层拒绝。"""
    from pydantic import ValidationError

    from bms_msg.schemas.message import PublishRequest

    default = PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME)
    assert default.acks == "1"
    assert PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, acks="all").acks == "all"
    with pytest.raises(ValidationError):
        PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, acks="2")  # type: ignore[arg-type]


def test_config_validation() -> None:
    """配置校验：关闭时不强制节点；开启时节点 / 因子 / 阈值非法即拒。"""
    _config(enabled=False, nodes=()).validate()
    with pytest.raises(ValueError, match="为空"):
        _config(nodes=()).validate()
    with pytest.raises(ValueError, match="本节点"):
        replace(_config(), node_id="n9").validate()
    with pytest.raises(ValueError, match="replication_factor"):
        _config(nodes=("n1",)).validate()
    with pytest.raises(ValueError, match="min_insync_replicas"):
        replace(_config(), min_insync_replicas=3).validate()
    with pytest.raises(ValueError, match="不能同时为 0"):
        replace(_config(), lag_time_max_ms=0, lag_records_max=0).validate()


async def test_topology_min_insync_clamped_to_replicas() -> None:
    """拓扑给的分区级 min 大于副本集长度：按副本集长度生效并告警（不拒启）。"""
    coordinator = ReplicationCoordinator(
        config=_config(min_insync=2),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1",), min_insync=2)),
    )
    state = await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.min_insync_replicas == 1


def test_metrics_partition_limit() -> None:
    """指标基数上限：超出部分不进入快照（只告警一次，后续溢出静默丢弃）。"""
    metrics = ReplicationMetrics(partition_limit=2)
    metrics.update(topic_id=1, partition_no=0, isr_size=1, lag_records=0)
    metrics.update(topic_id=1, partition_no=1, isr_size=1, lag_records=0)
    metrics.update(topic_id=1, partition_no=2, isr_size=1, lag_records=0)
    metrics.update(topic_id=1, partition_no=3, isr_size=1, lag_records=0)
    assert len(metrics.snapshot().rows) == 2


async def test_partition_inspector_uses_replication_snapshot() -> None:
    """管理面分区详情：hw / isr / leader 取复制快照。"""
    from bms_msg.broker import admin_view
    from bms_msg.tenant.context import CurrentTenant as Tenant

    class _Topic:
        id = TOPIC_ID
        partition_count = 1

    class _Router:
        async def resolve(self, *, namespace: str, name: str) -> _Topic:
            del namespace, name
            return _Topic()

        def partition(self, topic: _Topic, partition_no: int) -> int:
            del topic
            return partition_no

    class _Row:
        partition_no = 0
        leader = "db-leader"

    class _Repo:
        async def list_by_topic(self, topic_id: int) -> ConcurrentStableList[_Row]:
            del topic_id
            return ConcurrentStableList((_Row(),))

    class _SnapshotView(ReplicationView):
        @property
        def enabled(self) -> bool:
            return True

        async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
            del topic_id, partition_no, acks

        async def wait_committed(self, *, topic_id: int, partition_no: int, offset: int) -> None:
            del topic_id, partition_no, offset

        async def visible_limit(self, *, topic_id: int, partition_no: int) -> int | None:
            del topic_id, partition_no
            return 3

        async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
            del topic_id, partition_no
            return ReplicationSnapshot(
                leader="n1",
                epoch=1,
                leo=5,
                hw=3,
                isr=ConcurrentStableFrozenList(("n1", "n2")),
                replicas=ConcurrentStableFrozenList(("n1", "n2")),
            )

    tenant = Tenant(
        tenant=TenantContext(code="demo", db_key="tenant_demo", name="演示租户", tenant_id=1),
        namespace_allowlist=ConcurrentStableFrozenSet((NAMESPACE,)),
    )
    inspector = admin_view.PartitionInspector(
        router=cast("object", _Router()),  # type: ignore[arg-type]
        messages=InMemoryMessageStore(),
        offsets=InMemoryOffsetStore(),
        groups=cast("object", None),  # type: ignore[arg-type]
        partitions=cast("object", _Repo()),  # type: ignore[arg-type]
        replication=_SnapshotView(),
    )
    detail = await inspector.describe(
        tenant=tenant,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition=None,
        group=None,
    )
    view = detail.items[0]
    assert view.leader == "n1"
    assert view.hw == 3
    assert list(view.isr) == ["n1", "n2"]


def test_metrics_collector_exports_replication() -> None:
    """指标出口：ISR 收缩计数、ISR 大小与副本延迟按分区输出。"""
    from bms_core.core.concurrent import ConcurrentStableDict

    from bms_msg.metrics.collector import MsgMetricsCollector
    from bms_msg.metrics.lag import MetricsSnapshotStore
    from bms_msg.metrics.traffic import TrafficMetrics

    class _Empty:
        def snapshot(self) -> ConcurrentStableDict[str, object]:
            return ConcurrentStableDict()

    metrics = ReplicationMetrics()
    metrics.record_shrink()
    metrics.update(topic_id=TOPIC_ID, partition_no=0, isr_size=2, lag_records=4)
    collector = MsgMetricsCollector(
        service="msg",
        traffic=TrafficMetrics(),
        snapshots=MetricsSnapshotStore(),
        storage=_Empty(),
        delay=_Empty(),
        backpressure=_Empty(),
        lifecycle=_Empty(),
        purge=_Empty(),
        replication=metrics,
    )
    families: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    for family in collector.collect():
        families.set(family.name, family)
        families.set(f"{family.name}_total", family)
    assert families["msg_isr_shrink_total"].samples[0].value == 1.0
    isr = families["msg_isr_size"].samples[0]
    assert isr.labels == {"service": "msg", "topic_id": str(TOPIC_ID), "partition": "0"}
    assert isr.value == 2.0
    assert families["msg_replication_lag_records"].samples[0].value == 4.0


def test_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量解析：缺省关闭；开启时读取节点 / 因子；非法整数即拒。"""
    from bms_msg.core.config import (
        REPLICATION_ENABLED_ENV,
        REPLICATION_FACTOR_ENV,
        REPLICATION_MIN_INSYNC_ENV,
        REPLICATION_NODE_ID_ENV,
        REPLICATION_NODES_ENV,
    )

    for name in (
        REPLICATION_ENABLED_ENV,
        REPLICATION_NODE_ID_ENV,
        REPLICATION_NODES_ENV,
        REPLICATION_FACTOR_ENV,
        REPLICATION_MIN_INSYNC_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    default = ReplicaConfig.from_env()
    assert default.enabled is False

    monkeypatch.setenv(REPLICATION_ENABLED_ENV, "true")
    monkeypatch.setenv(REPLICATION_NODE_ID_ENV, "n1")
    monkeypatch.setenv(REPLICATION_NODES_ENV, "n1, n2")
    monkeypatch.setenv(REPLICATION_FACTOR_ENV, "2")
    monkeypatch.setenv(REPLICATION_MIN_INSYNC_ENV, "2")
    config = ReplicaConfig.from_env()
    assert config.enabled is True
    assert list(config.nodes) == ["n1", "n2"]
    assert config.replication_factor == 2

    monkeypatch.setenv(REPLICATION_FACTOR_ENV, "not-int")
    with pytest.raises(ValueError, match="须为整数"):
        ReplicaConfig.from_env()


async def test_topology_loader_supplies_replicas() -> None:
    """拓扑加载器给出副本集与分区级 min：协调器按其挂载。"""

    class Loader:
        async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology | None:
            return PartitionTopology(
                replicas=ConcurrentStableFrozenList(("n2", "n1")),
                min_insync_replicas=1,
                epoch=3,
            )

    coordinator = ReplicationCoordinator(
        config=_config(node_id="n2", nodes=("n1", "n2")),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", Loader()),
    )
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert snapshot.leader == "n2"
    assert snapshot.epoch == 3


async def test_replication_properties_and_metrics_forget() -> None:
    """开关 / 配置只读属性与指标行删除。"""
    cluster = _Cluster()
    assert cluster.leader.enabled is True
    assert cluster.leader.config.node_id == "n1"
    cluster.metrics.update(topic_id=TOPIC_ID, partition_no=PARTITION_NO, isr_size=2, lag_records=1)
    assert len(cluster.metrics.snapshot().rows) == 1
    cluster.metrics.forget(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert len(cluster.metrics.snapshot().rows) == 0
    cluster.transport.unregister("n2")
    cluster.transport.unregister("n2")


async def test_attach_is_idempotent() -> None:
    """重复挂载返回同一实例（惰性挂载不重复解析拓扑）。"""
    cluster = _Cluster()
    first = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    second = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert first is second


async def test_node_outside_replica_set_is_follower() -> None:
    """本节点不在副本集内：按 follower 挂载（只拉取、不承担 leader 职责）。"""
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n3", nodes=("n1", "n2", "n3")),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=1)),
    )
    state = await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.role.value == "follower"
    assert state.leader == "n1"


async def test_topology_loader_failure_falls_back_to_config() -> None:
    """拓扑加载失败：走配置轮转兜底并告警（不拒绝发布）。"""

    class BrokenLoader:
        async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology | None:
            raise RuntimeError("元数据库不可用")

    coordinator = ReplicationCoordinator(
        config=_config(node_id="n1", nodes=("n1", "n2")),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", BrokenLoader()),
    )
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None
    assert len(snapshot.replicas) == 2


async def test_follower_ensure_rejects_and_wait_is_noop() -> None:
    """follower 收到 acks=all：预检报 not_leader（350033）；提交等待不参与。"""
    cluster = _Cluster()
    with pytest.raises(MsgNotLeaderError) as captured:
        await cluster.follower.ensure_writable(topic_id=TOPIC_ID, partition_no=PARTITION_NO, acks="all")
    assert captured.value.code_ == MsgErrorCode.NOT_LEADER
    assert _error_data(captured.value)["reason"] == "not_leader"
    await cluster.follower.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0)


async def test_snapshot_failure_returns_none() -> None:
    """快照取数失败：管理面返回 None（不 500）。"""

    class BrokenStore(InMemoryMessageStore):
        async def leo(self, *, topic_id: int, partition_no: int) -> int:
            del topic_id, partition_no
            raise RuntimeError("存储不可用")

    coordinator = ReplicationCoordinator(config=_config(enabled=False, nodes=()), store=BrokenStore())
    assert await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO) is None


async def test_serve_fetch_validation_branches() -> None:
    """serve_fetch：非 leader / follower 不在副本集 / 位点早于起点均拒绝。"""
    cluster = _Cluster()
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    request = ReplicaFetchRequest(
        leader="n2",
        follower="n1",
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        from_offset=0,
        max_messages=10,
        epoch=0,
    )
    with pytest.raises(ReplicaUnavailableError):
        await cluster.follower.serve_fetch(request)

    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    unknown = ReplicaFetchRequest(
        leader="n1",
        follower="n9",
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        from_offset=0,
        max_messages=10,
        epoch=0,
    )
    with pytest.raises(ReplicaUnavailableError):
        await cluster.leader.serve_fetch(unknown)

    old = ReplicaFetchRequest(
        leader="n1",
        follower="n2",
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        from_offset=-1,
        max_messages=10,
        epoch=0,
    )
    with pytest.raises(ReplicationDivergedError):
        await cluster.leader.serve_fetch(old)


async def test_lag_records_non_leader_and_pull_non_follower() -> None:
    """非 leader 的副本延迟恒 0；leader 不执行 follower 拉取。"""
    cluster = _Cluster()
    follower_state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert follower_state.lag_records() == 0
    assert await cluster.leader.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0


async def test_pull_without_transport_raises() -> None:
    """follower 未装配传输：抛 ReplicaUnavailableError（退避由循环处理）。"""
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n2", nodes=("n1", "n2")),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=1)),
    )
    await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    with pytest.raises(ReplicaUnavailableError):
        await coordinator.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


class _ScriptedTransport(ReplicaTransport):
    """按脚本返回结果的传输替身（覆盖 follower 侧异常分支）。"""

    def __init__(self, *, result: ReplicaFetchResult) -> None:
        self.result = result

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        del request
        return self.result


async def test_follower_pull_rejects_lower_epoch_and_bad_offsets() -> None:
    """follower 侧：对端任期更低 / 起点不一致 / 记录 offset 不连续均拒绝。"""
    record = await _leader_record(seq=1)
    cases = [
        ReplicaFetchResult(
            leader_epoch=0,
            from_offset=0,
            records=ConcurrentStableList(),
            leader_leo=1,
            high_watermark=1,
        ),
        ReplicaFetchResult(
            leader_epoch=1,
            from_offset=99,
            records=ConcurrentStableList(),
            leader_leo=1,
            high_watermark=1,
        ),
        ReplicaFetchResult(
            leader_epoch=1,
            from_offset=0,
            records=ConcurrentStableList((record,)),
            leader_leo=1,
            high_watermark=1,
        ),
    ]
    for index, result in enumerate(cases):
        coordinator = ReplicationCoordinator(
            config=_config(node_id="n2", nodes=("n1", "n2")),
            store=InMemoryMessageStore(),
            transport=_ScriptedTransport(result=result),
            topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=1)),
        )
        state = await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
        state.epoch = 2 if index == 0 else 0
        with pytest.raises((StaleLeaderError, ReplicationDivergedError)):
            await coordinator.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)


async def _leader_record(*, seq: int) -> StoredMessage:
    """构造一条 offset 非 0 的记录（用于「记录 offset 不连续」分支）。

    Args:
        seq: 序号。

    Returns:
        StoredMessage: offset=5 的记录。
    """
    template = _message(seq=seq)
    return StoredMessage(
        message_id=template.message_id,
        log_id=template.log_id,
        topic_id=template.topic_id,
        partition_no=template.partition_no,
        offset=5,
        key=template.key,
        payload=template.payload,
        headers=template.headers,
        trace=template.trace,
        published_at=template.published_at,
    )


async def test_follower_hw_advance_notifies_own_waiter() -> None:
    """follower 拉到更高 HW：唤醒本地等待者。"""
    record = await _leader_record(seq=1)
    record = StoredMessage(
        message_id=record.message_id,
        log_id=record.log_id,
        topic_id=record.topic_id,
        partition_no=record.partition_no,
        offset=0,
        key=record.key,
        payload=record.payload,
        headers=record.headers,
        trace=record.trace,
        published_at=record.published_at,
    )
    waiter = DeliveryWaiter()
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n2", nodes=("n1", "n2")),
        store=InMemoryMessageStore(),
        transport=_ScriptedTransport(
            result=ReplicaFetchResult(
                leader_epoch=0,
                from_offset=0,
                records=ConcurrentStableList((record,)),
                leader_leo=1,
                high_watermark=1,
            )
        ),
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=1)),
        waiter=waiter,
    )
    await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    before = waiter.version((TOPIC_ID, PARTITION_NO))
    await coordinator.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert waiter.version((TOPIC_ID, PARTITION_NO)) > before


async def test_ensure_started_runs_loops_and_backoff() -> None:
    """后台循环：启动 / 停止幂等；拉取失败按退避重试（日志 60s 抑制）。"""
    transport = InProcessReplicaTransport()
    config = replace(
        _config(node_id="n2", nodes=("n1", "n2")),
        fetch_interval_ms=20,
        scan_interval_ms=1,
    )
    coordinator = ReplicationCoordinator(
        config=config,
        store=InMemoryMessageStore(),
        transport=transport,
        topology_loader=cast("ReplicaTopologyLoader", _Topology(nodes=("n1", "n2"), min_insync=1)),
        # 时钟步长（5ms）小于退避窗口（20ms）：先落进「退避中跳过」分支，再于下一次重试时
        # 命中 60s 日志抑制窗口。
        clock=_TickClock(step=5),
    )
    await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await coordinator.ensure_started()
    await coordinator.ensure_started()
    await asyncio.sleep(0.35)
    await coordinator.aclose()
    await coordinator.aclose()


async def test_scan_rejoin_updates_isr_and_hw() -> None:
    """扫描重入：follower 已追平且拉取新鲜 → 加回 ISR 并推进 HW。"""
    cluster = _Cluster()
    await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append(cluster.writer, seq=1)
    state = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    state.isr.discard("n2")
    progress = state.followers.get("n2")
    assert progress is not None
    state.followers.set(
        "n2",
        type(progress)(leo=1, last_fetch_ms=cluster.clock(), in_sync=False),
    )
    await cluster.leader.scan_isr_once()
    assert "n2" in state.isr
    assert state.hw == 1


async def test_replicated_store_delegates_read_and_cleanup() -> None:
    """复制包装：读 / 段 / 头视图 / 删除全部原样委托。"""
    cluster = _Cluster()
    message = await _append(cluster.writer, seq=1)
    assert await cluster.writer.leo(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 1
    assert await cluster.writer.start_offset(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    fetched = await cluster.writer.fetch(topic_id=TOPIC_ID, partition_no=PARTITION_NO, from_offset=0, max_messages=10)
    assert [item.offset for item in fetched] == [0]
    headers = await cluster.writer.scan_headers(
        topic_id=TOPIC_ID, partition_no=PARTITION_NO, from_offset=0, max_messages=10
    )
    assert [item.offset for item in headers] == [0]
    assert await cluster.writer.find_by_log_id(log_id=message.log_id) is not None
    assert await cluster.writer.segments(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert await cluster.writer.truncate_before(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0) == 0
    assert await cluster.writer.drop_topic(topic_id=TOPIC_ID) == 1


async def test_disabled_hw_tracks_leo_after_early_attach() -> None:
    """复制关闭：先挂载（hw=0）再写入，hw 随 LEO 前进（管理面不会停在旧值）。"""
    store = InMemoryMessageStore()
    coordinator = ReplicationCoordinator(config=_config(enabled=False, nodes=()), store=store)
    writer = ReplicatedMessageStore(store, replication=coordinator)
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None and snapshot.hw == 0
    await _append(writer, seq=1)
    snapshot = await coordinator.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert snapshot is not None and snapshot.hw == snapshot.leo == 1


def test_config_rejects_non_positive_and_negative() -> None:
    """配置校验：数值<=0 与 ISR 阈值为负均拒（防「没有判据」的静默退化）。"""
    with pytest.raises(ValueError, match="必须为正"):
        replace(_config(), fetch_batch=0).validate()
    with pytest.raises(ValueError, match="不得为负"):
        replace(_config(), lag_records_max=-1).validate()


async def test_concurrent_attach_returns_single_state() -> None:
    """并发挂载同一分区：锁内二次检查命中，两个调用拿到同一实例。"""

    class SlowTopology(_Topology):
        async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
            await asyncio.sleep(0.01)
            return await super().load(topic_id=topic_id, partition_no=partition_no, force=force)

    coordinator = ReplicationCoordinator(
        config=_config(),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", SlowTopology(nodes=("n1", "n2"), min_insync=1)),
    )
    first, second = await asyncio.gather(
        coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO),
        coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO),
    )
    assert first is second


async def test_on_append_on_follower_only_updates_metrics() -> None:
    """follower 侧 on_append：只前进本地 LEO，不推进 HW（本节点无提交权）。"""
    cluster = _Cluster()
    state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.on_append(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0)
    assert state.local_leo == 1
    assert state.hw == 0


async def test_wait_committed_fails_when_isr_shrinks_during_wait() -> None:
    """提交等待期间 ISR 收缩到不足：立即返回 350032（reason=insufficient_isr）。"""
    cluster = _Cluster(commit_timeout_ms=5_000)
    state = await cluster.leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append(cluster.writer, seq=1)
    task = asyncio.create_task(cluster.leader.wait_committed(topic_id=TOPIC_ID, partition_no=PARTITION_NO, offset=0))
    await asyncio.sleep(0.01)
    state.isr.discard("n2")
    async with state.condition:
        state.condition.notify_all()
    with pytest.raises(MsgInsufficientReplicasError) as captured:
        await asyncio.wait_for(task, timeout=1)
    assert _error_data(captured.value)["reason"] == "insufficient_isr"


async def test_pull_loop_skips_leader_and_pops_backoff_on_success() -> None:
    """后台拉取循环：leader 分区跳过；follower 成功拉取后清掉退避标记。"""
    transport = InProcessReplicaTransport()
    leader_store = InMemoryMessageStore()
    follower_store = InMemoryMessageStore()
    topology = _Topology(nodes=("n1", "n2"), min_insync=2)
    base = replace(_config(node_id="n1"), fetch_interval_ms=1, scan_interval_ms=1)
    leader = ReplicationCoordinator(
        config=base,
        store=leader_store,
        transport=transport,
        topology_loader=cast("ReplicaTopologyLoader", topology),
    )
    follower = ReplicationCoordinator(
        config=replace(base, node_id="n2"),
        store=follower_store,
        transport=transport,
        topology_loader=cast("ReplicaTopologyLoader", topology),
    )
    transport.register("n1", leader)
    transport.register("n2", follower)
    writer = ReplicatedMessageStore(leader_store, replication=leader)
    await leader.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    follower_state = await follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    await _append(writer, seq=1)
    await leader.ensure_started()
    await follower.ensure_started()
    await asyncio.sleep(0.05)
    await follower.aclose()
    await leader.aclose()
    assert follower_state.local_leo == 1


async def test_promotion_by_refresh_rebuilds_follower_progress() -> None:
    """刷新升主：按副本集重建 follower 进度表，旧 leader 的拉取不得被拒（02-8 真机缺陷回归）。

    构造期只登记「非 leader」副本，升主后原 leader 不在表里；若不重建，`serve_fetch` 会
    永久 503「follower 不在副本集内」，ISR 卡死、`acks=all` 提交超时。
    """

    class _MutableTopology(_Topology):
        def __init__(self, *, nodes: tuple[str, ...]) -> None:
            super().__init__(nodes=nodes, min_insync=2)
            self.leader = nodes[0]

        async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
            del force
            return PartitionTopology(
                replicas=ConcurrentStableFrozenList(self.nodes),
                min_insync_replicas=2,
                epoch=1,
                leader=self.leader,
            )

    topology = _MutableTopology(nodes=("n1", "n2"))
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n2"),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", topology),
    )
    state = await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.role.name == "FOLLOWER"

    topology.leader = "n2"
    await coordinator.refresh_once()

    assert state.role.name == "LEADER"
    result = await coordinator.serve_fetch(
        ReplicaFetchRequest(
            leader="n2",
            follower="n1",
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_offset=0,
            max_messages=10,
            epoch=1,
        )
    )
    assert result.leader_epoch == 1
    assert "n1" in state.followers


async def test_refresh_once_timeout_keeps_loop_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    """网卡断连恢复后 DB 拉取卡死：单分区超时被放弃，下一轮照常续约（02-8 真机缺陷回归）。

    实测现象：leader 断网恢复后 `_refresh_loop` 卡死在一次无上限的 DB 拉取上，
    `leadership_confirmed_at` 7 分钟不更新，写请求永久 `350033 lease_expired`，
    只能重启进程恢复。这里用「首次永久挂起、后续正常」的加载器复现并验证自愈。
    """

    class _StuckFirst(_Topology):
        def __init__(self, *, nodes: tuple[str, ...]) -> None:
            super().__init__(nodes=nodes, min_insync=2, epoch=1)
            self.calls = 0

        async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
            self.calls += 1
            if self.calls == 2:
                await asyncio.Event().wait()
            return await super().load(topic_id=topic_id, partition_no=partition_no, force=force)

    monkeypatch.setattr("bms_msg.broker.replication._REFRESH_LOAD_TIMEOUT_SEC", 0.05)
    topology = _StuckFirst(nodes=("n1", "n2"))
    clock = _Clock()
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n1"),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", topology),
        clock=clock,
    )
    state = await coordinator.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    confirmed = state.leadership_confirmed_at

    await coordinator.refresh_once()
    assert topology.calls == 2
    assert state.leadership_confirmed_at == confirmed

    clock.advance(1_000)
    await coordinator.refresh_once()
    assert topology.calls == 3
    assert state.leadership_confirmed_at == confirmed + 1_000
