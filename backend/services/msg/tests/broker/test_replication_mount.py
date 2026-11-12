# pyright: reportPrivateUsage=false
"""拉取态初始化（msg 任务 02-10；Kiwi 2489）。

覆盖详细设计 §10 的 G1 ~ G8：

- G1 配置：4 个新键缺省值、非法值拒启、空闲间隔与拉取周期 / ISR 时间阈值的组合约束、环境变量读取；
- G2 枚举与成员判定：成员挂载并立即首拉、非成员跳过、库内 `replicas` 为空走轮转推导、幂等、批量与待挂剩余；
- G3 空闲收敛：空分区不写入也维持 ISR 全量、收缩计数不增长；
- G4 视图与选举可见：空拉后最后已知视图就绪，自动选举看护循环能看到空闲分区并读到元数据权威；
- G5 空闲降频：连续空拉后到期时间切到空闲间隔，拉到记录立即回落；
- G6 失败隔离：枚举失败本轮返回 0、单分区挂载失败不阻塞其它分区、非成员不拉取；
- G7 并发：单轮拉取并发不超过 `pull_concurrency`；
- G8 单实例零变化：复制关闭时不枚举、不建后台任务。
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList

from bms_msg.broker import replication as replication_module
from bms_msg.broker.auto_election import AutoElectionConfig, AutoElectionWatchdog
from bms_msg.broker.election import LeaderControlTransport, LeaderSwitchService, LeaderSwitchStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.replication import (
    InProcessReplicaTransport,
    PartitionAssignment,
    PartitionTopology,
    ReplicaConfig,
    ReplicaDirectoryBinder,
    ReplicaFetchRequest,
    ReplicaFetchResult,
    ReplicaMountSource,
    ReplicatedMessageStore,
    ReplicationCoordinator,
    ReplicationMetrics,
    ReplicationSnapshot,
    ReplicaTopologyLoader,
    ReplicaTransport,
    ReplicaUnavailableError,
)
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.trace import EMPTY_TRACE

pytestmark = pytest.mark.kiwi_id(2489)

TOPIC_ID = 9101
PARTITION_NO = 0
NODES = ("n1", "n2")
KEY = (TOPIC_ID, PARTITION_NO)


class _Clock:
    """可推进的单调毫秒时钟。"""

    def __init__(self) -> None:
        """从 0 起（挂载与首拉同一时刻，便于断言到期时间）。"""
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


def _config(
    *,
    node_id: str = "n1",
    nodes: Iterable[str] = NODES,
    enabled: bool = True,
    factor: int = 2,
    min_insync: int = 2,
    lag_time_ms: int = 10_000,
    fetch_ms: int = 200,
    idle_ms: int = 1_000,
    sweep_ms: int = 30_000,
    batch: int = 64,
    concurrency: int = 8,
) -> ReplicaConfig:
    """构造复制配置（缺省两节点全量副本）。

    Args:
        node_id: 本节点标识。
        nodes: 集群节点。
        enabled: 是否开启复制。
        factor: 副本数。
        min_insync: 最小同步副本数。
        lag_time_ms: ISR 时间阈值。
        fetch_ms: 拉取周期。
        idle_ms: 空闲拉取间隔。
        sweep_ms: 挂载复查周期。
        batch: 单轮挂载上限。
        concurrency: 单轮拉取并发。

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
        lag_records_max=10_000,
        fetch_interval_ms=fetch_ms,
        fetch_batch=100,
        commit_timeout_ms=100,
        scan_interval_ms=1_000,
        topology_ttl_ms=1_000,
        mount_sweep_ms=sweep_ms,
        mount_batch=batch,
        idle_fetch_interval_ms=idle_ms,
        pull_concurrency=concurrency,
    )


class _Topology:
    """固定拓扑加载器（显式副本集，避免依赖轮转推导）。"""

    def __init__(self, *, nodes: Iterable[str] = NODES, min_insync: int = 2) -> None:
        """初始化。

        Args:
            nodes: 副本集。
            min_insync: 最小同步副本数。
        """
        self.nodes = ConcurrentStableFrozenList(nodes)
        self.min_insync = min_insync

    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology:
        """返回固定拓扑。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过缓存（本替身忽略）。

        Returns:
            PartitionTopology: 固定副本集。
        """
        del topic_id, partition_no, force
        return PartitionTopology(
            replicas=self.nodes,
            min_insync_replicas=self.min_insync,
        )


class _Source(ReplicaMountSource):
    """分配枚举替身：固定清单 / 抛错 / 调用计数。"""

    def __init__(self, assignments: Iterable[PartitionAssignment] = (), *, error: Exception | None = None) -> None:
        """初始化。

        Args:
            assignments: 分配事实。
            error: 枚举时抛出的异常。
        """
        self.assignments = ConcurrentStableList(assignments)
        self.error = error
        self.calls = 0

    async def list_assignments(self) -> ConcurrentStableList[PartitionAssignment]:
        """返回固定分配清单。

        Returns:
            ConcurrentStableList[PartitionAssignment]: 分配事实。

        Raises:
            Exception: 配置的异常。
        """
        self.calls += 1
        if self.error is not None:
            raise self.error
        return ConcurrentStableList(self.assignments)


class _ProbeTransport(ReplicaTransport):
    """包一层传输：统计调用次数与并发峰值。"""

    def __init__(self, inner: ReplicaTransport) -> None:
        """初始化。

        Args:
            inner: 真实传输（进程内实现）。
        """
        self._inner = inner
        self.calls = 0
        self.current = 0
        self.peak = 0

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """转发一次拉取并记录并发。

        Args:
            request: 拉取请求。

        Returns:
            ReplicaFetchResult: 拉取结果。
        """
        self.calls += 1
        self.current += 1
        self.peak = max(self.peak, self.current)
        try:
            await asyncio.sleep(0)
            return await self._inner.fetch(request)
        finally:
            self.current -= 1


class _FlakyStore(InMemoryMessageStore):
    """指定分区取 LEO 即抛错的存储（模拟单分区挂载失败）。"""

    def __init__(self, *, bad: Iterable[tuple[int, int]]) -> None:
        """初始化。

        Args:
            bad: 会抛错的分区键。
        """
        super().__init__()
        self._bad = ConcurrentStableList(bad)

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取 LEO；命中 `bad` 即抛错。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO。

        Raises:
            RuntimeError: 该分区被标记为不可读。
        """
        if (topic_id, partition_no) in self._bad:
            raise RuntimeError("store down")
        return await super().leo(topic_id=topic_id, partition_no=partition_no)


class _SlowSource(ReplicaMountSource):
    """慢枚举替身：远超注入的超时上限，用于覆盖超时分支。"""

    async def list_assignments(self) -> ConcurrentStableList[PartitionAssignment]:
        """故意慢返回。

        Returns:
            ConcurrentStableList[PartitionAssignment]: 空清单（实际先超时）。
        """
        await asyncio.sleep(0.05)
        return ConcurrentStableList()


class _FailingTransport(ReplicaTransport):
    """拉取即失败的传输替身（覆盖首拉失败分支）。"""

    def __init__(self) -> None:
        """初始化（调用计数从 0 起）。"""
        self.calls = 0

    async def fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        """抛不可用错误。

        Args:
            request: 拉取请求。

        Raises:
            ReplicaUnavailableError: 恒抛（模拟 leader 不可达）。
        """
        del request
        self.calls += 1
        raise ReplicaUnavailableError("leader down")


class _Binder(ReplicaDirectoryBinder):
    """主题目录登记替身：记录每次登记的三段名。"""

    def __init__(self) -> None:
        """初始化（登记记录为空）。"""
        self.calls: ConcurrentStableList[tuple[int, str, str, int]] = ConcurrentStableList()

    def bind(self, *, tenant_id: int, namespace: str, name: str, topic_id: int) -> bool:
        """记录一次登记。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。
            topic_id: 主题主键。

        Returns:
            bool: 恒 False（无冲突）。
        """
        self.calls.add((int(tenant_id), namespace, name, int(topic_id)))
        return False


class _Cluster:
    """两节点进程内复制簇（可注入分配枚举与传输包装）。"""

    def __init__(
        self,
        *,
        config: ReplicaConfig | None = None,
        source: ReplicaMountSource | None = None,
        follower_store: InMemoryMessageStore | None = None,
        clock: _Clock | None = None,
        probe: bool = False,
        topology_nodes: Iterable[str] | None = None,
        directory: ReplicaDirectoryBinder | None = None,
    ) -> None:
        """初始化（把 leader / follower 与传输、拓扑串起来）。

        Args:
            config: leader 配置（follower 用同一配置换 `node_id`）。
            source: 双方共用的分配枚举源。
            follower_store: follower 存储（缺省内存实现）。
            clock: 单调毫秒时钟。
            probe: 是否包装传输以统计并发。
            topology_nodes: 拓扑加载器给出的副本集（缺省两节点全量）。
            directory: 主题目录登记器（缺省不装配）。
        """
        base = _config() if config is None else config
        self.clock = clock if clock is not None else _Clock()
        self.raw_transport = InProcessReplicaTransport()
        self.transport: ReplicaTransport = _ProbeTransport(self.raw_transport) if probe else self.raw_transport
        self.leader_store = InMemoryMessageStore()
        self.follower_store = follower_store if follower_store is not None else InMemoryMessageStore()
        self.metrics = ReplicationMetrics()
        topology = _Topology(nodes=NODES if topology_nodes is None else topology_nodes)
        self.leader = ReplicationCoordinator(
            config=base,
            store=self.leader_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", topology),
            mount_source=source,
            directory=directory,
            waiter=DeliveryWaiter(),
            metrics=self.metrics,
            clock=self.clock,
        )
        self.follower = ReplicationCoordinator(
            config=replace(base, node_id="n2"),
            store=self.follower_store,
            transport=self.transport,
            topology_loader=cast("ReplicaTopologyLoader", topology),
            mount_source=source,
            directory=directory,
            metrics=ReplicationMetrics(),
            clock=self.clock,
        )
        self.raw_transport.register("n1", self.leader)
        self.raw_transport.register("n2", self.follower)
        self.writer = ReplicatedMessageStore(self.leader_store, replication=self.leader)

    @property
    def probe(self) -> _ProbeTransport:
        """取并发统计包装（未开启 `probe` 时断言会失败，属用例错误）。

        Returns:
            _ProbeTransport: 传输包装。
        """
        return cast("_ProbeTransport", self.transport)


def _assignment(
    *,
    topic_id: int = TOPIC_ID,
    partition_no: int = PARTITION_NO,
    replicas: Iterable[str] = NODES,
    tenant_id: int = 0,
    namespace: str = "",
    name: str = "",
) -> PartitionAssignment:
    """构造一条分配事实。

    Args:
        topic_id: 主题主键。
        partition_no: 分区号。
        replicas: 库内副本集（空表示按轮转推导）。
        tenant_id: 主题租户主键（0 表示未知身份）。
        namespace: 主题命名空间（空表示未知身份）。
        name: 主题名（空表示未知身份）。

    Returns:
        PartitionAssignment: 分配事实。
    """
    return PartitionAssignment(
        topic_id=topic_id,
        partition_no=partition_no,
        replicas=ConcurrentStableFrozenList(replicas),
        tenant_id=tenant_id,
        namespace=namespace,
        name=name,
    )


async def _append(writer: ReplicatedMessageStore, *, seq: int) -> None:
    """经复制包装写入一条记录（leader 本地追加并推进 LEO）。

    Args:
        writer: 复制包装后的存储。
        seq: 序号。
    """
    await writer.append(
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        message_id=f"m-{seq}",
        log_id=f"l-{seq}",
        key=None,
        payload=f"p-{seq}".encode(),
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# G1 配置
# ---------------------------------------------------------------------------


def test_mount_config_defaults() -> None:
    """缺省值：复查 30s / 单批 64 / 空闲 1s / 并发 8，且缺省组合能通过校验。"""
    config = ReplicaConfig(enabled=False)
    assert config.mount_sweep_ms == 30_000
    assert config.mount_batch == 64
    assert config.idle_fetch_interval_ms == 1_000
    assert config.pull_concurrency == 8
    config.validate()


def test_mount_config_rejects_bad_values() -> None:
    """非法值：非正数、空闲间隔比正常间隔更密、空闲间隔逼近 ISR 阈值均拒启。"""
    with pytest.raises(ValueError):
        replace(_config(), mount_batch=0).validate()
    with pytest.raises(ValueError):
        replace(_config(), pull_concurrency=0).validate()
    with pytest.raises(ValueError):
        replace(_config(), mount_sweep_ms=0).validate()
    with pytest.raises(ValueError):
        replace(_config(fetch_ms=1_000, idle_ms=500)).validate()
    with pytest.raises(ValueError):
        replace(_config(lag_time_ms=1_000, idle_ms=600)).validate()
    # 边界：2 × 空闲间隔恰等于 ISR 阈值仍合法（缺省 1s × 2 ≤ 10s）。
    replace(_config(lag_time_ms=2_000, idle_ms=1_000)).validate()


def test_mount_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量：4 个新键被读取并进入配置。"""
    for name, value in (
        ("MSG_REPLICATION_MOUNT_SWEEP_MS", "5000"),
        ("MSG_REPLICATION_MOUNT_BATCH", "16"),
        ("MSG_REPLICATION_IDLE_FETCH_INTERVAL_MS", "800"),
        ("MSG_REPLICATION_PULL_CONCURRENCY", "4"),
    ):
        monkeypatch.setenv(name, value)
    config = ReplicaConfig.from_env()
    assert config.mount_sweep_ms == 5_000
    assert config.mount_batch == 16
    assert config.idle_fetch_interval_ms == 800
    assert config.pull_concurrency == 4


# ---------------------------------------------------------------------------
# G2 枚举与成员判定
# ---------------------------------------------------------------------------


async def test_mount_round_mounts_member_and_triggers_first_pull() -> None:
    """成员分区：挂载成功并立即首拉一次（空批也算心跳）。"""
    cluster = _Cluster(config=_config(), source=_Source((_assignment(),)), probe=True)
    assert await cluster.leader.mount_once() == 1
    assert await cluster.follower.mount_once() == 1
    # 只有 follower 首拉（leader 是 leader 角色，不做 follower 拉取）。
    assert cluster.probe.calls == 1
    assert cluster.metrics.mounted_partitions == 1
    view = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert view is not None
    assert view.leader == "n1"


async def test_mount_round_skips_non_member() -> None:
    """非副本集成员：本轮不挂载、不计入已挂载数。"""
    coordinator = ReplicationCoordinator(
        config=_config(node_id="n3", nodes=("n1", "n2", "n3")),
        store=InMemoryMessageStore(),
        topology_loader=cast("ReplicaTopologyLoader", _Topology()),
        mount_source=_Source((_assignment(),)),
        clock=_Clock(),
    )
    assert await coordinator.mount_once() == 0
    assert list(coordinator.mounted_partition_keys()) == []


async def test_mount_round_derives_replicas_when_rows_empty() -> None:
    """库内 `replicas` 为空：按与协调器同源的轮转算法判定成员。"""
    # factor=1、nodes=(n1,n2)、(topic=2, partition=0) → (2*31+0) % 2 == 0 → n1。
    cluster = _Cluster(
        config=_config(factor=1, min_insync=1),
        source=_Source((_assignment(topic_id=2, partition_no=0, replicas=()),)),
    )
    assert await cluster.leader.mount_once() == 1
    assert await cluster.follower.mount_once() == 0


async def test_mount_binds_topic_directory_before_attach() -> None:
    """挂载前登记主题目录：带身份的分配会登记三段名，缺身份则不登记（存储未登记会抛错）。"""
    binder = _Binder()
    cluster = _Cluster(
        config=_config(),
        source=_Source(
            (
                _assignment(tenant_id=7001, namespace="msg", name="drill.replica"),
                _assignment(partition_no=1),
            )
        ),
        directory=binder,
    )
    assert await cluster.leader.mount_once() == 2
    assert list(binder.calls) == [(7001, "msg", "drill.replica", TOPIC_ID)]


async def test_mount_round_is_idempotent() -> None:
    """重复挂载：已挂载分区不再计数，也不重复首拉。"""
    cluster = _Cluster(config=_config(), source=_Source((_assignment(),)), probe=True)
    assert await cluster.leader.mount_once() == 1
    assert await cluster.leader.mount_once() == 0
    assert cluster.probe.calls == 0
    assert cluster.metrics.mounted_partitions == 1


async def test_mount_round_respects_batch_and_reports_backlog() -> None:
    """单轮上限：超出留到下一轮，待挂剩余记入 backlog。"""
    assignments = ConcurrentStableList(_assignment(partition_no=index) for index in range(3))
    cluster = _Cluster(config=_config(batch=1), source=_Source(assignments))
    assert await cluster.leader.mount_once() == 1
    assert cluster.leader._mount_backlog == 2
    assert await cluster.leader.mount_once() == 1
    assert cluster.leader._mount_backlog == 1
    assert await cluster.leader.mount_once() == 1
    assert cluster.leader._mount_backlog == 0


async def test_mount_loop_runs_first_round_without_waiting() -> None:
    """启动即首轮：`ensure_started` 后无需等待复查周期就已挂载（并回收任务）。"""
    cluster = _Cluster(
        config=_config(sweep_ms=60_000),
        source=_Source((_assignment(),)),
        clock=_Clock(),
    )
    await cluster.leader.ensure_started()
    await asyncio.sleep(0.01)
    assert cluster.metrics.mounted_partitions == 1
    await cluster.leader.aclose()


# ---------------------------------------------------------------------------
# G3 空闲收敛
# ---------------------------------------------------------------------------


async def test_idle_partition_keeps_isr_without_writes() -> None:
    """空分区不写入：按拍空拉即可维持 ISR 全量，收缩计数不增长。"""
    clock = _Clock()
    cluster = _Cluster(config=_config(), source=_Source((_assignment(),)), clock=clock)
    await cluster.leader.mount_once()
    await cluster.follower.mount_once()
    for _ in range(3):
        clock.advance(4_000)
        assert await cluster.follower.pull_round() == 0
        await cluster.leader.scan_isr_once()
        snapshot = await cluster.leader.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
        assert snapshot is not None
        assert set(snapshot.isr) == {"n1", "n2"}
    assert cluster.metrics.shrink_total == 0
    assert cluster.metrics.snapshot().rows[0].isr_size == 2


# ---------------------------------------------------------------------------
# G4 视图与选举可见
# ---------------------------------------------------------------------------


class _Control:
    """节点控制替身：只实现探活并记录探过谁（一律不可达）。"""

    def __init__(self) -> None:
        """初始化（无参数：本替身一律返回不可达）。"""
        self.probes: ConcurrentStableList[str] = ConcurrentStableList()

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """探活一个节点。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 一律 None（不可达）。
        """
        del topic_id, partition_no
        self.probes.add(node_id)
        return None


class _Meta:
    """切换元数据替身：只记录读次数，分区行恒为缺失。"""

    def __init__(self) -> None:
        """初始化（读次数从 0 起）。"""
        self.calls = 0

    async def load_partition(self, *, topic_id: int, partition_no: int) -> object | None:
        """读分区元数据（恒返回 None：让流程停在「无权威不动作」）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            object | None: 恒 None。
        """
        del topic_id, partition_no
        self.calls += 1
        return None


class _Switch:
    """切换服务替身（不该被调用：元数据缺失时流程已返回）。"""

    async def switch_partition(self, request: object) -> object:
        """被调用即判失败。

        Args:
            request: 切换请求。

        Raises:
            AssertionError: 元数据缺失时不应发起切主。
        """
        raise AssertionError(f"元数据缺失时不应发起切主：{request!r}")


async def test_idle_partition_provides_view_and_reaches_election() -> None:
    """空闲分区：空拉后最后已知视图就绪，看护循环能看到并读到元数据权威。"""
    clock = _Clock()
    cluster = _Cluster(config=_config(), source=_Source((_assignment(),)), clock=clock)
    await cluster.leader.mount_once()
    await cluster.follower.mount_once()
    view = await cluster.follower.snapshot(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert view is not None
    assert set(view.last_known_isr) == {"n1", "n2"}
    assert view.last_known_at == clock()

    control = _Control()
    meta = _Meta()
    watchdog = AutoElectionWatchdog(
        config=AutoElectionConfig(enabled=True, failure_threshold=1),
        node_id="n2",
        replication=cluster.follower,
        store=cast("LeaderSwitchStore", meta),
        control=cast("LeaderControlTransport", control),
        switch=cast("LeaderSwitchService", _Switch()),
        clock=clock,
    )
    await watchdog.run_once()
    assert list(control.probes) == ["n1"]
    assert meta.calls == 1


# ---------------------------------------------------------------------------
# G5 空闲降频
# ---------------------------------------------------------------------------


async def test_idle_fetch_backoff_and_recovery() -> None:
    """空闲降频：连续空拉后到期切到空闲间隔；拉到记录立即回落。"""
    clock = _Clock()
    cluster = _Cluster(config=_config(), source=_Source((_assignment(),)), clock=clock)
    await cluster.leader.mount_once()
    await cluster.follower.mount_once()
    # 挂载已做一次空拉（streak=1，到期 = 200ms）。
    assert cluster.follower._idle_streak[KEY] == 1
    for _ in range(4):
        clock.advance(200)
        assert await cluster.follower.pull_round() == 0
    assert cluster.follower._idle_streak[KEY] == 5
    assert cluster.follower._fetch_due_at[KEY] - clock() == 1_000

    await _append(cluster.writer, seq=1)
    clock.advance(1_000)
    assert await cluster.follower.pull_round() == 1
    assert KEY not in cluster.follower._idle_streak
    assert cluster.follower._fetch_due_at[KEY] - clock() == 200


# ---------------------------------------------------------------------------
# G6 失败隔离
# ---------------------------------------------------------------------------


async def test_mount_round_survives_enumeration_failure() -> None:
    """枚举失败：本轮返回 0，恢复后下一轮照常挂载。"""
    source = _Source((_assignment(),), error=RuntimeError("db down"))
    cluster = _Cluster(config=_config(), source=source)
    assert await cluster.leader.mount_once() == 0
    source.error = None
    assert await cluster.leader.mount_once() == 1


async def test_mount_round_isolates_single_partition_failure() -> None:
    """单分区挂载失败：跳过该分区，其余分区照常挂载。"""
    cluster = _Cluster(
        config=_config(),
        source=_Source((_assignment(partition_no=0), _assignment(partition_no=1))),
        follower_store=_FlakyStore(bad=((TOPIC_ID, 1),)),
    )
    assert await cluster.leader.mount_once() == 2
    assert await cluster.follower.mount_once() == 1
    assert list(cluster.follower.mounted_partition_keys()) == [KEY]


async def test_non_hosted_follower_does_not_pull() -> None:
    """非承载节点：即使被请求碰到（惰性挂载），也不发起拉取。"""
    cluster = _Cluster(
        config=_config(),
        source=_Source((_assignment(replicas=("n1",)),)),
        probe=True,
        topology_nodes=("n1",),
    )
    state = await cluster.follower.attach_partition(topic_id=TOPIC_ID, partition_no=PARTITION_NO)
    assert state.hosted is False
    assert await cluster.follower.pull_once(topic_id=TOPIC_ID, partition_no=PARTITION_NO) == 0
    assert await cluster.follower.pull_round() == 0
    assert cluster.probe.calls == 0


async def test_mount_round_times_out_on_slow_enumeration(monkeypatch: pytest.MonkeyPatch) -> None:
    """枚举超时：本轮返回 0（不阻塞循环，下一轮重试）。"""
    monkeypatch.setattr(replication_module, "_MOUNT_LOAD_TIMEOUT_SEC", 0.001)
    cluster = _Cluster(config=_config(), source=_SlowSource())
    assert await cluster.leader.mount_once() == 0
    assert list(cluster.leader.mounted_partition_keys()) == []


async def test_mount_succeeds_even_when_first_pull_fails() -> None:
    """首拉失败：挂载结果保留（交给既有退避与周期拉取重试）。"""
    failing = _FailingTransport()
    coordinator = ReplicationCoordinator(
        config=replace(_config(), node_id="n2"),
        store=InMemoryMessageStore(),
        transport=failing,
        topology_loader=cast("ReplicaTopologyLoader", _Topology()),
        mount_source=_Source((_assignment(),)),
        clock=_Clock(),
    )
    assert await coordinator.mount_once() == 1
    assert list(coordinator.mounted_partition_keys()) == [KEY]
    assert failing.calls == 1
    assert coordinator._fetch_backoff  # 已按退避登记，等待下一轮


# ---------------------------------------------------------------------------
# G7 并发
# ---------------------------------------------------------------------------


async def test_pull_round_is_bounded_by_concurrency() -> None:
    """单轮拉取：并发不超过 `pull_concurrency`（4 个分区 / 并发 2 → 峰值 2）。"""
    clock = _Clock()
    assignments = ConcurrentStableList(_assignment(partition_no=index) for index in range(4))
    cluster = _Cluster(config=_config(concurrency=2), source=_Source(assignments), clock=clock, probe=True)
    await cluster.follower.mount_once()
    cluster.probe.peak = 0
    cluster.probe.calls = 0
    clock.advance(200)
    await cluster.follower.pull_round()
    assert cluster.probe.calls == 4
    assert cluster.probe.peak == 2


# ---------------------------------------------------------------------------
# G8 单实例零变化
# ---------------------------------------------------------------------------


async def test_disabled_replication_does_not_mount_or_start() -> None:
    """复制关闭：不枚举分配、不建后台任务。"""
    source = _Source((_assignment(),))
    coordinator = ReplicationCoordinator(
        config=_config(enabled=False, nodes=()),
        store=InMemoryMessageStore(),
        mount_source=source,
    )
    assert await coordinator.mount_once() == 0
    assert source.calls == 0
    await coordinator.ensure_started()
    assert list(coordinator._tasks) == []
