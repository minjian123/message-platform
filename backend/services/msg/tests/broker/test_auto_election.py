# pyright: reportPrivateUsage=false
"""自动选举判活与抢主（msg 任务 02-9；Kiwi 2488）。

覆盖详细设计 §12.2 的 G1 ~ G4：

- 判活：单次失败不触发、连续失败达阈值触发、成功清零、超时与异常均记失败、视图 / 行缺失清跟踪；
- 门槛：本节点是 leader / 不在副本集 / 人工切换中 / 本地与 DB 冷却 / 尝试上限；
- 错峰：死 leader 后继副本序，前序可达即本节点不发；
- 参数：`target=None` / `force=False` / `expected_epoch` 取读库值 / `operator` / `reason`；
- 结果：`applied=True` 记成功并冷却、no-op 与 CAS 落败记冲突、预检拒绝与激活失败退避。
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.core.resources import ResourceManager
from fastapi import FastAPI

from bms_msg.broker.auto_election import (
    AUTO_ELECTION_OPERATOR,
    AUTO_ELECTION_RESULT_BLOCKED,
    AUTO_ELECTION_RESULT_CONFLICT,
    AUTO_ELECTION_RESULT_EXHAUSTED,
    AUTO_ELECTION_RESULT_SUCCEEDED,
    AUTO_ELECTION_RESULT_TRIGGERED,
    AutoElectionConfig,
    AutoElectionMetrics,
    AutoElectionWatchdog,
    _successor_order,
)
from bms_msg.broker.election import (
    SWITCH_STATE_IDLE,
    SWITCH_STATE_SWITCHING,
    LeaderControlTransport,
    LeaderSwitchError,
    LeaderSwitchService,
    LeaderSwitchStore,
    SwitchApplyCommand,
    SwitchApplyResult,
    SwitchPartitionRequest,
    SwitchPartitionState,
    SwitchResult,
)
from bms_msg.broker.lifecycle import AutoElectionCloser, start_background_tasks
from bms_msg.broker.replication import ReplicaKey, ReplicationCoordinator, ReplicationSnapshot

pytestmark = pytest.mark.kiwi_id(2488)

TOPIC_ID = 8801
PARTITION_NO = 0
KEY: ReplicaKey = (TOPIC_ID, PARTITION_NO)
NODES = ("a", "b", "c")
WALL_NOW = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)


class _Clock:
    """可推进的单调毫秒时钟。"""

    def __init__(self) -> None:
        """初始化（从 1000 起，避免与「未设退避」的 0 混淆）。"""
        self.value = 1_000.0

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
    enabled: bool = True,
    detect_interval_ms: int = 1_000,
    probe_timeout_ms: int = 800,
    max_attempts: int = 5,
    backoff_base_ms: int = 2_000,
    backoff_cap_ms: int = 60_000,
) -> AutoElectionConfig:
    """构造测试配置（其余字段与生产缺省一致）。

    Args:
        enabled: 总开关。
        detect_interval_ms: 判活节拍。
        probe_timeout_ms: 探活超时。
        max_attempts: 发起次数上限。
        backoff_base_ms: 退避基数。
        backoff_cap_ms: 退避上限。

    Returns:
        AutoElectionConfig: 配置。
    """
    return AutoElectionConfig(
        enabled=enabled,
        detect_interval_ms=detect_interval_ms,
        probe_timeout_ms=probe_timeout_ms,
        max_attempts=max_attempts,
        backoff_base_ms=backoff_base_ms,
        backoff_cap_ms=backoff_cap_ms,
    )


def _partition(
    *,
    leader: str = "b",
    epoch: int = 3,
    replicas: tuple[str, ...] = ("b", "a", "c"),
    switch_state: str = SWITCH_STATE_IDLE,
    last_switch_at: datetime | None = None,
) -> SwitchPartitionState:
    """构造分区元数据快照（缺省副本序让本节点 a 成为死 leader b 的直接后继）。

    Args:
        leader: 当前 leader。
        epoch: 当前任期。
        replicas: 副本集。
        switch_state: 切换状态。
        last_switch_at: 最近一次切换时刻。

    Returns:
        SwitchPartitionState: 分区快照。
    """
    return SwitchPartitionState(
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        tenant_id=9001,
        namespace="msg",
        topic="audit.auto",
        leader=leader,
        epoch=epoch,
        replicas=ConcurrentStableFrozenList(replicas),
        switch_state=switch_state,
        last_switch_at=last_switch_at,
    )


def _view(*, leader: str = "b") -> ReplicationSnapshot:
    """构造本地复制视图。

    Args:
        leader: 本地视图里的 leader。

    Returns:
        ReplicationSnapshot: 本地视图。
    """
    return ReplicationSnapshot(
        leader=leader,
        epoch=3,
        leo=10,
        hw=10,
        isr=ConcurrentStableFrozenList(NODES),
        replicas=ConcurrentStableFrozenList(NODES),
    )


class _Views:
    """复制协调器只读视图替身。"""

    def __init__(
        self,
        *,
        keys: tuple[ReplicaKey, ...] = (KEY,),
        view: ReplicationSnapshot | None = None,
        missing: bool = False,
        error: Exception | None = None,
    ) -> None:
        """初始化。

        Args:
            keys: 已挂载分区键。
            view: 本地视图（缺省为 leader=b）。
            missing: 本地视图缺失（模拟挂载失败）。
            error: `snapshot` 直接抛出的异常（覆盖整轮异常隔离）。
        """
        self._keys = keys
        self._view = _view() if view is None else view
        self._missing = missing
        self._error = error

    def mounted_partition_keys(self) -> ConcurrentStableFrozenList[ReplicaKey]:
        """返回已挂载分区键。

        Returns:
            ConcurrentStableFrozenList[ReplicaKey]: 分区键。
        """
        return ConcurrentStableFrozenList(self._keys)

    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """返回本地视图。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 本地视图（`missing` 时为 None）。

        Raises:
            Exception: 配置的异常。
        """
        del topic_id, partition_no
        if self._error is not None:
            raise self._error
        return None if self._missing else self._view


class _Control(LeaderControlTransport):
    """节点控制替身：按节点返回快照 / 抛错 / 延迟。"""

    def __init__(
        self,
        *,
        reachable: tuple[str, ...] = ("a", "c"),
        errors: tuple[str, ...] = (),
        slow: tuple[str, ...] = (),
        delay_sec: float = 0.05,
    ) -> None:
        """初始化。

        Args:
            reachable: 可达节点（缺省只有 a / c 可达，即 leader b 判死）。
            errors: 探活即抛错的节点。
            slow: 探活会超时的节点。
            delay_sec: 慢节点延迟（秒）。
        """
        self.reachable = reachable
        self.errors = errors
        self.slow = slow
        self.delay_sec = delay_sec
        self.probes: ConcurrentStableList[str] = ConcurrentStableList()

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """返回节点快照或按配置失败。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照（不可达为 None）。

        Raises:
            RuntimeError: 节点在 `errors` 内。
        """
        del topic_id, partition_no
        self.probes.add(node_id)
        if node_id in self.errors:
            raise RuntimeError("control down")
        if node_id in self.slow:
            await asyncio.sleep(self.delay_sec)
        return _view(leader=node_id) if node_id in self.reachable else None

    async def demote(
        self,
        *,
        node_id: str,
        topic_id: int,
        partition_no: int,
        to_leader: str,
        to_epoch: int,
    ) -> None:
        """不用（自动选举只探活，降级由切换服务调用）。"""
        raise AssertionError("自动选举不应直接 demote")

    async def apply_switch(self, *, node_id: str, command: SwitchApplyCommand) -> SwitchApplyResult:
        """不用（激活由切换服务调用）。"""
        raise AssertionError("自动选举不应直接 apply_switch")


class _Store(LeaderSwitchStore):
    """切换元数据存储替身（只实现读；写一律走切换服务）。"""

    def __init__(
        self,
        *,
        partition: SwitchPartitionState | None = None,
        present: bool = True,
        error: Exception | None = None,
    ) -> None:
        """初始化。

        Args:
            partition: 分区快照（缺省 leader=b）。
            present: 分区行是否存在。
            error: 读库异常。
        """
        self.partition = _partition() if partition is None else partition
        self.present = present
        self.error = error
        self.loads = 0

    async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
        """返回分区快照。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            SwitchPartitionState | None: 分区快照（`present=False` 时为 None）。

        Raises:
            Exception: 配置的读库异常。
        """
        del topic_id, partition_no
        self.loads += 1
        if self.error is not None:
            raise self.error
        return self.partition if self.present else None

    async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
        """不用（自动选举逐分区读）。"""
        raise AssertionError("自动选举不批量读主题")

    async def begin_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        expected_epoch: int,
        target: str,
        from_leader: str,
        from_epoch: int,
    ) -> bool:
        """不用（写路径走切换服务）。"""
        raise AssertionError("写路径走切换服务")

    async def finish_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        to_epoch: int,
        watermark: int | None,
        result: str,
        truncated_to: int | None,
        operator: str,
    ) -> bool:
        """不用（写路径走切换服务）。"""
        raise AssertionError("写路径走切换服务")


class _Switch:
    """切换服务替身：记录请求，返回预设结果或抛错。"""

    def __init__(self, *, applied: bool = True) -> None:
        """初始化。

        Args:
            applied: 成功结果的 `applied` 取值（False 模拟他人刚切完的 no-op）。
        """
        self.requests: ConcurrentStableList[SwitchPartitionRequest] = ConcurrentStableList()
        self.applied = applied
        self.error: LeaderSwitchError | None = None

    async def switch_partition(self, request: SwitchPartitionRequest) -> SwitchResult:
        """记录请求并返回结果。

        Args:
            request: 切换请求。

        Returns:
            SwitchResult: 预设结果。

        Raises:
            LeaderSwitchError: 配置的切换错误。
        """
        self.requests.add(request)
        if self.error is not None:
            raise self.error
        expected = int(request.expected_epoch or 0)
        return SwitchResult(
            topic_id=request.topic_id,
            partition_no=request.partition_no,
            from_leader="b",
            to_leader="a",
            from_epoch=expected,
            to_epoch=expected + 1,
            applied=self.applied,
            watermark=None,
            truncated_to=None,
            retained_tail=0,
            isr_verified=True,
            duration_ms=7,
        )


class _Ctx:
    """一次用例的看护循环与全部替身（具名访问，避免下标错位）。"""

    def __init__(
        self,
        *,
        config: AutoElectionConfig | None = None,
        views: _Views | None = None,
        store: _Store | None = None,
        control: _Control | None = None,
        switch: _Switch | None = None,
        metrics: AutoElectionMetrics | None = None,
    ) -> None:
        """装配看护循环与替身。

        Args:
            config: 配置（缺省开启）。
            views: 复制视图替身。
            store: 元数据存储替身。
            control: 控制传输替身。
            switch: 切换服务替身。
            metrics: 结果计数。
        """
        self.config = _config() if config is None else config
        self.views = _Views() if views is None else views
        self.store = _Store() if store is None else store
        self.control = _Control() if control is None else control
        self.switch = _Switch() if switch is None else switch
        self.metrics = AutoElectionMetrics() if metrics is None else metrics
        self.clock = _Clock()
        self.watchdog = AutoElectionWatchdog(
            config=self.config,
            node_id="a",
            replication=cast("ReplicationCoordinator", self.views),
            store=self.store,
            control=self.control,
            switch=cast("LeaderSwitchService", self.switch),
            metrics=self.metrics,
            clock=self.clock,
            wall_clock=lambda: WALL_NOW,
        )

    async def rounds(self, times: int) -> None:
        """连跑若干轮。

        Args:
            times: 轮数。
        """
        for _ in range(times):
            await self.watchdog.run_once()


async def test_disabled_round_is_noop() -> None:
    """开关关闭：整轮不探活、不读库、不发起。"""
    ctx = _Ctx(config=_config(enabled=False))
    await ctx.rounds(2)
    assert not ctx.control.probes
    assert ctx.store.loads == 0
    assert not ctx.switch.requests
    assert ctx.metrics.snapshot() == {}


async def test_round_without_mounted_partitions_is_noop() -> None:
    """没有已挂载分区：直接返回。"""
    ctx = _Ctx(views=_Views(keys=()))
    await ctx.rounds(1)
    assert not ctx.control.probes
    assert not ctx.switch.requests


async def test_below_threshold_does_not_claim() -> None:
    """连续失败 2 次（阈值 3）不发起，也不读库。"""
    ctx = _Ctx()
    await ctx.rounds(2)
    assert list(ctx.control.probes) == ["b", "b"]
    assert ctx.store.loads == 0
    assert not ctx.switch.requests
    assert ctx.metrics.snapshot() == {}


async def test_threshold_triggers_claim_with_expected_request() -> None:
    """第 3 次失败发起一次切换，参数按定稿取值。"""
    ctx = _Ctx()
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1
    request = ctx.switch.requests[0]
    assert request.target is None
    assert request.force is False
    assert request.expected_epoch == 3
    assert request.operator == AUTO_ELECTION_OPERATOR
    assert request.reason == "leader_unreachable:3"
    counts = ctx.metrics.snapshot()
    assert counts[AUTO_ELECTION_RESULT_TRIGGERED] == 1
    assert counts[AUTO_ELECTION_RESULT_SUCCEEDED] == 1


async def test_probe_success_resets_failures() -> None:
    """中途探活成功即清零，失败需重新累计三次。"""
    ctx = _Ctx()
    ctx.control.reachable = ()  # leader b 不可达
    await ctx.rounds(2)
    assert not ctx.switch.requests
    ctx.control.reachable = NODES  # 恢复 → 清零
    await ctx.rounds(1)
    ctx.control.reachable = ()
    await ctx.rounds(2)
    assert not ctx.switch.requests, "清零后只累计到 2 次"
    await ctx.rounds(1)
    assert len(ctx.switch.requests) == 1


async def test_leader_is_self_is_skipped() -> None:
    """本地视图显示自己是 leader：不探活自己。"""
    ctx = _Ctx(views=_Views(view=_view(leader="a")))
    await ctx.rounds(5)
    assert not ctx.control.probes
    assert not ctx.switch.requests


async def test_missing_view_is_forgotten() -> None:
    """本地视图缺失（挂载失败）：丢弃跟踪态。"""
    ctx = _Ctx(views=_Views(missing=True))
    ctx.watchdog._trackers.set(KEY, ctx.watchdog._tracker(KEY))
    await ctx.rounds(1)
    assert KEY not in ctx.watchdog._trackers
    assert not ctx.switch.requests


async def test_partition_row_missing_is_forgotten() -> None:
    """元数据行缺失：丢弃跟踪态。"""
    ctx = _Ctx(store=_Store(present=False))
    await ctx.rounds(3)
    assert KEY not in ctx.watchdog._trackers
    assert not ctx.switch.requests


async def test_metadata_error_keeps_counting() -> None:
    """读库异常：本轮不发起，计数保留。"""
    ctx = _Ctx(store=_Store(error=RuntimeError("db down")))
    await ctx.rounds(3)
    assert ctx.store.loads == 1
    assert not ctx.switch.requests


async def test_db_leader_changed_resets() -> None:
    """读库发现 leader 已换：清零不发起。"""
    ctx = _Ctx(store=_Store(partition=_partition(leader="c")))
    await ctx.rounds(3)
    assert not ctx.switch.requests


async def test_db_leader_is_self_resets() -> None:
    """读库发现自己是 leader（刷新未及）：清零不发起。"""
    ctx = _Ctx(store=_Store(partition=_partition(leader="a")))
    await ctx.rounds(3)
    assert not ctx.switch.requests


async def test_not_in_replicas_resets() -> None:
    """本节点不在副本集：清零不发起。"""
    ctx = _Ctx(store=_Store(partition=_partition(replicas=("b", "c"))))
    await ctx.rounds(3)
    assert not ctx.switch.requests


async def test_switching_defers_without_attempt() -> None:
    """人工切换中（switching 且非本进程发起）：让位、不计数。"""
    ctx = _Ctx(store=_Store(partition=_partition(switch_state=SWITCH_STATE_SWITCHING)))
    await ctx.rounds(3)
    assert not ctx.switch.requests
    assert ctx.metrics.snapshot() == {}


async def test_pending_epoch_allows_resume() -> None:
    """停在 switching 且 epoch 等于本进程记录的 pending_epoch：允许续跑。"""
    ctx = _Ctx(store=_Store(partition=_partition(switch_state=SWITCH_STATE_SWITCHING, epoch=4)))
    ctx.watchdog._tracker(KEY).pending_epoch = 4
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1
    assert ctx.switch.requests[0].expected_epoch == 4


async def test_local_cooldown_blocks_once() -> None:
    """成功后的本地冷却：再次判死只记一次 blocked，不重复发起。"""
    ctx = _Ctx()
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1
    await ctx.rounds(4)  # 失败重新累计到阈值后进冷却分支
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_BLOCKED] == 1
    assert len(ctx.switch.requests) == 1


async def test_db_cooldown_blocks_once() -> None:
    """按 DB `last_switch_at` 判冷却（跨节点）：阻住发起且只记一次。"""
    recent = datetime(2026, 11, 12, 11, 59, 30, tzinfo=UTC)  # 距墙钟 30s（冷却 60s）
    ctx = _Ctx(store=_Store(partition=_partition(last_switch_at=recent)))
    await ctx.rounds(3)
    await ctx.rounds(1)
    assert not ctx.switch.requests
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_BLOCKED] == 1


async def test_expired_db_cooldown_allows_claim() -> None:
    """`last_switch_at` 已超出冷却窗口：允许发起。"""
    stale = datetime(2026, 11, 12, 11, 50, tzinfo=UTC)
    ctx = _Ctx(store=_Store(partition=_partition(last_switch_at=stale)))
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1


async def test_naive_last_switch_at_is_treated_as_utc() -> None:
    """DB 读回的 naive 时间按 UTC 解释（不与带时区的墙钟比较报错）。"""
    naive = datetime(2026, 11, 12, 11, 59, 45)
    ctx = _Ctx(store=_Store(partition=_partition(last_switch_at=naive)))
    await ctx.rounds(3)
    assert not ctx.switch.requests


async def test_no_last_switch_at_skips_cooldown() -> None:
    """从未切换过（`last_switch_at` 为空）：不判冷却。"""
    ctx = _Ctx(store=_Store(partition=_partition(last_switch_at=None)))
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1


async def test_exhausted_after_max_attempts() -> None:
    """尝试次数达上限：记一次 exhausted 并进入冷却。"""
    switch = _Switch()
    switch.error = LeaderSwitchError("isr_unverified", "视图过期")
    ctx = _Ctx(switch=switch, config=_config(max_attempts=1))
    await ctx.rounds(3)
    assert len(switch.requests) == 1
    ctx.clock.advance(10_000)
    await ctx.rounds(1)
    await ctx.rounds(1)
    counts = ctx.metrics.snapshot()
    assert counts[AUTO_ELECTION_RESULT_EXHAUSTED] == 1
    assert counts[AUTO_ELECTION_RESULT_BLOCKED] == 1


async def test_noop_result_counts_conflict() -> None:
    """切换服务返回 applied=False（他人刚切完）：记 conflict。"""
    ctx = _Ctx(switch=_Switch(applied=False))
    await ctx.rounds(3)
    counts = ctx.metrics.snapshot()
    assert counts[AUTO_ELECTION_RESULT_CONFLICT] == 1
    assert AUTO_ELECTION_RESULT_SUCCEEDED not in counts


async def test_conflict_error_resets_without_backoff() -> None:
    """CAS 落败（switch_conflict）：记 conflict、清零且不设退避。"""
    switch = _Switch()
    switch.error = LeaderSwitchError("switch_conflict", "并发切换已推进")
    ctx = _Ctx(switch=switch)
    await ctx.rounds(3)
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_CONFLICT] == 1
    tracker = ctx.watchdog._tracker(KEY)
    assert tracker.attempts == 0
    assert tracker.next_attempt_at == 0.0


async def test_gone_reason_forgets_partition() -> None:
    """装配 / 数据已变（partition_not_found）：丢弃跟踪态。"""
    switch = _Switch()
    switch.error = LeaderSwitchError("partition_not_found", "分区已删")
    ctx = _Ctx(switch=switch)
    await ctx.rounds(3)
    assert KEY not in ctx.watchdog._trackers


async def test_apply_failed_backs_off_and_keeps_pending() -> None:
    """激活失败：保留 pending_epoch（便于续跑）并按退避等待。"""
    switch = _Switch()
    switch.error = LeaderSwitchError("apply_failed", "目标节点激活失败")
    ctx = _Ctx(switch=switch)
    await ctx.rounds(3)
    tracker = ctx.watchdog._tracker(KEY)
    assert tracker.pending_epoch == 4
    assert tracker.next_attempt_at == ctx.clock() + 2_000
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_BLOCKED] == 1
    await ctx.rounds(1)
    assert len(switch.requests) == 1, "退避未到点不得重试"


async def test_backoff_doubles_and_caps() -> None:
    """退避按 2 的幂增长并封顶。"""
    switch = _Switch()
    switch.error = LeaderSwitchError("no_candidate", "没有候选")
    ctx = _Ctx(switch=switch, config=_config(backoff_base_ms=1_000, backoff_cap_ms=1_500))
    await ctx.rounds(3)
    tracker = ctx.watchdog._tracker(KEY)
    assert tracker.next_attempt_at == ctx.clock() + 1_000
    ctx.clock.advance(1_000)
    await ctx.rounds(1)
    assert len(switch.requests) == 2
    assert tracker.next_attempt_at == ctx.clock() + 1_500


async def test_stagger_when_earlier_replica_reachable() -> None:
    """死 leader 后继序里前序副本可达：本节点不发起，按 rank 错峰等待。"""
    # replicas=(a,b,c)、死的是 b → 顺序 [c, a]；a 前有 c，c 可达 → 等待
    ctx = _Ctx(store=_Store(partition=_partition(replicas=NODES)))
    await ctx.rounds(3)
    assert not ctx.switch.requests
    assert list(ctx.control.probes) == ["b", "b", "b", "c"]
    tracker = ctx.watchdog._tracker(KEY)
    assert tracker.next_attempt_at == ctx.clock() + 200


async def test_claims_when_all_earlier_replicas_unreachable() -> None:
    """前序有多个副本但都不可达：逐个探活后由本节点发起（不因前序不可达而卡死）。"""
    # replicas=(a,b,c,d)、死的是 b → 顺序 [c, d, a]；a 前有 c、d，均不可达 → a 发起
    ctx = _Ctx(
        store=_Store(partition=_partition(replicas=("a", "b", "c", "d"), leader="b")),
        control=_Control(reachable=("a",)),
    )
    await ctx.rounds(3)
    assert list(ctx.control.probes) == ["b", "b", "b", "c", "d"]
    assert len(ctx.switch.requests) == 1


async def test_claims_when_first_reachable() -> None:
    """本节点是死 leader 的直接后继：直接发起。"""
    ctx = _Ctx(store=_Store(partition=_partition(replicas=("b", "a", "c"))))
    await ctx.rounds(3)
    assert len(ctx.switch.requests) == 1


async def test_successor_order_rotates_and_skips_leader() -> None:
    """后继顺序：从死 leader 之后开始并排除它；leader 不在副本集时按原序。"""
    assert list(_successor_order(_partition(leader="b", replicas=("a", "b", "c")))) == ["c", "a"]
    assert list(_successor_order(_partition(leader="x", replicas=("a", "b", "c")))) == ["a", "b", "c"]


async def test_probe_timeout_counts_failure() -> None:
    """探活超时按失败计（三次超时后照常发起）。"""
    ctx = _Ctx(control=_Control(slow=NODES, delay_sec=0.05), config=_config(probe_timeout_ms=1))
    await ctx.rounds(3)
    assert list(ctx.control.probes) == ["b", "b", "b"]
    assert len(ctx.switch.requests) == 1


async def test_probe_exception_counts_failure() -> None:
    """探活抛异常按失败计（三次异常后照常发起）。"""
    ctx = _Ctx(control=_Control(errors=NODES))
    await ctx.rounds(3)
    assert list(ctx.control.probes) == ["b", "b", "b"]
    assert len(ctx.switch.requests) == 1


async def test_metrics_optional() -> None:
    """未装配指标时不报错（`_record` 静默）。"""
    watchdog = AutoElectionWatchdog(
        config=_config(),
        node_id="a",
        replication=cast("ReplicationCoordinator", _Views()),
        store=_Store(),
        control=_Control(),
        switch=cast("LeaderSwitchService", _Switch()),
        clock=_Clock(),
        wall_clock=lambda: WALL_NOW,
    )
    for _ in range(3):
        await watchdog.run_once()


async def test_unexpected_error_is_isolated_per_partition() -> None:
    """单分区未预期异常被隔离，整轮继续。"""
    ctx = _Ctx(views=_Views(error=RuntimeError("view boom")))
    await ctx.rounds(1)
    assert not ctx.switch.requests


async def test_loop_runs_round_and_stops() -> None:
    """看护循环：按节拍跑一轮 run_once，收到取消后退出。"""
    sleeps = ConcurrentStableList[float]()

    async def fake_sleep(seconds: float) -> None:
        sleeps.add(seconds)
        if len(sleeps) > 1:
            raise asyncio.CancelledError

    watchdog = AutoElectionWatchdog(
        config=_config(detect_interval_ms=1),
        node_id="a",
        replication=cast("ReplicationCoordinator", _Views()),
        store=_Store(),
        control=_Control(),
        switch=cast("LeaderSwitchService", _Switch()),
        metrics=AutoElectionMetrics(),
        clock=_Clock(),
        wall_clock=lambda: WALL_NOW,
        sleep=fake_sleep,
    )
    watchdog.ensure_started()
    await asyncio.sleep(0.05)
    await watchdog.stop()
    assert watchdog.running is False
    assert len(sleeps) >= 2, "循环至少跑完一轮才被取消"


async def test_lifecycle_start_and_stop() -> None:
    """循环启停：启动幂等、停止幂等、停止后不再重启。"""
    ctx = _Ctx()
    assert ctx.watchdog.running is False
    ctx.watchdog.ensure_started()
    assert ctx.watchdog.running is True
    ctx.watchdog.ensure_started()
    assert len(ctx.watchdog._tasks) == 1
    await ctx.watchdog.stop()
    assert ctx.watchdog.running is False
    await ctx.watchdog.stop()
    ctx.watchdog.ensure_started()
    assert ctx.watchdog.running is False, "停止后不得重启"


def test_tracker_reset_keeps_cooldown() -> None:
    """跟踪态清零：冷却截止保留，其余归零。"""
    ctx = _Ctx()
    tracker = ctx.watchdog._tracker(KEY)
    tracker.failures = 4
    tracker.attempts = 2
    tracker.pending_epoch = 7
    tracker.next_attempt_at = 123.0
    tracker.cooldown_until = 456.0
    tracker.cooldown_reported = True
    tracker.exhausted_reported = True
    tracker.reset()
    assert tracker.failures == 0
    assert tracker.attempts == 0
    assert tracker.pending_epoch == 0
    assert tracker.next_attempt_at == 0.0
    assert tracker.cooldown_until == 456.0
    assert tracker.cooldown_reported is False
    assert tracker.exhausted_reported is False


def test_suspect_log_is_suppressed() -> None:
    """判死告警 60s 内只记一次（窗口内重复调用直接返回）。"""
    ctx = _Ctx()
    tracker = ctx.watchdog._tracker(KEY)
    ctx.watchdog._report_suspect(tracker=tracker, key=KEY, leader="b")
    first = tracker.suspect_logged_at
    ctx.clock.advance(1_000)
    ctx.watchdog._report_suspect(tracker=tracker, key=KEY, leader="b")
    assert tracker.suspect_logged_at == first
    ctx.clock.advance(60_000)
    ctx.watchdog._report_suspect(tracker=tracker, key=KEY, leader="b")
    assert first is not None
    assert tracker.suspect_logged_at == first + 61_000


def test_cooldown_reported_once() -> None:
    """冷却 blocked 只记一次（重复调用不重复计数）。"""
    ctx = _Ctx()
    tracker = ctx.watchdog._tracker(KEY)
    ctx.watchdog._report_cooldown(tracker=tracker, key=KEY)
    ctx.watchdog._report_cooldown(tracker=tracker, key=KEY)
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_BLOCKED] == 1


def test_exhausted_reported_once() -> None:
    """exhausted 只记一次并落冷却截止。"""
    ctx = _Ctx()
    tracker = ctx.watchdog._tracker(KEY)
    ctx.watchdog._report_exhausted(tracker=tracker, key=KEY, now=ctx.clock())
    ctx.watchdog._report_exhausted(tracker=tracker, key=KEY, now=ctx.clock())
    assert ctx.metrics.snapshot()[AUTO_ELECTION_RESULT_EXHAUSTED] == 1
    assert tracker.cooldown_until == ctx.clock() + 60_000


def test_metrics_snapshot_is_copy() -> None:
    """指标快照是副本（外部改写不影响内部计数）。"""
    metrics = AutoElectionMetrics()
    metrics.record(AUTO_ELECTION_RESULT_TRIGGERED)
    snapshot = metrics.snapshot()
    snapshot.set(AUTO_ELECTION_RESULT_TRIGGERED, 99)
    assert metrics.snapshot()[AUTO_ELECTION_RESULT_TRIGGERED] == 1


def test_tracker_uses_stable_dict() -> None:
    """跟踪表使用插入序并发字典（集合声明口径）。"""
    ctx = _Ctx()
    assert isinstance(ctx.watchdog._trackers, ConcurrentStableDict)
    ctx.watchdog._tracker(KEY)
    assert list(ctx.watchdog._trackers.keys()) == [KEY]


class _WatchdogStub:
    """看护循环替身（只验证启动钩子与回收句柄的接线）。"""

    def __init__(self) -> None:
        """初始化（未启动）。"""
        self.started = False
        self.stopped = False

    def ensure_started(self) -> None:
        """标记已启动。"""
        self.started = True

    async def stop(self) -> None:
        """标记已停止。"""
        self.stopped = True


class _OffsetStub:
    """位点刷盘替身（启动钩子会拉起它，此处只为钩子可跑通）。"""

    def ensure_started(self) -> None:
        """空实现。"""

    async def stop(self) -> None:
        """空实现。"""

    async def run_once(self) -> int:
        """不刷任何文件。

        Returns:
            int: 恒 0。
        """
        return 0

    async def aclose(self) -> None:
        """空实现。"""


def _lifecycle_app(*, watchdog: object | None) -> FastAPI:
    """构造只带后台任务与资源管理器的空应用。

    Args:
        watchdog: 看护循环替身（None 表示未装配）。

    Returns:
        FastAPI: 应用实例。
    """
    from bms_msg.broker.delay import InMemoryDelayStore

    app = FastAPI()
    app.state.delay_scheduler = SimpleNamespace(ensure_started=lambda: None, stop=_noop_async, running=False)
    app.state.replay_runner = SimpleNamespace(ensure_started=lambda: None, stop=_noop_async, running=False)
    app.state.offset_store = _OffsetStub()
    app.state.delay_store = InMemoryDelayStore()
    if watchdog is not None:
        app.state.auto_election = watchdog
    app.state.resources = ResourceManager()
    return app


async def _noop_async() -> None:
    """空异步函数。"""


async def test_start_hook_starts_and_stops_auto_election() -> None:
    """02-9：启动钩子拉起看护循环，停机随资源管理器取消。"""
    watchdog = _WatchdogStub()
    app = _lifecycle_app(watchdog=watchdog)

    await start_background_tasks(app)

    assert watchdog.started is True
    closer = app.state.auto_election_closer
    assert isinstance(closer, AutoElectionCloser)

    await app.state.resources.aclose()

    assert watchdog.stopped is True


async def test_auto_election_closer_registered_once() -> None:
    """重复执行启动钩子：看护循环回收句柄不重复登记。"""
    watchdog = _WatchdogStub()
    app = _lifecycle_app(watchdog=watchdog)

    await start_background_tasks(app)
    first = app.state.auto_election_closer
    await start_background_tasks(app)

    assert app.state.auto_election_closer is first


async def test_start_hook_without_auto_election() -> None:
    """未装配看护循环：启动钩子照常跑完，不登记回收句柄。"""
    app = _lifecycle_app(watchdog=None)

    await start_background_tasks(app)

    assert getattr(app.state, "auto_election_closer", None) is None
