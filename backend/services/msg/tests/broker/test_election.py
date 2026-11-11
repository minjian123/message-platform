"""选举与切换编排（msg 任务 02-5；Kiwi 2485）。

覆盖详细设计 §12.2 的 G1 / G2 / G3 / G7：

- 预检：复制关闭、分区缺失、目标不在副本集 / 不在 ISR / 落后水位、epoch 冲突、已是目标 no-op；
- 两阶段：phase 1 CAS、宽限、目标激活、phase 2 收口、中断续跑、并发冲突、批量部分失败；
- 尾部口径：旧 leader 可达按真实 HW 截断、等于水位 no-op、不可达保留 / force 降级；
- 审计：begin 失败拒切、finished 失败不回滚。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.exceptions import InternalError
from bms_core.events.base import EventEnvelope

from bms_msg.audit.trail import LEADER_SWITCH_ACTION
from bms_msg.broker.election import (
    SWITCH_STATE_IDLE,
    SWITCH_STATE_SWITCHING,
    InProcessLeaderControlTransport,
    LeaderControlTransport,
    LeaderSwitchError,
    LeaderSwitchService,
    LeaderSwitchStore,
    SwitchApplyCommand,
    SwitchApplyResult,
    SwitchBatchResult,
    SwitchFailure,
    SwitchPartitionRequest,
    SwitchPartitionState,
    SwitchTopicRequest,
)
from bms_msg.broker.replication import ReplicationCoordinator, ReplicationSnapshot

pytestmark = pytest.mark.kiwi_id(2485)

TENANT_ID = 7001
TOPIC_ID = 7001
PARTITION_NO = 0
NAMESPACE = "msg"
TOPIC_NAME = "audit.failover"


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


class _Sink:
    """审计出口替身（记录事件；`fail_on` 指定第几次发射失败）。"""

    def __init__(self, *, fail_on: int | None = None) -> None:
        """初始化。

        Args:
            fail_on: 第几次发射失败（1 起；None 表示恒成功）。
        """
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
        self.calls: ConcurrentStableList[tuple[str, ConcurrentStableDict[str, object]]] = ConcurrentStableList()
        self._fail_on = fail_on

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录事件并返回当前设定的结果。

        Args:
            event: 审计事件信封。
            required: 是否强审计（未使用）。

        Returns:
            bool: 成功 True；失败 False。
        """
        del required
        self.events.add(event)
        payload = event.payload
        raw_params = payload.get("params")
        params = cast(
            "ConcurrentStableDict[str, object]",
            ConcurrentStableDict(json.loads(str(raw_params))) if raw_params is not None else ConcurrentStableDict(),
        )
        self.calls.add((str(payload.get("action")), params))
        return self._fail_on is None or len(self.events) != self._fail_on


def _snapshot(
    *,
    leader: str,
    epoch: int,
    leo: int,
    hw: int,
    isr: tuple[str, ...],
    replicas: tuple[str, ...],
    last_known_hw: int = 0,
    last_known_isr: tuple[str, ...] = (),
    last_known_at: float = 0.0,
) -> ReplicationSnapshot:
    """构造复制运行态快照。

    Args:
        leader: leader 节点。
        epoch: 任期。
        leo: 本节点 LEO。
        hw: 高水位。
        isr: ISR。
        replicas: 副本集。
        last_known_hw: 最后已知 leader HW。
        last_known_isr: 最后已知 leader ISR。
        last_known_at: 最后已知采集时刻。

    Returns:
        ReplicationSnapshot: 快照。
    """
    return ReplicationSnapshot(
        leader=leader,
        epoch=epoch,
        leo=leo,
        hw=hw,
        isr=ConcurrentStableFrozenList(isr),
        replicas=ConcurrentStableFrozenList(replicas),
        last_known_hw=last_known_hw,
        last_known_isr=ConcurrentStableFrozenList(last_known_isr),
        last_known_at=last_known_at,
    )


class _Control(LeaderControlTransport):
    """节点控制替身：按节点返回快照，记录 demote / apply 调用。"""

    def __init__(self, snapshots: ConcurrentStableDict[str, ReplicationSnapshot | None]) -> None:
        """初始化。

        Args:
            snapshots: 节点 → 快照（None 表示不可达）。
        """
        self.snapshots = snapshots
        self.demoted: ConcurrentStableList[tuple[str, str, int]] = ConcurrentStableList()
        self.applied: ConcurrentStableList[SwitchApplyCommand] = ConcurrentStableList()
        self.apply_error: Exception | None = None
        self.apply_result: SwitchApplyResult | None = None

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """返回节点快照。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照。
        """
        del topic_id, partition_no
        return self.snapshots.get(node_id)

    async def demote(
        self,
        *,
        node_id: str,
        topic_id: int,
        partition_no: int,
        to_leader: str,
        to_epoch: int,
    ) -> None:
        """记录 demote。

        Args:
            node_id: 旧 leader。
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader。
            to_epoch: 新任期。
        """
        del topic_id, partition_no
        self.demoted.add((node_id, to_leader, to_epoch))

    async def apply_switch(self, *, node_id: str, command: SwitchApplyCommand) -> SwitchApplyResult:
        """记录并返回激活结果。

        Args:
            node_id: 新 leader。
            command: 切换命令。

        Returns:
            SwitchApplyResult: 激活结果。

        Raises:
            Exception: `apply_error` 非空时抛出。
        """
        del node_id
        self.applied.add(command)
        if self.apply_error is not None:
            raise self.apply_error
        if self.apply_result is not None:
            return self.apply_result
        if command.truncate and command.watermark is not None:
            truncated_to = command.watermark
            retained = 0
        elif command.watermark is not None:
            truncated_to = None
            retained = max(3 - command.watermark, 0)
        else:
            truncated_to = None
            retained = 0
        return SwitchApplyResult(
            to_leo=command.watermark if command.truncate and command.watermark is not None else 3,
            truncated_to=truncated_to,
            retained_tail=retained,
            already_applied=False,
        )


class _Store(LeaderSwitchStore):
    """内存切换存储：单分区状态 + 两阶段 CAS。"""

    def __init__(self, *, state: SwitchPartitionState) -> None:
        """初始化。

        Args:
            state: 初始分区状态。
        """
        self.states: ConcurrentStableList[SwitchPartitionState] = ConcurrentStableList()
        self.state = state
        self.begin_calls = 0
        self.finish_calls = 0
        self.begin_override: bool | None = None
        self.finish_override: bool | None = None
        self.last_finish: tuple[int | None, str, int | None, str] = (None, "", None, "")

    async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
        """返回当前状态。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            SwitchPartitionState | None: 状态。
        """
        if self.state.partition_no != partition_no:
            return None
        return self.state

    async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
        """返回当前状态（单分区）。

        Args:
            topic_id: 主题主键。

        Returns:
            ConcurrentStableList[SwitchPartitionState]: 分区状态。
        """
        del topic_id
        return ConcurrentStableList((self.state,))

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
        """phase 1 CAS。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            expected_epoch: 预期 epoch。
            target: 新 leader。
            from_leader: 切换前 leader。
            from_epoch: 切换前 epoch。

        Returns:
            bool: 成功 True。
        """
        self.begin_calls += 1
        if self.begin_override is not None:
            return self.begin_override
        if self.state.switch_state != SWITCH_STATE_IDLE or self.state.epoch != expected_epoch:
            return False
        self.state = replace(
            self.state,
            leader=target,
            epoch=from_epoch + 1,
            switch_state=SWITCH_STATE_SWITCHING,
            switch_from_leader=from_leader,
            switch_from_epoch=from_epoch,
        )
        return True

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
        """phase 2 CAS。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            to_epoch: 切换后 epoch。
            watermark: 切换水位。
            result: 结果文本。
            truncated_to: 截断位点。
            operator: 触发人。

        Returns:
            bool: 成功 True。
        """
        self.finish_calls += 1
        self.last_finish = (watermark, result, truncated_to, operator)
        if self.finish_override is not None:
            return self.finish_override
        if self.state.switch_state != SWITCH_STATE_SWITCHING or self.state.epoch != to_epoch:
            return False
        self.state = replace(self.state, switch_state=SWITCH_STATE_IDLE)
        return True


def _state(
    *,
    leader: str = "n1",
    epoch: int = 0,
    replicas: tuple[str, ...] = ("n1", "n2"),
    switch_state: str = SWITCH_STATE_IDLE,
    switch_from_leader: str | None = None,
    switch_from_epoch: int | None = None,
    partition_no: int = PARTITION_NO,
) -> SwitchPartitionState:
    """构造分区切换状态。

    Args:
        leader: 当前 leader。
        epoch: 当前 epoch。
        replicas: 副本集。
        switch_state: 切换状态。
        switch_from_leader: 切换前 leader。
        switch_from_epoch: 切换前 epoch。
        partition_no: 分区号。

    Returns:
        SwitchPartitionState: 状态。
    """
    return SwitchPartitionState(
        topic_id=TOPIC_ID,
        partition_no=partition_no,
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        leader=leader,
        epoch=epoch,
        replicas=ConcurrentStableFrozenList(replicas),
        switch_state=switch_state,
        switch_from_leader=switch_from_leader,
        switch_from_epoch=switch_from_epoch,
    )


def _control(
    *,
    old: ReplicationSnapshot | None = None,
    target: ReplicationSnapshot | None = None,
    extra: dict[str, ReplicationSnapshot | None] | None = None,
) -> _Control:
    """构造控制替身（旧 leader n1、目标 n2）。

    Args:
        old: 旧 leader 快照（None 表示不可达）。
        target: 目标快照。
        extra: 其他节点快照。

    Returns:
        _Control: 控制替身。
    """
    snapshots: ConcurrentStableDict[str, ReplicationSnapshot | None] = ConcurrentStableDict(
        {
            "n1": old
            if old is not None
            else _snapshot(leader="n1", epoch=0, leo=3, hw=2, isr=("n1", "n2"), replicas=("n1", "n2")),
            "n2": target
            if target is not None
            else _snapshot(leader="n1", epoch=0, leo=3, hw=2, isr=("n1", "n2"), replicas=("n1", "n2")),
        }
    )
    if extra:
        snapshots.update(extra)
    return _Control(snapshots)


def _request(
    *,
    target: str | None = "n2",
    expected_epoch: int | None = None,
    force: bool = False,
    operator: str = "system",
    reason: str = "",
) -> SwitchPartitionRequest:
    """构造单分区切换请求（固定主题 / 分区）。

    Args:
        target: 目标 leader。
        expected_epoch: 预期 epoch。
        force: 是否允许降级。
        operator: 触发人。
        reason: 原因。

    Returns:
        SwitchPartitionRequest: 请求。
    """
    return SwitchPartitionRequest(
        topic_id=TOPIC_ID,
        partition_no=PARTITION_NO,
        target=target,
        expected_epoch=expected_epoch,
        force=force,
        operator=operator,
        reason=reason,
    )


async def _noop(seconds: float) -> None:
    """零等待（用例不真的睡宽限）。

    Args:
        seconds: 秒数（忽略）。
    """
    del seconds


def _service(
    *,
    store: LeaderSwitchStore,
    control: LeaderControlTransport,
    sink: _Sink | None = None,
    enabled: bool = True,
    clock: _Clock | None = None,
    fresh_ms: int = 10_000,
    grace_ms: int = 0,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> LeaderSwitchService:
    """构造切换服务（零宽限 + 注入时钟）。

    Args:
        store: 切换存储。
        control: 节点控制。
        sink: 审计出口（缺省恒成功）。
        enabled: 复制开关。
        clock: 时钟。
        fresh_ms: 最后已知 ISR 新鲜窗口。
        grace_ms: 停写宽限（毫秒）。
        sleep: 等待函数（缺省零等待）。

    Returns:
        LeaderSwitchService: 服务。
    """
    return LeaderSwitchService(
        enabled=enabled,
        store=store,
        control=control,
        audit=sink if sink is not None else _Sink(),
        grace_ms=grace_ms,
        follower_fresh_ms=fresh_ms,
        clock=clock if clock is not None else _Clock(),
        sleep=sleep if sleep is not None else _noop,
    )


async def test_replication_disabled_rejects() -> None:
    """复制关闭：切换服务拒绝（单实例零变化）。"""
    service = _service(store=_Store(state=_state()), control=_control(), enabled=False)
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(_request(target="n2"))
    assert captured.value.reason == "replication_disabled"


async def test_partition_not_found() -> None:
    """分区行缺失：拒绝。"""
    service = _service(store=_Store(state=_state(partition_no=9)), control=_control())
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(_request(target="n2"))
    assert captured.value.reason == "partition_not_found"


async def test_target_not_in_replicas() -> None:
    """目标不在副本集：拒绝。"""
    service = _service(store=_Store(state=_state()), control=_control())
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(_request(target="n3"))
    assert captured.value.reason == "target_not_in_replicas"


async def test_target_unreachable() -> None:
    """目标节点不可达：拒绝。"""
    control = _control()
    control.snapshots["n2"] = None
    service = _service(store=_Store(state=_state()), control=control)
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(_request(target="n2"))
    assert captured.value.reason == "target_unreachable"


async def test_target_not_in_isr_rejects_even_with_force() -> None:
    """旧 leader 可达但目标不在 ISR：拒绝（force 只用于不可达时的降级）。"""
    old = _snapshot(leader="n1", epoch=0, leo=3, hw=2, isr=("n1",), replicas=("n1", "n2"))
    service = _service(store=_Store(state=_state()), control=_control(old=old))
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(
            SwitchPartitionRequest(topic_id=TOPIC_ID, partition_no=PARTITION_NO, target="n2", force=True)
        )
    assert captured.value.reason == "target_not_in_isr"


async def test_target_behind_watermark() -> None:
    """目标 LEO 落后于切换水位：拒绝。"""
    old = _snapshot(leader="n1", epoch=0, leo=3, hw=3, isr=("n1", "n2"), replicas=("n1", "n2"))
    target = _snapshot(leader="n1", epoch=0, leo=2, hw=2, isr=("n1", "n2"), replicas=("n1", "n2"))
    service = _service(store=_Store(state=_state()), control=_control(old=old, target=target))
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(_request(target="n2"))
    assert captured.value.reason == "target_behind_watermark"


async def test_expected_epoch_conflict() -> None:
    """预期 epoch 与当前不一致：冲突返回，不覆盖。"""
    service = _service(store=_Store(state=_state(epoch=2)), control=_control())
    with pytest.raises(LeaderSwitchError) as captured:
        await service.switch_partition(
            SwitchPartitionRequest(topic_id=TOPIC_ID, partition_no=PARTITION_NO, target="n2", expected_epoch=1)
        )
    assert captured.value.reason == "epoch_conflict"


async def test_already_target_is_noop() -> None:
    """已是目标 leader：幂等 no-op，不递增 epoch、不写审计。"""
    sink = _Sink()
    store = _Store(state=_state(leader="n2", epoch=3))
    service = _service(store=store, control=_control(), sink=sink)
    result = await service.switch_partition(_request(target="n2"))
    assert result.applied is False
    assert result.to_epoch == 3
    assert store.begin_calls == 0
    assert len(sink.events) == 0


async def test_auto_pick_target_by_leo() -> None:
    """未指定目标：在 ISR 内选 LEO 最大者（并列按副本集顺序）。"""
    old = _snapshot(leader="n1", epoch=0, leo=5, hw=3, isr=("n1", "n2", "n3"), replicas=("n1", "n2", "n3"))
    n2 = _snapshot(leader="n1", epoch=0, leo=3, hw=3, isr=("n1", "n2", "n3"), replicas=("n1", "n2", "n3"))
    n3 = _snapshot(leader="n1", epoch=0, leo=5, hw=3, isr=("n1", "n2", "n3"), replicas=("n1", "n2", "n3"))
    control = _control(old=old, target=n2)
    control.snapshots["n3"] = n3
    store = _Store(state=_state(replicas=("n1", "n2", "n3")))
    result = await _service(store=store, control=control).switch_partition(
        SwitchPartitionRequest(topic_id=TOPIC_ID, partition_no=PARTITION_NO, target=None)
    )
    assert result.to_leader == "n3"
    assert store.state.leader == "n3"


async def test_switch_truncates_to_real_hw() -> None:
    """正常两阶段：旧 leader 可达，目标按真实 HW 截断。"""
    sink = _Sink()
    store = _Store(state=_state())
    control = _control()
    service = _service(store=store, control=control, sink=sink)
    result = await service.switch_partition(
        SwitchPartitionRequest(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            target="n2",
            operator="system:drill",
            reason="drill",
        )
    )
    assert result.applied is True
    assert result.watermark == 2
    assert result.truncated_to == 2
    assert result.retained_tail == 0
    assert result.isr_verified is True
    assert control.demoted.to_list() == [("n1", "n2", 1)]
    assert control.applied[0].truncate is True
    assert store.state.switch_state == SWITCH_STATE_IDLE
    assert store.last_finish[1] == "truncated"
    assert [action for action, _ in sink.calls] == [LEADER_SWITCH_ACTION, LEADER_SWITCH_ACTION]
    assert sink.calls[0][1]["phase"] == "begin"
    assert sink.calls[1][1]["phase"] == "finished"


async def test_switch_waits_grace_before_apply() -> None:
    """宽限 > 0：先等旧租约窗口，再让目标节点激活。"""
    slept: ConcurrentStableList[float] = ConcurrentStableList()
    control = _control()

    async def _record(seconds: float) -> None:
        slept.add(seconds)

    result = await _service(
        store=_Store(state=_state()), control=control, grace_ms=500, sleep=_record
    ).switch_partition(_request(target="n2"))
    assert result.applied is True
    assert slept.to_list() == [0.5]


async def test_switch_noop_when_target_equals_watermark() -> None:
    """目标 LEO == 水位：不截断，结果 noop。"""
    store = _Store(state=_state())
    control = _control()
    control.apply_result = SwitchApplyResult(to_leo=2, truncated_to=None, retained_tail=0, already_applied=False)
    result = await _service(store=store, control=control).switch_partition(_request(target="n2"))
    assert result.truncated_to is None
    assert store.last_finish[1] == "noop"


async def test_unreachable_old_leader_retains_with_last_known_isr() -> None:
    """旧 leader 不可达但目标在最后已知 ISR 且新鲜：直接切，保留尾部。"""
    target = _snapshot(
        leader="n1",
        epoch=0,
        leo=3,
        hw=2,
        isr=("n1", "n2"),
        replicas=("n1", "n2"),
        last_known_hw=2,
        last_known_isr=("n1", "n2"),
        last_known_at=0.0,
    )
    store = _Store(state=_state())
    control = _control(old=None, target=target)
    control.snapshots["n1"] = None
    result = await _service(store=store, control=control).switch_partition(_request(target="n2"))
    assert result.isr_verified is True
    assert result.watermark == 2
    assert result.truncated_to is None
    assert result.retained_tail == 1
    assert control.applied[0].truncate is False
    assert store.last_finish[1] == "retained"


async def test_unreachable_old_leader_requires_force() -> None:
    """旧 leader 不可达且无法验证 ISR：非 force 拒绝。"""
    target = _snapshot(
        leader="n1",
        epoch=0,
        leo=3,
        hw=2,
        isr=("n1", "n2"),
        replicas=("n1", "n2"),
        last_known_hw=2,
        last_known_isr=("n2",),
        last_known_at=0.0,
    )
    control = _control(old=None, target=target)
    control.snapshots["n1"] = None
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=_Store(state=_state()), control=control).switch_partition(_request(target="n2"))
    assert captured.value.reason == "isr_unverified"


async def test_force_switches_degraded_and_retains() -> None:
    """force=True：降级切换，`isr_verified=false`，保留尾部。"""
    target = _snapshot(
        leader="n1",
        epoch=0,
        leo=3,
        hw=2,
        isr=("n1", "n2"),
        replicas=("n1", "n2"),
        last_known_hw=2,
        last_known_isr=("n2",),
        last_known_at=0.0,
    )
    control = _control(old=None, target=target)
    control.snapshots["n1"] = None
    result = await _service(store=_Store(state=_state()), control=control).switch_partition(
        SwitchPartitionRequest(topic_id=TOPIC_ID, partition_no=PARTITION_NO, target="n2", force=True)
    )
    assert result.isr_verified is False
    assert result.retained_tail == 1


async def test_resume_same_target_completes() -> None:
    """phase 1 后中断：同 target 重试从等待 / apply 续跑并收口（不重复 begin 审计）。"""
    sink = _Sink()
    store = _Store(
        state=_state(
            leader="n2",
            epoch=1,
            switch_state=SWITCH_STATE_SWITCHING,
            switch_from_leader="n1",
            switch_from_epoch=0,
        )
    )
    result = await _service(store=store, control=_control(), sink=sink).switch_partition(_request(target="n2"))
    assert result.applied is True
    assert store.begin_calls == 0
    assert store.state.switch_state == SWITCH_STATE_IDLE
    assert [params["phase"] for _, params in sink.calls] == ["finished"]


async def test_resume_rejects_different_target() -> None:
    """切换中改目标：拒绝，避免来回切换。"""
    store = _Store(
        state=_state(
            leader="n2",
            epoch=1,
            replicas=("n1", "n2", "n3"),
            switch_state=SWITCH_STATE_SWITCHING,
            switch_from_leader="n1",
            switch_from_epoch=0,
        )
    )
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=store, control=_control()).switch_partition(_request(target="n3"))
    assert captured.value.reason == "switch_in_progress"


async def test_resume_rejects_epoch_conflict() -> None:
    """切换中预期 epoch 与切换前 / 当前都不一致：拒绝。"""
    store = _Store(
        state=_state(
            leader="n2",
            epoch=1,
            switch_state=SWITCH_STATE_SWITCHING,
            switch_from_leader="n1",
            switch_from_epoch=0,
        )
    )
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=store, control=_control()).switch_partition(
            SwitchPartitionRequest(topic_id=TOPIC_ID, partition_no=PARTITION_NO, target=None, expected_epoch=5)
        )
    assert captured.value.reason == "epoch_conflict"


async def test_begin_conflict_after_concurrent_advance_is_noop() -> None:
    """并发切换已推进到同 target / 更高 epoch：重读后返回幂等 no-op。"""

    class RacingStore(_Store):
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
            del topic_id, partition_no, expected_epoch, target, from_leader, from_epoch
            self.state = replace(self.state, leader="n2", epoch=1)
            return False

    store = RacingStore(state=_state())
    result = await _service(store=store, control=_control()).switch_partition(_request(target="n2"))
    assert result.applied is False
    assert result.to_epoch == 1


async def test_begin_conflict_without_advance_raises() -> None:
    """phase 1 CAS 失败且没有推进：报冲突。"""
    store = _Store(state=_state())
    store.begin_override = False
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=store, control=_control()).switch_partition(_request(target="n2"))
    assert captured.value.reason == "switch_conflict"


async def test_finish_failure_raises() -> None:
    """phase 2 收口失败且分区仍 switching：报错（可重试续跑）。"""
    store = _Store(state=_state())
    store.finish_override = False
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=store, control=_control()).switch_partition(_request(target="n2"))
    assert captured.value.reason == "finish_failed"
    assert store.state.switch_state == SWITCH_STATE_SWITCHING


async def test_finish_already_idle_is_accepted() -> None:
    """phase 2 返回 0 但重读已 idle 且 epoch 一致：按幂等成功处理。"""

    class LosingRaceStore(_Store):
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
            del topic_id, partition_no, to_epoch, watermark, result, truncated_to, operator
            self.state = replace(self.state, switch_state=SWITCH_STATE_IDLE)
            return False

    result = await _service(store=LosingRaceStore(state=_state()), control=_control()).switch_partition(
        _request(target="n2")
    )
    assert result.applied is True


async def test_apply_failure_leaves_switching() -> None:
    """目标激活失败：DB 停在 switching，报 apply_failed。"""
    control = _control()
    control.apply_error = RuntimeError("目标节点宕机")
    store = _Store(state=_state())
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=store, control=control).switch_partition(_request(target="n2"))
    assert captured.value.reason == "apply_failed"
    assert store.state.switch_state == SWITCH_STATE_SWITCHING


async def test_demote_failure_is_tolerated() -> None:
    """旧 leader demote 失败（不可达）：继续按租约兜底。"""

    class FailingDemote(_Control):
        async def demote(
            self,
            *,
            node_id: str,
            topic_id: int,
            partition_no: int,
            to_leader: str,
            to_epoch: int,
        ) -> None:
            del node_id, topic_id, partition_no, to_leader, to_epoch
            raise RuntimeError("旧 leader 不可达")

    control = FailingDemote(_control().snapshots)
    result = await _service(store=_Store(state=_state()), control=control).switch_partition(_request(target="n2"))
    assert result.applied is True


async def test_audit_begin_failure_rejects_switch() -> None:
    """begin 审计失败：拒绝切换（没留痕不生效）。"""
    store = _Store(state=_state())
    with pytest.raises(InternalError):
        await _service(store=store, control=_control(), sink=_Sink(fail_on=1)).switch_partition(_request(target="n2"))
    assert store.begin_calls == 0


async def test_audit_finished_failure_does_not_rollback() -> None:
    """finished 审计失败：不回滚已完成的切换。"""
    store = _Store(state=_state())
    sink = _Sink(fail_on=2)
    result = await _service(store=store, control=_control(), sink=sink).switch_partition(_request(target="n2"))
    assert result.applied is True
    assert store.state.switch_state == SWITCH_STATE_IDLE


async def test_topic_batch_partial_failure() -> None:
    """主题批量：逐分区串行，单分区失败不影响其他分区，不回滚已成功项。"""

    class TwoPartitionStore(_Store):
        def __init__(self) -> None:
            super().__init__(state=_state(partition_no=0))
            self.other = _state(partition_no=1, leader="n1")
            self.states.add(self.state)
            self.states.add(self.other)

        async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
            return ConcurrentStableList((self.state, self.other))

        async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
            if partition_no == 0:
                return self.state
            if partition_no == 1:
                return self.other
            return None

        def _commit(self, target: SwitchPartitionState) -> None:
            if target.partition_no == 0:
                self.state = target
            else:
                self.other = target

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
            del topic_id, expected_epoch
            current = self.state if partition_no == 0 else self.other
            if partition_no == 1:
                return False
            self.state = replace(
                current,
                leader=target,
                epoch=from_epoch + 1,
                switch_state=SWITCH_STATE_SWITCHING,
                switch_from_leader=from_leader,
                switch_from_epoch=from_epoch,
            )
            return True

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
            del topic_id, partition_no, to_epoch, watermark, result, truncated_to, operator
            self.state = replace(self.state, switch_state=SWITCH_STATE_IDLE)
            return True

    store = TwoPartitionStore()
    result = await _service(store=store, control=_control()).switch_topic(
        SwitchTopicRequest(
            topic_id=TOPIC_ID,
            targets=ConcurrentStableFrozenDict({0: "n2", 1: "n2"}),
            operator="system:drill",
        )
    )
    assert isinstance(result, SwitchBatchResult)
    assert [item.partition_no for item in result.items] == [0]
    assert [failure.reason for failure in result.failures] == ["switch_conflict"]


async def test_topic_batch_empty() -> None:
    """主题无分区：返回空结果。"""

    class EmptyStore(_Store):
        async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
            return ConcurrentStableList()

    result = await _service(store=EmptyStore(state=_state()), control=_control()).switch_topic(
        SwitchTopicRequest(topic_id=TOPIC_ID)
    )
    assert list(result.items) == []
    assert list(result.failures) == []


async def test_switch_topic_expected_epochs_and_no_targets() -> None:
    """批量请求未给 targets / expected_epochs：按当前 epoch 自动选目标。"""
    store = _Store(state=_state())
    result = await _service(store=store, control=_control()).switch_topic(SwitchTopicRequest(topic_id=TOPIC_ID))
    assert result.items[0].to_leader == "n2"


async def test_unexpected_exception_becomes_failure() -> None:
    """非领域异常：也落进失败项，不中断整批。"""

    class ExplodingStore(_Store):
        async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
            return ConcurrentStableList((self.state,))

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
            del topic_id, partition_no, expected_epoch, target, from_leader, from_epoch
            raise RuntimeError("数据库不可用")

    result = await _service(store=ExplodingStore(state=_state()), control=_control()).switch_topic(
        SwitchTopicRequest(topic_id=TOPIC_ID)
    )
    assert result.failures[0].reason == "RuntimeError"


async def test_switch_failure_type_fields() -> None:
    """失败项字段完整（排障用）。"""
    failure = SwitchFailure(topic_id=TOPIC_ID, partition_no=PARTITION_NO, reason="x", detail="y")
    assert failure.reason == "x"


class _CoordinatorStub:
    """复制协调器替身（控制传输用例：只记录调用）。"""

    def __init__(self, *, snapshot: ReplicationSnapshot | None = None, explode: bool = False) -> None:
        """初始化。

        Args:
            snapshot: 返回的快照。
            explode: 取快照时是否抛错。
        """
        self.snapshot_value = snapshot
        self.explode = explode
        self.demoted: ConcurrentStableList[tuple[int, int, str, int]] = ConcurrentStableList()
        self.applied: ConcurrentStableList[SwitchApplyCommand] = ConcurrentStableList()

    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """返回快照或抛错。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 快照。

        Raises:
            RuntimeError: `explode=True` 时抛出。
        """
        del topic_id, partition_no
        if self.explode:
            raise RuntimeError("节点不可达")
        return self.snapshot_value

    async def demote(self, *, topic_id: int, partition_no: int, to_leader: str, to_epoch: int) -> None:
        """记录 demote。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader。
            to_epoch: 新任期。
        """
        self.demoted.add((topic_id, partition_no, to_leader, to_epoch))

    async def apply_switch(self, command: SwitchApplyCommand) -> SwitchApplyResult:
        """记录激活并按水位截断。

        Args:
            command: 切换命令。

        Returns:
            SwitchApplyResult: 激活结果。
        """
        self.applied.add(command)
        return SwitchApplyResult(
            to_leo=command.watermark or 0,
            truncated_to=command.watermark if command.truncate else None,
            retained_tail=0,
            already_applied=False,
        )


def _transport(coordinator: _CoordinatorStub) -> InProcessLeaderControlTransport:
    """构造已登记 n1 的进程内控制传输。

    Args:
        coordinator: 节点替身。

    Returns:
        InProcessLeaderControlTransport: 传输。
    """
    transport = InProcessLeaderControlTransport()
    transport.register("n1", cast("ReplicationCoordinator", coordinator))
    return transport


async def test_inprocess_transport_roundtrip() -> None:
    """进程内控制传输：快照 / demote / apply 转发到协调器。"""
    snapshot = _snapshot(leader="n1", epoch=0, leo=2, hw=1, isr=("n1",), replicas=("n1",))
    coordinator = _CoordinatorStub(snapshot=snapshot)
    transport = _transport(coordinator)
    assert await transport.snapshot(node_id="n1", topic_id=TOPIC_ID, partition_no=PARTITION_NO) is snapshot
    await transport.demote(node_id="n1", topic_id=TOPIC_ID, partition_no=PARTITION_NO, to_leader="n2", to_epoch=1)
    assert coordinator.demoted.to_list() == [(TOPIC_ID, PARTITION_NO, "n2", 1)]
    result = await transport.apply_switch(
        node_id="n1",
        command=SwitchApplyCommand(
            topic_id=TOPIC_ID,
            partition_no=PARTITION_NO,
            from_leader="n2",
            to_leader="n1",
            to_epoch=1,
            watermark=1,
            truncate=True,
            grace_ms=0,
        ),
    )
    assert result.truncated_to == 1
    transport.unregister("n1")
    assert await transport.snapshot(node_id="n1", topic_id=TOPIC_ID, partition_no=PARTITION_NO) is None
    await transport.demote(node_id="n1", topic_id=TOPIC_ID, partition_no=PARTITION_NO, to_leader="n2", to_epoch=1)
    with pytest.raises(LeaderSwitchError) as captured:
        await transport.apply_switch(
            node_id="n1",
            command=SwitchApplyCommand(
                topic_id=TOPIC_ID,
                partition_no=PARTITION_NO,
                from_leader="n2",
                to_leader="n1",
                to_epoch=1,
                watermark=None,
                truncate=False,
                grace_ms=0,
            ),
        )
    assert captured.value.reason == "target_unreachable"


async def test_inprocess_transport_snapshot_failure_returns_none() -> None:
    """进程内传输：快照异常按不可达处理（返回 None）。"""
    transport = _transport(_CoordinatorStub(explode=True))
    assert await transport.snapshot(node_id="n1", topic_id=TOPIC_ID, partition_no=PARTITION_NO) is None


async def test_default_clock_is_used() -> None:
    """未注入时钟：用单调毫秒缺省实现（no-op 路径也计时）。"""
    service = LeaderSwitchService(
        enabled=True,
        store=_Store(state=_state(leader="n2")),
        control=_control(),
        audit=_Sink(),
        grace_ms=0,
        follower_fresh_ms=10_000,
        sleep=_noop,
    )
    result = await service.switch_partition(_request(target="n2"))
    assert result.applied is False
    assert result.duration_ms >= 0


async def test_resume_with_unreachable_old_leader_retains() -> None:
    """中断续跑时旧 leader 不可达：用目标最后已知 HW 记录水位并保留尾部。"""
    target = _snapshot(
        leader="n1",
        epoch=1,
        leo=3,
        hw=2,
        isr=("n1", "n2"),
        replicas=("n1", "n2"),
        last_known_hw=2,
    )
    control = _control(target=target)
    control.snapshots["n1"] = None
    store = _Store(
        state=_state(
            leader="n2",
            epoch=1,
            switch_state=SWITCH_STATE_SWITCHING,
            switch_from_leader="n1",
            switch_from_epoch=0,
        )
    )
    result = await _service(store=store, control=control).switch_partition(_request(target="n2"))
    assert result.watermark == 2
    assert result.truncated_to is None
    assert result.retained_tail == 1


async def test_auto_pick_with_unreachable_old_leader() -> None:
    """旧 leader 不可达且未指定目标：按最后已知 ISR 选 LEO 最大者。"""
    n2 = _snapshot(
        leader="n1",
        epoch=0,
        leo=4,
        hw=1,
        isr=("n1", "n2", "n3"),
        replicas=("n1", "n2", "n3"),
        last_known_hw=1,
        last_known_isr=("n1", "n2", "n3"),
    )
    n3 = _snapshot(
        leader="n1",
        epoch=0,
        leo=2,
        hw=1,
        isr=("n1", "n2", "n3"),
        replicas=("n1", "n2", "n3"),
        last_known_hw=1,
        last_known_isr=("n1", "n2", "n3"),
    )
    control = _control(old=None, target=n2)
    control.snapshots["n1"] = None
    control.snapshots["n3"] = n3
    store = _Store(state=_state(replicas=("n1", "n2", "n3")))
    result = await _service(store=store, control=control).switch_partition(_request(target=None))
    assert result.to_leader == "n2"


async def test_auto_pick_without_candidate_rejects() -> None:
    """旧 leader 不可达且没有可验证候选：拒绝。"""
    n2 = _snapshot(
        leader="n1",
        epoch=0,
        leo=1,
        hw=0,
        isr=("n2",),
        replicas=("n1", "n2"),
        last_known_isr=("n2",),
    )
    control = _control(old=None, target=n2)
    control.snapshots["n1"] = None
    with pytest.raises(LeaderSwitchError) as captured:
        await _service(store=_Store(state=_state()), control=control).switch_partition(_request(target=None))
    assert captured.value.reason == "no_candidate"


async def test_force_watermark_uses_max_last_known_across_replicas() -> None:
    """force 降级：水位取可用副本最后已知 HW 的最大值（不用于截断）。"""
    n2 = _snapshot(
        leader="n1",
        epoch=0,
        leo=3,
        hw=2,
        isr=("n1", "n2", "n3", "n4"),
        replicas=("n1", "n2", "n3", "n4"),
        last_known_hw=2,
        last_known_isr=("n2",),
    )
    n3 = _snapshot(
        leader="n1",
        epoch=0,
        leo=4,
        hw=4,
        isr=("n1", "n2", "n3", "n4"),
        replicas=("n1", "n2", "n3", "n4"),
        last_known_hw=4,
        last_known_isr=("n3",),
    )
    control = _control(old=None, target=n2)
    control.snapshots["n1"] = None
    control.snapshots["n3"] = n3
    store = _Store(state=_state(replicas=("n1", "n2", "n3", "n4")))
    result = await _service(store=store, control=control).switch_partition(_request(target="n2", force=True))
    assert result.watermark == 4
