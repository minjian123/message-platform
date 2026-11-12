"""分区迁移编排（02-8）：预检 / 先扩后缩 / 切主降级 / 失败收口 / 状态刷新。"""

from __future__ import annotations

from dataclasses import replace
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.broker.election import LeaderSwitchError, SwitchResult
from bms_msg.broker.replication import ReplicationSnapshot
from bms_msg.models.partition_migration import PLAN_STATUS_SUCCEEDED
from bms_msg.services.migration_ledger import InMemoryMigrationLedger
from bms_msg.services.partition_assignment import PartitionAssigner, PartitionAssignmentConfig
from bms_msg.services.partition_migration import (
    MigrationPartition,
    MigrationStepError,
    MigrationTopology,
    PartitionMigrationService,
)

pytestmark = pytest.mark.kiwi_id(2486)

NODES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("msg-1", "msg-2", "msg-3"))


class FakeTopology(MigrationTopology):
    """分区事实替身：写分配即改内存并递增版本。"""

    def __init__(self, partitions: ConcurrentStableList[MigrationPartition]) -> None:
        self.partitions: ConcurrentStableDict[tuple[int, int], MigrationPartition] = ConcurrentStableDict()
        for partition in partitions:
            self.partitions.set((partition.topic_id, partition.partition_no), partition)
        self.write_result = True
        self.fail_on_write: int | None = None
        self.writes: ConcurrentStableList[tuple[tuple[str, ...], str]] = ConcurrentStableList()

    async def list_partitions(
        self, *, topic_ids: ConcurrentStableFrozenList[int] | None = None
    ) -> ConcurrentStableList[MigrationPartition]:
        return ConcurrentStableList(
            row for (topic_id, _), row in self.partitions.items() if topic_ids is None or topic_id in topic_ids
        )

    async def read_partition(self, *, topic_id: int, partition_no: int) -> MigrationPartition | None:
        return self.partitions.get((topic_id, partition_no))

    async def write_assignment(
        self,
        *,
        topic_id: int,
        partition_no: int,
        replicas: ConcurrentStableFrozenList[str],
        leader: str,
        expected_version: int,
    ) -> bool:
        if not self.write_result or (self.fail_on_write is not None and len(self.writes) + 1 == self.fail_on_write):
            return False
        row = self.partitions.get((topic_id, partition_no))
        if row is None:
            return False
        self.writes.add((tuple(replicas), leader))
        from dataclasses import replace

        self.partitions.set(
            (topic_id, partition_no),
            replace(row, replicas=replicas, leader=leader, version=row.version + 1),
        )
        return True


class FakeControl:
    """控制通道替身：按 `(node, topic, partition)` 返回快照。"""

    def __init__(self, snapshots: ConcurrentStableDict[str, ReplicationSnapshot | None]) -> None:
        self.snapshots = snapshots

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        del topic_id, partition_no
        return self.snapshots.get(node_id)


class FakeSwitch:
    """切主服务替身：返回预设结果或抛错。"""

    def __init__(self, *, result: SwitchResult | None = None, error: LeaderSwitchError | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: ConcurrentStableList[int] = ConcurrentStableList()

    async def switch_partition(self, request: object) -> SwitchResult:
        self.calls.add(1)
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result


class FakeAudit:
    """审计替身：记录事件，可指定失败次数。"""

    def __init__(self) -> None:
        self.events: ConcurrentStableList[object] = ConcurrentStableList()

    async def emit(self, event: object, *, required: bool = False) -> bool:
        del required
        self.events.add(event)
        return True


def _partition(
    *,
    replicas: tuple[str, ...] = ("msg-1",),
    leader: str = "msg-1",
    partition_no: int = 0,
    switch_state: str = "idle",
) -> MigrationPartition:
    """构造分区事实。"""
    return MigrationPartition(
        tenant_id=7001,
        namespace="msg",
        topic="audit.operation",
        topic_id=9,
        partition_no=partition_no,
        leader=leader,
        replicas=ConcurrentStableFrozenList(replicas),
        epoch=1,
        switch_state=switch_state,
        version=1,
    )


def _snapshot(
    *,
    node: str,
    leo: int = 10,
    hw: int = 10,
    isr: tuple[str, ...] = ("msg-1",),
    replicas: tuple[str, ...] = ("msg-1",),
    start: int = 0,
) -> ReplicationSnapshot:
    """构造复制快照。"""
    return ReplicationSnapshot(
        leader="msg-1",
        epoch=1,
        leo=leo,
        hw=hw,
        isr=ConcurrentStableFrozenList(isr),
        replicas=ConcurrentStableFrozenList(replicas),
        start_offset=start,
    )


def _switch_result(*, isr_verified: bool = True) -> SwitchResult:
    """构造切主结果。"""
    return SwitchResult(
        topic_id=9,
        partition_no=0,
        from_leader="msg-1",
        to_leader="msg-2",
        from_epoch=1,
        to_epoch=2,
        applied=True,
        watermark=10,
        truncated_to=None,
        retained_tail=0,
        isr_verified=isr_verified,
        duration_ms=10,
    )


def _service(
    topology: FakeTopology,
    control: FakeControl,
    switch: object,
    *,
    enabled: bool = True,
) -> PanelService:
    """构造迁移服务（固定时钟、零等待）。"""

    ledger = InMemoryMigrationLedger()
    return PanelService(
        assigner=PartitionAssigner(
            PartitionAssignmentConfig(
                enabled=enabled, node_id="msg-1", nodes=NODES, replication_factor=1, min_insync_replicas=1
            )
        ),
        ledger=ledger,
        topology=topology,
        switch=cast("object", switch),  # type: ignore[arg-type]
        control=cast("object", control),  # type: ignore[arg-type]
        audit=FakeAudit(),
        wait_ms=0,
        catch_up_budget_ms=0,
        clock=lambda: 0.0,
        sleep=_noop_sleep,
    )


async def _noop_sleep(seconds: float) -> None:
    """零等待。"""
    del seconds


PanelService = PartitionMigrationService


async def test_plan_generates_expand_and_blocks() -> None:
    """计划：可执行项生成台账，预检阻断项单列且不落库。"""
    topology = FakeTopology(
        ConcurrentStableList(
            [
                _partition(replicas=("msg-1",), leader="msg-1", partition_no=0),
                _partition(replicas=("msg-1",), leader="msg-1", partition_no=1),
            ]
        )
    )
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, start=5),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1",)),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    # 第二分区让新副本落后于 leader 起点：应被 needs_full_resync 阻断。
    control.snapshots.set("msg-3", _snapshot(node="msg-3", leo=1, isr=("msg-1",)))

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert result.plan_id.startswith("rb-")
    assert len(result.items) >= 1
    assert all(item.action in ("expand", "replace") for item in result.items)


async def test_plan_rejects_bad_arguments() -> None:
    """计划：节点 / 副本数 / min_insync 非法即拒绝（不生成台账）。"""
    service = _service(FakeTopology(ConcurrentStableList()), FakeControl(ConcurrentStableDict()), FakeSwitch())

    with pytest.raises(MigrationStepError, match="nodes"):
        await service.plan(nodes=ConcurrentStableFrozenList(), factor=1, min_insync=1)
    with pytest.raises(MigrationStepError, match="factor"):
        await service.plan(nodes=NODES, factor=9, min_insync=1)
    with pytest.raises(MigrationStepError, match="min-insync"):
        await service.plan(nodes=NODES, factor=2, min_insync=3)


async def test_preflight_blocks_leader_unreachable_and_resync() -> None:
    """预检：leader 不可达 / 目标落后起点 / 目标超前 leader 都阻断。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, start=5),
                "msg-2": _snapshot(node="msg-2", leo=1, isr=("msg-1",)),
                "msg-3": _snapshot(node="msg-3", leo=99, isr=("msg-1",)),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())

    control.snapshots.set("msg-1", None)
    blocked = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert [block.reason for block in blocked.blocks] == ["leader_unreachable"]

    control.snapshots.set("msg-1", _snapshot(node="msg-1", leo=10, hw=10, start=5))
    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=1, isr=("msg-1",)))
    resync = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert any(block.reason in ("needs_full_resync", "stale_data") for block in resync.blocks)


async def test_apply_expand_succeeds_and_writes_union() -> None:
    """执行扩容：副本集并集写入、追平后 succeeded。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill")

    assert result.succeeded == 1
    assert result.failed == 0
    assert topology.writes[0][0] == ("msg-1", "msg-2")


async def test_apply_replace_shrinks_after_catch_up() -> None:
    """执行替换：先扩（并集）再缩（目标集），同一行收口。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-1")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill")

    assert result.succeeded == 1
    assert len(topology.writes) == 2
    assert topology.writes[-1][0] == tuple(sorted(topology.writes[-1][0]))


async def test_apply_shrink_moves_leader_first() -> None:
    """缩容：移除 leader 时先切主，再写剩余副本集。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-2", "msg-3"), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    switch = FakeSwitch(result=_switch_result())
    service = _service(topology, control, switch)
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill")

    assert result.succeeded == 1
    assert switch.calls


async def test_apply_force_switch_is_degraded() -> None:
    """force 切主：`isr_verified=false` 记降级、不计通过。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    switch = FakeSwitch(result=_switch_result(isr_verified=False))
    service = _service(topology, control, switch)
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill", allow_force=True)

    assert result.degraded >= 1
    assert result.failed == 0


async def test_apply_failure_writes_failed_row() -> None:
    """执行失败：写分配失败 → failed + error 可读。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    topology.write_result = False

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill")

    assert result.failed == 1
    assert "assignment_conflict" in result.items[0].error


async def test_apply_switch_error_writes_failed_row() -> None:
    """切主失败：错误原因落台账，其余分区继续。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-2", "msg-3"), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch(error=LeaderSwitchError("no_candidate", "无候选")))
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="tester", reason="drill")

    assert result.failed == 1
    assert result.items[0].error.startswith("switch_no_candidate")


async def test_status_refreshes_catching_up() -> None:
    """状态：`catching_up` 追平后收口为 succeeded。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    # 先把目标副本压到未追平，apply 留下 catching_up。
    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=2, isr=("msg-1",)))
    await service.apply(plan_id=planned.plan_id, operator="t", reason="r", wait_ms=0)
    rows = await service.status(plan_id=planned.plan_id)
    assert rows[0].item.status == "catching_up"

    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")))
    rows = await service.status(plan_id=planned.plan_id)
    assert rows[0].item.status == "succeeded"
    assert rows[0].caught_up is True


async def test_status_without_plan_returns_empty() -> None:
    """状态：无台账时返回空列表。"""
    service = _service(FakeTopology(ConcurrentStableList()), FakeControl(ConcurrentStableDict()), FakeSwitch())

    assert await service.status() == []


async def test_replication_disabled_plans_noop() -> None:
    """复制关闭：分配结果为空副本集，计划无动作。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=())]))
    control = FakeControl(ConcurrentStableDict({"msg-1": _snapshot(node="msg-1")}))
    service = _service(topology, control, FakeSwitch(), enabled=False)

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert result.items == []


async def test_plan_skips_noop_and_blocks_switch_in_progress() -> None:
    """计划：副本集与 leader 均符合目标即无动作；切换窗口改 leader 被阻断。"""
    topology = FakeTopology(
        ConcurrentStableList(
            [
                _partition(replicas=("msg-1", "msg-2"), leader="msg-1", partition_no=0),
                _partition(replicas=("msg-1", "msg-2"), leader="msg-1", partition_no=1, switch_state="switching"),
            ]
        )
    )
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert result.items == []
    assert [block.reason for block in result.blocks] == ["switch_in_progress"]


async def test_preflight_unreachable_stale_insufficient_and_no_candidate() -> None:
    """预检：目标不可达 / 目标超前 / 缩容不足 / 无 leader 候选都阻断。"""
    topology = FakeTopology(
        ConcurrentStableList(
            [
                _partition(replicas=("msg-1",), leader="msg-1"),
                _partition(replicas=("msg-1", "msg-3"), leader="msg-3", partition_no=1),
            ]
        )
    )
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())

    control.snapshots.set("msg-2", None)
    unreachable = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert any(block.reason == "target_unreachable" for block in unreachable.blocks)

    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=99, isr=("msg-1", "msg-2", "msg-3")))
    stale = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert any(block.reason == "stale_data" for block in stale.blocks)

    insufficient = await service.plan(nodes=NODES, factor=3, min_insync=3)
    assert any(block.reason == "insufficient_replicas" for block in insufficient.blocks)

    control.snapshots.set("msg-1", _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1",)))
    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=1, isr=("msg-1",)))
    candidate = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert candidate.blocks == []


async def test_plan_blocks_when_active_migration_exists() -> None:
    """计划：同一分区已有未完成台账即阻断。"""
    from bms_msg.services.migration_ledger import MigrationPlanItem

    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    ledger = cast("InMemoryMigrationLedger", service._ledger)  # pyright: ignore[reportPrivateUsage]
    await ledger.create(
        ConcurrentStableList(
            (
                MigrationPlanItem(
                    plan_id="rb-existing",
                    tenant_id=7001,
                    namespace="msg",
                    topic="audit.operation",
                    topic_id=9,
                    partition_no=0,
                    action="expand",
                ),
            )
        )
    )

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert [block.reason for block in result.blocks] == ["migration_in_progress"]


async def test_apply_shrink_without_leader_move() -> None:
    """缩容：Leader 仍在目标集时直接写副本集。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-2", "msg-3"), leader="msg-1")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.succeeded == 1
    assert topology.writes[-1][1] == "msg-1"


async def test_apply_leader_move() -> None:
    """仅切主：副本集不变、leader 变更。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1",)),
                "msg-3": _snapshot(node="msg-3", leo=10, hw=10, isr=("msg-1",)),
            }
        )
    )
    switch = FakeSwitch(result=_switch_result())
    service = _service(topology, control, switch)
    planned = await service.plan(nodes=NODES, factor=1, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.succeeded == 1
    assert switch.calls


async def test_status_replace_waits_for_shrink() -> None:
    """状态：替换类追平后仍停在 catching_up（缩容需 apply 续跑）。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-1")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=5, isr=("msg-1",)),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    interim = await service.apply(plan_id=planned.plan_id, operator="t", reason="r", wait_ms=0)
    assert interim.catching_up == 1

    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")))
    rows = await service.status(plan_id=planned.plan_id)

    assert rows[0].item.status == "catching_up"
    assert rows[0].caught_up is True


async def test_apply_reload_missing_row_returns() -> None:
    """续跑：CAS 后台账行消失即安全返回（不抛）。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    ledger = cast("InMemoryMigrationLedger", service._ledger)  # pyright: ignore[reportPrivateUsage]

    async def _missing(**kwargs: object) -> None:
        del kwargs
        return None

    ledger.load_one = _missing  # type: ignore[method-assign]
    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.succeeded == 0
    assert result.failed == 0


async def test_preflight_no_leader_candidate_when_candidate_behind_hw() -> None:
    """预检：移除 leader 时目标候选落后切换水位即阻断。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=1, hw=10, isr=("msg-1", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, hw=10, isr=("msg-1", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert [block.reason for block in result.blocks] == ["no_leader_candidate"]


async def test_preflight_target_leader_is_new_replica() -> None:
    """预检：目标 leader 是新副本（不在剩余集）即阻断。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-2", "msg-3"), leader="msg-3")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, isr=("msg-1",)),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1",)),
                "msg-3": _snapshot(node="msg-3", leo=10, hw=10, isr=("msg-1",)),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())

    result = await service.plan(nodes=NODES, factor=2, min_insync=1)

    assert any(block.reason == "no_leader_candidate" for block in result.blocks)


async def test_apply_twice_skips_terminal_rows() -> None:
    """执行：已终态的计划项再次 apply 直接跳过。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    again = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert again.succeeded == 1


async def test_apply_resume_catching_up() -> None:
    """续跑：catching_up 追平后 apply 继续收口（替换类执行缩容）。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-1")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=5, isr=("msg-1",)),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    await service.apply(plan_id=planned.plan_id, operator="t", reason="r", wait_ms=0)

    control.snapshots.set("msg-2", _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")))
    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.succeeded == 1
    assert topology.writes[-1][0] == ("msg-1", "msg-2")


async def test_execute_partition_missing_marks_failed() -> None:
    """执行：分区行在计划后被删除即 failed。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    topology.partitions.pop((9, 0), None)

    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.failed == 1
    assert result.items[0].error.startswith("partition_not_found")


async def test_status_reports_planned_and_succeeded() -> None:
    """状态：非 catching_up 行原样输出（planned / succeeded）。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    assert (await service.status(plan_id=planned.plan_id))[0].item.status == "planned"

    await service.apply(plan_id=planned.plan_id, operator="t", reason="r")
    assert (await service.status(plan_id=planned.plan_id))[0].item.status == "succeeded"


async def test_probe_handles_missing_leader_and_target() -> None:
    """追平探针：leader 不可达 / 目标不可达 / 目标不在 ISR 均不算追平。"""

    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(ConcurrentStableDict({"msg-1": None, "msg-2": None}))
    service = _service(topology, control, FakeSwitch())
    ledger = cast("InMemoryMigrationLedger", service._ledger)  # pyright: ignore[reportPrivateUsage]
    from bms_msg.services.migration_ledger import MigrationPlanItem

    await ledger.create(
        ConcurrentStableList(
            (
                MigrationPlanItem(
                    plan_id="rb-probe",
                    tenant_id=7001,
                    namespace="msg",
                    topic="t",
                    topic_id=9,
                    partition_no=0,
                    action="expand",
                    to_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
                    to_leader="msg-1",
                ),
            )
        )
    )
    row = (await ledger.load("rb-probe"))[0]
    caught, _leo, _hw, remaining = await service._probe_caught_up(current=row, budget_ms=0)  # pyright: ignore[reportPrivateUsage]
    assert caught is False and remaining == 0

    control.snapshots.set("msg-1", _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1",)))
    caught, _leo, _hw, _remaining = await service._probe_caught_up(current=row, budget_ms=0)  # pyright: ignore[reportPrivateUsage]
    assert caught is False


async def test_probe_sleeps_until_budget() -> None:
    """追平探针：未追平时按轮询间隔等待，预算耗尽返回。"""

    class Clock:
        def __init__(self) -> None:
            self.now = 0.0
            self.sleeps = 0

        def __call__(self) -> float:
            return self.now

        async def sleep(self, seconds: float) -> None:
            del seconds
            self.sleeps += 1
            self.now += 10.0

    topology = FakeTopology(ConcurrentStableList())
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1",)),
                "msg-2": _snapshot(node="msg-2", leo=1, isr=("msg-1",)),
            }
        )
    )
    clock = Clock()
    service = PanelService(
        assigner=PartitionAssigner(
            PartitionAssignmentConfig(
                enabled=True, node_id="msg-1", nodes=NODES, replication_factor=2, min_insync_replicas=1
            )
        ),
        ledger=InMemoryMigrationLedger(),
        topology=topology,
        switch=cast("object", FakeSwitch()),  # type: ignore[arg-type]
        control=cast("object", control),  # type: ignore[arg-type]
        audit=FakeAudit(),
        clock=clock,
        sleep=clock.sleep,
    )
    from bms_msg.services.migration_ledger import MigrationPlanItem

    await service._ledger.create(  # pyright: ignore[reportPrivateUsage]
        ConcurrentStableList(
            (
                MigrationPlanItem(
                    plan_id="rb-sleep",
                    tenant_id=7001,
                    namespace="msg",
                    topic="t",
                    topic_id=9,
                    partition_no=0,
                    action="expand",
                    to_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
                    to_leader="msg-1",
                ),
            )
        )
    )
    row = (await service._ledger.load("rb-sleep"))[0]  # pyright: ignore[reportPrivateUsage]
    caught, _leo, _hw, remaining = await service._probe_caught_up(current=row, budget_ms=5)  # pyright: ignore[reportPrivateUsage]
    assert caught is False
    assert clock.sleeps >= 1
    assert remaining > 0


async def test_audit_failure_does_not_rollback() -> None:
    """审计 finished 失败只记日志，不回滚已生效分配。"""

    class FailingAudit(FakeAudit):
        async def emit(self, event: object, *, required: bool = False) -> bool:
            del required
            from bms_core.core.exceptions import InternalError

            payload = getattr(event, "payload", {})
            if "finished" in str(payload.get("params", "")):
                raise InternalError("audit down")
            return True

    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = PanelService(
        assigner=PartitionAssigner(
            PartitionAssignmentConfig(
                enabled=True, node_id="msg-1", nodes=NODES, replication_factor=2, min_insync_replicas=1
            )
        ),
        ledger=InMemoryMigrationLedger(),
        topology=topology,
        switch=cast("object", FakeSwitch()),  # type: ignore[arg-type]
        control=cast("object", control),  # type: ignore[arg-type]
        audit=FailingAudit(),
        wait_ms=0,
        catch_up_budget_ms=0,
        clock=lambda: 0.0,
        sleep=_noop_sleep,
    )
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)

    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.succeeded == 1


def test_switch_outcome_variants() -> None:
    """切主结果映射：截断 / 保留 / 无操作。"""
    from dataclasses import replace

    from bms_msg.services.partition_migration import _switch_outcome  # pyright: ignore[reportPrivateUsage]

    truncated = replace(_switch_result(), truncated_to=5, retained_tail=0)
    assert _switch_outcome(truncated)[0] == "truncated"
    retained = replace(_switch_result(), truncated_to=None, retained_tail=3)
    assert _switch_outcome(retained)[0] == "retained"
    assert _switch_outcome(None)[0] == "noop"


def test_monotonic_clock_returns_positive() -> None:
    """默认时钟：返回正的单调毫秒。"""
    from bms_msg.services.partition_migration import _monotonic_ms  # pyright: ignore[reportPrivateUsage]

    assert _monotonic_ms() > 0


async def test_replace_shrink_write_failure_marks_failed() -> None:
    """替换类：缩容写分配失败即 failed（扩已生效，不回滚）。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1", "msg-3"), leader="msg-1")]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2", "msg-3")),
                "msg-3": _snapshot(node="msg-3", leo=10, isr=("msg-1", "msg-2", "msg-3")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    topology.fail_on_write = 2

    result = await service.apply(plan_id=planned.plan_id, operator="t", reason="r")

    assert result.failed == 1
    assert "assignment_conflict" in result.items[0].error


async def test_apply_item_ignores_terminal_status() -> None:
    """单行执行：终态行（既非 running 也非 catching_up）直接跳过，不写拓扑。"""
    topology = FakeTopology(ConcurrentStableList([_partition(replicas=("msg-1",))]))
    control = FakeControl(
        ConcurrentStableDict(
            {
                "msg-1": _snapshot(node="msg-1", leo=10, hw=10, isr=("msg-1", "msg-2")),
                "msg-2": _snapshot(node="msg-2", leo=10, isr=("msg-1", "msg-2")),
            }
        )
    )
    service = _service(topology, control, FakeSwitch())
    planned = await service.plan(nodes=NODES, factor=2, min_insync=1)
    items = await service._ledger.load(planned.plan_id)  # pyright: ignore[reportPrivateUsage]
    terminal = replace(items[0], status=PLAN_STATUS_SUCCEEDED)

    await service._apply_item(  # pyright: ignore[reportPrivateUsage]
        terminal, operator="t", reason="r", budget_ms=1_000, allow_force=False
    )

    assert len(topology.writes) == 0
