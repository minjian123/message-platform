"""运维 CLI（02-8）：rebalance / failover 的退出码、JSON 与结果输出。"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.broker.election import (
    SwitchBatchResult,
    SwitchFailure,
    SwitchResult,
)
from bms_msg.services.migration_ledger import MigrationItem, MigrationPlanItem
from bms_msg.services.partition_migration import (
    MigrationApplyResult,
    MigrationBlock,
    MigrationPartition,
    MigrationPlanResult,
    MigrationStatusRow,
    MigrationStepError,
)
from ops import failover, rebalance


class FakeMigration:
    """迁移编排替身。"""

    def __init__(self) -> None:
        self.plan_result = MigrationPlanResult(
            plan_id="rb-1",
            items=ConcurrentStableList(
                [
                    MigrationPlanItem(
                        plan_id="rb-1",
                        tenant_id=7001,
                        namespace="msg",
                        topic="t",
                        topic_id=9,
                        partition_no=0,
                        action="expand",
                        from_replicas=ConcurrentStableFrozenList(("msg-1",)),
                        to_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
                        from_leader="msg-1",
                        to_leader="msg-1",
                    )
                ]
            ),
        )
        self.plan_error: MigrationStepError | None = None
        self.status_rows: ConcurrentStableList[MigrationStatusRow] = ConcurrentStableList()

    async def plan(self, **kwargs: object) -> MigrationPlanResult:
        del kwargs
        if self.plan_error is not None:
            raise self.plan_error
        return self.plan_result

    async def apply(self, **kwargs: object) -> MigrationApplyResult:
        del kwargs
        return MigrationApplyResult(plan_id="rb-1", items=self._items())

    async def status(self, **kwargs: object) -> ConcurrentStableList[MigrationStatusRow]:
        del kwargs
        return self.status_rows

    def _items(self) -> ConcurrentStableList[MigrationItem]:
        return ConcurrentStableList()


def _item(status: str, *, error: str = "") -> MigrationItem:
    """构造台账行。"""
    now = datetime(2026, 11, 12, tzinfo=UTC)
    return MigrationItem(
        id=1,
        plan_id="rb-1",
        tenant_id=7001,
        namespace="msg",
        topic="t",
        topic_id=9,
        partition_no=0,
        attempt=1,
        action="expand",
        step="",
        from_replicas=ConcurrentStableFrozenList(("msg-1",)),
        to_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        from_leader="msg-1",
        to_leader="msg-1",
        status=status,
        result="",
        isr_verified=True,
        retained_tail=0,
        progress_leo=0,
        progress_hw=0,
        remaining_records=0,
        operator="t",
        reason="r",
        error=error,
        started_at=now,
        finished_at=None,
        version=2,
    )


async def test_rebalance_plan_blocked_and_json(capsys: pytest.CaptureFixture[str]) -> None:
    """rebalance plan：有阻断退 2；JSON 输出含 items / blocks。"""
    service = FakeMigration()
    service.plan_result = MigrationPlanResult(
        plan_id="rb-1",
        items=ConcurrentStableList(),
        blocks=ConcurrentStableList(
            (MigrationBlock(topic_id=9, partition_no=0, reason="leader_unreachable", detail="x"),)
        ),
    )

    code = await rebalance.run_plan(
        cast("object", service),  # type: ignore[arg-type]
        nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        factor=2,
        min_insync=1,
        topic_ids=None,
        as_json=True,
    )
    assert code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["blocks"][0]["reason"] == "leader_unreachable"


async def test_rebalance_plan_error_and_ok(capsys: pytest.CaptureFixture[str]) -> None:
    """rebalance plan：参数错误退 2；无阻断退 0 并渲染表。"""
    service = FakeMigration()
    service.plan_error = MigrationStepError("invalid_argument", "bad")
    assert (
        await rebalance.run_plan(
            cast("object", service),  # type: ignore[arg-type]
            nodes=ConcurrentStableFrozenList(("msg-1",)),
            factor=1,
            min_insync=1,
            topic_ids=None,
            as_json=False,
        )
        == 2
    )
    service.plan_error = None
    assert (
        await rebalance.run_plan(
            cast("object", service),  # type: ignore[arg-type]
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            factor=2,
            min_insync=1,
            topic_ids=None,
            as_json=False,
        )
        == 0
    )
    assert "plan_id: rb-1" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("status", "expected"),
    [("succeeded", 0), ("catching_up", 2), ("degraded", 2), ("failed", 1)],
)
async def test_rebalance_apply_exit_codes(status: str, expected: int) -> None:
    """rebalance apply：成功 0 / 追平或降级 2 / 失败 1。"""
    service = FakeMigration()
    service.apply = _apply_returning(ConcurrentStableList((_item(status),)))  # type: ignore[method-assign]

    code = await rebalance.run_apply(
        cast("object", service),  # type: ignore[arg-type]
        plan_id="rb-1",
        operator="t",
        reason="r",
        wait_ms=0,
        allow_force=False,
        allow_degraded=False,
        resume=False,
        as_json=False,
    )
    assert code == expected


def _apply_returning(items: ConcurrentStableList[MigrationItem]) -> object:
    """构造返回固定台账的 apply 替身。"""

    async def _apply(**kwargs: object) -> MigrationApplyResult:
        del kwargs
        return MigrationApplyResult(plan_id="rb-1", items=items)

    return _apply


async def test_rebalance_status_json(capsys: pytest.CaptureFixture[str]) -> None:
    """rebalance status：输出台账 + 实时进度。"""
    service = FakeMigration()
    service.status_rows = ConcurrentStableList((MigrationStatusRow(item=_item("succeeded"), caught_up=True),))

    assert await rebalance.run_status(cast("object", service), plan_id="rb-1", as_json=True) == 0  # type: ignore[arg-type]
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["caught_up"] is True


class FakeSwitch:
    """切主服务替身。"""

    def __init__(self) -> None:
        self.result = SwitchResult(
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
            isr_verified=True,
            duration_ms=5,
        )
        self.batch: SwitchBatchResult | None = None

    async def switch_partition(self, request: object) -> SwitchResult:
        del request
        return self.result

    async def switch_topic(self, request: object) -> SwitchBatchResult:
        del request
        assert self.batch is not None
        return self.batch


async def test_failover_switch_single_and_batch(capsys: pytest.CaptureFixture[str]) -> None:
    """failover switch：单分区输出结果；整主题失败退 1、降级退 2。"""
    service = FakeSwitch()
    assert (
        await failover.run_switch(
            cast("object", service),  # type: ignore[arg-type]
            topic_id=9,
            partition_no=0,
            target="msg-2",
            operator="system:drill",
            reason="drill",
            force=False,
            as_json=True,
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["to_leader"] == "msg-2"

    service.batch = SwitchBatchResult(
        items=ConcurrentStableFrozenList((service.result,)),
        failures=ConcurrentStableFrozenList(
            (SwitchFailure(topic_id=9, partition_no=1, reason="no_candidate", detail="x"),)
        ),
    )
    assert (
        await failover.run_switch(
            cast("object", service),  # type: ignore[arg-type]
            topic_id=9,
            partition_no=None,
            target=None,
            operator="system:drill",
            reason="drill",
            force=True,
            as_json=False,
        )
        == 1
    )

    degraded = service.result
    service.batch = SwitchBatchResult(
        items=ConcurrentStableFrozenList((replace(degraded, isr_verified=False),)),
        failures=ConcurrentStableFrozenList(),
    )
    assert (
        await failover.run_switch(
            cast("object", service),  # type: ignore[arg-type]
            topic_id=9,
            partition_no=None,
            target=None,
            operator="system:drill",
            reason="drill",
            force=True,
            as_json=False,
        )
        == 2
    )


async def test_failover_status(capsys: pytest.CaptureFixture[str]) -> None:
    """failover status：输出分区 leader / epoch / 切换状态。"""

    class FakeTopology:
        async def list_partitions(self, *, topic_ids: object = None) -> ConcurrentStableList[MigrationPartition]:
            del topic_ids
            return ConcurrentStableList(
                (
                    MigrationPartition(
                        tenant_id=7001,
                        namespace="msg",
                        topic="t",
                        topic_id=9,
                        partition_no=0,
                        leader="msg-1",
                        replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
                        epoch=1,
                        switch_state="idle",
                        version=1,
                    ),
                )
            )

    assert await failover.run_status(cast("object", FakeTopology()), topic_id=9, as_json=True) == 0  # type: ignore[arg-type]
    payload = json.loads(capsys.readouterr().out)
    assert payload["partitions"][0]["leader"] == "msg-1"
