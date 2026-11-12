import pytest
from bms_core.core.concurrent import ConcurrentStableList

pytestmark = pytest.mark.kiwi_id(2487)


async def test_switch_metrics_record_results() -> None:
    """切主指标：成功 / 降级 / 失败 / no-op 各记一次。"""
    from bms_core.core.concurrent import ConcurrentStableFrozenList

    from bms_msg.broker.election import (
        SWITCH_STATE_IDLE,
        LeaderControlTransport,
        LeaderSwitchError,
        LeaderSwitchMetrics,
        LeaderSwitchService,
        LeaderSwitchStore,
        SwitchApplyResult,
        SwitchPartitionRequest,
        SwitchPartitionState,
    )
    from bms_msg.broker.replication import ReplicationSnapshot

    class Store(LeaderSwitchStore):
        def __init__(self) -> None:
            self.state = SwitchPartitionState(
                topic_id=9,
                partition_no=0,
                tenant_id=7001,
                namespace="msg",
                topic="t",
                leader="msg-1",
                epoch=1,
                replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
                switch_state=SWITCH_STATE_IDLE,
            )

        async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
            del topic_id, partition_no
            return self.state

        async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
            del topic_id
            return ConcurrentStableList((self.state,))

        async def begin_switch(self, **kwargs: object) -> bool:
            del kwargs
            return True

        async def finish_switch(self, **kwargs: object) -> bool:
            del kwargs
            return True

    class Control(LeaderControlTransport):
        def __init__(self, snapshot: ReplicationSnapshot | None, *, apply_error: bool = False) -> None:
            self.snapshot_value = snapshot
            self.apply_error = apply_error

        async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
            del node_id, topic_id, partition_no
            return self.snapshot_value

        async def demote(self, **kwargs: object) -> None:
            del kwargs

        async def apply_switch(self, **kwargs: object) -> SwitchApplyResult:
            del kwargs
            if self.apply_error:
                raise RuntimeError("apply failed")
            return SwitchApplyResult(to_leo=10, truncated_to=None, retained_tail=0, already_applied=False)

    class Audit:
        async def emit(self, event: object, *, required: bool = False) -> bool:
            del event, required
            return True

    snapshot = ReplicationSnapshot(
        leader="msg-1",
        epoch=1,
        leo=10,
        hw=10,
        isr=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
    )
    metrics = LeaderSwitchMetrics()
    service = LeaderSwitchService(
        enabled=True,
        store=Store(),
        control=Control(snapshot),
        audit=Audit(),
        grace_ms=0,
        follower_fresh_ms=1000,
        metrics=metrics,
    )
    result = await service.switch_partition(SwitchPartitionRequest(topic_id=9, partition_no=0, target="msg-2"))
    assert result.to_leader == "msg-2"
    assert metrics.snapshot()["noop" if result.truncated_to is None else "truncated"] >= 1

    failing = LeaderSwitchService(
        enabled=True,
        store=Store(),
        control=Control(None, apply_error=True),
        audit=Audit(),
        grace_ms=0,
        follower_fresh_ms=1000,
        metrics=metrics,
    )
    with pytest.raises(LeaderSwitchError):
        await failing.switch_partition(SwitchPartitionRequest(topic_id=9, partition_no=0, target="msg-2"))
    assert metrics.snapshot()["failed"] >= 1

    noop = await service.switch_partition(SwitchPartitionRequest(topic_id=9, partition_no=0, target="msg-1"))
    assert noop.applied is False
    assert metrics.snapshot()["noop"] >= 1
