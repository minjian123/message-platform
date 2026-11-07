"""记录级审计扩展（07D 任务 03 附录 D；Kiwi 2451）。

覆盖：重放终态审计事件载荷带 `record_ref`，强审计开关 `required=True` 下发给出口；
审计写入失败时异常上抛（业务失败），不再是「只告警」。
"""

from collections.abc import Mapping
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.events.base import EventEnvelope

from bms_msg.broker.replay import ReplayAuditor
from bms_msg.models.replay import MsgReplayTask

pytestmark = pytest.mark.kiwi_id(2451)


class _RecordingSink:
    """审计出口替身：记录事件与强审计开关。"""

    def __init__(self) -> None:
        """初始化（无事件）。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
        self.required: ConcurrentStableList[bool] = ConcurrentStableList()

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一次写入。

        Args:
            event: 审计事件信封。
            required: 强审计开关。

        Returns:
            bool: 恒成功。
        """
        self.events.add(event)
        self.required.add(required)
        return True


class _ExplodingSink:
    """审计出口替身：发射即抛（模拟写入失败）。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """恒抛异常。

        Args:
            event: 审计事件信封（忽略）。
            required: 强审计开关（忽略）。

        Returns:
            bool: 不返回。

        Raises:
            RuntimeError: 模拟审计写入失败。
        """
        del event, required
        raise RuntimeError("审计写入失败")


def _task() -> MsgReplayTask:
    """构造最小重放任务替身（`_emit` 只读取下列字段）。

    Returns:
        MsgReplayTask: 具备审计所需属性的任务对象。
    """
    return cast(
        "MsgReplayTask",
        SimpleNamespace(
            id=123,
            tenant_id=7001,
            created_by=None,
            namespace="msg",
            topic="audit.operation",
            topic_id=1,
            partitions=ConcurrentStableList([0]),
            rate_per_second=10,
            status="completed",
            total=1,
            replayed=1,
            offset_from=None,
            offset_to=None,
            time_from=None,
            time_to=None,
            filter_key=None,
            log_id_first="l-1",
            log_id_last="l-1",
            error_msg=None,
            finished_at=None,
        ),
    )


async def test_replay_audit_carries_record_ref() -> None:
    """终态审计载荷带 `record_ref = {type: msg.replay, id: 123}`，可按 id 一次定位。"""
    sink = _RecordingSink()
    auditor = ReplayAuditor(sink=sink)

    assert await auditor.record_finished(task=_task()) is True
    event = sink.events[0]
    ref = cast("Mapping[str, object]", event.payload["record_ref"])
    assert ref["type"] == "msg.replay"
    assert ref["id"] == "123"


async def test_replay_audit_uses_required_emit() -> None:
    """记录级操作按强审计写入：`required=True` 下发给出口。"""
    sink = _RecordingSink()
    await ReplayAuditor(sink=sink).record_finished(task=_task())
    assert list(sink.required) == [True]


async def test_replay_audit_failure_propagates() -> None:
    """强审计写入失败即上抛使业务失败（附录 D 已确认的行为变更）。"""
    with pytest.raises(RuntimeError, match="审计写入失败"):
        await ReplayAuditor(sink=_ExplodingSink()).record_finished(task=_task())
