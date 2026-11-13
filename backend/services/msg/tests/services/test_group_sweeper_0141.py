"""空组回收扫描（01-4-1；Kiwi 2496）：只回收空组、幂等、启停。"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.broker.group import (
    GROUP_STATE_DEAD,
    GROUP_STATE_EMPTY,
    GROUP_STATE_STABLE,
    ConsumerGroup,
    InMemoryGroupStore,
)
from bms_msg.services.group_sweeper import GroupSweeper

pytestmark = pytest.mark.kiwi_id(2496)

TENANT_ID = 7001
TOPIC_ID = 5_550_002
NAMESPACE = "msg"


def _record(group: str, *, state: str) -> ConsumerGroup:
    """构造组快照（无成员）。"""
    return ConsumerGroup(
        tenant_id=TENANT_ID,
        topic_id=TOPIC_ID,
        group=group,
        namespace=NAMESPACE,
        state=state,
        generation=0,
        members=ConcurrentStableList(),
    )


async def test_run_once_marks_only_idle_empty_groups() -> None:
    """空组超期 → `dead`；有成员的组不动；重复执行幂等。"""
    store = InMemoryGroupStore()
    await store.save(_record("idle", state=GROUP_STATE_EMPTY))
    await store.save(_record("busy", state=GROUP_STATE_STABLE))

    later = datetime.now(UTC) + timedelta(days=2)
    sweeper = GroupSweeper(groups=store, idle_days=1, now=lambda: later)

    assert await sweeper.run_once() == 1
    groups = {record.group: record.state for record in await store.list_all()}
    assert groups == {"idle": GROUP_STATE_DEAD, "busy": GROUP_STATE_STABLE}
    assert await sweeper.run_once() == 0


async def test_start_and_stop_is_idempotent() -> None:
    """启停幂等：运行标记正确，重复 stop 无副作用。"""
    sweeper = GroupSweeper(groups=InMemoryGroupStore(), interval_seconds=3600.0)
    sweeper.ensure_started()
    assert sweeper.running is True
    await sweeper.stop()
    assert sweeper.running is False
    await sweeper.stop()


class _BrokenStore(InMemoryGroupStore):
    """组存储替身：回收恒抛错（覆盖扫描循环的告警分支）。"""

    async def mark_idle_dead(self, *, before: datetime) -> int:
        """恒抛错。

        Args:
            before: 空闲判定截止时刻（忽略）。

        Raises:
            OSError: 恒抛。
        """
        del before
        raise OSError("db down")


@pytest.mark.kiwi_id(2496)
async def test_run_once_uses_default_clock() -> None:
    """默认时钟路径可用：空组超期即回收。"""
    store = InMemoryGroupStore()
    await store.save(_record("idle", state=GROUP_STATE_EMPTY))
    sweeper = GroupSweeper(groups=store, idle_days=0)
    assert await sweeper.run_once() == 1


@pytest.mark.kiwi_id(2496)
async def test_loop_swallows_errors_and_start_is_idempotent() -> None:
    """扫描循环吞掉单轮异常；重复 `ensure_started` 不起第二个循环。"""
    sweeper = GroupSweeper(groups=_BrokenStore(), interval_seconds=0.01)
    sweeper.ensure_started()
    sweeper.ensure_started()
    await asyncio.sleep(0.03)
    await sweeper.stop()
    assert sweeper.running is False


class _BlockingStore(InMemoryGroupStore):
    """组存储替身：回收挂起，直到被取消（覆盖循环的取消分支）。"""

    def __init__(self) -> None:
        """初始化进入事件。"""
        super().__init__()
        self.entered = asyncio.Event()
        self._wait = asyncio.Event()

    async def mark_idle_dead(self, *, before: datetime) -> int:
        """先标记进入，再挂起直到取消。

        Args:
            before: 空闲判定截止时刻（忽略）。

        Returns:
            int: 不会返回。
        """
        del before
        self.entered.set()
        await self._wait.wait()
        return 0


@pytest.mark.kiwi_id(2496)
async def test_loop_propagates_cancellation() -> None:
    """扫描进行中被停止：取消原样抛出（不被异常兜底吞掉）。"""
    store = _BlockingStore()
    sweeper = GroupSweeper(groups=store, interval_seconds=3600.0)
    sweeper.ensure_started()
    await store.entered.wait()
    await sweeper.stop()
    assert sweeper.running is False
