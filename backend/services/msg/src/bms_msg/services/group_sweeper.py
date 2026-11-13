"""空组回收扫描器（01-4-1）：把空闲超过阈值的 `empty` 组置 `dead`。

- **只动 `empty`**：`stable` / `preparing_rebalance` 说明还有成员，交给会话超时清理；
- **保留行与位点**：`dead` 只是管理面标记，重新 `join` 即恢复，显式删除才真删；
- 进程级、幂等启停（与 `TopicCleanupService` 同形），DB 异常只告警、下一轮重试。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject

from bms_msg.broker.group import GroupStore
from bms_msg.core.config import GROUP_IDLE_DAYS, GROUP_SWEEP_INTERVAL_SECONDS

__all__ = ["GroupSweeper"]

_LOGGER = get_logger("bms_msg.group_sweeper")


def _utcnow() -> datetime:
    """当前 UTC 时间（用例注入固定时钟）。

    Returns:
        datetime: 当前 UTC 时间。
    """
    return datetime.now(UTC)


class GroupSweeper(BaseFrameworkObject):
    """空组回收扫描器（进程级；`ensure_started()` / `stop()` 幂等）。"""

    def __init__(
        self,
        *,
        groups: GroupStore,
        interval_seconds: float = GROUP_SWEEP_INTERVAL_SECONDS,
        idle_days: int = GROUP_IDLE_DAYS,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        """初始化。

        Args:
            groups: 组存储（需实现 `mark_idle_dead`）。
            interval_seconds: 扫描间隔（秒）。
            idle_days: 空闲天数阈值。
            now: 时钟（用例注入固定时钟）。
        """
        self._groups = groups
        self._interval = interval_seconds
        self._idle = timedelta(days=idle_days)
        self._now = now
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        """扫描循环是否存活（测试与幂等判定用）。

        Returns:
            bool: 存活为 True。
        """
        return self._task is not None and not self._task.done()

    def ensure_started(self) -> None:
        """启动周期扫描（幂等；需在事件循环内调用）。"""
        if self.running:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """停止扫描循环（幂等）。"""
        if self._task is None:
            return
        task = self._task
        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    async def run_once(self) -> int:
        """扫一轮：置 `dead` 并返回本次回收的组数。

        Returns:
            int: 本次置为 `dead` 的组数。
        """
        cutoff = self._now() - self._idle
        changed = await self._groups.mark_idle_dead(before=cutoff)
        if changed:
            _LOGGER.info("msg_group_marked_dead", count=changed, cutoff=cutoff.isoformat())
        return changed

    async def _loop(self) -> None:
        """周期扫描（首轮立即执行，重启后马上回收）。"""
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _LOGGER.warning("msg_group_sweep_failed", error=type(exc).__name__)
            await asyncio.sleep(self._interval)
