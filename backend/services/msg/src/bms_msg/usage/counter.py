"""今日投递计数（进程内，UTC 日切）。

**为什么是进程内**：`delivered_today` 是观测值，不是账本；重启清零可接受，多实例一致性归 8B_05
（届时换共享计数存储，端口不变）。计数只增不复用，日切时整体归零。
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime

__all__ = ["DeliveryCounter", "default_delivery_counter", "reset_default_delivery_counter"]


def _utc_today() -> date:
    """当前 UTC 日期。

    Returns:
        date: UTC 日期。
    """
    return datetime.now(UTC).date()


class DeliveryCounter:
    """今日投递计数器（UTC 日切；增量 + 读取）。"""

    def __init__(self, *, today: Callable[[], date] | None = None) -> None:
        """初始化。

        Args:
            today: 日期提供者（缺省 UTC 当前日期；用例注入固定日期）。
        """
        self._today = today if today is not None else _utc_today
        self._day = self._today()
        self._count = 0

    def _roll(self) -> None:
        """跨日归零（惰性：每次访问时判定）。"""
        current = self._today()
        if current != self._day:
            self._day = current
            self._count = 0

    def increment(self, amount: int = 1) -> None:
        """累加投递条数。

        Args:
            amount: 本次新增条数（负值按 0 处理，避免统计被打负）。
        """
        self._roll()
        self._count += max(int(amount), 0)

    def value(self) -> int:
        """取今日累计投递条数。

        Returns:
            int: 条数（日切后为 0）。
        """
        self._roll()
        return self._count

    def reset(self) -> None:
        """清零（测试隔离 / 运维复位）。"""
        self._day = self._today()
        self._count = 0


_DEFAULT_COUNTER = DeliveryCounter()
"""进程级缺省计数器：所有投递路径（JSON / 帧 / 长驻流）共用，避免逐路透传漏计。"""


def default_delivery_counter() -> DeliveryCounter:
    """取进程级缺省计数器。

    Returns:
        DeliveryCounter: 计数器单例。
    """
    return _DEFAULT_COUNTER


def reset_default_delivery_counter() -> None:
    """清零进程级缺省计数器（测试隔离）。"""
    _DEFAULT_COUNTER.reset()
