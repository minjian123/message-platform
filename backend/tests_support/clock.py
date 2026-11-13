"""可手动推进的测试时钟（跨用例文件共享的测试辅助类型）。

**为什么单独放这里**：`services/msg/tests` 不是包，用例里写 `from tests.conftest import ManualClock`
依赖 pytest 对同名包 `tests` 的解析结果——全量收集时它可能先解析到仓内另一个 `tests` 命名空间，
在 Linux CI 上表现为 `ModuleNotFoundError: No module named 'tests.conftest'`（2026-11-13 msg#1614，
本地单文件跑得通、CI 全量必崩）。与 `tests_support.msg_fixtures` 同一口径：**跨文件共享的测试辅助
一律放 `tests_support`**，用例只从本包导入。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

__all__ = ["ManualClock"]


class ManualClock:
    """可手动推进的时钟（模拟可见性超时，用例不 sleep）。"""

    def __init__(self) -> None:
        """初始化（固定起点 2026-01-01T00:00:00Z）。"""
        self._now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        """当前时间。

        Returns:
            datetime: 当前（可推进的）UTC 时间。
        """
        return self._now

    def advance(self, milliseconds: int) -> None:
        """推进时钟。

        Args:
            milliseconds: 推进的毫秒数。
        """
        self._now += timedelta(milliseconds=milliseconds)
