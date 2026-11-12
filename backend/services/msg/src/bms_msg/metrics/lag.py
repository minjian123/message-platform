"""异步指标的采样快照容器（06-3-1）：积压 / 死信存量 / 重放任务状态。

**为什么单独一层**：这三类指标的事实源是异步查询（组 / 分区 / 死信队列 / 任务表），
不能在同步的 `/metrics` 里计算。采样器负责写入，Collector 只读最近一次快照。
**键编码**：`tenant\\x1ftopic`（与 `TrafficMetrics` 同一分隔符）；重放按状态分键。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

__all__ = ["LagSnapshot", "MetricsSnapshotStore"]

_PAIR_SEPARATOR = "\x1f"
"""键内分隔符（与 `TrafficMetrics` 一致）。"""


def topic_key(*, tenant: str, topic: str) -> str:
    """拼 `(tenant, topic)` 快照键。

    Args:
        tenant: 租户编码。
        topic: 主题名。

    Returns:
        str: 快照键。
    """
    return f"{tenant}{_PAIR_SEPARATOR}{topic}"


@dataclass(frozen=True)
class LagSnapshot(BaseValueObject):
    """一次采样结果（积压 / 死信存量 / 重放任务数）。"""

    lag: ConcurrentStableFrozenDict[str, int] = field(default_factory=lambda: ConcurrentStableFrozenDict())
    """`(tenant, topic) → 积压条数`（所有组、所有分区求和）。"""

    dlq: ConcurrentStableFrozenDict[str, int] = field(default_factory=lambda: ConcurrentStableFrozenDict())
    """`(tenant, topic) → 当前死信存量`。"""

    dlq_growth: ConcurrentStableFrozenDict[str, int] = field(default_factory=lambda: ConcurrentStableFrozenDict())
    """`(tenant, topic) → 累计转入死信条数`（按相邻两次存量差值累加；重启归零）。"""

    replay: ConcurrentStableFrozenDict[str, int] = field(default_factory=lambda: ConcurrentStableFrozenDict())
    """`status → 重放任务数`。"""

    replay_replayed: int = 0
    """重放累计写入条数（任务表 `replayed` 求和）。"""

    replay_failed: int = 0
    """失败重放任务累计数（按 `failed` 状态任务数；非严格单调）。"""

    migration: ConcurrentStableFrozenDict[str, int] = field(default_factory=lambda: ConcurrentStableFrozenDict())
    """`status → 迁移台账行数`（02-8 采样器读台账）。"""

    migration_remaining: int = 0
    """所有 `catching_up` 迁移任务的剩余条数之和（02-8）。"""

    messages_in_window: int = 0
    """本采样周期的入口消息数（06-1-1：概览「入口吞吐」数字卡的分子）。"""

    window_seconds: float = 0.0
    """本采样周期的实际秒数（分母；为 0 表示尚无差分结果）。"""


class MetricsSnapshotStore(BaseFrameworkObject):
    """采样快照存储（进程级；采样器写、Collector 读）。"""

    def __init__(self) -> None:
        """初始化（空快照）。"""
        self._snapshot = LagSnapshot()

    @property
    def snapshot(self) -> LagSnapshot:
        """当前快照（整体替换，读到的是一致视图）。

        Returns:
            LagSnapshot: 最近一次采样结果。
        """
        return self._snapshot

    def replace(
        self,
        *,
        lag: ConcurrentStableDict[str, int] | None = None,
        dlq: ConcurrentStableDict[str, int] | None = None,
        dlq_growth: ConcurrentStableDict[str, int] | None = None,
        replay: ConcurrentStableDict[str, int] | None = None,
        replay_replayed: int | None = None,
        replay_failed: int | None = None,
        migration: ConcurrentStableDict[str, int] | None = None,
        migration_remaining: int | None = None,
        messages_in_window: int | None = None,
        window_seconds: float | None = None,
    ) -> None:
        """整体替换快照（未给的维度沿用上一次）。

        Args:
            lag: 积压（None 表示不更新该维度）。
            dlq: 死信存量（None 表示不更新）。
            dlq_growth: 死信累计增长（None 表示不更新）。
            replay: 重放任务数（None 表示不更新）。
            replay_replayed: 重放累计写入条数（None 表示不更新）。
            replay_failed: 失败重放任务累计数（None 表示不更新）。
            migration: 迁移台账状态计数（None 表示不更新）。
            migration_remaining: 迁移剩余条数之和（None 表示不更新）。
            messages_in_window: 本周期入口消息数（None 表示不更新）。
            window_seconds: 本周期实际秒数（None 表示不更新）。
        """
        current = self._snapshot
        self._snapshot = LagSnapshot(
            lag=ConcurrentStableFrozenDict(lag) if lag is not None else current.lag,
            dlq=ConcurrentStableFrozenDict(dlq) if dlq is not None else current.dlq,
            dlq_growth=ConcurrentStableFrozenDict(dlq_growth) if dlq_growth is not None else current.dlq_growth,
            replay=ConcurrentStableFrozenDict(replay) if replay is not None else current.replay,
            replay_replayed=replay_replayed if replay_replayed is not None else current.replay_replayed,
            replay_failed=replay_failed if replay_failed is not None else current.replay_failed,
            migration=ConcurrentStableFrozenDict(migration) if migration is not None else current.migration,
            migration_remaining=(
                migration_remaining if migration_remaining is not None else current.migration_remaining
            ),
            messages_in_window=(messages_in_window if messages_in_window is not None else current.messages_in_window),
            window_seconds=window_seconds if window_seconds is not None else current.window_seconds,
        )

    def reset(self) -> None:
        """清空快照（测试隔离用）。"""
        self._snapshot = LagSnapshot()
