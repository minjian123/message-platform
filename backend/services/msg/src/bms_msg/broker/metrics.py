"""背压指标 collector（任务 03-5）。

**只收集、不暴露**：本模块提供计数与快照，HTTP / Prometheus 出口归 06-3（同一条指标面不定义两遍）。
`snapshot()` 返回纯数据结构，06-3 转成自己的格式即可。

**五个口径**（与详细设计 §11 一致）：

| 指标 | 类型 | 维度 |
| --- | --- | --- |
| `msg_inflight_messages` / `msg_inflight_bytes` | gauge（覆盖式） | 全局 + `(tenant, topic)` |
| `msg_window_stall_total` | counter | 全局 + `(tenant, topic)` |
| `msg_rate_limit_rejected_total` | counter | 全局 + `scope=tenant` / `scope=topic` |
| `msg_long_poll_waiters` | gauge | 全局（直接读等待器，不重复记一份） |
| `msg_long_poll_{changed,timeout,rejected}_total` | counter | 全局 |

**gauge 是覆盖不是累加**：`observe_inflight()` 每次写入该键的当前值；`__total__` 在读快照时求和，
避免维护两份账（两份必有一份会漂）。

**分键有界**：`(tenant, topic)` 键数超过 `topic_key_limit` 后归入 `__other__`，防高基数打爆内存。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.broker.backpressure import InflightUsage
from bms_msg.broker.waiter import DeliveryWaiter, WaitOutcome
from bms_msg.core.config import METRICS_TOPIC_KEY_LIMIT

__all__ = ["OTHER_KEY", "TOTAL_KEY", "BackpressureMetrics"]

OTHER_KEY = "__other__"
"""分键溢出后的归并键（高基数保护）。"""

TOTAL_KEY = "__total__"
"""全局合计键（读快照时求和，不单独维护）。"""


class BackpressureMetrics(BaseFrameworkObject):
    """背压指标收集器（进程级单例；判据读它、断言读它、06-3 也读它）。"""

    def __init__(self, *, waiter: DeliveryWaiter | None = None, topic_key_limit: int = METRICS_TOPIC_KEY_LIMIT) -> None:
        """初始化（全零）。

        Args:
            waiter: 分区唤醒器（取 `msg_long_poll_waiters` 的实时值；None 时该指标恒 0）。
            topic_key_limit: `(tenant, topic)` 分键上限（超出归 `__other__`）。
        """
        self._waiter = waiter
        self._topic_key_limit = topic_key_limit
        self._topic_keys: set[str] = set()
        self._inflight_messages: dict[str, int] = {}
        self._inflight_bytes: dict[str, int] = {}
        self._window_stalls: dict[str, int] = {}
        self._rate_rejected: dict[str, int] = {}
        self._wait_outcomes: dict[str, int] = {
            WaitOutcome.CHANGED.value: 0,
            WaitOutcome.TIMEOUT.value: 0,
            WaitOutcome.REJECTED.value: 0,
        }

    def observe_inflight(self, *, tenant: str, topic: str, usage: InflightUsage) -> None:
        """覆盖记录某主题的在途用量（投递后与归还后各调一次）。

        Args:
            tenant: 租户编码。
            topic: 主题名。
            usage: 当前用量。
        """
        key = self._key(tenant, topic)
        self._inflight_messages[key] = usage.messages
        self._inflight_bytes[key] = usage.bytes

    def record_window_stall(self, *, tenant: str, topic: str) -> None:
        """记一次传输层暂停（两次推进间隔超过阈值）。

        Args:
            tenant: 租户编码。
            topic: 主题名。
        """
        key = self._key(tenant, topic)
        self._window_stalls[key] = self._window_stalls.get(key, 0) + 1

    def record_rate_rejected(self, *, scope: str, tenant: str, topic: str) -> None:
        """记一次限流拒绝（按触发级别分键）。

        Args:
            scope: 触发级别（`tenant` / `topic`）。
            tenant: 租户编码。
            topic: 主题名。
        """
        label = f"scope={scope}:{tenant}|{topic}"
        self._rate_rejected[label] = self._rate_rejected.get(label, 0) + 1

    def record_wait(self, outcome: WaitOutcome) -> None:
        """记一次长轮询等待结果。

        Args:
            outcome: 等待结果（`changed` / `timeout` / `rejected`）。
        """
        self._wait_outcomes[outcome.value] = self._wait_outcomes.get(outcome.value, 0) + 1

    def snapshot(self) -> ConcurrentStableDict[str, Any]:
        """导出当前指标快照（06-3 消费；用例断言）。

        Returns:
            ConcurrentStableDict[str, Any]: 指标名 → 值 / 分键字典。
        """
        snapshot: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
        snapshot.set("msg_inflight_messages", _with_total(self._inflight_messages))
        snapshot.set("msg_inflight_bytes", _with_total(self._inflight_bytes))
        snapshot.set("msg_window_stall_total", _with_total(self._window_stalls))
        snapshot.set("msg_rate_limit_rejected_total", _with_total(self._rate_rejected))
        snapshot.set("msg_long_poll_waiters", self._waiter.waiting() if self._waiter is not None else 0)
        for outcome, count in self._wait_outcomes.items():
            snapshot.set(f"msg_long_poll_{outcome}_total", count)
        return snapshot

    def _key(self, tenant: str, topic: str) -> str:
        """把 `(tenant, topic)` 归一成有界的分键。

        Args:
            tenant: 租户编码。
            topic: 主题名。

        Returns:
            str: 分键（溢出时为 `__other__`）。
        """
        key = f"{tenant}|{topic}"
        if key in self._topic_keys:
            return key
        if len(self._topic_keys) >= self._topic_key_limit:
            return OTHER_KEY
        self._topic_keys.add(key)
        return key


def _with_total(values: dict[str, int]) -> ConcurrentStableDict[str, int]:
    """把分键字典转成「合计 + 分键」的只读快照。

    Args:
        values: 分键计数。

    Returns:
        ConcurrentStableDict[str, int]: 含 `__total__` 的快照。
    """
    result: ConcurrentStableDict[str, int] = ConcurrentStableDict()
    result.set(TOTAL_KEY, sum(values.values()))
    for key, value in values.items():
        result.set(key, value)
    return result
