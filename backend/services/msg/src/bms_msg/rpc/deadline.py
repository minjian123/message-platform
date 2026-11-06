"""RPC 预算（deadline）值对象（任务 04-2）：相对剩余毫秒的解析、扣减与下游预算。

**只存「本跳收到的剩余预算 + 收到时刻」**：绝对时间戳要求全链路时钟同步（NTP 偏差即误判），
相对量跨机没有前提；与 gRPC `grpc-timeout` 同一取向。

**锚点用 `time.monotonic()`**：墙钟回拨（NTP 校正、运维改时间）会让「已用毫秒」变负或跳变，
而预算判定不能因为改时间而失效。

**判据只有一份**：`validate_budget_ms` 同时服务服务端两形态、客户端与用例——JSON 路径由 pydantic
的 `ge` / `le` 先拦、帧路径在 `Deadline.start()` 里用同一函数判，两侧结论不会分叉（`bool` 与越界
在一侧被拒、另一侧被放行是最难查的那类不一致）。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from bms_core.core.exceptions import ParamError
from bms_core.core.roots import BaseValueObject

from bms_msg.core.config import (
    MS_PER_SECOND,
    RPC_DEADLINE_MAX_MS,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_RETURN_RESERVE_MS,
)
from bms_msg.errors import Outcome

__all__ = ["OUTCOME_UNKNOWN", "Deadline", "validate_budget_ms"]

OUTCOME_UNKNOWN = Outcome.UNKNOWN
"""超时的结果语义：**结果未知**（可能已生效、也可能没跑），不是「失败」。

04-4 已升为 `Outcome` 正式枚举；这里是别名，`Outcome` 是 `StrEnum`，值仍是字符串 `"unknown"`，
因此 04-2 既有的 `detail.outcome == "unknown"` 断言与响应形状零破坏。
"""


def _clock(now: float | None) -> float:
    """取当前单调时刻（用例可注入固定值，避免 sleep 猜时序）。

    Args:
        now: 注入的时刻；None 表示取真实单调时钟。

    Returns:
        float: 单调秒。
    """
    return time.monotonic() if now is None else now


def validate_budget_ms(raw: object | None, *, default: int = RPC_DEFAULT_TIMEOUT_MS) -> int:
    """校验并归一预算值（缺省 / 整数 / 区间）。

    Args:
        raw: 原始值（帧头、JSON 体或客户端参数）。
        default: 缺失时取的缺省预算（毫秒）。

    Returns:
        int: 合法预算（毫秒）。

    Raises:
        ParamError: 非整数 / 布尔 / 越界（契约层 `10001`）。
    """
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, int):
        # `bool` 是 `int` 子类：不单独排掉，`True` 会被当成 1ms 静默接受。
        raise ParamError(f"deadline_ms 必须是整数（毫秒），收到 {type(raw).__name__}")
    if raw < 0 or raw > RPC_DEADLINE_MAX_MS:
        raise ParamError(f"deadline_ms 超出范围 0~{RPC_DEADLINE_MAX_MS}：{raw}")
    return raw


@dataclass(frozen=True)
class Deadline(BaseValueObject):
    """一次调用在本跳的预算视图（冻结：判定过程不改它，只读时刻）。"""

    budget_ms: int
    """本跳收到时的剩余预算（毫秒）。"""

    entered_at: float
    """收到时刻（`time.monotonic()` 秒）。"""

    @classmethod
    def start(cls, raw: object | None, *, now: float | None = None) -> Deadline:
        """按原始预算值构造（校验 + 打锚点）。

        Args:
            raw: 原始预算值（帧头 / JSON 体字段；None 取缺省）。
            now: 注入的锚点时刻（用例用；None 取真实单调时钟）。

        Returns:
            Deadline: 预算视图。

        Raises:
            ParamError: 预算值非法（`10001`）。
        """
        return cls(budget_ms=validate_budget_ms(raw), entered_at=_clock(now))

    def spent_ms(self, now: float | None = None) -> int:
        """已用毫秒（自锚点起，含排队 / 校验 / 限流 / 路由的耗时）。

        Args:
            now: 注入的当前时刻（None 取真实单调时钟）。

        Returns:
            int: 已用毫秒（向下取整）。
        """
        return int((_clock(now) - self.entered_at) * MS_PER_SECOND)

    def remaining_ms(self, now: float | None = None) -> int:
        """剩余毫秒（夹取到 0，响应头里不出现负数）。

        Args:
            now: 注入的当前时刻。

        Returns:
            int: 剩余毫秒。
        """
        return max(0, self.budget_ms - self.spent_ms(now))

    def is_exhausted(self, now: float | None = None) -> bool:
        """预算是否已耗尽（`已用 >= 预算`）。

        Args:
            now: 注入的当前时刻。

        Returns:
            bool: 耗尽为 True。
        """
        return self.spent_ms(now) >= self.budget_ms

    def downstream_ms(self, now: float | None = None) -> int:
        """传给下游的预算（剩余 − 返回余量，夹取到 0）。

        Args:
            now: 注入的当前时刻。

        Returns:
            int: 下游预算（毫秒）；`0` 表示下游应直接拒（连回程都不够）。
        """
        return max(0, self.remaining_ms(now) - RPC_RETURN_RESERVE_MS)
