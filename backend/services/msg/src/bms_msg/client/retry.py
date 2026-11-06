"""客户端重试策略（04-4）：值对象 / 退避与抖动 / 调用链预算 / 重试判定。

**为什么单独一个模块**：`client/rpc.py` 只负责「组帧 → 发送 → 解释」，重试是另一层策略。
拆开后策略可以被 04-5 适配层与 04-6 换实例重试直接复用，且每个纯函数都能在 fake clock /
固定随机源下逐条断言，不需要起网络。

**两个可注入端口**：`Clock`（单调毫秒 + 可等待）与 `JitterSource`（整数均匀随机数）。生产实现
`SystemClock` / `SystemJitter` 走 `asyncio.sleep` / `random`；用例注入确定性替身，退避序列可精确断言。

**预算随调用链共享**：进程内用 `ContextVar` 承载 `RetryBudget`，跨服务用帧头 `retry_budget` 传递。
上游 handler 里发起的下游调用会复用同一个预算对象，A→B→C 不会各重试 2 次凑成 8 次。
"""

from __future__ import annotations

import asyncio
import random
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from bms_core.core.roots import BaseValueObject

from bms_msg.core.config import (
    MS_PER_SECOND,
    RPC_CIRCUIT_FAILURE_THRESHOLD,
    RPC_CIRCUIT_HALF_OPEN_MAX_CALLS,
    RPC_CIRCUIT_OPEN_MS,
    RPC_CIRCUIT_WINDOW_MS,
    RPC_RETRY_BASE_MS,
    RPC_RETRY_CAP_MS,
    RPC_RETRY_MAX_ATTEMPTS,
    RPC_RETRY_MAX_BUDGET,
    RPC_RETRY_MULTIPLIER,
)
from bms_msg.errors import ErrorCategory

__all__ = [
    "CircuitPolicy",
    "Clock",
    "JitterMode",
    "JitterSource",
    "RetryBudget",
    "RetryDecision",
    "RetryPolicy",
    "RpcRetryBlockedError",
    "SystemClock",
    "SystemJitter",
    "acquire_budget",
    "compute_backoff_ms",
    "decide_retry",
    "parse_retry_after",
    "release_budget",
]


class JitterMode(StrEnum):
    """抖动形态。"""

    FULL = "full"
    """全抖动：在 `[1, cap]` 内均匀取值，抗惊群最优（生产）。"""

    NONE = "none"
    """不抖动：取指数基线原值。**仅供对照用例**，生产禁用。"""


@dataclass(frozen=True)
class RetryPolicy(BaseValueObject):
    """重试策略值对象（构造注入 + 逐调用覆盖；默认值写 `core/config.py`）。"""

    max_retries: int = RPC_RETRY_MAX_ATTEMPTS
    """最大重试次数（不含首次）。"""

    base_ms: int = RPC_RETRY_BASE_MS
    """初始退避基数（毫秒）。"""

    multiplier: int = RPC_RETRY_MULTIPLIER
    """退避倍数。"""

    cap_ms: int = RPC_RETRY_CAP_MS
    """退避与 `Retry-After` 的共同封顶（毫秒）。"""

    jitter: JitterMode = JitterMode.FULL
    """抖动形态。"""

    max_budget: int = RPC_RETRY_MAX_BUDGET
    """调用链共享的重试预算上限。"""

    auto_idempotency_key: bool = True
    """未显式给键、且未声明幂等时是否自动生成稳定幂等键。

    关闭后（如纯查询网关、或已有外部幂等层的调用方）无键调用遇到可重试错误即被阻止，
    这正是任务「无键的非幂等操作不得自动重试」的可测路径。
    """


@dataclass(frozen=True)
class CircuitPolicy(BaseValueObject):
    """熔断三态参数（04-4 决策 D4）。"""

    failure_threshold: int = RPC_CIRCUIT_FAILURE_THRESHOLD
    """窗口内连续失败达到该次数即打开。"""

    window_ms: int = RPC_CIRCUIT_WINDOW_MS
    """连续失败计数的有效窗口（毫秒）。"""

    open_ms: int = RPC_CIRCUIT_OPEN_MS
    """打开态时长（毫秒），到点转半开。"""

    half_open_max_calls: int = RPC_CIRCUIT_HALF_OPEN_MAX_CALLS
    """半开期最大在途探测数。"""


@dataclass
class RetryBudget:
    """一次调用链剩余的可重试次数（可变：每次重试递减）。

    **不是 frozen**：预算的本质就是被消费；跨协程共享同一实例，所有跳看到同一份余额。
    """

    remaining: int

    def consume(self) -> None:
        """消费一次重试额度（调用方保证 `remaining > 0`）。"""
        self.remaining -= 1


_BUDGET: ContextVar[RetryBudget | None] = ContextVar("msg_rpc_retry_budget", default=None)
"""进程内调用链预算；`None` 表示当前不在任何 RPC 调用链里。"""


def acquire_budget(policy: RetryPolicy) -> tuple[RetryBudget, Token[RetryBudget | None] | None]:
    """取当前调用链的预算（有上游则复用，没有则新建并绑定）。

    Args:
        policy: 重试策略（新建时取 `min(max_retries, max_budget)` 作初值）。

    Returns:
        tuple: `(预算, 绑定令牌)`；复用上游时令牌为 `None`（不重复绑定 / 解绑）。
    """
    upstream = _BUDGET.get()
    if upstream is not None:
        return upstream, None
    budget = RetryBudget(min(policy.max_retries, policy.max_budget))
    return budget, _BUDGET.set(budget)


def release_budget(token: Token[RetryBudget | None] | None) -> None:
    """解绑本调用链新建的预算（复用上游时什么都不做）。

    Args:
        token: `acquire_budget` 返回的令牌；`None` 表示复用上游。
    """
    if token is not None:
        _BUDGET.reset(token)


class Clock(Protocol):
    """可注入时钟（用例用 fake clock，生产用 `SystemClock`）。"""

    def now_ms(self) -> int:
        """当前单调毫秒。"""
        ...

    async def sleep_ms(self, ms: int) -> None:
        """等待指定毫秒。"""
        ...


class JitterSource(Protocol):
    """可注入抖动随机源（闭区间整数毫秒）。"""

    def uniform(self, low: int, high: int) -> int:
        """在 `[low, high]` 内取一个整数。"""
        ...


class SystemClock:
    """生产时钟：`time.monotonic()` + `asyncio.sleep`。"""

    def now_ms(self) -> int:
        """当前单调毫秒。

        Returns:
            int: 单调毫秒。
        """
        return int(time.monotonic() * MS_PER_SECOND)

    async def sleep_ms(self, ms: int) -> None:
        """等待指定毫秒。

        Args:
            ms: 毫秒数。
        """
        await asyncio.sleep(ms / MS_PER_SECOND)


class SystemJitter:
    """生产抖动源：`random.randint`。"""

    def uniform(self, low: int, high: int) -> int:
        """在 `[low, high]` 内取一个整数。

        Args:
            low: 下界（含）。
            high: 上界（含）。

        Returns:
            int: 随机整数。
        """
        return random.randint(low, high)


def parse_retry_after(headers: tuple[tuple[str, str], ...]) -> int | None:
    """解析 HTTP `Retry-After`（只认 delta-seconds，转毫秒）。

    HTTP-date 形态需要时区 / 日期解析，本任务明确不解析（决策 D3）；非法 / 负值忽略，
    回落本地退避——错误路径不该再抛错。头名大小写不敏感。

    Args:
        headers: 响应头（`(name, value)` 序列）。

    Returns:
        int | None: 建议等待毫秒；无合法建议为 `None`。
    """
    for name, value in headers:
        if name.lower() != "retry-after":
            continue
        text = value.strip()
        if not text.isascii() or not text.isdecimal():
            return None
        return int(text) * MS_PER_SECOND
    return None


def compute_backoff_ms(attempt: int, policy: RetryPolicy, jitter: JitterSource) -> int:
    """计算第 `attempt` 次重试的等待毫秒（指数基线 + full jitter + 封顶）。

    Args:
        attempt: 重试序号（从 1 起）。
        policy: 重试策略。
        jitter: 抖动随机源。

    Returns:
        int: 等待毫秒，恒 `>= 1`（禁止无退避的立即重试）。
    """
    raw_cap = min(policy.cap_ms, policy.base_ms * policy.multiplier ** (attempt - 1))
    if policy.jitter is JitterMode.NONE:
        return max(1, raw_cap)
    # 夹到 raw_cap：随机源若（被替换成）返回越界值，也不能突破封顶承诺。
    return min(max(1, raw_cap), max(1, jitter.uniform(1, max(1, raw_cap))))


@dataclass(frozen=True)
class RetryDecision:
    """一次「允许重试」的判定结果。"""

    delay_ms: int
    """本次应等待的毫秒（已含 `Retry-After` 取 max 与封顶）。"""


class RpcRetryBlockedError(Exception):
    """非幂等且无幂等键：重试被阻止（本地异常，不新增产品码）。

    携带最终错误对象，调用方能同时拿到原始码 / `category` / 尝试次数 / 退避序列。
    """

    def __init__(self, error: object) -> None:
        """初始化。

        Args:
            error: 触发阻止的 `RpcCallError`（带尝试记录）。
        """
        super().__init__("非幂等且无幂等键的 RPC 调用不允许自动重试")
        self.error = error


def decide_retry(
    error: object,
    *,
    attempts: int,
    policy: RetryPolicy,
    budget: RetryBudget,
    idempotency_key: str | None,
    idempotent: bool,
    retry_after_ms: int | None,
    jitter: JitterSource,
) -> RetryDecision | None:
    """判定本次错误能否重试（设计 §5.1 的六步短路，顺序不可调）。

    `retryable` 表示「可否常规重试」，`category == unknown` 是**独立于它**的特例：结果未知
    只在带幂等键时才谨慎重试。两者不是同一件事。

    Args:
        error: 本次 `RpcCallError`（读 `category` / `retryable`）。
        attempts: 已完成的重试次数（不含首次）。
        policy: 重试策略。
        budget: 调用链共享预算。
        idempotency_key: 本次调用的幂等键（重试全程稳定）。
        idempotent: 调用方是否显式声明方法幂等。
        retry_after_ms: 服务端建议等待毫秒（帧头 / HTTP 头解析结果）。
        jitter: 抖动随机源。

    Returns:
        RetryDecision | None: 允许重试的判定；`None` 表示上抛最终错误。

    Raises:
        RpcRetryBlockedError: 非幂等且无键（不发起任何重试）。
    """
    category = getattr(error, "category", ErrorCategory.SERVER_BUG)
    retryable = bool(getattr(error, "retryable", False))
    if category in (ErrorCategory.PARAM_ERROR, ErrorCategory.BUSINESS_REJECT, ErrorCategory.SERVER_BUG):
        return None
    if category is ErrorCategory.UNKNOWN and idempotency_key is None:
        return None
    if category is ErrorCategory.TRANSIENT and not retryable:
        return None
    if attempts >= policy.max_retries or budget.remaining <= 0:
        return None
    if not (idempotent or idempotency_key is not None):
        raise RpcRetryBlockedError(error)
    backoff = compute_backoff_ms(attempts + 1, policy, jitter)
    delay = min(policy.cap_ms, max(backoff, retry_after_ms or 0)) if retry_after_ms else backoff
    return RetryDecision(delay_ms=delay)
