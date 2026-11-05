"""接入层背压参数与生产限速（任务 03-5）。

三块内容：

1. **`InflightUsage` / `BackpressureTuning`**：单分区持有者的在途额度。条数与字节双阈值，
   超限**暂停推送**（不是错误）——消费侧暂停是正常状态，返错误会让客户端重试风暴。
2. **`TokenBucket`**：令牌桶。允许短突发（消息系统常态），长期速率受控；容量 = 速率 × 突发秒数，
   进程停摆最多攒一个突发的量（与 01-6 重放 `REPLAY_BURST_SECONDS` 同手法）。
3. **`RateLimiter`**：两级（租户 + 主题）限速。两级**都判、都扣**，任一不足即拒——只判租户会让单主题
   热点打满租户额度、连带同租户其它主题被拒；只判主题则「租户总量」无处表达（05-3 配额落地时要返工）。

**缺省关闭**（`rate = 0` 表示该级不限）：本任务只保证「一开就生效、超限可验收」，数值由 05-3（租户配额）
与 8B_12（容量定参）给；默认开启会改变既有用例与 03-1 基准的行为。

**单线程假设**：令牌桶的「判够 → 扣减」两步之间没有 `await`，在单事件循环里是原子的，故不加锁。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

from bms_core.core.objects import BaseValueObject
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.core.config import (
    INFLIGHT_MAX_BYTES,
    INFLIGHT_MAX_MESSAGES,
    MS_PER_SECOND,
    RATE_LIMIT_BUCKET_LIMIT,
    RATE_LIMIT_BURST_SECONDS,
    RATE_LIMIT_RETRY_AFTER_MS,
    RATE_LIMIT_TENANT_PER_SECOND,
    RATE_LIMIT_TOPIC_PER_SECOND,
)

__all__ = [
    "BackpressureTuning",
    "InflightUsage",
    "RateLimitTuning",
    "RateLimiter",
    "RateVerdict",
    "TokenBucket",
    "TopicKey",
    "default_backpressure_tuning",
    "default_rate_limit_tuning",
    "retry_after_ms",
]

TopicKey = tuple[str, str, str]
"""主题限流键：`(tenant, namespace, topic)`——同租户不同命名空间的同名主题互不影响。"""


@dataclass(frozen=True)
class InflightUsage(BaseValueObject):
    """在途用量（条数 + 字节），也是阈值与剩余额度的共同载体。"""

    messages: int
    """在途消息条数（已投出、未确认）。"""

    bytes: int
    """在途消息体字节合计（按 `len(payload)` 线上字节计）。"""

    def remaining(self, limit: InflightUsage) -> InflightUsage:
        """按阈值算剩余额度（逐项取 `max(0, limit - usage)`）。

        Args:
            limit: 阈值。

        Returns:
            InflightUsage: 剩余额度（不会为负）。
        """
        return InflightUsage(
            messages=max(limit.messages - self.messages, 0),
            bytes=max(limit.bytes - self.bytes, 0),
        )

    def exhausted(self, limit: InflightUsage) -> bool:
        """是否已达或超过阈值（任一维度到顶即算满）。

        Args:
            limit: 阈值。

        Returns:
            bool: 是否已满。
        """
        return self.messages >= limit.messages or self.bytes >= limit.bytes


@dataclass(frozen=True)
class BackpressureTuning(BaseValueObject):
    """inflight 双阈值（值对象；用例经依赖覆盖注入小阈值，不必造大数据）。"""

    inflight_messages: int
    """单分区持有者在途条数上限。"""

    inflight_bytes: int
    """单分区持有者在途字节上限。"""

    @property
    def limit(self) -> InflightUsage:
        """阈值本身（与 `InflightUsage` 同一形态，便于 `remaining` / `exhausted` 比较）。

        Returns:
            InflightUsage: 阈值。
        """
        return InflightUsage(messages=self.inflight_messages, bytes=self.inflight_bytes)

    @property
    def enabled(self) -> bool:
        """是否启用（任一阈值 > 0；全 0 表示不限）。

        Returns:
            bool: 是否启用。
        """
        return self.inflight_messages > 0 or self.inflight_bytes > 0


def default_backpressure_tuning() -> BackpressureTuning:
    """按模块常量构造缺省阈值。

    Returns:
        BackpressureTuning: 100 条 / 16 MiB。
    """
    return BackpressureTuning(inflight_messages=INFLIGHT_MAX_MESSAGES, inflight_bytes=INFLIGHT_MAX_BYTES)


@dataclass(frozen=True)
class RateLimitTuning(BaseValueObject):
    """生产限速参数（0 表示该级不限）。"""

    tenant_per_second: int
    """租户级速率（条/秒）。"""

    topic_per_second: int
    """主题级速率（条/秒）。"""

    burst_seconds: int
    """桶容量折算秒数（容量 = 速率 × 本值）。"""

    @property
    def enabled(self) -> bool:
        """是否启用限速（两级都为 0 即关闭）。

        Returns:
            bool: 是否启用。
        """
        return self.tenant_per_second > 0 or self.topic_per_second > 0


def default_rate_limit_tuning() -> RateLimitTuning:
    """按模块常量构造缺省限速参数（缺省关闭）。

    Returns:
        RateLimitTuning: 两级均为 0。
    """
    return RateLimitTuning(
        tenant_per_second=RATE_LIMIT_TENANT_PER_SECOND,
        topic_per_second=RATE_LIMIT_TOPIC_PER_SECOND,
        burst_seconds=RATE_LIMIT_BURST_SECONDS,
    )


@dataclass(frozen=True)
class RateVerdict(BaseValueObject):
    """一次限流判定的结果。"""

    allowed: bool
    """是否放行。"""

    retry_after_ms: int
    """建议重试间隔（毫秒；放行时为 0）。"""

    scope: str
    """触发的级别：`tenant` / `topic`（放行时为空串）。"""


class TokenBucket:
    """令牌桶（惰性补充：每次访问按 `monotonic` 补，空闲桶不占后台任务）。"""

    def __init__(self, *, rate_per_second: int, burst_seconds: int) -> None:
        """初始化（起始满桶）。

        Args:
            rate_per_second: 每秒补充令牌数（须为正）。
            burst_seconds: 容量折算秒数（容量 = 速率 × 本值，至少 1）。
        """
        self._rate = rate_per_second
        self._capacity = float(rate_per_second * max(burst_seconds, 1))
        self._tokens = self._capacity
        self._updated: float | None = None

    @property
    def capacity(self) -> float:
        """桶容量（用例断言突发边界）。

        Returns:
            float: 容量令牌数。
        """
        return self._capacity

    def _refill(self, now: float) -> None:
        """按经过时间补充令牌（首次访问只记时刻）。

        Args:
            now: 当前 `monotonic` 秒。
        """
        if self._updated is None:
            self._updated = now
            return
        elapsed = max(now - self._updated, 0.0)
        self._updated = now
        if elapsed:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

    def can(self, count: int, now: float) -> int:
        """判是否够 `count` 个令牌（**不改状态**）。

        Args:
            count: 需要的令牌数。
            now: 当前 `monotonic` 秒。

        Returns:
            int: 0 表示够；>0 为还需等待的毫秒数（至少 1）。
        """
        self._refill(now)
        if self._tokens >= count:
            return 0
        return max(math.ceil((count - self._tokens) / self._rate * MS_PER_SECOND), 1)

    def consume(self, count: int, now: float) -> None:
        """扣减令牌（调用方须先 `can()` 通过）。

        Args:
            count: 扣减数。
            now: 当前 `monotonic` 秒。
        """
        self._refill(now)
        self._tokens = max(self._tokens - count, 0.0)

    def is_full(self, now: float) -> bool:
        """桶是否已满（空闲桶可被安全清理）。

        Args:
            now: 当前 `monotonic` 秒。

        Returns:
            bool: 是否满桶。
        """
        self._refill(now)
        return self._tokens >= self._capacity


class RateLimiter(BaseFrameworkObject):
    """两级令牌桶限流（进程级单例；桶跨请求存续）。

    「都判后都扣」而非「先扣再判」：主题桶拒绝时若已扣了租户桶，租户额度会被无效请求吃掉，
    表现为「同样的负载，限流比预期更早触发」。
    """

    def __init__(self, tuning: RateLimitTuning, *, bucket_limit: int = RATE_LIMIT_BUCKET_LIMIT) -> None:
        """初始化。

        Args:
            tuning: 两级速率（0 表示该级不限）。
            bucket_limit: 桶数量上限（超出先清满桶，再按插入序淘汰最旧）。
        """
        self._tuning = tuning
        self._bucket_limit = bucket_limit
        self._tenant_buckets: dict[str, TokenBucket] = {}
        self._topic_buckets: dict[TopicKey, TokenBucket] = {}

    @property
    def tuning(self) -> RateLimitTuning:
        """当前参数（用例与实施记录口径）。

        Returns:
            RateLimitTuning: 参数。
        """
        return self._tuning

    def buckets(self) -> int:
        """当前桶总数（两级合计；用例断言清理生效）。

        Returns:
            int: 桶数量。
        """
        return len(self._tenant_buckets) + len(self._topic_buckets)

    def check(self, *, tenant: str, topic: TopicKey, now: float | None = None) -> RateVerdict:
        """判一次生产请求是否放行（放行时同时扣两级令牌）。

        Args:
            tenant: 租户编码。
            topic: 主题键 `(tenant, namespace, topic)`。
            now: 当前 `monotonic` 秒（缺省取当前时刻；用例注入固定值）。

        Returns:
            RateVerdict: 判定结果（含建议重试毫秒与触发的级别）。
        """
        if not self._tuning.enabled:
            return RateVerdict(allowed=True, retry_after_ms=0, scope="")
        moment = time.monotonic() if now is None else now
        if self._tuning.tenant_per_second > 0:
            tenant_bucket = self._tenant_bucket(tenant, moment)
            wait = tenant_bucket.can(1, moment)
            if wait:
                return RateVerdict(allowed=False, retry_after_ms=wait, scope="tenant")
        if self._tuning.topic_per_second > 0:
            topic_bucket = self._topic_bucket(topic, moment)
            wait = topic_bucket.can(1, moment)
            if wait:
                return RateVerdict(allowed=False, retry_after_ms=wait, scope="topic")
        if self._tuning.tenant_per_second > 0:
            self._tenant_buckets[tenant].consume(1, moment)
        if self._tuning.topic_per_second > 0:
            self._topic_buckets[topic].consume(1, moment)
        return RateVerdict(allowed=True, retry_after_ms=0, scope="")

    def _tenant_bucket(self, tenant: str, now: float) -> TokenBucket:
        """取或建租户桶。

        Args:
            tenant: 租户编码。
            now: 当前 `monotonic` 秒。

        Returns:
            TokenBucket: 桶实例。
        """
        bucket = self._tenant_buckets.get(tenant)
        if bucket is not None:
            return bucket
        self._prune(now)
        created = TokenBucket(rate_per_second=self._tuning.tenant_per_second, burst_seconds=self._tuning.burst_seconds)
        self._tenant_buckets[tenant] = created
        return created

    def _topic_bucket(self, topic: TopicKey, now: float) -> TokenBucket:
        """取或建主题桶。

        Args:
            topic: 主题键。
            now: 当前 `monotonic` 秒。

        Returns:
            TokenBucket: 桶实例。
        """
        bucket = self._topic_buckets.get(topic)
        if bucket is not None:
            return bucket
        self._prune(now)
        created = TokenBucket(rate_per_second=self._tuning.topic_per_second, burst_seconds=self._tuning.burst_seconds)
        self._topic_buckets[topic] = created
        return created

    def _prune(self, now: float) -> None:
        """桶数达到上限时清理：先清满桶，再按插入序淘汰最旧。

        Args:
            now: 当前 `monotonic` 秒。
        """
        if self.buckets() < self._bucket_limit:
            return
        for tenant_name in [name for name, bucket in self._tenant_buckets.items() if bucket.is_full(now)]:
            self._tenant_buckets.pop(tenant_name, None)
        for topic_name in [name for name, bucket in self._topic_buckets.items() if bucket.is_full(now)]:
            self._topic_buckets.pop(topic_name, None)
        while self.buckets() >= self._bucket_limit and self._topic_buckets:
            table_key = next(iter(self._topic_buckets))
            self._topic_buckets.pop(table_key, None)
        while self.buckets() >= self._bucket_limit and self._tenant_buckets:
            tenant_key = next(iter(self._tenant_buckets))
            self._tenant_buckets.pop(tenant_key, None)


def retry_after_ms(verdict: RateVerdict) -> int:
    """取「对外建议重试毫秒」（桶未给出时回落常量，保证响应恒有值）。

    Args:
        verdict: 判定结果。

    Returns:
        int: 建议重试毫秒。
    """
    return verdict.retry_after_ms or RATE_LIMIT_RETRY_AFTER_MS
