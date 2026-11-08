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

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.objects import BaseValueObject
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.core.config import (
    INFLIGHT_MAX_BYTES,
    INFLIGHT_MAX_MESSAGES,
    MS_PER_SECOND,
    RATE_LIMIT_BUCKET_LIMIT,
    RATE_LIMIT_BURST_SECONDS,
    RATE_LIMIT_RETRY_AFTER_MS,
    RATE_LIMIT_RPC_PER_SECOND,
    RATE_LIMIT_TENANT_PER_SECOND,
    RATE_LIMIT_TOPIC_PER_SECOND,
)

__all__ = [
    "BackpressureTuning",
    "InflightUsage",
    "RateLimitTuning",
    "RateLimiter",
    "RateVerdict",
    "RpcKey",
    "TokenBucket",
    "TopicKey",
    "default_backpressure_tuning",
    "default_rate_limit_tuning",
    "retry_after_ms",
]

TopicKey = tuple[str, str, str]
"""主题限流键：`(tenant, namespace, topic)`——同租户不同命名空间的同名主题互不影响。"""

RpcKey = tuple[str, str]
"""RPC 限流键：`(tenant, service)`。

基数 = 租户数 × 注册表服务数（有界，典型十几个）；不细分到 method——那会让基数随方法数线性膨胀，
而「哪个方法在打爆」应该看指标而不是看桶。
"""


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
    """限速参数（0 表示该级不限）。"""

    tenant_per_second: int
    """租户级速率（条/秒）。"""

    topic_per_second: int
    """主题级速率（条/秒）。"""

    burst_seconds: int
    """桶容量折算秒数（容量 = 速率 × 本值）。"""

    rpc_per_second: int = 0
    """RPC 级速率（次/秒，按 `(tenant, service)` 计；04-1）。

    放在最后并给默认值：04-1 之前构造 `RateLimitTuning` 的地方（用例、配置推导）都用关键字参数，
    加一个带默认的维度不会让它们失效。
    """

    @property
    def enabled(self) -> bool:
        """是否启用限速（任一级 > 0 即启用）。

        Returns:
            bool: 是否启用。
        """
        return self.tenant_per_second > 0 or self.topic_per_second > 0 or self.rpc_per_second > 0


def default_rate_limit_tuning() -> RateLimitTuning:
    """按模块常量构造缺省限速参数（缺省关闭）。

    Returns:
        RateLimitTuning: 三级均为 0。
    """
    return RateLimitTuning(
        tenant_per_second=RATE_LIMIT_TENANT_PER_SECOND,
        topic_per_second=RATE_LIMIT_TOPIC_PER_SECOND,
        rpc_per_second=RATE_LIMIT_RPC_PER_SECOND,
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
    """令牌桶限流（进程级单例；桶跨请求存续）。

    **生产两级 + RPC 一级，三组桶互不消耗**：

    - `check()`：租户 + 主题两级，**都判后都扣**（不是「先扣再判」——主题桶拒绝时若已扣了租户桶，
      租户额度会被无效请求吃掉，表现为「同样的负载，限流比预期更早触发」）；
    - `check_rpc()`：只判 `(tenant, service)` 的 RPC 桶——大消息发布打满 topic 桶不会连带拒 RPC，
      反之亦然（04-1 §8.2）。

    「生产两级互不消耗 RPC」不是靠加锁隔开的，而是**三组独立字典**：任何一组的扣减都碰不到另外两组。
    """

    def __init__(self, tuning: RateLimitTuning, *, bucket_limit: int = RATE_LIMIT_BUCKET_LIMIT) -> None:
        """初始化。

        Args:
            tuning: 三级速率（0 表示该级不限）。
            bucket_limit: 桶数量上限（超出先清满桶，再按插入序淘汰最旧）。
        """
        self._tuning = tuning
        self._bucket_limit = bucket_limit
        self._tenant_buckets: dict[str, TokenBucket] = {}
        self._topic_buckets: dict[TopicKey, TokenBucket] = {}
        self._rpc_buckets: dict[RpcKey, TokenBucket] = {}
        self._quota_buckets: ConcurrentStableDict[tuple[str, str], tuple[int, TokenBucket]] = ConcurrentStableDict()

    @property
    def tuning(self) -> RateLimitTuning:
        """当前参数（用例与实施记录口径）。

        Returns:
            RateLimitTuning: 参数。
        """
        return self._tuning

    def buckets(self) -> int:
        """当前桶总数（三级合计；用例断言清理生效）。

        Returns:
            int: 桶数量。
        """
        return len(self._tenant_buckets) + len(self._topic_buckets) + len(self._rpc_buckets) + len(self._quota_buckets)

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

    def check_quota(
        self,
        *,
        tenant: str,
        namespace: str,
        rate_per_second: int,
        now: float | None = None,
    ) -> RateVerdict:
        """判一次生产是否放行（05-2 配额速率；与静态两级桶互相独立）。

        速率由配额服务按当前生效限额传入：调整配额后下一次请求即用新值，无需跨键同步。
        `rate_per_second <= 0` 表示不限（不建桶、不扣令牌）。

        Args:
            tenant: 租户编码。
            namespace: 命名空间。
            rate_per_second: 当前生效速率（条/秒；0 = 不限）。
            now: 当前 `monotonic` 秒（缺省取当前时刻；用例注入固定值）。

        Returns:
            RateVerdict: 判定结果（`scope = "tenant"` 表示触发配额速率）。
        """
        if rate_per_second <= 0:
            return RateVerdict(allowed=True, retry_after_ms=0, scope="")
        moment = time.monotonic() if now is None else now
        bucket = self._quota_bucket((tenant, namespace), rate_per_second, moment)
        wait = bucket.can(1, moment)
        if wait:
            return RateVerdict(allowed=False, retry_after_ms=wait, scope="tenant")
        bucket.consume(1, moment)
        return RateVerdict(allowed=True, retry_after_ms=0, scope="")

    def check_rpc(self, *, tenant: str, service: str, now: float | None = None) -> RateVerdict:
        """判一次 RPC 调用是否放行（放行时扣 RPC 桶；**不碰**生产两级桶）。

        与 `check()` 分开而不是复用它：生产的「租户 / 主题」与 RPC 的「租户 / 服务」是两组无关维度，
        混在一处判会让「大消息发布打满 topic 桶」连带拒掉 RPC，正是 04-1 §8.2 要避免的挤占。

        Args:
            tenant: 租户编码。
            service: 服务标识。
            now: 当前 `monotonic` 秒（缺省取当前时刻；用例注入固定值）。

        Returns:
            RateVerdict: 判定结果（`scope = "rpc"` 表示触发的是 RPC 维度）。
        """
        if self._tuning.rpc_per_second <= 0:
            return RateVerdict(allowed=True, retry_after_ms=0, scope="")
        moment = time.monotonic() if now is None else now
        bucket = self._rpc_bucket((tenant, service), moment)
        wait = bucket.can(1, moment)
        if wait:
            return RateVerdict(allowed=False, retry_after_ms=wait, scope="rpc")
        bucket.consume(1, moment)
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

    def _rpc_bucket(self, key: RpcKey, now: float) -> TokenBucket:
        """取或建 RPC 桶。

        Args:
            key: RPC 键 `(tenant, service)`。
            now: 当前 `monotonic` 秒。

        Returns:
            TokenBucket: 桶实例。
        """
        bucket = self._rpc_buckets.get(key)
        if bucket is not None:
            return bucket
        self._prune(now)
        created = TokenBucket(rate_per_second=self._tuning.rpc_per_second, burst_seconds=self._tuning.burst_seconds)
        self._rpc_buckets[key] = created
        return created

    def _quota_bucket(self, key: tuple[str, str], rate_per_second: int, now: float) -> TokenBucket:
        """取或建配额速率桶（速率变化即重建，保证调整即时生效）。

        Args:
            key: 配额键 `(tenant, namespace)`。
            rate_per_second: 当前生效速率（正数）。
            now: 当前 `monotonic` 秒。

        Returns:
            TokenBucket: 桶实例。
        """
        existing = self._quota_buckets.get(key)
        if existing is not None and existing[0] == rate_per_second:
            return existing[1]
        self._prune(now)
        created = TokenBucket(rate_per_second=rate_per_second, burst_seconds=self._tuning.burst_seconds)
        self._quota_buckets[key] = (rate_per_second, created)
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
        for rpc_name in [name for name, bucket in self._rpc_buckets.items() if bucket.is_full(now)]:
            self._rpc_buckets.pop(rpc_name, None)
        for quota_key, (_rate, bucket) in list(self._quota_buckets.items()):
            if bucket.is_full(now):
                self._quota_buckets.pop(quota_key, None)
        while self.buckets() >= self._bucket_limit and self._quota_buckets:
            quota_key = next(iter(self._quota_buckets))
            self._quota_buckets.pop(quota_key, None)
        while self.buckets() >= self._bucket_limit and self._topic_buckets:
            table_key = next(iter(self._topic_buckets))
            self._topic_buckets.pop(table_key, None)
        while self.buckets() >= self._bucket_limit and self._rpc_buckets:
            rpc_key = next(iter(self._rpc_buckets))
            self._rpc_buckets.pop(rpc_key, None)
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
