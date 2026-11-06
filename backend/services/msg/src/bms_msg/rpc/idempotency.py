"""服务端幂等去重（04-4）：进程内实现 + 作用域键 + 调用上下文。

**形状对齐 bms、不继承**：bms `IdempotencyStore` 是 `BasePluggable`（`begin` / `load` / `save` /
`release`），真实实现走 Redis SETNX。msg 是独立进程、首期不引 Redis，因此按同样的方法语义给一份
进程内实现——8B_05 换共享存储时调用点（`invoke_rpc`）零改动，只需换实例。不继承 `BasePluggable`
是为了不把 bms 的插件注册表 / `FastAPI Request` 提供者拖进 msg 的 broker 进程。

**首期限制（设计 §4.5，三份记录都要写）**：进程内单实例、不跨实例、不跨重启；重启后去重表为空，
带键重试可能产生重复副作用。多副本 / 滚动重启必须换共享存储。

**为什么 `begin` 内部不 `await`**：单线程事件循环里同步占位天然原子，不会出现「两个协程同时
判定为首次」的并发穿透；8B_05 换 Redis 时由 SETNX 保证同一性质（设计里的第二道兜底）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from bms_msg.core.config import RPC_IDEMPOTENCY_TTL_MS

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查期
    from bms_msg.rpc.registry import RpcReply

__all__ = ["IdempotencyContext", "InProcessIdempotencyStore", "scoped_idempotency_key"]


def scoped_idempotency_key(*, tenant: str, service: str, method: str, key: str) -> str:
    """构造作用域键：`msg:{tenant}:idem:{service}.{method}:{key}`。

    形状对齐 bms `bms:{tenant|global}:idem:{key}`，但补上 `service` / `method`——同一个业务
    键（如订单号）在不同方法上不该互相误判为重复。

    Args:
        tenant: 租户编码（隔离的第一维）。
        service: 服务标识。
        method: 方法名。
        key: 客户端给的幂等键业务部分。

    Returns:
        str: 去重表主键。
    """
    return f"msg:{tenant}:idem:{service}.{method}:{key}"


@dataclass
class _Record:
    """一条去重记录（`reply=None` 表示首次仍在执行）。"""

    reply: RpcReply | None
    expires_at_ms: int


class InProcessIdempotencyStore:
    """进程内去重存储：`begin` 占位 / `load` 取首次结果 / `save` 写结果 / `release` 释放。

    `Clock` 经构造注入（与重试 / 熔断同一端口），用例用 fake clock 精确驱动 TTL 过期，
    不依赖真实等待。
    """

    def __init__(self, *, clock: object | None = None, ttl_ms: int = RPC_IDEMPOTENCY_TTL_MS) -> None:
        """初始化。

        Args:
            clock: 提供 `now_ms()` 的时钟（缺省 `SystemClock()`）。
            ttl_ms: 键有效期（毫秒）。
        """
        from bms_msg.client.retry import SystemClock

        self._clock = clock if clock is not None else SystemClock()
        self._ttl_ms = ttl_ms
        self._records: dict[str, _Record] = {}

    def _now_ms(self) -> int:
        """取当前单调毫秒。

        Returns:
            int: 单调毫秒。
        """
        return int(self._clock.now_ms())  # type: ignore[attr-defined]

    def _purge(self, now_ms: int) -> None:
        """惰性清理过期条目（请求驱动，空载零唤醒）。

        Args:
            now_ms: 当前单调毫秒。
        """
        expired = [key for key, record in self._records.items() if record.expires_at_ms <= now_ms]
        for key in expired:
            del self._records[key]

    async def begin(self, key: str, *, ttl_ms: int | None = None) -> bool:
        """前置去重：首次占位返回 True，重复返回 False。

        Args:
            key: 作用域键。
            ttl_ms: 本次键有效期（缺省取构造值）。

        Returns:
            bool: True = 首次（继续执行业务）；False = 重复（调用方应 `load` 首次结果）。
        """
        now_ms = self._now_ms()
        self._purge(now_ms)
        if key in self._records:
            return False
        self._records[key] = _Record(reply=None, expires_at_ms=now_ms + (self._ttl_ms if ttl_ms is None else ttl_ms))
        return True

    async def load(self, key: str) -> RpcReply | None:
        """取首次结果（尚未写出返回 `None`，由调用方转「处理中」错）。

        Args:
            key: 作用域键。

        Returns:
            RpcReply | None: 首次结果；未缓存为 `None`。
        """
        record = self._records.get(key)
        return None if record is None else record.reply

    async def save(self, key: str, reply: RpcReply, *, ttl_ms: int | None = None) -> None:
        """写首次结果（供重复请求复用）。

        Args:
            key: 作用域键。
            reply: 首次调用的结果。
            ttl_ms: 本次键有效期（缺省取构造值）。
        """
        now_ms = self._now_ms()
        self._records[key] = _Record(
            reply=reply,
            expires_at_ms=now_ms + (self._ttl_ms if ttl_ms is None else ttl_ms),
        )

    async def release(self, key: str) -> None:
        """释放键（首次失败 / 被取消时；允许带键重试重新执行业务）。

        Args:
            key: 作用域键。
        """
        self._records.pop(key, None)


@dataclass(frozen=True)
class IdempotencyContext:
    """一次调用的幂等上下文（闸门构造 → `invoke_rpc` 消费）。"""

    store: InProcessIdempotencyStore
    """去重存储（进程级单例）。"""

    key: str
    """作用域键。"""

    ttl_ms: int = RPC_IDEMPOTENCY_TTL_MS
    """键有效期（毫秒）。"""
