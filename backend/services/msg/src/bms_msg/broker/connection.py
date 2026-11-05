"""连接管理基元（任务 03-4）：派生连接键、连接登记表、连接参数与重连退避。

**为什么需要「派生理连接键」**：ASGI 不提供 HTTP/2 连接标识，`scope` 里没有连接号；但 hypercorn 把
TCP 对端的 `getpeername()` 传给**同一条连接上的每个 stream**，因此 `(tenant, client_host, client_port)`
在同一条 h2 连接内恒定——足以当进程内的连接身份用（**服务器实现行为，非 ASGI 保证**，换服务器须复验）。

**三层分工（本模块只负责中间那层）**：

| 层 | 判据 | 释放什么 | 归属 |
| --- | --- | --- | --- |
| 成员会话 | 成员 `last_seen` 30s | 关长驻流（`member_lost`） | 01-4 |
| 连接 | 连接 `last_seen`（含心跳） | 连接条目与名额（stale） | 本模块 |
| TCP | 无活跃流后的 `keep_alive_timeout` | 关连接（hypercorn，**不发 GOAWAY**） | 服务器 |

**刻意不做的事**：不关连接（ASGI 无入口）、不注销长驻流（流由成员会话与时长上限收口，见 03-1 §5.4）、
不判短请求并发（依赖里拿不到请求结束时机，由协议层 SETTINGS 约束）。
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, replace

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.logging import get_logger
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.broker.transport import ConnectionMeta
from bms_msg.core.config import (
    CONNECTION_EVICT_SCAN_LIMIT,
    CONNECTION_HEARTBEAT_INTERVAL_MS,
    CONNECTION_HEARTBEAT_TIMEOUT_MS,
    CONNECTION_RETRY_AFTER_MS,
    MAX_CONNECTIONS,
    MAX_STREAMS_PER_CONNECTION,
    MS_PER_SECOND,
    RECONNECT_BASE_MS,
    RECONNECT_JITTER_RATIO,
    RECONNECT_MAX_MS,
)
from bms_msg.errors import MsgStreamLimitExceededError

__all__ = [
    "ANONYMOUS_TENANT",
    "ConnectionRecord",
    "ConnectionRegistry",
    "ConnectionTuning",
    "connection_key",
    "connection_tuning_from_settings",
    "default_connection_tuning",
    "next_backoff_ms",
]

ANONYMOUS_TENANT = "anonymous"
"""无租户上下文时的连接归属（心跳与连接自报不强制租户，见详细设计 §6.1）。"""

_ANONYMOUS_HOST = "-"
"""无客户端地址时的占位（Unix socket 等场景）。"""

_MAX_BACKOFF_EXPONENT = 30
"""退避指数的上限（`2^30 × 200ms` 远超上限，再算下去只是浪费）。"""

_LOGGER = get_logger("bms_msg.connection")


def connection_key(meta: ConnectionMeta, *, tenant: str | None) -> str:
    """构造派生连接键（`tenant|host:port`）。

    Args:
        meta: 连接元数据（对端地址）。
        tenant: 租户编码；None / 空串归为 `anonymous`。

    Returns:
        str: 派生连接键（同一 TCP 连接恒定；见模块文档的边界说明）。
    """
    host = meta.client_host or _ANONYMOUS_HOST
    resolved = tenant if tenant else ANONYMOUS_TENANT
    return f"{resolved}|{host}:{meta.client_port}"


@dataclass(frozen=True)
class ConnectionRecord:
    """一条连接的登记快照（frozen；每次变更产出新快照，避免读到半更新状态）。"""

    key: str
    """派生连接键。"""

    tenant: str
    """租户编码或 `anonymous`。"""

    client_host: str
    """客户端地址（无地址为 `-`）。"""

    client_port: int
    """客户端端口（无地址为 0）。"""

    http_version: str
    """首次登记时协商到的 HTTP 版本。"""

    scheme: str
    """承载协议（http / https）。"""

    opened_at: float
    """建立时刻（`time.monotonic()` 秒，算存活时长）。"""

    opened_wall: float
    """建立时刻（`time.time()` 秒，供响应与日志给人看）。"""

    last_seen: float
    """最近一次请求时刻（monotonic 秒；**超时判定的唯一依据**）。"""

    last_seen_wall: float
    """最近一次请求时刻（wall clock 秒，供 `ConnectionInfo.last_seen` 派生）。"""

    last_heartbeat_wall: float | None
    """最近一次心跳时刻（wall clock 秒；None = 从未心跳）。"""

    streams: int
    """该连接上的长驻流数（应用层并发流判据）。"""

    requests: int
    """累计请求数（观测用，不判上限）。"""

    drain_signaled: bool
    """是否已向该连接下发过 drain 信号（防日志刷屏）。"""

    stale: bool
    """是否已被清扫标记（名额已回收；可被同键请求复活）。"""


class ConnectionRegistry(BaseFrameworkObject):
    """进程级连接登记表（单副本进程内状态；跨副本共享归 8B_05）。

    只做四件事：登记 / 刷新、注销、长驻流名额、过期清扫。**不做**限流决策与错误码映射
    （判据在 `api/frame/connection.py` 的守卫里，错误码在 `errors.py`）。
    """

    def __init__(self) -> None:
        """初始化空登记表与 drain 记账位。"""
        self._records: ConcurrentStableDict[str, ConnectionRecord] = ConcurrentStableDict()
        self._drain_noted = False

    def touch(
        self,
        *,
        key: str,
        meta: ConnectionMeta,
        tenant: str | None,
        now: float,
        wall: float,
        heartbeat: bool,
    ) -> ConnectionRecord:
        """登记或刷新一条连接（新键即建、stale 键即复活、其余只刷新时间与计数）。

        Args:
            key: 派生连接键。
            meta: 连接元数据（协议版本 / 承载 / 地址）。
            tenant: 租户编码（None 归 `anonymous`）。
            now: 当前时刻（monotonic 秒）。
            wall: 当前时刻（wall clock 秒）。
            heartbeat: 本次是否由心跳触发（是则更新 `last_heartbeat_wall`）。

        Returns:
            ConnectionRecord: 刷新后的快照。
        """
        record = self._records.get(key)
        if record is None:
            created = ConnectionRecord(
                key=key,
                tenant=tenant if tenant else ANONYMOUS_TENANT,
                client_host=meta.client_host,
                client_port=meta.client_port,
                http_version=meta.http_version,
                scheme=meta.scheme,
                opened_at=now,
                opened_wall=wall,
                last_seen=now,
                last_seen_wall=wall,
                last_heartbeat_wall=wall if heartbeat else None,
                streams=0,
                requests=1,
                drain_signaled=False,
                stale=False,
            )
            self._records.set(key, created)
            _LOGGER.info(
                "msg_connection_opened",
                connection_key=key,
                tenant=created.tenant,
                http_version=created.http_version,
            )
            return created
        revived = record.stale
        updated = replace(
            record,
            http_version=meta.http_version,
            scheme=meta.scheme,
            # 复活按「新连接拿到同一个键」处理：存活时长与计数重新计时，名额从零开始。
            opened_at=now if revived else record.opened_at,
            opened_wall=wall if revived else record.opened_wall,
            last_seen=now,
            last_seen_wall=wall,
            last_heartbeat_wall=wall if heartbeat else (None if revived else record.last_heartbeat_wall),
            streams=0 if revived else record.streams,
            requests=1 if revived else record.requests + 1,
            stale=False,
            drain_signaled=False if revived else record.drain_signaled,
        )
        self._records.set(key, updated)
        if revived:
            _LOGGER.info(
                "msg_connection_opened",
                connection_key=key,
                tenant=updated.tenant,
                http_version=updated.http_version,
                reason="revived",
            )
        return updated

    def release(self, key: str, *, reason: str) -> ConnectionRecord | None:
        """注销一条连接（幂等：未登记返回 None）。

        Args:
            key: 派生连接键。
            reason: 注销原因（`client_closed` / `drain` / `test`）。

        Returns:
            ConnectionRecord | None: 注销前的快照。
        """
        record = self._records.pop(key, None)
        if record is None:
            return None
        _LOGGER.info(
            "msg_connection_released",
            connection_key=key,
            tenant=record.tenant,
            reason=reason,
            duration_ms=int((time.monotonic() - record.opened_at) * MS_PER_SECOND),
        )
        return record

    def open_stream(self, key: str, *, limit: int) -> ConnectionRecord | None:
        """占用一个长驻流名额。

        Args:
            key: 派生连接键。
            limit: 单连接长驻流上限（取自 `ConnectionTuning`）。

        Returns:
            ConnectionRecord | None: 占用后的快照；连接未登记（防御分支）时为 None。

        Raises:
            MsgStreamLimitExceededError: 已达上限（`350021`）。
        """
        record = self._records.get(key)
        if record is None:
            return None
        if record.streams >= limit:
            _LOGGER.warning(
                "msg_connection_stream_limit_rejected",
                connection_key=key,
                tenant=record.tenant,
                active_streams=record.streams,
                reason="stream_limit",
            )
            raise MsgStreamLimitExceededError(f"单连接长驻流数已达上限 {limit}")
        updated = replace(record, streams=record.streams + 1)
        self._records.set(key, updated)
        return updated

    def close_stream(self, key: str) -> None:
        """释放一个长驻流名额（幂等；连接未登记或已归零时忽略）。

        Args:
            key: 派生连接键。
        """
        record = self._records.get(key)
        if record is None or record.streams <= 0:
            return
        self._records.set(key, replace(record, streams=record.streams - 1))

    def evict(self, *, now: float, limit: int, idle_timeout_ms: int) -> ConcurrentStableList[ConnectionRecord]:
        """惰性清扫：把过了空闲阈值的连接标 stale 并回收名额，过久者删除条目。

        两段式（`> idle` 标 stale、`> 2 × idle` 才删）是为了让同键请求能识别「复活」——端口复用后
        新连接会拿到同一个键，留一条 stale 记录既能让日志区分，也不让条目无限堆积。

        Args:
            now: 当前时刻（monotonic 秒）。
            limit: 本次最多检查的条目数（请求驱动，避免单请求扫全表）。
            idle_timeout_ms: 空闲阈值（毫秒）。

        Returns:
            ConcurrentStableList[ConnectionRecord]: 本次被标记 stale 的记录（供日志与用例断言）。
        """
        evicted: ConcurrentStableList[ConnectionRecord] = ConcurrentStableList()
        idle_seconds = idle_timeout_ms / MS_PER_SECOND
        for key, record in list(self._records.items())[:limit]:
            idle = now - record.last_seen
            if not record.stale and idle > idle_seconds:
                stale = replace(record, stale=True, streams=0)
                self._records.set(key, stale)
                evicted.add(stale)
                _LOGGER.info(
                    "msg_connection_evicted",
                    connection_key=key,
                    tenant=record.tenant,
                    active_streams=record.streams,
                    reason="heartbeat_timeout",
                    duration_ms=int(idle * MS_PER_SECOND),
                )
            elif record.stale and idle > idle_seconds * 2:
                self._records.delete(key)
        return evicted

    def get(self, key: str) -> ConnectionRecord | None:
        """取单条连接快照。

        Args:
            key: 派生连接键。

        Returns:
            ConnectionRecord | None: 快照；未登记为 None。
        """
        return self._records.get(key)

    def count(self) -> int:
        """当前有效连接数（不含 stale —— 上限判定与观测都按有效连接算）。

        Returns:
            int: 有效连接数。
        """
        return sum(1 for record in self._records.values() if not record.stale)

    def records(self) -> ConcurrentStableList[ConnectionRecord]:
        """全部连接快照（含 stale；日志与用例用）。

        Returns:
            ConcurrentStableList[ConnectionRecord]: 快照列表（登记序）。
        """
        return ConcurrentStableList(self._records.values())

    def mark_drain_signaled(self, key: str) -> bool:
        """给某条连接记「drain 信号已下发」（同连接只记一次日志）。

        Args:
            key: 派生连接键。

        Returns:
            bool: 本次是否首次下发（True 表示调用方应记 `drain_signal_sent`）。
        """
        record = self._records.get(key)
        if record is None or record.drain_signaled:
            return False
        self._records.set(key, replace(record, drain_signaled=True))
        return True

    def note_drain(self) -> bool:
        """记「本进程已进入摘流」（幂等，只首次记 `drain_started`）。

        Returns:
            bool: 本次是否首次观察（True 表示调用方应记 `drain_started`）。
        """
        if self._drain_noted:
            return False
        self._drain_noted = True
        return True


@dataclass(frozen=True)
class ConnectionTuning:
    """连接参数（值对象；用例经依赖覆盖注入毫秒级参数）。"""

    heartbeat_interval_ms: int
    """建议心跳间隔（回传客户端）。"""

    heartbeat_timeout_ms: int
    """无心跳判半开的阈值。"""

    idle_timeout_ms: int
    """名额回收阈值（= 服务器空闲回收与心跳超时的较大者，见 `connection_tuning_from_settings`）。"""

    max_connections: int
    """连接数上限（`350020`）。"""

    max_streams_per_connection: int
    """单连接长驻流上限（`350021`）。"""

    evict_scan_limit: int
    """单次清扫批量。"""

    retry_after_ms: int
    """建议重试间隔（drain / 限流）。"""


def default_connection_tuning() -> ConnectionTuning:
    """按模块常量构造缺省连接参数（不读配置，供纯单测与用例覆盖）。

    Returns:
        ConnectionTuning: 缺省参数（10s / 30s / 30s / 1024 / 100 / 64 / 1s）。
    """
    return ConnectionTuning(
        heartbeat_interval_ms=CONNECTION_HEARTBEAT_INTERVAL_MS,
        heartbeat_timeout_ms=CONNECTION_HEARTBEAT_TIMEOUT_MS,
        idle_timeout_ms=max(2 * CONNECTION_HEARTBEAT_INTERVAL_MS, CONNECTION_HEARTBEAT_TIMEOUT_MS),
        max_connections=MAX_CONNECTIONS,
        max_streams_per_connection=MAX_STREAMS_PER_CONNECTION,
        evict_scan_limit=CONNECTION_EVICT_SCAN_LIMIT,
        retry_after_ms=CONNECTION_RETRY_AFTER_MS,
    )


def connection_tuning_from_settings(settings: Settings) -> ConnectionTuning:
    """按基座 `[server]` 计算连接参数（空闲阈值跟随服务器，不硬编码）。

    空闲阈值取 `max(2 × keep_alive_timeout, heartbeat_timeout)`：既不早于服务器回收（否则会把还活着的
    连接名额先收掉），也不会在服务器调大空闲时间后长期滞后。

    Args:
        settings: 应用配置（读 `server.keep_alive_timeout_seconds`）。

    Returns:
        ConnectionTuning: 连接参数。
    """
    keep_alive_ms = int(settings.server.keep_alive_timeout_seconds) * MS_PER_SECOND
    defaults = default_connection_tuning()
    return replace(defaults, idle_timeout_ms=max(2 * keep_alive_ms, defaults.heartbeat_timeout_ms))


def next_backoff_ms(
    attempt: int,
    *,
    base_ms: int = RECONNECT_BASE_MS,
    max_ms: int = RECONNECT_MAX_MS,
    jitter_ratio: float = RECONNECT_JITTER_RATIO,
    rand: Callable[[], float] = random.random,
) -> int:
    """算第 `attempt` 次重连的等待毫秒（指数退避 + 抖动）。

    公式：`raw = min(max_ms, base_ms × 2^attempt)`；`delay = raw × (1 + jitter_ratio × (2r − 1))`，
    `r ∈ [0,1)` 由调用方注入（默认 `random.random`），结果夹在 `[0, max_ms × (1 + jitter_ratio)]`。
    抖动在封顶后仍然保留——否则长时间故障后所有客户端会同时按同一个 `max_ms` 重连，形成脉冲。

    Args:
        attempt: 第几次重连（从 0 起）。
        base_ms: 退避基数（毫秒）。
        max_ms: 退避上限（毫秒；抖动前的封顶值）。
        jitter_ratio: 抖动比例（0 表示不抖动）。
        rand: 随机源（`[0, 1)`）；用例注入固定值断言上下界。

    Returns:
        int: 建议等待毫秒。
    """
    exponent = min(max(attempt, 0), _MAX_BACKOFF_EXPONENT)
    raw = min(max_ms, base_ms * (2**exponent))
    factor = 1 + jitter_ratio * (2 * rand() - 1)
    ceiling = max_ms * (1 + jitter_ratio)
    return round(min(ceiling, max(0.0, raw * factor)))
