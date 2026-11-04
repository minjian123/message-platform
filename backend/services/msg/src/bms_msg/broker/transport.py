"""HTTP/2 传输入口基元：连接元数据、流登记、流参数与 SSE 编码（任务 03-1）。

- **`ConnectionMeta` / `connection_meta()`**：从 ASGI `scope` 取协议版本与客户端地址——协议版本是
  需求 03-1 的「排障第一问」，`RST_STREAM` 与多 stream 的排障都从这里开始。
- **`StreamSession` / `StreamRegistry`**：进程级活跃流登记。存在的理由是「取消后已释放」必须可断言——
  `RST_STREAM` 只会给生成器一个 `CancelledError`，没有登记表就只能靠「后续请求还能通」间接猜。
- **`StreamTuning`**：流参数值对象（轮询间隔 / 时长上限 / 节点心跳间隔），用例经依赖覆盖注入毫秒级
  参数，不改全局常量；03-5 把轮询换成事件唤醒时只改这里。
- **`sse_event()` / `sse_comment()`**：SSE 线编码。`data` **一律单行**——SSE 以空行分隔事件，
  跨行 `data:` 会让「一条消息 = 一个事件」的对应关系依赖客户端实现细节。

边界：本模块**不碰** HTTP 连接本身（监听 / 多路复用 / 窗口由 hypercorn 负责），也不碰投递语义
（归 `broker/delivery.py`）——它只回答「这条连接是什么协议」「现在有几条流活着」。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.core.config import (
    SUBSCRIBE_KEEPALIVE_MS,
    SUBSCRIBE_MAX_SECONDS,
    SUBSCRIBE_POLL_INTERVAL_MS,
)

__all__ = [
    "ConnectionMeta",
    "StreamRegistry",
    "StreamSession",
    "StreamTuning",
    "connection_meta",
    "default_stream_tuning",
    "sse_comment",
    "sse_event",
]

_UNKNOWN_HTTP_VERSION = "unknown"
"""拿不到协议版本时的占位值（不让元数据反过来成为故障源）。"""

_VERSION_LABELS = {"2": "HTTP/2", "1.1": "HTTP/1.1", "1.0": "HTTP/1.0"}
"""ASGI `scope["http_version"]` → 可读标签。

ASGI 规范给的是 `"2"` / `"1.1"` / `"1.0"`（不是 `HTTP/2`），而客户端（`httpx.Response.http_version`）
与排障口径都写 `HTTP/2`——不归一化就会出现「客户端说 2、服务端说 HTTP/2」的两套说法。
"""


@dataclass(frozen=True)
class ConnectionMeta:
    """一条连接的元数据（协议版本 / 承载方式 / 客户端地址）。"""

    http_version: str
    """协商到的 HTTP 版本：`HTTP/2` / `HTTP/1.1`（排障第一问）。"""

    scheme: str
    """承载协议：`http` / `https`。"""

    is_tls: bool
    """是否 TLS 承载（`https` 即 true；TLS 下由 ALPN 协商 h2）。"""

    client_host: str
    """客户端地址（无地址场景为空串）。"""

    client_port: int
    """客户端端口（无地址场景为 0）。"""


def _client_address(scope: Mapping[str, object]) -> tuple[str, int]:
    """从 `scope["client"]` 取（地址, 端口）。

    hypercorn / uvicorn 均给 `(host, port)` 二元组；Unix socket 等无地址场景为 `None`。
    这里对形态做容错：元数据是排障输出，**不能**因为字段缺失而把请求打挂。

    Args:
        scope: ASGI 连接作用域。

    Returns:
        tuple[str, int]: 客户端地址与端口；缺失时为 `("", 0)`。
    """
    client = scope.get("client")
    if isinstance(client, tuple):
        pair = cast("tuple[object, object]", client)
        host, port = pair
        return (host if isinstance(host, str) else ""), (port if isinstance(port, int) else 0)
    return "", 0


def connection_meta(scope: Mapping[str, object]) -> ConnectionMeta:
    """按 ASGI 作用域构造连接元数据（纯函数，不读应用状态、不抛异常）。

    Args:
        scope: ASGI 连接作用域（`http_version` / `scheme` / `client`）。

    Returns:
        ConnectionMeta: 连接元数据；缺失字段取缺省值。
    """
    version = scope.get("http_version")
    scheme = scope.get("scheme")
    host, port = _client_address(scope)
    resolved_scheme = scheme if isinstance(scheme, str) else "http"
    raw_version = version if isinstance(version, str) else ""
    return ConnectionMeta(
        http_version=_VERSION_LABELS.get(raw_version, raw_version or _UNKNOWN_HTTP_VERSION),
        scheme=resolved_scheme,
        is_tls=resolved_scheme == "https",
        client_host=host,
        client_port=port,
    )


@dataclass(frozen=True)
class StreamSession:
    """一条长驻流的登记快照（`reason` 在 `open` 时为空，`close` 时填关闭原因）。"""

    stream_id: str
    """进程内流标识（`s<n>`，自增；仅用于排障与用例断言，不对外承诺稳定）。"""

    namespace: str
    """命名空间。"""

    topic: str
    """主题名。"""

    group: str
    """消费组。"""

    consumer_id: str
    """成员标识。"""

    partition: int
    """分区号。"""

    opened_at: float
    """建立时刻（`time.monotonic()` 秒，只用于算存活时长）。"""

    reason: str
    """关闭原因（`open` 时为 `""`）：`client_closed` / `deadline` / `member_lost` / `error`。"""


class StreamRegistry(BaseFrameworkObject):
    """进程级活跃流登记表（单副本进程内状态；多副本共享归 8B_05）。

    只做三件事：分配 id、登记 / 注销、报活跃数。**不做**限流与上限判定——`MAX_CONCURRENT_STREAMS`
    归 03-4，本表届时只是它的判据来源之一。
    """

    def __init__(self) -> None:
        """初始化空登记表（自增计数从 0 起）。"""
        self._seq = 0
        self._sessions: ConcurrentStableDict[str, StreamSession] = ConcurrentStableDict()

    def open(
        self,
        *,
        namespace: str,
        topic: str,
        group: str,
        consumer_id: str,
        partition: int,
        opened_at: float,
    ) -> str:
        """登记一条流。

        Args:
            namespace: 命名空间。
            topic: 主题名。
            group: 消费组。
            consumer_id: 成员标识。
            partition: 分区号。
            opened_at: 建立时刻（`time.monotonic()` 秒）。

        Returns:
            str: 流标识（`s<n>`）。
        """
        self._seq += 1
        stream_id = f"s{self._seq}"
        self._sessions.set(
            stream_id,
            StreamSession(
                stream_id=stream_id,
                namespace=namespace,
                topic=topic,
                group=group,
                consumer_id=consumer_id,
                partition=partition,
                opened_at=opened_at,
                reason="",
            ),
        )
        return stream_id

    def close(self, stream_id: str, *, reason: str) -> StreamSession | None:
        """注销一条流（幂等：已注销返回 `None`）。

        Args:
            stream_id: 流标识。
            reason: 关闭原因（`client_closed` / `deadline` / `member_lost` / `error`）。

        Returns:
            StreamSession | None: 带关闭原因的会话快照；未登记或已注销为 `None`。
        """
        session = self._sessions.get(stream_id)
        if session is None:
            return None
        self._sessions.pop(stream_id, None)
        return replace(session, reason=reason)

    def active(self) -> int:
        """当前活跃流数（用例据此断言「断开即释放」）。

        Returns:
            int: 活跃流数。
        """
        return len(self._sessions)

    def sessions(self) -> ConcurrentStableList[StreamSession]:
        """活跃流快照（登记序；03-4 连接视图与 06 域指标的输入）。

        Returns:
            ConcurrentStableList[StreamSession]: 会话副本。
        """
        return ConcurrentStableList(self._sessions.values())


@dataclass(frozen=True)
class StreamTuning:
    """长驻流参数（值对象；用例经依赖覆盖注入毫秒级参数）。"""

    poll_interval_ms: int
    """无消息时的重试拉取间隔（毫秒）。"""

    max_seconds: int
    """单条流时长上限（秒，缺省值；请求可覆盖到 `SUBSCRIBE_MAX_SECONDS_LIMIT`）。"""

    keepalive_ms: int
    """空闲期 SSE 注释行心跳间隔（毫秒）。"""


def default_stream_tuning() -> StreamTuning:
    """按模块常量构造缺省流参数。

    Returns:
        StreamTuning: 缺省参数（200ms / 60s / 15s）。
    """
    return StreamTuning(
        poll_interval_ms=SUBSCRIBE_POLL_INTERVAL_MS,
        max_seconds=SUBSCRIBE_MAX_SECONDS,
        keepalive_ms=SUBSCRIBE_KEEPALIVE_MS,
    )


def sse_event(event: str, data: Mapping[str, Any]) -> bytes:
    """编码一条 SSE 事件（单行 `data`，UTF-8）。

    Args:
        event: 事件名（`open` / `message` / `error` / `close`）。
        data: 事件负载（JSON 对象）。

    Returns:
        bytes: `event: <名>\\ndata: <单行 JSON>\\n\\n`。
    """
    payload = json.dumps(dict(data.items()), ensure_ascii=False, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n".encode()


def sse_comment(text: str) -> bytes:
    """编码一行 SSE 注释（客户端忽略，仅保活）。

    Args:
        text: 注释内容。

    Returns:
        bytes: `: <内容>\\n\\n`。
    """
    return f": {text}\n\n".encode()
