"""连接守卫与连接自报（任务 03-4）。

**守卫做什么**（挂在数据面两处挂载与连接 / 心跳路由上，一个判据贯穿全部入口）：

1. 惰性清扫：每次请求顺带扫至多 `evict_scan_limit` 条过期连接（请求驱动，空载零唤醒）；
2. 登记 / 刷新：按派生连接键 `touch`（新键建、stale 键复活、其余只刷新时间与计数）；
3. 连接数上限：新键且有效连接数已达上限 → `350020`（不静默丢弃）；
4. drain 标记：进程在摘流时给响应带 `draining` / `retry_after_ms`，并对每连接只记一次日志。

**错误形态分两种**（同一个判据、两种承载）：帧请求回 **200 + `error` 帧**（03-2 §6.5）；JSON 请求回
**429 + 统一错误体**。`FrameHandled` 用局部导入是为了断环——`gate.py` 依赖数据面模块，数据面模块又要
依赖守卫（路由级依赖），模块级互相导入会成环。

边界：不关连接（ASGI 无入口）、不注销长驻流（流由成员会话与时长上限收口）、不判短请求并发
（依赖里拿不到请求结束时机，由协议层 SETTINGS 约束）。详见详细设计 §5.1 三层分工。
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Annotated, cast

from bms_core.core.logging import get_logger
from bms_core.db.tenant import TenantContext
from bms_core.msgclient.frame import PROTOCOL_VERSION
from fastapi import Depends, Request, Response

from bms_msg.api.deps import ConnectionRegistryDep, ConnectionTuningDep
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame
from bms_msg.broker.connection import (
    ANONYMOUS_TENANT,
    ConnectionRecord,
    ConnectionTuning,
    connection_key,
)
from bms_msg.broker.transport import connection_meta
from bms_msg.core.config import MS_PER_SECOND
from bms_msg.errors import MsgConnectionLimitExceededError, MsgError
from bms_msg.schemas.transport import ConnectionInfo

__all__ = [
    "CONNECTION_GUARD",
    "CONNECTION_INFO_GUARD",
    "HEARTBEAT_GUARD",
    "ConnectionInfoRecordDep",
    "ConnectionRecordDep",
    "HeartbeatRecordDep",
    "build_connection_guard",
    "connection_info",
]

_LOGGER = get_logger("bms_msg.connection")


def _tenant_code(request: Request) -> str | None:
    """读中间件已解析的租户编码（**不触发解析**：心跳与连接自报不强制租户）。

    Args:
        request: 当前请求。

    Returns:
        str | None: 租户编码；未解析或无内部身份为 None。
    """
    state = cast("dict[str, object]", request.scope.get("state") or {})
    tenant = state.get("tenant")
    if isinstance(tenant, TenantContext):
        return tenant.code or None
    return None


def _is_draining(request: Request) -> bool:
    """读基座摘流标记（`ServiceRuntime.start_drain()` 置位；无该属性按未摘流处理）。

    Args:
        request: 当前请求。

    Returns:
        bool: 是否在摘流。
    """
    return bool(getattr(request.app.state, "draining", False))


def _to_datetime(seconds: float | None) -> datetime | None:
    """把 wall clock 秒转 UTC 时间（None 原样返回）。

    Args:
        seconds: `time.time()` 秒。

    Returns:
        datetime | None: UTC 时间。
    """
    return None if seconds is None else datetime.fromtimestamp(seconds, UTC)


def connection_info(
    request: Request,
    record: ConnectionRecord,
    tuning: ConnectionTuning,
    *,
    protocol_version: str | None = None,
) -> ConnectionInfo:
    """把连接快照组装成自报契约（三处共用：connection / heartbeat JSON / heartbeat 帧）。

    Args:
        request: 当前请求（取本请求协商到的协议版本与 TLS 状态）。
        record: 连接快照。
        tuning: 连接参数（回传客户端据以自适应）。
        protocol_version: 本请求所用帧协议版本（帧形态给值，JSON 形态为 None）。

    Returns:
        ConnectionInfo: 连接自报。
    """
    meta = connection_meta(request.scope)
    now = time.monotonic()
    return ConnectionInfo(
        http_version=meta.http_version,
        scheme=meta.scheme,
        is_tls=meta.is_tls,
        client_host=meta.client_host,
        client_port=meta.client_port,
        protocol_version=protocol_version,
        supported_protocol_version=PROTOCOL_VERSION,
        connection_key=record.key,
        tenant=record.tenant,
        active_streams=record.streams,
        requests=record.requests,
        uptime_ms=int((now - record.opened_at) * MS_PER_SECOND),
        idle_ms=int((now - record.last_seen) * MS_PER_SECOND),
        last_heartbeat_at=_to_datetime(record.last_heartbeat_wall),
        heartbeat_interval_ms=tuning.heartbeat_interval_ms,
        heartbeat_timeout_ms=tuning.heartbeat_timeout_ms,
        idle_timeout_ms=tuning.idle_timeout_ms,
        max_streams_per_connection=tuning.max_streams_per_connection,
        max_connections=tuning.max_connections,
        draining=_is_draining(request),
        retry_after_ms=tuning.retry_after_ms,
        server_time=datetime.now(UTC),
    )


def _reject(request: Request, exc: MsgError, tuning: ConnectionTuning) -> Exception:
    """把「已超限」转成对应形态的异常（帧 → 短路异常直出 error 帧；JSON → 原样抛）。

    Args:
        request: 当前请求。
        exc: 待抛出的业务异常（携带产品码）。
        tuning: 连接参数（取 `retry_after_ms`）。

    Returns:
        Exception: 供 `raise` 的异常实例。
    """
    if not is_frame_request(request):
        return exc
    # 局部导入：`gate` 依赖数据面模块，而数据面模块依赖本模块（路由级依赖），模块级导入会成环。
    from bms_msg.api.frame.gate import FrameHandled

    return FrameHandled(
        Response(
            content=error_frame(code=exc.code, message=exc.message or str(exc), retry_after_ms=tuning.retry_after_ms),
            media_type=FRAME_MEDIA_TYPE,
            status_code=200,
        )
    )


def build_connection_guard(
    *, heartbeat: bool = False, enforce_limit: bool = True
) -> Callable[..., Awaitable[ConnectionRecord]]:
    """构造连接守卫依赖（数据面两处挂载与连接 / 心跳路由共用同一份判据）。

    Args:
        heartbeat: 本次是否由心跳触发（是则刷新 `last_heartbeat_at`）。
        enforce_limit: 是否判连接数上限（`GET /msg/connection` 传 False——排障入口必须在打满时也能查）。

    Returns:
        Callable[..., Awaitable[ConnectionRecord]]: 可直接 `Depends(...)` 的守卫函数。
    """

    async def guard(
        request: Request,
        connections: ConnectionRegistryDep,
        tuning: ConnectionTuningDep,
    ) -> ConnectionRecord:
        """清扫 → 登记 / 刷新 → 连接数上限 → drain 标记。

        Args:
            request: 当前请求。
            connections: 连接登记表。
            tuning: 连接参数。

        Returns:
            ConnectionRecord: 刷新后的连接快照（同时挂到 `request.state.connection_record`）。

        Raises:
            MsgConnectionLimitExceededError: 有效连接数已达上限（`350020`）。
            FrameHandled: 同一超限的帧形态（200 + error 帧）。
        """
        meta = connection_meta(request.scope)
        tenant = _tenant_code(request)
        key = connection_key(meta, tenant=tenant)
        now = time.monotonic()
        wall = time.time()
        connections.evict(now=now, limit=tuning.evict_scan_limit, idle_timeout_ms=tuning.idle_timeout_ms)
        if enforce_limit and connections.get(key) is None and connections.count() >= tuning.max_connections:
            _LOGGER.warning(
                "msg_connection_limit_rejected",
                connection_key=key,
                tenant=tenant if tenant else ANONYMOUS_TENANT,
                reason="connection_limit",
            )
            raise _reject(
                request,
                MsgConnectionLimitExceededError(f"连接数已达上限 {tuning.max_connections}，请换实例或退避后重连"),
                tuning,
            )
        record = connections.touch(key=key, meta=meta, tenant=tenant, now=now, wall=wall, heartbeat=heartbeat)
        request.state.connection_record = record
        request.state.connection_key = key
        if _is_draining(request):
            if connections.note_drain():
                _LOGGER.info("msg_connection_drain_started")
            if connections.mark_drain_signaled(key):
                _LOGGER.info(
                    "msg_connection_drain_signal_sent",
                    connection_key=key,
                    tenant=record.tenant,
                    retry_after_ms=tuning.retry_after_ms,
                )
        return record

    return guard


CONNECTION_GUARD = build_connection_guard()
"""数据面与心跳共用的守卫（路由级依赖；含连接数上限判定）。"""

CONNECTION_INFO_GUARD = build_connection_guard(enforce_limit=False)
"""连接自报专用守卫（`GET /msg/connection`）：登记 / 清扫照旧，不判上限。"""

HEARTBEAT_GUARD = build_connection_guard(heartbeat=True)
"""心跳专用守卫（额外刷新 `last_heartbeat_at`）。"""

ConnectionRecordDep = Annotated[ConnectionRecord, Depends(CONNECTION_GUARD)]
"""数据面路由参数写法：`record: ConnectionRecordDep`（与路由级依赖同实例，FastAPI 只执行一次）。"""

ConnectionInfoRecordDep = Annotated[ConnectionRecord, Depends(CONNECTION_INFO_GUARD)]
"""连接自报路由参数写法：`record: ConnectionInfoRecordDep`。"""

HeartbeatRecordDep = Annotated[ConnectionRecord, Depends(HEARTBEAT_GUARD)]
"""心跳路由参数写法：`record: HeartbeatRecordDep`。"""
