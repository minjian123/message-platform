"""连接列表只读端点（`GET /api/v1/msg/admin/connections`；06-1-4）。

- **权限码**：`msg:query`；只返回当前租户的连接，匿名连接不展示。
- **数据源**：进程级 `ConnectionRegistry`（单实例视图；跨实例聚合归 02-8-4）。
- **脱敏**：IPv4 只留前两段、IPv6 只留前 64 位；`connection_id` 用连接键短哈希，不回传原始键。
"""

import time
from datetime import UTC, datetime
from hashlib import sha1
from typing import Annotated

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.api.deps import ConnectionRegistryDep, ConnectionTuningDep, CurrentTenantDep
from bms_msg.broker.connection import ConnectionRecord
from bms_msg.schemas.connection import ConnectionItem, ConnectionPage
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

router = BaseRouter(
    key="msg_admin_connections",
    prefix="/msg/admin/connections",
    tags=["msg-admin-connections"],
)


def _short_id(key: str) -> str:
    """把连接键压成短哈希（不回传原始 `tenant|host:port`）。

    Args:
        key: 连接键。

    Returns:
        str: 12 位十六进制短哈希。
    """
    return sha1(key.encode("utf-8")).hexdigest()[:12]


def _masked_host(host: str) -> str:
    """脱敏客户端地址。

    Args:
        host: 原始地址（`-` 表示无地址）。

    Returns:
        str: IPv4 `a.b.*.*`；IPv6 只留前 4 组 + `::/64`；其它原样。
    """
    if not host or host == "-":
        return "-"
    if ":" in host:
        groups = host.split(":")
        if len(groups) >= 4:
            return ":".join(groups[:4]) + "::/64"
        return host
    octets = host.split(".")
    if len(octets) == 4:
        return f"{octets[0]}.{octets[1]}.*.*"
    return host


def _state(record: ConnectionRecord, *, now: float, idle_timeout_ms: int) -> str:
    """按优先级判定连接状态。

    Args:
        record: 连接快照。
        now: 当前 monotonic 秒。
        idle_timeout_ms: 空闲阈值（毫秒）。

    Returns:
        str: `stale` / `draining` / `idle` / `active`。
    """
    if record.stale:
        return "stale"
    if record.drain_signaled:
        return "draining"
    if now - record.last_seen > idle_timeout_ms / 1000:
        return "idle"
    return "active"


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": []})
async def list_connections(
    tenant: CurrentTenantDep,
    connections: ConnectionRegistryDep,
    tuning: ConnectionTuningDep,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[ConnectionPage]:
    """连接列表（单实例视图）。

    Args:
        tenant: 当前租户。
        connections: 连接登记表。
        tuning: 连接参数（空闲阈值）。
        page: 页码。
        size: 每页条数。

    Returns:
        ApiResponse[ConnectionPage]: 按建立时刻倒序的当前页。
    """
    tenant_id = int(tenant.tenant_id)
    rows: ConcurrentStableList[ConnectionRecord] = ConcurrentStableList(
        sorted(
            (record for record in connections.records() if record.tenant_id == tenant_id),
            key=lambda record: record.opened_wall,
            reverse=True,
        )
    )
    total = len(rows)
    start = (page - 1) * size
    now = time.monotonic()
    items: ConcurrentStableList[ConnectionItem] = ConcurrentStableList(
        ConnectionItem(
            connection_id=_short_id(record.key),
            tenant_id=record.tenant_id,
            remote_prefix=_masked_host(record.client_host),
            http_version=record.http_version,
            scheme=record.scheme,
            opened_at=datetime.fromtimestamp(record.opened_wall, UTC),
            last_seen_at=datetime.fromtimestamp(record.last_seen_wall, UTC),
            last_heartbeat_at=(
                None if record.last_heartbeat_wall is None else datetime.fromtimestamp(record.last_heartbeat_wall, UTC)
            ),
            streams=record.streams,
            requests=record.requests,
            state=_state(record, now=now, idle_timeout_ms=tuning.idle_timeout_ms),
        )
        for record in rows[start : start + size]
    )
    return ApiResponse.ok(ConnectionPage(items=items, total=total, page=page, size=size))
