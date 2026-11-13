"""连接列表端点（06-1-4）：租户过滤、脱敏、分页、状态与空页。"""

import time
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.api.admin.connections import list_connections
from bms_msg.broker.connection import (
    ConnectionRegistry,
    connection_key,
    default_connection_tuning,
)
from bms_msg.broker.transport import ConnectionMeta
from bms_msg.schemas.connection import ConnectionPage
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2493)
"""本文件 4 例共用 Kiwi 2493（06-1-4 管理面错误映射与数据口径）。"""

TENANT_ID = 7
OTHER_TENANT_ID = 8


def _meta(host: str, *, port: int = 5000, version: str = "2") -> ConnectionMeta:
    """构造连接元数据。

    Args:
        host: 客户端地址。
        port: 客户端端口。
        version: HTTP 版本。

    Returns:
        ConnectionMeta: 元数据。
    """
    return ConnectionMeta(client_host=host, client_port=port, http_version=version, scheme="http", is_tls=False)


def _register(
    registry: ConnectionRegistry,
    *,
    host: str,
    tenant: str,
    tenant_id: int,
    port: int = 5000,
    age_seconds: float = 0.0,
) -> None:
    """登记一条连接。

    Args:
        registry: 连接登记表。
        host: 客户端地址。
        tenant: 租户编码。
        tenant_id: 租户内部身份。
        port: 客户端端口。
        age_seconds: 最后活跃距今秒数（模拟空闲）。
    """
    now = time.monotonic() - age_seconds
    wall = time.time() - age_seconds
    meta = _meta(host, port=port)
    registry.touch(
        key=connection_key(meta, tenant=tenant),
        meta=meta,
        tenant=tenant,
        now=now,
        wall=wall,
        heartbeat=True,
        tenant_id=tenant_id,
    )


async def _fetch(registry: ConnectionRegistry, *, page: int = 1, size: int = 20) -> ConnectionPage:
    """调用端点并取分页体。

    Args:
        registry: 连接登记表。
        page: 页码。
        size: 每页条数。

    Returns:
        ConnectionPage: 响应体。
    """
    response = await list_connections(
        tenant=cast("CurrentTenant", SimpleNamespace(tenant_id=str(TENANT_ID))),
        connections=registry,
        tuning=default_connection_tuning(),
        page=page,
        size=size,
    )
    return cast("ConnectionPage", response.data)


async def test_connections_filters_tenant_and_masks_host() -> None:
    """只返回当前租户；IPv4 只留前两段；连接标识为短哈希。"""
    registry = ConnectionRegistry()
    _register(registry, host="10.1.2.3", tenant="demo", tenant_id=TENANT_ID, port=1111)
    _register(registry, host="10.9.9.9", tenant="other", tenant_id=OTHER_TENANT_ID, port=2222)
    _register(registry, host="10.8.8.8", tenant="demo", tenant_id=TENANT_ID, port=3333)

    page = await _fetch(registry)

    assert page.total == 2
    assert len(page.items) == 2
    assert {item.remote_prefix for item in page.items} == {"10.1.*.*", "10.8.*.*"}
    assert all(len(item.connection_id) == 12 for item in page.items)
    assert all(item.state == "active" for item in page.items)


async def test_connections_pagination_and_idle_state() -> None:
    """分页按建立时刻倒序；超过空闲阈值判 idle。"""
    registry = ConnectionRegistry()
    _register(registry, host="10.1.0.1", tenant="demo", tenant_id=TENANT_ID, port=1111, age_seconds=120)
    _register(registry, host="10.1.0.2", tenant="demo", tenant_id=TENANT_ID, port=2222)

    first = await _fetch(registry, page=1, size=1)
    second = await _fetch(registry, page=2, size=1)

    assert first.total == 2
    assert len(first.items) == 1
    assert first.items[0].remote_prefix == "10.1.*.*"
    assert first.items[0].state == "active"
    assert second.items[0].state == "idle"


async def test_connections_state_and_host_edges() -> None:
    """draining / stale 与无地址 / 短 IPv6 的分支。"""
    registry = ConnectionRegistry()
    _register(registry, host="10.1.0.1", tenant="demo", tenant_id=TENANT_ID, port=1111)
    draining_key = connection_key(_meta("10.1.0.1", port=1111), tenant="demo")
    assert registry.mark_drain_signaled(draining_key)
    _register(registry, host="", tenant="demo", tenant_id=TENANT_ID, port=0)
    _register(registry, host="::1", tenant="demo", tenant_id=TENANT_ID, port=2222)
    _register(registry, host="10.1.2", tenant="demo", tenant_id=TENANT_ID, port=3333)

    page = await _fetch(registry)
    states = {item.remote_prefix: item.state for item in page.items}

    assert states["10.1.*.*"] == "draining"
    assert states["-"] == "active"
    assert states["::1"] == "active"
    assert states["10.1.2"] == "active"

    registry.evict(now=time.monotonic() + 3600, limit=10, idle_timeout_ms=1)
    stale = await _fetch(registry)
    assert any(item.state == "stale" for item in stale.items)


async def test_connections_ipv6_and_empty() -> None:
    """IPv6 只留前 64 位；无连接返回空页。"""
    empty = await _fetch(ConnectionRegistry())
    assert empty.total == 0
    assert len(empty.items) == 0

    registry = ConnectionRegistry()
    _register(registry, host="2001:db8:85a3:8d3:1319:8a2e:370:7348", tenant="demo", tenant_id=TENANT_ID)
    page = await _fetch(registry)

    assert page.items[0].remote_prefix == "2001:db8:85a3:8d3::/64"


def test_connection_page_items_collection_type() -> None:
    """分页 items 用插入序集合类（契约口径）。"""
    page = ConnectionPage(items=ConcurrentStableList(), total=0, page=1, size=20)
    assert isinstance(page.items, ConcurrentStableList)
