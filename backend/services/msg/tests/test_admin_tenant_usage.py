"""06-8 租户用量管理面端点（Kiwi 2471）。

覆盖：响应结构（列元数据 + 一行值 + total）、四列顺序与 `USAGE_COLUMNS` 同源、
`storage_bytes` 不可用透传 `null`、`msg:query` 权限依赖、错误码声明与 OpenAPI 登记。
"""

import json
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.db.tenant import TenantContext
from fastapi.routing import APIRoute
from pydantic import ValidationError

from bms_msg.api.admin.usage import get_tenant_usage, router
from bms_msg.tenant.context import CurrentTenant
from bms_msg.usage.columns import USAGE_COLUMNS

pytestmark = pytest.mark.kiwi_id(2471)

_TENANT = CurrentTenant.from_context(TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=7001))
"""当前租户（端点只读租户主键）。"""


def _snapshot(*, storage_bytes: int | None = 1024) -> SimpleNamespace:
    """构造用量快照替身。

    Args:
        storage_bytes: 存储字节数（`None` 表示采集失败）。

    Returns:
        SimpleNamespace: 快照替身。
    """
    return SimpleNamespace(
        topic_count=3,
        partition_count=6,
        storage_bytes=storage_bytes,
        delivered_today=9,
        group_count=1,
        unacked_count=2,
        connection_count=4,
    )


class _FakeUsage:
    """用量快照服务替身。"""

    def __init__(self, snapshot: SimpleNamespace) -> None:
        """初始化。

        Args:
            snapshot: 返回的固定快照。
        """
        self._snapshot = snapshot

    async def snapshot(self, tenant_id: int) -> object:
        """返回固定快照（记录入参租户以断言取上下文租户）。

        Args:
            tenant_id: 租户主键。

        Returns:
            object: 快照。
        """
        del tenant_id
        return self._snapshot


async def test_endpoint_returns_columns_and_row() -> None:
    """端点返回四列元数据 + 一行真实用量，列顺序与 `USAGE_COLUMNS` 一致。"""
    response = await get_tenant_usage(cast("object", _FakeUsage(_snapshot())), _TENANT)  # type: ignore[arg-type]
    payload = json.loads(response.model_dump_json())
    assert payload["code"] == 0
    data = payload["data"]
    assert data["columns"] == [{"key": column.key, "label": column.label} for column in USAGE_COLUMNS]
    assert data["total"] == 1
    assert data["items"] == [{"topic_count": 3, "partition_count": 6, "storage_bytes": 1024, "delivered_today": 9}]


async def test_storage_unavailable_passes_null() -> None:
    """存储采集失败时该列为 null（不写 0，前端才能区分「零」与「不可用」）。"""
    response = await get_tenant_usage(cast("object", _FakeUsage(_snapshot(storage_bytes=None))), _TENANT)  # type: ignore[arg-type]
    data = json.loads(response.model_dump_json())["data"]
    assert data["items"][0]["storage_bytes"] is None


async def test_missing_counter_is_rejected_by_contract() -> None:
    """计数项缺失时由契约拒绝（不悄悄返回 null，避免前端把「坏数据」显示成零）。"""
    usage = _FakeUsage(SimpleNamespace(topic_count=1))
    with pytest.raises(ValidationError):
        await get_tenant_usage(cast("object", usage), _TENANT)  # type: ignore[arg-type]


def test_route_requires_query_permission() -> None:
    """路由挂 `msg:query` 权限依赖（浏览器直连也过服务端校验）。"""
    routes = [route for route in router.routes if isinstance(route, APIRoute)]
    assert len(routes) == 1
    route = routes[0]
    assert route.path == "/msg/admin/tenant-usage"
    assert route.methods == {"GET"}
    assert len(route.dependencies) == 1
    assert route.openapi_extra is not None
    assert route.openapi_extra["x-error-codes"] == [350011, 350012]


def test_route_registered_in_openapi() -> None:
    """端点进入应用 OpenAPI（契约快照的事实源）。"""
    from bms_core.core.plugin import default_plugin_registry

    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    app = ApplicationFactory().create(None)
    paths = app.openapi()["paths"]
    assert "/api/v1/msg/admin/tenant-usage" in paths
