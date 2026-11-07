"""05-1 用量 / 配额双端点（Kiwi 2452）。

覆盖：usage 端点四列 + 空数据；quota 端点空清单；两条路由均登记进公开契约。
"""

import json

import pytest
from bms_core.core.plugin import default_plugin_registry

from bms_msg.api.admin.quota import quota_projection, tenant_usage
from bms_msg.usage.columns import USAGE_COLUMNS

pytestmark = pytest.mark.kiwi_id(2452)


async def test_usage_endpoint_returns_columns_and_empty_items() -> None:
    """usage 端点返回四列元数据 + `items=[]` + `total=0`，不写 0 冒充用量。"""
    response = await tenant_usage()
    assert response.status_code == 200
    payload = json.loads(bytes(response.body))
    assert payload == {
        "columns": [{"key": column.key, "label": column.label} for column in USAGE_COLUMNS],
        "items": [],
        "total": 0,
    }


async def test_quota_endpoint_returns_empty_items() -> None:
    """quota 端点返回配额投影契约形状的空清单。"""
    response = await quota_projection()
    assert response.status_code == 200
    assert json.loads(bytes(response.body)) == {"items": []}


def test_routes_registered_in_openapi() -> None:
    """两条物理端点都进入应用 OpenAPI（契约快照的事实源）。"""
    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    app = ApplicationFactory().create(None)
    paths = app.openapi()["paths"]
    assert "/api/v1/msg/tenant-usage" in paths
    assert "/api/v1/msg/tenant-usage/quota" in paths
