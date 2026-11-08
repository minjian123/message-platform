"""05-1 用量 / 配额双端点（Kiwi 2452；05-2 接入真实取数后同步契约）。

覆盖：usage 端点四列 + 当前租户一行真实用量；quota 投影端点六维形状（按单位拆分）；
三条路由均登记进公开契约。
"""

import json
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.tenant import TenantContext

from bms_msg.api.admin.quota import (
    quota_projection,
    quota_projection_bytes,
    quota_projection_rate,
    tenant_usage,
)
from bms_msg.quota.cache import QuotaCache
from bms_msg.quota.service import QuotaService
from bms_msg.tenant.context import CurrentTenant
from bms_msg.usage.columns import USAGE_COLUMNS

pytestmark = pytest.mark.kiwi_id(2452)

_TENANT = CurrentTenant.from_context(TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=7001))
"""当前租户（端点只读租户主键）。"""


class _FakeUsage:
    """用量快照替身：返回固定四列值。"""

    def __init__(self) -> None:
        """初始化。"""
        self._snapshot = SimpleNamespace(
            topic_count=3,
            partition_count=6,
            storage_bytes=1024,
            delivered_today=9,
            group_count=1,
            unacked_count=2,
            connection_count=4,
        )

    async def snapshot(self, tenant_id: int) -> object:
        """返回固定快照。

        Args:
            tenant_id: 租户主键。

        Returns:
            object: 快照。
        """
        del tenant_id
        return self._snapshot


def _quota() -> QuotaService:
    """只读缓存的配额服务（缺省限额，用量不可用）。

    Returns:
        QuotaService: 配额服务。
    """
    return QuotaService(cache=QuotaCache())


async def test_usage_endpoint_returns_columns_and_real_row() -> None:
    """usage 端点返回四列元数据 + 一行真实用量。"""
    response = await tenant_usage(cast("object", _FakeUsage()), _TENANT)  # type: ignore[arg-type]
    assert response.status_code == 200
    payload = json.loads(bytes(response.body))
    assert payload["columns"] == [{"key": column.key, "label": column.label} for column in USAGE_COLUMNS]
    assert payload["total"] == 1
    assert payload["items"] == [{"topic_count": 3, "partition_count": 6, "storage_bytes": 1024, "delivered_today": 9}]


async def test_quota_projection_endpoints_return_contract_shape() -> None:
    """三条投影端点各返回契约形状（usage 不可用为 null，不写 0）。"""
    count = json.loads(bytes((await quota_projection(_quota(), _TENANT, None)).body))
    bytes_payload = json.loads(bytes((await quota_projection_bytes(_quota(), _TENANT, None)).body))
    rate = json.loads(bytes((await quota_projection_rate(_quota(), _TENANT, None)).body))
    assert [item["quota_key"] for item in count["items"]] == [
        "topic_count",
        "group_count",
        "unacked_count",
        "connection_count",
    ]
    assert [item["quota_key"] for item in bytes_payload["items"]] == ["storage_bytes"]
    assert [item["quota_key"] for item in rate["items"]] == ["produce_rate"]
    assert all(item["usage"] is None for item in count["items"])


def test_routes_registered_in_openapi() -> None:
    """三条投影端点与管理面两条端点都进入应用 OpenAPI（契约快照的事实源）。"""
    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    app = ApplicationFactory().create(None)
    paths = app.openapi()["paths"]
    assert "/api/v1/msg/tenant-usage" in paths
    assert "/api/v1/msg/tenant-usage/quota" in paths
    assert "/api/v1/msg/tenant-usage/quota/bytes" in paths
    assert "/api/v1/msg/tenant-usage/quota/rate" in paths
    assert "/api/v1/msg/admin/quotas" in paths
