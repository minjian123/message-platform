"""配额只读投影声明（07D 任务 03 附录 C；Kiwi 2451）。

覆盖：`form` 与三条 `quota` 声明并存且可装载；声明字段合 02-3 契约（按单位拆分）；
投影端点返回契约形状（`usage` 为 null 表示不可用，不写 0）。
"""

# pyright: reportPrivateUsage=false
import json
import tomllib
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.tenant import TenantContext
from bms_core.registry.products import load_product_manifest

from bms_msg.api.admin.quota import quota_projection
from bms_msg.quota.cache import QuotaCache
from bms_msg.quota.service import QuotaService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2451)

_MANIFEST = Path(__file__).resolve().parents[4] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（数据源与错误码的唯一事实源）。"""

_TENANT = CurrentTenant.from_context(TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=7001))


async def test_projection_endpoint_returns_contract_shape() -> None:
    """端点返回契约形状：四维条数配额，用量不可用为 null，不用 0 冒充。"""
    response = await quota_projection(QuotaService(cache=QuotaCache()), _TENANT, None)
    payload = json.loads(bytes(response.body))
    assert response.status_code == 200
    assert [item["quota_key"] for item in payload["items"]] == [
        "topic_count",
        "group_count",
        "unacked_count",
        "connection_count",
    ]
    assert all(item["usage"] is None for item in payload["items"])


def test_form_and_quota_sources_coexist() -> None:
    """四条声明并存：`form` 原样保留，三条 `quota` 各带单位与维度。"""
    payload = tomllib.loads(_MANIFEST.read_text(encoding="utf-8"))
    sources = {str(item["data_source_key"]): item for item in payload["data_sources"]}
    assert sources["msg_tenant_usage"]["kind"] == "form"
    assert sources["msg_tenant_usage"]["path"] == "/tenant-usage"
    expected = {
        "msg_quota_count": ("/api/v1/msg/tenant-usage/quota", "count", 4),
        "msg_quota_bytes": ("/api/v1/msg/tenant-usage/quota/bytes", "bytes", 1),
        "msg_quota_rate": ("/api/v1/msg/tenant-usage/quota/rate", "qps", 1),
    }
    for key, (path, unit, dimensions) in expected.items():
        source = sources[key]
        assert source["kind"] == "quota"
        assert source["origin"] == "product"
        assert source["product_key"] == "msg"
        assert source["path"] == path
        assert source["quota_unit"] == unit
        assert source["quota_period"] == "none"
        assert len(set(source["quota_dimensions"])) == dimensions


def test_manifest_loads_quota_declarations() -> None:
    """装载期校验通过：产品清单能解析出四条数据源（含三条 `kind=quota`）。"""
    manifest = load_product_manifest(_MANIFEST)
    keys = {str(record.data_source_key) for record in manifest.data_sources}
    assert {"msg_tenant_usage", "msg_quota_count", "msg_quota_bytes", "msg_quota_rate"} <= keys


def test_projection_payload_skips_other_units() -> None:
    """投影只输出声明单位对应的维度（count / bytes / qps 不串源）。"""
    from bms_msg.api.admin.quota import _projection_payload
    from bms_msg.quota.service import QuotaAssessment

    cache = QuotaCache()
    limit = cache.resolve(7001, "", "storage_bytes")
    assessment = QuotaAssessment(limit=limit, usage=5, state="ok")
    payload = _projection_payload(ConcurrentStableList([assessment]), unit="count")
    assert payload == []
