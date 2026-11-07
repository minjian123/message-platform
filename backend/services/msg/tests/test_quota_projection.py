"""配额只读投影（07D 任务 03 附录 C；Kiwi 2451）。

覆盖：`form` 与 `quota` 两条数据源声明并存且可装载；新增声明字段合 02-3 契约；
最小取数端点返回契约形状 `{"items": []}`（不写 0、不臆造用量）。
"""

import json
import tomllib
from pathlib import Path

import pytest
from bms_core.registry.products import load_product_manifest

from bms_msg.api.admin.quota import quota_projection

pytestmark = pytest.mark.kiwi_id(2451)

_MANIFEST = Path(__file__).resolve().parents[4] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（数据源与错误码的唯一事实源）。"""


async def test_projection_endpoint_returns_empty_items() -> None:
    """端点返回契约形状的空清单：平台侧四个声明维度判 `default`，不用 0 冒充用量。"""
    response = await quota_projection()
    assert json.loads(bytes(response.body)) == {"items": []}
    assert response.status_code == 200


def test_form_and_quota_sources_coexist() -> None:
    """两条声明并存：`form` 原样保留（前端直取），`quota` 补齐 02-3 五字段。"""
    payload = tomllib.loads(_MANIFEST.read_text(encoding="utf-8"))
    sources = {str(item["data_source_key"]): item for item in payload["data_sources"]}
    assert sources["msg_tenant_usage"]["kind"] == "form"
    assert sources["msg_tenant_usage"]["path"] == "/tenant-usage"
    quota = sources["msg_quota_projection"]
    assert quota["kind"] == "quota"
    assert quota["origin"] == "product"
    assert quota["product_key"] == "msg"
    assert str(quota["path"]).startswith("/api/v1/")
    assert quota["path"] == "/api/v1/msg/tenant-usage/quota"
    assert len(set(quota["quota_dimensions"])) == 4
    assert quota["quota_unit"] == "count"
    assert quota["quota_period"] == "none"


def test_manifest_loads_quota_declaration() -> None:
    """装载期校验通过：产品清单能解析出两条数据源（含 `kind=quota` 一条）。"""
    manifest = load_product_manifest(_MANIFEST)
    keys = {str(record.data_source_key) for record in manifest.data_sources}
    assert {"msg_tenant_usage", "msg_quota_projection"} <= keys
