"""路由命名空间口径（Kiwi 2473；任务 07-1-2 §5）：内部前缀 / 对外派生 / 禁止自拼。

三层证据：

1. **契约快照**：业务路径一律 `/api/v1/msg/...`（对外 `/api/msg/v1/msg/...` 经网关剥离 `/api/msg/v1`
   后回到这里，域段保留为服务内首段）；
2. **真实装配**：`ApplicationFactory().create(None)` 的 OpenAPI 与快照同口径——证明路由确实挂在基座
   统一前缀下，而不是只在生成物里对；
3. **静态扫描**：服务源码不出现 `/api/msg` 字面量（对外前缀只由网关派生，模块自拼就是第二套口径）。
"""

import json
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.registry.products import load_product_manifest
from bms_core.services.gateway_catalog import product_route_prefix, upstream_node
from bms_core.services.module_registry import ServiceGroup

pytestmark = pytest.mark.kiwi_id(2473)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_CONTRACT = _REPO / "deploy" / "contracts" / "msg.json"
_MANIFEST = _REPO / "deploy" / "registry" / "msg.toml"
_SOURCE = _REPO / "backend" / "services" / "msg" / "src"

INTERNAL_PREFIX = "/api/v1/msg"
"""服务内统一接口前缀。"""

EXTERNAL_PREFIX = "/api/msg/v1/msg"
"""对外命名空间（网关按产品级路由派生）。"""

NON_BUSINESS_PATHS = ("/", "/healthz", "/readyz")
"""非业务路径（探针出口，豁免前缀断言）。"""

STREAM_PREFIXES = ("/msg/", "/rpc/")
"""长驻流入口前缀：帧协议（03-2）与 RPC（04-1）挂在根路径；对外经网关剥离 `/api/msg/v1` 后同样落到这里。"""


def _contract_paths() -> ConcurrentStableList[str]:
    """读契约快照的全部路径。

    Returns:
        ConcurrentStableList[str]: 路径（生成序）。
    """
    payload: Any = json.loads(_CONTRACT.read_text(encoding="utf-8"))
    return ConcurrentStableList(cast("dict[str, Any]", cast("dict[str, Any]", payload)["paths"]).keys())


def _business(paths: ConcurrentStableList[str]) -> ConcurrentStableList[str]:
    """筛出业务路径（以 `/api/` 开头）。

    Args:
        paths: 全部路径。

    Returns:
        ConcurrentStableList[str]: 业务路径。
    """
    return ConcurrentStableList(path for path in paths if path.startswith("/api/"))


def test_contract_paths_live_under_internal_prefix() -> None:
    """T4-a：业务路径全在 `/api/v1/msg` 下；其余只有探针出口与长驻流入口（`/msg/*`、`/rpc/*`）。"""
    paths = _contract_paths()
    business = _business(paths)

    assert business, "契约快照须含业务路径"
    misrouted = ConcurrentStableList(path for path in business if not path.startswith(f"{INTERNAL_PREFIX}/"))
    assert misrouted.to_list() == []

    streams = ConcurrentStableList(path for path in paths if any(path.startswith(prefix) for prefix in STREAM_PREFIXES))
    assert streams, "长驻流入口须在契约内（帧 / RPC 是同一命名空间的另一种形态）"
    others = ConcurrentStableList(
        path
        for path in paths
        if not path.startswith("/api/") and not any(path.startswith(prefix) for prefix in STREAM_PREFIXES)
    )
    assert sorted(others.to_list()) == sorted(NON_BUSINESS_PATHS)


def test_assembled_app_mounts_every_route_under_internal_prefix() -> None:
    """T4-b：真实装配后的 OpenAPI 与快照同口径（路由挂载在基座统一前缀下）。"""
    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    try:
        app = ApplicationFactory().create(None)
        paths = ConcurrentStableList(cast("dict[str, Any]", app.openapi()["paths"]).keys())
    finally:
        default_plugin_registry().reset()

    business = _business(paths)
    assert business, "装配后的应用须有业务路由"
    assert [path for path in business if not path.startswith(f"{INTERNAL_PREFIX}/")] == []


def test_external_namespace_is_derived_from_manifest() -> None:
    """T5-a：对外前缀 / 上游节点由清单三元组派生为任务字面值；服务行为产品分组。"""
    manifest = load_product_manifest(_MANIFEST)
    route = manifest.product_routes[0]
    service = manifest.services[0]

    assert product_route_prefix(route.product_key, route.domain) == EXTERNAL_PREFIX
    assert upstream_node(route.service_key) == "msg:8000"
    assert (route.product_key, route.domain, route.service_key) == ("msg", "msg", "msg")
    assert service.service_group == ServiceGroup.PRODUCT, "产品分组不生成服务级路由（避免 /api/msg/v1 双路径）"


def test_source_never_declares_product_prefix() -> None:
    """T5-b：源码不出现 `/api/msg` 字面量（自拼产品前缀 = 第二套对外口径）。"""
    offenders = ConcurrentStableList(
        path.relative_to(_REPO).as_posix()
        for path in sorted(_SOURCE.rglob("*.py"))
        if "/api/msg" in path.read_text(encoding="utf-8")
    )

    assert offenders.to_list() == []
