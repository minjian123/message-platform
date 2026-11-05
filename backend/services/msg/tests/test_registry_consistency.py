"""代码清单 ↔ 产品声明逐字段同值（Kiwi 2411；00-1 遗留缺陷的护栏）。

背景：`services/module_registry.py`（代码清单）与 `deploy/registry/msg.toml`（产品声明）在应用装配期
**同时登记**，合并规则是「同键同值去重、**异值即拒**」。两者一旦漂移（本次实测：产品档案缺
`frontend_package_source`、服务行 `build_batch` 不一致），`ApplicationFactory().create()` 直接
`CatalogError` —— 而 00-1 只验证了「可导入 / 可构造」，应用实际装配不起来直到契约导出时才暴露。
本用例把该漂移变成可回归的断言。
"""

from dataclasses import asdict
from pathlib import Path

import pytest
from bms_core.registry.products import load_product_manifest

from services.module_registry import MSG_PRODUCT_RECORDS, MSG_SERVICE_RECORDS, PRODUCT_KEY

pytestmark = pytest.mark.kiwi_id(2411)

_MANIFEST = Path(__file__).resolve().parents[4] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（与代码清单同值）。"""


def _manifest() -> object:
    """装载产品声明。

    Returns:
        object: 产品清单对象（`ProductManifest`）。
    """
    return load_product_manifest(_MANIFEST)


def _diff(declared: object, coded: object) -> dict[str, tuple[object, object]]:
    """逐字段比对声明与代码清单。

    Args:
        declared: 声明侧记录。
        coded: 代码侧记录。

    Returns:
        dict[str, tuple[object, object]]: 字段 → (声明值, 代码值)；空字典表示同值。
    """
    diffs: dict[str, tuple[object, object]] = {}
    for field, value in asdict(declared).items():  # type: ignore[call-overload]
        coded_value = getattr(coded, field)
        if value != coded_value:
            diffs[field] = (value, coded_value)
    return diffs


def test_product_record_matches_manifest() -> None:
    """产品档案同值：漏 `frontend_package_source` 等字段即装配期拒启。"""
    manifest = _manifest()
    assert _diff(manifest.products[0], MSG_PRODUCT_RECORDS[0]) == {}  # type: ignore[attr-defined]


def test_service_record_matches_manifest() -> None:
    """服务行同值：`build_batch` / 段位 / 表前缀等任一漂移都会让装配期合并拒启。"""
    manifest = _manifest()
    assert _diff(manifest.services[0], MSG_SERVICE_RECORDS[0]) == {}  # type: ignore[attr-defined]
    assert MSG_SERVICE_RECORDS[0].product_key == PRODUCT_KEY


def test_application_assembles_with_manifest() -> None:
    """真实装配：`create(None)` 能带着代码清单 + 产品声明起来（00-1 只验证到「可构造」）。

    **先 `reset()`**：注册表构建后运行期只读，前置用例（`test_entrypoints` 跑 `run_service`、
    `test_skeleton` 装配冒烟）跑过之后会停在「已构建」态，不重置就会抛
    `PluginError: 插件注册表已构建`——那是用例顺序问题，不是装配问题（02-3 选测暴露）。
    """
    from bms_core.core.plugin import default_plugin_registry

    from bms_msg.main import ApplicationFactory

    default_plugin_registry().reset()
    app = ApplicationFactory().create(None)
    assert app.title
