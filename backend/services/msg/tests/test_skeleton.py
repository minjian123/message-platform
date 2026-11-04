"""后端骨架用例（08B 00_1，Kiwi 2410）：后端根标记 / 主模块可导入 / 迁移配置与版本目录一致。

背景：`bms_core` 以 `alembic.ini` 为后端工程根标记（`resolve_backend_root`），该文件缺失时
`import bms_msg.main` 直接 `ConfigError`（bms 04_02 遗留 54）。本用例是骨架的可回归判据。
"""

import configparser
from pathlib import Path

import pytest
from bms_core.core.paths import resolve_backend_root

from bms_msg.main import ApplicationFactory
from services.table_registry import register as register_product_tables

_BACKEND_ROOT = Path(__file__).resolve().parents[3]
"""后端工程根（本文件 `backend/services/msg/tests/` 上溯三级）。"""

_SERIES_SECTIONS = ("alembic", "alembic:msg:platform")
"""迁移配置段：缺省段 + 消息平台域 × 平台服务库。"""


@pytest.mark.kiwi_id(2410)
def test_backend_root_is_resolvable() -> None:
    """`alembic.ini` 存在且被解析为 msg 后端根（缺文件即 ConfigError）。"""
    root = resolve_backend_root("alembic.ini", anchor=Path(__file__))

    assert root == _BACKEND_ROOT
    assert (root / "alembic.ini").is_file()


@pytest.mark.kiwi_id(2410)
def test_application_factory_is_constructible() -> None:
    """主模块可导入、工厂可构造（服务名 / 产品键与包声明一致）。"""
    factory = ApplicationFactory()

    assert factory.service_name == "msg"
    assert factory.product_key == "msg"
    assert factory.contract_version != ""


@pytest.mark.kiwi_id(2410)
def test_migration_sections_point_to_existing_version_dir() -> None:
    """两段配置的 `version_locations` 一致且目录存在（Alembic 在 env.py 之前读取该路径）。"""
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(_BACKEND_ROOT / "alembic.ini", encoding="utf-8")

    first: Path | None = None
    for section in _SERIES_SECTIONS:
        raw = parser.get(section, "version_locations")
        location = Path(raw.replace("%(here)s", str(_BACKEND_ROOT)))
        assert location.is_dir(), f"{section} 的版本目录不存在：{location}"
        if first is None:
            first = location
        else:
            assert location == first, "缺省段与显式清单段必须共用同一版本目录"


@pytest.mark.kiwi_id(2410)
def test_factory_surface_lists_are_declared() -> None:
    """工厂三张清单可用：服务行一条、表归属为空（8B_00）、业务路由为空（8B_01 起接入）。"""
    factory = ApplicationFactory()

    assert [record.module_key for record in factory.service_records()] == ["msg"]
    assert list(factory.table_records()) == []
    assert list(factory.service_routers()) == []


@pytest.mark.kiwi_id(2410)
def test_table_registry_register_is_idempotent() -> None:
    """表归属装载入口可调用且幂等（8B_00 为空清单，首批表登记后自动生效）。"""
    register_product_tables()
    register_product_tables()
