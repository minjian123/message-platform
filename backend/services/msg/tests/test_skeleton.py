"""后端骨架用例（08B 00_1，Kiwi 2410）：后端根标记 / 主模块可导入 / 迁移配置与版本目录一致。

背景：`bms_core` 以 `alembic.ini` 为后端工程根标记（`resolve_backend_root`），该文件缺失时
`import bms_msg.main` 直接 `ConfigError`（bms 04_02 遗留 54）。本用例是骨架的可回归判据。
"""

import configparser
from pathlib import Path

import pytest
from bms_core.core.paths import resolve_backend_root
from bms_core.core.plugin import default_plugin_registry

from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.main import ApplicationFactory
from services.table_registry import register as register_product_tables
from services.table_registry import table_names

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
@pytest.mark.kiwi_id(2412)
def test_factory_surface_lists_are_declared() -> None:
    """工厂三张清单可用：服务行一条、表归属三表（01-1 / 01-6）、业务路由一条（主题管理）。"""
    factory = ApplicationFactory()

    assert [record.module_key for record in factory.service_records()] == ["msg"]
    assert [record.table_name for record in factory.table_records()] == [
        "msg_topic",
        "msg_partition",
        "msg_replay_task",
    ]
    assert len(factory.service_routers()) == 1


@pytest.mark.kiwi_id(2410)
@pytest.mark.kiwi_id(2412)
def test_table_registry_register_is_idempotent() -> None:
    """表归属装载入口可调用且幂等（同值重复登记无副作用）。"""
    register_product_tables()
    register_product_tables()
    assert table_names() == ("msg_topic", "msg_partition", "msg_replay_task")


@pytest.mark.kiwi_id(2414)
@pytest.mark.kiwi_id(2416)
@pytest.mark.kiwi_id(2417)
def test_application_assembles_data_plane_state() -> None:
    """装配冒烟（01-2 / 01-4 / 01-5）：契约含数据面、组与延迟端点，进程级状态齐备。

    改能力域 / 装配代码后必须能起得来——`create()` 走完中间件、路由与 `configure_service()`，
    任何一处漏登记都会在这里暴露（而不是等推送预检或运行时才现）。
    """
    registry = default_plugin_registry()
    registry.reset()  # 先解冻：前置用例可能已构建过注册表（运行期只读，无法再登记）
    try:
        app = ApplicationFactory().create(None)
        paths = set(app.openapi()["paths"])
        assert {
            "/api/v1/msg/publish",
            "/api/v1/msg/consume",
            "/api/v1/msg/ack",
            "/api/v1/msg/join",
            "/api/v1/msg/heartbeat",
            "/api/v1/msg/leave",
            "/api/v1/msg/admin/groups",
            "/api/v1/msg/admin/groups/{namespace}/{topic}/{group}",
            "/api/v1/msg/admin/delayed",
        } <= paths
        assert isinstance(app.state.message_store, InMemoryMessageStore)
        assert isinstance(app.state.offset_store, InMemoryOffsetStore)
        assert isinstance(app.state.dead_letter_queue, InMemoryDeadLetterQueue)
        assert isinstance(app.state.delivery_state, DeliveryState)
        assert isinstance(app.state.group_store, InMemoryGroupStore)
        assert isinstance(app.state.delay_store, InMemoryDelayStore)
        assert isinstance(app.state.delay_scheduler, DelayScheduler)
        assert app.state.delay_scheduler.wheel is not None
        assert app.state.delay_scheduler.running is False, "推进器不得在装配期启动（惰性启动）"
    finally:
        # 解除构建冻结，避免后续用例因「注册表已构建」无法登记
        registry.reset()
