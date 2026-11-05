"""03-5 用例共用的最小装配与两个 autouse 夹具（能力域：背压与流控）。

为什么单独放这里：`services/msg/tests` 不是包（跨文件 import 不可靠），而五个 03-5 用例文件
都需要「产品码覆盖已登记 + 错误码声明已装载 + 最小应用」。把三份夹具集中一处后，各文件只写
`from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins`。

夹具依赖（`db_session` / `tenant_source`）由 `services/msg/tests/conftest.py` 提供——pytest 按
「请求方模块可见的名字」解析，故这里不 import 它们。
"""

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

_MANIFEST = Path(__file__).resolve().parents[2] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""


@pytest.fixture(autouse=True)
def msg_plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def msg_declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（统一错误响应据此解析文案与状态）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def msg_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用（真实模块路由 + 租户中间件 + 统一错误渲染 + 内存库会话 + 背压进程状态）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例（`/api/v1` 全量业务路由 + 根路径 `/msg/*` 聚合）。
    """
    return build_frame_app(session=db_session, tenant_source=tenant_source)
