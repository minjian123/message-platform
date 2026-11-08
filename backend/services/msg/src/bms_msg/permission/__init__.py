"""msg 权限能力接入（05-3）：真实 RBAC + platform 权限数据来源。

- 登记数据来源（经 platform 内部只读契约取数）与真实校验器（`rbac`，实现随基座分发）；
- 未配置 `[permission].provider` 时由 `main.ApplicationFactory.prepare_settings` 置为 `rbac`；
- 插件注册按注册表实例幂等，数据来源每次补齐（用例隔离会重置数据来源注册表）。
"""

from __future__ import annotations

from typing import cast

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.plugin import default_plugin_registry
from bms_core.permission.rbac import register_rbac_permission
from bms_core.permission.sources import register_permission_data_source_factory
from bms_core.servicecall.base import BaseServiceClient
from fastapi import Request

from bms_msg.permission.source import MSG_PERMISSION_DATA_SOURCE_KEY, MsgPlatformPermissionDataSource

__all__ = [
    "MSG_PERMISSION_DATA_SOURCE_KEY",
    "build_msg_permission_source",
    "register_msg_permission",
]

_REGISTERED_REGISTRIES: ConcurrentStableList[object] = ConcurrentStableList()
"""已登记 msg 侧权限装配的注册表实例清单（按实例幂等）。"""


def register_msg_permission(settings: Settings) -> None:
    """登记 msg 侧权限装配（真实校验器 + 数据来源工厂）。

    Args:
        settings: 应用配置（读 `[permission]` 的档位与选项）。
    """
    register_permission_data_source_factory(MSG_PERMISSION_DATA_SOURCE_KEY, build_msg_permission_source)
    registry = default_plugin_registry()
    if any(item is registry for item in _REGISTERED_REGISTRIES):
        return
    register_rbac_permission(settings)
    _REGISTERED_REGISTRIES.add(registry)


async def build_msg_permission_source(request: Request) -> MsgPlatformPermissionDataSource:
    """按当前请求构造权限数据来源（服务间客户端取自应用装配）。

    Args:
        request: 当前请求（取 `app.state.service_client`）。

    Returns:
        MsgPlatformPermissionDataSource: 经契约取数的数据来源实例。
    """
    client = cast("BaseServiceClient", request.app.state.service_client)
    return MsgPlatformPermissionDataSource(client)
