"""msg 租户可用性守卫：继承基座默认实现，补命名空间校验（消费入口用，16_01 接入）。

- **继承不改基座**：`MsgTenantGuard` 只覆写扩展钩子 `verify(context, scope)`，租户解析、状态判定、
  错误码（80001 / 80002 / 10007）全部沿用基座；停用即时生效靠基座租户源的全局版本戳。
- **命名空间校验**：`scope` 由消费入口传消息的 `namespace`，须属服务目录**已登记且启用**的
  `module_key`（`service_catalog_view()` + `ModuleStatus.ENABLED`），否则拒绝——msg 不自造模块
  （《架构设计 · 多租户与安全》§2.2）。
- **登记**：`configure_plugins()` 内 `register_plugin("tenant_guard", "msg", …)`，配置
  `[tenant_guard].provider = "msg"` 生效（注册表冻结前登记、运行期只读）。
"""

from bms_core.core.exceptions import ParamError
from bms_core.core.factory import BasePluginFactory
from bms_core.core.plugin import DEFAULT_CONTRACT_VERSION, register_plugin
from bms_core.db.tenant import TenantContext
from bms_core.services.module_registry import ModuleStatus, service_catalog_view
from bms_core.tenant.guard import SourceTenantGuard, tenant_source_reader
from fastapi import FastAPI

__all__ = ["MSG_TENANT_GUARD", "MsgTenantGuard", "MsgTenantGuardFactory"]

MSG_TENANT_GUARD = "msg"
"""msg 守卫实现名（配置 `[tenant_guard].provider = "msg"`）。"""


class MsgTenantGuard(SourceTenantGuard):
    """消息平台租户守卫：租户可用性 + 命名空间白名单。"""

    plugin_name: str = MSG_TENANT_GUARD
    contract_version: str = DEFAULT_CONTRACT_VERSION

    def verify(self, context: TenantContext, scope: str | None = None) -> None:
        """命名空间校验（`scope` 即消息的 namespace）。

        Args:
            context: 已通过租户可用性判定的上下文。
            scope: 消息命名空间；`None` 表示调用方未指定范围，跳过命名空间校验（仅做租户校验）。

        Raises:
            ParamError: 命名空间未在服务目录登记或未启用（10001；msg 自有错误码段 35010 随
                8B_01_01 错误码模块建立后替换）。
        """
        del context
        if scope is None:
            return
        for record in service_catalog_view():
            if record.module_key == scope and record.status == ModuleStatus.ENABLED:
                return
        raise ParamError(f"命名空间未登记或未启用（module_key 口径）：{scope}")


class MsgTenantGuardFactory(BasePluginFactory[MsgTenantGuard]):
    """msg 守卫工厂（装配期登记 `tenant_guard:msg`）。"""

    plugin_key: str = "tenant_guard"
    plugin_name: str = MSG_TENANT_GUARD

    def __init__(self, app: object) -> None:
        """初始化。

        Args:
            app: 应用实例（惰性读取 `app.state.tenant_source`）。
        """
        self._app = app

    def create(self, options: None = None) -> MsgTenantGuard:
        """构造 msg 守卫（租户源在每次判定时读取，不捕获实例）。

        Args:
            options: 未使用（零参口径）。

        Returns:
            MsgTenantGuard: 守卫实例。
        """
        del options
        return MsgTenantGuard(source_provider=tenant_source_reader(self._app))


def register_msg_tenant_guard(app: FastAPI) -> None:
    """登记 msg 守卫实现（服务装配钩子内调用，须在插件注册表冻结前）。

    Args:
        app: 应用实例。
    """
    register_plugin("tenant_guard", MSG_TENANT_GUARD, MsgTenantGuardFactory(app))
