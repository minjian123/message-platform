"""msg 服务应用入口：共享基座 + 产品服务装配。

> 8B_00 设计阶段：仅声明服务身份、产品服务清单与表归属清单；业务域 / 路由 / 消费组
> 在 8B_01 起按详细设计逐步接入。当前不提供任何业务端点（探针由基座统一挂载）。
"""

from bms_core.application import BaseServiceApplicationFactory
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.services.module_registry import ModuleRecord
from bms_core.services.table_registry import TableRecord
from fastapi import APIRouter, FastAPI

from bms_msg import CONTRACT_VERSION, PRODUCT_KEY, SERVICE_NAME, SERVICE_TITLE, __version__
from bms_msg.tenant_guard import register_msg_tenant_guard
from services.module_registry import MSG_SERVICE_RECORDS
from services.table_registry import MSG_TABLE_RECORDS


class ApplicationFactory(BaseServiceApplicationFactory):
    """msg 消息平台服务应用工厂（通用装配由共享基座承载）。"""

    key: str = "application_factory"
    service_name: str = SERVICE_NAME
    service_title: str = SERVICE_TITLE
    version: str = __version__
    contract_version: str = CONTRACT_VERSION
    product_key: str | None = PRODUCT_KEY

    def service_records(self) -> ConcurrentStableList[ModuleRecord]:
        """本产品服务清单（产品自持；与 `deploy/registry/msg.toml` 服务行同值）。"""
        return ConcurrentStableList(MSG_SERVICE_RECORDS)

    def table_records(self) -> ConcurrentStableList[TableRecord]:
        """本产品表归属清单（8B_00 设计阶段为空，表随详细设计登记）。"""
        return ConcurrentStableList(MSG_TABLE_RECORDS)

    def service_routers(self) -> ConcurrentStableList[APIRouter]:
        """业务路由（8B_01 起接入；当前无业务端点）。"""
        return ConcurrentStableList()

    def configure_plugins(self, app: FastAPI, settings: Settings) -> None:
        """服务侧插件登记：把 msg 守卫登记为 `tenant_guard:msg`（须在建注册表之前）。

        Args:
            app: 应用实例（守卫工厂惰性读取 `app.state.tenant_source`）。
            settings: 应用配置（`[tenant_guard].provider = "msg"` 时生效）。
        """
        del settings
        register_msg_tenant_guard(app)
