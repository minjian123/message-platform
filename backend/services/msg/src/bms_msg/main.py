"""msg 服务应用入口：共享基座 + 产品服务装配。

> 8B_00 设计阶段声明服务身份、产品服务清单与表归属清单；01-1 接入主题与分区的元数据与管理面路由；
> 01-2 接入数据面投递（消息存储 / 位点 / 死信端口 + 内存实现，进程级单例落 `app.state`）。
> RPC / 其余控制面路由随对应任务追加。
"""

from bms_core.application import BaseServiceApplicationFactory
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.services.module_registry import ModuleRecord
from bms_core.services.table_registry import TableRecord
from fastapi import APIRouter, FastAPI

from bms_msg import CONTRACT_VERSION, PRODUCT_KEY, SERVICE_NAME, SERVICE_TITLE, __version__
from bms_msg.api.router import api_router
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.errors import register_msg_error_mapper
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
        """本产品表归属清单（元数据两表：`msg_topic` / `msg_partition`）。"""
        return ConcurrentStableList(MSG_TABLE_RECORDS)

    def service_routers(self) -> ConcurrentStableList[APIRouter]:
        """业务路由（管理面主题路由 + 数据面消息路由，均挂 `/api/v1`）。"""
        return ConcurrentStableList([api_router])

    def configure_service(self, app: FastAPI, settings: Settings) -> None:
        """数据面进程级状态注入：消息存储 / 位点 / 死信 / 投递状态各一份。

        替换实现只改这里（8B_02 换文件段与位点文件，端口与端点不变）；首期内存态随进程重启清空，
        已登记为遗留（详细设计 §12 风险 1）。

        Args:
            app: 应用实例（供 `api/deps.py` 读取）。
            settings: 应用配置（本任务不读）。
        """
        del settings
        app.state.message_store = InMemoryMessageStore()
        app.state.offset_store = InMemoryOffsetStore()
        app.state.dead_letter_queue = InMemoryDeadLetterQueue()
        app.state.delivery_state = DeliveryState()

    def configure_plugins(self, app: FastAPI, settings: Settings) -> None:
        """服务侧插件登记：租户守卫 `tenant_guard:msg` 与产品码覆盖 `error_mapper:msg`。

        Args:
            app: 应用实例（守卫工厂惰性读取 `app.state.tenant_source`）。
            settings: 应用配置（`[tenant_guard].provider = "msg"` / `[error_mapper].provider = "msg"` 时生效）。
        """
        del settings
        register_msg_tenant_guard(app)
        register_msg_error_mapper()
