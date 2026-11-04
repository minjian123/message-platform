"""msg 服务应用入口：共享基座 + 产品服务装配。

> 8B_00 设计阶段声明服务身份、产品服务清单与表归属清单；01-1 接入主题与分区的元数据与管理面路由；
> 01-2 接入数据面投递（消息存储 / 位点 / 死信端口 + 内存实现，进程级单例落 `app.state`）；
> 01-4 接入消费组（组存储 + 协议编排 + 组只读视图）；
> 01-5 接入延迟区（延迟存储 + 时间轮推进器 + 只读待投视图，推进任务惰性启动）；
> 01-6 接入保留期清理（端口 + 内存实现，请求驱动）与重放任务（任务表 + 执行器 + 审计出口）。
> RPC / 其余控制面路由随对应任务追加。
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from bms_core.application import BaseServiceApplicationFactory
from bms_core.audit.emit import AuditEventEmitter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.db.keys import PLATFORM_DB_KEY
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import SessionFactory, session_scope
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.services.module_registry import ModuleRecord
from bms_core.services.table_registry import TableRecord
from fastapi import APIRouter, FastAPI

from bms_msg import CONTRACT_VERSION, PRODUCT_KEY, SERVICE_NAME, SERVICE_TITLE, __version__
from bms_msg.api.router import api_router
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import AuditSink, ReplayAuditor, ReplayRunner, UowFactory
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
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
        """本产品表归属清单（元数据三表：`msg_topic` / `msg_partition` / `msg_replay_task`）。"""
        return ConcurrentStableList(MSG_TABLE_RECORDS)

    def service_routers(self) -> ConcurrentStableList[APIRouter]:
        """业务路由（管理面主题 / 组 / 延迟 / 重放路由 + 数据面消息路由，均挂 `/api/v1`）。"""
        return ConcurrentStableList([api_router])

    def configure_service(self, app: FastAPI, settings: Settings) -> None:
        """数据面进程级状态注入：消息存储 / 位点 / 死信 / 投递状态 / 组存储 / 延迟区与推进器 /
        审计出口 / 重放执行器各一份。

        替换实现只改这里（8B_02 换文件段与 `.delay` 段、位点文件与 `msg_group` 两表，端口与端点不变）；
        首期内存态随进程重启清空，已登记为遗留（详细设计 §12 风险 1）。

        **两个后台任务都不在此启动**：延迟推进器由首个延迟发布触发、重放执行器由首个建任务触发
        （基座 `service_lifespan` 无产品启动钩子，显式注册归 8B_01）——没有对应消息 / 任务就不起后台任务。

        Args:
            app: 应用实例（供 `api/deps.py` 读取）。
            settings: 应用配置（审计发件箱实现选择取自 `[outbox_store]`）。
        """
        app.state.message_store = InMemoryMessageStore()
        app.state.offset_store = InMemoryOffsetStore()
        app.state.dead_letter_queue = InMemoryDeadLetterQueue()
        app.state.delivery_state = DeliveryState()
        app.state.group_store = InMemoryGroupStore()
        delay_store = InMemoryDelayStore()
        app.state.delay_store = delay_store
        app.state.delay_scheduler = DelayScheduler(
            store=delay_store,
            messages=app.state.message_store,
            wheel=DelayWheel(),
        )
        sink: AuditSink = AuditEventEmitter(
            registry=cast("EngineRegistry", app.state.engine_registry),
            session_factory=cast("SessionFactory", app.state.session_factory),
            settings=settings,
        )
        app.state.audit_sink = sink
        app.state.replay_runner = ReplayRunner(
            messages=app.state.message_store,
            uow_factory=platform_uow_factory(app),
            auditor=ReplayAuditor(sink=sink),
        )

    def configure_plugins(self, app: FastAPI, settings: Settings) -> None:
        """服务侧插件登记：租户守卫 `tenant_guard:msg` 与产品码覆盖 `error_mapper:msg`。

        Args:
            app: 应用实例（守卫工厂惰性读取 `app.state.tenant_source`）。
            settings: 应用配置（`[tenant_guard].provider = "msg"` / `[error_mapper].provider = "msg"` 时生效）。
        """
        del settings
        register_msg_tenant_guard(app)
        register_msg_error_mapper()


def platform_uow_factory(app: FastAPI) -> UowFactory:
    """构造「无请求上下文时也能开平台库工作单元」的工厂（重放执行器用）。

    Args:
        app: 应用实例（取引擎注册表与会话工厂）。

    Returns:
        UowFactory: 可调用对象，返回异步上下文管理器（产出绑定平台库会话的工作单元）。
    """
    registry = cast("EngineRegistry", app.state.engine_registry)
    session_factory = cast("SessionFactory", app.state.session_factory)

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """开一个平台库短会话并包装成工作单元。

        Yields:
            UnitOfWork: 绑定平台库会话的工作单元。
        """
        async with session_scope(registry, db_key=PLATFORM_DB_KEY, factory=session_factory) as session:
            yield DbUnitOfWork(session)

    return _uow
