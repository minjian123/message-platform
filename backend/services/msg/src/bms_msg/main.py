"""msg 服务应用入口：共享基座 + 产品服务装配。

> 8B_00 设计阶段声明服务身份、产品服务清单与表归属清单；01-1 接入主题与分区的元数据与管理面路由；
> 01-2 接入数据面投递（消息存储 / 位点 / 死信端口 + 内存实现，进程级单例落 `app.state`）；
> 01-4 接入消费组（组存储 + 协议编排 + 组只读视图）；
> 01-5 接入延迟区（延迟存储 + 时间轮推进器 + 只读待投视图）；
> 01-6 接入保留期清理（端口 + 内存实现，请求驱动）与重放任务（任务表 + 执行器 + 审计出口）；
> 03-1 接入 HTTP/2 帧入口（根路径 `/msg/*` 复用数据面 + SSE 长驻流 + 连接元数据），并把
> 延迟推进器与重放执行器挂到基座 `startup_hooks()`（装配后启动、停机随资源管理器回收）；
> 03-4 接入连接管理（连接登记表 + 连接数 / 长驻流上限 + 应用层心跳 + drain 协商）；
> 03-5 接入背压与流控（分区唤醒器 + inflight 双阈值 + 两级限流 + 指标 collector，消息存储包一层通知）；
> 02-1 把消息存储换成**文件段实现**（目录登记表 + 记录格式 + 段与索引 + length 级尾部恢复），
> `NotifyingMessageStore` 仍是最外层。
> RPC / 其余控制面路由随对应任务追加。
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from bms_core.application import BaseServiceApplicationFactory, StartupHook
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
from bms_msg.api.frame.gate import register_frame_handlers
from bms_msg.api.router import api_router, frame_router
from bms_msg.broker.backpressure import RateLimiter, default_rate_limit_tuning
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory, data_root
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.lifecycle import start_background_tasks
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import AuditSink, ReplayAuditor, ReplayRunner, UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore
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
        """业务路由：`/api/v1` 聚合（管理面 + 既有数据面）与根路径聚合（`/msg/*` 帧入口）。

        Returns:
            ConcurrentStableList[APIRouter]: 两条聚合路由（`/api/v1` 与空前缀）。
        """
        return ConcurrentStableList([api_router, frame_router])

    def startup_hooks(self) -> ConcurrentStableList[StartupHook]:
        """启动钩子：装配完成后启动延迟推进器与重放执行器（停机随资源管理器回收）。

        登记在 `startup_hooks()`（而非 `configure_service()`）的原因：推进器需要事件循环与已装配的
        数据库引擎，`create()` 阶段两者都还没就绪；基座保证钩子在装配完成后、就绪标记前执行。

        Returns:
            ConcurrentStableList[StartupHook]: 启动钩子（执行序）。
        """
        return ConcurrentStableList([start_background_tasks])

    def configure_service(self, app: FastAPI, settings: Settings) -> None:
        """数据面进程级状态注入：消息存储（含唤醒包装） / 位点 / 死信 / 投递状态 / 组存储 /
        延迟区与推进器 / 审计出口 / 重放执行器 / 流登记表 / **连接登记表** / 工作单元工厂 /
        **分区唤醒器 / 背压指标 / 生产限流器** 各一份。

        替换实现只改这里（8B_02 换文件段与 `.delay` 段、位点文件与 `msg_group` 两表，端口与端点不变）；
        首期内存态随进程重启清空，已登记为遗留（详细设计 §12 风险 1）。

        **两个后台任务不在此启动**：`configure_service()` 早于事件循环装配完成，推进器 / 执行器由
        `startup_hooks()` 里的 `start_background_tasks` 启动（任务 03-1，替代原「首个请求惰性启动」）。

        Args:
            app: 应用实例（供 `api/deps.py` 读取）。
            settings: 应用配置（审计发件箱实现选择取自 `[outbox_store]`）。
        """
        waiter = DeliveryWaiter()
        app.state.waiter = waiter
        # 02-1：主题目录登记表（topic_id → tenant/namespace/name）+ 文件段存储。
        # 目录表由 TopicRouter.resolve 登记；存储层只在「未登记且盘上已有目录」时反查。
        app.state.partition_directory = PartitionDirectory(root=data_root())
        # 消息存储包一层「写入即唤醒」：普通发布 / 延迟转投 / 重放三处写入自动唤醒长轮询与长驻流。
        # **装配顺序硬约束**：通知包装必须是最外层（装反了不报错，只是长轮询退化为等到超时）。
        app.state.message_store = NotifyingMessageStore(
            FileMessageStore(directory=app.state.partition_directory), waiter
        )
        app.state.backpressure_metrics = BackpressureMetrics(waiter=waiter)
        app.state.rate_limiter = RateLimiter(default_rate_limit_tuning())
        app.state.offset_store = InMemoryOffsetStore()
        app.state.dead_letter_queue = InMemoryDeadLetterQueue()
        app.state.delivery_state = DeliveryState()
        app.state.group_store = InMemoryGroupStore()
        app.state.stream_registry = StreamRegistry()
        app.state.connection_registry = ConnectionRegistry()
        # 01-7：主题元数据快照缓存与保留清理节流窗口（进程级；跨实例一致性归 8B_05）。
        app.state.topic_cache = TopicCache()
        app.state.retention_throttle = RetentionThrottle()
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
        app.state.uow_factory = platform_uow_factory(app)
        app.state.replay_runner = ReplayRunner(
            messages=app.state.message_store,
            uow_factory=app.state.uow_factory,
            auditor=ReplayAuditor(sink=sink),
        )
        register_frame_handlers(app)

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
