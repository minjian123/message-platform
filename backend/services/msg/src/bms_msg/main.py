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
> `NotifyingMessageStore` 仍是最外层；
> 02-2 补崩溃恢复（恢复点 + CRC 尾部扫描与截断取证 + 索引残条修复 + 非末段对账 + 位点对齐），
> 并把存储登记进资源管理器（停机先停后台循环、再刷日志与恢复点）；
> 02-3 把位点换成**文件实现**（单文件双槽 + 内存为准 + 后台定时刷 + 脏阈值提前刷 + 关闭强制刷），
> 并按其偏序分两处登记：`OffsetFlusher` 早于 `StorageCloser`（先刷日志、后刷位点）、
> `OffsetFlushLoop` 在启动钩子里先于 `BackgroundLoops`（停机先断上游、再停刷盘）；
> 02-6 接入存储水位（`StorageWatermark` + `StorageMetrics`，惰性采样、无后台任务），并把
> `health_check_registry` 切到 msg 自持实现（`msg_storage` / `msg_recovery` / `database` 三项）；
> 02-7 把延迟区换成**文件实现**（`.delay` 独立序号段 + tombstone 回收 + 启动全量重建），
> 转投走 `durable=file_store`（append 后显式 fsync 再清），并按关闭偏序登记 `DelayCloser`
> （`OffsetFlusher` → `DelayCloser` → `StorageCloser`，逆序释放即刷日志 → 刷 `.delay` → 刷位点）。
> 04-1 接入 RPC 一元调用（方法注册表 + `/rpc/{service}/{method}` 双形态端点 + 客户端 stub），
> 05-1 接入租户用量表单登记（`form_records()` 注入 + `tenant.created` 消费 + usage/quota 双端点），
> 其余控制面路由随对应任务追加。
"""

import importlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from bms_core.application import BaseServiceApplicationFactory, StartupHook
from bms_core.audit.emit import AuditEventEmitter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.resources import ResourceManager
from bms_core.db.keys import PLATFORM_DB_KEY
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import SessionFactory, session_scope
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.consumer_records import EventConsumerRecord
from bms_core.extension import SCOPE_RULE_KEY, ExtensionRequirement
from bms_core.registry.forms import FormRecord
from bms_core.services.module_registry import ModuleRecord
from bms_core.services.table_registry import TableRecord
from fastapi import APIRouter, FastAPI

from bms_msg import CONTRACT_VERSION, PRODUCT_KEY, SERVICE_NAME, SERVICE_TITLE, __version__
from bms_msg.api.frame.gate import register_frame_handlers
from bms_msg.api.router import api_router, frame_router
from bms_msg.broker.backpressure import RateLimiter, default_rate_limit_tuning
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayMetrics
from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory, data_root
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.lifecycle import DelayCloser, OffsetFlusher, StorageCloser, start_background_tasks
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import FileOffsetStore
from bms_msg.broker.replay import AuditSink, ReplayAuditor, ReplayRunner, UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore
from bms_msg.broker.watermark import StorageMetrics, StorageWatermark
from bms_msg.health import register_msg_health_registry
from bms_msg.rpc.builtin import default_rpc_registry
from bms_msg.rpc.idempotency import InProcessIdempotencyStore
from bms_msg.tenant.lifecycle import event_consumer_records as tenant_lifecycle_records
from bms_msg.tenant.scope_rule import MSG_NAMESPACE_RULE
from services.form_registry import form_records as msg_form_records
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

    def form_records(self) -> ConcurrentStableList[FormRecord]:
        """本产品 `form_key` 登记清单（05-1；产品自持）。

        基座装配期把本清单与 `FORM_CATALOG` 经 `merge_form_records()` 拼为**应用级视图**
        `app.state.form_registry`；平台 `/api/v1/form-keys` 聚合产品表单需 bms 侧产品清单
        下发 / 聚合通道（mdm 已登记同项），本任务只保证 msg 应用级视图可见。

        Returns:
            ConcurrentStableList[FormRecord]: 本产品表单标识记录（插入序）。
        """
        return msg_form_records()

    def event_consumer_records(self) -> ConcurrentStableList[EventConsumerRecord]:
        """本服务自持的事件消费组声明（05-1：`tenant.created` → 租户目录 + 幂等账本）。

        Returns:
            ConcurrentStableList[EventConsumerRecord]: 消费组声明（插入序）。
        """
        return tenant_lifecycle_records()

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
        resources = cast("ResourceManager", app.state.resources)
        # 02-3：审计出口提前构造——位点重置 / 清理必须留痕（fail-closed），位点存储要用同一个实例。
        sink: AuditSink = AuditEventEmitter(
            registry=cast("EngineRegistry", app.state.engine_registry),
            session_factory=cast("SessionFactory", app.state.session_factory),
            settings=settings,
        )
        app.state.audit_sink = sink
        # 02-1：主题目录登记表（topic_id → tenant/namespace/name）+ 文件段存储。
        # 目录表由 TopicRouter.resolve 登记；存储层只在「未登记且盘上已有目录」时反查。
        root = data_root()
        app.state.partition_directory = PartitionDirectory(root=root)
        # 02-3：位点换成文件实现（单文件双槽 + 内存为准 + 后台刷盘），`OffsetStore` 端口签名不变。
        offset_store = FileOffsetStore(directory=app.state.partition_directory, root=root, audit=sink)
        app.state.offset_store = offset_store
        # 02-3：位点刷盘句柄**必须早于 `StorageCloser` 注册**——`ResourceManager` 逆序释放，
        # 早注册即晚释放，才得到「先刷日志、后刷位点」的正确偏序（反过来会让位点越过日志尾）。
        resources.register(OffsetFlusher(store=offset_store))
        # 消息存储包一层「写入即唤醒」：普通发布 / 延迟转投 / 重放三处写入自动唤醒长轮询与长驻流。
        # **装配顺序硬约束**：通知包装必须是最外层（装反了不报错，只是长轮询退化为等到超时）。
        file_store = FileMessageStore(
            directory=app.state.partition_directory,
            realigner=offset_store,
        )
        # 02-3：双向依赖只能后绑——store 构造时要 realigner，位点判越界时要 store 的 LEO。
        offset_store.bind_ceiling(file_store)
        app.state.file_store = file_store
        app.state.message_store = NotifyingMessageStore(file_store, waiter)
        # 02-7：延迟区换成**文件实现**（`.delay` 独立序号段），端口签名不变；
        # 去重源与耐久点都指向同一个 `file_store`（不另建实例，避免读到两份段状态）。
        app.state.delay_metrics = DelayMetrics()
        delay_store = FileDelayStore(
            directory=app.state.partition_directory,
            dedupe=file_store,
            ledger=file_store.recovery_ledger(),
            metrics=app.state.delay_metrics,
        )
        app.state.delay_store = delay_store
        # 02-7：延迟区刷盘句柄**必须晚于 `StorageCloser` 释放**（先刷日志、再刷 `.delay`），
        # 逆序释放要求它**早于** `StorageCloser` 注册。
        resources.register(DelayCloser(store=delay_store))
        # 02-2：日志关闭钩子。此处注册晚于位点刷盘句柄，逆序释放即「先刷日志、后刷位点」。
        resources.register(StorageCloser(store=file_store))
        app.state.backpressure_metrics = BackpressureMetrics(waiter=waiter)
        # 02-6：存储水位（卷级采样 + 滞回闸门）与指标采集点。惰性采样，**无后台任务**，
        # 因此不参与 02-2 / 02-3 的关闭偏序；发布侧经 `api/deps.py::StorageGateDep` 取用。
        app.state.storage_metrics = StorageMetrics()
        app.state.storage_watermark = StorageWatermark(root=root, metrics=app.state.storage_metrics)
        app.state.rate_limiter = RateLimiter(default_rate_limit_tuning())
        app.state.dead_letter_queue = InMemoryDeadLetterQueue()
        app.state.delivery_state = DeliveryState()
        app.state.group_store = InMemoryGroupStore()
        app.state.stream_registry = StreamRegistry()
        app.state.connection_registry = ConnectionRegistry()
        # 01-7：主题元数据快照缓存与保留清理节流窗口（进程级；跨实例一致性归 8B_05）。
        app.state.topic_cache = TopicCache()
        app.state.retention_throttle = RetentionThrottle()
        app.state.delay_scheduler = DelayScheduler(
            store=delay_store,
            messages=app.state.message_store,
            wheel=DelayWheel(),
            durable=file_store,
        )
        app.state.uow_factory = platform_uow_factory(app)
        # 04-1：RPC 方法注册表（装配期静态登记；重复 (service, method) 在 register 里抛错即拒启）。
        app.state.rpc_registry = default_rpc_registry()
        # 04-4：幂等去重存储（进程内；不跨实例 / 不跨重启，8B_05 换共享存储时只改这一行）。
        app.state.idempotency_store = InProcessIdempotencyStore()
        app.state.replay_runner = ReplayRunner(
            messages=app.state.message_store,
            uow_factory=app.state.uow_factory,
            auditor=ReplayAuditor(sink=sink),
        )
        register_frame_handlers(app)

    def extension_requirements(self) -> ConcurrentStableList[ExtensionRequirement]:
        """本产品声明的扩展点需求（07D 任务 03）：命名空间规则必需，缺失即拒启。

        Returns:
            ConcurrentStableList[ExtensionRequirement]: 有序需求清单。
        """
        return ConcurrentStableList(
            (ExtensionRequirement(plugin_key=SCOPE_RULE_KEY, plugin_name=MSG_NAMESPACE_RULE, required=True),)
        )

    def configure_plugins(self, app: FastAPI, settings: Settings) -> None:
        """服务侧插件登记：命名空间规则 `scope_rule:msg.namespace` 与健康检查注册表
        `health_check_registry:msg`。

        租户守卫回落基座默认（`[tenant_guard].provider = "source"`）、产品码改由
        `[[errcodes]].maps_from` 声明装载（`[error_mapper].provider` 置空），两者不再登记产品实现
        （07D 任务 03 附录 A / B）。

        Args:
            app: 应用实例（注册表工厂读 `app.state`）。
            settings: 应用配置（注册表工厂取 `[health]` 两档超时）。
        """
        # 命名空间规则靠 `BasePluggable` 导入期自动收集，这里只需保证模块已导入（不显式登记，避免撞名）。
        importlib.import_module("bms_msg.tenant.scope_rule")
        # 02-6：`/readyz` 的检查项来自 msg 自持注册表（基座缺省 provider 为空 → 空注册表，探针空转）。
        register_msg_health_registry(app, settings)


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
