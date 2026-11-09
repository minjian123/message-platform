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
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import cast

from bms_core.application import BaseServiceApplicationFactory, StartupHook
from bms_core.audit.classes import AuditClass
from bms_core.audit.emit import AuditEventEmitter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.config import Settings
from bms_core.core.logging import get_logger
from bms_core.core.plugin import register_plugin
from bms_core.core.resources import ResourceManager
from bms_core.db.keys import PLATFORM_DB_KEY
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import DbSession, SessionFactory, session_scope
from bms_core.db.tenant import TenantLookup
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.consumer_records import EventConsumerRecord
from bms_core.extension import RESOURCE_ACL_KEY, SCOPE_RULE_KEY, ExtensionRequirement
from bms_core.permission.rbac import RBAC_PROVIDER_NAME
from bms_core.registry.forms import FormRecord
from bms_core.services.module_registry import ModuleRecord
from bms_core.services.table_registry import TableRecord
from bms_core.tenant.guard import SourceTenantGuard, tenant_source_reader
from fastapi import APIRouter, FastAPI

from bms_msg import CONTRACT_VERSION, PRODUCT_KEY, SERVICE_NAME, SERVICE_TITLE, __version__
from bms_msg.acl.cache import TopicAclCache
from bms_msg.acl.keys import MSG_TOPIC_ACL_PLUGIN
from bms_msg.acl.port import MsgTopicAclFactory
from bms_msg.api.access_audit import AccessAnomalyAuditMiddleware
from bms_msg.api.frame.gate import register_frame_handlers
from bms_msg.api.router import api_router, frame_router
from bms_msg.audit.trail import ACTION_CLASSES
from bms_msg.broker.backpressure import RateLimiter, default_rate_limit_tuning
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayMetrics
from bms_msg.broker.delay_disposal import DelayDisposalService
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
from bms_msg.core.config import DATA_ROOT_SUBDIR, OFFSETS_SUBDIR
from bms_msg.health import register_msg_health_registry
from bms_msg.metrics import MetricsSnapshotStore, get_traffic_metrics
from bms_msg.metrics.collector import MsgMetricsCollector
from bms_msg.metrics.sampler import MetricsSampler
from bms_msg.permission import register_msg_permission
from bms_msg.quota.cache import QuotaCache
from bms_msg.rpc.builtin import default_rpc_registry
from bms_msg.rpc.idempotency import InProcessIdempotencyStore
from bms_msg.services.topic_cleanup import TopicCleanupService
from bms_msg.tenant.ledger import DbTenantLifecycleLedger
from bms_msg.tenant.lifecycle import event_consumer_records as tenant_lifecycle_records
from bms_msg.tenant.purge import TenantPurgeService, get_tenant_purge_metrics
from bms_msg.tenant.runtime import (
    GuardTenantAvailability,
    TenantLifecycleRuntime,
    get_tenant_lifecycle_metrics,
    set_tenant_lifecycle_runtime,
)
from bms_msg.tenant.scope_rule import MSG_NAMESPACE_RULE
from bms_msg.usage.counter import default_delivery_counter
from bms_msg.usage.snapshot import StorageUsageSampler
from services.form_registry import form_records as msg_form_records
from services.module_registry import MSG_SERVICE_RECORDS
from services.table_registry import MSG_TABLE_RECORDS

_LOGGER = get_logger("bms_msg.main")


class ApplicationFactory(BaseServiceApplicationFactory):
    """msg 消息平台服务应用工厂（通用装配由共享基座承载）。"""

    key: str = "application_factory"
    service_name: str = SERVICE_NAME
    service_title: str = SERVICE_TITLE
    version: str = __version__
    contract_version: str = CONTRACT_VERSION
    product_key: str | None = PRODUCT_KEY

    def prepare_settings(self, settings: Settings) -> None:
        """启用真实 RBAC 与主题 ACL（05-3）。

        - `[permission].provider` 未显式配置时置为 `rbac`；显式配置（含 `null`）尊重；
        - `[resource_acl].provider` 未显式配置时置为 `msg.topic`；
        - 登记权限数据来源与真实校验器（插件注册表构建前）。

        Args:
            settings: 应用配置（可变）。
        """
        if not settings.permission.provider:
            settings.permission.provider = RBAC_PROVIDER_NAME
        if not settings.resource_acl.provider or settings.resource_acl.provider == "null":
            settings.resource_acl.provider = MSG_TOPIC_ACL_PLUGIN
        register_msg_permission(settings)

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
        """本服务自持的事件消费组声明（05-1 / 05-5：五类租户生命周期事件各一条）。

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
        # 05-2：配额快照缓存 / 存储用量采样器 / 今日投递计数（进程级；跨实例一致性归 8B_05）。
        app.state.quota_cache = QuotaCache()
        # 05-3：主题 ACL 规则快照缓存（进程级；版本戳由 DB 提供，跨 worker 一致）。
        app.state.topic_acl_cache = TopicAclCache()
        app.state.storage_usage_sampler = StorageUsageSampler(root=root)
        app.state.delivery_counter = default_delivery_counter()
        # 05-5：延迟转投前的租户可用性检查——复用基座租户守卫（租户源 + 全局版本戳），不建本地状态。
        tenant_guard = SourceTenantGuard(source_provider=tenant_source_reader(app))
        app.state.tenant_availability = GuardTenantAvailability(guard=tenant_guard)
        app.state.delay_scheduler = DelayScheduler(
            store=delay_store,
            messages=app.state.message_store,
            wheel=DelayWheel(),
            durable=file_store,
            availability=app.state.tenant_availability,
        )
        # 05-4：延迟转投异常处置（服务原语；06-2 的延迟区取消端点复用同一实例）。
        app.state.delay_disposal = DelayDisposalService(
            store=delay_store,
            wheel=app.state.delay_scheduler.wheel,
            sink=sink,
        )
        app.state.uow_factory = platform_uow_factory(app)
        lifecycle_ledger = DbTenantLifecycleLedger(session_scope=lambda: lifecycle_session_scope(app))
        lifecycle_runtime = TenantLifecycleRuntime(
            ledger=lifecycle_ledger,
            audit_sink=sink,
            connections=app.state.connection_registry,
            streams=app.state.stream_registry,
            delay_store=delay_store,
            delay_wheel=app.state.delay_scheduler.wheel,
            message_store=file_store,
            offset_store=offset_store,
            groups=app.state.group_store,
            dead_letters=app.state.dead_letter_queue,
            delivery_state=app.state.delivery_state,
            waiter=waiter,
            topic_cache=app.state.topic_cache,
            retention_throttle=app.state.retention_throttle,
            directory=app.state.partition_directory,
        )
        set_tenant_lifecycle_runtime(lifecycle_runtime)
        app.state.tenant_lifecycle_runtime = lifecycle_runtime
        tenant_purge_service = TenantPurgeService(
            ledger=lifecycle_ledger,
            runtime=lifecycle_runtime,
            data_root=root / DATA_ROOT_SUBDIR,
            offsets_root=root / OFFSETS_SUBDIR,
            quota_session_scope=lambda: lifecycle_session_scope(app),
            quota_cache=app.state.quota_cache,
            storage_sampler=app.state.storage_usage_sampler,
            acl_session_scope=lambda: lifecycle_session_scope(app),
            acl_cache=app.state.topic_acl_cache,
        )
        app.state.tenant_purge_service = tenant_purge_service
        # `tenant.purged` → 一次性物理清理：清扫服务依赖运行时，故建好后回挂到运行时上（不反向注入）。
        lifecycle_runtime.attach_purge(tenant_purge_service)
        # 04-1：RPC 方法注册表（装配期静态登记；重复 (service, method) 在 register 里抛错即拒启）。
        app.state.rpc_registry = default_rpc_registry()
        # 04-4：幂等去重存储（进程内；不跨实例 / 不跨重启，8B_05 换共享存储时只改这一行）。
        app.state.idempotency_store = InProcessIdempotencyStore()
        app.state.replay_runner = ReplayRunner(
            messages=app.state.message_store,
            uow_factory=app.state.uow_factory,
            auditor=ReplayAuditor(sink=sink),
        )
        # 06-2：删主题清理编排（受理时 kick + 周期扫描兜底；停机随 `BackgroundLoops` 回收）。
        app.state.topic_cleanup_service = TopicCleanupService(
            messages=app.state.message_store,
            offsets=offset_store,
            groups=app.state.group_store,
            delay=delay_store,
            wheel=app.state.delay_scheduler.wheel,
            dead_letters=app.state.dead_letter_queue,
            delivery=app.state.delivery_state,
            waiter=waiter,
            throttle=app.state.retention_throttle,
            directory=app.state.partition_directory,
            cache=app.state.topic_cache,
            watermark=app.state.storage_watermark,
            uow_factory=app.state.uow_factory,
        )
        register_frame_handlers(app)
        # 06-3-1：指标出口——自定义 Collector 直读既有采集器快照与事件点采集器。
        # 注册进基座注册表（根路径 /metrics 由基座提供）；基座不支持时降级并留 warning。
        self._install_metrics(app, settings)
        # 05-4：访问异常拒绝审计。最后注册即最外层，能看到租户中间件提前拒绝的响应；
        # 只补基座未记录的 401 / 403 / 429，不改变原响应。
        app.add_middleware(AccessAnomalyAuditMiddleware, settings=settings.audit_log)

    @staticmethod
    def _install_metrics(app: FastAPI, settings: Settings) -> None:
        """装配指标采集器与后台采样器（06-3-1）。

        Args:
            app: 应用实例（取已装配的采集器与资源管理器）。
            settings: 应用配置（取服务身份）。
        """
        traffic = get_traffic_metrics()
        snapshots = MetricsSnapshotStore()
        collector = MsgMetricsCollector(
            service=settings.app.service or SERVICE_NAME,
            traffic=traffic,
            snapshots=snapshots,
            storage=app.state.storage_metrics,
            delay=app.state.delay_metrics,
            backpressure=app.state.backpressure_metrics,
            lifecycle=get_tenant_lifecycle_metrics(),
            purge=get_tenant_purge_metrics(),
        )
        registered = app.state.metrics.register_collector(collector)
        if not registered:
            _LOGGER.warning("msg_metrics_collector_unsupported", provider=type(app.state.metrics).__name__)
        app.state.traffic_metrics = traffic
        app.state.metrics_snapshots = snapshots
        app.state.metrics_collector = collector
        app.state.metrics_sampler = MetricsSampler(
            snapshots=snapshots,
            groups=app.state.group_store,
            messages=app.state.message_store,
            offsets=app.state.offset_store,
            dead_letters=app.state.dead_letter_queue,
            uow_factory=app.state.uow_factory,
            watermark=app.state.storage_watermark,
            tenant_lookup=cast("TenantLookup", app.state.tenant_source),
        )

    def audit_classes(self) -> ConcurrentStableDict[str, AuditClass]:
        """本服务的留痕类别（05-4）：动作 → 类别 / 强审计 / 记录级。

        Returns:
            ConcurrentStableDict[str, AuditClass]: 与运行期 helper 同源的唯一映射。
        """
        return ACTION_CLASSES

    def extension_requirements(self) -> ConcurrentStableList[ExtensionRequirement]:
        """本产品声明的扩展点需求（07D 任务 03 / 05-3）：命名空间规则与资源级 ACL 必需。

        Returns:
            ConcurrentStableList[ExtensionRequirement]: 有序需求清单。
        """
        return ConcurrentStableList(
            (
                ExtensionRequirement(plugin_key=SCOPE_RULE_KEY, plugin_name=MSG_NAMESPACE_RULE, required=True),
                ExtensionRequirement(plugin_key=RESOURCE_ACL_KEY, plugin_name=MSG_TOPIC_ACL_PLUGIN, required=True),
            )
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
        # 05-3：资源级 ACL 端口实现（工厂需应用引用，显式登记；导入期自动收集的类无缓存依赖）。
        register_plugin(RESOURCE_ACL_KEY, MSG_TOPIC_ACL_PLUGIN, MsgTopicAclFactory(app))
        # 02-6：`/readyz` 的检查项来自 msg 自持注册表（基座缺省 provider 为空 → 空注册表，探针空转）。
        register_msg_health_registry(app, settings)


def lifecycle_session_scope(app: FastAPI) -> AbstractAsyncContextManager[DbSession]:
    """生命周期账本 / 清扫的平台库短会话入口（后台消费无请求上下文）。

    Args:
        app: 应用实例（取引擎注册表）。

    Returns:
        AbstractAsyncContextManager[DbSession]: 平台库会话上下文。
    """
    return session_scope(
        cast("EngineRegistry", app.state.engine_registry),
        db_key=PLATFORM_DB_KEY,
        factory=SessionFactory(),
    )


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
