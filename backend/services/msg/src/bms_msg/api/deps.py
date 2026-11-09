"""msg 依赖注入出口：租户两条唯一出口 + 数据面进程级状态。

- **管理面**：`get_current_tenant_context()`（只读基座中间件注入的上下文）。
- **数据面**：`get_data_plane_tenant()`——内部**只调用** `tenant/resolve.py::frame_tenant()`，
  令牌租户位取中间件已解析的租户主键（dev 无令牌时按 `[tenant].dev_override_tenant` 显式指定）；
  8B_01 的 h2 帧入口复用同一函数，数据面只有一套解析与错误口径（01-2 详细设计 §7.1）。
- 后续模块（主题 / 分区 / 配额 / 管理台 / 投递）一律经上述两条出口取租户，**不得再写解析代码**。
- 命名空间校验用 `CurrentTenant.ensure_namespace()`，与消费守卫共用同一判定（`350010`）。
"""

from collections.abc import Callable
from typing import Annotated, cast

from bms_core.core.config import get_settings
from bms_core.core.exceptions import ConfigError
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.extension.base import RESOURCE_ACL_KEY
from bms_core.resource_scope.acl import get_resource_acl
from fastapi import Depends, Request

from bms_msg.acl.cache import TopicAclCache
from bms_msg.acl.service import TopicAclAdminService, TopicAclService
from bms_msg.broker.backpressure import BackpressureTuning, RateLimiter, default_backpressure_tuning
from bms_msg.broker.connection import ConnectionRegistry, ConnectionTuning, connection_tuning_from_settings
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import GroupStore, InMemoryGroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import OffsetEnumerator, OffsetPurger, OffsetResetter, OffsetStore
from bms_msg.broker.replay import AuditSink, ReplayRunner, UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry, StreamTuning, default_stream_tuning
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.broker.watermark import StorageGate, StorageMetrics
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.quota.cache import QuotaCache
from bms_msg.quota.service import QuotaService
from bms_msg.repositories.quota import QuotaRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.repositories.topic_acl import MsgTopicAclRepository, MsgTopicAclScopeRepository
from bms_msg.rpc.idempotency import InProcessIdempotencyStore
from bms_msg.rpc.registry import RpcRegistry
from bms_msg.services.topic_cleanup import TopicCleanupService
from bms_msg.tenant.context import CurrentTenant
from bms_msg.tenant.resolve import frame_tenant, management_tenant
from bms_msg.usage.counter import DeliveryCounter, default_delivery_counter
from bms_msg.usage.snapshot import StorageUsageSampler, UsageSnapshotService

__all__ = [
    "AuditSinkDep",
    "BackpressureMetricsDep",
    "BackpressureTuningDep",
    "CachedQuotaServiceDep",
    "ConnectionRegistryDep",
    "ConnectionTuningDep",
    "CurrentTenantDep",
    "DataPlaneTenantDep",
    "DeadLetterQueueDep",
    "DelaySchedulerDep",
    "DelayStoreDep",
    "DeliveryCounterDep",
    "DeliveryStateDep",
    "GroupStoreDep",
    "IdempotencyStoreDep",
    "MessageStoreDep",
    "MetricsSnapshotsDep",
    "OffsetEnumeratorDep",
    "OffsetPurgerDep",
    "OffsetResetterDep",
    "OffsetStoreDep",
    "PartitionDirectoryDep",
    "QuotaCacheDep",
    "QuotaServiceDep",
    "RateLimiterDep",
    "ReplayRunnerDep",
    "RetentionThrottleDep",
    "RpcRegistryDep",
    "StorageGateDep",
    "StorageMetricsDep",
    "StreamRegistryDep",
    "StreamTuningDep",
    "TopicAclAdminServiceDep",
    "TopicAclServiceDep",
    "TopicCacheDep",
    "TopicCleanupDep",
    "UowFactoryDep",
    "UsageSnapshotDep",
    "WaiterDep",
    "get_audit_sink",
    "get_backpressure_metrics",
    "get_backpressure_tuning",
    "get_connection_registry",
    "get_connection_tuning",
    "get_current_tenant_context",
    "get_data_plane_tenant",
    "get_dead_letter_queue",
    "get_delay_scheduler",
    "get_delay_store",
    "get_delivery_state",
    "get_group_store",
    "get_idempotency_store",
    "get_message_store",
    "get_metrics_snapshots",
    "get_offset_enumerator",
    "get_offset_purger",
    "get_offset_resetter",
    "get_offset_store",
    "get_partition_directory",
    "get_rate_limiter",
    "get_replay_runner",
    "get_retention_throttle",
    "get_rpc_registry",
    "get_storage_metrics",
    "get_stream_registry",
    "get_stream_tuning",
    "get_topic_acl_admin_service",
    "get_topic_acl_cache",
    "get_topic_acl_service",
    "get_topic_cache",
    "get_uow_factory",
    "get_usage_counter",
    "get_usage_snapshot",
    "get_waiter",
]


async def get_current_tenant_context(request: Request) -> CurrentTenant:
    """取当前租户（管理面唯一出口）。

    Args:
        request: 当前请求（基座中间件已注入 `scope["state"]["tenant"]`）。

    Returns:
        CurrentTenant: 当前租户（含主键与可用命名空间）。

    Raises:
        MsgTenantNotFoundError: 未携带可用租户上下文（`350012`）。
    """
    return CurrentTenant.from_context(management_tenant(request))


CurrentTenantDep = Annotated[CurrentTenant, Depends(get_current_tenant_context)]
"""路由参数写法（管理面）：`tenant: CurrentTenantDep`。"""


async def get_data_plane_tenant(request: Request) -> CurrentTenant:
    """取当前租户（数据面唯一出口，走 `frame_tenant()`）。

    Args:
        request: 当前请求（基座中间件已解析并注入租户上下文；应用状态含租户源）。

    Returns:
        CurrentTenant: 当前租户。

    Raises:
        MsgTenantNotFoundError: 未携带可用租户上下文 / 租户不存在（`350012`）。
        MsgTenantSuspendedError: 租户已停用（`350011`）。
        ServiceUnavailableError: 租户源不可达（`10007` / 503，不静默放行）。
    """
    context = management_tenant(request)
    resolved = await frame_tenant(
        token_tenant_id=str(context.tenant_id),
        source=request.app.state.tenant_source,
        dev_override_tenant=get_settings().tenant.dev_override_tenant,
    )
    return CurrentTenant.from_context(resolved)


DataPlaneTenantDep = Annotated[CurrentTenant, Depends(get_data_plane_tenant)]
"""路由参数写法（数据面）：`tenant: DataPlaneTenantDep`。"""


def get_message_store(request: Request) -> MessageStore:
    """取消息存储（进程级单例，`configure_service()` 注入）。

    Args:
        request: 当前请求。

    Returns:
        MessageStore: 消息存储实现。
    """
    return cast("MessageStore", request.app.state.message_store)


def get_offset_store(request: Request) -> OffsetStore:
    """取位点存储（进程级单例）。

    Args:
        request: 当前请求。

    Returns:
        OffsetStore: 位点存储实现。
    """
    return cast("OffsetStore", request.app.state.offset_store)


def get_offset_resetter(request: Request) -> OffsetResetter:
    """取位点重置端口（02-3；与 `OffsetStore` 同一个实例，端口按能力分开声明）。

    Args:
        request: 当前请求。

    Returns:
        OffsetResetter: 位点重置实现。
    """
    return cast("OffsetResetter", request.app.state.offset_store)


def get_offset_enumerator(request: Request) -> OffsetEnumerator:
    """取位点枚举端口（02-3；位点查询端点归 06-2）。

    Args:
        request: 当前请求。

    Returns:
        OffsetEnumerator: 位点枚举实现。
    """
    return cast("OffsetEnumerator", request.app.state.offset_store)


def get_offset_purger(request: Request) -> OffsetPurger:
    """取孤儿位点清理端口（02-3；触发端点归 06-2）。

    Args:
        request: 当前请求。

    Returns:
        OffsetPurger: 位点清理实现。
    """
    return cast("OffsetPurger", request.app.state.offset_store)


def get_dead_letter_queue(request: Request) -> DeadLetterQueue:
    """取死信队列（进程级单例）。

    Args:
        request: 当前请求。

    Returns:
        DeadLetterQueue: 死信队列实现。
    """
    return cast("DeadLetterQueue", request.app.state.dead_letter_queue)


def get_delivery_state(request: Request) -> DeliveryState:
    """取投递状态（进程级单例；在途与暂存确认跨请求存续）。

    Args:
        request: 当前请求。

    Returns:
        DeliveryState: 投递状态。
    """
    return cast("DeliveryState", request.app.state.delivery_state)


def get_group_store(request: Request) -> GroupStore:
    """取组存储（进程级单例；组 / 成员 / 分配跨请求存续）。

    Args:
        request: 当前请求。

    Returns:
        GroupStore: 组存储实现。
    """
    return cast("GroupStore", request.app.state.group_store)


def get_delay_store(request: Request) -> DelayStore:
    """取延迟区存储（进程级单例；延迟记录跨请求存续）。

    Args:
        request: 当前请求。

    Returns:
        DelayStore: 延迟区存储实现。
    """
    return cast("DelayStore", request.app.state.delay_store)


def get_delay_scheduler(request: Request) -> DelayScheduler:
    """取到期推进器（进程级单例；发布侧装填与后台转投共用同一时间轮）。

    Args:
        request: 当前请求。

    Returns:
        DelayScheduler: 推进器。
    """
    return cast("DelayScheduler", request.app.state.delay_scheduler)


def get_replay_runner(request: Request) -> ReplayRunner:
    """取重放执行器（进程级单例；首个建任务请求触发惰性启动）。

    Args:
        request: 当前请求。

    Returns:
        ReplayRunner: 执行器。
    """
    return cast("ReplayRunner", request.app.state.replay_runner)


def get_audit_sink(request: Request) -> AuditSink:
    """取审计出口（装配期为基座 `AuditEventEmitter`，用例注入替身）。

    Args:
        request: 当前请求。

    Returns:
        AuditSink: 审计出口。
    """
    return cast("AuditSink", request.app.state.audit_sink)


MessageStoreDep = Annotated[MessageStore, Depends(get_message_store)]
"""路由参数写法：`store: MessageStoreDep`。"""

OffsetStoreDep = Annotated[OffsetStore, Depends(get_offset_store)]
"""路由参数写法：`offsets: OffsetStoreDep`。"""

OffsetResetterDep = Annotated[OffsetResetter, Depends(get_offset_resetter)]
"""路由参数写法：`resetter: OffsetResetterDep`（位点重置端点归 06-2）。"""

OffsetEnumeratorDep = Annotated[OffsetEnumerator, Depends(get_offset_enumerator)]
"""路由参数写法：`offsets: OffsetEnumeratorDep`（位点查询端点归 06-2）。"""

OffsetPurgerDep = Annotated[OffsetPurger, Depends(get_offset_purger)]
"""路由参数写法：`purger: OffsetPurgerDep`（孤儿位点清理触发归 06-2）。"""

DeadLetterQueueDep = Annotated[DeadLetterQueue, Depends(get_dead_letter_queue)]
"""路由参数写法：`dead_letters: DeadLetterQueueDep`。"""

DeliveryStateDep = Annotated[DeliveryState, Depends(get_delivery_state)]
"""路由参数写法：`state: DeliveryStateDep`。"""

GroupStoreDep = Annotated[GroupStore, Depends(get_group_store)]
"""路由参数写法：`groups: GroupStoreDep`。"""

DelayStoreDep = Annotated[DelayStore, Depends(get_delay_store)]
"""路由参数写法：`store: DelayStoreDep`（延迟区）。"""

DelaySchedulerDep = Annotated[DelayScheduler, Depends(get_delay_scheduler)]
"""路由参数写法：`scheduler: DelaySchedulerDep`（到期推进器）。"""

ReplayRunnerDep = Annotated[ReplayRunner, Depends(get_replay_runner)]
"""路由参数写法：`runner: ReplayRunnerDep`（重放执行器）。"""

AuditSinkDep = Annotated[AuditSink, Depends(get_audit_sink)]
"""路由参数写法：`sink: AuditSinkDep`（审计出口）。"""


def get_stream_registry(request: Request) -> StreamRegistry:
    """取长驻流登记表（进程级单例；长驻流的建立 / 注销与活跃数）。

    Args:
        request: 当前请求。

    Returns:
        StreamRegistry: 流登记表。
    """
    return cast("StreamRegistry", request.app.state.stream_registry)


def get_uow_factory(request: Request) -> UowFactory:
    """取「无请求上下文也能开平台库工作单元」的工厂（长驻流每轮拉取开短会话用）。

    长驻流可能活 60s：整条流复用一个请求会话会把连接池拖死，故每轮现开现关
    （与 01-6 重放执行器同一口径，装配期由 `configure_service()` 注入）。

    Args:
        request: 当前请求。

    Returns:
        UowFactory: 工作单元工厂。
    """
    return cast("UowFactory", request.app.state.uow_factory)


def get_stream_tuning() -> StreamTuning:
    """取长驻流参数（缺省取模块常量；用例经依赖覆盖注入毫秒级参数）。

    Returns:
        StreamTuning: 流参数（兜底轮询间隔 / 时长上限 / 心跳间隔 / 等待上限 / 预取）。
    """
    return default_stream_tuning()


def get_backpressure_tuning() -> BackpressureTuning:
    """取 inflight 双阈值（缺省取模块常量；用例经依赖覆盖注入小阈值）。

    Returns:
        BackpressureTuning: 条数与字节阈值。
    """
    return default_backpressure_tuning()


def get_waiter(request: Request) -> DeliveryWaiter:
    """取分区唤醒器（进程级单例；长轮询与长驻流共用同一份版本表）。

    Args:
        request: 当前请求。

    Returns:
        DeliveryWaiter: 唤醒器。
    """
    return cast("DeliveryWaiter", request.app.state.waiter)


def get_backpressure_metrics(request: Request) -> BackpressureMetrics:
    """取背压指标收集器（进程级单例；暴露归 06-3）。

    Args:
        request: 当前请求。

    Returns:
        BackpressureMetrics: 收集器。
    """
    return cast("BackpressureMetrics", request.app.state.backpressure_metrics)


def get_rate_limiter(request: Request) -> RateLimiter:
    """取生产限流器（进程级单例；令牌桶状态跨请求存续）。

    Args:
        request: 当前请求。

    Returns:
        RateLimiter: 两级令牌桶。
    """
    return cast("RateLimiter", request.app.state.rate_limiter)


def get_connection_registry(request: Request) -> ConnectionRegistry:
    """取连接登记表（进程级单例；连接条目 / 名额 / 半开清扫）。

    Args:
        request: 当前请求。

    Returns:
        ConnectionRegistry: 连接登记表。
    """
    return cast("ConnectionRegistry", request.app.state.connection_registry)


def get_connection_tuning() -> ConnectionTuning:
    """取连接参数（按基座 `[server]` 计算；用例经依赖覆盖注入毫秒级参数）。

    Returns:
        ConnectionTuning: 连接参数（心跳间隔 / 超时 / 空闲阈值 / 两个上限 / 清扫批量 / 重试间隔）。
    """
    return connection_tuning_from_settings(get_settings())


StreamRegistryDep = Annotated[StreamRegistry, Depends(get_stream_registry)]
"""路由参数写法：`registry: StreamRegistryDep`。"""

UowFactoryDep = Annotated[UowFactory, Depends(get_uow_factory)]
"""路由参数写法：`uow_factory: UowFactoryDep`。"""

StreamTuningDep = Annotated[StreamTuning, Depends(get_stream_tuning)]
"""路由参数写法：`tuning: StreamTuningDep`。"""

ConnectionRegistryDep = Annotated[ConnectionRegistry, Depends(get_connection_registry)]
"""路由参数写法：`connections: ConnectionRegistryDep`。"""

ConnectionTuningDep = Annotated[ConnectionTuning, Depends(get_connection_tuning)]
"""路由参数写法：`tuning: ConnectionTuningDep`（连接参数，区别于 `StreamTuning`）。"""

BackpressureTuningDep = Annotated[BackpressureTuning, Depends(get_backpressure_tuning)]
"""路由参数写法：`budget: BackpressureTuningDep`（inflight 双阈值）。"""

WaiterDep = Annotated[DeliveryWaiter, Depends(get_waiter)]
"""路由参数写法：`waiter: WaiterDep`（长轮询 / 长驻流的唤醒器）。"""

BackpressureMetricsDep = Annotated[BackpressureMetrics, Depends(get_backpressure_metrics)]
"""路由参数写法：`metrics: BackpressureMetricsDep`（背压指标收集器）。"""

RateLimiterDep = Annotated[RateLimiter, Depends(get_rate_limiter)]
"""路由参数写法：`limiter: RateLimiterDep`（生产限流）。"""


def get_topic_cache(request: Request) -> TopicCache:
    """取主题元数据缓存（进程级单例；01-7）。

    Args:
        request: 当前请求。

    Returns:
        TopicCache: 主题快照缓存。
    """
    return cast("TopicCache", request.app.state.topic_cache)


def get_retention_throttle(request: Request) -> RetentionThrottle:
    """取保留清理节流窗口表（进程级单例；01-7）。

    Args:
        request: 当前请求。

    Returns:
        RetentionThrottle: 窗口表。
    """
    return cast("RetentionThrottle", request.app.state.retention_throttle)


TopicCacheDep = Annotated[TopicCache, Depends(get_topic_cache)]
"""路由参数写法：`cache: TopicCacheDep`（主题元数据缓存）。"""

RetentionThrottleDep = Annotated[RetentionThrottle, Depends(get_retention_throttle)]
"""路由参数写法：`throttle: RetentionThrottleDep`（保留清理节流）。"""


def get_topic_cleanup_service(request: Request) -> TopicCleanupService:
    """取删主题清理编排（进程级单例；装配期由 `configure_service()` 注入）。

    **不做惰性兜底**：它要七个跨域端口，缺一个就不是「降级」而是错装配；这里直接报 `ConfigError`，
    让装配缺陷在第一次删除请求上暴露，而不是静默地什么都不清。

    Args:
        request: 当前请求。

    Returns:
        TopicCleanupService: 清理编排。

    Raises:
        ConfigError: 装配期未注入（配置错误）。
    """
    service = getattr(request.app.state, "topic_cleanup_service", None)
    if service is None:
        raise ConfigError("删主题清理服务未装配（topic_cleanup_service）")
    return cast("TopicCleanupService", service)


def get_partition_directory(request: Request) -> PartitionDirectory:
    """取分区目录登记表（进程级单例；02-1）。

    Args:
        request: 当前请求。

    Returns:
        PartitionDirectory: 主题目录身份登记表。
    """
    return cast("PartitionDirectory", request.app.state.partition_directory)


PartitionDirectoryDep = Annotated[PartitionDirectory, Depends(get_partition_directory)]
"""路由参数写法：`directory: PartitionDirectoryDep`（分区目录登记表）。"""

TopicCleanupDep = Annotated[TopicCleanupService, Depends(get_topic_cleanup_service)]
"""路由参数写法：`cleanup: TopicCleanupDep`（删主题清理编排）。"""


def get_storage_gate(request: Request) -> StorageGate | None:
    """取存储水位闸门（进程级单例；02-6）。

    **缺省 None 是设计**：最小测试应用与「内存实现直连」场景没有水位组件，此时发布不受水位约束
    （与既有用例行为一致）；生产装配总会注入 `StorageWatermark`。

    Args:
        request: 当前请求。

    Returns:
        StorageGate | None: 水位闸门；未装配返回 None。
    """
    return cast("StorageGate | None", getattr(request.app.state, "storage_watermark", None))


StorageGateDep = Annotated[StorageGate | None, Depends(get_storage_gate)]
"""路由参数写法：`gate: StorageGateDep`（存储水位闸门；None 表示不判）。"""


def get_rpc_registry(request: Request) -> RpcRegistry:
    """取 RPC 方法注册表（进程级单例；装配期由 `default_rpc_registry()` 注入）。

    **实例而非模块级全局**：与其它进程级状态（位点 / 组 / 限流器）同口径——用例各造一份、多 app
    实例互不串扰；04-5 复用时由 bms 自己的装配决定注册表内容。

    Args:
        request: 当前请求。

    Returns:
        RpcRegistry: 方法注册表。
    """
    return cast("RpcRegistry", request.app.state.rpc_registry)


RpcRegistryDep = Annotated[RpcRegistry, Depends(get_rpc_registry)]
"""路由参数写法：`registry: RpcRegistryDep`（RPC 方法注册表）。"""


def get_idempotency_store(request: Request) -> InProcessIdempotencyStore:
    """取进程级幂等去重存储（04-4；生产由 `main.py` 装配期注入 `app.state`）。

    **惰性装配**：用例自建的最小 app 不带 `idempotency_store`，这里补一份挂回 `app.state`——
    每个 app 各持一份、多 app 实例互不串扰（与仓内其它进程级状态同口径），且不要求测试辅助
    跟着改装配。生产路径已在 `create_app()` 里显式赋值，不会走这条分支。

    Args:
        request: 当前请求。

    Returns:
        InProcessIdempotencyStore: 去重存储单例。
    """
    state = request.app.state
    store = getattr(state, "idempotency_store", None)
    if store is None:
        store = InProcessIdempotencyStore()
        state.idempotency_store = store
    return cast("InProcessIdempotencyStore", store)


IdempotencyStoreDep = Annotated[InProcessIdempotencyStore, Depends(get_idempotency_store)]
"""路由参数写法：`store: IdempotencyStoreDep`（进程内幂等去重）。"""


def get_quota_cache(request: Request) -> QuotaCache:
    """取配额快照缓存（进程级单例；装配期注入，测试 app 缺失时惰性补一份）。

    Args:
        request: 当前请求。

    Returns:
        QuotaCache: 配额缓存。
    """
    state = request.app.state
    cache = getattr(state, "quota_cache", None)
    if cache is None:
        cache = QuotaCache()
        state.quota_cache = cache
    return cast("QuotaCache", cache)


def get_usage_counter(request: Request) -> DeliveryCounter:
    """取今日投递计数器（进程级单例；测试 app 缺失时惰性补一份）。

    Args:
        request: 当前请求。

    Returns:
        DeliveryCounter: 计数器。
    """
    state = request.app.state
    counter = getattr(state, "delivery_counter", None)
    if counter is None:
        counter = default_delivery_counter()
        state.delivery_counter = counter
    return cast("DeliveryCounter", counter)


def get_storage_sampler(request: Request) -> StorageUsageSampler:
    """取存储用量采样器（进程级单例；测试 app 缺失时按缺省数据根补一份）。

    Args:
        request: 当前请求。

    Returns:
        StorageUsageSampler: 采样器。
    """
    state = request.app.state
    sampler = getattr(state, "storage_usage_sampler", None)
    if sampler is None:
        # 局部导入：`data_root()` 依赖文件存储模块，放在模块顶层会拉大依赖面。
        from bms_msg.broker.file_store import data_root

        sampler = StorageUsageSampler(root=data_root())
        state.storage_usage_sampler = sampler
    return cast("StorageUsageSampler", sampler)


def get_usage_snapshot(
    request: Request,
    uow: Annotated[UnitOfWork, Depends(get_platform_uow)],
) -> UsageSnapshotService:
    """取用量快照服务（请求级会话 + 进程级运行态）。

    Args:
        request: 当前请求。
        uow: 平台库工作单元（元数据计数）。

    Returns:
        UsageSnapshotService: 用量快照服务。
    """
    return UsageSnapshotService(
        session=cast("DbSession", uow.session),
        groups=_state_or(request, "group_store", InMemoryGroupStore),
        delivery_state=_state_or(request, "delivery_state", DeliveryState),
        connections=_state_or(request, "connection_registry", ConnectionRegistry),
        counter=get_usage_counter(request),
        sampler=get_storage_sampler(request),
    )


def _state_or[StateT](request: Request, name: str, factory: Callable[[], StateT]) -> StateT:
    """取进程级状态；最小测试 app 缺失时惰性补一份并挂回 `app.state`。

    Args:
        request: 当前请求。
        name: 状态属性名。
        factory: 缺失时的构造器。

    Returns:
        StateT: 状态实例。
    """
    state = request.app.state
    value = getattr(state, name, None)
    if value is None:
        value = factory()
        setattr(state, name, value)
    return cast("StateT", value)


def get_quota_service(
    request: Request,
    uow: Annotated[UnitOfWork, Depends(get_platform_uow)],
    usage: Annotated[UsageSnapshotService, Depends(get_usage_snapshot)],
) -> QuotaService:
    """取配额服务（请求级仓储 + 进程级缓存 / 用量）。

    Args:
        request: 当前请求。
        uow: 平台库工作单元。
        usage: 用量快照服务。

    Returns:
        QuotaService: 配额服务。
    """
    return QuotaService(
        cache=get_quota_cache(request),
        repo=QuotaRepository(cast("DbSession", uow.session)),
        uow=uow,
        usage=usage,
    )


def get_quota_service_cached(request: Request) -> QuotaService:
    """取只读缓存的配额服务（连接守卫用：不查库、按缺省值兜底）。

    Args:
        request: 当前请求。

    Returns:
        QuotaService: 无仓储 / 无用量的配额服务。
    """
    return QuotaService(cache=get_quota_cache(request))


QuotaCacheDep = Annotated[QuotaCache, Depends(get_quota_cache)]
"""路由参数写法：`cache: QuotaCacheDep`（配额快照缓存）。"""

DeliveryCounterDep = Annotated[DeliveryCounter, Depends(get_usage_counter)]
"""路由参数写法：`counter: DeliveryCounterDep`（今日投递计数）。"""

UsageSnapshotDep = Annotated[UsageSnapshotService, Depends(get_usage_snapshot)]
"""路由参数写法：`usage: UsageSnapshotDep`（用量快照）。"""

QuotaServiceDep = Annotated[QuotaService, Depends(get_quota_service)]
"""路由参数写法：`quota: QuotaServiceDep`（请求级配额服务）。"""

CachedQuotaServiceDep = Annotated[QuotaService, Depends(get_quota_service_cached)]
"""连接守卫参数写法：`quota: CachedQuotaServiceDep`（只读缓存）。"""


def get_topic_acl_cache(request: Request) -> TopicAclCache:
    """取 ACL 规则快照缓存（进程级单例；装配期注入，测试 app 缺失时惰性补一份）。

    Args:
        request: 当前请求。

    Returns:
        TopicAclCache: 快照缓存。
    """
    state = request.app.state
    cache = getattr(state, "topic_acl_cache", None)
    if cache is None:
        cache = TopicAclCache()
        state.topic_acl_cache = cache
    return cast("TopicAclCache", cache)


def _resource_acl_declared(request: Request) -> bool:
    """产品是否声明了 `resource_acl` 扩展点（未声明 = 无 ACL 模型，守卫跳过）。

    Args:
        request: 当前请求。

    Returns:
        bool: 已声明 True。
    """
    requirements = getattr(request.app.state, "extension_requirements", None) or ()
    return any(getattr(item, "plugin_key", None) == RESOURCE_ACL_KEY for item in requirements)


def get_topic_acl_service(
    request: Request,
    uow: Annotated[UnitOfWork, Depends(get_platform_uow)],
) -> TopicAclService:
    """取主题 ACL 守卫（请求级仓储 + 进程级快照缓存 + 端口读取器）。

    Args:
        request: 当前请求。
        uow: 平台库工作单元。

    Returns:
        TopicAclService: ACL 守卫。
    """
    session = cast("DbSession", uow.session)
    acl_provider = (lambda: get_resource_acl(request)) if _resource_acl_declared(request) else (lambda: None)
    return TopicAclService(
        rules=MsgTopicAclRepository(session),
        scopes=MsgTopicAclScopeRepository(session),
        cache=get_topic_acl_cache(request),
        acl_provider=acl_provider,
        release=uow.rollback,
    )


def get_topic_acl_admin_service(
    request: Request,
    uow: Annotated[UnitOfWork, Depends(get_platform_uow)],
    topic_acl: Annotated[TopicAclService, Depends(get_topic_acl_service)],
    sink: Annotated[AuditSink, Depends(get_audit_sink)],
) -> TopicAclAdminService:
    """取 ACL 管理服务（请求级仓储 + 守卫 + 审计出口）。

    Args:
        request: 当前请求。
        uow: 平台库工作单元。
        topic_acl: ACL 守卫（写前判目标主题 manage）。
        sink: 审计出口。

    Returns:
        TopicAclAdminService: ACL 管理服务。
    """
    session = cast("DbSession", uow.session)
    return TopicAclAdminService(
        rules=MsgTopicAclRepository(session),
        scopes=MsgTopicAclScopeRepository(session),
        topics=TopicRepository(session),
        uow=uow,
        cache=get_topic_acl_cache(request),
        authorizer=topic_acl,
        sink=sink,
    )


TopicAclServiceDep = Annotated[TopicAclService, Depends(get_topic_acl_service)]
"""路由参数写法：`acl: TopicAclServiceDep`（ACL 守卫）。"""

TopicAclAdminServiceDep = Annotated[TopicAclAdminService, Depends(get_topic_acl_admin_service)]
"""路由参数写法：`acls: TopicAclAdminServiceDep`（ACL 管理服务）。"""


def get_metrics_snapshots(request: Request) -> MetricsSnapshotStore:
    """取 06-3 采样快照（概览只读出口）。

    自建最小 app 未装配该状态时惰性建一个空快照：进程刚启动本就应表现为
    「快照不可用、四值返回 0」，不能让端点在测试 / 降级装配下抛 AttributeError。

    Args:
        request: 当前请求。

    Returns:
        MetricsSnapshotStore: 进程级快照存储。
    """
    existing = getattr(request.app.state, "metrics_snapshots", None)
    if isinstance(existing, MetricsSnapshotStore):
        return existing
    created = MetricsSnapshotStore()
    request.app.state.metrics_snapshots = created
    return created


def get_storage_metrics(request: Request) -> StorageMetrics:
    """取存储水位采集器（概览磁盘使用率出口；未装配时惰性建空采集器）。

    Args:
        request: 当前请求。

    Returns:
        StorageMetrics: 进程级水位采集器。
    """
    existing = getattr(request.app.state, "storage_metrics", None)
    if isinstance(existing, StorageMetrics):
        return existing
    created = StorageMetrics()
    request.app.state.storage_metrics = created
    return created


MetricsSnapshotsDep = Annotated[MetricsSnapshotStore, Depends(get_metrics_snapshots)]
"""路由参数写法：`snapshots: MetricsSnapshotsDep`（06-3 采样快照）。"""

StorageMetricsDep = Annotated[StorageMetrics, Depends(get_storage_metrics)]
"""路由参数写法：`storage: StorageMetricsDep`（磁盘水位快照）。"""
