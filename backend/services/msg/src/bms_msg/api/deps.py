"""msg 依赖注入出口：租户两条唯一出口 + 数据面进程级状态。

- **管理面**：`get_current_tenant_context()`（只读基座中间件注入的上下文）。
- **数据面**：`get_data_plane_tenant()`——内部**只调用** `tenant/resolve.py::frame_tenant()`，
  令牌租户位取中间件已解析的租户主键（dev 无令牌时按 `[tenant].dev_override_tenant` 显式指定）；
  8B_01 的 h2 帧入口复用同一函数，数据面只有一套解析与错误口径（01-2 详细设计 §7.1）。
- 后续模块（主题 / 分区 / 配额 / 管理台 / 投递）一律经上述两条出口取租户，**不得再写解析代码**。
- 命名空间校验用 `CurrentTenant.ensure_namespace()`，与消费守卫共用同一判定（`350010`）。
"""

from typing import Annotated, cast

from bms_core.core.config import get_settings
from fastapi import Depends, Request

from bms_msg.broker.backpressure import BackpressureTuning, RateLimiter, default_backpressure_tuning
from bms_msg.broker.connection import ConnectionRegistry, ConnectionTuning, connection_tuning_from_settings
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import OffsetEnumerator, OffsetPurger, OffsetResetter, OffsetStore
from bms_msg.broker.replay import AuditSink, ReplayRunner, UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry, StreamTuning, default_stream_tuning
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.broker.watermark import StorageGate
from bms_msg.tenant.context import CurrentTenant
from bms_msg.tenant.resolve import frame_tenant, management_tenant

__all__ = [
    "AuditSinkDep",
    "BackpressureMetricsDep",
    "BackpressureTuningDep",
    "ConnectionRegistryDep",
    "ConnectionTuningDep",
    "CurrentTenantDep",
    "DataPlaneTenantDep",
    "DeadLetterQueueDep",
    "DelaySchedulerDep",
    "DelayStoreDep",
    "DeliveryStateDep",
    "GroupStoreDep",
    "MessageStoreDep",
    "OffsetEnumeratorDep",
    "OffsetPurgerDep",
    "OffsetResetterDep",
    "OffsetStoreDep",
    "PartitionDirectoryDep",
    "RateLimiterDep",
    "ReplayRunnerDep",
    "RetentionThrottleDep",
    "StorageGateDep",
    "StreamRegistryDep",
    "StreamTuningDep",
    "TopicCacheDep",
    "UowFactoryDep",
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
    "get_message_store",
    "get_offset_enumerator",
    "get_offset_purger",
    "get_offset_resetter",
    "get_offset_store",
    "get_partition_directory",
    "get_rate_limiter",
    "get_replay_runner",
    "get_retention_throttle",
    "get_stream_registry",
    "get_stream_tuning",
    "get_topic_cache",
    "get_uow_factory",
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
