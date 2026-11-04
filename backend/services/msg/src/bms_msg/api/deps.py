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

from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import AuditSink, ReplayRunner
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.tenant.context import CurrentTenant
from bms_msg.tenant.resolve import frame_tenant, management_tenant

__all__ = [
    "AuditSinkDep",
    "CurrentTenantDep",
    "DataPlaneTenantDep",
    "DeadLetterQueueDep",
    "DelaySchedulerDep",
    "DelayStoreDep",
    "DeliveryStateDep",
    "GroupStoreDep",
    "MessageStoreDep",
    "OffsetStoreDep",
    "ReplayRunnerDep",
    "get_audit_sink",
    "get_current_tenant_context",
    "get_data_plane_tenant",
    "get_dead_letter_queue",
    "get_delay_scheduler",
    "get_delay_store",
    "get_delivery_state",
    "get_group_store",
    "get_message_store",
    "get_offset_store",
    "get_replay_runner",
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
