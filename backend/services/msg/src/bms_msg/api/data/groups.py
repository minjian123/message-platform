"""消费组数据面接口（`/api/v1/msg/join`、`/msg/heartbeat`、`/msg/leave`；详细设计 §7.1、§7.2）。

- **权限码**：三端点一律 `msg:consume`（成员身份属消费能力，不单开权限码）。
- **租户**：数据面唯一出口 `get_data_plane_tenant()`（内部 `frame_tenant()`）；请求体不接收 `tenant`。
- **协议形态**：服务端主导 + eager 全量再均衡 + 单段下发——`join` / `heartbeat` 直接返回
  `generation + partitions`；`leave` 幂等（未注册成员也返回成功）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.api.deps import DataPlaneTenantDep, DeliveryStateDep, GroupStoreDep
from bms_msg.broker.coordinator import GroupCoordinator, MemberState
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import ConsumerGroup, GroupStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import HEARTBEAT_INTERVAL_MS, SESSION_TIMEOUT_MS
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.group import LeaveResult, MemberAssignment, MemberRequest

__all__ = ["router"]

_CONSUME = Depends(require_permission("msg:consume"))

_JOIN_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""加入端点声明的错误码（`x-error-codes`；`join` 新建组，故不报 `350013`）。"""

_HEARTBEAT_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.GROUP_NOT_FOUND),
    int(MsgErrorCode.MEMBER_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""心跳端点声明的错误码（`x-error-codes`）。"""

_LEAVE_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.GROUP_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""离开端点声明的错误码（`x-error-codes`；`leave` 幂等，不报 `350014`）。"""

router = BaseRouter(
    key="msg_data_groups",
    prefix="/msg",
    tags=["msg-data"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def _coordinator(uow: UnitOfWork, groups: GroupStore, delivery: DeliveryState) -> GroupCoordinator:
    """组装协调器（请求级主题仓储 + 进程级组存储 / 投递状态）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        groups: 组存储。
        delivery: 投递状态（撤销易主分区时清在途）。

    Returns:
        GroupCoordinator: 消费组协调器。
    """
    session = cast("DbSession", uow.session)
    return GroupCoordinator(groups=groups, router=TopicRouter(TopicRepository(session)), delivery=delivery)


def _assignment(state: MemberState, request: MemberRequest) -> MemberAssignment:
    """成员状态 → 分配响应。

    Args:
        state: 协调器返回的组快照 + 成员。
        request: 原始请求（取组名 / 主题名 / 成员标识）。

    Returns:
        MemberAssignment: 分配响应。
    """
    return MemberAssignment(
        group=state.group.group,
        topic=request.topic,
        consumer_id=state.member.consumer_id,
        generation=state.group.generation,
        state=state.group.state,
        partitions=ConcurrentStableList(state.member.partitions),
        member_count=len(state.group.members),
        heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
        session_timeout_ms=SESSION_TIMEOUT_MS,
    )


@router.post("/join", dependencies=[_CONSUME], openapi_extra={"x-error-codes": _JOIN_ERRORS})
async def join(
    req: MemberRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    groups: GroupStoreDep,
    delivery: DeliveryStateDep,
) -> ApiResponse[MemberAssignment]:
    """加入消费组（服务端主导分配）。

    需要 msg:consume 权限；组按 `(namespace, topic)` 维度建，同 `consumer_id` 再次加入视为重连
    （刷新存活、不递增世代、分配不变）。返回 `generation` 与 `partitions`，客户端按分配拉取。
    主题不存在 350001，命名空间非已启用 module_key 返回 350010。
    """
    state = await _coordinator(uow, groups, delivery).join(tenant=tenant, request=req)
    return ApiResponse.ok(_assignment(state, req))


@router.post("/heartbeat", dependencies=[_CONSUME], openapi_extra={"x-error-codes": _HEARTBEAT_ERRORS})
async def heartbeat(
    req: MemberRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    groups: GroupStoreDep,
    delivery: DeliveryStateDep,
) -> ApiResponse[MemberAssignment]:
    """成员心跳（独立于业务处理：处理慢不会因心跳断而被误判死亡）。

    需要 msg:consume 权限；顺带清理同组超时成员，必要时重算分配并返回新 `generation`。
    组不存在 350013，成员不在组内或已超时失效 350014。
    """
    state = await _coordinator(uow, groups, delivery).heartbeat(tenant=tenant, request=req)
    return ApiResponse.ok(_assignment(state, req))


@router.post("/leave", dependencies=[_CONSUME], openapi_extra={"x-error-codes": _LEAVE_ERRORS})
async def leave(
    req: MemberRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    groups: GroupStoreDep,
    delivery: DeliveryStateDep,
) -> ApiResponse[LeaveResult]:
    """主动离开消费组（幂等：未注册成员也返回成功）。

    需要 msg:consume 权限；离开后若组内仍有他人则立即重算分配，最后一名成员离开则组转 `empty`
    （组记录与位点保留）。组不存在 350013。
    """
    record: ConsumerGroup = await _coordinator(uow, groups, delivery).leave(tenant=tenant, request=req)
    return ApiResponse.ok(
        LeaveResult(
            group=record.group,
            topic=req.topic,
            consumer_id=req.consumer_id,
            generation=record.generation,
            state=record.state,
        )
    )
