"""消费位点管理接口（`/api/v1/msg/admin/offsets`）：位点重置（06-2，复用 02-3 端口）。

- **权限码**：`msg:manage`（高危写操作）。
- **复用而非重写**：真正写位点与写审计（`record_ref = msg.offset`）都在 `OffsetResetter.reset()` 里，
  端点只做「确认参数 + 归属与边界校验 + 组装响应」——不另写一套重置逻辑（需求 06-2 第 5 条）。
- **判定顺序**：确认 → 租户 → 主题（`350001` / 状态 `350028`）→ 组（`350013`）→ 分区号（`350009`）
  → 起点（`350003`）→ 端口（`350002` / `10001`）。
- **不改变活跃成员分配**：重置只改已提交位点，成员分配由 01-4 的世代号驱动，本端点不碰。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.acl.keys import ACL_ACTION_MANAGE
from bms_msg.api.deps import (
    CurrentTenantDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetResetterDep,
    OffsetStoreDep,
    TopicAclServiceDep,
)
from bms_msg.errors import (
    MsgErrorCode,
    MsgGroupNotFoundError,
    MsgMessageNotFoundError,
    MsgTopicNotFoundError,
    MsgTopicStateInvalidError,
)
from bms_msg.models.topic import STATUS_ACTIVE
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.offset import OffsetResetRequest, OffsetResetResult

__all__ = ["router"]

_LOGGER = get_logger("bms_msg.admin_offsets")

router = BaseRouter(
    key="msg_admin_offsets",
    prefix="/msg/admin/offsets",
    tags=["msg-admin-offsets"],
)

_MANAGE = Depends(require_permission("msg:manage"))

_RESET_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.TOPIC_STATE_INVALID),
    int(MsgErrorCode.GROUP_NOT_FOUND),
    int(MsgErrorCode.MESSAGE_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.OFFSET_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""位点重置端点声明的错误码（`x-error-codes`；`10001` 属契约层，不登记）。"""

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据在平台库）。"""


@router.post("/reset", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _RESET_ERRORS})
async def reset_offset(
    req: OffsetResetRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    groups: GroupStoreDep,
    offsets: OffsetStoreDep,
    resetter: OffsetResetterDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[OffsetResetResult]:
    """重置某组在某主题某分区上的已提交位点（可倒退可前跳，需 `confirm=true`）。

    需要 msg:manage 权限；`confirm` 缺失 10001；主题不存在 350001、状态不允许 350028；
    组不存在 350013；分区号越界 350009；位点早于分区起点 350003（消息已被保留期清理）；
    位点超过 LEO 350002；写审计由 `OffsetResetter` 以 `record_ref=msg.offset` 落库。
    """
    if not req.confirm:
        _LOGGER.warning("msg_write_confirm_missing", action="msg:offset.reset", topic=f"{req.namespace}/{req.topic}")
        raise ParamError("高危写操作须显式确认（confirm=true）")
    session = cast("DbSession", uow.session)
    topic = await TopicRepository(session).get_by_identity(namespace=req.namespace, name=req.topic)
    if topic is None:
        raise MsgTopicNotFoundError(f"主题不存在：{req.namespace}/{req.topic}")
    if topic.status != STATUS_ACTIVE:
        raise MsgTopicStateInvalidError(f"主题状态不允许该操作：{req.namespace}/{req.topic} 当前为 {topic.status}")
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_MANAGE,
    )
    topic_id = int(topic.id)
    if req.partition >= int(topic.partition_count):
        raise ParamError(f"分区号越界：{req.partition} ≥ 分区数 {int(topic.partition_count)}")
    if await groups.load(tenant_id=int(tenant.tenant_id), topic_id=topic_id, group=req.group) is None:
        raise MsgGroupNotFoundError(f"消费组不存在：{req.group}")
    start = await messages.start_offset(topic_id=topic_id, partition_no=req.partition)
    if req.offset < start:
        raise MsgMessageNotFoundError(f"位点早于分区起点：{req.offset} < {start}（该段消息已被保留期清理）")
    previous = await offsets.get(group=req.group, topic_id=topic_id, partition_no=req.partition)
    value = await resetter.reset(
        group=req.group,
        topic_id=topic_id,
        partition_no=req.partition,
        offset=req.offset,
    )
    _LOGGER.info(
        "msg_offset_reset",
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        group=req.group,
        partition=req.partition,
        previous=previous,
        offset=value,
    )
    return ApiResponse.ok(
        OffsetResetResult(
            namespace=req.namespace,
            topic=req.topic,
            group=req.group,
            partition=req.partition,
            previous_offset=int(previous),
            offset=int(value),
        )
    )
