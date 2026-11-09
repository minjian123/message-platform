"""分区详情只读端点（`GET /api/v1/msg/admin/partitions`；06-1-1 详细设计 §5）。

- 权限 `msg:query` + 主题级 `describe` 授权；租户仅由 `CurrentTenantDep` 提供。
- 实时 LEO 取 `MessageStore.leo()`（不是落库检查点）；`hw` / `isr` 恒为可空占位，
  02-4 / 02-5 交付后只换取值、不改契约。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE
from bms_msg.api.deps import (
    CurrentTenantDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    TopicAclServiceDep,
)
from bms_msg.broker.admin_view import PartitionInspector
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.admin_view import PartitionItem, PartitionPage

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.GROUP_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""只读端点声明的产品错误码（`10001` 属契约层，不进本表）。"""

router = BaseRouter(
    key="msg_admin_partitions",
    prefix="/msg/admin/partitions",
    tags=["msg-admin-partitions"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题 / 分区元数据归平台库）。"""


def _inspector(
    uow: UnitOfWork,
    messages: MessageStoreDep,
    offsets: OffsetStoreDep,
    groups: GroupStoreDep,
) -> PartitionInspector:
    """组装分区只读投影。

    Args:
        uow: 平台库工作单元。
        messages: 消息存储。
        offsets: 位点存储。
        groups: 组存储。

    Returns:
        PartitionInspector: 只读投影。
    """
    session = cast("DbSession", uow.session)
    return PartitionInspector(
        router=TopicRouter(TopicRepository(session)),
        messages=messages,
        offsets=offsets,
        groups=groups,
        partitions=PartitionRepository(session),
    )


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_partitions(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    offsets: OffsetStoreDep,
    groups: GroupStoreDep,
    acl: TopicAclServiceDep,
    namespace: Annotated[str, Query(max_length=64, description="命名空间")],
    topic: Annotated[str, Query(max_length=200, description="主题名")],
    partition: Annotated[int | None, Query(description="分区号过滤（缺省全部）")] = None,
    group: Annotated[str | None, Query(max_length=200, description="消费组（缺省不给已提交位点）")] = None,
) -> ApiResponse[PartitionPage]:
    """分区详情（按分区号升序；只读）。

    命名空间非法 350010，主题不存在 350001，分区越界 350009，组不存在 350013。
    """
    await acl.authorize(tenant_id=int(tenant.tenant_id), namespace=namespace, topic=topic, action=ACL_ACTION_DESCRIBE)
    detail = await _inspector(uow, messages, offsets, groups).describe(
        tenant=tenant,
        namespace=namespace,
        topic=topic,
        partition=partition,
        group=group,
    )
    items: ConcurrentStableList[PartitionItem] = ConcurrentStableList(
        PartitionItem(
            partition=view.partition_no,
            leader=view.leader,
            leo=view.leo,
            hw=None,
            isr=ConcurrentStableList(),
            committed=view.committed,
            lag=view.lag,
            owner=view.owner,
        )
        for view in detail.items
    )
    return ApiResponse.ok(
        PartitionPage(
            namespace=namespace,
            topic=topic,
            group=group,
            partition_count=detail.partition_count,
            items=items,
        )
    )
