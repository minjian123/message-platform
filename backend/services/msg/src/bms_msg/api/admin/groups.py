"""消费组管理接口（`/api/v1/msg/admin/groups`）：列表 / 详情（详细设计 §7.4、§8.5）。

- **权限码**：`msg:query`（只读）；`/admin/*` 与 `/msg/*` 路径分离，便于分别限流与鉴权。
- **租户**：唯一出口 `get_current_tenant_context()`；请求体不接收 `tenant`。
- **数据源**：组状态在进程级组存储（`app.state`），主题名与分区数在平台库主题表，位点 / LEO 在进程级
  位点与消息存储——本接口只做**只读拼装**，不写任何状态。
- **范围**：本任务只落最小只读集（状态 / 世代 / 成员 / 分配 / 位点 / 积压）；06 域在此基础上扩字段与筛选。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE
from bms_msg.api.deps import CurrentTenantDep, GroupStoreDep, MessageStoreDep, OffsetStoreDep, TopicAclServiceDep
from bms_msg.broker.group import GroupMember, GroupStore
from bms_msg.broker.inspector import GroupInspector, GroupRow, PartitionLag
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.group import GroupDetail, GroupItem, GroupMemberItem, GroupPage, GroupPartitionItem
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.GROUP_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""查询端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_admin_groups",
    prefix="/msg/admin/groups",
    tags=["msg-admin-groups"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""

NamespacePath = Annotated[str, Path(min_length=1, max_length=64, description="命名空间")]
"""命名空间路径参数。"""

TopicPath = Annotated[str, Path(min_length=1, max_length=200, description="主题名")]
"""主题名路径参数。"""

GroupPath = Annotated[str, Path(min_length=1, max_length=128, description="消费组名")]
"""组名路径参数。"""


def _inspector(uow: UnitOfWork, groups: GroupStore, messages: MessageStore, offsets: OffsetStore) -> GroupInspector:
    """组装只读投影（请求级主题仓储 + 进程级组 / 消息 / 位点存储）。

    Args:
        uow: 平台库工作单元。
        groups: 组存储。
        messages: 消息存储（取 LEO）。
        offsets: 位点存储（取已提交位点）。

    Returns:
        GroupInspector: 组只读投影。
    """
    session = cast("DbSession", uow.session)
    topics = TopicRepository(session)
    return GroupInspector(
        groups=groups,
        topics=topics,
        router=TopicRouter(topics),
        messages=messages,
        offsets=offsets,
    )


def _item(row: GroupRow) -> GroupItem:
    """组列表行 → 响应明细。

    Args:
        row: 组列表行。

    Returns:
        GroupItem: 响应明细。
    """
    return GroupItem(
        group=row.record.group,
        namespace=row.namespace,
        topic=row.topic,
        state=row.record.state,
        generation=row.record.generation,
        member_count=len(row.record.members),
        assigned_partitions=row.assigned_partitions,
    )


def _member(member: GroupMember) -> GroupMemberItem:
    """成员 → 响应明细。

    Args:
        member: 成员。

    Returns:
        GroupMemberItem: 响应明细。
    """
    return GroupMemberItem(
        consumer_id=member.consumer_id,
        joined_at=member.joined_at,
        last_seen=member.last_seen,
        partitions=ConcurrentStableList(member.partitions),
    )


def _partition(lag: PartitionLag) -> GroupPartitionItem:
    """分区位点 → 响应明细。

    Args:
        lag: 分区位点与积压。

    Returns:
        GroupPartitionItem: 响应明细。
    """
    return GroupPartitionItem(
        partition=lag.partition_no,
        owner=lag.owner,
        committed=lag.committed,
        leo=lag.leo,
        lag=lag.lag,
    )


async def _filter_rows(
    acl: TopicAclServiceDep,
    tenant: CurrentTenantDep,
    rows: ConcurrentStableList[GroupRow],
) -> ConcurrentStableList[GroupRow]:
    """按 `describe` ACL 过滤组列表（无权限主题不返回）。

    Args:
        acl: ACL 守卫。
        tenant: 当前租户。
        rows: 候选组行。

    Returns:
        ConcurrentStableList[GroupRow]: 允许查看的组行（保持入参顺序）。
    """
    visible: ConcurrentStableList[GroupRow] = ConcurrentStableList()
    for row in rows:
        allowed = await acl.filter_allowed(
            tenant_id=int(tenant.tenant_id),
            namespace=row.namespace,
            topics=ConcurrentStableList([row.topic]),
            action=ACL_ACTION_DESCRIBE,
        )
        if allowed:
            visible.add(row)
    return visible


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_groups(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    groups: GroupStoreDep,
    messages: MessageStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[GroupPage]:
    """消费组列表（组名升序；可按命名空间 / 主题过滤）。

    需要 msg:query 权限；按租户隔离；主题已删的孤儿组记录不展示。命名空间过滤值非法返回 350010。
    """
    rows, total = await _inspector(uow, groups, messages, offsets).list_groups(
        tenant=tenant, namespace=namespace, topic=topic, page=page, size=size
    )
    visible = await _filter_rows(acl, tenant, rows)
    return ApiResponse.ok(
        GroupPage(
            items=ConcurrentStableList(_item(row) for row in visible),
            total=total,
            page=page,
            size=size,
        )
    )


@router.get(
    "/{namespace}/{topic}/{group}",
    dependencies=[_QUERY],
    openapi_extra={"x-error-codes": _QUERY_ERRORS},
)
async def get_group(
    namespace: NamespacePath,
    topic: TopicPath,
    group: GroupPath,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    groups: GroupStoreDep,
    messages: MessageStoreDep,
    offsets: OffsetStoreDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[GroupDetail]:
    """消费组详情（状态 / 世代 / 成员 / 分配 / 各分区位点与积压）。

    需要 msg:query 权限；主题不存在 350001，组不存在 350013。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=namespace,
        topic=topic,
        action=ACL_ACTION_DESCRIBE,
    )
    view = await _inspector(uow, groups, messages, offsets).describe(
        tenant=tenant, namespace=namespace, topic=topic, group=group
    )
    return ApiResponse.ok(
        GroupDetail(
            group=view.row.record.group,
            namespace=view.row.namespace,
            topic=view.row.topic,
            state=view.row.record.state,
            generation=view.row.record.generation,
            partition_count=view.partition_count,
            members=ConcurrentStableList(_member(member) for member in view.row.record.members),
            partitions=ConcurrentStableList(_partition(lag) for lag in view.partitions),
        )
    )
