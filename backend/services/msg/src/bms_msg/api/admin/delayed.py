"""延迟中消息的只读视图（`/api/v1/msg/admin/delayed`；01-5 详细设计 §7.3、§8.5）。

- **权限码**：`msg:query`（只读）；与 `/admin/topics`、`/admin/groups` 同权限码。
- **租户**：唯一出口 `get_current_tenant_context()`；请求体不接收 `tenant`。
- **数据源**：延迟区在进程级存储（`app.state`）；过滤值给定时与发布链路**同码同判据**
  （命名空间 350010 / 主题 350001 / 分区越界 350009 / 档位 10001），排障时两侧结论一致。
- **只读**：不改期、不取消、不触发转投；06 域在此基础上扩筛选与指标看板。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.context import get_current_client_ip, get_current_trace_id, get_current_user_id
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE
from bms_msg.api.deps import (
    CurrentTenantDep,
    DelayDisposalDep,
    DelayStoreDep,
    TopicAclServiceDep,
)
from bms_msg.broker.delay import DelayedRow, DelayInspector, DelayStore
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.delay import DelayDiscardRequest, DelayDiscardResult, DelayedItem, DelayedPage
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))
_MANAGE = Depends(require_permission("msg:manage"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""查询端点声明的错误码（`x-error-codes`）。"""

_MANAGE_ERRORS = [
    int(MsgErrorCode.MESSAGE_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""延迟丢弃端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_admin_delayed",
    prefix="/msg/admin/delayed",
    tags=["msg-admin-delayed"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def _inspector(uow: UnitOfWork, store: DelayStore) -> DelayInspector:
    """组装只读投影（请求级主题仓储 + 进程级延迟区）。

    Args:
        uow: 平台库工作单元。
        store: 延迟区存储。

    Returns:
        DelayInspector: 延迟区只读投影。
    """
    session = cast("DbSession", uow.session)
    return DelayInspector(store=store, router=TopicRouter(TopicRepository(session)))


def _item(row: DelayedRow) -> DelayedItem:
    """待投列表行 → 响应明细。

    Args:
        row: 待投列表行。

    Returns:
        DelayedItem: 响应明细。
    """
    return DelayedItem(
        log_id=row.record.log_id,
        message_id=row.record.message_id,
        namespace=row.record.namespace,
        topic=row.record.topic,
        partition=row.record.partition_no,
        level=row.record.delay_level,
        deliver_at=row.record.deliver_at,
        remaining_ms=row.remaining_ms,
    )


async def _filter_rows(
    acl: TopicAclServiceDep,
    tenant: CurrentTenantDep,
    rows: ConcurrentStableList[DelayedRow],
) -> ConcurrentStableList[DelayedRow]:
    """按 `describe` ACL 过滤延迟消息列表（无权限主题不返回）。

    Args:
        acl: ACL 守卫。
        tenant: 当前租户。
        rows: 候选延迟记录。

    Returns:
        ConcurrentStableList[DelayedRow]: 允许查看的记录（保持入参顺序）。
    """
    visible: ConcurrentStableList[DelayedRow] = ConcurrentStableList()
    for row in rows:
        allowed = await acl.filter_allowed(
            tenant_id=int(tenant.tenant_id),
            namespace=row.record.namespace,
            topics=ConcurrentStableList([row.record.topic]),
            action=ACL_ACTION_DESCRIBE,
        )
        if allowed:
            visible.add(row)
    return visible


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def list_delayed(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: DelayStoreDep,
    acl: TopicAclServiceDep,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    partition: Annotated[int | None, Query(ge=0, description="分区号过滤")] = None,
    level: Annotated[str | None, Query(max_length=8, description="延迟档位过滤（1s / 5s / … / 60m）")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[DelayedPage]:
    """延迟中消息列表（到期时刻升序，最急的在前；只读，不触发转投）。

    需要 msg:query 权限；按租户隔离。命名空间非法 350010，主题不存在或未启用 350001，
    分区号越界 350009，档位非法 10001；`size` 上界 100（越界 10001）。
    """
    rows, total = await _inspector(uow, store).list_delayed(
        tenant=tenant,
        namespace=namespace,
        topic=topic,
        partition=partition,
        level=level,
        page=page,
        size=size,
    )
    visible = await _filter_rows(acl, tenant, rows)
    return ApiResponse.ok(
        DelayedPage(
            items=ConcurrentStableList(_item(row) for row in visible),
            total=total,
            page=page,
            size=size,
        )
    )


@router.post("/{log_id}/discard", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def discard_delayed(
    log_id: str,
    req: DelayDiscardRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    disposal: DelayDisposalDep,
    confirm: Annotated[bool, Query(description="显式确认（必须为 true）")] = False,
) -> ApiResponse[DelayDiscardResult]:
    """丢弃一条延迟记录（未到期也丢弃；强审计失败即恢复）。

    需要 msg:manage 权限；`confirm` 缺失 / 原因为空 10001；记录不存在或已转投 350003。
    """
    if not confirm:
        raise ParamError("高危写操作须显式确认（confirm=true）")
    discarded = await disposal.discard(
        tenant_id=int(tenant.tenant_id),
        log_id=log_id,
        reason=req.reason,
        actor_id=get_current_user_id(),
        ip=get_current_client_ip(),
        trace_id=get_current_trace_id(),
        session=uow.session,
    )
    return ApiResponse.ok(DelayDiscardResult(log_id=log_id, discarded=discarded))
