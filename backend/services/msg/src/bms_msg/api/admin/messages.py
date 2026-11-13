"""消息检索只读端点（`GET /api/v1/msg/admin/messages`；06-1-1 详细设计 §6）。

- 权限 `msg:query` + 主题级 `describe` 授权；`namespace` / `topic` 必填。
- 条件跨类相与；`log_id` 精确命中走存储只读端口索引，未命中返回空页、不回退全量扫描。
- 响应只有头信息与载荷长度 / SHA-256 摘要，**不含消息正文**。
"""

from datetime import datetime
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE, ACL_ACTION_MANAGE
from bms_msg.api.deps import AuditSinkDep, CurrentTenantDep, MessageStoreDep, TopicAclServiceDep
from bms_msg.audit.trail import (
    MESSAGE_DELETE_ACTION,
    MESSAGE_SEARCH_ACTION,
    AuditEmitter,
    emit_record_operation,
    emit_sensitive,
)
from bms_msg.broker.admin_view import MessageInspector
from bms_msg.broker.log import MessageHeaderView, MessageStore
from bms_msg.broker.replay import AuditSink
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.admin_view import (
    MessageDeleteBatchRequest,
    MessageDeleteItem,
    MessageDeleteResult,
    MessageItem,
    MessagePage,
)
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
    int(MsgErrorCode.ACL_DENIED),
]
"""只读端点声明的产品错误码（`10001` 属契约层，不进本表）。"""

_MANAGE_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""删消息端点声明的产品错误码（`10001` 属契约层）。"""

MAX_DELETE_BATCH = 100
"""批量删消息的单次上限。"""

router = BaseRouter(
    key="msg_admin_messages",
    prefix="/msg/admin/messages",
    tags=["msg-admin-messages"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def _inspector(uow: UnitOfWork, messages: MessageStoreDep) -> MessageInspector:
    """组装消息检索只读投影。

    Args:
        uow: 平台库工作单元。
        messages: 消息存储（只读头视图）。

    Returns:
        MessageInspector: 只读投影。
    """
    session = cast("DbSession", uow.session)
    return MessageInspector(router=TopicRouter(TopicRepository(session)), messages=messages)


def _admin_item(view: MessageHeaderView) -> MessageItem:
    """把头视图映射为管理面响应项（字段口径唯一出处：检索响应与用例共用）。

    **只搬头信息**：载荷长度与摘要由存储侧算好随视图传入，本函数在类型上拿不到正文；
    trace 四字段（06-4）一并回显，排障时可凭 `span_id` 定位到具体一跳。

    Args:
        view: 存储侧只读头视图。

    Returns:
        MessageItem: 管理面消息项（不含正文）。
    """
    return MessageItem(
        log_id=view.log_id,
        message_id=view.message_id,
        partition=view.partition_no,
        offset=view.offset,
        published_at=view.published_at,
        headers=dict(view.headers.items()),
        trace_id=view.trace.trace_id,
        span_id=view.trace.span_id,
        parent_span_id=view.trace.parent_span_id,
        trace_flags=view.trace.trace_flags,
        payload_len=view.payload_len,
        payload_sha256=view.payload_sha256,
    )


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def search_messages(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    acl: TopicAclServiceDep,
    sink: AuditSinkDep,
    namespace: Annotated[str, Query(max_length=64, description="命名空间")],
    topic: Annotated[str, Query(max_length=200, description="主题名")],
    partition: Annotated[int | None, Query(description="分区号过滤（缺省全部）")] = None,
    offset_from: Annotated[int | None, Query(description="起始 offset（含）")] = None,
    offset_to: Annotated[int | None, Query(description="结束 offset（不含）")] = None,
    key: Annotated[str | None, Query(max_length=200, description="分区键精确值")] = None,
    time_from: Annotated[datetime | None, Query(description="发布时间下界（ISO 8601，含）")] = None,
    time_to: Annotated[datetime | None, Query(description="发布时间上界（ISO 8601，不含）")] = None,
    log_id: Annotated[str | None, Query(max_length=128, description="记录 ID（精确命中走索引）")] = None,
    page: Annotated[int, Query(description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(description=f"每页条数（1~{MAX_LIST_SIZE}）")] = 20,
) -> ApiResponse[MessagePage]:
    """按组合条件检索消息头（只返回头与摘要，不返回正文）。

    命名空间非法 350010，主题不存在 350001，分区越界 350009；
    条件区间倒置 / 分页越界 10001。
    """
    await acl.authorize(tenant_id=int(tenant.tenant_id), namespace=namespace, topic=topic, action=ACL_ACTION_DESCRIBE)
    views, total = await _inspector(uow, messages).search(
        tenant=tenant,
        namespace=namespace,
        topic=topic,
        partition=partition,
        offset_from=offset_from,
        offset_to=offset_to,
        key=key,
        time_from=time_from,
        time_to=time_to,
        log_id=log_id,
        page=page,
        size=size,
    )
    items: ConcurrentStableList[MessageItem] = ConcurrentStableList(_admin_item(view) for view in views)
    page_result = MessagePage(
        namespace=namespace,
        topic=topic,
        total=total,
        page=page,
        size=size,
        items=items,
    )
    await emit_sensitive(
        cast("AuditEmitter", sink),
        action=MESSAGE_SEARCH_ACTION,
        tenant_id=int(tenant.tenant_id),
        params=ConcurrentStableDict(
            {
                "namespace": namespace,
                "topic": topic,
                "partition": partition,
                "log_id": log_id,
                "page": page,
                "size": size,
                "matched": total,
            }
        ),
        method="GET",
        path="/api/v1/msg/admin/messages",
        module_key=namespace,
        resource_key=topic,
        session=uow.session,
    )
    return ApiResponse.ok(page_result)


async def _delete_one(
    *,
    messages: MessageStore,
    uow: UnitOfWork,
    tenant: CurrentTenantDep,
    acl: TopicAclServiceDep,
    sink: AuditSink,
    log_id: str,
) -> str:
    """删除一条消息（墓碑标记）并写审计。

    Args:
        messages: 消息存储。
        uow: 平台库工作单元（主题元数据与审计同事务）。
        tenant: 当前租户。
        acl: 主题授权服务。
        sink: 审计出口。
        log_id: 记录 ID。

    Returns:
        str: `deleted` / `not_found` / `already_deleted`。

    Raises:
        InternalError: 强审计写入失败（业务回滚）。
    """
    view = await messages.find_by_log_id(log_id=log_id)
    if view is None:
        return "not_found"
    topic_row = await TopicRepository(cast("DbSession", uow.session)).get(int(view.topic_id))
    if topic_row is None:
        return "not_found"
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=topic_row.namespace,
        topic=topic_row.name,
        action=ACL_ACTION_MANAGE,
    )
    deleted = await messages.delete(topic_id=int(view.topic_id), partition_no=int(view.partition_no), log_id=log_id)
    result = "deleted" if deleted else "not_found"
    if deleted:
        await emit_record_operation(
            cast("AuditEmitter", sink),
            action=MESSAGE_DELETE_ACTION,
            tenant_id=int(tenant.tenant_id),
            log_id=log_id,
            params=ConcurrentStableDict({"partition": int(view.partition_no), "offset": int(view.offset)}),
            method="POST",
            path=f"/api/v1/msg/admin/messages/{log_id}/delete",
            module_key=topic_row.namespace,
            resource_key=topic_row.name,
            session=uow.session,
        )
    return result


@router.post("/{log_id}/delete", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def delete_message(
    log_id: str,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    acl: TopicAclServiceDep,
    sink: AuditSinkDep,
    confirm: Annotated[bool, Query(description="显式确认（必须为 true）")] = False,
) -> ApiResponse[MessageDeleteResult]:
    """删除一条消息（墓碑标记；位点与 LEO 不变）。

    需要 msg:manage 权限 + 主题级 `manage`；`confirm` 缺失 10001；
    记录不存在或已删除返回 `not_found`（不报 500）。
    """
    if not confirm:
        raise ParamError("高危写操作须显式确认（confirm=true）")
    result = await _delete_one(messages=messages, uow=uow, tenant=tenant, acl=acl, sink=sink, log_id=log_id)
    item = MessageDeleteItem(log_id=log_id, result=result)
    return ApiResponse.ok(
        MessageDeleteResult(
            items=ConcurrentStableList([item]),
            deleted=1 if result == "deleted" else 0,
            not_found=0 if result == "deleted" else 1,
        )
    )


@router.post("/delete", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def delete_messages(
    req: MessageDeleteBatchRequest,
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    acl: TopicAclServiceDep,
    sink: AuditSinkDep,
) -> ApiResponse[MessageDeleteResult]:
    """批量删除消息（逐条墓碑标记；一次最多 100 条）。

    需要 msg:manage 权限 + 主题级 `manage`；`confirm` 缺失或批量超限 10001。
    """
    if not req.confirm:
        raise ParamError("高危写操作须显式确认（confirm=true）")
    if len(req.log_ids) > MAX_DELETE_BATCH:
        raise ParamError(f"批量删除上限 {MAX_DELETE_BATCH} 条：{len(req.log_ids)}")
    items: ConcurrentStableList[MessageDeleteItem] = ConcurrentStableList()
    deleted = 0
    not_found = 0
    for log_id in req.log_ids:
        result = await _delete_one(messages=messages, uow=uow, tenant=tenant, acl=acl, sink=sink, log_id=log_id)
        items.add(MessageDeleteItem(log_id=log_id, result=result))
        if result == "deleted":
            deleted += 1
        else:
            not_found += 1
    return ApiResponse.ok(MessageDeleteResult(items=items, deleted=deleted, not_found=not_found))
