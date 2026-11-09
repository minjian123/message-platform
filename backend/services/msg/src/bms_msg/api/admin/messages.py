"""消息检索只读端点（`GET /api/v1/msg/admin/messages`；06-1-1 详细设计 §6）。

- 权限 `msg:query` + 主题级 `describe` 授权；`namespace` / `topic` 必填。
- 条件跨类相与；`log_id` 精确命中走存储只读端口索引，未命中返回空页、不回退全量扫描。
- 响应只有头信息与载荷长度 / SHA-256 摘要，**不含消息正文**。
"""

from datetime import datetime
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE
from bms_msg.api.deps import CurrentTenantDep, MessageStoreDep, TopicAclServiceDep
from bms_msg.broker.admin_view import MessageInspector
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.admin_view import MessageItem, MessagePage
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
    int(MsgErrorCode.ACL_DENIED),
]
"""只读端点声明的产品错误码（`10001` 属契约层，不进本表）。"""

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


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def search_messages(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    messages: MessageStoreDep,
    acl: TopicAclServiceDep,
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
    items: ConcurrentStableList[MessageItem] = ConcurrentStableList(
        MessageItem(
            log_id=view.log_id,
            message_id=view.message_id,
            partition=view.partition_no,
            offset=view.offset,
            published_at=view.published_at,
            headers=dict(view.headers.items()),
            payload_len=view.payload_len,
            payload_sha256=view.payload_sha256,
        )
        for view in views
    )
    return ApiResponse.ok(
        MessagePage(
            namespace=namespace,
            topic=topic,
            total=total,
            page=page,
            size=size,
            items=items,
        )
    )
