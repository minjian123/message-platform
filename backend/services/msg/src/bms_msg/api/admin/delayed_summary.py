"""延迟汇总只读端点（`GET /api/v1/msg/admin/delayed/summary`；06-1-1 详细设计 §8）。

独立端点，`/admin/delayed` 契约零改动；档位分布与堆积提示取自同一份
`DelayStore.list_for_tenant()` 当前租户快照，逐记录做 `describe` 授权过滤。
"""

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query

from bms_msg.api.deps import CurrentTenantDep, DelayStoreDep, TopicAclServiceDep
from bms_msg.broker.admin_view import DelaySummaryInspector
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.admin_view import DelaySummary, LevelBucket

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""只读端点声明的产品错误码。"""

router = BaseRouter(
    key="msg_admin_delayed_summary",
    prefix="/msg/admin/delayed/summary",
    tags=["msg-admin-delayed-summary"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def _inspector(uow: UnitOfWork, store: DelayStoreDep) -> DelaySummaryInspector:
    """组装延迟汇总只读投影。

    Args:
        uow: 平台库工作单元。
        store: 延迟区存储。

    Returns:
        DelaySummaryInspector: 只读投影。
    """
    session = cast("DbSession", uow.session)
    return DelaySummaryInspector(router=TopicRouter(TopicRepository(session)), store=store)


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def get_delay_summary(
    tenant: CurrentTenantDep,
    uow: PlatformUowDep,
    store: DelayStoreDep,
    acl: TopicAclServiceDep,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    partition: Annotated[int | None, Query(description="分区号过滤")] = None,
) -> ApiResponse[DelaySummary]:
    """延迟档位分布与堆积提示（服务端当前时间计算剩余时间；只读，不触发转投）。

    命名空间非法 350010，主题不存在 350001，分区越界 350009。
    """
    summary = await _inspector(uow, store).summarize(
        tenant=tenant,
        namespace=namespace,
        topic=topic,
        partition=partition,
        acl=acl,
    )
    levels: ConcurrentStableList[LevelBucket] = ConcurrentStableList(
        LevelBucket(level=bucket.level, count=bucket.count, min_remaining_ms=bucket.min_remaining_ms)
        for bucket in summary.levels
    )
    return ApiResponse.ok(
        DelaySummary(
            namespace=namespace,
            topic=topic,
            total=summary.total,
            levels=levels,
            stale_count=summary.stale_count,
            quota_used_ratio=summary.quota_used_ratio,
            hint=summary.hint,
        )
    )
