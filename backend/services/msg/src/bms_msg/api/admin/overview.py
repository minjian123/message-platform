"""平台概览只读端点（`GET /api/v1/msg/admin/overview`；06-1-1 详细设计 §7）。

四张数字卡优先读 02-8-4 落库的**每租户快照**（跨实例统一口径），读不到或已过期才回退进程内
06-3 采样快照（单实例 / 冷启动兼容）。不在请求内全量扫描。
**授权口径**：本端点无主题参数、不输出主题分项，按当前租户编码前缀汇总；
主题级 `describe` 过滤由分区 / 消息 / 延迟汇总三端点承担（设计「对齐记录」第 10 项例外）。
"""

from datetime import UTC, datetime
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.api.deps import CurrentTenantDep, MetricsSnapshotsDep, StorageMetricsDep
from bms_msg.broker.admin_view import OverviewInspector
from bms_msg.core.config import OVERVIEW_SNAPSHOT_STALE_SECONDS
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.overview import OverviewSnapshotRepository
from bms_msg.schemas.admin_view import Overview

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""只读端点声明的产品错误码（本端点不解析主题，无主题类错误码）。"""


def _utc_now() -> datetime:
    """当前 UTC 时间（naive；与库列同口径）。

    Returns:
        datetime: 当前 UTC 时间（无时区）。
    """
    return datetime.now(UTC).replace(tzinfo=None)


router = BaseRouter(
    key="msg_admin_overview",
    prefix="/msg/admin/overview",
    tags=["msg-admin-overview"],
)


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _QUERY_ERRORS})
async def get_overview(
    tenant: CurrentTenantDep,
    snapshots: MetricsSnapshotsDep,
    storage: StorageMetricsDep,
    uow: Annotated[UnitOfWork, Depends(get_platform_uow)],
) -> ApiResponse[Overview]:
    """四张数字卡（入口吞吐 / 当前积压 / 死信数 / 磁盘使用率）。

    优先读每租户落库快照（02-8-4，跨实例一致）；无行或超过 3 个采样周期则回退进程内快照。
    快照尚未产出时四值返回 0 且 `snapshot_available=false`，不回源扫描。
    """
    session = cast("DbSession", uow.session)
    row = await OverviewSnapshotRepository(session).get_for_tenant(tenant_id=int(tenant.tenant_id))
    if row is not None and (_utc_now() - row.sampled_at).total_seconds() <= OVERVIEW_SNAPSHOT_STALE_SECONDS:
        return ApiResponse.ok(
            Overview(
                messages_in_per_second=float(row.messages_in_per_second),
                consumer_lag=int(row.consumer_lag),
                dlq_total=int(row.dlq_total),
                disk_used_ratio=float(row.disk_used_ratio),
                snapshot_available=True,
            )
        )
    view = OverviewInspector(snapshots=snapshots, storage=storage).read(tenant=tenant)
    return ApiResponse.ok(
        Overview(
            messages_in_per_second=view.messages_in_per_second,
            consumer_lag=view.consumer_lag,
            dlq_total=view.dlq_total,
            disk_used_ratio=view.disk_used_ratio,
            snapshot_available=view.snapshot_available,
        )
    )
