"""平台概览只读端点（`GET /api/v1/msg/admin/overview`；06-1-1 详细设计 §7）。

四张数字卡只读 06-3 采样快照与存储水位，不重复采集、不在请求内全量扫描。
**授权口径**：本端点无主题参数、不输出主题分项，按当前租户编码前缀汇总；
主题级 `describe` 过滤由分区 / 消息 / 延迟汇总三端点承担（设计「对齐记录」第 10 项例外）。
"""

from bms_core.api.base import BaseRouter
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.api.deps import CurrentTenantDep, MetricsSnapshotsDep, StorageMetricsDep
from bms_msg.broker.admin_view import OverviewInspector
from bms_msg.errors import MsgErrorCode
from bms_msg.schemas.admin_view import Overview

__all__ = ["router"]

_QUERY = Depends(require_permission("msg:query"))

_QUERY_ERRORS = [
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""只读端点声明的产品错误码（本端点不解析主题，无主题类错误码）。"""

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
) -> ApiResponse[Overview]:
    """四张数字卡（入口吞吐 / 当前积压 / 死信数 / 磁盘使用率）。

    快照尚未产出时四值返回 0 且 `snapshot_available=false`，不回源扫描。
    """
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
