"""租户用量管理面只读端点（06-8 详细设计 §4）。

`GET /api/v1/msg/admin/tenant-usage`（挂 `msg:query`，套统一响应）。

**为什么另起端点**：05-1 的 `GET /api/v1/msg/tenant-usage` 服务于平台 `kind=form` 投影读取器，
返回**裸 JSON**（不套 `ApiResponse`）且以服务身份读取；产品前端统一经宿主 `api.product()` 取数，
该通道强制解包 `{code,message,data}`，裸 JSON 会被判成业务错误。两条端点**共用**
`UsageSnapshotDep.snapshot()`、`column_payloads()` 与 `usage_row()`，口径与列序只有一份。

**只读约束**：不写任何数据、不接受 `tenant_id` 入参、不做跨租户查询。
"""

from typing import cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.api.deps import CurrentTenantDep, UsageSnapshotDep
from bms_msg.errors import MsgErrorCode
from bms_msg.schemas.usage import TenantUsageRow, TenantUsageView, UsageColumnView
from bms_msg.usage.columns import column_payloads, usage_row

__all__ = ["router"]

router = BaseRouter(
    key="msg_admin_tenant_usage",
    prefix="/msg/admin/tenant-usage",
    tags=["msg-admin-tenant-usage"],
)

_QUERY = Depends(require_permission("msg:query"))

_TENANT_ERRORS = [
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""本端点声明的错误码（经租户中间件的入口拒绝；无主题类错误码）。"""


def _row_payload(row: ConcurrentStableDict[str, int | None]) -> TenantUsageRow:
    """把四键行装配成契约行（三项计数由快照保证非空，存储可为空）。

    Args:
        row: `usage_row()` 产出的四键行。

    Returns:
        TenantUsageRow: 契约行。

    Raises:
        ValidationError: 计数项为空（快照实现违背，非业务分支）。
    """
    return TenantUsageRow(
        topic_count=cast("int", row["topic_count"]),
        partition_count=cast("int", row["partition_count"]),
        storage_bytes=row["storage_bytes"],
        delivered_today=cast("int", row["delivered_today"]),
    )


@router.get("", dependencies=[_QUERY], openapi_extra={"x-error-codes": _TENANT_ERRORS})
async def get_tenant_usage(usage: UsageSnapshotDep, tenant: CurrentTenantDep) -> ApiResponse[TenantUsageView]:
    """返回当前租户的消息平台用量（四列元数据 + 一行真实值）。

    Args:
        usage: 用量快照服务（与 05-1 投影端点同一实现）。
        tenant: 当前租户（不接受入参租户，杜绝跨租户读取）。

    Returns:
        ApiResponse[TenantUsageView]: `{columns, items:[row], total:1}`。
    """
    snapshot = await usage.snapshot(int(tenant.tenant_id))
    columns: ConcurrentStableList[UsageColumnView] = ConcurrentStableList(
        UsageColumnView(**dict(item)) for item in column_payloads()
    )
    items: ConcurrentStableList[TenantUsageRow] = ConcurrentStableList()
    items.add(_row_payload(usage_row(snapshot)))
    return ApiResponse.ok(TenantUsageView(columns=columns, items=items, total=len(items)))
