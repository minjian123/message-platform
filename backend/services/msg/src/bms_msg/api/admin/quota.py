"""消息平台用量 / 配额只读投影端点（05-1；07D 任务 03 附录 C）。

两条物理端点、各司其职，共用一个取数口径：

- `GET /api/v1/msg/tenant-usage`（前端 / `kind=form`）：返回四列元数据 + 空数据
  `{columns, items, total}`；列由 `bms_msg.usage.columns` 派生，与种子写入的 `sys_field`
  同源。
- `GET /api/v1/msg/tenant-usage/quota`（平台 / `kind=quota`）：返回配额投影契约形状
  `{"items": [...]}`，不套 `ApiResponse` 包装（`QuotaProjectionReader` 按响应体解析 `items`）。

本期 `msg_quota` 与真实用量尚未落地，两条端点都返回空清单：平台对未提供的配额维度判
`default`，不写 0、不臆造用量、不双写；真实数据随 05-2 接入，届时只改取数来源，声明与
平台侧读取器零改动。
"""

from bms_core.api.base import BaseRouter
from fastapi.responses import JSONResponse

from bms_msg.errors import MsgErrorCode
from bms_msg.usage.columns import column_payloads

__all__ = ["router"]

router = BaseRouter(
    key="msg_tenant_usage",
    prefix="/msg/tenant-usage",
    tags=["msg-tenant-usage"],
)

_ERRORS = [int(MsgErrorCode.TENANT_SUSPENDED), int(MsgErrorCode.TENANT_NOT_FOUND)]
"""端点声明的错误码（经租户中间件的入口拒绝；`x-error-codes` 反向对账用）。"""


@router.get("", openapi_extra={"x-error-codes": _ERRORS})
async def tenant_usage() -> JSONResponse:
    """返回消息平台用量板块（本期四列元数据 + 空数据）。

    Returns:
        JSONResponse: `{"columns": [{key, label}...], "items": [], "total": 0}`。
    """
    columns = [dict(item) for item in column_payloads()]
    return JSONResponse(content={"columns": columns, "items": [], "total": 0})


@router.get("/quota", openapi_extra={"x-error-codes": _ERRORS})
async def quota_projection() -> JSONResponse:
    """返回产品配额投影条目（本期空清单）。

    Returns:
        JSONResponse: `{"items": []}`，结构合配额投影读取器契约。
    """
    return JSONResponse(content={"items": []})
