"""产品配额只读投影端点（07D 任务 03 附录 C）。

平台侧配额投影读取器经 `[[data_sources]] kind=quota` 声明的 `path` 实时只读取数；
端点直接返回契约形状 `{"items": [...]}`，不套 `ApiResponse` 包装（读取器按响应体解析 `items`）。

本期 msg 侧配额表（`msg_quota`）与 05-1 用量板块尚未落地，返回空清单：四个声明维度
由基座判为 `default`，不写 0、不臆造用量、不双写。真实用量随产品配额实现接入，
届时只改本端点的取数来源，声明与平台侧读取器零改动。
"""

from bms_core.api.base import BaseRouter
from fastapi.responses import JSONResponse

from bms_msg.errors import MsgErrorCode

__all__ = ["router"]

router = BaseRouter(
    key="msg_quota_projection",
    prefix="/msg/tenant-usage",
    tags=["msg-quota-projection"],
)

_ERRORS = [int(MsgErrorCode.TENANT_SUSPENDED), int(MsgErrorCode.TENANT_NOT_FOUND)]
"""端点声明的错误码（经租户中间件的入口拒绝；`x-error-codes` 反向对账用）。"""


@router.get("", openapi_extra={"x-error-codes": _ERRORS})
async def quota_projection() -> JSONResponse:
    """返回产品配额投影条目（本期空清单）。

    Returns:
        JSONResponse: `{"items": []}`，结构合配额投影读取器契约。
    """
    return JSONResponse(content={"items": []})
