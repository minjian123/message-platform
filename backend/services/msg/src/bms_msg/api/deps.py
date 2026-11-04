"""msg 依赖注入出口：**「当前租户」唯一依赖**（详细设计 §3.3）。

后续模块（主题 / 分区 / 配额 / 管理台）一律 `Depends(get_current_tenant_context)` 取租户，
**不得再写解析代码**（同一仓内第二套解析即违规，由代码检索与评审把关）。

口径：
- 只读基座中间件注入的上下文，不重复解析、不再回源（状态校验在中间件完成，见 §3.1 / §4.2）；
- 上下文缺失或租户无内部身份 → `350012`（fail-closed，不泄露租户是否存在）；
- 命名空间校验用 `CurrentTenant.ensure_namespace()`，与消费守卫共用同一判定（`350010`）。
"""

from typing import Annotated

from fastapi import Depends, Request

from bms_msg.tenant.context import CurrentTenant
from bms_msg.tenant.resolve import management_tenant

__all__ = ["CurrentTenantDep", "get_current_tenant_context"]


async def get_current_tenant_context(request: Request) -> CurrentTenant:
    """取当前租户（唯一出口）。

    Args:
        request: 当前请求（基座中间件已注入 `scope["state"]["tenant"]`）。

    Returns:
        CurrentTenant: 当前租户（含主键与可用命名空间）。

    Raises:
        MsgTenantNotFoundError: 未携带可用租户上下文（`350012`）。
    """
    return CurrentTenant.from_context(management_tenant(request))


CurrentTenantDep = Annotated[CurrentTenant, Depends(get_current_tenant_context)]
"""路由参数写法：`tenant: CurrentTenantDep`。"""
