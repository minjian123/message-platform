"""主题 ACL 管理接口（`/api/v1/msg/admin/acls`；05-3 详细设计 §8）。

- **权限码**：`msg:manage`（含列表；ACL 规则含主体标识，属敏感配置）；
- **写权限**：`PUT` / `DELETE` 另由管理服务判目标主题 `manage` ACL，防权限提升；
- **租户**：唯一出口 `get_current_tenant_context()`；请求体不接收 `tenant`；
- **审计**：显式调用 05-4 `emit_acl_change`（强审计）；基座中间件已排除该路径；
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 错误码反向对账。
"""

from typing import Annotated

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Path, Query

from bms_msg.acl.keys import ACL_ACTIONS, ACL_PRINCIPAL_TYPES, ACL_STATUSES
from bms_msg.acl.service import AclChange, AclRuleSpec, AclRuleView
from bms_msg.api.deps import CurrentTenantDep, TopicAclAdminServiceDep
from bms_msg.errors import MsgErrorCode
from bms_msg.schemas.acl import (
    AclBatchRequest,
    AclBatchResult,
    AclItem,
    AclPage,
    AclRevokeResult,
    AclRuleRequest,
)
from bms_msg.services.topic import MAX_LIST_SIZE

__all__ = ["router"]

_MANAGE = Depends(require_permission("msg:manage"))

_MANAGE_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.ACL_DENIED),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""ACL 管理端点声明的错误码（`x-error-codes`；10001 属契约层，不进产品码声明）。"""

router = BaseRouter(
    key="msg_admin_acls",
    prefix="/msg/admin/acls",
    tags=["msg-admin-acls"],
)

AclIdPath = Annotated[int, Path(description="ACL 规则 ID（雪花）")]
"""ACL 规则 ID 路径参数。"""


def _item(view: AclRuleView) -> AclItem:
    """规则视图 → 响应明细。

    Args:
        view: 规则视图。

    Returns:
        AclItem: 响应明细。
    """
    return AclItem(
        acl_id=int(view.acl_id),
        namespace=view.namespace,
        topic=view.topic,
        principal_type=view.principal_type,
        principal_key=view.principal_key,
        action=view.action,
        effect=view.effect,
        status=view.status,
        updated_at=view.updated_at,
    )


def _spec(item: AclRuleRequest) -> AclRuleSpec:
    """请求项 → 服务规格（pydantic 字段逐项取，避免传入可变模型）。

    Args:
        item: `AclRuleRequest`。

    Returns:
        AclRuleSpec: 服务规格。
    """
    return AclRuleSpec(
        namespace=item.namespace,
        topic=item.topic,
        principal_type=item.principal_type,
        principal_key=item.principal_key,
        action=item.action,
        effect=item.effect,
    )


def _ensure_filter(
    *,
    principal_type: str | None,
    action: str | None,
    status: str | None,
) -> None:
    """校验列表过滤枚举（非法即 10001）。

    Args:
        principal_type: 主体类型过滤。
        action: 动作过滤。
        status: 状态过滤。

    Raises:
        ParamError: 过滤值不在枚举内（10001）。
    """
    if principal_type is not None and principal_type not in ACL_PRINCIPAL_TYPES:
        raise ParamError(f"ACL 主体类型非法：{principal_type}")
    if action is not None and action not in ACL_ACTIONS:
        raise ParamError(f"ACL 动作非法：{action}")
    if status is not None and status not in ACL_STATUSES:
        raise ParamError(f"ACL 状态非法：{status}")


@router.get("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def list_acls(
    tenant: CurrentTenantDep,
    acls: TopicAclAdminServiceDep,
    namespace: Annotated[str | None, Query(max_length=64, description="命名空间过滤")] = None,
    topic: Annotated[str | None, Query(max_length=200, description="主题名过滤")] = None,
    principal_type: Annotated[str | None, Query(max_length=16, description="主体类型过滤（user / role）")] = None,
    principal_key: Annotated[str | None, Query(max_length=64, description="主体标识过滤")] = None,
    action: Annotated[str | None, Query(max_length=16, description="动作过滤")] = None,
    status: Annotated[str | None, Query(max_length=16, description="状态过滤（enabled / disabled）")] = None,
    page: Annotated[int, Query(ge=1, description="页码（从 1 起）")] = 1,
    size: Annotated[int, Query(ge=1, le=MAX_LIST_SIZE, description="每页条数")] = 20,
) -> ApiResponse[AclPage]:
    """ACL 规则列表（分页 + 命名空间 / 主题 / 主体 / 动作 / 状态过滤）。

    需要 msg:manage 权限；按租户隔离；命名空间过滤值非法 350010，过滤枚举非法 10001。
    """
    _ensure_filter(principal_type=principal_type, action=action, status=status)
    rows, total = await acls.list_rules(
        tenant=tenant,
        namespace=namespace,
        topic=topic,
        principal_type=principal_type,
        principal_key=principal_key,
        action=action,
        status=status,
        page=page,
        size=size,
    )
    return ApiResponse.ok(
        AclPage(
            items=ConcurrentStableList(_item(AclRuleView.from_row(row)) for row in rows),
            total=total,
            page=page,
            size=size,
        )
    )


@router.put("", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def upsert_acls(
    req: AclBatchRequest,
    tenant: CurrentTenantDep,
    acls: TopicAclAdminServiceDep,
) -> ApiResponse[AclBatchResult]:
    """批量授予 / 更新 ACL（按唯一键 upsert；整批校验、整批成功 / 失败）。

    需要 msg:manage 权限 + 目标主题 manage ACL；主题不存在 350001，命名空间非法 350010，
    ACL 拒绝 350027，参数非法 10001。新建 / 恢复记 grant，改 effect 记 update，幂等不变更不写审计。
    """
    specs: ConcurrentStableList[AclRuleSpec] = ConcurrentStableList(_spec(item) for item in req.items)
    changes: ConcurrentStableList[AclChange] = await acls.upsert(tenant=tenant, specs=specs)
    return ApiResponse.ok(
        AclBatchResult(
            items=ConcurrentStableList(_item(change.view) for change in changes),
            changed=ConcurrentStableList(change.view.acl_id for change in changes if change.changed),
        )
    )


@router.delete("/{acl_id}", dependencies=[_MANAGE], openapi_extra={"x-error-codes": _MANAGE_ERRORS})
async def revoke_acl(
    acl_id: AclIdPath,
    tenant: CurrentTenantDep,
    acls: TopicAclAdminServiceDep,
) -> ApiResponse[AclRevokeResult]:
    """收回一条 ACL（置 `status=disabled`；幂等）。

    需要 msg:manage 权限 + 目标主题 manage ACL；规则不存在 / 不属于当前租户返回 `changed=false`，
    ACL 拒绝 350027。
    """
    change = await acls.revoke(tenant=tenant, acl_id=acl_id)
    return ApiResponse.ok(AclRevokeResult(acl_id=int(acl_id), changed=change.changed if change is not None else False))
