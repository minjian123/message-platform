"""消息平台用量 / 配额端点（05-1 用量、05-2 配额）。

三类物理端点：

- `GET /api/v1/msg/tenant-usage`（前端 / `kind=form`）：四列元数据 + 当前租户一行真实用量；
- `GET /api/v1/msg/tenant-usage/quota[/bytes|/rate]`（平台 / `kind=quota`）：按单位拆分的
  配额投影，返回 `{"items": [...]}`（不套 `ApiResponse`，`QuotaProjectionReader` 按 `items` 解析）；
- `GET|PUT /api/v1/msg/admin/quotas`（管理台）：六维查询与批量调整（`msg:query` / `msg:manage`）。

**单一事实源**：限额在 `msg_quota`，用量由 `usage_snapshot()` 实时计算；平台只读、不双写。
"""

from __future__ import annotations

from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.session import get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query
from fastapi.responses import JSONResponse

from bms_msg.api.deps import (
    AuditSinkDep,
    CurrentTenantDep,
    QuotaServiceDep,
    UsageSnapshotDep,
)
from bms_msg.audit.trail import QUOTA_UPDATE_ACTION, AuditEmitter, emit_sensitive
from bms_msg.core.config import (
    QUOTA_UNIT_BYTES,
    QUOTA_UNIT_COUNT,
    QUOTA_UNIT_QPS,
)
from bms_msg.errors import MsgErrorCode
from bms_msg.quota.service import QuotaAssessment, QuotaService, QuotaUpdate
from bms_msg.schemas.quota import QuotaItem, QuotaUpdateRequest, QuotaView
from bms_msg.usage.columns import USAGE_COLUMNS, column_payloads

__all__ = ["admin_router", "router"]

router = BaseRouter(
    key="msg_tenant_usage",
    prefix="/msg/tenant-usage",
    tags=["msg-tenant-usage"],
)

admin_router = BaseRouter(
    key="msg_admin_quotas",
    prefix="/msg/admin/quotas",
    tags=["msg-admin-quotas"],
)

_QUERY = Depends(require_permission("msg:query"))
_MANAGE = Depends(require_permission("msg:manage"))

_TENANT_ERRORS = [
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""投影端点声明的错误码（经租户中间件的入口拒绝；`x-error-codes` 反向对账用）。"""

_ADMIN_QUERY_ERRORS = [
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""管理面查询端点声明的错误码。"""

_ADMIN_UPDATE_ERRORS = [
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""管理面调整端点声明的错误码（参数错误 `10001` 不属产品码，不登记）。"""

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（元数据表归平台库）。"""


def _usage_row(snapshot: object) -> ConcurrentStableDict[str, int | None]:
    """把用量快照装配成表单行（列键与 `USAGE_COLUMNS` 闭合）。

    Args:
        snapshot: 用量快照（`UsageSnapshot`）。

    Returns:
        dict[str, int | None]: 行值。
    """
    values: ConcurrentStableDict[str, int | None] = ConcurrentStableDict()
    for column in USAGE_COLUMNS:
        values[column.key] = cast("int | None", getattr(snapshot, column.key, None))
    return values


@router.get("", openapi_extra={"x-error-codes": _TENANT_ERRORS})
async def tenant_usage(usage: UsageSnapshotDep, tenant: CurrentTenantDep) -> JSONResponse:
    """返回当前租户的消息平台用量（四列 + 一行真实值）。

    Args:
        usage: 用量快照服务。
        tenant: 当前租户。

    Returns:
        JSONResponse: `{"columns": [...], "items": [row], "total": 1}`。
    """
    snapshot = await usage.snapshot(int(tenant.tenant_id))
    columns = [dict(item) for item in column_payloads()]
    row = dict(_usage_row(snapshot))
    return JSONResponse(content={"columns": columns, "items": [row], "total": 1})


def _projection_payload(
    assessments: ConcurrentStableList[QuotaAssessment], *, unit: str
) -> ConcurrentStableList[ConcurrentStableDict[str, object]]:
    """把判定视图收窄为平台投影条目（按单位过滤）。

    Args:
        assessments: 六维判定。
        unit: 目标单位（count / bytes / qps）。

    Returns:
        list[dict[str, object]]: `[{quota_key, usage, limit, soft_limit, hard_limit, updated_at}]`。
    """
    items: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    for assessment in assessments:
        limit = assessment.limit
        if limit.unit != unit:
            continue
        items.add(
            ConcurrentStableDict(
                {
                    "quota_key": limit.key,
                    "usage": assessment.usage,
                    "limit": limit.hard_limit,
                    "soft_limit": limit.soft_limit_or_none,
                    "hard_limit": limit.hard_limit,
                    "updated_at": None if limit.updated_at is None else limit.updated_at.isoformat(),
                }
            )
        )
    return items


async def _projection(
    quota: QuotaService,
    *,
    tenant_id: int,
    unit: str,
) -> JSONResponse:
    """取某一单位的配额投影。

    Args:
        quota: 配额服务。
        tenant_id: 租户主键。
        unit: 目标单位。

    Returns:
        JSONResponse: `{"items": [...]}`。
    """
    assessments = await quota.assess_all(tenant_id)
    items = [dict(item) for item in _projection_payload(assessments, unit=unit)]
    return JSONResponse(content={"items": items})


@router.get("/quota", openapi_extra={"x-error-codes": _TENANT_ERRORS})
async def quota_projection(
    quota: QuotaServiceDep,
    tenant: CurrentTenantDep,
    tenant_id: int | None = Query(default=None, description="平台投影读取器传入的租户主键（缺省取上下文）"),
) -> JSONResponse:
    """返回条数类配额投影（topic / group / unacked / connection）。

    Args:
        quota: 配额服务。
        tenant: 当前租户。
        tenant_id: 查询参数租户主键（与上下文一致性由网关保证）。

    Returns:
        JSONResponse: `{"items": [...]}`。
    """
    return await _projection(quota, tenant_id=tenant_id or int(tenant.tenant_id), unit=QUOTA_UNIT_COUNT)


@router.get("/quota/bytes", openapi_extra={"x-error-codes": _TENANT_ERRORS})
async def quota_projection_bytes(
    quota: QuotaServiceDep,
    tenant: CurrentTenantDep,
    tenant_id: int | None = Query(default=None, description="平台投影读取器传入的租户主键（缺省取上下文）"),
) -> JSONResponse:
    """返回存储配额投影（storage_bytes）。

    Args:
        quota: 配额服务。
        tenant: 当前租户。
        tenant_id: 查询参数租户主键。

    Returns:
        JSONResponse: `{"items": [...]}`。
    """
    return await _projection(quota, tenant_id=tenant_id or int(tenant.tenant_id), unit=QUOTA_UNIT_BYTES)


@router.get("/quota/rate", openapi_extra={"x-error-codes": _TENANT_ERRORS})
async def quota_projection_rate(
    quota: QuotaServiceDep,
    tenant: CurrentTenantDep,
    tenant_id: int | None = Query(default=None, description="平台投影读取器传入的租户主键（缺省取上下文）"),
) -> JSONResponse:
    """返回生产速率配额投影（produce_rate）。

    Args:
        quota: 配额服务。
        tenant: 当前租户。
        tenant_id: 查询参数租户主键。

    Returns:
        JSONResponse: `{"items": [...]}`。
    """
    return await _projection(quota, tenant_id=tenant_id or int(tenant.tenant_id), unit=QUOTA_UNIT_QPS)


def _quota_item(assessment: QuotaAssessment) -> QuotaItem:
    """判定视图 → 管理面条目。

    Args:
        assessment: 判定视图。

    Returns:
        QuotaItem: 管理面条目。
    """
    limit = assessment.limit
    return QuotaItem(
        quota_key=limit.key,
        namespace=limit.namespace,
        limit=limit.hard_limit,
        soft_limit=limit.soft_limit_or_none,
        hard_limit=limit.hard_limit,
        unit=limit.unit,
        period=limit.period,
        usage=assessment.usage,
        state=assessment.state,
        source=assessment.source,
        updated_at=limit.updated_at,
    )


async def _view(quota: QuotaService, *, tenant_id: int, namespace: str) -> QuotaView:
    """取六维管理面视图。

    Args:
        quota: 配额服务。
        tenant_id: 租户主键。
        namespace: 作用域（空串 = 租户级）。

    Returns:
        QuotaView: 视图。
    """
    assessments = await quota.assess_all(tenant_id, namespace=namespace)
    return QuotaView(items=ConcurrentStableList(_quota_item(item) for item in assessments))


@admin_router.get(
    "",
    dependencies=[_QUERY],
    openapi_extra={"x-error-codes": _ADMIN_QUERY_ERRORS},
)
async def get_quotas(
    tenant: CurrentTenantDep,
    quota: QuotaServiceDep,
    namespace: str = Query(default="", max_length=64, description="作用域（空串 = 租户级）"),
) -> ApiResponse[QuotaView]:
    """查询六维配额（含缺省值 / 当前用量 / 状态）。

    Args:
        tenant: 当前租户。
        quota: 配额服务。
        namespace: 作用域。

    Returns:
        ApiResponse[QuotaView]: 六维视图。
    """
    if namespace:
        tenant.ensure_namespace(namespace)
    return ApiResponse.ok(await _view(quota, tenant_id=int(tenant.tenant_id), namespace=namespace))


def _update_payload(request: QuotaUpdateRequest) -> ConcurrentStableList[QuotaUpdate]:
    """契约请求 → 服务调整项（保持插入序）。

    Args:
        request: 批量调整请求。

    Returns:
        list[QuotaUpdate]: 调整项。
    """
    return ConcurrentStableList(
        QuotaUpdate(
            quota_key=item.quota_key,
            namespace=item.namespace,
            limit=item.limit,
            soft_limit=item.soft_limit,
        )
        for item in request.items
    )


async def _audit_update(
    sink: AuditEmitter,
    *,
    tenant_id: int,
    payload: ConcurrentStableList[QuotaUpdate],
) -> None:
    """写一条配额调整审计（配置变更，强审计；失败即调整失败）。

    Args:
        sink: 审计出口。
        tenant_id: 租户主键。
        payload: 调整项。

    Raises:
        InternalError: 强审计写入失败（10000 / 500）。
    """
    params = {
        "items": [
            {
                "quota_key": item.quota_key,
                "namespace": item.namespace,
                "limit": item.limit,
                "soft_limit": item.soft_limit,
            }
            for item in payload
        ]
    }
    await emit_sensitive(
        sink,
        action=QUOTA_UPDATE_ACTION,
        tenant_id=int(tenant_id),
        params=params,
        method="PUT",
        path="/api/v1/msg/admin/quotas",
    )


@admin_router.put(
    "",
    dependencies=[_MANAGE],
    openapi_extra={"x-error-codes": _ADMIN_UPDATE_ERRORS},
)
async def update_quotas(
    request: QuotaUpdateRequest,
    tenant: CurrentTenantDep,
    quota: QuotaServiceDep,
    uow: PlatformUowDep,
    sink: AuditSinkDep,
) -> ApiResponse[QuotaView]:
    """批量调整配额（整批校验、整批失败；提交后即时生效并写审计）。

    Args:
        request: 批量调整请求。
        tenant: 当前租户。
        quota: 配额服务。
        uow: 平台库工作单元。
        sink: 审计出口。

    Returns:
        ApiResponse[QuotaView]: 调整后的六维视图。

    Raises:
        ParamError: 未知维度 / 重复作用域 / 软限非法（10001）。
    """
    tenant_id = int(tenant.tenant_id)
    payload = _update_payload(request)
    for item in payload:
        if item.namespace:
            tenant.ensure_namespace(item.namespace)
    try:
        async with uow.begin():
            await quota.update(tenant_id, payload)
    except (KeyError, ValueError) as exc:
        raise ParamError(f"配额调整非法：{exc}") from exc
    quota.cache.invalidate(tenant_id)
    await _audit_update(sink, tenant_id=tenant_id, payload=payload)
    namespace = payload[0].namespace if payload else ""
    return ApiResponse.ok(await _view(quota, tenant_id=tenant_id, namespace=namespace))
