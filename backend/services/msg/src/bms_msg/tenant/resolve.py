"""租户解析：两个入口、一套解析（管理面读中间件结果，数据面复用基座解析函数）。

- **管理面 / RPC**：基座 `TenantMiddleware` 已按解析链（子域名 → `X-Tenant-ID` → 令牌租户位）解析并
  校验状态，本层**只读结果**，不重复解析、不再回源（详细设计 §4.2）。
- **数据面**（HTTP/2 帧，不经过 ASGI）：帧入口调用 `frame_tenant()`，内部复用基座
  `resolve_request_tenant()`（**纯 async 函数、不依赖 ASGI**），来源取值与管理面同源；
  **不采信客户端自报的 Host 与 `X-Tenant-ID`**——只认已校验令牌里的租户位，避免「自报头即换租户」。
- **错误码**：停用 → `350011`、不存在（含无任何来源）→ `350012`；租户源不可达原样冒泡
  `ServiceUnavailableError`（`10007` / 503，不静默放行）。
"""

from typing import cast

from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from bms_core.db.tenant import TenantContext, TenantLookup, resolve_request_tenant
from fastapi import Request

from bms_msg.errors import MsgTenantNotFoundError, MsgTenantSuspendedError

__all__ = [
    "FRAME_PATH",
    "frame_tenant",
    "management_tenant",
]

FRAME_PATH = "/broker/frame"
"""数据面帧入口的伪路径：仅供基座解析链做路径豁免判定（帧入口不豁免、不延迟解析）。"""


def management_tenant(request: Request) -> TenantContext:
    """取管理面当前租户（只读基座中间件注入的上下文，fail-closed）。

    Args:
        request: 当前请求（`scope["state"]["tenant"]` 由 `TenantMiddleware` 写入）。

    Returns:
        TenantContext: 租户上下文（`tenant_id` 必非空）。

    Raises:
        MsgTenantNotFoundError: 未注入上下文（豁免路径 / 未装配中间件），或上下文无内部身份
            （演示兜底租户）。两者与「租户不存在」同码，不泄露租户是否存在。
    """
    state = cast("dict[str, object]", request.scope.get("state") or {})
    tenant = state.get("tenant")
    if isinstance(tenant, TenantContext) and tenant.tenant_id is not None:
        return tenant
    raise MsgTenantNotFoundError("请求未携带可用的租户上下文（缺租户标识，或租户无内部身份）")


async def frame_tenant(
    *,
    token_tenant_id: str | None,
    source: TenantLookup | None,
    dev_override_tenant: str = "",
) -> TenantContext:
    """数据面解析租户（只信令牌租户位；dev 无令牌时按配置显式指定）。

    Args:
        token_tenant_id: 已校验令牌中的租户主键（雪花 id 字符串）；客户端自报头与 Host 一律不收。
        source: 租户源（应用装配的 `app.state.tenant_source`）。
        dev_override_tenant: 开发态显式租户**编码**（`[tenant].dev_override_tenant`；生产由基座
            启动校验禁止非空，故此处无需再判环境）。

    Returns:
        TenantContext: 租户上下文。

    Raises:
        MsgTenantSuspendedError: 租户已停用（`350011`）。
        MsgTenantNotFoundError: 租户不存在 / 未提供任何来源（`350012`）。
        ServiceUnavailableError: 租户源不可达（`10007` / 503，不静默放行）。
    """
    try:
        if not token_tenant_id and dev_override_tenant:
            return await _by_code(source, dev_override_tenant)
        # 帧入口路径非豁免、非延迟解析，且不启用演示兜底：解析链必出上下文或抛错（故直接转型）。
        return cast(
            "TenantContext",
            await resolve_request_tenant(
                path=FRAME_PATH,
                token_tenant_id=token_tenant_id,
                source=source,
                exempt_paths=(),
                deferred_paths=(),
                allow_demo_fallback=False,
            ),
        )
    except TenantSuspendedError as exc:
        # 不透传基座文案（可能含租户编码 / 内部原因）：对外只给产品码与文案键。
        raise MsgTenantSuspendedError() from exc
    except TenantNotFoundError as exc:
        raise MsgTenantNotFoundError() from exc


async def _by_code(source: TenantLookup | None, code: str) -> TenantContext:
    """按租户编码取上下文（开发态显式租户用；无租户源即按不存在处理）。

    Args:
        source: 租户源。
        code: 租户编码。

    Returns:
        TenantContext: 租户上下文。

    Raises:
        TenantNotFoundError: 租户源未装配（开发态直连且无租户源）。
    """
    if source is None:
        raise TenantNotFoundError(f"缺少租户源，无法按开发态租户编码解析：{code}")
    return await source.by_code(code)
