"""数据面租户解析（msg 任务 `01-1-1`；Kiwi 2411）。

数据面 = HTTP/2 帧入口（不经过 ASGI）。口径：复用基座 `resolve_request_tenant()`，
**只信已校验令牌里的租户位**，不采信客户端自报的 Host / `X-Tenant-ID`；与管理面解析出同一个租户；
停用 / 不存在映射为 msg 产品码，源不可达原样冒泡。
"""

import inspect
import json
import types
from typing import cast

import pytest
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from bms_core.db.tenant import TenantContext
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import CurrentTenantDep
from bms_msg.errors import MsgErrorCode, MsgTenantNotFoundError, MsgTenantSuspendedError
from bms_msg.tenant.resolve import frame_tenant

pytestmark = pytest.mark.kiwi_id(2411)

_TENANT_ID = 7
"""测试用租户主键（雪花 id；数据面令牌里的租户位）。"""


class _Source:
    """租户源替身：记录调用来源，按编码 / 主键给出不同状态。"""

    def __init__(self, *, suspended: bool = False) -> None:
        """初始化。

        Args:
            suspended: 是否模拟停用。
        """
        self.suspended = suspended
        self.calls: ConcurrentStableList[str] = ConcurrentStableList()
        self.readonly_calls: ConcurrentStableList[str] = ConcurrentStableList()
        """`readonly=True` 的调用记录（管理面只读通道，bms `04_03-01`）。"""

    async def by_code(self, code: str, *, readonly: bool = False) -> TenantContext:
        """按编码取上下文（`missing` 恒不存在）。"""
        self.calls.add(f"code:{code}")
        if readonly:
            self.readonly_calls.add(f"code:{code}")
        if code == "missing":
            raise TenantNotFoundError("未知租户")
        if self.suspended:
            raise TenantSuspendedError("租户已停用")
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=_TENANT_ID)

    async def by_domain(self, domain: str, *, readonly: bool = False) -> TenantContext:
        """按子域名取上下文（数据面不采信 Host，本方法不应被调用）。"""
        self.calls.add(f"domain:{domain}")
        if readonly:
            self.readonly_calls.add(f"domain:{domain}")
        raise TenantNotFoundError(f"未知租户域名：{domain}")

    async def by_id(self, tenant_id: str, *, readonly: bool = False) -> TenantContext:
        """按主键取上下文。"""
        self.calls.add(f"id:{tenant_id}")
        if readonly:
            self.readonly_calls.add(f"id:{tenant_id}")
        if self.suspended:
            raise TenantSuspendedError("租户已停用")
        return TenantContext(code="acme", db_key="tenant_acme", name="Acme", tenant_id=int(tenant_id))

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（数据面不使用）。"""
        return None


def _app(source: _Source) -> FastAPI:
    """装配最小应用：基座租户中间件 + 「当前租户」依赖端点（代表管理面）。

    Args:
        source: 租户源替身。

    Returns:
        FastAPI: 最小应用。
    """
    app = FastAPI()
    app.state.settings = types.SimpleNamespace(
        tenant=types.SimpleNamespace(
            exempt_paths=ConcurrentStableList(),
            deferred_paths=ConcurrentStableList(),
            allow_demo_fallback=False,
        ),
        error_mapper=types.SimpleNamespace(provider=""),
        # 基座 04_03-01 只读放行口：白名单空即全拒（本用例走原口径，非只读窗口）。
        tenant_guard=types.SimpleNamespace(readonly_routes=ConcurrentStableList()),
    )
    app.state.tenant_source = source
    app.add_middleware(TenantMiddleware)

    @app.get("/api/v1/msg/topics", response_model=None)
    async def _list(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """回显管理面解析到的租户主键。"""
        return ConcurrentStableDict({"tenant_id": tenant.tenant_id})

    return app


async def _management_tenant_id(source: _Source, code: str) -> str:
    """经管理面（基座中间件 + 统一依赖）取租户主键。

    Args:
        source: 租户源替身。
        code: `X-Tenant-ID` 携带的租户编码。

    Returns:
        str: 租户主键。
    """
    transport = ASGITransport(app=_app(source))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response: Response = await client.get("/api/v1/msg/topics", headers={"X-Tenant-ID": code})
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))
    return str(body["tenant_id"])


async def test_frame_matches_management_tenant() -> None:
    """同一令牌租户位：数据面与管理面解析出同一个 `tenant_id`。"""
    source = _Source()
    management = await _management_tenant_id(source, "acme")
    frame = await frame_tenant(token_tenant_id=str(_TENANT_ID), source=source)
    assert management == str(frame.tenant_id) == str(_TENANT_ID)


async def test_frame_only_trusts_token_tenant() -> None:
    """数据面只认令牌：接口不暴露 Host / `X-Tenant-ID` 入参，且只按主键取数。"""
    parameters = inspect.signature(frame_tenant).parameters
    assert "host" not in parameters
    assert "header" not in parameters
    source = _Source()
    await frame_tenant(token_tenant_id=str(_TENANT_ID), source=source)
    assert list(source.calls) == [f"id:{_TENANT_ID}"]


async def test_frame_without_token_rejected() -> None:
    """无令牌（且无开发态显式租户）：按不存在拒绝，不回落演示租户。"""
    with pytest.raises(MsgTenantNotFoundError) as excinfo:
        await frame_tenant(token_tenant_id=None, source=_Source())
    assert excinfo.value.code == MsgErrorCode.TENANT_NOT_FOUND


async def test_frame_with_malformed_token_rejected() -> None:
    """令牌租户位形态非法：等同未提供来源（不送租户源）。"""
    source = _Source()
    with pytest.raises(MsgTenantNotFoundError):
        await frame_tenant(token_tenant_id="not-a-snowflake", source=source)
    assert len(source.calls) == 0


async def test_frame_suspended_maps_product_code() -> None:
    """停用租户：`350011`（基座 80002 映射），不透传基座文案。"""
    with pytest.raises(MsgTenantSuspendedError) as excinfo:
        await frame_tenant(token_tenant_id=str(_TENANT_ID), source=_Source(suspended=True))
    assert (excinfo.value.code, excinfo.value.message) == (MsgErrorCode.TENANT_SUSPENDED, None)


async def test_frame_dev_override_resolves_by_code() -> None:
    """开发态显式租户：无令牌时按配置的租户编码解析（仅 dev 生效，prod 由基座启动校验禁止）。"""
    source = _Source()
    context = await frame_tenant(token_tenant_id=None, source=source, dev_override_tenant="acme")
    assert context.tenant_id == _TENANT_ID
    assert list(source.calls) == ["code:acme"]


async def test_frame_dev_override_without_source_rejected() -> None:
    """开发态显式租户但租户源未装配：按不存在拒绝（不静默构造上下文）。"""
    with pytest.raises(MsgTenantNotFoundError):
        await frame_tenant(token_tenant_id=None, source=None, dev_override_tenant="acme")
