"""租户状态三态与产品码映射（msg 任务 `01-1-1`；Kiwi 2411）。

走**基座真实中间件**（`TenantMiddleware`，解析链 + 状态判定 + 就地拒绝）与**基座统一渲染单点**
（`build_error_response`），验证 msg 产品码覆盖写入后的对外口径：启用放行、停用 `350011` / 403、
不存在 `350012` / 404（不泄露存在性）、停用即时生效（同一应用实例、无需重启）。
"""

import json
import types
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.db.tenant import TenantContext
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import CurrentTenantDep
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, MsgErrorMapper

pytestmark = pytest.mark.kiwi_id(2411)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源）。"""

_TENANT_ID = 7
"""测试用租户主键（雪花 id）。"""


class _TenantSource:
    """租户源替身：按编码区分「不存在」，并可切换停用状态（验证即时生效）。"""

    def __init__(self) -> None:
        """初始化（缺省启用）。"""
        self.suspended = False

    async def by_code(self, code: str) -> TenantContext:
        """按编码取上下文（`missing` 恒不存在）。"""
        if code == "missing":
            raise TenantNotFoundError("未知租户")
        if self.suspended:
            raise TenantSuspendedError(f"租户已停用：{code}")
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=_TENANT_ID)

    async def by_domain(self, domain: str) -> TenantContext:
        """按子域名取上下文（测试不走该来源）。"""
        raise TenantNotFoundError(f"未知租户域名：{domain}")

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取上下文（测试不走该来源）。"""
        raise TenantNotFoundError(f"未知租户：{tenant_id}")

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（测试不使用）。"""
        return None


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明并登记进合并视图（用例后复位）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture(autouse=True)
def _mapper_registered() -> Iterator[None]:
    """在默认注册表登记 msg 产品码覆盖（与 `configure_plugins()` 同一入口）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    try:
        register_plugin("error_mapper", MSG_ERROR_MAPPER, MsgErrorMapper)
        yield
    finally:
        registry.reset()


def _app(source: _TenantSource) -> FastAPI:
    """装配最小应用：基座租户中间件 + 「当前租户」依赖端点。

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
        error_mapper=types.SimpleNamespace(provider=MSG_ERROR_MAPPER),
    )
    app.state.tenant_source = source
    app.add_middleware(TenantMiddleware)

    @app.get("/api/v1/msg/topics", response_model=None)
    async def _list(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """回显依赖取到的租户主键（隔离校验通过即正常返回）。"""
        return ConcurrentStableDict({"tenant_id": tenant.tenant_id})

    return app


async def _get(source: _TenantSource, *, code: str | None) -> Response:
    """按租户编码发一次请求（`X-Tenant-ID` 为基座解析链的请求头来源）。

    Args:
        source: 租户源替身。
        code: 请求头携带的租户编码（`None` 表示不带来源）。

    Returns:
        Response: 响应。
    """
    headers = {} if code is None else {"X-Tenant-ID": code}
    transport = ASGITransport(app=_app(source))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get("/api/v1/msg/topics", headers=headers)


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 响应 JSON。
    """
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


async def test_active_tenant_allowed() -> None:
    """启用租户：解析通过并按解析结果取到租户主键。"""
    response = await _get(_TenantSource(), code="acme")
    assert (response.status_code, _body(response)["tenant_id"]) == (200, str(_TENANT_ID))


async def test_suspended_tenant_returns_product_code() -> None:
    """停用租户：基座 80002 经产品码覆盖为 `350011` / 403（声明状态优先）。"""
    source = _TenantSource()
    source.suspended = True
    response = await _get(source, code="acme")
    assert (response.status_code, _body(response)["code"]) == (403, int(MsgErrorCode.TENANT_SUSPENDED))


async def test_unknown_tenant_returns_product_code() -> None:
    """不存在的租户：`350012` / 404，文案不泄露租户是否存在。"""
    response = await _get(_TenantSource(), code="missing")
    body = _body(response)
    assert (response.status_code, body["code"]) == (404, int(MsgErrorCode.TENANT_NOT_FOUND))
    assert body["message"] == "msg.error.tenant_not_found"


async def test_missing_source_returns_not_found() -> None:
    """未提供任何租户来源（生产不回落演示租户）：同样按不存在拒绝，不泄露。"""
    response = await _get(_TenantSource(), code=None)
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TENANT_NOT_FOUND))


async def test_status_change_takes_effect_immediately() -> None:
    """停用即时生效：同一应用实例上，状态切换后下一次请求即被拒绝（无需重启）。"""
    source = _TenantSource()
    transport = ASGITransport(app=_app(source))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.get("/api/v1/msg/topics", headers={"X-Tenant-ID": "acme"})
        source.suspended = True
        second = await client.get("/api/v1/msg/topics", headers={"X-Tenant-ID": "acme"})
    assert (first.status_code, second.status_code) == (200, 403)
    assert _body(second)["code"] == int(MsgErrorCode.TENANT_SUSPENDED)
