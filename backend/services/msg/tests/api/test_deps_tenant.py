"""管理面「当前租户」统一依赖（msg 任务 `01-1-1`；Kiwi 2411）。

覆盖：依赖从基座中间件注入的上下文取租户（含主键 / 编码 / 状态）、请求体携带的租户字段被忽略、
无上下文与无内部身份（演示兜底）两类 fail-closed 分支、命名空间校验（`350010`）。
"""

import inspect
from collections.abc import AsyncIterator
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.tenant import TenantContext
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request as StarletteRequest
from starlette.types import ASGIApp, Receive, Scope, Send

from bms_msg.api.deps import CurrentTenantDep, get_current_tenant_context
from bms_msg.errors import MsgErrorCode, MsgNamespaceInvalidError, MsgTenantNotFoundError

pytestmark = pytest.mark.kiwi_id(2411)

_TENANT = TenantContext(code="acme", db_key="tenant_acme", name="Acme", tenant_id=7)
"""已解析的租户上下文（雪花主键非空）。"""

_REGISTERED_NAMESPACE = "file"
"""服务目录中已登记启用的模块键（命名空间校验正例）。"""


def _request(tenant: object = _TENANT) -> StarletteRequest:
    """构造带租户请求态的请求对象（模拟基座中间件的注入结果）。

    Args:
        tenant: 请求态中的租户值（可为 None / 无主键上下文）。

    Returns:
        StarletteRequest: 请求对象。
    """
    scope: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/msg/topics",
            "query_string": b"",
            "headers": ConcurrentStableList(),
            "state": ConcurrentStableDict({"tenant": tenant}),
        }
    )
    return StarletteRequest(cast("Scope", scope))


async def test_dependency_returns_tenant_of_middleware() -> None:
    """依赖返回中间件注入的租户：主键 / 编码 / 状态与上下文一致。"""
    current = await get_current_tenant_context(_request())
    assert (current.tenant_id, current.code, current.status) == ("7", "acme", "active")


async def test_dependency_rejects_missing_context() -> None:
    """无租户上下文（豁免路径 / 未装中间件）：按「租户不存在」拒绝，不泄露存在性。"""
    with pytest.raises(MsgTenantNotFoundError) as excinfo:
        await get_current_tenant_context(_request(None))
    assert (excinfo.value.code, excinfo.value.http_status) == (MsgErrorCode.TENANT_NOT_FOUND, 404)


async def test_dependency_rejects_tenant_without_id() -> None:
    """演示兜底租户（无内部身份）不可作为存储边界：拒绝（同上码）。"""
    demo = TenantContext(code="demo", db_key="tenant_demo", name="演示租户")
    with pytest.raises(MsgTenantNotFoundError):
        await get_current_tenant_context(_request(demo))


async def test_namespace_check_on_current_tenant() -> None:
    """命名空间校验：已启用通过、未登记 `350010`、未指定跳过。"""
    current = await get_current_tenant_context(_request())
    current.ensure_namespace(_REGISTERED_NAMESPACE)
    current.ensure_namespace(None)
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        current.ensure_namespace("not-a-module")
    assert (excinfo.value.code, excinfo.value.http_status) == (MsgErrorCode.NAMESPACE_INVALID, 400)


class _TenantStateMiddleware:
    """复刻基座 `TenantMiddleware` 的注入结果：把已解析租户写入请求态。"""

    def __init__(self, app: ASGIApp, *, tenant: object) -> None:
        """初始化。

        Args:
            app: 下游 ASGI 应用。
            tenant: 注入请求态的租户上下文。
        """
        self._app = app
        self._tenant = tenant

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """写入请求态后转发。

        Args:
            scope: ASGI 作用域。
            receive: 接收通道。
            send: 发送通道。
        """
        cast("dict[str, object]", scope.setdefault("state", {}))["tenant"] = self._tenant
        await self._app(scope, receive, send)


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """装配最小端点：只声明依赖，返回取到的租户主键（供验证「请求体租户字段被忽略」）。

    Yields:
        AsyncClient: 绑定最小应用的客户端。
    """
    app = FastAPI()
    app.add_middleware(_TenantStateMiddleware, tenant=_TENANT)  # type: ignore[arg-type]

    @app.post("/api/v1/msg/topics")
    async def _create(tenant: CurrentTenantDep, payload: dict[str, object]) -> dict[str, object]:
        """回显依赖取到的租户主键与请求体（用于比对来源）。"""
        return {"tenant_id": tenant.tenant_id, "body": payload}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


async def test_request_body_tenant_is_ignored(client: AsyncClient) -> None:
    """请求体携带的租户字段被忽略：以解析链结果为准。"""
    response = await client.post("/api/v1/msg/topics", json={"tenant_id": 9, "name": "t"})
    assert response.status_code == 200
    assert response.json() == {"tenant_id": "7", "body": {"tenant_id": 9, "name": "t"}}


def test_dependency_signature_has_no_body_tenant() -> None:
    """依赖签名不含请求体入参：租户只可能来自上下文（防「顺手加个 body 覆盖」）。"""
    assert list(inspect.signature(get_current_tenant_context).parameters) == ["request"]


async def test_dependency_is_reentrant() -> None:
    """依赖不做取数：同一请求重复调用结果一致（只读上下文的可重入性）。"""
    request = _request()
    first = await get_current_tenant_context(request)
    second = await get_current_tenant_context(request)
    assert first.tenant_id == second.tenant_id == "7"
