"""租户源不可达的降级口径（msg 任务 `01-1-1`；Kiwi 2411）。

口径（详细设计 §3.2）：租户源不可达**不静默放行**——管理面按基座策略返回 `10007` / 503
（不冒泡 5xx 未处理错误、不改写成「租户不存在」），数据面原样冒泡 `ServiceUnavailableError`，
由帧服务的连接级响应收口。
"""

import json
import types
from collections.abc import Iterator
from typing import cast

import pytest
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ServiceUnavailableError
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.tenant import TenantContext
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import CurrentTenantDep
from bms_msg.tenant.resolve import frame_tenant

pytestmark = pytest.mark.kiwi_id(2411)

_SERVICE_UNAVAILABLE = 10007
"""基座通用段码：下游服务不可用（503）。"""


class _UnavailableSource:
    """租户源替身：任何取数都不可达（模拟租户服务宕机 / 契约超时）。"""

    async def by_code(self, code: str) -> TenantContext:
        """按编码取上下文（恒不可达）。"""
        raise ServiceUnavailableError(f"租户注册契约不可达：{code}")

    async def by_domain(self, domain: str) -> TenantContext:
        """按子域名取上下文（恒不可达）。"""
        raise ServiceUnavailableError(f"租户注册契约不可达：{domain}")

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取上下文（恒不可达）。"""
        raise ServiceUnavailableError(f"租户注册契约不可达：{tenant_id}")

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（恒不可达，不使用）。"""
        raise ServiceUnavailableError("租户注册契约不可达")


@pytest.fixture(autouse=True)
def _mapper_registered() -> Iterator[None]:
    """登记 msg 产品码覆盖（验证它**不**吞掉源不可用分支）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    try:
        yield
    finally:
        registry.reset()


def _app() -> FastAPI:
    """装配最小应用：基座租户中间件 + 「当前租户」依赖端点（租户源恒不可达）。

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
    )
    app.state.tenant_source = _UnavailableSource()
    app.add_middleware(TenantMiddleware)

    @app.get("/api/v1/msg/topics", response_model=None)
    async def _list(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """正常情况下回显租户主键（本用例不应到达）。"""
        return ConcurrentStableDict({"tenant_id": tenant.tenant_id})

    return app


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 响应 JSON。
    """
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


async def test_management_plane_returns_503() -> None:
    """管理面：租户源不可达按基座策略返回 503 / 10007（不改写为租户不存在，不静默放行）。"""
    transport = ASGITransport(app=_app())
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/msg/topics", headers={"X-Tenant-ID": "acme"})
    assert (response.status_code, _body(response)["code"]) == (503, _SERVICE_UNAVAILABLE)


async def test_data_plane_propagates_unavailable() -> None:
    """数据面：源不可达原样冒泡（帧入口据此断连 / 回 503，不降级为「无租户」）。"""
    with pytest.raises(ServiceUnavailableError):
        await frame_tenant(token_tenant_id="7", source=_UnavailableSource())
