"""管理面只读放行窗口（05-5 §4 / bms `04_03-01`；Kiwi 2453）。

覆盖：`backend/config.toml` 里 `[tenant_guard].readonly_routes` 的真值（两条管理面前缀、带服务键、
不含数据面）；停用 / 注销窗口内已登记前缀的 `GET` / `HEAD` 放行且拿到真实状态；写方法照原口径拒
（`350011`）；未登记的数据面前缀仍拒；空白名单 fail-closed（配错只影响「读不到」，不会放开写）。

**为什么读 `config.toml` 而不是 `Settings()`**：`bms_core` 的配置目录在**首次导入**时锚定（`bms_msg`
在 `__init__` 里设 `BMS_BACKEND_ROOT`），用例进程内的导入序不可控，`Settings()` 可能取到 bms 的基线
配置；读文件是唯一稳定的事实源，也让「配置被误删」直接表现为用例失败。
"""

import json
import tomllib
import types
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from bms_core.db.tenant import TenantContext
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import CurrentTenantDep
from bms_msg.errors import MsgErrorCode
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2453)

_BACKEND_ROOT = Path(__file__).resolve().parents[4]
"""`msg/backend` 工程根。"""

_CONFIG = _BACKEND_ROOT / "config.toml"
"""被测配置（`[tenant_guard].readonly_routes` 的唯一事实源）。"""

_MANIFEST = _BACKEND_ROOT.parent / "deploy" / "registry" / "msg.toml"
"""产品自持声明（产品码文案与 HTTP 状态的唯一事实源）。"""

_TENANT_ID = 7
"""测试用租户主键（雪花 id）。"""


def _configured_routes() -> ConcurrentStableList[str]:
    """读配置里的只读白名单。

    Returns:
        ConcurrentStableList[str]: 登记的 `服务键:路径前缀` 条目（顺序即配置顺序）。
    """
    with _CONFIG.open("rb") as handle:
        document = tomllib.load(handle)
    raw = cast("list[object]", document["tenant_guard"]["readonly_routes"])
    return ConcurrentStableList(str(entry) for entry in raw)


_ROUTES = _configured_routes()
"""配置真值（用例与装配共用；配置被改写即本文件失败）。"""


class _Source:
    """租户源替身：状态可切换，只读通道放行非启用状态（与基座 `TenantLookup.readonly` 同口径）。"""

    def __init__(self, status: str = "active") -> None:
        """初始化。

        Args:
            status: 租户状态（`active` / `suspended` / `deleted`）。
        """
        self.status = status
        self.readonly_calls: ConcurrentStableList[bool] = ConcurrentStableList()

    async def by_code(self, code: str, *, readonly: bool = False) -> TenantContext:
        """按编码取上下文。

        Args:
            code: 租户编码（`missing` 恒不存在）。
            readonly: 是否管理面只读通道。

        Returns:
            TenantContext: 租户上下文。

        Raises:
            TenantNotFoundError: 编码未登记。
            TenantSuspendedError: 非只读通道且租户非启用。
        """
        return self._hit(code, readonly)

    async def by_domain(self, domain: str, *, readonly: bool = False) -> TenantContext:
        """按子域名取上下文（本替身不支持域名来源）。

        Args:
            domain: 子域名。
            readonly: 是否管理面只读通道。

        Returns:
            TenantContext: 永不返回。

        Raises:
            TenantNotFoundError: 恒抛。
        """
        del readonly
        raise TenantNotFoundError(f"未知租户域名：{domain}")

    async def by_id(self, tenant_id: str, *, readonly: bool = False) -> TenantContext:
        """按主键取上下文。

        Args:
            tenant_id: 租户主键字符串。
            readonly: 是否管理面只读通道。

        Returns:
            TenantContext: 租户上下文（编码固定 `acme`）。

        Raises:
            TenantSuspendedError: 非只读通道且租户非启用。
        """
        return self._hit("acme", readonly)

    def _hit(self, code: str, readonly: bool) -> TenantContext:
        """记录 `readonly` 调用并按状态判定。

        Args:
            code: 租户编码。
            readonly: 是否管理面只读通道。

        Returns:
            TenantContext: 带真实状态的上下文。

        Raises:
            TenantNotFoundError: 编码未登记。
            TenantSuspendedError: 非只读通道且租户非启用（80002 / 403）。
        """
        self.readonly_calls.add(readonly)
        if code == "missing":
            raise TenantNotFoundError("未知租户")
        if self.status != "active" and not readonly:
            raise TenantSuspendedError(f"租户已停用：{code}")
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=_TENANT_ID, status=self.status)


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（统一错误响应据此解析文案与状态）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


def _echo(tenant: CurrentTenant) -> ConcurrentStableDict[str, object]:
    """回显依赖取到的租户（只读窗口能读到的证据）。

    Args:
        tenant: 当前租户。

    Returns:
        dict[str, object]: 租户主键与状态。
    """
    return ConcurrentStableDict({"tenant_id": tenant.tenant_id, "status": tenant.tenant.status})


def _app(source: _Source, routes: ConcurrentStableList[str]) -> FastAPI:
    """装配最小应用：真实租户中间件 + 四个桩端点（管理面读 / 管理面写 / 用量投影 / 数据面）。

    Args:
        source: 租户源替身。
        routes: 只读白名单（配置真值或空白名单）。

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
        tenant_guard=types.SimpleNamespace(readonly_routes=routes),
        error_mapper=types.SimpleNamespace(provider=""),
    )
    app.state.tenant_source = source
    app.add_middleware(TenantMiddleware)

    @app.get("/api/v1/msg/admin/topics", response_model=None)
    async def _admin_topics(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """管理面读端点（已登记前缀）。"""
        return _echo(tenant)

    @app.post("/api/v1/msg/admin/topics", response_model=None)
    async def _create_topic(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """管理面写端点（同前缀，但写方法永不进只读通道）。"""
        return _echo(tenant)

    @app.get("/api/v1/msg/tenant-usage", response_model=None)
    async def _usage(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """租户用量投影（已登记前缀）。"""
        return _echo(tenant)

    @app.get("/api/v1/msg/publish", response_model=None)
    async def _publish(tenant: CurrentTenantDep) -> ConcurrentStableDict[str, object]:
        """数据面路径（未登记：即便 GET 也按原口径拒）。"""
        return _echo(tenant)

    return app


async def _request(
    source: _Source, *, method: str, path: str, routes: ConcurrentStableList[str] | None = None
) -> Response:
    """发一次请求。

    Args:
        source: 租户源替身。
        method: HTTP 方法。
        path: 请求路径。
        routes: 只读白名单（缺省用配置真值）。

    Returns:
        Response: 响应。
    """
    transport = ASGITransport(app=_app(source, _ROUTES if routes is None else routes))
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, path, headers={"X-Tenant-ID": "acme"})


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        dict[str, object]: 响应 JSON。
    """
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


def test_config_registers_management_prefixes_only() -> None:
    """配置真值：两条管理面前缀，带服务键，不含数据面 / RPC 前缀。"""
    assert list(_ROUTES) == ["msg:/api/v1/msg/admin/", "msg:/api/v1/msg/tenant-usage"]
    assert all(entry.startswith("msg:") for entry in _ROUTES)
    assert not any(entry.startswith(("msg:/msg", "msg:/rpc", "msg:/api/v1/msg/publish")) for entry in _ROUTES)


async def test_suspended_tenant_reads_registered_admin_endpoints() -> None:
    """停用租户：已登记前缀的 `GET` 放行，且拿到真实状态 `suspended`。

    只验 `GET`：msg 管理面端点只声明 `GET`，FastAPI 不为 `GET` 路由隐式补 `HEAD`，
    `HEAD` 即便命中白名单也会 405——白名单的 `HEAD` 位由基座用例覆盖，本仓暂无用例依赖。
    """
    source = _Source("suspended")
    response = await _request(source, method="GET", path="/api/v1/msg/admin/topics")

    assert response.status_code == 200
    body = _body(response)
    assert (body["tenant_id"], body["status"]) == (str(_TENANT_ID), "suspended")
    assert source.readonly_calls[0] is True


async def test_deleted_tenant_reads_usage_projection() -> None:
    """注销租户：用量投影（第二条前缀）同样只读可见，便于运营核对清理进度。"""
    source = _Source("deleted")
    response = await _request(source, method="GET", path="/api/v1/msg/tenant-usage")

    assert response.status_code == 200
    assert _body(response)["status"] == "deleted"


async def test_write_method_still_rejected_in_window() -> None:
    """写方法不进只读通道：同一前缀 `POST` 仍按 `350011` / 403 拒。"""
    source = _Source("suspended")
    response = await _request(source, method="POST", path="/api/v1/msg/admin/topics")

    assert response.status_code == 403
    assert _body(response)["code"] == int(MsgErrorCode.TENANT_SUSPENDED)
    assert source.readonly_calls[0] is False


async def test_unregistered_data_plane_path_still_rejected() -> None:
    """未登记前缀（数据面）不放行：即使方法是 `GET` 也按原口径拒。"""
    source = _Source("suspended")
    response = await _request(source, method="GET", path="/api/v1/msg/publish")

    assert response.status_code == 403
    assert _body(response)["code"] == int(MsgErrorCode.TENANT_SUSPENDED)


async def test_empty_whitelist_is_fail_closed() -> None:
    """空白名单：已登记前缀也全拒（配置写错只影响「读不到」，不会把写通道打开）。"""
    source = _Source("suspended")
    response = await _request(source, method="GET", path="/api/v1/msg/admin/topics", routes=ConcurrentStableList())
    assert response.status_code == 403
    assert source.readonly_calls[0] is False


async def test_active_tenant_unaffected_by_whitelist() -> None:
    """启用租户：命中只读通道但状态判定不受影响，照常返回 `active`。"""
    source = _Source()
    response = await _request(source, method="GET", path="/api/v1/msg/admin/topics")

    assert response.status_code == 200
    assert _body(response)["status"] == "active"
    assert source.readonly_calls[0] is True
