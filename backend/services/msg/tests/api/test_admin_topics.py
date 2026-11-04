"""主题管理接口（msg 任务 01-1；Kiwi 2412）。

覆盖：三端点状态码与响应结构、请求体不接收租户（`tenant_id` 以解析结果为准）、权限码拦截、
错误码（`350001` / `350008` / `350009` / `350010` / `350011` / `350012`）、列表分页与过滤。

装配方式：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由（`mount_service_routers`）；
只把平台库工作单元换成内存库会话（不经引擎注册表）。
"""

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.api.base import mount_service_routers
from bms_core.api.errors import register_exception_handlers
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import topics
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, register_msg_error_mapper

pytestmark = pytest.mark.kiwi_id(2412)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

BASE_URL = "http://test"
"""测试基址。"""


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝（验证权限码拦截；不声明 `plugin_name`，避免被自动收集与显式登记重名）。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。

        Args:
            code: 权限码（本桩不区分）。

        Returns:
            bool: False。
        """
        del code
        return False


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记本用例所需的插件实现（产品码覆盖 + 确保占位权限 / 租户模块门禁已导入）。

    占位实现（`Null*`）靠导入期的 `__init_subclass__` 自动成为候选（默认 provider 空串即解析到
    `null`），**不能再用 `register_plugin(..., NULL_PLUGIN_NAME, ...)` 显式登记**——那会与自动候选撞
    「重名」而拒建注册表；这里只保证模块已导入。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    # 解除构建冻结，避免后续用例（含其它文件的装配用例）因「注册表已构建」无法登记
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（`build_error_response` 据此解析文案键与 HTTP 状态）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题路由，平台库工作单元换成内存会话。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        permission=SimpleNamespace(provider=""),
        tenant_module=SimpleNamespace(provider=""),
        error_mapper=SimpleNamespace(provider=MSG_ERROR_MAPPER),
        tenant=SimpleNamespace(
            exempt_paths=ConcurrentStableList(),
            deferred_paths=ConcurrentStableList(),
            allow_demo_fallback=False,
        ),
    )
    app.state.tenant_source = tenant_source
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router])))
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。

        Returns:
            DbUnitOfWork: 绑定内存会话的工作单元。
        """
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 响应 JSON。
    """
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


def _data(response: Response) -> ConcurrentStableDict[str, object]:
    """取响应 `data` 段。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 业务数据。
    """
    return ConcurrentStableDict(cast("dict[str, object]", _body(response)["data"]))


async def _create(client: AsyncClient, name: str = "audit.operation", **overrides: object) -> Response:
    """发一次创建请求（缺省 `X-Tenant-ID: demo`）。

    Args:
        client: 客户端。
        name: 主题名。
        **overrides: 覆盖请求体字段。

    Returns:
        Response: 响应。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"namespace": NAMESPACE, "name": name, "partition_count": 3}
    )
    for key, value in overrides.items():
        payload.set(key, value)
    return await client.post("/api/v1/msg/admin/topics", json=dict(payload.items()), headers={"X-Tenant-ID": "demo"})


async def test_create_topic_returns_metadata(client: AsyncClient) -> None:
    """创建：200 + 完整元数据（雪花 ID 字符串化、分区数、状态、创建时间）。"""
    response = await _create(client)
    body = _body(response)
    data = _data(response)
    assert (response.status_code, body["code"]) == (200, 0)
    assert (data["namespace"], data["name"], data["partition_count"], data["status"]) == (
        NAMESPACE,
        "audit.operation",
        3,
        "active",
    )
    assert isinstance(data["id"], str)
    assert str(data["tenant_id"]) == "7001"
    assert data["created_at"]


async def test_create_topic_takes_tenant_from_resolution(client: AsyncClient) -> None:
    """`tenant_id` 以解析结果为准：换一个租户编码请求，落库租户随之改变。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": "audit.other", "partition_count": 1},
        headers={"X-Tenant-ID": "other"},
    )
    assert (response.status_code, str(_data(response)["tenant_id"])) == (200, "8002")


async def test_create_topic_body_rejects_tenant_field(client: AsyncClient) -> None:
    """请求体携带租户字段：契约拒绝（`10001`），不会让客户端自报租户生效。"""
    response = await _create(client, tenant_id=9999)
    assert _body(response)["code"] == 10001


async def test_create_duplicate_returns_350008(client: AsyncClient) -> None:
    """重名：409 + `350008`。"""
    await _create(client)
    response = await _create(client)
    assert (response.status_code, _body(response)["code"]) == (409, int(MsgErrorCode.TOPIC_EXISTS))


async def test_create_invalid_partition_count_returns_350009(client: AsyncClient) -> None:
    """分区数越界：400 + `350009`。"""
    response = await _create(client, partition_count=0)
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))


async def test_create_invalid_namespace_returns_350010(client: AsyncClient) -> None:
    """命名空间未登记：400 + `350010`。"""
    response = await _create(client, namespace="nope")
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.NAMESPACE_INVALID))


async def test_list_topics_paginates(client: AsyncClient) -> None:
    """列表：200 + `{items,total,page,size}`，按页码切片。"""
    for index in range(3):
        await _create(client, name=f"audit.operation.{index}", partition_count=1)
    response = await client.get(
        "/api/v1/msg/admin/topics",
        params={"page": 1, "size": 2},
        headers={"X-Tenant-ID": "demo"},
    )
    data = _data(response)
    assert response.status_code == 200
    assert (len(cast("list[object]", data["items"])), data["total"], data["page"], data["size"]) == (2, 3, 1, 2)


async def test_list_topics_filters_by_namespace_and_keyword(client: AsyncClient) -> None:
    """列表过滤：命名空间 + 名称关键词。"""
    await _create(client, name="audit.operation")
    await _create(client, name="notify.sent")
    response = await client.get(
        "/api/v1/msg/admin/topics",
        params={"namespace": NAMESPACE, "keyword": "audit"},
        headers={"X-Tenant-ID": "demo"},
    )
    data = _data(response)
    names = [cast("dict[str, object]", item)["name"] for item in cast("list[object]", data["items"])]
    assert (response.status_code, data["total"], names) == (200, 1, ["audit.operation"])


async def test_list_rejects_invalid_query(client: AsyncClient) -> None:
    """列表参数越界（`size=0`）与非法的命名空间过滤：`10001` / `350010`。"""
    bad_size = await client.get("/api/v1/msg/admin/topics", params={"size": 0}, headers={"X-Tenant-ID": "demo"})
    assert _body(bad_size)["code"] == 10001
    bad_namespace = await client.get(
        "/api/v1/msg/admin/topics", params={"namespace": "nope"}, headers={"X-Tenant-ID": "demo"}
    )
    assert (bad_namespace.status_code, _body(bad_namespace)["code"]) == (400, int(MsgErrorCode.NAMESPACE_INVALID))


async def test_detail_returns_partitions(client: AsyncClient) -> None:
    """详情：200 + 分区明细（分区号升序、leader、检查点）。"""
    created = _data(await _create(client, partition_count=2))
    response = await client.get(f"/api/v1/msg/admin/topics/{created['id']}", headers={"X-Tenant-ID": "demo"})
    data = _data(response)
    partitions = [cast("dict[str, object]", item) for item in cast("list[object]", data["partitions"])]
    assert response.status_code == 200
    assert [item["partition_no"] for item in partitions] == [0, 1]
    assert {str(item["leader"]) for item in partitions} == {"msg-0"}
    assert {item["epoch"] for item in partitions} == {0}


async def test_detail_missing_returns_350001(client: AsyncClient) -> None:
    """详情不存在：404 + `350001`。"""
    response = await client.get("/api/v1/msg/admin/topics/123456789", headers={"X-Tenant-ID": "demo"})
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_suspended_tenant_returns_350011(client: AsyncClient, tenant_source: object) -> None:
    """停用租户：403 + `350011`（基座中间件拒绝，经产品码覆盖改写）。"""
    setattr(tenant_source, "suspended", True)  # noqa: B010  # 替身状态切换（夹具返回实现类实例）
    response = await client.get("/api/v1/msg/admin/topics", headers={"X-Tenant-ID": "demo"})
    assert (response.status_code, _body(response)["code"]) == (403, int(MsgErrorCode.TENANT_SUSPENDED))


async def test_unknown_tenant_returns_350012(client: AsyncClient) -> None:
    """不存在的租户：404 + `350012`（不泄露存在性）。"""
    response = await client.get("/api/v1/msg/admin/topics", headers={"X-Tenant-ID": "missing"})
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TENANT_NOT_FOUND))


async def test_permission_denied_blocks_both_endpoints(client: AsyncClient, api_app: FastAPI) -> None:
    """无权限码：创建与查询都被拒（30001 / 403），错误响应不进入业务逻辑。"""
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"
    created = await _create(client)
    listed = await client.get("/api/v1/msg/admin/topics", headers={"X-Tenant-ID": "demo"})
    assert (created.status_code, listed.status_code) == (403, 403)
    assert _body(created)["code"] == 30001


@pytest.mark.kiwi_id(2418)
async def test_retention_bytes_is_accepted_and_echoed(client: AsyncClient) -> None:
    """01-6：容量上限随创建透传并在响应回显（缺省 0 = 不限）。"""
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={
            "namespace": "msg",
            "name": "audit.capacity",
            "partition_count": 1,
            "retention_bytes": 1048576,
        },
        headers={"X-Tenant-ID": "demo"},
    )
    defaulted = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": "msg", "name": "audit.unlimited", "partition_count": 1},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _data(created)["retention_bytes"] == 1048576
    assert _data(defaulted)["retention_bytes"] == 0


@pytest.mark.kiwi_id(2418)
async def test_retention_bytes_over_limit_returns_350009(client: AsyncClient) -> None:
    """01-6：容量上限超上界 → `350009`（与保留期同码）。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={
            "namespace": "msg",
            "name": "audit.too-big",
            "partition_count": 1,
            "retention_bytes": 2_000_000_000_000,
        },
        headers={"X-Tenant-ID": "demo"},
    )
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))
