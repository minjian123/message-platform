"""主题 ACL 管理端点（Kiwi 2456）：GET / PUT / DELETE、校验、跨租户与审计。

装配方式：最小应用 + 基座租户中间件 + 统一错误渲染 + ACL 路由；平台库工作单元换内存会话。
"""

from __future__ import annotations

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
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.acl.cache import TopicAclCache
from bms_msg.api.admin import acls
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2456)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明唯一事实源）。"""

NAMESPACE = "msg"
TOPIC = "audit.operation"
BASE_URL = "http://test"


class FakeAuditSink:
    """审计替身：记录事件（本文件断言动作序列）。"""

    def __init__(self) -> None:
        """初始化空事件列表。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """记录事件并返回成功。"""
        del required
        self.events.add(cast("EventEnvelope", event))
        return True


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """隔离插件注册表并确保权限 / 租户模块占位实现已导入。"""
    import importlib

    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单错误码声明（350027 的 maps_from 与文案键）。"""
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
async def topic_row(db_session: AsyncSession, current_tenant: CurrentTenant) -> MsgTopic:
    """建一个主题（ACL 授予前置）。"""
    del current_tenant
    session = cast("DbSession", db_session)
    row = await TopicRepository(session).create(
        tenant_id=7001,
        namespace=NAMESPACE,
        name=TOPIC,
        partition_count=1,
        retention_ms=86_400_000,
        cleanup_policy="delete",
    )
    await db_session.commit()
    return row


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + ACL 路由。"""
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        # 管理面只读白名单：最小应用走原口径（空 = 全拒）；只读窗口见 test_tenant_readonly。
        tenant_guard=SimpleNamespace(readonly_routes=ConcurrentStableList()),
        permission=SimpleNamespace(provider=""),
        tenant_module=SimpleNamespace(provider=""),
        error_mapper=SimpleNamespace(provider=""),
        tenant=SimpleNamespace(
            exempt_paths=ConcurrentStableList(),
            deferred_paths=ConcurrentStableList(),
            allow_demo_fallback=False,
        ),
    )
    app.state.tenant_source = tenant_source
    app.state.topic_acl_cache = TopicAclCache()
    app.state.audit_sink = FakeAuditSink()
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([acls.router])))
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。"""
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。"""
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。"""
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


def _data(response: Response) -> ConcurrentStableDict[str, object]:
    """取响应 `data` 段。"""
    return ConcurrentStableDict(cast("dict[str, object]", _body(response)["data"]))


def _code(response: Response) -> int:
    """取响应业务码（JSON 数字）。"""
    return int(cast("int", _body(response)["code"]))


def _rule_payload(**overrides: object) -> ConcurrentStableDict[str, object]:
    """构造一条规则请求体。"""
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "namespace": NAMESPACE,
            "topic": TOPIC,
            "principal_type": "user",
            "principal_key": "7001",
            "action": "publish",
            "effect": "allow",
        }
    )
    for key, value in overrides.items():
        payload.set(key, value)
    return payload


async def test_put_get_delete_roundtrip(client: AsyncClient, topic_row: MsgTopic, api_app: FastAPI) -> None:
    """PUT 授予 → GET 列表 → DELETE 收回；审计三条强动作。"""
    del topic_row
    put = await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload().items())]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert put.status_code == 200
    data = _data(put)
    item = cast("ConcurrentStableDict[str, object]", data["items"][0])  # type: ignore[index]
    acl_id = str(item["acl_id"])
    assert (item["effect"], item["status"]) == ("allow", "enabled")
    assert list(cast("ConcurrentStableList[str]", data["changed"])) == [acl_id]

    listed = await client.get("/api/v1/msg/admin/acls", headers={"X-Tenant-ID": "demo"})
    assert listed.status_code == 200
    page = _data(listed)
    assert int(cast("int", page["total"])) == 1

    deleted = await client.delete(f"/api/v1/msg/admin/acls/{acl_id}", headers={"X-Tenant-ID": "demo"})
    assert deleted.status_code == 200
    assert _data(deleted)["changed"] is True

    sink = cast("FakeAuditSink", api_app.state.audit_sink)
    actions = [str(event.payload["action"]) for event in sink.events]
    assert actions == ["msg:acl.grant", "msg:acl.revoke"]


async def test_list_filters_and_namespace(client: AsyncClient, topic_row: MsgTopic) -> None:
    """列表过滤枚举非法 10001；命名空间过滤走 ensure_namespace。"""
    del topic_row
    await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload().items())]},
        headers={"X-Tenant-ID": "demo"},
    )
    listed = await client.get("/api/v1/msg/admin/acls?namespace=msg", headers={"X-Tenant-ID": "demo"})
    assert listed.status_code == 200
    assert int(cast("int", _data(listed)["total"])) == 1
    for query in ("principal_type=bad", "action=bad", "status=bad"):
        response = await client.get(f"/api/v1/msg/admin/acls?{query}", headers={"X-Tenant-ID": "demo"})
        assert _code(response) == 10001


async def test_delete_is_idempotent(client: AsyncClient, topic_row: MsgTopic) -> None:
    """DELETE 不存在的规则 → changed=false（不泄露跨租户）。"""
    del topic_row
    response = await client.delete("/api/v1/msg/admin/acls/999", headers={"X-Tenant-ID": "demo"})
    assert response.status_code == 200
    assert _data(response)["changed"] is False


async def test_put_rejects_unknown_topic(client: AsyncClient) -> None:
    """主题不存在 → 350001 / 404。"""
    response = await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload(topic="missing.topic").items())]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert response.status_code == 404
    assert _code(response) == 350001


async def test_put_rejects_invalid_action(client: AsyncClient, topic_row: MsgTopic) -> None:
    """动作非法 → 10001 / 400。"""
    del topic_row
    response = await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload(action="unknown").items())]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _code(response) == 10001


async def test_cross_tenant_isolated(client: AsyncClient, topic_row: MsgTopic) -> None:
    """跨租户：另一租户看不到 demo 的规则；给 demo 主题授权 → 350001。"""
    del topic_row
    await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload().items())]},
        headers={"X-Tenant-ID": "demo"},
    )
    other = await client.get("/api/v1/msg/admin/acls", headers={"X-Tenant-ID": "other"})
    assert int(cast("int", _data(other)["total"])) == 0
    denied = await client.put(
        "/api/v1/msg/admin/acls",
        json={"items": [dict(_rule_payload().items())]},
        headers={"X-Tenant-ID": "other"},
    )
    assert denied.status_code == 404
    assert _code(denied) == 350001
