"""05-2 配额管理面（Kiwi 2454）：查询 / 批量调整 / 校验 / 审计。"""

from __future__ import annotations

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
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import quota as quota_api
from bms_msg.api.deps import get_audit_sink, get_storage_sampler, get_usage_counter
from bms_msg.quota.cache import QuotaCache
from bms_msg.usage.counter import DeliveryCounter
from bms_msg.usage.snapshot import StorageUsageSampler

pytestmark = pytest.mark.kiwi_id(2454)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
BASE_URL = "http://test"


class _Sink:
    """审计替身。"""

    def __init__(self) -> None:
        self.events: list[object] = []

    async def emit(self, event: object, *, required: bool = False) -> bool:
        del required
        self.events.append(event)
        return True


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def sink() -> _Sink:
    return _Sink()


@pytest.fixture
def api_app(
    db_session: AsyncSession,
    tenant_source: object,
    sink: _Sink,
    tmp_path: Path,
) -> FastAPI:
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
    app.state.quota_cache = QuotaCache()
    app.state.delivery_counter = DeliveryCounter()
    app.state.storage_usage_sampler = StorageUsageSampler(root=tmp_path)
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([quota_api.admin_router])))
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    app.dependency_overrides[get_audit_sink] = lambda: sink
    app.dependency_overrides[get_usage_counter] = lambda: app.state.delivery_counter
    app.dependency_overrides[get_storage_sampler] = lambda: app.state.storage_usage_sampler
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


def _body(response: object) -> dict[str, object]:
    return cast("dict[str, object]", json.loads(cast("object", response).text))  # type: ignore[attr-defined]


def _data(body: dict[str, object]) -> dict[str, object]:
    """取响应 data 段。"""
    return cast("dict[str, object]", body["data"])


def _items(body: dict[str, object]) -> list[dict[str, object]]:
    """取响应 data.items（测试断言用）。"""
    return cast("list[dict[str, object]]", _data(body)["items"])


async def test_get_quotas_returns_six_defaults(client: AsyncClient) -> None:
    """查询：六维、缺省来源、用量为 0。"""
    response = await client.get("/api/v1/msg/admin/quotas", headers={"X-Tenant-ID": "demo"})
    body = _body(response)
    assert (response.status_code, body["code"]) == (200, 0)
    items = _items(body)
    assert [item["quota_key"] for item in items] == [
        "topic_count",
        "group_count",
        "produce_rate",
        "storage_bytes",
        "unacked_count",
        "connection_count",
    ]
    assert all(item["source"] == "default" for item in items)
    assert all(item["usage"] == 0 for item in items if item["quota_key"] != "produce_rate")
    assert all(item["usage"] is None for item in items if item["quota_key"] == "produce_rate")


async def test_put_quotas_updates_and_audits(client: AsyncClient, sink: _Sink) -> None:
    """调整：写入配置值、派生软限、写一条审计。"""
    response = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "namespace": "msg", "limit": 10}]},
        headers={"X-Tenant-ID": "demo"},
    )
    body = _body(response)
    assert (response.status_code, body["code"]) == (200, 0)
    items = _items(body)
    topic = next(item for item in items if item["quota_key"] == "topic_count")
    assert (topic["hard_limit"], topic["soft_limit"], topic["source"]) == (10, 8, "configured")
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.payload["action"] == "msg:quota.update"  # type: ignore[attr-defined]
    assert event.payload["status"] == 200  # type: ignore[attr-defined]


async def test_put_quotas_rejects_invalid(client: AsyncClient) -> None:
    """非法调整：未知维度 / 软限大于硬限 / 命名空间非法。"""
    unknown = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "unknown", "limit": 1}]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _body(unknown)["code"] == 10001
    soft = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "limit": 1, "soft_limit": 2}]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _body(soft)["code"] == 10001
    namespace = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "namespace": "bad space", "limit": 1}]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _body(namespace)["code"] == 350010


async def test_get_quotas_rejects_unknown_namespace(client: AsyncClient) -> None:
    """查询：未知命名空间按 `350010` 拒绝。"""
    response = await client.get("/api/v1/msg/admin/quotas?namespace=not-enabled", headers={"X-Tenant-ID": "demo"})
    assert _body(response)["code"] == 350010


async def test_put_quotas_audit_failure_is_covered_by_05_4(api_app: FastAPI, client: AsyncClient) -> None:
    """强审计失败行为归 05-4：本文件只保留 05-2 的成功 / 校验路径。

    失败处置见 `tests/api/test_audit_config_changes.py::test_update_quotas_audit_failure_fails`。
    """
    del api_app, client
