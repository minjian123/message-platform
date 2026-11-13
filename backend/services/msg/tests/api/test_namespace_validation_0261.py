"""B15 命名空间形态校验（02-6-1 / Kiwi 2498）。

覆盖：`validate_namespace` 形态优先于 allowlist；配额查询 / 调整端点对非法形态返回
`350010` 且不落库、不写审计。
"""

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
from bms_core.core.concurrent import ConcurrentStableFrozenSet, ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import quota as quota_api
from bms_msg.api.deps import get_audit_sink, get_storage_sampler, get_usage_counter
from bms_msg.errors import MsgErrorCode, MsgNamespaceInvalidError
from bms_msg.models.quota import MsgQuota
from bms_msg.quota.cache import QuotaCache
from bms_msg.tenant.context import validate_namespace
from bms_msg.usage.counter import DeliveryCounter
from bms_msg.usage.snapshot import StorageUsageSampler

pytestmark = pytest.mark.kiwi_id(2498)

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
def api_app(db_session: AsyncSession, tenant_source: object, sink: _Sink, tmp_path: Path) -> FastAPI:
    app = FastAPI()
    app.state.settings = SimpleNamespace(
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


def test_validate_namespace_shape_precedes_allowlist() -> None:
    """形态优先：即使 allowlist 含非法键，仍按形态拒绝。"""
    allowlist = ConcurrentStableFrozenSet({"Bad.Name", "msg"})
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        validate_namespace("Bad.Name", allowlist)
    assert excinfo.value.code == MsgErrorCode.NAMESPACE_INVALID
    validate_namespace("msg", allowlist)
    validate_namespace(None, allowlist)


async def test_quota_get_rejects_malformed_namespace(client: AsyncClient) -> None:
    """查询：非法形态返回 350010。"""
    response = await client.get("/api/v1/msg/admin/quotas?namespace=Bad.Name", headers={"X-Tenant-ID": "demo"})
    assert _body(response)["code"] == 350010


async def test_quota_put_rejects_malformed_namespace_without_write(
    client: AsyncClient, db_session: AsyncSession, sink: _Sink
) -> None:
    """调整：非法形态 350010，不写配额行、不写审计。"""
    response = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "namespace": "Bad.Name", "limit": 1}]},
        headers={"X-Tenant-ID": "demo"},
    )
    assert _body(response)["code"] == 350010
    count = await db_session.execute(select(func.count()).select_from(MsgQuota))
    assert count.scalar_one() == 0
    assert sink.events == []
