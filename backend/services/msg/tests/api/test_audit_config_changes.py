"""配置变更审计（05-4；Kiwi 2455）：建主题 / 配额调整的显式强审计与失败处置。"""

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
from bms_core.events.base import EventEnvelope
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import quota as quota_api
from bms_msg.api.admin import topics
from bms_msg.api.deps import get_audit_sink, get_storage_sampler, get_usage_counter
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.quota.cache import QuotaCache
from bms_msg.usage.counter import DeliveryCounter
from bms_msg.usage.snapshot import StorageUsageSampler

pytestmark = pytest.mark.kiwi_id(2455)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码唯一事实源）。"""

NAMESPACE = "msg"
"""可用命名空间。"""

BASE_URL = "http://test"
"""测试基址。"""


class _Sink:
    """审计替身：记录事件与强审计开关。"""

    def __init__(self) -> None:
        """初始化。"""
        self.events: list[EventEnvelope] = []
        self.required: list[bool] = []

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一次写入。

        Args:
            event: 审计事件。
            required: 强审计开关。

        Returns:
            bool: 恒成功。
        """
        self.events.append(event)
        self.required.append(required)
        return True


class _ExplodingSink:
    """审计替身：发射即抛（强审计失败即业务失败）。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """恒抛异常。

        Args:
            event: 审计事件（忽略）。
            required: 强审计开关（忽略）。

        Returns:
            bool: 不返回。

        Raises:
            RuntimeError: 模拟审计不可用。
        """
        del event, required
        raise RuntimeError("audit down")


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记本用例所需的占位插件。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品错误码声明。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def sink() -> _Sink:
    """审计替身。

    Returns:
        _Sink: 空替身。
    """
    return _Sink()


@pytest.fixture
def api_app(
    db_session: AsyncSession,
    tenant_source: object,
    sink: _Sink,
    tmp_path: Path,
) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 配额路由。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。
        sink: 审计替身。
        tmp_path: 存储用量采样根目录。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.state.settings = SimpleNamespace(
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
    app.state.topic_cache = TopicCache()
    app.state.quota_cache = QuotaCache()
    app.state.delivery_counter = DeliveryCounter()
    app.state.storage_usage_sampler = StorageUsageSampler(root=tmp_path)
    app.state.audit_sink = sink
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, quota_api.admin_router])))
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。

        Returns:
            DbUnitOfWork: 绑定内存会话的工作单元。
        """
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    app.dependency_overrides[get_audit_sink] = lambda: sink
    app.dependency_overrides[get_usage_counter] = lambda: app.state.delivery_counter
    app.dependency_overrides[get_storage_sampler] = lambda: app.state.storage_usage_sampler
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


def _body(response: Response) -> dict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        dict[str, object]: 响应 JSON。
    """
    return cast("dict[str, object]", json.loads(response.text))


async def _create_topic(client: AsyncClient, name: str = "audit.operation") -> Response:
    """发一次建主题请求。

    Args:
        client: 客户端。
        name: 主题名。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": 2},
        headers={"X-Tenant-ID": "demo"},
    )


async def test_create_topic_writes_one_strong_audit(client: AsyncClient, sink: _Sink) -> None:
    """建主题：成功写一条 `msg:topic.create` 强审计，载荷带主题作用域。"""
    response = await _create_topic(client)

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.event_type == "sys.operation.logged"
    assert (event.payload["action"], event.payload["module"]) == ("msg:topic.create", "msg")
    assert (event.payload["module_key"], event.payload["resource_key"]) == (NAMESPACE, "audit.operation")
    params = cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))
    assert (params["namespace"], params["topic"], params["partition_count"]) == (
        NAMESPACE,
        "audit.operation",
        2,
    )
    assert isinstance(params["topic_id"], str)
    assert sink.required == [True]


async def test_create_topic_audit_failure_fails(api_app: FastAPI, client: AsyncClient) -> None:
    """建主题：强审计失败返回 10000 / 500。"""
    api_app.dependency_overrides[get_audit_sink] = lambda: _ExplodingSink()

    response = await _create_topic(client)

    assert (response.status_code, _body(response)["code"]) == (500, 10000)


async def test_update_quotas_writes_one_strong_audit(client: AsyncClient, sink: _Sink) -> None:
    """配额调整：只写一条 `msg:quota.update` 强审计（中间件路径已排除）。"""
    response = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "namespace": NAMESPACE, "limit": 10}]},
        headers={"X-Tenant-ID": "demo"},
    )

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert len(sink.events) == 1
    event = sink.events[0]
    assert (event.payload["action"], event.payload["module"]) == ("msg:quota.update", "msg")
    params = cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))
    assert cast("list[dict[str, object]]", params["items"])[0]["quota_key"] == "topic_count"
    assert sink.required == [True]


async def test_update_quotas_audit_failure_fails(api_app: FastAPI, client: AsyncClient) -> None:
    """配额调整：强审计失败返回 10000 / 500。"""
    api_app.dependency_overrides[get_audit_sink] = lambda: _ExplodingSink()

    response = await client.put(
        "/api/v1/msg/admin/quotas",
        json={"items": [{"quota_key": "topic_count", "limit": 5}]},
        headers={"X-Tenant-ID": "demo"},
    )

    assert (response.status_code, _body(response)["code"]) == (500, 10000)
