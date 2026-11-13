"""消费组删除端点（01-4-1；Kiwi 2496）：默认保留位点 / 可选清位点 / 确认与错误码 / 强审计。

装配与 `test_admin_groups.py` 同构：最小应用 + 真实租户中间件 + 统一错误渲染 + 真实路由；
组存储换成真实 `DatabaseGroupStore`（两表 + 内存库会话）。
"""

import importlib
import json
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
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
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import groups as admin_groups
from bms_msg.api.admin import topics
from bms_msg.broker.group import GROUP_STATE_STABLE, ConsumerGroup
from bms_msg.broker.group_db import DatabaseGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from tests_support.frame_app import install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2496)

MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""
NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
BASE_URL = "http://test"
HEADERS = {"X-Tenant-ID": "demo"}
TENANT_ID = 7001


class _Sink:
    """审计替身：记录事件（强审计走 `emit`）。"""

    def __init__(self) -> None:
        """初始化空记录。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一条审计。

        Args:
            event: 审计事件。
            required: 强审计开关（本替身不区分）。

        Returns:
            bool: True。
        """
        del required
        self.events.add(event)
        return True


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """重置插件注册表并导入占位实现。"""
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明。"""
    reset_error_code_records()
    manifest = load_product_manifest(MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 组路由。"""
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
    app.state.audit_sink = _Sink()
    app.state.message_store = InMemoryMessageStore()
    app.state.offset_store = InMemoryOffsetStore()
    install_backpressure_state(app)

    session = cast("DbSession", db_session)

    @asynccontextmanager
    async def _factory() -> AsyncGenerator[UnitOfWork]:
        yield DbUnitOfWork(session)

    app.state.group_store = DatabaseGroupStore(uow_factory=_factory, instance_id="n1")
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, admin_groups.router])))

    async def _platform_uow() -> DbUnitOfWork:
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


async def _seed(client: AsyncClient, api_app: FastAPI) -> int:
    """建主题 + 写一个组 + 提交一个位点，返回主题主键。"""
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=HEADERS,
    )
    assert created.status_code == 200, created.text
    topic_id = int(cast("str", _data(created)["id"]))
    store = cast("DatabaseGroupStore", api_app.state.group_store)
    await store.save(
        ConsumerGroup(
            tenant_id=TENANT_ID,
            topic_id=topic_id,
            group=GROUP,
            namespace=NAMESPACE,
            state=GROUP_STATE_STABLE,
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    await offsets.commit(group=GROUP, topic_id=topic_id, partition_no=0, offset=3)
    return topic_id


def _path() -> str:
    """组删除路径。"""
    return f"/api/v1/msg/admin/groups/{NAMESPACE}/{TOPIC_NAME}/{GROUP}"


async def test_delete_group_keeps_offsets_by_default(client: AsyncClient, api_app: FastAPI) -> None:
    """默认只删组与成员：位点保留，删除审计留痕。"""
    topic_id = await _seed(client, api_app)
    response = await client.delete(_path(), params={"confirm": True}, headers=HEADERS)
    assert response.status_code == 200, response.text
    data = _data(response)
    assert data["deleted"] is True
    assert data["offsets_purged"] == 0

    store = cast("DatabaseGroupStore", api_app.state.group_store)
    assert await store.load(tenant_id=TENANT_ID, topic_id=topic_id, group=GROUP) is None
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    assert await offsets.get(group=GROUP, topic_id=topic_id, partition_no=0) == 3

    sink = cast("_Sink", api_app.state.audit_sink)
    actions = [event.payload.get("action") for event in sink.events]
    assert "msg:group.delete" in actions


async def test_delete_group_can_purge_offsets(client: AsyncClient, api_app: FastAPI) -> None:
    """`purge_offsets=true`：位点一并清理。"""
    topic_id = await _seed(client, api_app)
    response = await client.delete(_path(), params={"confirm": True, "purge_offsets": True}, headers=HEADERS)
    assert response.status_code == 200, response.text
    assert _data(response)["offsets_purged"] == 1
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    assert await offsets.get(group=GROUP, topic_id=topic_id, partition_no=0) == 0


async def test_delete_group_requires_confirm(client: AsyncClient, api_app: FastAPI) -> None:
    """缺 `confirm`：契约层 10001。"""
    await _seed(client, api_app)
    response = await client.delete(_path(), headers=HEADERS)
    assert _body(response)["code"] == 10001


async def test_delete_missing_group_returns_350013(client: AsyncClient, api_app: FastAPI) -> None:
    """组不存在：350013，且不误删任何行。"""
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=HEADERS,
    )
    assert created.status_code == 200
    response = await client.delete(_path(), params={"confirm": True}, headers=HEADERS)
    assert _body(response)["code"] == 350013


async def test_delete_group_tolerates_offsets_purge_failure(client: AsyncClient, api_app: FastAPI) -> None:
    """位点清理失败只告警：组已删、响应 200 且 `offsets_purged=0`。"""

    class _BrokenOffsets(InMemoryOffsetStore):
        async def purge_group(self, *, tenant_id: int, group: str) -> int:
            del tenant_id, group
            raise OSError("offset store down")

    topic_id = await _seed(client, api_app)
    api_app.state.offset_store = _BrokenOffsets()
    response = await client.delete(_path(), params={"confirm": True, "purge_offsets": True}, headers=HEADERS)
    assert response.status_code == 200, response.text
    assert _data(response)["offsets_purged"] == 0
    store = cast("DatabaseGroupStore", api_app.state.group_store)
    assert await store.load(tenant_id=TENANT_ID, topic_id=topic_id, group=GROUP) is None
