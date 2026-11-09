"""死信管理接口用例（06-5-2；Kiwi 2467）：列表 / 详情 / 重放 / 忽略 / 归档 / 权限。"""

from __future__ import annotations

import importlib
import json
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
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
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import dlq as admin_dlq
from bms_msg.api.admin import topics
from bms_msg.api.deps import get_topic_acl_service
from bms_msg.broker.deadletter import DatabaseDeadLetterQueue, DeadLetter, DeadLetterReason
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner
from tests_support.frame_app import install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2467)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明唯一事实源）。"""

NAMESPACE = "msg"
"""可用命名空间。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

BASE_URL = "http://test"
"""测试基址。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准时间。"""


class RecordingSink:
    """审计出口替身：记录事件、可切换成功 / 失败。"""

    def __init__(self) -> None:
        """初始化（缺省成功）。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
        self.result = True

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录事件并返回设定结果。"""
        self.events.add(event)
        return self.result


class _AllowAcl:
    """主题 ACL 放行桩（接口用例只验业务编排）。"""

    async def authorize(self, **scope: object) -> None:
        """恒放行。"""
        del scope

    async def filter_allowed(self, *, topics: ConcurrentStableList[str], **scope: object) -> ConcurrentStableList[str]:
        """返回全部主题。"""
        del scope
        return topics


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。"""
        del code
        return False


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
    """装载产品清单错误码声明。"""
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(
    db_session: AsyncSession,
    tenant_source: object,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 死信路由 + 进程级状态。"""
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
    app.state.message_store = InMemoryMessageStore()
    install_backpressure_state(app)
    app.state.offset_store = InMemoryOffsetStore()
    app.state.group_store = InMemoryGroupStore()
    app.state.dead_letter_queue = DatabaseDeadLetterQueue(uow_factory=replay_uow)
    app.state.delivery_state = DeliveryState()
    sink = RecordingSink()
    app.state.audit_sink = sink
    session = cast("DbSession", db_session)
    app.state.replay_runner = ReplayRunner(
        messages=app.state.message_store,
        uow_factory=replay_uow,
        auditor=ReplayAuditor(sink=sink),
        interval_ms=10_000,
    )
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, admin_dlq.router])))

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。"""
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    app.dependency_overrides[get_topic_acl_service] = lambda: _AllowAcl()
    return app


@pytest.fixture(autouse=True)
async def _stop_runner(api_app: FastAPI) -> AsyncIterator[None]:
    """用例结束后停掉重放执行器。"""
    yield
    await cast("ReplayRunner", api_app.state.replay_runner).stop()


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


async def _create_topic(client: AsyncClient) -> int:
    """建一个两分区主题并返回主键。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 2},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
    return int(cast("str", _data(response)["id"]))


async def _seed_message(api_app: FastAPI, *, topic_id: int, offset: int = 0) -> None:
    """直接写一条历史记录（重放 / 原记录校验用）。"""
    store = cast("InMemoryMessageStore", api_app.state.message_store)
    await store.append(
        topic_id=topic_id,
        partition_no=0,
        message_id=f"m-{offset}",
        log_id=f"l-{offset}",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace_id=None,
        published_at=_BASE,
    )


async def _seed_dead_letter(
    api_app: FastAPI,
    *,
    topic_id: int,
    offset: int = 0,
    status: str = "pending",
    log_id: str = "l-0",
    message_id: str | None = None,
    topic: str = TOPIC_NAME,
    partition_no: int = 0,
    reason: DeadLetterReason = DeadLetterReason.PROCESSING_FAILED,
    failed_at: datetime = _BASE,
) -> None:
    """写一条死信（队列落库）。"""
    queue = cast("DatabaseDeadLetterQueue", api_app.state.dead_letter_queue)
    await queue.add(
        DeadLetter(
            message_id=message_id or f"m-{offset}",
            topic_id=topic_id,
            partition_no=partition_no,
            offset=offset,
            attempts=5,
            failed_at=failed_at,
            reason=reason,
            detail=ConcurrentStableDict({"error": "boom"}),
            tenant_id=7001,
            namespace=NAMESPACE,
            topic=topic,
            log_id=log_id,
            trace_id="trace-1",
            status=status,
        )
    )


def _first_id(response: Response) -> int:
    """列表响应取首行主键。"""
    items = cast("list[object]", _data(response)["items"])
    return int(cast("dict[str, str]", items[0])["id"])


@pytest.mark.kiwi_id(2467)
async def test_list_and_detail(client: AsyncClient, api_app: FastAPI) -> None:
    """列表返回死信摘要；详情返回失败上下文。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id, offset=0)
    await _seed_dead_letter(api_app, topic_id=topic_id, offset=1, log_id="l-1")

    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    assert listed.status_code == 200
    assert _data(listed)["total"] == 2
    items = cast("list[object]", _data(listed)["items"])
    assert [cast("dict[str, object]", item)["status"] for item in items] == ["pending", "pending"]

    dlq_id = _first_id(listed)
    detail = await client.get(f"/api/v1/msg/admin/dlq/{dlq_id}", headers=DEMO_HEADERS)
    assert detail.status_code == 200
    assert _data(detail)["detail"] == {"error": "boom"}


@pytest.mark.kiwi_id(2467)
async def test_list_filters(client: AsyncClient, api_app: FastAPI) -> None:
    """列表筛选：状态 / 原因 / 主题 / 分区 / 失败时间窗 / 关键字。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id, offset=0, log_id="l-0")
    await _seed_dead_letter(
        api_app,
        topic_id=topic_id,
        offset=9,
        log_id="l-9",
        message_id="m-9",
        topic="audit.other",
        partition_no=1,
        reason=DeadLetterReason.SCHEMA_INVALID,
        failed_at=datetime(2026, 1, 1, 1, tzinfo=UTC),
    )

    filtered = await client.get(
        "/api/v1/msg/admin/dlq",
        params={
            "status": "pending",
            "reason": "schema_invalid",
            "topic": "audit.other",
            "partition": 1,
            "failed_from": "2026-01-01T00:30:00",
            "failed_to": "2026-01-01T02:00:00",
            "keyword": "l-9",
        },
        headers=DEMO_HEADERS,
    )
    assert filtered.status_code == 200
    assert _data(filtered)["total"] == 1
    items = cast("list[object]", _data(filtered)["items"])
    assert [cast("dict[str, object]", item)["log_id"] for item in items] == ["l-9"]

    by_status = await client.get("/api/v1/msg/admin/dlq", params={"status": "pending"}, headers=DEMO_HEADERS)
    assert by_status.status_code == 200
    assert _data(by_status)["total"] == 2


@pytest.mark.kiwi_id(2467)
async def test_dispose_requires_confirm(client: AsyncClient, api_app: FastAPI) -> None:
    """重放 / 忽略 / 归档缺 `confirm=true` 一律 10001。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id)
    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    dlq_id = _first_id(listed)

    for action in ("replay", "ignore", "archive"):
        response = await client.post(f"/api/v1/msg/admin/dlq/{dlq_id}/{action}", json={}, headers=DEMO_HEADERS)
        assert (response.status_code, _body(response)["code"]) == (200, 10001)


@pytest.mark.kiwi_id(2467)
async def test_ignore_rejects_blank_reason(client: AsyncClient, api_app: FastAPI) -> None:
    """忽略原因全空白（去空白后为空）返回 10001。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id)
    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    dlq_id = _first_id(listed)

    response = await client.post(
        f"/api/v1/msg/admin/dlq/{dlq_id}/ignore",
        json={"confirm": True, "reason": "   "},
        headers=DEMO_HEADERS,
    )
    assert (response.status_code, _body(response)["code"]) == (200, 10001)


@pytest.mark.kiwi_id(2467)
async def test_ignore_requires_reason_and_archives(client: AsyncClient, api_app: FastAPI) -> None:
    """忽略原因必填（10001）；成功置 ignored，随后可归档。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id)
    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    dlq_id = _first_id(listed)

    missing = await client.post(f"/api/v1/msg/admin/dlq/{dlq_id}/ignore", json={"confirm": True}, headers=DEMO_HEADERS)
    assert (missing.status_code, _body(missing)["code"]) == (200, 10001)

    ignored = await client.post(
        f"/api/v1/msg/admin/dlq/{dlq_id}/ignore",
        json={"confirm": True, "reason": "上游已作废"},
        headers=DEMO_HEADERS,
    )
    assert ignored.status_code == 200
    assert _data(ignored)["status"] == "ignored"

    archived = await client.post(
        f"/api/v1/msg/admin/dlq/{dlq_id}/archive",
        json={"confirm": True, "reason": "归档"},
        headers=DEMO_HEADERS,
    )
    assert archived.status_code == 200
    assert _data(archived)["status"] == "archived"

    again = await client.post(f"/api/v1/msg/admin/dlq/{dlq_id}/archive", json={"confirm": True}, headers=DEMO_HEADERS)
    assert (again.status_code, _body(again)["code"]) == (409, 350030)


@pytest.mark.kiwi_id(2467)
async def test_replay_creates_single_task_and_marks_replaying(client: AsyncClient, api_app: FastAPI) -> None:
    """重放按原分区 / offset 建单条任务并置 replaying；重复处置 350030。"""
    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id, offset=0)
    await _seed_dead_letter(api_app, topic_id=topic_id, offset=0)
    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    dlq_id = _first_id(listed)

    replayed = await client.post(
        f"/api/v1/msg/admin/dlq/{dlq_id}/replay",
        json={"confirm": True, "rate_per_second": 50},
        headers=DEMO_HEADERS,
    )
    assert replayed.status_code == 200
    data = _data(replayed)
    assert data["status"] == "replaying"
    assert data["replay_task_id"] is not None

    again = await client.post(f"/api/v1/msg/admin/dlq/{dlq_id}/replay", json={"confirm": True}, headers=DEMO_HEADERS)
    assert (again.status_code, _body(again)["code"]) == (409, 350030)


@pytest.mark.kiwi_id(2467)
async def test_replay_rejects_missing_original(client: AsyncClient, api_app: FastAPI) -> None:
    """原记录不存在（超出保留期）返回 350031，不建空任务。"""
    topic_id = await _create_topic(client)
    await _seed_dead_letter(api_app, topic_id=topic_id, offset=9)
    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    dlq_id = _first_id(listed)

    response = await client.post(f"/api/v1/msg/admin/dlq/{dlq_id}/replay", json={"confirm": True}, headers=DEMO_HEADERS)
    assert (response.status_code, _body(response)["code"]) == (409, 350031)


@pytest.mark.kiwi_id(2467)
async def test_permission_denied_blocks_dispose(client: AsyncClient, api_app: FastAPI) -> None:
    """无 `msg:manage` → 403（30001）。"""
    register_plugin("permission", "deny", _DenyChecker)
    cast("SimpleNamespace", api_app.state.settings).permission.provider = "deny"
    response = await client.post(
        "/api/v1/msg/admin/dlq/1/ignore", json={"confirm": True, "reason": "x"}, headers=DEMO_HEADERS
    )
    assert (response.status_code, _body(response)["code"]) == (403, 30001)
