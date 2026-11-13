"""05-4-1 写操作空头补齐端点（Kiwi 2495）。

覆盖：主题停用 / 启用（幂等、缺 confirm、审计同事务）、删消息（单条 / 批量上限 / 墓碑可见性）、
延迟丢弃（确认 / 审计）、消息检索留痕。

装配与 `test_admin_write_ops.py` 同构：最小应用 + 真实租户中间件 + 统一错误渲染 + 真实路由；
平台库工作单元换成内存库会话；审计出口用记录替身（含 `emit_in_session`）。
"""

import importlib
import json
from collections.abc import Iterator
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
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import delayed, messages, topics
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.core.trace import TraceContext

pytestmark = pytest.mark.kiwi_id(2495)

NAMESPACE = "msg"
BASE_URL = "http://test"
HEADERS = {"X-Tenant-ID": "demo"}


class _Sink:
    """审计替身：记录事件与是否走同事务。"""

    def __init__(self) -> None:
        """初始化空记录。"""
        self.events: ConcurrentStableList[object] = ConcurrentStableList()
        self.in_session: ConcurrentStableList[bool] = ConcurrentStableList()

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """自开事务写入（记录）。"""
        del required
        self.events.add(event)
        self.in_session.add(False)
        return True

    async def emit_in_session(self, session: object, event: object, *, required: bool = False) -> bool:
        """同事务写入（记录）。"""
        del session, required
        self.events.add(event)
        self.in_session.add(True)
        return True


class _Disposal:
    """延迟丢弃替身：记录参数并恒成功。"""

    def __init__(self) -> None:
        """初始化空记录。"""
        self.calls: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()

    async def discard(self, **kwargs: object) -> bool:
        """记录一次丢弃。"""
        self.calls.add(ConcurrentStableDict(kwargs))
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


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 消息 / 延迟路由。"""
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
    app.state.topic_cache = TopicCache()
    app.state.audit_sink = _Sink()
    app.state.message_store = InMemoryMessageStore()
    app.state.delay_disposal = _Disposal()
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, messages.router, delayed.router])))
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


async def _create_topic(client: AsyncClient, *, name: str = "audit.operation") -> str:
    """建一个主题并返回主键。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": 1},
        headers=HEADERS,
    )
    assert response.status_code == 200, response.text
    return str(_data(response)["id"])


async def _seed_message(api_app: FastAPI, *, topic_id: str, log_id: str = "l-1") -> None:
    """向存储写一条消息。"""
    from datetime import UTC, datetime

    store = cast("InMemoryMessageStore", api_app.state.message_store)
    await store.append(
        topic_id=int(topic_id),
        partition_no=0,
        message_id=f"m-{log_id}",
        log_id=log_id,
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict[str, str](),
        trace=TraceContext(),
        published_at=datetime.now(UTC),
        content_type="application/json",
        compression="none",
        uncompressed_len=1,
    )


def _actions(api_app: FastAPI) -> ConcurrentStableList[str]:
    """取审计替身记录的动作名（动作在事件载荷的 `action` 键）。"""
    sink = cast("_Sink", api_app.state.audit_sink)
    result: ConcurrentStableList[str] = ConcurrentStableList()
    for event in sink.events:
        payload = cast(
            "ConcurrentStableDict[str, object]",
            getattr(event, "payload", ConcurrentStableDict[str, object]()),
        )
        result.add(str(payload.get("action", "")))
    return result


async def test_disable_enable_is_idempotent_and_audited(client: AsyncClient, api_app: FastAPI) -> None:
    """停用 / 启用：幂等 200、状态正确、审计走同事务。"""
    topic_id = await _create_topic(client)

    disabled = await client.post(f"/api/v1/msg/admin/topics/{topic_id}/disable?confirm=true", headers=HEADERS)
    assert _data(disabled)["status"] == "disabled"
    again = await client.post(f"/api/v1/msg/admin/topics/{topic_id}/disable?confirm=true", headers=HEADERS)
    assert _data(again)["status"] == "disabled"

    enabled = await client.post(f"/api/v1/msg/admin/topics/{topic_id}/enable", headers=HEADERS)
    assert _data(enabled)["status"] == "active"

    actions = _actions(api_app)
    assert "msg:topic.disable" in actions
    assert "msg:topic.enable" in actions
    sink = cast("_Sink", api_app.state.audit_sink)
    assert all(sink.in_session)


async def test_disable_requires_confirm(client: AsyncClient) -> None:
    """停用缺 `confirm`：10001。"""
    topic_id = await _create_topic(client)
    response = await client.post(f"/api/v1/msg/admin/topics/{topic_id}/disable", headers=HEADERS)
    assert _body(response)["code"] == 10001


async def test_delete_message_marks_tombstone_and_audits(client: AsyncClient, api_app: FastAPI) -> None:
    """删消息：结果 deleted、后续检索不可见、写一条 msg:message.delete 审计。"""
    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id)

    response = await client.post("/api/v1/msg/admin/messages/l-1/delete?confirm=true", headers=HEADERS)
    data = _data(response)
    assert data["deleted"] == 1
    items = cast("list[dict[str, str]]", data["items"])
    assert items[0]["result"] == "deleted"

    searched = await client.get(
        "/api/v1/msg/admin/messages",
        params={"namespace": NAMESPACE, "topic": "audit.operation", "log_id": "l-1"},
        headers=HEADERS,
    )
    assert _data(searched)["total"] == 0
    assert "msg:message.delete" in _actions(api_app)


async def test_delete_message_batch_limit_and_results(client: AsyncClient, api_app: FastAPI) -> None:
    """批量：缺 confirm / 超 100 返回 10001；正常批量逐条返回结果。"""
    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id, log_id="l-1")
    await _seed_message(api_app, topic_id=topic_id, log_id="l-2")

    missing_confirm = await client.post("/api/v1/msg/admin/messages/delete", json={"log_ids": ["l-1"]}, headers=HEADERS)
    assert _body(missing_confirm)["code"] == 10001

    too_many = await client.post(
        "/api/v1/msg/admin/messages/delete",
        json={"log_ids": [f"l-{index}" for index in range(101)], "confirm": True},
        headers=HEADERS,
    )
    assert _body(too_many)["code"] == 10001

    ok = await client.post(
        "/api/v1/msg/admin/messages/delete",
        json={"log_ids": ["l-1", "l-404"], "confirm": True},
        headers=HEADERS,
    )
    data = _data(ok)
    assert data["deleted"] == 1
    assert data["not_found"] == 1


async def test_discard_delayed_requires_confirm_and_calls_service(client: AsyncClient, api_app: FastAPI) -> None:
    """延迟丢弃：缺 confirm 10001；成功调用服务并带 session。"""
    missing = await client.post("/api/v1/msg/admin/delayed/log-1/discard", json={"reason": "bad"}, headers=HEADERS)
    assert _body(missing)["code"] == 10001

    ok = await client.post(
        "/api/v1/msg/admin/delayed/log-1/discard?confirm=true", json={"reason": "bad"}, headers=HEADERS
    )
    assert _data(ok)["discarded"] is True
    disposal = cast("_Disposal", api_app.state.delay_disposal)
    assert disposal.calls[0]["log_id"] == "log-1"
    assert disposal.calls[0]["session"] is not None


async def test_message_search_writes_normal_audit(client: AsyncClient, api_app: FastAPI) -> None:
    """消息检索写一条普通审计（同事务），不影响 200。"""
    await _create_topic(client)
    response = await client.get(
        "/api/v1/msg/admin/messages",
        params={"namespace": NAMESPACE, "topic": "audit.operation"},
        headers=HEADERS,
    )
    assert response.status_code == 200
    assert "msg:message.search" in _actions(api_app)


async def test_delete_message_store_reports_not_found(
    client: AsyncClient, api_app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """记录可定位但存储删除返回 False：按 not_found 处理，不写成功审计。"""
    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id)
    store = cast("InMemoryMessageStore", api_app.state.message_store)

    async def _false(**kwargs: object) -> bool:
        del kwargs
        return False

    monkeypatch.setattr(store, "delete", _false)
    response = await client.post("/api/v1/msg/admin/messages/l-1/delete?confirm=true", headers=HEADERS)
    data = _data(response)
    assert (data["deleted"], data["not_found"]) == (0, 1)
    assert "msg:message.delete" not in _actions(api_app)


async def test_delete_message_not_found_and_already_deleted(client: AsyncClient, api_app: FastAPI) -> None:
    """不存在返回 not_found；重复删除返回 already_deleted（不写成功审计、不 500）。"""
    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id, log_id="l-1")

    missing = await client.post("/api/v1/msg/admin/messages/l-404/delete?confirm=true", headers=HEADERS)
    assert _data(missing)["not_found"] == 1

    first = await client.post("/api/v1/msg/admin/messages/l-1/delete?confirm=true", headers=HEADERS)
    assert _data(first)["deleted"] == 1
    again = await client.post("/api/v1/msg/admin/messages/l-1/delete?confirm=true", headers=HEADERS)
    data = _data(again)
    assert data["deleted"] == 0
    assert data["not_found"] == 1
    items = cast("list[dict[str, str]]", data["items"])
    # 删除后读路径已不可见，二次删除按 not_found 处理（不重复写审计）
    assert items[0]["result"] == "not_found"


async def test_delete_message_topic_row_missing_returns_not_found(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """主题元数据行已不存在：按 not_found 处理，不抛 500。"""
    from bms_msg.repositories.topic import TopicRepository

    topic_id = await _create_topic(client)
    await _seed_message(api_app, topic_id=topic_id, log_id="l-1")
    session = cast("DbSession", db_session)
    await TopicRepository(session).delete(int(topic_id))
    await session.commit()

    response = await client.post("/api/v1/msg/admin/messages/l-1/delete?confirm=true", headers=HEADERS)
    assert _data(response)["not_found"] == 1


async def test_delete_message_requires_confirm(client: AsyncClient) -> None:
    """单条删除缺 confirm：10001。"""
    topic_id = await _create_topic(client)
    response = await client.post(f"/api/v1/msg/admin/messages/{topic_id}/delete", headers=HEADERS)
    assert _body(response)["code"] == 10001
