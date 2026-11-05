"""数据面延迟发布接口（msg 任务 01-5；Kiwi 2417）。

覆盖（详细设计 §7.1、§7.2，用例 #19~#22）：

- **#19 延迟发布响应**：`offset` 为 null、`deliver_at` ≈ 当前 + 档位、`log_id` 非空、
  首个延迟发布即启动推进任务；
- **#20 非法档位**：契约层拒绝（`10001`）；
- **#21 超容量**：延迟区软上限到顶（`350015` / 429）；
- **#22 到期前不可见**：先 `join` 再 `consume` → 空列表（延迟期不产生免成员的投递通道）。

装配方式同 01-2 / 01-4 的接口用例：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由，
平台库工作单元换内存会话，进程级状态（含延迟区与推进器）挂 `app.state`。
延迟推进器在用例结束时停掉，避免后台任务泄漏到下一个用例。
"""

import importlib
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
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
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, register_msg_error_mapper

pytestmark = pytest.mark.kiwi_id(2417)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

GROUP = "audit"
"""用例消费组。"""

CONSUMER = "c1"
"""用例成员（`consume` 须先加入并携带成员标识）。"""

BASE_URL = "http://test"
"""测试基址。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝（不声明 `plugin_name`，避免与占位实现撞自动登记）。"""

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
    """登记产品码覆盖并确保占位权限 / 租户模块门禁已导入（不显式登记占位实现）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（统一错误响应据此解析文案键与 HTTP 状态）。

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
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 数据面消息 / 成员路由 + 进程级状态。

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
    app.state.message_store = InMemoryMessageStore()
    app.state.offset_store = InMemoryOffsetStore()
    app.state.dead_letter_queue = InMemoryDeadLetterQueue()
    app.state.delivery_state = DeliveryState()
    app.state.group_store = InMemoryGroupStore()
    app.state.connection_registry = ConnectionRegistry()
    app.state.delay_store = InMemoryDelayStore()
    app.state.delay_scheduler = DelayScheduler(
        store=app.state.delay_store,
        messages=app.state.message_store,
        wheel=DelayWheel(),
    )
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(
        mount_service_routers(ConcurrentStableList([topics.router, messages.router, data_groups.router]))
    )
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。

        Returns:
            DbUnitOfWork: 绑定内存会话的工作单元。
        """
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    return app


@pytest.fixture(autouse=True)
async def _stop_scheduler(api_app: FastAPI) -> AsyncIterator[None]:
    """用例结束后停掉延迟推进任务（惰性启动的后台任务不跨用例泄漏）。

    Args:
        api_app: 应用实例。

    Yields:
        None: 用例运行期。
    """
    yield
    await cast("DelayScheduler", api_app.state.delay_scheduler).stop()


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


async def _create_topic(client: AsyncClient, *, partition_count: int = 1) -> Response:
    """经管理面创建一个主题。

    Args:
        client: 客户端。
        partition_count: 分区数。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": partition_count},
        headers=DEMO_HEADERS,
    )


async def _publish(client: AsyncClient, *, delay_level: object = "5s") -> Response:
    """发一次发布请求（缺省带 5s 档位）。

    Args:
        client: 客户端。
        delay_level: 延迟档位（`None` 为普通发布；非法值由契约层拒绝）。

    Returns:
        Response: 响应。
    """
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "cGF5bG9hZA==", "key": "order-1"}
    )
    if delay_level is not None:
        body.set("delay_level", delay_level)
    return await client.post("/api/v1/msg/publish", json=dict(body.items()), headers=DEMO_HEADERS)


async def _join(client: AsyncClient) -> Response:
    """加入消费组（`consume` 的前置）。

    Args:
        client: 客户端。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )


async def _consume(client: AsyncClient) -> Response:
    """拉取一批消息。

    Args:
        client: 客户端。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "max_messages": 10,
        },
        headers=DEMO_HEADERS,
    )


async def test_delayed_publish_returns_deliver_at(client: AsyncClient, api_app: FastAPI) -> None:
    """延迟发布响应：`offset` 为 null、`deliver_at` ≈ 当前 + 档位、`log_id` 非空，推进任务已启动。"""
    await _create_topic(client)
    before = datetime.now(UTC)
    response = await _publish(client, delay_level="5s")
    data = _data(response)
    deliver_at = datetime.fromisoformat(str(data["deliver_at"]))
    scheduler = cast("DelayScheduler", api_app.state.delay_scheduler)

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert data["offset"] is None
    assert (data["topic"], data["partition"]) == (TOPIC_NAME, 0)
    assert str(data["message_id"]).isdigit() and str(data["log_id"]).isdigit()
    assert before + timedelta(seconds=5) <= deliver_at <= datetime.now(UTC) + timedelta(seconds=5)
    assert scheduler.running is True


async def test_delayed_publish_rejects_invalid_level(client: AsyncClient) -> None:
    """非法档位：契约层拒绝（`10001`），不进业务逻辑。"""
    await _create_topic(client)
    response = await _publish(client, delay_level="7m")

    assert _body(response)["code"] == 10001


async def test_delayed_publish_over_quota_returns_350015(client: AsyncClient, api_app: FastAPI) -> None:
    """超容量：软上限到顶返回 `350015`（429），不静默丢弃。"""
    await _create_topic(client)
    store = cast("InMemoryDelayStore", api_app.state.delay_store)
    store.quota = 1
    await _publish(client, delay_level="1m")
    rejected = await _publish(client, delay_level="5m")

    assert (rejected.status_code, _body(rejected)["code"]) == (429, int(MsgErrorCode.DELAY_QUOTA_EXCEEDED))
    assert await store.count_for_tenant(tenant_id=7001) == 1


async def test_delayed_message_invisible_before_due(client: AsyncClient) -> None:
    """到期前不可见：先 `join` 再 `consume` → 空列表（延迟消息不在日志里，任何人拉不到）。"""
    await _create_topic(client)
    published = await _publish(client, delay_level="1m")
    joined = await _join(client)
    consumed = await _consume(client)

    assert published.status_code == 200
    assert joined.status_code == 200
    assert _data(consumed)["messages"] == []


async def test_permission_denied_blocks_delayed_publish(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """无 `msg:publish` 权限：延迟发布同样 403（30001），不因延迟分派绕过鉴权。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"

    response = await _publish(client, delay_level="5s")
    assert (response.status_code, _body(response)["code"]) == (403, 30001)
