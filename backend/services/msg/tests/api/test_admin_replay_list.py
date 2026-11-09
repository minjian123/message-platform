"""重放任务列表接口（任务 06-6-1；Kiwi 2468）。

覆盖（详细设计 §6）：

- 空态；`status` / `topic` / `namespace` / 创建时间区间过滤；
- 分页（`total` 为过滤前计数）与 `page` 越界 `10001`、非法状态 `10001`；
- 主题 `describe` 权限过滤 items（`total` 不变）；
- 租户隔离（他租户看不到本租户任务）；
- 列表项字段与详情一致，影响段固定为 null。

装配同 01-6 接口用例：最小应用 + 真实租户中间件 + 统一错误渲染 + 真实模块路由；平台库工作单元换内存
会话，进程级状态（消息 / 位点 / 组存储 / 审计出口 / 执行器）挂 `app.state`。执行器周期放大到 10s，
避免后台推进改变断言用的状态。
"""

import importlib
import json
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
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

from bms_msg.api.admin import replay as admin_replay
from bms_msg.api.admin import topics
from bms_msg.api.deps import get_topic_acl_service
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner
from bms_msg.errors import MsgErrorCode
from tests_support.frame_app import install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2468)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源）。"""

NAMESPACE = "msg"
"""可用命名空间。"""

TOPIC_ONE = "audit.operation"
"""用例主题名（默认）。"""

TOPIC_TWO = "audit.security"
"""第二个主题名（ACL 过滤用例）。"""

BASE_URL = "http://test"
"""测试基址。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""

OTHER_HEADERS = {"X-Tenant-ID": "other"}
"""另一租户头（隔离断言）。"""


class RecordingSink:
    """审计出口替身（只收事件，不校验）。"""

    def __init__(self) -> None:
        """初始化。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录事件。

        Args:
            event: 审计事件信封。
            required: 强审计标记（替身忽略）。

        Returns:
            bool: True。
        """
        del required
        self.events.add(event)
        return True


class FilteredAcl:
    """主题 ACL 替身：按 `denied` 拒绝 describe。"""

    def __init__(self, denied: ConcurrentStableList[str]) -> None:
        """初始化。

        Args:
            denied: 不可见主题名。
        """
        self._denied = denied

    async def authorize(self, **scope: object) -> None:
        """恒放行（写操作不在本用例范围）。

        Args:
            scope: 作用域（忽略）。
        """
        del scope

    async def filter_allowed(self, *, topics: ConcurrentStableList[str], **scope: object) -> ConcurrentStableList[str]:
        """过滤不可见主题。

        Args:
            topics: 候选主题名。
            scope: 作用域（忽略）。

        Returns:
            ConcurrentStableList[str]: 允许的主题名。
        """
        del scope
        return ConcurrentStableList(item for item in topics if item not in self._denied)


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记占位权限 / 租户模块门禁。

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
    """装载产品清单的错误码声明。

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
    """最小应用（列表 / 建任务 / 详情路由 + 进程级状态）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
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
    app.state.dead_letter_queue = InMemoryDeadLetterQueue()
    app.state.delivery_state = DeliveryState()
    sink = RecordingSink()
    app.state.audit_sink = sink
    session = cast("DbSession", db_session)

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[DbUnitOfWork]:
        """执行器用工作单元（复用内存会话）。

        Yields:
            DbUnitOfWork: 绑定内存会话的工作单元。
        """
        yield DbUnitOfWork(session)

    app.state.replay_runner = ReplayRunner(
        messages=app.state.message_store,
        uow_factory=lambda: _uow(),
        auditor=ReplayAuditor(sink=sink),
        interval_ms=10_000,
    )
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, admin_replay.router])))
    app.dependency_overrides[get_topic_acl_service] = lambda: FilteredAcl(ConcurrentStableList())

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（复用内存会话）。

        Returns:
            DbUnitOfWork: 绑定内存会话的工作单元。
        """
        return DbUnitOfWork(session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    return app


@pytest.fixture(autouse=True)
async def _stop_runner(api_app: FastAPI) -> AsyncIterator[None]:
    """用例结束后停掉重放执行器。

    Args:
        api_app: 应用实例。

    Yields:
        None: 用例运行期。
    """
    yield
    await cast("ReplayRunner", api_app.state.replay_runner).stop()


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


def _items(response: Response) -> ConcurrentStableList[ConcurrentStableDict[str, object]]:
    """取列表 `items` 段。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, object]]: 列表项。
    """
    return ConcurrentStableList(
        ConcurrentStableDict(item) for item in cast("list[dict[str, object]]", _data(response)["items"])
    )


async def _create_topic(client: AsyncClient, *, name: str = TOPIC_ONE, partition_count: int = 2) -> int:
    """建主题并返回主键。

    Args:
        client: 客户端。
        name: 主题名。
        partition_count: 分区数。

    Returns:
        int: 主题主键。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": partition_count},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
    return int(cast("str", _data(response)["id"]))


async def _create_task(client: AsyncClient, *, topic: str = TOPIC_ONE) -> str:
    """建一个重放任务并返回任务 ID。

    Args:
        client: 客户端。
        topic: 主题名。

    Returns:
        str: 任务 ID。
    """
    response = await client.post(
        "/api/v1/msg/admin/replay",
        json={"namespace": NAMESPACE, "topic": topic, "partitions": [0], "confirm": True},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
    return cast("str", _data(response)["task_id"])


async def _list(client: AsyncClient, headers: dict[str, str] | None = None, **params: object) -> Response:
    """发列表请求。

    Args:
        client: 客户端。
        headers: 请求头（缺省 demo 租户）。
        **params: 查询参数。

    Returns:
        Response: 响应。
    """
    return await client.get(
        "/api/v1/msg/admin/replay",
        params=cast("dict[str, str]", params) or None,
        headers=DEMO_HEADERS if headers is None else headers,
    )


async def test_list_empty(client: AsyncClient) -> None:
    """空态：无任务时 items 空、total 0、分页参数回显。"""
    response = await _list(client)
    data = _data(response)

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert (data["items"], data["total"], data["page"], data["size"]) == ([], 0, 1, 20)


async def test_list_returns_summary_without_impact(client: AsyncClient) -> None:
    """列表项字段与详情一致，但影响段固定为 null。"""
    await _create_topic(client)
    task_id = await _create_task(client)

    items = _items(await _list(client))

    assert len(items) == 1
    assert (items[0]["task_id"], items[0]["topic"], items[0]["status"]) == (task_id, TOPIC_ONE, "pending")
    assert (items[0]["affected_groups"], items[0]["affected_truncated"]) == (None, False)
    assert items[0]["total"] == 0


async def test_list_filter_by_status_and_topic(client: AsyncClient) -> None:
    """状态与主题过滤相与；已取消任务只在 `status=cancelled` 命中。"""
    await _create_topic(client, name=TOPIC_ONE)
    await _create_topic(client, name=TOPIC_TWO)
    pending_id = await _create_task(client, topic=TOPIC_ONE)
    cancelled_head = await _create_task(client, topic=TOPIC_TWO)
    cancelled = await client.post(f"/api/v1/msg/admin/replay/{cancelled_head}/cancel", headers=DEMO_HEADERS)
    assert cancelled.status_code == 200

    by_status = _items(await _list(client, status="cancelled"))
    by_topic = _items(await _list(client, topic=TOPIC_ONE))
    both = _items(await _list(client, status="cancelled", topic=TOPIC_ONE))

    assert [item["task_id"] for item in by_status] == [cancelled_head]
    assert [item["task_id"] for item in by_topic] == [pending_id]
    assert both == []


async def test_list_filter_by_namespace_and_created_range(client: AsyncClient) -> None:
    """命名空间过滤命中；创建时间区间左闭右开（结束时刻不含）。"""
    await _create_topic(client)
    task_id = await _create_task(client)
    now = datetime.now(UTC)

    hit = _items(await _list(client, namespace=NAMESPACE, created_from=(now - timedelta(days=1)).isoformat()))
    miss = _items(await _list(client, created_to=(now - timedelta(days=1)).isoformat()))

    assert [item["task_id"] for item in hit] == [task_id]
    assert miss == []


async def test_list_pagination(client: AsyncClient) -> None:
    """分页：3 条任务按 size=2 分成两页，total 恒为 3。"""
    await _create_topic(client)
    created = ConcurrentStableList([await _create_task(client) for _ in range(3)])

    first = await _list(client, page=1, size=2)
    second = await _list(client, page=2, size=2)

    assert ([item["task_id"] for item in _items(first)], _data(first)["total"]) == (list(reversed(created))[:2], 3)
    assert ([item["task_id"] for item in _items(second)], _data(second)["total"]) == (list(reversed(created))[2:], 3)


async def test_list_rejects_illegal_params(client: AsyncClient) -> None:
    """非法状态 / 页码 / 每页条数 / 区间颠倒 → 10001。"""
    await _create_topic(client)
    now = datetime.now(UTC)

    unknown_status = await _list(client, status="unknown")
    zero_page = await _list(client, page=0)
    huge_size = await _list(client, size=101)
    reversed_window = await _list(
        client, created_from=now.isoformat(), created_to=(now - timedelta(days=1)).isoformat()
    )

    assert _body(unknown_status)["code"] == 10001
    assert _body(zero_page)["code"] == 10001
    assert _body(huge_size)["code"] == 10001
    assert _body(reversed_window)["code"] == 10001


async def test_list_filters_items_by_topic_acl(client: AsyncClient, api_app: FastAPI) -> None:
    """无 describe 权限的主题任务不出现在 items，但 total 仍是过滤前计数。"""
    await _create_topic(client, name=TOPIC_ONE)
    await _create_topic(client, name=TOPIC_TWO)
    await _create_task(client, topic=TOPIC_ONE)
    await _create_task(client, topic=TOPIC_TWO)
    api_app.dependency_overrides[get_topic_acl_service] = lambda: FilteredAcl(ConcurrentStableList([TOPIC_TWO]))

    response = await _list(client)
    data = _data(response)

    assert [item["topic"] for item in _items(response)] == [TOPIC_ONE]
    assert data["total"] == 2


async def test_list_is_tenant_scoped(client: AsyncClient) -> None:
    """他租户看不到本租户的任务。"""
    await _create_topic(client)
    await _create_task(client)

    response = await _list(client, headers=OTHER_HEADERS)

    assert (_items(response), _data(response)["total"]) == ([], 0)


async def test_list_rejects_suspended_tenant(client: AsyncClient, tenant_source: object) -> None:
    """租户停用 → 列表拒绝（350011）。"""
    await _create_topic(client)
    cast("SimpleNamespace", tenant_source).suspended = True

    response = await _list(client)

    assert _body(response)["code"] == int(MsgErrorCode.TENANT_SUSPENDED)
