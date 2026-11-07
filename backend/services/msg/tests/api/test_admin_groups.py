"""消费组管理接口（msg 任务 01-4；Kiwi 2416）。

覆盖：`GET /api/v1/msg/admin/groups` 列表（过滤 / 分页 / 孤儿组记录跳过）与
`GET /msg/admin/groups/{namespace}/{topic}/{group}` 详情（状态 / 世代 / 成员 / 分配 / 各分区位点与积压）；
错误码 `350001` / `350010` / `350013` 与权限码 `msg:query` 拦截。

装配方式同 01-1 管理面用例：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由，
平台库工作单元换内存会话，进程级状态（含组存储）挂 `app.state`。
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

from bms_msg.api.admin import groups as admin_groups
from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import GROUP_STATE_STABLE, ConsumerGroup, InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.errors import MsgErrorCode
from bms_msg.tenant.context import CurrentTenant
from tests_support.frame_app import install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2416)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

OTHER_TOPIC = "audit.other"
"""第二个用例主题名（过滤与分页用）。"""

GROUP = "audit"
"""用例消费组。"""

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
    """最小应用：租户中间件 + 统一错误渲染 + 四组路由 + 进程级状态。

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
        mount_service_routers(
            ConcurrentStableList([topics.router, messages.router, data_groups.router, admin_groups.router])
        )
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


def _items(response: Response) -> list[dict[str, object]]:
    """取列表响应中的 `items`。

    Args:
        response: 响应。

    Returns:
        list[dict[str, object]]: 列表项。
    """
    return [cast("dict[str, object]", item) for item in cast("list[object]", _data(response)["items"])]


async def _create_topic(client: AsyncClient, *, name: str = TOPIC_NAME, partition_count: int = 2) -> Response:
    """经管理面创建一个主题。

    Args:
        client: 客户端。
        name: 主题名。
        partition_count: 分区数。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": partition_count},
        headers=DEMO_HEADERS,
    )


async def _join(client: AsyncClient, *, group: str = GROUP, topic: str = TOPIC_NAME) -> Response:
    """加入组（成员 `c1`）。

    Args:
        client: 客户端。
        group: 组名。
        topic: 主题名。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/join",
        json={"group": group, "namespace": NAMESPACE, "topic": topic, "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )


async def _list(client: AsyncClient, query: str = "") -> Response:
    """取组列表。

    Args:
        client: 客户端。
        query: 查询串（含 `?`）。

    Returns:
        Response: 响应。
    """
    return await client.get(f"/api/v1/msg/admin/groups{query}", headers=DEMO_HEADERS)


async def _detail(client: AsyncClient, *, group: str = GROUP, topic: str = TOPIC_NAME) -> Response:
    """取组详情。

    Args:
        client: 客户端。
        group: 组名。
        topic: 主题名。

    Returns:
        Response: 响应。
    """
    return await client.get(
        f"/api/v1/msg/admin/groups/{NAMESPACE}/{topic}/{group}",
        headers=DEMO_HEADERS,
    )


async def test_list_groups(client: AsyncClient) -> None:
    """列表：200 + 一行（状态 / 世代 / 成员数 / 已分配分区数）+ 过滤后总数。"""
    await _create_topic(client)
    await _join(client)
    data = _data(await _list(client))
    items = _items(await _list(client))
    assert data["total"] == 1
    assert (items[0]["group"], items[0]["namespace"], items[0]["topic"]) == (GROUP, NAMESPACE, TOPIC_NAME)
    assert (items[0]["state"], items[0]["generation"]) == ("stable", 1)
    assert (items[0]["member_count"], items[0]["assigned_partitions"]) == (1, 2)


async def test_list_groups_filters(client: AsyncClient) -> None:
    """过滤：按命名空间命中 / 未命中、按主题名命中 / 未命中。"""
    await _create_topic(client)
    await _create_topic(client, name=OTHER_TOPIC)
    await _join(client)
    await _join(client, group="notify", topic=OTHER_TOPIC)
    assert _data(await _list(client, "?namespace=msg"))["total"] == 2
    assert _data(await _list(client, f"?topic={TOPIC_NAME}"))["total"] == 1
    assert _data(await _list(client, "?topic=audit.missing"))["total"] == 0
    assert [item["group"] for item in _items(await _list(client))] == [GROUP, "notify"]


async def test_list_groups_paginates(client: AsyncClient) -> None:
    """分页：`size=1` 取两页，`total` 恒为过滤后总数，页序按组名升序。"""
    await _create_topic(client)
    await _create_topic(client, name=OTHER_TOPIC)
    await _join(client)
    await _join(client, group="notify", topic=OTHER_TOPIC)
    first = _data(await _list(client, "?page=1&size=1"))
    second = _data(await _list(client, "?page=2&size=1"))
    first_groups = [cast("dict[str, object]", item)["group"] for item in cast("list[object]", first["items"])]
    second_groups = [cast("dict[str, object]", item)["group"] for item in cast("list[object]", second["items"])]
    assert (first["total"], second["total"]) == (2, 2)
    assert (first_groups, second_groups) == ([GROUP], ["notify"])
    assert (first["page"], second["page"], first["size"]) == (1, 2, 1)


async def test_list_skips_group_of_deleted_topic(
    client: AsyncClient, api_app: FastAPI, current_tenant: CurrentTenant
) -> None:
    """孤儿组记录（主题已不存在）不展示：避免管理面把失效记录当成有效组。"""
    await _create_topic(client)
    await _join(client)
    store = cast("InMemoryGroupStore", api_app.state.group_store)
    await store.save(
        ConsumerGroup(
            tenant_id=int(current_tenant.tenant_id),
            topic_id=999_999,
            group="ghost",
            state=GROUP_STATE_STABLE,
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    assert [item["group"] for item in _items(await _list(client))] == [GROUP]


async def test_list_invalid_namespace_returns_350010(client: AsyncClient) -> None:
    """命名空间过滤值非法：400 + `350010`。"""
    response = await _list(client, "?namespace=nope")
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.NAMESPACE_INVALID))


async def test_describe_group_returns_partitions(client: AsyncClient) -> None:
    """详情：200 + 世代 / 成员 / 每分区 `owner` / `committed` / `leo` / `lag`（位点与积压）。"""
    await _create_topic(client)
    await _join(client)
    await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "cGF5bG9hZA=="},
        headers=DEMO_HEADERS,
    )
    data = _data(await _detail(client))
    assert (data["state"], data["generation"], data["partition_count"]) == ("stable", 1, 2)
    members = [cast("dict[str, object]", item) for item in cast("list[object]", data["members"])]
    assert [(item["consumer_id"], list(cast("list[int]", item["partitions"]))) for item in members] == [("c1", [0, 1])]
    partitions = [cast("dict[str, object]", item) for item in cast("list[object]", data["partitions"])]
    assert [item["owner"] for item in partitions] == ["c1", "c1"]
    assert sum(cast("int", item["leo"]) for item in partitions) == 1
    assert all(
        cast("int", item["lag"]) == cast("int", item["leo"]) - cast("int", item["committed"]) for item in partitions
    )


async def test_describe_empty_group_has_no_owner(client: AsyncClient) -> None:
    """空组详情：`state=empty`、分区无持有者（排障时一眼看出「有积压但无人消费」）。"""
    await _create_topic(client)
    await _join(client)
    await client.post(
        "/api/v1/msg/leave",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )
    data = _data(await _detail(client))
    partitions = [cast("dict[str, object]", item) for item in cast("list[object]", data["partitions"])]
    assert data["state"] == "empty"
    assert [item["owner"] for item in partitions] == [None, None]
    assert data["members"] == []


async def test_describe_missing_group_returns_350013(client: AsyncClient) -> None:
    """组不存在：404 + `350013`。"""
    await _create_topic(client)
    response = await _detail(client, group="ghost")
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.GROUP_NOT_FOUND))


async def test_describe_missing_topic_returns_350001(client: AsyncClient) -> None:
    """主题不存在：404 + `350001`。"""
    response = await _detail(client, topic="audit.missing")
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_permission_denied_blocks_group_admin(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """无 `msg:query` 权限：列表与详情均 403（30001）。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"
    listed = await _list(client)
    detail = await _detail(client)
    assert (listed.status_code, detail.status_code) == (403, 403)
    assert (_body(listed)["code"], _body(detail)["code"]) == (30001, 30001)
