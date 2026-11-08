"""消费组成员接口（msg 任务 01-4；Kiwi 2416）。

覆盖：`/api/v1/msg/join`、`/msg/heartbeat`、`/msg/leave` 三端点的正常返回与字段；成员制对
`/msg/consume` / `/msg/ack` 的约束（组不存在 `350013`、分区非本成员 `350014`）；主题 / 命名空间错误码；
权限码拦截。

装配方式同 01-1 / 01-2 用例：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由；
只把平台库工作单元换成内存库会话，并把进程级状态（含组存储）挂到 `app.state`。超时 / 死信这类需要
推进时钟的路径由服务层用例覆盖（接口层不注入时钟，与 01-2 / 01-3 同处置）。
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
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.errors import MsgErrorCode
from tests_support.frame_app import NoopAuditSink, install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2416)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

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
    # 05-4：建主题显式强审计的可写出口（不断言内容）。
    app.state.audit_sink = NoopAuditSink()
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


async def _create_topic(client: AsyncClient, *, partition_count: int = 2) -> Response:
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


async def _join(client: AsyncClient, *, consumer_id: str = "c1", topic: str = TOPIC_NAME) -> Response:
    """发一次加入请求。

    Args:
        client: 客户端。
        consumer_id: 成员标识。
        topic: 主题名。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": topic, "consumer_id": consumer_id},
        headers=DEMO_HEADERS,
    )


async def _heartbeat(client: AsyncClient, *, consumer_id: str = "c1") -> Response:
    """发一次心跳请求。

    Args:
        client: 客户端。
        consumer_id: 成员标识。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/heartbeat",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": consumer_id},
        headers=DEMO_HEADERS,
    )


async def _leave(client: AsyncClient, *, consumer_id: str = "c1") -> Response:
    """发一次离开请求。

    Args:
        client: 客户端。
        consumer_id: 成员标识。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/leave",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": consumer_id},
        headers=DEMO_HEADERS,
    )


async def _publish(client: AsyncClient) -> Response:
    """发一条消息（无 key，按轮询落分区）。

    Args:
        client: 客户端。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "cGF5bG9hZA=="},
        headers=DEMO_HEADERS,
    )


async def _consume(client: AsyncClient, *, consumer_id: str = "c1", partition: int = 0) -> Response:
    """发一次拉取请求。

    Args:
        client: 客户端。
        consumer_id: 成员标识。
        partition: 分区号。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": consumer_id,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": partition,
            "max_messages": 10,
            "max_wait_ms": 0,
        },
        headers=DEMO_HEADERS,
    )


async def _ack(client: AsyncClient, *, consumer_id: str = "c1", offset: int = 0) -> Response:
    """发一次确认请求（分区 0）。

    Args:
        client: 客户端。
        consumer_id: 成员标识。
        offset: 待确认 offset。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/ack",
        json={
            "group": GROUP,
            "consumer_id": consumer_id,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "offset": offset,
        },
        headers=DEMO_HEADERS,
    )


async def test_join_returns_assignment(client: AsyncClient) -> None:
    """加入：200 + 世代 1 / 状态 stable / 分到全部分区 / 回传心跳与会话超时参数。"""
    await _create_topic(client)
    response = await _join(client)
    data = _data(response)
    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert (data["generation"], data["state"], data["member_count"]) == (1, "stable", 1)
    assert list(cast("list[int]", data["partitions"])) == [0, 1]
    assert (data["heartbeat_interval_ms"], data["session_timeout_ms"]) == (10_000, 30_000)


async def test_join_second_member_rebalances(client: AsyncClient) -> None:
    """第二成员加入：世代 2、两人分区不重叠；同 id 再 join 不改世代（重连）。"""
    await _create_topic(client)
    first = _data(await _join(client, consumer_id="c1"))
    second = _data(await _join(client, consumer_id="c2"))
    assert (first["generation"], list(cast("list[int]", first["partitions"]))) == (1, [0, 1])
    assert (second["generation"], list(cast("list[int]", second["partitions"]))) == (2, [1])
    again = _data(await _join(client, consumer_id="c1"))
    assert (again["generation"], list(cast("list[int]", again["partitions"]))) == (2, [0])


async def test_heartbeat_returns_current_assignment(client: AsyncClient) -> None:
    """心跳：200 + 世代与分配与 join 一致（心跳独立于拉取，处理慢不影响存活判定）。"""
    await _create_topic(client)
    joined = _data(await _join(client))
    beat = _data(await _heartbeat(client))
    assert (beat["generation"], list(cast("list[int]", beat["partitions"]))) == (
        joined["generation"],
        list(cast("list[int]", joined["partitions"])),
    )


async def test_leave_removes_member(client: AsyncClient) -> None:
    """离开：200 + 单成员组转 empty、世代 +1。"""
    await _create_topic(client)
    await _join(client)
    response = await _leave(client)
    data = _data(response)
    assert (data["state"], data["generation"]) == ("empty", 2)


async def test_join_missing_topic_returns_350001(client: AsyncClient) -> None:
    """主题不存在：404 + `350001`。"""
    response = await _join(client, topic="audit.missing")
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_heartbeat_unknown_group_returns_350013(client: AsyncClient) -> None:
    """未加入即心跳：404 + `350013`。"""
    await _create_topic(client)
    response = await _heartbeat(client)
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.GROUP_NOT_FOUND))


async def test_consume_requires_membership(client: AsyncClient) -> None:
    """不加入直接拉取：404 + `350013`（拼错组名不再静默产生新位点）。"""
    await _create_topic(client)
    await _publish(client)
    response = await _consume(client)
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.GROUP_NOT_FOUND))


async def test_consume_rejects_foreign_partition(client: AsyncClient) -> None:
    """分区不在该成员分配内：409 + `350014`（组内不重复分配的运行期约束）。"""
    await _create_topic(client)
    await _publish(client)
    await _join(client, consumer_id="c1")
    await _join(client, consumer_id="c2")
    response = await _consume(client, consumer_id="c1", partition=1)
    assert (response.status_code, _body(response)["code"]) == (409, int(MsgErrorCode.MEMBER_INVALID))
    allowed = await _consume(client, consumer_id="c1", partition=0)
    assert allowed.status_code == 200


async def test_ack_rejects_unknown_member(client: AsyncClient) -> None:
    """成员不在组内即确认：409 + `350014`。"""
    await _create_topic(client)
    await _publish(client)
    await _join(client, consumer_id="c1")
    await _consume(client, consumer_id="c1", partition=0)
    response = await _ack(client, consumer_id="c9", offset=0)
    assert (response.status_code, _body(response)["code"]) == (409, int(MsgErrorCode.MEMBER_INVALID))


async def test_consume_partition_out_of_range_returns_350009(client: AsyncClient) -> None:
    """分区号越界先于归属判定：400 + `350009`（判定顺序：组 → 分区号 → 成员）。"""
    await _create_topic(client)
    await _join(client)
    response = await _consume(client, partition=9)
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))


async def test_permission_denied_blocks_group_endpoints(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """无 `msg:consume` 权限：三端点均 403（30001），不进入业务逻辑。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"
    joined = await _join(client)
    beat = await _heartbeat(client)
    left = await _leave(client)
    assert (joined.status_code, beat.status_code, left.status_code) == (403, 403, 403)
    assert (_body(joined)["code"], _body(beat)["code"], _body(left)["code"]) == (30001, 30001, 30001)
