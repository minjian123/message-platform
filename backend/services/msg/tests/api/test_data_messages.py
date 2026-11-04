"""数据面消息接口（msg 任务 01-2；Kiwi 2414）。

覆盖：`/api/v1/msg/publish`、`/msg/consume`、`/msg/ack` 三端点；`frame_tenant()` 取租户（请求体不接收
`tenant`）；权限码 `msg:publish` / `msg:consume` 拦截；载荷 base64 往返；`acks=0` 与缺省档行为可区分；
错误码 350001 / 350002 / 350003 / 350009 / 350011 / 350012 与契约校验 10001。

装配方式同 01-1 管理面用例：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由；
只把平台库工作单元换成内存库会话，并把数据面进程级状态挂到 `app.state`。
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

from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, register_msg_error_mapper

pytestmark = pytest.mark.kiwi_id(2414)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

GROUP = "audit"
"""用例消费组。"""

CONSUMER = "c1"
"""用例成员（01-4 起 `consume` / `ack` 须先加入并携带成员标识）。"""

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
    """最小应用：租户中间件 + 统一错误渲染 + 管理面 / 数据面路由 + 数据面进程级状态。

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


async def _create_topic(
    client: AsyncClient, *, name: str = TOPIC_NAME, partition_count: int = 1, tenant: str = "demo"
) -> Response:
    """经管理面创建一个主题。

    Args:
        client: 客户端。
        name: 主题名。
        partition_count: 分区数。
        tenant: 租户编码（`X-Tenant-ID`）。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": partition_count},
        headers={"X-Tenant-ID": tenant},
    )


async def _publish(
    client: AsyncClient,
    *,
    payload: str = "cGF5bG9hZA==",
    key: str | None = "order-1",
    name: str = TOPIC_NAME,
    tenant: str = "demo",
) -> Response:
    """发一次发布请求。

    Args:
        client: 客户端。
        payload: base64 载荷。
        key: 分区键。
        name: 主题名。
        tenant: 租户编码。

    Returns:
        Response: 响应。
    """
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"namespace": NAMESPACE, "topic": name, "payload": payload}
    )
    if key is not None:
        body.set("key", key)
    return await client.post("/api/v1/msg/publish", json=dict(body.items()), headers={"X-Tenant-ID": tenant})


async def _join(client: AsyncClient, *, consumer_id: str = CONSUMER, topic: str = TOPIC_NAME) -> Response:
    """加入消费组（01-4 起 `consume` / `ack` 的前置：成员制强约束）。

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


async def _consume(
    client: AsyncClient,
    *,
    max_messages: int = 10,
    acks: int = 1,
    partition: int = 0,
    tenant: str = "demo",
    consumer_id: str = CONSUMER,
) -> Response:
    """发一次拉取请求。

    Args:
        client: 客户端。
        max_messages: 拉取上限。
        acks: 投递档位。
        partition: 分区号。
        tenant: 租户编码。
        consumer_id: 成员标识（须已加入该组）。

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
            "max_messages": max_messages,
            "acks": acks,
        },
        headers={"X-Tenant-ID": tenant},
    )


async def _ack(client: AsyncClient, offset: int, *, partition: int = 0, consumer_id: str = CONSUMER) -> Response:
    """发一次确认请求。

    Args:
        client: 客户端。
        offset: 待确认 offset。
        partition: 分区号。
        consumer_id: 成员标识（须是该分区的当前持有者）。

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
            "partition": partition,
            "offset": offset,
        },
        headers=DEMO_HEADERS,
    )


def _messages(response: Response) -> list[dict[str, object]]:
    """取拉取响应中的消息数组。

    Args:
        response: 响应。

    Returns:
        list[dict[str, object]]: 消息列表。
    """
    return [cast("dict[str, object]", item) for item in cast("list[object]", _data(response)["messages"])]


async def test_publish_returns_partition_and_offset(client: AsyncClient) -> None:
    """发布：200 + `message_id` 字符串 / 分区在范围内 / offset 从 0 起。"""
    await _create_topic(client)
    response = await _publish(client)
    data = _data(response)
    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert isinstance(data["message_id"], str) and str(data["message_id"]).isdigit()
    assert (data["topic"], data["offset"], data["partition"]) == (TOPIC_NAME, 0, 0)


async def test_publish_takes_tenant_from_resolution(client: AsyncClient) -> None:
    """`tenant` 以解析结果为准：为 `other` 建的主题在 `demo` 下不可见（350001）。"""
    await _create_topic(client, tenant="other")
    published = await _publish(client, tenant="other")
    assert published.status_code == 200
    missing = await _publish(client, tenant="demo")
    assert (missing.status_code, _body(missing)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_publish_rejects_tenant_field_in_body(client: AsyncClient) -> None:
    """请求体携带 `tenant_id`：契约拒绝（`10001`），客户端自报租户不生效。"""
    await _create_topic(client)
    response = await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "", "tenant_id": 9999},
        headers=DEMO_HEADERS,
    )
    assert _body(response)["code"] == 10001


async def test_publish_rejects_invalid_base64(client: AsyncClient) -> None:
    """非法 base64：`10001`（形态属契约层）。"""
    await _create_topic(client)
    response = await _publish(client, payload="not-base64!!")
    assert _body(response)["code"] == 10001


async def test_publish_missing_topic_returns_350001(client: AsyncClient) -> None:
    """主题不存在：404 + `350001`。"""
    response = await _publish(client, name="audit.missing")
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_consume_and_ack_round_trip(client: AsyncClient) -> None:
    """拉取 → 确认：载荷 base64 往返、`delivery_count=1`、位点提交值 = offset + 1、确认后不再投。"""
    await _create_topic(client)
    await _join(client)
    created = _data(await _publish(client))
    consumed = await _consume(client)
    items = _messages(consumed)
    assert len(items) == 1
    assert (items[0]["message_id"], items[0]["payload"]) == (created["message_id"], "cGF5bG9hZA==")
    assert items[0]["delivery_count"] == 1

    acked = await _ack(client, 0)
    assert _data(acked)["committed_offset"] == 1
    assert _messages(await _consume(client)) == []


@pytest.mark.kiwi_id(2415)
async def test_consume_stops_at_unacked_head(client: AsyncClient) -> None:
    """顺序性（01-3）：队头未 ack 时再次拉取为空，ack 后继续投下一条（HTTP 层停等）。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    await _publish(client)
    first = await _consume(client, max_messages=1)
    assert [item["offset"] for item in _messages(first)] == [0]
    assert _messages(await _consume(client, max_messages=1)) == []
    assert _data(await _ack(client, 0))["committed_offset"] == 1
    second = await _consume(client, max_messages=1)
    assert [item["offset"] for item in _messages(second)] == [1]


async def test_consume_acks_zero_commits_without_ack(client: AsyncClient) -> None:
    """`acks=0`：不调 ack 也不重投（位点已前移）；缺省档在同一场景会重投（服务层用例覆盖）。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    delivered = await _consume(client, acks=0)
    assert [item["delivery_count"] for item in _messages(delivered)] == [1]
    assert _messages(await _consume(client)) == []


async def test_consume_partition_out_of_range_returns_350009(client: AsyncClient) -> None:
    """分区号越界：400 + `350009`（判定顺序：组 → 分区号 → 成员归属）。"""
    await _create_topic(client)
    await _join(client)
    response = await _consume(client, partition=9)
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))


async def test_ack_offset_beyond_leo_returns_350002(client: AsyncClient) -> None:
    """位点越界：400 + `350002`。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    response = await _ack(client, 9)
    assert (response.status_code, _body(response)["code"]) == (400, int(MsgErrorCode.OFFSET_INVALID))


async def test_ack_undelivered_offset_returns_350003(client: AsyncClient) -> None:
    """未投递过的 offset：404 + `350003`。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    await _publish(client)
    await _consume(client, max_messages=1)
    response = await _ack(client, 1)
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.MESSAGE_NOT_FOUND))


async def test_suspended_tenant_returns_350011(client: AsyncClient, tenant_source: object) -> None:
    """停用租户：403 + `350011`（中间件拒绝，经产品码覆盖改写）。"""
    await _create_topic(client)
    setattr(tenant_source, "suspended", True)  # noqa: B010  # 替身状态切换（夹具返回实现类实例）
    response = await _publish(client)
    assert (response.status_code, _body(response)["code"]) == (403, int(MsgErrorCode.TENANT_SUSPENDED))


async def test_unknown_tenant_returns_350012(client: AsyncClient) -> None:
    """不存在的租户：404 + `350012`（不泄露存在性）。"""
    response = await client.post(
        "/api/v1/msg/consume",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "partition": 0},
        headers={"X-Tenant-ID": "missing"},
    )
    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.TENANT_NOT_FOUND))


async def test_permission_denied_blocks_data_plane(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """无权限码：发布与拉取均 403（30001），错误响应不进入业务逻辑。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"
    published = await _publish(client)
    consumed = await _consume(client)
    assert (published.status_code, consumed.status_code) == (403, 403)
    assert (_body(published)["code"], _body(consumed)["code"]) == (30001, 30001)
