"""待投列表只读视图（msg 任务 01-5；Kiwi 2417）。

覆盖（详细设计 §7.3，用例 #23~#24）：

- **#23 待投列表**：过滤（命名空间 / 主题 / 分区 / 档位）、分页、`total`、`remaining_ms`、
  到期时刻升序；
- **#24 待投列表错误**：命名空间非法 `350010`、主题不存在 `350001`、分区越界 `350009`、
  档位非法 `10001`、`size` 越界 `10001`。

装配同 01-4 的组只读视图用例；延迟消息经数据面发布（走同一条发布链路，不另造记录）。
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

from bms_msg.api.admin import delayed as admin_delayed
from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.core.routing import select_partition
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, register_msg_error_mapper

pytestmark = pytest.mark.kiwi_id(2417)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_A = "audit.operation"
"""用例主题（两分区）。"""

TOPIC_B = "audit.notify"
"""第二个用例主题（过滤用）。"""

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
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 数据面消息 / 待投视图路由 + 进程级状态。

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
        mount_service_routers(
            ConcurrentStableList([topics.router, messages.router, data_groups.router, admin_delayed.router])
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


def _items(response: Response) -> list[dict[str, object]]:
    """取列表响应中的 `items`。

    Args:
        response: 响应。

    Returns:
        list[dict[str, object]]: 列表项。
    """
    return [cast("dict[str, object]", item) for item in cast("list[object]", _data(response)["items"])]


async def _create_topic(client: AsyncClient, *, name: str = TOPIC_A, partition_count: int = 2) -> Response:
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


async def _publish(client: AsyncClient, *, name: str = TOPIC_A, key: str, delay_level: str = "1m") -> Response:
    """发一次延迟发布请求。

    Args:
        client: 客户端。
        name: 主题名。
        key: 分区键（决定落哪个分区）。
        delay_level: 延迟档位。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/publish",
        json={
            "namespace": NAMESPACE,
            "topic": name,
            "key": key,
            "payload": "cGF5bG9hZA==",
            "delay_level": delay_level,
        },
        headers=DEMO_HEADERS,
    )


async def _list(client: AsyncClient, query: str = "") -> Response:
    """取待投列表。

    Args:
        client: 客户端。
        query: 查询串（含 `?`）。

    Returns:
        Response: 响应。
    """
    return await client.get(f"/api/v1/msg/admin/delayed{query}", headers=DEMO_HEADERS)


def _key_for(partition_no: int) -> str:
    """找一个落在指定分区的分区键（两分区主题）。

    Args:
        partition_no: 目标分区号。

    Returns:
        str: 分区键。
    """
    return next(key for key in (f"k{index}" for index in range(100)) if select_partition(key, 2) == partition_no)


async def test_list_delayed_filters_and_paginates(client: AsyncClient) -> None:
    """待投列表：过滤（命名空间 / 主题 / 分区 / 档位）、分页、`total`、`remaining_ms`、到期升序。"""
    await _create_topic(client)
    await _create_topic(client, name=TOPIC_B)
    await _publish(client, key=_key_for(0), delay_level="1m")
    await _publish(client, key=_key_for(1), delay_level="5m")
    await _publish(client, name=TOPIC_B, key=_key_for(0), delay_level="1m")

    everything = _data(await _list(client))
    items = _items(await _list(client))
    assert everything["total"] == 3
    assert [item["topic"] for item in items] == [TOPIC_A, TOPIC_B, TOPIC_A]
    assert [item["level"] for item in items] == ["1m", "1m", "5m"]
    assert all(cast("int", item["remaining_ms"]) > 0 for item in items)
    deliver_ats = [datetime.fromisoformat(str(item["deliver_at"])) for item in items]
    assert deliver_ats == sorted(deliver_ats)
    assert {"log_id", "message_id", "namespace", "topic", "partition", "level", "deliver_at", "remaining_ms"} == set(
        items[0]
    )

    assert _data(await _list(client, "?namespace=msg"))["total"] == 3
    assert _data(await _list(client, f"?topic={TOPIC_B}"))["total"] == 1
    assert _data(await _list(client, "?level=5m"))["total"] == 1
    assert _data(await _list(client, "?partition=1"))["total"] == 1
    assert _data(await _list(client, f"?namespace=msg&topic={TOPIC_A}&level=1m"))["total"] == 1

    second_page = _data(await _list(client, "?page=2&size=2"))
    assert (second_page["total"], second_page["page"], second_page["size"]) == (3, 2, 2)
    assert [cast("dict[str, object]", item)["topic"] for item in cast("list[object]", second_page["items"])] == [
        TOPIC_A
    ]


async def test_list_delayed_remaining_matches_level(client: AsyncClient) -> None:
    """剩余时间与档位一致（1m 档 ≈ 60000ms，容许请求耗时带来的少量收缩）。"""
    await _create_topic(client)
    await _publish(client, key=_key_for(0), delay_level="1m")
    remaining = cast("int", _items(await _list(client))[0]["remaining_ms"])

    assert 59_000 < remaining <= 60_000


async def test_list_delayed_errors(client: AsyncClient) -> None:
    """待投列表错误：命名空间 `350010`、主题 `350001`、分区越界 `350009`、档位与 `size` `10001`。"""
    await _create_topic(client)

    invalid_namespace = await _list(client, "?namespace=nope")
    missing_topic = await _list(client, "?namespace=msg&topic=audit.missing")
    invalid_partition = await _list(client, f"?namespace=msg&topic={TOPIC_A}&partition=9")
    invalid_level = await _list(client, "?level=7m")
    invalid_size = await _list(client, "?size=101")

    assert (invalid_namespace.status_code, _body(invalid_namespace)["code"]) == (
        400,
        int(MsgErrorCode.NAMESPACE_INVALID),
    )
    assert (missing_topic.status_code, _body(missing_topic)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))
    assert (invalid_partition.status_code, _body(invalid_partition)["code"]) == (
        400,
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
    )
    assert (_body(invalid_level)["code"], _body(invalid_size)["code"]) == (10001, 10001)


async def test_permission_denied_blocks_delayed_view(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """无 `msg:query` 权限：待投列表 403（30001）。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"

    response = await _list(client)
    assert (response.status_code, _body(response)["code"]) == (403, 30001)


async def test_list_delayed_stays_empty_without_publish(client: AsyncClient) -> None:
    """无延迟消息时列表为空（`total=0`，不报错），且不因查询而启动推进任务。"""
    await _create_topic(client)
    data = _data(await _list(client))

    assert (data["total"], data["items"]) == (0, [])


async def test_remaining_zero_for_overdue_record(client: AsyncClient, api_app: FastAPI) -> None:
    """已到期未转投的记录剩余时间为 0（不为负，视图不承担转投职责）。"""
    store = cast("InMemoryDelayStore", api_app.state.delay_store)
    published_at = datetime.now(UTC) - timedelta(seconds=90)
    await store.enqueue(
        DelayRecord(
            log_id="log-overdue",
            message_id="msg-overdue",
            tenant_id=7001,
            topic_id=1,
            namespace=NAMESPACE,
            topic=TOPIC_A,
            partition_no=0,
            delay_level="1m",
            key=None,
            payload=b"payload",
            headers=ConcurrentStableDict(),
            trace_id=None,
            published_at=published_at,
            deliver_at=published_at + timedelta(seconds=60),
        )
    )
    items = _items(await _list(client))

    assert [(item["log_id"], item["remaining_ms"]) for item in items] == [("log-overdue", 0)]
