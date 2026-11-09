"""主题与位点写操作端点（msg 任务 06-2-1；Kiwi 2465）。

覆盖：删主题受理（confirm / 幂等 / 状态机 / 清理进度）、影响预检、扩分区（只增）、改保留期（部分更新）、
位点重置（确认 / 越界 / 组不存在 / 状态）、权限拦截、重放创建的显式确认。

装配方式与 `test_admin_topics.py` 同构：最小应用 + 基座真实租户中间件 + 统一错误渲染 + 真实模块路由；
平台库工作单元换成内存库会话，进程级端口用内存实现；删主题的清理编排用替身（真编排见
`tests/services/test_topic_cleanup.py`）。
"""

import importlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.api.base import mount_service_routers
from bms_core.api.errors import register_exception_handlers
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ConfigError
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.permission.base import BasePermissionChecker
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import offsets, replay, topics
from bms_msg.api.deps import get_topic_cleanup_service
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.models.topic_cleanup import CLEANUP_PENDING
from bms_msg.schemas.topic import DeleteImpact

pytestmark = pytest.mark.kiwi_id(2465)


def _utcnow() -> datetime:
    """当前 UTC 时刻（构造消息用）。

    Returns:
        datetime: 带时区的当前时刻。
    """
    return datetime.now(UTC)


NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

BASE_URL = "http://test"
"""测试基址。"""

HEADERS = {"X-Tenant-ID": "demo"}
"""缺省请求头（演示租户，与夹具租户源同值）。"""


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝（验证写端点权限码拦截）。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。

        Args:
            code: 权限码（本桩不区分）。

        Returns:
            bool: False。
        """
        del code
        return False


class _Sink:
    """审计替身：显式强审计只要求写入成功。"""

    def __init__(self) -> None:
        """初始化（记录写入次数）。"""
        self.events: ConcurrentStableList[object] = ConcurrentStableList()

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """记录并恒成功。

        Args:
            event: 审计事件。
            required: 强审计开关（本替身不区分）。

        Returns:
            bool: True。
        """
        del required
        self.events.add(event)
        return True


class _CleanupStub:
    """清理编排替身：影响预检给固定五项、kick 只记账（真编排在服务用例里验）。"""

    def __init__(self) -> None:
        """初始化（空 kick 记录）。"""
        self.kicked: ConcurrentStableList[int] = ConcurrentStableList()

    async def impact(self, *, tenant_id: int, topic_id: int, partition_count: int) -> DeleteImpact:
        """返回固定影响范围。

        Args:
            tenant_id: 租户主键（忽略）。
            topic_id: 主题主键（忽略）。
            partition_count: 分区数（原样回填）。

        Returns:
            DeleteImpact: 五项影响。
        """
        del tenant_id, topic_id
        return DeleteImpact(
            partition_count=partition_count,
            message_total=42,
            group_count=2,
            delayed_total=3,
            dlq_total=1,
        )

    def kick(self, topic_id: int) -> None:
        """记录一次即时触发。

        Args:
            topic_id: 主题主键。
        """
        self.kicked.add(int(topic_id))


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """重置插件注册表并确保占位实现已导入（与 `test_admin_topics.py` 同口径）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 位点 / 重放路由。

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
    app.state.topic_cache = TopicCache()
    app.state.audit_sink = _Sink()
    app.state.topic_cleanup_service = _CleanupStub()
    app.state.message_store = InMemoryMessageStore()
    app.state.offset_store = InMemoryOffsetStore()
    app.state.group_store = InMemoryGroupStore()
    app.state.dead_letter_queue = InMemoryDeadLetterQueue()
    # 重放端点声明 `ReplayRunnerDep`（依赖先于端点体解析）；本文件只验 confirm 拒绝，故给最小替身。
    app.state.replay_runner = SimpleNamespace(ensure_started=lambda: None)
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, offsets.router, replay.router])))
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
    payload = _body(response)["data"]
    return ConcurrentStableDict(cast("dict[str, object]", payload))


async def _create_topic(client: AsyncClient, name: str = "audit.operation", partition_count: int = 3) -> str:
    """建一个主题并返回主键字符串。

    Args:
        client: 客户端。
        name: 主题名。
        partition_count: 分区数。

    Returns:
        str: 主题主键（雪花，字符串）。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": partition_count},
        headers=HEADERS,
    )
    assert response.status_code == 200, response.text
    return str(_data(response)["id"])


async def test_delete_requires_confirm(client: AsyncClient) -> None:
    """删主题缺 `confirm`：400 / 10001，主题状态不变。"""
    topic_id = await _create_topic(client)
    response = await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", headers=HEADERS)
    # 契约层参数错误在本仓统一错误响应里以业务码表达（HTTP 200 + code 10001），与 01-6 既有用例同口径。
    assert _body(response)["code"] == 10001
    detail = await client.get(f"/api/v1/msg/admin/topics/{topic_id}", headers=HEADERS)
    assert _data(detail)["status"] == "active"


async def test_delete_accepts_and_reports_progress(client: AsyncClient, api_app: FastAPI) -> None:
    """删主题受理：200 + `deleting` + 台账 pending；详情可查进度；列表不再展示；触发一次 kick。"""
    topic_id = await _create_topic(client, name="audit.delete")
    response = await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    data = _data(response)
    assert (response.status_code, data["status"]) == (200, "deleting")
    cleanup = cast("dict[str, object]", data["cleanup"])
    assert (cleanup["status"], cleanup["attempts"], cleanup["done_steps"]) == (CLEANUP_PENDING, 0, [])
    detail = await client.get(f"/api/v1/msg/admin/topics/{topic_id}", headers=HEADERS)
    assert cast("dict[str, object]", _data(detail)["cleanup"])["status"] == CLEANUP_PENDING
    listed = await client.get("/api/v1/msg/admin/topics", headers=HEADERS)
    assert [item["name"] for item in cast("list[dict[str, object]]", _data(listed)["items"])] == []
    stub = cast("_CleanupStub", api_app.state.topic_cleanup_service)
    assert list(stub.kicked) == [int(topic_id)]


async def test_delete_is_idempotent(client: AsyncClient) -> None:
    """重复删除：仍 200，复用同一台账（不产生第二行）。"""
    topic_id = await _create_topic(client, name="audit.delete.twice")
    first = await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    second = await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    assert (first.status_code, second.status_code) == (200, 200)
    assert _data(first)["cleanup"] == _data(second)["cleanup"]


async def test_delete_unknown_topic_returns_350001(client: AsyncClient) -> None:
    """删不存在的主题：404 / 350001。"""
    response = await client.delete("/api/v1/msg/admin/topics/999999", params={"confirm": "true"}, headers=HEADERS)
    assert (response.status_code, _body(response)["code"]) == (404, 350001)


async def test_delete_impact_returns_five_values(client: AsyncClient) -> None:
    """影响预检：五项齐备（替身固定值 + 分区数取主题现状）。"""
    topic_id = await _create_topic(client, name="audit.impact", partition_count=4)
    response = await client.get(f"/api/v1/msg/admin/topics/{topic_id}/delete-impact", headers=HEADERS)
    data = _data(response)
    assert (data["partition_count"], data["message_total"], data["group_count"]) == (4, 42, 2)
    assert (data["delayed_total"], data["dlq_total"]) == (3, 1)


async def test_delete_impact_unknown_topic_returns_350001(client: AsyncClient) -> None:
    """影响预检不存在的主题：404 / 350001。"""
    response = await client.get("/api/v1/msg/admin/topics/999999/delete-impact", headers=HEADERS)
    assert (response.status_code, _body(response)["code"]) == (404, 350001)


async def test_expand_partitions_returns_detail(client: AsyncClient) -> None:
    """扩分区：200 + 完整详情（分区数与分区明细同步）。"""
    topic_id = await _create_topic(client, name="audit.expand", partition_count=2)
    response = await client.post(
        f"/api/v1/msg/admin/topics/{topic_id}/partitions",
        json={"partition_count": 4},
        headers=HEADERS,
    )
    data = _data(response)
    assert (response.status_code, data["partition_count"]) == (200, 4)
    assert [item["partition_no"] for item in cast("list[dict[str, object]]", data["partitions"])] == [0, 1, 2, 3]


async def test_expand_partitions_rejects_shrink(client: AsyncClient) -> None:
    """扩分区减少：400 / 350009。"""
    topic_id = await _create_topic(client, name="audit.expand.shrink", partition_count=3)
    response = await client.post(
        f"/api/v1/msg/admin/topics/{topic_id}/partitions",
        json={"partition_count": 2},
        headers=HEADERS,
    )
    assert (response.status_code, _body(response)["code"]) == (400, 350009)


async def test_expand_rejected_when_deleting(client: AsyncClient) -> None:
    """`deleting` 状态下扩分区：409 / 350028。"""
    topic_id = await _create_topic(client, name="audit.expand.deleting")
    await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    response = await client.post(
        f"/api/v1/msg/admin/topics/{topic_id}/partitions",
        json={"partition_count": 5},
        headers=HEADERS,
    )
    assert (response.status_code, _body(response)["code"]) == (409, 350028)


async def test_patch_retention_partial_update(client: AsyncClient) -> None:
    """改保留期：只给一项即只改该项，响应回显新值。"""
    topic_id = await _create_topic(client, name="audit.retention")
    response = await client.patch(
        f"/api/v1/msg/admin/topics/{topic_id}",
        json={"retention_bytes": 1048576},
        headers=HEADERS,
    )
    data = _data(response)
    assert (response.status_code, data["retention_bytes"]) == (200, 1048576)
    assert data["retention_ms"] != 0


async def test_patch_retention_rejects_empty_body(client: AsyncClient) -> None:
    """空 PATCH（两项都不给）：400 / 10001。"""
    topic_id = await _create_topic(client, name="audit.retention.empty")
    response = await client.patch(f"/api/v1/msg/admin/topics/{topic_id}", json={}, headers=HEADERS)
    assert _body(response)["code"] == 10001


async def test_patch_retention_rejects_out_of_range(client: AsyncClient) -> None:
    """保留期越界：400 / 350009。"""
    topic_id = await _create_topic(client, name="audit.retention.range")
    response = await client.patch(
        f"/api/v1/msg/admin/topics/{topic_id}",
        json={"retention_ms": 0},
        headers=HEADERS,
    )
    assert (response.status_code, _body(response)["code"]) == (400, 350009)


async def test_patch_rejected_when_deleting(client: AsyncClient) -> None:
    """`deleting` 状态下改保留期：409 / 350028。"""
    topic_id = await _create_topic(client, name="audit.retention.deleting")
    await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    response = await client.patch(
        f"/api/v1/msg/admin/topics/{topic_id}",
        json={"retention_ms": 60000},
        headers=HEADERS,
    )
    assert (response.status_code, _body(response)["code"]) == (409, 350028)


async def _seed_offset(client: AsyncClient, api_app: FastAPI, *, group: str = "audit-group") -> tuple[str, int]:
    """建主题、建组、提交一个位点，返回（主题主键, LEO）。

    Args:
        client: 客户端。
        api_app: 应用实例（取进程级端口）。
        group: 组名。

    Returns:
        tuple[str, int]: 主题主键与已提交位点。
    """
    topic_id = int(await _create_topic(client, name="audit.offset", partition_count=2))
    groups = cast("InMemoryGroupStore", api_app.state.group_store)
    from bms_msg.broker.group import ConsumerGroup  # 局部导入：只用构造组记录

    await groups.save(
        ConsumerGroup(
            tenant_id=7001,
            topic_id=topic_id,
            group=group,
            state="stable",
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    await offsets.commit(group=group, topic_id=topic_id, partition_no=0, offset=5)
    return str(topic_id), 5


def _reset_body(topic: str, *, group: str = "audit-group", offset: int = 3, confirm: bool = True) -> dict[str, object]:
    """构造位点重置请求体。

    Args:
        topic: 主题名。
        group: 组名。
        offset: 目标位点。
        confirm: 确认参数。

    Returns:
        dict[str, object]: 请求体。
    """
    return {
        "namespace": NAMESPACE,
        "topic": topic,
        "group": group,
        "partition": 0,
        "offset": offset,
        "confirm": confirm,
    }


async def test_offset_reset_moves_backwards(client: AsyncClient, api_app: FastAPI) -> None:
    """位点重置倒退：200，`previous_offset` 为旧值、`offset` 为新值，端口内已落库。"""
    _topic_id, seed = await _seed_offset(client, api_app)
    response = await client.post(
        "/api/v1/msg/admin/offsets/reset", json=_reset_body("audit.offset", offset=2), headers=HEADERS
    )
    data = _data(response)
    assert (response.status_code, data["previous_offset"], data["offset"]) == (200, seed, 2)
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    assert await offsets.get(group="audit-group", topic_id=int(_topic_id), partition_no=0) == 2


async def test_offset_reset_requires_confirm(client: AsyncClient, api_app: FastAPI) -> None:
    """位点重置缺 `confirm`：400 / 10001，位点不变。"""
    _topic_id, seed = await _seed_offset(client, api_app)
    response = await client.post(
        "/api/v1/msg/admin/offsets/reset",
        json=_reset_body("audit.offset", offset=1, confirm=False),
        headers=HEADERS,
    )
    assert _body(response)["code"] == 10001
    offsets = cast("InMemoryOffsetStore", api_app.state.offset_store)
    assert await offsets.get(group="audit-group", topic_id=int(_topic_id), partition_no=0) == seed


async def test_offset_reset_rejects_group_missing(client: AsyncClient, api_app: FastAPI) -> None:
    """组不存在：404 / 350013。"""
    await _seed_offset(client, api_app)
    response = await client.post(
        "/api/v1/msg/admin/offsets/reset", json=_reset_body("audit.offset", group="nope"), headers=HEADERS
    )
    assert (response.status_code, _body(response)["code"]) == (404, 350013)


async def test_offset_reset_rejects_partition_out_of_range(client: AsyncClient, api_app: FastAPI) -> None:
    """分区号越界：400 / 10001（契约层形态之外的业务判据取参数错误，与既有分区判据同口径）。"""
    await _seed_offset(client, api_app)
    body = _reset_body("audit.offset")
    body["partition"] = 9
    response = await client.post("/api/v1/msg/admin/offsets/reset", json=body, headers=HEADERS)
    assert _body(response)["code"] == 10001


async def test_offset_reset_rejects_before_start_offset(client: AsyncClient, api_app: FastAPI) -> None:
    """位点早于分区起点：404 / 350003（该段消息已被保留期清理）。"""
    await _seed_offset(client, api_app)
    messages = cast("InMemoryMessageStore", api_app.state.message_store)
    topic_id = int(await _topic_of(client, "audit.offset"))
    for index in range(4):
        await messages.append(
            topic_id=topic_id,
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"log-{index}",
            key=None,
            payload=b"x",
            headers=ConcurrentStableDict[str, str](),
            trace=EMPTY_TRACE,
            published_at=_utcnow(),
        )
    await messages.truncate_before(topic_id=topic_id, partition_no=0, offset=4)
    response = await client.post(
        "/api/v1/msg/admin/offsets/reset", json=_reset_body("audit.offset", offset=1), headers=HEADERS
    )
    assert (response.status_code, _body(response)["code"]) == (404, 350003)


async def _topic_of(client: AsyncClient, name: str) -> str:
    """按名称取主题主键（走列表端点，避免直接依赖仓储）。

    Args:
        client: 客户端。
        name: 主题名。

    Returns:
        str: 主题主键。
    """
    listed = await client.get(f"/api/v1/msg/admin/topics?keyword={name}", headers=HEADERS)
    items = cast("list[dict[str, object]]", _data(listed)["items"])
    return str(items[0]["id"])


async def test_offset_reset_rejects_unknown_topic(client: AsyncClient) -> None:
    """主题不存在：404 / 350001。"""
    response = await client.post("/api/v1/msg/admin/offsets/reset", json=_reset_body("nope.topic"), headers=HEADERS)
    assert (response.status_code, _body(response)["code"]) == (404, 350001)


async def test_offset_reset_rejects_deleting_topic(client: AsyncClient) -> None:
    """`deleting` 状态下重置位点：409 / 350028。"""
    topic_id = await _create_topic(client, name="audit.offset.deleting")
    await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    response = await client.post(
        "/api/v1/msg/admin/offsets/reset", json=_reset_body("audit.offset.deleting"), headers=HEADERS
    )
    assert (response.status_code, _body(response)["code"]) == (409, 350028)


async def test_replay_create_requires_confirm(client: AsyncClient) -> None:
    """重放创建缺 `confirm`：400 / 10001，且不落任务行。"""
    await _create_topic(client, name="audit.replay")
    response = await client.post(
        "/api/v1/msg/admin/replay",
        json={"namespace": NAMESPACE, "topic": "audit.replay", "partitions": [0]},
        headers=HEADERS,
    )
    assert _body(response)["code"] == 10001
    tasks = await client.get("/api/v1/msg/admin/replay/999999", headers=HEADERS)
    assert _body(tasks)["code"] == 350016


async def test_write_endpoints_reject_without_manage_permission(client: AsyncClient, api_app: FastAPI) -> None:
    """无 `msg:manage`：四个写端点全部 403 / 30001。"""
    # 必须先登记再发请求：注册表在首次判定时即构建，构建后拒绝再登记（与 `test_admin_topics` 同约束）。
    register_plugin("permission", "deny", _DenyChecker)
    cast("SimpleNamespace", api_app.state.settings).permission.provider = "deny"
    topic_id = "123"
    deleted = await client.delete(f"/api/v1/msg/admin/topics/{topic_id}", params={"confirm": "true"}, headers=HEADERS)
    patched = await client.patch(f"/api/v1/msg/admin/topics/{topic_id}", json={"retention_ms": 1}, headers=HEADERS)
    expanded = await client.post(
        f"/api/v1/msg/admin/topics/{topic_id}/partitions", json={"partition_count": 5}, headers=HEADERS
    )
    reset = await client.post("/api/v1/msg/admin/offsets/reset", json=_reset_body("audit.denied"), headers=HEADERS)
    assert {response.status_code for response in (deleted, patched, expanded, reset)} == {403}
    assert _body(deleted)["code"] == 30001


def test_cleanup_service_dep_reports_missing_assembly() -> None:
    """装配缺陷可见：未注入清理编排时取依赖直接 `ConfigError`，注入后原样返回。"""
    state = SimpleNamespace()
    request = cast("Request", SimpleNamespace(app=SimpleNamespace(state=state)))
    with pytest.raises(ConfigError):
        get_topic_cleanup_service(request)

    stub = _CleanupStub()
    state.topic_cleanup_service = stub
    assert get_topic_cleanup_service(request) is stub
