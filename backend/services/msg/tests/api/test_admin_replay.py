"""重放任务接口（msg 任务 01-6；Kiwi 2418）。

覆盖（详细设计 §8、§10.1 用例 A1~A8）：

- **A1** 建任务（`total` 正确、`status = pending`、首个请求启动执行器）；
- **A2** 审计写入（`action = msg:replay`、`params` 含范围与 `task_id`）；
- **A3** 审计失败不影响（出口返回 `False` 仍 200）；
- **A4** 进度查询；**A5** 暂停 / 恢复 / 取消 + 终态再暂停 `350017`；
- **A6** 任务不存在 `350016`；**A7** 范围非法 `10001` / 分区越界 `350009`；**A8** 无 `msg:manage` → 403。

装配同 01-4 / 01-5 的接口用例：最小应用 + 基座真实租户中间件 + 基座统一错误渲染 + 真实模块路由，
平台库工作单元换内存会话，进程级状态（消息存储 / 审计出口 / 执行器）挂 `app.state`。
**执行器周期放大到 10s**：用例只验接口与留痕，不让后台任务把任务跑完而干扰进度断言。
"""

import importlib
import json
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Protocol, cast

import pytest
from bms_core.api.base import mount_service_routers
from bms_core.api.errors import register_exception_handlers
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import replay as admin_replay
from bms_msg.api.admin import topics
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner
from bms_msg.errors import MSG_ERROR_MAPPER, MsgErrorCode, register_msg_error_mapper

pytestmark = pytest.mark.kiwi_id(2418)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明的唯一事实源，供文案键解析）。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

BASE_URL = "http://test"
"""测试基址。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间（历史数据用固定值，不与真实时钟耦合）。"""


class RecordingSink:
    """审计出口替身（可切换成功 / 失败）。"""

    def __init__(self) -> None:
        """初始化（缺省成功）。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
        self.result = True

    async def emit(self, event: EventEnvelope) -> bool:
        """记录事件并返回当前设定结果。

        Args:
            event: 审计事件信封。

        Returns:
            bool: `self.result`。
        """
        self.events.add(event)
        return self.result


class SinkView(Protocol):
    """审计出口替身的静态形态（用例断言用）。"""

    events: ConcurrentStableList[EventEnvelope]
    """已记录的事件信封。"""

    result: bool
    """`emit` 的返回值。"""


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
    """登记产品码覆盖并确保占位权限 / 租户模块门禁已导入。

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
    """最小应用：租户中间件 + 统一错误渲染 + 主题 / 重放路由 + 进程级状态。

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
    """用例结束后停掉重放执行器（惰性启动的后台任务不跨用例泄漏）。

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


async def _create_topic(client: AsyncClient) -> int:
    """建一个两分区主题并返回主键。

    Args:
        client: 客户端。

    Returns:
        int: 主题主键。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 2},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
    return int(cast("str", _data(response)["id"]))


async def _seed(api_app: FastAPI, *, topic_id: int, count: int) -> None:
    """直接向存储写入历史记录（用例要精确控制条数，不走发布链路）。

    Args:
        api_app: 应用实例。
        topic_id: 主题主键。
        count: 条数。
    """
    store = cast("InMemoryMessageStore", api_app.state.message_store)
    for index in range(count):
        await store.append(
            topic_id=topic_id,
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"l-{index}",
            key="order-1",
            payload=b"payload",
            headers=ConcurrentStableDict({"source": "test"}),
            trace_id=None,
            published_at=_BASE,
        )


async def _create_replay(client: AsyncClient, **overrides: object) -> Response:
    """发建任务请求。

    Args:
        client: 客户端。
        **overrides: 覆盖字段。

    Returns:
        Response: 响应。
    """
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"namespace": NAMESPACE, "topic": TOPIC_NAME, "partitions": [0]}
    )
    for key, value in overrides.items():
        body.set(key, value)
    return await client.post("/api/v1/msg/admin/replay", json=dict(body.items()), headers=DEMO_HEADERS)


async def test_create_replay_task(client: AsyncClient, api_app: FastAPI) -> None:
    """A1：建任务返回精确 `total`、`status = pending`，并惰性启动执行器。"""
    topic_id = await _create_topic(client)
    await _seed(api_app, topic_id=topic_id, count=3)

    response = await _create_replay(client)
    data = _data(response)

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert (data["status"], data["total"], data["replayed"]) == ("pending", 3, 0)
    assert data["partitions"] == [0]
    assert cast("ReplayRunner", api_app.state.replay_runner).running is True


async def test_create_replay_writes_audit(client: AsyncClient, api_app: FastAPI) -> None:
    """A2：建任务写一条 `msg:replay` 审计，载荷含范围与 `task_id`。"""
    topic_id = await _create_topic(client)
    await _seed(api_app, topic_id=topic_id, count=2)

    data = _data(await _create_replay(client))
    sink = cast("SinkView", api_app.state.audit_sink)
    event = sink.events[0]
    params = json.loads(cast("str", event.payload["params"]))

    assert len(sink.events) == 1
    assert (event.event_type, event.payload["action"], event.payload["module"]) == (
        "sys.operation.logged",
        "msg:replay",
        "msg",
    )
    assert params["task_id"] == data["task_id"]
    assert (params["namespace"], params["topic"], params["partitions"]) == (NAMESPACE, TOPIC_NAME, [0])
    assert params["total"] == 2


async def test_audit_failure_does_not_block_create(client: AsyncClient, api_app: FastAPI) -> None:
    """A3：审计出口返回 False → 任务照建、接口仍 200。"""
    topic_id = await _create_topic(client)
    await _seed(api_app, topic_id=topic_id, count=1)
    cast("SinkView", api_app.state.audit_sink).result = False

    response = await _create_replay(client)

    assert (response.status_code, _body(response)["code"]) == (200, 0)
    assert _data(response)["total"] == 1


async def test_get_replay_progress(client: AsyncClient, api_app: FastAPI) -> None:
    """A4：进度查询返回同一份快照（`total` / `replayed` / `status`）。"""
    topic_id = await _create_topic(client)
    await _seed(api_app, topic_id=topic_id, count=2)
    created = _data(await _create_replay(client))

    response = await client.get(f"/api/v1/msg/admin/replay/{created['task_id']}", headers=DEMO_HEADERS)
    data = _data(response)

    assert (response.status_code, data["task_id"]) == (200, created["task_id"])
    assert (data["total"], data["replayed"], data["status"]) == (2, 0, "pending")


async def test_pause_resume_cancel_state_machine(client: AsyncClient, api_app: FastAPI) -> None:
    """A5：暂停 / 恢复 / 取消各 200；终态再暂停返回 `350017`（409）。"""
    topic_id = await _create_topic(client)
    await _seed(api_app, topic_id=topic_id, count=2)
    task_id = _data(await _create_replay(client))["task_id"]
    base = f"/api/v1/msg/admin/replay/{task_id}"

    paused = await client.post(f"{base}/pause", headers=DEMO_HEADERS)
    resumed = await client.post(f"{base}/resume", headers=DEMO_HEADERS)
    cancelled = await client.post(f"{base}/cancel", headers=DEMO_HEADERS)
    rejected = await client.post(f"{base}/pause", headers=DEMO_HEADERS)

    assert (_data(paused)["status"], _data(resumed)["status"], _data(cancelled)["status"]) == (
        "paused",
        "running",
        "cancelled",
    )
    assert _data(cancelled)["finished_at"] is not None
    assert (rejected.status_code, _body(rejected)["code"]) == (409, int(MsgErrorCode.REPLAY_TASK_INVALID_STATE))
    assert len(cast("SinkView", api_app.state.audit_sink).events) == 2


async def test_missing_task_returns_350016(client: AsyncClient) -> None:
    """A6：任务不存在（或不属于当前租户）→ `350016`（404）。"""
    await _create_topic(client)

    response = await client.get("/api/v1/msg/admin/replay/123456789", headers=DEMO_HEADERS)

    assert (response.status_code, _body(response)["code"]) == (404, int(MsgErrorCode.REPLAY_TASK_NOT_FOUND))


async def test_invalid_range_is_rejected(client: AsyncClient) -> None:
    """A7：契约层拒绝空分区 / 越界速率 / 区间颠倒（`10001`），分区越界 `350009`。"""
    await _create_topic(client)

    empty = await _create_replay(client, partitions=[])
    slow = await _create_replay(client, rate_per_second=0)
    reversed_range = await _create_replay(client, offset_from=9, offset_to=2)
    reversed_time = await _create_replay(client, time_from="2026-01-02T00:00:00Z", time_to="2026-01-01T00:00:00Z")
    out_of_range = await _create_replay(client, partitions=[5])

    assert _body(empty)["code"] == 10001
    assert _body(slow)["code"] == 10001
    assert _body(reversed_range)["code"] == 10001
    assert _body(reversed_time)["code"] == 10001
    assert (out_of_range.status_code, _body(out_of_range)["code"]) == (
        400,
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
    )


async def test_permission_denied_blocks_create(client: AsyncClient, api_app: FastAPI, db_session: AsyncSession) -> None:
    """A8：无 `msg:manage` 权限 → 403（30001），请求不进入业务逻辑（不建主题即直接拒）。"""
    del db_session
    register_plugin("permission", "deny", _DenyChecker)
    cast("SimpleNamespace", api_app.state.settings).permission.provider = "deny"

    response = await _create_replay(client)

    assert (response.status_code, _body(response)["code"]) == (403, 30001)
