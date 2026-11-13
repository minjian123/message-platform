"""管理面四条只读端点（msg 任务 06-1-1；Kiwi 2462）。

覆盖：分区字段与可空占位、已提交位点与积压、消息组合过滤与 `log_id` 索引、
载荷摘要不含正文、概览四卡与冷启动、延迟档位分布与堆积提示、权限与错误码、
02-6 `350022` 的 `data` 不再退化成 500。
"""

import importlib
import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from urllib.parse import quote

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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.admin import delayed_summary as admin_delayed_summary
from bms_msg.api.admin import messages as admin_messages
from bms_msg.api.admin import overview as admin_overview
from bms_msg.api.admin import partitions as admin_partitions
from bms_msg.api.admin import topics
from bms_msg.api.data import messages as data_messages
from bms_msg.broker import watermark as watermark_module
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import ConsumerGroup, GroupMember, InMemoryGroupStore
from bms_msg.broker.log import (
    InMemoryMessageStore,
    MessageHeaderView,
    StoredMessage,
    message_header_view,
)
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.broker.watermark import StorageMetrics, StorageReading
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.errors import MsgErrorCode, MsgStorageWatermarkExceededError
from bms_msg.metrics.lag import MetricsSnapshotStore
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.overview import OverviewSnapshotRepository
from tests_support.frame_app import NoopAuditSink, install_backpressure_state

pytestmark = pytest.mark.kiwi_id(2462)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
NAMESPACE = "msg"
TOPIC_A = "audit.operation"
BASE_URL = "http://test"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}

_payload_fn = cast("Callable[[StorageReading, str], dict[str, object]]", watermark_module.__dict__["_payload"])
"""02-6 私有构造入口（测试直接验 `data` 形态，避免经 pyright 私有使用告警）。"""


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。

        Args:
            code: 权限码。

        Returns:
            bool: False。
        """
        del code
        return False


class _CountingStore(InMemoryMessageStore):
    """消息存储替身：记录只读端口调用次数（验证 `log_id` 命中不触发扫描）。"""

    def __init__(self) -> None:
        """初始化计数器。"""
        super().__init__()
        self.find_calls = 0
        self.scan_calls = 0
        self.forced: MessageHeaderView | None = None

    async def find_by_log_id(self, *, log_id: str) -> MessageHeaderView | None:
        """计数后可注入伪造命中（跨租户边界用例）。

        Args:
            log_id: 记录标识。

        Returns:
            MessageHeaderView | None: 命中视图。
        """
        self.find_calls += 1
        return self.forced if self.forced is not None else await super().find_by_log_id(log_id=log_id)

    async def scan_headers(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[MessageHeaderView]:
        """计数后转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 上限。

        Returns:
            ConcurrentStableList[MessageHeaderView]: 头视图。
        """
        self.scan_calls += 1
        return await super().scan_headers(
            topic_id=topic_id,
            partition_no=partition_no,
            from_offset=from_offset,
            max_messages=max_messages,
        )


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记占位插件并装载产品错误码声明。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()
    registry.reset()


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：租户中间件 + 统一错误渲染 + 四条只读端点 + 建主题 / 发布端点。

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
    app.state.audit_sink = NoopAuditSink()
    app.state.message_store = _CountingStore()
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
    app.state.metrics_snapshots = MetricsSnapshotStore()
    app.state.storage_metrics = StorageMetrics()
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(
        mount_service_routers(
            ConcurrentStableList(
                [
                    topics.router,
                    data_messages.router,
                    admin_partitions.router,
                    admin_messages.router,
                    admin_overview.router,
                    admin_delayed_summary.router,
                ]
            )
        )
    )
    session = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身。

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


async def _publish(client: AsyncClient, *, key: str, delay_level: str | None = None) -> Response:
    """经数据面发布一条消息。

    Args:
        client: 客户端。
        key: 分区键。
        delay_level: 延迟档位（None 为立即入日志）。

    Returns:
        Response: 响应。
    """
    body: dict[str, object] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_A,
        "key": key,
        "payload": "cGF5bG9hZA==",
    }
    if delay_level is not None:
        body["delay_level"] = delay_level
    return await client.post("/api/v1/msg/publish", json=body, headers=DEMO_HEADERS)


async def _partitions(client: AsyncClient, query: str = "") -> Response:
    """取分区详情。

    Args:
        client: 客户端。
        query: 查询串（含 `?`）。

    Returns:
        Response: 响应。
    """
    return await client.get(
        f"/api/v1/msg/admin/partitions?namespace={NAMESPACE}&topic={TOPIC_A}{query}", headers=DEMO_HEADERS
    )


async def _messages(client: AsyncClient, query: str = "") -> Response:
    """取消息检索结果。

    Args:
        client: 客户端。
        query: 追加查询串（含 `&`）。

    Returns:
        Response: 响应。
    """
    return await client.get(
        f"/api/v1/msg/admin/messages?namespace={NAMESPACE}&topic={TOPIC_A}{query}", headers=DEMO_HEADERS
    )


async def test_partitions_fields_and_placeholders(client: AsyncClient) -> None:
    """分区字段来自端口；HW / ISR 为可空占位，未传组时位点与积压为 null。"""
    await _create_topic(client)
    response = await _partitions(client)
    assert _body(response)["code"] == 0
    data = _data(response)
    assert (data["partition_count"], data["group"]) == (2, None)
    items = _items(response)
    assert [item["partition"] for item in items] == [0, 1]
    assert all(item["hw"] is None and item["isr"] == [] for item in items)
    assert all(item["epoch"] == 0 for item in items)
    assert all(item["committed"] is None and item["lag"] is None and item["owner"] is None for item in items)
    assert all(isinstance(item["leo"], int) for item in items)


async def test_partitions_with_group_shows_committed_and_lag(
    client: AsyncClient, api_app: FastAPI, db_session: AsyncSession
) -> None:
    """传组时返回已提交位点、积压与持有者；组不存在给 350013。"""
    await _create_topic(client)
    store = cast("InMemoryGroupStore", api_app.state.group_store)
    topic = (await db_session.execute(select(MsgTopic))).scalar_one()
    now = datetime.now(UTC)
    await store.save(
        ConsumerGroup(
            tenant_id=7001,
            topic_id=int(topic.id),
            group="g1",
            state="stable",
            generation=1,
            members=ConcurrentStableList(
                [
                    GroupMember(
                        consumer_id="c1",
                        joined_at=now,
                        last_seen=now,
                        partitions=ConcurrentStableList([0, 1]),
                    )
                ]
            ),
        )
    )
    response = await _partitions(client, "&group=g1")
    items = _items(response)
    assert _body(response)["code"] == 0
    assert len(items) == 2
    assert all(item["committed"] == 0 and item["lag"] == item["leo"] for item in items)
    missing = await _partitions(client, "&group=nope")
    assert (missing.status_code, _body(missing)["code"]) == (404, int(MsgErrorCode.GROUP_NOT_FOUND))


async def test_partitions_single_partition_and_errors(client: AsyncClient) -> None:
    """分区过滤、命名空间非法 350010、主题不存在 350001、分区越界 350009。"""
    await _create_topic(client)
    one = await _partitions(client, "&partition=1")
    assert [cast("int", item["partition"]) for item in _items(one)] == [1]
    invalid_partition = await _partitions(client, "&partition=9")
    assert (invalid_partition.status_code, _body(invalid_partition)["code"]) == (
        400,
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
    )
    invalid_namespace = await client.get(
        f"/api/v1/msg/admin/partitions?namespace=nope&topic={TOPIC_A}", headers=DEMO_HEADERS
    )
    assert (invalid_namespace.status_code, _body(invalid_namespace)["code"]) == (
        400,
        int(MsgErrorCode.NAMESPACE_INVALID),
    )
    missing_topic = await client.get(
        f"/api/v1/msg/admin/partitions?namespace={NAMESPACE}&topic=audit.missing", headers=DEMO_HEADERS
    )
    assert (missing_topic.status_code, _body(missing_topic)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_message_search_combination_and_no_body(client: AsyncClient) -> None:
    """offset / key / 时间 / log_id 跨类相与；响应只有长度与 SHA-256，不含正文。"""
    await _create_topic(client)
    await _publish(client, key="alpha")
    await _publish(client, key="beta")
    everything = await _messages(client)
    items = _items(everything)
    assert _data(everything)["total"] == 2
    assert "payload" not in items[0] and "body" not in items[0]
    assert items[0]["payload_len"] == len(b"payload")
    assert cast("str", items[0]["payload_sha256"]) == sha256(b"payload").hexdigest()
    assert cast("dict[str, object]", items[0]["headers"]) == {}
    by_key = await _messages(client, "&key=alpha")
    assert _data(by_key)["total"] == 1
    by_offset = await _messages(client, "&offset_from=0&offset_to=1")
    assert _data(by_offset)["total"] == 2
    one_partition = await _messages(client, "&partition=0&offset_from=0&offset_to=1")
    assert cast("int", _data(one_partition)["total"]) <= 1
    now = datetime.now(UTC)
    by_time = await _messages(client, f"&time_from={quote((now - timedelta(minutes=1)).isoformat())}")
    assert _data(by_time)["total"] == 2
    log_id = cast("str", items[0]["log_id"])
    by_log = await _messages(client, f"&log_id={log_id}")
    assert _data(by_log)["total"] == 1
    assert not _data(await _messages(client, "&log_id=missing"))["total"]


async def test_message_log_id_uses_index_and_blocks_foreign_topic(client: AsyncClient, api_app: FastAPI) -> None:
    """`log_id` 命中只走 `find_by_log_id`；命中其它主题的记录必须被拒（不跨租户直读）。"""
    await _create_topic(client)
    await _publish(client, key="alpha")
    first = _items(await _messages(client))[0]
    log_id = cast("str", first["log_id"])
    wrapped = cast("object", api_app.state.message_store)
    inner = cast("_CountingStore", getattr(wrapped, "_inner", wrapped))
    inner.find_calls = 0
    inner.scan_calls = 0
    hit = await _messages(client, f"&log_id={log_id}")
    assert _data(hit)["total"] == 1
    assert inner.find_calls == 1 and inner.scan_calls == 0
    foreign = MessageHeaderView(
        message_id=cast("str", first["message_id"]),
        log_id=log_id,
        topic_id=-1,
        partition_no=0,
        offset=0,
        key=None,
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
        content_type="application/msgpack",
        payload_len=8,
        payload_sha256=sha256(b"payload").hexdigest(),
    )
    inner.forced = foreign
    blocked = await _messages(client, f"&log_id={log_id}")
    assert _data(blocked)["total"] == 0
    inner.forced = None


async def test_message_search_invalid_conditions(client: AsyncClient) -> None:
    """条件区间倒置、负 offset、分页越界一律 10001。"""
    await _create_topic(client)
    inverted_offset = await _messages(client, "&offset_from=5&offset_to=2")
    negative_offset = await _messages(client, "&offset_from=-1")
    now = datetime.now(UTC).isoformat()
    earlier = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
    inverted_time = await _messages(client, f"&time_from={quote(now)}&time_to={quote(earlier)}")
    bad_page = await _messages(client, "&page=0")
    bad_size = await _messages(client, "&size=101")
    assert all(
        _body(response)["code"] == 10001
        for response in (inverted_offset, negative_offset, inverted_time, bad_page, bad_size)
    )


async def test_overview_values_and_cold_start(client: AsyncClient, api_app: FastAPI) -> None:
    """四卡取自 06-3 快照与水位；快照未产出时四值 0 且 `snapshot_available=false`。"""
    cold = await client.get("/api/v1/msg/admin/overview", headers=DEMO_HEADERS)
    assert _body(cold)["code"] == 0
    assert _data(cold)["snapshot_available"] is False
    assert all(
        _data(cold)[key] == 0 for key in ("messages_in_per_second", "consumer_lag", "dlq_total", "disk_used_ratio")
    )

    snapshots = cast("MetricsSnapshotStore", api_app.state.metrics_snapshots)
    snapshots.replace(
        lag=ConcurrentStableDict({"demo\x1faudit.operation": 7, "other\x1faudit.operation": 99}),
        dlq=ConcurrentStableDict({"demo\x1faudit.operation": 3, "other\x1faudit.operation": 50}),
        messages_in_window=30,
        window_seconds=15.0,
    )
    storage = cast("StorageMetrics", api_app.state.storage_metrics)
    storage.record_reading(
        StorageReading(
            volume="v1", used_ratio=0.42, free_bytes=1000, total_bytes=2000, warn_ratio=0.7, reject_ratio=0.85
        )
    )
    warm = await client.get("/api/v1/msg/admin/overview", headers=DEMO_HEADERS)
    data = _data(warm)
    assert data["snapshot_available"] is True
    assert (data["messages_in_per_second"], data["consumer_lag"], data["dlq_total"]) == (2.0, 7, 3)
    assert data["disk_used_ratio"] == 0.42


@pytest.mark.kiwi_id(2497)
async def test_overview_prefers_persisted_snapshot(client: AsyncClient, db_session: AsyncSession) -> None:
    """02-8-4：库里有未过期的每租户快照时优先读库（跨实例统一口径）。"""
    await OverviewSnapshotRepository(cast("DbSession", db_session)).upsert(
        tenant_id=7001,
        messages_in_per_second=9.0,
        consumer_lag=11,
        dlq_total=4,
        disk_used_ratio=0.66,
        sampled_at=datetime.now(UTC).replace(tzinfo=None),
    )
    await db_session.commit()

    response = await client.get("/api/v1/msg/admin/overview", headers=DEMO_HEADERS)
    data = _data(response)
    assert data["snapshot_available"] is True
    assert (data["messages_in_per_second"], data["consumer_lag"], data["dlq_total"]) == (9.0, 11, 4)
    assert data["disk_used_ratio"] == 0.66


async def test_delay_summary_matches_list_and_stale_hint(client: AsyncClient, api_app: FastAPI) -> None:
    """档位分布与待投列表同源；已到期未转投给 `stale` 提示。"""
    await _create_topic(client)
    await _publish(client, key="alpha", delay_level="1m")
    await _publish(client, key="beta", delay_level="5m")
    summary = await client.get("/api/v1/msg/admin/delayed/summary", headers=DEMO_HEADERS)
    assert _body(summary)["code"] == 0
    data = _data(summary)
    assert data["total"] == 2 and data["stale_count"] == 0 and data["hint"] == "normal"
    levels = cast("list[dict[str, object]]", data["levels"])
    assert len(levels) == 9
    assert {cast("str", bucket["level"]): bucket["count"] for bucket in levels}["1m"] == 1
    assert {cast("str", bucket["level"]): bucket["count"] for bucket in levels}["5m"] == 1

    store = cast("InMemoryDelayStore", api_app.state.delay_store)
    now = datetime.now(UTC)
    await store.enqueue(
        DelayRecord(
            log_id="stale-1",
            message_id="stale-msg",
            tenant_id=7001,
            topic_id=1,
            namespace=NAMESPACE,
            topic=TOPIC_A,
            partition_no=0,
            delay_level="1s",
            key=None,
            payload=b"x",
            headers=ConcurrentStableDict(),
            trace=EMPTY_TRACE,
            published_at=now - timedelta(hours=1),
            deliver_at=now - timedelta(minutes=1),
            attempts=0,
        )
    )
    stale = await client.get("/api/v1/msg/admin/delayed/summary", headers=DEMO_HEADERS)
    assert _data(stale)["stale_count"] == 1 and _data(stale)["hint"] == "stale"


async def test_delay_summary_filters_and_errors(client: AsyncClient) -> None:
    """汇总过滤与错误码沿用待投列表判据。"""
    await _create_topic(client)
    await _publish(client, key="alpha", delay_level="1m")
    filtered = await client.get(
        f"/api/v1/msg/admin/delayed/summary?namespace={NAMESPACE}&topic={TOPIC_A}&partition=9",
        headers=DEMO_HEADERS,
    )
    assert (filtered.status_code, _body(filtered)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))
    bad_namespace = await client.get("/api/v1/msg/admin/delayed/summary?namespace=nope", headers=DEMO_HEADERS)
    assert (bad_namespace.status_code, _body(bad_namespace)["code"]) == (
        400,
        int(MsgErrorCode.NAMESPACE_INVALID),
    )
    missing_topic = await client.get(
        f"/api/v1/msg/admin/delayed/summary?namespace={NAMESPACE}&topic=audit.missing", headers=DEMO_HEADERS
    )
    assert (missing_topic.status_code, _body(missing_topic)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))
    by_topic = await client.get(
        f"/api/v1/msg/admin/delayed/summary?namespace={NAMESPACE}&topic={TOPIC_A}", headers=DEMO_HEADERS
    )
    assert _data(by_topic)["total"] == 1


async def test_permission_denied_blocks_readonly_views(client: AsyncClient, api_app: FastAPI) -> None:
    """无 `msg:query`：四条端点都 403（30001）。"""
    register_plugin("permission", "deny", _DenyChecker)
    cast("SimpleNamespace", api_app.state.settings).permission.provider = "deny"
    targets = [
        f"/api/v1/msg/admin/partitions?namespace={NAMESPACE}&topic={TOPIC_A}",
        f"/api/v1/msg/admin/messages?namespace={NAMESPACE}&topic={TOPIC_A}",
        "/api/v1/msg/admin/overview",
        "/api/v1/msg/admin/delayed/summary",
    ]
    for target in targets:
        response = await client.get(target, headers=DEMO_HEADERS)
        assert (response.status_code, _body(response)["code"]) == (403, 30001)


async def test_watermark_payload_is_plain_dict_and_http_renders(client: AsyncClient, api_app: FastAPI) -> None:
    """02-6 修复：`350022` 的 `data` 为普通 dict，统一错误渲染不再退化成 500。"""
    reading = StorageReading(
        volume="v1", used_ratio=0.9, free_bytes=100, total_bytes=1000, warn_ratio=0.7, reject_ratio=0.85
    )
    payload = _payload_fn(reading, "watermark")
    assert type(payload) is dict
    assert payload["used_ratio"] == 0.9

    probe = FastAPI()
    register_exception_handlers(probe)

    @probe.get("/boom")
    async def _boom() -> None:
        """模拟存储水位拒绝。

        Raises:
            MsgStorageWatermarkExceededError: 水位超限。
        """
        raise MsgStorageWatermarkExceededError("磁盘水位超限", data=_payload_fn(reading, "watermark"))

    async with AsyncClient(transport=ASGITransport(app=probe), base_url=BASE_URL) as probe_client:
        response = await probe_client.get("/boom")
    assert response.status_code == 507
    assert _body(response)["code"] == int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED)
    assert cast("dict[str, object]", _body(response)["data"])["used_ratio"] == 0.9
    del client, api_app


def test_stored_message_header_view_never_carries_payload() -> None:
    """端口值对象结构上不含 `payload`（响应拿不到正文的类型保证）。"""
    message = StoredMessage(
        message_id="m1",
        log_id="l1",
        topic_id=1,
        partition_no=0,
        offset=0,
        key=None,
        payload=b"secret",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=datetime.now(UTC),
    )
    view = message_header_view(message)
    assert not hasattr(view, "payload")
    assert view.payload_len == len(b"secret")
    assert len(view.payload_sha256) == 64
