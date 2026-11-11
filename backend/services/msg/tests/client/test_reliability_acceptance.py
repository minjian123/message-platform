"""01-8-3 可靠性验收用例（Kiwi 2483）。

在同一条真实链路上验收 8B_04 的退出条件：业务提交 → `sys_outbox` → relay → msg 数据面 →
消费端。bms 仓 `tests/outbox/` 已覆盖单点语义（Kiwi 2442 / 2481），本文件只补端到端组合：

- V1 审计事件全链路：`build_operation_event` + `AuditEventEmitter` → 真发件箱 → msg relay →
  真数据面 → 消费端解码，断言 `event_id` / 租户 / `trace_id` / 载荷 / `record_ref` 一致；
- V2 / V3 重复投递与 outbox 重放：同一 `event_id` 被消费多次，`ProcessedEventStore` 只放行一次；
- V4 迁移在途：失败保行，切到可用链路后补发恰好一次；
- V5 失败与恢复：主题缺失 / 连接失败 / 超限转死信均可定位，恢复后补发一次；
- V6 进程重启：重建引擎与会话后 pending 补发、delivered 不重投；
- V7 / V8 双写窗口与回滚切回：两实现先后各跑一轮，消息总数不增、行为恢复。

只在 msg 仓能同时 import 两侧（`bms-core` 是路径依赖），故验收证据落在这里。
"""

from __future__ import annotations

import base64
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

import httpx
import pytest
from bms_core.audit.base import RecordRef
from bms_core.audit.emit import AuditEventEmitter
from bms_core.audit.events import OPERATION_LOG_EVENT, build_operation_event
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.config import Settings, get_settings
from bms_core.db.engine import PLATFORM_DB_KEY, EngineFactory
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import SessionFactory, session_scope
from bms_core.db.sync import DbSession
from bms_core.events.base import EventEnvelope, EventPublisher
from bms_core.events.topic import decode_event
from bms_core.metrics.base import BaseMetrics, MetricLabels
from bms_core.models.base import Base
from bms_core.models.outbox import SysEventConsumed, SysEventDeadLetter, SysOutbox
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher
from bms_core.outbox.base import (
    OUTBOX_STATUS_DEAD,
    OUTBOX_STATUS_DELIVERED,
    OUTBOX_STATUS_PENDING,
    DeadLetterRecord,
)
from bms_core.outbox.consumed import ProcessedEventStore
from bms_core.outbox.dispatcher import MsgOutboxDispatcher, PollOutboxDispatcher
from bms_core.outbox.store import SqlOutboxStore
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Connection, select, update
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql.schema import Table

from tests_support.bms_port_contract import EVENT_NAMESPACE, EVENT_TOPIC, EVENT_TYPE
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2483)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

BUSINESS_TENANT = "demo"
"""业务事件租户编码（与 msg 测试租户源一致）。"""

AUDIT_TENANT = "7001"
"""审计事件租户主键（`demo` 的雪花 id；纯数字由 msg 中间件按 id 解析）。"""

AUDIT_TOPIC = "bms-sys-operation"
"""审计事件 topic（`sys.operation.logged` 按 `topic_for()` 派生）。"""

AUDIT_EVENT_ID = "audit-2483"
"""审计事件幂等键（用例内固定，便于逐项定位）。"""

CONSUMER = "acceptance-2483"
"""消费幂等账本的消费者标识（全部断言共用）。"""

GROUP_PREFIX = "acceptance-2483"
"""消费组前缀（每个消费批次取独立组，避免位点互相干扰）。"""


@dataclass
class OutboxEnvironment:
    """临时发件箱库 + 引擎注册表（进程重启用例会替换这两个实例）。"""

    url: str
    settings: Settings
    registry: EngineRegistry
    session_factory: SessionFactory
    store: SqlOutboxStore

    async def restart(self) -> None:
        """丢弃并重建引擎与会话工厂（模拟进程重启后的重新装配）。"""
        await self.registry.aclose()
        self.registry = EngineRegistry(EngineFactory(self.settings))
        self.session_factory = SessionFactory()


class RecordingPublisher(EventPublisher):
    """记录成功发布事件的发布器（回滚后原链路的行为证据）。"""

    def __init__(self) -> None:
        """初始化。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()

    @property
    def event_type(self) -> str:
        """事件类型（测试固定）。"""
        return "test.acceptance"

    async def publish(self, event: EventEnvelope) -> None:
        """记录一条已投递事件。"""
        self.events.add(event)

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。"""
        await self.publish(event)


class RecordingMetrics(BaseMetrics):
    """记录指标调用（投递失败 / 死信 / 积压的可定位证据）。"""

    def __init__(self) -> None:
        """初始化。"""
        self.counters: ConcurrentStableList[tuple[str, float, str]] = ConcurrentStableList()
        self.gauges: ConcurrentStableList[tuple[str, float]] = ConcurrentStableList()

    async def counter(self, name: str, *, value: float = 1.0, labels: MetricLabels | None = None) -> None:
        """记录计数器（只保留状态标签，便于断言）。"""
        status = "" if labels is None else labels.get("status", "")
        self.counters.add((name, value, status))

    async def gauge(self, name: str, *, value: float, labels: MetricLabels | None = None) -> None:
        """记录瞬时值。"""
        del labels
        self.gauges.add((name, value))

    async def histogram(self, name: str, *, value: float, labels: MetricLabels | None = None) -> None:
        """直方图（本任务不采集）。"""
        del name, value, labels

    def counter_total(self, name: str, status: str) -> float:
        """取指定指标与状态标签的累计值。

        Args:
            name: 指标名。
            status: 状态标签。

        Returns:
            float: 累计值。
        """
        return sum(
            value for item_name, value, item_status in self.counters if item_name == name and item_status == status
        )

    def gauge_values(self, name: str) -> ConcurrentStableList[float]:
        """取指定瞬时值指标的按序取值。

        Args:
            name: 指标名。

        Returns:
            ConcurrentStableList[float]: 取值序列。
        """
        return ConcurrentStableList(value for item_name, value in self.gauges if item_name == name)


_OUTBOX_TABLES: ConcurrentStableFrozenList[Table] = ConcurrentStableFrozenList(
    (
        cast("Table", SysOutbox.__table__),
        cast("Table", SysEventConsumed.__table__),
        cast("Table", SysEventDeadLetter.__table__),
    )
)


def _create_all(connection: Connection) -> None:
    """在同步连接上建发件箱三表。

    Args:
        connection: SQLAlchemy 同步连接。
    """
    Base.metadata.create_all(connection, tables=list(_OUTBOX_TABLES))


async def _create_tables(url: str) -> None:
    """建临时发件箱库三表。

    Args:
        url: 数据库连接串。
    """
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(_create_all)
    finally:
        await engine.dispose()


@pytest.fixture
async def outbox_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[OutboxEnvironment]:
    """临时 SQLite 发件箱库（三表）+ 引擎注册表 + 存储。

    Args:
        tmp_path: pytest 临时目录。
        monkeypatch: 环境变量覆盖。

    Yields:
        OutboxEnvironment: 发件箱环境。
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'acceptance.db'}"
    monkeypatch.setenv("BMS_ENV", "test")
    monkeypatch.setenv("BMS_DATABASE__PLATFORM__URL", url)
    get_settings.cache_clear()
    await _create_tables(url)
    settings = get_settings()
    registry = EngineRegistry(EngineFactory(settings))
    env = OutboxEnvironment(
        url=url,
        settings=settings,
        registry=registry,
        session_factory=SessionFactory(),
        store=SqlOutboxStore(),
    )
    try:
        yield env
    finally:
        await env.registry.aclose()
        get_settings.cache_clear()


@pytest.fixture
def audit_emitter(outbox_env: OutboxEnvironment, monkeypatch: pytest.MonkeyPatch) -> AuditEventEmitter:
    """审计事件发射器（真实发射器 + 真实发件箱存储）。

    测试隔离只替换插件解析：msg 仓 CI 不保证能读 bms 仓的事件契约快照，故用
    `contract_mode=off` 的 `SqlOutboxStore()`；写入 / 事务 / 失败分档仍是真实实现。

    Args:
        outbox_env: 发件箱环境。
        monkeypatch: 替换插件解析。

    Returns:
        AuditEventEmitter: 审计发射器。
    """
    store = SqlOutboxStore()

    def _resolve(plugin_key: str, provider: str | None = None, *, expected_version: str | None = None) -> object:
        del provider, expected_version
        assert plugin_key == "outbox_store"
        return store

    monkeypatch.setattr("bms_core.audit.emit.resolve_plugin", _resolve)
    return AuditEventEmitter(
        registry=outbox_env.registry,
        session_factory=outbox_env.session_factory,
        settings=outbox_env.settings,
    )


def _msg_publisher(app: FastAPI, *, tenant: str) -> MsgEventPublisher:
    """构造走真 msg 应用的发布器（ASGI 直连，不启网络）。

    Args:
        app: 真 msg 应用。
        tenant: 缺省租户编码。

    Returns:
        MsgEventPublisher: bms 侧发布器。
    """
    return MsgEventPublisher(
        base_url="http://msg",
        namespace=EVENT_NAMESPACE,
        tenant=tenant,
        transport=httpx.ASGITransport(app=app),
    )


def _unreachable_publisher() -> MsgEventPublisher:
    """构造连接失败的发布器（真实 `MsgEventPublisher` + 注入传输异常）。

    Returns:
        MsgEventPublisher: 恒抛 `ConnectError` 的发布器。
    """

    async def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("msg 实例不可达（注入）", request=request)

    return MsgEventPublisher(
        base_url="http://msg-unreachable",
        namespace=EVENT_NAMESPACE,
        tenant=BUSINESS_TENANT,
        transport=httpx.MockTransport(_handler),
    )


def _msg_dispatcher(
    env: OutboxEnvironment,
    publisher: EventPublisher,
    *,
    max_retries: int = 3,
    metrics: BaseMetrics | None = None,
) -> MsgOutboxDispatcher:
    """构造 msg 版投递器（零退避，失败行下一轮立即可取）。

    Args:
        env: 发件箱环境。
        publisher: 事件发布端口。
        max_retries: 转死信前的最大重试次数。
        metrics: 指标记录器。

    Returns:
        MsgOutboxDispatcher: 投递器。
    """
    return MsgOutboxDispatcher(
        store=env.store,
        publisher=publisher,
        engine_registry=env.registry,
        session_factory=env.session_factory,
        batch_size=100,
        max_retries=max_retries,
        backoff=0.0,
        db_keys=ConcurrentStableList((PLATFORM_DB_KEY,)),
        metrics=metrics,
    )


def _poll_dispatcher(env: OutboxEnvironment, publisher: EventPublisher) -> PollOutboxDispatcher:
    """构造原链路投递器（回滚后行为证据）。

    Args:
        env: 发件箱环境。
        publisher: 事件发布端口。

    Returns:
        PollOutboxDispatcher: 投递器。
    """
    return PollOutboxDispatcher(
        store=env.store,
        publisher=publisher,
        engine_registry=env.registry,
        session_factory=env.session_factory,
        batch_size=100,
        max_retries=3,
        backoff=0.0,
        db_keys=ConcurrentStableList((PLATFORM_DB_KEY,)),
    )


@asynccontextmanager
async def _transaction(env: OutboxEnvironment) -> AsyncGenerator[DbSession]:
    """开一个平台库事务（会话 + 提交边界）。

    Args:
        env: 发件箱环境。

    Yields:
        DbSession: 事务内会话。
    """
    async with (
        session_scope(env.registry, db_key=PLATFORM_DB_KEY, factory=env.session_factory) as session,
        session.begin(),
    ):
        yield session


async def _enqueue(env: OutboxEnvironment, event_type: str = EVENT_TYPE, *, event_id: str | None = None) -> str:
    """在业务事务内写一条发件箱事件。

    Args:
        env: 发件箱环境。
        event_type: 事件类型。
        event_id: 幂等键（缺省由发件箱生成）。

    Returns:
        str: 事件 ID。
    """
    event = EventEnvelope(
        event_type=event_type,
        payload=ConcurrentStableDict({"order_id": "o-2483"}),
        event_id=event_id,
        trace_id="trace-2483",
        aggregate_key=f"order-{event_id or '2483'}",
    )
    async with _transaction(env) as session:
        return await env.store.enqueue(session, event)


async def _outbox_rows(env: OutboxEnvironment) -> ConcurrentStableList[SysOutbox]:
    """读取全部发件箱行（按主键升序）。

    Args:
        env: 发件箱环境。

    Returns:
        ConcurrentStableList[SysOutbox]: 行列表。
    """
    async with session_scope(env.registry, db_key=PLATFORM_DB_KEY, factory=env.session_factory) as session:
        rows: ConcurrentStableList[SysOutbox] = ConcurrentStableList()
        for row in (await session.execute(select(SysOutbox).order_by(SysOutbox.id))).scalars():
            rows.add(row)
        return rows


async def _row_by_event_id(env: OutboxEnvironment, event_id: str) -> SysOutbox | None:
    """按事件 ID 取发件箱行。

    Args:
        env: 发件箱环境。
        event_id: 事件 ID。

    Returns:
        SysOutbox | None: 行；不存在 None。
    """
    async with session_scope(env.registry, db_key=PLATFORM_DB_KEY, factory=env.session_factory) as session:
        return (await session.execute(select(SysOutbox).where(SysOutbox.event_id == event_id))).scalars().first()


async def _mark_pending(env: OutboxEnvironment, event_id: str) -> None:
    """把已投递行重置为待投递（模拟「发布成功但确认丢失」的重投来源）。

    Args:
        env: 发件箱环境。
        event_id: 事件 ID。
    """
    async with _transaction(env) as session:
        statement = (
            update(SysOutbox)
            .where(SysOutbox.event_id == event_id)
            .values(
                status=OUTBOX_STATUS_PENDING,
                retry_count=0,
                next_retry_at=None,
                delivered_at=None,
                error_msg=None,
            )
        )
        await session.execute(statement)


async def _replay(env: OutboxEnvironment, event_id: str) -> int:
    """重放一条已投递 / 死信事件。

    Args:
        env: 发件箱环境。
        event_id: 事件 ID。

    Returns:
        int: 重置条数。
    """
    async with _transaction(env) as session:
        return await env.store.replay(session, event_id=event_id)


async def _dead_letters(env: OutboxEnvironment) -> tuple[ConcurrentStableList[DeadLetterRecord], int]:
    """列死信。

    Args:
        env: 发件箱环境。

    Returns:
        tuple[ConcurrentStableList[DeadLetterRecord], int]: （记录，总数）。
    """
    async with session_scope(env.registry, db_key=PLATFORM_DB_KEY, factory=env.session_factory) as session:
        return await env.store.list_dead_letters(session)


async def _get_dead_letter(env: OutboxEnvironment, dead_letter_id: int) -> DeadLetterRecord | None:
    """取单条死信。

    Args:
        env: 发件箱环境。
        dead_letter_id: 死信主键。

    Returns:
        DeadLetterRecord | None: 记录。
    """
    async with session_scope(env.registry, db_key=PLATFORM_DB_KEY, factory=env.session_factory) as session:
        return await env.store.get_dead_letter(session, dead_letter_id)


async def _create_topic(client: AsyncClient, *, topic: str, tenant: str) -> None:
    """经管理面创建主题。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": EVENT_NAMESPACE, "name": topic, "partition_count": 1},
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text


async def _join(client: AsyncClient, *, group: str, topic: str, tenant: str) -> None:
    """加入消费组。"""
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": group, "namespace": EVENT_NAMESPACE, "topic": topic, "consumer_id": "c1"},
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text


async def _consume(
    client: AsyncClient, *, group: str, topic: str, tenant: str
) -> ConcurrentStableList[dict[str, object]]:
    """拉取主题当前可投递批（新组从最早位点开始）。"""
    response = await client.post(
        "/api/v1/msg/consume",
        json={
            "group": group,
            "consumer_id": "c1",
            "namespace": EVENT_NAMESPACE,
            "topic": topic,
            "partition": 0,
            "max_messages": 10,
            "max_wait_ms": 0,
        },
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text
    body = cast("dict[str, object]", response.json())
    data = cast("dict[str, object]", body["data"])
    items: ConcurrentStableList[dict[str, object]] = ConcurrentStableList()
    for item in cast("list[object]", data["messages"]):
        items.add(cast("dict[str, object]", item))
    return items


def _headers(item: dict[str, object]) -> ConcurrentStableDict[str, str]:
    """取消息结构化头。"""
    return ConcurrentStableDict(cast("dict[str, str]", item["headers"]))


def _decode(item: dict[str, object]) -> EventEnvelope:
    """解码一条消息为事件信封。"""
    body = base64.b64decode(cast("str", item["payload"]))
    return decode_event(body, _headers(item))


def _decoded(items: ConcurrentStableList[dict[str, object]]) -> ConcurrentStableList[EventEnvelope]:
    """批量解码。"""
    events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
    for item in items:
        events.add(_decode(item))
    return events


async def _consumer_side_effects(
    env: OutboxEnvironment, events: ConcurrentStableList[EventEnvelope]
) -> ConcurrentStableList[str]:
    """按消费幂等账本处理事件，返回实际执行的副作用（首次才执行）。

    Args:
        env: 发件箱环境（`sys_event_consumed` 同库）。
        events: 消费到的事件。

    Returns:
        ConcurrentStableList[str]: 实际执行副作用的 `event_id` 序列。
    """
    side_effects: ConcurrentStableList[str] = ConcurrentStableList()
    async with _transaction(env) as session:
        store = ProcessedEventStore(session)
        for event in events:
            assert event.event_id is not None
            if await store.mark(consumer=CONSUMER, event_id=event.event_id, event_type=event.event_type):
                side_effects.add(event.event_id)
    return side_effects


def _audit_event() -> EventEnvelope:
    """构造一条带记录级定位的操作审计事件。

    Returns:
        EventEnvelope: 审计事件信封。
    """
    return build_operation_event(
        tenant_id=AUDIT_TENANT,
        user_id=1,
        username="acceptance",
        module="消息平台",
        action="msg.publish",
        method="POST",
        path="/api/v1/msg/publish",
        query=None,
        params=None,
        ip="127.0.0.1",
        user_agent="pytest",
        cost_time=12,
        status=200,
        error_msg=None,
        trace_id="trace-2483",
        module_key="msg",
        resource_key="topic",
        record_ref=RecordRef(type="msg.message", id="log-2483"),
    )


# ---------------------------------------------------------------------------
# V1 审计事件全链路
# ---------------------------------------------------------------------------


async def test_audit_event_reaches_msg_and_keeps_record_ref(
    msg_app: FastAPI, outbox_env: OutboxEnvironment, audit_emitter: AuditEventEmitter
) -> None:
    """V1：审计事件经发件箱 → msg relay → 真数据面 → 消费端，字段与记录级定位不丢。"""
    event = replace(_audit_event(), event_id=AUDIT_EVENT_ID)
    assert await audit_emitter.emit(event) is True

    pending = await _row_by_event_id(outbox_env, AUDIT_EVENT_ID)
    assert pending is not None

    publisher = _msg_publisher(msg_app, tenant=AUDIT_TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=AUDIT_TOPIC, tenant=AUDIT_TENANT)
            result = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert result.published == 1
            await _join(client, group=f"{GROUP_PREFIX}-audit", topic=AUDIT_TOPIC, tenant=AUDIT_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-audit", topic=AUDIT_TOPIC, tenant=AUDIT_TENANT)
    finally:
        await publisher.aclose()

    assert len(items) == 1
    item = items[0]
    assert item["key"] == event.aggregate_key
    headers = _headers(item)
    assert headers["event_id"] == AUDIT_EVENT_ID
    assert headers["event_type"] == OPERATION_LOG_EVENT
    assert headers["tenant_id"] == AUDIT_TENANT
    assert headers["trace_id"] == "trace-2483"

    decoded = _decode(item)
    assert decoded.event_id == AUDIT_EVENT_ID
    assert decoded.event_type == OPERATION_LOG_EVENT
    assert decoded.tenant_id == AUDIT_TENANT
    assert decoded.trace_id == "trace-2483"
    assert decoded.aggregate_key == "tenant:7001"
    assert decoded.payload.get("module") == "消息平台"
    assert decoded.payload.get("action") == "msg.publish"
    assert decoded.payload.get("record_ref") == {"type": "msg.message", "id": "log-2483"}

    delivered = await _row_by_event_id(outbox_env, AUDIT_EVENT_ID)
    assert delivered is not None
    assert delivered.status == OUTBOX_STATUS_DELIVERED
    assert delivered.delivered_at is not None


# ---------------------------------------------------------------------------
# V2 / V3 效果一次
# ---------------------------------------------------------------------------


async def test_duplicate_delivery_has_single_side_effect(msg_app: FastAPI, outbox_env: OutboxEnvironment) -> None:
    """V2：同一条事件因确认丢失被重投两次，消费端只执行一次副作用。"""
    event_id = await _enqueue(outbox_env, event_id="order-dup-2483")
    publisher = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            first = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert first.published == 1
            await _mark_pending(outbox_env, event_id)
            second = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert second.published == 1
            await _join(client, group=f"{GROUP_PREFIX}-dup", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-dup", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await publisher.aclose()

    assert len(items) == 2
    events = _decoded(items)
    assert [event.event_id for event in events] == [event_id, event_id]

    side_effects = await _consumer_side_effects(outbox_env, events)
    assert list(side_effects) == [event_id]


async def test_outbox_replay_keeps_single_side_effect(msg_app: FastAPI, outbox_env: OutboxEnvironment) -> None:
    """V3：已投递事件经 `replay()` 重投，消费端收到两份但副作用仍一次。"""
    event_id = await _enqueue(outbox_env, event_id="order-replay-2483")
    publisher = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            first = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert first.published == 1
            assert await _replay(outbox_env, event_id) == 1
            second = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert second.published == 1
            await _join(client, group=f"{GROUP_PREFIX}-replay", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-replay", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await publisher.aclose()

    assert len(items) == 2
    events = _decoded(items)
    assert [event.event_id for event in events] == [event_id, event_id]

    side_effects = await _consumer_side_effects(outbox_env, events)
    assert list(side_effects) == [event_id]


# ---------------------------------------------------------------------------
# V4 迁移在途
# ---------------------------------------------------------------------------


async def test_in_flight_event_survives_switch_and_delivers_once(
    msg_app: FastAPI, outbox_env: OutboxEnvironment
) -> None:
    """V4：未确认事件在切换中不丢，切到可用链路后补发恰好一次。"""
    event_id = await _enqueue(outbox_env, event_id="order-inflight-2483")
    broken = _unreachable_publisher()
    working = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    try:
        failed = await _msg_dispatcher(outbox_env, broken).dispatch_once(db_key=PLATFORM_DB_KEY)
        assert (failed.published, failed.failed) == (0, 1)
        row = await _row_by_event_id(outbox_env, event_id)
        assert row is not None
        assert row.status == OUTBOX_STATUS_PENDING
        assert row.retry_count == 1
        assert row.error_msg is not None and ("ConnectError" in row.error_msg or "不可达" in row.error_msg)

        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            ok = await _msg_dispatcher(outbox_env, working).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert (ok.published, ok.failed) == (1, 0)
            await _join(client, group=f"{GROUP_PREFIX}-inflight", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-inflight", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await broken.aclose()
        await working.aclose()

    assert len(items) == 1
    events = _decoded(items)
    assert events[0].event_id == event_id
    side_effects = await _consumer_side_effects(outbox_env, events)
    assert list(side_effects) == [event_id]


# ---------------------------------------------------------------------------
# V5 失败与恢复
# ---------------------------------------------------------------------------


async def test_unreachable_and_exhausted_failures_are_observable(
    msg_app: FastAPI, outbox_env: OutboxEnvironment
) -> None:
    """V5a：主题缺失 404 / 连接失败 / 超限转死信均可定位，不静默丢事件。"""
    first_id = await _enqueue(outbox_env, event_id="order-fail-404-2483")
    second_id = await _enqueue(outbox_env, event_id="order-fail-conn-2483")
    missing_topic = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    unreachable = _unreachable_publisher()
    metrics = RecordingMetrics()
    try:
        first_round = await _msg_dispatcher(outbox_env, missing_topic, max_retries=3, metrics=metrics).dispatch_once(
            db_key=PLATFORM_DB_KEY
        )
        assert (first_round.published, first_round.failed) == (0, 2)
        for event_id in (first_id, second_id):
            row = await _row_by_event_id(outbox_env, event_id)
            assert row is not None
            assert row.status == OUTBOX_STATUS_PENDING
            assert row.retry_count == 1
            assert row.error_msg is not None and "404" in row.error_msg

        second_round = await _msg_dispatcher(outbox_env, unreachable, max_retries=2, metrics=metrics).dispatch_once(
            db_key=PLATFORM_DB_KEY
        )
        assert (second_round.published, second_round.failed, second_round.dead) == (0, 2, 2)
    finally:
        await missing_topic.aclose()
        await unreachable.aclose()

    for event_id in (first_id, second_id):
        row = await _row_by_event_id(outbox_env, event_id)
        assert row is not None
        assert row.status == OUTBOX_STATUS_DEAD
        assert row.error_msg is not None and ("ConnectError" in row.error_msg or "不可达" in row.error_msg)

    records, total = await _dead_letters(outbox_env)
    assert total == 2
    assert {record.event_id for record in records} == {first_id, second_id}
    detail = await _get_dead_letter(outbox_env, records[0].id)
    assert detail is not None
    assert detail.source == "outbox"
    assert detail.retry_count == 2
    assert detail.error_msg

    assert metrics.counter_total("bms_outbox_delivery_total", "failed") == 4.0
    assert metrics.counter_total("bms_outbox_delivery_total", "dead") == 2.0
    assert list(metrics.gauge_values("bms_outbox_backlog")) == [2.0, 0.0]


async def test_dead_letter_recovery_delivers_once(msg_app: FastAPI, outbox_env: OutboxEnvironment) -> None:
    """V5b：转死信后建主题并 `replay()`，恢复投递恰好一次。"""
    event_id = await _enqueue(outbox_env, event_id="order-dead-2483")
    unreachable = _unreachable_publisher()
    recovery = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    try:
        for _ in range(2):
            result = await _msg_dispatcher(outbox_env, unreachable, max_retries=2).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert result.failed == 1
        dead = await _row_by_event_id(outbox_env, event_id)
        assert dead is not None and dead.status == OUTBOX_STATUS_DEAD

        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            assert await _replay(outbox_env, event_id) == 1
            ok = await _msg_dispatcher(outbox_env, recovery).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert (ok.published, ok.failed) == (1, 0)
            await _join(client, group=f"{GROUP_PREFIX}-dead", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-dead", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await unreachable.aclose()
        await recovery.aclose()

    assert len(items) == 1
    events = _decoded(items)
    assert events[0].event_id == event_id
    side_effects = await _consumer_side_effects(outbox_env, events)
    assert list(side_effects) == [event_id]


# ---------------------------------------------------------------------------
# V6 进程重启
# ---------------------------------------------------------------------------


async def test_process_restart_delivers_pending_and_skips_delivered(
    msg_app: FastAPI, outbox_env: OutboxEnvironment
) -> None:
    """V6：重建引擎 / 会话 / 投递器后，pending 补发一次、delivered 不重投。"""
    delivered_id = await _enqueue(outbox_env, event_id="order-delivered-2483")
    publisher = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            only_first = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert only_first.published == 1

            # 重启前再入一条：它必须留在 pending，等重启后的新装配补发。
            pending_id = await _enqueue(outbox_env, event_id="order-pending-2483")
            row = await _row_by_event_id(outbox_env, pending_id)
            assert row is not None and row.status == OUTBOX_STATUS_PENDING

            await outbox_env.restart()
            after = await _msg_dispatcher(outbox_env, publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert after.published == 1
            await _join(client, group=f"{GROUP_PREFIX}-restart", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-restart", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await publisher.aclose()

    rows = await _outbox_rows(outbox_env)
    assert {row.event_id for row in rows if row.status == OUTBOX_STATUS_DELIVERED} == {
        delivered_id,
        pending_id,
    }
    assert len(items) == 2
    assert {event.event_id for event in _decoded(items)} == {delivered_id, pending_id}


# ---------------------------------------------------------------------------
# V7 / V8 双写窗口与回滚
# ---------------------------------------------------------------------------


async def test_two_dispatchers_do_not_duplicate(msg_app: FastAPI, outbox_env: OutboxEnvironment) -> None:
    """V7：msg 与 poll 两实现先后各跑一轮，msg 侧消息总数恰好 1。"""
    msg_id = await _enqueue(outbox_env, event_id="order-msg-2483")
    msg_publisher = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    recorder = RecordingPublisher()
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            msg_first = await _msg_dispatcher(outbox_env, msg_publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert msg_first.published == 1
            poll_after = await _poll_dispatcher(outbox_env, recorder).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert poll_after.published == 0

            poll_id = await _enqueue(outbox_env, event_id="order-poll-2483")
            poll_first = await _poll_dispatcher(outbox_env, recorder).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert poll_first.published == 1
            msg_after = await _msg_dispatcher(outbox_env, msg_publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert msg_after.published == 0

            await _join(client, group=f"{GROUP_PREFIX}-dual", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            items = await _consume(client, group=f"{GROUP_PREFIX}-dual", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
    finally:
        await msg_publisher.aclose()

    assert len(items) == 1
    assert _decoded(items)[0].event_id == msg_id
    assert [event.event_id for event in recorder.events] == [poll_id]


async def test_rollback_to_poll_restores_behavior(msg_app: FastAPI, outbox_env: OutboxEnvironment) -> None:
    """V8：切回原链路后新事件由 poll 投递，已投递事件不重投、不丢失。"""
    audit_id = await _enqueue(outbox_env, event_id="order-before-rollback-2483")
    msg_publisher = _msg_publisher(msg_app, tenant=BUSINESS_TENANT)
    recorder = RecordingPublisher()
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            before = await _msg_dispatcher(outbox_env, msg_publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert before.published == 1
            await _join(client, group=f"{GROUP_PREFIX}-before", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            before_items = await _consume(
                client, group=f"{GROUP_PREFIX}-before", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT
            )
            assert [event.event_id for event in _decoded(before_items)] == [audit_id]

            # 切回 poll：已投递行不重投。
            rollback_round = await _poll_dispatcher(outbox_env, recorder).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert rollback_round.published == 0

            # 新事件由 poll 投递一次，再切回 msg 不重投。
            new_id = await _enqueue(outbox_env, event_id="order-after-rollback-2483")
            poll_round = await _poll_dispatcher(outbox_env, recorder).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert poll_round.published == 1
            msg_round = await _msg_dispatcher(outbox_env, msg_publisher).dispatch_once(db_key=PLATFORM_DB_KEY)
            assert msg_round.published == 0

            await _join(client, group=f"{GROUP_PREFIX}-after", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT)
            after_items = await _consume(
                client, group=f"{GROUP_PREFIX}-after", topic=EVENT_TOPIC, tenant=BUSINESS_TENANT
            )
    finally:
        await msg_publisher.aclose()

    assert [event.event_id for event in _decoded(after_items)] == [audit_id]
    assert [event.event_id for event in recorder.events] == [new_id]
