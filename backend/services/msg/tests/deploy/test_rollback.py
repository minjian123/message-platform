"""回滚演练与记录（Kiwi 2478；任务 07-4-2 §13）。

覆盖策略：

- **闭环走真实对象**：场景 / 矩阵 / 前置清单用仓内真实 TOML；msg 侧真路由 → 真帧 → 真数据面；
  原链路侧真 `HttpServiceClient` + 真实业务函数；未确认事件走真 bms `SqlOutboxStore` 与
  `PollOutboxDispatcher`（独立内存库建 `sys_outbox`）。
- **判据只有一份**：编排、注入、回滚、恢复、四类一致性全部走 `ops.rollback.run_rollback_drill`；
  网络目标与进程内目标只差「字节往哪送」。
- **失败分支走真实损坏 / 可控目标**：坏 TOML、判据未命中、回滚没生效、恢复超预算、重复投递、
  位点回退、死信变化、审计链断，脚本按真实读取与调用判失败。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from bms_core.catalog.base import fetch_catalog_snapshot
from bms_core.core import assembly
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.config import Settings
from bms_core.core.plugin import default_plugin_registry
from bms_core.core.resources import ResourceManager
from bms_core.db.engine import PLATFORM_DB_KEY
from bms_core.events.base import EventEnvelope, EventPublisher, EventPublishError
from bms_core.events.topic import topic_for
from bms_core.fallback.null import NullFallbackPolicy
from bms_core.models.base import Base
from bms_core.models.outbox import SysOutbox
from bms_core.msgclient.adapter.policy import default_circuit
from bms_core.msgclient.retry import RetryPolicy
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.outbox.base import OUTBOX_STATUS_DELIVERED
from bms_core.outbox.dispatcher import PollOutboxDispatcher
from bms_core.outbox.store import SqlOutboxStore
from bms_core.ratelimit.null import NullRateLimiter
from bms_core.servicecall.http import HttpServiceClient
from fastapi import FastAPI, Response
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from bms_msg.rpc.registry import RpcRegistrationError
from ops import migration, rollback
from tests_support.bms_port_contract import (
    BASE_URL,
    EVENT_NAMESPACE,
    EVENT_TYPE,
    SNAPSHOT_PATH,
    TARGET_METHOD,
    TARGET_SERVICE,
    TrackingHandler,
    msg_client,
    msg_event_publisher,
    snapshot_body,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_transport import AsgiRpcTransport, RpcHttpRequest, RpcHttpResponse

pytestmark = pytest.mark.kiwi_id(2478)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]
"""夹具经 import 暴露给 pytest（`tests_support` 是辅助包，不是本文件的实现依赖）。"""

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_PRECONDITIONS = _REPO / "deploy" / "migration" / "preconditions.toml"
_MATRIX = _REPO / "deploy" / "migration" / "switch_matrix.toml"
_SCENARIOS = _REPO / "deploy" / "migration" / "rollback_scenarios.toml"
_DESIGN = (
    _REPO
    / "msg文档"
    / "项目"
    / "08B_消息平台"
    / "任务"
    / "07_部署迁移与验收"
    / "07_部署迁移与验收_04_bms迁移与回滚"
    / "07_部署迁移与验收_04_bms迁移与回滚_02_回滚演练与记录"
    / "设计"
    / "01_详细设计_07-4-2_回滚演练与记录.md"
)

_NOW = datetime(2026, 11, 10, 12, 0, 0, tzinfo=UTC)
"""固定时刻（记录可复现）。"""

_TENANT_ID = 7001
"""测试租户主键（与 `tests/conftest.py` 同值；位点枚举需要）。"""


class _StepClock:
    """每次调用推进 5ms 的假时钟（`elapsed_ms` 可断言且不真实等待）。"""

    def __init__(self) -> None:
        """初始化。"""
        self._value = 0.0

    def __call__(self) -> float:
        """返回当前值并推进。"""
        current = self._value
        self._value += 0.005
        return current


class _RecordingSink:
    """带哈希链的审计出口替身（`verify()` 复算链路是否连续）。"""

    def __init__(self) -> None:
        """初始化（创世摘要 + 空链）。"""
        self.entries: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
        self._head = "genesis"

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """追加一条审计事件。

        Args:
            event: 事件信封。
            required: 强审计开关（忽略）。

        Returns:
            bool: 恒 True。
        """
        del required
        envelope = cast("EventEnvelope", event)
        event_id = str(envelope.event_id or "")
        payload = f"{self._head}|{event_id}|{envelope.event_type}"
        digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
        row: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        row.set("prev", self._head)
        row.set("digest", digest)
        row.set("event_id", event_id)
        row.set("event_type", envelope.event_type)
        self.entries.add(row)
        self._head = digest
        return True

    def count(self) -> int:
        """审计条数。

        Returns:
            int: 条数。
        """
        return len(self.entries)

    def digest(self) -> str:
        """当前链头摘要。

        Returns:
            str: 摘要。
        """
        return self._head

    def verify(self) -> bool:
        """复算哈希链（prev 连续且摘要可重现）。

        Returns:
            bool: 完整为 True。
        """
        head = "genesis"
        for row in self.entries:
            expected = hashlib.sha256(f"{head}|{row['event_id']}|{row['event_type']}".encode()).hexdigest()[:16]
            if row["prev"] != head or row["digest"] != expected:
                return False
            head = row["digest"]
        return True


class _Switchable(EventPublisher):
    """可切换发布端口（只换实现，不改调用点）。"""

    def __init__(self, publisher: EventPublisher) -> None:
        """初始化。

        Args:
            publisher: 当前实现。
        """
        self.publisher = publisher

    @property
    def event_type(self) -> str:
        """事件类型（跟随当前实现）。"""
        return self.publisher.event_type

    async def publish(self, event: EventEnvelope) -> None:
        """转发发布。

        Args:
            event: 事件信封。
        """
        await self.publisher.publish(event)

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """转发事务发布。

        Args:
            event: 事件信封。
        """
        await self.publisher.publish_transactional(event)

    async def aclose(self) -> None:
        """释放当前实现。"""
        await self.publisher.aclose()


class _RecordedPublisher(EventPublisher):
    """原链路事件侧记录型替身（RocketMQ broker 未部署）。"""

    def __init__(self) -> None:
        """初始化（空记录）。"""
        self.delivered: ConcurrentStableList[str] = ConcurrentStableList()

    @property
    def event_type(self) -> str:
        """事件类型（固定 `rocketmq`）。"""
        return "rocketmq"

    async def publish(self, event: EventEnvelope) -> None:
        """记录一条投递。

        Args:
            event: 事件信封。
        """
        self.delivered.add(event.event_id or "")

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。

        Args:
            event: 事件信封。
        """
        await self.publish(event)


class _UnavailablePublisher(EventPublisher):
    """不可达发布器（注入 `unavailable`）。"""

    @property
    def event_type(self) -> str:
        """事件类型（固定 `msg`）。"""
        return "msg"

    async def publish(self, event: EventEnvelope) -> None:
        """恒抛投递失败。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 恒定失败。
        """
        del event
        raise EventPublishError("msg 实例不可达（注入）")

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 恒定失败。
        """
        await self.publish(event)


class _FlakyPublisher(EventPublisher):
    """每第 N 次失败（注入 `error_rate`）。"""

    def __init__(self, inner: EventPublisher, *, fail_every: int) -> None:
        """初始化。

        Args:
            inner: 正常实现。
            fail_every: 失败频率。
        """
        self._inner = inner
        self._fail_every = max(fail_every, 1)
        self._attempts = 0

    @property
    def event_type(self) -> str:
        """事件类型（跟随内层）。"""
        return self._inner.event_type

    async def publish(self, event: EventEnvelope) -> None:
        """按频率失败后转发。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 命中失败频率。
        """
        self._attempts += 1
        if self._attempts % self._fail_every == 0:
            raise EventPublishError("msg 发布错误率超阈值（注入）")
        await self._inner.publish(event)

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。

        Args:
            event: 事件信封。
        """
        await self.publish(event)


class _Registry:
    """最小引擎注册表适配（投递器只需要 `get` / `is_sync_only`）。"""

    def __init__(self, engine: AsyncEngine) -> None:
        """初始化。

        Args:
            engine: 内存异步引擎。
        """
        self._engine = engine

    def is_sync_only(self, db_key: str) -> bool:
        """是否同步方言。

        Args:
            db_key: 数据源键。

        Returns:
            bool: 恒 False。
        """
        del db_key
        return False

    async def get(self, db_key: str, *, read_only: bool = False) -> AsyncEngine:
        """取引擎。

        Args:
            db_key: 数据源键。
            read_only: 是否只读（忽略）。

        Returns:
            AsyncEngine: 内存引擎。
        """
        del db_key, read_only
        return self._engine


class _FailOnceTransport:
    """首跳抛传输错误、次跳委托真 ASGI 传输（在途重试注入面）。"""

    def __init__(self, inner: AsgiRpcTransport) -> None:
        """初始化。

        Args:
            inner: 真 ASGI 传输。
        """
        self._inner = inner
        self._remaining = 1
        self.keys: ConcurrentStableList[str] = ConcurrentStableList()

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """记录幂等键；首跳失败。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 响应。

        Raises:
            RpcTransportError: 首跳注入失败。
        """
        from bms_core.msgclient.frame import decode_frame
        from bms_core.msgclient.transport import RpcTransportError

        self.keys.add(decode_frame(request.body).header.get("idempotency_key") or "")
        if self._remaining:
            self._remaining -= 1
            raise RpcTransportError("首跳失败（注入）")
        return await self._inner.send(request)

    def open_stream(self, request: RpcHttpRequest) -> Any:
        """流式面委托。

        Args:
            request: 传输层入参。

        Returns:
            Any: 内层流。
        """
        return self._inner.open_stream(request)

    async def aclose(self) -> None:
        """释放内部传输。"""
        await self._inner.aclose()

    def stats(self) -> Any:
        """统计委托。

        Returns:
            Any: 内层统计。
        """
        return self._inner.stats()


def _settings(
    *,
    event: str = "msg",
    client: str = "msg",
    namespace: str = EVENT_NAMESPACE,
    service: str = "msg",
    base_url: str = BASE_URL,
) -> Settings:
    """构造一份可控配置（只改开关与实例地址）。

    Args:
        event: `[event].provider`。
        client: `[service_client].provider`。
        namespace: 事件命名空间。
        service: `[app].service`。
        base_url: msg 实例基址。

    Returns:
        Settings: 应用配置。
    """
    settings = Settings()
    settings.app.service = service
    settings.event.provider = event
    settings.event.options.set("namespace", namespace)
    settings.service_client.provider = client
    settings.service_client.instances.set("msg", ConcurrentStableList([base_url]))
    return settings


@pytest.fixture
def switch_settings() -> Iterator[Settings]:
    """登记平台插件后的配置（`resolve_plugin` 能解析 event / service_client）。

    Yields:
        Settings: 已装配的配置。
    """
    registry = default_plugin_registry()
    registry.reset()
    settings = _settings()
    assembly.register_platform_plugins(settings, FastAPI(), ResourceManager())
    try:
        yield settings
    finally:
        registry.reset()


@pytest.fixture
async def outbox_engine() -> AsyncIterator[AsyncEngine]:
    """独立内存库（只建发件箱 / 事件死信两张表）。

    Yields:
        AsyncEngine: 内存 SQLite 引擎。
    """
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as connection:
        for table in ("sys_outbox", "sys_event_dead_letter"):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield engine
    finally:
        await engine.dispose()


_write_seq = 0
"""非幂等写方法序号（同一 app 上多次演练不撞名）。"""


def _register_method(app: FastAPI, method: str, handler: TrackingHandler, *, idempotent: bool) -> None:
    """登记目标方法（重复登记视为幂等）。

    Args:
        app: 真 msg 应用。
        method: 方法名。
        handler: 处理器。
        idempotent: 是否幂等。
    """
    with suppress(RpcRegistrationError):
        app.state.rpc_registry.register(TARGET_SERVICE, method, handler, idempotent=idempotent)


class _InProcessTargets:
    """进程内双链路目标：真 msg 应用 + 真 outbox + 原链路真客户端。"""

    def __init__(
        self,
        *,
        app: FastAPI,
        settings: Settings,
        engine: AsyncEngine,
        event_type: str,
        transport: str = "recording",
    ) -> None:
        """初始化。

        Args:
            app: 真 msg 应用。
            settings: 已装配配置。
            engine: 发件箱内存引擎。
            event_type: 本次演练事件类型（每轮新主题）。
            transport: 原链路事件侧载体口径（`recording` / `rocketmq`）。
        """
        self._app = app
        self._settings = settings
        self._event_type = event_type
        self._topic = topic_for(event_type)
        self._http = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        self._msg_publisher = msg_event_publisher(app, tenant="demo")
        self._recording = _RecordedPublisher()
        self._switchable = _Switchable(self._msg_publisher)
        self._base_publisher: EventPublisher = self._msg_publisher
        self._transport = transport
        self._injected = ""
        self._latency_ms = 0
        self._effects: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        self._closed = False
        self._audit = _RecordingSink()
        app.state.audit_sink = self._audit
        self._store = SqlOutboxStore()
        self._registry = _Registry(engine)
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._dispatcher = PollOutboxDispatcher(
            store=self._store,
            publisher=self._switchable,
            engine_registry=cast("Any", self._registry),
            session_factory=None,
            batch_size=100,
            max_retries=3,
            backoff=0.0,
            db_keys=ConcurrentStableList((PLATFORM_DB_KEY,)),
        )
        self._read_only_handler = TrackingHandler()
        _register_method(app, TARGET_METHOD, self._read_only_handler, idempotent=True)

    def original_config(self) -> migration.SwitchReport:
        """回滚前的配置解析。

        Returns:
            migration.SwitchReport: 两端口判定。
        """
        self._settings.event.provider = "rocketmq"
        self._settings.service_client.provider = "http"
        self._base_publisher = self._recording
        self._switchable.publisher = self._recording
        self._injected = ""
        return migration.resolve_switch(self._settings)

    def switch_to_msg(self) -> migration.SwitchReport:
        """只改配置切到 msg。

        Returns:
            migration.SwitchReport: 两端口判定。
        """
        self._settings.event.provider = "msg"
        self._settings.service_client.provider = "msg"
        self._base_publisher = self._msg_publisher
        self._switchable.publisher = self._msg_publisher
        self._injected = ""
        return migration.resolve_switch(self._settings)

    def switch_to_original(self) -> migration.SwitchReport:
        """只改配置回原实现。

        Returns:
            migration.SwitchReport: 两端口判定。
        """
        self._settings.event.provider = "rocketmq"
        self._settings.service_client.provider = "http"
        self._base_publisher = self._recording
        self._switchable.publisher = self._recording
        self._injected = ""
        return migration.resolve_switch(self._settings)

    def config_version(self) -> str:
        """配置版本（排除 provider）。

        Returns:
            str: 摘要。
        """
        return rollback.stable_config_version(self._settings)

    def original_event_transport(self) -> str:
        """原链路事件侧载体。

        Returns:
            str: 口径。
        """
        return self._transport

    def latency_source(self) -> str:
        """延迟来源。

        Returns:
            str: `simulated`。
        """
        return rollback.LATENCY_SOURCE_SIMULATED

    def supports_outbox(self) -> bool:
        """是否提供发件箱。

        Returns:
            bool: 恒 True。
        """
        return True

    async def prepare(self) -> None:
        """建主题并加入消费组。"""
        response = await self._http.post(
            "/api/v1/msg/admin/topics",
            json={"namespace": EVENT_NAMESPACE, "name": self._topic, "partition_count": 1},
            headers={"X-Tenant-ID": "demo"},
        )
        assert response.status_code in (200, 409), response.text
        joined = await self._http.post(
            "/api/v1/msg/join",
            json={
                "group": migration.MIGRATION_DEFAULT_GROUP,
                "namespace": EVENT_NAMESPACE,
                "topic": self._topic,
                "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
            },
            headers={"X-Tenant-ID": "demo"},
        )
        assert joined.status_code == 200, joined.text

    async def publish(self, event: EventEnvelope) -> None:
        """经当前 provider 发布。"""
        await self._switchable.publish(event)

    async def consume(self) -> ConcurrentStableFrozenList[migration.DrillMessage]:
        """拉取并确认当前批（同时记效果账本）。"""
        response = await self._http.post(
            "/api/v1/msg/consume",
            json={
                "group": migration.MIGRATION_DEFAULT_GROUP,
                "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
                "namespace": EVENT_NAMESPACE,
                "topic": self._topic,
                "partition": 0,
                "max_messages": 100,
                "max_wait_ms": 0,
            },
            headers={"X-Tenant-ID": "demo"},
        )
        assert response.status_code == 200, response.text
        body = cast("dict[str, object]", response.json())
        data = cast("dict[str, object]", body["data"])
        raw_items = cast("list[dict[str, object]]", data["messages"])
        messages: ConcurrentStableList[migration.DrillMessage] = ConcurrentStableList()
        for item in raw_items:
            offset = int(cast("int", item["offset"]))
            headers = cast("dict[str, str]", item["headers"])
            event_id = headers.get("event_id") or str(item["message_id"])
            self._effects.set(event_id, self._effects.get(event_id, 0) + 1)
            messages.add(
                migration.DrillMessage(
                    message_id=str(item["message_id"]),
                    offset=offset,
                    payload=base64.b64decode(cast("str", item["payload"])),
                    headers=ConcurrentStableFrozenDict(headers),
                    key=cast("str | None", item["key"]),
                )
            )
            acked = await self._http.post(
                "/api/v1/msg/ack",
                json={
                    "group": migration.MIGRATION_DEFAULT_GROUP,
                    "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
                    "namespace": EVENT_NAMESPACE,
                    "topic": self._topic,
                    "partition": 0,
                    "offset": offset,
                },
                headers={"X-Tenant-ID": "demo"},
            )
            assert acked.status_code == 200, acked.text
        return ConcurrentStableFrozenList(messages)

    async def read_only_msg(self) -> rollback.ProbeResult:
        """msg 通道只读调用（延迟按注入值观测）。

        Returns:
            rollback.ProbeResult: 结果与延迟。

        Raises:
            rollback.RollbackError: 注入不可达时。
        """
        if self._injected == rollback.INJECTION_UNAVAILABLE:
            raise rollback.RollbackError("rollback.inject: msg 实例不可达（注入）")
        client = msg_client(AsgiRpcTransport(self._app))
        try:
            records = await fetch_catalog_snapshot(client)
        finally:
            await client.aclose()
        return rollback.ProbeResult(
            target=f"{TARGET_SERVICE}.{TARGET_METHOD}",
            digest=migration.digest_json([asdict(record) for record in records]),
            count=len(records),
            latency_ms=self._latency_ms,
        )

    async def read_only_original(self) -> rollback.ProbeResult:
        """原链路只读调用（真 `HttpServiceClient` + 同一份响应体）。"""
        client = HttpServiceClient(
            circuit_breaker=default_circuit(),
            fallback_policy=NullFallbackPolicy(),
            rate_limiter=NullRateLimiter(),
            base_url_template="http://{service}:8000",
            caller="platform",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=snapshot_body())),
        )
        try:
            records = await fetch_catalog_snapshot(client)
        finally:
            await client.aclose()
        return rollback.ProbeResult(
            target="http:snapshot",
            digest=migration.digest_json([asdict(record) for record in records]),
            count=len(records),
            latency_ms=0,
        )

    async def inject(self, mode: str, *, threshold: float) -> None:
        """注入失败。"""
        self._injected = mode
        if mode == rollback.INJECTION_UNAVAILABLE:
            self._switchable.publisher = _UnavailablePublisher()
        elif mode == rollback.INJECTION_ERROR_RATE:
            self._switchable.publisher = _FlakyPublisher(self._base_publisher, fail_every=5)
        elif mode == rollback.INJECTION_LATENCY:
            self._latency_ms = int(threshold) + 50

    async def clear_injection(self) -> None:
        """解除注入（恢复到当前模式的正常实现）。"""
        self._injected = ""
        self._latency_ms = 0
        self._switchable.publisher = self._base_publisher

    async def inflight_attempt(self, key: str) -> migration.WriteAttemptResult | None:
        """在途：首跳失败、次跳成功，服务端按幂等键只执行一次。"""
        global _write_seq
        handler = TrackingHandler()
        method = f"modules.create_{_write_seq}"
        _write_seq += 1
        _register_method(self._app, method, handler, idempotent=False)
        transport = _FailOnceTransport(AsgiRpcTransport(self._app))
        client = RpcClient(
            StaticInstanceSelector([BASE_URL]),
            tenant="demo",
            transport=transport,
            circuit=default_circuit(),
        )
        try:
            reply = await client.call(
                TARGET_SERVICE,
                method,
                b"{}",
                idempotency_key=key,
                policy=RetryPolicy(max_retries=1, base_ms=0, cap_ms=0),
            )
        finally:
            await client.aclose()
        return migration.WriteAttemptResult(
            executions=len(handler.calls),
            keys=ConcurrentStableFrozenList(transport.keys),
            digest=migration.digest_bytes(reply.payload),
        )

    async def enqueue_outbox(self, event: EventEnvelope) -> str:
        """写发件箱（真 `SqlOutboxStore`）。"""
        async with self._session_factory() as session, session.begin():
            return await self._store.enqueue(cast("Any", session), event)

    async def dispatch_outbox(self) -> rollback.OutboxRound:
        """投递一轮（当前 provider）。"""
        result = await self._dispatcher.dispatch_once(db_key=PLATFORM_DB_KEY)
        return rollback.OutboxRound(
            published=result.published, failed=result.failed, dead=result.dead, backlog=result.backlog
        )

    async def snapshot(self) -> rollback.StateSnapshot:
        """一致性快照（位点 / 死信 / 重放 / 审计 / outbox / 效果）。"""
        store = self._app.state.offset_store
        entries = await store.list_group(tenant_id=_TENANT_ID, group=migration.MIGRATION_DEFAULT_GROUP)
        offsets: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for entry in entries:
            offsets.set(f"{entry.group}|{entry.topic_id}|{entry.partition_no}", entry.offset)
        dead = await self._app.state.dead_letter_queue.list_all()
        async with self._session_factory() as session:
            pending = await self._store.backlog(cast("Any", session))
            delivered = int(
                (
                    await session.execute(
                        select(func.count()).select_from(SysOutbox).where(SysOutbox.status == OUTBOX_STATUS_DELIVERED)
                    )
                ).scalar_one()
            )
            dead_rows = int(
                (
                    await session.execute(select(func.count()).select_from(SysOutbox).where(SysOutbox.status == "dead"))
                ).scalar_one()
            )
        effects: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for key, value in self._effects.items():
            effects.set(key, value)
        return rollback.StateSnapshot(
            available=True,
            offsets=ConcurrentStableFrozenDict(offsets),
            dead_letters=len(dead),
            replay_tasks=ConcurrentStableFrozenDict(),
            audit_count=self._audit.count(),
            audit_digest=self._audit.digest(),
            audit_chain_ok=self._audit.verify(),
            outbox=rollback.OutboxState(pending=pending, delivered=delivered, dead=dead_rows),
            effects=ConcurrentStableFrozenDict(effects),
        )

    async def cleanup(self) -> None:
        """清理隔离环境（内存库随夹具释放）。"""

    async def aclose(self) -> None:
        """释放连接（幂等）。"""
        if self._closed:
            return
        self._closed = True
        await self._http.aclose()
        await self._msg_publisher.aclose()
        await self._dispatcher.aclose()


def _load() -> tuple[
    migration.SwitchMatrix,
    migration.PreconditionSet,
    rollback.ScenarioSet,
]:
    """读取仓内真实清单 / 矩阵 / 场景。

    Returns:
        tuple: 三份配置。
    """
    matrix, matrix_errors = migration.load_matrix(_MATRIX)
    preconditions, precondition_errors = migration.load_preconditions(_PRECONDITIONS)
    scenarios, scenario_errors = rollback.load_scenarios(_SCENARIOS)
    assert not matrix_errors
    assert not precondition_errors
    assert not scenario_errors
    return matrix, preconditions, scenarios


_run_seq = 0
"""演练序号（每次演练用独立主题，避免同一 app 上状态串场）。"""


async def _drill(
    *,
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    transport: str = "recording",
    clock_scale: float = 0.005,
    budget: int | None = None,
) -> rollback.RollbackReport:
    """跑一次完整回滚演练（编排用例共用）。

    Args:
        msg_app: 真 msg 应用。
        switch_settings: 已装配配置。
        outbox_engine: 发件箱内存引擎。
        transport: 原链路事件侧口径。
        clock_scale: 假时钟步长（超预算用例调大）。
        budget: 覆盖恢复预算（None 取场景表）。

    Returns:
        rollback.RollbackReport: 演练报告。
    """
    global _run_seq
    _run_seq += 1
    matrix, preconditions, scenarios = _load()
    if budget is not None:
        scenarios = replace(scenarios, recovery_budget_ms=budget)
    target = _InProcessTargets(
        app=msg_app,
        settings=switch_settings,
        engine=outbox_engine,
        event_type=f"{EVENT_TYPE}_{_run_seq}",
        transport=transport,
    )
    return await rollback.run_rollback_drill(
        matrix=matrix,
        preconditions=preconditions,
        scenarios=scenarios,
        targets=target,
        env="test",
        backup=rollback.BackupEvidence(mode="not_applicable", backup_id="", note="内存存储"),
        event_type=f"{EVENT_TYPE}_{_run_seq}",
        run_nonce=f"run{_run_seq}",
        now=lambda: _NOW,
        clock=_StepClock(),
    )


# ---------------------------------------------------------------------------
# R1~R3 场景单源
# ---------------------------------------------------------------------------


def test_scenario_file_parses_and_validates() -> None:
    """仓内场景表可解析、三类齐备、判据可追溯到矩阵。"""
    matrix, _preconditions, scenarios = _load()
    assert scenarios.schema_version == 1
    assert scenarios.recovery_budget_ms > 0
    assert len(scenarios.scenarios) == 3
    assert {item.mode for item in scenarios.scenarios} == set(rollback.INJECTION_MODES)
    assert rollback.validate_scenarios(scenarios, matrix) == ()


def test_scenario_document_table_matches_toml() -> None:
    """设计文档的场景表与 TOML 渲染结果逐行一致（单源不漂移）。"""
    _matrix, _preconditions, scenarios = _load()
    rows = rollback.render_scenario_rows(scenarios)
    text = _DESIGN.read_text(encoding="utf-8")
    begin = text.index("<!-- rollback-scenarios:begin -->")
    end = text.index("<!-- rollback-scenarios:end -->")
    block = text[begin:end]
    for row in rows:
        assert row in block


def test_scenario_parse_failures(tmp_path: Path) -> None:
    """场景缺失 / 损坏 / 空 / 条目不是表：解析错误逐项报出。"""
    missing, errors = rollback.load_scenarios(tmp_path / "missing.toml")
    assert missing.scenarios == ()
    assert "不存在" in errors[0]

    broken = tmp_path / "broken.toml"
    broken.write_text("schema_version = [", encoding="utf-8")
    _scenarios, errors = rollback.load_scenarios(broken)
    assert "解析失败" in errors[0]

    empty = tmp_path / "empty.toml"
    empty.write_text("schema_version = 1\n", encoding="utf-8")
    _scenarios, errors = rollback.load_scenarios(empty)
    assert any("scenarios" in line for line in errors)

    bad_item = tmp_path / "bad.toml"
    bad_item.write_text("schema_version = 1\nscenarios = [1, 2]\n", encoding="utf-8")
    _scenarios, errors = rollback.load_scenarios(bad_item)
    assert any("不是表" in line for line in errors)


def test_scenario_validation_rejects_fields() -> None:
    """模式 / 目标 / 判据 / 阈值 / 采样 / 期望 / 预算逐项校验。"""
    matrix, _preconditions, scenarios = _load()
    base = scenarios.scenarios[0]
    cases: ConcurrentStableList[rollback.ScenarioSet] = ConcurrentStableList(
        (
            replace(scenarios, schema_version=2),
            replace(scenarios, recovery_budget_ms=0),
            replace(scenarios, scenarios=ConcurrentStableFrozenList()),
            replace(scenarios, scenarios=ConcurrentStableFrozenList((replace(base, name=""),))),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, mode="timeout"))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", target="event"))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", criterion="自造判据"))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", criterion=""))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", threshold=-1.0))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", samples=0))),
            ),
            replace(
                scenarios,
                scenarios=ConcurrentStableFrozenList((base, replace(base, name="x", expected=""))),
            ),
        )
    )
    messages: ConcurrentStableList[str] = ConcurrentStableList()
    for item in cases:
        messages.extend(rollback.validate_scenarios(item, matrix))
    joined = "\n".join(messages)
    for keyword in (
        "schema_version",
        "恢复预算",
        "场景为空",
        "缺少 name",
        "mode",
        "target",
        "criterion",
        "threshold",
        "samples",
        "expected",
    ):
        assert keyword in joined
    only_unavailable = replace(scenarios, scenarios=ConcurrentStableFrozenList((base,)))
    assert any("缺少注入模式" in line for line in rollback.validate_scenarios(only_unavailable, matrix))


def test_error_rate_and_criterion_math() -> None:
    """错误率零尝试不除零；三类判据命中 / 未命中可区分。"""
    assert rollback.error_rate_percent(0, 0) == 0.0
    assert rollback.error_rate_percent(4, 20) == 20.0
    unavailable = rollback.InjectionScenario("u", "unavailable", "both", "c", 0.0, 1, "e", "")
    error_rate = rollback.InjectionScenario("r", "error_rate", "event", "c", 1.0, 20, "e", "")
    latency = rollback.InjectionScenario("l", "latency", "readonly", "c", 200.0, 1, "e", "")
    assert rollback.criterion_matched(unavailable, failures=1, attempts=1, latency_ms=0)
    assert not rollback.criterion_matched(unavailable, failures=0, attempts=1, latency_ms=0)
    assert rollback.criterion_matched(error_rate, failures=1, attempts=20, latency_ms=0)
    assert not rollback.criterion_matched(error_rate, failures=0, attempts=20, latency_ms=0)
    assert rollback.criterion_matched(latency, failures=0, attempts=1, latency_ms=250)
    assert not rollback.criterion_matched(latency, failures=0, attempts=1, latency_ms=10)


def test_stable_config_version_ignores_provider() -> None:
    """配置版本排除 provider：只改开关不算改配置。"""
    settings = _settings(event="rocketmq", client="http")
    before = rollback.stable_config_version(settings)
    settings.event.provider = "msg"
    settings.service_client.provider = "msg"
    assert rollback.stable_config_version(settings) == before


# ---------------------------------------------------------------------------
# R4~R15 闭环编排（真应用 + 真 outbox）
# ---------------------------------------------------------------------------


def _fixed_event(index: int, *, tenant: str | None = "demo") -> EventEnvelope:
    """构造一条固定事件（网络目标用例用）。

    Args:
        index: 序号。
        tenant: 租户编码。

    Returns:
        EventEnvelope: 事件信封。
    """
    return EventEnvelope(
        event_type=EVENT_TYPE,
        payload=ConcurrentStableDict({"index": index}),
        tenant_id=tenant,
        event_id=f"http-{index:04d}",
        occurred_at=_NOW,
        aggregate_key=f"k{index % 4}",
    )


def _report_stub(conclusion: str) -> rollback.RollbackReport:
    """构造最小报告（CLI `--repeat` 分支用例）。

    Args:
        conclusion: 结论。

    Returns:
        rollback.RollbackReport: 最小报告。
    """
    return rollback.RollbackReport(
        mode="drill",
        env="test",
        started_at=_NOW,
        finished_at=_NOW,
        config_version="cfg",
        switch_domain=ConcurrentStableFrozenList(),
        baseline=ConcurrentStableFrozenDict(),
        switch=ConcurrentStableFrozenDict(),
        injection=ConcurrentStableFrozenDict({"scenarios": ConcurrentStableFrozenList()}),
        observe_metrics=ConcurrentStableFrozenDict(),
        rollback=ConcurrentStableFrozenDict({"cycles": ConcurrentStableFrozenList(), "recovery_budget_ms": 300_000}),
        gate=ConcurrentStableFrozenDict(
            {"allowed": True, "blocked_by": ConcurrentStableFrozenList(), "checked_env": "test"}
        ),
        steps=ConcurrentStableFrozenList(),
        counts=ConcurrentStableFrozenDict(),
        integrity=rollback.IntegrityReport(
            effect_once=True,
            no_loss=True,
            state=True,
            audit_chain=True,
            detail=ConcurrentStableFrozenList(),
        ),
        backup=rollback.BackupEvidence("not_applicable", "", ""),
        conclusion=conclusion,
        data_integrity=rollback.DATA_INTEGRITY_GAP,
        reasons=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
        artifacts=ConcurrentStableFrozenDict(),
    )


async def test_full_rollback_drill_recovers_original_link(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """完整闭环：切 msg → 三类注入 → 回滚 → 原链路恢复；结论降级（broker 未部署）。"""
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine)
    steps = {step.step: step.result for step in report.steps}
    assert report.conclusion == rollback.CONCLUSION_DEGRADED
    assert report.data_integrity == rollback.DATA_INTEGRITY_GAP
    assert report.env == "test"
    assert report.switch_domain[0]["key"] == "low_risk_event"
    assert report.switch_domain[1]["key"] == "readonly_rpc"
    for name in ("preconditions", "matrix", "scenarios", "baseline", "baseline_event", "baseline_readonly"):
        assert steps[name] == rollback.RESULT_OK
    for name in ("switch_to_msg", "prepare", "event_chain_msg", "readonly_msg", "dual_write_msg"):
        assert steps[name] == rollback.RESULT_OK
    assert steps["inflight_idempotency"] == rollback.RESULT_OK
    for scenario in ("msg_unavailable", "publish_error_rate", "rpc_latency"):
        assert steps[f"inject.{scenario}"] == rollback.RESULT_OK
        assert steps[f"rollback.{scenario}"] == rollback.RESULT_OK
        assert steps[f"recover.{scenario}"] == rollback.RESULT_DEGRADED
    assert steps["unconfirmed.msg_unavailable"] == rollback.RESULT_OK
    assert steps["consistency"] == rollback.RESULT_OK
    assert steps["business_zero_change"] == rollback.RESULT_OK
    assert report.counts["cycles"] == 3
    assert report.counts["executions"] == 1
    assert report.counts["duplicates"] == 0
    assert report.counts["published_msg"] == report.counts["consumed_msg"]
    assert report.integrity.ok
    assert report.integrity.state
    assert report.integrity.audit_chain
    assert "原链路事件侧为记录型替身" in "\n".join(report.reasons)
    assert report.gate["blocked_by"] == ("copy_replication",)
    assert report.backup.mode == "not_applicable"
    assert report.observe_metrics["msg_rpc_duration_ms"] > 0
    injection_rows = cast(
        "ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]", report.injection["scenarios"]
    )
    assert injection_rows[0]["matched"] == "true"
    cycles = cast("ConcurrentStableFrozenList[rollback.CycleRecord]", report.rollback["cycles"])
    assert cycles[0].recovery_ms >= 0
    assert cast("int", report.rollback["recovery_budget_ms"]) == 300_000


async def test_full_rollback_drill_passes_with_real_transport(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """原链路事件侧为真实载体时四类全过、结论通过（退出码 0 分支）。"""
    report = await _drill(
        msg_app=msg_app,
        switch_settings=switch_settings,
        outbox_engine=outbox_engine,
        transport="rocketmq",
    )
    assert report.conclusion == rollback.CONCLUSION_PASS
    assert report.data_integrity == rollback.DATA_INTEGRITY_COMPLETE
    assert all(step.result == rollback.RESULT_OK for step in report.steps)
    assert rollback.EXIT_CODES[report.conclusion] == 0


async def test_drill_rejects_production(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """生产环境拒绝执行（生产回滚归 bms 8B_08）。"""
    matrix, preconditions, scenarios = _load()
    target = _InProcessTargets(
        app=msg_app,
        settings=switch_settings,
        engine=outbox_engine,
        event_type=f"{EVENT_TYPE}_prod",
    )
    with pytest.raises(ValueError, match="生产环境禁止"):
        await rollback.run_rollback_drill(
            matrix=matrix,
            preconditions=preconditions,
            scenarios=scenarios,
            targets=target,
            env="prod",
            backup=rollback.BackupEvidence("not_applicable", "", ""),
        )


async def test_drill_rerun_same_signature(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    db_session: AsyncSession,
) -> None:
    """固定时钟下同一场景重复执行，签名一致（可复跑）。"""
    first = await _drill(msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine)
    await db_session.rollback()
    second = await _drill(msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine)
    assert rollback.report_signature(first) == rollback.report_signature(second)
    assert first.steps[0].elapsed_ms == second.steps[0].elapsed_ms


class _BrokenTargets(_InProcessTargets):
    """按开关破坏一条判据的演练目标（失败分支用例）。"""

    def __init__(
        self,
        *,
        app: FastAPI,
        settings: Settings,
        engine: AsyncEngine,
        event_type: str,
        broken: str,
    ) -> None:
        """初始化。

        Args:
            app: 真 msg 应用。
            settings: 已装配配置。
            engine: 发件箱内存引擎。
            event_type: 事件类型。
            broken: 破坏点（见各方法同名分支）。
        """
        super().__init__(app=app, settings=settings, engine=engine, event_type=event_type)
        self._broken = broken
        self._dispatch_count = 0
        self._snapshot_count = 0
        self._publish_count = 0
        self._consume_count = 0
        self._enqueue_count = 0
        self._readonly_msg_count = 0
        self._original_readonly_count = 0
        self._switch_msg_count = 0
        self._in_switch_back = False
        self._final_dual = False
        self._version_bump = ""

    def _original_mode(self) -> bool:
        """当前是否原实现模式。"""
        return self._settings.event.provider == "rocketmq"

    def original_config(self) -> migration.SwitchReport:
        """破坏：基线不改配置。"""
        if self._broken == "baseline":
            return migration.resolve_switch(self._settings)
        return super().original_config()

    def switch_to_msg(self) -> migration.SwitchReport:
        """破坏：重切失败 / 配置版本漂移。"""
        self._switch_msg_count += 1
        if self._broken == "switch_msg":
            return migration.resolve_switch(self._settings)
        if self._broken == "switch_back_errors" and self._switch_msg_count > 1:
            return migration.resolve_switch(self._settings)
        if self._broken == "switch_version":
            self._version_bump = "v2"
        self._in_switch_back = self._switch_msg_count > 1
        return super().switch_to_msg()

    def switch_to_original(self) -> migration.SwitchReport:
        """破坏：回滚不改配置 / 配置版本漂移。"""
        if self._broken == "rollback":
            return migration.resolve_switch(self._settings)
        if self._broken == "rollback_version":
            self._version_bump = "v3"
        return super().switch_to_original()

    def config_version(self) -> str:
        """破坏：版本含漂移后缀。"""
        return super().config_version() + self._version_bump

    async def prepare(self) -> None:
        """破坏：建主题失败。"""
        if self._broken == "prepare":
            raise RuntimeError("建主题失败（注入）")
        await super().prepare()

    async def publish(self, event: EventEnvelope) -> None:
        """破坏：吞错 / 错异常类型 / 双写失败 / 重切失败。"""
        self._publish_count += 1
        if self._broken == "swallow" and self._injected == rollback.INJECTION_UNAVAILABLE:
            return
        if self._broken == "unavailable_wrong_exc" and self._injected == rollback.INJECTION_UNAVAILABLE:
            raise ValueError("不可达抛错类型不对（注入）")
        if self._broken == "error_rate_wrong_exc" and self._injected == rollback.INJECTION_ERROR_RATE:
            raise ValueError("错误率抛错类型不对（注入）")
        if self._broken == "dual_write_raise" and self._publish_count in (2, 3):
            raise RuntimeError("双写发布失败（注入）")
        if self._broken == "switch_back_raise" and self._in_switch_back:
            self._in_switch_back = False
            raise RuntimeError("重切发布失败（注入）")
        await super().publish(event)

    async def consume(self) -> ConcurrentStableFrozenList[migration.DrillMessage]:
        """破坏：事件链缺条 / 抛错 / 双写少条 / 重切多消费。"""
        messages = await super().consume()
        self._consume_count += 1
        if self._broken == "chain_short" and self._consume_count == 1:
            return ConcurrentStableFrozenList()
        if self._broken == "chain_raise" and self._consume_count == 1:
            raise RuntimeError("消费失败（注入）")
        if self._broken == "dual_write_short" and self._consume_count == 2:
            return ConcurrentStableFrozenList((messages[0],)) if messages else messages
        if self._broken == "switch_back_count" and self._in_switch_back:
            self._in_switch_back = False
            return ConcurrentStableFrozenList()
        return messages

    async def read_only_msg(self) -> rollback.ProbeResult:
        """破坏：只读调用抛错 / 空结果 / 不可达未失败。"""
        self._readonly_msg_count += 1
        if self._broken == "readonly_msg_raise" and self._readonly_msg_count == 1:
            raise RuntimeError("msg 只读失败（注入）")
        if self._broken == "readonly_msg_empty" and self._readonly_msg_count == 1:
            return rollback.ProbeResult(target="msg:empty", digest="", count=0, latency_ms=0)
        if self._broken == "unavailable_readonly_ok" and self._injected == rollback.INJECTION_UNAVAILABLE:
            self._injected = ""
            return rollback.ProbeResult(target="msg:ok", digest="d", count=1, latency_ms=0)
        return await super().read_only_msg()

    async def read_only_original(self) -> rollback.ProbeResult:
        """破坏：基线 / 恢复只读抛错、空结果、摘要不同构。"""
        self._original_readonly_count += 1
        count = self._original_readonly_count
        if self._broken == "baseline_readonly_raise" and count == 1:
            raise RuntimeError("原链路只读失败（注入）")
        if self._broken == "baseline_readonly_empty" and count == 1:
            return rollback.ProbeResult(target="http:empty", digest="", count=0, latency_ms=0)
        if self._broken == "recover_readonly_raise" and count >= 2:
            raise RuntimeError("恢复只读失败（注入）")
        if self._broken == "recover_readonly_empty" and count >= 2:
            return rollback.ProbeResult(target="http:empty", digest="", count=0, latency_ms=0)
        if self._broken == "recover_digest" and count >= 2:
            return replace(await super().read_only_original(), digest="different")
        return await super().read_only_original()

    def supports_outbox(self) -> bool:
        """破坏：不提供发件箱。"""
        if self._broken == "no_outbox":
            return False
        return super().supports_outbox()

    async def inject(self, mode: str, *, threshold: float) -> None:
        """破坏：错误率 / 延迟注入不生效 / 注入抛错。"""
        if self._broken == "inject_raise":
            raise RuntimeError("注入失败（注入）")
        if self._broken == "error_rate" and mode == rollback.INJECTION_ERROR_RATE:
            self._injected = mode
            return
        if self._broken == "latency" and mode == rollback.INJECTION_LATENCY:
            self._injected = mode
            return
        await super().inject(mode, threshold=threshold)

    async def inflight_attempt(self, key: str) -> migration.WriteAttemptResult | None:
        """破坏：在途抛错 / 无结果 / 执行两次 / 幂等键不一致。"""
        if self._broken == "inflight_raise":
            raise RuntimeError("在途调用失败（注入）")
        if self._broken == "inflight_none":
            return None
        result = await super().inflight_attempt(key)
        if result is None:
            return None
        if self._broken == "inflight_two":
            return replace(result, executions=2)
        if self._broken == "inflight_keys":
            return replace(result, keys=ConcurrentStableFrozenList(("a", "b")))
        return result

    async def enqueue_outbox(self, event: EventEnvelope) -> str:
        """破坏：发件箱写入失败。"""
        self._enqueue_count += 1
        if self._broken == "baseline_event_raise" and self._enqueue_count == 1:
            raise RuntimeError("发件箱写入失败（注入）")
        if self._broken == "dual_original_raise" and self._enqueue_count >= 3:
            raise RuntimeError("原链路双写写入失败（注入）")
        if self._broken == "dual_original_mismatch" and self._enqueue_count >= 3:
            self._final_dual = True
        return await super().enqueue_outbox(event)

    async def dispatch_outbox(self) -> rollback.OutboxRound:
        """破坏：投递失败 / 未确认被投递 / 恢复零投 / 恢复重投 / 双写少投。"""
        self._dispatch_count += 1
        count = self._dispatch_count
        if self._broken == "baseline_event_zero" and count == 1:
            return rollback.OutboxRound(published=0, failed=0, dead=0, backlog=0)
        if self._broken == "unconfirmed_raise" and count == 2:
            raise RuntimeError("未确认投递失败（注入）")
        if self._broken == "unconfirmed_published" and count == 2:
            return rollback.OutboxRound(published=1, failed=0, dead=0, backlog=0)
        if self._broken == "recover_zero" and count >= 3:
            return rollback.OutboxRound(published=0, failed=0, dead=0, backlog=1)
        if self._broken == "recover_second" and count >= 4:
            return rollback.OutboxRound(published=1, failed=0, dead=0, backlog=0)
        if self._broken == "dual_original_mismatch" and self._final_dual:
            return rollback.OutboxRound(published=1, failed=0, dead=0, backlog=1)
        result = await super().dispatch_outbox()
        if self._broken == "duplicate" and result.published == 0 and self._original_mode():
            return rollback.OutboxRound(published=1, failed=0, dead=0, backlog=result.backlog)
        return result

    async def snapshot(self) -> rollback.StateSnapshot:
        """破坏：快照不可用 / 抛错 / 位点回退 / 死信审计变化。"""
        self._snapshot_count += 1
        count = self._snapshot_count
        if self._broken == "snapshot_msg_raise" and count == 1:
            raise RuntimeError("快照失败（注入）")
        if self._broken == "snapshot_scenario_raise" and count == 2:
            raise RuntimeError("场景快照失败（注入）")
        if self._broken == "snapshot_final_raise" and count >= 5:
            raise RuntimeError("结束快照失败（注入）")
        result = await super().snapshot()
        if self._broken == "snapshot_msg_unavailable" and count == 1:
            return replace(result, available=False)
        if self._broken == "consistency_degraded":
            return replace(result, available=False)
        if self._broken == "state" and count > 1:
            offsets: ConcurrentStableDict[str, int] = ConcurrentStableDict()
            for key, value in result.offsets.items():
                offsets.set(key, max(value - 1, 0))
            return replace(
                result,
                offsets=ConcurrentStableFrozenDict(offsets),
                dead_letters=result.dead_letters + 1,
                audit_chain_ok=False,
            )
        return result

    async def cleanup(self) -> None:
        """破坏：清理失败。"""
        if self._broken == "cleanup_raise":
            raise RuntimeError("清理失败（注入）")
        await super().cleanup()


async def _broken_drill(
    *,
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    broken: str,
) -> rollback.RollbackReport:
    """用破坏目标跑一次演练。

    Args:
        msg_app: 真 msg 应用。
        switch_settings: 已装配配置。
        outbox_engine: 发件箱内存引擎。
        broken: 破坏点。

    Returns:
        rollback.RollbackReport: 演练报告。
    """
    global _run_seq
    _run_seq += 1
    event_type = f"{EVENT_TYPE}_broken_{_run_seq}"
    matrix, preconditions, scenarios = _load()
    target = _BrokenTargets(
        app=msg_app,
        settings=switch_settings,
        engine=outbox_engine,
        event_type=event_type,
        broken=broken,
    )
    return await rollback.run_rollback_drill(
        matrix=matrix,
        preconditions=preconditions,
        scenarios=scenarios,
        targets=target,
        env="test",
        backup=rollback.BackupEvidence("not_applicable", "", "内存存储"),
        event_type=event_type,
        run_nonce=f"broken{_run_seq}",
        now=lambda: _NOW,
        clock=_StepClock(),
    )


async def test_drill_fails_when_rollback_not_effective(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """回滚没改配置：`rollback.*` 失败，结论失败。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="rollback"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    assert report.data_integrity == rollback.DATA_INTEGRITY_UNAVAILABLE
    steps = {step.step: step.result for step in report.steps}
    assert steps["rollback.msg_unavailable"] == rollback.RESULT_FAILED


async def test_drill_fails_when_publish_failure_swallowed(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """发布失败被吞：不可达注入判为未命中。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="swallow"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    steps = {step.step: step.result for step in report.steps}
    assert steps["inject.msg_unavailable"] == rollback.RESULT_FAILED


async def test_drill_fails_when_error_rate_not_triggered(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """错误率未超阈值：判据未命中即失败。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="error_rate"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    steps = {step.step: step.result for step in report.steps}
    assert steps["inject.publish_error_rate"] == rollback.RESULT_FAILED


async def test_drill_fails_when_latency_not_triggered(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """延迟未超阈值：判据未命中即失败。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="latency"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    steps = {step.step: step.result for step in report.steps}
    assert steps["inject.rpc_latency"] == rollback.RESULT_FAILED


async def test_drill_fails_when_redelivered(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """回滚后重复投递：`recover.*` 失败（不默认通过）。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="duplicate"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    steps = {step.step: step.result for step in report.steps}
    assert steps["recover.msg_unavailable"] == rollback.RESULT_FAILED


async def test_drill_fails_when_state_regressed(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """位点回退 / 死信变化 / 审计链断：一致性判失败。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="state"
    )
    assert report.conclusion == rollback.CONCLUSION_FAIL
    steps = {step.step: step.result for step in report.steps}
    assert steps["consistency"] == rollback.RESULT_FAILED
    assert not report.integrity.state
    assert not report.integrity.audit_chain


# ---------------------------------------------------------------------------
# R16 一致性判据（纯函数，逐分支）
# ---------------------------------------------------------------------------


def test_integrity_report_branches() -> None:
    """四类一致性逐分支：空快照 / 效果重复 / 对账不平 / 位点回退 / 死信重放 / 审计断 / 不可用。"""
    empty = rollback.integrity_report(ConcurrentStableFrozenList(), counts=ConcurrentStableFrozenDict())
    assert not empty.ok
    assert "没有任何一致性快照" in empty.detail[0]

    base = rollback.StateSnapshot(
        available=True,
        offsets=ConcurrentStableFrozenDict({"g|1|0": 5}),
        dead_letters=1,
        replay_tasks=ConcurrentStableFrozenDict({"task-1": "running"}),
        audit_count=2,
        audit_digest="d1",
        audit_chain_ok=True,
        outbox=rollback.OutboxState(pending=0, delivered=2, dead=0),
        effects=ConcurrentStableFrozenDict({"e1": 1}),
    )
    good = rollback.integrity_report(
        ConcurrentStableFrozenList((base, base)),
        counts=ConcurrentStableFrozenDict(
            {"outbox_enqueued": 2, "delivered_original": 2, "outbox_pending": 0, "published_msg": 1, "consumed_msg": 1}
        ),
    )
    assert good.ok
    assert not good.detail

    duplicate = replace(base, effects=ConcurrentStableFrozenDict({"e1": 2}))
    report = rollback.integrity_report(
        ConcurrentStableFrozenList((base, duplicate)),
        counts=ConcurrentStableFrozenDict({"outbox_enqueued": 1, "delivered_original": 1}),
    )
    assert not report.effect_once
    assert any("效果重复" in line for line in report.detail)

    report = rollback.integrity_report(
        ConcurrentStableFrozenList((base, base)),
        counts=ConcurrentStableFrozenDict(
            {"outbox_enqueued": 3, "delivered_original": 1, "outbox_pending": 1, "published_msg": 2, "consumed_msg": 1}
        ),
    )
    assert not report.no_loss
    assert any("发件箱对账不平" in line for line in report.detail)
    assert any("消息对账不平" in line for line in report.detail)

    regressed = replace(base, offsets=ConcurrentStableFrozenDict({"g|1|0": 4}))
    report = rollback.integrity_report(
        ConcurrentStableFrozenList((base, regressed)),
        counts=ConcurrentStableFrozenDict({"outbox_enqueued": 1, "delivered_original": 1}),
    )
    assert not report.state
    assert any("位点回退" in line for line in report.detail)

    lost_key = replace(base, offsets=ConcurrentStableFrozenDict())
    report = rollback.integrity_report(
        ConcurrentStableFrozenList((base, lost_key)),
        counts=ConcurrentStableFrozenDict({"outbox_enqueued": 1, "delivered_original": 1}),
    )
    assert any("位点键丢失" in line for line in report.detail)

    changed = replace(
        base,
        dead_letters=2,
        replay_tasks=ConcurrentStableFrozenDict({"task-1": "done"}),
        audit_count=1,
        audit_chain_ok=False,
    )
    report = rollback.integrity_report(
        ConcurrentStableFrozenList((base, changed)),
        counts=ConcurrentStableFrozenDict({"outbox_enqueued": 1, "delivered_original": 1}),
    )
    assert not report.state
    assert not report.audit_chain
    assert any("死信条数变化" in line for line in report.detail)
    assert any("重放任务" in line for line in report.detail)

    unavailable = replace(base, available=False)
    report = rollback.integrity_report(
        ConcurrentStableFrozenList((unavailable, unavailable)),
        counts=ConcurrentStableFrozenDict({"outbox_enqueued": 1, "delivered_original": 1}),
    )
    assert not report.state
    assert not report.audit_chain
    assert any("不可用" in line for line in report.detail)


# ---------------------------------------------------------------------------
# R17 记录契约
# ---------------------------------------------------------------------------


async def test_record_contract_and_markdown(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    tmp_path: Path,
) -> None:
    """记录字段齐备、Markdown 与 JSON 同源、产物路径回填。"""
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine)
    json_path, markdown_path = rollback.write_record(report, tmp_path / "rollback-record")
    record = cast("dict[str, object]", json.loads(json_path.read_text(encoding="utf-8")))
    assert record["task"] == "07-4-2"
    assert record["mode"] == "drill"
    for key in (
        "environment",
        "service",
        "config_version",
        "switch_domain",
        "baseline",
        "switch",
        "injection",
        "observe_metrics",
        "rollback",
        "production_gate",
        "steps",
        "counts",
        "integrity",
        "backup",
        "conclusion",
        "issues",
        "leftovers",
        "artifacts",
    ):
        assert key in record
    conclusion = cast("dict[str, object]", record["conclusion"])
    assert conclusion["result"] == rollback.CONCLUSION_DEGRADED
    assert conclusion["data_integrity"] == rollback.DATA_INTEGRITY_GAP
    assert conclusion["requires_review"] is True
    cycles = cast("list[dict[str, object]]", cast("dict[str, object]", record["rollback"])["cycles"])
    assert len(cycles) == 3
    assert "recovery_ms" in cycles[0]
    text = markdown_path.read_text(encoding="utf-8")
    for heading in ("# 回滚演练记录（07-4-2）", "## 2. 注入与判据", "## 3. 回滚与恢复", "## 5. 计数与一致性"):
        assert heading in text
    assert "降级通过" in text
    assert record["artifacts"] == {"report_json": str(json_path), "report_md": str(markdown_path)}


async def test_record_written_on_failure(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    tmp_path: Path,
) -> None:
    """失败也写记录（失败步与原因进产物）。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="rollback"
    )
    json_path, markdown_path = rollback.write_record(report, tmp_path / "failed")
    record = cast("dict[str, object]", json.loads(json_path.read_text(encoding="utf-8")))
    conclusion = cast("dict[str, object]", record["conclusion"])
    assert conclusion["result"] == rollback.CONCLUSION_FAIL
    assert "失败" in markdown_path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# R19 CLI 与静态闸门
# ---------------------------------------------------------------------------


def test_check_gate_allows_test_but_blocks_prod() -> None:
    """`check`：test 放行、prod 仍被 8B_05 阻断、未知环境退 2。"""
    code, lines = rollback.run_check(
        preconditions_path=_PRECONDITIONS,
        matrix_path=_MATRIX,
        scenarios_path=_SCENARIOS,
        env="test",
    )
    assert code == 0
    assert any("copy_replication" in line for line in lines)
    prod, prod_lines = rollback.run_check(
        preconditions_path=_PRECONDITIONS,
        matrix_path=_MATRIX,
        scenarios_path=_SCENARIOS,
        env="prod",
    )
    assert prod == 1
    joined = "\n".join(prod_lines)
    assert "copy_replication" in joined
    assert "rollback_drill" not in joined
    assert rollback.main(["check", "--env", "staging"]) == 2


def test_check_rejects_broken_scenarios(tmp_path: Path) -> None:
    """场景缺失 / 损坏时 `check` 失败（退出 1）。"""
    code = rollback.main(["check", "--env", "test", "--scenarios", str(tmp_path / "missing.toml")])
    assert code == 1
    broken = tmp_path / "broken.toml"
    broken.write_text("schema_version = 2\n", encoding="utf-8")
    code = rollback.main(["check", "--env", "test", "--scenarios", str(broken)])
    assert code == 1


def test_main_matrix_and_broken(tmp_path: Path) -> None:
    """`matrix` 渲染场景表；坏文件退 1。"""
    assert rollback.main(["matrix"]) == 0
    broken = tmp_path / "broken.toml"
    broken.write_text("schema_version = [", encoding="utf-8")
    assert rollback.main(["matrix", "--scenarios", str(broken)]) == 1


async def test_cli_drill_runs_with_injected_targets(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI `drill` 走同一份编排（目标工厂可注入，离线可跑）。"""
    global _run_seq
    _run_seq += 1
    event_type = f"{EVENT_TYPE}_cli_{_run_seq}"

    def _factory(settings: Settings, args: argparse.Namespace) -> rollback.RollbackTargets:
        del settings, args
        return _InProcessTargets(
            app=msg_app,
            settings=switch_settings,
            engine=outbox_engine,
            event_type=event_type,
        )

    monkeypatch.setattr(rollback, "_TARGETS_FACTORY", _factory)
    report = tmp_path / "cli"
    code = await asyncio.to_thread(
        rollback.main,
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--report",
            str(report),
        ],
    )
    assert code == 2
    assert report.with_suffix(".json").exists()
    assert report.with_suffix(".md").exists()


def test_cli_drill_rejects_production_and_unknown_env() -> None:
    """`drill --env prod/staging` 参数级拒绝（退出 2）。"""
    assert rollback.main(["drill", "--env", "prod", "--msg-url", BASE_URL]) == 2
    assert rollback.main(["drill", "--env", "staging", "--msg-url", BASE_URL]) == 2


def test_cli_drill_rejects_broken_files(tmp_path: Path) -> None:
    """场景 / 矩阵缺失时 `drill` 失败（退出 1）。"""
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--scenarios",
            str(tmp_path / "missing.toml"),
        ]
    )
    assert code == 1


def test_cli_drill_backup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """演练前备份失败即中止（退出 1），不带着无备份状态演练。"""
    from ops import backup as backup_module

    def _boom(env: object, *, repo_root: Path, dry_run: bool, now: datetime) -> object:
        del env, repo_root, dry_run, now
        raise RuntimeError("备份根不可写")

    monkeypatch.setattr(backup_module, "config_from_env", _boom)
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--backup",
            "--report",
            str(tmp_path / "backup"),
        ]
    )
    assert code == 1


def test_cli_repeat_detects_divergence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`--repeat`：签名一致放行、不一致退 1。"""
    base = _report_stub(rollback.CONCLUSION_DEGRADED)
    calls: ConcurrentStableList[int] = ConcurrentStableList()

    async def _same(**kwargs: object) -> rollback.RollbackReport:
        del kwargs
        calls.add(1)
        return base

    def _factory(settings: Settings, args: argparse.Namespace) -> rollback.RollbackTargets:
        del settings, args
        return cast("rollback.RollbackTargets", object())

    monkeypatch.setattr(rollback, "run_rollback_drill", _same)
    monkeypatch.setattr(rollback, "_TARGETS_FACTORY", _factory)
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--repeat",
            "--report",
            str(tmp_path / "repeat"),
        ]
    )
    assert code == 2
    assert len(calls) == 2

    diverged: ConcurrentStableList[int] = ConcurrentStableList()

    async def _different(**kwargs: object) -> rollback.RollbackReport:
        del kwargs
        diverged.add(1)
        if len(diverged) == 1:
            return base
        return _report_stub(rollback.CONCLUSION_FAIL)

    monkeypatch.setattr(rollback, "run_rollback_drill", _different)
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--repeat",
            "--report",
            str(tmp_path / "repeat-diverged"),
        ]
    )
    assert code == 1


# ---------------------------------------------------------------------------
# R20 网络目标（同一份判据，只换传输）
# ---------------------------------------------------------------------------


async def test_http_targets_round_trip(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """网络目标：切 msg / 发布 / 消费 / 只读 / 三类注入 / 发件箱 / 快照 / 清理。"""
    fallback_app = FastAPI()

    async def _snapshot() -> Response:
        """原链路只读响应。"""
        return Response(content=snapshot_body(), media_type="application/json")

    fallback_app.add_api_route(SNAPSHOT_PATH, _snapshot, methods=["GET"])
    _register_method(msg_app, TARGET_METHOD, TrackingHandler(), idempotent=True)
    targets = rollback.HttpRollbackTargets(
        settings=switch_settings,
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=f"{EVENT_TYPE}_http",
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
        http_transport=ASGITransport(app=msg_app),
        original_transport=ASGITransport(app=fallback_app),
        outbox_engine=outbox_engine,
        rpc_transport=AsgiRpcTransport(app=msg_app),
    )
    try:
        assert targets.supports_outbox()
        assert targets.config_version()
        assert targets.original_event_transport() in ("rocketmq", "recording")
        assert targets.latency_source() == rollback.LATENCY_SOURCE_MEASURED
        baseline = targets.original_config()
        assert baseline.event.mode == "original"
        switched = targets.switch_to_msg()
        assert switched.event.mode == "msg"
        await targets.prepare()
        await targets.publish(_fixed_event(900))
        readonly = await targets.read_only_msg()
        original = await targets.read_only_original()
        assert readonly.count == 1
        assert original.count == 1
        await targets.inject(rollback.INJECTION_UNAVAILABLE, threshold=0.0)
        with pytest.raises(EventPublishError):
            await targets.publish(_fixed_event(901))
        with pytest.raises(rollback.RollbackError):
            await targets.read_only_msg()
        await targets.clear_injection()
        await targets.inject(rollback.INJECTION_ERROR_RATE, threshold=1.0)
        failures = 0
        for index in range(100):
            try:
                await targets.publish(_fixed_event(1000 + index))
            except EventPublishError:
                failures += 1
        assert failures >= 1
        await targets.inject(rollback.INJECTION_LATENCY, threshold=1.0)
        await targets.inject("unknown", threshold=0.0)
        await targets.clear_injection()
        event_id = await targets.enqueue_outbox(_fixed_event(2000, tenant=None))
        assert event_id
        snapshot = await targets.snapshot()
        assert snapshot.available is False
        assert snapshot.outbox.pending >= 1
        await targets.cleanup()
        await targets.cleanup()
    finally:
        await targets.aclose()
        await targets.aclose()


async def test_http_targets_without_outbox(switch_settings: Settings) -> None:
    """未配置平台库：不支持发件箱、写入即报错、快照为空计数。"""
    targets = rollback.HttpRollbackTargets(
        settings=switch_settings,
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=f"{EVENT_TYPE}_nooutbox",
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
    )
    try:
        assert not targets.supports_outbox()
        with pytest.raises(rollback.RollbackError):
            await targets.enqueue_outbox(_fixed_event(3000, tenant=None))
        snapshot = await targets.snapshot()
        assert snapshot.outbox.pending == 0
        await targets.cleanup()
    finally:
        await targets.aclose()


async def test_http_targets_with_rocketmq_transport(switch_settings: Settings) -> None:
    """配置了 RocketMQ 端点：原链路载体为 `rocketmq`，回滚解析到该实现。"""
    settings = switch_settings
    settings.rocketmq.endpoints = "127.0.0.1:8081"
    targets = rollback.HttpRollbackTargets(
        settings=settings,
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=f"{EVENT_TYPE}_rocket",
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
        http_transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )
    try:
        assert targets.original_event_transport() == "rocketmq"
        targets.switch_to_msg()
        report = targets.switch_to_original()
        assert report.event.implementation == "rocketmq"
        await targets.clear_injection()
    finally:
        await targets.aclose()


def test_build_targets_factory(switch_settings: Settings) -> None:
    """缺省目标工厂按命令行参数构造网络目标（不发起调用）。"""
    args = argparse.Namespace(
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=EVENT_TYPE,
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
        outbox_url="",
    )
    targets = rollback.build_targets(switch_settings, args)
    assert isinstance(targets, rollback.HttpRollbackTargets)
    assert targets.original_config().event.implementation == "rocketmq"


_FAILURE_MODES: ConcurrentStableFrozenList[tuple[str, str, str]] = ConcurrentStableFrozenList(
    (
        ("baseline", "baseline", "failed"),
        ("baseline_readonly_raise", "baseline_readonly", "failed"),
        ("baseline_readonly_empty", "baseline_readonly", "failed"),
        ("no_outbox", "baseline_event", "skipped"),
        ("baseline_event_raise", "baseline_event", "failed"),
        ("baseline_event_zero", "baseline_event", "failed"),
        ("baseline", "baseline", "failed"),
        ("switch_msg", "switch_to_msg", "failed"),
        ("switch_version", "switch_to_msg", "failed"),
        ("prepare", "prepare", "failed"),
        ("chain_short", "event_chain_msg", "failed"),
        ("chain_raise", "event_chain_msg", "failed"),
        ("readonly_msg_raise", "readonly_msg", "failed"),
        ("readonly_msg_empty", "readonly_msg", "failed"),
        ("snapshot_msg_raise", "snapshot_msg", "failed"),
        ("snapshot_msg_unavailable", "snapshot_msg", "degraded"),
        ("consistency_degraded", "consistency", "degraded"),
        ("dual_write_short", "dual_write_msg", "failed"),
        ("dual_write_raise", "dual_write_msg", "failed"),
        ("inflight_raise", "inflight_idempotency", "failed"),
        ("inflight_none", "inflight_idempotency", "skipped"),
        ("inflight_two", "inflight_idempotency", "failed"),
        ("inflight_keys", "inflight_idempotency", "failed"),
        ("unavailable_wrong_exc", "inject.msg_unavailable", "failed"),
        ("unavailable_readonly_ok", "inject.msg_unavailable", "failed"),
        ("error_rate_wrong_exc", "inject.publish_error_rate", "failed"),
        ("inject_raise", "inject.msg_unavailable", "failed"),
        ("unconfirmed_raise", "unconfirmed.msg_unavailable", "failed"),
        ("unconfirmed_published", "unconfirmed.msg_unavailable", "failed"),
        ("rollback_version", "rollback.msg_unavailable", "failed"),
        ("recover_zero", "recover.msg_unavailable", "failed"),
        ("recover_second", "recover.msg_unavailable", "failed"),
        ("recover_readonly_raise", "recover.msg_unavailable", "failed"),
        ("recover_readonly_empty", "recover.msg_unavailable", "failed"),
        ("recover_digest", "recover.msg_unavailable", "failed"),
        ("snapshot_scenario_raise", "snapshot.msg_unavailable", "failed"),
        ("switch_back_errors", "switch_back.msg_unavailable", "failed"),
        ("switch_back_raise", "switch_back.msg_unavailable", "failed"),
        ("switch_back_count", "switch_back.msg_unavailable", "failed"),
        ("dual_original_raise", "dual_write_original", "failed"),
        ("dual_original_mismatch", "dual_write_original", "failed"),
        ("snapshot_final_raise", "snapshot_final", "failed"),
    )
)
"""失败分支模式 → 期望失败 / 降级的步骤。"""


@pytest.mark.parametrize(("broken", "step", "expected"), _FAILURE_MODES)
async def test_drill_failure_branches(
    broken: str,
    step: str,
    expected: str,
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """逐条破坏一个判据：对应步骤失败 / 降级，结论不是通过。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken=broken
    )
    steps = {item.step: item.result for item in report.steps}
    assert steps.get(step) == expected, f"{broken}: {steps}"
    assert report.conclusion != rollback.CONCLUSION_PASS


async def test_drill_fails_when_recovery_over_budget(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """恢复超预算：`recover.*` 失败（不静默放行）。"""
    report = await _drill(
        msg_app=msg_app,
        switch_settings=switch_settings,
        outbox_engine=outbox_engine,
        budget=1,
    )
    steps = {item.step: item.result for item in report.steps}
    assert steps["recover.msg_unavailable"] == rollback.RESULT_FAILED
    assert any("恢复超预算" in reason for reason in report.reasons)


async def test_drill_cleanup_failure_is_registered(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """清理失败不判演练失败，但必须登记为问题。"""
    report = await _broken_drill(
        msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine, broken="cleanup_raise"
    )
    assert any("清理隔离环境失败" in issue["summary"] for issue in report.issues)


async def test_drill_business_zero_change_failure(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """业务签名漂移：`business_zero_change` 失败，结论失败。"""

    def _drift() -> tuple[ConcurrentStableFrozenList[str], ConcurrentStableFrozenDict[str, str]]:
        return ConcurrentStableFrozenList(("端口签名漂移",)), ConcurrentStableFrozenDict()

    monkeypatch.setattr(
        rollback,
        "business_zero_change",
        _drift,
    )
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings, outbox_engine=outbox_engine)
    steps = {item.step: item.result for item in report.steps}
    assert steps["business_zero_change"] == rollback.RESULT_FAILED
    assert report.conclusion == rollback.CONCLUSION_FAIL


def test_publishers_and_registry_adapter() -> None:
    """内部发布器替身与注册表适配（网络目标的构件）逐方法可用。"""
    recording = rollback._RecordingPublisher()  # pyright: ignore[reportPrivateUsage]
    recording_publish = asyncio.run(recording.publish(_fixed_event(1)))
    asyncio.run(recording.publish_transactional(_fixed_event(2)))
    assert recording.event_type == "rocketmq"
    assert recording_publish is None
    assert len(recording.delivered) == 2

    switchable = rollback._SwitchablePublisher(recording)  # pyright: ignore[reportPrivateUsage]
    assert switchable.event_type == "rocketmq"
    asyncio.run(switchable.publish(_fixed_event(3)))
    asyncio.run(switchable.publish_transactional(_fixed_event(4)))
    asyncio.run(switchable.aclose())

    unavailable = rollback._UnavailablePublisher()  # pyright: ignore[reportPrivateUsage]
    assert unavailable.event_type == "msg"
    with pytest.raises(EventPublishError):
        asyncio.run(unavailable.publish(_fixed_event(5)))
    with pytest.raises(EventPublishError):
        asyncio.run(unavailable.publish_transactional(_fixed_event(6)))

    flaky = rollback._FlakyPublisher(recording, fail_every=2)  # pyright: ignore[reportPrivateUsage]
    assert flaky.event_type == "rocketmq"
    asyncio.run(flaky.publish(_fixed_event(7)))
    with pytest.raises(EventPublishError):
        asyncio.run(flaky.publish(_fixed_event(8)))
    asyncio.run(flaky.publish_transactional(_fixed_event(9)))
    with pytest.raises(EventPublishError):
        asyncio.run(flaky.publish_transactional(_fixed_event(10)))

    engine = cast("AsyncEngine", object())
    registry = rollback._RegistryAdapter(engine)  # pyright: ignore[reportPrivateUsage]
    assert registry.is_sync_only("platform") is False
    assert asyncio.run(registry.get("platform")) is engine


def test_build_targets_requires_original_url(switch_settings: Settings) -> None:
    """缺原链路只读地址即拒绝构造（不给出无法验证恢复的演练）。"""
    args = argparse.Namespace(
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=EVENT_TYPE,
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="",
        outbox_url="",
    )
    with pytest.raises(rollback.RollbackError):
        rollback.build_targets(switch_settings, args)


def test_cli_backup_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """演练前备份成功：记录备份标识并继续（目标用最小替身）。"""
    from ops import backup as backup_module

    class _Config:
        now = _NOW

    def _config_from_env(*args: object, **kwargs: object) -> _Config:
        del args, kwargs
        return _Config()

    def _run_backup(config: object, io: object) -> int:
        del config, io
        return 0

    def _factory(settings: Settings, args: argparse.Namespace) -> rollback.RollbackTargets:
        del settings, args
        return cast("rollback.RollbackTargets", object())

    monkeypatch.setattr(backup_module, "config_from_env", _config_from_env)
    monkeypatch.setattr(backup_module, "run", _run_backup)
    monkeypatch.setattr(rollback, "_TARGETS_FACTORY", _factory)

    async def _run(**kwargs: object) -> rollback.RollbackReport:
        del kwargs
        return _report_stub(rollback.CONCLUSION_PASS)

    monkeypatch.setattr(rollback, "run_rollback_drill", _run)
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--backup",
            "--report",
            str(tmp_path / "backup-ok"),
        ]
    )
    assert code == 0


async def test_http_targets_consume_and_outbox_url(
    msg_app: FastAPI,
    switch_settings: Settings,
    tmp_path: Path,
) -> None:
    """网络目标：消费接口、在途跳过、按连接串建发件箱（内存 SQLite）与清理。"""
    url = f"sqlite+aiosqlite:///{tmp_path.as_posix()}/outbox.db"
    setup_engine = create_async_engine(url)
    async with setup_engine.begin() as connection:
        await connection.run_sync(Base.metadata.tables["sys_outbox"].create)
    await setup_engine.dispose()
    targets = rollback.HttpRollbackTargets(
        settings=switch_settings,
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=f"{EVENT_TYPE}_consume",
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
        http_transport=ASGITransport(app=msg_app),
        rpc_transport=AsgiRpcTransport(app=msg_app),
        outbox_url=f"sqlite+aiosqlite:///{tmp_path.as_posix()}/outbox.db",
    )
    try:
        targets.switch_to_msg()
        await targets.prepare()
        assert await targets.consume() == ()
        assert await targets.inflight_attempt("k") is None
        await targets.enqueue_outbox(_fixed_event(4000, tenant=None))
        round_result = await targets.dispatch_outbox()
        assert round_result.published == 1
        await targets.enqueue_outbox(_fixed_event(4001, tenant=None))
        snapshot = await targets.snapshot()
        assert snapshot.outbox.pending == 1
    finally:
        await targets.cleanup()
        await targets.aclose()


def test_switch_errors_reports_parse_failure() -> None:
    """切换判定带错误时逐项列出（不把解析失败吞成通过）。"""
    decision = migration.SwitchDecision(
        plugin_key="event",
        configured="bad",
        implementation="",
        mode="invalid",
        error_kind="plugin",
        error="未注册实现",
    )
    report = migration.SwitchReport(event=decision, service_client=decision, fingerprint="")
    errors = rollback.switch_errors(report, "msg")
    assert len(errors) == 2
    assert all("未注册实现" in line for line in errors)


async def test_drill_step_failures_on_bad_inputs(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """前置清单 / 矩阵 / 场景非法：对应步骤失败，不进入编排。"""
    matrix, preconditions, scenarios = _load()
    global _run_seq
    cases: ConcurrentStableList[tuple[str, migration.PreconditionSet, migration.SwitchMatrix, rollback.ScenarioSet]] = (
        ConcurrentStableList(
            (
                ("preconditions", replace(preconditions, items=ConcurrentStableFrozenList()), matrix, scenarios),
                ("matrix", preconditions, replace(matrix, schema_version=2), scenarios),
                ("scenarios", preconditions, matrix, replace(scenarios, recovery_budget_ms=0)),
            )
        )
    )
    for step, bad_pre, bad_matrix, bad_scenarios in cases:
        _run_seq += 1
        target = _InProcessTargets(
            app=msg_app,
            settings=switch_settings,
            engine=outbox_engine,
            event_type=f"{EVENT_TYPE}_bad_{_run_seq}",
        )
        report = await rollback.run_rollback_drill(
            matrix=bad_matrix,
            preconditions=bad_pre,
            scenarios=bad_scenarios,
            targets=target,
            env="test",
            backup=rollback.BackupEvidence("not_applicable", "", ""),
            event_type=f"{EVENT_TYPE}_bad_{_run_seq}",
            run_nonce=f"bad{_run_seq}",
            now=lambda: _NOW,
            clock=_StepClock(),
        )
        steps = {item.step: item.result for item in report.steps}
        assert steps[step] == rollback.RESULT_FAILED


async def test_drill_gate_all_satisfied_has_no_blocking_issue(
    msg_app: FastAPI,
    switch_settings: Settings,
    outbox_engine: AsyncEngine,
) -> None:
    """前置全绿时不再登记「生产迁移被阻断」问题（闸门分支另一侧）。"""
    matrix, preconditions, scenarios = _load()
    satisfied = replace(
        preconditions,
        items=ConcurrentStableFrozenList(replace(item, status="satisfied") for item in preconditions.items),
    )
    global _run_seq
    _run_seq += 1
    event_type = f"{EVENT_TYPE}_satisfied_{_run_seq}"
    target = _InProcessTargets(
        app=msg_app,
        settings=switch_settings,
        engine=outbox_engine,
        event_type=event_type,
    )
    report = await rollback.run_rollback_drill(
        matrix=matrix,
        preconditions=satisfied,
        scenarios=scenarios,
        targets=target,
        env="test",
        backup=rollback.BackupEvidence("not_applicable", "", ""),
        event_type=event_type,
        run_nonce=f"satisfied{_run_seq}",
        now=lambda: _NOW,
        clock=_StepClock(),
    )
    assert report.gate["blocked_by"] == ()
    assert not any("生产迁移被前置清单阻断" in issue["summary"] for issue in report.issues)


async def test_http_targets_without_rocketmq_endpoints(switch_settings: Settings) -> None:
    """未配置 RocketMQ 端点：载体为记录型替身，关闭时不触碰 RocketMQ 客户端。"""
    settings = switch_settings
    settings.rocketmq.endpoints = ""
    targets = rollback.HttpRollbackTargets(
        settings=settings,
        msg_url=BASE_URL,
        unavailable_url="http://unavailable",
        namespace=EVENT_NAMESPACE,
        event_type=f"{EVENT_TYPE}_norocket",
        group=migration.MIGRATION_DEFAULT_GROUP,
        consumer_id=migration.MIGRATION_DEFAULT_CONSUMER_ID,
        tenant="demo",
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        original_readonly_url="http://fallback",
        http_transport=httpx.MockTransport(lambda request: httpx.Response(200)),
    )
    try:
        assert targets.original_event_transport() == "recording"
    finally:
        await targets.aclose()
        await targets.aclose()


def test_cli_backup_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """演练前备份非 0 退出即中止。"""
    from ops import backup as backup_module

    class _Config:
        now = _NOW

    def _config_from_env(*args: object, **kwargs: object) -> _Config:
        del args, kwargs
        return _Config()

    def _run_backup(config: object, io: object) -> int:
        del config, io
        return 2

    monkeypatch.setattr(backup_module, "config_from_env", _config_from_env)
    monkeypatch.setattr(backup_module, "run", _run_backup)
    code = rollback.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--original-readonly-url",
            "http://fallback",
            "--backup",
            "--report",
            str(tmp_path / "backup-bad"),
        ]
    )
    assert code == 1


def test_cli_drill_target_factory_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """目标构造抛 ValueError / RollbackError：分别退 2 / 1。"""
    args = [
        "drill",
        "--env",
        "test",
        "--msg-url",
        BASE_URL,
        "--original-readonly-url",
        "http://fallback",
    ]

    def _value_error(settings: Settings, args: argparse.Namespace) -> rollback.RollbackTargets:
        del settings, args
        raise ValueError("目标参数非法")

    monkeypatch.setattr(rollback, "_TARGETS_FACTORY", _value_error)
    assert rollback.main(args) == 2

    def _rollback_error(settings: Settings, args: argparse.Namespace) -> rollback.RollbackTargets:
        del settings, args
        raise rollback.RollbackError("目标不可用")

    monkeypatch.setattr(rollback, "_TARGETS_FACTORY", _rollback_error)
    assert rollback.main(args) == 1


def test_main_matrix_validation_failure(tmp_path: Path) -> None:
    """场景能解析但校验不过：`matrix` 退 1（另一条分支）。"""
    single = tmp_path / "single.toml"
    single.write_text(
        """schema_version = 1
[meta]
updated_at = "2026-11-10"
owner = "minjian"
recovery_budget_ms = 1000

[[scenarios]]
name = "only"
mode = "unavailable"
target = "both"
criterion = "观察窗口内出现消费方报错或事件缺失"
threshold = 0.0
samples = 1
expected = "不可达"
note = ""
""",
        encoding="utf-8",
    )
    assert rollback.main(["matrix", "--scenarios", str(single)]) == 1


def test_module_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m ops.rollback` 入口按 `main()` 返回码退出。"""
    import runpy
    import sys

    monkeypatch.setattr(sys, "argv", ["ops.rollback", "check", "--env", "test"])
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("ops.rollback", run_name="__main__")
    assert exc.value.code == 0


def test_check_all_satisfied_has_no_blocker_lines(tmp_path: Path) -> None:
    """前置全绿：`check` 不输出阻断项（分支另一侧）。"""
    satisfied = tmp_path / "preconditions.toml"
    satisfied.write_text(
        _PRECONDITIONS.read_text(encoding="utf-8").replace('status = "not_ready"', 'status = "satisfied"'),
        encoding="utf-8",
    )
    code, lines = rollback.run_check(
        preconditions_path=satisfied,
        matrix_path=_MATRIX,
        scenarios_path=_SCENARIOS,
        env="test",
    )
    assert code == 0
    assert not any("阻断项" in line for line in lines)
