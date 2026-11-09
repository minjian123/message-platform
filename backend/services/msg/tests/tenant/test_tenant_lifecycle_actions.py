"""05-5 生命周期动作（Kiwi 2453）：停用断连、启用取消清理、注销停写（不排期）与易失清理。"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ConfigError
from bms_core.events.base import EventEnvelope
from bms_core.tenant.lifecycle import build_tenant_lifecycle_event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import DeadLetter, DeadLetterReason
from bms_msg.broker.delay import DelayRecord
from bms_msg.broker.group import ConsumerGroup
from bms_msg.broker.transport import ConnectionMeta
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.tenant.lifecycle import MsgTenantResourceHandler, TenantLifecycleConsumer
from bms_msg.tenant.runtime import TenantLifecycleAuditError, TenantLifecycleRuntime

pytestmark = pytest.mark.kiwi_id(2453)

TENANT_ID = 7001
"""测试租户主键。"""


def lifecycle_event(
    event_type: str,
    *,
    tenant_id: int = TENANT_ID,
    version: int = 1,
    status: str = "active",
) -> EventEnvelope:
    """构造一条租户生命周期事件。

    Args:
        event_type: 事件类型。
        tenant_id: 租户主键。
        version: 版本戳。
        status: 租户状态。

    Returns:
        EventEnvelope: 事件信封。
    """
    return build_tenant_lifecycle_event(
        event_type=event_type,
        tenant_id=tenant_id,
        db_key="tenant_demo",
        status=status,
        version=version,
    )


_TOPIC_ID = 9001
"""测试主题主键。"""

_META = ConnectionMeta(http_version="HTTP/2", scheme="http", is_tls=False, client_host="10.0.0.1", client_port=1)
"""测试连接元数据。"""


def _audit_events(runtime: TenantLifecycleRuntime) -> list[tuple[EventEnvelope, bool]]:
    """取审计替身记录（测试专用）。

    Args:
        runtime: 运行时。

    Returns:
        list[tuple[EventEnvelope, bool]]: 审计事件与是否强审计。
    """
    return cast("list[tuple[EventEnvelope, bool]]", cast("Any", runtime.audit_sink).events)


async def _seed_topic(engine: AsyncEngine, *, retention_ms: int = 1_000) -> None:
    """写入一条主题与一个分区。

    Args:
        engine: 内存库引擎。
        retention_ms: 保留时长（毫秒）。
    """
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session, session.begin():
        session.add(
            MsgTopic(
                id=_TOPIC_ID,
                tenant_id=TENANT_ID,
                namespace="msg",
                name="audit.operation",
                partition_count=1,
                retention_ms=retention_ms,
            )
        )
        session.add(MsgPartition(topic_id=_TOPIC_ID, partition_no=0, leader="msg-0"))


def _seed_connection(runtime: TenantLifecycleRuntime) -> str:
    """登记一条连接并开一条长驻流。

    Args:
        runtime: 运行时。

    Returns:
        str: 流标识。
    """
    key = "demo|10.0.0.1:1"
    runtime.connections.touch(
        key=key,
        meta=_META,
        tenant="demo",
        now=1.0,
        wall=1.0,
        heartbeat=False,
        tenant_id=TENANT_ID,
    )
    return runtime.streams.open(
        namespace="msg",
        topic="audit.operation",
        group="audit",
        consumer_id="c1",
        partition=0,
        opened_at=1.0,
        connection_key=key,
        tenant_id=TENANT_ID,
    )


async def _seed_volatile(runtime: TenantLifecycleRuntime) -> None:
    """写入延迟记录 / 组 / 死信（删除动作应全部清掉）。

    Args:
        runtime: 运行时。
    """
    now = datetime.now(UTC)
    delay_store = cast("Any", runtime.delay_store)
    await delay_store.enqueue(
        DelayRecord(
            log_id="l1",
            message_id="m1",
            tenant_id=TENANT_ID,
            topic_id=_TOPIC_ID,
            namespace="msg",
            topic="audit.operation",
            partition_no=0,
            delay_level="1s",
            key=None,
            payload=b"x",
            headers=ConcurrentStableDict(),
            trace=EMPTY_TRACE,
            published_at=now,
            deliver_at=now + timedelta(seconds=1),
        )
    )
    await runtime.groups.save(
        ConsumerGroup(
            tenant_id=TENANT_ID,
            topic_id=_TOPIC_ID,
            group="audit",
            state="stable",
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    await runtime.dead_letters.add(
        DeadLetter(
            message_id="m1",
            topic_id=_TOPIC_ID,
            partition_no=0,
            offset=0,
            attempts=1,
            failed_at=now,
            reason=DeadLetterReason.PROCESSING_FAILED,
        )
    )


async def test_created_creates_tenant_directory(tmp_path: Path) -> None:
    """`tenant.created` 回归：只建租户目录并记账。"""
    handler = MsgTenantResourceHandler(root=tmp_path)
    consumer = TenantLifecycleConsumer(handler=handler)
    await consumer.consume(lifecycle_event("tenant.created", version=1))
    assert (tmp_path / str(TENANT_ID)).is_dir()


async def test_suspend_cancels_connections_and_streams(runtime: TenantLifecycleRuntime) -> None:
    """停用：按 tenant_id 取消连接与长驻流，不动数据。"""
    stream_id = _seed_connection(runtime)
    handler = MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)
    consumer = TenantLifecycleConsumer(event_type="tenant.suspended", handler=handler)
    await consumer.consume(lifecycle_event("tenant.suspended", version=2, status="suspended"))

    assert runtime.connections.records() == []
    assert runtime.streams.is_cancelled(stream_id) is True
    assert [event.payload.get("action") for event, _required in _audit_events(runtime)] == ["msg:tenant.suspend"]


async def test_activate_cancels_purge(runtime: TenantLifecycleRuntime) -> None:
    """启用：取消待清理；保留删除事件时刻供审计。"""
    deleted_at = datetime(2026, 11, 8, 12, 0, 0)
    await runtime.ledger.mark_deleted(
        tenant_id=TENANT_ID,
        version=1,
        deleted_event_at=deleted_at,
        purge_after=deleted_at + timedelta(days=30),
    )
    handler = MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)
    consumer = TenantLifecycleConsumer(event_type="tenant.activated", handler=handler)
    await consumer.consume(lifecycle_event("tenant.activated", version=2))

    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None
    assert state.purge_after is None
    assert state.deleted_event_at == deleted_at


async def test_delete_stops_writes_without_scheduling_purge(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, root: Path
) -> None:
    """删除（第一步）：清连接 / 流 / 延迟 / 组 / 死信并写 deleted_event_at，**不排定数据清理**。"""
    await _seed_topic(engine, retention_ms=1_000)
    stream_id = _seed_connection(runtime)
    await _seed_volatile(runtime)
    data_dir = root / "data" / str(TENANT_ID) / "msg" / f"audit.operation.{_TOPIC_ID}" / "0"
    data_dir.mkdir(parents=True)
    (data_dir / "00000000000000000000.log").write_bytes(b"payload")
    handler = MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)
    consumer = TenantLifecycleConsumer(event_type="tenant.deleted", handler=handler)
    await consumer.consume(lifecycle_event("tenant.deleted", version=3, status="deleted"))

    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None
    assert state.deleted_event_at is not None
    assert state.purge_after is None, "两步口径下 tenant.deleted 不得自算清理时点"
    assert state.purged_at is None
    assert runtime.connections.records() == []
    assert runtime.streams.is_cancelled(stream_id) is True
    assert await runtime.delay_store.count_for_tenant(tenant_id=TENANT_ID) == 0
    assert list(await runtime.groups.list_for_tenant(tenant_id=TENANT_ID)) == []
    assert list(await runtime.dead_letters.list_all()) == []
    # 数据与元数据保留到 tenant.purged。
    assert (data_dir / "00000000000000000000.log").exists()
    assert await runtime.ledger.count_topics(tenant_id=TENANT_ID) == 1
    audit = [(event, required) for event, required in _audit_events(runtime) if required]
    assert [event.payload.get("action") for event, _required in audit] == ["msg:tenant.delete"]
    params = json.loads(str(audit[0][0].payload.get("params")))
    assert "deleted_event_at" in params
    assert "purge_after" not in params


async def test_delete_same_version_is_idempotent(engine: AsyncEngine, runtime: TenantLifecycleRuntime) -> None:
    """同版本重复删除：deleted_event_at 不变、不产生排期；新版本以最新事件时刻为准。"""
    await _seed_topic(engine, retention_ms=1_000)
    handler = MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)
    consumer = TenantLifecycleConsumer(event_type="tenant.deleted", handler=handler)
    event = lifecycle_event("tenant.deleted", version=3, status="deleted")
    await consumer.consume(event)
    first = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    await consumer.consume(event)
    second = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert first is not None and second is not None
    assert second.deleted_event_at == first.deleted_event_at
    assert second.purge_after is None
    assert second.applied_version == 3


async def test_delete_audit_failure_raises(engine: AsyncEngine, runtime: TenantLifecycleRuntime) -> None:
    """删除强审计失败：上抛，applied_version 不推进。"""
    await _seed_topic(engine)
    runtime.audit_sink.fail = True  # type: ignore[attr-defined]
    handler = MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)
    consumer = TenantLifecycleConsumer(event_type="tenant.deleted", handler=handler)
    with pytest.raises(TenantLifecycleAuditError):
        await consumer.consume(lifecycle_event("tenant.deleted", version=3, status="deleted"))
    assert await runtime.ledger.applied_version(tenant_id=TENANT_ID, action="deleted") == -1


async def test_actions_require_runtime() -> None:
    """停用 / 启用 / 删除 / 物理清理在未装配运行时 fail-closed。"""
    handler = MsgTenantResourceHandler()
    for event_type in ("tenant.suspended", "tenant.activated", "tenant.deleted", "tenant.purged"):
        with pytest.raises(ConfigError):
            if event_type == "tenant.suspended":
                await handler.on_suspended(_context(event_type))
            elif event_type == "tenant.activated":
                await handler.on_activated(_context(event_type))
            elif event_type == "tenant.deleted":
                await handler.on_deleted(_context(event_type))
            else:
                await handler.on_purged(_context(event_type))


def _context(event_type: str):
    """构造生命周期上下文（仅用于直接调动作）。

    Args:
        event_type: 事件类型。

    Returns:
        TenantLifecycleContext: 上下文。
    """
    from bms_core.tenant.lifecycle import TenantLifecycleContext

    return TenantLifecycleContext(
        tenant_id=TENANT_ID,
        action=event_type.split(".", 1)[1],
        version=1,
        db_key="tenant_demo",
        status="active",
    )


def test_connection_registry_purge_is_scoped() -> None:
    """连接取消只影响目标 tenant_id。"""
    registry = ConnectionRegistry()
    registry.touch(key="a|1:1", meta=_META, tenant="a", now=1, wall=1, heartbeat=False, tenant_id=1)
    registry.touch(key="b|1:1", meta=_META, tenant="b", now=1, wall=1, heartbeat=False, tenant_id=2)
    assert registry.purge_tenant(tenant_id=1, reason="tenant_deleted") == 1
    assert [record.tenant_id for record in registry.records()] == [2]
