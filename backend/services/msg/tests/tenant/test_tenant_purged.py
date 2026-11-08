# pyright: reportPrivateUsage=false
"""05-5 两步删除接线（Kiwi 2453）：deleted 只停写、purged 才删、重复幂等、激活竞态与跨仓契约。

- `tenant.deleted`：停写 + 清易失资源，**数据 / 位点 / 元数据全部保留**（不自算清理时点）。
- `tenant.purged`：平台已物理清理 → 一次性删除该租户消息资源并写 `purged_at`；重复消费按 `purged_at` 跳过。
- 激活竞态：`activated` 早到不阻止后续 `purged`；`purged` 之后 `activated` 不复活已删数据。
- 跨仓契约：直接读 bms 契约快照校验 `tenant.purged` 五字段与五事件集合一致。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ConfigError
from bms_core.events.base import EventEnvelope
from bms_core.tenant.lifecycle import TenantLifecycleContext, build_tenant_lifecycle_event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.tenant.lifecycle import MsgTenantResourceHandler, TenantLifecycleConsumer, event_consumer_records
from bms_msg.tenant.purge import TenantPurgeService
from bms_msg.tenant.runtime import TenantLifecycleMetrics, TenantLifecycleRuntime

pytestmark = pytest.mark.kiwi_id(2453)

TENANT_ID = 7001
"""测试租户主键。"""

_TOPIC_ID = 9201
"""测试主题主键。"""

_NOW = datetime(2026, 11, 9, 8, 0, 0)
"""测试基准时刻（UTC naive）。"""

_BMS_EVENT_CONTRACTS = Path(__file__).resolve().parents[5] / ".." / "bms" / "deploy" / "events" / "contracts.json"
"""bms 事件契约快照（跨仓事实源；msg 对 bms 是路径依赖，缺克隆即环境不完整）。"""


def lifecycle_event(
    event_type: str,
    *,
    version: int | None = 1,
    status: str = "deleted",
) -> EventEnvelope:
    """构造一条租户生命周期事件（`tenant.purged` 契约无 `version`，传 None 即不写载荷）。

    Args:
        event_type: 事件类型。
        version: 版本戳；None 表示省略。
        status: 事件发生后的租户状态。

    Returns:
        EventEnvelope: 事件信封。
    """
    return build_tenant_lifecycle_event(
        event_type=event_type,
        tenant_id=TENANT_ID,
        db_key="tenant_demo",
        status=status,
        version=version,
    )


async def _seed_topic(engine: AsyncEngine, *, retention_ms: int = 30 * 24 * 3600 * 1000) -> None:
    """写入主题与一个分区（默认 30 天保留期，验证强制路径不看保留期）。

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


def _create_data_dir(root: Path) -> Path:
    """创建主题数据目录与一个段文件。

    Args:
        root: 数据根（`data` 的父目录）。

    Returns:
        Path: 主题目录。
    """
    topic_dir = root / "data" / str(TENANT_ID) / "msg" / f"audit.operation.{_TOPIC_ID}"
    partition = topic_dir / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000000.log").write_bytes(b"x")
    return topic_dir


def _create_offset(root: Path) -> Path:
    """创建一个位点文件。

    Args:
        root: 数据根。

    Returns:
        Path: 位点文件。
    """
    group_dir = root / "offsets" / str(TENANT_ID) / "audit-12345678"
    group_dir.mkdir(parents=True)
    path = group_dir / f"{_TOPIC_ID}-0.offset"
    path.write_bytes(b"offset")
    return path


def _handler(runtime: TenantLifecycleRuntime) -> MsgTenantResourceHandler:
    """构造接运行时的处理器。

    Args:
        runtime: 生命周期运行时。

    Returns:
        MsgTenantResourceHandler: 处理器。
    """
    return MsgTenantResourceHandler(runtime=runtime, ledger=runtime.ledger)


async def _consume(
    runtime: TenantLifecycleRuntime,
    event: EventEnvelope,
    *,
    metrics: TenantLifecycleMetrics | None = None,
) -> None:
    """消费一条事件（按事件类型构造处理器）。

    Args:
        runtime: 生命周期运行时。
        event: 事件信封。
        metrics: 计数容器（可空）。
    """
    consumer = TenantLifecycleConsumer(
        event_type=event.event_type,
        handler=_handler(runtime),
        metrics=metrics,
    )
    await consumer.consume(event)


def _audit_actions(runtime: TenantLifecycleRuntime) -> list[object]:
    """取审计替身中记录的动作名。

    Args:
        runtime: 运行时。

    Returns:
        list[object]: 动作名序列。
    """
    events = cast("list[tuple[EventEnvelope, bool]]", cast("Any", runtime.audit_sink).events)
    return [event.payload.get("action") for event, _required in events]


async def test_deleted_keeps_data_and_purged_removes_it(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, purge_service: TenantPurgeService, root: Path
) -> None:
    """两步删除：`deleted` 后数据 / 位点 / 元数据都在，`purged` 后全部消失。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    offset = _create_offset(root)

    await _consume(runtime, lifecycle_event("tenant.deleted", version=3))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purge_after is None and state.purged_at is None
    assert topic_dir.exists() and offset.exists()
    assert await runtime.ledger.count_topics(tenant_id=TENANT_ID) == 1

    await _consume(runtime, lifecycle_event("tenant.purged", version=None, status="purged"))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is not None
    assert not topic_dir.exists() and not offset.exists()
    assert await runtime.ledger.count_topics(tenant_id=TENANT_ID) == 0
    actions = [action for action in _audit_actions(runtime) if action != "msg:offset.purge"]
    assert actions == ["msg:tenant.delete", "msg:tenant.purge"]


async def test_repeated_purged_is_skipped(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, purge_service: TenantPurgeService, root: Path
) -> None:
    """重复投递 `tenant.purged`：首次 applied，第二次按版本判定 skipped（不重复删 / 不重复审计）。"""
    await _seed_topic(engine)
    _create_data_dir(root)
    metrics = TenantLifecycleMetrics()
    event = lifecycle_event("tenant.purged", version=None, status="purged")

    await _consume(runtime, event, metrics=metrics)
    await _consume(runtime, event, metrics=metrics)

    assert metrics.snapshot() == {"applied": 1, "skipped": 1, "failed": 0}
    assert _audit_actions(runtime) == ["msg:tenant.purge"]


async def test_purged_without_deleted_row_still_purges(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, purge_service: TenantPurgeService, root: Path
) -> None:
    """孤立 `tenant.purged`（deleted 事件丢失）：照常清理并补建账本行。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)

    await _consume(runtime, lifecycle_event("tenant.purged", version=None, status="purged"))

    assert not topic_dir.exists()
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is not None


async def test_activated_before_purged_does_not_block_purge(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, purge_service: TenantPurgeService, root: Path
) -> None:
    """激活先到（取消存量待清理）后仍有 `purged`：平台授权优先，照常清理。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    await runtime.ledger.mark_deleted(
        tenant_id=TENANT_ID,
        version=3,
        deleted_event_at=_NOW,
        purge_after=_NOW + timedelta(days=30),
    )

    await _consume(runtime, lifecycle_event("tenant.activated", version=4, status="active"))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purge_after is None
    assert topic_dir.exists()

    await _consume(runtime, lifecycle_event("tenant.purged", version=None, status="purged"))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is not None
    assert not topic_dir.exists()


async def test_activated_after_purged_does_not_resurrect(
    engine: AsyncEngine, runtime: TenantLifecycleRuntime, purge_service: TenantPurgeService, root: Path
) -> None:
    """`purged` 之后到达 `activated`：不抹掉 `purged_at`、不重建数据目录。"""
    await _seed_topic(engine)
    topic_dir = _create_data_dir(root)
    await _consume(runtime, lifecycle_event("tenant.purged", version=None, status="purged"))
    before = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert before is not None and before.purged_at is not None

    await _consume(runtime, lifecycle_event("tenant.activated", version=9, status="active"))

    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None
    assert state.purged_at == before.purged_at
    assert not topic_dir.exists()


async def test_late_deleted_event_keeps_purged_evidence(runtime: TenantLifecycleRuntime) -> None:
    """晚到的 `deleted`（事件时刻早于清理）不抹 `purged_at`；更晚的删除事件才开新周期。"""
    await runtime.ledger.mark_purged(tenant_id=TENANT_ID, at=_NOW)
    await runtime.ledger.mark_deleted(tenant_id=TENANT_ID, version=1, deleted_event_at=_NOW - timedelta(minutes=5))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at == _NOW

    await runtime.ledger.mark_deleted(tenant_id=TENANT_ID, version=2, deleted_event_at=_NOW + timedelta(minutes=5))
    state = await runtime.ledger.get_deleted(tenant_id=TENANT_ID)
    assert state is not None and state.purged_at is None


async def test_purged_requires_attached_entry(runtime: TenantLifecycleRuntime) -> None:
    """未挂载物理清理入口（装配缺漏）：fail-closed 而非静默跳过。"""
    with pytest.raises(ConfigError):
        await runtime.purge(tenant_id=TENANT_ID, version=0, context=_context())


def _context() -> TenantLifecycleContext:
    """构造生命周期上下文（仅用于直接调运行时入口）。

    Returns:
        TenantLifecycleContext: 上下文。
    """
    return TenantLifecycleContext(
        tenant_id=TENANT_ID,
        action="purged",
        version=0,
        db_key="tenant_demo",
        status="purged",
    )


def _snapshot_contract(event_type: str) -> ConcurrentStableDict[str, object]:
    """从 bms 契约快照取一条事件契约。

    Args:
        event_type: 事件类型。

    Returns:
        ConcurrentStableDict[str, object]: 契约条目。

    Raises:
        pytest.fail.Exception: 快照缺失或未登记该事件（跨仓口径不一致即失败）。
    """
    if not _BMS_EVENT_CONTRACTS.is_file():
        pytest.fail(f"缺 bms 事件契约快照（msg 对 bms 是路径依赖，工作区应含 bms 克隆）：{_BMS_EVENT_CONTRACTS}")
    raw = cast("dict[str, object]", json.loads(_BMS_EVENT_CONTRACTS.read_text(encoding="utf-8")))
    contracts = cast("list[dict[str, object]]", raw["contracts"])
    for item in contracts:
        if item.get("event_type") == event_type:
            return ConcurrentStableDict(item)
    pytest.fail(f"bms 事件契约快照未登记 {event_type}")


def test_bms_snapshot_declares_purged_contract() -> None:
    """跨仓契约：`tenant.purged` 为独立 `1.0.0`，五字段与必填 / 类型与 msg 消费口径一致。"""
    contract = _snapshot_contract("tenant.purged")
    assert contract["version"] == "1.0.0"
    assert contract["deprecated"] is False

    fields = cast("dict[str, dict[str, object]]", contract["fields"])
    assert set(fields) == {"tenant_id", "db_key", "status", "dropped_services", "failed_services"}
    assert fields["tenant_id"] == {"required": True, "type": "string"}
    assert fields["db_key"] == {"required": True, "type": "string"}
    assert fields["status"] == {"required": True, "type": "string"}
    assert fields["dropped_services"] == {"required": False, "type": "integer"}
    assert fields["failed_services"] == {"required": False, "type": "integer"}


def test_consumer_declarations_match_bms_snapshot() -> None:
    """msg 声明的五类事件与 bms 快照登记的 `tenant.*` 事件集合一一对应。"""
    raw = cast("dict[str, object]", json.loads(_BMS_EVENT_CONTRACTS.read_text(encoding="utf-8")))
    contracts = cast("list[dict[str, object]]", raw["contracts"])
    snapshot_types = {str(item["event_type"]) for item in contracts if str(item["event_type"]).startswith("tenant.")}
    declared = {record.event_type for record in event_consumer_records()}
    assert declared == snapshot_types
