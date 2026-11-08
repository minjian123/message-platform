"""05-1 `tenant.created` 消费（Kiwi 2452）。

覆盖：消费组声明与契约校验；同版本重复投递跳过；新版本重跑；失败原样上抛并计数；
账本缺失 / 损坏 / 非法值处理；`mark_applied` 保留其它动作；应用工厂钩子。
"""

import json
from pathlib import Path

import pytest
from bms_core.events.base import EventEnvelope
from bms_core.events.consumer_records import validate_event_consumer_records
from bms_core.events.platform_events import register_platform_event_contracts
from bms_core.tenant.lifecycle import TenantLifecycleContext, build_tenant_lifecycle_event

from bms_msg.main import ApplicationFactory
from bms_msg.tenant.lifecycle import (
    LEDGER_NAME,
    MSG_TENANT_LIFECYCLE_GROUP,
    MsgTenantResourceHandler,
    TenantLifecycleConsumer,
    TenantLifecycleMetrics,
    event_consumer_records,
    reset_tenant_lifecycle_metrics,
    tenant_lifecycle_consumer,
)

pytestmark = pytest.mark.kiwi_id(2452)


@pytest.fixture(autouse=True)
def _platform_event_contracts() -> None:
    """登记平台默认事件契约（直接调工厂钩子时也要能通过 `tenant.created` 契约校验）。"""
    register_platform_event_contracts()


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """每个用例前清零进程级计数（覆盖 `reset_tenant_lifecycle_metrics`）。"""
    reset_tenant_lifecycle_metrics()


class _FailingHandler(MsgTenantResourceHandler):
    """初始化必失败的处理器（验证失败上抛与计数）。"""

    async def on_created(self, context: TenantLifecycleContext) -> None:
        """恒抛异常。

        Args:
            context: 生命周期上下文。

        Raises:
            RuntimeError: 恒抛。
        """
        del context
        raise RuntimeError("boom")


def _event(version: int) -> EventEnvelope:
    """构造 `tenant.created` 事件。

    Args:
        version: 版本戳。

    Returns:
        EventEnvelope: 租户生命周期事件。
    """
    return build_tenant_lifecycle_event(
        event_type="tenant.created",
        tenant_id=7001,
        db_key="tenant_demo",
        status="active",
        version=version,
    )


def test_event_consumer_records_declares_five_lifecycle_events() -> None:
    """消费组声明五条生命周期事件，工厂可调用，声明通过基座校验。"""
    records = event_consumer_records()
    assert [record.event_type for record in records] == [
        "tenant.created",
        "tenant.suspended",
        "tenant.activated",
        "tenant.deleted",
        "tenant.purged",
    ]
    for record in records:
        assert record.consumer == MSG_TENANT_LIFECYCLE_GROUP
        assert record.ordered is True
        assert callable(record.handler_factory)
    assert tenant_lifecycle_consumer().event_type == "tenant.created"
    assert list(validate_event_consumer_records(records)) == []


def test_factory_event_consumer_records_hook() -> None:
    """应用工厂 `event_consumer_records()` 钩子返回五条租户生命周期声明。"""
    records = ApplicationFactory().event_consumer_records()
    assert [record.event_type for record in records] == [
        "tenant.created",
        "tenant.suspended",
        "tenant.activated",
        "tenant.deleted",
        "tenant.purged",
    ]
    assert list(validate_event_consumer_records(records)) == []


async def test_consume_creates_directory_and_ledger_idempotently(tmp_path: Path) -> None:
    """同租户同版本重复消费：目录只建一次、账本记录版本、第二次跳过。"""
    metrics = TenantLifecycleMetrics()
    consumer = TenantLifecycleConsumer(handler=MsgTenantResourceHandler(root=tmp_path), metrics=metrics)
    event = _event(1)

    await consumer.consume(event)
    tenant_dir = tmp_path / "7001"
    assert tenant_dir.is_dir()
    assert json.loads((tenant_dir / LEDGER_NAME).read_text(encoding="utf-8")) == {"created": 1}
    assert metrics.snapshot() == {"applied": 1, "skipped": 0, "failed": 0}

    await consumer.consume(event)
    assert metrics.snapshot() == {"applied": 1, "skipped": 1, "failed": 0}


async def test_consume_new_version_reapplies(tmp_path: Path) -> None:
    """新版本到达：重跑（目录幂等）并把账本版本推进到新值。"""
    metrics = TenantLifecycleMetrics()
    consumer = TenantLifecycleConsumer(handler=MsgTenantResourceHandler(root=tmp_path), metrics=metrics)

    await consumer.consume(_event(1))
    await consumer.consume(_event(2))

    assert json.loads((tmp_path / "7001" / LEDGER_NAME).read_text(encoding="utf-8")) == {"created": 2}
    assert metrics.snapshot() == {"applied": 2, "skipped": 0, "failed": 0}


async def test_consume_failure_raises_and_counts(tmp_path: Path) -> None:
    """初始化失败：原样上抛供重试 / 死信，失败计数 +1，不写账本。"""
    metrics = TenantLifecycleMetrics()
    consumer = TenantLifecycleConsumer(handler=_FailingHandler(root=tmp_path), metrics=metrics)

    with pytest.raises(RuntimeError, match="boom"):
        await consumer.consume(_event(1))

    assert metrics.snapshot() == {"applied": 0, "skipped": 0, "failed": 1}
    assert not (tmp_path / "7001" / LEDGER_NAME).exists()


async def test_corrupt_ledger_recovers(tmp_path: Path) -> None:
    """账本损坏按未初始化重跑，并修复为合法账本。"""
    tenant_dir = tmp_path / "7001"
    tenant_dir.mkdir()
    (tenant_dir / LEDGER_NAME).write_text("{not-json", encoding="utf-8")
    metrics = TenantLifecycleMetrics()
    consumer = TenantLifecycleConsumer(handler=MsgTenantResourceHandler(root=tmp_path), metrics=metrics)

    await consumer.consume(_event(1))

    assert json.loads((tenant_dir / LEDGER_NAME).read_text(encoding="utf-8")) == {"created": 1}
    assert metrics.snapshot() == {"applied": 1, "skipped": 0, "failed": 0}


async def test_applied_version_handles_missing_and_invalid_values(tmp_path: Path) -> None:
    """账本缺失 / 非对象 / 布尔 / 字符串版本均按 `-1`，合法整数版本原样返回。"""
    handler = MsgTenantResourceHandler(root=tmp_path)
    assert await handler.applied_version(tenant_id=7001, action="created") == -1

    tenant_dir = tmp_path / "7001"
    tenant_dir.mkdir()
    ledger = tenant_dir / LEDGER_NAME
    ledger.write_text(json.dumps({"created": True, "suspended": "3", "other": 7}), encoding="utf-8")
    assert await handler.applied_version(tenant_id=7001, action="created") == -1
    assert await handler.applied_version(tenant_id=7001, action="suspended") == -1
    assert await handler.applied_version(tenant_id=7001, action="other") == 7

    ledger.write_text("[1, 2]", encoding="utf-8")
    assert await handler.applied_version(tenant_id=7001, action="other") == -1


async def test_mark_applied_preserves_other_actions(tmp_path: Path) -> None:
    """记账只覆盖当前动作，保留账本内其它动作的版本。"""
    handler = MsgTenantResourceHandler(root=tmp_path)
    tenant_dir = tmp_path / "7001"
    tenant_dir.mkdir()
    ledger = tenant_dir / LEDGER_NAME
    ledger.write_text(json.dumps({"suspended": 5}), encoding="utf-8")

    await handler.mark_applied(tenant_id=7001, action="created", version=2)

    assert json.loads(ledger.read_text(encoding="utf-8")) == {"created": 2, "suspended": 5}
