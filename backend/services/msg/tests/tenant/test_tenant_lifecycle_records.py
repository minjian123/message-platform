"""05-5 消费组声明与工厂（Kiwi 2453）：四事件、零参兼容入口与实例事件类型。"""

from __future__ import annotations

import pytest

from bms_msg.tenant.lifecycle import event_consumer_records, tenant_lifecycle_consumer

pytestmark = pytest.mark.kiwi_id(2453)


def test_event_records_and_factories() -> None:
    """四条事件声明；工厂实例的事件类型与声明一致；零参入口仍是 created。"""
    records = event_consumer_records()
    assert [record.event_type for record in records] == [
        "tenant.created",
        "tenant.suspended",
        "tenant.activated",
        "tenant.deleted",
    ]
    for record in records:
        handler = record.handler_factory()
        assert handler.event_type == record.event_type
    assert tenant_lifecycle_consumer().event_type == "tenant.created"
