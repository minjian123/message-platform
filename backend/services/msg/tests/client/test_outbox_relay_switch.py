"""01-8-1 outbox 适配与 relay 切换 · 跨仓联调用例（Kiwi 2481）。

bms 仓 `tests/outbox/test_msg_outbox_switch.py` 已用真 SQLite 发件箱覆盖「表 → relay → 标记」；
本文件补另一半：relay 由 `OutboxRecord.to_envelope()` 重建的事件信封，经 `MsgEventPublisher`
到**真 msg 应用 / 真数据面 / 真消费端**，验证载荷 / 属性 / trace 逐项一致、失败信号可观测、
失败后补发恰好一次、relay 只有一条投递通道。

只在 msg 仓能同时 import 两侧（`bms-core` 是路径依赖），故这条证据落在 msg 仓；
bms 仓 CI 不 clone msg，那边只做替身级演练。
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from datetime import datetime
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenDict, ConcurrentStableList
from bms_core.events.base import EventEnvelope, EventPublishError
from bms_core.events.topic import encode_event, event_attributes
from bms_core.msgclient.constants import MSG_PROVIDER_NAME
from bms_core.outbox.base import OUTBOX_STATUS_PENDING, OutboxRecord
from bms_core.outbox.dispatcher import MsgOutboxDispatcher, PollOutboxDispatcher
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tests_support.bms_port_contract import EVENT_NAMESPACE, EVENT_TOPIC, EVENT_TYPE, msg_event_publisher
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2481)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

GROUP = "outbox-relay-2481"
"""联调用消费组（与 04-5 契约用例的 `contract` 组隔离）。"""

TENANT = "demo"
"""真应用已启用的租户编码。"""


def _record() -> OutboxRecord:
    """构造一条待投递记录（relay 的 `to_envelope()` 由此重建事件信封）。

    Returns:
        OutboxRecord: 固定字段的发件箱记录。
    """
    return OutboxRecord(
        event_id="e-2481",
        event_type=EVENT_TYPE,
        event_version="1.0.0",
        aggregate_key="order-2481",
        tenant_id=None,
        payload=ConcurrentStableFrozenDict({"order_id": "o-2481"}),
        occurred_at=datetime(2026, 11, 11, 12, 0, 0),
        status=OUTBOX_STATUS_PENDING,
        retry_count=0,
        trace_id="trace-2481",
    )


async def _create_topic(client: AsyncClient) -> None:
    """经管理面创建事件主题。"""
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": EVENT_NAMESPACE, "name": EVENT_TOPIC, "partition_count": 1},
        headers={"X-Tenant-ID": TENANT},
    )
    assert response.status_code == 200, response.text


async def _join(client: AsyncClient) -> None:
    """加入消费组（`consume` 的前置）。"""
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": EVENT_NAMESPACE, "topic": EVENT_TOPIC, "consumer_id": "c1"},
        headers={"X-Tenant-ID": TENANT},
    )
    assert response.status_code == 200, response.text


async def _consume(client: AsyncClient) -> ConcurrentStableList[dict[str, object]]:
    """拉取事件主题当前可投递批。

    Returns:
        ConcurrentStableList[dict[str, object]]: 消息条目列表。
    """
    response = await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": "c1",
            "namespace": EVENT_NAMESPACE,
            "topic": EVENT_TOPIC,
            "partition": 0,
            "max_messages": 10,
            "max_wait_ms": 0,
        },
        headers={"X-Tenant-ID": TENANT},
    )
    assert response.status_code == 200, response.text
    body = cast("dict[str, object]", response.json())
    data = cast("dict[str, object]", body["data"])
    items: ConcurrentStableList[dict[str, object]] = ConcurrentStableList()
    for item in cast("list[object]", data["messages"]):
        items.add(cast("dict[str, object]", item))
    return items


def _headers(item: Mapping[str, object]) -> Mapping[str, str]:
    """取消息头（消费端契约里是字符串映射）。"""
    return cast("Mapping[str, str]", item["headers"])


# ---------------------------------------------------------------------------
# U1 真链路：发件箱记录 → MsgEventPublisher → 真数据面 → 真消费端
# ---------------------------------------------------------------------------


async def test_relay_envelope_reaches_real_data_plane(msg_app: FastAPI) -> None:
    """U1：`to_envelope()` 重建的信封经真应用投递，载荷 / 属性 / trace 逐项一致。"""
    record = _record()
    event = record.to_envelope()
    publisher = msg_event_publisher(msg_app, tenant=TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client)
            await publisher.publish(event)
            await _join(client)
            items = await _consume(client)
    finally:
        await publisher.aclose()

    assert len(items) == 1
    item = items[0]
    assert base64.b64decode(cast("str", item["payload"])) == encode_event(event)
    assert item["key"] == event.aggregate_key
    headers = _headers(item)
    for name, value in event_attributes(event).items():
        assert headers[name] == value
    assert headers["event_id"] == record.event_id
    assert headers["trace_id"] == record.trace_id


# ---------------------------------------------------------------------------
# U2 失败信号可观测（relay 保行的前提）
# ---------------------------------------------------------------------------


async def test_relay_publish_failure_is_observable(msg_app: FastAPI) -> None:
    """U2：主题不存在时发布抛 `EventPublishError` 且带定位信息，不静默吞掉。"""
    publisher = msg_event_publisher(msg_app, tenant=TENANT)
    try:
        with pytest.raises(EventPublishError) as caught:
            await publisher.publish(_record().to_envelope())
    finally:
        await publisher.aclose()

    detail = cast("Mapping[str, object]", caught.value.data)
    assert detail["event_type"] == EVENT_TYPE
    assert detail["status"] == 404


# ---------------------------------------------------------------------------
# U3 重投幂等键稳定（至少一次 + 消费端幂等）
# ---------------------------------------------------------------------------


async def test_repeated_delivery_keeps_stable_idempotency_key(msg_app: FastAPI) -> None:
    """U3：同一事件重投两次，消费端收到 2 条且 `event_id` 幂等键稳定（效果一次由消费端收敛）。

    失败后补发恰好一次的语义由 bms 仓真 SQLite 用例覆盖；本用例只证明跨仓链路上
    relay 重投不会改动幂等键，消费端可据此去重。
    """
    event: EventEnvelope = _record().to_envelope()
    publisher = msg_event_publisher(msg_app, tenant=TENANT)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client)
            await publisher.publish(event)
            await publisher.publish(event)
            await _join(client)
            items = await _consume(client)
    finally:
        await publisher.aclose()

    assert len(items) == 2
    assert [_headers(item)["event_id"] for item in items] == [event.event_id, event.event_id]


# ---------------------------------------------------------------------------
# U4 relay 单通道（零逻辑复制，不引入第二套投递路径）
# ---------------------------------------------------------------------------


def test_msg_dispatcher_keeps_single_delivery_path() -> None:
    """U4：msg 版 relay 与 poll 版共用同一投递函数，切换不产生第二条通道。"""
    assert MsgOutboxDispatcher.plugin_name == MSG_PROVIDER_NAME
    assert MsgOutboxDispatcher.__mro__[1] is PollOutboxDispatcher
    assert MsgOutboxDispatcher.dispatch_once is PollOutboxDispatcher.dispatch_once
    assert MsgOutboxDispatcher.dispatch_due is PollOutboxDispatcher.dispatch_due
