"""bms 端口适配的两仓契约用例（07B `06_01` 步 3 / 04-5；Kiwi 2442）。

**这条用例为什么在 msg 仓**：`bms-core` 是 msg 的路径依赖，只有这边能同时 import 两侧——真 msg
应用（真路由 → 真帧 → 真处理器）+ bms 适配层 + bms 真实业务函数。bms 仓 CI 不 clone msg，那边
只能做替身级演练（`tests/msgclient/adapter/test_rollback.py`）。

六类 RPC 契约（夹具在 `tests_support/bms_port_contract.py`）：
请求 / 响应走真应用；错误五类走脚本帧（要精确控制 `category`）；timeout 与 cancel 走脚本传输
（真应用的这两条路径由 04-2 / 04-3 用例覆盖，这里只验「到 bms 端口后不外泄」）。

**事件发布真链路（04-5 收口补充）**：bms `MsgEventPublisher` 经 `httpx.ASGITransport` 调真实
msg 应用 `/api/v1/msg/publish`（真路由 → 真数据面），再由真消费端拉取比对——补上 bms 侧
`MockTransport` 覆盖不到的那一半。

**目标方法的处理器是测试替身**：`platform` 服务本体不在场，`platform.modules.snapshot` 的语义由
`TrackingHandler` 等价给出（返回同一份统一响应包裹）。验收的是「真实调用点经真帧往返仍成立」，
不是「platform 服务正确」。
"""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Mapping
from datetime import datetime
from typing import cast

import pytest
from bms_core.catalog.base import fetch_catalog_snapshot
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import BizError, ServiceUnavailableError
from bms_core.events.base import EventEnvelope, EventPublishError
from bms_core.events.topic import encode_event, event_attributes
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.msgclient.transport import RpcHttpRequest, RpcHttpResponse
from bms_core.servicecall.base import ServiceRequest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from tests_support.bms_port_contract import (
    BASE_URL,
    ERROR_CASES,
    EVENT_NAMESPACE,
    EVENT_TOPIC,
    EVENT_TYPE,
    TARGET_METHOD,
    TARGET_SERVICE,
    ErrorCase,
    TrackingHandler,
    cancel_transport,
    error_response,
    msg_client,
    msg_event_publisher,
    snapshot_request,
    timeout_transport,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_transport import AsgiRpcTransport, ScriptedRpcTransport, TransportHandler

pytestmark = pytest.mark.kiwi_id(2442)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]


def _register_target(app: FastAPI, *, method: str = TARGET_METHOD, idempotent: bool = True) -> TrackingHandler:
    """把目标方法登记进应用注册表。

    Args:
        app: 最小应用（`build_frame_app` 产物）。
        method: 方法名（缺省为真实调用点）。
        idempotent: 幂等声明（服务端只对「非幂等 + 带键」启用去重）。

    Returns:
        TrackingHandler: 记录调用次数的处理器。
    """
    handler = TrackingHandler()
    app.state.rpc_registry.register(TARGET_SERVICE, method, handler, idempotent=idempotent)
    return handler


async def test_real_call_point_over_real_frames(msg_app: FastAPI) -> None:
    """真实调用点：`fetch_catalog_snapshot` 经真 msg 应用（真帧）拿到与 http 同构的结果。"""
    handler = _register_target(msg_app)
    client = msg_client(AsgiRpcTransport(msg_app))
    try:
        records = await fetch_catalog_snapshot(client)
    finally:
        await client.aclose()

    assert [record.module_key for record in records] == ["platform"]
    assert handler.calls == [(TARGET_SERVICE, TARGET_METHOD, b"")]


async def test_same_idempotency_key_executes_once(msg_app: FastAPI) -> None:
    """幂等键：非幂等方法带同一个键重复调用，服务端只执行一次处理器（重试不产生第二次副作用）。

    **为什么另立一个写方法**：真实调用点 `modules.snapshot` 是 GET、注册时声明幂等——服务端对
    幂等方法**不去重**（重复执行无副作用，去重表没必要留记录，见 `api/rpc/idempotency.py`）；
    要验去重，必须用「非幂等 + 带键」这一条真实路径。
    """
    write_method = "modules.create"
    handler = _register_target(msg_app, method=write_method, idempotent=False)
    async with RpcClient(
        StaticInstanceSelector([BASE_URL]),
        tenant="demo",
        transport=AsgiRpcTransport(msg_app),
    ) as rpc:
        first = await rpc.call(TARGET_SERVICE, write_method, b"{}", idempotency_key="k-2442")
        second = await rpc.call(TARGET_SERVICE, write_method, b"{}", idempotency_key="k-2442")

    assert len(handler.calls) == 1
    assert first.payload == second.payload


@pytest.mark.parametrize("case", ERROR_CASES, ids=[f"{case.code}-{case.category}" for case in ERROR_CASES])
async def test_error_frame_maps_to_expected_bms_exception(case: ErrorCase) -> None:
    """错误帧按 `category` 分派到 bms 异常，原始码进 `data`（类别决定类，码值不改语义）。"""
    client = msg_client(ScriptedRpcTransport(handler=_fixed(error_response(case))))
    with pytest.raises(case.expected) as caught:
        await client.call(snapshot_request())

    error = cast("BizError", caught.value)
    detail = cast("Mapping[str, object]", error.data)
    assert detail["code"] == case.code
    assert detail["category"] == case.category
    assert error.code == case.bms_code


async def test_timeout_is_unknown_outcome_not_failure() -> None:
    """timeout：映射为 `ServiceUnavailableError` 且 `data.outcome=unknown`（「结果未知」不是「失败」）。"""
    client = msg_client(timeout_transport())
    with pytest.raises(ServiceUnavailableError) as caught:
        await client.call(snapshot_request())

    detail = cast("Mapping[str, object]", caught.value.data)
    assert detail["code"] == 350004
    assert detail["outcome"] == "unknown"


async def test_cancel_passes_through_untouched() -> None:
    """cancel：`CancelledError` 原样穿透（不重试、不映射成业务异常）。"""
    client = msg_client(cancel_transport())
    with pytest.raises(asyncio.CancelledError):
        await client.call(snapshot_request())


async def test_unregistered_method_over_real_app_is_business_reject(msg_app: FastAPI) -> None:
    """真实应用上打到未注册方法：`350023` 业务拒绝 → `BizError`（保留原码，不吞成不可用）。"""
    client = msg_client(AsgiRpcTransport(msg_app))
    absent = ServiceRequest(service=TARGET_SERVICE, method="GET", path="/api/v1/modules/absent")
    try:
        with pytest.raises(BizError) as caught:
            await client.call(absent)
    finally:
        await client.aclose()

    assert caught.value.code == 350023


async def test_event_publish_reaches_real_data_plane(msg_app: FastAPI) -> None:
    """事件发布：bms 发布器 → 真 msg 数据面 → 真消费端，载荷与属性逐项一致。"""
    event = _event()
    publisher = msg_event_publisher(msg_app, tenant="demo")
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
    headers = cast("Mapping[str, str]", item["headers"])
    for name, value in event_attributes(event).items():
        assert headers[name] == value


async def test_event_publish_missing_topic_is_publish_error(msg_app: FastAPI) -> None:
    """未创建主题：msg 返回非 200，发布器抛 `EventPublishError` 并保留 topic / 状态。"""
    publisher = msg_event_publisher(msg_app, tenant="demo")
    try:
        with pytest.raises(EventPublishError) as caught:
            await publisher.publish(_event())
    finally:
        await publisher.aclose()

    detail = cast("Mapping[str, object]", caught.value.data)
    assert detail["event_type"] == EVENT_TYPE
    assert detail["topic"] == EVENT_TOPIC
    assert detail["status"] == 404


async def test_event_publish_resolves_event_tenant_id(msg_app: FastAPI) -> None:
    """事件信封的 `tenant_id`（雪花 id）须解析到对应租户：`other` 租户消费端可见。"""
    event = _event(tenant_id="8002")
    publisher = msg_event_publisher(msg_app)
    try:
        async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
            await _create_topic(client, tenant="other")
            await publisher.publish(event)
            await _join(client, tenant="other")
            items = await _consume(client, tenant="other")
    finally:
        await publisher.aclose()

    assert len(items) == 1


def _event(*, tenant_id: str | None = None) -> EventEnvelope:
    """构造一条固定字段的事件信封（时间钉死，载荷可逐字节比对）。

    Args:
        tenant_id: 租户主键（雪花 id 字符串；None 表示无租户事件）。

    Returns:
        EventEnvelope: 事件信封。
    """
    return EventEnvelope(
        event_type=EVENT_TYPE,
        payload=ConcurrentStableDict({"order_id": "o-2442"}),
        tenant_id=tenant_id,
        trace_id="trace-2442",
        event_id="e-2442",
        occurred_at=datetime(2026, 11, 6, 12, 0, 0),
        aggregate_key="order-2442",
    )


async def _create_topic(client: AsyncClient, *, tenant: str = "demo", name: str = EVENT_TOPIC) -> None:
    """经管理面创建事件主题。

    Args:
        client: ASGI 客户端。
        tenant: 租户编码（`X-Tenant-ID`）。
        name: 主题名。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": EVENT_NAMESPACE, "name": name, "partition_count": 1},
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text


async def _join(client: AsyncClient, *, tenant: str = "demo") -> None:
    """加入事件消费组（`consume` 的前置）。

    Args:
        client: ASGI 客户端。
        tenant: 租户编码（`X-Tenant-ID`）。
    """
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": "contract", "namespace": EVENT_NAMESPACE, "topic": EVENT_TOPIC, "consumer_id": "c1"},
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text


async def _consume(client: AsyncClient, *, tenant: str = "demo") -> list[dict[str, object]]:
    """拉取事件主题的消息（不等待，直接返回当前可投递批）。

    Args:
        client: ASGI 客户端。
        tenant: 租户编码（`X-Tenant-ID`）。

    Returns:
        list[dict[str, object]]: 消息条目列表。
    """
    response = await client.post(
        "/api/v1/msg/consume",
        json={
            "group": "contract",
            "consumer_id": "c1",
            "namespace": EVENT_NAMESPACE,
            "topic": EVENT_TOPIC,
            "partition": 0,
            "max_messages": 10,
            "max_wait_ms": 0,
        },
        headers={"X-Tenant-ID": tenant},
    )
    assert response.status_code == 200, response.text
    body = cast("dict[str, object]", response.json())
    data = cast("dict[str, object]", body["data"])
    return [cast("dict[str, object]", item) for item in cast("list[object]", data["messages"])]


def _fixed(response: RpcHttpResponse) -> TransportHandler:
    """构造「恒定返回给定响应」的一元处理器。

    Args:
        response: 要返回的传输响应。

    Returns:
        一元处理器（`RpcHttpRequest` → `RpcHttpResponse`）。
    """

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """忽略请求，返回固定响应。"""
        del request
        return response

    return handler
