"""SSE 长驻流（任务 03-1；Kiwi 2420）：事件序列 / 保活 / 到期 / 成员撤销 / 取消释放。

两层验证：

- **生成器级**（`stream_events` 直调）：事件顺序与负载可逐条断言，时间用毫秒级 `StreamTuning` 注入，
  不必 sleep 秒级；
- **HTTP 级**（`GET /msg/subscribe`）：预检失败必须是**普通 JSON 错误**（还没进流，状态码可改），
  进流后是 `text/event-stream` 且以 `close(deadline)` 收尾。

取消/`RST_STREAM` 的真网络验证在 `test_frame_transport_h2c.py`（ASGI 直调没有连接概念）。
"""

import importlib
import json
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from pathlib import Path
from typing import cast

import pytest
from bms_core.api.base import APIRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.deps import get_stream_tuning
from bms_msg.api.frame.routes import stream_events
from bms_msg.broker.transport import StreamTuning
from bms_msg.errors import register_msg_error_mapper
from bms_msg.schemas.transport import SubscribeQuery
from bms_msg.tenant.context import CurrentTenant
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2420)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"
FAST = StreamTuning(poll_interval_ms=5, max_seconds=1, keepalive_ms=10)
"""毫秒级流参数：到期用例约 1s、保活用例约 10ms，用例不 sleep 秒级。"""


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。"""
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（错误响应文案与状态码来源）。"""
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用（`/api/v1` + 根路径 `/msg/*`）。"""
    return build_frame_app(session=db_session, tenant_source=tenant_source)


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。"""
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


async def _prepare(client: AsyncClient, *, partition_count: int = 1, join: bool = True, publish: bool = False) -> None:
    """建主题（+ 加入消费组 / 发布一条消息）。

    Args:
        client: 客户端。
        partition_count: 分区数。
        join: 是否加入消费组。
        publish: 是否发布一条消息。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": partition_count},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200
    if join:
        joined = await client.post(
            "/api/v1/msg/join",
            json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
            headers=DEMO_HEADERS,
        )
        assert joined.status_code == 200
    if publish:
        published = await client.post(
            "/api/v1/msg/publish",
            json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "key": "k", "payload": "cGF5bG9hZA=="},
            headers=DEMO_HEADERS,
        )
        assert published.status_code == 200


def _query(**overrides: object) -> SubscribeQuery:
    """构造长驻流查询参数（缺省一条合法的、单分区的订阅）。"""
    payload: dict[str, object] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_NAME,
        "group": GROUP,
        "consumer_id": CONSUMER,
        "partition": 0,
        "max_messages": 20,
        "max_seconds": 1,
    }
    payload.update(overrides)
    return SubscribeQuery.model_validate(payload)


def _events(app: FastAPI, tenant: CurrentTenant, query: SubscribeQuery, tuning: StreamTuning) -> AsyncGenerator[bytes]:
    """构造生成器（复用应用状态里的存储与登记表）。

    Args:
        app: 应用实例。
        tenant: 当前租户。
        query: 长驻流参数。
        tuning: 流参数（毫秒级）。

    Returns:
        AsyncGenerator[bytes, None]: 事件字节流（需要 `aclose()`，故不用更宽的 `AsyncIterator`）。
    """
    return stream_events(
        uow_factory=app.state.uow_factory,
        registry=app.state.stream_registry,
        store=app.state.message_store,
        offsets=app.state.offset_store,
        dead_letters=app.state.dead_letter_queue,
        state=app.state.delivery_state,
        groups=app.state.group_store,
        tenant=tenant,
        query=query,
        tuning=tuning,
    )


def _parse(chunk: bytes) -> tuple[str, dict[str, object] | None]:
    """解析一条 SSE 事件（注释行返回 `("comment", None)`）。

    Args:
        chunk: 生成器产出的一块字节。

    Returns:
        tuple[str, dict[str, object] | None]: 事件名与负载。
    """
    text = chunk.decode()
    if text.startswith(": "):
        return "comment", None
    lines = [line for line in text.strip().split("\n") if line]
    name = next(line[len("event: ") :] for line in lines if line.startswith("event: "))
    payload = next(line[len("data: ") :] for line in lines if line.startswith("data: "))
    return name, cast("dict[str, object]", json.loads(payload))


async def _next(agen: AsyncIterator[bytes]) -> tuple[str, dict[str, object] | None]:
    """取下一块并解析（同步推进生成器）。"""
    return _parse(await anext(agen))


async def _drain_until_close(
    agen: AsyncIterator[bytes], *, limit: int = 500
) -> ConcurrentStableList[tuple[str, dict[str, object] | None]]:
    """推进生成器直到 `close` 事件，并**继续推进到结束**（否则 `finally` 里的注销不执行）。

    Args:
        agen: 事件生成器。
        limit: 最大推进次数（防用例挂死）。

    Returns:
        ConcurrentStableList[tuple[str, dict[str, object] | None]]: 已解析事件（含 `close`）。

    Raises:
        AssertionError: 超过 `limit` 仍未收到 `close`。
    """
    events: ConcurrentStableList[tuple[str, dict[str, object] | None]] = ConcurrentStableList()
    seen_close = False
    for _ in range(limit):
        try:
            chunk = await anext(agen)
        except StopAsyncIteration:
            assert seen_close, "生成器在收到 close 之前就结束了"
            return events
        parsed = _parse(chunk)
        events.add(parsed)
        if parsed[0] == "close":
            seen_close = True
    raise AssertionError(f"推进 {limit} 次仍未收到 close 事件")


async def test_stream_emits_open_then_message_then_deadline(
    client: AsyncClient, api_app: FastAPI, current_tenant: CurrentTenant
) -> None:
    """事件序列：`open`（参数回显）→ `message`（与 consume 同结构）→ `close(deadline)`；登记表归零。"""
    await _prepare(client, publish=True)
    agen = _events(api_app, current_tenant, _query(), FAST)

    name, opened = await _next(agen)
    assert name == "open"
    assert opened is not None
    assert opened["max_seconds"] == 1
    assert opened["poll_interval_ms"] == 5
    assert str(opened["stream_id"]).startswith("s")

    name, message = await _next(agen)
    assert name == "message"
    assert message is not None
    assert message["offset"] == 0
    assert message["payload"] == "cGF5bG9hZA=="
    assert message["delivery_count"] == 1
    assert message["topic"] == TOPIC_NAME

    events = await _drain_until_close(agen)
    assert events[-1] == ("close", {"reason": "deadline"})
    assert api_app.state.stream_registry.active() == 0


async def test_stream_keeps_alive_when_idle(
    client: AsyncClient, api_app: FastAPI, current_tenant: CurrentTenant
) -> None:
    """无消息发注释行保活（客户端忽略），且不忙等（每次拉取之间按轮询间隔让出）。"""
    await _prepare(client)
    agen = _events(api_app, current_tenant, _query(), FAST)

    assert (await _next(agen))[0] == "open"
    name, payload = await _next(agen)

    assert (name, payload) == ("comment", None)
    await agen.aclose()
    assert api_app.state.stream_registry.active() == 0


async def test_stream_reports_member_lost_after_leave(
    client: AsyncClient, api_app: FastAPI, current_tenant: CurrentTenant
) -> None:
    """流存续期间成员离开：`error`（350013 / 350014）+ `close(member_lost)`，不静默继续推。"""
    await _prepare(client)
    agen = _events(api_app, current_tenant, _query(), FAST)
    assert (await _next(agen))[0] == "open"

    left = await client.post(
        "/api/v1/msg/leave",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert left.status_code == 200

    events = await _drain_until_close(agen)
    names = ConcurrentStableList(name for name, _ in events)
    error = next(payload for name, payload in events if name == "error")

    assert "error" in names
    assert error is not None
    assert error["code"] in (350013, 350014)
    assert events[-1] == ("close", {"reason": "member_lost"})
    assert api_app.state.stream_registry.active() == 0


async def test_stream_cancel_releases_registry_but_keeps_inflight(
    client: AsyncClient, api_app: FastAPI, current_tenant: CurrentTenant
) -> None:
    """取消（`aclose()`）只释放流级资源：登记表归零，但已投未 ack 的消息仍可 ack。"""
    await _prepare(client, publish=True)
    agen = _events(api_app, current_tenant, _query(), FAST)
    assert (await _next(agen))[0] == "open"
    name, message = await _next(agen)
    assert name == "message"
    assert api_app.state.stream_registry.active() == 1

    await agen.aclose()

    assert api_app.state.stream_registry.active() == 0
    assert message is not None
    acked = await client.post(
        "/api/v1/msg/ack",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "offset": message["offset"],
        },
        headers=DEMO_HEADERS,
    )
    assert acked.status_code == 200, "取消流不得丢弃消息状态（在途必须仍可确认）"


async def test_subscribe_rejects_before_streaming(client: AsyncClient) -> None:
    """预检失败是普通 JSON 错误（还没进流）：主题不存在 350001、未入组 350013、参数越界 10001。"""
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200
    params = {
        "namespace": NAMESPACE,
        "topic": "no.such.topic",
        "group": GROUP,
        "consumer_id": CONSUMER,
        "partition": 0,
    }

    missing_topic = await client.get("/msg/subscribe", params=params, headers=DEMO_HEADERS)
    not_joined = await client.get(
        "/msg/subscribe",
        params={**params, "topic": TOPIC_NAME},
        headers=DEMO_HEADERS,
    )
    bad_window = await client.get(
        "/msg/subscribe",
        params={**params, "topic": TOPIC_NAME, "max_seconds": 601},
        headers=DEMO_HEADERS,
    )

    assert (missing_topic.status_code, missing_topic.json()["code"]) == (404, 350001)
    assert (not_joined.status_code, not_joined.json()["code"]) == (404, 350013)
    assert bad_window.json()["code"] == 10001, "参数越界属契约层（10001），不占产品码"


async def test_subscribe_over_http_is_event_stream(client: AsyncClient, api_app: FastAPI) -> None:
    """HTTP 表面：200 + `text/event-stream`，内容以 `event: open` 起、`close(deadline)` 收尾。"""
    await _prepare(client)
    api_app.dependency_overrides[get_stream_tuning] = lambda: FAST

    response: Response = await client.get(
        "/msg/subscribe",
        params={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "group": GROUP,
            "consumer_id": CONSUMER,
            "partition": 0,
            "max_seconds": 1,
        },
        headers=DEMO_HEADERS,
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    text = response.text
    assert text.startswith("event: open")
    assert text.rstrip().endswith('data: {"reason":"deadline"}')
    assert api_app.state.stream_registry.active() == 0


def test_build_frame_app_mounts_frame_router(api_app: FastAPI) -> None:
    """装配护栏：根路径聚合已挂（`/msg/subscribe` 在契约里），且路由类型为聚合路由。"""
    assert "/msg/subscribe" in api_app.openapi()["paths"]
    assert isinstance(api_app.router, APIRouter)
