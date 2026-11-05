"""帧式订阅推送与长驻流背压参数（任务 03-5；Kiwi 2425）。

覆盖：

- `POST /msg/subscribe` 帧流：`open` 帧（4+RESPONSE）→ 逐条 `MESSAGE` 帧（原始字节 + 三字段）→ EOF；
  预检失败 / 类型不符 / 带 payload / 缺必填头 / 值形态非法 / 租户不一致各自的错误形态；
- `GET /msg/subscribe` 的 `open` 事件带上 `max_wait_ms` / 预取 / 两个 inflight 阈值；
- 预取开启后仍能收全（预取只改变「什么时候拉」，不改变「推什么」），且流被提前关闭时预取任务被取消；
- 传输层暂停计数（`window_stall`）与「等待者超限」在流里的表现；
- `_wait_timeout_ms` 取三者最小值（纯函数）。
"""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from typing import cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_stream_tuning, get_waiter
from bms_msg.api.frame.routes import stream_events, wait_timeout_ms
from bms_msg.broker.backpressure import default_backpressure_tuning
from bms_msg.broker.transport import StreamTuning, connection_meta
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.protocol.frame import FrameType, decode_frames
from bms_msg.schemas.transport import SubscribeQuery
from bms_msg.tenant.context import CurrentTenant
from tests_support.frame_client import frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2425)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
TOPIC_NAME = "audit.stream"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
FAST = StreamTuning(poll_interval_ms=5, max_seconds=1, keepalive_ms=20, max_wait_ms=1_000, prefetch=0)
"""毫秒级流参数：到期 1s（契约下界），兜底轮询 5ms，保活 20ms。"""

_TEST_CONNECTION_KEY = "demo|127.0.0.1:7"


async def _create_topic(client: AsyncClient) -> None:
    """建一个单分区主题。

    Args:
        client: 客户端。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert created.status_code == 200, created.text


async def _join(client: AsyncClient) -> None:
    """成员加入组。

    Args:
        client: 客户端。
    """
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert joined.status_code == 200, joined.text


async def _publish(client: AsyncClient, payload: bytes = b"x", *, content_type: str | None = None) -> None:
    """发布一条消息。

    Args:
        client: 客户端。
        payload: 消息体。
        content_type: 可选内容类型（验帧头三字段回显）。
    """
    body: dict[str, object] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_NAME,
        "payload": base64.b64encode(payload).decode("ascii"),
    }
    if content_type is not None:
        body["content_type"] = content_type
    published = await client.post("/api/v1/msg/publish", json=body, headers=DEMO_HEADERS)
    assert published.status_code == 200, published.text


def _subscribe_header(**overrides: object) -> dict[str, object]:
    """构造订阅帧头（必填项齐备）。

    Args:
        overrides: 覆盖项。

    Returns:
        dict[str, object]: 帧头。
    """
    extra: dict[str, object] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_NAME,
        "group": GROUP,
        "consumer_id": CONSUMER,
        "partition": 0,
        "max_seconds": 1,
    }
    extra.update(overrides)
    return header(extra=extra)


async def _post_subscribe(client: AsyncClient, request_header: dict[str, object]) -> Response:
    """发一次帧流订阅请求（响应体在流结束后一次性返回）。

    Args:
        client: 客户端。
        request_header: 帧头。

    Returns:
        Response: 响应。
    """
    body = frame_bytes(int(FrameType.SUBSCRIBE), request_header)
    return await post_frame(client, "/msg/subscribe", body)


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """ASGI 客户端（流参数与连接参数都调快）。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    msg_app.dependency_overrides[get_stream_tuning] = lambda: FAST
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as opened:
        yield opened


async def test_frame_stream_pushes_open_then_messages(msg_app: FastAPI, client: AsyncClient) -> None:
    """帧流：open 帧带参数，message 帧带原始字节与三字段，正文 EOF 即结束。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client, b"first", content_type="application/json")
    await _publish(client, b"second")

    response = await _post_subscribe(client, _subscribe_header())

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-msg-frame")
    batch = decode_frames(response.content)
    frames = list(batch.frames)
    assert len(frames) == 3
    assert frames[0].frame_type == int(FrameType.SUBSCRIBE)
    assert frames[0].header["request_id"] == "r1"
    assert str(frames[0].header["stream_id"]).startswith("s")
    assert frames[0].header["max_wait_ms"] == FAST.max_wait_ms
    assert frames[0].header["prefetch"] == 0
    assert frames[0].header["inflight_messages"] > 0

    assert [frame.frame_type for frame in frames[1:]] == [int(FrameType.MESSAGE), int(FrameType.MESSAGE)]
    assert bytes(frames[1].payload) == b"first"
    assert frames[1].header["content_type"] == "application/json"
    assert frames[1].header["offset"] == 0
    assert frames[2].header["delivery_count"] == 1


async def test_frame_stream_preflight_failure_is_single_error_frame(msg_app: FastAPI, client: AsyncClient) -> None:
    """未加入组 → 单帧 `error`（350013），不进入流。"""
    await _create_topic(client)

    response = await _post_subscribe(client, _subscribe_header())

    batch = decode_frames(response.content)
    frames = list(batch.frames)
    assert len(frames) == 1
    assert frames[0].frame_type == int(FrameType.ERROR)
    assert frames[0].header["code"] == 350013
    assert msg_app.state.stream_registry.active() == 0


async def test_frame_stream_rejects_wrong_type(msg_app: FastAPI, client: AsyncClient) -> None:
    """类型不是订阅帧 → `350018`。"""
    await _create_topic(client)
    body = frame_bytes(int(FrameType.CONSUME), _subscribe_header())

    response = await post_frame(client, "/msg/subscribe", body)

    assert decode_frames(response.content).frames[0].header["code"] == 350018


async def test_frame_stream_rejects_payload(msg_app: FastAPI, client: AsyncClient) -> None:
    """订阅帧不得携带 payload（参数只走 header）→ `350018`。"""
    await _create_topic(client)
    body = frame_bytes(int(FrameType.SUBSCRIBE), _subscribe_header(), b"x")

    response = await post_frame(client, "/msg/subscribe", body)

    assert decode_frames(response.content).frames[0].header["code"] == 350018


async def test_frame_stream_requires_headers(msg_app: FastAPI, client: AsyncClient) -> None:
    """缺必填头 → `350018`（协议层：键都不到）。"""
    await _create_topic(client)
    incomplete = header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME})

    response = await post_frame(client, "/msg/subscribe", frame_bytes(int(FrameType.SUBSCRIBE), incomplete))

    assert decode_frames(response.content).frames[0].header["code"] == 350018


async def test_frame_stream_rejects_bad_value_shape(msg_app: FastAPI, client: AsyncClient) -> None:
    """键在而值形态非法 → 契约层 `10001`（与数据面闸门口径一致）。"""
    await _create_topic(client)

    response = await _post_subscribe(client, _subscribe_header(max_seconds=0))

    assert decode_frames(response.content).frames[0].header["code"] == 10001


async def test_frame_stream_rejects_foreign_tenant(msg_app: FastAPI, client: AsyncClient) -> None:
    """帧头 tenant 与请求身份不一致 → `350012`（帧不是第二个鉴权入口）。"""
    await _create_topic(client)

    response = await _post_subscribe(client, _subscribe_header(tenant="other"))

    assert decode_frames(response.content).frames[0].header["code"] == 350012


async def test_sse_open_event_carries_backpressure_params(msg_app: FastAPI, client: AsyncClient) -> None:
    """SSE `open` 事件把等待与背压参数一次给全（客户端不必查常量）。"""
    await _create_topic(client)
    await _join(client)

    response = await client.get(
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
    first = json.loads(response.text.split("data: ", 1)[1].split("\n", 1)[0])
    assert first["max_wait_ms"] == FAST.max_wait_ms
    assert first["prefetch"] == 0
    assert first["inflight_messages"] > 0
    assert first["inflight_bytes"] > 0


async def test_prefetch_still_delivers_everything(msg_app: FastAPI, client: AsyncClient) -> None:
    """预取开启仍收全 3 条。

    注意预取**不能**越过分区内停等：头未 ack 时下一轮拉取本就为空（顺序性优先于吞吐），
    预取只是把「下一次拉取」提前发起，故这里验「不丢、不重、顺序不变」。
    """
    msg_app.dependency_overrides[get_stream_tuning] = lambda: StreamTuning(
        poll_interval_ms=5, max_seconds=1, keepalive_ms=20, max_wait_ms=1_000, prefetch=1
    )
    await _create_topic(client)
    await _join(client)
    for index in range(3):
        await _publish(client, f"m{index}".encode())

    response = await _post_subscribe(client, _subscribe_header())

    frames = [frame for frame in decode_frames(response.content).frames if frame.frame_type == int(FrameType.MESSAGE)]
    assert [bytes(frame.payload) for frame in frames] == [b"m0", b"m1", b"m2"]


async def test_window_stall_is_counted(msg_app: FastAPI, client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """两次推进间隔超过阈值即记一次传输层暂停（阈值调到 0 以在 ASGI 下稳定命中）。"""
    monkeypatch.setattr("bms_msg.api.frame.routes.WINDOW_STALL_THRESHOLD_MS", 0)
    await _create_topic(client)
    await _join(client)
    await _publish(client)

    await _post_subscribe(client, _subscribe_header())

    snapshot = msg_app.state.backpressure_metrics.snapshot()
    assert snapshot["msg_window_stall_total"]["__total__"] >= 1


async def test_stream_counts_rejected_waiters(msg_app: FastAPI, client: AsyncClient) -> None:
    """等待者超限时流不报错，只退一个兜底间隔再试（到期正常 close）。"""
    waiter = DeliveryWaiter(max_waiters=1)
    msg_app.dependency_overrides[get_waiter] = lambda: waiter
    await _create_topic(client)
    await _join(client)
    holder = asyncio.create_task(waiter.wait((404, 0), version=0, timeout_ms=5_000))
    await asyncio.sleep(0)

    response = await _post_subscribe(client, _subscribe_header())

    assert [frame.frame_type for frame in decode_frames(response.content).frames] == [int(FrameType.SUBSCRIBE)]
    snapshot = msg_app.state.backpressure_metrics.snapshot()
    assert snapshot["msg_long_poll_rejected_total"] >= 1
    waiter.notify((404, 0))
    assert await holder is not None


async def test_direct_generator_resolves_ids_and_cancels_prefetch(
    msg_app: FastAPI, current_tenant: CurrentTenant, client: AsyncClient
) -> None:
    """直调生成器（不传 topic_id / partition_no）走兜底解析；提前关闭时预取任务被取消。"""
    msg_app.dependency_overrides[get_stream_tuning] = lambda: StreamTuning(
        poll_interval_ms=5, max_seconds=1, keepalive_ms=20, max_wait_ms=1_000, prefetch=1
    )
    await _create_topic(client)
    await _join(client)
    for index in range(2):
        await _publish(client, f"p{index}".encode())
    msg_app.state.connection_registry.touch(
        key=_TEST_CONNECTION_KEY,
        meta=connection_meta({"http_version": "2", "scheme": "http", "client": ("127.0.0.1", 7)}),
        tenant=current_tenant.code,
        now=0.0,
        wall=0.0,
        heartbeat=False,
    )

    agen = stream_events(
        uow_factory=msg_app.state.uow_factory,
        registry=msg_app.state.stream_registry,
        connections=msg_app.state.connection_registry,
        store=msg_app.state.message_store,
        offsets=msg_app.state.offset_store,
        dead_letters=msg_app.state.dead_letter_queue,
        state=msg_app.state.delivery_state,
        groups=msg_app.state.group_store,
        tenant=current_tenant,
        query=SubscribeQuery.model_validate(
            {
                "namespace": NAMESPACE,
                "topic": TOPIC_NAME,
                "group": GROUP,
                "consumer_id": CONSUMER,
                "partition": 0,
                "max_messages": 1,
                "max_seconds": 1,
            }
        ),
        tuning=cast("StreamTuning", msg_app.dependency_overrides[get_stream_tuning]()),
        connection_key=_TEST_CONNECTION_KEY,
        waiter=msg_app.state.waiter,
        backpressure=default_backpressure_tuning(),
        metrics=msg_app.state.backpressure_metrics,
        cache=msg_app.state.topic_cache,
    )

    first = await anext(agen)
    assert b"event: open" in first
    second = await anext(agen)
    assert b"event: message" in second
    await agen.aclose()

    assert msg_app.state.stream_registry.active() == 0


def test_wait_timeout_is_min_of_three() -> None:
    """等待时长取「长轮询上限 / 剩余总时长 / 距上次保活」的最小值，且至少 1ms。"""
    tuning = StreamTuning(poll_interval_ms=5, max_seconds=60, keepalive_ms=1_000, max_wait_ms=30_000, prefetch=0)

    # 保活最先到期
    assert wait_timeout_ms(now=100.0, deadline=200.0, last_emit=99.5, tuning=tuning) == 500
    # 长轮询上限最小
    assert wait_timeout_ms(now=100.0, deadline=200.0, last_emit=100.0, tuning=tuning) == 1_000
    # 剩余总时长最小（且不为负）
    assert wait_timeout_ms(now=100.0, deadline=100.01, last_emit=100.0, tuning=tuning) == 10
    assert wait_timeout_ms(now=100.0, deadline=100.0, last_emit=100.0, tuning=tuning) == 1


async def test_stream_falls_back_to_polling_when_wait_disabled(msg_app: FastAPI, client: AsyncClient) -> None:
    """`max_wait_ms=0` 时回落固定间隔轮询（保留 03-5 之前的等待策略，供排障与对照）。"""
    msg_app.dependency_overrides[get_stream_tuning] = lambda: StreamTuning(
        poll_interval_ms=5, max_seconds=1, keepalive_ms=20, max_wait_ms=0, prefetch=0
    )
    await _create_topic(client)
    await _join(client)

    response = await client.get(
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
    assert "event: close" in response.text
    assert '"reason":"deadline"' in response.text


async def test_frame_stream_emits_error_when_member_lost(msg_app: FastAPI, client: AsyncClient) -> None:
    """流内成员被撤销：`error` 帧（同码 350014）随后 EOF，不静默继续推。"""
    await _create_topic(client)
    await _join(client)

    task = asyncio.create_task(_post_subscribe(client, _subscribe_header()))
    await asyncio.sleep(0.05)
    left = await client.post(
        "/api/v1/msg/leave",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert left.status_code == 200
    await _publish(client)
    response = await asyncio.wait_for(task, timeout=5)

    frames = list(decode_frames(response.content).frames)
    kinds = [frame.frame_type for frame in frames]
    assert int(FrameType.ERROR) in kinds
    error = frames[kinds.index(int(FrameType.ERROR))]
    assert error.header["code"] == 350014
    assert msg_app.state.stream_registry.active() == 0


async def test_frame_stream_requires_non_empty_request_id(msg_app: FastAPI, client: AsyncClient) -> None:
    """`request_id` 存在但为空 → `350018`（键在而值为空，属协议层必填）。"""
    await _create_topic(client)
    request_header = _subscribe_header()
    request_header["request_id"] = ""

    response = await post_frame(client, "/msg/subscribe", frame_bytes(int(FrameType.SUBSCRIBE), request_header))

    assert decode_frames(response.content).frames[0].header["code"] == 350018


async def test_frame_stream_releases_slot_on_unexpected_error(
    msg_app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    """预检抛非业务异常：仍归还长驻流名额（否则一次故障永久占一个名额）。"""

    async def _boom(**_kwargs: object) -> tuple[int, int]:
        """模拟预检期的未预期异常。

        Args:
            **_kwargs: 忽略所有入参。

        Returns:
            tuple[int, int]: 不会返回。

        Raises:
            RuntimeError: 恒抛。
        """
        raise RuntimeError("boom")

    monkeypatch.setattr("bms_msg.api.frame.routes._preflight", _boom)
    # `raise_app_exceptions=False`：让未捕获异常走 500 响应，而不是从 ASGI 传输层冒出来
    transport = ASGITransport(app=msg_app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as failing:
        await _create_topic(failing)
        response = await failing.post(
            "/msg/subscribe",
            content=frame_bytes(int(FrameType.SUBSCRIBE), _subscribe_header()),
            headers=DEMO_HEADERS,
        )

    assert response.status_code == 500
    assert [record.streams for record in msg_app.state.connection_registry.records()] == [0]
