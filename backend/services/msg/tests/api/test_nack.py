"""拒绝与死信（任务 03-3；Kiwi 2423）：校验失败的上报通道。

覆盖（详细设计 §5）：

- **转入**：JSON / 帧两种形态的 `nack` 都把消息转死信并前移位点，其后 offset 可继续投；
- **幂等**：同一 offset 重复上报不重复入队（至少一次语义下客户端会重试）；
- **边界**：越界 `350002`、未投递 `350003`、非成员 `350013`、原因非枚举 `10001`；
- **死信内容**：原因与字段级详情原样保存，06 域可直接筛选展示。

投递路径一行未改：`nack` 只是把「消费端拒绝」接到 01-2 已有的「转死信 + 位点前移」上。
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import (
    CONSUMER,
    DEMO_HEADERS,
    GROUP,
    NAMESPACE,
    TOPIC_NAME,
    create_topic,
    frame_bytes,
    header,
    join_group,
    post_frame,
)

pytestmark = pytest.mark.kiwi_id(2423)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并确保占位模块门禁已导入。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: Any, tenant_source: object) -> FastAPI:
    """最小应用（业务路由 + `/msg/*` 聚合 + 帧闸门）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    return build_frame_app(session=db_session, tenant_source=tenant_source)


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test")


async def _consume(client: AsyncClient, *, max_messages: int = 20) -> Any:
    """拉取一批（帧形态，便于读 payload 段）。

    Args:
        client: 客户端。
        max_messages: 本次最多拉取条数。

    Returns:
        httpx.Response: 响应。
    """
    request_header = header(
        extra={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "group": GROUP,
            "consumer_id": CONSUMER,
            "partition": 0,
            "max_messages": max_messages,
            "max_wait_ms": 0,
        }
    )
    return await post_frame(client, "/msg/consume", frame_bytes(int(FrameType.CONSUME), request_header))


def _nack_body(*, offset: int, reason: str = "schema_invalid", detail: dict[str, str] | None = None) -> dict[str, Any]:
    """构造 nack 请求体。

    Args:
        offset: 待拒绝的分区内序号。
        reason: 拒绝原因。
        detail: 字段级详情。

    Returns:
        dict[str, Any]: 请求体。
    """
    body: dict[str, Any] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_NAME,
        "group": GROUP,
        "consumer_id": CONSUMER,
        "partition": 0,
        "offset": offset,
        "reason": reason,
    }
    if detail is not None:
        body["detail"] = detail
    return body


async def _nack(client: AsyncClient, **kwargs: Any) -> Response:
    """发一个 nack 请求（JSON 形态）。

    Args:
        client: 客户端。
        kwargs: 传给 `_nack_body` 的参数。

    Returns:
        httpx.Response: 响应。
    """
    return await client.post("/api/v1/msg/nack", json=_nack_body(**kwargs), headers=DEMO_HEADERS)


async def test_nack_moves_message_to_dead_letter(client: AsyncClient, api_app: FastAPI) -> None:
    """JSON nack：转死信 + 位点前移，其后消费不再拿到这条。"""
    await create_topic(client)
    await join_group(client)
    await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "b2s="},
        headers=DEMO_HEADERS,
    )
    consumed = await _consume(client)
    assert decode_frame(consumed.content).header["count"] == 1

    response = await _nack(client, offset=0, detail={"field": "order_id", "expected": "str", "actual": "int"})

    assert response.status_code == 200
    assert response.json()["data"]["committed_offset"] == 1
    letters = await api_app.state.dead_letter_queue.list_all()
    assert len(letters) == 1
    assert letters[0].offset == 0
    assert str(letters[0].reason) == "schema_invalid"
    assert letters[0].detail["field"] == "order_id"

    again = await _consume(client)
    frame = decode_frame(again.content)
    assert frame.header["count"] == 0
    assert bytes(frame.payload) == b""


async def test_nack_frame_returns_result_frame(client: AsyncClient) -> None:
    """帧形态 nack：结果帧 `type=5` + response 位 + committed_offset。"""
    await create_topic(client)
    await join_group(client)
    await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "b2s="},
        headers=DEMO_HEADERS,
    )
    await _consume(client)

    request_header = header(
        extra={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "group": GROUP,
            "consumer_id": CONSUMER,
            "partition": 0,
            "offset": 0,
            "reason": "processing_failed",
            "detail": {"reason": "不可重试"},
        }
    )
    response = await post_frame(client, "/msg/nack", frame_bytes(int(FrameType.NACK), request_header))

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.NACK)
    assert frame.flags & FrameFlags.RESPONSE
    assert frame.header["request_id"] == "r1"
    assert frame.header["committed_offset"] == 1


async def test_nack_is_idempotent(client: AsyncClient, api_app: FastAPI) -> None:
    """同一 offset 重复上报：不重复入队、位点不变（客户端重试安全）。"""
    await create_topic(client)
    await join_group(client)
    await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "b2s="},
        headers=DEMO_HEADERS,
    )
    await _consume(client)

    first = await _nack(client, offset=0)
    second = await _nack(client, offset=0)

    assert first.json()["data"]["committed_offset"] == 1
    assert second.json()["data"]["committed_offset"] == 1
    letters = await api_app.state.dead_letter_queue.list_all()
    assert len(letters) == 1


async def test_nack_offset_not_in_flight(client: AsyncClient) -> None:
    """未投递的 offset（还在 LEO 之内）→ `350003`。"""
    await create_topic(client)
    await join_group(client)
    for _ in range(2):
        await client.post(
            "/api/v1/msg/publish",
            json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "b2s="},
            headers=DEMO_HEADERS,
        )
    await _consume(client, max_messages=1)

    response = await _nack(client, offset=1)

    assert response.json()["code"] == 350003


async def test_nack_offset_beyond_leo(client: AsyncClient) -> None:
    """位点越界（`>= LEO`）→ `350002`（与 ack 同码）。"""
    await create_topic(client)
    await join_group(client)

    response = await _nack(client, offset=99)

    assert response.json()["code"] == 350002


async def test_nack_reason_must_be_client_side(client: AsyncClient) -> None:
    """`delivery_exhausted` 由 broker 独占，外部指定即契约拒绝 `10001`。"""
    await create_topic(client)
    await join_group(client)

    response = await _nack(client, offset=0, reason="delivery_exhausted")

    assert response.json()["code"] == 10001


async def test_nack_requires_membership(client: AsyncClient) -> None:
    """未加入消费组 → `350013`（成员制与 ack 同口径）。"""
    await create_topic(client)

    response = await _nack(client, offset=0)

    assert response.json()["code"] == 350013


async def test_nack_permission_denied(client: AsyncClient, api_app: FastAPI) -> None:
    """无权限 → 统一 403 JSON（权限码复用 `msg:consume`）。"""

    class _Deny(BasePermissionChecker):
        """权限桩：一律拒绝。"""

        def check(self, code: str) -> bool:
            """恒定拒绝。

            Args:
                code: 权限码。

            Returns:
                bool: False。
            """
            del code
            return False

    register_plugin("permission", "deny", _Deny)
    provider = api_app.state.settings.permission
    provider.provider = "deny"

    response = await _nack(client, offset=0)

    assert response.status_code == 403
    assert response.json()["code"] == 30001
