"""数据面帧入口端到端（任务 03-2；Kiwi 2421）：同路径按 Content-Type 分派。

覆盖：帧形态三端点可用、错误帧（200）与不可解析帧（4xx JSON）的分界、必填头、类型匹配、
租户一致性、未知头 / 未知位忽略、JSON 形态零改动。
"""

from __future__ import annotations

import importlib
import struct
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.errors import register_msg_error_mapper
from bms_msg.protocol.frame import (
    FRAME_MAGIC,
    FrameFlags,
    FrameType,
    decode_frame,
    encode_frame,
)
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2421)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
CONSUMER = "c1"
FRAME_TYPE = "application/x-msg-frame"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"

_HEADER = struct.Struct(">2sBBBHII")


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并确保占位权限 / 租户模块门禁已导入。

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
    """装载产品清单的错误码声明（统一错误响应据此解析文案键与 HTTP 状态）。

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
    """最小应用（`/api/v1` 业务路由 + 根路径 `/msg/*` 聚合 + 帧闸门）。

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
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


def _header(
    *,
    request_id: str | None = "r1",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构造帧头（缺省含 `request_id`，可按需追加）。

    Args:
        request_id: 请求标识；None 表示故意不带。
        extra: 追加字段。

    Returns:
        dict[str, Any]: 帧头。
    """
    header: dict[str, Any] = {} if request_id is None else {"request_id": request_id}
    header.update(extra or {})
    return header


def _frame(
    frame_type: int,
    header: dict[str, Any],
    payload: bytes = b"",
    *,
    flags: int = int(FrameFlags.NEED_RESPONSE),
) -> bytes:
    """编码一帧（缺省置 need_response 位）。

    Args:
        frame_type: 帧类型。
        header: 帧头。
        payload: 消息体。
        flags: 标志位。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(frame_type=frame_type, header=header, payload=payload, flags=flags)


async def _post_frame(client: AsyncClient, path: str, body: bytes) -> Response:
    """发一个帧请求。

    Args:
        client: 客户端。
        path: 路径。
        body: 帧字节。

    Returns:
        Response: 响应。
    """
    headers = {**DEMO_HEADERS, "Content-Type": FRAME_TYPE}
    return await client.post(path, content=body, headers=headers)


async def _create_topic(client: AsyncClient) -> None:
    """建主题（两处入口的前置元数据）。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def _join(client: AsyncClient) -> None:
    """加入消费组（成员制前置）。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def test_frame_publish_consume_ack_roundtrip(client: AsyncClient) -> None:
    """帧形态三端点端到端：发布 → 拉取（payload 原始字节）→ 确认。"""
    await _create_topic(client)
    await _join(client)

    published = await _post_frame(
        client,
        "/msg/publish",
        _frame(
            int(FrameType.PUBLISH),
            _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "key": "order-1"}),
            b"payload",
        ),
    )
    assert published.status_code == 200
    assert published.headers["content-type"].startswith(FRAME_TYPE)
    publish_frame = decode_frame(published.content)
    assert publish_frame.frame_type == int(FrameType.PUBLISH)
    assert publish_frame.flags & FrameFlags.RESPONSE
    assert publish_frame.header["request_id"] == "r1"
    assert publish_frame.header["offset"] == 0

    consumed = await _post_frame(
        client,
        "/msg/consume",
        _frame(
            int(FrameType.CONSUME),
            _header(
                extra={
                    "namespace": NAMESPACE,
                    "topic": TOPIC_NAME,
                    "group": GROUP,
                    "consumer_id": CONSUMER,
                    "partition": 0,
                }
            ),
        ),
    )
    assert consumed.status_code == 200
    consume_frame = decode_frame(consumed.content)
    metas = consume_frame.header["messages"]
    assert consume_frame.header["count"] == 1
    assert bytes(consume_frame.payload[: metas[0]["length"]]) == b"payload"

    acked = await _post_frame(
        client,
        "/msg/ack",
        _frame(
            int(FrameType.ACK),
            _header(
                extra={
                    "namespace": NAMESPACE,
                    "topic": TOPIC_NAME,
                    "group": GROUP,
                    "consumer_id": CONSUMER,
                    "partition": 0,
                    "offset": 0,
                }
            ),
        ),
    )
    assert acked.status_code == 200
    assert decode_frame(acked.content).header["committed_offset"] == 1


async def test_frame_delay_publish(client: AsyncClient) -> None:
    """延迟发布经帧路径同样可用（`offset` 为 null、`deliver_at` 有值）。"""
    await _create_topic(client)

    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(
            int(FrameType.PUBLISH),
            _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "delay_level": "1s"}),
            b"x",
        ),
    )

    frame = decode_frame(response.content)
    assert frame.header["offset"] is None
    assert frame.header["deliver_at"] is not None


async def test_frame_business_error_returns_frame_with_200(client: AsyncClient) -> None:
    """业务失败：HTTP 200 + 错误帧（码与 JSON 路径同一个 350001）。"""
    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(int(FrameType.PUBLISH), _header(extra={"namespace": NAMESPACE, "topic": "no.such.topic"})),
    )

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert (frame.header["code"], frame.header["layer"], frame.header["retryable"]) == (350001, "business", False)


async def test_frame_missing_request_id_is_protocol_error(client: AsyncClient) -> None:
    """缺必填头 `request_id`：`350018`（协议层），请求可改后重试。"""
    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(int(FrameType.PUBLISH), _header(request_id=None, extra={"namespace": NAMESPACE, "topic": TOPIC_NAME})),
    )

    frame = decode_frame(response.content)
    assert (frame.header["code"], frame.header["layer"]) == (350018, "protocol")


async def test_frame_type_mismatch_rejected(client: AsyncClient) -> None:
    """类型与路径不匹配（把 ack 发到 publish）：`350018`。"""
    response = await _post_frame(client, "/msg/publish", _frame(int(FrameType.ACK), _header()))

    frame = decode_frame(response.content)
    assert frame.header["code"] == 350018
    assert "不匹配" in frame.header["message"]


async def test_known_unsupported_type_has_hint(client: AsyncClient) -> None:
    """登记但未服务（subscribe）与走独立端点的（heartbeat）：都回 `350018` 并给出正确去处。"""
    unsupported = await _post_frame(client, "/msg/publish", _frame(int(FrameType.SUBSCRIBE), _header()))
    frame = decode_frame(unsupported.content)
    assert frame.header["code"] == 350018
    assert "03-" in frame.header["message"]

    misplaced = await _post_frame(client, "/msg/publish", _frame(int(FrameType.HEARTBEAT), _header()))
    hinted = decode_frame(misplaced.content)
    assert hinted.header["code"] == 350018
    assert "/msg/connection/heartbeat" in hinted.header["message"]


async def test_consume_frame_with_payload_rejected(client: AsyncClient) -> None:
    """consume / ack 不得携带 payload（消息体只属 publish）。"""
    response = await _post_frame(
        client,
        "/msg/consume",
        _frame(
            int(FrameType.CONSUME),
            _header(
                extra={
                    "namespace": NAMESPACE,
                    "topic": TOPIC_NAME,
                    "group": GROUP,
                    "consumer_id": CONSUMER,
                    "partition": 0,
                }
            ),
            b"oops",
        ),
    )

    assert decode_frame(response.content).header["code"] == 350018


async def test_unknown_header_key_and_flag_ignored(client: AsyncClient) -> None:
    """未知 header 键与未知标志位一律忽略（铁律 2）。

    只置**未定义位**（bit5~15）：bit0 `compressed` 自 03-3 起是已定义位，置位却不给算法属矛盾，
    会按 `350018` 拒绝（该分支由 `test_frame_payload.py` 覆盖）。
    """
    await _create_topic(client)

    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(
            int(FrameType.PUBLISH),
            _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "future_field": {"x": 1}}),
            b"ok",
            flags=0xFFE0,
        ),
    )

    assert response.status_code == 200
    assert decode_frame(response.content).header["offset"] == 0


async def test_tenant_mismatch_rejected(client: AsyncClient) -> None:
    """帧头 tenant 与身份不一致：`350012`（帧不是鉴权第二入口）。"""
    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(
            int(FrameType.PUBLISH),
            _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "tenant": "other"}),
        ),
    )

    frame = decode_frame(response.content)
    assert (frame.header["code"], frame.header["layer"]) == (350012, "business")


async def test_magic_mismatch_is_400_json(client: AsyncClient) -> None:
    """magic 不符：不是本协议流量，回 400 + JSON（回帧对方也读不懂）。"""
    bad = bytearray(_frame(int(FrameType.PUBLISH), _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME})))
    bad[0:2] = b"XX"

    response = await _post_frame(client, "/msg/publish", bytes(bad))

    assert response.status_code == 400
    assert response.json()["code"] == 350018


async def test_oversize_payload_is_413_json(client: AsyncClient) -> None:
    """声明 payload 超 1 MiB：`350006`（413 + JSON），且不按声明长度分配内存。"""
    prefix = _HEADER.pack(FRAME_MAGIC, 1, 0, int(FrameType.PUBLISH), 0, 2, 1024 * 1024 + 1)
    body = prefix + b"\x80"

    response = await _post_frame(client, "/msg/publish", body)

    assert response.status_code == 413
    assert response.json()["code"] == 350006


async def test_json_form_still_served(client: AsyncClient) -> None:
    """JSON 形态零改动：同一路径不带帧 Content-Type 时仍回统一 JSON 响应。"""
    await _create_topic(client)

    response = await client.post(
        "/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "key": "k", "payload": "cGF5bG9hZA=="},
        headers=DEMO_HEADERS,
    )

    assert response.status_code == 200
    assert response.json()["data"]["offset"] == 0


async def test_permission_denied_returns_json(client: AsyncClient, api_app: FastAPI) -> None:
    """鉴权失败发生在闸门之前：仍回统一 403 JSON（身份未定阶段不按帧回应）。"""

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
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"

    response = await _post_frame(client, "/msg/publish", _frame(int(FrameType.PUBLISH), _header()))

    assert response.status_code == 403
    assert response.json()["code"] == 30001


async def test_payload_key_in_header_rejected(client: AsyncClient) -> None:
    """头里出现 `payload` 键：消息体必须走 payload 段（否则两处来源打架，`350018`）。"""
    response = await _post_frame(
        client,
        "/msg/publish",
        _frame(
            int(FrameType.PUBLISH),
            _header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "in-header"}),
            b"real",
        ),
    )

    assert decode_frame(response.content).header["code"] == 350018


async def test_invalid_header_value_is_contract_error(client: AsyncClient) -> None:
    """键在但值形态非法归契约层（`10001`），与「缺键」的 `350018` 区分开。"""
    response = await _post_frame(
        client,
        "/msg/consume",
        _frame(
            int(FrameType.CONSUME),
            _header(
                extra={
                    "namespace": NAMESPACE,
                    "topic": TOPIC_NAME,
                    "group": GROUP,
                    "consumer_id": CONSUMER,
                    "partition": "not-a-number",
                }
            ),
        ),
    )

    frame = decode_frame(response.content)
    assert (frame.header["code"], frame.header["layer"]) == (10001, "protocol")
