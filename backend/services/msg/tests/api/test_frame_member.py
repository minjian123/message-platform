"""成员端点帧形态（任务 03-6-2；Kiwi 2514）：同路径按 Content-Type 分派，根路径双形态。

覆盖：加入 / 成员心跳 / 离开的帧往返（与 JSON 形态逐字段一致）、根路径 JSON 形态回归、
跨端点错发的提示、缺必填头 / 带 payload 的协议层拒绝、业务拒绝同码、未知类型忽略。
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.msgclient.frame import (
    FrameFlags,
    FrameType,
    decode_frame,
    encode_frame,
)
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2514)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
CONSUMER = "c1"
FRAME_TYPE = "application/x-msg-frame"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"

_MEMBER_KEYS = (
    "group",
    "topic",
    "consumer_id",
    "generation",
    "state",
    "partitions",
    "member_count",
    "heartbeat_interval_ms",
    "session_timeout_ms",
)
"""成员分配的帧头键（与客户端 `Assignment` 同名）。"""


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并确保占位权限 / 租户模块门禁已导入。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
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


def _header(*, request_id: str | None = "r1") -> ConcurrentStableDict[str, Any]:
    """构造成员帧头（成员请求四字段 + 可选 `request_id`）。

    Args:
        request_id: 请求标识；None 表示故意不带。

    Returns:
        ConcurrentStableDict[str, Any]: 帧头。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    if request_id is not None:
        header.set("request_id", request_id)
    header.set("group", GROUP)
    header.set("namespace", NAMESPACE)
    header.set("topic", TOPIC_NAME)
    header.set("consumer_id", CONSUMER)
    return header


def _frame(frame_type: int, header: ConcurrentStableDict[str, Any], payload: bytes = b"") -> bytes:
    """编码一帧（缺省置 need_response 位）。

    Args:
        frame_type: 帧类型。
        header: 帧头。
        payload: 消息体（成员端点必须为空）。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=frame_type,
        header=header,
        payload=payload,
        flags=FrameFlags.NEED_RESPONSE,
    )


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
    """建主题（成员端点的前置元数据）。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def test_member_frame_roundtrip_matches_json(client: AsyncClient) -> None:
    """加入 → 心跳 → 离开的帧往返，分配字段与 JSON 形态逐项一致。"""
    await _create_topic(client)

    joined = await _post_frame(client, "/msg/join", _frame(int(FrameType.JOIN), _header()))
    assert joined.status_code == 200
    assert joined.headers["content-type"].startswith(FRAME_TYPE)
    join_frame = decode_frame(joined.content)
    assert join_frame.frame_type == int(FrameType.JOIN)
    assert join_frame.flags & FrameFlags.RESPONSE
    assert join_frame.header["request_id"] == "r1"
    assert join_frame.header["partitions"] == [0]

    heartbeat = await _post_frame(client, "/msg/heartbeat", _frame(int(FrameType.MEMBER_HEARTBEAT), _header()))
    beat_frame = decode_frame(heartbeat.content)
    assert beat_frame.frame_type == int(FrameType.MEMBER_HEARTBEAT)
    # 心跳不递增世代、分配不变（与 JSON 形态同口径）
    assert beat_frame.header["generation"] == join_frame.header["generation"]
    assert beat_frame.header["partitions"] == [0]

    # 同一消费者再走一次 JSON 形态：字段必须与帧形态完全一致（两形态同语义）
    parity = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert parity.status_code == 200
    parity_data = parity.json()["data"]
    for name in _MEMBER_KEYS:
        assert join_frame.header[name] == parity_data[name], name

    left = await _post_frame(client, "/msg/leave", _frame(int(FrameType.LEAVE), _header()))
    leave_frame = decode_frame(left.content)
    assert leave_frame.frame_type == int(FrameType.LEAVE)
    assert leave_frame.header["consumer_id"] == CONSUMER
    assert leave_frame.header["state"] == "empty"


async def test_member_json_form_still_served_at_root(client: AsyncClient) -> None:
    """根路径成员端点保留 JSON 形态：不带帧媒体类型时按统一响应体返回（挂载不改变 JSON 行为）。"""
    await _create_topic(client)

    response = await client.post(
        "/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["code"] == 0
    assert body["data"]["partitions"] == [0]
    assert body["data"]["consumer_id"] == CONSUMER


async def test_member_frame_type_mismatch_reports_target_endpoint(client: AsyncClient) -> None:
    """跨端点错发：三条提示各指向正确的成员端点。"""
    cases = (
        (int(FrameType.LEAVE), "/msg/join", "/msg/leave"),
        (int(FrameType.JOIN), "/msg/leave", "/msg/join"),
        (int(FrameType.MEMBER_HEARTBEAT), "/msg/join", "/msg/heartbeat"),
    )
    for frame_type, path, expected_hint in cases:
        response = await _post_frame(client, path, _frame(frame_type, _header()))
        assert response.status_code == 200
        frame = decode_frame(response.content)
        assert frame.frame_type == int(FrameType.ERROR)
        assert frame.header["code"] == 350018
        assert expected_hint in str(frame.header["message"])


async def test_member_frame_missing_request_id_is_protocol_error(client: AsyncClient) -> None:
    """缺 `request_id`：协议层 `350018`，仍是 200 的错误帧。"""
    response = await _post_frame(client, "/msg/join", _frame(int(FrameType.JOIN), _header(request_id=None)))
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == 350018
    assert "request_id" in str(frame.header["message"])


async def test_member_frame_with_payload_rejected(client: AsyncClient) -> None:
    """成员帧不得携带 payload（只有发布帧可带）。"""
    response = await _post_frame(client, "/msg/join", _frame(int(FrameType.JOIN), _header(), payload=b"x"))
    frame = decode_frame(response.content)
    assert frame.header["code"] == 350018
    assert "payload" in str(frame.header["message"])


async def test_member_heartbeat_without_group_is_business_error(client: AsyncClient) -> None:
    """心跳先于加入：组不存在 `350013`，错误帧 200（与 JSON 形态同码）。"""
    await _create_topic(client)

    response = await _post_frame(client, "/msg/heartbeat", _frame(int(FrameType.MEMBER_HEARTBEAT), _header()))
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == 350013


async def test_member_join_unknown_topic_is_business_error(client: AsyncClient) -> None:
    """主题不存在 `350001`（加入端点声明的错误码在帧形态同样生效）。"""
    header = _header()
    header.set("topic", "no.such.topic")
    response = await _post_frame(client, "/msg/join", _frame(int(FrameType.JOIN), header))
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == 350001


async def test_member_unknown_type_without_need_response_ignored(client: AsyncClient) -> None:
    """未登记类型且不要求响应：按「忽略」回空 200（成员面与数据面同口径）。"""
    body = encode_frame(frame_type=100, header=_header(), flags=FrameFlags(0))
    headers = {**DEMO_HEADERS, "Content-Type": FRAME_TYPE}
    response = await client.post("/msg/join", content=body, headers=headers)
    assert response.status_code == 200
    assert response.content == b""
