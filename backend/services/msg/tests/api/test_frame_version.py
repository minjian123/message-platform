"""版本演进与未知处理（任务 03-2；Kiwi 2421）：主版本断流 / 次版本任接受 / 未知类型滑过。

三条演进铁律的可断言形态都在这里：次版本不打断旧端、未知类型「忽略而非报错」、版本可见。
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame, encode_frame
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2421)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
FRAME_TYPE = "application/x-msg-frame"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"


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
    """最小应用（含根路径 `/msg/*` 与帧闸门）。

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


def _publish_frame(
    *, version: tuple[int, int] = (1, 0), frame_type: int = int(FrameType.PUBLISH), flags: int = 0
) -> bytes:
    """构造一个合法 publish 帧（可改版本 / 类型 / 位，用于兼容性验证）。

    Args:
        version: 协议版本。
        frame_type: 帧类型。
        flags: 标志位。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=frame_type,
        header={"request_id": "r1", "namespace": NAMESPACE, "topic": TOPIC_NAME},
        payload=b"x",
        flags=flags,
        version=version,
    )


async def _post(client: AsyncClient, body: bytes) -> Response:
    """发帧请求。

    Args:
        client: 客户端。
        body: 帧字节。

    Returns:
        Response: 响应。
    """
    return await client.post("/msg/publish", content=body, headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE})


async def _create_topic(client: AsyncClient) -> None:
    """建主题。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def test_major_version_mismatch_returns_transport_error(client: AsyncClient) -> None:
    """主版本不支持：200 + `350019`（layer=transport，客户端据此断开连接）。"""
    response = await _post(client, _publish_frame(version=(2, 0)))

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert (frame.header["code"], frame.header["layer"], frame.header["retryable"]) == (350019, "transport", False)
    assert frame.header["request_id"] is None


async def test_minor_version_forward_compatible(client: AsyncClient) -> None:
    """次版本更高也能正常收发（只增可选字段，老服务端忽略未知）。"""
    await _create_topic(client)

    response = await _post(client, _publish_frame(version=(1, 9)))

    assert response.status_code == 200
    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.PUBLISH)
    assert frame.header["protocol_version"] == "1.0"
    assert frame.header["offset"] == 0


async def test_unknown_type_with_need_response_gets_error(client: AsyncClient) -> None:
    """未知类型但要求响应：回 `350018`（静默会让调用方挂到超时）。"""
    response = await _post(client, _publish_frame(frame_type=200, flags=int(FrameFlags.NEED_RESPONSE)))

    frame = decode_frame(response.content)
    assert (frame.header["code"], frame.header["layer"]) == (350018, "protocol")


async def test_unknown_type_without_need_response_is_ignored(client: AsyncClient) -> None:
    """未知类型且不要求响应：忽略（200 空体，不报错不断连）。"""
    response = await _post(client, _publish_frame(frame_type=200))

    assert response.status_code == 200
    assert response.content == b""


async def test_subscribe_type_is_known_but_unsupported(client: AsyncClient) -> None:
    """`subscribe` 已登记但未服务：`350018` 并提示归 03-5。"""
    body = _publish_frame(frame_type=int(FrameType.SUBSCRIBE), flags=int(FrameFlags.NEED_RESPONSE))
    response = await _post(client, body)

    frame = decode_frame(response.content)
    assert frame.header["code"] == 350018
    assert "03-5" in frame.header["message"]
