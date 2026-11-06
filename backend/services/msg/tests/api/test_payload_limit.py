"""载荷上限（任务 03-2；Kiwi 2421）：JSON 路径与帧路径同一 1 MiB 判据。

闭环计划风险 8（「上限只靠传输层拒绝」）：上限落在 `decode_payload` 一处，普通发布与延迟发布共用，
所以**不依赖客户端走帧**——JSON 路径超限同样 `350006`（413）。
"""

from __future__ import annotations

import base64
import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES
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
    """最小应用。

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


def _body(*, size: int, delay_level: str | None = None) -> dict[str, Any]:
    """构造发布请求体（载荷按 size 字节 base64）。

    Args:
        size: 消息体字节数。
        delay_level: 延迟档位（给定即延迟发布）。

    Returns:
        dict[str, Any]: 请求体。
    """
    body: dict[str, Any] = {
        "namespace": NAMESPACE,
        "topic": TOPIC_NAME,
        "payload": base64.b64encode(b"\x00" * size).decode("ascii"),
    }
    if delay_level is not None:
        body["delay_level"] = delay_level
    return body


async def _create_topic(client: AsyncClient) -> None:
    """建主题（发布的前置元数据）。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def _publish(client: AsyncClient, body: dict[str, Any]) -> Response:
    """经 JSON 数据面发布。

    Args:
        client: 客户端。
        body: 请求体。

    Returns:
        Response: 响应。
    """
    return await client.post("/api/v1/msg/publish", json=body, headers=DEMO_HEADERS)


async def test_exact_limit_is_accepted(client: AsyncClient) -> None:
    """恰好 1 MiB 通过（上限含端点）。"""
    await _create_topic(client)

    response = await _publish(client, _body(size=MAX_FRAME_PAYLOAD_BYTES))

    assert response.status_code == 200
    assert response.json()["data"]["offset"] == 0


async def test_over_limit_is_rejected(client: AsyncClient) -> None:
    """1 MiB + 1 字节即 `350006`（413），与帧路径同码。"""
    await _create_topic(client)

    response = await _publish(client, _body(size=MAX_FRAME_PAYLOAD_BYTES + 1))

    assert response.status_code == 413
    assert response.json()["code"] == 350006


async def test_delay_publish_uses_same_limit(client: AsyncClient) -> None:
    """延迟发布共用同一判据（不是只在普通发布分支拦）。"""
    await _create_topic(client)

    response = await _publish(client, _body(size=MAX_FRAME_PAYLOAD_BYTES + 1, delay_level="1s"))

    assert response.status_code == 413
    assert response.json()["code"] == 350006
