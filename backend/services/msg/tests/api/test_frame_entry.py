"""根路径数据面入口（任务 03-1；Kiwi 2420）：`/msg/*` 与 `/api/v1/msg/*` 同语义、同错误码。

覆盖点（详细设计 §6.1 / §7.1）：
- 三端点经根路径可用，响应结构与 `/api/v1` 一致（含延迟发布分支）；
- 同一业务失败（主题不存在）在两条路径上给出**同一个码**（350001）——规格单点化的回归判据；
- 权限码在根路径同样生效（拒绝时 `30001` / 403）；
- `GET /msg/connection` 回协议版本与客户端地址；
- 公开契约里能看到 5 条根路径（三端点 + subscribe + connection）。
"""

import importlib
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2420)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
GROUP = "audit"
CONSUMER = "c1"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝（不声明 `plugin_name`，避免与占位实现撞自动登记）。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。

        Args:
            code: 权限码（本桩不区分）。

        Returns:
            bool: False。
        """
        del code
        return False


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
def api_app(db_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用（`/api/v1` 业务路由 + 根路径 `/msg/*` 聚合 + 数据面进程级状态）。

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


def _data(response: Response) -> dict[str, object]:
    """取统一响应的 `data` 段。

    Args:
        response: 响应。

    Returns:
        dict[str, object]: 业务数据。
    """
    return cast("dict[str, object]", response.json()["data"])


async def _create_topic(client: AsyncClient, *, partition_count: int = 1) -> None:
    """经管理面建一个主题（两处入口的前置元数据）。

    Args:
        client: 客户端。
        partition_count: 分区数。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": partition_count},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def _join(client: AsyncClient, *, consumer_id: str = CONSUMER) -> None:
    """加入消费组（成员制前置）。

    Args:
        client: 客户端。
        consumer_id: 成员标识。
    """
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": consumer_id},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


def _publish_body(*, topic: str = TOPIC_NAME, delay_level: str | None = None) -> dict[str, object]:
    """构造发布请求体。

    Args:
        topic: 主题名。
        delay_level: 延迟档位（给定即延迟发布）。

    Returns:
        dict[str, object]: 请求体。
    """
    body: dict[str, object] = {"namespace": NAMESPACE, "topic": topic, "key": "order-1", "payload": "cGF5bG9hZA=="}
    if delay_level is not None:
        body["delay_level"] = delay_level
    return body


async def test_root_entry_serves_same_publish_consume_ack(client: AsyncClient) -> None:
    """根路径三端点可用：发布返回 offset、拉取返回同一载荷、确认推进位点。"""
    await _create_topic(client)
    await _join(client)

    published = await client.post("/msg/publish", json=_publish_body(), headers=DEMO_HEADERS)
    assert published.status_code == 200
    published_data = _data(published)
    assert published_data["offset"] == 0
    assert published_data["partition"] == 0
    assert isinstance(published_data["message_id"], str)

    consumed = await client.post(
        "/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
        },
        headers=DEMO_HEADERS,
    )
    assert consumed.status_code == 200
    messages = cast("list[dict[str, object]]", _data(consumed)["messages"])
    assert [message["payload"] for message in messages] == ["cGF5bG9hZA=="]
    assert messages[0]["message_id"] == published_data["message_id"]

    acked = await client.post(
        "/msg/ack",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": 0,
            "offset": 0,
        },
        headers=DEMO_HEADERS,
    )
    assert acked.status_code == 200
    assert _data(acked)["committed_offset"] == 1


async def test_root_entry_supports_delay_publish(client: AsyncClient) -> None:
    """延迟发布分支经根路径同样可用：`offset` 为 null、返回 `deliver_at` 与 `log_id`。"""
    await _create_topic(client)

    response = await client.post("/msg/publish", json=_publish_body(delay_level="1s"), headers=DEMO_HEADERS)

    assert response.status_code == 200
    data = _data(response)
    assert data["offset"] is None
    assert data["deliver_at"] is not None
    assert data["log_id"]


async def test_root_and_api_paths_report_same_error_code(client: AsyncClient) -> None:
    """同一业务失败（主题不存在）在两条路径上码值与状态一致（规格单点化的回归判据）。"""
    root = await client.post("/msg/publish", json=_publish_body(topic="no.such.topic"), headers=DEMO_HEADERS)
    legacy = await client.post("/api/v1/msg/publish", json=_publish_body(topic="no.such.topic"), headers=DEMO_HEADERS)

    assert (root.status_code, legacy.status_code) == (404, 404)
    assert root.json()["code"] == legacy.json()["code"] == 350001


async def test_root_entry_enforces_permission(client: AsyncClient, api_app: FastAPI) -> None:
    """根路径同样受权限码约束：无 `msg:publish` / `msg:consume` 时 403 + `30001`。"""
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"

    published = await client.post("/msg/publish", json=_publish_body(), headers=DEMO_HEADERS)
    subscribed = await client.get(
        "/msg/subscribe",
        params={"namespace": NAMESPACE, "topic": TOPIC_NAME, "group": GROUP, "consumer_id": CONSUMER, "partition": 0},
        headers=DEMO_HEADERS,
    )

    assert (published.status_code, subscribed.status_code) == (403, 403)
    assert (published.json()["code"], subscribed.json()["code"]) == (30001, 30001)


async def test_connection_endpoint_reports_version_and_client(client: AsyncClient) -> None:
    """连接元数据自报：ASGI 直调下协议版本归一为 `HTTP/1.1`、带客户端地址、非 TLS。"""
    response = await client.get("/msg/connection", headers=DEMO_HEADERS)

    assert response.status_code == 200
    data = _data(response)
    assert data["http_version"] == "HTTP/1.1"
    assert data["scheme"] == "http"
    assert data["is_tls"] is False
    assert data["client_host"] == "127.0.0.1"
    assert isinstance(data["client_port"], int)


def test_openapi_contains_root_paths(api_app: FastAPI) -> None:
    """公开契约含 5 条根路径，且三个数据面端点声明了 `x-error-codes`。"""
    paths = api_app.openapi()["paths"]

    for path in ("/msg/publish", "/msg/consume", "/msg/ack", "/msg/subscribe", "/msg/connection"):
        assert path in paths
    assert "x-error-codes" in paths["/msg/subscribe"]["get"]
    assert "x-error-codes" in paths["/msg/publish"]["post"]


def test_both_mounts_share_the_same_spec(api_app: FastAPI) -> None:
    """两条路径的契约**逐字段同源**：`x-error-codes`、请求体 schema、描述都相同。

    `DATA_PLANE_ROUTES` 是唯一出处（路径 / 权限 / 错误码 / 端点函数只写一次），本用例是「不会分叉」的
    回归判据：谁把其中一处改成手写路由，这里立刻红。
    （FastAPI 0.141 起 `include_router` 保留 `_IncludedRouter` 不做扁平化，故从 OpenAPI 侧比对。）
    """
    paths = api_app.openapi()["paths"]
    root = paths["/msg/publish"]["post"]
    legacy = paths["/api/v1/msg/publish"]["post"]

    assert root["x-error-codes"] == legacy["x-error-codes"]
    assert root["requestBody"] == legacy["requestBody"]
    assert root["description"] == legacy["description"]
