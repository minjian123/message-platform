"""连接自报与心跳端点（任务 03-4；Kiwi 2424）：双入口、双形态、drain 标记。

覆盖：`GET /msg/connection` 的新字段与登记一致、`POST /msg/connection/heartbeat` 的 JSON 与帧 17 形态、
心跳刷新 `last_heartbeat_at`、帧形态的四类校验（类型 / payload / 缺 request_id / 主版本）、
draining 时的 `draining` + `retry_after_ms`。
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.db.tenant import TenantContext
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame, encode_frame
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_connection_tuning
from bms_msg.api.frame.guard import _tenant_code  # pyright: ignore[reportPrivateUsage]
from bms_msg.broker.connection import ConnectionTuning
from bms_msg.errors import MsgErrorCode
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2424)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
FRAME_TYPE = "application/x-msg-frame"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"

FAST = ConnectionTuning(
    heartbeat_interval_ms=100,
    heartbeat_timeout_ms=300,
    idle_timeout_ms=1_000,
    max_connections=8,
    max_streams_per_connection=2,
    evict_scan_limit=8,
    retry_after_ms=10,
)
"""毫秒级连接参数：用例不必等秒级阈值。"""


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并导入占位权限 / 租户模块门禁。

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
    """装载产品清单的错误码声明。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    register_error_code_records(ConcurrentStableList(load_product_manifest(_MANIFEST).error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: Any, tenant_source: object) -> FastAPI:
    """最小应用 + 毫秒级连接参数。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    app = build_frame_app(session=db_session, tenant_source=tenant_source)
    app.dependency_overrides[get_connection_tuning] = lambda: FAST
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


def _heartbeat_frame(*, request_id: str | None = "hb1", payload: bytes = b"", frame_type: int = 17) -> bytes:
    """构造心跳帧（可按需制造非法形态）。

    Args:
        request_id: 请求标识；None 表示故意不带。
        payload: 消息体（心跳必须为空）。
        frame_type: 帧类型（缺省 17）。

    Returns:
        bytes: 帧字节。
    """
    header: dict[str, Any] = {} if request_id is None else {"request_id": request_id}
    return encode_frame(frame_type=frame_type, header=header, payload=payload, flags=FrameFlags.NEED_RESPONSE)


async def _post_heartbeat(client: AsyncClient, body: bytes) -> Response:
    """发帧形态心跳。

    Args:
        client: 客户端。
        body: 帧字节。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/msg/connection/heartbeat", content=body, headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE}
    )


async def test_connection_info_reports_full_parameters(client: AsyncClient, api_app: FastAPI) -> None:
    """`ConnectionInfo` 一次给全自适应参数，且与登记表一致（排障与客户端共用一份）。"""
    response = await client.get("/msg/connection", headers=DEMO_HEADERS)
    assert response.status_code == 200

    data = cast("dict[str, Any]", response.json()["data"])
    record = api_app.state.connection_registry.get(data["connection_key"])
    assert record is not None
    assert data["connection_key"] == f"demo|{record.client_host}:{record.client_port}"
    assert data["tenant"] == "demo"
    assert data["http_version"] == "HTTP/1.1", "ASGI 直调没有连接概念，恒为 1.1"
    assert data["protocol_version"] is None
    assert data["supported_protocol_version"] == "1.0"
    assert data["active_streams"] == 0
    assert data["requests"] == 1
    assert data["heartbeat_interval_ms"] == FAST.heartbeat_interval_ms
    assert data["heartbeat_timeout_ms"] == FAST.heartbeat_timeout_ms
    assert data["idle_timeout_ms"] == FAST.idle_timeout_ms
    assert data["max_streams_per_connection"] == FAST.max_streams_per_connection
    assert data["max_connections"] == FAST.max_connections
    assert data["retry_after_ms"] == FAST.retry_after_ms
    assert data["draining"] is False
    assert data["last_heartbeat_at"] is None, "从未心跳时为 null"
    assert data["server_time"]

    again = await client.get("/msg/connection", headers=DEMO_HEADERS)
    assert again.status_code == 200
    assert cast("dict[str, Any]", again.json()["data"])["requests"] == 2, "同一连接键只刷新计数，不新增条目"
    assert api_app.state.connection_registry.count() == 1


async def test_heartbeat_json_form_refreshes_last_heartbeat(client: AsyncClient) -> None:
    """JSON 形态心跳：回同一模型并刷新 `last_heartbeat_at`。"""
    response = await client.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)
    assert response.status_code == 200

    data = cast("dict[str, Any]", response.json()["data"])
    assert data["last_heartbeat_at"] is not None
    assert data["heartbeat_interval_ms"] == FAST.heartbeat_interval_ms


async def test_heartbeat_frame_form_echoes_request_and_parameters(client: AsyncClient) -> None:
    """帧形态心跳：类型 17 + response 位，header 平铺自报字段并回显 request_id。"""
    response = await _post_heartbeat(client, _heartbeat_frame(request_id="hb-42"))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith(FRAME_TYPE)

    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.HEARTBEAT)
    assert frame.flags & FrameFlags.RESPONSE
    assert bytes(frame.payload) == b""
    assert frame.header["request_id"] == "hb-42"
    assert frame.header["protocol_version"] == "1.0"
    assert frame.header["heartbeat_interval_ms"] == FAST.heartbeat_interval_ms
    assert frame.header["max_streams_per_connection"] == FAST.max_streams_per_connection
    assert frame.header["draining"] is False
    assert frame.header["server_time"]
    assert frame.header["connection_key"]


@pytest.mark.parametrize(
    ("body", "expected_code"),
    [
        (_heartbeat_frame(payload=b"x"), int(MsgErrorCode.FRAME_INVALID)),
        (_heartbeat_frame(request_id=None), int(MsgErrorCode.FRAME_INVALID)),
        (_heartbeat_frame(frame_type=int(FrameType.PUBLISH)), int(MsgErrorCode.FRAME_INVALID)),
    ],
)
async def test_heartbeat_frame_rejects_invalid_forms(client: AsyncClient, body: bytes, expected_code: int) -> None:
    """带 payload / 缺 request_id / 帧类型不是 17 → 200 + error 帧（协议内失败）。"""
    response = await _post_heartbeat(client, body)
    assert response.status_code == 200

    frame = decode_frame(response.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == expected_code
    assert frame.header["layer"] == "protocol"


async def test_heartbeat_frame_rejects_unsupported_major(client: AsyncClient) -> None:
    """主版本不符 → `350019`（transport 层，客户端须断开重连）。"""
    body = encode_frame(
        frame_type=int(FrameType.HEARTBEAT),
        header={"request_id": "hb"},
        flags=FrameFlags.NEED_RESPONSE,
        version=(2, 0),
    )
    response = await _post_heartbeat(client, body)
    assert response.status_code == 200

    frame = decode_frame(response.content)
    assert frame.header["code"] == int(MsgErrorCode.VERSION_UNSUPPORTED)
    assert frame.header["layer"] == "transport"


async def test_draining_is_visible_on_both_entries(client: AsyncClient, api_app: FastAPI) -> None:
    """摘流标记在两条入口都可见（客户端据此换实例）；`retry_after_ms` 随同一并给出。"""
    api_app.state.draining = True

    info = cast("dict[str, Any]", (await client.get("/msg/connection", headers=DEMO_HEADERS)).json()["data"])
    assert info["draining"] is True
    assert info["retry_after_ms"] == FAST.retry_after_ms

    frame = decode_frame((await _post_heartbeat(client, _heartbeat_frame())).content)
    assert frame.header["draining"] is True


def test_tenant_code_treats_missing_and_empty_as_anonymous() -> None:
    """守卫读租户只读中间件结果：无上下文 / 空编码都归 None（心跳与连接自报不强制租户）。"""
    no_state = cast("Request", SimpleNamespace(scope={"state": {}}))
    assert _tenant_code(no_state) is None

    empty = TenantContext(code="", db_key="tenant_empty", name="空编码")
    empty_request = cast("Request", SimpleNamespace(scope={"state": {"tenant": empty}}))
    assert _tenant_code(empty_request) is None

    resolved = TenantContext(code="demo", db_key="tenant_demo", name="演示租户")
    resolved_request = cast("Request", SimpleNamespace(scope={"state": {"tenant": resolved}}))
    assert _tenant_code(resolved_request) == "demo"
