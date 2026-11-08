"""连接数与长驻流上限（任务 03-4；Kiwi 2424）：两码 × 两形态 × 两处挂载。

覆盖：
- 连接数超限 `350020`：JSON 429 与帧 200 + error 帧（`layer=business`、`retryable=true`）；
- 已登记连接不受影响、`GET /msg/connection` 满额仍可查（排障入口豁免）；
- `/api/v1/msg/*` 同受限（网关路径不构成绕过口）；
- 清扫后放行（过期连接名额被回收）；
- 长驻流超限 `350021` 与预检失败归还名额。
"""

from __future__ import annotations

import importlib
import time
from collections.abc import Iterator
from dataclasses import replace
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
from bms_msg.api.frame import guard as guard_module
from bms_msg.broker.connection import ConnectionTuning
from bms_msg.errors import MsgErrorCode
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2424)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
FRAME_TYPE = "application/x-msg-frame"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
BASE_URL = "http://test"

BASE_TUNING = ConnectionTuning(
    heartbeat_interval_ms=100,
    heartbeat_timeout_ms=300,
    idle_timeout_ms=1_000,
    max_connections=1,
    max_streams_per_connection=1,
    evict_scan_limit=8,
    retry_after_ms=10,
)


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
    """最小应用（连接上限收到 1，便于用例制造「满」）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    app = build_frame_app(session=db_session, tenant_source=tenant_source)
    _use_tuning(app, BASE_TUNING)
    return app


def _use_tuning(app: FastAPI, tuning: ConnectionTuning) -> None:
    """覆盖连接参数（用例按需调小上限）。

    Args:
        app: 应用实例。
        tuning: 连接参数。
    """
    app.dependency_overrides[get_connection_tuning] = lambda: tuning


def _client(app: FastAPI, *, host: str = "10.0.0.1", port: int = 5001) -> AsyncClient:
    """按指定的对端地址造客户端（一个 `(host, port)` 即一条连接键）。

    Args:
        app: 应用实例。
        host: 对端地址。
        port: 对端端口。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=app, client=(host, port)), base_url=BASE_URL)


def _heartbeat_frame() -> bytes:
    """构造一帧心跳请求。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.HEARTBEAT), header={"request_id": "hb"}, flags=FrameFlags.NEED_RESPONSE
    )


def _body(response: Response) -> dict[str, Any]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        dict[str, Any]: 响应 JSON。
    """
    return cast("dict[str, Any]", response.json())


async def test_connection_limit_rejects_new_key_with_json(api_app: FastAPI) -> None:
    """第一条连接占满后，第二条连接的 JSON 请求回 429 + `350020`（不静默丢弃）。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        assert (await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)).status_code == 200
    async with _client(api_app, host="10.0.0.2", port=2) as second:
        rejected = await second.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)

    assert rejected.status_code == 429
    assert _body(rejected)["code"] == int(MsgErrorCode.CONNECTION_LIMIT_EXCEEDED)


async def test_connection_limit_rejects_frame_form_with_error_frame(api_app: FastAPI) -> None:
    """帧形态同一判据另一种承载：HTTP 200 + error 帧（business / retryable，带 retry_after_ms）。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)
    async with _client(api_app, host="10.0.0.2", port=2) as second:
        rejected = await second.post(
            "/msg/connection/heartbeat",
            content=_heartbeat_frame(),
            headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE},
        )

    assert rejected.status_code == 200
    frame = decode_frame(rejected.content)
    assert frame.frame_type == int(FrameType.ERROR)
    assert frame.header["code"] == int(MsgErrorCode.CONNECTION_LIMIT_EXCEEDED)
    assert frame.header["layer"] == "business"
    assert frame.header["retryable"] is True
    assert frame.header["retry_after_ms"] == BASE_TUNING.retry_after_ms


async def test_registered_connection_unaffected_when_full(api_app: FastAPI) -> None:
    """已登记连接（同键）在「满」时照常可用：上限只拦新键。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        assert (await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)).status_code == 200
        assert (await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)).status_code == 200


async def test_connection_info_still_readable_when_full(api_app: FastAPI) -> None:
    """`GET /msg/connection` 不判上限：打满时仍能查到协议版本与连接参数（排障入口）。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)
    async with _client(api_app, host="10.0.0.2", port=2) as second:
        info = await second.get("/msg/connection", headers=DEMO_HEADERS)

    assert info.status_code == 200
    assert _body(info)["data"]["http_version"] == "HTTP/1.1"


async def test_api_v1_data_plane_is_guarded_too(api_app: FastAPI) -> None:
    """`/api/v1/msg/*` 同受限（网关对外路径落在这里，不挂守卫就等于留了绕过口）。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)
    async with _client(api_app, host="10.0.0.2", port=2) as second:
        rejected = await second.post(
            "/api/v1/msg/publish",
            json={"namespace": "msg", "topic": "audit.operation", "payload": "cGF5bG9hZA=="},
            headers=DEMO_HEADERS,
        )

    assert rejected.status_code == 429, "连接上限先于主题解析（连接打满时连租户都不必解析）"
    assert _body(rejected)["code"] == int(MsgErrorCode.CONNECTION_LIMIT_EXCEEDED)


async def test_eviction_frees_slot_for_new_connection(api_app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    """过期连接被惰性清扫后名额归还：新连接键不再被拒。"""
    async with _client(api_app, host="10.0.0.1", port=1) as first:
        await first.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)

    base = time.monotonic()
    later = SimpleNamespace(monotonic=lambda: base + 10_000.0, time=lambda: 1_700_000_000.0)
    monkeypatch.setattr(guard_module, "time", later)

    async with _client(api_app, host="10.0.0.2", port=2) as second:
        accepted = await second.post("/msg/connection/heartbeat", headers=DEMO_HEADERS)

    assert accepted.status_code == 200
    assert api_app.state.connection_registry.count() == 1, "旧连接转 stale、新连接占名额"


async def test_stream_limit_rejects_with_350021(api_app: FastAPI) -> None:
    """单连接长驻流达上限回 429 + `350021`（此时尚未进流，是普通 JSON 错误）。"""
    _use_tuning(api_app, replace(BASE_TUNING, max_streams_per_connection=0))
    async with _client(api_app) as client:
        rejected = await client.get(
            "/msg/subscribe",
            params={
                "namespace": "msg",
                "topic": "audit.operation",
                "group": "audit",
                "consumer_id": "c1",
                "partition": 0,
            },
            headers=DEMO_HEADERS,
        )

    assert rejected.status_code == 429
    assert _body(rejected)["code"] == int(MsgErrorCode.STREAM_LIMIT_EXCEEDED)


async def test_failed_preflight_returns_slot(api_app: FastAPI) -> None:
    """预检失败必须归还名额：否则一次失败会永久占一个长驻流名额（上限 1 时第二次会被误判超限）。"""
    params = {
        "namespace": "msg",
        "topic": "missing.topic",
        "group": "audit",
        "consumer_id": "c1",
        "partition": 0,
    }
    async with _client(api_app) as client:
        first = await client.get("/msg/subscribe", params=params, headers=DEMO_HEADERS)
        second = await client.get("/msg/subscribe", params=params, headers=DEMO_HEADERS)

    assert first.status_code == 404
    assert _body(first)["code"] == int(MsgErrorCode.TOPIC_NOT_FOUND)
    assert second.status_code == 404, "若名额未归还，这里会是 429（350021）"
    assert _body(second)["code"] == int(MsgErrorCode.TOPIC_NOT_FOUND)


def test_tenant_id_helper_reads_context_and_falls_back_to_none() -> None:
    """`_tenant_id` 读中间件解析结果；未解析 / 无内部身份返回 None（生命周期按 id 取消连接）。"""
    assert guard_module._tenant_id(Request({"type": "http", "state": {}})) is None  # pyright: ignore[reportPrivateUsage]
    anonymous = Request(
        {
            "type": "http",
            "state": {"tenant": TenantContext(code="demo", db_key="tenant_demo", name="演示租户")},
        }
    )
    assert guard_module._tenant_id(anonymous) is None  # pyright: ignore[reportPrivateUsage]
    parsed = Request(
        {
            "type": "http",
            "state": {"tenant": TenantContext(code="demo", db_key="tenant_demo", name="演示租户", tenant_id=7001)},
        }
    )
    assert guard_module._tenant_id(parsed) == 7001  # pyright: ignore[reportPrivateUsage]
