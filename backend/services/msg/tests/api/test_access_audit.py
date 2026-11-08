"""访问异常审计中间件（05-4；Kiwi 2455）。"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import AuditLogSettings
from bms_core.db.tenant import TenantContext
from bms_core.events.base import EventEnvelope
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from starlette.types import Message, Receive, Scope, Send

from bms_msg.api.access_audit import AccessAnomalyAuditMiddleware

pytestmark = pytest.mark.kiwi_id(2455)

BASE_URL = "http://test"
"""测试基址。"""

DATA_PLANE_PATH = "/api/v1/msg/publish"
"""数据面路径（基座排除、由访问异常中间件补记）。"""

CONFIG_PATH = "/api/v1/msg/admin/topics"
"""配置路径（显式审计路径；测试中可按需加入 / 不加入排除）。"""


class _Clock:
    """单调时钟替身（秒）。"""

    def __init__(self) -> None:
        """初始化（10 秒：让首次抑制日志的时间判据可命中）。"""
        self.now = 10.0

    def __call__(self) -> float:
        """当前秒数。

        Returns:
            float: 当前秒数。
        """
        return self.now

    def advance(self, seconds: float) -> None:
        """推进时钟。

        Args:
            seconds: 秒数。
        """
        self.now += seconds


class _Sink:
    """审计替身（记录事件；可配置抛错）。"""

    def __init__(self, *, explode: bool = False) -> None:
        """初始化。

        Args:
            explode: `emit` 是否抛错。
        """
        self.events: list[EventEnvelope] = []
        self.explode = explode

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一次写入。

        Args:
            event: 审计事件。
            required: 强审计开关（忽略）。

        Returns:
            bool: 恒成功（`explode` 为 False）。

        Raises:
            RuntimeError: `explode=True`。
        """
        del required
        if self.explode:
            raise RuntimeError("audit down")
        self.events.append(event)
        return True


class _StateMiddleware:
    """请求态替身：写入租户 / 尝试动作 / trace（模拟基座上下文）。"""

    def __init__(self, app: Callable[..., Awaitable[None]], *, tenant_id: int = 7001) -> None:
        """初始化。

        Args:
            app: 下游 ASGI 应用。
            tenant_id: 租户主键。
        """
        self.app = app
        self.tenant_id = tenant_id

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """写入请求态后直通。

        Args:
            scope: ASGI 作用域。
            receive: 接收通道。
            send: 发送通道。
        """
        state = cast("dict[str, object]", scope.setdefault("state", {}))
        state["tenant_id"] = self.tenant_id
        state["audit_action"] = "msg:publish"
        state["trace_id"] = "trace-1"
        await self.app(scope, receive, send)


def _settings(*, enabled: bool = True, exclude_paths: list[str] | None = None) -> AuditLogSettings:
    """构造审计设置。

    Args:
        enabled: 是否启用。
        exclude_paths: 排除路径。

    Returns:
        AuditLogSettings: 设置。
    """
    paths = [DATA_PLANE_PATH, CONFIG_PATH] if exclude_paths is None else exclude_paths
    return AuditLogSettings(enabled=enabled, exclude_paths=ConcurrentStableList(paths))


def _app(
    *,
    status: int,
    path: str,
    method: str = "POST",
    settings: AuditLogSettings,
    sink: _Sink,
    clock: _Clock,
    max_keys: int = 1_024,
) -> FastAPI:
    """构造最小应用（单路由固定状态码 + 访问异常中间件）。

    Args:
        status: 固定响应状态码。
        path: 路由路径。
        method: 路由方法。
        settings: 审计设置。
        sink: 审计替身。
        clock: 单调时钟。
        max_keys: 节流键上限。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()

    @app.api_route(path, methods=[method])
    async def _endpoint() -> JSONResponse:  # pyright: ignore[reportUnusedFunction]
        """固定状态响应。"""
        return JSONResponse(status_code=status, content={"code": status})

    app.state.audit_sink = sink
    app.add_middleware(
        AccessAnomalyAuditMiddleware,
        settings=settings,
        clock=clock,
        window_ms=1_000,
        max_keys=max_keys,
    )
    app.add_middleware(_StateMiddleware)
    return app


async def _call(app: FastAPI, *, path: str, method: str = "POST") -> int:
    """发一次请求并返回状态码。

    Args:
        app: 应用。
        path: 路径。
        method: 方法。

    Returns:
        int: 响应状态码。
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as client:
        response = await client.request(method, path)
    return response.status_code


def _params(event: EventEnvelope) -> dict[str, object]:
    """取事件载荷中的 params 映射。

    Args:
        event: 审计事件。

    Returns:
        dict[str, object]: 解析后的载荷。
    """
    return cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))


async def test_excluded_data_plane_denial_emits() -> None:
    """数据面 403（基座排除）→ 一条 `msg:access.denied`，带租户 / 尝试动作 / trace。"""
    sink = _Sink()
    app = _app(status=403, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=_Clock())

    assert await _call(app, path=DATA_PLANE_PATH) == 403

    assert len(sink.events) == 1
    event = sink.events[0]
    assert (event.payload["action"], event.payload["status"], event.payload["module"]) == (
        "msg:access.denied",
        403,
        "msg",
    )
    assert (event.payload["tenant_id"], event.trace_id) == ("7001", "trace-1")
    assert _params(event) == {"status": 403, "reason": "forbidden", "attempted_action": "msg:publish"}


async def test_get_denial_emits() -> None:
    """GET 403（基座默认不记）→ 补记一条。"""
    sink = _Sink()
    app = _app(
        status=403,
        path="/api/v1/msg/admin/topics",
        method="GET",
        settings=_settings(exclude_paths=[]),
        sink=sink,
        clock=_Clock(),
    )

    assert await _call(app, path="/api/v1/msg/admin/topics", method="GET") == 403
    assert len(sink.events) == 1


async def test_base_recorded_path_denial_skipped() -> None:
    """基座已记录路径的 403 不重复。"""
    sink = _Sink()
    app = _app(
        status=403,
        path="/api/v1/msg/admin/topics",
        settings=_settings(exclude_paths=[]),
        sink=sink,
        clock=_Clock(),
    )

    assert await _call(app, path="/api/v1/msg/admin/topics") == 403
    assert sink.events == []


async def test_non_anomaly_statuses_not_recorded() -> None:
    """200 / 400 / 500 不记（5xx 是故障不是访问异常）。"""
    for status in (200, 400, 500):
        sink = _Sink()
        app = _app(status=status, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=_Clock())
        assert await _call(app, path=DATA_PLANE_PATH) == status
        assert sink.events == []


async def test_throttle_window_keeps_first_and_reemits_after_window() -> None:
    """同键窗口内只留首条；窗口推进后重新记。"""
    sink = _Sink()
    clock = _Clock()
    app = _app(status=403, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=clock)

    assert await _call(app, path=DATA_PLANE_PATH) == 403
    assert await _call(app, path=DATA_PLANE_PATH) == 403
    assert len(sink.events) == 1
    clock.advance(0.5)
    assert await _call(app, path=DATA_PLANE_PATH) == 403
    assert len(sink.events) == 1
    clock.advance(2.0)
    assert await _call(app, path=DATA_PLANE_PATH) == 403
    assert len(sink.events) == 2


async def test_auth_and_probe_paths_skipped() -> None:
    """认证路径与探针不记。"""
    sink = _Sink()
    auth = _app(
        status=403,
        path="/api/v1/auth/login",
        settings=_settings(exclude_paths=[]),
        sink=sink,
        clock=_Clock(),
    )
    assert await _call(auth, path="/api/v1/auth/login") == 403
    probe = _app(status=403, path="/healthz", method="GET", settings=_settings(), sink=sink, clock=_Clock())
    assert await _call(probe, path="/healthz", method="GET") == 403
    assert sink.events == []


async def test_disabled_settings_skipped() -> None:
    """审计关闭时不记。"""
    sink = _Sink()
    app = _app(status=403, path=DATA_PLANE_PATH, settings=_settings(enabled=False), sink=sink, clock=_Clock())

    assert await _call(app, path=DATA_PLANE_PATH) == 403
    assert sink.events == []


async def test_sink_failure_does_not_change_response() -> None:
    """审计写失败不影响原拒绝响应。"""
    sink = _Sink(explode=True)
    app = _app(status=403, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=_Clock())

    assert await _call(app, path=DATA_PLANE_PATH) == 403


async def test_sink_missing_does_not_break_response() -> None:
    """装配缺失审计出口时不抛错，原响应不变。"""
    sink = _Sink()
    app = _app(status=403, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=_Clock())
    del app.state.audit_sink

    assert await _call(app, path=DATA_PLANE_PATH) == 403


async def test_max_keys_clear_keeps_recording() -> None:
    """节流键达到上限后清空，仍能记录新键。"""

    async def _noop(scope: Scope, receive: Receive, send: Send) -> None:
        """空应用。"""
        del scope, receive, send

    middleware = AccessAnomalyAuditMiddleware(
        _noop, settings=AuditLogSettings(), clock=_Clock(), window_ms=1_000, max_keys=1
    )
    assert middleware._throttled(tenant_id="a", path="/x", status=403) is False  # pyright: ignore[reportPrivateUsage]
    assert middleware._throttled(tenant_id="b", path="/y", status=403) is False  # pyright: ignore[reportPrivateUsage]
    assert middleware._throttled(tenant_id="b", path="/y", status=403) is True  # pyright: ignore[reportPrivateUsage]


async def test_401_and_429_reasons() -> None:
    """401 / 429 分别落 `unauthenticated` / `rate_limited`。"""
    for status, reason in ((401, "unauthenticated"), (429, "rate_limited")):
        sink = _Sink()
        app = _app(status=status, path=DATA_PLANE_PATH, settings=_settings(), sink=sink, clock=_Clock())
        assert await _call(app, path=DATA_PLANE_PATH) == status
        assert _params(sink.events[0])["reason"] == reason


def test_context_helpers_cover_tenant_and_identity_branches() -> None:
    """请求态解析：TenantContext / tenant_id / edge identity / 路由模板与 IP 头。"""
    from bms_msg.api.access_audit import _client_ip, _summary_path, _tenant_id_of, _user_id_of  # pyright: ignore[reportPrivateUsage]

    tenant = TenantContext(code="demo", db_key="tenant_demo", name="演示", tenant_id=7001)
    scope = cast(
        "Scope",
        {
            "type": "http",
            "path": "/api/v1/msg/publish",
            "headers": [(b"x-forwarded-for", b"10.0.0.1, 10.0.0.2")],
            "state": {
                "tenant": tenant,
                "edge_identity": SimpleNamespace(user_id=42),
                "trace_id": "trace-1",
            },
            "route": SimpleNamespace(path="/api/v1/msg/publish"),
        },
    )
    assert _tenant_id_of(scope) == "7001"
    assert _user_id_of(scope) == 42
    assert _client_ip(scope) == "10.0.0.1"
    assert _summary_path(scope) == "/api/v1/msg/publish"
    fallback = cast("Scope", {"type": "http", "path": "/raw/path", "headers": [], "state": {}})
    assert _summary_path(fallback) == "/raw/path"
    assert _client_ip(fallback) is None


async def test_non_http_scope_passthrough() -> None:
    """非 HTTP 作用域直通（不构造状态）。"""
    called: list[str] = []

    async def _app(scope: Scope, receive: Receive, send: Send) -> None:
        """记录作用域类型。"""
        del receive, send
        called.append(str(scope["type"]))

    middleware = AccessAnomalyAuditMiddleware(_app, settings=AuditLogSettings())

    async def _receive() -> Message:
        """空接收。"""
        return cast("Message", {"type": "lifespan.startup"})

    async def _send(message: Message) -> None:
        """空发送。"""
        del message

    await middleware(cast("Scope", {"type": "lifespan"}), _receive, _send)
    assert called == ["lifespan"]
