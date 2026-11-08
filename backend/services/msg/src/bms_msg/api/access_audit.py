"""访问异常拒绝审计中间件（05-4；需求 05-5）。

**为什么单独一层**：基座 `OperationAuditMiddleware` 只记「`record_methods` 内且不在
`exclude_paths`」的请求；数据面 publish / consume / subscribe / 心跳 / RPC 与 GET 拒绝都不留痕。
本中间件只补「基座不会记录、但结果是访问异常」的请求，避免同一拒绝两条。

**范围**：仅 `401` / `403` / `429`；2xx / 3xx / 其他 4xx / 5xx 不记。`/api/v1/auth/*` 由 bms 登录
审计承担，探针与文档不记。**节流**：按 `(tenant, 路径, 状态)` 在窗口内只留首条，防止客户端重试刷量。

**失败**：普通审计，写失败只记日志，不改变原拒绝响应。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.config import AuditLogSettings
from bms_core.core.context import get_current_user_id, get_scope_context
from bms_core.core.logging import get_logger
from bms_core.db.tenant import TenantContext
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from bms_msg.audit.trail import ACCESS_DENIED_ACTION, AuditEmitter, emit_access_denied
from bms_msg.core.config import ACCESS_ANOMALY_MAX_KEYS, ACCESS_ANOMALY_WINDOW_MS

__all__ = ["AccessAnomalyAuditMiddleware"]

_LOGGER = get_logger("bms_msg.access_audit")

_ANOMALY_STATUSES = frozenset({401, 403, 429})
"""访问异常状态码（只记这三类）。"""

_EXCLUDED_PATHS = frozenset({"/healthz", "/readyz", "/metrics", "/docs", "/redoc", "/openapi.json"})
"""探针与文档路径（任何级别不记）。"""

_AUTH_PREFIX = "/api/v1/auth"
"""认证路径前缀（bms 登录 / 刷新审计承担，msg 不重复）。"""


def _client_ip(scope: Scope) -> str | None:
    """解析来源 IP（`X-Forwarded-For` 首值 → 连接地址）。

    Args:
        scope: ASGI 作用域。

    Returns:
        str | None: 来源 IP。
    """
    forwarded = Headers(scope=scope).get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    client = scope.get("client")
    return str(client[0]) if client else None


def _summary_path(scope: Scope) -> str:
    """取路由模板路径（无匹配回落原始路径）。

    Args:
        scope: ASGI 作用域。

    Returns:
        str: 路径。
    """
    route = scope.get("route")
    return str(getattr(route, "path", "") or scope.get("path", ""))


def _tenant_id_of(scope: Scope) -> str | None:
    """从请求态取租户主键（与基座审计中间件同口径）。

    Args:
        scope: ASGI 作用域。

    Returns:
        str | None: 租户主键字符串。
    """
    state = cast("dict[str, object]", scope.get("state") or {})
    tenant = state.get("tenant")
    if isinstance(tenant, TenantContext) and tenant.tenant_id is not None:
        return str(tenant.tenant_id)
    raw = state.get("tenant_id")
    return None if raw is None else str(raw)


def _user_id_of(scope: Scope) -> int | None:
    """取操作人主键（边缘身份优先，其次请求上下文）。

    Args:
        scope: ASGI 作用域。

    Returns:
        int | None: 操作人主键。
    """
    state = cast("dict[str, object]", scope.get("state") or {})
    identity = state.get("edge_identity")
    identity_user = getattr(identity, "user_id", None)
    if isinstance(identity_user, int):
        return identity_user
    return get_current_user_id()


class AccessAnomalyAuditMiddleware:
    """访问异常审计中间件：响应结束后对 401 / 403 / 429 补一条普通审计。"""

    def __init__(
        self,
        app: ASGIApp,
        *,
        settings: AuditLogSettings,
        clock: Callable[[], float] | None = None,
        window_ms: int = ACCESS_ANOMALY_WINDOW_MS,
        max_keys: int = ACCESS_ANOMALY_MAX_KEYS,
    ) -> None:
        """初始化。

        Args:
            app: 下游 ASGI 应用。
            settings: 审计日志参数（与基座中间件同源，用于判断「基座是否已记录」）。
            clock: 单调时钟（秒；用例注入以控制窗口）。
            window_ms: 节流窗口（毫秒）。
            max_keys: 节流键数量上限（防随机路径打爆内存）。
        """
        self.app = app
        self._settings = settings
        self._clock = clock if clock is not None else time.monotonic
        self._window_ms = window_ms
        self._max_keys = max_keys
        self._seen: ConcurrentStableDict[tuple[str, str, int], float] = ConcurrentStableDict()
        self._suppressed = 0
        self._last_suppressed_log = 0.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """处理请求（非 HTTP 直通；只捕获状态码，不缓冲响应体）。

        Args:
            scope: ASGI 作用域。
            receive: 接收通道。
            send: 发送通道。
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        status = 0

        async def _send(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
            await send(message)

        start = time.perf_counter()
        try:
            await self.app(scope, receive, _send)
        finally:
            if status in _ANOMALY_STATUSES and self._should_record(scope):
                await self._emit(scope, status=status, start=start)

    def _should_record(self, scope: Scope) -> bool:
        """是否需要补记（排除基座已记录、auth、探针与文档）。

        Args:
            scope: ASGI 作用域。

        Returns:
            bool: 需要补记 True。
        """
        if not self._settings.enabled:
            return False
        path = str(scope.get("path", ""))
        if path in _EXCLUDED_PATHS or path == _AUTH_PREFIX or path.startswith(f"{_AUTH_PREFIX}/"):
            return False
        state = cast("dict[str, object]", scope.get("state") or {})
        method = str(scope.get("method", "")).upper()
        base_excluded = any(path == prefix or path.startswith(f"{prefix}/") for prefix in self._settings.exclude_paths)
        base_records = method in self._settings.record_methods and not base_excluded
        return not (base_records or state.get("audit_read"))

    def _throttled(self, *, tenant_id: str | None, path: str, status: int) -> bool:
        """按 `(tenant, 路径, 状态)` 窗口节流（窗口内重复返回 True）。

        Args:
            tenant_id: 租户主键。
            path: 路径。
            status: 状态码。

        Returns:
            bool: 被节流 True。
        """
        now_ms = self._clock() * 1000
        key = (tenant_id or "-", path, status)
        last = self._seen.get(key)
        if last is not None and now_ms - last < self._window_ms:
            self._suppressed += 1
            if now_ms - self._last_suppressed_log >= self._window_ms:
                self._last_suppressed_log = now_ms
                _LOGGER.info("msg_access_audit_suppressed", count=self._suppressed, path=path, status=status)
                self._suppressed = 0
            return True
        if len(self._seen) >= self._max_keys:
            self._seen.clear()
        self._seen.set(key, now_ms)
        return False

    async def _emit(self, scope: Scope, *, status: int, start: float) -> None:
        """组装并发射一条访问异常审计（失败不影响原响应）。

        Args:
            scope: ASGI 作用域。
            status: 实际状态码。
            start: 请求开始时刻（`perf_counter`）。
        """
        app = scope.get("app")
        sink = getattr(getattr(app, "state", None), "audit_sink", None)
        if sink is None:  # pragma: no cover - 装配缺失属编码错误，按不记处理
            _LOGGER.warning("msg_access_audit_sink_missing")
            return
        path = _summary_path(scope)
        tenant_id = _tenant_id_of(scope)
        if self._throttled(tenant_id=tenant_id, path=path, status=status):
            return
        state = cast("dict[str, object]", scope.get("state") or {})
        attempted = state.get("audit_action")
        context = get_scope_context()
        await emit_access_denied(
            cast("AuditEmitter", sink),
            tenant_id=tenant_id,
            status=status,
            method=str(scope.get("method", "")).upper(),
            path=path,
            attempted_action=attempted if isinstance(attempted, str) and attempted else None,
            cost_time=round((time.perf_counter() - start) * 1000),
            user_id=_user_id_of(scope),
            ip=_client_ip(scope),
            trace_id=cast("str | None", state.get("trace_id")),
            module_key=context.module_key if context is not None else None,
            resource_key=context.resource_key if context is not None else None,
        )
        _LOGGER.warning(
            "msg_access_denied",
            status=status,
            path=path,
            method=str(scope.get("method", "")).upper(),
            action=ACCESS_DENIED_ACTION,
            tenant_id=tenant_id,
        )
