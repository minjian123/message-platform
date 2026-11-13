"""发布路径请求体大小早闸（02-6-1 / B17）：大 JSON 不进入解析。

**为什么是中间件而不是端点依赖**：FastAPI 在求解依赖之前就 `await request.body()`，
依赖里再判已经晚了一步；纯 ASGI 中间件可以在「读 body」之前按 `Content-Length` 拒绝，
并对无长度（chunked）请求包装 `receive` 累计字节，超限即停。

**范围**：只覆盖 `POST` + 路径后缀 `/msg/publish` + `application/json`。帧路径有自己的
64 KiB header / 1 MiB payload 判据与 error frame 语义，不套本中间件；其余端点不受影响。
"""

from __future__ import annotations

from bms_core.api.errors import build_error_response
from bms_core.api.middleware import BaseMiddleware
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from bms_msg.core.config import MAX_PUBLISH_BODY_BYTES
from bms_msg.errors import MsgPayloadTooLargeError

__all__ = ["PublishBodyLimitMiddleware"]

_PATH_SUFFIX = "/msg/publish"
"""发布端点路径后缀（`/api/v1/msg/publish` 与根路径 `/msg/publish` 都命中）。"""

_JSON_TYPE = "application/json"
"""只对 JSON 发布早闸；帧请求走自己的长度判据。"""


class _BodyTooLarge(Exception):
    """接收通道累计字节超限（中间件内部信号，不进入业务异常体系）。"""


class PublishBodyLimitMiddleware(BaseMiddleware):
    """发布 JSON 请求体早闸（纯 ASGI）：Content-Length 与累计读取双判。"""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int = MAX_PUBLISH_BODY_BYTES,
    ) -> None:
        """初始化中间件。

        Args:
            app: 下游 ASGI 应用。
            max_body_bytes: 请求体字节上限（缺省与 base64 载荷上限同源）。
        """
        self.app = app
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """处理请求（非 HTTP / 非发布 JSON 直通）。

        Args:
            scope: ASGI 作用域。
            receive: ASGI 接收通道。
            send: ASGI 发送通道。
        """
        if not self._applies(scope):
            await self.app(scope, receive, send)
            return

        content_length = self._content_length(scope)
        if content_length is not None and content_length > self._max_body_bytes:
            await self._reject(scope, receive, send)
            return

        received = 0

        async def _limited_receive() -> Message:
            """累计读取字节；超限抛内部信号（响应未开始前由本中间件收敛为 413）。"""
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self._max_body_bytes:
                    raise _BodyTooLarge
            return message

        started = False

        async def _tracking_send(message: Message) -> None:
            """记录响应是否已开始（已开始后不能再改写为 413）。"""
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, _limited_receive, _tracking_send)
        except _BodyTooLarge:
            if started:
                raise
            await self._reject(scope, receive, send)

    @staticmethod
    def _applies(scope: Scope) -> bool:
        """是否命中发布 JSON 早闸。

        Args:
            scope: ASGI 作用域。

        Returns:
            bool: 命中为 True。
        """
        if scope["type"] != "http":
            return False
        if str(scope.get("method", "")).upper() != "POST":
            return False
        if not str(scope.get("path", "")).endswith(_PATH_SUFFIX):
            return False
        content_type = Headers(scope=scope).get("content-type", "")
        return content_type.split(";", 1)[0].strip().lower() == _JSON_TYPE

    @staticmethod
    def _content_length(scope: Scope) -> int | None:
        """取 Content-Length（缺失 / 非法按 None 处理，交给累计读取判）。

        Args:
            scope: ASGI 作用域。

        Returns:
            int | None: 声明的字节数；不可用时 None。
        """
        raw = Headers(scope=scope).get("content-length")
        if raw is None:
            return None
        try:
            value = int(raw)
        except ValueError:
            return None
        return value if value >= 0 else None

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        """返回 413 / `350006`（与既有载荷上限同码）。

        Args:
            scope: ASGI 作用域。
            receive: ASGI 接收通道。
            send: ASGI 发送通道。
        """
        exc = MsgPayloadTooLargeError(f"发布请求体超过上限 {self._max_body_bytes} 字节")
        response = await build_error_response(scope, exc)
        await response(scope, receive, send)
