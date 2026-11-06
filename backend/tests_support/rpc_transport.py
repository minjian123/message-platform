"""测试辅助：两个 `RpcTransport` 替身（任务 04-7-1）。

**为什么要自己写**：客户端用例原来靠 `httpx.ASGITransport` / `httpx.MockTransport` 注入，等于「测试走 httpx、
生产走 h2」——04-2 的「测试全绿、真机不发 `RST_STREAM`」正是这么来的。换成同一份 `RpcTransport` 契约后，
`RpcClient` 里不再有「测试才走的分支」，生产与测试只差「字节往哪送」。

- `AsgiRpcTransport`：进程内驱动真 app（真路由 → 真 handler → 真帧），替代 `ASGITransport`；
- `ScriptedRpcTransport`：按脚本返响应 / 抛传输异常，替代 `MockTransport` 的错误分支。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI

from bms_msg.client.transport import (
    RpcHttpRequest,
    RpcHttpResponse,
    TransportStats,
)

__all__ = ["AsgiRpcTransport", "ScriptedRpcTransport", "TransportHandler"]

TransportHandler = Callable[[RpcHttpRequest], Awaitable[RpcHttpResponse]]
"""脚本传输的处理器类型（返回响应或抛传输异常）。"""


class AsgiRpcTransport:
    """在进程内驱动 ASGI 应用（不起服务、不碰 socket）。"""

    def __init__(self, app: FastAPI) -> None:
        """初始化。

        Args:
            app: 目标应用（测试里是 `build_frame_app(...)` 的产物）。
        """
        self._app = app
        self.closed = False

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """把传输层请求折成 ASGI 调用并收集响应。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 应用产出的状态码、头与体。
        """
        parsed = urlsplit(request.origin)
        path, _, query = request.target.partition("?")
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": parsed.scheme,
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": query.encode("utf-8"),
            "root_path": "",
            "headers": [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in request.headers],
            "client": ("127.0.0.1", 0),
            "server": (parsed.hostname or "test", parsed.port or 80),
        }
        collected: MutableMapping[str, Any] = {"status": 0, "headers": (), "body": bytearray()}
        delivered = False
        finished = asyncio.Event()

        async def receive() -> MutableMapping[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": request.body, "more_body": False}
            # 请求体给完之后必须「挂住」而不是立刻回 `http.disconnect`：服务端 04-2 的断开探测
            # 会轮询 `request.is_disconnected()`，立刻回 disconnect 等于宣告「客户端跑了」，
            # 于是每个 handler 都被当成取消（真服务端只在真的断开时才会给这条消息）。
            await finished.wait()
            return {"type": "http.disconnect"}

        async def send(message: MutableMapping[str, Any]) -> None:
            if message["type"] == "http.response.start":
                collected["status"] = message["status"]
                collected["headers"] = message.get("headers", ())
            elif message["type"] == "http.response.body":
                collected["body"].extend(message.get("body", b""))
                if not message.get("more_body", False):
                    finished.set()

        await self._app(scope, receive, send)
        headers = tuple((name.decode("latin-1"), value.decode("latin-1")) for name, value in collected["headers"])
        return RpcHttpResponse(
            status=int(collected["status"]),
            headers=headers,
            body=bytes(collected["body"]),
            stream_id=0,
        )

    async def aclose(self) -> None:
        """标记关闭（无连接可释放）。"""
        self.closed = True

    def stats(self) -> TransportStats:
        """进程内传输没有连接概念。

        Returns:
            TransportStats: 恒零。
        """
        return TransportStats(connections=0, open_streams=0)


class ScriptedRpcTransport:
    """按脚本应答 / 抛错的传输替身（覆盖错误映射与超时分支）。"""

    def __init__(self, handler: TransportHandler) -> None:
        """初始化。

        Args:
            handler: 处理器（返回响应，或抛 `RpcTransportError` / 其它异常）。
        """
        self._handler = handler
        self.closed = False
        self.requests: list[RpcHttpRequest] = []

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """记录请求并交给脚本。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 脚本给出的响应。

        Raises:
            RpcTransportError: 脚本抛出（原样上抛，供 `RpcClient` 映射）。
        """
        self.requests.append(request)
        return await self._handler(request)

    async def aclose(self) -> None:
        """标记关闭。"""
        self.closed = True

    def stats(self) -> TransportStats:
        """脚本传输没有真实流。

        Returns:
            TransportStats: 恒零。
        """
        return TransportStats(connections=0, open_streams=0)
