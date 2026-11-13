"""内部复制服务生命周期（02-8）：独立端口上的 ASGI 监听 / 关闭 / 存活判定。

**为什么单独一段生命周期**：关闭偏序要求「先停内部服务（不再接新拉取 / 控制），再停后台循环、
最后刷日志 / 位点」；把它做成独立可关闭对象，`ResourceManager` 才能按注册逆序得到正确偏序。

**绑定失败即启动失败**：端口占用 / 地址非法不让服务带着「半可用」状态起来——多实例下缺了内部
通道就是无复制，静默降级比拒启更危险。
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
from typing import cast

from bms_core.core.capability import BaseAsyncResource
from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.core.logging import get_logger
from fastapi import FastAPI
from hypercorn.asyncio import serve  # pyright: ignore[reportUnknownVariableType]
from hypercorn.config import Config

__all__ = ["InternalReplicationServer", "InternalServerCloser", "InternalServerError"]

_LOGGER = get_logger("bms_msg.internal_server")

_READY_TIMEOUT_MS = 10_000
"""绑定 / 就绪等待上限（毫秒）：真装配冷启动也远小于该值。"""

_READY_PROBE_INTERVAL_MS = 50
"""就绪探测间隔（毫秒）。"""

_SHUTDOWN_TIMEOUT_MS = 5_000
"""关闭等待上限（毫秒）：超时只记 warning，不阻塞主停机。"""

_WILDCARD_HOSTS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("0.0.0.0", "::", ""))
"""通配监听地址（就绪探测改连回环）。"""


class InternalServerError(RuntimeError):
    """内部服务启动 / 绑定失败（服务拒启，不静默退回单实例）。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        """初始化。

        Args:
            reason: 结构化原因（`bind_failed` / `serve_failed` / `ready_timeout`）。
            detail: 排障详情。
        """
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


class InternalReplicationServer:
    """8001 内部复制服务（启动 / 关闭 / 存活）。"""

    def __init__(
        self,
        app: FastAPI,
        *,
        host: str,
        port: int,
        ready_timeout_ms: int = _READY_TIMEOUT_MS,
        shutdown_timeout_ms: int = _SHUTDOWN_TIMEOUT_MS,
    ) -> None:
        """初始化（不监听）。

        Args:
            app: 内部 ASGI 应用。
            host: 监听地址。
            port: 监听端口（须为正）。
            ready_timeout_ms: 就绪等待上限（毫秒）。
            shutdown_timeout_ms: 关闭等待上限（毫秒）。
        """
        if port <= 0:
            raise ValueError(f"内部端口须为正：{port}")
        self._app = app
        self._host = host
        self._port = int(port)
        self._ready_timeout_ms = int(ready_timeout_ms)
        self._shutdown_timeout_ms = int(shutdown_timeout_ms)
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._started = False

    @property
    def host(self) -> str:
        """监听地址。

        Returns:
            str: 主机。
        """
        return self._host

    @property
    def port(self) -> int:
        """监听端口。

        Returns:
            int: 端口。
        """
        return self._port

    @property
    def alive(self) -> bool:
        """服务任务是否存活且已完成首次绑定。

        Returns:
            bool: 已启动且任务未结束为 True。
        """
        return self._started and self._task is not None and not self._task.done()

    async def start(self) -> None:
        """绑定并启动监听（幂等；失败抛 `InternalServerError`）。

        Raises:
            InternalServerError: 端口占用 / 监听失败 / 就绪超时。
        """
        if self._task is not None:
            return
        try:
            self._preflight_bind()
        except OSError as exc:
            raise InternalServerError("bind_failed", f"{self._host}:{self._port}：{exc}") from exc
        config = Config()
        config.bind = [f"{self._host}:{self._port}"]
        config.accesslog = None
        config.errorlog = None
        self._shutdown = asyncio.Event()
        self._task = asyncio.create_task(
            serve(self._app, config, shutdown_trigger=self._shutdown.wait)  # pyright: ignore[reportArgumentType]
        )
        await self._wait_ready()
        self._started = True
        _LOGGER.info("msg_internal_server_started", host=self._host, port=self._port)

    async def aclose(self) -> None:
        """停止监听（幂等；超时只记 warning）。"""
        task = self._task
        if task is None:
            return
        self._shutdown.set()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=self._shutdown_timeout_ms / 1000)
        except TimeoutError:
            _LOGGER.warning("msg_internal_server_stop_timeout", host=self._host, port=self._port)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        except Exception as exc:  # pragma: no cover - 监听任务异常退出时关闭路径只做清理
            _LOGGER.warning("msg_internal_server_stop_failed", reason=type(exc).__name__)
        finally:
            self._task = None
            self._started = False
            _LOGGER.info("msg_internal_server_stopped", host=self._host, port=self._port)

    def _preflight_bind(self) -> None:
        """预绑定一次（只探测端口占用，立即释放）。

        Raises:
            OSError: 端口被占用 / 地址不可用。
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Windows 专有选项：Linux 类型存根里没有该属性（直接取会让 pyright 在 Linux 上报错），
        # 故按「可选常量」取；缺失即回落 `SO_REUSEADDR`，两个平台行为与原来一致。
        exclusive = cast("int | None", getattr(socket, "SO_EXCLUSIVEADDRUSE", None))
        try:
            if exclusive is not None:  # pragma: no cover - Windows 专属分支（Linux 无该常量）
                sock.setsockopt(socket.SOL_SOCKET, exclusive, 1)
            else:  # pragma: no cover - 非 Windows 分支
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self._host, self._port))
        finally:
            sock.close()

    async def _wait_ready(self) -> None:
        """轮询到端口可连（监听任务提前结束时按失败处理）。

        Raises:
            InternalServerError: 监听任务异常结束 / 就绪超时。
        """
        task = self._task
        if task is None:  # pragma: no cover - start 内先后赋值
            raise InternalServerError("serve_failed", "监听任务未创建")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ready_timeout_ms / 1000
        probe_host = "127.0.0.1" if self._host in _WILDCARD_HOSTS else self._host
        while loop.time() < deadline:
            if task.done():
                exc = task.exception()
                raise InternalServerError("serve_failed", f"{type(exc).__name__}: {exc}" if exc else "监听任务提前结束")
            try:
                reader, writer = await asyncio.open_connection(probe_host, self._port)
            except OSError:
                await asyncio.sleep(_READY_PROBE_INTERVAL_MS / 1000)
                continue
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            del reader
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        self._task = None
        raise InternalServerError("ready_timeout", f"{self._host}:{self._port} 在 {self._ready_timeout_ms}ms 内未就绪")


class InternalServerCloser(BaseAsyncResource):
    """内部服务关闭句柄（02-8）：登记进 ResourceManager，停机时**最先**释放。

    注册时机在 startup_hooks()（晚于后台循环与存储句柄），ResourceManager 逆序释放，
    因此得到「先停内部服务（不再接新拉取 / 控制）→ 再停后台循环 → 最后刷日志 / 位点」。
    """

    def __init__(self, *, server: InternalReplicationServer) -> None:
        """初始化。

        Args:
            server: 内部复制服务。
        """
        self._server = server

    async def aclose(self) -> None:
        """停止监听（幂等；超时只记 warning）。"""
        await self._server.aclose()
