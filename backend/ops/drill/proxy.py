"""客户端侧本地 TCP 代理（任务 07-6-1）：网络分区的注入面。

为什么是客户端侧：不碰容器网络与系统防火墙，误操作面为零；断连 / 丢包 / 延迟三档都由代理切，
客户端怎么连、怎么重连是被测对象的真实行为，不是模拟出来的。

四档模式：

- `pass`：直通（基线）；
- `drop`：拒绝新连接并关闭现有连接（网络断开）；
- `loss`：按比例丢弃数据块（弱网）；
- `delay`：每块延迟固定毫秒（高延迟链路）。
"""

from __future__ import annotations

import contextlib
import random
import socket
import threading
import time

from bms_core.core.concurrent import ConcurrentStableSet

from ops.drill.env import DrillError

__all__ = ["MODE_DELAY", "MODE_DROP", "MODE_LOSS", "MODE_PASS", "TcpProxy"]

MODE_PASS = "pass"
"""直通。"""

MODE_DROP = "drop"
"""断连：拒绝新连接并断开现有连接。"""

MODE_LOSS = "loss"
"""丢包：按比例丢弃数据块。"""

MODE_DELAY = "delay"
"""延迟：每块延迟固定毫秒。"""

_RECV_BYTES = 65536
"""单次读取块大小。"""


class TcpProxy:
    """本地 TCP 代理（`127.0.0.1:<proxy_port>` → `127.0.0.1:<target_port>`）。"""

    def __init__(self, target_port: int, *, delay_ms: int = 50, loss: float = 0.5, seed: int = 7) -> None:
        """初始化（不监听）。

        Args:
            target_port: 目标端口（隔离实例）。
            delay_ms: `delay` 模式每块延迟（毫秒）。
            loss: `loss` 模式丢块比例（0~1）。
            seed: 丢包随机种子（复跑可复现）。
        """
        self._target_port = target_port
        self._delay_ms = delay_ms
        self._loss = loss
        self._rng = random.Random(seed)
        self._mode = MODE_PASS
        self._stop = threading.Event()
        self._connections: ConcurrentStableSet[socket.socket] = ConcurrentStableSet()
        self._server: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self.port = 0

    @property
    def mode(self) -> str:
        """当前模式。

        Returns:
            str: 模式标识。
        """
        return self._mode

    def start(self) -> int:
        """启动监听（只绑 127.0.0.1，端口由内核分配）。

        Returns:
            int: 代理端口。

        Raises:
            DrillError: 监听失败。
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("127.0.0.1", 0))
            server.listen(128)
        except OSError as exc:
            server.close()
            raise DrillError(f"drill.proxy 代理监听失败：{exc}") from exc
        self._server = server
        self.port = int(server.getsockname()[1])
        self._accept_thread = threading.Thread(target=self._accept_loop, name="drill-proxy-accept", daemon=True)
        self._accept_thread.start()
        return self.port

    def set_mode(self, mode: str) -> None:
        """切换模式（`drop` 会同时断开现有连接）。

        Args:
            mode: 模式标识。
        """
        self._mode = mode
        if mode == MODE_DROP:
            self._close_connections()

    def close(self) -> None:
        """停止代理并回收全部连接（幂等）。"""
        self._stop.set()
        self._close_connections()
        if self._server is not None:
            try:
                self._server.close()
            finally:
                self._server = None
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=10)
            self._accept_thread = None

    def _close_connections(self) -> None:
        """关闭所有在册连接（关闭后 socket 从集合移除）。"""
        for conn in list(self._connections):
            with contextlib.suppress(OSError):
                conn.shutdown(socket.SHUT_RDWR)
            conn.close()
            self._connections.discard(conn)

    def _accept_loop(self) -> None:
        """接受连接并起两条泵线程。"""
        while not self._stop.is_set():
            server = self._server
            if server is None:
                return
            try:
                client, _ = server.accept()
            except OSError:
                return
            if self._mode == MODE_DROP:
                client.close()
                continue
            try:
                upstream = socket.create_connection(("127.0.0.1", self._target_port), timeout=10)
            except OSError:
                client.close()
                continue
            self._connections.add(client)
            self._connections.add(upstream)
            threading.Thread(target=self._pump, args=(client, upstream), name="drill-proxy-up", daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), name="drill-proxy-down", daemon=True).start()

    def _pump(self, source: socket.socket, sink: socket.socket) -> None:
        """把 `source` 的数据按当前模式搬到 `sink`（块级丢包 / 延迟）。

        Args:
            source: 读端。
            sink: 写端。
        """
        while not self._stop.is_set():
            try:
                data = source.recv(_RECV_BYTES)
            except OSError:
                break
            if not data:
                break
            if self._mode == MODE_DROP:
                break
            if self._mode == MODE_LOSS and self._rng.random() < self._loss:
                continue
            if self._mode == MODE_DELAY:
                time.sleep(self._delay_ms / 1000)
            try:
                sink.sendall(data)
            except OSError:
                break
        for conn in (source, sink):
            with contextlib.suppress(OSError):
                conn.shutdown(socket.SHUT_RDWR)
            self._connections.discard(conn)
