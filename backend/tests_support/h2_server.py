"""测试辅助：裸 h2 服务端（任务 04-7-1）。

**为什么不直接用 hypercorn**：传输层用例要断言的是**帧级事实**——「客户端发没发 `RST_STREAM`、
错误码是多少、`GOAWAY` 的 `last_stream_id` 有没有被尊重、窗口更新有没有推动发送」。这些在 hypercorn
后面全看不见，只能靠「自己就是服务端」来精确制造与观测。真 hypercorn 的端到端验证另有其用例
（04-7-2 的 `test_rpc_client_cancel_h2c.py`）。

**路由由路径决定**（`/ok` `/echo` `/gated` `/slow` `/reset` `/goaway` `/boom` `/close` `/status/{code}`），
需要「什么时候回」的路由用 `release` 事件控制，用例据此得到确定性时序，不靠 `sleep` 猜。
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

__all__ = ["CERT_DIR", "H2TestServer", "client_ssl_context", "server_tls_context"]

CERT_DIR = Path(__file__).resolve().parent / "certs"
"""静态自签证书目录（`CN=127.0.0.1` + `IP:127.0.0.1` SAN，长有效期；不依赖运行期生成）。"""

RouteHandler = Callable[["_Connection", int, bytes], Awaitable[None]]
"""路由处理器：连接、流号、请求体 → 自行完成响应。"""


def server_tls_context(*, alpn: bool = True) -> ssl.SSLContext:
    """构造服务端 TLS 上下文（可关掉 ALPN 以覆盖「协商不到 h2」的分支）。

    Args:
        alpn: 是否声明支持 `h2`。

    Returns:
        ssl.SSLContext: 服务端上下文。
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(CERT_DIR / "h2-test.crt", CERT_DIR / "h2-test.key")
    if alpn:
        context.set_alpn_protocols(["h2"])
    return context


def client_ssl_context() -> ssl.SSLContext:
    """构造信任测试证书的客户端上下文（声明 ALPN `h2`，与服务端对齐）。

    Returns:
        ssl.SSLContext: 客户端上下文（信任 `certs/h2-test.crt`）。
    """
    context = ssl.create_default_context(cafile=str(CERT_DIR / "h2-test.crt"))
    context.set_alpn_protocols(["h2"])
    return context


class _Connection:
    """一条服务端连接（含 h2 状态机与写路径）。"""

    def __init__(self, server: H2TestServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """初始化。

        Args:
            server: 所属测试服务端（共享事件记录与 `release` 开关）。
            reader: 输入流。
            writer: 输出流。
        """
        self._server = server
        self._reader = reader
        self._writer = writer
        self.h2 = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
        self.paths: dict[int, str] = {}
        self.bodies: dict[int, bytearray] = {}

    async def serve(self) -> None:
        """握手并循环处理请求，直到对端关闭。"""
        self.h2.initiate_connection()
        if self._server.max_concurrent_streams is not None:
            self.h2.update_settings(
                {h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: self._server.max_concurrent_streams}
            )
        if self._server.settings_without_max:
            # 只改别的设置项：用来覆盖「远端 SETTINGS 里没有并发流上限」那条分支
            self.h2.update_settings({h2.settings.SettingCodes.MAX_FRAME_SIZE: 16384})
        self._flush()
        await self._writer.drain()
        try:
            while True:
                data = await self._reader.read(65535)
                if not data:
                    return
                for event in self.h2.receive_data(data):
                    self._on_event(event)
                self._flush()
                await self._writer.drain()
        except OSError, h2.exceptions.H2Error:
            return

    def _on_event(self, event: h2.events.Event) -> None:
        """按事件推进（请求头 / 数据 / 结束 / 复位）。"""
        if isinstance(event, h2.events.RequestReceived):
            self.paths[event.stream_id] = dict(event.headers).get(b":path", b"").decode("latin-1")
            self._server.frames.append(("headers", event.stream_id, 0))
        elif isinstance(event, h2.events.DataReceived):
            if not self._server.pause_acks:
                self.h2.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
            self.bodies.setdefault(event.stream_id, bytearray()).extend(event.data)
        elif isinstance(event, h2.events.StreamEnded):
            self._server.frames.append(("end", event.stream_id, 0))
            # 路由放到独立任务里跑：`/gated` 要等用例放行，若在读取循环里 await 会卡住整条连接
            # （服务端读不到同连接后续流的帧，用例表现为「第二条请求永远不回」）。
            task = asyncio.create_task(self._route(event.stream_id))
            self._server.tasks.add(task)
            task.add_done_callback(self._server.tasks.discard)
        elif isinstance(event, h2.events.StreamReset):
            self._server.frames.append(("reset", event.stream_id, event.error_code))
        elif isinstance(event, h2.events.WindowUpdated):
            self._server.window_event.set()

    async def _route(self, stream_id: int) -> None:
        """按路径分派（`/gated` 等 `release`，`/slow` 永不响应）。"""
        path = self.paths.get(stream_id, "")
        body = bytes(self.bodies.pop(stream_id, b""))
        if path == "/ok":
            await self.respond(stream_id, b"ok")
        elif path == "/echo":
            await self.respond(stream_id, body)
        elif path.startswith("/status/"):
            await self.respond(stream_id, b"nope", status=int(path.rsplit("/", 1)[1]))
        elif path == "/gated":
            await self._server.release.wait()
            await self.respond(stream_id, b"gated")
        elif path == "/drip":
            # 帧字节分两次写：客户端第一次读到的只是半帧，`receive_data()` 返回空事件列表
            # （覆盖「读到半帧」这条路径，真实网络的分包就是这样）。
            self.h2.send_headers(stream_id, [(b":status", b"200"), (b"content-length", b"5")])
            self.h2.send_data(stream_id, b"hello", end_stream=True)
            payload = self.h2.data_to_send()
            half = max(1, len(payload) // 2)
            self._writer.write(payload[:half])
            await self._writer.drain()
            await asyncio.sleep(0.05)
            self._writer.write(payload[half:])
            await self._writer.drain()
        elif path == "/reset":
            self.h2.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.INTERNAL_ERROR)
            self._flush()
        elif path.startswith("/goaway"):
            # `last >= 本流`：先把本流回完再 GOAWAY（「排空」形态）；`last < 本流`：只发 GOAWAY，
            # 让客户端这条流落入「未被受理」分支。
            last = int(path.rsplit("/", 1)[1]) if "/" in path[1:] else stream_id
            if last >= stream_id:
                await self.respond(stream_id, b"bye")
            self.h2.close_connection(error_code=h2.errors.ErrorCodes.NO_ERROR, last_stream_id=last)
            self._flush()
        elif path == "/boom":
            # 一个「看起来像帧、但违反协议」的 9 字节帧头（SETTINGS 的 stream_id 必须为 0）：
            # h2 立刻抛 ProtocolError。未知帧类型不行——h2 按 RFC 直接忽略，表现为超时。
            self._writer.write(bytes([0, 0, 0, 4, 0, 0, 0, 0, 1]))
        elif path == "/close":
            self._writer.close()
        # "/slow"：什么都不做（服务端永不响应，用来制造「在途」）

    async def respond(self, stream_id: int, payload: bytes, *, status: int = 200) -> None:
        """回一个一元响应（头 + 按窗口分片的体 + `END_STREAM`）。

        按窗口分片是必须的：服务端对每个流也有 64 KiB 出窗口，一次性 `send_data(200 KB)` 会
        `FlowControlError`；真实服务端同样要靠客户端 `WINDOW_UPDATE` 推动。

        Args:
            stream_id: h2 流号。
            payload: 响应体。
            status: 状态码。
        """
        self.h2.send_headers(
            stream_id,
            [(b":status", str(status).encode()), (b"content-length", str(len(payload)).encode())],
            end_stream=not payload,
        )
        view = memoryview(payload)
        while view:
            window = self.h2.local_flow_control_window(stream_id)
            while window <= 0:
                await self._server.window_event.wait()
                self._server.window_event.clear()
                window = self.h2.local_flow_control_window(stream_id)
            chunk = view[: min(window, self.h2.max_outbound_frame_size)]
            view = view[len(chunk) :]
            self.h2.send_data(stream_id, bytes(chunk), end_stream=not view)
            self._flush()
            await self._writer.drain()

    def _flush(self) -> None:
        """把攒的帧写出。"""
        data = self.h2.data_to_send()
        if data:
            self._writer.write(data)


class H2TestServer:
    """裸 h2 服务端（真 TCP / 可选 TLS），记录收帧供用例断言。"""

    def __init__(
        self,
        *,
        tls: ssl.SSLContext | None = None,
        max_concurrent_streams: int | None = None,
        settings_without_max: bool = False,
        pause_acks: bool = False,
    ) -> None:
        """初始化（尚未监听）。

        Args:
            tls: 服务端 TLS 上下文；None 表示 h2c。
            max_concurrent_streams: 握手后下发的并发流上限；None 表示不下发。
            settings_without_max: 是否下发一份「不含并发流上限」的 SETTINGS（覆盖那条分支）。
            pause_acks: 是否停止回 `WINDOW_UPDATE`（让客户端发送窗口卡住，用于中止在途发送）。
        """
        self._tls = tls
        self.max_concurrent_streams = max_concurrent_streams
        self.settings_without_max = settings_without_max
        self.pause_acks = pause_acks
        self.frames: list[tuple[str, int, int]] = []
        self.tasks: set[asyncio.Task[None]] = set()
        self.window_event = asyncio.Event()
        self.connections = 0
        self.release = asyncio.Event()
        self._server: asyncio.base_events.Server | None = None
        self.port = 0

    async def __aenter__(self) -> H2TestServer:
        """启动监听。

        Returns:
            H2TestServer: 自身。
        """
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0, ssl=self._tls)
        self.port = int(self._server.sockets[0].getsockname()[1])
        return self

    async def __aexit__(self, *_: object) -> None:
        """停止监听（连接由对端关闭）。"""
        if self._server is not None:
            self._server.close()
            with suppress(OSError):
                await self._server.wait_closed()

    @property
    def scheme(self) -> str:
        """监听协议（`https` / `http`）。

        Returns:
            str: scheme。
        """
        return "https" if self._tls is not None else "http"

    @property
    def url(self) -> str:
        """实例基址。

        Returns:
            str: `scheme://127.0.0.1:port`。
        """
        return f"{self.scheme}://127.0.0.1:{self.port}"

    def reset_recorded(self, stream_id: int) -> bool:
        """是否收到过指定流的 `RST_STREAM`。

        Args:
            stream_id: h2 流号。

        Returns:
            bool: 收到为 True。
        """
        return any(kind == "reset" and sid == stream_id for kind, sid, _ in self.frames)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """单连接入口。

        Args:
            reader: 输入流。
            writer: 输出流。
        """
        self.connections += 1
        connection = _Connection(self, reader, writer)
        with suppress(OSError):
            await connection.serve()
        for task in list(self.tasks):
            task.cancel()
        self.tasks.clear()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()
