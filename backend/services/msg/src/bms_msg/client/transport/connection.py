"""单条 h2 连接（任务 04-7-1）：握手、读取任务、事件分派、流表与流控。

**单连接只有一条读取任务**：`reader.read()` → `receive_data()` → 按事件分派给各流。好处是 h2 状态机
永远在同一个协程里被推进，不需要为它加锁，也不会出现「两条流各自读半帧」；读取任务本身的异常则
一次性唤醒该连接**全部**在途流（半连接复用比整条重建危险得多）。

**写路径不加锁**：所有写都是「同步 mutate h2 + 同步 `writer.write()`」一对，中间不 `await`，
所以不会与他流交错成半帧；`drain()` 单独 await（只做背压，不参与原子性）。这与「持锁跨越等窗口」
的写法相反——后者会和读取任务抢锁：读任务拿不到锁就停止读取，而发送方正等的窗口更新又在读取路径
上，直接死锁。

**并发流上限用「在途数 + 事件」而不是信号量**：远端 `SETTINGS_MAX_CONCURRENT_STREAMS` 可以在
连接中途调小，信号量只能加不能减（硬减要么在读取任务里 await 抢锁、要么漏放）。改成「等容量」
循环后，读取任务只负责置事件，不会阻塞自己。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

from bms_msg.client.transport.base import (
    RpcHttpRequest,
    RpcTransportClosedError,
    RpcTransportError,
)
from bms_msg.client.transport.stream import H2Stream

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查期
    from asyncio import StreamReader, StreamWriter

__all__ = ["H2ClientConnection"]

_DEFAULT_MAX_STREAMS = 100
"""并发流缺省上限（与 `httpcore` / `h2` 的缺省一致；对端 `SETTINGS` 更小时取更小值）。"""

_RECEIVE_WINDOW_BYTES = 16 * 1024 * 1024
"""本端接收窗口（一次提到 16 MiB）：一元响应上限 1 MiB，逐帧调窗会白花一串往返。"""


class H2ClientConnection:
    """一条 HTTP/2 客户端连接（一元的多路复用载体）。"""

    READ_NUM_BYTES = 64 * 1024
    """每次从 socket 读的字节数。"""

    def __init__(
        self,
        *,
        origin: str,
        authority: str,
        scheme: str,
        reader: StreamReader,
        writer: StreamWriter,
    ) -> None:
        """初始化（不做任何 IO：握手在 `start()`）。

        Args:
            origin: 实例基址（池键，仅用于日志）。
            authority: `:authority` 伪头取值（`host:port`）。
            scheme: `:scheme` 伪头取值（`http` / `https`）。
            reader: 已建立的输入流。
            writer: 已建立的输出流。
        """
        self._origin = origin
        self._authority = authority.encode("ascii")
        self._scheme = scheme.encode("ascii")
        self._reader = reader
        self._writer = writer
        self._h2 = h2.connection.H2Connection()
        self._streams: dict[int, H2Stream] = {}
        self._window_event = asyncio.Event()
        self._capacity_event = asyncio.Event()
        self._reader_task: asyncio.Task[None] | None = None
        self._max_streams = _DEFAULT_MAX_STREAMS
        self._closed = False
        self._draining = False
        self._last_used_at = time.monotonic()

    # ------------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        """握手：发 preface + `SETTINGS`、放大本端窗口，然后起读取任务。"""
        self._h2.initiate_connection()
        self._h2.increment_flow_control_window(_RECEIVE_WINDOW_BYTES)
        self._flush()
        self._reader_task = asyncio.create_task(self._reader_loop())

    async def aclose(self) -> None:
        """关闭连接（发 `GOAWAY`、唤醒在途流、取消读取任务、关 socket）；幂等。"""
        if self._closed:
            return
        self._h2.close_connection()
        self._flush()
        self._fail(RpcTransportClosedError(f"连接已关闭：{self._origin}"))
        if self._reader_task is not None:
            self._reader_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        self._writer.close()
        with suppress(OSError):
            await self._writer.wait_closed()

    @property
    def is_available(self) -> bool:
        """连接是否还能承接新流（未关闭且未进入 `GOAWAY` 收尾）。

        Returns:
            bool: 可用为 True。
        """
        return not self._closed and not self._draining

    @property
    def open_streams(self) -> int:
        """在途流数（已收口的不计入）。

        Returns:
            int: 在途流数。
        """
        return len(self._streams)

    def is_idle_for(self, seconds: float) -> bool:
        """连接是否已空闲超过给定秒数（无在途流才算空闲）。

        Args:
            seconds: 空闲阈值（秒）。

        Returns:
            bool: 超时为 True。
        """
        if self._streams:
            return False
        return time.monotonic() - self._last_used_at > seconds

    # ------------------------------------------------------------------ 发送

    async def send_request(self, request: RpcHttpRequest) -> H2Stream:
        """发一次一元请求（头 + 体），返回可等待结果的流。

        Args:
            request: 传输层入参。

        Returns:
            H2Stream: 已发出请求的流。

        Raises:
            RpcTransportClosedError: 连接不可用（已关闭 / GOAWAY）；或 stream id 用尽。
            asyncio.CancelledError: 发送期间被取消（流会被中止，不留半截）。
        """
        if not self.is_available:
            raise RpcTransportClosedError(f"连接不可用：{self._origin}")
        await self._wait_for_capacity()
        try:
            stream_id = self._h2.get_next_available_stream_id()
        except h2.exceptions.NoAvailableStreamIDError:  # pragma: no cover - 2^31 条流后不可达，属协议上限
            raise RpcTransportClosedError(f"h2 stream id 用尽：{self._origin}") from None
        stream = H2Stream(self, stream_id)
        self._streams[stream_id] = stream
        self._last_used_at = time.monotonic()
        self._h2.send_headers(stream_id, self._request_headers(request), end_stream=not request.body)
        self._flush()
        try:
            if request.body:
                await self._send_body(stream_id, request.body)
        except BaseException:
            self.abort_stream(stream_id)
            raise
        return stream

    def abort_stream(self, stream_id: int) -> None:
        """中止一条流（发 `RST_STREAM(CANCEL)` 并释放位）；已被读取任务收口时只返回。

        Args:
            stream_id: h2 流号。
        """
        if self._streams.pop(stream_id, None) is None:
            return
        self._capacity_event.set()
        # 连接已关闭时流表必为空（`_fail` 会清空并唤醒全部流），所以这里不需要额外的 `_closed` 判断。
        self._h2.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        self._flush()

    def _request_headers(self, request: RpcHttpRequest) -> list[tuple[bytes, bytes]]:
        """构造请求头（伪头 + `content-length` + 调用方给的普通头）。

        Args:
            request: 传输层入参。

        Returns:
            list[tuple[bytes, bytes]]: h2 头列表。
        """
        pairs: list[tuple[bytes, bytes]] = [
            (b":method", b"POST"),
            (b":authority", self._authority),
            (b":scheme", self._scheme),
            (b":path", request.target.encode("utf-8")),
            (b"content-length", str(len(request.body)).encode("ascii")),
        ]
        pairs.extend((name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in request.headers)
        return pairs

    async def _send_body(self, stream_id: int, body: bytes) -> None:
        """按窗口切片发送请求体，最后置 `END_STREAM`。

        Args:
            stream_id: h2 流号。
            body: 请求体字节。
        """
        view = memoryview(body)
        while view:
            window = await self._wait_for_window(stream_id)
            chunk = view[: min(window, self._h2.max_outbound_frame_size)]
            view = view[len(chunk) :]
            self._h2.send_data(stream_id, bytes(chunk))
            self._flush()
            await self._writer.drain()
        self._h2.end_stream(stream_id)
        self._flush()

    async def _wait_for_window(self, stream_id: int) -> int:
        """等到本流有可用发送窗口（对端初始 64 KiB，1 MiB 参数必须等 `WINDOW_UPDATE`）。

        Args:
            stream_id: h2 流号。

        Returns:
            int: 可用窗口字节数（> 0）。
        """
        while True:
            window = self._h2.local_flow_control_window(stream_id)
            if window > 0:
                return window
            await self._window_event.wait()
            self._window_event.clear()

    async def _wait_for_capacity(self) -> None:
        """等在途流数低于上限（远端 `SETTINGS` 调小时也不阻塞读取任务）。"""
        while len(self._streams) >= self._max_streams:
            await self._capacity_event.wait()
            self._capacity_event.clear()

    # ------------------------------------------------------------------ 接收

    async def _reader_loop(self) -> None:
        """读取任务：读字节 → 推进状态机 → 分派事件；异常即整条连接故障。"""
        try:
            while True:
                data = await self._reader.read(self.READ_NUM_BYTES)
                if not data:
                    raise RpcTransportClosedError(f"h2 连接被对端关闭：{self._origin}")
                self._dispatch(self._h2.receive_data(data))
                self._flush()
        except Exception as exc:
            self._fail(exc)
            self._writer.close()

    def _dispatch(self, events: Iterable[h2.events.Event]) -> None:
        """按事件类型更新连接 / 流状态。

        Args:
            events: `receive_data()` 返回的事件序列。
        """
        for event in events:
            if isinstance(event, h2.events.WindowUpdated):
                self._window_event.set()
            elif isinstance(event, h2.events.RemoteSettingsChanged):
                self._apply_settings(event)
            elif isinstance(event, h2.events.ConnectionTerminated):
                self._apply_goaway(event)
            elif isinstance(
                event,
                (
                    h2.events.ResponseReceived,
                    h2.events.DataReceived,
                    h2.events.StreamEnded,
                    h2.events.StreamReset,
                ),
            ):
                self._on_stream_event(event)
            # 其余事件（`SettingsAcknowledged` / `PingAcknowledged`）不带 `stream_id`，也无需处理。

    def _on_stream_event(self, event: Any) -> None:
        """把一条流事件落到对应的流上。

        `h2` 未提供 `StreamEvent` 类型，四类流事件各字段也不同，故按 `Any` 收口、逐类 `isinstance` 收窄。

        Args:
            event: 带 `stream_id` 的 h2 事件。
        """
        stream_id = cast("int", event.stream_id)
        stream = self._streams.get(stream_id)
        if stream is None:  # pragma: no cover - 实测（h2 4.4）对已 reset 流的迟到帧静默丢弃、不产出事件
            return
        if isinstance(event, h2.events.DataReceived):
            self._h2.acknowledge_received_data(event.flow_controlled_length, stream_id)
            stream.on_data(event.data)
        elif isinstance(event, h2.events.ResponseReceived):
            self._on_headers(stream, cast("list[tuple[bytes, bytes]]", event.headers))
        elif isinstance(event, h2.events.StreamEnded):
            stream.on_end()
            self._retire(stream_id)
        else:
            # 走到这里的只剩 `StreamReset`（`_dispatch` 只把四类流事件送进来）。
            reset = cast("h2.events.StreamReset", event)
            stream.on_reset(reset.error_code)
            self._retire(reset.stream_id)

    def _on_headers(self, stream: H2Stream, raw: list[tuple[bytes, bytes]]) -> None:
        """把 h2 的响应头折成 `(status, 普通头元组)` 交给流。

        Args:
            stream: 目标流。
            raw: `ResponseReceived.headers`（`h2` 保证 `ResponseReceived` 时非空，故调用方已 cast）。
        """
        status = 0
        headers: list[tuple[str, str]] = []
        for name, value in raw:
            if name == b":status":
                status = int(value.decode("ascii"))
            else:
                headers.append((name.decode("latin-1"), value.decode("latin-1")))
        stream.on_headers(status, tuple(headers))

    def _apply_settings(self, event: h2.events.RemoteSettingsChanged) -> None:
        """按对端 `SETTINGS` 调整并发流上限。

        Args:
            event: 远端设置变更事件。
        """
        changed = event.changed_settings.get(h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS)
        if changed is None:
            return
        self._max_streams = max(1, min(changed.new_value, _DEFAULT_MAX_STREAMS))
        self._capacity_event.set()

    def _apply_goaway(self, event: h2.events.ConnectionTerminated) -> None:
        """按 `GOAWAY.last_stream_id` 处置在途流：之后的立刻失败，之内的允许跑完。

        Args:
            event: 连接终止事件。
        """
        self._draining = True
        last_stream_id = event.last_stream_id or 0
        for stream_id in [sid for sid in self._streams if sid > last_stream_id]:
            stream = self._streams[stream_id]
            stream.on_connection_error(
                RpcTransportClosedError(f"连接已 GOAWAY（last_stream_id={last_stream_id}），本流未被受理")
            )
            self._retire(stream_id)

    def _retire(self, stream_id: int) -> None:
        """流正常/异常收口后释放位（不中止、不发帧）。

        Args:
            stream_id: h2 流号。
        """
        self._streams.pop(stream_id, None)
        self._capacity_event.set()

    def _fail(self, exc: BaseException) -> None:
        """整条连接故障：唤醒全部在途流并置为关闭。

        Args:
            exc: 触发故障的异常（非传输层异常一律包成 `RpcTransportClosedError`）。
        """
        self._closed = True
        error = exc if isinstance(exc, RpcTransportError) else RpcTransportClosedError(f"h2 连接异常：{exc}")
        for stream in list(self._streams.values()):
            stream.on_connection_error(error)
            self._retire(stream.stream_id)

    def _flush(self) -> None:
        """把状态机里攒的帧一次写出（同步：与 mutate 之间不 `await`，故无需锁）。"""
        data = self._h2.data_to_send()
        if data:
            self._writer.write(data)
