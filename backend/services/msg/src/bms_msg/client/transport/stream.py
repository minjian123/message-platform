"""单条 h2 流（任务 04-7-1）：状态由连接的读取任务推进，调用方只 `await` 结果。

**一元语义**：本任务只做「一请求一响应」，所以流不需要「逐块读」的接口——`wait_response()` 等到
`END_STREAM` 或错误后一次返回。流式（多次读、背压消费、流内错误）归 04-3，届时在本类上加读队列，
而不是改连接层。

**中止（`abort`）是关键原语**：一元响应不需要 `RST_STREAM` 之外的任何东西就能让服务端停下
（04-2 的 50ms 断开探测据此生效）。已结束的流再 `abort` 必须不发帧——对已关闭流发 `RST_STREAM`
属协议违规，对端可能直接 `GOAWAY`，一条流的事故会放大成整条连接的事故。
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from bms_msg.client.transport.base import (
    RpcHttpResponse,
    RpcTransportClosedError,
    RpcTransportError,
)

if TYPE_CHECKING:  # pragma: no cover - 仅类型检查期
    from bms_msg.client.transport.connection import H2ClientConnection

__all__ = ["H2Stream"]


class H2Stream:
    """单条 h2 流（一请求一响应）。"""

    __slots__ = ("_body", "_connection", "_error", "_event", "_finished", "_headers", "_status", "stream_id")

    def __init__(self, connection: H2ClientConnection, stream_id: int) -> None:
        """初始化。

        Args:
            connection: 所属连接（中止时据此发 `RST_STREAM`）。
            stream_id: h2 流号（客户端 flink 侧分配，单调递增不复用）。
        """
        self._connection = connection
        self.stream_id = stream_id
        self._event = asyncio.Event()
        self._status = 0
        self._headers: tuple[tuple[str, str], ...] = ()
        self._body = bytearray()
        self._error: RpcTransportError | None = None
        self._finished = False

    # ------------------------------------------------------------------ 读取任务入口

    def on_headers(self, status: int, headers: tuple[tuple[str, str], ...]) -> None:
        """收到响应头。

        Args:
            status: `:status` 数值。
            headers: 普通响应头（已剔除伪头）。
        """
        self._status = status
        self._headers = headers
        self._event.set()

    def on_data(self, data: bytes) -> None:
        """收到响应体片段。

        Args:
            data: 片段字节。
        """
        # 不唤醒等待者：一元语义只等「结束」，body 累积过程中唤醒只会让等待者空转一轮。
        self._body.extend(data)

    def on_end(self) -> None:
        """收到 `END_STREAM`（正常结束）。"""
        self._finished = True
        self._event.set()

    def on_reset(self, error_code: int) -> None:
        """对端发来 `RST_STREAM`（本流被中止）。

        Args:
            error_code: h2 错误码。
        """
        self._error = RpcTransportClosedError(f"对端中止了 h2 流（error_code={error_code}）")
        self._finished = True
        self._event.set()

    def on_connection_error(self, exc: RpcTransportError) -> None:
        """连接级故障：把错误转给本流的等待者。

        Args:
            exc: 传输层错误（连接关闭 / 协议错）。
        """
        self._error = exc
        self._finished = True
        self._event.set()

    # ------------------------------------------------------------------ 调用方入口

    async def wait_response(self) -> RpcHttpResponse:
        """等到流收口并返回响应。

        **不带自己的超时**：超时只在 `H2Transport.send()` 一处判（`asyncio.timeout` 包住整段），
        否则同一份预算会写两遍、两处同时到点，排查时看不出到底谁先放弃。

        Returns:
            RpcHttpResponse: 响应（状态码原样给调用方判）。

        Raises:
            RpcTransportClosedError: 对端 reset 或连接故障。
        """
        while not self._finished:
            await self._event.wait()
            self._event.clear()
        if self._error is not None:
            raise self._error
        return RpcHttpResponse(
            status=self._status,
            headers=self._headers,
            body=bytes(self._body),
            stream_id=self.stream_id,
        )

    def abort(self) -> None:
        """本地中止本流（取消 / 超时 / 其它异常）：发 `RST_STREAM(CANCEL)`。

        幂等由连接侧流表判：已收口的流不在表里，`abort_stream()` 直接返回，不会重复发帧。
        """
        self._finished = True
        self._connection.abort_stream(self.stream_id)
