"""单条 h2 流（任务 04-7-1 / 04-7-2）：状态由连接的读取任务推进，调用方只 `await` 结果。

**一元语义**：本任务只做「一请求一响应」，所以流不需要「逐块读」的接口——`wait_response()` 等到
`END_STREAM` 或错误后一次返回。流式（多次读、背压消费、流内错误）归 04-3，届时在本类上加读队列，
而不是改连接层。

**中止（`abort`）是关键原语**：一元响应不需要 `RST_STREAM` 之外的任何东西就能让服务端停下
（04-2 的 50ms 断开探测据此生效）。已结束的流再 `abort` 必须不发帧——对已关闭流发 `RST_STREAM`
属协议违规，对端可能直接 `GOAWAY`，一条流的事故会放大成整条连接的事故。

**`close_reason`（04-7-2）**：流收口的原因只有本类知道——`done`（正常收 `END_STREAM`）、`peer_reset`
（对端 reset）、`connection_error`（整条连接故障）、`cancelled` / `timeout` / `error`（本地中止）。
它与 `msg_rpc_client_cancel` 的 `reason` 同源，保证「日志说超时、流说取消」这种自相矛盾不会出现；
`abort()` 的返回值（`rst_sent`）则直接回答「RST 到底发出去没有」。
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

__all__ = [
    "CLOSE_CANCELLED",
    "CLOSE_CONNECTION_ERROR",
    "CLOSE_DONE",
    "CLOSE_ERROR",
    "CLOSE_PEER_RESET",
    "CLOSE_TIMEOUT",
    "H2Stream",
    "abort_reason_of",
]

CLOSE_DONE = "done"
"""正常收口（收到 `END_STREAM`）。"""

CLOSE_PEER_RESET = "peer_reset"
"""对端发 `RST_STREAM` 中止本流。"""

CLOSE_CONNECTION_ERROR = "connection_error"
"""整条连接故障（读写出错、对端关连接、协议错）带下的收口。"""

CLOSE_CANCELLED = "cancelled"
"""调用方取消（`task.cancel()`）。"""

CLOSE_TIMEOUT = "timeout"
"""本地超时（`timeout_ms` 到点）。"""

CLOSE_ERROR = "error"
"""其它异常中止（编码 / 流控 / 未预期错误）。"""


def abort_reason_of(exc: BaseException) -> str:
    """把「导致中止的异常」折成 `close_reason` / 日志 `reason`（唯一判定处）。

    `CancelledError` 必须排在最前：它继承 `BaseException` 而不是 `Exception`，且 `asyncio.timeout`
    到点时抛的是 `TimeoutError`（不是 `CancelledError`），两者语义不同——「人不等了」与「等超了」
    在 `reason` 上必须可区分，否则 04-2 那套 `cancelled` / `deadline_exceeded` 的服务端分工在客户端
    没有对偶。

    Args:
        exc: 触发中止的异常。

    Returns:
        str: `cancelled` / `timeout` / `error`。
    """
    if isinstance(exc, asyncio.CancelledError):
        return CLOSE_CANCELLED
    if isinstance(exc, TimeoutError):
        return CLOSE_TIMEOUT
    return CLOSE_ERROR


class H2Stream:
    """单条 h2 流（一请求一响应）。"""

    __slots__ = (
        "_body",
        "_close_reason",
        "_connection",
        "_error",
        "_event",
        "_finished",
        "_headers",
        "_rst_sent",
        "_status",
        "stream_id",
    )

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
        self._close_reason: str | None = None
        self._rst_sent = False

    @property
    def close_reason(self) -> str | None:
        """本流为什么收口（未收口为 None；取值见模块常量 `CLOSE_*`）。

        Returns:
            str | None: 收口原因。
        """
        return self._close_reason

    @property
    def rst_sent(self) -> bool:
        """本流是否**已经**发出过 `RST_STREAM`（`abort()` 幂等，重复调用不会重置）。

        Returns:
            bool: 发过为 True。
        """
        return self._rst_sent

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
        self._close_reason = CLOSE_DONE
        self._event.set()

    def on_reset(self, error_code: int) -> None:
        """对端发来 `RST_STREAM`（本流被中止）。

        Args:
            error_code: h2 错误码。
        """
        self._error = RpcTransportClosedError(f"对端中止了 h2 流（error_code={error_code}）")
        self._finished = True
        self._close_reason = CLOSE_PEER_RESET
        self._event.set()

    def on_connection_error(self, exc: RpcTransportError) -> None:
        """连接级故障：把错误转给本流的等待者。

        Args:
            exc: 传输层错误（连接关闭 / 协议错）。
        """
        self._error = exc
        self._finished = True
        self._close_reason = CLOSE_CONNECTION_ERROR
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

    def abort(self, reason: str | None = None) -> bool:
        """本地中止本流（取消 / 超时 / 其它异常）：发 `RST_STREAM(CANCEL)`。

        幂等由连接侧流表判：已收口的流不在表里，`abort_stream()` 直接返回，不会重复发帧。

        **返回值就是「这一次是否发了帧」**：`False` 只出现在「本流已经收口、没有可中止的东西」时；
        这时 `close_reason` 与 `rst_sent` 都保持原值（例如正常收口的 `done` 不会被后来的 `abort()`
        改写成 `cancelled`，已经发过的 RST 也不会因为二次调用被记成没发）。

        Args:
            reason: 中止原因（`CLOSE_CANCELLED` / `CLOSE_TIMEOUT` / `CLOSE_ERROR`）；
                None 表示调用方只做清理、不记录原因。

        Returns:
            bool: 本次是否真的发出了 `RST_STREAM`（累计结果见 `rst_sent`）。
        """
        self._finished = True
        sent = self._connection.abort_stream(self.stream_id)
        if sent:
            self._rst_sent = True
            if reason is not None:
                self._close_reason = reason
        return sent
