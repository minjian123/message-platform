"""单条 h2 流（任务 04-7-1 / 04-7-2 / 04-3-2）：状态由连接的读取任务推进，调用方 `await` 结果或逐块读。

**两条读路径共存、互不改写**：

| 路径 | 入口 | 语义 |
| --- | --- | --- |
| 一元 | `wait_response()` | 等 `_finished` 后一次返回整段响应体（04-7-1） |
| 流式 | `read_chunk()` | 到一块读一块，`None` 表示已收 `END_STREAM`（04-3-2） |

两者共用 `_finished` / `_error` / `_close_reason` 这套「流什么时候完」的事实，**不新建第二套状态机**
——两套状态必然漂移，最后会出现「一元说完了、流式说还在」这种无法自证的日志。数据也只有一份：
`_chunks` 是字节块的唯一存放处，一元路径在收口后把它拼成整段。

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
from collections import deque
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
    """单条 h2 流（一请求一响应；流式时逐块读）。"""

    __slots__ = (
        "_chunk_event",
        "_chunks",
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
        self._chunk_event = asyncio.Event()
        self._status = 0
        self._headers: tuple[tuple[str, str], ...] = ()
        # 字节块只存一处：一元在收口时拼成整段，流式逐块取走。
        # **不设队列上限**：上限由 h2 窗口决定——`connection.py` 只在 `DataReceived` 时
        # `acknowledge_received_data()`，未读走的字节不会让窗口继续打开，服务端写就会被阻塞。
        # 若将来改成「读完再 ack」，这里必须同时加上限，否则无界缓冲。
        self._chunks: deque[bytes] = deque()
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
        """收到响应体片段（两块都记：一元拼整段、流式逐块取）。

        **唤醒读等待者**（04-3-2）：流式语义就是「到一块读一块」，不唤醒会让 `read_chunk()` 一直睡到
        下一块或收口。一元路径不读 `_chunk_event`，多一次 `set()` 无副作用——无人等待时事件保持置位，
        下次进 `wait()` 立即返回（这正是 `clear()` 放在 `wait()` 之前的原因）。

        Args:
            data: 片段字节。
        """
        if not data:
            # 空 DATA 帧（`h2.end_stream()` 就是这种形态）没有信息量：排进队列只会让读循环空转一圈，
            # 而「结束了」这件事由 `on_end()` 单独表达，不会因为丢掉空块而丢信息。
            return
        self._chunks.append(data)
        self._chunk_event.set()

    def on_end(self) -> None:
        """收到 `END_STREAM`（正常结束）。"""
        self._finished = True
        self._close_reason = CLOSE_DONE
        self._event.set()
        # 收口也要唤醒读者：只读 `_finished` 的循环靠这一下才会退出去看「结束了」。
        self._chunk_event.set()

    def on_reset(self, error_code: int) -> None:
        """对端发来 `RST_STREAM`（本流被中止）。

        Args:
            error_code: h2 错误码。
        """
        self._error = RpcTransportClosedError(f"对端中止了 h2 流（error_code={error_code}）")
        self._finished = True
        self._close_reason = CLOSE_PEER_RESET
        self._event.set()
        self._chunk_event.set()

    def on_connection_error(self, exc: RpcTransportError) -> None:
        """连接级故障：把错误转给本流的等待者。

        Args:
            exc: 传输层错误（连接关闭 / 协议错）。
        """
        self._error = exc
        self._finished = True
        self._close_reason = CLOSE_CONNECTION_ERROR
        self._event.set()
        self._chunk_event.set()

    # ------------------------------------------------------------------ 调用方入口

    async def wait_response(self) -> RpcHttpResponse:
        """等到流收口并返回响应（一元路径；语义与 04-7-1 一致）。

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
            body=b"".join(self._chunks),
            stream_id=self.stream_id,
        )

    async def wait_headers(self) -> int:
        """等到响应头（流式打开阶段用：非 200 与建连失败都在这里落地）。

        Returns:
            int: `:status` 数值。

        Raises:
            RpcTransportClosedError: 等头期间连接故障 / 对端 reset / 未收头即结束。
        """
        while self._status == 0 and not self._finished:
            await self._event.wait()
            self._event.clear()
        if self._error is not None:
            # 头没到就以错误收口：连接故障 / 对端 reset 都会先把 `_error` 放进来，原样给出去
            # ——「对端 reset 了」比「HTTP 0」有用得多。
            raise self._error
        return self._status

    async def read_chunk(self) -> bytes | None:
        """读下一块响应体字节（流式路径）。

        **顺序是先取队列、再看收口**：`END_STREAM` 之前到达的字节块必须全部交出去，否则末帧会丢。
        队列空且已收口时才回 `None`；带着 `_error` 收口（对端 reset / 连接故障 / 本地已中止）则抛错
        ——「悄悄结束」会让句柄层把一次故障误判成「对方没写收尾帧」。

        Returns:
            bytes | None: 下一块字节；None 表示已收 `END_STREAM`。

        Raises:
            RpcTransportError: 本流以错误收口（对端 reset / 连接故障 / 本地已中止）。
        """
        while True:
            if self._chunks:
                return self._chunks.popleft()
            if self._finished:
                if self._error is not None:
                    raise self._error
                return None
            self._chunk_event.clear()
            await self._chunk_event.wait()

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
            # 本地中止之后本流不再可读：让 `read_chunk()` / `wait_response()` 的第二个调用方
            # 拿到「被中止了」而不是「正常结束」（前者才是事实）。这里直接覆盖、不判 `_error`：
            # `sent` 为真意味着本流还在流表里（没被 reset / 连接故障收口过），此时不可能已有错误；
            # 即使将来出现，本地中止也是更晚、更准确的那条原因。
            self._error = RpcTransportClosedError(f"本流已被本地中止（reason={reason or 'cleanup'}）")
        # 无论发没发帧都要唤醒读者：正卡在 `read_chunk()` 的调用方不该等它自己的超时才醒。
        self._chunk_event.set()
        return sent
