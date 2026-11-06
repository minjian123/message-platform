"""RPC 客户端传输层协议（任务 04-7-1 / 04-3-2）：值对象、异常梯度与 `RpcTransport` 契约。

**为什么单独一层**：`client/rpc.py` 只该管「怎么调用」（帧编码、预算、错误码），不该管「怎么发字节」。
把传输抽成协议后，生产实现（`pool.H2Transport`）与测试实现（`tests_support.rpc_transport`）走同一份
契约，`RpcClient` 里不再有「测试时才走的分支」——04-2 的教训正是「测试全绿、真机不发 `RST_STREAM`」。

**为什么不复用 `httpx` / `httpcore`**（实测结论，见 04-7-1 详细设计 §2）：`httpx 0.28.1` 在等待响应头
期间被取消时**不发 `RST_STREAM`**；`httpcore 1.0.9` 没有公开的流重置出口（`stream_id` 只能从
`_events` 反推），要摸 5 处私有属性。精确取消依赖「拿到单条 stream 并发 RST」，只能自持连接。

**取消不走异常梯度**：`asyncio.CancelledError` 原样穿透本层与 `RpcClient`，不包装、不吞。取消的
接线已在 04-7-2 落地：`H2Transport.send()` 的取消 / 超时路径调 `H2Stream.abort()` 发
`RST_STREAM(CANCEL)`，并在同一处记 `msg_rpc_client_cancel`（`rst_sent` / `stream_id` / `reason`）。

**流式只加一类能力（04-3-2）**：`open_stream()` 给出可逐块 `read()` 的 `TransportStream`。它返回
**异步上下文管理器**而不是裸对象——发请求要取连接（可能还要建连），是异步的；把「进入 = 发请求」
放进 `__aenter__`，调用方写 `async with` 就够，不必 `async with await client.stream(...)`。一元
`send()` 与池行为**一个字节不改**（两条路径共用同一份连接与异常梯度）。
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from bms_core.core.roots import BaseValueObject

__all__ = [
    "RpcHttpRequest",
    "RpcHttpResponse",
    "RpcRequestTrace",
    "RpcTransport",
    "RpcTransportClosedError",
    "RpcTransportError",
    "RpcTransportProtocolError",
    "RpcTransportTimeoutError",
    "RpcTransportUnsupportedError",
    "TransportStats",
    "TransportStream",
]


class RpcTransportError(Exception):
    """传输层错误基类（与业务判定无关：码与分类一律由 `RpcClient` 映射）。"""


class RpcTransportTimeoutError(RpcTransportError):
    """连接 / 等响应头 / 读体超时（`RpcClient` 映射为 `RpcCallTimeoutError`，`350004`）。"""


class RpcTransportClosedError(RpcTransportError):
    """连接被关闭、GOAWAY 拒收、对端 reset 本流、连接池满（映射为 transport 层 `RpcCallError`）。"""


class RpcTransportProtocolError(RpcTransportError):
    """帧不可解析、缺 `:status`、ALPN 未协商到 `h2`（映射为 protocol 层 `RpcCallError`）。"""


class RpcTransportUnsupportedError(RpcTransportError):
    """origin 的 scheme 不是 `http` / `https`（本地先拦，不发网络）。"""


@dataclass(frozen=True)
class RpcRequestTrace(BaseValueObject):
    """日志关联用的请求标识：传输层不解释语义，只把它转发给日志（04-7-2 §5.3）。"""

    request_id: str
    """请求标识（帧头同名字段，服务端 `msg_rpc` 日志可对账）。"""

    service: str
    """服务标识。"""

    method: str
    """方法名。"""


@dataclass(frozen=True)
class RpcHttpRequest(BaseValueObject):
    """一次一元调用的传输层入参（与 RPC 语义无关，只有 HTTP/2 需要的东西）。"""

    origin: str
    """实例基址（`scheme://host:port`，不带尾斜线）：池键。"""

    target: str
    """请求路径（如 `/rpc/demo/echo`）。"""

    headers: tuple[tuple[str, str], ...]
    """请求头（元组而非 dict：头部有序且可多值，dict 会把「同名覆盖」变成隐式行为）。"""

    body: bytes
    """请求体（帧字节）。"""

    timeout_ms: int
    """本地超时（毫秒；`RpcClient` 按「预算 + 余量」算出）。"""

    trace: RpcRequestTrace | None = None
    """日志关联信息（`None` 表示调用方不要求关联，如健康探测）。"""


@dataclass(frozen=True)
class RpcHttpResponse(BaseValueObject):
    """一次一元调用的传输层出参（状态码语义由 `RpcClient` 判，传输层不解释）。"""

    status: int
    """HTTP 状态码。"""

    headers: tuple[tuple[str, str], ...]
    """响应头（`str` 形态，已按 latin-1 解码）。"""

    body: bytes
    """响应体（一元：已读满 `END_STREAM`）。"""

    stream_id: int
    """本条响应对应的 h2 流号（观测与用例断言；测试传输给 0）。"""


@dataclass(frozen=True)
class TransportStats(BaseValueObject):
    """传输层快照（可断言的事实：连接数 / 在途流数）。"""

    connections: int
    """当前存活的连接数（按 origin 各一条）。"""

    open_streams: int
    """当前在途（未收 `END_STREAM` 且未中止）的流数。"""


class TransportStream(Protocol):
    """一条已发出请求、可逐块读的流（04-3-2 流式消费的唯一传输面）。

    **只承诺三件事**：本流的 h2 流号、读到一块字节或「没有了」、发起中止。切帧、序号、结束三态
    一律归句柄层（`client/stream.py`）——传输层不认识帧，把帧语义塞回来会让它依赖 `protocol.frame`
    （与 04-7-1「协议与字节分开」同一取向）。

    **不暴露 `close_reason`**：句柄层只需要「RST 发出去没有」（即 `abort()` 的返回值）；细分状态留在
    `H2Stream` 里由传输层日志消费，与 04-7-2 §5.3 同取向。
    """

    @property
    def stream_id(self) -> int:
        """本条响应对应的 h2 流号（观测与用例断言；进程内替身给 0）。

        Returns:
            int: 流号。
        """
        ...

    async def read(self) -> bytes | None:
        """读下一块响应体字节。

        **用 `None` 表示结束而不是抛 `StopAsyncIteration`**：传输层不认识迭代协议；`None` 是最小的
        「没有更多字节」表达，句柄层再翻译成 `StopAsyncIteration`。读完一块就可以继续读下一块，
        不需要调用方记账。

        Returns:
            bytes | None: 下一块字节；None 表示已收 `END_STREAM`。

        Raises:
            RpcTransportClosedError: 本流被对端 reset / 本地已中止 / 连接故障。
            RpcTransportProtocolError: 连接级协议异常（`h2` 状态机拒绝）。
            asyncio.CancelledError: 调用方取消（原样穿透）。
        """
        ...

    def abort(self, reason: str | None = None) -> bool:
        """本地中止本流（取消 / 超时 / 解析错）：发 `RST_STREAM(CANCEL)`。

        Args:
            reason: 中止原因（`cancelled` / `timeout` / `error`；None 表示只清理、不记原因）。

        Returns:
            bool: 本次是否真的发出了 `RST_STREAM`（已收口的流返回 False，不发帧）。
        """
        ...


class RpcTransport(Protocol):
    """RPC 传输层契约（生产 `H2Transport`，测试 `AsgiRpcTransport` / `ScriptedRpcTransport`）。"""

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """发送一次一元调用并读满响应。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 响应（非 2xx 也照常返回，状态码语义归调用方）。

        Raises:
            RpcTransportTimeoutError: 连接 / 等头 / 读体超时。
            RpcTransportClosedError: 连接被关闭、GOAWAY、对端 reset、池不可用。
            RpcTransportProtocolError: 帧或 ALPN 协商异常。
            RpcTransportUnsupportedError: scheme 不受支持。
            asyncio.CancelledError: 调用方取消（原样穿透；本流已被 `RST_STREAM(CANCEL)` 中止）。
        """
        ...

    def open_stream(self, request: RpcHttpRequest) -> AbstractAsyncContextManager[TransportStream]:
        """打开一条流式请求（**进入上下文时才发请求**）。

        `request.timeout_ms` 在本方法里只约束「建连 + 发请求 + 等到响应头」这一段：整条流的期限由
        服务端预算兜底，客户端不套整段本地超时（04-3-2 §5.5）——把一元的「预算 + 余量」包到流上，
        预算 30s 的流 30s 后必被本地掐断，长流等于不可用。

        Args:
            request: 传输层入参（`target` 为 `/rpc/{service}/{method}/stream`）。

        Returns:
            AbstractAsyncContextManager[TransportStream]: 进入后拿到可 `read()` / `abort()` 的流。

        Raises:
            RpcTransportTimeoutError: 建连 / 发请求 / 等响应头超时。
            RpcTransportClosedError: 连接不可用、对端 reset、流端点返回非 200。
            RpcTransportProtocolError: ALPN 未协商到 `h2`。
            RpcTransportUnsupportedError: scheme 不受支持。
        """
        ...

    async def aclose(self) -> None:
        """释放全部连接与后台任务（幂等）。"""
        ...

    def stats(self) -> TransportStats:
        """取当前快照（用例断言「连接没涨 / 在途流归零」用）。

        Returns:
            TransportStats: 连接数与在途流数。
        """
        ...
