"""RPC 客户端传输层协议（任务 04-7-1）：值对象、异常梯度与 `RpcTransport` 契约。

**为什么单独一层**：`client/rpc.py` 只该管「怎么调用」（帧编码、预算、错误码），不该管「怎么发字节」。
把传输抽成协议后，生产实现（`pool.H2Transport`）与测试实现（`tests_support.rpc_transport`）走同一份
契约，`RpcClient` 里不再有「测试时才走的分支」——04-2 的教训正是「测试全绿、真机不发 `RST_STREAM`」。

**为什么不复用 `httpx` / `httpcore`**（实测结论，见 04-7-1 详细设计 §2）：`httpx 0.28.1` 在等待响应头
期间被取消时**不发 `RST_STREAM`**；`httpcore 1.0.9` 没有公开的流重置出口（`stream_id` 只能从
`_events` 反推），要摸 5 处私有属性。精确取消依赖「拿到单条 stream 并发 RST」，只能自持连接。

**取消不走异常梯度**：`asyncio.CancelledError` 原样穿透本层与 `RpcClient`，不包装、不吞。取消的
接线已在 04-7-2 落地：`H2Transport.send()` 的取消 / 超时路径调 `H2Stream.abort()` 发
`RST_STREAM(CANCEL)`，并在同一处记 `msg_rpc_client_cancel`（`rst_sent` / `stream_id` / `reason`）。
"""

from __future__ import annotations

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

    async def aclose(self) -> None:
        """释放全部连接与后台任务（幂等）。"""
        ...

    def stats(self) -> TransportStats:
        """取当前快照（用例断言「连接没涨 / 在途流归零」用）。

        Returns:
            TransportStats: 连接数与在途流数。
        """
        ...
