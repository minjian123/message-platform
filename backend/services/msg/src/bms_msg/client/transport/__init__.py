"""RPC 客户端传输层（任务 04-7-1）：协议、值对象、异常与 h2 实现。

出口只有本模块：`client/rpc.py` 只 import 这里，不认识 `h2`，也不认识 socket。
"""

from __future__ import annotations

from bms_msg.client.transport.base import (
    RpcHttpRequest,
    RpcHttpResponse,
    RpcRequestTrace,
    RpcTransport,
    RpcTransportClosedError,
    RpcTransportError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
    RpcTransportUnsupportedError,
    TransportStats,
)
from bms_msg.client.transport.pool import H2Transport

__all__ = [
    "H2Transport",
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
