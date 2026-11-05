"""RPC 帧响应装配（任务 04-1）：成功结果帧。

**为什么结果帧不复用 `api/frame/responses.py::result_frame`**：那个函数按数据面四端点的结果模型
（`PublishResult` / `ConsumeResult` / `AckResult` / `ConnectionInfo`）分支装配 header，RPC 的结果是
「原始字节 + 结构化头」，塞进去会多一个与四端点无关的分支，且要让 `api/frame` 依赖 `rpc` 的类型
（方向反了）。两边共用的是同一条铁律：**响应 = 请求同 type + `flags.response`**。

错误帧仍然复用 `error_frame`：错误语义（码 → 层 → 重试）只有一份，RPC 不另立一套。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.protocol.frame import PROTOCOL_VERSION, FrameFlags, FrameType, encode_frame
from bms_msg.rpc.registry import RpcReply

__all__ = ["rpc_result_frame"]


def rpc_result_frame(*, request_id: str, service: str, method: str, reply: RpcReply) -> bytes:
    """编码 RPC 成功结果帧（`type=rpc` + response 位）。

    Args:
        request_id: 请求标识（回显；客户端据此对齐多路调用）。
        service: 服务标识（回显，便于客户端断言「响应确实是发给这个服务的」）。
        method: 方法名（回显）。
        reply: 处理结果。

    Returns:
        bytes: 完整帧字节（header 带 `length`，payload 为结果原始字节）。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    header.set("service", service)
    header.set("method", method)
    header.set("length", len(reply.payload))
    header.set("headers", dict(reply.headers.items()))
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.RESPONSE,
        header=header,
        payload=reply.payload,
    )
