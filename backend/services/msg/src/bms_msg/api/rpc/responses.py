"""RPC 帧响应装配（任务 04-1 / 04-3-1）：成功结果帧与流式结果帧。

**为什么结果帧不复用 `api/frame/responses.py::result_frame`**：那个函数按数据面四端点的结果模型
（`PublishResult` / `ConsumeResult` / `AckResult` / `ConnectionInfo`）分支装配 header，RPC 的结果是
「原始字节 + 结构化头」，塞进去会多一个与四端点无关的分支，且要让 `api/frame` 依赖 `rpc` 的类型
（方向反了）。两边共用的是同一条铁律：**响应 = 请求同 type + `flags.response`**。

错误帧仍然复用 `error_frame`：错误语义（码 → 层 → 重试）只有一份，RPC 不另立一套。

**流式帧为什么另加一个函数而不是给 `rpc_result_frame` 加参数**：一元帧与流帧的字段集不同（流帧多
`seq`、可能置 `end_of_stream`），合并后一元路径要恒传「不是末帧」这类无意义实参，且下次再加流式字段
会把一元签名一起改；两个函数各自完整，一眼能看出两种形状。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.msgclient.frame import PROTOCOL_VERSION, FrameFlags, FrameType, encode_frame

from bms_msg.rpc.registry import RpcReply

__all__ = ["rpc_result_frame", "rpc_stream_frame"]


def rpc_result_frame(
    *,
    request_id: str,
    service: str,
    method: str,
    reply: RpcReply,
    used_ms: int,
    remaining_ms: int,
) -> bytes:
    """编码 RPC 成功结果帧（`type=rpc` + response 位）。

    Args:
        request_id: 请求标识（回显；客户端据此对齐多路调用）。
        service: 服务标识（回显，便于客户端断言「响应确实是发给这个服务的」）。
        method: 方法名（回显）。
        reply: 处理结果。
        used_ms: 本次调用已用毫秒（04-2 回传；只读）。
        remaining_ms: 本次调用剩余毫秒（04-2 回传；夹取到 0）。

    Returns:
        bytes: 完整帧字节（header 带 `length`，payload 为结果原始字节）。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    header.set("service", service)
    header.set("method", method)
    header.set("length", len(reply.payload))
    header.set("used_ms", used_ms)
    header.set("remaining_ms", remaining_ms)
    header.set("headers", dict(reply.headers.items()))
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.RESPONSE,
        header=header,
        payload=reply.payload,
    )


def rpc_stream_frame(
    *,
    request_id: str,
    service: str,
    method: str,
    reply: RpcReply,
    seq: int,
    used_ms: int,
    remaining_ms: int,
    end_of_stream: bool = False,
) -> bytes:
    """编码流式结果帧（`type=rpc` + response 位；末帧另置 `end_of_stream`）。

    **序号进帧头而不是 payload 前缀**：payload 必须保持「结果原始字节」——04-1 的契约、handler 的
    返回值、压缩与校验的将来接入都依赖这一点；放进 body 前 4 字节等于给 payload 加一层私有头，
    每个消费方都要记住偏移（04-3-1 决策 3）。

    Args:
        request_id: 请求标识（逐帧回显；客户端据此对齐多路流）。
        service: 服务标识（回显）。
        method: 方法名（回显）。
        reply: 本帧结果。
        seq: 帧序号（0 起逐帧 +1）。
        used_ms: 该帧产出时刻的已用毫秒（整条流的预算视图）。
        remaining_ms: 该帧产出时刻的剩余毫秒（夹取到 0）。
        end_of_stream: 是否为该流的末帧（正常结束的唯一标记）。

    Returns:
        bytes: 完整帧字节（header 带 `seq` / `length`）。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    header.set("request_id", request_id)
    header.set("protocol_version", PROTOCOL_VERSION)
    header.set("service", service)
    header.set("method", method)
    header.set("seq", seq)
    header.set("length", len(reply.payload))
    header.set("used_ms", used_ms)
    header.set("remaining_ms", remaining_ms)
    header.set("headers", dict(reply.headers.items()))
    flags = FrameFlags.RESPONSE | FrameFlags.END_OF_STREAM if end_of_stream else FrameFlags.RESPONSE
    return encode_frame(
        frame_type=int(FrameType.RPC),
        flags=flags,
        header=header,
        payload=reply.payload,
    )
