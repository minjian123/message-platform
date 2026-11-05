"""调用方标识与 trace 的透传（任务 04-1）：HTTP 头优先，冲突只告警不拒。

**为什么不拒**：`caller` 不是鉴权字段（真正的授权归 05 域的服务令牌）。拒绝会给 SDK 多一条失败
分支而换不来安全；而 HTTP 头由网络栈 / 网关注入，可信度高于业务代码填的帧头 / JSON 体——这与
「租户以解析结果为准」同一取向：**可信来源优先，不可信来源只记录**。
"""

from __future__ import annotations

from bms_core.core.logging import get_logger
from fastapi import Request

from bms_msg.core.config import RPC_CALLER_HEADER, RPC_TRACE_HEADER

__all__ = ["CALLER_HEADER", "TRACE_HEADER", "resolve_caller", "resolve_trace_id"]

CALLER_HEADER = RPC_CALLER_HEADER
"""调用方服务标识的 HTTP 头（优先级高于帧头 / JSON 体的 `caller`；名字与客户端同源）。"""

TRACE_HEADER = RPC_TRACE_HEADER
"""链路标识的 HTTP 头（缺省时回落帧头 / JSON 体的 `trace_id`）。"""

_LOGGER = get_logger("bms_msg.rpc")


def _header_value(request: Request, name: str) -> str | None:
    """取 HTTP 头（去空白；空串视为未给）。

    Args:
        request: 当前请求。
        name: 头名。

    Returns:
        str | None: 头值；缺失或空串为 None。
    """
    raw = request.headers.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def resolve_caller(request: Request, declared: str | None) -> str | None:
    """解析调用方标识（HTTP 头优先，冲突告警）。

    Args:
        request: 当前请求。
        declared: 帧头 / JSON 体声明的调用方（可为 None）。

    Returns:
        str | None: 最终调用方标识。
    """
    header = _header_value(request, CALLER_HEADER)
    if header is not None and declared is not None and header != declared:
        # 只记不拒：两个值都进日志，排障时能看出「谁在冒充 / 谁没对齐配置」。
        _LOGGER.warning("msg_rpc_caller_conflict", header=header, declared=declared)
    if header is not None:
        return header
    return declared or None


def resolve_trace_id(request: Request, declared: str | None) -> str | None:
    """解析链路标识（HTTP 头优先；与 `caller` 同一优先级口径）。

    Args:
        request: 当前请求。
        declared: 帧头 / JSON 体声明的链路标识（可为 None）。

    Returns:
        str | None: 最终链路标识。
    """
    header = _header_value(request, TRACE_HEADER)
    if header is not None:
        return header
    return declared or None
