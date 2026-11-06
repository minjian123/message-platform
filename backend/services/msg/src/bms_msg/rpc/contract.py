"""RPC 契约片段（任务 04-1 / 04-2）：帧类型 / 响应约定 / 请求头字段 / 预算 / 方法清单。

由 `protocol.contract.frame_contract()` 摊平进 `x-msg-frame.rpc`——帧类型与 RPC 方法是同一层语义
（都是帧契约的一部分），放一处而不是新增顶层 `x-rpc-*`：拆成两个顶层键后，评审容易只看到一个。

**方法清单与运行期注册表同源**：两者都走 `default_rpc_registry()`，不可能出现「契约说有三个方法、
实际只注册了两个」。

**预算段只写算法与字段，不写死每跳毫秒数**（04-2 决策 14）：部署或实现一变不会导致契约漂移。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

from bms_msg.core.config import (
    RPC_DEADLINE_HEADER,
    RPC_DEADLINE_MAX_MS,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_RETURN_RESERVE_MS,
)
from bms_msg.errors import MsgErrorCode
from bms_msg.protocol.frame import FrameType
from bms_msg.rpc.builtin import default_rpc_registry

__all__ = ["rpc_contract"]


def _request_fields() -> tuple[ConcurrentStableList[str], ConcurrentStableList[str]]:
    """请求帧头的必填 / 可选字段（与 `api/rpc/gate.py` 的判定同一口径）。

    Returns:
        tuple: `(必填字段, 可选字段)`。
    """
    return (
        ConcurrentStableList(["request_id"]),
        ConcurrentStableList(
            [
                "service",
                "method",
                "caller",
                "trace_id",
                "tenant",
                "content_type",
                "protocol_version",
                "deadline_ms",
            ]
        ),
    )


def _methods() -> ConcurrentStableList[ConcurrentStableDict[str, str]]:
    """方法清单（对象数组；后续加 `idempotent` / `deadline_required` 不破坏形状）。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, str]]: 方法条目（插入序）。
    """
    methods: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
    for service, method in default_rpc_registry().methods():
        entry: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        entry.set("service", service)
        entry.set("method", method)
        methods.add(entry)
    return methods


def rpc_contract() -> ConcurrentStableDict[str, Any]:
    """构造 RPC 契约片段（供 `frame_contract()` 摊平）。

    Returns:
        ConcurrentStableDict[str, Any]: 单键 `rpc`，内含帧类型 / 响应约定 / 字段 / payload 规则 /
        错误码 / 方法清单。
    """
    required, optional = _request_fields()
    return ConcurrentStableDict(
        {
            "rpc": ConcurrentStableDict(
                {
                    "frame_type": int(FrameType.RPC),
                    "response": "same type + flags.response（与 result_frame / open_frame 同构）",
                    "request_required": required,
                    "request_optional": optional,
                    "payload": "调用参数原始字节（broker 不解析；上限 1 MiB；结果同限）",
                    "compression": "不支持：flags.compressed 置位或 header.compression 非 none 即 350018",
                    "deadline": ConcurrentStableDict(
                        {
                            "header": RPC_DEADLINE_HEADER,
                            "encoding": "relative-remaining-ms（相对剩余毫秒；跨机不需要时钟同步）",
                            "default_ms": RPC_DEFAULT_TIMEOUT_MS,
                            "range": ConcurrentStableList([0, RPC_DEADLINE_MAX_MS]),
                            "deduction": (
                                "每跳扣实际服务端耗时（收帧 → handler 入口，monotonic），"
                                "传下游前再减 return_reserve_ms；下游只接收剩余预算"
                            ),
                            "return_reserve_ms": RPC_RETURN_RESERVE_MS,
                            "on_exhausted": int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
                            "outcome": "unknown（超时是「结果未知」，不是失败；不得直接重试）",
                        }
                    ),
                    "response_optional": ConcurrentStableList(["used_ms", "remaining_ms"]),
                    "errors": ConcurrentStableDict(
                        {
                            "method_not_found": int(MsgErrorCode.RPC_METHOD_NOT_FOUND),
                            "deadline_exceeded": int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
                        }
                    ),
                    "methods": _methods(),
                }
            )
        }
    )
