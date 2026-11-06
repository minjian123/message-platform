"""RPC 契约片段（任务 04-1 / 04-2 / 04-3-1）：帧类型 / 响应约定 / 请求头字段 / 预算 / 方法清单 / 流式段。

由 `protocol.contract.frame_contract()` 摊平进 `x-msg-frame.rpc`——帧类型与 RPC 方法是同一层语义
（都是帧契约的一部分），放一处而不是新增顶层 `x-rpc-*`：拆成两个顶层键后，评审容易只看到一个。

**方法清单与运行期注册表同源**：两者都走 `default_rpc_registry()`，不可能出现「契约说有三个方法、
实际只注册了两个」。一元（`methods`）与流式（`stream.methods`）**分开两张清单**：混在一起会让客户端
以为所有方法都能流式调（04-3-1 决策 2 的配套口径）。

**预算段只写算法与字段，不写死每跳毫秒数**（04-2 决策 14）：部署或实现一变不会导致契约漂移。
**流式段同理只写阈值常量与错误码**，不写帧数上界（攒批是性能细节，不是对外承诺）。
"""

from __future__ import annotations

from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

from bms_msg.core.config import (
    RPC_DEADLINE_HEADER,
    RPC_DEADLINE_MAX_MS,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_RETURN_RESERVE_MS,
    RPC_STREAM_IDLE_MS,
)
from bms_msg.errors import MsgErrorCode
from bms_msg.protocol.frame import FrameType
from bms_msg.rpc.builtin import default_rpc_registry

__all__ = ["rpc_contract"]

STREAM_ENDPOINT = "POST /rpc/{service}/{method}/stream"
"""流式端点路径（契约与端点声明同值，写在一处避免两处漂移）。"""


def _request_fields() -> tuple[ConcurrentStableList[str], ConcurrentStableList[str]]:
    """请求帧头的必填 / 可选字段（与 `api/rpc/request.py` 的判定同一口径）。

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


def _method_entries(methods: tuple[tuple[str, str], ...]) -> ConcurrentStableList[ConcurrentStableDict[str, str]]:
    """把 `(service, method)` 序列编成契约里的对象数组（后续加字段不破坏形状）。

    Args:
        methods: `(service, method)` 序列（插入序）。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, str]]: 方法条目。
    """
    entries: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
    for service, method in methods:
        entry: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        entry.set("service", service)
        entry.set("method", method)
        entries.add(entry)
    return entries


def _methods() -> ConcurrentStableList[ConcurrentStableDict[str, str]]:
    """一元方法清单（`default_rpc_registry().methods()` 同源）。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, str]]: 方法条目（插入序）。
    """
    return _method_entries(default_rpc_registry().methods())


def _stream_methods() -> ConcurrentStableList[ConcurrentStableDict[str, str]]:
    """流式方法清单（`default_rpc_registry().stream_methods()` 同源）。

    Returns:
        ConcurrentStableList[ConcurrentStableDict[str, str]]: 方法条目（插入序）。
    """
    return _method_entries(default_rpc_registry().stream_methods())


def _stream_segment() -> ConcurrentStableDict[str, Any]:
    """构造 `rpc.stream` 段（端点 / 结束位 / 序号 / 空闲阈值 / 流内错误码 / 方法清单）。

    Returns:
        ConcurrentStableDict[str, Any]: 流式契约片段。
    """
    return ConcurrentStableDict(
        {
            "endpoint": STREAM_ENDPOINT,
            "request": "同一元请求帧（type=48，不置 response / end_of_stream）",
            "response": "同 type + flags.response；末帧另置 flags.end_of_stream",
            "sequence": "header.seq，0 起逐帧 +1（缺帧 / 乱序由客户端判协议错）",
            "idle_ms": RPC_STREAM_IDLE_MS,
            "errors": ConcurrentStableDict(
                {
                    "method_not_found": int(MsgErrorCode.RPC_METHOD_NOT_FOUND),
                    "deadline_exceeded": int(MsgErrorCode.RPC_DEADLINE_EXCEEDED),
                    "stream_idle": int(MsgErrorCode.RPC_STREAM_IDLE),
                }
            ),
            "methods": _stream_methods(),
        }
    )


def rpc_contract() -> ConcurrentStableDict[str, Any]:
    """构造 RPC 契约片段（供 `frame_contract()` 摊平）。

    Returns:
        ConcurrentStableDict[str, Any]: 单键 `rpc`，内含帧类型 / 响应约定 / 字段 / payload 规则 /
        错误码 / 方法清单 / 流式段。
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
                    "stream": _stream_segment(),
                }
            )
        }
    )
