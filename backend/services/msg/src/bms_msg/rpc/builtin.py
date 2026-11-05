"""内置 RPC 处理器与缺省注册表（任务 04-1）。

**为什么要内置两个**：端点、双形态、配额、客户端都要一个「一定能跑通」的目标来验收。用真实业务
方法会把 RPC 骨架绑到某个业务域上（后续要跟着改），而两个纯字节处理器只依赖「入参字节 → 出参字节」
这条 RPC 唯一承诺：

- `demo.echo`：原样回传参数字节——验证「原始字节透传」与跨服务调用闭环；
- `demo.length`：回传十进制长度字节——验证「处理器能读参数并产生不同结果」。

两者都不假定参数编码（不要求 JSON / UTF-8），因此不会因为「参数不是 JSON」而失败。

**单一构造入口**：装配（`main.py`）与契约导出（`rpc_contract()`）都调 `default_rpc_registry()`，
方法清单因此不可能与运行期注册表漂移。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply

__all__ = [
    "DEMO_ECHO",
    "DEMO_LENGTH",
    "DEMO_SERVICE",
    "default_rpc_registry",
    "echo_handler",
    "length_handler",
]

DEMO_SERVICE = "demo"
"""内置处理器所属服务标识。"""

DEMO_ECHO = "echo"
"""原样回传处理方法名。"""

DEMO_LENGTH = "length"
"""回传参数长度的方法名。"""


def _headers(handler: str) -> ConcurrentStableDict[str, str]:
    """构造结果头（标明是哪个处理器产生的，用例与排障据此断言）。

    Args:
        handler: 处理器全名（`service.method`）。

    Returns:
        ConcurrentStableDict[str, str]: 结果头。
    """
    headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    headers.set("handler", handler)
    return headers


async def echo_handler(call: RpcCall) -> RpcReply:
    """原样回传参数字节（结果头标明 `demo.echo`）。

    Args:
        call: 调用入参。

    Returns:
        RpcReply: 结果（payload 与入参逐字节相同）。
    """
    return RpcReply(payload=call.payload, headers=_headers(f"{DEMO_SERVICE}.{DEMO_ECHO}"))


async def length_handler(call: RpcCall) -> RpcReply:
    """回传参数长度（十进制 ASCII 字节，如 `b"5"`）。

    Args:
        call: 调用入参。

    Returns:
        RpcReply: 结果（payload 为长度文本）。
    """
    return RpcReply(
        payload=str(len(call.payload)).encode("ascii"),
        headers=_headers(f"{DEMO_SERVICE}.{DEMO_LENGTH}"),
    )


def default_rpc_registry() -> RpcRegistry:
    """构造缺省注册表（两个内置处理器；装配与契约导出共用）。

    Returns:
        RpcRegistry: 已登记 `demo.echo` / `demo.length` 的注册表。
    """
    registry = RpcRegistry()
    registry.register(DEMO_SERVICE, DEMO_ECHO, echo_handler)
    registry.register(DEMO_SERVICE, DEMO_LENGTH, length_handler)
    return registry
