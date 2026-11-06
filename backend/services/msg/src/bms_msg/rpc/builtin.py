"""内置 RPC 处理器与缺省注册表（任务 04-1 / 04-3-1）。

**为什么要内置这几个**：端点、双形态、配额、客户端、流式都要一个「一定能跑通」的目标来验收。用真实
业务方法会把 RPC 骨架绑到某个业务域上（后续要跟着改），而纯字节处理器只依赖「入参字节 → 出参字节」
这条 RPC 唯一承诺：

- `demo.echo`：原样回传参数字节——验证「原始字节透传」与跨服务调用闭环；
- `demo.length`：回传十进制长度字节——验证「处理器能读参数并产生不同结果」；
- `demo.ticker`（流式，04-3-1）：按参数条数推 N 帧——验证帧序 / `seq` / 末帧 `end_of_stream` / 攒批；
- `demo.idle`（流式，04-3-1）：推 1 帧后不再产出——验证空闲超时（`350024`）与取消。

四者都不假定参数编码（不要求 JSON / UTF-8），因此不会因为「参数不是 JSON」而失败。`ticker` 的参数
是唯一例外（ASCII 十进制条数）：它需要一个「能按需造 N 帧」的旋钮，若改用 JSON 就要给 RPC 引入
JSON 依赖，与「RPC 只承诺字节」相悖；非法参数以 `ParamError` 抛出，正好让用例覆盖「流内契约错误」。

**单一构造入口**：装配（`main.py`）与契约导出（`rpc_contract()`）都调 `default_rpc_registry()`，
方法清单因此不可能与运行期注册表漂移。
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply

__all__ = [
    "DEMO_ECHO",
    "DEMO_IDLE",
    "DEMO_LENGTH",
    "DEMO_SERVICE",
    "DEMO_TICKER",
    "TICKER_MAX_FRAMES",
    "default_rpc_registry",
    "echo_handler",
    "idle_handler",
    "length_handler",
    "ticker_handler",
]

DEMO_SERVICE = "demo"
"""内置处理器所属服务标识。"""

DEMO_ECHO = "echo"
"""原样回传处理方法名。"""

DEMO_LENGTH = "length"
"""回传参数长度的方法名。"""

DEMO_TICKER = "ticker"
"""按参数条数推 N 帧的流式方法名（04-3-1）。"""

DEMO_IDLE = "idle"
"""推 1 帧后不再产出的流式方法名（04-3-1；空闲超时用）。"""

TICKER_MAX_FRAMES = 1000
"""`demo.ticker` 单次最多推的帧数：内置方法不该能被当成压测装置（越界按契约层 `10001` 拒绝）。"""


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


def _ticker_count(payload: bytes) -> int:
    """解析 `demo.ticker` 的条数参数（ASCII 十进制；空 = 0 帧）。

    Args:
        payload: 调用参数原始字节。

    Returns:
        int: 帧数（0 ~ `TICKER_MAX_FRAMES`）。

    Raises:
        ParamError: 非 ASCII 十进制整数，或超出 `0 ~ TICKER_MAX_FRAMES`（契约层 `10001`）。
    """
    if not payload:
        return 0
    try:
        count = int(payload.decode("ascii"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ParamError("demo.ticker 的参数须为 ASCII 十进制条数") from exc
    if count < 0 or count > TICKER_MAX_FRAMES:
        raise ParamError(f"demo.ticker 的条数超出范围 0~{TICKER_MAX_FRAMES}：{count}")
    return count


async def ticker_handler(call: RpcCall) -> AsyncIterator[RpcReply]:
    """按参数条数推 N 帧（第 i 帧 payload 为 `i` 的十进制文本，结果头带 `index`）。

    Args:
        call: 调用入参（`payload` 为 ASCII 十进制条数）。

    Yields:
        RpcReply: 逐帧结果。

    Raises:
        ParamError: 参数不是合法条数（`10001`，服务层转流内 ERROR 帧）。
    """
    count = _ticker_count(call.payload)
    for index in range(count):
        headers = _headers(f"{DEMO_SERVICE}.{DEMO_TICKER}")
        headers.set("index", str(index))
        yield RpcReply(payload=str(index).encode("ascii"), headers=headers)


async def idle_handler(call: RpcCall) -> AsyncIterator[RpcReply]:
    """推 1 帧后不再产出（用来验证流空闲超时与服务端收尾）。

    **为什么用「永不完成的事件」而不是 `sleep` 一个很大的数**：两者都能让服务端等不到下一帧，但
    `Event().wait()` 没有「到点自己醒」的第二条时间线——空闲判据只能由服务端触发，用例断言不会因为
    处理器自己醒来而时灵时不灵。

    Args:
        call: 调用入参（未使用）。

    Yields:
        RpcReply: 唯一一帧。
    """
    del call
    yield RpcReply(payload=b"idle", headers=_headers(f"{DEMO_SERVICE}.{DEMO_IDLE}"))
    await asyncio.Event().wait()


def default_rpc_registry() -> RpcRegistry:
    """构造缺省注册表（两个一元 + 两个流式内置处理器；装配与契约导出共用）。

    Returns:
        RpcRegistry: 已登记的注册表（`demo.echo` / `demo.length` 一元，`demo.ticker` / `demo.idle` 流式）。
    """
    registry = RpcRegistry()
    registry.register(DEMO_SERVICE, DEMO_ECHO, echo_handler)
    registry.register(DEMO_SERVICE, DEMO_LENGTH, length_handler)
    registry.register_stream(DEMO_SERVICE, DEMO_TICKER, ticker_handler)
    registry.register_stream(DEMO_SERVICE, DEMO_IDLE, idle_handler)
    return registry
