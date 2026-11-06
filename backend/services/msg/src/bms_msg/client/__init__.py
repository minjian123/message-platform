"""调用方侧出口（任务 04-1 / 04-2 / 04-3-2）：RPC 客户端（一元 + 流式句柄）。

**与 broker 侧解耦**：本包只依赖 `protocol`（帧编解码）、`rpc.registry`（`RpcReply` 值对象）与
`rpc.deadline`（预算判据，同样不依赖 broker），不 import `api/` / `broker/` / `main`——04-5 的
bms 适配只拿这一包就能发调用。

**流式从本包出口给 `RpcStream`**（而不是让调用方去 `client.stream` 摸）：`RpcClient.stream()` 的
返回类型就在这一层出现，入口与类型同处一处，SDK 封装（8B_03）只需依赖 `bms_msg.client`。
"""

from bms_msg.client.circuit import InProcessCircuitBreaker
from bms_msg.client.retry import CircuitPolicy, RetryBudget, RetryPolicy
from bms_msg.client.rpc import (
    InstanceProvider,
    RpcCallError,
    RpcCallTimeoutError,
    RpcClient,
    StaticInstanceProvider,
)
from bms_msg.client.stream import RpcStream
from bms_msg.errors import ErrorCategory, Outcome

__all__ = [
    "CircuitPolicy",
    "ErrorCategory",
    "InProcessCircuitBreaker",
    "InstanceProvider",
    "Outcome",
    "RetryBudget",
    "RetryPolicy",
    "RpcCallError",
    "RpcCallTimeoutError",
    "RpcClient",
    "RpcStream",
    "StaticInstanceProvider",
]
