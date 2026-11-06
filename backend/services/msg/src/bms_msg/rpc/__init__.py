"""RPC 域出口（任务 04-1 / 04-2）：方法注册表 / 调用契约 / 名称校验 / 预算 / 取消复核。

**这里只导出「不依赖 broker 侧」的东西**：`RpcCall` / `RpcReply` / `RpcRegistry` / `ensure_rpc_name` /
`Deadline` / `validate_budget_ms` / `ensure_not_cancelled` 与服务端分发无关，客户端（`bms_msg.client`）
与 04-5 的 bms 适配都可以安全 import。

`invoke_rpc`（依赖 `broker.backpressure.RateLimiter`）与 `default_rpc_registry`（内置处理器）**不在此
转出**，由服务端自行从 `bms_msg.rpc.service` / `bms_msg.rpc.builtin` 直取——否则调用方进程会在
import 阶段就把 broker 侧拉进来。
"""

from bms_msg.rpc.cancel import ensure_not_cancelled
from bms_msg.rpc.deadline import Deadline, validate_budget_ms
from bms_msg.rpc.naming import ensure_rpc_name
from bms_msg.rpc.registry import (
    RpcCall,
    RpcHandler,
    RpcRegistrationError,
    RpcRegistry,
    RpcReply,
)

__all__ = [
    "Deadline",
    "RpcCall",
    "RpcHandler",
    "RpcRegistrationError",
    "RpcRegistry",
    "RpcReply",
    "ensure_not_cancelled",
    "ensure_rpc_name",
    "validate_budget_ms",
]
