"""调用方侧出口（任务 04-1）：RPC 客户端。

**与 broker 侧解耦**：本包只依赖 `protocol`（帧编解码）与 `rpc.registry`（`RpcReply` 值对象），
不 import `api/` / `broker/` / `main`——04-5 的 bms 适配只拿这一包就能发调用。
"""

from bms_msg.client.rpc import (
    InstanceProvider,
    RpcCallError,
    RpcCallTimeoutError,
    RpcClient,
    StaticInstanceProvider,
)

__all__ = [
    "InstanceProvider",
    "RpcCallError",
    "RpcCallTimeoutError",
    "RpcClient",
    "StaticInstanceProvider",
]
