"""RPC 接口层出口（任务 04-1）。"""

from bms_msg.api.rpc.routes import RPC_ERROR_CODES, RPC_PERMISSION, router

__all__ = ["RPC_ERROR_CODES", "RPC_PERMISSION", "router"]
