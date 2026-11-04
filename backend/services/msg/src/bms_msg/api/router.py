"""msg 服务业务路由聚合：模块路由经服务级登记表统一挂到 `/api/v1`。

探针路由（`/` `/healthz` `/readyz`）由共享应用基座统一挂载，不在此处登记。
本阶段登记主题管理路由（`/msg/admin/topics`，任务 01-1）；数据面 / RPC / 其余控制面路由随对应任务追加。
"""

from bms_core.api.base import BaseRouter, mount_service_routers
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.api.admin import topics

api_router = mount_service_routers(ConcurrentStableList[BaseRouter]([topics.router]))
"""本服务业务路由聚合（统一挂 `/api/v1` 前缀）。"""
