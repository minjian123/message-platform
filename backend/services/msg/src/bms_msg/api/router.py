"""msg 服务业务路由聚合：模块路由经服务级登记表统一挂到 `/api/v1` 或根路径。

探针路由（`/` `/healthz` `/readyz`）由共享应用基座统一挂载，不在此处登记。

**两条聚合（任务 03-1）**：

- `api_router`（`/api/v1`）：管理面主题 / 组 / 延迟 / 重放路由 + 数据面消息路由 + 成员路由；
- `frame_router`（根路径）：数据面三端点（复用同一规格）+ SSE 长驻流 `/msg/subscribe` +
  连接元数据 `/msg/connection`——HTTP/2 帧入口的固定位置。

两条聚合各自建登记表（`mount_service_routers` 每次新建 `RouterRegistry`），路由 `key` 不同即可共存；
RPC / 其余控制面路由随对应任务追加。
"""

from bms_core.api.base import BaseRouter, mount_service_routers
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.api.admin import delayed as admin_delayed
from bms_msg.api.admin import groups as admin_groups
from bms_msg.api.admin import replay as admin_replay
from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.api.frame import routes as frame_routes

api_router = mount_service_routers(
    ConcurrentStableList[BaseRouter](
        [
            topics.router,
            messages.router,
            data_groups.router,
            admin_groups.router,
            admin_delayed.router,
            admin_replay.router,
        ]
    )
)
"""本服务 `/api/v1` 业务路由聚合。"""

frame_router = mount_service_routers(
    ConcurrentStableList[BaseRouter]([frame_routes.router]),
    prefix="",
)
"""本服务根路径数据面聚合（`/msg/*`；HTTP/2 帧入口，不挂 `/api/v1`）。"""
