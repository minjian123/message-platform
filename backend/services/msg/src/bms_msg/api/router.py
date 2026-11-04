"""msg 服务业务路由聚合：模块路由经服务级登记表统一挂到 `/api/v1`。

探针路由（`/` `/healthz` `/readyz`）由共享应用基座统一挂载，不在此处登记。
当前登记：管理面主题路由（`/msg/admin/topics`，任务 01-1）、数据面消息路由
（`/msg/publish`、`/msg/consume`、`/msg/ack`，任务 01-2）、数据面成员路由
（`/msg/join`、`/msg/heartbeat`、`/msg/leave`）与管理面组 / 延迟 / 重放视图
（`/msg/admin/groups`、`/msg/admin/delayed`、`/msg/admin/replay`，任务 01-4 / 01-5 / 01-6）；
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
"""本服务业务路由聚合（统一挂 `/api/v1` 前缀）。"""
