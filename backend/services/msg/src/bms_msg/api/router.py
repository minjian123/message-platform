"""msg 服务业务路由聚合：模块路由经服务级登记表统一挂到 `/api/v1` 或根路径。

探针路由（`/` `/healthz` `/readyz`）由共享应用基座统一挂载，不在此处登记。

**两条聚合（任务 03-1 / 03-4）**：

- `api_router`（`/api/v1`）：管理面主题 / 组 / 延迟 / 重放 / 配额投影路由 + 数据面消息路由 + 成员路由；
- `frame_router`（根路径）：数据面三端点（复用同一规格）+ SSE 长驻流 `/msg/subscribe` +
  连接自报 `/msg/connection` + 连接心跳 `/msg/connection/heartbeat`——HTTP/2 帧入口的固定位置。

两条聚合各自建登记表（`mount_service_routers` 每次新建 `RouterRegistry`），路由 `key` 不同即可共存；
RPC 已随 04-1 追加到根路径聚合（帧形态与 JSON 形态同一实现）、随 04-3-1 追加流式路由（独立
`stream_router`，不带一元帧闸门），其余控制面路由随对应任务追加。
"""

from bms_core.api.base import BaseRouter, mount_service_routers
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.api.admin import acls as admin_acls
from bms_msg.api.admin import delayed as admin_delayed
from bms_msg.api.admin import groups as admin_groups
from bms_msg.api.admin import quota as admin_quota
from bms_msg.api.admin import replay as admin_replay
from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.api.frame import connection as frame_connection
from bms_msg.api.frame import routes as frame_routes
from bms_msg.api.rpc import routes as rpc_routes
from bms_msg.api.rpc import stream as rpc_stream

api_router = mount_service_routers(
    ConcurrentStableList[BaseRouter](
        [
            topics.router,
            messages.router,
            data_groups.router,
            admin_groups.router,
            admin_delayed.router,
            admin_acls.router,
            admin_replay.router,
            admin_quota.router,
            admin_quota.admin_router,
        ]
    )
)
"""本服务 `/api/v1` 业务路由聚合（数据面四端点带连接守卫：与根路径同口径，网关路径不构成绕过口）。"""

frame_router = mount_service_routers(
    ConcurrentStableList[BaseRouter](
        [
            frame_routes.router,
            frame_routes.connection_router,
            frame_connection.router,
            rpc_routes.router,
            rpc_stream.stream_router,
        ]
    ),
    prefix="",
)
"""本服务根路径接入面聚合（`/msg/*` 与 `/rpc/{service}/{method}[/stream]`；HTTP/2 入口，不挂 `/api/v1`）。

五条路由各带各的守卫：数据面 + 长驻流（判上限）、连接自报（不判上限，排障入口）、心跳（判上限并刷新心跳时间）、
RPC 一元（连接守卫 + `msg:rpc` 权限 + 帧闸门；帧形态与 JSON 形态同一实现）、RPC 流式（连接守卫 +
`msg:rpc` 权限，**不带**一元帧闸门——闸门会把帧请求整单短路）。
"""
