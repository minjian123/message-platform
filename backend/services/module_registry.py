"""msg 产品服务目录本地登记（产品自持清单，与 `bms_core.services.module_registry` 同口径）。

- 单一来源：本清单是 msg 侧产品服务注册要素（服务键 / 表前缀 / 业务码 / 错误码段 / 事件域）
  的登记出处；与 `deploy/registry/msg.toml` 的服务行逐字段同值。
- 装载时注册（导入即登记）：应用装配校验 / 库键解析 / 表归属前缀校验读合并视图。
- `errcode_segment = "35"` 为 8B_00 暂定值，最终以 07C 标识符登记为准（段号注册即定）。
"""

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.services.module_registry import (
    ModuleRecord,
    ModuleStatus,
    ProductRecord,
    ProductRouteRecord,
    ServiceGroup,
    register_product_records,
    register_product_routes,
    register_service_records,
)

PRODUCT_KEY = "msg"
"""msg 产品标识（与 `deploy/registry/msg.toml` 产品档案一致）。"""

SERVICE_KEY = "msg"
"""运行服务标识（本产品当前唯一在运服务）。"""

MSG_SERVICE_RECORDS: tuple[ModuleRecord, ...] = (
    ModuleRecord(
        module_key="msg",
        service_key=SERVICE_KEY,
        name="msg 消息平台服务",
        table_prefix="msg_",
        business_code="msg",
        errcode_segment="35",
        event_domain="msg",
        service_group=ServiceGroup.PRODUCT,
        build_batch=4,
        product_key=PRODUCT_KEY,
        status=ModuleStatus.ENABLED,
        tenant_base=True,
    ),
)
"""msg 产品服务清单（唯一在运服务 `msg`；与产品声明服务行同值）。"""

MSG_PRODUCT_RECORDS: tuple[ProductRecord, ...] = (
    ProductRecord(
        product_key=PRODUCT_KEY,
        name="消息平台",
    ),
)
"""msg 产品档案（与 `deploy/registry/msg.toml` 的 `products` 段同值）。"""

MSG_PRODUCT_ROUTES: tuple[ProductRouteRecord, ...] = (
    ProductRouteRecord(
        product_key=PRODUCT_KEY,
        domain=SERVICE_KEY,
        service_key=SERVICE_KEY,
        description="msg 消息平台服务（对外 /api/msg/v1/msg/...）",
    ),
)
"""msg 产品级路由映射（与 `deploy/registry/msg.toml` 的 `product_routes` 段同值）。"""

register_product_records(MSG_PRODUCT_RECORDS)
register_service_records(ConcurrentStableList(MSG_SERVICE_RECORDS))
register_product_routes(MSG_PRODUCT_ROUTES)
