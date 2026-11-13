"""msg 租户接入能力域（当前租户上下文、命名空间校验与两个入口的解析）。"""

from bms_msg.tenant.context import CurrentTenant, enabled_namespaces, validate_namespace
from bms_msg.tenant.resolve import FRAME_PATH, frame_tenant, management_tenant

__all__ = [
    "FRAME_PATH",
    "CurrentTenant",
    "enabled_namespaces",
    "frame_tenant",
    "management_tenant",
    "validate_namespace",
]
