"""msg 当前租户上下文与命名空间校验（「当前租户」取值口径的**唯一实现**）。

- `CurrentTenant`：统一依赖的返回值（基座 `TenantContext` + 已启用命名空间快照 + 状态）。
  主题 / 分区 / 配额等模块**只经它取租户**，不得自行解析（详细设计 §3.3）。
- `enabled_namespaces()`：服务目录中已登记且启用的 `module_key` 快照。目录视图是进程内常量
  （无 IO），管理面与数据面同进程可读，因此不需要装配期落 `app.state`（详细设计 §7 风险 3 的结论）。
- `validate_namespace()`：命名空间判定的**单点**（形态 + allowlist，非法即 `350010`），依赖出口与消费守卫共用。
"""

from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableFrozenSet
from bms_core.core.objects import BaseValueObject
from bms_core.db.tenant import TenantContext
from bms_core.services.module_registry import ModuleStatus, service_catalog_view

from bms_msg.core.config import NAMESPACE_PATTERN
from bms_msg.errors import MsgNamespaceInvalidError

__all__ = [
    "CurrentTenant",
    "enabled_namespaces",
    "validate_namespace",
]


def enabled_namespaces() -> ConcurrentStableFrozenSet[str]:
    """取已登记且启用的命名空间（`module_key` 口径，进程内常量视图）。

    Returns:
        ConcurrentStableFrozenSet[str]: 可用命名空间集合。
    """
    return ConcurrentStableFrozenSet(
        record.module_key for record in service_catalog_view() if record.status == ModuleStatus.ENABLED
    )


def validate_namespace(namespace: str | None, allowlist: ConcurrentStableFrozenSet[str]) -> None:
    """校验命名空间形态与可用性（`350010` 的唯一判定点）。

    形态先行、allowlist 后判：形态是请求契约前提，先报「形态非法」比「未登记」更可定位；
    `None` 表示调用方未指定范围，只做租户校验（跳过本步）。

    Args:
        namespace: 命名空间；`None` 表示调用方未指定范围。
        allowlist: 可用命名空间集合（`enabled_namespaces()`）。

    Raises:
        MsgNamespaceInvalidError: 形态非法，或未登记 / 未启用（`350010`）。
    """
    if namespace is None:
        return
    if not NAMESPACE_PATTERN.match(namespace):
        raise MsgNamespaceInvalidError(f"命名空间形态非法：{namespace}")
    if namespace not in allowlist:
        raise MsgNamespaceInvalidError(f"命名空间未登记或未启用（module_key 口径）：{namespace}")


@dataclass(frozen=True)
class CurrentTenant(BaseValueObject):
    """当前租户（统一依赖返回值）：租户上下文 + 可用命名空间。

    主键 `tenant_id` 必非空——构造前由解析层保证（无主键的演示兜底一律拒，见 `tenant/resolve.py`）。
    """

    tenant: TenantContext
    """基座租户上下文（编码 / 库键 / 名称 / 主键 / 状态）。"""

    namespace_allowlist: ConcurrentStableFrozenSet[str]
    """已登记且启用的命名空间（`module_key` 口径）。"""

    @classmethod
    def from_context(cls, context: TenantContext) -> CurrentTenant:
        """由基座上下文构造（并取命名空间快照）。

        Args:
            context: 基座租户上下文（调用方须保证 `tenant_id` 非空）。

        Returns:
            CurrentTenant: 当前租户。
        """
        return cls(tenant=context, namespace_allowlist=enabled_namespaces())

    @property
    def tenant_id(self) -> str:
        """租户主键（雪花 id 十进制字符串，内部身份）。

        Returns:
            str: 租户主键。
        """
        return str(self.tenant.tenant_id)

    @property
    def code(self) -> str:
        """租户编码（仅日志 / 展示用，不作存储边界）。

        Returns:
            str: 租户编码。
        """
        return self.tenant.code

    @property
    def status(self) -> str:
        """租户状态（解析链已判定可用，正常路径恒 `active`）。

        Returns:
            str: 租户状态。
        """
        return self.tenant.status

    def ensure_namespace(self, namespace: str | None) -> None:
        """校验命名空间（口径同 `validate_namespace()`，单点复用）。

        Args:
            namespace: 命名空间（`None` 跳过校验）。

        Raises:
            MsgNamespaceInvalidError: 形态非法或未登记 / 未启用（`350010`）。
        """
        validate_namespace(namespace, self.namespace_allowlist)
