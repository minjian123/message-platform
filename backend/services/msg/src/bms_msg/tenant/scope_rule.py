"""msg 命名空间作用域规则（07D 任务 03 附录 A）。

命名空间判定从租户级钩子 `MsgTenantGuard` 下移到基座 `scope_rule` 规则链第四步：
管理面 / 数据面 / 异步统一经 `BaseScopeResolver.verify()`，产品只在第四步追加语义。

- 规则只拿得到已通过基座前四步的 `ResolvedScope`，不解析租户、不查服务目录状态、
  不自建缓存；取值来自 `scope.module_key`（即 msg 的 `namespace`）。
- 判定复用 `tenant/context.py::validate_namespace` 单点，失败码保持 `350010`。
- 声明即契约：`extension_requirements()` 声明必需，缺失即拒启；未声明的规则零调用。
- 实现经 `BasePluggable` 导入期自动成为候选；**不再显式 `register_plugin`**
  （自动候选与显式登记同名会撞名拒建注册表）。
"""

from bms_core.resource_scope import ResolvedScope
from bms_core.resource_scope.rules import BaseScopeRule

from bms_msg.tenant.context import enabled_namespaces, validate_namespace

__all__ = ["MSG_NAMESPACE_RULE", "MsgNamespaceRule"]

MSG_NAMESPACE_RULE = "msg.namespace"
"""msg 命名空间规则实现名（`scope_rule` 端口）。"""


class MsgNamespaceRule(BaseScopeRule):
    """消息平台命名空间规则：作用域第二级须属已登记且启用的 `module_key`。"""

    plugin_name: str = MSG_NAMESPACE_RULE

    def verify(self, scope: ResolvedScope) -> None:
        """判定命名空间（`scope.module_key` 即 msg 的 namespace）。

        Args:
            scope: 已通过基座前四步的作用域。

        Raises:
            MsgNamespaceInvalidError: 未登记 / 未启用（`350010` / 400）。
        """
        validate_namespace(scope.module_key, enabled_namespaces())
