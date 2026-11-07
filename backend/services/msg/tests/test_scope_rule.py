"""msg 命名空间作用域规则（07D 任务 03 附录 A；Kiwi 2451）。

覆盖：规则经 `scope_rule` 端口登记可解析；已登记 `module_key` 放行、未登记抛 `350010`；
判定复用 `tenant/context.py::ensure_namespace` 单点，规则自身不解析租户、不查服务目录状态。
"""

from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenSet
from bms_core.core.plugin import default_plugin_registry, resolve_plugin
from bms_core.extension import SCOPE_RULE_KEY
from bms_core.resource_scope import ResolvedScope
from bms_core.resource_scope.rules import BaseScopeRule

from bms_msg.errors import MsgErrorCode, MsgNamespaceInvalidError
from bms_msg.tenant import scope_rule as scope_rule_module
from bms_msg.tenant.scope_rule import MSG_NAMESPACE_RULE, MsgNamespaceRule

pytestmark = pytest.mark.kiwi_id(2451)

_ALLOWLIST = ConcurrentStableFrozenSet(("msg",))
"""用例内固定白名单（不依赖进程级服务目录状态）。"""


def _scope(module_key: str) -> ResolvedScope:
    """构造规则入参替身（规则只读取 `module_key`）。

    Args:
        module_key: 作用域第二级。

    Returns:
        ResolvedScope: 具备 `module_key` 属性的入参。
    """
    return cast("ResolvedScope", SimpleNamespace(module_key=module_key))


@pytest.fixture(autouse=True)
def _allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """固定可用命名空间集合，保证用例不随其它用例的目录登记变化。"""
    monkeypatch.setattr(scope_rule_module, "enabled_namespaces", lambda: _ALLOWLIST)


def test_registered_module_key_accepted() -> None:
    """命名空间取自已登记启用的 `module_key`：放行。"""
    MsgNamespaceRule().verify(_scope("msg"))


def test_unknown_module_key_rejected() -> None:
    """命名空间未登记：产品拒绝（350010 / 400），与改造前同码。"""
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        MsgNamespaceRule().verify(_scope("not-a-module"))
    assert excinfo.value.code == MsgErrorCode.NAMESPACE_INVALID


def test_rule_extends_base_scope_rule() -> None:
    """继承关系与实现名：产品只实现第四步语义，基座顺序与判定不变。"""
    assert issubclass(MsgNamespaceRule, BaseScopeRule)
    assert MsgNamespaceRule.plugin_name == MSG_NAMESPACE_RULE


def test_rule_auto_registered_by_import() -> None:
    """实现经导入期自动成为候选：注册表解析得到 `scope_rule:msg.namespace`。"""
    registry = default_plugin_registry()
    registry.reset()
    try:
        resolved = resolve_plugin(
            SCOPE_RULE_KEY,
            MSG_NAMESPACE_RULE,
            expected_version=BaseScopeRule.contract_version,
        )
        assert isinstance(resolved, MsgNamespaceRule)
    finally:
        registry.reset()
