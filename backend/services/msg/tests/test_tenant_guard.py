"""msg 租户可用性守卫测试（基座任务 04_02；Kiwi 2408）。

覆盖：`MsgTenantGuard` 继承基座默认实现（不改基座判定）、命名空间接受 / 拒绝 / 缺省跳过、
租户停用在命名空间校验之前拦截（fail-fast）、工厂按 `app.state.tenant_source` 构造、
`configure_plugins()` 登记后 `tenant_guard:msg` 可经插件解析取到。
"""

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import TenantSuspendedError
from bms_core.core.plugin import default_plugin_registry, resolve_plugin
from bms_core.db.tenant import TenantContext
from bms_core.tenant.guard import BaseTenantGuard, TenantTarget
from fastapi import FastAPI

from bms_msg.errors import MsgErrorCode, MsgNamespaceInvalidError
from bms_msg.tenant_guard import MSG_TENANT_GUARD, MsgTenantGuard, MsgTenantGuardFactory, register_msg_tenant_guard

pytestmark = pytest.mark.kiwi_id(2408)

_CONTEXT = TenantContext(code="acme", db_key="tenant_acme", name="Acme", tenant_id=7)
"""测试用租户上下文（含雪花主键）。"""

_REGISTERED_NAMESPACE = "file"
"""服务目录中已登记启用的模块键（命名空间白名单正例）。"""


class _StubSource:
    """租户源替身：记录调用并返回固定上下文 / 固定异常。"""

    def __init__(self, *, error: Exception | None = None) -> None:
        """初始化。

        Args:
            error: 命中时抛出的异常（None 表示正常返回）。
        """
        self.error = error
        self.calls: ConcurrentStableList[str] = ConcurrentStableList()

    async def by_code(self, code: str) -> TenantContext:
        """按编码取上下文。"""
        return self._hit(f"code:{code}")

    async def by_domain(self, domain: str) -> TenantContext:
        """按子域名取上下文。"""
        return self._hit(f"domain:{domain}")

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取上下文。"""
        return self._hit(f"id:{tenant_id}")

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（本替身不使用）。"""
        return None

    def _hit(self, label: str) -> TenantContext:
        """记一次调用并按配置返回。

        Args:
            label: 调用标识。

        Returns:
            TenantContext: 固定上下文。

        Raises:
            Exception: 配置的异常（原样抛出）。
        """
        self.calls.add(label)
        if self.error is not None:
            raise self.error
        return _CONTEXT


def _guard(source: _StubSource) -> MsgTenantGuard:
    """构造 msg 守卫。

    Args:
        source: 租户源替身。

    Returns:
        MsgTenantGuard: 守卫实例。
    """
    return MsgTenantGuard(source_provider=lambda: source)


def _target() -> TenantTarget:
    """构造租户定位（雪花主键）。

    Returns:
        TenantTarget: 租户定位。
    """
    return TenantTarget(tenant_id="7")


def test_guard_extends_base_without_rewriting() -> None:
    """继承关系与登记名：只补命名空间校验，基座判定不变。"""
    assert issubclass(MsgTenantGuard, BaseTenantGuard)
    assert MsgTenantGuard.plugin_name == MSG_TENANT_GUARD


async def test_namespace_of_registered_module_accepted() -> None:
    """命名空间取自已登记启用的 module_key：通过。"""
    guard = _guard(_StubSource())
    context = await guard.per_round(_target(), scope=_REGISTERED_NAMESPACE)
    assert context is _CONTEXT


async def test_unknown_namespace_rejected() -> None:
    """命名空间未登记：产品拒绝（msg 段位码 350010，与统一依赖共用同一判定）。"""
    guard = _guard(_StubSource())
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        await guard.per_round(_target(), scope="not-a-module")
    assert excinfo.value.code == MsgErrorCode.NAMESPACE_INVALID


async def test_missing_scope_skips_namespace_check() -> None:
    """未指定命名空间：只做租户校验（供管理面 / 运维脚本复用）。"""
    guard = _guard(_StubSource())
    assert await guard.require(_target()) is _CONTEXT


async def test_suspended_tenant_blocked_before_namespace_check() -> None:
    """租户停用先拦截（fail-fast），命名空间校验不再执行。"""
    guard = _guard(_StubSource(error=TenantSuspendedError("租户已停用")))
    with pytest.raises(TenantSuspendedError):
        await guard.per_round(_target(), scope=_REGISTERED_NAMESPACE)


async def test_factory_uses_app_tenant_source() -> None:
    """工厂按 `app.state.tenant_source` 构造：判定走应用装配的租户源。"""
    source = _StubSource()
    app = FastAPI()
    app.state.tenant_source = source
    guard = MsgTenantGuardFactory(app).create()
    assert await guard.require(_target(), scope=_REGISTERED_NAMESPACE) is _CONTEXT
    assert list(source.calls) == ["id:7"]


def test_register_guard_resolves_by_plugin_name() -> None:
    """登记后可按 `tenant_guard:msg` 解析（`configure_plugins()` 调用的同一入口）。"""
    registry = default_plugin_registry()
    registry.reset()
    app = FastAPI()
    try:
        register_msg_tenant_guard(app)
        resolved = resolve_plugin("tenant_guard", MSG_TENANT_GUARD, expected_version=BaseTenantGuard.contract_version)
        assert isinstance(resolved, MsgTenantGuard)
    finally:
        registry.reset()
