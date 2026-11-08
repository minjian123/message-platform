# pyright: reportPrivateUsage=false
"""05-5 生命周期运行时（Kiwi 2453）：租户可用性适配、审计强弱与时间工具。"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from bms_msg.tenant.runtime import (
    GuardTenantAvailability,
    TenantLifecycleAuditError,
    TenantLifecycleRuntime,
    _utc_naive,
    _utc_now_naive,
    get_tenant_lifecycle_metrics,
    get_tenant_lifecycle_runtime,
    reset_tenant_lifecycle_metrics,
    reset_tenant_lifecycle_runtime,
    set_tenant_lifecycle_runtime,
)

pytestmark = pytest.mark.kiwi_id(2453)


class _Guard:
    """租户守卫替身：可指定抛错。"""

    def __init__(self, exc: Exception | None = None) -> None:
        """初始化。

        Args:
            exc: 调用时抛出的异常；None 表示通过。
        """
        self.exc = exc
        self.calls: list[object] = []

    async def require(self, target: object) -> object:
        """模拟守卫判定。

        Args:
            target: 租户定位。

        Returns:
            object: 通过时的占位上下文。

        Raises:
            Exception: 构造时指定的异常。
        """
        self.calls.append(target)
        if self.exc is not None:
            raise self.exc
        return object()


class _Sink:
    """审计替身：可返回拒绝或抛错。"""

    def __init__(self, *, accepted: bool = True, exc: Exception | None = None) -> None:
        """初始化。

        Args:
            accepted: 是否接受。
            exc: 可选的抛出异常。
        """
        self.accepted = accepted
        self.exc = exc

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """模拟写审计。

        Args:
            event: 事件。
            required: 是否强审计。

        Returns:
            bool: 接受结果。

        Raises:
            Exception: 构造时指定的异常。
        """
        del event, required
        if self.exc is not None:
            raise self.exc
        return self.accepted


async def test_guard_availability_active() -> None:
    """守卫通过时租户可用。"""
    guard = _Guard()
    availability = GuardTenantAvailability(guard=guard)  # type: ignore[arg-type]
    assert await availability.is_active(7001) is True
    assert len(guard.calls) == 1


@pytest.mark.parametrize("exc", [TenantSuspendedError("停用"), TenantNotFoundError("不存在")])
async def test_guard_availability_known_inactive(exc: Exception) -> None:
    """停用 / 不存在返回 False。"""
    availability = GuardTenantAvailability(guard=_Guard(exc))  # type: ignore[arg-type]
    assert await availability.is_active(7001) is False


async def test_guard_availability_unavailable_fails_closed() -> None:
    """租户源不可达也按 False（fail-closed）。"""
    availability = GuardTenantAvailability(guard=_Guard(RuntimeError("down")))  # type: ignore[arg-type]
    assert await availability.is_active(7001) is False


def test_metrics_snapshot_and_reset() -> None:
    """进程级计数快照与清零。"""
    reset_tenant_lifecycle_metrics()
    metrics = get_tenant_lifecycle_metrics()
    metrics.applied = 1
    metrics.skipped = 2
    metrics.failed = 3
    assert metrics.snapshot() == {"applied": 1, "skipped": 2, "failed": 3}
    reset_tenant_lifecycle_metrics()
    assert metrics.snapshot() == {"applied": 0, "skipped": 0, "failed": 0}


def test_runtime_holder_set_get_reset() -> None:
    """进程级运行时登记 / 读取 / 重置。"""
    reset_tenant_lifecycle_runtime()
    assert get_tenant_lifecycle_runtime() is None
    sentinel = object()
    set_tenant_lifecycle_runtime(sentinel)  # type: ignore[arg-type]
    assert get_tenant_lifecycle_runtime() is sentinel
    reset_tenant_lifecycle_runtime()
    assert get_tenant_lifecycle_runtime() is None


async def test_record_audit_normal_rejected(runtime: TenantLifecycleRuntime) -> None:
    """普通审计被拒只返回 False，不阻断动作。"""
    runtime.audit_sink = _Sink(accepted=False)  # type: ignore[assignment]
    assert (
        await runtime.record_audit(
            action="msg:tenant.suspend",
            path="/x",
            tenant_id=7001,
            payload=ConcurrentStableDict({"a": 1}),
            strong=False,
        )
        is False
    )


async def test_record_audit_normal_exception(runtime: TenantLifecycleRuntime) -> None:
    """普通审计异常只返回 False，不阻断动作。"""
    runtime.audit_sink = _Sink(exc=RuntimeError("down"))  # type: ignore[assignment]
    assert (
        await runtime.record_audit(
            action="msg:tenant.suspend", path="/x", tenant_id=7001, payload=ConcurrentStableDict(), strong=False
        )
        is False
    )


async def test_record_audit_strong_rejected_and_exception(runtime: TenantLifecycleRuntime) -> None:
    """强审计被拒 / 异常都抛 TenantLifecycleAuditError。"""
    runtime.audit_sink = _Sink(accepted=False)  # type: ignore[assignment]
    with pytest.raises(TenantLifecycleAuditError):
        await runtime.record_audit(
            action="msg:tenant.delete", path="/x", tenant_id=7001, payload=ConcurrentStableDict(), strong=True
        )
    runtime.audit_sink = _Sink(exc=RuntimeError("down"))  # type: ignore[assignment]
    with pytest.raises(TenantLifecycleAuditError):
        await runtime.record_audit(
            action="msg:tenant.delete", path="/x", tenant_id=7001, payload=ConcurrentStableDict(), strong=True
        )


async def test_lifecycle_session_scope_opens_platform_session() -> None:
    """装配期会话入口可开会话（生命周期消费无请求上下文）。"""
    from bms_msg.main import lifecycle_session_scope

    engine: AsyncEngine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)

    class _Registry:
        def is_sync_only(self, key: str) -> bool:
            del key
            return False

        async def get(self, key: str, *, read_only: bool = False) -> AsyncEngine:
            del key, read_only
            return engine

    app = SimpleNamespace(state=SimpleNamespace(engine_registry=_Registry()))
    async with lifecycle_session_scope(app) as session:  # type: ignore[arg-type]
        assert (await session.execute(text("select 1"))).scalar_one() == 1
    await engine.dispose()


def test_utc_helpers() -> None:
    """时间工具：None 原样、naive 原样、aware 转 UTC naive。"""
    assert _utc_naive(None) is None
    naive = datetime(2026, 1, 1, 0, 0, 0)
    assert _utc_naive(naive) == naive
    aware = datetime(2026, 1, 1, 8, 0, 0, tzinfo=UTC)
    converted = _utc_naive(aware)
    assert converted is not None and converted.tzinfo is None
    assert _utc_now_naive().tzinfo is None
