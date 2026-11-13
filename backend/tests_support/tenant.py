"""租户上下文替身与编码映射（跨用例文件共享的测试辅助）。

**为什么单独放这里**：`services/msg/tests` 不是包，用例里写 `from tests.conftest import FakeTenantSource`
依赖 pytest 对同名包 `tests` 的解析结果——单文件运行恰能命中，**全量收集时**可能先解析到仓内另一个
`tests` 命名空间，在 Linux CI 上表现为 `ModuleNotFoundError: No module named 'tests.conftest'`
（2026-11-13 msg#1614 / #1615 两次踩到）。与 `tests_support.msg_fixtures`、`tests_support.clock`
同一口径：**跨文件共享的测试辅助一律放 `tests_support`**。

`TENANT_ID` / `TENANT_CODE` 由 `conftest` 再导出，既有夹具与用例的取值口径不变。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.db.tenant import TenantContext

__all__ = ["TENANT_CODE", "TENANT_ID", "TENANT_IDS", "FakeTenantSource"]

TENANT_ID = 7001
"""测试租户主键（雪花）。"""

TENANT_CODE = "demo"
"""测试租户编码。"""

TENANT_IDS: ConcurrentStableDict[str, int] = ConcurrentStableDict({"demo": TENANT_ID, "other": 8002})
"""租户编码 → 固定主键（`demo` 与夹具上下文同值；`other` 用于验证「以解析结果为准」）。"""


class FakeTenantSource:
    """租户源替身：两个启用租户（`demo` / `other`）+ 其他编码恒不存在，可切换停用。"""

    def __init__(self) -> None:
        """初始化（缺省启用）。"""
        self.suspended = False

    async def by_code(self, code: str) -> TenantContext:
        """按编码取上下文。

        Args:
            code: 租户编码（未登记编码恒不存在）。

        Returns:
            TenantContext: 租户上下文。

        Raises:
            TenantNotFoundError: 编码未登记。
            TenantSuspendedError: 替身被置为停用。
        """
        from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError

        tenant_id = TENANT_IDS.get(code)
        if tenant_id is None:
            raise TenantNotFoundError("未知租户")
        if self.suspended:
            raise TenantSuspendedError(f"租户已停用：{code}")
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=tenant_id)

    async def by_domain(self, domain: str) -> TenantContext:
        """按子域名取上下文（本替身不支持域名来源）。

        Args:
            domain: 子域名。

        Returns:
            TenantContext: 永不返回。

        Raises:
            TenantNotFoundError: 恒抛。
        """
        from bms_core.core.exceptions import TenantNotFoundError

        raise TenantNotFoundError(f"未知租户域名：{domain}")

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取上下文。

        Args:
            tenant_id: 租户主键字符串。

        Returns:
            TenantContext: 租户上下文。

        Raises:
            TenantNotFoundError: 主键不在替身映射内。
        """
        from bms_core.core.exceptions import TenantNotFoundError

        for code, value in TENANT_IDS.items():
            if str(value) == tenant_id:
                return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=value)
        raise TenantNotFoundError(f"未知租户：{tenant_id}")

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（返回 `demo`）。

        Returns:
            TenantContext | None: `demo` 租户上下文。
        """
        return TenantContext(code=TENANT_CODE, db_key=f"tenant_{TENANT_CODE}", name="演示租户", tenant_id=TENANT_ID)
