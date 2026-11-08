"""配额仓储（`msg_quota`）：按作用域取行、幂等 upsert 与租户清理。

**为什么显式过滤 `tenant_id`**：配额读取可能发生在后台（租户到期清理）与请求路径两条链上；
显式条件让两条链行为一致，不依赖 `ContextVar` 是否存在。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import delete, select

from bms_msg.core.config import QUOTA_ORIGIN_CONFIGURED, QUOTA_ORIGIN_DEFAULT
from bms_msg.models.quota import MsgQuota

__all__ = ["QuotaRepository"]


class QuotaRepository(BaseDbRepository[MsgQuota]):
    """配额仓储：只 flush，事务边界归调用方工作单元。"""

    model = MsgQuota
    tenant_scoped = True
    soft_delete_enabled = False

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[MsgQuota]:
        """列出某租户全部配额行（含命名空间级）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[MsgQuota]: 按 `quota_key, namespace` 升序。
        """
        statement = (
            select(MsgQuota)
            .where(MsgQuota.tenant_id == int(tenant_id))
            .order_by(MsgQuota.quota_key.asc(), MsgQuota.namespace.asc())
        )
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def get_scoped(self, *, tenant_id: int, namespace: str, quota_key: str) -> MsgQuota | None:
        """按唯一键取一行（显式租户条件，不依赖请求上下文）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 配额维度键。

        Returns:
            MsgQuota | None: 配额行；不存在返回 None。
        """
        statement = select(MsgQuota).where(
            MsgQuota.tenant_id == int(tenant_id),
            MsgQuota.namespace == namespace,
            MsgQuota.quota_key == quota_key,
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def upsert_default(
        self,
        *,
        tenant_id: int,
        namespace: str,
        quota_key: str,
        limit_value: int,
        soft_limit: int,
        unit: str,
        period: str,
    ) -> tuple[MsgQuota, bool]:
        """幂等写入缺省行（已存在即原样返回，不覆盖管理面调整）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 配额维度键。
            limit_value: 硬限。
            soft_limit: 软限（0 = 未显式设置）。
            unit: 单位。
            period: 周期。

        Returns:
            tuple[MsgQuota, bool]: 配额行 + 是否新建。
        """
        existing = await self.get_scoped(tenant_id=tenant_id, namespace=namespace, quota_key=quota_key)
        if existing is not None:
            return existing, False
        created = await self.create(
            tenant_id=int(tenant_id),
            namespace=namespace,
            quota_key=quota_key,
            limit_value=limit_value,
            soft_limit=soft_limit,
            unit=unit,
            period=period,
            origin=QUOTA_ORIGIN_DEFAULT,
        )
        return created, True

    async def upsert_configured(
        self,
        *,
        tenant_id: int,
        namespace: str,
        quota_key: str,
        limit_value: int,
        soft_limit: int,
        unit: str,
        period: str,
    ) -> MsgQuota:
        """写入管理面调整行（存在即覆盖，`origin=configured`）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 配额维度键。
            limit_value: 硬限。
            soft_limit: 软限（0 = 未显式设置）。
            unit: 单位。
            period: 周期。

        Returns:
            MsgQuota: 写入后的行。
        """
        existing = await self.get_scoped(tenant_id=tenant_id, namespace=namespace, quota_key=quota_key)
        if existing is None:
            return await self.create(
                tenant_id=int(tenant_id),
                namespace=namespace,
                quota_key=quota_key,
                limit_value=limit_value,
                soft_limit=soft_limit,
                unit=unit,
                period=period,
                origin=QUOTA_ORIGIN_CONFIGURED,
            )
        existing.limit_value = limit_value
        existing.soft_limit = soft_limit
        existing.unit = unit
        existing.period = period
        existing.origin = QUOTA_ORIGIN_CONFIGURED
        await self._session.flush()
        return existing

    async def delete_for_tenant(self, *, tenant_id: int) -> int:
        """物理删除某租户全部配额行（租户到期清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(delete(MsgQuota).where(MsgQuota.tenant_id == int(tenant_id)))
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)
