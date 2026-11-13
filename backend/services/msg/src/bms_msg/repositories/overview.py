"""概览快照与入口计数仓储（02-8-4）。

口径：

- 两张表都是观测数据，**不启用自动租户过滤**（采样器跨租户写入，读侧显式传 `tenant_id`）；
- 不做软删除；
- 仓储只 `flush` 不 `commit`，事务边界在调用方（采样器自开短事务）。
"""

from __future__ import annotations

from datetime import datetime

from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import delete, func, select, update

from bms_msg.models.overview import MsgIngressCounter, MsgOverviewSnapshot

__all__ = ["IngressCounterRepository", "OverviewSnapshotRepository"]


class OverviewSnapshotRepository(BaseDbRepository[MsgOverviewSnapshot]):
    """概览快照仓储（每租户一行）。"""

    model = MsgOverviewSnapshot
    tenant_scoped = False
    soft_delete_enabled = False

    async def get_for_tenant(self, *, tenant_id: int) -> MsgOverviewSnapshot | None:
        """按租户取快照行。

        Args:
            tenant_id: 租户主键。

        Returns:
            MsgOverviewSnapshot | None: 快照行；未采样过返回 None。
        """
        statement = self._select().where(self._column("tenant_id") == int(tenant_id))
        return (await self._session.execute(statement)).scalars().first()

    async def upsert(
        self,
        *,
        tenant_id: int,
        messages_in_per_second: float,
        consumer_lag: int,
        dlq_total: int,
        disk_used_ratio: float,
        sampled_at: datetime,
    ) -> None:
        """覆盖写一个租户的快照（存在即更新，否则插入）。

        Args:
            tenant_id: 租户主键。
            messages_in_per_second: 入口速率（条/秒）。
            consumer_lag: 当前积压。
            dlq_total: 死信存量。
            disk_used_ratio: 磁盘使用率。
            sampled_at: 采样时刻（UTC）。
        """
        existing = await self.get_for_tenant(tenant_id=tenant_id)
        if existing is None:
            self._session.add(
                MsgOverviewSnapshot(
                    tenant_id=int(tenant_id),
                    messages_in_per_second=float(messages_in_per_second),
                    consumer_lag=int(consumer_lag),
                    dlq_total=int(dlq_total),
                    disk_used_ratio=float(disk_used_ratio),
                    sampled_at=sampled_at,
                )
            )
            await self._session.flush()
            return
        await self._session.execute(
            update(MsgOverviewSnapshot)
            .where(self._column("id") == int(existing.id))
            .values(
                messages_in_per_second=float(messages_in_per_second),
                consumer_lag=int(consumer_lag),
                dlq_total=int(dlq_total),
                disk_used_ratio=float(disk_used_ratio),
                sampled_at=sampled_at,
            )
        )
        await self._session.flush()

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的快照行（租户清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(
            delete(MsgOverviewSnapshot).where(self._column("tenant_id") == int(tenant_id))
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)


class IngressCounterRepository(BaseDbRepository[MsgIngressCounter]):
    """入口计数仓储（租户 × 时间窗累加）。"""

    model = MsgIngressCounter
    tenant_scoped = False
    soft_delete_enabled = False

    async def add(self, *, tenant_id: int, window_start: int, delta: int) -> None:
        """把增量累加进一个时间窗（无行即插入）。

        Args:
            tenant_id: 租户主键。
            window_start: 窗口起点（epoch 秒）。
            delta: 本实例本拍增量（负数按 0 处理）。
        """
        amount = max(int(delta), 0)
        statement = select(self._column("id")).where(
            self._column("tenant_id") == int(tenant_id),
            self._column("window_start") == int(window_start),
        )
        row_id = (await self._session.execute(statement)).scalar_one_or_none()
        if row_id is None:
            if amount == 0:
                return
            self._session.add(MsgIngressCounter(tenant_id=int(tenant_id), window_start=int(window_start), count=amount))
            await self._session.flush()
            return
        if amount == 0:
            return
        await self._session.execute(
            update(MsgIngressCounter)
            .where(self._column("id") == int(row_id))
            .values(count=self._column("count") + amount)
        )
        await self._session.flush()

    async def sum_since(self, *, tenant_id: int, since: int) -> tuple[int, int, int]:
        """汇总某租户不早于 `since` 的窗口计数。

        Args:
            tenant_id: 租户主键。
            since: 最早窗口起点（epoch 秒，含）。

        Returns:
            tuple[int, int, int]: `(总数, 最早窗口起点, 最晚窗口起点)`；无数据为 `(0, 0, 0)`。
        """
        statement = select(
            func.coalesce(func.sum(self._column("count")), 0),
            func.min(self._column("window_start")),
            func.max(self._column("window_start")),
        ).where(
            self._column("tenant_id") == int(tenant_id),
            self._column("window_start") >= int(since),
        )
        total, earliest, latest = (await self._session.execute(statement)).one()
        return int(total or 0), int(earliest or 0), int(latest or 0)

    async def purge_before(self, *, cutoff: int) -> int:
        """删除早于截止时刻的窗口（幂等）。

        Args:
            cutoff: 截止窗口起点（epoch 秒；更早的行删除）。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(
            delete(MsgIngressCounter).where(self._column("window_start") < int(cutoff))
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的全部窗口（租户清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(
            delete(MsgIngressCounter).where(self._column("tenant_id") == int(tenant_id))
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)
