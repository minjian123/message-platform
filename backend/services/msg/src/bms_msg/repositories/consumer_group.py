"""消费组仓储（`msg_consumer_group` / `msg_group_member`；01-4-1）。

口径：

- **不启用自动租户过滤**：组加载 / 心跳 / 后台扫描都可能没有请求上下文，查询侧显式传 `tenant_id`；
- **不做软删除**：组删除 / 主题删除 / 租户清理走物理删除；
- **成员整替**：保存时先删该组全部成员行再插入快照，库内始终是一份完整快照；
- 仓储只 `flush` 不 `commit`，事务边界在调用方（`DatabaseGroupStore` 每次操作自开 UoW）。
"""

from __future__ import annotations

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import delete, func, select, update

from bms_msg.broker.group import ConsumerGroup, GroupMember
from bms_msg.models.consumer_group import GROUP_STATE_DEAD, GROUP_STATE_EMPTY, MsgConsumerGroup, MsgGroupMember

__all__ = ["ConsumerGroupRepository"]


class ConsumerGroupRepository(BaseDbRepository[MsgConsumerGroup]):
    """消费组仓储（组行 + 成员行）。"""

    model = MsgConsumerGroup
    tenant_scoped = False
    soft_delete_enabled = False

    async def get_with_members(self, *, tenant_id: int, topic_id: int, name: str) -> ConsumerGroup | None:
        """按组身份取组快照（含成员）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            name: 组名。

        Returns:
            ConsumerGroup | None: 组快照；不存在返回 None。
        """
        statement = self._select().where(
            self._column("tenant_id") == tenant_id,
            self._column("topic_id") == topic_id,
            self._column("name") == name,
        )
        row = (await self._session.execute(statement)).scalars().first()
        if row is None:
            return None
        return await self._to_record(row)

    async def get_by_id(self, group_id: int) -> ConsumerGroup | None:
        """按主键取组快照（含成员）。

        Args:
            group_id: 组主键。

        Returns:
            ConsumerGroup | None: 组快照；不存在返回 None。
        """
        row = await self._session.get(MsgConsumerGroup, group_id)
        return None if row is None else await self._to_record(row)

    async def save(self, record: ConsumerGroup, *, instance_id: str | None = None) -> None:
        """保存组快照（组行 upsert + 成员整替）。

        Args:
            record: 组快照。
            instance_id: 当前实例标识（写入成员行的运维字段；None 表示不记）。
        """
        group_id = await self._upsert_group(record)
        await self._replace_members(group_id=group_id, record=record, instance_id=instance_id)

    async def list_for_tenant(
        self, *, tenant_id: int, topic_id: int | None = None
    ) -> ConcurrentStableList[ConsumerGroup]:
        """列出某租户的组快照（组名升序，同名按主题主键兜底）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题过滤；None 表示不限。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 组快照。
        """
        statement = self._select().where(self._column("tenant_id") == tenant_id)
        if topic_id is not None:
            statement = statement.where(self._column("topic_id") == topic_id)
        statement = statement.order_by(self._column("name"), self._column("topic_id"))
        rows = (await self._session.execute(statement)).scalars().all()
        return ConcurrentStableList([await self._to_record(row) for row in rows])

    async def list_all(self) -> ConcurrentStableList[ConsumerGroup]:
        """列出全部组快照（指标采样用；按租户 / 主题 / 组名稳定排序）。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 组快照。
        """
        statement = self._select().order_by(self._column("tenant_id"), self._column("topic_id"), self._column("name"))
        rows = (await self._session.execute(statement)).scalars().all()
        return ConcurrentStableList([await self._to_record(row) for row in rows])

    async def delete_identity(self, *, tenant_id: int, topic_id: int, name: str) -> bool:
        """按组身份物理删除组与成员（位点不动）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            name: 组名。

        Returns:
            bool: 是否命中并删除。
        """
        statement = select(self._column("id")).where(
            self._column("tenant_id") == tenant_id,
            self._column("topic_id") == topic_id,
            self._column("name") == name,
        )
        group_id = (await self._session.execute(statement)).scalar_one_or_none()
        if group_id is None:
            return False
        await self._delete_members(int(group_id))
        await self._session.execute(delete(MsgConsumerGroup).where(self._column("id") == int(group_id)))
        await self._session.flush()
        return True

    async def mark_idle_dead(self, *, before: datetime) -> int:
        """把空闲超过阈值的空组置 `dead`（幂等）。

        Args:
            before: 空闲判定截止时刻（UTC；`updated_at < before` 即超期）。

        Returns:
            int: 本次置为 `dead` 的组数。
        """
        statement = (
            update(MsgConsumerGroup)
            .where(
                self._column("state") == GROUP_STATE_EMPTY,
                self._column("updated_at") < before,
            )
            .values(state=GROUP_STATE_DEAD)
        )
        result = await self._session.execute(statement)
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的全部组与成员。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的组数。
        """
        return await self._purge(condition=self._column("tenant_id") == tenant_id)

    async def purge_topic(self, *, topic_id: int) -> int:
        """删除某主题的全部组与成员。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的组数。
        """
        return await self._purge(condition=self._column("topic_id") == topic_id)

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """统计某租户的组数（组数配额预检用）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 组数。
        """
        statement = select(func.count()).select_from(MsgConsumerGroup).where(self._column("tenant_id") == tenant_id)
        return int((await self._session.execute(statement)).scalar_one())

    async def _upsert_group(self, record: ConsumerGroup) -> int:
        """写入组行（存在即更新状态 / 世代 / 命名空间）。

        Args:
            record: 组快照。

        Returns:
            int: 组主键。
        """
        statement = select(self._column("id")).where(
            self._column("tenant_id") == record.tenant_id,
            self._column("topic_id") == record.topic_id,
            self._column("name") == record.group,
        )
        group_id = (await self._session.execute(statement)).scalar_one_or_none()
        if group_id is None:
            row = MsgConsumerGroup(
                tenant_id=record.tenant_id,
                topic_id=record.topic_id,
                name=record.group,
                namespace=record.namespace,
                state=record.state,
                generation=record.generation,
            )
            self._session.add(row)
            await self._session.flush()
            return int(row.id)
        await self._session.execute(
            update(MsgConsumerGroup)
            .where(self._column("id") == int(group_id))
            .values(state=record.state, generation=record.generation, namespace=record.namespace)
        )
        await self._session.flush()
        return int(group_id)

    async def _replace_members(self, *, group_id: int, record: ConsumerGroup, instance_id: str | None) -> None:
        """整替成员行（先删后插）。

        Args:
            group_id: 组主键。
            record: 组快照。
            instance_id: 当前实例标识。
        """
        await self._delete_members(group_id)
        for member in record.members:
            self._session.add(
                MsgGroupMember(
                    group_id=group_id,
                    member_id=member.consumer_id,
                    instance_id=instance_id,
                    joined_at=member.joined_at,
                    last_heartbeat=member.last_seen,
                    partitions=ConcurrentStableList(member.partitions),
                )
            )
        await self._session.flush()

    async def _delete_members(self, group_id: int) -> None:
        """删除某组的全部成员行。

        Args:
            group_id: 组主键。
        """
        await self._session.execute(delete(MsgGroupMember).where(MsgGroupMember.group_id == group_id))
        await self._session.flush()

    async def _purge(self, *, condition: object) -> int:
        """按条件删除组与成员（物理删除）。

        Args:
            condition: 组行的筛选条件。

        Returns:
            int: 删除的组数。
        """
        statement = select(self._column("id")).where(condition)  # type: ignore[arg-type]
        ids = [int(item) for item in (await self._session.execute(statement)).scalars().all()]
        if not ids:
            return 0
        await self._session.execute(delete(MsgGroupMember).where(MsgGroupMember.group_id.in_(ids)))
        await self._session.execute(delete(MsgConsumerGroup).where(self._column("id").in_(ids)))
        await self._session.flush()
        return len(ids)

    async def _to_record(self, row: MsgConsumerGroup) -> ConsumerGroup:
        """组行 + 成员行 → 值对象快照。

        Args:
            row: 组行。

        Returns:
            ConsumerGroup: 组快照（成员按 `member_id` 升序）。
        """
        statement = (
            select(MsgGroupMember).where(MsgGroupMember.group_id == int(row.id)).order_by(MsgGroupMember.member_id)
        )
        members = (await self._session.execute(statement)).scalars().all()
        return ConsumerGroup(
            tenant_id=int(row.tenant_id),
            topic_id=int(row.topic_id),
            group=row.name,
            namespace=row.namespace,
            state=row.state,
            generation=int(row.generation),
            members=ConcurrentStableList(
                GroupMember(
                    consumer_id=member.member_id,
                    joined_at=member.joined_at,
                    last_seen=member.last_heartbeat,
                    partitions=ConcurrentStableList(sorted(int(item) for item in member.partitions)),
                )
                for member in members
            ),
        )
