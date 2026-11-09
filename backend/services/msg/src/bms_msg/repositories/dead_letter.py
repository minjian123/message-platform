"""死信仓储（`msg_dead_letter`）：转入落库、三元组索引、列表 / 详情、状态迁移与清理。

口径：

- **不启用自动租户过滤**：`add` / `purge_topics` 在投递与清理路径无请求上下文，
  查询侧由服务层显式传 `tenant_id`（跨租户按「不存在」处理，不泄露存在性）；
- **不做软删除**：`soft_delete_enabled = False`；主题删除 / 租户清理走物理删除；
- 仓储只 `flush` 不 `commit`，事务边界在调用方的 `uow.begin()`。
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList, ConcurrentStableSet
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import delete, func, or_, select, update

from bms_msg.models.dead_letter import MsgDeadLetter

__all__ = ["DeadLetterRepository"]


class DeadLetterRepository(BaseDbRepository[MsgDeadLetter]):
    """死信仓储（`msg_dead_letter`）。"""

    model = MsgDeadLetter
    tenant_scoped = False
    soft_delete_enabled = False
    sortable_fields = ConcurrentStableSet({"id", "failed_at", "created_at", "status"})

    async def list_keys(self) -> ConcurrentStableList[tuple[int, int, int]]:
        """取全部死信定位三元组（启动恢复内存索引）。

        Returns:
            ConcurrentStableList[tuple[int, int, int]]: `(topic_id, partition_no, msg_offset)` 列表。
        """
        statement = select(self._column("topic_id"), self._column("partition_no"), self._column("msg_offset")).order_by(
            self._column("id")
        )
        rows = (await self._session.execute(statement)).all()
        return ConcurrentStableList(
            (int(topic_id), int(partition_no), int(offset)) for topic_id, partition_no, offset in rows
        )

    async def exists_key(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """按定位三元组判定死信是否已存在。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已存在 True。
        """
        statement = (
            select(func.count())
            .select_from(MsgDeadLetter)
            .where(
                self._column("topic_id") == topic_id,
                self._column("partition_no") == partition_no,
                self._column("msg_offset") == offset,
            )
        )
        return bool((await self._session.execute(statement)).scalar_one())

    async def list_all(self) -> ConcurrentStableList[MsgDeadLetter]:
        """列出全部死信（按 `failed_at`、`id` 升序，保持插入序语义）。

        Returns:
            ConcurrentStableList[MsgDeadLetter]: 死信行。
        """
        statement = self._select().order_by(self._column("failed_at"), self._column("id"))
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def get_for_tenant(self, *, item_id: int, tenant_id: int) -> MsgDeadLetter | None:
        """按主键 + 租户取死信（跨租户视为不存在）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键。

        Returns:
            MsgDeadLetter | None: 死信行；不存在 / 跨租户为 None。
        """
        statement = self._select().where(self._column("id") == item_id, self._column("tenant_id") == tenant_id)
        return (await self._session.execute(statement)).scalars().first()

    async def page_for_tenant(
        self,
        *,
        tenant_id: int,
        page: int,
        size: int,
        status: str | None = None,
        exclude_archived: bool = False,
        reason: str | None = None,
        topic: str | None = None,
        partition_no: int | None = None,
        failed_from: datetime | None = None,
        failed_to: datetime | None = None,
        keyword: str | None = None,
    ) -> tuple[ConcurrentStableList[MsgDeadLetter], int]:
        """按租户 + 筛选条件分页（`failed_at` 倒序、`id` 倒序稳定）。

        Args:
            tenant_id: 租户主键。
            page: 页码（从 1 起）。
            size: 每页条数。
            status: 状态精确匹配；为空按 `exclude_archived` 决定是否排除归档。
            exclude_archived: 未指定状态时是否排除 `archived`。
            reason: 转入原因精确匹配。
            topic: 主题名精确匹配。
            partition_no: 分区号。
            failed_from: 失败时间下限（含）。
            failed_to: 失败时间上限（不含）。
            keyword: `log_id` 或 `message_id` 模糊匹配。

        Returns:
            tuple[ConcurrentStableList[MsgDeadLetter], int]: 当页行与过滤后总数。
        """
        conditions = ConcurrentStableList([self._column("tenant_id") == tenant_id])
        if status:
            conditions.add(self._column("status") == status)
        elif exclude_archived:
            conditions.add(self._column("status") != "archived")
        if reason:
            conditions.add(self._column("reason") == reason)
        if topic:
            conditions.add(self._column("topic") == topic)
        if partition_no is not None:
            conditions.add(self._column("partition_no") == partition_no)
        if failed_from is not None:
            conditions.add(self._column("failed_at") >= failed_from)
        if failed_to is not None:
            conditions.add(self._column("failed_at") < failed_to)
        if keyword:
            pattern = f"%{keyword}%"
            conditions.add(or_(self._column("log_id").like(pattern), self._column("message_id").like(pattern)))

        statement = self._select().where(*conditions)
        count_statement = select(func.count()).select_from(MsgDeadLetter).where(*conditions)
        total = int((await self._session.execute(count_statement)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    statement.order_by(self._column("failed_at").desc(), self._column("id").desc())
                    .offset((page - 1) * size)
                    .limit(size)
                )
            )
            .scalars()
            .all()
        )
        return ConcurrentStableList(rows), total

    async def purge_topics(self, *, topic_ids: Iterable[int]) -> int:
        """按主题物理删除死信（幂等）。

        Args:
            topic_ids: 主题主键集合。

        Returns:
            int: 删除行数。
        """
        targets = ConcurrentStableSet([int(item) for item in topic_ids])
        if not targets:
            return 0
        statement = delete(MsgDeadLetter).where(self._column("topic_id").in_(tuple(targets)))
        result = await self._session.execute(statement)
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)

    async def mark_replaying(self, *, item_id: int, tenant_id: int, replay_task_id: int) -> bool:
        """把死信从 `pending` 置为 `replaying` 并写重放任务（条件更新，防并发重复处置）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键（防跨租户误更新）。
            replay_task_id: 重放任务主键。

        Returns:
            bool: 是否命中并更新。
        """
        statement = (
            update(MsgDeadLetter)
            .where(
                self._column("id") == item_id,
                self._column("tenant_id") == tenant_id,
                self._column("status") == "pending",
            )
            .values(status="replaying", replay_task_id=replay_task_id)
        )
        result = await self._session.execute(statement)
        await self._session.flush()
        return bool(getattr(result, "rowcount", 0) or 0)
