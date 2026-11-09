"""重放任务仓储（`msg_replay_task`）：任务与游标的读写。

口径：

- 声明 `tenant_scoped`：请求侧按上下文自动过滤 / 注入租户（跨租户访问 → 查不到 → `350016`）；
  **执行器侧无租户上下文**，`list_active()` 因此能跨租户捞任务——这是后台推进器的必要行为，
  不是隔离漏洞（任务主键是雪花，写入仍带 `tenant_id`）。
- `page_for_tenant()` 是**管理面列表（06-6-1）**的唯一查询出口：租户条件显式入 SQL，条件只在这里
  拼一次，分页与 `total` 共用同一份条件。
- 元数据表**不做软删除**（终态用 `status` 表达），故 `soft_delete_enabled = False`；
- 仓储只 `flush` 不 `commit`，事务边界在服务层 / 执行器的工作单元。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList, ConcurrentStableSet
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import func, select

from bms_msg.models.replay import TASK_PENDING, TASK_RUNNING, MsgReplayTask

__all__ = ["ReplayTaskRepository"]


class ReplayTaskRepository(BaseDbRepository[MsgReplayTask]):
    """重放任务仓储：按状态捞待推进任务 + 通用读写（分页 / 历史列表归 06 域）。"""

    model = MsgReplayTask
    tenant_scoped = True
    soft_delete_enabled = False
    sortable_fields = ConcurrentStableSet({"id", "created_at"})

    async def list_active(self, *, limit: int) -> ConcurrentStableList[MsgReplayTask]:
        """取待推进任务（`pending` / `running`，按主键升序）。

        执行器**无租户上下文**调用，故本条查询不带租户条件（跨租户推进）；`pending` 也纳入，
        否则「已建未跑」的任务永远等不到第一个执行器拾取。

        Args:
            limit: 单轮上限（条）。

        Returns:
            ConcurrentStableList[MsgReplayTask]: 待推进任务。
        """
        statement = (
            self._select()
            .where(self._column("status").in_((TASK_PENDING, TASK_RUNNING)))
            .order_by(self._column("id"))
            .limit(limit)
        )
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def page_for_tenant(
        self,
        *,
        tenant_id: int,
        page: int,
        size: int,
        status: str | None = None,
        namespace: str | None = None,
        topic: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
    ) -> tuple[ConcurrentStableList[MsgReplayTask], int]:
        """按租户 + 条件分页（`created_at` 倒序、`id` 倒序稳定排序）。

        **租户条件显式写进 SQL**：请求侧 `_select()` 虽已按上下文过滤，但这里再写一次是为了让
        「取哪一页」与「算 total」用同一份条件——两处口径分叉时会出现「第 3 页为空但 total 很大」。

        Args:
            tenant_id: 租户主键。
            page: 页码（从 1 起）。
            size: 每页条数。
            status: 状态精确匹配；None 不限制。
            namespace: 命名空间精确匹配；None 不限制。
            topic: 主题名精确匹配（快照列，主题停用后仍可查）；None 不限制。
            created_from: 创建时间下限（含）；None 不限制。
            created_to: 创建时间上限（不含）；None 不限制。

        Returns:
            tuple[ConcurrentStableList[MsgReplayTask], int]: 当页行与过滤后总数。
        """
        conditions = ConcurrentStableList([self._column("tenant_id") == tenant_id])
        if status:
            conditions.add(self._column("status") == status)
        if namespace:
            conditions.add(self._column("namespace") == namespace)
        if topic:
            conditions.add(self._column("topic") == topic)
        if created_from is not None:
            conditions.add(self._column("created_at") >= created_from)
        if created_to is not None:
            conditions.add(self._column("created_at") < created_to)

        statement = self._select().where(*conditions)
        count_statement = select(func.count()).select_from(MsgReplayTask).where(*conditions)
        total = int((await self._session.execute(count_statement)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    statement.order_by(self._column("created_at").desc(), self._column("id").desc())
                    .offset((page - 1) * size)
                    .limit(size)
                )
            )
            .scalars()
            .all()
        )
        return ConcurrentStableList(rows), total
