"""重放任务仓储（`msg_replay_task`）：任务与游标的读写。

口径：

- 声明 `tenant_scoped`：请求侧按上下文自动过滤 / 注入租户（跨租户访问 → 查不到 → `350016`）；
  **执行器侧无租户上下文**，`list_active()` 因此能跨租户捞任务——这是后台推进器的必要行为，
  不是隔离漏洞（任务主键是雪花，写入仍带 `tenant_id`）。
- 元数据表**不做软删除**（终态用 `status` 表达），故 `soft_delete_enabled = False`；
- 仓储只 `flush` 不 `commit`，事务边界在服务层 / 执行器的工作单元。
"""

from bms_core.core.concurrent import ConcurrentStableList, ConcurrentStableSet
from bms_core.repositories.base_db_repository import BaseDbRepository

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
