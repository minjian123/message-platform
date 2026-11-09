"""主题删除清理台账仓储（`msg_topic_cleanup`）。

**为什么不声明 `tenant_scoped`**：周期扫描与首轮执行都在**无请求租户上下文**的后台任务里跑，跨租户
按 `topic_id` 精确取行；若声明租户作用域，后台取不到任何行（当前租户为空）。租户隔离在这里不是
「自动加条件」，而是「调用方显式带 `tenant_id`」（受理时写入、影响预检按租户读）。

- 台账不做软删除（`soft_delete_enabled = False`），`delete` 即物理删除（本域不使用）；
- 写操作只 `flush`，事务边界在服务层工作单元。
"""

from bms_core.core.concurrent import ConcurrentStableList, ConcurrentStableSet
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import ColumnElement, select

from bms_msg.models.topic_cleanup import CLEANUP_PENDING, CLEANUP_RUNNING, MsgTopicCleanup

__all__ = ["TopicCleanupRepository"]


class TopicCleanupRepository(BaseDbRepository[MsgTopicCleanup]):
    """清理台账仓储：按主题取行、取待办行、推进步骤与状态。"""

    model = MsgTopicCleanup
    soft_delete_enabled = False
    sortable_fields = ConcurrentStableSet({"created_at", "status"})

    async def get_by_topic(self, topic_id: int) -> MsgTopicCleanup | None:
        """按主题主键取台账（`topic_id` 唯一）。

        Args:
            topic_id: 主题主键。

        Returns:
            MsgTopicCleanup | None: 台账；不存在返回 None。
        """
        statement = select(MsgTopicCleanup).where(MsgTopicCleanup.topic_id == int(topic_id))
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def list_active(self) -> ConcurrentStableList[MsgTopicCleanup]:
        """取待办台账（`pending` / `running`，按受理时间升序）。

        周期扫描与首轮执行共用：`running` 也要收进来——上一轮可能在进程退出时中断，
        台账停在 `running` 而没有后续驱动。

        Returns:
            ConcurrentStableList[MsgTopicCleanup]: 待办台账。
        """
        conditions: ConcurrentStableList[ColumnElement[bool]] = ConcurrentStableList(
            [
                MsgTopicCleanup.status.in_((CLEANUP_PENDING, CLEANUP_RUNNING)),
            ]
        )
        statement = select(MsgTopicCleanup).where(*conditions).order_by(MsgTopicCleanup.created_at, MsgTopicCleanup.id)
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())
