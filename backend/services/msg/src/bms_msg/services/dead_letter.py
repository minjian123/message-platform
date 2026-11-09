"""死信服务（06-5）：查询、惰性回写与处置状态迁移。

口径（详细设计 06-5-1 §7 / §8、06-5-2 §4）：

- **状态迁移只在本层**：仓储只做行级读写，接口层只编排；
- **事务边界**：查询走读事务，处置 / 回写各自开 `begin()` 并提交（请求会话不自动提交）；
- **惰性回写**：列表 / 详情查询时把 `replaying` 行按重放任务终态收口，
  回写失败只告警、不阻塞查询；
- **终态字段**：进入 `resolved` / `ignored` / `archived` 写处置人 / 时间 / 原因；
- **不软删除**：主题删除 / 租户清理走仓储 `purge_topics`。
"""

from __future__ import annotations

from datetime import UTC, datetime

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.roots import BaseFrameworkObject
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.errors import MsgDeadLetterInvalidStateError, MsgDeadLetterNotFoundError
from bms_msg.models.dead_letter import (
    DLQ_ARCHIVABLE_STATUSES,
    DLQ_STATUS_ARCHIVED,
    DLQ_STATUS_IGNORED,
    DLQ_STATUS_PENDING,
    DLQ_STATUS_REPLAYING,
    DLQ_STATUS_RESOLVED,
    MsgDeadLetter,
)
from bms_msg.models.replay import TASK_CANCELLED, TASK_COMPLETED, TASK_FAILED
from bms_msg.repositories.dead_letter import DeadLetterRepository
from bms_msg.repositories.replay import ReplayTaskRepository

__all__ = ["DeadLetterService"]

_LOGGER = get_logger("bms_msg.dead_letter_service")


def _utcnow() -> datetime:
    """当前 UTC naive 时间（与库列口径一致）。"""
    return datetime.now(UTC).replace(tzinfo=None)


class DeadLetterService(BaseFrameworkObject):
    """死信服务：列表 / 详情 / 忽略 / 归档 / 重放状态迁移。"""

    def __init__(
        self,
        *,
        uow: UnitOfWork,
        dead_letters: DeadLetterRepository,
        replay_tasks: ReplayTaskRepository,
    ) -> None:
        """初始化。

        Args:
            uow: 平台库工作单元（事务边界）。
            dead_letters: 死信仓储。
            replay_tasks: 重放任务仓储（惰性回写查任务终态）。
        """
        self._uow = uow
        self._dead_letters = dead_letters
        self._replay_tasks = replay_tasks

    async def list(
        self,
        *,
        tenant_id: int,
        page: int,
        size: int,
        status: str | None = None,
        reason: str | None = None,
        topic: str | None = None,
        partition_no: int | None = None,
        failed_from: datetime | None = None,
        failed_to: datetime | None = None,
        keyword: str | None = None,
    ) -> tuple[ConcurrentStableList[MsgDeadLetter], int]:
        """按租户 + 条件分页（含惰性回写）。

        Args:
            tenant_id: 租户主键。
            page: 页码（从 1 起）。
            size: 每页条数。
            status: 状态精确匹配。
            reason: 转入原因。
            topic: 主题名。
            partition_no: 分区号。
            failed_from: 失败时间下限（含）。
            failed_to: 失败时间上限（不含）。
            keyword: `log_id` / `message_id` 模糊。

        Returns:
            tuple[ConcurrentStableList[MsgDeadLetter], int]: 当页行与过滤后总数。
        """
        rows, total = await self._page(
            tenant_id=tenant_id,
            page=page,
            size=size,
            status=status,
            reason=reason,
            topic=topic,
            partition_no=partition_no,
            failed_from=failed_from,
            failed_to=failed_to,
            keyword=keyword,
        )
        replaying_ids = ConcurrentStableList(
            int(row.id) for row in rows if row.status == DLQ_STATUS_REPLAYING and row.replay_task_id is not None
        )
        if not replaying_ids:
            return rows, total
        await self._uow.rollback()
        try:
            async with self._uow.begin():
                await self._sync_replaying_ids(replaying_ids, tenant_id=tenant_id)
        except Exception as exc:  # 惰性回写失败不阻塞查询
            _LOGGER.warning("msg_dead_letter_sync_failed", error=repr(exc))
            # 回滚已使加载对象过期，重新取一页（不再回写）后再返回。
            return await self._page(
                tenant_id=tenant_id,
                page=page,
                size=size,
                status=status,
                reason=reason,
                topic=topic,
                partition_no=partition_no,
                failed_from=failed_from,
                failed_to=failed_to,
                keyword=keyword,
            )
        rows, total = await self._page(
            tenant_id=tenant_id,
            page=page,
            size=size,
            status=status,
            reason=reason,
            topic=topic,
            partition_no=partition_no,
            failed_from=failed_from,
            failed_to=failed_to,
            keyword=keyword,
        )
        return rows, total

    async def detail(self, *, item_id: int, tenant_id: int) -> MsgDeadLetter:
        """取死信详情（含惰性回写）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键。

        Returns:
            MsgDeadLetter: 死信行。

        Raises:
            MsgDeadLetterNotFoundError: 不存在 / 跨租户（350029）。
        """
        row = await self._dead_letters.get_for_tenant(item_id=item_id, tenant_id=tenant_id)
        if row is None:
            raise MsgDeadLetterNotFoundError("死信不存在")
        if row.status != DLQ_STATUS_REPLAYING:
            return row
        row_id = int(row.id)
        await self._uow.rollback()
        try:
            async with self._uow.begin():
                await self._sync_replaying_ids(ConcurrentStableList([row_id]), tenant_id=tenant_id)
        except Exception as exc:  # 惰性回写失败不阻塞查询
            _LOGGER.warning("msg_dead_letter_sync_failed", error=repr(exc))
            return await self._get(item_id=item_id, tenant_id=tenant_id)
        return await self._get(item_id=item_id, tenant_id=tenant_id)

    async def ignore(self, *, item_id: int, tenant_id: int, reason: str, actor_id: int | None) -> str:
        """忽略死信（原因必填由契约层校验）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键。
            reason: 处置原因。
            actor_id: 操作人主键。

        Returns:
            str: 新状态（`ignored`）。

        Raises:
            MsgDeadLetterNotFoundError: 不存在 / 跨租户（350029）。
            MsgDeadLetterInvalidStateError: 非 `pending`（350030）。
        """
        await self._uow.rollback()
        async with self._uow.begin():
            row = await self._require_row(item_id=item_id, tenant_id=tenant_id)
            self._require_status(row, allowed=ConcurrentStableFrozenList((DLQ_STATUS_PENDING,)), action="忽略")
            row.status = DLQ_STATUS_IGNORED
            row.disposition_reason = reason
            row.disposed_by = actor_id
            row.disposed_at = _utcnow()
            await self._flush()
        return DLQ_STATUS_IGNORED

    async def archive(self, *, item_id: int, tenant_id: int, reason: str | None, actor_id: int | None) -> str:
        """归档死信（`pending` / `ignored` / `resolved` 可归档）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键。
            reason: 处置原因（可选）。
            actor_id: 操作人主键。

        Returns:
            str: 新状态（`archived`）。

        Raises:
            MsgDeadLetterNotFoundError: 不存在 / 跨租户（350029）。
            MsgDeadLetterInvalidStateError: `replaying` / `archived`（350030）。
        """
        await self._uow.rollback()
        async with self._uow.begin():
            row = await self._require_row(item_id=item_id, tenant_id=tenant_id)
            self._require_status(row, allowed=DLQ_ARCHIVABLE_STATUSES, action="归档")
            row.status = DLQ_STATUS_ARCHIVED
            row.disposition_reason = reason
            row.disposed_by = actor_id
            row.disposed_at = _utcnow()
            await self._flush()
        return DLQ_STATUS_ARCHIVED

    async def mark_replaying(self, *, item_id: int, tenant_id: int, replay_task_id: int) -> str:
        """把死信置为 `replaying` 并记录重放任务（条件更新防并发）。

        Args:
            item_id: 死信主键。
            tenant_id: 租户主键。
            replay_task_id: 重放任务主键。

        Returns:
            str: 新状态（`replaying`）。

        Raises:
            MsgDeadLetterNotFoundError: 不存在 / 跨租户（350029）。
            MsgDeadLetterInvalidStateError: 非 `pending` 或并发已被其他请求处置（350030）。
        """
        await self._uow.rollback()
        async with self._uow.begin():
            row = await self._require_row(item_id=item_id, tenant_id=tenant_id)
            self._require_status(row, allowed=ConcurrentStableFrozenList((DLQ_STATUS_PENDING,)), action="重放")
            updated = await self._dead_letters.mark_replaying(
                item_id=item_id, tenant_id=tenant_id, replay_task_id=replay_task_id
            )
            if not updated:
                raise MsgDeadLetterInvalidStateError("死信已被其他请求处置，请刷新后重试")
            await self._flush()
        return DLQ_STATUS_REPLAYING

    async def _page(
        self,
        *,
        tenant_id: int,
        page: int,
        size: int,
        status: str | None,
        reason: str | None,
        topic: str | None,
        partition_no: int | None,
        failed_from: datetime | None,
        failed_to: datetime | None,
        keyword: str | None,
    ) -> tuple[ConcurrentStableList[MsgDeadLetter], int]:
        """按条件分页（单次读事务）。"""
        return await self._dead_letters.page_for_tenant(
            tenant_id=tenant_id,
            page=page,
            size=size,
            status=status,
            exclude_archived=not status,
            reason=reason,
            topic=topic,
            partition_no=partition_no,
            failed_from=failed_from,
            failed_to=failed_to,
            keyword=keyword,
        )

    async def _get(self, *, item_id: int, tenant_id: int) -> MsgDeadLetter:
        """重新取一行（事务切换后对象需重新加载）。"""
        row = await self._dead_letters.get_for_tenant(item_id=item_id, tenant_id=tenant_id)
        if row is None:
            raise MsgDeadLetterNotFoundError("死信不存在")
        return row

    async def _require_row(self, *, item_id: int, tenant_id: int) -> MsgDeadLetter:
        """取行并要求存在。"""
        return await self._get(item_id=item_id, tenant_id=tenant_id)

    async def _sync_replaying_ids(self, item_ids: ConcurrentStableList[int], *, tenant_id: int) -> None:
        """按主键把 `replaying` 行按重放任务终态回写（调用方持有事务）。"""
        for item_id in item_ids:
            row = await self._dead_letters.get_for_tenant(item_id=int(item_id), tenant_id=tenant_id)
            if row is None or row.status != DLQ_STATUS_REPLAYING or row.replay_task_id is None:
                continue
            task = await self._replay_tasks.get(int(row.replay_task_id))
            if task is None:
                continue
            if task.status == TASK_COMPLETED:
                row.status = DLQ_STATUS_RESOLVED
                row.disposed_at = _utcnow()
            elif task.status in (TASK_FAILED, TASK_CANCELLED):
                row.status = DLQ_STATUS_PENDING
                row.disposed_at = _utcnow()
        await self._flush()

    async def _flush(self) -> None:
        """刷写当前事务（工作单元会话）。"""
        session = self._uow.session
        if session is not None:
            await session.flush()

    @staticmethod
    def _require_status(row: MsgDeadLetter, *, allowed: ConcurrentStableFrozenList[str], action: str) -> None:
        """校验处置前置状态。

        Args:
            row: 死信行。
            allowed: 允许的状态集合。
            action: 动作中文名（错误信息用）。

        Raises:
            MsgDeadLetterInvalidStateError: 当前状态不在允许集合（350030）。
        """
        if row.status not in allowed:
            raise MsgDeadLetterInvalidStateError(f"死信当前状态为 {row.status}，不允许{action}")
