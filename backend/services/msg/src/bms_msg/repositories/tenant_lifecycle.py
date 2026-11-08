"""租户生命周期账本仓储（`msg_tenant_lifecycle`）与删除清理查询。

**为什么不用 `BaseDbRepository` 的 `tenant_scoped`**：生命周期消费在后台运行，没有请求租户上下文；
所有查询必须显式按事件载荷里的 `tenant_id` 过滤，不能依赖 `ContextVar`。本仓储因此直接用
SQLAlchemy 表达式，不做请求级租户注入。

**读写边界**：仓储只 `flush` / 执行语句，不 `commit`；事务边界由账本或清扫服务的外层会话控制。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.objects import BaseValueObject
from bms_core.db.session import DbSession
from sqlalchemy import delete, func, select, update

from bms_msg.models.partition import MsgPartition
from bms_msg.models.replay import TASK_CANCELLED, TASK_PENDING, TASK_RUNNING, MsgReplayTask
from bms_msg.models.tenant_lifecycle import ACTION_DELETED, MsgTenantLifecycle
from bms_msg.models.topic import MsgTopic

__all__ = [
    "TenantLifecycleRepository",
    "TenantPurgeDue",
    "TenantReplayRef",
    "TenantTopicRef",
]


@dataclass(frozen=True)
class TenantPurgeDue(BaseValueObject):
    """一条到期待清理的租户账本行（清扫入口的最小视图）。"""

    tenant_id: int
    """租户主键。"""

    purge_after: datetime
    """本轮到期点（UTC naive）。"""

    deleted_event_at: datetime | None
    """删除事件生效时刻（UTC naive；旧数据可能为空）。"""


@dataclass(frozen=True)
class TenantTopicRef(BaseValueObject):
    """删除清理所需的主题最小视图（不读 ORM 全字段，避免会话关闭后懒加载）。"""

    tenant_id: int
    """租户主键。"""

    topic_id: int
    """主题主键。"""

    namespace: str
    """命名空间。"""

    name: str
    """主题名。"""

    partition_count: int
    """分区数。"""

    retention_ms: int
    """保留时长（毫秒）。"""


@dataclass(frozen=True)
class TenantReplayRef(BaseValueObject):
    """删除清理所需的重放任务最小视图。"""

    task_id: int
    """任务主键。"""

    topic_id: int
    """主题主键。"""

    status: str
    """任务状态。"""


class TenantLifecycleRepository:
    """生命周期账本与删除清理的数据库访问（显式 `tenant_id`，不依赖请求上下文）。"""

    def __init__(self, session: DbSession) -> None:
        """初始化。

        Args:
            session: 当前数据库会话（事务边界由调用方控制）。
        """
        self._session = session

    async def get_by_action(self, *, tenant_id: int, action: str) -> MsgTenantLifecycle | None:
        """按 `(tenant_id, action)` 取账本行。

        Args:
            tenant_id: 租户主键。
            action: 动作名。

        Returns:
            MsgTenantLifecycle | None: 账本行；不存在返回 None。
        """
        statement = select(MsgTenantLifecycle).where(
            MsgTenantLifecycle.tenant_id == int(tenant_id),
            MsgTenantLifecycle.action == action,
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def get_version(self, *, tenant_id: int, action: str) -> int | None:
        """取已处理版本；无行返回 None（由调用方映射为 `-1`）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。

        Returns:
            int | None: 已处理版本；无行返回 None。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=action)
        return None if row is None else int(row.applied_version)

    async def upsert_applied(
        self, *, tenant_id: int, action: str, version: int, event_id: str | None
    ) -> MsgTenantLifecycle:
        """推进某动作的已处理版本（同版本 / 旧版本不重写）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。
            version: 事件版本戳。
            event_id: 事件 ID（排障用；可空）。

        Returns:
            MsgTenantLifecycle: 当前账本行。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=action)
        if row is None:
            row = MsgTenantLifecycle(
                tenant_id=int(tenant_id),
                action=action,
                applied_version=int(version),
                last_event_id=event_id,
            )
            self._session.add(row)
            await self._session.flush()
            return row
        if int(version) > int(row.applied_version):
            row.applied_version = int(version)
            row.last_event_id = event_id
            await self._session.flush()
        return row

    async def mark_deleted(
        self,
        *,
        tenant_id: int,
        version: int,
        deleted_event_at: datetime,
        purge_after: datetime,
    ) -> MsgTenantLifecycle:
        """写 / 更新 `deleted` 行的清理排期（不推进 `applied_version`）。

        **不在这里写 `applied_version`**：动作完整成功（含易失资源清理与审计）后由分发器统一
        `mark_applied`；否则「排期已写、动作失败」会被误判为已处理而跳过重试。

        Args:
            tenant_id: 租户主键。
            version: 事件版本戳。
            deleted_event_at: 删除事件生效时刻（UTC naive）。
            purge_after: 最早到期点（UTC naive）。

        Returns:
            MsgTenantLifecycle: 当前账本行。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
        if row is None:
            row = MsgTenantLifecycle(
                tenant_id=int(tenant_id),
                action=ACTION_DELETED,
                applied_version=-1,
                deleted_event_at=deleted_event_at,
                purge_after=purge_after,
                purged_at=None,
                failure_count=0,
                last_error=None,
            )
            self._session.add(row)
            await self._session.flush()
            return row
        # 同版本重试不延长保留期；新版本以最新删除事件为准。
        if int(version) > int(row.applied_version):
            row.deleted_event_at = deleted_event_at
            row.purge_after = purge_after
            row.purged_at = None
            row.failure_count = 0
            row.last_error = None
            await self._session.flush()
        return row

    async def cancel_purge(self, *, tenant_id: int, event_id: str | None) -> bool:
        """取消 `deleted` 行的待清理（保留 `deleted_event_at` 供审计）。

        Args:
            tenant_id: 租户主键。
            event_id: 启用事件 ID（可空）。

        Returns:
            bool: 是否确有行被取消（无 `deleted` 行返回 False）。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
        if row is None:
            return False
        row.purge_after = None
        row.last_event_id = event_id
        await self._session.flush()
        return True

    async def set_purge_after(self, *, tenant_id: int, at: datetime) -> None:
        """把下一次清理时刻推进到剩余主题的最早到期点。

        Args:
            tenant_id: 租户主键。
            at: 下一次到期点（UTC naive）。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
        if row is None:  # pragma: no cover - 清扫入口必先读到行
            return
        row.purge_after = at
        await self._session.flush()

    async def mark_purged(self, *, tenant_id: int, at: datetime) -> None:
        """标记该租户全部主题清理完成。

        Args:
            tenant_id: 租户主键。
            at: 完成时刻（UTC naive）。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
        if row is None:  # pragma: no cover - 清扫入口必先读到行
            return
        row.purged_at = at
        row.purge_after = None
        row.failure_count = 0
        row.last_error = None
        await self._session.flush()

    async def record_purge_failure(self, *, tenant_id: int, error: str) -> None:
        """记录一次到期清扫失败（连续计数 + 最近原因）。

        Args:
            tenant_id: 租户主键。
            error: 失败原因（截断 512）。
        """
        row = await self.get_by_action(tenant_id=tenant_id, action=ACTION_DELETED)
        if row is None:  # pragma: no cover - 清扫入口必先读到行
            return
        row.failure_count = int(row.failure_count) + 1
        row.last_error = error[:512]
        await self._session.flush()

    async def list_due_purges(self, *, now: datetime, limit: int = 100) -> ConcurrentStableList[TenantPurgeDue]:
        """取到期待清理的租户（按 `purge_after` 升序）。

        Args:
            now: 当前时刻（UTC naive）。
            limit: 单轮上限。

        Returns:
            ConcurrentStableList[TenantPurgeDue]: 到期待清理行。
        """
        statement = (
            select(
                MsgTenantLifecycle.tenant_id,
                MsgTenantLifecycle.purge_after,
                MsgTenantLifecycle.deleted_event_at,
            )
            .where(
                MsgTenantLifecycle.action == ACTION_DELETED,
                MsgTenantLifecycle.purged_at.is_(None),
                MsgTenantLifecycle.purge_after.is_not(None),
                MsgTenantLifecycle.purge_after <= now,
            )
            .order_by(MsgTenantLifecycle.purge_after, MsgTenantLifecycle.tenant_id)
            .limit(limit)
        )
        rows = (await self._session.execute(statement)).all()
        return ConcurrentStableList(
            TenantPurgeDue(
                tenant_id=int(row.tenant_id),
                purge_after=row.purge_after,
                deleted_event_at=row.deleted_event_at,
            )
            for row in rows
            if row.purge_after is not None
        )

    async def list_topics(self, *, tenant_id: int) -> ConcurrentStableList[TenantTopicRef]:
        """列出该租户的主题最小视图（删除清理权威清单）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantTopicRef]: 主题清单（主键升序）。
        """
        statement = (
            select(
                MsgTopic.tenant_id,
                MsgTopic.id,
                MsgTopic.namespace,
                MsgTopic.name,
                MsgTopic.partition_count,
                MsgTopic.retention_ms,
            )
            .where(MsgTopic.tenant_id == int(tenant_id))
            .order_by(MsgTopic.id)
        )
        rows = (await self._session.execute(statement)).all()
        return ConcurrentStableList(
            TenantTopicRef(
                tenant_id=int(row.tenant_id),
                topic_id=int(row.id),
                namespace=row.namespace,
                name=row.name,
                partition_count=int(row.partition_count),
                retention_ms=int(row.retention_ms),
            )
            for row in rows
        )

    async def list_replay_tasks(self, *, tenant_id: int) -> ConcurrentStableList[TenantReplayRef]:
        """列出该租户的重放任务最小视图。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableList[TenantReplayRef]: 任务清单（主键升序）。
        """
        statement = (
            select(MsgReplayTask.id, MsgReplayTask.topic_id, MsgReplayTask.status)
            .where(MsgReplayTask.tenant_id == int(tenant_id))
            .order_by(MsgReplayTask.id)
        )
        rows = (await self._session.execute(statement)).all()
        return ConcurrentStableList(
            TenantReplayRef(task_id=int(row.id), topic_id=int(row.topic_id), status=row.status) for row in rows
        )

    async def cancel_active_replay_tasks(self, *, tenant_id: int, at: datetime) -> int:
        """把该租户非终态重放任务置为 `cancelled`（不删行）。

        Args:
            tenant_id: 租户主键。
            at: 终态时刻（UTC naive）。

        Returns:
            int: 受影响行数。
        """
        statement = (
            update(MsgReplayTask)
            .where(
                MsgReplayTask.tenant_id == int(tenant_id),
                MsgReplayTask.status.in_((TASK_PENDING, TASK_RUNNING)),
            )
            .values(status=TASK_CANCELLED, finished_at=at)
        )
        result = await self._session.execute(statement)
        await self._session.flush()
        return max(int(getattr(result, "rowcount", 0) or 0), 0)

    async def delete_topics(self, *, tenant_id: int, topic_ids: Iterable[int]) -> int:
        """删除到期主题的元数据（先子后父；单事务由调用方保证）。

        Args:
            tenant_id: 租户主键。
            topic_ids: 到期主题主键。

        Returns:
            int: 删除行数合计。
        """
        ids = ConcurrentStableList(int(item) for item in topic_ids)
        if not ids:
            return 0
        deleted = 0
        replay = await self._session.execute(
            delete(MsgReplayTask).where(
                MsgReplayTask.tenant_id == int(tenant_id),
                MsgReplayTask.topic_id.in_(ids),
            )
        )
        deleted += max(int(getattr(replay, "rowcount", 0) or 0), 0)
        partitions = await self._session.execute(delete(MsgPartition).where(MsgPartition.topic_id.in_(ids)))
        deleted += max(int(getattr(partitions, "rowcount", 0) or 0), 0)
        topics = await self._session.execute(
            delete(MsgTopic).where(MsgTopic.tenant_id == int(tenant_id), MsgTopic.id.in_(ids))
        )
        deleted += max(int(getattr(topics, "rowcount", 0) or 0), 0)
        await self._session.flush()
        return deleted

    async def count_topics(self, *, tenant_id: int) -> int:
        """统计该租户主题数（删除排期为空时的判定）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 主题数。
        """
        statement = select(func.count()).select_from(MsgTopic).where(MsgTopic.tenant_id == int(tenant_id))
        return int((await self._session.execute(statement)).scalar_one())
