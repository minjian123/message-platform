"""迁移台账仓储（02-8）：`msg_partition_migration` 的读写、CAS 与进度刷新。

**为什么单独一层**：`ops.rebalance`（离线 CLI）与迁移编排共用同一套状态机，避免「CLI 一套写、
服务另一套写」；台账是「迁到哪了、谁执行的、结果如何」的唯一事实源。

**CAS 口径**：所有状态推进都用 `WHERE status = 期望值` 的条件更新并 `version + 1`，
并发执行被唯一约束与 CAS 双保险挡住（详见 02-8 详细设计 §7.4 / §15 #7）。
"""

from __future__ import annotations

import secrets
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.db.session import DbSession
from sqlalchemy import CursorResult, delete, func, select, update

from bms_msg.broker.replay import UowFactory
from bms_msg.core.config import (
    MIGRATION_OPERATOR_MAX_LENGTH,
    MIGRATION_PLAN_ID_PREFIX,
    MIGRATION_REASON_MAX_LENGTH,
)
from bms_msg.models.partition_migration import (
    PLAN_STATUS_CATCHING_UP,
    PLAN_STATUS_PLANNED,
    PLAN_STATUS_RUNNING,
    MsgPartitionMigration,
)

__all__ = [
    "DbMigrationLedger",
    "InMemoryMigrationLedger",
    "MigrationItem",
    "MigrationLedger",
    "MigrationPlanItem",
    "new_plan_id",
]

_LOGGER = get_logger("bms_msg.migration_ledger")

_PLAN_ID_TIME_FORMAT = "%Y%m%dT%H%M%SZ"
"""计划标识的时间段格式（UTC，字典序即时间序）。"""

_PLAN_ID_RANDOM_BYTES = 3
"""计划标识随机段字节数（6 位十六进制）。"""


def new_plan_id(*, now: datetime | None = None) -> str:
    """生成迁移计划标识（`rb-<UTC 时间>-<短随机>`）。

    Args:
        now: 当前时刻（用例注入）。

    Returns:
        str: 计划标识（≤ 64 字符）。
    """
    moment = now if now is not None else datetime.now(UTC)
    stamp = moment.strftime(_PLAN_ID_TIME_FORMAT)
    return f"{MIGRATION_PLAN_ID_PREFIX}-{stamp}-{secrets.token_hex(_PLAN_ID_RANDOM_BYTES)}"


def _clip(value: str, limit: int) -> str:
    """截断到列宽。

    Args:
        value: 原值。
        limit: 上限。

    Returns:
        str: 截断结果。
    """
    return value if len(value) <= limit else value[:limit]


@dataclass(frozen=True)
class MigrationPlanItem(BaseValueObject):
    """计划项（`plan` 生成、`apply` 落库的输入）。"""

    plan_id: str
    tenant_id: int
    namespace: str
    topic: str
    topic_id: int
    partition_no: int
    action: str
    from_replicas: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    to_replicas: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    from_leader: str = ""
    to_leader: str = ""
    attempt: int = 1


@dataclass(frozen=True)
class MigrationItem(BaseValueObject):
    """台账行（读模型）。"""

    id: int
    plan_id: str
    tenant_id: int
    namespace: str
    topic: str
    topic_id: int
    partition_no: int
    attempt: int
    action: str
    step: str
    from_replicas: ConcurrentStableFrozenList[str]
    to_replicas: ConcurrentStableFrozenList[str]
    from_leader: str
    to_leader: str
    status: str
    result: str
    isr_verified: bool
    retained_tail: int
    progress_leo: int
    progress_hw: int
    remaining_records: int
    operator: str
    reason: str
    error: str
    started_at: datetime | None
    finished_at: datetime | None
    version: int


class MigrationLedger(BaseFrameworkObject, ABC):
    """迁移台账端口（生产实现走平台库；用例注入内存实现）。"""

    @abstractmethod
    async def create(self, items: ConcurrentStableList[MigrationPlanItem]) -> int:
        """批量创建计划项（幂等：已存在的 `(plan, topic, partition)` 跳过）。

        Args:
            items: 计划项。

        Returns:
            int: 实际新建行数。
        """

    @abstractmethod
    async def load(self, plan_id: str) -> ConcurrentStableList[MigrationItem]:
        """读一个计划的全部行（按分区号升序）。

        Args:
            plan_id: 计划标识。

        Returns:
            ConcurrentStableList[MigrationItem]: 台账行。
        """

    @abstractmethod
    async def load_one(self, *, plan_id: str, topic_id: int, partition_no: int) -> MigrationItem | None:
        """读单个计划项。

        Args:
            plan_id: 计划标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MigrationItem | None: 台账行；不存在返回 None。
        """

    @abstractmethod
    async def latest_plan_id(self) -> str | None:
        """取最近一个计划标识（按创建时间倒序）。

        Returns:
            str | None: 计划标识；无记录返回 None。
        """

    @abstractmethod
    async def mark_running(self, *, item_id: int, operator: str, reason: str, started_at: datetime) -> bool:
        """`planned` → `running`（CAS）。

        Args:
            item_id: 台账主键。
            operator: 执行人。
            reason: 原因。
            started_at: 开始时刻（UTC）。

        Returns:
            bool: 更新成功 True；状态已变 False。
        """

    @abstractmethod
    async def mark_catching_up(self, *, item_id: int, result: str, isr_verified: bool, retained_tail: int) -> bool:
        """`running` → `catching_up`（分配已生效，等待追平）。

        Args:
            item_id: 台账主键。
            result: 切主结果（`truncated` / `retained` / `noop` / 空）。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。

        Returns:
            bool: 更新成功 True。
        """

    @abstractmethod
    async def update_progress(self, *, item_id: int, leo: int, hw: int, remaining: int) -> bool:
        """刷新追平进度（`catching_up` 行）。

        Args:
            item_id: 台账主键。
            leo: 目标副本 LEO。
            hw: leader HW。
            remaining: 剩余条数。

        Returns:
            bool: 更新成功 True。
        """

    @abstractmethod
    async def update_step(self, *, item_id: int, step: str) -> bool:
        """刷新执行阶段。

        Args:
            item_id: 台账主键。
            step: 阶段标识。

        Returns:
            bool: 更新成功 True。
        """

    @abstractmethod
    async def mark_terminal(
        self,
        *,
        item_id: int,
        status: str,
        result: str,
        isr_verified: bool,
        retained_tail: int,
        error: str,
        finished_at: datetime,
    ) -> bool:
        """进入终态（`succeeded` / `failed` / `degraded` / `cancelled`）。

        Args:
            item_id: 台账主键。
            status: 终态。
            result: 切主结果。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。
            error: 失败 / 降级说明（截断 512）。
            finished_at: 完成时刻（UTC）。

        Returns:
            bool: 更新成功 True。
        """

    @abstractmethod
    async def has_active(self, *, topic_id: int, partition_no: int, exclude_plan_id: str | None = None) -> bool:
        """同一分区是否已有未完成台账（`planned` / `running` / `catching_up`）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            exclude_plan_id: 排除的计划（`--resume` 同计划续跑）。

        Returns:
            bool: 存在未完成项 True。
        """

    @abstractmethod
    async def count_by_status(self) -> ConcurrentStableDict[str, int]:
        """按状态汇总台账行数（指标采样）。

        Returns:
            ConcurrentStableDict[str, int]: 状态 → 行数。
        """

    @abstractmethod
    async def sum_remaining(self) -> int:
        """汇总 `catching_up` 行的剩余条数（指标采样）。

        Returns:
            int: 剩余条数之和。
        """

    @abstractmethod
    async def delete_plan(self, plan_id: str) -> int:
        """物理删除一个计划（仅测试 / 清理用）。

        Args:
            plan_id: 计划标识。

        Returns:
            int: 删除行数。
        """


_ACTIVE_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (PLAN_STATUS_PLANNED, PLAN_STATUS_RUNNING, PLAN_STATUS_CATCHING_UP)
)
"""未完成状态（`has_active` 判据）。"""


def _item_from_row(row: MsgPartitionMigration) -> MigrationItem:
    """把 ORM 行转成读模型。

    Args:
        row: ORM 行。

    Returns:
        MigrationItem: 读模型。
    """
    return MigrationItem(
        id=int(row.id),
        plan_id=str(row.plan_id),
        tenant_id=int(row.tenant_id),
        namespace=str(row.namespace),
        topic=str(row.topic),
        topic_id=int(row.topic_id),
        partition_no=int(row.partition_no),
        attempt=int(row.attempt),
        action=str(row.action),
        step=str(row.step or ""),
        from_replicas=ConcurrentStableFrozenList(str(item) for item in row.from_replicas),
        to_replicas=ConcurrentStableFrozenList(str(item) for item in row.to_replicas),
        from_leader=str(row.from_leader),
        to_leader=str(row.to_leader),
        status=str(row.status),
        result=str(row.result),
        isr_verified=bool(row.isr_verified),
        retained_tail=int(row.retained_tail),
        progress_leo=int(row.progress_leo),
        progress_hw=int(row.progress_hw),
        remaining_records=int(row.remaining_records),
        operator=str(row.operator),
        reason=str(row.reason),
        error=str(row.error),
        started_at=row.started_at,
        finished_at=row.finished_at,
        version=int(row.version),
    )


class DbMigrationLedger(MigrationLedger):
    """平台库台账仓储（每次调用开短会话；无请求上下文）。"""

    def __init__(self, *, uow_factory: UowFactory) -> None:
        """初始化。

        Args:
            uow_factory: 平台库工作单元工厂。
        """
        self._uow_factory = uow_factory

    async def create(self, items: ConcurrentStableList[MigrationPlanItem]) -> int:
        """批量创建计划项（唯一键冲突按「已存在」跳过）。

        Args:
            items: 计划项。

        Returns:
            int: 实际新建行数。
        """
        if not items:
            return 0
        created = 0
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            existing = await self._existing_keys(session, items[0].plan_id)
            for item in items:
                key = (item.topic_id, item.partition_no)
                if key in existing:
                    continue
                session.add(
                    MsgPartitionMigration(
                        tenant_id=int(item.tenant_id),
                        namespace=item.namespace,
                        topic=item.topic,
                        plan_id=item.plan_id,
                        topic_id=int(item.topic_id),
                        partition_no=int(item.partition_no),
                        attempt=int(item.attempt),
                        action=item.action,
                        from_replicas=ConcurrentStableList(item.from_replicas),
                        to_replicas=ConcurrentStableList(item.to_replicas),
                        from_leader=item.from_leader,
                        to_leader=item.to_leader,
                        status=PLAN_STATUS_PLANNED,
                    )
                )
                created += 1
            await session.flush()
            await uow.commit()
        return created

    async def _existing_keys(self, session: DbSession, plan_id: str) -> ConcurrentStableDict[tuple[int, int], bool]:
        """取计划内已存在的分区键（幂等跳过依据）。

        Args:
            session: 平台库会话。
            plan_id: 计划标识。

        Returns:
            ConcurrentStableDict[tuple[int, int], bool]: 已存在键集合。
        """
        statement = select(MsgPartitionMigration.topic_id, MsgPartitionMigration.partition_no).where(
            MsgPartitionMigration.plan_id == plan_id
        )
        rows = (await session.execute(statement)).all()
        keys: ConcurrentStableDict[tuple[int, int], bool] = ConcurrentStableDict()
        for topic_id, partition_no in rows:
            keys.set((int(topic_id), int(partition_no)), True)
        return keys

    async def load(self, plan_id: str) -> ConcurrentStableList[MigrationItem]:
        """读一个计划的全部行。

        Args:
            plan_id: 计划标识。

        Returns:
            ConcurrentStableList[MigrationItem]: 台账行。
        """
        statement = (
            select(MsgPartitionMigration)
            .where(MsgPartitionMigration.plan_id == plan_id)
            .order_by(MsgPartitionMigration.topic_id, MsgPartitionMigration.partition_no)
        )
        async with self._uow_factory() as uow:
            rows = (await cast("DbSession", uow.session).execute(statement)).scalars().all()
        return ConcurrentStableList(_item_from_row(row) for row in rows)

    async def load_one(self, *, plan_id: str, topic_id: int, partition_no: int) -> MigrationItem | None:
        """读单个计划项。

        Args:
            plan_id: 计划标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MigrationItem | None: 台账行。
        """
        statement = select(MsgPartitionMigration).where(
            MsgPartitionMigration.plan_id == plan_id,
            MsgPartitionMigration.topic_id == int(topic_id),
            MsgPartitionMigration.partition_no == int(partition_no),
        )
        async with self._uow_factory() as uow:
            row = (await cast("DbSession", uow.session).execute(statement)).scalar_one_or_none()
        return None if row is None else _item_from_row(row)

    async def latest_plan_id(self) -> str | None:
        """取最近一个计划标识。

        Returns:
            str | None: 计划标识。
        """
        statement = select(MsgPartitionMigration.plan_id).order_by(MsgPartitionMigration.created_at.desc()).limit(1)
        async with self._uow_factory() as uow:
            row = (await cast("DbSession", uow.session).execute(statement)).first()
        return None if row is None else str(row[0])

    async def mark_running(self, *, item_id: int, operator: str, reason: str, started_at: datetime) -> bool:
        """`planned` → `running`（CAS）。

        Args:
            item_id: 台账主键。
            operator: 执行人。
            reason: 原因。
            started_at: 开始时刻（UTC）。

        Returns:
            bool: 更新成功 True。
        """
        return await self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED,)),
            values=ConcurrentStableDict(
                {
                    "status": PLAN_STATUS_RUNNING,
                    "operator": _clip(operator, MIGRATION_OPERATOR_MAX_LENGTH),
                    "reason": _clip(reason, MIGRATION_REASON_MAX_LENGTH),
                    "started_at": started_at,
                }
            ),
        )

    async def mark_catching_up(self, *, item_id: int, result: str, isr_verified: bool, retained_tail: int) -> bool:
        """`running` → `catching_up`（CAS）。

        Args:
            item_id: 台账主键。
            result: 切主结果。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。

        Returns:
            bool: 更新成功 True。
        """
        return await self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_RUNNING,)),
            values=ConcurrentStableDict(
                {
                    "status": PLAN_STATUS_CATCHING_UP,
                    "result": result,
                    "isr_verified": isr_verified,
                    "retained_tail": int(retained_tail),
                }
            ),
        )

    async def update_progress(self, *, item_id: int, leo: int, hw: int, remaining: int) -> bool:
        """刷新追平进度。

        Args:
            item_id: 台账主键。
            leo: 目标副本 LEO。
            hw: leader HW。
            remaining: 剩余条数。

        Returns:
            bool: 更新成功 True。
        """
        return await self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_CATCHING_UP,)),
            values=ConcurrentStableDict(
                {"progress_leo": int(leo), "progress_hw": int(hw), "remaining_records": int(remaining)}
            ),
        )

    async def update_step(self, *, item_id: int, step: str) -> bool:
        """刷新执行阶段。

        Args:
            item_id: 台账主键。
            step: 阶段标识。

        Returns:
            bool: 更新成功 True。
        """
        return await self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED, PLAN_STATUS_RUNNING, PLAN_STATUS_CATCHING_UP)),
            values=ConcurrentStableDict({"step": step}),
        )

    async def mark_terminal(
        self,
        *,
        item_id: int,
        status: str,
        result: str,
        isr_verified: bool,
        retained_tail: int,
        error: str,
        finished_at: datetime,
    ) -> bool:
        """进入终态（CAS 接受 `running` / `catching_up` / `planned`）。

        Args:
            item_id: 台账主键。
            status: 终态。
            result: 切主结果。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。
            error: 说明（截断 512）。
            finished_at: 完成时刻（UTC）。

        Returns:
            bool: 更新成功 True。
        """
        return await self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED, PLAN_STATUS_RUNNING, PLAN_STATUS_CATCHING_UP)),
            values=ConcurrentStableDict(
                {
                    "status": status,
                    "result": result,
                    "isr_verified": isr_verified,
                    "retained_tail": int(retained_tail),
                    "error": _clip(error, MIGRATION_REASON_MAX_LENGTH),
                    "finished_at": finished_at,
                }
            ),
        )

    async def has_active(self, *, topic_id: int, partition_no: int, exclude_plan_id: str | None = None) -> bool:
        """同一分区是否已有未完成台账。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            exclude_plan_id: 排除的计划。

        Returns:
            bool: 存在未完成项 True。
        """
        statement = (
            select(func.count())
            .select_from(MsgPartitionMigration)
            .where(
                MsgPartitionMigration.topic_id == int(topic_id),
                MsgPartitionMigration.partition_no == int(partition_no),
                MsgPartitionMigration.status.in_(tuple(_ACTIVE_STATUSES)),
            )
        )
        if exclude_plan_id is not None:
            statement = statement.where(MsgPartitionMigration.plan_id != exclude_plan_id)
        async with self._uow_factory() as uow:
            count = (await cast("DbSession", uow.session).execute(statement)).scalar_one()
        return int(count) > 0

    async def count_by_status(self) -> ConcurrentStableDict[str, int]:
        """按状态汇总台账行数。

        Returns:
            ConcurrentStableDict[str, int]: 状态 → 行数。
        """
        statement = select(MsgPartitionMigration.status, func.count()).group_by(MsgPartitionMigration.status)
        async with self._uow_factory() as uow:
            rows = (await cast("DbSession", uow.session).execute(statement)).all()
        result: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for status, count in rows:
            result.set(str(status), int(count))
        return result

    async def sum_remaining(self) -> int:
        """汇总 `catching_up` 行的剩余条数。

        Returns:
            int: 剩余条数之和。
        """
        statement = select(func.coalesce(func.sum(MsgPartitionMigration.remaining_records), 0)).where(
            MsgPartitionMigration.status == PLAN_STATUS_CATCHING_UP
        )
        async with self._uow_factory() as uow:
            total = (await cast("DbSession", uow.session).execute(statement)).scalar_one()
        return int(total)

    async def delete_plan(self, plan_id: str) -> int:
        """物理删除一个计划。

        Args:
            plan_id: 计划标识。

        Returns:
            int: 删除行数。
        """
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            result = await session.execute(
                delete(MsgPartitionMigration).where(MsgPartitionMigration.plan_id == plan_id)
            )
            await session.flush()
            await uow.commit()
        return int(cast("CursorResult[object]", result).rowcount or 0)

    async def _cas(
        self, item_id: int, *, expected: ConcurrentStableFrozenList[str], values: ConcurrentStableDict[str, object]
    ) -> bool:
        """按状态条件更新（CAS + version + 1）。

        Args:
            item_id: 台账主键。
            expected: 允许的当前状态。
            values: 目标列。

        Returns:
            bool: 更新成功 True（0 行即状态已变）。
        """
        payload = ConcurrentStableDict(values)
        payload.set("version", MsgPartitionMigration.version + 1)
        statement = (
            update(MsgPartitionMigration)
            .where(
                MsgPartitionMigration.id == int(item_id),
                MsgPartitionMigration.status.in_(expected),
            )
            .values(**dict(payload.items()))
        )
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            result = await session.execute(statement)
            await session.flush()
            await uow.commit()
        updated = int(cast("CursorResult[object]", result).rowcount or 0) > 0
        if not updated:
            _LOGGER.warning("msg_partition_migration_cas_miss", item_id=item_id, expected=list(expected))
        return updated


class InMemoryMigrationLedger(MigrationLedger):
    """内存台账（用例替身；语义与 DB 实现一致）。"""

    def __init__(self) -> None:
        """初始化（空表）。"""
        self._rows: ConcurrentStableDict[int, MigrationItem] = ConcurrentStableDict()
        self._next_id = 1

    async def create(self, items: ConcurrentStableList[MigrationPlanItem]) -> int:
        """批量创建计划项（同计划同分区跳过）。

        Args:
            items: 计划项。

        Returns:
            int: 新建行数。
        """
        created = 0
        for item in items:
            if any(
                row.plan_id == item.plan_id and row.topic_id == item.topic_id and row.partition_no == item.partition_no
                for row in self._rows.values()
            ):
                continue
            row_id = self._next_id
            self._next_id += 1
            self._rows.set(
                row_id,
                MigrationItem(
                    id=row_id,
                    plan_id=item.plan_id,
                    tenant_id=item.tenant_id,
                    namespace=item.namespace,
                    topic=item.topic,
                    topic_id=item.topic_id,
                    partition_no=item.partition_no,
                    attempt=item.attempt,
                    action=item.action,
                    step="",
                    from_replicas=item.from_replicas,
                    to_replicas=item.to_replicas,
                    from_leader=item.from_leader,
                    to_leader=item.to_leader,
                    status=PLAN_STATUS_PLANNED,
                    result="",
                    isr_verified=True,
                    retained_tail=0,
                    progress_leo=0,
                    progress_hw=0,
                    remaining_records=0,
                    operator="",
                    reason="",
                    error="",
                    started_at=None,
                    finished_at=None,
                    version=1,
                ),
            )
            created += 1
        return created

    async def load(self, plan_id: str) -> ConcurrentStableList[MigrationItem]:
        """读一个计划的全部行。

        Args:
            plan_id: 计划标识。

        Returns:
            ConcurrentStableList[MigrationItem]: 台账行。
        """
        rows = [row for row in self._rows.values() if row.plan_id == plan_id]
        return ConcurrentStableList(sorted(rows, key=lambda row: (row.topic_id, row.partition_no)))

    async def load_one(self, *, plan_id: str, topic_id: int, partition_no: int) -> MigrationItem | None:
        """读单个计划项。

        Args:
            plan_id: 计划标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MigrationItem | None: 台账行。
        """
        for row in self._rows.values():
            if row.plan_id == plan_id and row.topic_id == int(topic_id) and row.partition_no == int(partition_no):
                return row
        return None

    async def latest_plan_id(self) -> str | None:
        """取最近一个计划标识。

        Returns:
            str | None: 计划标识。
        """
        if not self._rows:
            return None
        return max(self._rows.values(), key=lambda row: row.id).plan_id

    async def mark_running(self, *, item_id: int, operator: str, reason: str, started_at: datetime) -> bool:
        """`planned` → `running`（CAS）。

        Args:
            item_id: 台账主键。
            operator: 执行人。
            reason: 原因。
            started_at: 开始时刻。

        Returns:
            bool: 更新成功 True。
        """
        return self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED,)),
            values=ConcurrentStableDict(
                {
                    "status": PLAN_STATUS_RUNNING,
                    "operator": _clip(operator, MIGRATION_OPERATOR_MAX_LENGTH),
                    "reason": _clip(reason, MIGRATION_REASON_MAX_LENGTH),
                    "started_at": started_at,
                }
            ),
        )

    async def mark_catching_up(self, *, item_id: int, result: str, isr_verified: bool, retained_tail: int) -> bool:
        """`running` → `catching_up`（CAS）。

        Args:
            item_id: 台账主键。
            result: 切主结果。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。

        Returns:
            bool: 更新成功 True。
        """
        return self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_RUNNING,)),
            values=ConcurrentStableDict(
                {
                    "status": PLAN_STATUS_CATCHING_UP,
                    "result": result,
                    "isr_verified": isr_verified,
                    "retained_tail": int(retained_tail),
                }
            ),
        )

    async def update_progress(self, *, item_id: int, leo: int, hw: int, remaining: int) -> bool:
        """刷新追平进度。

        Args:
            item_id: 台账主键。
            leo: 目标副本 LEO。
            hw: leader HW。
            remaining: 剩余条数。

        Returns:
            bool: 更新成功 True。
        """
        return self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_CATCHING_UP,)),
            values=ConcurrentStableDict(
                {"progress_leo": int(leo), "progress_hw": int(hw), "remaining_records": int(remaining)}
            ),
        )

    async def update_step(self, *, item_id: int, step: str) -> bool:
        """刷新执行阶段。

        Args:
            item_id: 台账主键。
            step: 阶段标识。

        Returns:
            bool: 更新成功 True。
        """
        return self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED, PLAN_STATUS_RUNNING, PLAN_STATUS_CATCHING_UP)),
            values=ConcurrentStableDict({"step": step}),
        )

    async def mark_terminal(
        self,
        *,
        item_id: int,
        status: str,
        result: str,
        isr_verified: bool,
        retained_tail: int,
        error: str,
        finished_at: datetime,
    ) -> bool:
        """进入终态（CAS）。

        Args:
            item_id: 台账主键。
            status: 终态。
            result: 切主结果。
            isr_verified: 是否已验证 ISR。
            retained_tail: 保留的未提交条数。
            error: 说明。
            finished_at: 完成时刻。

        Returns:
            bool: 更新成功 True。
        """
        return self._cas(
            item_id,
            expected=ConcurrentStableFrozenList((PLAN_STATUS_PLANNED, PLAN_STATUS_RUNNING, PLAN_STATUS_CATCHING_UP)),
            values=ConcurrentStableDict(
                {
                    "status": status,
                    "result": result,
                    "isr_verified": isr_verified,
                    "retained_tail": int(retained_tail),
                    "error": _clip(error, MIGRATION_REASON_MAX_LENGTH),
                    "finished_at": finished_at,
                }
            ),
        )

    async def has_active(self, *, topic_id: int, partition_no: int, exclude_plan_id: str | None = None) -> bool:
        """同一分区是否已有未完成台账。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            exclude_plan_id: 排除的计划。

        Returns:
            bool: 存在未完成项 True。
        """
        for row in self._rows.values():
            if row.topic_id != int(topic_id) or row.partition_no != int(partition_no):
                continue
            if row.status not in _ACTIVE_STATUSES:
                continue
            if exclude_plan_id is not None and row.plan_id == exclude_plan_id:
                continue
            return True
        return False

    async def count_by_status(self) -> ConcurrentStableDict[str, int]:
        """按状态汇总台账行数。

        Returns:
            ConcurrentStableDict[str, int]: 状态 → 行数。
        """
        result: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for row in self._rows.values():
            result.set(row.status, result.get(row.status, 0) + 1)
        return result

    async def sum_remaining(self) -> int:
        """汇总 `catching_up` 行的剩余条数。

        Returns:
            int: 剩余条数之和。
        """
        return sum(row.remaining_records for row in self._rows.values() if row.status == PLAN_STATUS_CATCHING_UP)

    async def delete_plan(self, plan_id: str) -> int:
        """物理删除一个计划。

        Args:
            plan_id: 计划标识。

        Returns:
            int: 删除行数。
        """
        targets = [row_id for row_id, row in self._rows.items() if row.plan_id == plan_id]
        for row_id in targets:
            self._rows.pop(row_id, None)
        return len(targets)

    def _cas(
        self, item_id: int, *, expected: ConcurrentStableFrozenList[str], values: ConcurrentStableDict[str, object]
    ) -> bool:
        """按状态条件更新（内存实现）。

        Args:
            item_id: 台账主键。
            expected: 允许的当前状态。
            values: 目标列。

        Returns:
            bool: 更新成功 True。
        """
        row = self._rows.get(int(item_id))
        if row is None or row.status not in expected:
            _LOGGER.warning("msg_partition_migration_cas_miss", item_id=item_id, expected=list(expected))
            return False
        from dataclasses import replace

        updated = replace(row, **dict(values.items()), version=row.version + 1)
        self._rows.set(int(item_id), updated)
        return True
