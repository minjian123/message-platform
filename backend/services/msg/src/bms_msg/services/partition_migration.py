"""分区迁移编排（02-8）：计划预检、先扩后缩、追平等待与台账收口。

**边界**：本模块只编排「分配 → 切主 → 写副本集 → 等追平」；副本拉取与 ISR 判定归 02-4，
两阶段切主归 02-5，持久化与 CAS 归台账。

**为什么串行**：逐分区迁移避免同一时刻大量 epoch 推进与 ISR 抖动；单分区失败不回滚其他分区，
调用方按台账逐项重试（02-8 详细设计 §7.2）。
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.exceptions import InternalError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject
from bms_core.db.session import DbSession
from sqlalchemy import CursorResult, select, update

from bms_msg.audit.trail import REBALANCE_ACTION, AuditEmitter, emit_sensitive
from bms_msg.broker.election import (
    LeaderControlTransport,
    LeaderSwitchError,
    LeaderSwitchService,
    SwitchPartitionRequest,
    SwitchResult,
)
from bms_msg.broker.replay import UowFactory
from bms_msg.core.config import (
    MIGRATION_CATCH_UP_BUDGET_MS,
    MIGRATION_DEFAULT_WAIT_MS,
    MIGRATION_FORCE_DEGRADED_NOTE,
    MIGRATION_PROGRESS_POLL_MS,
    MS_PER_SECOND,
)
from bms_msg.models.partition import SWITCH_STATE_IDLE, MsgPartition
from bms_msg.models.partition_migration import (
    ACTION_EXPAND,
    ACTION_LEADER_MOVE,
    ACTION_NOOP,
    ACTION_REPLACE,
    ACTION_SHRINK,
    PLAN_STATUS_CATCHING_UP,
    PLAN_STATUS_DEGRADED,
    PLAN_STATUS_FAILED,
    PLAN_STATUS_PLANNED,
    PLAN_STATUS_RUNNING,
    PLAN_STATUS_SUCCEEDED,
    PLAN_TERMINAL_STATUSES,
    RESULT_NOOP,
    RESULT_RETAINED,
    RESULT_TRUNCATED,
)
from bms_msg.models.topic import MsgTopic
from bms_msg.services.migration_ledger import (
    MigrationItem,
    MigrationLedger,
    MigrationPlanItem,
    new_plan_id,
)
from bms_msg.services.partition_assignment import (
    PartitionAssigner,
    PartitionAssignment,
    PartitionAssignmentConfig,
)

__all__ = [
    "DbMigrationTopology",
    "MigrationApplyResult",
    "MigrationBlock",
    "MigrationPartition",
    "MigrationPlanResult",
    "MigrationStatusRow",
    "MigrationStepError",
    "MigrationTopology",
    "PartitionMigrationService",
]

_LOGGER = get_logger("bms_msg.partition_migration")

_REBALANCE_AUDIT_PATH = "/internal/msg/partition/rebalance"
"""重平衡审计路径（不是 HTTP 端点，只用于审计定位）。"""

_STEP_EXPAND = "expand"
"""执行阶段：扩容（新副本追平）。"""

_STEP_SHRINK = "shrink"
"""执行阶段：缩容（移除旧副本）。"""


@dataclass(frozen=True)
class MigrationPartition(BaseValueObject):
    """迁移所需的分区事实（含主题身份，供审计与台账）。"""

    tenant_id: int
    namespace: str
    topic: str
    topic_id: int
    partition_no: int
    leader: str
    replicas: ConcurrentStableFrozenList[str]
    epoch: int
    switch_state: str
    version: int


@dataclass(frozen=True)
class MigrationBlock(BaseValueObject):
    """预检阻断项（不生成台账行）。"""

    topic_id: int
    partition_no: int
    reason: str
    detail: str


@dataclass(frozen=True)
class MigrationPlanResult(BaseValueObject):
    """计划结果（动作表 + 阻断项）。"""

    plan_id: str
    items: ConcurrentStableList[MigrationPlanItem] = field(
        default_factory=lambda: ConcurrentStableList[MigrationPlanItem]()
    )
    blocks: ConcurrentStableList[MigrationBlock] = field(default_factory=lambda: ConcurrentStableList[MigrationBlock]())


@dataclass(frozen=True)
class MigrationApplyResult(BaseValueObject):
    """执行结果（终态计数 + 全部台账行）。"""

    plan_id: str
    items: ConcurrentStableList[MigrationItem] = field(default_factory=lambda: ConcurrentStableList[MigrationItem]())

    @property
    def succeeded(self) -> int:
        """成功条数。

        Returns:
            int: `succeeded` 行数。
        """
        return sum(1 for item in self.items if item.status == PLAN_STATUS_SUCCEEDED)

    @property
    def degraded(self) -> int:
        """降级条数。

        Returns:
            int: `degraded` 行数。
        """
        return sum(1 for item in self.items if item.status == PLAN_STATUS_DEGRADED)

    @property
    def failed(self) -> int:
        """失败条数。

        Returns:
            int: `failed` 行数。
        """
        return sum(1 for item in self.items if item.status == PLAN_STATUS_FAILED)

    @property
    def catching_up(self) -> int:
        """追平中条数。

        Returns:
            int: `catching_up` 行数。
        """
        return sum(1 for item in self.items if item.status == PLAN_STATUS_CATCHING_UP)


@dataclass(frozen=True)
class MigrationStatusRow(BaseValueObject):
    """`status` 输出行（台账 + 实时进度）。"""

    item: MigrationItem
    leader_leo: int = 0
    leader_hw: int = 0
    target_leo: int = 0
    remaining: int = 0
    caught_up: bool = False


class MigrationStepError(RuntimeError):
    """迁移步骤失败（原因 + 明细；写台账 `error`）。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        """初始化。

        Args:
            reason: 结构化原因。
            detail: 排障详情。
        """
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


class MigrationTopology(BaseFrameworkObject, ABC):
    """分区事实与分配写入端口（生产走平台库；用例注入内存实现）。"""

    @abstractmethod
    async def list_partitions(
        self, *, topic_ids: ConcurrentStableFrozenList[int] | None = None
    ) -> ConcurrentStableList[MigrationPartition]:
        """列出待迁移分区（可按主题过滤）。

        Args:
            topic_ids: 主题主键过滤（None = 全部）。

        Returns:
            ConcurrentStableList[MigrationPartition]: 分区事实（按主题 / 分区号升序）。
        """

    @abstractmethod
    async def read_partition(self, *, topic_id: int, partition_no: int) -> MigrationPartition | None:
        """读单个分区事实。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MigrationPartition | None: 分区事实；不存在返回 None。
        """

    @abstractmethod
    async def write_assignment(
        self,
        *,
        topic_id: int,
        partition_no: int,
        replicas: ConcurrentStableFrozenList[str],
        leader: str,
        expected_version: int,
    ) -> bool:
        """CAS 写副本集与 leader（`version` 不符即失败）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            replicas: 目标副本集。
            leader: 目标 leader。
            expected_version: 期望的当前版本。

        Returns:
            bool: 写入成功 True。
        """


class DbMigrationTopology(MigrationTopology):
    """平台库分区事实与分配写入（每次调用开短会话）。"""

    def __init__(self, *, uow_factory: UowFactory) -> None:
        """初始化。

        Args:
            uow_factory: 平台库工作单元工厂。
        """
        self._uow_factory = uow_factory

    async def list_partitions(
        self, *, topic_ids: ConcurrentStableFrozenList[int] | None = None
    ) -> ConcurrentStableList[MigrationPartition]:
        """列出待迁移分区。

        Args:
            topic_ids: 主题主键过滤。

        Returns:
            ConcurrentStableList[MigrationPartition]: 分区事实。
        """
        statement = (
            select(MsgPartition, MsgTopic)
            .join(MsgTopic, MsgPartition.topic_id == MsgTopic.id)
            .order_by(MsgPartition.topic_id, MsgPartition.partition_no)
        )
        if topic_ids:
            statement = statement.where(MsgPartition.topic_id.in_(tuple(int(item) for item in topic_ids)))
        async with self._uow_factory() as uow:
            rows = (await cast("DbSession", uow.session).execute(statement)).all()
        return ConcurrentStableList(_to_partition(partition, topic) for partition, topic in rows)

    async def read_partition(self, *, topic_id: int, partition_no: int) -> MigrationPartition | None:
        """读单个分区事实。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MigrationPartition | None: 分区事实。
        """
        statement = (
            select(MsgPartition, MsgTopic)
            .join(MsgTopic, MsgPartition.topic_id == MsgTopic.id)
            .where(MsgPartition.topic_id == int(topic_id), MsgPartition.partition_no == int(partition_no))
        )
        async with self._uow_factory() as uow:
            row = (await cast("DbSession", uow.session).execute(statement)).first()
        if row is None:
            return None
        return _to_partition(row[0], row[1])

    async def write_assignment(
        self,
        *,
        topic_id: int,
        partition_no: int,
        replicas: ConcurrentStableFrozenList[str],
        leader: str,
        expected_version: int,
    ) -> bool:
        """CAS 写副本集与 leader。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            replicas: 目标副本集。
            leader: 目标 leader。
            expected_version: 期望的当前版本。

        Returns:
            bool: 写入成功 True。
        """
        statement = (
            update(MsgPartition)
            .where(
                MsgPartition.topic_id == int(topic_id),
                MsgPartition.partition_no == int(partition_no),
                MsgPartition.version == int(expected_version),
            )
            .values(
                replicas=ConcurrentStableList(replicas),
                leader=leader,
                version=MsgPartition.version + 1,
            )
        )
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            result = await session.execute(statement)
            await session.flush()
        return int(cast("CursorResult[object]", result).rowcount or 0) > 0


def _to_partition(partition: MsgPartition, topic: MsgTopic) -> MigrationPartition:
    """把 ORM 行组成迁移事实。

    Args:
        partition: 分区行。
        topic: 主题行。

    Returns:
        MigrationPartition: 迁移事实。
    """
    return MigrationPartition(
        tenant_id=int(topic.tenant_id),
        namespace=str(topic.namespace),
        topic=str(topic.name),
        topic_id=int(partition.topic_id),
        partition_no=int(partition.partition_no),
        leader=str(partition.leader),
        replicas=ConcurrentStableFrozenList(str(item) for item in partition.replicas),
        epoch=int(partition.epoch or 0),
        switch_state=str(partition.switch_state or SWITCH_STATE_IDLE),
        version=int(partition.version),
    )


class PartitionMigrationService(BaseFrameworkObject):
    """重平衡编排（plan / apply / status）。"""

    def __init__(
        self,
        *,
        assigner: PartitionAssigner,
        ledger: MigrationLedger,
        topology: MigrationTopology,
        switch: LeaderSwitchService,
        control: LeaderControlTransport,
        audit: AuditEmitter,
        wait_ms: int = MIGRATION_DEFAULT_WAIT_MS,
        poll_interval_ms: int = MIGRATION_PROGRESS_POLL_MS,
        catch_up_budget_ms: int = MIGRATION_CATCH_UP_BUDGET_MS,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """初始化。

        Args:
            assigner: 分配器（缺省节点 / 副本数，plan 时按命令行参数重建）。
            ledger: 迁移台账。
            topology: 分区事实与分配写入。
            switch: 02-5 切主服务。
            control: 节点控制通道（读运行态）。
            audit: 审计出口（begin fail-closed；finished fail-open）。
            wait_ms: 追平等待缺省预算（毫秒）。
            poll_interval_ms: 进度轮询间隔（毫秒）。
            catch_up_budget_ms: 单分区追平上限（毫秒）。
            clock: 单调毫秒时钟（用例注入）。
            sleep: 异步等待函数（用例注入零等待）。
        """
        self._assigner = assigner
        self._ledger = ledger
        self._topology = topology
        self._switch = switch
        self._control = control
        self._audit = audit
        self._wait_ms = int(wait_ms)
        self._poll_interval_ms = int(poll_interval_ms)
        self._catch_up_budget_ms = int(catch_up_budget_ms)
        self._clock = clock if clock is not None else _monotonic_ms
        self._sleep = sleep if sleep is not None else asyncio.sleep

    async def plan(
        self,
        *,
        nodes: ConcurrentStableFrozenList[str],
        factor: int,
        min_insync: int,
        topic_ids: ConcurrentStableFrozenList[int] | None = None,
    ) -> MigrationPlanResult:
        """生成迁移计划（只读运行态；写 `planned` 台账与动作表）。

        Args:
            nodes: 候选节点（命令行口径）。
            factor: 副本数。
            min_insync: 最小同步副本数。
            topic_ids: 主题过滤（None = 全部）。

        Returns:
            MigrationPlanResult: 计划标识、动作项与阻断项。

        Raises:
            MigrationStepError: 参数非法（不生成计划）。
        """
        if not nodes:
            raise MigrationStepError("invalid_argument", "--nodes 不能为空")
        if not 1 <= factor <= len(nodes):
            raise MigrationStepError("invalid_argument", f"--factor 须在 1~{len(nodes)}：{factor}")
        if not 1 <= min_insync <= factor:
            raise MigrationStepError("invalid_argument", f"--min-insync 须在 1~{factor}：{min_insync}")
        plan_assigner = PartitionAssigner(
            PartitionAssignmentConfig(
                enabled=True,
                node_id=self._assigner.config.node_id,
                nodes=ConcurrentStableFrozenList(nodes),
                replication_factor=int(factor),
                min_insync_replicas=int(min_insync),
            )
        )
        plan_id = new_plan_id()
        items: ConcurrentStableList[MigrationPlanItem] = ConcurrentStableList()
        blocks: ConcurrentStableList[MigrationBlock] = ConcurrentStableList()
        partitions = await self._topology.list_partitions(topic_ids=topic_ids)
        for partition in partitions:
            target = plan_assigner.assign(
                topic_id=partition.topic_id,
                partition_no=partition.partition_no,
                current_replicas=partition.replicas,
                current_leader=partition.leader,
            )
            action = _classify(partition=partition, target=target)
            if action == ACTION_NOOP:
                continue
            block = await self._preflight(
                partition=partition, target=target, action=action, min_insync=int(min_insync), topic_ids=topic_ids
            )
            if block is not None:
                blocks.add(block)
                _LOGGER.warning(
                    "msg_partition_rebalance_blocked",
                    plan_id=plan_id,
                    topic_id=partition.topic_id,
                    partition=partition.partition_no,
                    reason=block.reason,
                )
                continue
            items.add(
                MigrationPlanItem(
                    plan_id=plan_id,
                    tenant_id=partition.tenant_id,
                    namespace=partition.namespace,
                    topic=partition.topic,
                    topic_id=partition.topic_id,
                    partition_no=partition.partition_no,
                    action=action,
                    from_replicas=partition.replicas,
                    to_replicas=target.replicas,
                    from_leader=partition.leader,
                    to_leader=target.leader,
                )
            )
            _LOGGER.info(
                "msg_partition_rebalance_planned",
                plan_id=plan_id,
                topic_id=partition.topic_id,
                partition=partition.partition_no,
                action=action,
                **{"from_replicas": "/".join(partition.replicas), "to_replicas": "/".join(target.replicas)},
            )
        await self._ledger.create(items)
        return MigrationPlanResult(plan_id=plan_id, items=items, blocks=blocks)

    async def _preflight(
        self,
        *,
        partition: MigrationPartition,
        target: PartitionAssignment,
        action: str,
        min_insync: int,
        topic_ids: ConcurrentStableFrozenList[int] | None,
    ) -> MigrationBlock | None:
        """九项预检（命中即返回阻断项，不生成台账行）。

        Args:
            partition: 当前分区事实。
            target: 目标分配。
            action: 动作。
            min_insync: 最小同步副本数。
            topic_ids: 主题过滤（未使用，保留签名一致性）。

        Returns:
            MigrationBlock | None: 阻断项；通过返回 None。
        """
        del topic_ids
        where = dict(topic_id=partition.topic_id, partition_no=partition.partition_no)
        leader_snapshot = await self._control.snapshot(node_id=partition.leader, **where)
        if leader_snapshot is None:
            return MigrationBlock(reason="leader_unreachable", detail=f"leader {partition.leader} 不可达", **where)
        remaining = ConcurrentStableFrozenList(member for member in partition.replicas if member in target.replicas)
        if len(remaining) < max(min_insync, 1):
            return MigrationBlock(
                reason="insufficient_replicas",
                detail=f"缩容后剩余 {len(remaining)} 个副本，少于 min_insync={min_insync}",
                **where,
            )
        new_replicas = ConcurrentStableFrozenList(
            member for member in target.replicas if member not in partition.replicas
        )
        for node in new_replicas:
            snapshot = await self._control.snapshot(node_id=node, **where)
            if snapshot is None:
                return MigrationBlock(reason="target_unreachable", detail=f"目标副本 {node} 不可达", **where)
            if snapshot.leo < leader_snapshot.start_offset:
                return MigrationBlock(
                    reason="needs_full_resync",
                    detail=(
                        f"目标 {node} LEO={snapshot.leo} 早于 leader 起点 {leader_snapshot.start_offset}"
                        "（保留期已裁剪，全量重同步不在本任务范围）"
                    ),
                    **where,
                )
            if snapshot.leo > leader_snapshot.leo:
                return MigrationBlock(
                    reason="stale_data",
                    detail=f"目标 {node} LEO={snapshot.leo} 高于 leader LEO={leader_snapshot.leo}",
                    **where,
                )
        if partition.leader not in target.replicas:
            if target.leader not in remaining:
                return MigrationBlock(
                    reason="no_leader_candidate", detail=f"目标 leader {target.leader} 不在剩余副本集内", **where
                )
            candidate = await self._control.snapshot(node_id=target.leader, **where)
            if candidate is None or candidate.leo < leader_snapshot.hw:
                return MigrationBlock(
                    reason="no_leader_candidate",
                    detail=f"目标 leader {target.leader} 落后于切换水位 {leader_snapshot.hw}",
                    **where,
                )
        if partition.switch_state != SWITCH_STATE_IDLE and target.leader != partition.leader:
            return MigrationBlock(
                reason="switch_in_progress",
                detail=f"分区处于切换窗口（目标 {partition.leader}），不允许改为 {target.leader}",
                **where,
            )
        if await self._ledger.has_active(topic_id=partition.topic_id, partition_no=partition.partition_no):
            return MigrationBlock(reason="migration_in_progress", detail="同一分区已有未完成迁移", **where)
        if action == ACTION_SHRINK and min_insync > len(target.replicas):  # pragma: no cover - 参数校验已保证
            return MigrationBlock(reason="insufficient_replicas", detail="目标副本数少于 min_insync", **where)
        return None

    async def apply(
        self,
        *,
        plan_id: str,
        operator: str,
        reason: str,
        wait_ms: int | None = None,
        allow_force: bool = False,
        allow_degraded: bool = False,
        resume: bool = False,
    ) -> MigrationApplyResult:
        """执行计划（逐分区串行；单分区失败不回滚其他分区）。

        Args:
            plan_id: 计划标识。
            operator: 执行人。
            reason: 执行原因。
            wait_ms: 追平等待预算（None = 缺省）。
            allow_force: 是否允许 `force` 切主（结果记降级）。
            allow_degraded: 是否继续执行其余分区（降级项仍不计通过）。
            resume: 是否续跑中断项（同计划）。

        Returns:
            MigrationApplyResult: 执行后的全部台账行。
        """
        del allow_degraded, resume
        budget = self._wait_ms if wait_ms is None else int(wait_ms)
        items = await self._ledger.load(plan_id)
        for item in items:
            if item.status in PLAN_TERMINAL_STATUSES:
                continue
            await self._apply_item(item, operator=operator, reason=reason, budget_ms=budget, allow_force=allow_force)
        return MigrationApplyResult(plan_id=plan_id, items=await self._ledger.load(plan_id))

    async def _apply_item(
        self, item: MigrationItem, *, operator: str, reason: str, budget_ms: int, allow_force: bool
    ) -> None:
        """执行单个计划项（含中断续跑）。

        Args:
            item: 台账行。
            operator: 执行人。
            reason: 原因。
            budget_ms: 追平预算（毫秒）。
            allow_force: 是否允许 force 切主。
        """
        current = item
        if current.status == PLAN_STATUS_PLANNED:
            if not await self._ledger.mark_running(
                item_id=current.id, operator=operator, reason=reason, started_at=datetime.now(UTC)
            ):  # pragma: no cover - 并发竞争分支（台账已被他处推进）
                return
            await self._audit_begin(item=current, operator=operator, reason=reason)
            current = await self._reload(current)
            if current is None:
                return
        if current.status == PLAN_STATUS_RUNNING:
            await self._execute(current, budget_ms=budget_ms, allow_force=allow_force)
        elif current.status == PLAN_STATUS_CATCHING_UP:
            await self._finish_catch_up(current, budget_ms=budget_ms)

    async def _reload(self, item: MigrationItem) -> MigrationItem | None:
        """重新读台账行（CAS 后取最新状态）。

        Args:
            item: 旧行。

        Returns:
            MigrationItem | None: 新行。
        """
        return await self._ledger.load_one(plan_id=item.plan_id, topic_id=item.topic_id, partition_no=item.partition_no)

    async def _execute(self, item: MigrationItem, *, budget_ms: int, allow_force: bool) -> None:
        """按动作执行：先扩后缩 / 仅缩容 / 仅切主。

        Args:
            item: 台账行（`running`）。
            budget_ms: 追平预算（毫秒）。
            allow_force: 是否允许 force 切主。
        """
        partition = await self._topology.read_partition(topic_id=item.topic_id, partition_no=item.partition_no)
        if partition is None:
            await self._fail(item, "partition_not_found", "分区行不存在")
            return
        try:
            if item.action == ACTION_LEADER_MOVE:
                switch = await self._switch_to(item=item, allow_force=allow_force)
                await self._write(item, replicas=item.to_replicas, leader=item.to_leader, version=partition.version)
                await self._mark_catching_up(item, switch)
                await self._finish_catch_up(item, budget_ms=budget_ms)
                return
            if item.action == ACTION_SHRINK:
                switch: SwitchResult | None = None
                if partition.leader not in item.to_replicas:
                    switch = await self._switch_to(item=item, allow_force=allow_force)
                await self._write(item, replicas=item.to_replicas, leader=item.to_leader, version=partition.version)
                await self._succeed(item, switch)
                return
            # expand / replace：先扩（新副本追平）再缩（替换类）。
            switch = None
            if partition.leader not in item.to_replicas:
                switch = await self._switch_to(item=item, allow_force=allow_force)
            expanded = _union(partition.replicas, item.to_replicas)
            await self._ledger.update_step(item_id=item.id, step=_STEP_EXPAND)
            await self._write(item, replicas=expanded, leader=item.to_leader, version=partition.version)
            await self._mark_catching_up(item, switch)
            await self._finish_catch_up(item, budget_ms=budget_ms)
        except MigrationStepError as exc:
            await self._fail(item, exc.reason, exc.detail)
        except Exception as exc:  # pragma: no cover - 非预期异常也要落台账
            await self._fail(item, type(exc).__name__, str(exc))

    async def _switch_to(self, *, item: MigrationItem, allow_force: bool) -> SwitchResult:
        """调用 02-5 切换服务切到目标 leader。

        Args:
            item: 台账行。
            allow_force: 是否允许 force。

        Returns:
            SwitchResult: 切换结果。

        Raises:
            MigrationStepError: 切换失败。
        """
        try:
            result = await self._switch.switch_partition(
                SwitchPartitionRequest(
                    topic_id=item.topic_id,
                    partition_no=item.partition_no,
                    target=item.to_leader,
                    force=allow_force,
                    operator=item.operator or "ops.rebalance",
                    reason=item.reason or "rebalance",
                )
            )
        except LeaderSwitchError as exc:
            raise MigrationStepError(f"switch_{exc.reason}", exc.detail) from exc
        _LOGGER.info(
            "msg_partition_rebalance_step",
            step="leader_switch",
            result="ok",
            plan_id=item.plan_id,
            topic_id=item.topic_id,
            partition=item.partition_no,
        )
        return result

    async def _write(
        self,
        item: MigrationItem,
        *,
        replicas: ConcurrentStableFrozenList[str],
        leader: str,
        version: int,
    ) -> None:
        """CAS 写分配结果。

        Args:
            item: 台账行。
            replicas: 目标副本集。
            leader: 目标 leader。
            version: 期望版本。

        Raises:
            MigrationStepError: 写入失败（版本冲突 / 行已变）。
        """
        written = await self._topology.write_assignment(
            topic_id=item.topic_id,
            partition_no=item.partition_no,
            replicas=replicas,
            leader=leader,
            expected_version=version,
        )
        if not written:
            raise MigrationStepError("assignment_conflict", "副本集写入失败（version 冲突或分区行已变）")
        _LOGGER.info(
            "msg_partition_rebalance_step",
            step="update_replicas",
            result="ok",
            plan_id=item.plan_id,
            topic_id=item.topic_id,
            partition=item.partition_no,
            **{"replicas": "/".join(replicas), "leader": leader},
        )

    async def _mark_catching_up(self, item: MigrationItem, switch: SwitchResult | None) -> None:
        """`running` → `catching_up`（记录切主结果）。

        Args:
            item: 台账行。
            switch: 切主结果（None = 未切主）。
        """
        result_text, isr_verified, retained = _switch_outcome(switch)
        await self._ledger.mark_catching_up(
            item_id=item.id, result=result_text, isr_verified=isr_verified, retained_tail=retained
        )

    async def _finish_catch_up(self, item: MigrationItem, *, budget_ms: int) -> None:
        """等待目标副本追平并收口（replace 先缩后成功）。

        Args:
            item: 台账行。
            budget_ms: 追平预算（毫秒）。
        """
        current = await self._reload(item)
        if current is None:  # pragma: no cover - 台账被并发删除
            return
        caught_up, leo, hw, remaining = await self._probe_caught_up(current=current, budget_ms=budget_ms)
        if not caught_up:
            _LOGGER.info(
                "msg_partition_rebalance_pending",
                plan_id=current.plan_id,
                topic_id=current.topic_id,
                partition=current.partition_no,
                remaining=remaining,
            )
            return
        if current.action == ACTION_REPLACE and current.step != _STEP_SHRINK:
            partition = await self._topology.read_partition(
                topic_id=current.topic_id, partition_no=current.partition_no
            )
            if partition is None:  # pragma: no cover - 分区在追平期间被删除
                await self._fail(current, "partition_not_found", "分区行不存在")
                return
            await self._ledger.update_step(item_id=current.id, step=_STEP_SHRINK)
            try:
                await self._write(
                    current,
                    replicas=current.to_replicas,
                    leader=current.to_leader,
                    version=partition.version,
                )
            except MigrationStepError as exc:
                await self._fail(current, exc.reason, exc.detail)
                return
        await self._succeed(current, None)
        _LOGGER.info(
            "msg_partition_rebalance_caught_up",
            plan_id=current.plan_id,
            topic_id=current.topic_id,
            partition=current.partition_no,
            progress_leo=leo,
            progress_hw=hw,
            remaining_records=remaining,
        )

    async def _probe_caught_up(self, *, current: MigrationItem, budget_ms: int) -> tuple[bool, int, int, int]:
        """轮询 leader 与目标副本运行态，判是否追平。

        Args:
            current: 台账行。
            budget_ms: 预算（毫秒；0 = 只探测一次）。

        Returns:
            tuple[bool, int, int, int]: 是否追平、目标 LEO、leader HW、剩余条数。
        """
        deadline = self._clock() + max(int(budget_ms), 0)
        target_names = ConcurrentStableFrozenList(
            member for member in current.to_replicas if member != current.to_leader
        )
        last: tuple[bool, int, int, int] = (False, 0, 0, 0)
        while True:
            leader = await self._control.snapshot(
                node_id=current.to_leader, topic_id=current.topic_id, partition_no=current.partition_no
            )
            if leader is None:
                last = (False, 0, 0, 0)
            else:
                leos: ConcurrentStableList[int] = ConcurrentStableList()
                in_sync = True
                for node in target_names:
                    snapshot = await self._control.snapshot(
                        node_id=node, topic_id=current.topic_id, partition_no=current.partition_no
                    )
                    if snapshot is None:
                        in_sync = False
                        leos.add(0)
                        continue
                    leos.add(snapshot.leo)
                    if node not in leader.isr:
                        in_sync = False
                target_leo = min(leos) if leos else leader.leo
                remaining = max(leader.hw - target_leo, 0)
                caught = in_sync and target_leo >= leader.hw
                last = (caught, target_leo, leader.hw, remaining)
                await self._ledger.update_progress(
                    item_id=current.id, leo=int(target_leo), hw=int(leader.hw), remaining=int(remaining)
                )
                if caught:
                    return last
            if self._clock() >= deadline:
                return last
            await self._sleep(self._poll_interval_ms / MS_PER_SECOND)

    async def _succeed(self, item: MigrationItem, switch: SwitchResult | None) -> None:
        """收口为成功 / 降级（`isr_verified=false` 记降级）。

        Args:
            item: 台账行。
            switch: 切主结果（None = 未切主）。
        """
        current = await self._reload(item) or item
        result_text, isr_verified, retained = _switch_outcome(switch)
        if not isr_verified or not current.isr_verified or current.status == PLAN_STATUS_DEGRADED:
            await self._ledger.mark_terminal(
                item_id=current.id,
                status=PLAN_STATUS_DEGRADED,
                result=result_text,
                isr_verified=isr_verified,
                retained_tail=retained,
                error=MIGRATION_FORCE_DEGRADED_NOTE,
                finished_at=datetime.now(UTC),
            )
        else:
            await self._ledger.mark_terminal(
                item_id=current.id,
                status=PLAN_STATUS_SUCCEEDED,
                result=result_text,
                isr_verified=isr_verified,
                retained_tail=retained,
                error="",
                finished_at=datetime.now(UTC),
            )
        await self._audit_finished(current)

    async def _fail(self, item: MigrationItem, reason: str, detail: str) -> None:
        """收口为失败（写台账 error + 错误日志 + finished 审计）。

        Args:
            item: 台账行。
            reason: 原因。
            detail: 明细。
        """
        await self._ledger.mark_terminal(
            item_id=item.id,
            status=PLAN_STATUS_FAILED,
            result="",
            isr_verified=False,
            retained_tail=0,
            error=f"{reason}: {detail}",
            finished_at=datetime.now(UTC),
        )
        _LOGGER.error(
            "msg_partition_rebalance_failed",
            plan_id=item.plan_id,
            topic_id=item.topic_id,
            partition=item.partition_no,
            reason=reason,
            detail=detail,
        )
        await self._audit_finished(item)

    async def status(self, *, plan_id: str | None = None) -> ConcurrentStableList[MigrationStatusRow]:
        """读台账并刷新 `catching_up` 进度。

        Args:
            plan_id: 计划标识（None = 最近一个计划）。

        Returns:
            ConcurrentStableList[MigrationStatusRow]: 状态行。
        """
        target_plan = plan_id if plan_id is not None else await self._ledger.latest_plan_id()
        if target_plan is None:
            return ConcurrentStableList()
        items = await self._ledger.load(target_plan)
        rows: ConcurrentStableList[MigrationStatusRow] = ConcurrentStableList()
        for item in items:
            if item.status != PLAN_STATUS_CATCHING_UP:
                rows.add(MigrationStatusRow(item=item))
                continue
            caught, leo, hw, remaining = await self._probe_caught_up(current=item, budget_ms=0)
            if caught and item.action != ACTION_REPLACE:
                await self._succeed(item, None)
            item = await self._reload(item) or item
            rows.add(
                MigrationStatusRow(
                    item=item,
                    leader_leo=leo,
                    leader_hw=hw,
                    target_leo=leo,
                    remaining=remaining,
                    caught_up=caught,
                )
            )
        return rows

    async def _audit_begin(self, *, item: MigrationItem, operator: str, reason: str) -> None:
        """写 begin 审计（强审计，失败即拒绝执行）。

        Args:
            item: 台账行。
            operator: 执行人。
            reason: 原因。

        Raises:
            InternalError: 审计写入失败。
        """
        params = ConcurrentStableDict(
            {
                "phase": "begin",
                "plan_id": item.plan_id,
                "action": item.action,
                "topic": item.topic,
                "partition": item.partition_no,
                "from_replicas": list(item.from_replicas),
                "to_replicas": list(item.to_replicas),
                "from_leader": item.from_leader,
                "to_leader": item.to_leader,
                "operator": operator,
                "reason": reason,
            }
        )
        await emit_sensitive(
            self._audit,
            action=REBALANCE_ACTION,
            tenant_id=item.tenant_id,
            params=params,
            method="POST",
            path=_REBALANCE_AUDIT_PATH,
            module_key=item.namespace,
            resource_key=item.topic,
        )

    async def _audit_finished(self, item: MigrationItem) -> None:
        """写 finished 审计（fail-open：失败只记错误日志，不回滚已生效分配）。

        Args:
            item: 台账行。
        """
        latest = await self._reload(item) or item
        params = ConcurrentStableDict(
            {
                "phase": "finished",
                "plan_id": latest.plan_id,
                "action": latest.action,
                "topic": latest.topic,
                "partition": latest.partition_no,
                "from_replicas": list(latest.from_replicas),
                "to_replicas": list(latest.to_replicas),
                "from_leader": latest.from_leader,
                "to_leader": latest.to_leader,
                "operator": latest.operator,
                "reason": latest.reason,
                "result": latest.status,
            }
        )
        try:
            await emit_sensitive(
                self._audit,
                action=REBALANCE_ACTION,
                tenant_id=latest.tenant_id,
                params=params,
                method="POST",
                path=_REBALANCE_AUDIT_PATH,
                module_key=latest.namespace,
                resource_key=latest.topic,
            )
        except InternalError as exc:
            _LOGGER.error("msg_partition_rebalance_audit_failed", phase="finished", error=repr(exc))


def _classify(*, partition: MigrationPartition, target: PartitionAssignment) -> str:
    """判定动作类型。

    Args:
        partition: 当前分区事实。
        target: 目标分配。

    Returns:
        str: 动作（`expand` / `shrink` / `replace` / `leader_move` / `noop`）。
    """
    current = ConcurrentStableList(partition.replicas)
    wanted = ConcurrentStableList(target.replicas)
    added = [item for item in wanted if item not in current]
    removed = [item for item in current if item not in wanted]
    if not current:
        return ACTION_EXPAND
    if added and removed:
        return ACTION_REPLACE
    if added:
        return ACTION_EXPAND
    if removed:
        return ACTION_SHRINK
    if target.leader != partition.leader:
        return ACTION_LEADER_MOVE
    return ACTION_NOOP


def _union(
    current: ConcurrentStableFrozenList[str], target: ConcurrentStableFrozenList[str]
) -> ConcurrentStableFrozenList[str]:
    """副本集并集（保持当前顺序，追加新副本）。

    Args:
        current: 当前副本集。
        target: 目标副本集。

    Returns:
        ConcurrentStableFrozenList[str]: 并集。
    """
    merged: ConcurrentStableList[str] = ConcurrentStableList(current)
    for item in target:
        if item not in merged:
            merged.add(item)
    return ConcurrentStableFrozenList(merged)


def _switch_outcome(switch: SwitchResult | None) -> tuple[str, bool, int]:
    """把切主结果转成台账字段。

    Args:
        switch: 切主结果（None = 未切主）。

    Returns:
        tuple[str, bool, int]: 结果文本、是否验证 ISR、保留条数。
    """
    if switch is None:
        return RESULT_NOOP, True, 0
    if switch.truncated_to is not None:
        text = RESULT_TRUNCATED
    elif switch.retained_tail > 0:
        text = RESULT_RETAINED
    else:
        text = RESULT_NOOP
    return text, switch.isr_verified, switch.retained_tail


def _monotonic_ms() -> float:
    """单调毫秒时钟。

    Returns:
        float: 单调毫秒数。
    """
    return time.monotonic() * MS_PER_SECOND
