"""切换元数据存储（任务 02-5）：`msg_partition` 两阶段 CAS。

**为什么单独一层**：`broker/election.py` 只认 `LeaderSwitchStore` 协议，不依赖 SQLAlchemy 会话；
本模块把「读哪张表、CAS 条件怎么写」留在服务装配侧，02-8 改分区分配时只改数据来源。

**CAS 是幂等与并发的唯一手段**：phase 1 用 `epoch = expected_epoch AND switch_state = 'idle'`
条件更新（`epoch + 1`），phase 2 用 `switch_state = 'switching' AND epoch = to_epoch` 收口；
两个运维同时切换时，后者必然看到前者结果并重新决策，不会互相覆盖 leader / epoch。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.db.session import DbSession
from sqlalchemy import select, update

from bms_msg.broker.election import LeaderSwitchStore, SwitchPartitionState
from bms_msg.broker.replay import UowFactory
from bms_msg.models.partition import SWITCH_STATE_IDLE, SWITCH_STATE_SWITCHING, MsgPartition
from bms_msg.models.topic import MsgTopic

__all__ = ["DbLeaderSwitchStore"]


class DbLeaderSwitchStore(LeaderSwitchStore):
    """平台库实现：`msg_partition` 条件更新 + 主题身份补齐（审计字段）。"""

    def __init__(self, *, uow_factory: UowFactory) -> None:
        """初始化。

        Args:
            uow_factory: 平台库工作单元工厂（无请求上下文也能开会话）。
        """
        self._uow_factory = uow_factory

    async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
        """读分区切换状态（含主题身份）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            SwitchPartitionState | None: 分区状态；行 / 主题缺失返回 None。
        """
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            row = await self._partition_row(session, topic_id=int(topic_id), partition_no=int(partition_no))
            if row is None:
                return None
            topic = await session.get(MsgTopic, int(topic_id))
            if topic is None:
                return None
            return self._state(row=row, topic=topic)

    async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
        """读主题全部分区（按分区号升序）。

        Args:
            topic_id: 主题主键。

        Returns:
            ConcurrentStableList[SwitchPartitionState]: 分区状态（主题缺失时为空）。
        """
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            topic = await session.get(MsgTopic, int(topic_id))
            if topic is None:
                return ConcurrentStableList()
            statement = (
                select(MsgPartition).where(MsgPartition.topic_id == int(topic_id)).order_by(MsgPartition.partition_no)
            )
            rows = ConcurrentStableList((await session.execute(statement)).scalars().all())
            return ConcurrentStableList(self._state(row=row, topic=topic) for row in rows)

    async def begin_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        expected_epoch: int,
        target: str,
        from_leader: str,
        from_epoch: int,
    ) -> bool:
        """phase 1 CAS：`idle` + 预期 epoch → `switching` + 新 leader + epoch+1。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            expected_epoch: 预期 epoch。
            target: 新 leader。
            from_leader: 切换前 leader。
            from_epoch: 切换前 epoch。

        Returns:
            bool: 更新成功 True；0 行（并发推进 / 状态不符）False。
        """
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            statement = (
                update(MsgPartition)
                .where(
                    MsgPartition.topic_id == int(topic_id),
                    MsgPartition.partition_no == int(partition_no),
                    MsgPartition.switch_state == SWITCH_STATE_IDLE,
                    MsgPartition.epoch == int(expected_epoch),
                )
                .values(
                    switch_state=SWITCH_STATE_SWITCHING,
                    switch_started_at=datetime.now(UTC),
                    switch_from_leader=from_leader,
                    switch_from_epoch=int(from_epoch),
                    leader=target,
                    epoch=int(from_epoch) + 1,
                )
            )
            result = await session.execute(statement)
            await uow.commit()
            return int(getattr(result, "rowcount", 0)) == 1

    async def finish_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        to_epoch: int,
        watermark: int | None,
        result: str,
        truncated_to: int | None,
        operator: str,
    ) -> bool:
        """phase 2 CAS：`switching` + 目标 epoch → `idle` + 最近切换结果。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            to_epoch: 切换后的 epoch。
            watermark: 切换水位（写入 `checkpoint_hw`；None 时回落到截断位点）。
            result: `truncated` / `retained` / `noop`。
            truncated_to: 截断后的 LEO（None = 未截断）。
            operator: 触发人。

        Returns:
            bool: 更新成功 True；0 行（已 idle / epoch 不符）False。
        """
        values = ConcurrentStableDict[str, object](
            {
                "switch_state": SWITCH_STATE_IDLE,
                "last_switch_at": datetime.now(UTC),
                "last_switch_by": operator,
                "last_switch_epoch": int(to_epoch),
                "last_switch_result": result,
                "last_truncate_to": truncated_to,
            }
        )
        checkpoint = watermark if watermark is not None else truncated_to
        if checkpoint is not None:
            values.set("checkpoint_hw", int(checkpoint))
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            statement = (
                update(MsgPartition)
                .where(
                    MsgPartition.topic_id == int(topic_id),
                    MsgPartition.partition_no == int(partition_no),
                    MsgPartition.switch_state == SWITCH_STATE_SWITCHING,
                    MsgPartition.epoch == int(to_epoch),
                )
                .values(**dict(values))
            )
            updated = await session.execute(statement)
            await uow.commit()
            return int(getattr(updated, "rowcount", 0)) == 1

    @staticmethod
    async def _partition_row(session: DbSession, *, topic_id: int, partition_no: int) -> MsgPartition | None:
        """按 `(topic_id, partition_no)` 取分区行。

        Args:
            session: 平台库会话。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            MsgPartition | None: 分区行。
        """
        statement = select(MsgPartition).where(
            MsgPartition.topic_id == int(topic_id),
            MsgPartition.partition_no == int(partition_no),
        )
        return (await session.execute(statement)).scalar_one_or_none()

    @staticmethod
    def _state(*, row: MsgPartition, topic: MsgTopic) -> SwitchPartitionState:
        """分区行 + 主题 → 切换状态。

        Args:
            row: 分区行。
            topic: 主题行。

        Returns:
            SwitchPartitionState: 切换状态。
        """
        return SwitchPartitionState(
            topic_id=int(row.topic_id),
            partition_no=int(row.partition_no),
            tenant_id=int(topic.tenant_id),
            namespace=str(topic.namespace),
            topic=str(topic.name),
            leader=str(row.leader),
            epoch=int(row.epoch or 0),
            replicas=ConcurrentStableFrozenList(str(item) for item in row.replicas),
            switch_state=str(row.switch_state or SWITCH_STATE_IDLE),
            switch_from_leader=row.switch_from_leader,
            switch_from_epoch=row.switch_from_epoch,
            last_switch_at=row.last_switch_at,
        )
