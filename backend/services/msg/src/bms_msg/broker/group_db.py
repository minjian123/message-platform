"""消费组 DB 存储（`msg_consumer_group` / `msg_group_member`；01-4-1）。

**每次操作自开短事务**（`uow_factory`）：心跳 / 加入是高频短操作，不自持长会话、不复用请求会话，
避免长驻订阅流与后台扫描互相拖住连接池。

**失败语义**：DB 异常直接上抛（500），不静默返回「无组」——静默会让客户端误以为组被删而重建，
掩盖真实故障。

**实例标识**：`instance_id` 只写进成员行做排障，不参与存活判定；存活判据是 `last_heartbeat + 会话超时`。
"""

from __future__ import annotations

from datetime import datetime
from typing import cast

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession

from bms_msg.broker.group import ConsumerGroup, GroupStore
from bms_msg.broker.replay import UowFactory
from bms_msg.repositories.consumer_group import ConsumerGroupRepository

__all__ = ["DatabaseGroupStore"]


class DatabaseGroupStore(GroupStore):
    """消费组存储（平台库两表；端口签名与内存实现一致）。"""

    def __init__(self, *, uow_factory: UowFactory, instance_id: str = "") -> None:
        """初始化。

        Args:
            uow_factory: 平台库工作单元工厂（每次操作自开短事务）。
            instance_id: 当前实例标识（写入成员行，排障用；空串表示不记）。
        """
        self._uow_factory = uow_factory
        self._instance_id = instance_id

    async def load(self, *, tenant_id: int, topic_id: int, group: str) -> ConsumerGroup | None:
        """取组记录（含成员）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            ConsumerGroup | None: 组记录；未 `join` 过返回 None。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.get_with_members(tenant_id=tenant_id, topic_id=topic_id, name=group)

    async def save(self, record: ConsumerGroup) -> None:
        """整条覆盖保存（组行 upsert + 成员整替）。

        Args:
            record: 组记录快照。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            await repository.save(record, instance_id=self._instance_id or None)

    async def list_for_tenant(
        self, *, tenant_id: int, topic_id: int | None = None
    ) -> ConcurrentStableList[ConsumerGroup]:
        """列出某租户的组记录（组名升序）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题过滤；None 表示不限。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 组记录。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.list_for_tenant(tenant_id=tenant_id, topic_id=topic_id)

    async def list_all(self) -> ConcurrentStableList[ConsumerGroup]:
        """列出全部组记录（指标采样用）。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 全部组记录。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.list_all()

    async def delete(self, *, tenant_id: int, topic_id: int, group: str) -> bool:
        """物理删除组与成员（位点保留）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            bool: 是否命中并删除。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.delete_identity(tenant_id=tenant_id, topic_id=topic_id, name=group)

    async def mark_idle_dead(self, *, before: datetime) -> int:
        """把空闲超过阈值的空组置 `dead`。

        Args:
            before: 空闲判定截止时刻（UTC）。

        Returns:
            int: 本次置为 `dead` 的组数。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.mark_idle_dead(before=before)

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的全部组记录。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的组记录数。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.purge_tenant(tenant_id=tenant_id)

    async def purge_topic(self, *, topic_id: int) -> int:
        """删除某主题的全部组记录。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 删除的组记录数。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.purge_topic(topic_id=topic_id)

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """统计某租户的组数（配额预检用；端口外的可选能力）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 组数。
        """
        async with self._uow_factory() as uow, uow.begin():
            repository = ConsumerGroupRepository(cast("DbSession", uow.session))
            return await repository.count_for_tenant(tenant_id=tenant_id)
