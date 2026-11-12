"""分区复制拓扑加载（任务 02-4）：平台库 `msg_partition` → 副本集 / 最小同步副本 / 任期。

**为什么单独一层**：`broker/replication.py` 只认 `PartitionTopology` 协议，不依赖仓储与会话；
本模块把「读哪张表、怎么缓存」留在服务装配侧，02-8 改分区分配时只改这里的数据来源。

**缓存**：按 `topic_id` 缓存整主题分区行（一次查询取回上限 1024 行），TTL 由
`MSG_REPLICATION_TOPOLOGY_TTL_MS` 控制（缺省 5s）。热路径（发布 / 消费）不查库；
02-8 改分配 / 02-5 改 epoch 的滞后窗口即 TTL。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.db.session import DbSession

from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.replication import (
    PartitionAssignment,
    PartitionTopology,
    ReplicaDirectoryBinder,
    ReplicaMountSource,
    ReplicaTopologyLoader,
)
from bms_msg.core.config import MS_PER_SECOND
from bms_msg.models.partition import MsgPartition
from bms_msg.repositories.topic import PartitionRepository, TopicRepository

__all__ = ["DbReplicaTopologyLoader", "PartitionDirectoryBinder"]


class PartitionDirectoryBinder(ReplicaDirectoryBinder):
    """把文件存储的分区目录表接成复制挂载的目录登记（02-10）。

    **为什么要适配层**：`broker/replication.py` 只认「登记主题身份」这一动作，不依赖文件存储实现；
    目录表本身在 `broker/file_store.py`，由本层桥接，存储与复制两侧互不引用。
    """

    def __init__(self, directory: PartitionDirectory) -> None:
        """初始化。

        Args:
            directory: 分区目录表（`topic_id → 租户 / 命名空间 / 名称`）。
        """
        self._directory = directory

    def bind(self, *, tenant_id: int, namespace: str, name: str, topic_id: int) -> object:
        """登记主题目录身份（幂等；冲突由目录表记日志并保留首次值）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。
            topic_id: 主题主键。

        Returns:
            object: 目录表的冲突标记（True 表示保留首次绑定）。
        """
        return self._directory.bind(tenant_id=int(tenant_id), namespace=namespace, name=name, topic_id=int(topic_id))


class DbReplicaTopologyLoader(ReplicaTopologyLoader, ReplicaMountSource):
    """从平台库加载分区复制拓扑（按主题缓存 + TTL）与分配枚举（02-10）。"""

    def __init__(
        self,
        *,
        uow_factory: UowFactory,
        ttl_ms: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """初始化。

        Args:
            uow_factory: 平台库工作单元工厂（无请求上下文也能开会话）。
            ttl_ms: 缓存 TTL（毫秒）。
            clock: 单调毫秒时钟（用例注入以确定性驱动过期）。
        """
        self._uow_factory = uow_factory
        self._ttl_ms = ttl_ms
        self._clock = clock if clock is not None else _monotonic_ms
        self._rows: ConcurrentStableDict[int, tuple[float, ConcurrentStableList[MsgPartition]]] = ConcurrentStableDict()

    async def load(self, *, topic_id: int, partition_no: int, force: bool = False) -> PartitionTopology | None:
        """加载分区拓扑（行缺失返回 None，由协调器走配置兜底）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            force: 是否绕过缓存（02-5 刷新循环要求秒级收敛）。

        Returns:
            PartitionTopology | None: 拓扑；行不存在返回 None。
        """
        rows = await self._topic_rows(topic_id=int(topic_id), force=force)
        for row in rows:
            if int(row.partition_no) != int(partition_no):
                continue
            return PartitionTopology(
                replicas=ConcurrentStableFrozenList(str(item) for item in row.replicas),
                min_insync_replicas=int(getattr(row, "min_insync_replicas", 0) or 0),
                epoch=int(row.epoch or 0),
                leader=str(getattr(row, "leader", "") or ""),
                switch_state=str(getattr(row, "switch_state", "") or "idle"),
            )
        return None

    def forget(self, *, topic_id: int) -> None:
        """清除某主题的缓存（02-8 改分配 / 主题清理时调用）。

        Args:
            topic_id: 主题主键。
        """
        self._rows.pop(int(topic_id), None)

    async def list_assignments(self) -> ConcurrentStableList[PartitionAssignment]:
        """枚举全部分区分配（02-10；只读、不走缓存，保证新建主题与迁移能被及时看见）。

        同时带上主题身份（`tenant_id` / `namespace` / `name`）：后台挂载没有请求上下文，
        文件存储的主题目录需要显式登记，否则取 LEO 会抛 `PartitionNotBoundError`。
        已删除 / 清理中的主题不进清单（挂载它们只会拉起无主数据）。

        Returns:
            ConcurrentStableList[PartitionAssignment]: 分配事实（按 `(topic_id, partition_no)` 升序）。
        """
        async with self._uow_factory() as uow:
            session = cast("DbSession", uow.session)
            topics = await TopicRepository(session).list_active_unscoped()
            rows = await PartitionRepository(session).list_assignments()
        identities: ConcurrentStableDict[int, tuple[int, str, str]] = ConcurrentStableDict()
        for topic in topics:
            identities.set(
                int(topic.id),
                (int(topic.tenant_id), str(topic.namespace), str(topic.name)),
            )
        assignments: ConcurrentStableList[PartitionAssignment] = ConcurrentStableList()
        for row in rows:
            identity = identities.get(int(row.topic_id))
            if identity is None:
                continue
            tenant_id, namespace, name = identity
            assignments.add(
                PartitionAssignment(
                    topic_id=int(row.topic_id),
                    partition_no=int(row.partition_no),
                    replicas=ConcurrentStableFrozenList(str(item) for item in row.replicas),
                    tenant_id=tenant_id,
                    namespace=namespace,
                    name=name,
                )
            )
        return assignments

    async def _topic_rows(self, *, topic_id: int, force: bool = False) -> ConcurrentStableList[MsgPartition]:
        """取某主题的分区行（缓存命中且未过期即复用；`force` 绕过缓存）。

        Args:
            topic_id: 主题主键。
            force: 是否强制重载（02-5 刷新循环）。

        Returns:
            ConcurrentStableList[MsgPartition]: 分区行（按分区号升序）。
        """
        cached = self._rows.get(topic_id)
        now = self._clock()
        if not force and cached is not None and now - cached[0] <= self._ttl_ms:
            return cached[1]
        async with self._uow_factory() as uow:
            rows = await PartitionRepository(cast("DbSession", uow.session)).list_by_topic(topic_id)
        self._rows.set(topic_id, (now, rows))
        return rows


def _monotonic_ms() -> float:
    """单调毫秒时钟。

    Returns:
        float: 单调递增毫秒数。
    """
    return time.monotonic() * MS_PER_SECOND
