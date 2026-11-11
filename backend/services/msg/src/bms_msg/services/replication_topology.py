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

from bms_msg.broker.replay import UowFactory
from bms_msg.broker.replication import PartitionTopology, ReplicaTopologyLoader
from bms_msg.core.config import MS_PER_SECOND
from bms_msg.models.partition import MsgPartition
from bms_msg.repositories.topic import PartitionRepository

__all__ = ["DbReplicaTopologyLoader"]


class DbReplicaTopologyLoader(ReplicaTopologyLoader):
    """从平台库加载分区复制拓扑（按主题缓存 + TTL）。"""

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

    async def load(self, *, topic_id: int, partition_no: int) -> PartitionTopology | None:
        """加载分区拓扑（行缺失返回 None，由协调器走配置兜底）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            PartitionTopology | None: 拓扑；行不存在返回 None。
        """
        rows = await self._topic_rows(topic_id=int(topic_id))
        for row in rows:
            if int(row.partition_no) != int(partition_no):
                continue
            return PartitionTopology(
                replicas=ConcurrentStableFrozenList(str(item) for item in row.replicas),
                min_insync_replicas=int(getattr(row, "min_insync_replicas", 0) or 0),
                epoch=int(row.epoch or 0),
            )
        return None

    def forget(self, *, topic_id: int) -> None:
        """清除某主题的缓存（02-8 改分配 / 主题清理时调用）。

        Args:
            topic_id: 主题主键。
        """
        self._rows.pop(int(topic_id), None)

    async def _topic_rows(self, *, topic_id: int) -> ConcurrentStableList[MsgPartition]:
        """取某主题的分区行（缓存命中且未过期即复用）。

        Args:
            topic_id: 主题主键。

        Returns:
            ConcurrentStableList[MsgPartition]: 分区行（按分区号升序）。
        """
        cached = self._rows.get(topic_id)
        now = self._clock()
        if cached is not None and now - cached[0] <= self._ttl_ms:
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
