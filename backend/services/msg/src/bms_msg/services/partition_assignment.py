"""分区副本分配器（02-8）：确定性轮转 + 持久化口径的唯一实现。

**为什么单独一层**：建主题、增分区、`ops.rebalance` 三处必须共用同一算法——各写一套就会出现
「新建的分区落在节点 A、rebalance 认为应在节点 B」的漂移。协调器的配置兜底（02-4）保留同一算法，
保证升级不改变既有分配。

**口径**（02-8 详细设计 §6.1）：

- 节点列表按插入序；副本数 `min(replication_factor, len(nodes))`；
- 起点 `(topic_id * 31 + partition_no) % len(nodes)`（与 02-4 现有推导逐字一致）；
- leader：已有 leader 仍在副本集内则保留，否则取副本集首个；
- 复制关闭：`replicas=[]`、`leader=node_id`，与 8B_02 单实例行为一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

__all__ = ["PartitionAssigner", "PartitionAssignment", "PartitionAssignmentConfig"]


@dataclass(frozen=True)
class PartitionAssignmentConfig(BaseValueObject):
    """分配配置（来自复制配置；关闭时只保留开关与本节点）。"""

    enabled: bool = False
    """复制总开关。"""

    node_id: str = "local"
    """本节点标识（关闭时的 leader）。"""

    nodes: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())
    """候选节点（插入序）。"""

    replication_factor: int = 1
    """副本数（配置兜底值）。"""

    min_insync_replicas: int = 1
    """全局最小同步副本数（写分区行，供协调器生效）。"""


@dataclass(frozen=True)
class PartitionAssignment(BaseValueObject):
    """一个分区的分配结果。"""

    replicas: ConcurrentStableFrozenList[str]
    """副本集（插入序）。"""

    leader: str
    """leader 标识。"""

    min_insync_replicas: int
    """分区级最小同步副本数（0 = 继承全局；本分配器写生效值）。"""


class PartitionAssigner(BaseFrameworkObject):
    """确定性分配器（建主题 / 增分区 / rebalance 共用）。"""

    def __init__(self, config: PartitionAssignmentConfig) -> None:
        """初始化。

        Args:
            config: 分配配置（已校验）。
        """
        self._config = config

    @property
    def config(self) -> PartitionAssignmentConfig:
        """分配配置。

        Returns:
            PartitionAssignmentConfig: 配置。
        """
        return self._config

    def assign(
        self,
        *,
        topic_id: int,
        partition_no: int,
        current_replicas: ConcurrentStableFrozenList[str] | None = None,
        current_leader: str = "",
    ) -> PartitionAssignment:
        """计算一个分区的目标副本集与 leader。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            current_replicas: 已有副本集（None = 新分区；空表示未分配）。
            current_leader: 已有 leader（空 = 未指定）。

        Returns:
            PartitionAssignment: 目标分配。
        """
        if not self._config.enabled or len(self._config.nodes) <= 1:
            return PartitionAssignment(
                replicas=ConcurrentStableFrozenList(),
                leader=self._config.node_id,
                min_insync_replicas=1,
            )
        nodes = self._config.nodes
        count = min(self._config.replication_factor, len(nodes))
        start = (int(topic_id) * 31 + int(partition_no)) % len(nodes)
        chosen: ConcurrentStableList[str] = ConcurrentStableList()
        for index in range(count):
            chosen.add(nodes[(start + index) % len(nodes)])
        replicas = ConcurrentStableFrozenList(chosen)
        leader = current_leader if current_leader and current_leader in replicas else replicas[0]
        return PartitionAssignment(
            replicas=replicas,
            leader=leader,
            min_insync_replicas=max(1, min(self._config.min_insync_replicas, len(replicas))),
        )

    def assign_range(
        self, *, topic_id: int, start_no: int, count: int
    ) -> ConcurrentStableDict[int, PartitionAssignment]:
        """为一个连续分区区间计算分配（建主题 / 增分区用）。

        Args:
            topic_id: 主题主键。
            start_no: 起始分区号。
            count: 分区数。

        Returns:
            ConcurrentStableDict[int, PartitionAssignment]: 分区号 → 分配（插入序）。
        """
        result: ConcurrentStableDict[int, PartitionAssignment] = ConcurrentStableDict()
        for partition_no in range(start_no, start_no + count):
            result.set(partition_no, self.assign(topic_id=topic_id, partition_no=partition_no))
        return result
