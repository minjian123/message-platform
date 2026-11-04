"""消费位点端口与内存实现（详细设计 §5.2）。

- **粒度** `(group, topic_id, partition_no)`：与消费组语义一致（组间位点天然隔离，01-4 不改键）。
- **提交值 = 已处理完的最后一条 + 1**（即下一条待消费的 offset）；提交只前进不后退，重复提交幂等。
- **不吞异常**：提交失败必须冒泡，由投递服务保证「位点不前移」（先落存储、再改内存状态）。
- 8B_02 以独立位点文件替换 `InMemoryOffsetStore`，端口签名不变。
"""

from abc import ABC, abstractmethod

__all__ = ["InMemoryOffsetStore", "OffsetKey", "OffsetStore"]

OffsetKey = tuple[str, int, int]
"""位点键：`(group, topic_id, partition_no)`。"""


class OffsetStore(ABC):
    """消费位点端口（读写分离；只前进不后退）。"""

    @abstractmethod
    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点（无记录即 0，表示从头消费）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """

    @abstractmethod
    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（只前进不后退；小于等于现值即幂等无操作）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点（下一条待消费的 offset）。
        """


class InMemoryOffsetStore(OffsetStore):
    """内存位点存储（首期实现；8B_02 换独立位点文件，端口与装配点不变）。"""

    def __init__(self) -> None:
        """初始化（无记录即空）。"""
        self._offsets: dict[OffsetKey, int] = {}

    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        """取已提交位点（无记录即 0）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 已提交位点。
        """
        return self._offsets.get((group, topic_id, partition_no), 0)

    async def commit(self, *, group: str, topic_id: int, partition_no: int, offset: int) -> None:
        """提交位点（只前进不后退；重复提交幂等）。

        Args:
            group: 消费组。
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 目标位点。
        """
        key = (group, topic_id, partition_no)
        if offset > self._offsets.get(key, 0):
            self._offsets[key] = offset
