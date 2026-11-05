"""消费位点端口与内存实现（详细设计 §5.2），以及崩溃恢复的位点对齐协议（02-2 §7）。

- **粒度** `(group, topic_id, partition_no)`：与消费组语义一致（组间位点天然隔离，01-4 不改键）。
- **提交值 = 已处理完的最后一条 + 1**（即下一条待消费的 offset）；提交只前进不后退，重复提交幂等。
- **不吞异常**：提交失败必须冒泡，由投递服务保证「位点不前移」（先落存储、再改内存状态）。

**为什么对齐单开一个协议**：`OffsetStore` 只有 `get` / `commit`，没有枚举能力；「已提交位点超过新 LEO
就回退」需要按 `(topic, partition)` 枚举所有组的位点。把 `realign` 塞进 `OffsetStore` 会改动 02-3 的
端口签名（其前置契约明确「端口签名不变」），故单开 `OffsetRealigner`——02-3 的文件实现只需实现同一
协议，恢复流程一行不改。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.objects import BaseValueObject

__all__ = ["InMemoryOffsetStore", "OffsetAdjustment", "OffsetKey", "OffsetRealigner", "OffsetStore"]

OffsetKey = tuple[str, int, int]
"""位点键：`(group, topic_id, partition_no)`。"""


@dataclass(frozen=True)
class OffsetAdjustment(BaseValueObject):
    """一次位点回退（崩溃恢复的「数据丢失事件」凭证）。"""

    group: str
    """消费组。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    previous: int
    """回退前的已提交位点。"""

    current: int
    """回退后的位点（= 恢复后的新 LEO）。"""


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


class OffsetRealigner(Protocol):
    """崩溃恢复的位点对齐端口（独立于 `OffsetStore`，不改其签名）。"""

    async def realign_partition(
        self, *, topic_id: int, partition_no: int, ceiling: int
    ) -> ConcurrentStableList[OffsetAdjustment]:
        """把该分区超过 `ceiling` 的已提交位点夹到 `ceiling`。

        **只回退到新 LEO，不归零**：归零等于让整组从头重消费，而「宁可重复、不可丢」要求
        回退到「数据确实还在的最后一条之后」。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            ceiling: 恢复后的新 LEO。

        Returns:
            ConcurrentStableList[OffsetAdjustment]: 本次调整清单（无调整即空）。
        """
        ...


class InMemoryOffsetStore(OffsetStore):
    """内存位点存储（首期实现；02-3 换独立位点文件，端口与装配点不变）。"""

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

    async def realign_partition(
        self, *, topic_id: int, partition_no: int, ceiling: int
    ) -> ConcurrentStableList[OffsetAdjustment]:
        """把该分区超过 `ceiling` 的位点夹到 `ceiling`（内存实现；02-3 换成文件实现）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            ceiling: 恢复后的新 LEO。

        Returns:
            ConcurrentStableList[OffsetAdjustment]: 本次调整清单。
        """
        adjusted: ConcurrentStableList[OffsetAdjustment] = ConcurrentStableList()
        for (group, topic, partition), value in list(self._offsets.items()):
            if topic != topic_id or partition != partition_no or value <= ceiling:
                continue
            self._offsets[(group, topic, partition)] = ceiling
            adjusted.add(
                OffsetAdjustment(
                    group=group,
                    topic_id=topic,
                    partition_no=partition,
                    previous=value,
                    current=ceiling,
                )
            )
        return adjusted
