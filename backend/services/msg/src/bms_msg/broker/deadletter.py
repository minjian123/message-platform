"""死信队列端口与内存实现（详细设计 §4.3、§6.3）。

- **转入时机**：投递次数达 `MAX_DELIVERY_ATTEMPTS` 后仍在超时重投，说明消息本身（或消费逻辑）有毒，
  转死信并把位点前移，避免一条坏消息长期阻塞分区。
- **不删日志**：死信只在投递侧「不再投」，offset 仍可读——重放（01-6）与排障需要原始记录。
- **通道**：06 域的死信查询 / 处置直接消费本队列（`list_all` / `contains`），不需另建数据通道。
- 8B_02 落盘时随消息存储一并持久化，端口签名不变。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList, ConcurrentStableSet
from bms_core.core.objects import BaseValueObject

__all__ = [
    "DEAD_LETTER_EXHAUSTED",
    "DeadLetter",
    "DeadLetterQueue",
    "InMemoryDeadLetterQueue",
]

DEAD_LETTER_EXHAUSTED = "delivery_exhausted"
"""转入原因：重投次数超限。"""

DeadLetterKey = tuple[int, int, int]
"""死信定位键：`(topic_id, partition_no, offset)`。"""


@dataclass(frozen=True)
class DeadLetter(BaseValueObject):
    """一条死信记录（定位三元组 + 失败上下文）。"""

    message_id: str
    """原消息 ID（重放时不变）。"""

    topic_id: int
    """主题主键。"""

    partition_no: int
    """分区号。"""

    offset: int
    """分区内序号。"""

    attempts: int
    """累计投递次数（达上限值）。"""

    failed_at: datetime
    """转入死信时间（UTC）。"""

    reason: str
    """转入原因（如 `delivery_exhausted`）。"""


class DeadLetterQueue(ABC):
    """死信队列端口：转入 / 全量列出 / 判定是否已死信。"""

    @abstractmethod
    async def add(self, letter: DeadLetter) -> None:
        """转入一条死信（同定位三元组重复转入幂等）。

        Args:
            letter: 死信记录。
        """

    @abstractmethod
    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """列出全部死信（插入序）。

        Returns:
            ConcurrentStableList[DeadLetter]: 死信列表。
        """

    @abstractmethod
    async def contains(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """判定某条消息是否已死信。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已死信返回 True。
        """


class InMemoryDeadLetterQueue(DeadLetterQueue):
    """内存死信队列（首期实现；06 域查询端点直接消费）。"""

    def __init__(self) -> None:
        """初始化（空队列）。"""
        self._letters: ConcurrentStableList[DeadLetter] = ConcurrentStableList()
        self._keys: ConcurrentStableSet[DeadLetterKey] = ConcurrentStableSet()

    async def add(self, letter: DeadLetter) -> None:
        """转入一条死信（同三元组重复转入幂等）。

        Args:
            letter: 死信记录。
        """
        key = (letter.topic_id, letter.partition_no, letter.offset)
        if key in self._keys:
            return
        self._keys.add(key)
        self._letters.add(letter)

    async def list_all(self) -> ConcurrentStableList[DeadLetter]:
        """列出全部死信（插入序）。

        Returns:
            ConcurrentStableList[DeadLetter]: 死信列表。
        """
        return ConcurrentStableList(self._letters)

    async def contains(self, *, topic_id: int, partition_no: int, offset: int) -> bool:
        """判定某条消息是否已死信。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 分区内序号。

        Returns:
            bool: 已死信返回 True。
        """
        return (topic_id, partition_no, offset) in self._keys
