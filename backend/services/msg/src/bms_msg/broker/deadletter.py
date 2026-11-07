"""死信队列端口与内存实现（详细设计 §4.3、§6.3）。

- **转入时机**：① 投递次数达 `MAX_DELIVERY_ATTEMPTS` 后仍在超时重投（broker 判定，`delivery_exhausted`）；
  ② 消费端校验失败或业务拒绝后经 `nack` 上报（`schema_invalid` / `processing_failed`，03-3）。两条路径
  共用「转死信 + 位点前移」，只是触发方不同。
- **不删日志**：死信只在投递侧「不再投」，offset 仍可读——重放（01-6）与排障需要原始记录。
- **通道**：06 域的死信查询 / 处置直接消费本队列（`list_all` / `contains`），不需另建数据通道。
- 8B_02 落盘时随消息存储一并持久化，端口签名不变。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

__all__ = [
    "DEAD_LETTER_EXHAUSTED",
    "DeadLetter",
    "DeadLetterQueue",
    "DeadLetterReason",
    "InMemoryDeadLetterQueue",
]


class DeadLetterReason(StrEnum):
    """转入死信的原因（登记枚举：06 域按原因筛选与展示，不靠字符串匹配）。"""

    DELIVERY_EXHAUSTED = "delivery_exhausted"
    """重投次数超限（broker 判定；外部不得通过 `nack` 指定）。"""

    SCHEMA_INVALID = "schema_invalid"
    """消息体不符合 schema（消费端校验失败后经 `nack` 上报）。"""

    PROCESSING_FAILED = "processing_failed"
    """业务处理失败且确认不可重试（消费端经 `nack` 上报）。"""


DEAD_LETTER_EXHAUSTED = DeadLetterReason.DELIVERY_EXHAUSTED
"""转入原因：重投次数超限（既有调用点的常量别名）。"""

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
    """累计投递次数（达上限值；消费端上报时为首投次数）。"""

    failed_at: datetime
    """转入死信时间（UTC）。"""

    reason: DeadLetterReason
    """转入原因（枚举见 `DeadLetterReason`）。"""

    detail: ConcurrentStableDict[str, str] = field(default_factory=lambda: ConcurrentStableDict[str, str]())
    """失败详情（字段名 → 说明；消费端上报时原样保存，broker 判定时为空）。"""


class DeadLetterQueue(BaseFrameworkObject, ABC):
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
