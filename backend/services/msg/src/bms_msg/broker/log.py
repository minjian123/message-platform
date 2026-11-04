"""消息存储端口与内存实现（详细设计 §5.1）。

- **端口** `MessageStore`：`append` / `fetch` / `leo` / `start_offset` 四个方法即数据面对存储的全部要求；
  8B_02 以文件段 + 稀疏索引 + CRC 恢复替换 `InMemoryMessageStore`，**端口签名不变**（只换实现）。
- **offset 唯一分配方**：`append` 返回记录时已带上分区内序号；其他模块不得自行编号（概要设计 §5 约束）。
- **不解析 payload**：store 只搬运字节，可解析的头信息由消息记录原样携带（零拷贝前提）。
- **进程内实现**：数据按「主题 + 分区」分桶，`list` 的追加与切片在单事件循环内原子（无 `await` 切换点）。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.objects import BaseValueObject

__all__ = [
    "InMemoryMessageStore",
    "MessageStore",
    "PartitionKey",
    "StoredMessage",
]

PartitionKey = tuple[int, int]
"""分区键：`(topic_id, partition_no)`。"""


@dataclass(frozen=True)
class StoredMessage(BaseValueObject):
    """存储中的一条消息（不可变：重投返回同一条记录的不同投递，记录本身不改）。"""

    message_id: str
    """全局唯一标识（基座雪花字符串；重投 / 重放不变）。"""

    topic_id: int
    """主题主键（雪花）。"""

    partition_no: int
    """分区号（0 起）。"""

    offset: int
    """分区内序号（**唯一由 `append` 分配**）。"""

    key: str | None
    """分区键（幂等与顺序性的业务入口；可空）。"""

    payload: bytes
    """业务字节（store 不解析；HTTP 契约以 base64 传输）。"""

    headers: ConcurrentStableDict[str, str]
    """结构化头（trace / 业务标记）。"""

    trace_id: str | None
    """链路标识（排障）。"""

    published_at: datetime
    """发布时间（UTC）。"""


class MessageStore(ABC):
    """消息存储端口（数据面对存储的全部要求；实现可替换，端口不变）。"""

    @abstractmethod
    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
    ) -> StoredMessage:
        """追加一条消息并分配 offset。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识（调用方生成）。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace_id: 链路标识（可空）。
            published_at: 发布时间（UTC）。

        Returns:
            StoredMessage: 含分配到的 `offset` 的完整记录。
        """

    @abstractmethod
    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """从 `from_offset` 起取至多 `max_messages` 条。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）；小于日志起点时按起点返回。
            max_messages: 本次上限（须为正）。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的消息（不足即返回实际条数）。
        """

    @abstractmethod
    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志末端位点（下一条将被分配的 offset）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO（空分区为 0）。
        """

    @abstractmethod
    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志起点（保留期清理后大于 0；内存实现恒 0）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点（可读区间为 `[start_offset, leo)`）。
        """


class InMemoryMessageStore(MessageStore):
    """内存消息存储（首期实现；8B_02 换文件段实现，端口与装配点不变）。

    单事件循环内追加与切片无 `await` 切换点，故不需要额外加锁；多副本 / 多线程不在此层考虑
    （首期 `workers = 1`，多副本归 8B_02 / 8B_05）。
    """

    def __init__(self) -> None:
        """初始化（无分区即空日志）。"""
        self._logs: dict[PartitionKey, list[StoredMessage]] = {}

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
    ) -> StoredMessage:
        """追加一条消息并分配 offset（= 当前日志长度）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace_id: 链路标识（可空）。
            published_at: 发布时间（UTC）。

        Returns:
            StoredMessage: 含分配到的 `offset` 的完整记录。
        """
        log = self._logs.setdefault((topic_id, partition_no), [])
        message = StoredMessage(
            message_id=message_id,
            topic_id=topic_id,
            partition_no=partition_no,
            offset=len(log),
            key=key,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            published_at=published_at,
        )
        log.append(message)
        return message

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """从 `from_offset` 起取至多 `max_messages` 条（不足即返回实际条数）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）；负数按 0 处理。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的消息。
        """
        log = self._logs.get((topic_id, partition_no), [])
        start = max(from_offset, 0)
        return ConcurrentStableList(log[start : start + max_messages])

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志末端位点。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO（空分区为 0）。
        """
        return len(self._logs.get((topic_id, partition_no), []))

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志起点（内存实现不清理，恒 0）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点（恒 0）。
        """
        del topic_id, partition_no
        return 0
