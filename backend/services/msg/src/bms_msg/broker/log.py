"""消息存储端口与内存实现（详细设计 §5.1；01-6 扩段元数据与起点前移）。

- **端口** `MessageStore`：`append` / `fetch` / `leo` / `start_offset` 是数据面对存储的基本要求；
  01-6 增 `segments` / `truncate_before` 支撑「按段保留 + 可读区间起点前移」。生产实现是
  `broker/file_store.py::FileMessageStore`（文件段 + 稀疏索引 + length 级尾部恢复，02-1），
  **端口签名不变**；`InMemoryMessageStore` 保留作轻量用例替身（段粒度按条数是替身口径，不是生产口径）。
- **offset 唯一分配方**：`append` 返回记录时已带上分区内序号；其他模块不得自行编号（概要设计 §5 约束）。
- **可读区间 = `[start_offset, leo)`**：`truncate_before` 只前移起点、不改末端；`fetch` 早于起点时按
  起点返回（**不抛错**——「位点早于起点」的业务判定只在 `consume` 一处给 `350003`，端口抛错会盖掉它）。
- **两类 ID 分工**（架构 04 §2.2）：`message_id` 是**消息身份**（重投 / 重放 / 转投都不变），
  `log_id` 是**记录身份**（发布时分配、延迟转投沿用、重放新分配）——重放会合法地产生同一
  `message_id` 的多条记录，故记录级去重（延迟恢复期）只能按 `log_id` 判。
- **不解析 payload**：store 只搬运字节，可解析的头信息由消息记录原样携带（零拷贝前提）。
- **进程内实现**：数据按「主题 + 分区」分桶，`list` 的追加与切片在单事件循环内原子（无 `await` 切换点）。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.objects import BaseValueObject

from bms_msg.core.config import RETENTION_SEGMENT_RECORDS
from bms_msg.protocol.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

__all__ = [
    "InMemoryMessageStore",
    "MessageStore",
    "PartitionKey",
    "SegmentMeta",
    "StoredMessage",
]

PartitionKey = tuple[int, int]
"""分区键：`(topic_id, partition_no)`。"""


@dataclass(frozen=True)
class StoredMessage(BaseValueObject):
    """存储中的一条消息（不可变：重投返回同一条记录的不同投递，记录本身不改）。"""

    message_id: str
    """全局唯一标识（基座雪花字符串；重投 / 重放不变）。"""

    log_id: str
    """记录标识（发布时分配、延迟转投沿用、重放新分配；记录级去重按它判）。"""

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

    content_type: str = DEFAULT_CONTENT_TYPE
    """消息体内容类型（03-3：随记录长期保留；缺省 MessagePack，恒有值）。"""

    compression: str = COMPRESSION_NONE
    """压缩算法（03-3：`none` 表示未压缩；压缩后字节即 `payload`）。"""

    uncompressed_len: int | None = None
    """解压后字节数（生产者可选声明；broker 不校验也不解压）。"""


@dataclass(frozen=True)
class SegmentMeta(BaseValueObject):
    """段元数据（保留判定的唯一输入；真实段文件由 8B_02 提供同一份元数据）。

    端口只暴露元数据、不暴露文件：内存实现按定长条数切段，文件实现由索引读段——`segments()` 签名不变。
    """

    base_offset: int
    """段首 offset（含）。"""

    end_offset: int
    """段尾 offset（不含）。"""

    max_published_at: datetime
    """段内最大发布时间（时间维度判据：整段都过期才删）。"""

    size_bytes: int
    """段内字节合计（容量维度判据；内存实现为近似值，真实值由文件大小给）。"""


class MessageStore(ABC):
    """消息存储端口（数据面对存储的全部要求；实现可替换，端口不变）。"""

    @abstractmethod
    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
    ) -> StoredMessage:
        """追加一条消息并分配 offset。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识（调用方生成；重投 / 重放 / 转投不变）。
            log_id: 记录标识（普通发布新生成、延迟转投沿用、重放新分配）。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace_id: 链路标识（可空）。
            published_at: 发布时间（UTC）。
            content_type: 消息体内容类型（缺省 MessagePack）。
            compression: 压缩算法（缺省 `none`）。
            uncompressed_len: 解压后字节数（生产者可选声明）。

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
        """取分区日志起点（保留期清理后大于 0；可读区间为 `[start_offset, leo)`）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。
        """

    @abstractmethod
    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """按 offset 升序取段元数据（保留判定的输入；空分区返回空列表）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据（首段 `base_offset` 即 `start_offset`）。
        """

    @abstractmethod
    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """把可读区间起点前移到 `offset`（保留期清理的唯一写入口）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新的起点（`<= 现起点` 幂等无操作，`> leo` 拒绝；非段边界向下取整到段边界）。

        Returns:
            int: 前移后的起点。

        Raises:
            ParamError: `offset > leo`（10001）。
        """


class InMemoryMessageStore(MessageStore):
    """内存消息存储（首期实现；8B_02 换文件段实现，端口与装配点不变）。

    **内部状态**：`_logs[key]` 只保留可读记录（头部被截掉的直接不在列表里），`_starts[key]` 记起点；
    由此 `leo = start + len(log)`、`append` 的 offset = `start + len(log)`——**offset 仍是绝对编号**，
    起点前移不会让 offset 重排（重排会让已提交位点指向另一条消息）。

    单事件循环内追加与切片无 `await` 切换点，故不需要额外加锁；多副本 / 多线程不在此层考虑
    （首期 `workers = 1`，多副本归 8B_02 / 8B_05）。
    """

    def __init__(self, *, segment_records: int = RETENTION_SEGMENT_RECORDS) -> None:
        """初始化（无分区即空日志）。

        Args:
            segment_records: 段替身条数（缺省取常量；用例注入小值验证段边界，8B_02 换文件实现后不再使用）。
        """
        self._segment_records = segment_records
        self._logs: dict[PartitionKey, list[StoredMessage]] = {}
        self._starts: dict[PartitionKey, int] = {}

    async def append(
        self,
        *,
        topic_id: int,
        partition_no: int,
        message_id: str,
        log_id: str,
        key: str | None,
        payload: bytes,
        headers: ConcurrentStableDict[str, str],
        trace_id: str | None,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
    ) -> StoredMessage:
        """追加一条消息并分配 offset（= 起点 + 当前可读条数）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识。
            log_id: 记录标识。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace_id: 链路标识（可空）。
            published_at: 发布时间（UTC）。
            content_type: 消息体内容类型（缺省 MessagePack）。
            compression: 压缩算法（缺省 `none`）。
            uncompressed_len: 解压后字节数（生产者可选声明）。

        Returns:
            StoredMessage: 含分配到的 `offset` 的完整记录。
        """
        bucket: PartitionKey = (topic_id, partition_no)
        log = self._logs.setdefault(bucket, [])
        message = StoredMessage(
            message_id=message_id,
            log_id=log_id,
            topic_id=topic_id,
            partition_no=partition_no,
            offset=self._starts.get(bucket, 0) + len(log),
            key=key,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
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
            from_offset: 起始位点（含）；早于起点（含负数）按起点处理。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的消息。
        """
        bucket: PartitionKey = (topic_id, partition_no)
        log = self._logs.get(bucket, [])
        index = max(from_offset - self._starts.get(bucket, 0), 0)
        return ConcurrentStableList(log[index : index + max_messages])

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志末端位点（下一条将被分配的 offset；起点前移不影响它）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO（空分区为起点，缺省 0）。
        """
        bucket: PartitionKey = (topic_id, partition_no)
        return self._starts.get(bucket, 0) + len(self._logs.get(bucket, []))

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取分区日志起点（保留期清理后大于 0；未清理为 0）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。
        """
        return self._starts.get((topic_id, partition_no), 0)

    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """按定长条数切段并返回元数据（替身：与真实「整段删」语义同构）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据（`base_offset` 升序；空分区为空列表）。
        """
        bucket: PartitionKey = (topic_id, partition_no)
        log = self._logs.get(bucket, [])
        start = self._starts.get(bucket, 0)
        metas: ConcurrentStableList[SegmentMeta] = ConcurrentStableList()
        for index in range(0, len(log), self._segment_records):
            chunk = log[index : index + self._segment_records]
            metas.add(
                SegmentMeta(
                    base_offset=start + index,
                    end_offset=start + index + len(chunk),
                    max_published_at=max(message.published_at for message in chunk),
                    size_bytes=sum(_record_bytes(message) for message in chunk),
                )
            )
        return metas

    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """把可读区间起点前移到 `offset`（幂等；越界拒绝）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新的起点。

        Returns:
            int: 前移后的起点。

        Raises:
            ParamError: `offset > leo`（10001）。
        """
        # 替身按「任意 offset 切片」：它没有真实段文件，切片粒度比真实实现更细；
        # 调用方（RetentionService）一律传段边界，故两者在**契约内**行为一致。
        bucket: PartitionKey = (topic_id, partition_no)
        start = self._starts.get(bucket, 0)
        log = self._logs.setdefault(bucket, [])
        leo = start + len(log)
        if offset > leo:
            raise ParamError(f"清理起点越界：{offset} > LEO {leo}")
        if offset <= start:
            return start
        del log[: offset - start]
        self._starts[bucket] = offset
        return offset


def _record_bytes(message: StoredMessage) -> int:
    """估算一条记录的字节占用（容量维度判据的**近似值**，不含 Python 对象开销）。

    Args:
        message: 存储中的消息。

    Returns:
        int: 载荷 + 键 + 头的字节数合计。
    """
    headers = sum(len(name) + len(value) for name, value in message.headers.items())
    return len(message.payload) + len(message.key or "") + headers + len(message.trace_id or "")
