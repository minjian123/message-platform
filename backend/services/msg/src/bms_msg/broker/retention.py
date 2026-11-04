"""保留期清理（01-6 详细设计 §4）：双维度到期判定 + 可读区间起点前移。

三条结构性口径：

1. **按段整段删**：段内只要还有一条未过期记录，整段保留——真实段删除就是这个语义，内存实现必须与之一致，
   否则 8B_02 换成文件段后同一批判据会得出不同结论（详细设计 §4.1）。
2. **两个维度不互相让步**：时间维度可删到空（历史全过期就该全丢）；容量维度**最多删到只剩最后一段**
   （宁可短暂超配额，也不把刚写入的数据删空）。
3. **只前移起点、不动末端**：`truncate_before` 不改变 `LEO`，因此「位点早于起点 → `350003`」这条
   01-2 契约原样成立（本模块不碰位点）。

**触发归调用方**：请求驱动（`api/data/messages.py` 的 publish / consume 入口），本模块不做任何后台调度。
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.log import MessageStore, SegmentMeta

__all__ = ["RetentionOutcome", "RetentionService"]


def _utcnow() -> datetime:
    """当前 UTC 时间（到期判据；用例注入可推进时钟）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


@dataclass(frozen=True)
class RetentionOutcome(BaseValueObject):
    """一次清理的结果（排障与用例断言用；无删除时 `deleted_segments = 0`）。"""

    deleted_segments: int
    """本次删掉的段数。"""

    start_offset: int
    """清理后的可读区间起点。"""

    leo: int
    """分区日志末端位点（清理不改它）。"""


class RetentionService:
    """保留期清理（纯判定 + 一次端口写；无状态，可请求级构造）。"""

    def __init__(self, *, store: MessageStore, now: Callable[[], datetime] = _utcnow) -> None:
        """初始化。

        Args:
            store: 消息存储（段元数据与起点前移的唯一入口）。
            now: 时钟（缺省 UTC 当前时间；用例注入可推进时钟）。
        """
        self._store = store
        self._now = now

    async def apply(
        self, *, topic_id: int, partition_no: int, retention_ms: int, retention_bytes: int
    ) -> RetentionOutcome:
        """按时间与容量判定到期段并前移起点（幂等：无可删段即不动存储）。

        判定顺序（详细设计 §4.2）：

        1. 时间维度：从最老段起，`段内最大发布时间 + retention_ms <= now` 即整段删除，遇到未过期段停止；
        2. 容量维度：`retention_bytes > 0` 且剩余总量超限时继续从最老段删，**最多删到只剩最后一段**。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            retention_ms: 保留时长（毫秒，取自主题列）。
            retention_bytes: 容量上限（字节，`0` = 不限）。

        Returns:
            RetentionOutcome: 删除段数 + 清理后的起点与 LEO。
        """
        segments = await self._store.segments(topic_id=topic_id, partition_no=partition_no)
        leo = await self._store.leo(topic_id=topic_id, partition_no=partition_no)
        current = await self._store.start_offset(topic_id=topic_id, partition_no=partition_no)
        if not segments:
            return RetentionOutcome(deleted_segments=0, start_offset=current, leo=leo)

        cutoff = self._now() - timedelta(milliseconds=retention_ms)
        keep_from = 0
        while keep_from < len(segments) and segments[keep_from].max_published_at <= cutoff:
            keep_from += 1

        if retention_bytes > 0:
            keep_from = self._apply_capacity(segments=segments, keep_from=keep_from, retention_bytes=retention_bytes)

        if keep_from == 0:
            return RetentionOutcome(deleted_segments=0, start_offset=current, leo=leo)

        target = segments[keep_from].base_offset if keep_from < len(segments) else leo
        started = await self._store.truncate_before(topic_id=topic_id, partition_no=partition_no, offset=target)
        return RetentionOutcome(deleted_segments=keep_from, start_offset=started, leo=leo)

    def _apply_capacity(
        self, *, segments: ConcurrentStableList[SegmentMeta], keep_from: int, retention_bytes: int
    ) -> int:
        """容量维度补删（从时间维度留下的段里继续删最老的，保留最后一段）。

        Args:
            segments: 全部段元数据。
            keep_from: 时间维度判定后的首个保留段下标。
            retention_bytes: 容量上限（字节，`> 0`）。

        Returns:
            int: 容量维度判定后的首个保留段下标。
        """
        remaining = sum(segment.size_bytes for segment in segments[keep_from:])
        while remaining > retention_bytes and len(segments) - keep_from > 1:
            remaining -= segments[keep_from].size_bytes
            keep_from += 1
        return keep_from
