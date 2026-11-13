"""长轮询等待器与「写入即唤醒」存储包装（任务 03-5）。

**为什么需要等待器**：拉取端点与长驻流都靠「没有消息就再拉一次」推进。固定短间隔轮询（03-1 的 200ms）
在空载时既耗 CPU 又带来最坏 200ms 的发现延迟；事件唤醒把「消息到达 → 返回」压到一次调度，
把「没有消息」的代价降到一次协程挂起。

**三条口径**：

1. **键是 `(topic_id, partition_no)`**：与消息存储的桶同键——发布、延迟转投、重放三处写入都天然有它，
   不必为了唤醒再去解析命名空间 / 主题名。
2. **版本号 + Event 双轨**：只靠 Event 会丢唤醒（「拉空 → 建 Event → 等」之间到达的消息没人 set）；
   版本号让等待者在进入等待前就能判定「已经变了」。
3. **跨进程不通知**：消息存储本身是进程内实现，另一个副本收不到这条消息，通知它没有意义；多副本归 8B_05。

**上限不报错**：等待者超过 `max_waiters` 时返回 `rejected`，调用方立即返空批——挂起是延迟优化，
不是契约承诺，把「挂起太多」变成错误会让客户端按错误退避，反而放大压力。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from enum import StrEnum

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.roots import BaseFrameworkObject
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker.log import MessageHeaderView, MessageStore, SegmentMeta, StoredMessage
from bms_msg.core.config import MAX_LONG_POLL_WAITERS, MS_PER_SECOND
from bms_msg.core.trace import TraceContext

__all__ = ["DeliveryWaiter", "NotifyingMessageStore", "WaitKey", "WaitOutcome"]

WaitKey = tuple[int, int]
"""等待键：`(topic_id, partition_no)`，与消息存储的分区桶同键。"""


class WaitOutcome(StrEnum):
    """一次等待的结果（三态，调用方据此决定「重拉 / 返空 / 计数」）。"""

    CHANGED = "changed"
    """等待期间该分区发生了变化（或有消息在等待前就已到达）：应立即重拉。"""

    TIMEOUT = "timeout"
    """等到超时仍无变化：返空批（`max_wait_ms` 到时，不是错误）。"""

    REJECTED = "rejected"
    """等待者已达上限：不排队、立即返空（不报错）。"""


class DeliveryWaiter(BaseFrameworkObject):
    """进程级分区唤醒器（单副本；多副本共享归 8B_05）。

    只做四件事：记版本、发通知、挂起等待、报等待数。**不做**投递与预算判定——那些归
    `DeliveryService` 与 `BackpressureTuning`，本类只回答「这条分区现在有没有新东西」。
    """

    def __init__(self, *, max_waiters: int = MAX_LONG_POLL_WAITERS) -> None:
        """初始化（无等待者、无版本记录）。

        Args:
            max_waiters: 同时挂起的等待者上限；超限返回 `rejected`。
        """
        self._max_waiters = max_waiters
        self._versions: dict[WaitKey, int] = {}
        self._events: dict[WaitKey, asyncio.Event] = {}
        self._waiting = 0

    @property
    def max_waiters(self) -> int:
        """等待者上限（用例与指标口径）。

        Returns:
            int: 上限条数。
        """
        return self._max_waiters

    def version(self, key: WaitKey) -> int:
        """取当前版本号（未出现过的键为 0）。

        Args:
            key: 等待键。

        Returns:
            int: 版本号（单调递增）。
        """
        return self._versions.get(key, 0)

    def notify(self, key: WaitKey) -> None:
        """标记该分区有变化并唤醒等待者（无等待者时只加版本）。

        Args:
            key: 等待键。
        """
        self._versions[key] = self._versions.get(key, 0) + 1
        event = self._events.get(key)
        if event is not None:
            event.set()

    def forget_topic(self, *, topic_id: int) -> int:
        """忘掉某主题的全部分区版本 / 事件（租户删除时的易失资源清理）。

        正在挂起的等待者不会被强制唤醒；它们最多等到自己的超时，随后因基座租户守卫拒绝而结束。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 清掉的等待键数。
        """
        target = int(topic_id)
        keys = {key for key in self._versions if key[0] == target}
        keys.update(key for key in self._events if key[0] == target)
        for key in keys:
            self._versions.pop(key, None)
            self._events.pop(key, None)
        return len(keys)

    def waiting(self) -> int:
        """当前挂起的等待者数（指标 `msg_long_poll_waiters` 与用例断言）。

        Returns:
            int: 等待者数。
        """
        return self._waiting

    async def wait(self, key: WaitKey, *, version: int, timeout_ms: int) -> WaitOutcome:
        """等待该分区发生变化（最长 `timeout_ms`）。

        Args:
            key: 等待键。
            version: 调用方在**拉取之前**记下的版本号（拉取期间的变化据此判定，不必睡满超时）。
            timeout_ms: 最长等待毫秒数。

        Returns:
            WaitOutcome: `changed` / `timeout` / `rejected`。
        """
        if self.version(key) != version:
            return WaitOutcome.CHANGED
        if self._waiting >= self._max_waiters:
            return WaitOutcome.REJECTED
        event = self._events.get(key)
        if event is None:
            event = asyncio.Event()
            self._events[key] = event
        self._waiting += 1
        try:
            await asyncio.wait_for(event.wait(), timeout_ms / MS_PER_SECOND)
            return WaitOutcome.CHANGED
        except TimeoutError:
            return WaitOutcome.TIMEOUT
        finally:
            self._waiting -= 1
            if event.is_set() and self._events.get(key) is event:
                # 已被唤醒：丢弃该 Event，下一次等待重建——否则后到的等待者会立刻「假醒」。
                self._events.pop(key, None)


class NotifyingMessageStore(MessageStore):
    """消息存储装饰器：`append` 成功后唤醒该分区的等待者，其余方法原样委托。

    **为什么是装饰器而不是在三个写入点各加一行**：写入方有三处（普通发布、延迟到期转投、重放），
    逐处加必然漏一处；包装器把「写入即唤醒」收敛成单点，且 8B_02 换文件段实现时它不需要改。
    """

    def __init__(self, inner: MessageStore, waiter: DeliveryWaiter) -> None:
        """初始化。

        Args:
            inner: 被装饰的存储实现（内存实现或 8B_02 的文件段实现）。
            waiter: 分区唤醒器。
        """
        self._inner = inner
        self._waiter = waiter

    @property
    def inner(self) -> MessageStore:
        """被包装的存储实现（装配冒烟与排障用：确认「包装不改变实现」）。

        Returns:
            MessageStore: 内层存储。
        """
        return self._inner

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
        trace: TraceContext,
        published_at: datetime,
        content_type: str = DEFAULT_CONTENT_TYPE,
        compression: str = COMPRESSION_NONE,
        uncompressed_len: int | None = None,
        epoch: int = 0,
    ) -> StoredMessage:
        """追加一条消息并唤醒该分区（参数与返回值原样透传）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 全局唯一标识。
            log_id: 记录标识。
            key: 分区键（可空）。
            payload: 业务字节。
            headers: 结构化头。
            trace: 链路字段（06-4；四个字段一物一传）。
            published_at: 发布时间（UTC）。
            content_type: 消息体内容类型。
            compression: 压缩算法。
            uncompressed_len: 解压后字节数（生产者可选声明）。
            epoch: leader 任期（02-4；原样透传）。

        Returns:
            StoredMessage: 含分配到的 `offset` 的完整记录。
        """
        message = await self._inner.append(
            topic_id=topic_id,
            partition_no=partition_no,
            message_id=message_id,
            log_id=log_id,
            key=key,
            payload=payload,
            headers=headers,
            trace=trace,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
            epoch=epoch,
        )
        self._waiter.notify((topic_id, partition_no))
        return message

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """取一段消息（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 按 offset 升序的消息。
        """
        return await self._inner.fetch(
            topic_id=topic_id, partition_no=partition_no, from_offset=from_offset, max_messages=max_messages
        )

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """取日志末端位点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO。
        """
        return await self._inner.leo(topic_id=topic_id, partition_no=partition_no)

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """取可读区间起点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。
        """
        return await self._inner.start_offset(topic_id=topic_id, partition_no=partition_no)

    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """取段元数据（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据。
        """
        return await self._inner.segments(topic_id=topic_id, partition_no=partition_no)

    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """前移可读区间起点（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新的起点。

        Returns:
            int: 前移后的起点。
        """
        return await self._inner.truncate_before(topic_id=topic_id, partition_no=partition_no, offset=offset)

    async def truncate_to(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """截掉末端记录（原样委托；切换截断用）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 保留到的位置（不含）。

        Returns:
            int: 截断后的 LEO。
        """
        return await self._inner.truncate_to(topic_id=topic_id, partition_no=partition_no, offset=offset)

    async def delete(self, *, topic_id: int, partition_no: int, log_id: str) -> bool:
        """追加墓碑标记（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            log_id: 目标记录标识。

        Returns:
            bool: 首次标记 True。
        """
        return await self._inner.delete(topic_id=topic_id, partition_no=partition_no, log_id=log_id)

    async def find_by_log_id(self, *, log_id: str) -> MessageHeaderView | None:
        """按记录身份取只读头视图（原样委托）。

        Args:
            log_id: 记录标识。

        Returns:
            MessageHeaderView | None: 头视图；未命中返回 None。
        """
        return await self._inner.find_by_log_id(log_id=log_id)

    async def scan_headers(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[MessageHeaderView]:
        """取一段只读头视图（原样委托）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点（含）。
            max_messages: 本次上限。

        Returns:
            ConcurrentStableList[MessageHeaderView]: 按 offset 升序的头视图。
        """
        return await self._inner.scan_headers(
            topic_id=topic_id,
            partition_no=partition_no,
            from_offset=from_offset,
            max_messages=max_messages,
        )

    async def drop_topic(self, *, topic_id: int) -> int:
        """删除该主题的全部日志数据（原样委托；删主题清理用）。

        Args:
            topic_id: 主题主键。

        Returns:
            int: 受影响的分区数。
        """
        return await self._inner.drop_topic(topic_id=topic_id)
