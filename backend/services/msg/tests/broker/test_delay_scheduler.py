"""到期推进与转投（msg 任务 01-5；Kiwi 2417）。

覆盖（详细设计 §5、§6、§8.4，用例 #8~#18）：

- **未到期不投**：#8 时钟未到 → `LEO` 不变、记录仍在；
- **到期转投**：#9 `append` 到锁定分区、`log_id` / `message_id` 沿用、记录被清、`LEO + 1`；
- **不提前**：#10 `deliver_at - 1ms` 不投，`= deliver_at` 才投；
- **回拨**：#11 时钟后退 → 不投、不丢、指针不后退；
- **前跳限速**：#12 一次跳 5 分钟 → 单轮只推进 60s 窗口，其余留给下一轮；
- **单轮批上限**：#13 条数超上限 → 本轮只投上限条，下一轮续投；
- **重放未到期**：#14 L1 桶被分钟边界取出但未到期 → 重新装填，不投；
- **转投失败可见**：#15 `append` 抛异常替身 → 记录保留、`attempts + 1`、下轮重试成功；
- **幂等**：#16 连跑两次 `run_once` 不重复投；
- **生命周期**：#17 `ensure_started` 幂等、`stop` 后不再推进且可重入、单轮异常不终止任务；
- **重建**：#18 `wheel.clear()` + 从 `list_all()` 重装 → 到期仍能投（B 段入口）。

转投用端口级直接入队构造（`deliver_at` 精确可控，不依赖档位）；全部用可推进时钟驱动 `run_once`
（不 sleep），只有生命周期用例走真实 10ms 周期。
"""

import asyncio
import base64
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE

from bms_msg.broker.delay import DelayRecord, DelayStore
from bms_msg.broker.log import InMemoryMessageStore, MessageStore, SegmentMeta, StoredMessage
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel

pytestmark = pytest.mark.kiwi_id(2417)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""

TENANT_ID = 7001
"""测试租户主键（与夹具上下文同值）。"""

Advance = Callable[[int], None]
"""时钟推进器类型（夹具 `advance`）。"""

Now = Callable[[], datetime]
"""时钟类型（夹具 `clock`）。"""


def _record(*, published_at: datetime, span_ms: int, log_id: str, partition_no: int = 0) -> DelayRecord:
    """构造一条延迟记录（端口级直接入队：`deliver_at` 精确可控）。

    Args:
        published_at: 发布时刻。
        span_ms: 延迟跨度（毫秒）。
        log_id: 记录标识（也是去重判据）。
        partition_no: 分区号。

    Returns:
        DelayRecord: 延迟记录。
    """
    return DelayRecord(
        log_id=log_id,
        message_id=f"msg-{log_id}",
        tenant_id=TENANT_ID,
        topic_id=1,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=partition_no,
        delay_level="1m",
        key="order-1",
        payload=base64.b64decode("cGF5bG9hZA=="),
        headers=ConcurrentStableDict({"source": "test"}),
        trace_id="trace-1",
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
    )


async def _enqueue(*, store: DelayStore, wheel: DelayWheel, record: DelayRecord) -> None:
    """端口级入队并装填（模拟发布侧已完成的两步）。

    Args:
        store: 延迟区。
        wheel: 时间轮。
        record: 延迟记录。
    """
    await store.enqueue(record)
    wheel.place(record)


async def _wait_leo(store: MessageStore, *, expected: int, attempts: int = 200) -> None:
    """等待分区 LEO 达到预期（后台任务推进，不固定 sleep）。

    Args:
        store: 消息存储。
        expected: 预期 LEO。
        attempts: 最大轮询次数（每次 5ms）。
    """
    for _ in range(attempts):
        if await store.leo(topic_id=1, partition_no=0) >= expected:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"等待超时：LEO 未达到 {expected}")


class _FailingMessageStore(MessageStore):
    """消息存储替身：首次 `append` 抛异常、之后成功（验证失败可见与重试）。"""

    def __init__(self, inner: MessageStore) -> None:
        """初始化（缺省第一次失败）。

        Args:
            inner: 真实实现。
        """
        self._inner = inner
        self.fail_next = True

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
        """转调真实实现（`fail_next` 为真时抛错，模拟主题停用 / 存储故障）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 消息标识。
            log_id: 记录标识。
            key: 分区键。
            payload: 载荷。
            headers: 结构化头。
            trace_id: 链路标识。
            published_at: 发布时间。

        Returns:
            StoredMessage: 追加后的记录。

        Raises:
            RuntimeError: 模拟存储故障。
        """
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("消息存储不可用")
        return await self._inner.append(
            topic_id=topic_id,
            partition_no=partition_no,
            message_id=message_id,
            log_id=log_id,
            key=key,
            payload=payload,
            headers=headers,
            trace_id=trace_id,
            published_at=published_at,
            content_type=content_type,
            compression=compression,
            uncompressed_len=uncompressed_len,
        )

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 消息列表。
        """
        return await self._inner.fetch(
            topic_id=topic_id, partition_no=partition_no, from_offset=from_offset, max_messages=max_messages
        )

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        """转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: LEO。
        """
        return await self._inner.leo(topic_id=topic_id, partition_no=partition_no)

    async def start_offset(self, *, topic_id: int, partition_no: int) -> int:
        """转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            int: 起点位点。
        """
        return await self._inner.start_offset(topic_id=topic_id, partition_no=partition_no)

    async def segments(self, *, topic_id: int, partition_no: int) -> ConcurrentStableList[SegmentMeta]:
        """转调真实实现（保留清理不在本用例链路，仅满足端口）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ConcurrentStableList[SegmentMeta]: 段元数据。
        """
        return await self._inner.segments(topic_id=topic_id, partition_no=partition_no)

    async def truncate_before(self, *, topic_id: int, partition_no: int, offset: int) -> int:
        """转调真实实现（保留清理不在本用例链路，仅满足端口）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            offset: 新起点。

        Returns:
            int: 前移后的起点。
        """
        return await self._inner.truncate_before(topic_id=topic_id, partition_no=partition_no, offset=offset)


class _ExplodingWheel(DelayWheel):
    """时间轮替身：首次推进抛异常，之后转调真实轮（验证单轮异常不终止推进任务）。"""

    def __init__(self, *, inner: DelayWheel, now: Now) -> None:
        """初始化（装填一律转给真实轮，替身不自攒桶）。

        Args:
            inner: 真实时间轮。
            now: 时钟。
        """
        super().__init__(now=now)
        self._inner = inner
        self.exploded = False

    def place(self, record: DelayRecord) -> None:
        """转调真实轮装填。

        Args:
            record: 延迟记录。
        """
        self._inner.place(record)

    def take_due(self, now: datetime) -> ConcurrentStableList[DelayRecord]:
        """首次抛异常，之后转调真实轮。

        Args:
            now: 当前时刻。

        Returns:
            ConcurrentStableList[DelayRecord]: 候选记录。

        Raises:
            RuntimeError: 首次调用。
        """
        if not self.exploded:
            self.exploded = True
            raise RuntimeError("时间轮故障")
        return self._inner.take_due(now)


async def test_not_due_keeps_log_unchanged(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
) -> None:
    """未到期不投：时钟未到 → `LEO` 不变、记录仍在延迟区。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=5_000, log_id="l1")
    )

    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 0
    assert len(await delay_store.list_all()) == 1


async def test_due_transfers_to_locked_partition(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """到期转投：`append` 到锁定分区、`log_id` / `message_id` 沿用、记录被清、`LEO + 1`。"""
    record = _record(published_at=clock(), span_ms=1_000, log_id="l1", partition_no=1)
    await _enqueue(store=delay_store, wheel=delay_wheel, record=record)
    advance(1_000)

    assert await delay_scheduler.run_once() == 1
    stored = await message_store.fetch(topic_id=1, partition_no=1, from_offset=0, max_messages=10)
    assert [(item.log_id, item.message_id, item.offset) for item in stored] == [("l1", "msg-l1", 0)]
    assert stored[0].published_at == record.deliver_at
    assert stored[0].key == "order-1"
    assert dict(stored[0].headers.items()) == {"source": "test"}
    assert list(await delay_store.list_all()) == []
    assert await message_store.leo(topic_id=1, partition_no=1) == 1


async def test_no_early_delivery(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """不提前：`deliver_at - 1ms` 不投，`= deliver_at` 才投。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="l1")
    )

    advance(999)
    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 0

    advance(1)
    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_clock_rollback_does_not_deliver(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """回拨：指针不后退、不投、不丢；时钟追上后照常到期。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=5_000, log_id="l1")
    )
    advance(1_000)
    assert await delay_scheduler.run_once() == 0

    advance(-10_000)
    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 0
    assert len(await delay_store.list_all()) == 1

    advance(14_000)
    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_forward_jump_limits_catchup_per_round(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """前跳限速：一次跳 5 分钟 → 单轮只推进 60s 窗口，其余留给下一轮（不丢、不早投）。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="early")
    )
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=180_000, log_id="late")
    )
    advance(300_000)

    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 1
    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 1
    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 2


async def test_batch_limit_defers_remainder(
    delay_store: DelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: Now,
    advance: Advance,
) -> None:
    """单轮批上限：本轮只投上限条，其余进待重试队列，下一轮续投。"""
    scheduler = DelayScheduler(store=delay_store, messages=message_store, wheel=delay_wheel, now=clock, batch_limit=2)
    for index in range(5):
        await _enqueue(
            store=delay_store,
            wheel=delay_wheel,
            record=_record(published_at=clock(), span_ms=1_000, log_id=f"l{index}"),
        )
    advance(1_000)

    counts = [await scheduler.run_once() for _ in range(3)]
    assert counts == [2, 2, 1]
    assert await message_store.leo(topic_id=1, partition_no=0) == 5


async def test_reloaded_from_minute_bucket_when_not_yet_due(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """重放未到期：L1 桶被分钟边界取出但未到期 → 重新装填（落 L0），到期才投。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=90_000, log_id="l1")
    )

    advance(60_000)
    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 0
    assert len(await delay_store.list_all()) == 1

    advance(30_000)
    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_transfer_failure_keeps_record_and_retries(
    delay_store: DelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: Now,
    advance: Advance,
) -> None:
    """转投失败可见：记录保留、`attempts + 1`、下一轮重试成功（不丢弃）。"""
    failing = _FailingMessageStore(message_store)
    scheduler = DelayScheduler(store=delay_store, messages=failing, wheel=delay_wheel, now=clock)
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="l1")
    )
    advance(1_000)

    assert await scheduler.run_once() == 0
    kept = await delay_store.list_all()
    assert [(record.log_id, record.attempts) for record in kept] == [("l1", 1)]
    assert await message_store.leo(topic_id=1, partition_no=0) == 0

    assert await scheduler.run_once() == 1
    assert list(await delay_store.list_all()) == []
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_run_once_is_idempotent(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """幂等：到期记录只投一次，连跑 `run_once` 不重复入日志。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="l1")
    )
    advance(1_000)

    assert await delay_scheduler.run_once() == 1
    assert await delay_scheduler.run_once() == 0
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_hour_layer_delivers_after_catch_up(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """L2（小时桶）：装填进时位后，逐轮追赶（每轮 60s）最终仍按期转投。"""
    record = _record(published_at=clock(), span_ms=7_200_000, log_id="l2")
    await _enqueue(store=delay_store, wheel=delay_wheel, record=record)
    assert delay_wheel.layer_of(record) == "L2"
    advance(7_200_000)

    for _ in range(120):
        await delay_scheduler.run_once()

    assert await message_store.leo(topic_id=1, partition_no=0) == 1
    assert list(await delay_store.list_all()) == []


async def test_rebuild_from_list_all(
    delay_scheduler: DelayScheduler,
    delay_store: DelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    clock: Now,
    advance: Advance,
) -> None:
    """重建：`wheel.clear()` + 从 `list_all()` 重装 → 到期仍能投（B 段启动入口）。"""
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=5_000, log_id="l1")
    )
    delay_wheel.clear()
    assert await delay_scheduler.run_once() == 0

    for record in await delay_store.list_all():
        delay_wheel.place(record)
    advance(5_000)

    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 1


async def test_lifecycle_start_and_stop(
    delay_store: DelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: Now,
    advance: Advance,
) -> None:
    """生命周期：`ensure_started` 幂等（只一个任务）、后台按时转投、`stop` 后停推且可重入。"""
    scheduler = DelayScheduler(store=delay_store, messages=message_store, wheel=delay_wheel, now=clock, interval_ms=10)
    assert scheduler.running is False

    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="l1")
    )
    advance(1_000)
    scheduler.ensure_started()
    scheduler.ensure_started()  # 幂等：不新建任务（若建了第二个，下面的「stop 后不得再推进」会失败）
    assert scheduler.running is True

    await _wait_leo(message_store, expected=1)
    await scheduler.stop()
    assert scheduler.running is False

    await _enqueue(store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=0, log_id="l2"))
    await asyncio.sleep(0.05)
    assert await message_store.leo(topic_id=1, partition_no=0) == 1, "stop 后不得再推进"

    await scheduler.stop()


async def test_loop_survives_tick_failure(
    delay_store: DelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: Now,
    advance: Advance,
) -> None:
    """单轮异常不终止推进任务：时间轮首轮抛错后，下一轮照常转投。"""
    exploding = _ExplodingWheel(inner=delay_wheel, now=clock)
    scheduler = DelayScheduler(store=delay_store, messages=message_store, wheel=exploding, now=clock, interval_ms=10)
    await _enqueue(
        store=delay_store, wheel=delay_wheel, record=_record(published_at=clock(), span_ms=1_000, log_id="l1")
    )
    advance(1_000)
    scheduler.ensure_started()
    try:
        await _wait_leo(message_store, expected=1)
        assert exploding.exploded is True
    finally:
        await scheduler.stop()


def test_wheel_forget_tenant_removes_bucket_and_overdue(clock: Now) -> None:
    """按租户清时间轮：桶内记录与待重试队列都移除。"""
    wheel = DelayWheel(now=clock)
    first = replace(_record(published_at=clock(), span_ms=60_000, log_id="a"), tenant_id=1)
    second = replace(_record(published_at=clock(), span_ms=30_000, log_id="b"), tenant_id=2)
    overdue = replace(_record(published_at=clock(), span_ms=60_000, log_id="c"), tenant_id=1)
    overdue = replace(overdue, deliver_at=clock() - timedelta(seconds=1))
    wheel.place(first)
    wheel.place(second)
    wheel.place(overdue)

    assert wheel.forget_tenant(tenant_id=1) == 2
    due = wheel.take_due(clock() + timedelta(minutes=2))
    assert [record.tenant_id for record in due] == [2]


async def test_inactive_tenant_defers_delivery(
    delay_store: DelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: Now,
    advance: Advance,
) -> None:
    """停用租户：延迟记录保留、不转投；启用后同轮恢复。"""

    class _Availability:
        def __init__(self) -> None:
            self.active = False
            self.calls: list[int] = []

        async def is_active(self, tenant_id: int) -> bool:
            self.calls.append(tenant_id)
            return self.active

    availability = _Availability()
    scheduler = DelayScheduler(
        store=delay_store,
        messages=message_store,
        wheel=delay_wheel,
        now=clock,
        availability=availability,
    )
    await _enqueue(
        store=delay_store,
        wheel=delay_wheel,
        record=_record(published_at=clock(), span_ms=1_000, log_id="l1"),
    )
    advance(1_100)

    assert await scheduler.run_once() == 0
    assert await scheduler.run_once() == 0, "同窗口第二次只记一次日志"
    assert availability.calls == [TENANT_ID, TENANT_ID]
    assert await delay_store.count_for_tenant(tenant_id=TENANT_ID) == 1
    assert await message_store.leo(topic_id=1, partition_no=0) == 0

    availability.active = True
    assert await scheduler.run_once() == 1
    assert await delay_store.count_for_tenant(tenant_id=TENANT_ID) == 0
