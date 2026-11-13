"""重放执行器（msg 任务 01-6；Kiwi 2418）。

覆盖（详细设计 §10.1 用例 P1~P10）：

- **P1** 新 `log_id` + 原 `message_id` + 载荷 / 头 / key / trace 原样复制；
- **P2** 追加到分区尾部（原记录 offset 与可读性不变）；
- **P3** 不自噬（扫描上界锁定建任务时的 `LEO`，完成任务后不再选中自己写入的记录）；
- **P4** 限速（令牌额度：首轮 1 秒量，之后按经过时间累计）；
- **P5** 暂停 / 恢复（不重复、不跳条）；
- **P6** 取消（不再写入、末态 `cancelled`、执行器不发终态审计——取消由请求侧记）；
- **P7** 完成（`replayed == total`、首末 `log_id` 非空、终态审计一条）；
- **P8** 单批异常 → `failed` + `error_msg`（不静默重试）；
- **P9** 生命周期（`ensure_started` 幂等、`stop` 幂等）；
- **P10** 单轮异常不终止推进（下一轮照常）；
- **P11** 记录级强审计写入失败即上抛、业务失败（07D 任务 03 附录 D 的行为变更）。

时钟用 `ManualClock` 精确驱动 `run_once`（不 sleep）；只有 P10 的生命周期用例用短周期真实等待。
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner, ReplayService
from bms_msg.broker.router import TopicRouter
from bms_msg.core.trace import EMPTY_TRACE, TraceContext
from bms_msg.models.replay import TASK_CANCELLED, TASK_COMPLETED, TASK_FAILED, TASK_PAUSED, TASK_RUNNING
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.services.replay_impact import ReplayImpactInspector
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2418)


class RecordingSink(Protocol):
    """审计出口替身的静态形态（`conftest.FakeAuditSink` 结构上满足）。"""

    events: ConcurrentStableList[EventEnvelope]
    """已记录的事件信封。"""


NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（与 `topic` 夹具一致）。"""

PAYLOAD = b"payload"
"""固定载荷（断言「载荷不变」用）。"""

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间。"""


class RecordingStore(InMemoryMessageStore):
    """消息存储替身：可让下一次 `append` 抛异常（验证 `failed` 终态）。"""

    def __init__(self) -> None:
        """初始化（缺省正常）。"""
        super().__init__()
        self.fail = False

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
        """写入前按开关抛异常。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            message_id: 消息标识。
            log_id: 记录标识。
            key: 分区键。
            payload: 载荷。
            headers: 结构化头。
            trace: 链路字段。
            published_at: 发布时间。

        Returns:
            StoredMessage: 写入后的记录。

        Raises:
            RuntimeError: `fail` 为真时恒抛。
        """
        if self.fail:
            raise RuntimeError("消息存储不可用")
        return await super().append(
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


class PagedStore(InMemoryMessageStore):
    """读取替身：单次最多返回 2 条，且游标落在 `hole_at` 时返回空批。

    模拟「分页读 + 推进期间并发清理把可读起点前移」——读空必须把游标收尾，否则会原地打转。
    """

    def __init__(self, *, hole_at: int) -> None:
        """初始化。

        Args:
            hole_at: 该 offset 起的读取返回空批。
        """
        super().__init__()
        self._hole = hole_at

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """在空洞处返回空批，其余按 2 条一页转调真实实现。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            from_offset: 起始位点。
            max_messages: 上限。

        Returns:
            ConcurrentStableList[StoredMessage]: 记录批次（空洞处为空）。
        """
        if from_offset == self._hole:
            return ConcurrentStableList()
        return await super().fetch(
            topic_id=topic_id,
            partition_no=partition_no,
            from_offset=from_offset,
            max_messages=min(max_messages, 2),
        )


async def _seed(store: InMemoryMessageStore, *, topic: MsgTopic, count: int, partition: int = 0) -> None:
    """向指定分区写入 `count` 条历史记录。

    Args:
        store: 消息存储。
        topic: 主题记录。
        count: 条数。
        partition: 分区号。
    """
    for index in range(count):
        await store.append(
            topic_id=int(topic.id),
            partition_no=partition,
            message_id=f"m-{partition}-{index}",
            log_id=f"l-{partition}-{index}",
            key="order-1",
            payload=PAYLOAD,
            headers=ConcurrentStableDict({"source": "test"}),
            trace=TraceContext(trace_id="trace-1"),
            published_at=_BASE,
        )


def _request(*, rate: int = 100, **overrides: object) -> ReplayRequest:
    """构造建任务请求（缺省单分区、最高速率）。

    Args:
        rate: 限速（条/秒）。
        **overrides: 覆盖字段。

    Returns:
        ReplayRequest: 建任务请求。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"namespace": NAMESPACE, "topic": TOPIC_NAME, "partitions": [0], "rate_per_second": rate}
    )
    for name, value in overrides.items():
        payload.set(name, value)
    return ReplayRequest.model_validate(dict(payload.items()))


def _service(*, store: InMemoryMessageStore, db_session: AsyncSession, now: Callable[[], datetime]) -> ReplayService:
    """组装重放服务（绑定指定存储；替身存储场景用，避免与夹具存储不一致）。

    Args:
        store: 消息存储（与执行器同一实例）。
        db_session: 内存库会话。
        now: 时钟。

    Returns:
        ReplayService: 重放服务。
    """
    session = cast("DbSession", db_session)
    return ReplayService(
        tasks=ReplayTaskRepository(session),
        router=TopicRouter(TopicRepository(session)),
        store=store,
        uow=DbUnitOfWork(session),
        impact=ReplayImpactInspector(
            groups=InMemoryGroupStore(), offsets=InMemoryOffsetStore(), messages=store, now=now
        ),
        now=now,
    )


def _runner(
    *,
    store: InMemoryMessageStore,
    db_session: AsyncSession,
    auditor: ReplayAuditor,
    now: Callable[[], datetime],
    uow_failing: bool = False,
) -> ReplayRunner:
    """组装执行器（工作单元复用内存会话；可选首轮抛异常）。

    Args:
        store: 消息存储。
        db_session: 内存库会话。
        auditor: 审计记录器。
        now: 时钟。
        uow_failing: 首次取工作单元即抛异常（验证单轮异常不终止）。

    Returns:
        ReplayRunner: 执行器。
    """
    session = cast("DbSession", db_session)
    calls = {"count": 0}

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """产出工作单元（可让首次调用失败）。

        Yields:
            UnitOfWork: 工作单元。

        Raises:
            RuntimeError: 首次调用且 `uow_failing` 为真。
        """
        calls["count"] += 1
        if uow_failing and calls["count"] == 1:
            raise RuntimeError("台账暂不可用")
        yield DbUnitOfWork(session)

    factory: Callable[[], AbstractAsyncContextManager[UnitOfWork]] = _uow
    return ReplayRunner(messages=store, uow_factory=factory, auditor=auditor, now=now, interval_ms=10)


async def test_replay_keeps_message_id_and_payload(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P1：新 `log_id`、原 `message_id`，载荷 / 头 / key / trace 原样复制。"""
    await _seed(message_store, topic=topic, count=1)
    task = await replay_service.create(tenant=current_tenant, request=_request())

    await _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock).run_once()

    records = await message_store.fetch(topic_id=int(topic.id), partition_no=0, from_offset=0, max_messages=10)
    origin, replayed = records[0], records[1]
    assert replayed.message_id == origin.message_id
    assert replayed.log_id != origin.log_id
    assert (replayed.payload, replayed.key, replayed.trace_id) == (origin.payload, origin.key, origin.trace_id)
    assert dict(replayed.headers.items()) == dict(origin.headers.items())
    assert task.total == 1


async def test_replay_appends_to_tail(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P2：重放记录追加到分区尾部，原记录 offset 不变。"""
    await _seed(message_store, topic=topic, count=3)
    await replay_service.create(tenant=current_tenant, request=_request())

    await _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock).run_once()

    records = await message_store.fetch(topic_id=int(topic.id), partition_no=0, from_offset=0, max_messages=20)
    assert [message.offset for message in records] == [0, 1, 2, 3, 4, 5]
    assert await message_store.start_offset(topic_id=int(topic.id), partition_no=0) == 0


async def test_completed_task_does_not_replay_its_own_output(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P3：扫描上界锁定建任务的 LEO → 已完成任务不再写入（不自噬）。"""
    await _seed(message_store, topic=topic, count=2)
    await replay_service.create(tenant=current_tenant, request=_request())
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    await runner.run_once()
    await runner.run_once()

    assert await message_store.leo(topic_id=int(topic.id), partition_no=0) == 4


async def test_rate_limit_caps_each_round(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    advance: Callable[[int], None],
) -> None:
    """P4：限速生效——首轮 1 秒额度，之后按经过时间累计。"""
    await _seed(message_store, topic=topic, count=5)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=2))
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    assert await runner.run_once() == 2
    assert await runner.run_once() == 0
    advance(1_000)
    assert await runner.run_once() == 2
    advance(1_000)
    assert await runner.run_once() == 1

    reloaded = await replay_service.get(tenant=current_tenant, task_id=int(task.id))
    assert reloaded.replayed == 5
    assert reloaded.status == TASK_COMPLETED


async def test_pause_and_resume_keep_progress(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    advance: Callable[[int], None],
) -> None:
    """P5：暂停后不再写入，恢复后继续且不重复、不跳条。"""
    await _seed(message_store, topic=topic, count=4)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=2))
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    await runner.run_once()
    paused = await replay_service.pause(tenant=current_tenant, task_id=int(task.id))
    assert paused.status == TASK_PAUSED
    advance(1_000)
    assert await runner.run_once() == 0
    resumed = await replay_service.resume(tenant=current_tenant, task_id=int(task.id))
    assert resumed.status == TASK_RUNNING
    advance(1_000)
    assert await runner.run_once() == 2

    replayed = await replay_service.get(tenant=current_tenant, task_id=int(task.id))
    records = await message_store.fetch(topic_id=int(topic.id), partition_no=0, from_offset=0, max_messages=20)
    assert replayed.replayed == 4
    assert [message.message_id for message in records] == ["m-0-0", "m-0-1", "m-0-2", "m-0-3"] * 2


async def test_cancel_stops_writes_without_audit(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    audit_sink: RecordingSink,
) -> None:
    """P6：取消后不再写入、末态 `cancelled`；执行器不为取消写终态审计（请求侧记）。"""
    await _seed(message_store, topic=topic, count=3)
    task = await replay_service.create(tenant=current_tenant, request=_request(rate=1))
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    await runner.run_once()
    cancelled = await replay_service.cancel(tenant=current_tenant, task_id=int(task.id))
    assert cancelled.status == TASK_CANCELLED
    assert cancelled.finished_at is not None
    assert await runner.run_once() == 0
    assert len(audit_sink.events) == 0


async def test_completion_audits_once_with_log_ids(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    audit_sink: RecordingSink,
) -> None:
    """P7：完成后 `replayed == total`、首末 `log_id` 非空、终态审计恰一条。"""
    await _seed(message_store, topic=topic, count=2)
    task = await replay_service.create(tenant=current_tenant, request=_request())
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    await runner.run_once()

    finished = await replay_service.get(tenant=current_tenant, task_id=int(task.id))
    assert (finished.status, finished.replayed, finished.total) == (TASK_COMPLETED, 2, 2)
    assert finished.log_id_first is not None
    assert finished.log_id_last is not None
    assert len(audit_sink.events) == 1
    params = cast("str", audit_sink.events[0].payload["params"])
    assert params.find(str(finished.log_id_last)) >= 0


async def test_append_failure_marks_task_failed(
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    audit_sink: RecordingSink,
) -> None:
    """P8：单批写入异常 → `failed` + `error_msg`，不再被拾取，并写一条终态审计。"""
    store = RecordingStore()
    await _seed(store, topic=topic, count=2)
    service = _service(store=store, db_session=db_session, now=clock)
    task = await service.create(tenant=current_tenant, request=_request())
    runner = _runner(store=store, db_session=db_session, auditor=replay_auditor, now=clock)
    store.fail = True

    await runner.run_once()
    failed = await service.get(tenant=current_tenant, task_id=int(task.id))
    assert (failed.status, failed.replayed) == (TASK_FAILED, 0)
    assert failed.error_msg is not None
    assert len(audit_sink.events) == 1

    store.fail = False
    assert await runner.run_once() == 0


async def test_runner_lifecycle_is_idempotent(
    message_store: InMemoryMessageStore,
    replay_auditor: ReplayAuditor,
    clock: Callable[[], datetime],
    db_session: AsyncSession,
) -> None:
    """P9：`ensure_started` 幂等、`stop` 幂等可重入。"""
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    runner.ensure_started()
    runner.ensure_started()
    assert runner.running is True
    await runner.stop()
    await runner.stop()
    assert runner.running is False


async def test_loop_survives_single_round_failure(
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P10：单轮异常只记日志，下一轮照常推进（任务不得静默退出）。"""
    store = InMemoryMessageStore()
    await _seed(store, topic=topic, count=1)
    await _service(store=store, db_session=db_session, now=clock).create(tenant=current_tenant, request=_request())
    runner = _runner(store=store, db_session=db_session, auditor=replay_auditor, now=clock, uow_failing=True)

    runner.ensure_started()
    try:
        for _ in range(50):
            if await store.leo(topic_id=int(topic.id), partition_no=0) == 2:
                break
            await asyncio.sleep(0.01)
        assert runner.running is True
        assert await store.leo(topic_id=int(topic.id), partition_no=0) == 2
    finally:
        await runner.stop()


async def test_audit_failure_breaks_replay(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    exploding_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P11：记录级强审计写入失败 → 异常上抛使业务失败（附录 D 已确认的行为变更）。"""
    await _seed(message_store, topic=topic, count=1)
    await replay_service.create(tenant=current_tenant, request=_request())
    runner = _runner(store=message_store, db_session=db_session, auditor=exploding_auditor, now=clock)

    with pytest.raises(RuntimeError, match="审计出口不可用"):
        await runner.run_once()


async def test_runner_skips_records_outside_key_filter(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P12：游标区间内不命中的记录被跳过（只重放命中的），进度按命中条数计。"""
    await _seed(message_store, topic=topic, count=1)
    for index in range(2):
        await message_store.append(
            topic_id=int(topic.id),
            partition_no=0,
            message_id=f"other-{index}",
            log_id=f"other-l-{index}",
            key="order-2",
            payload=b"payload",
            headers=ConcurrentStableDict({"source": "test"}),
            trace=EMPTY_TRACE,
            published_at=_BASE,
        )
    task = await replay_service.create(tenant=current_tenant, request=_request(key="order-1"))

    written = await _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock).run_once()

    finished = await replay_service.get(tenant=current_tenant, task_id=int(task.id))
    assert (written, task.total, finished.replayed) == (1, 1, 1)


async def test_runner_closes_cursor_when_store_reads_empty(
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
) -> None:
    """P13：推进期间读空（并发清理前移起点）→ 游标直接到底，不卡死。"""
    store = PagedStore(hole_at=2)
    await _seed(store, topic=topic, count=4)
    service = _service(store=store, db_session=db_session, now=clock)
    task = await service.create(tenant=current_tenant, request=_request())

    await _runner(store=store, db_session=db_session, auditor=replay_auditor, now=clock).run_once()

    left = await service.get(tenant=current_tenant, task_id=int(task.id))
    assert (left.replayed, left.status, left.total) == (2, TASK_COMPLETED, 2)


async def test_runner_ignores_vanished_task(
    message_store: InMemoryMessageStore,
    replay_auditor: ReplayAuditor,
    clock: Callable[[], datetime],
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P14：拣任务后任务被删 / 不存在 → 本轮跳过（不抛、不误写）。

    只替换「取待推进任务」这一步来模拟竞态窗口，任务表本身仍走真实仓储。
    """
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    async def _ghost_ids(**_kwargs: object) -> ConcurrentStableList[int]:
        """返回一个不存在的任务主键。

        Returns:
            ConcurrentStableList[int]: 幽灵主键。
        """
        return ConcurrentStableList([987654321])

    monkeypatch.setattr(runner, "_active_ids", _ghost_ids)

    assert await runner.run_once() == 0


async def test_runner_ignores_task_cancelled_before_advance(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    clock: Callable[[], datetime],
    replay_auditor: ReplayAuditor,
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P15：拣任务后任务已被取消 → 本轮跳过（批边界响应取消）。"""
    await _seed(message_store, topic=topic, count=2)
    task = await replay_service.create(tenant=current_tenant, request=_request())
    await replay_service.cancel(tenant=current_tenant, task_id=int(task.id))
    runner = _runner(store=message_store, db_session=db_session, auditor=replay_auditor, now=clock)

    async def _cancelled_ids(**_kwargs: object) -> ConcurrentStableList[int]:
        """把已取消的任务主键塞回待推进清单（模拟取消发生在拣任务之后）。

        Returns:
            ConcurrentStableList[int]: 任务主键。
        """
        return ConcurrentStableList([int(task.id)])

    monkeypatch.setattr(runner, "_active_ids", _cancelled_ids)

    assert await runner.run_once() == 0
    assert await message_store.leo(topic_id=int(topic.id), partition_no=0) == 2
