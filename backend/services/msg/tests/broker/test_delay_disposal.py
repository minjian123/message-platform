"""延迟转投异常处置服务原语（05-4；Kiwi 2455）。"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import InternalError, ParamError
from bms_core.events.base import EventEnvelope

from bms_msg.audit.trail import DELAY_DISCARD_ACTION, AuditEmitter
from bms_msg.broker.delay import DelayRecord, InMemoryDelayStore
from bms_msg.broker.delay_disposal import DelayDisposalService
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.core.trace import TraceContext
from bms_msg.errors import MsgMessageNotFoundError

pytestmark = pytest.mark.kiwi_id(2455)

TENANT_ID = 9_001
"""用例租户主键。"""

OTHER_TENANT = 9_002
"""另一个租户（隔离用例）。"""

TOPIC_ID = 11
"""用例主题主键。"""

NAMESPACE = "msg"
"""用例命名空间。"""

TOPIC_NAME = "audit.operation"
"""用例主题名。"""


class _Sink:
    """审计替身：记录事件与强审计开关。"""

    def __init__(self) -> None:
        """初始化。"""
        self.events: list[EventEnvelope] = []
        self.required: list[bool] = []

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一次写入。

        Args:
            event: 审计事件。
            required: 强审计开关。

        Returns:
            bool: 恒成功。
        """
        self.events.append(event)
        self.required.append(required)
        return True


class _ExplodingSink:
    """审计替身：发射即抛。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """恒抛异常。

        Args:
            event: 审计事件（忽略）。
            required: 强审计开关（忽略）。

        Returns:
            bool: 不返回。

        Raises:
            RuntimeError: 恒抛。
        """
        del event, required
        raise RuntimeError("audit down")


class _FailingMessages:
    """消息存储替身：`append` 恒失败（覆盖转投失败日志抑制分支）。"""

    async def append(self, **kwargs: object) -> object:
        """恒抛异常。

        Args:
            **kwargs: 追加参数（忽略）。

        Returns:
            object: 不返回。

        Raises:
            RuntimeError: 恒抛。
        """
        del kwargs
        raise RuntimeError("append down")


class _RestoreFailStore(InMemoryDelayStore):
    """恢复失败替身：第二次及以后的 `enqueue` 直接抛错（覆盖 critical 日志分支）。"""

    def __init__(self) -> None:
        """初始化（`fail` 为 False）。"""
        super().__init__()
        self.fail = False

    async def enqueue(self, record: DelayRecord) -> None:
        """写入 / 覆盖（`fail=True` 时抛错）。

        Args:
            record: 延迟记录。

        Raises:
            RuntimeError: 模拟恢复失败。
        """
        if self.fail:
            raise RuntimeError("restore down")
        await super().enqueue(record)


def _record(
    clock: Callable[[], datetime],
    *,
    log_id: str = "l-1",
    span_ms: int = 60_000,
    tenant_id: int = TENANT_ID,
) -> DelayRecord:
    """构造一条延迟记录。

    Args:
        clock: 可推进时钟。
        log_id: 记录标识。
        span_ms: 延迟跨度（毫秒）。
        tenant_id: 租户主键。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = clock()
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=tenant_id,
        topic_id=TOPIC_ID,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=0,
        delay_level="1m",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "audit"}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
        attempts=3,
    )


def _service(store: InMemoryDelayStore, wheel: DelayWheel, sink: AuditEmitter) -> DelayDisposalService:
    """组装处置服务。

    Args:
        store: 延迟区。
        wheel: 时间轮。
        sink: 审计替身。

    Returns:
        DelayDisposalService: 处置服务。
    """
    return DelayDisposalService(store=store, wheel=wheel, sink=sink)


async def test_discard_removes_store_wheel_and_writes_record_ref(
    clock: Callable[[], datetime],
    delay_store: InMemoryDelayStore,
    delay_wheel: DelayWheel,
) -> None:
    """成功丢弃：延迟区与时间轮都移除；强审计带 `record_ref = msg.message/log_id`。"""
    record = _record(clock)
    await delay_store.enqueue(record)
    delay_wheel.place(record)
    sink = _Sink()

    assert (
        await _service(delay_store, delay_wheel, sink).discard(
            tenant_id=TENANT_ID, log_id="l-1", reason="poison message", actor_id=7, ip="10.0.0.1", trace_id="t-1"
        )
        is True
    )

    assert list(await delay_store.list_for_tenant(tenant_id=TENANT_ID)) == []
    assert delay_wheel.forget(log_id="l-1") == 0
    event = sink.events[0]
    assert (event.payload["action"], event.payload["module"]) == (DELAY_DISCARD_ACTION, "msg")
    ref = cast("dict[str, object]", event.payload["record_ref"])
    assert (ref["type"], ref["id"]) == ("msg.message", "l-1")
    params = cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))
    assert (params["reason"], params["attempts"], params["topic"]) == ("poison message", 3, TOPIC_NAME)
    assert sink.required == [True]


async def test_discard_audit_failure_restores_and_does_not_deliver(
    clock: Callable[[], datetime],
    delay_store: InMemoryDelayStore,
    delay_wheel: DelayWheel,
    message_store: InMemoryMessageStore,
    delay_scheduler: DelayScheduler,
) -> None:
    """审计失败：记录恢复；推进器仍能转投（不是静默丢弃）。"""
    record = _record(clock, span_ms=1_000)
    await delay_store.enqueue(record)
    delay_wheel.place(record)

    with pytest.raises(InternalError):
        await _service(delay_store, delay_wheel, _ExplodingSink()).discard(
            tenant_id=TENANT_ID, log_id="l-1", reason="poison"
        )

    assert len(await delay_store.list_for_tenant(tenant_id=TENANT_ID)) == 1
    clock.advance(2_000)  # pyright: ignore[reportFunctionMemberAccess]
    assert await delay_scheduler.run_once() == 1
    assert await message_store.leo(topic_id=TOPIC_ID, partition_no=0) == 1


async def test_discard_unknown_log_id(
    clock: Callable[[], datetime], delay_store: InMemoryDelayStore, delay_wheel: DelayWheel
) -> None:
    """记录不存在：`350003`。"""
    with pytest.raises(MsgMessageNotFoundError) as exc:
        await _service(delay_store, delay_wheel, _Sink()).discard(
            tenant_id=TENANT_ID, log_id="missing", reason="poison"
        )
    assert exc.value.code == 350003


async def test_discard_other_tenant_not_visible(
    clock: Callable[[], datetime], delay_store: InMemoryDelayStore, delay_wheel: DelayWheel
) -> None:
    """跨租户不可见：按当前租户查不到，记录保留。"""
    record = _record(clock)
    await delay_store.enqueue(record)
    delay_wheel.place(record)

    with pytest.raises(MsgMessageNotFoundError):
        await _service(delay_store, delay_wheel, _Sink()).discard(tenant_id=OTHER_TENANT, log_id="l-1", reason="poison")
    assert len(await delay_store.list_for_tenant(tenant_id=TENANT_ID)) == 1


async def test_discard_requires_reason(
    clock: Callable[[], datetime], delay_store: InMemoryDelayStore, delay_wheel: DelayWheel
) -> None:
    """原因为空：`10001`，记录保留。"""
    record = _record(clock)
    await delay_store.enqueue(record)
    delay_wheel.place(record)

    with pytest.raises(ParamError):
        await _service(delay_store, delay_wheel, _Sink()).discard(tenant_id=TENANT_ID, log_id="l-1", reason="   ")
    assert len(await delay_store.list_for_tenant(tenant_id=TENANT_ID)) == 1


async def test_discard_restore_failure_is_logged(clock: Callable[[], datetime], delay_wheel: DelayWheel) -> None:
    """审计失败且恢复也失败：仍上抛审计异常（critical 日志分支被执行）。"""
    store = _RestoreFailStore()
    record = _record(clock)
    await store.enqueue(record)
    delay_wheel.place(record)
    store.fail = True

    with pytest.raises(InternalError):
        await _service(store, delay_wheel, _ExplodingSink()).discard(tenant_id=TENANT_ID, log_id="l-1", reason="poison")


async def test_transfer_failure_log_is_suppressed_within_window(
    clock: Callable[[], datetime],
    delay_store: InMemoryDelayStore,
    delay_wheel: DelayWheel,
) -> None:
    """同分区同原因连续失败：窗口内第二条不再写日志（只计数）。"""
    scheduler = DelayScheduler(
        store=delay_store,
        messages=_FailingMessages(),  # type: ignore[arg-type]
        wheel=delay_wheel,
        now=clock,
        clock=lambda: 1_000.0,
        failure_log_window_ms=60_000,
    )
    record = _record(clock, span_ms=0)
    await delay_store.enqueue(record)
    delay_wheel.place(record)

    assert await scheduler.run_once() == 0
    assert await scheduler.run_once() == 0
    kept = await delay_store.list_for_tenant(tenant_id=TENANT_ID)
    assert [item.attempts for item in kept] == [5]


async def test_wheel_forget_removes_overdue_and_buckets(clock: Callable[[], datetime], delay_wheel: DelayWheel) -> None:
    """`forget` 同时清桶与待重试队列；重复调用返回 0。"""
    future = _record(clock, log_id="future", span_ms=120_000)
    sibling = _record(clock, log_id="sibling", span_ms=120_000)
    overdue = _record(clock, log_id="overdue", span_ms=0)
    delay_wheel.place(future)
    delay_wheel.place(sibling)
    delay_wheel.place(overdue)

    assert delay_wheel.forget(log_id="future") == 1
    assert delay_wheel.forget(log_id="overdue") == 1
    assert delay_wheel.forget(log_id="future") == 0
    assert delay_wheel.forget(log_id="sibling") == 1
