"""链路字段的承载与透传（msg 任务 06-4；Kiwi 2469）。

覆盖：

- `TraceContext` 三条不变量（不可变 / 各自可空 / 空即单例）与 `trace_of()` 取值口径；
- 发布 → 存储 → 投递全链原样透传；未注入时四项皆 `null`；
- 批量消费逐条回显（不同 trace 两条各回各的，无批级字段）；
- 采样标记只透传：`trace_flags=00` 照常投递，不改写、不丢弃；
- 延迟发布 → 到期转投后四项逐字保持；延迟区记录头往返（含 tombstone）；
- 记录头兼容：无 `trace` 子结构可读、子结构非 map / 值非字符串退化为 `None`、无 span 时不写该键；
- `trace_id` 只读属性在 `RecordHeader` / `StoredMessage` / `DelayRecord` / `MessageHeaderView` 上都可用。
"""

# pyright: reportPrivateUsage=false

import base64
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from typing import cast

import msgpack
import pytest
from bms_core.core.concurrent import ConcurrentStableDict

from bms_msg.broker.delay import DelayRecord, DelayService, InMemoryDelayStore
from bms_msg.broker.delay_file import _clear_header, _put_header, _to_record
from bms_msg.broker.delivery import DeliveryService
from bms_msg.broker.log import InMemoryMessageStore, message_header_view
from bms_msg.broker.record import DelayStamp, RecordHeader, _decode_trace, _pack_header, _unpack_header
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.core.trace import EMPTY_TRACE, TRACE_FIELD_MAX_LENGTH, TraceContext, trace_of
from bms_msg.models.topic import MsgTopic
from bms_msg.schemas.message import ConsumeRequest, DelayLevel, PublishRequest
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2469)

NAMESPACE = "msg"
"""可用命名空间。"""

TOPIC_NAME = "audit.tracing"
"""用例主题名（单分区，分区号恒 0）。"""

GROUP = "audit"
"""用例消费组。"""

CONSUMER = "c1"
"""用例成员标识。"""

TRACE = TraceContext(
    trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
    span_id="00f067aa0ba902b7",
    parent_span_id="b7ad6b7169203331",
    trace_flags="01",
)
"""一组完整链路字段（W3C 形态，便于人工辨认）。"""

_PACK = cast("Callable[..., bytes]", msgpack.packb)  # pyright: ignore[reportUnknownMemberType]
"""编码入口（msgpack 无类型存根，与 `broker/record.py` 同法收窄）。"""

_UNPACK = cast("Callable[..., object]", msgpack.unpackb)  # pyright: ignore[reportUnknownMemberType]
"""解码入口（同上）。"""

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器类型（夹具 `make_request`）。"""

Advance = Callable[[int], None]
"""时钟推进器类型（夹具 `advance`）。"""


@pytest.fixture
async def topic(service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest) -> MsgTopic:
    """单分区主题（覆盖同名夹具：本模块只走分区 0）。

    Args:
        service: 主题服务。
        current_tenant: 当前租户。
        make_request: 创建请求构造器。

    Returns:
        MsgTopic: 新建主题（一分区）。
    """
    return await service.create_topic(tenant=current_tenant, request=make_request(name=TOPIC_NAME, partition_count=1))


def _publish(
    *, trace: TraceContext | None = None, payload: bytes = b"payload", delay: DelayLevel | None = None
) -> PublishRequest:
    """构造发布请求（载荷 base64；可按需带四项链路字段与延迟档位）。

    Args:
        trace: 链路字段（None 表示生产者未注入）。
        payload: 原始载荷。
        delay: 延迟档位（None 为立即发布）。

    Returns:
        PublishRequest: 发布请求。
    """
    context = trace if trace is not None else EMPTY_TRACE
    return PublishRequest(
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        key="order-1",
        payload=base64.b64encode(payload).decode("ascii"),
        trace_id=context.trace_id,
        span_id=context.span_id,
        parent_span_id=context.parent_span_id,
        trace_flags=context.trace_flags,
        delay_level=delay,
    )


def _consume(*, max_messages: int = 10) -> ConsumeRequest:
    """构造拉取请求（`acks=0`：不登记在途，批量取回便于逐条断言）。

    Args:
        max_messages: 本次上限。

    Returns:
        ConsumeRequest: 拉取请求。
    """
    return ConsumeRequest(
        group=GROUP,
        consumer_id=CONSUMER,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition=0,
        max_messages=max_messages,
        acks=0,
    )


def _header(*, trace: TraceContext) -> RecordHeader:
    """构造一条最简记录头（只填判据需要的字段）。

    Args:
        trace: 链路字段。

    Returns:
        RecordHeader: 记录头。
    """
    return RecordHeader(
        format_version=1,
        topic_id=1,
        partition_no=0,
        tenant_id=7001,
        message_id="m-1",
        log_id="l-1",
        key=None,
        trace=trace,
        published_at_us=0,
        headers=(),
        content_type="application/json",
        compression="none",
        uncompressed_len=None,
    )


def _delay_record(*, trace: TraceContext) -> DelayRecord:
    """构造一条延迟记录（仅用于记录头往返与内存区断言）。

    Args:
        trace: 链路字段。

    Returns:
        DelayRecord: 延迟记录。
    """
    return DelayRecord(
        log_id="l-1",
        message_id="m-1",
        tenant_id=7001,
        topic_id=1,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=0,
        delay_level="1s",
        key="k",
        payload=b"payload",
        headers=ConcurrentStableDict({"a": "b"}),
        trace=trace,
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
        deliver_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
    )


def test_trace_context_invariants() -> None:
    """`TraceContext`：空判定 / span 判定 / 换 trace 不动 span，且 `trace_of` 兼容 None 与载体。"""
    assert EMPTY_TRACE.is_empty
    assert not EMPTY_TRACE.has_span
    assert not TRACE.is_empty
    assert TRACE.has_span

    partial = TraceContext(trace_id="t-1")
    assert (partial.is_empty, partial.has_span) == (False, False)

    swapped = TRACE.with_trace_id("t-2")
    assert (swapped.trace_id, swapped.span_id, swapped.parent_span_id, swapped.trace_flags) == (
        "t-2",
        TRACE.span_id,
        TRACE.parent_span_id,
        TRACE.trace_flags,
    )
    assert trace_of(None) is EMPTY_TRACE
    assert trace_of(_publish(trace=TRACE)) == TRACE


async def test_publish_and_consume_round_trip(
    delivery_service: DeliveryService,
    message_store: InMemoryMessageStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """发布 → 存储 → 投递：四项逐字原样，未注入时为 None。"""
    written = await delivery_service.publish(tenant=current_tenant, request=_publish(trace=TRACE))
    assert written.trace == TRACE
    assert written.trace_id == TRACE.trace_id

    stored = await message_store.fetch(
        topic_id=int(topic.id), partition_no=written.partition_no, from_offset=written.offset, max_messages=1
    )
    assert stored[0].trace == TRACE

    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert [item.message.trace for item in delivered] == [TRACE]

    bare = await delivery_service.publish(tenant=current_tenant, request=_publish())
    assert bare.trace == EMPTY_TRACE


async def test_batch_consume_keeps_per_message_trace(
    delivery_service: DeliveryService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """批量消费逐条回显：两条不同 trace 各回各的，不串链。"""
    first = TraceContext(trace_id="t-a", span_id="s-a", trace_flags="01")
    second = TraceContext(trace_id="t-b", span_id="s-b", trace_flags="00")
    await delivery_service.publish(tenant=current_tenant, request=_publish(trace=first))
    await delivery_service.publish(tenant=current_tenant, request=_publish(trace=second))

    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert [item.message.trace for item in delivered] == [first, second]


async def test_sampling_flag_is_not_interpreted(
    delivery_service: DeliveryService,
    message_store: InMemoryMessageStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """`trace_flags=00`（未采样式）照常落盘与投递：broker 不改写、不据此丢弃。"""
    unsampled = TraceContext(trace_id="t-1", span_id="s-1", trace_flags="00")
    written = await delivery_service.publish(tenant=current_tenant, request=_publish(trace=unsampled))
    stored = await message_store.fetch(
        topic_id=int(topic.id), partition_no=written.partition_no, from_offset=0, max_messages=10
    )
    assert (len(stored), stored[0].trace.trace_flags) == (1, "00")


async def test_payload_bytes_are_untouched(
    delivery_service: DeliveryService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """载荷原样：随机二进制往返后 SHA-256 一致（链路字段只动头，不动体）。"""
    payload = bytes(range(256)) + b"\x00\xff\x00"
    written = await delivery_service.publish(tenant=current_tenant, request=_publish(trace=TRACE, payload=payload))
    delivered = await delivery_service.consume(tenant=current_tenant, request=_consume())
    assert sha256(delivered[0].message.payload).digest() == sha256(payload).digest()
    assert sha256(written.payload).digest() == sha256(payload).digest()


async def test_delay_transfer_keeps_trace(
    delay_service: DelayService,
    delay_scheduler: DelayScheduler,
    message_store: InMemoryMessageStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    advance: Advance,
) -> None:
    """延迟发布 → 到期转投：四项与发布时逐字相同，`log_id` 沿用、`trace` 不重新生成。"""
    order = await delay_service.publish(tenant=current_tenant, request=_publish(trace=TRACE, delay=DelayLevel.SECOND_1))
    advance(1_000)

    assert await delay_scheduler.run_once() == 1
    delivered = await message_store.fetch(
        topic_id=int(topic.id), partition_no=order.partition_no, from_offset=0, max_messages=10
    )
    assert [message.trace for message in delivered] == [TRACE]
    assert delivered[0].log_id == order.log_id


def test_delay_record_header_round_trip() -> None:
    """延迟区记录头往返：`trace` 子结构随记录落盘，重建后仍在；tombstone 为空链路。"""
    record = _delay_record(trace=TRACE)
    decoded = _unpack_header(_pack_header(_put_header(record)))
    rebuilt = _to_record(decoded, b"payload", stamp=cast("DelayStamp", decoded.delay))

    assert rebuilt.trace == TRACE
    assert rebuilt.trace_id == TRACE.trace_id
    assert _clear_header(record, now=datetime(2026, 1, 1, tzinfo=UTC)).trace == EMPTY_TRACE


async def test_delay_store_keeps_trace(delay_store: InMemoryDelayStore, current_tenant: CurrentTenant) -> None:
    """延迟区内记录保持四项（读侧不只靠记录头）。"""
    await delay_store.enqueue(_delay_record(trace=TRACE))
    records = await delay_store.list_for_tenant(tenant_id=int(current_tenant.tenant_id))
    assert [record.trace for record in records] == [TRACE]
    assert [record.trace_id for record in records] == [TRACE.trace_id]


def test_record_header_stays_compatible() -> None:
    """记录头兼容：无 `trace` 子结构可读；子结构非 map / 值非字符串一律退化为 None。"""
    legacy = _PACK(
        {
            "format_version": 1,
            "topic_id": 1,
            "partition_no": 0,
            "tenant_id": 7001,
            "message_id": "m-1",
            "log_id": "l-1",
            "key": None,
            "trace_id": "t-legacy",
            "published_at": 0,
            "headers": {},
            "content_type": "application/json",
            "compression": "none",
            "uncompressed_len": None,
        },
        use_bin_type=True,
    )
    assert _unpack_header(legacy).trace == TraceContext(trace_id="t-legacy")

    broken = _PACK(
        {
            "format_version": 1,
            "trace_id": 123,
            "trace": {"span_id": 5, "parent_span_id": None, "trace_flags": "01"},
            "headers": {},
        },
        use_bin_type=True,
    )
    assert _unpack_header(broken).trace == TraceContext(trace_flags="01")

    not_a_map = _PACK({"format_version": 1, "trace_id": "t-1", "trace": "oops"}, use_bin_type=True)
    assert _unpack_header(not_a_map).trace == TraceContext(trace_id="t-1")

    # 解码助手自身也要能吞掉非 map 入参（解出的头不是 map 时退化为空链路，不抛错）
    assert _decode_trace(None) is EMPTY_TRACE
    assert _decode_trace("oops") is EMPTY_TRACE


def test_no_span_writes_no_trace_key() -> None:
    """无 span 侧信息时不写 `trace` 子结构（普通消息头部字节不变）；有则写三项。"""
    plain = cast("dict[str, object]", _UNPACK(_pack_header(_header(trace=TraceContext(trace_id="t-1"))), raw=False))
    assert plain["trace_id"] == "t-1"
    assert "trace" not in plain

    with_span = cast("dict[str, object]", _UNPACK(_pack_header(_header(trace=TRACE)), raw=False))
    assert with_span["trace"] == {
        "span_id": TRACE.span_id,
        "parent_span_id": TRACE.parent_span_id,
        "trace_flags": TRACE.trace_flags,
    }
    assert _header(trace=TRACE).trace_id == TRACE.trace_id
    assert replace(_header(trace=TRACE), trace=EMPTY_TRACE).trace_id is None


async def test_header_view_carries_trace(
    delivery_service: DeliveryService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """只读头视图带四项：管理面检索与按 `log_id` 精查都能拿到链路字段。"""
    written = await delivery_service.publish(tenant=current_tenant, request=_publish(trace=TRACE))
    view = message_header_view(written)
    assert (view.trace, view.trace_id) == (TRACE, TRACE.trace_id)

    found = await message_store.find_by_log_id(log_id=written.log_id)
    assert found is not None
    assert found.trace == TRACE


def test_trace_fields_share_one_limit() -> None:
    """四项共用同一上限常量（128），与 `trace_id` 同尺；超限在契约层拒绝。"""
    assert TRACE_FIELD_MAX_LENGTH == 128
    with pytest.raises(ValueError):
        PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, span_id="x" * (TRACE_FIELD_MAX_LENGTH + 1))
    with pytest.raises(ValueError):
        PublishRequest(namespace=NAMESPACE, topic=TOPIC_NAME, trace_flags="x" * (TRACE_FIELD_MAX_LENGTH + 1))
