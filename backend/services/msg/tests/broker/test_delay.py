"""延迟区模型与端口（msg 任务 01-5；Kiwi 2417）。

覆盖（详细设计 §4、§8.2、§8.3、§8.5，用例 #1~#7）：

- **档位表**：#1 九档齐全、毫秒单调递增、与契约枚举一一对应，非法档位 `10001`；
- **不占 offset**：#2 延迟发布后 `LEO` / 位点 / 积压三者均不变，日志里查不到该消息；
- **分区锁定**：#3 同 key 恒同分区，分区号写进记录；
- **配额软上限**：#4 注入小上限即 `350015`（429），未超限正常入队；
- **记录可见**：#5 `list_for_tenant` 可取，剩余时间与档位一致；
- **分桶**：#6 `<60s` 落 L0、`90s` 落 L1、`7200s` 落 L2（档位最长 60m，L2 只能由端口级直接入队造出来）；
- **跨度上限**：#7 超 24h 由端口层拒绝（`10001`）；
- 补口：只读视图的分页护栏（接口层被 `Query` 挡在门外，故在服务层直接断言 `ParamError`）。

延迟区不落盘（8B_02 承接），用例全部走内存实现与可推进时钟，不 sleep。
"""

import base64
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.delay import (
    DelayInspector,
    DelayRecord,
    DelayService,
    InMemoryDelayStore,
    delay_level_ms,
)
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.scheduler import DELAY_LAYER_HOUR, DELAY_LAYER_MINUTE, DELAY_LAYER_SECOND, DelayWheel
from bms_msg.core.config import DELAY_LEVEL_MS, DELAY_MAX_SPAN_MS
from bms_msg.core.routing import select_partition
from bms_msg.core.trace import TraceContext
from bms_msg.errors import MsgDelayQuotaExceededError
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import DelayLevel, PublishRequest
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2417)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（夹具 `topic` 的两分区主题）。"""

GROUP = "delay"
"""用例消费组（本文件只用于验证「延迟期不计积压」，不建组）。"""

TENANT_ID = 7001
"""测试租户主键（与夹具上下文同值）。"""


def _publish(*, delay: str | None = None, key: str | None = "order-1", topic: str = TOPIC_NAME) -> PublishRequest:
    """构造发布请求（载荷自动 base64）。

    Args:
        delay: 延迟档位（None 为普通发布，本文件不用）。
        key: 分区键。
        topic: 主题名。

    Returns:
        PublishRequest: 发布请求。
    """
    return PublishRequest.model_validate(
        {
            "namespace": NAMESPACE,
            "topic": topic,
            "key": key,
            "payload": base64.b64encode(b"payload").decode("ascii"),
            "delay_level": delay,
        }
    )


def _record(
    *,
    clock: Callable[[], datetime],
    span_ms: int,
    level: str = "60m",
    log_id: str = "log-1",
    message_id: str = "msg-1",
    partition_no: int = 0,
) -> DelayRecord:
    """构造一条延迟记录（端口级直接入队用：档位表造不出的跨度由此覆盖）。

    Args:
        clock: 时钟（发布时刻基准）。
        span_ms: 延迟跨度（毫秒）。
        level: 档位名（落库口径，不影响分桶）。
        log_id: 记录标识。
        message_id: 消息标识。
        partition_no: 分区号。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = clock()
    return DelayRecord(
        log_id=log_id,
        message_id=message_id,
        tenant_id=TENANT_ID,
        topic_id=1,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=partition_no,
        delay_level=level,
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace=TraceContext(trace_id="trace-1"),
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
    )


def test_delay_levels_match_contract_enum() -> None:
    """档位表：九档齐全、毫秒单调递增、与契约枚举逐值对应；非法档位 `10001`。"""
    levels = list(DELAY_LEVEL_MS)
    milliseconds = list(DELAY_LEVEL_MS.values())
    assert levels == ["1s", "5s", "10s", "30s", "1m", "5m", "10m", "30m", "60m"]
    assert milliseconds == sorted(milliseconds)
    assert [level.value for level in DelayLevel] == levels
    assert [delay_level_ms(level) for level in levels] == milliseconds

    with pytest.raises(ParamError) as invalid:
        delay_level_ms("7m")
    assert invalid.value.code == 10001


async def test_delayed_publish_does_not_touch_offset(
    delay_service: DelayService,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
    clock: Callable[[], datetime],
) -> None:
    """延迟发布不占 offset：`LEO` / 位点 / 积压不变，日志里一条都没有，到期时刻 = 发布时刻 + 档位。"""
    topic_id = int(topic.id)
    order = await delay_service.publish(tenant=current_tenant, request=_publish(delay="5s"))
    leo = await message_store.leo(topic_id=topic_id, partition_no=order.partition_no)
    committed = await offset_store.get(group=GROUP, topic_id=topic_id, partition_no=order.partition_no)
    fetched = await message_store.fetch(
        topic_id=topic_id, partition_no=order.partition_no, from_offset=0, max_messages=10
    )

    assert (leo, committed, leo - committed) == (0, 0, 0)
    assert list(fetched) == []
    assert order.deliver_at == clock() + timedelta(seconds=5)


async def test_delayed_publish_locks_partition(
    delay_service: DelayService,
    delay_store: InMemoryDelayStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """分区锁定：同 key 两次发布落在同一分区（与 `select_partition` 一致）。"""
    del topic
    expected = select_partition("order-1", 2)
    first = await delay_service.publish(tenant=current_tenant, request=_publish(delay="10s"))
    second = await delay_service.publish(tenant=current_tenant, request=_publish(delay="1m"))
    records = await delay_store.list_for_tenant(tenant_id=int(current_tenant.tenant_id))

    assert (first.partition_no, second.partition_no) == (expected, expected)
    assert [record.partition_no for record in records] == [expected, expected]
    assert [record.delay_level for record in records] == ["10s", "1m"]


async def test_quota_rejects_over_soft_limit(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    clock: Callable[[], datetime],
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """配额软上限：注入两条件即第 3 条 `350015`（429），前两条正常入队。"""
    del message_store, topic
    store = InMemoryDelayStore(quota=2)
    service = DelayService(
        store=store,
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        wheel=DelayWheel(now=clock),
        now=clock,
    )
    await service.publish(tenant=current_tenant, request=_publish(delay="30s"))
    await service.publish(tenant=current_tenant, request=_publish(delay="1m"))
    with pytest.raises(MsgDelayQuotaExceededError) as rejected:
        await service.publish(tenant=current_tenant, request=_publish(delay="5m"))

    assert (rejected.value.code, rejected.value.http_status) == (350015, 429)
    assert await store.count_for_tenant(tenant_id=int(current_tenant.tenant_id)) == 2


async def test_pending_record_visible_with_remaining(
    delay_service: DelayService,
    delay_inspector: DelayInspector,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """记录可见：按租户可取，剩余时间与档位一致、字段与发布结果对得上。"""
    del topic
    order = await delay_service.publish(tenant=current_tenant, request=_publish(delay="1m"))
    rows, total = await delay_inspector.list_delayed(
        tenant=current_tenant, namespace=None, topic=None, partition=None, level=None, page=1, size=20
    )
    row = rows[0]

    assert total == 1
    assert (row.record.log_id, row.record.message_id) == (order.log_id, order.message_id)
    assert (row.record.partition_no, row.record.delay_level) == (order.partition_no, "1m")
    assert row.remaining_ms == 60_000


async def test_wheel_layers_follow_remaining(
    delay_store: InMemoryDelayStore, delay_wheel: DelayWheel, clock: Callable[[], datetime]
) -> None:
    """分桶：剩余 `<60s` 落 L0、`90s` 落 L1、`7200s` 落 L2（端口级直接入队，不依赖档位表）。"""
    second = _record(clock=clock, span_ms=30_000, level="30s", log_id="l0")
    minute = _record(clock=clock, span_ms=90_000, level="1m", log_id="l1")
    hour = _record(clock=clock, span_ms=7_200_000, level="60m", log_id="l2")
    for record in (second, minute, hour):
        await delay_store.enqueue(record)

    assert [delay_wheel.layer_of(record) for record in (second, minute, hour)] == [
        DELAY_LAYER_SECOND,
        DELAY_LAYER_MINUTE,
        DELAY_LAYER_HOUR,
    ]
    assert [record.log_id for record in await delay_store.list_all()] == ["l0", "l1", "l2"]


async def test_span_over_limit_rejected(delay_store: InMemoryDelayStore, clock: Callable[[], datetime]) -> None:
    """跨度上限：超 24h 由端口层拒绝（`10001`），边界值本身放行。"""
    over = _record(clock=clock, span_ms=DELAY_MAX_SPAN_MS + 1_000, log_id="over")
    with pytest.raises(ParamError) as rejected:
        await delay_store.enqueue(over)
    assert rejected.value.code == 10001

    exact = _record(clock=clock, span_ms=DELAY_MAX_SPAN_MS, log_id="exact")
    await delay_store.enqueue(exact)
    assert [record.log_id for record in await delay_store.list_all()] == ["exact"]


async def test_inspector_filters_by_namespace(
    delay_store: InMemoryDelayStore,
    delay_inspector: DelayInspector,
    clock: Callable[[], datetime],
    current_tenant: CurrentTenant,
) -> None:
    """命名空间过滤：他域记录不串味（发布链路只产出本域记录，故端口级造一条他域记录）。"""
    await delay_store.enqueue(_record(clock=clock, span_ms=60_000, log_id="mine"))
    await delay_store.enqueue(replace(_record(clock=clock, span_ms=60_000, log_id="other"), namespace="nope"))
    rows, total = await delay_inspector.list_delayed(
        tenant=current_tenant, namespace=NAMESPACE, topic=None, partition=None, level=None, page=1, size=20
    )

    assert (total, [row.record.log_id for row in rows]) == (1, ["mine"])


async def test_inspector_rejects_invalid_paging(delay_inspector: DelayInspector, current_tenant: CurrentTenant) -> None:
    """分页护栏：页码从 1 起、每页 1~100（接口层的 `Query` 之外再兜一层）。"""
    for page, size in ((0, 20), (1, 101)):
        with pytest.raises(ParamError) as rejected:
            await delay_inspector.list_delayed(
                tenant=current_tenant, namespace=None, topic=None, partition=None, level=None, page=page, size=size
            )
        assert rejected.value.code == 10001


async def test_delayed_publish_keeps_payload_marks(
    delay_service: DelayService,
    delay_store: InMemoryDelayStore,
    topic: MsgTopic,
    current_tenant: CurrentTenant,
) -> None:
    """延迟记录保留内容类型 / 压缩标记 / 解压长度（到期转投时原样带入日志记录）。

    Args:
        delay_service: 延迟发布服务。
        delay_store: 延迟区。
        topic: 单分区主题。
        current_tenant: 当前租户。
    """
    del topic
    request = PublishRequest(
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        key="order-1",
        payload=base64.b64encode(b"packed").decode("ascii"),
        delay_level=DelayLevel.SECOND_5,
        content_type="application/json",
        compression="gzip",
        uncompressed_len=64,
    )

    order = await delay_service.publish(tenant=current_tenant, request=request)
    records = await delay_store.list_for_tenant(tenant_id=int(current_tenant.tenant_id))

    record = next(item for item in records if item.log_id == order.log_id)
    assert (record.content_type, record.compression, record.uncompressed_len) == ("application/json", "gzip", 64)
