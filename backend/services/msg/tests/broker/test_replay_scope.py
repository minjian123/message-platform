"""重放范围解析与计数（msg 任务 01-6；Kiwi 2418）。

覆盖（详细设计 §10.1 用例 S1~S7）：

- **S1** offset 左闭右开；**S2** 时间范围（按 `published_at`）；**S3** key 精确过滤（跨分区）；
- **S4** 组合条件取交集；**S5** `partitions` 缺省 = 全部分区；
- **S6** 条件落在已清理区间 → **裁剪而非报错**（历史已消失是正常状态）；
- **S7** 裁剪后为空 → `total = 0`（任务照建，执行器首轮直接判完成）。

记录直接经存储层写入（端口级入队），不走发布链路：用例要精确控制 `published_at` 与 key，
且不必为「造历史数据」在生产分支开口子。
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore, StoredMessage
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayService
from bms_msg.broker.router import TopicRouter
from bms_msg.models.replay import MsgReplayTask
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.services.replay_impact import ReplayImpactInspector
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2418)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（与 `topic` 夹具一致）。"""

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间（用例内按秒偏移）。"""


def _cursors(task: MsgReplayTask) -> list[dict[str, int]]:
    """把游标快照转成普通字典列表（断言可读）。

    Args:
        task: 任务行。

    Returns:
        list[dict[str, int]]: 游标列表。
    """
    return [dict(cursor.items()) for cursor in task.cursor_state]


def _at(offset_seconds: int) -> datetime:
    """基准时刻 + 秒数。

    Args:
        offset_seconds: 相对基准的秒数。

    Returns:
        datetime: 带时区的时刻。
    """
    return _BASE + timedelta(seconds=offset_seconds)


async def _seed(
    store: InMemoryMessageStore, *, topic: MsgTopic, partition: int, keys: list[str | None], at: datetime
) -> None:
    """向指定分区写入若干记录（同一发布时间、给定 key 序列）。

    Args:
        store: 消息存储。
        topic: 主题记录（取主键）。
        partition: 分区号。
        keys: 各记录的 key。
        at: 发布时间。
    """
    for index, key in enumerate(keys):
        await store.append(
            topic_id=int(topic.id),
            partition_no=partition,
            message_id=f"m-{partition}-{index}",
            log_id=f"l-{partition}-{index}",
            key=key,
            payload=b"payload",
            headers=ConcurrentStableDict({"source": "test"}),
            trace_id=None,
            published_at=at,
        )


def _request(**overrides: object) -> ReplayRequest:
    """构造建任务请求。

    Args:
        **overrides: 覆盖字段。

    Returns:
        ReplayRequest: 建任务请求。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict({"namespace": NAMESPACE, "topic": TOPIC_NAME})
    for key, value in overrides.items():
        payload.set(key, value)
    return ReplayRequest.model_validate(dict(payload.items()))


async def test_offset_range_is_half_open(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S1：offset 左闭右开（`[offset_from, offset_to)`）。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None] * 5, at=_at(0))

    task = await replay_service.create(
        tenant=current_tenant, request=_request(partitions=[0], offset_from=1, offset_to=3)
    )

    assert task.total == 2
    assert _cursors(task) == [{"partition": 0, "next": 1, "end": 3}]


async def test_time_range_filters_by_published_at(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S2：时间范围按 `published_at` 过滤（左闭右开）。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(0))
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(10))
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(20))

    task = await replay_service.create(
        tenant=current_tenant, request=_request(partitions=[0], time_from=_at(5), time_to=_at(20))
    )

    assert task.total == 1


async def test_key_filter_spans_partitions(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S3：key 精确匹配（跨分区按各分区分别命中）。"""
    await _seed(message_store, topic=topic, partition=0, keys=["order-1", "order-2"], at=_at(0))
    await _seed(message_store, topic=topic, partition=1, keys=["order-1"], at=_at(0))

    task = await replay_service.create(tenant=current_tenant, request=_request(key="order-1"))

    assert task.total == 2
    assert [int(cursor["partition"]) for cursor in task.cursor_state] == [0, 1]


async def test_conditions_are_conjunctive(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S4：跨类条件取交集（offset ∩ 时间 ∩ key）。"""
    await _seed(message_store, topic=topic, partition=0, keys=["order-1", "order-1", "order-2"], at=_at(0))
    await _seed(message_store, topic=topic, partition=0, keys=["order-1"], at=_at(30))

    task = await replay_service.create(
        tenant=current_tenant,
        request=_request(partitions=[0], offset_from=0, offset_to=3, time_to=_at(10), key="order-1"),
    )

    assert task.total == 2


async def test_default_partitions_cover_topic(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S5：`partitions` 缺省 → 主题全部分区（两分区主题，创建时固化）。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(0))
    await _seed(message_store, topic=topic, partition=1, keys=[None], at=_at(0))

    task = await replay_service.create(tenant=current_tenant, request=_request())

    assert [int(cursor["partition"]) for cursor in task.cursor_state] == [0, 1]
    assert task.total == 2
    assert list(task.partitions) == [0, 1]


async def test_range_is_clipped_to_readable_window(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S6：条件早于可读起点 → 裁剪到起点，不报错。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None] * 4, at=_at(0))
    await message_store.truncate_before(topic_id=int(topic.id), partition_no=0, offset=2)

    task = await replay_service.create(
        tenant=current_tenant, request=_request(partitions=[0], offset_from=0, offset_to=4)
    )

    assert task.total == 2
    assert _cursors(task) == [{"partition": 0, "next": 2, "end": 4}]


async def test_empty_range_yields_zero_total(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S7：裁剪后为空 → `total = 0`，任务照建（「没东西可重放」不是错误）。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None] * 2, at=_at(0))

    task = await replay_service.create(
        tenant=current_tenant, request=_request(partitions=[0], offset_from=10, offset_to=20)
    )

    assert task.total == 0
    assert _cursors(task) == [{"partition": 0, "next": 10, "end": 10}]


class HollowStore(InMemoryMessageStore):
    """读取替身：单次最多返回 `page` 条，且游标落在 `hole_at` 时返回空批。

    模拟「分页读 + 扫描期间并发清理把可读起点前移」——读空必须收尾，否则 `_count` 会死循环。
    """

    def __init__(self, *, hole_at: int, page: int = 2) -> None:
        """初始化。

        Args:
            hole_at: 该 offset 起的读取返回空批。
            page: 单次最多返回条数（强制分批）。
        """
        super().__init__()
        self._hole = hole_at
        self._page = page

    async def fetch(
        self, *, topic_id: int, partition_no: int, from_offset: int, max_messages: int
    ) -> ConcurrentStableList[StoredMessage]:
        """在空洞处返回空批，其余按页上限转调真实实现。

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
            max_messages=min(max_messages, self._page),
        )


def _service(*, store: InMemoryMessageStore, db_session: AsyncSession, now: Callable[[], datetime]) -> ReplayService:
    """组装重放服务（绑定指定存储；替身存储场景用）。

    Args:
        store: 消息存储。
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


async def test_scan_stops_when_store_reads_empty(
    db_session: AsyncSession, current_tenant: CurrentTenant, topic: MsgTopic, clock: Callable[[], datetime]
) -> None:
    """S8：扫描期间读空（并发清理前移起点）→ 收尾不计数，不进入死循环。"""
    store = HollowStore(hole_at=2)
    await _seed(store, topic=topic, partition=0, keys=[None] * 4, at=_at(0))

    task = await _service(store=store, db_session=db_session, now=clock).create(
        tenant=current_tenant, request=_request(partitions=[0])
    )

    assert task.total == 2


async def test_naive_time_is_interpreted_as_utc(
    replay_service: ReplayService,
    message_store: InMemoryMessageStore,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
) -> None:
    """S9：无时区的时间按 UTC 解释（不与记录的 aware 时间比较报错）。"""
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(0))
    await _seed(message_store, topic=topic, partition=0, keys=[None], at=_at(600))

    task = await replay_service.create(
        tenant=current_tenant, request=_request(partitions=[0], time_from=datetime(2026, 1, 1, 0, 5))
    )

    assert task.total == 1
