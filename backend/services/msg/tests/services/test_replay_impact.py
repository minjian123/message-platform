"""重放影响投影（任务 06-6-1；Kiwi 2468）。

覆盖（详细设计 §5）：

- **采集**：按 `topic_id` 过滤租户内组、组名升序、分区级明细与组级汇总、无组时空基线；
- **截断**：组数超 `REPLAY_IMPACT_MAX_GROUPS` 只留前 100 并置 `truncated`；
- **度量**：按基线分区集合重取，`current_lag` / `delta` 正确；位点超前 LEO 时**不夹取负值**；
- **扩分区**：基线后主题扩分区不影响度量（只算基线里的分区）；
- **编解码**：基线与快照的 payload ↔ 值对象往返，缺列 / 空列返回 None。

用例只依赖三个进程级端口（组 / 位点 / 消息存储），不碰数据库与 HTTP 层。
"""

from datetime import UTC, datetime

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

from bms_msg.broker.group import ConsumerGroup, InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.core.config import REPLAY_IMPACT_MAX_GROUPS
from bms_msg.services.replay_impact import ImpactBaseline, ReplayImpactInspector

pytestmark = pytest.mark.kiwi_id(2468)

TENANT_ID = 8_000_001
"""用例租户主键。"""

TOPIC_ID = 7_000_001
"""用例主题主键。"""

BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准发布时间（历史数据用固定值）。"""


def _inspector(
    *,
    groups: InMemoryGroupStore,
    offsets: InMemoryOffsetStore,
    messages: InMemoryMessageStore,
    now: datetime | None = None,
) -> ReplayImpactInspector:
    """组装影响投影（固定时钟，避免用例与真实时间耦合）。

    Args:
        groups: 组存储。
        offsets: 位点存储。
        messages: 消息存储。
        now: 采集时刻；None 用默认时钟。

    Returns:
        ReplayImpactInspector: 影响投影。
    """
    if now is None:
        return ReplayImpactInspector(groups=groups, offsets=offsets, messages=messages)
    return ReplayImpactInspector(groups=groups, offsets=offsets, messages=messages, now=lambda: now)


async def _group(store: InMemoryGroupStore, *, name: str, topic_id: int = TOPIC_ID, tenant_id: int = TENANT_ID) -> None:
    """向组存储写入一个空组（无成员，位点照旧）。

    Args:
        store: 组存储。
        name: 组名。
        topic_id: 主题主键。
        tenant_id: 租户主键。
    """
    await store.save(
        ConsumerGroup(
            tenant_id=tenant_id,
            topic_id=topic_id,
            group=name,
            state="empty",
            generation=1,
            members=ConcurrentStableList(),
        )
    )


async def _seed(store: InMemoryMessageStore, *, partition_no: int, count: int, topic_id: int = TOPIC_ID) -> None:
    """向分区追加历史记录（直接写存储，不走发布链路）。

    Args:
        store: 消息存储。
        partition_no: 分区号。
        count: 条数。
        topic_id: 主题主键。
    """
    for index in range(count):
        await store.append(
            topic_id=topic_id,
            partition_no=partition_no,
            message_id=f"m-{partition_no}-{index}",
            log_id=f"l-{partition_no}-{index}",
            key=None,
            payload=b"payload",
            headers=ConcurrentStableDict({"source": "test"}),
            trace_id=None,
            published_at=BASE,
        )


async def test_capture_keeps_only_groups_of_topic() -> None:
    """采集只取本主题的组，组名升序，组级积压为各分区之和。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="zeta")
    await _group(groups, name="alpha")
    await _group(groups, name="other-topic", topic_id=TOPIC_ID + 1)
    await _group(groups, name="other-tenant", tenant_id=TENANT_ID + 1)
    await _seed(messages, partition_no=0, count=6)
    await _seed(messages, partition_no=1, count=4)
    await offsets.commit(group="alpha", topic_id=TOPIC_ID, partition_no=0, offset=2)

    baseline = await _inspector(
        groups=groups, offsets=offsets, messages=messages, now=datetime(2026, 2, 1, tzinfo=UTC)
    ).capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=2)

    assert [item.group for item in baseline.groups] == ["alpha", "zeta"]
    assert baseline.truncated is False
    assert baseline.captured_at == datetime(2026, 2, 1)
    alpha = baseline.groups[0]
    assert (alpha.baseline_lag, len(alpha.partitions)) == (8, 2)
    assert [(item.partition_no, item.lag) for item in alpha.partitions] == [(0, 4), (1, 4)]
    assert baseline.groups[1].baseline_lag == 10


async def test_capture_without_groups_returns_empty_baseline() -> None:
    """无订阅组 → 空组清单，但采集时刻照写（详情不报错）。"""
    baseline = await _inspector(
        groups=InMemoryGroupStore(),
        offsets=InMemoryOffsetStore(),
        messages=InMemoryMessageStore(),
        now=datetime(2026, 2, 2, tzinfo=UTC),
    ).capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=2)

    assert (len(baseline.groups), baseline.truncated, baseline.captured_at) == (0, False, datetime(2026, 2, 2))


async def test_capture_truncates_beyond_limit() -> None:
    """组数超过上限 → 只留前 100 个（组名升序）并置 `truncated`。"""
    groups = InMemoryGroupStore()
    for index in range(REPLAY_IMPACT_MAX_GROUPS + 1):
        await _group(groups, name=f"g-{index:03d}")

    baseline = await _inspector(groups=groups, offsets=InMemoryOffsetStore(), messages=InMemoryMessageStore()).capture(
        tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1
    )

    assert len(baseline.groups) == REPLAY_IMPACT_MAX_GROUPS
    assert baseline.truncated is True
    assert (baseline.groups[0].group, baseline.groups[-1].group) == ("g-000", "g-099")


async def test_measure_tracks_appended_records() -> None:
    """基线之后继续写入 → 当前积压变大，delta = 当前 - 基线。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="alpha")
    await _seed(messages, partition_no=0, count=3)
    inspector = _inspector(groups=groups, offsets=offsets, messages=messages)
    baseline = await inspector.capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1)

    await _seed(messages, partition_no=0, count=2)
    affected = await inspector.measure(topic_id=TOPIC_ID, baseline=baseline)

    assert [(item.baseline_lag, item.current_lag, item.delta) for item in affected] == [(3, 5, 2)]
    assert affected[0].captured_at == baseline.captured_at


async def test_measure_keeps_negative_lag() -> None:
    """位点被重置到 LEO 之后 → 积压为负，不夹取（与 01-4 组详情同口径）。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="alpha")
    inspector = _inspector(groups=groups, offsets=offsets, messages=messages)
    baseline = await inspector.capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1)

    await offsets.commit(group="alpha", topic_id=TOPIC_ID, partition_no=0, offset=4)
    affected = await inspector.measure(topic_id=TOPIC_ID, baseline=baseline)

    assert [(item.baseline_lag, item.current_lag, item.delta) for item in affected] == [(0, -4, -4)]


async def test_measure_ignores_partitions_added_after_capture() -> None:
    """基线后扩分区 → 度量只算基线里的分区（口径可比）。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="alpha")
    await _seed(messages, partition_no=0, count=2)
    inspector = _inspector(groups=groups, offsets=offsets, messages=messages)
    baseline = await inspector.capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1)

    await _seed(messages, partition_no=1, count=5)
    affected = await inspector.measure(topic_id=TOPIC_ID, baseline=baseline)

    assert (affected[0].current_lag, affected[0].delta) == (2, 0)


async def test_baseline_payload_round_trip() -> None:
    """基线 payload → 解码还原（含分区明细），空列 / 缺列返回 None。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="alpha")
    await _seed(messages, partition_no=0, count=2)
    inspector = _inspector(groups=groups, offsets=offsets, messages=messages, now=datetime(2026, 3, 1, tzinfo=UTC))
    baseline = await inspector.capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1)

    decoded = inspector.decode_baseline(baseline.payload())

    assert decoded == baseline
    assert inspector.decode_baseline(ConcurrentStableDict()) is None
    assert inspector.decode_baseline(ConcurrentStableDict({"groups": []})) is None


async def test_snapshot_payload_round_trip() -> None:
    """快照 payload → 解码还原（组级字段 + 采集时刻），空列返回 None。"""
    groups = InMemoryGroupStore()
    messages = InMemoryMessageStore()
    offsets = InMemoryOffsetStore()
    await _group(groups, name="alpha")
    await _seed(messages, partition_no=0, count=2)
    inspector = _inspector(groups=groups, offsets=offsets, messages=messages)
    baseline = await inspector.capture(tenant_id=TENANT_ID, topic_id=TOPIC_ID, partition_count=1)
    affected = await inspector.measure(
        topic_id=TOPIC_ID, baseline=baseline, captured_at=datetime(2026, 4, 1, tzinfo=UTC)
    )

    payload = inspector.snapshot_payload(
        affected=affected, truncated=True, captured_at=datetime(2026, 4, 1, tzinfo=UTC)
    )
    decoded = inspector.decode_snapshot(payload)

    assert decoded is not None
    snapshot, truncated = decoded
    assert (truncated, len(snapshot)) == (True, 1)
    assert (snapshot[0].group, snapshot[0].current_lag, snapshot[0].captured_at) == ("alpha", 2, datetime(2026, 4, 1))
    assert inspector.decode_snapshot(None) is None
    assert inspector.decode_snapshot(ConcurrentStableDict({"groups": []})) is None


async def test_impact_baseline_without_groups_measure_empty() -> None:
    """空基线度量 → 空结果（不报错，接口层据此返回空数组）。"""
    inspector = _inspector(groups=InMemoryGroupStore(), offsets=InMemoryOffsetStore(), messages=InMemoryMessageStore())
    baseline = ImpactBaseline(captured_at=datetime(2026, 5, 1), truncated=False, groups=ConcurrentStableList())

    assert list(await inspector.measure(topic_id=TOPIC_ID, baseline=baseline)) == []
