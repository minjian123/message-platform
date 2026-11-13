"""B21 指标采样按主题隔离（02-6-1 / Kiwi 2498）。

覆盖：单主题未绑定不影响其它主题；失败键保留上一轮值；无旧值不伪造 0；
死信维度同源隔离；失败日志 60s 抑制与恢复日志。
"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from structlog.testing import capture_logs

from bms_msg.broker.log import PartitionNotBoundError
from bms_msg.metrics import sampler as sampler_module
from bms_msg.metrics.lag import MetricsSnapshotStore, topic_key
from bms_msg.metrics.sampler import MetricsSampler

pytestmark = pytest.mark.kiwi_id(2498)


class _Record:
    def __init__(self, tenant_id: int, topic_id: int, group: str) -> None:
        self.tenant_id = tenant_id
        self.topic_id = topic_id
        self.group = group


class _Topic:
    def __init__(self, tenant_id: int, name: str, partition_count: int = 1) -> None:
        self.tenant_id = tenant_id
        self.name = name
        self.partition_count = partition_count


class _Letter:
    def __init__(self, topic_id: int, status: str = "pending") -> None:
        self.topic_id = topic_id
        self.status = status
        self.tenant_id = 1 if topic_id == 100 else 2
        self.topic = "orders" if topic_id == 200 else "bad"


class _BareLetter:
    """无 tenant_id / topic 字段的死信替身（覆盖键回退为 None 的分支）。"""

    def __init__(self, topic_id: int = 100) -> None:
        self.topic_id = topic_id
        self.status = "pending"


class _Groups:
    def __init__(self, records: ConcurrentStableList[_Record]) -> None:
        self._records = records

    async def list_all(self) -> ConcurrentStableList[_Record]:
        return self._records


class _Messages:
    def __init__(self) -> None:
        self.fail = False

    async def leo(self, *, topic_id: int, partition_no: int) -> int:
        del partition_no
        if self.fail and topic_id == 100:
            raise PartitionNotBoundError("主题目录未绑定")
        return 10


class _Offsets:
    async def get(self, *, group: str, topic_id: int, partition_no: int) -> int:
        del group, topic_id, partition_no
        return 7


class _DeadLetters:
    def __init__(self, letters: ConcurrentStableList[_Letter]) -> None:
        self._letters = letters

    async def list_all(self) -> ConcurrentStableList[_Letter]:
        return self._letters


class _Watermark:
    def force_refresh(self) -> None:
        pass


class _Tenant:
    def __init__(self, code: str) -> None:
        self.code = code


class _TenantLookup:
    async def by_id(self, tenant_id: str, *, readonly: bool = False) -> _Tenant:
        del readonly
        return _Tenant(f"code-{tenant_id}")


class _Session:
    def __init__(self, topics: dict[int, _Topic], *, fail_topic: int | None = None) -> None:
        self._topics = topics
        self._fail_topic = fail_topic

    async def get(self, model: object, key: object) -> _Topic | None:
        del model
        topic_id = int(cast("int", key))
        if self._fail_topic == topic_id:
            raise PartitionNotBoundError("主题目录未绑定")
        return self._topics.get(topic_id)


class _Uow:
    def __init__(self, session: _Session) -> None:
        self.session = session


class _UowContext:
    def __init__(self, uow: _Uow) -> None:
        self._uow = uow

    async def __aenter__(self) -> _Uow:
        return self._uow

    async def __aexit__(self, *args: object) -> bool:
        del args
        return False


def _build(*, fail_topic: int | None = None) -> tuple[MetricsSampler, MetricsSnapshotStore, _Messages]:
    snapshots = MetricsSnapshotStore()
    messages = _Messages()
    groups = _Groups(ConcurrentStableList([_Record(1, 100, "g1"), _Record(2, 200, "g1")]))
    letters = ConcurrentStableList([_Letter(100), _Letter(200)])
    topics = {100: _Topic(1, "bad"), 200: _Topic(2, "orders")}

    def _uow_factory() -> _UowContext:
        return _UowContext(_Uow(_Session(topics, fail_topic=fail_topic)))

    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", groups),
        messages=cast("Any", messages),
        offsets=cast("Any", _Offsets()),
        dead_letters=cast("Any", _DeadLetters(letters)),
        uow_factory=cast("Any", _uow_factory),
        watermark=cast("Any", _Watermark()),
        tenant_lookup=cast("Any", _TenantLookup()),
    )
    return sampler, snapshots, messages


def _build_with_letters(
    letters: ConcurrentStableList[object], *, fail_topic: int | None = None
) -> tuple[MetricsSampler, MetricsSnapshotStore]:
    """按给定死信行构造采样器（覆盖键回退分支）。"""
    snapshots = MetricsSnapshotStore()
    topics = {100: _Topic(1, "bad"), 200: _Topic(2, "orders")}

    def _uow_factory() -> _UowContext:
        return _UowContext(_Uow(_Session(topics, fail_topic=fail_topic)))

    sampler = MetricsSampler(
        snapshots=snapshots,
        groups=cast("Any", _Groups(ConcurrentStableList())),
        messages=cast("Any", _Messages()),
        offsets=cast("Any", _Offsets()),
        dead_letters=cast("Any", _DeadLetters(cast("Any", letters))),
        uow_factory=cast("Any", _uow_factory),
        watermark=cast("Any", _Watermark()),
        tenant_lookup=cast("Any", _TenantLookup()),
    )
    return sampler, snapshots


def _key(tenant: int, topic: str) -> str:
    return topic_key(tenant=f"code-{tenant}", topic=topic)


async def test_lag_isolates_unbound_topic_and_keeps_previous() -> None:
    """主题 100 未绑定：主题 200 照常更新，100 保留上一轮值。"""
    sampler, snapshots, messages = _build()
    key_bad = _key(1, "bad")
    key_ok = _key(2, "orders")
    previous: ConcurrentStableDict[str, int] = ConcurrentStableDict({key_bad: 42, key_ok: 9})
    snapshots.replace(lag=previous)
    sampler._topic_keys.set(100, key_bad)

    messages.fail = True
    lag = await sampler._sample_lag()

    assert lag[key_bad] == 42
    assert lag[key_ok] == 3


async def test_lag_without_previous_does_not_fake_zero() -> None:
    """首次采样即失败且无旧值：该键不出现在快照中（不伪造 0）。"""
    sampler, _, messages = _build()
    sampler._topic_keys.set(100, _key(1, "bad"))
    messages.fail = True

    lag = await sampler._sample_lag()

    assert _key(1, "bad") not in lag
    assert lag[_key(2, "orders")] == 3


async def test_dlq_isolates_failed_topic() -> None:
    """死信维度：主题 100 反查失败时，主题 200 仍统计，100 保留旧值。"""
    sampler, snapshots, _ = _build(fail_topic=100)
    key_bad = _key(1, "bad")
    key_ok = _key(2, "orders")
    snapshots.replace(dlq=ConcurrentStableDict({key_bad: 5, key_ok: 0}))
    sampler._topic_keys.set(100, key_bad)

    dlq = await sampler._sample_dlq()

    assert dlq[key_bad] == 5
    assert dlq[key_ok] == 1


async def test_failure_log_is_suppressed_and_recovered(monkeypatch: pytest.MonkeyPatch) -> None:
    """同键 60s 内只一条 warning；恢复记一条 info。"""
    sampler, _, messages = _build()
    await sampler._sample_lag()

    messages.fail = True
    with capture_logs() as logs:
        await sampler._sample_lag()
        await sampler._sample_lag()
    warnings = [item for item in logs if item["event"] == "msg_metrics_sample_failed"]
    assert len(warnings) == 1
    assert warnings[0]["dimension"] == "lag"

    monkeypatch.setattr(sampler_module, "METRICS_SAMPLE_FAILURE_LOG_WINDOW_SECONDS", 0.0)
    with capture_logs() as logs:
        await sampler._sample_lag()
    assert len([item for item in logs if item["event"] == "msg_metrics_sample_failed"]) == 1

    messages.fail = False
    with capture_logs() as logs:
        await sampler._sample_lag()
    assert [item for item in logs if item["event"] == "msg_metrics_sample_recovered"]


async def test_lag_failure_before_key_known_logs_topic_marker() -> None:
    """主题反查阶段失败且无键缓存：只记 `topic:{id}`，不中断其它主题。"""
    sampler, _, _ = _build(fail_topic=100)
    with capture_logs() as logs:
        lag = await sampler._sample_lag()
    assert _key(1, "bad") not in lag
    assert lag[_key(2, "orders")] == 3
    assert [item["key"] for item in logs if item["event"] == "msg_metrics_sample_failed"] == ["topic:100"]


async def test_dlq_fallback_key_and_missing_fields() -> None:
    """死信键回退：行内字段可用时成功；字段缺失时返回 None 并记 topic 标记。"""
    sampler, _, _ = _build()
    assert await sampler._letter_key(_Letter(100)) == _key(1, "bad")
    assert await sampler._letter_key(_BareLetter()) is None

    sampler, _ = _build_with_letters(cast("Any", ConcurrentStableList([_BareLetter()])), fail_topic=100)
    with capture_logs() as logs:
        dlq = await sampler._sample_dlq()
    assert dlq == {}
    assert [item["key"] for item in logs if item["event"] == "msg_metrics_sample_failed"] == ["topic:100"]


async def test_dlq_failed_key_without_previous_is_omitted() -> None:
    """失败键上一轮无值：不写入（不伪造 0）。"""
    sampler, _ = _build_with_letters(cast("Any", ConcurrentStableList([_Letter(100)])), fail_topic=100)
    sampler._topic_keys.set(100, _key(1, "bad"))
    dlq = await sampler._sample_dlq()
    assert dlq == {}


def test_ingress_counter_without_tenant_label_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    """入口计数缺 `tenant` 标签：不计入每租户增量，但总量照常累计。"""
    sampler, snapshots, _ = _build()

    class _Traffic:
        def counter_snapshot(self) -> dict[str, dict[str, int]]:
            return {"msg_messages_in_total": {"no-tenant": 1}}

    monkeypatch.setattr(sampler_module, "get_traffic_metrics", lambda: _Traffic())
    sampler._sample_ingress()
    assert snapshots.snapshot.messages_in_window == 0
    assert len(sampler._ingress_deltas) == 0
