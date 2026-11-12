"""存储水位与拒绝策略（msg 任务 02-6；Kiwi 2430）。

覆盖（对应详细设计 §12 用例表 T1~T19 与 §14 验收映射）：

- **口径**：卷级使用率、数据根缺失向上取祖先、采样失败 / 容量未知判不可用；
- **滞回**：含等于进入拒绝、中间区间保持、降到告警水位以下才解除、阈值附近不抖动；
- **缓存**：TTL 内复用同一读数、`force_refresh()` 立即重采；
- **拒绝**：普通发布与延迟发布均返回 `350022` / 507，`data` 五字段齐全，且**没有写入发生**；
- **闸门缺省**：`gate=None` 时既有发布语义不变；
- **指标**：按 volume 覆盖写、拒绝按 reason 计数、分键有界、不可用不写；
- **日志**：进入 / 解除各一条、不可读一条、重复采样不重复记；
- **转投失败**：日志带 `reason` / `errno`，同分区同原因按窗口抑制，记录不丢；
- **恢复台账**：`RecoveryLedger` 的登记 / 过滤与文件实现的只读出口。
"""

import base64
import errno
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker import scheduler as scheduler_module
from bms_msg.broker import watermark as watermark_module
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayRecord, DelayService, InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.recovery import RecoveryLedger, RecoveryReport
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.broker.watermark import (
    OTHER_KEY,
    STORAGE_RATIO_DIGITS,
    TOTAL_KEY,
    StorageMetrics,
    StorageReading,
    StorageWatermark,
)
from bms_msg.core.config import STORAGE_REJECT_RATIO, STORAGE_SAMPLE_TTL_MS, STORAGE_WARN_RATIO
from bms_msg.core.trace import EMPTY_TRACE
from bms_msg.errors import MsgErrorCode, MsgStorageWatermarkExceededError
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import DelayLevel, PublishRequest
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2430)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（复用 conftest 的两分区主题）。"""

TOTAL = 1_000
"""采样注入的卷总容量（字节；配合 used 值直接得到千分比，便于写成精确比例）。"""


class _Clock:
    """单调毫秒假时钟（TTL 与抑制窗口判据）。"""

    def __init__(self) -> None:
        """初始化（从 0 开始）。"""
        self.now = 0.0

    def __call__(self) -> float:
        """取当前时刻。

        Returns:
            float: 单调毫秒。
        """
        return self.now

    def advance(self, ms: float) -> None:
        """推进时钟。

        Args:
            ms: 推进毫秒数。
        """
        self.now += ms


class _Usage:
    """可编排的容量读数（每次调用取下一个 used；耗尽后沿用最后一个）。"""

    def __init__(self, used: Sequence[int], *, total: int = TOTAL) -> None:
        """初始化。

        Args:
            used: 依次返回的已用字节序列。
            total: 卷总容量。
        """
        self._used = list(used)
        self._total = total
        self.calls = 0
        self.paths: list[Path] = []

    def __call__(self, path: Path) -> tuple[int, int, int]:
        """读一次容量。

        Args:
            path: 被采样的路径。

        Returns:
            tuple[int, int, int]: `(total, used, free)`。
        """
        self.paths.append(path)
        used = self._used[min(self.calls, len(self._used) - 1)]
        self.calls += 1
        return self._total, used, self._total - used


class _Recorder:
    """日志替身：只记录事件名与字段（断言结构化语义，不依赖渲染格式）。"""

    def __init__(self) -> None:
        """初始化（空记录）。"""
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **fields: object) -> None:
        """记录 INFO 事件。

        Args:
            event: 事件名。
            **fields: 结构化字段。
        """
        self.events.append((event, dict(fields)))

    def warning(self, event: str, **fields: object) -> None:
        """记录 WARNING 事件。

        Args:
            event: 事件名。
            **fields: 结构化字段。
        """
        self.events.append((event, dict(fields)))

    def names(self) -> list[str]:
        """取事件名序列。

        Returns:
            list[str]: 事件名。
        """
        return [name for name, _fields in self.events]


def _watermark(
    root: Path,
    usage: Callable[[Path], tuple[int, int, int]],
    *,
    clock: _Clock | None = None,
    warn: float = 0.70,
    reject: float = 0.85,
    ttl_ms: int = STORAGE_SAMPLE_TTL_MS,
    metrics: StorageMetrics | None = None,
) -> StorageWatermark:
    """构造水位组件（阈值与 TTL 显式给出，避免环境变量干扰）。

    Args:
        root: 数据根。
        usage: 容量读数。
        clock: 假时钟。
        warn: 告警阈值。
        reject: 拒绝阈值。
        ttl_ms: 采样缓存有效期。
        metrics: 指标采集器。

    Returns:
        StorageWatermark: 水位组件。
    """
    return StorageWatermark(
        root=root,
        warn_ratio=warn,
        reject_ratio=reject,
        ttl_ms=ttl_ms,
        clock=clock if clock is not None else _Clock(),
        usage=usage,
        metrics=metrics,
    )


def _rejecting(root: Path) -> StorageWatermark:
    """构造「已进入拒绝」的水位组件（95% 使用率）。

    Args:
        root: 数据根。

    Returns:
        StorageWatermark: 拒绝态水位组件。
    """
    watermark = _watermark(root, _Usage([950]))
    watermark.refresh()
    return watermark


def _publish(*, delay_level: DelayLevel | None = None) -> PublishRequest:
    """构造发布请求。

    Args:
        delay_level: 延迟档位（None 表示普通发布）。

    Returns:
        PublishRequest: 发布请求。
    """
    return PublishRequest(
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        key="order-1",
        payload=base64.b64encode(b"payload").decode("ascii"),
        delay_level=delay_level,
    )


async def _leo_sum(store: InMemoryMessageStore, topic: MsgTopic) -> int:
    """取主题全部分区的 LEO 之和（「没有写入」的判据）。

    Args:
        store: 内存消息存储。
        topic: 主题。

    Returns:
        int: 各分区 LEO 之和。
    """
    total = 0
    for partition_no in range(topic.partition_count):
        total += await store.leo(topic_id=int(topic.id), partition_no=partition_no)
    return total


def test_reading_uses_volume_usage(tmp_path: Path) -> None:
    """T1：读数取注入的卷使用率，卷标识为数据根绝对路径。"""
    usage = _Usage([400])
    watermark = _watermark(tmp_path, usage)

    reading = watermark.reading()

    assert reading.used_ratio == pytest.approx(0.4)
    assert reading.total_bytes == TOTAL
    assert reading.free_bytes == TOTAL - 400
    assert reading.volume == str(tmp_path.absolute())
    assert reading.unreadable is False
    assert reading.exceeded is False
    assert usage.paths == [tmp_path]
    watermark.ensure_writable()  # 未超阈值：闸门静默放行（不抛、不计数）


def test_missing_root_walks_to_existing_ancestor(tmp_path: Path) -> None:
    """T2：数据根不存在 → 向上取最近存在的祖先（目录懒创建是常态）。"""
    missing = tmp_path / "var" / "lib" / "msg"
    usage = _Usage([100])
    watermark = _watermark(missing, usage)

    reading = watermark.reading()

    assert usage.paths == [tmp_path]
    assert reading.volume == str(tmp_path.absolute())
    assert reading.used_ratio == pytest.approx(0.1)


def test_ancestor_walk_exhausted_is_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """T3a：一路上溯到根仍不存在（盘符不存在 / 卷未挂载）→ 不可用，不当水位 0。"""

    def _missing(_self: Path) -> Any:
        raise FileNotFoundError(2, "no such file or directory")

    monkeypatch.setattr(Path, "stat", _missing)
    watermark = _watermark(tmp_path, _Usage([100]))

    reading = watermark.reading()

    assert reading.unreadable is True
    assert reading.error == "FileNotFoundError"
    assert reading.used_ratio is None
    assert watermark.rejected is True


def test_sampling_failure_is_unavailable(tmp_path: Path) -> None:
    """T3b：采样抛 `OSError` → 不可用（fail-closed：拿不到容量就不保证能写）。"""

    def _boom(_path: Path) -> tuple[int, int, int]:
        raise PermissionError(13, "permission denied")

    watermark = _watermark(tmp_path, _boom)
    reading = watermark.reading()

    assert reading.unreadable is True
    assert reading.error == "PermissionError"
    assert reading.exceeded is False, "不可用由 error 表达，不与「超阈值」混为一谈"
    with pytest.raises(MsgStorageWatermarkExceededError) as excinfo:
        watermark.ensure_writable()
    data = cast("dict[str, object]", excinfo.value.data)
    assert data["reason"] == "sampling_failed"
    assert data["used_ratio"] is None
    assert "不可用" in (excinfo.value.message or "")


def test_zero_capacity_is_unavailable(tmp_path: Path) -> None:
    """T4：`total == 0`（容器 quota 异常）→ 不可用，不出现除零。"""
    watermark = _watermark(tmp_path, _Usage([0], total=0))

    reading = watermark.reading()

    assert reading.unreadable is True
    assert reading.error == "StorageUnavailableError"
    assert watermark.rejected is True


def test_reject_at_threshold_is_inclusive(tmp_path: Path) -> None:
    """T5：正好等于拒绝阈值即进入拒绝（含等于）。"""
    watermark = _watermark(tmp_path, _Usage([850]), clock=_Clock())

    reading = watermark.reading()

    assert reading.used_ratio == pytest.approx(0.85)
    assert reading.exceeded is True
    assert watermark.rejected is True


def test_hysteresis_keeps_rejected_in_middle_band(tmp_path: Path) -> None:
    """T6：从 90% 降到 80%（中间区间）→ 仍拒绝（滞回的全部意义）。"""
    clock = _Clock()
    watermark = _watermark(tmp_path, _Usage([900, 800]), clock=clock)
    watermark.refresh()
    assert watermark.rejected is True

    clock.advance(2_000)
    reading = watermark.refresh()

    assert reading.used_ratio == pytest.approx(0.8)
    assert watermark.rejected is True


def test_hysteresis_releases_below_warn(tmp_path: Path) -> None:
    """T7：降到告警阈值以下才解除（严格小于）。"""
    clock = _Clock()
    watermark = _watermark(tmp_path, _Usage([900, 700, 699]), clock=clock)
    watermark.refresh()

    clock.advance(2_000)
    watermark.refresh()
    assert watermark.rejected is True, "正好 70% 不解除"

    clock.advance(2_000)
    watermark.refresh()
    assert watermark.rejected is False


def test_hysteresis_does_not_flap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T8：阈值附近连续采样（84/85/84/86）→ 只在第一次 ≥85% 翻转，不抖动。"""
    recorder = _Recorder()
    monkeypatch.setattr(watermark_module, "_LOGGER", recorder)
    clock = _Clock()
    watermark = _watermark(tmp_path, _Usage([840, 850, 840, 860]), clock=clock)

    for _ in range(4):
        watermark.refresh()
        clock.advance(2_000)

    assert recorder.names() == ["msg_storage_watermark_enter"]
    assert watermark.rejected is True


def test_ttl_reuses_reading(tmp_path: Path) -> None:
    """T9：TTL 内复用同一份读数（publish 热路径只做一次时间比较）。"""
    clock = _Clock()
    usage = _Usage([100, 200])
    watermark = _watermark(tmp_path, usage, clock=clock)

    first = watermark.reading()
    second = watermark.reading()
    assert usage.calls == 1
    assert first is second

    clock.advance(STORAGE_SAMPLE_TTL_MS)
    third = watermark.reading()
    assert usage.calls == 2
    assert third.used_ratio == pytest.approx(0.2)


def test_force_refresh_bypasses_ttl(tmp_path: Path) -> None:
    """T10：`force_refresh()` 立即重采（06-2 清理 / 扩容后的唯一即时入口）。"""
    clock = _Clock()
    usage = _Usage([900, 100])
    watermark = _watermark(tmp_path, usage, clock=clock)

    watermark.reading()
    assert watermark.rejected is True

    refreshed = watermark.force_refresh()

    assert usage.calls == 2
    assert refreshed.used_ratio == pytest.approx(0.1)
    assert watermark.rejected is False


def test_invalid_thresholds_fail_fast(tmp_path: Path) -> None:
    """T11：阈值非法即启动失败（不静默降级成某个档）。"""
    with pytest.raises(ValueError, match="水位阈值须满足"):
        _watermark(tmp_path, _Usage([100]), warn=0.9, reject=0.85)
    with pytest.raises(ValueError, match="取值越界"):
        _watermark(tmp_path, _Usage([100]), warn=1.5)
    with pytest.raises(ValueError, match="取值越界"):
        _watermark(tmp_path, _Usage([100]), reject=0.0)


def test_env_overrides_and_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T11b：环境变量优先于常量；非法取值即失败。"""
    monkeypatch.setenv("MSG_STORAGE_WARN_RATIO", "0.5")
    monkeypatch.setenv("MSG_STORAGE_REJECT_RATIO", "0.6")
    watermark = StorageWatermark(root=tmp_path, usage=_Usage([650]))

    assert (watermark.warn_ratio, watermark.reject_ratio) == (0.5, 0.6)
    assert watermark.reading().exceeded is True

    monkeypatch.setenv("MSG_STORAGE_WARN_RATIO", "abc")
    with pytest.raises(ValueError, match="取值非法"):
        StorageWatermark(root=tmp_path, usage=_Usage([100]))


def test_sample_ttl_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T11c：采样 TTL 非正 / 非整数即失败；常量缺省值不变。"""
    with pytest.raises(ValueError, match="须为正数"):
        _watermark(tmp_path, _Usage([100]), ttl_ms=0)
    monkeypatch.setenv("MSG_STORAGE_SAMPLE_TTL_MS", "abc")
    with pytest.raises(ValueError, match="取值非法"):
        StorageWatermark(root=tmp_path, usage=_Usage([100]))
    monkeypatch.setenv("MSG_STORAGE_SAMPLE_TTL_MS", "")
    from_env = StorageWatermark(root=tmp_path, usage=_Usage([100]))
    assert from_env._ttl_ms == STORAGE_SAMPLE_TTL_MS  # pyright: ignore[reportPrivateUsage]
    assert (STORAGE_WARN_RATIO, STORAGE_REJECT_RATIO) == (0.70, 0.85)


async def test_gate_none_keeps_publish_working(
    db_session: AsyncSession, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """T12：`gate=None`（未装配水位）时发布语义不变。"""
    store = InMemoryMessageStore(segment_records=2)
    service = DeliveryService(
        store=store,
        offsets=InMemoryOffsetStore(),
        dead_letters=InMemoryDeadLetterQueue(),
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        state=DeliveryState(),
        gate=None,
    )

    message = await service.publish(tenant=current_tenant, request=_publish())

    assert message.offset == 0
    assert await _leo_sum(store, topic) == 1


async def test_publish_rejected_when_watermark_exceeded(
    tmp_path: Path, db_session: AsyncSession, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """T13：普通发布命中高水位 → `350022` / 507，`data` 五字段齐全，且没有写入。"""
    store = InMemoryMessageStore(segment_records=2)
    metrics = StorageMetrics()
    watermark = _watermark(tmp_path, _Usage([9_500_000], total=10_000_000), metrics=metrics)
    service = DeliveryService(
        store=store,
        offsets=InMemoryOffsetStore(),
        dead_letters=InMemoryDeadLetterQueue(),
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        state=DeliveryState(),
        gate=watermark,
    )

    with pytest.raises(MsgStorageWatermarkExceededError) as excinfo:
        await service.publish(tenant=current_tenant, request=_publish())

    error = excinfo.value
    assert (error.code, error.http_status) == (350022, 507)
    data = cast("dict[str, object]", error.data)
    assert set(data) == {"used_ratio", "threshold", "volume", "free_bytes", "reason"}
    assert data["reason"] == "watermark"
    assert data["used_ratio"] == round(0.95, STORAGE_RATIO_DIGITS)
    assert data["threshold"] == pytest.approx(0.85)
    assert "95.0%" in (error.message or "")
    assert "85.0%" in (error.message or "")
    assert str(tmp_path) in (error.message or "")
    assert "488.3 KiB" in (error.message or "")  # 剩余空间按 KiB 渲染（覆盖单位换算链）
    assert await _leo_sum(store, topic) == 0
    rejected = cast("dict[str, int]", metrics.snapshot()["msg_storage_rejected_total"])
    assert rejected[TOTAL_KEY] == 1


async def test_delay_publish_rejected_when_watermark_exceeded(
    tmp_path: Path, db_session: AsyncSession, topic: MsgTopic, current_tenant: CurrentTenant
) -> None:
    """T14：延迟发布命中高水位 → 同样 `350022`，且**没有进延迟区**（不得先收再丢）。"""
    delay_store = InMemoryDelayStore()
    service = DelayService(
        store=delay_store,
        router=TopicRouter(TopicRepository(cast("DbSession", db_session))),
        wheel=DelayWheel(),
        gate=_rejecting(tmp_path),
    )

    with pytest.raises(MsgStorageWatermarkExceededError):
        await service.publish(tenant=current_tenant, request=_publish(delay_level=DelayLevel.SECOND_1))

    assert await delay_store.list_all() == []


def test_error_code_declared() -> None:
    """T15：码值与状态（与 `deploy/registry/msg.toml` 的逐条对账由 `test_error_codes` 覆盖）。"""
    assert int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED) == 350022
    assert MsgStorageWatermarkExceededError().http_status == 507


def test_metrics_snapshot(tmp_path: Path) -> None:
    """T17：按 volume 覆盖写、拒绝按 reason 计数、分键有界。"""
    metrics = StorageMetrics(volume_key_limit=2)
    watermark = _watermark(tmp_path, _Usage([400]), metrics=metrics)
    watermark.refresh()
    watermark.refresh()

    snapshot = metrics.snapshot()
    key = str(tmp_path.absolute())
    assert cast("dict[str, object]", snapshot["msg_disk_used_ratio"]) == {key: 0.4}
    assert cast("dict[str, object]", snapshot["msg_storage_free_bytes"]) == {key: TOTAL - 400}

    metrics.record_rejection("watermark")
    metrics.record_rejection("watermark")
    metrics.record_rejection("sampling_failed")
    rejected = cast("dict[str, int]", metrics.snapshot()["msg_storage_rejected_total"])
    assert rejected[TOTAL_KEY] == 3
    assert rejected["watermark"] == 2
    assert rejected["sampling_failed"] == 1

    for volume in ("a", "b", "c"):
        metrics.record_reading(
            StorageReading(
                volume=volume,
                used_ratio=0.5,
                free_bytes=1,
                total_bytes=2,
                warn_ratio=0.7,
                reject_ratio=0.85,
            )
        )
    assert OTHER_KEY in cast("dict[str, object]", metrics.snapshot()["msg_disk_used_ratio"])


def test_metrics_ignore_unreadable_reading() -> None:
    """T17b：不可用读数不写指标（避免把故障读成「0% 占用」）。"""
    metrics = StorageMetrics()
    unreadable = StorageReading(
        volume="v",
        used_ratio=None,
        free_bytes=None,
        total_bytes=None,
        warn_ratio=0.7,
        reject_ratio=0.85,
        error="OSError",
    )

    metrics.record_reading(unreadable)

    assert metrics.snapshot()["msg_disk_used_ratio"] == {}


def test_logs_on_enter_and_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T18：进入 / 解除各一条；重复采样不重复记。"""
    recorder = _Recorder()
    monkeypatch.setattr(watermark_module, "_LOGGER", recorder)
    clock = _Clock()
    watermark = _watermark(tmp_path, _Usage([900, 900, 100, 100]), clock=clock)

    for _ in range(4):
        watermark.refresh()
        clock.advance(2_000)

    assert recorder.names() == ["msg_storage_watermark_enter", "msg_storage_watermark_release"]
    fields = recorder.events[0][1]
    assert fields["volume"] == str(tmp_path.absolute())
    assert fields["threshold"] == 0.85
    assert fields["used_ratio"] == 0.9


def test_unreadable_is_logged_once_and_recovers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """T18b：不可读只记一条；恢复采样后按比例重新判定（并可解除）。"""
    recorder = _Recorder()
    monkeypatch.setattr(watermark_module, "_LOGGER", recorder)
    clock = _Clock()
    failing = {"flag": True}

    def _usage(_path: Path) -> tuple[int, int, int]:
        if failing["flag"]:
            raise OSError(errno.EIO, "io error")
        return TOTAL, 100, TOTAL - 100

    watermark = _watermark(tmp_path, _usage, clock=clock)
    watermark.refresh()
    clock.advance(2_000)
    watermark.refresh()
    assert recorder.names() == ["msg_storage_watermark_unreadable"]
    assert watermark.rejected is True

    failing["flag"] = False
    clock.advance(2_000)
    watermark.refresh()
    assert watermark.rejected is False
    assert recorder.names() == ["msg_storage_watermark_unreadable", "msg_storage_watermark_release"]


def test_recovery_ledger_records_and_filters() -> None:
    """T21：台账登记 / 过滤（截断与段损坏都算降级；同分区覆盖）。"""
    ledger = RecoveryLedger()
    assert ledger.entries() == ()
    assert ledger.degraded() == ()

    ledger.record(
        topic_id=1,
        partition_no=0,
        report=RecoveryReport(truncated_from_bytes=100, truncated_bytes=100, segments=1),
    )
    ledger.record(topic_id=1, partition_no=1, report=RecoveryReport(corrupted_segments=(0,)))
    ledger.record(topic_id=2, partition_no=0, report=RecoveryReport(truncated_from_bytes=120, truncated_bytes=100))

    assert len(ledger) == 3
    degraded = ledger.degraded()
    assert [(item.topic_id, item.partition_no) for item in degraded] == [(1, 1), (2, 0)]
    assert degraded[1].truncated_bytes == 20


async def test_file_store_exposes_recovery_ledger(tmp_path: Path) -> None:
    """T22：文件实现的台账出口（启动期为空；首次访问分区后登记一条）。"""
    directory = PartitionDirectory(root=tmp_path)
    store = FileMessageStore(directory=directory, fsync_mode="os")

    assert store.recovery_ledger().entries() == ()

    directory.bind(tenant_id=1, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=7)
    assert await store.leo(topic_id=7, partition_no=0) == 0

    entries = store.recovery_ledger().entries()
    assert [(item.topic_id, item.partition_no) for item in entries] == [(7, 0)]
    assert store.recovery_ledger().degraded() == ()


async def test_delay_transfer_failure_logs_reason_with_window_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T19：转投失败日志带 `reason` / `errno`，同分区同原因按窗口抑制，记录不丢。"""
    recorder = _Recorder()
    monkeypatch.setattr(scheduler_module, "_LOGGER", recorder)
    delay_store = InMemoryDelayStore()
    messages = InMemoryMessageStore(segment_records=2)

    async def _boom(**_kwargs: object) -> None:
        raise OSError(errno.ENOSPC, "no space left on device")

    monkeypatch.setattr(messages, "append", _boom)
    clock = _Clock()
    now = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
    wheel = DelayWheel(now=lambda: now)
    scheduler = DelayScheduler(
        store=delay_store,
        messages=messages,
        wheel=wheel,
        now=lambda: now,
        clock=clock,
        failure_log_window_ms=1_000,
    )
    record = DelayRecord(
        log_id="l-1",
        message_id="m-1",
        tenant_id=1,
        topic_id=7,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=0,
        delay_level=DelayLevel.SECOND_1.value,
        key=None,
        payload=b"x",
        headers=ConcurrentStableDict(),
        trace=EMPTY_TRACE,
        published_at=now,
        deliver_at=now,
    )
    await delay_store.enqueue(record)
    wheel.place(record)

    await scheduler.run_once()
    await scheduler.run_once()

    assert recorder.names() == ["delay_deliver_failed"]
    assert recorder.events[0][1]["reason"] == "OSError"
    assert recorder.events[0][1]["errno"] == errno.ENOSPC
    assert len(await delay_store.list_all()) == 1, "转投失败必须保留记录（不丢）"

    clock.advance(1_500)
    await scheduler.run_once()
    assert recorder.names() == ["delay_deliver_failed", "delay_deliver_failed"]
    assert (await delay_store.list_all())[0].attempts == 3


def test_default_usage_reads_real_volume(tmp_path: Path) -> None:
    """T1b：缺省容量读数走 `shutil.disk_usage`（不注入 usage 时的真实路径）。"""
    watermark = StorageWatermark(root=tmp_path, warn_ratio=0.7, reject_ratio=0.85, ttl_ms=1_000)

    reading = watermark.reading()

    assert reading.unreadable is False
    assert reading.total_bytes is not None and reading.total_bytes > 0
    assert 0.0 <= cast("float", reading.used_ratio) <= 1.0


def test_message_scales_small_sizes(tmp_path: Path) -> None:
    """T13b：剩余空间只有几十字节时文案按 B 渲染（不写成 `0.0 GiB`）。"""
    watermark = _watermark(tmp_path, _Usage([1990], total=2_000), reject=0.99)
    watermark.reading()

    with pytest.raises(MsgStorageWatermarkExceededError) as excinfo:
        watermark.ensure_writable()

    assert "10 B" in (excinfo.value.message or "")
