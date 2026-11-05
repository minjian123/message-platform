"""连接管理基元（任务 03-4；Kiwi 2424）：派生连接键 / 登记表 / 半开清扫 / 名额 / 退避。

覆盖点对应详细设计 §4：
- `connection_key()` 三种归一（无地址 / 端口 0 / 无租户）；
- `ConnectionRegistry` 新建 / 刷新 / stale 复活、注销幂等、`count` 只数有效连接、两段式清扫；
- `open_stream` 超限 `350021`、`close_stream` 幂等、未登记连接返回 None（防御分支）；
- `ConnectionTuning` 缺省值与 `max(2 × keep_alive, heartbeat_timeout)` 两侧；
- `next_backoff_ms` 指数递增 / 封顶 / 抖动上下界 / 入参越界。
"""

from __future__ import annotations

import pytest
from bms_core.core.config import Settings

from bms_msg.broker.connection import (
    ANONYMOUS_TENANT,
    ConnectionRecord,
    ConnectionRegistry,
    connection_key,
    connection_tuning_from_settings,
    default_connection_tuning,
    next_backoff_ms,
)
from bms_msg.broker.transport import ConnectionMeta, StreamRegistry
from bms_msg.core.config import (
    CONNECTION_EVICT_SCAN_LIMIT,
    CONNECTION_HEARTBEAT_INTERVAL_MS,
    CONNECTION_HEARTBEAT_TIMEOUT_MS,
    CONNECTION_RETRY_AFTER_MS,
    MAX_CONNECTIONS,
    MAX_STREAMS_PER_CONNECTION,
    RECONNECT_BASE_MS,
    RECONNECT_JITTER_RATIO,
    RECONNECT_MAX_MS,
)
from bms_msg.errors import MsgErrorCode, MsgStreamLimitExceededError

pytestmark = pytest.mark.kiwi_id(2424)

_META = ConnectionMeta(http_version="HTTP/2", scheme="http", is_tls=False, client_host="10.0.0.1", client_port=1234)


def _touch(registry: ConnectionRegistry, *, key: str = "demo|10.0.0.1:1234", now: float = 100.0) -> ConnectionRecord:
    """按固定参数登记一条连接（用例共用）。

    Args:
        registry: 连接登记表。
        key: 派生连接键。
        now: 当前时刻（monotonic 秒）。

    Returns:
        ConnectionRecord: 刷新后的快照。
    """
    return registry.touch(key=key, meta=_META, tenant="demo", now=now, wall=1_700_000_000.0, heartbeat=False)


def test_connection_key_normalizes_missing_parts() -> None:
    """连接键 = `tenant|host:port`；无地址归一 `-`、无租户归一 `anonymous`。"""
    assert connection_key(_META, tenant="demo") == "demo|10.0.0.1:1234"
    assert connection_key(_META, tenant=None) == f"{ANONYMOUS_TENANT}|10.0.0.1:1234"
    assert connection_key(_META, tenant="") == f"{ANONYMOUS_TENANT}|10.0.0.1:1234"
    no_address = ConnectionMeta(http_version="HTTP/1.1", scheme="http", is_tls=False, client_host="", client_port=0)
    assert connection_key(no_address, tenant="demo") == "demo|-:0"


def test_registry_touch_creates_and_refreshes() -> None:
    """首见建条（记 opened 与计数）、再见只刷新时间与请求数、心跳单独记 `last_heartbeat_wall`。"""
    registry = ConnectionRegistry()
    created = _touch(registry)
    assert created.requests == 1
    assert created.streams == 0
    assert created.last_heartbeat_wall is None
    assert registry.count() == 1

    refreshed = registry.touch(
        key=created.key, meta=_META, tenant="demo", now=130.0, wall=1_700_000_030.0, heartbeat=True
    )
    assert refreshed.requests == 2
    assert refreshed.last_seen == 130.0
    assert refreshed.last_heartbeat_wall == 1_700_000_030.0
    assert refreshed.opened_at == created.opened_at, "存活时长起点不得被刷新"


def test_registry_revives_stale_record() -> None:
    """stale 条目被同键请求复活：名额归零、标记清除、存活时长重新计时。"""
    registry = ConnectionRegistry()
    _touch(registry, now=100.0)
    registry.open_stream("demo|10.0.0.1:1234", limit=2)
    evicted = registry.evict(now=200.0, limit=8, idle_timeout_ms=10_000)
    assert [record.key for record in evicted] == ["demo|10.0.0.1:1234"]
    assert registry.count() == 0

    revived = _touch(registry, now=201.0)
    assert revived.stale is False
    assert revived.streams == 0, "复活即重置名额（旧连接上的流已不属于新连接）"
    assert revived.opened_at == 201.0
    assert registry.count() == 1


def test_registry_evict_is_two_stage() -> None:
    """两段式清扫：超 idle 标 stale 并回收名额，超 2×idle 才删条目；未过期不动。"""
    registry = ConnectionRegistry()
    _touch(registry, now=100.0)
    assert registry.evict(now=105.0, limit=8, idle_timeout_ms=10_000) == [], "未超阈值不动"
    assert registry.count() == 1

    registry.evict(now=120.0, limit=8, idle_timeout_ms=10_000)
    stale = registry.get("demo|10.0.0.1:1234")
    assert stale is not None and stale.stale is True
    assert len(registry.records()) == 1, "第一段只标 stale，条目还在（等复活）"

    registry.evict(now=141.0, limit=8, idle_timeout_ms=10_000)
    assert registry.get("demo|10.0.0.1:1234") is None, "第二段才真正删除"


def test_registry_evict_respects_scan_limit() -> None:
    """惰性清扫单次只查 `limit` 条（请求驱动，不扫全表）。"""
    registry = ConnectionRegistry()
    for index in range(3):
        _touch(registry, key=f"demo|10.0.0.{index}:1", now=100.0)
    evicted = registry.evict(now=10_000.0, limit=2, idle_timeout_ms=10_000)
    assert len(evicted) == 2
    assert registry.count() == 1


def test_registry_release_is_idempotent() -> None:
    """注销幂等：首返回快照，再返回 None；未登记同键也无异常。"""
    registry = ConnectionRegistry()
    _touch(registry)
    released = registry.release("demo|10.0.0.1:1234", reason="client_closed")
    assert released is not None and released.key == "demo|10.0.0.1:1234"
    assert registry.release("demo|10.0.0.1:1234", reason="client_closed") is None


def test_registry_stream_slots() -> None:
    """长驻流名额：达上限抛 `350021`；释放幂等；未登记连接不记账（防御分支）。"""
    registry = ConnectionRegistry()
    assert registry.open_stream("missing|1.1.1.1:1", limit=1) is None
    registry.close_stream("missing|1.1.1.1:1")

    _touch(registry)
    registry.open_stream("demo|10.0.0.1:1234", limit=2)
    registry.open_stream("demo|10.0.0.1:1234", limit=2)
    with pytest.raises(MsgStreamLimitExceededError) as excinfo:
        registry.open_stream("demo|10.0.0.1:1234", limit=2)
    assert excinfo.value.code == int(MsgErrorCode.STREAM_LIMIT_EXCEEDED)

    registry.close_stream("demo|10.0.0.1:1234")
    record = registry.get("demo|10.0.0.1:1234")
    assert record is not None and record.streams == 1
    registry.close_stream("demo|10.0.0.1:1234")
    registry.close_stream("demo|10.0.0.1:1234")
    record = registry.get("demo|10.0.0.1:1234")
    assert record is not None and record.streams == 0, "多余释放不得把名额压成负数"


def test_registry_drain_flags_are_idempotent() -> None:
    """drain 记账：进程级只记一次；每连接只下发一次（防日志刷屏）。"""
    registry = ConnectionRegistry()
    _touch(registry)
    assert registry.note_drain() is True
    assert registry.note_drain() is False
    assert registry.mark_drain_signaled("demo|10.0.0.1:1234") is True
    assert registry.mark_drain_signaled("demo|10.0.0.1:1234") is False
    assert registry.mark_drain_signaled("missing|1.1.1.1:1") is False


def test_records_snapshot_contains_all_entries() -> None:
    """`records()` 给全部条目（含 stale），供日志与用例断言。"""
    registry = ConnectionRegistry()
    _touch(registry, key="demo|10.0.0.1:1", now=100.0)
    _touch(registry, key="demo|10.0.0.2:1", now=100.0)
    registry.evict(now=10_000.0, limit=8, idle_timeout_ms=10_000)
    assert len(registry.records()) == 2
    assert registry.count() == 0


def test_default_tuning_matches_constants() -> None:
    """缺省参数与模块常量同值（口径只有一处）。"""
    tuning = default_connection_tuning()
    assert tuning.heartbeat_interval_ms == CONNECTION_HEARTBEAT_INTERVAL_MS
    assert tuning.heartbeat_timeout_ms == CONNECTION_HEARTBEAT_TIMEOUT_MS
    assert tuning.idle_timeout_ms == max(2 * CONNECTION_HEARTBEAT_INTERVAL_MS, CONNECTION_HEARTBEAT_TIMEOUT_MS)
    assert tuning.max_connections == MAX_CONNECTIONS
    assert tuning.max_streams_per_connection == MAX_STREAMS_PER_CONNECTION
    assert tuning.evict_scan_limit == CONNECTION_EVICT_SCAN_LIMIT
    assert tuning.retry_after_ms == CONNECTION_RETRY_AFTER_MS


def test_tuning_idle_follows_server_keep_alive() -> None:
    """空闲阈值 = `max(2 × keep_alive_timeout, heartbeat_timeout)`：两支各测一次。"""
    settings = Settings()
    settings.server.keep_alive_timeout_seconds = 5
    assert connection_tuning_from_settings(settings).idle_timeout_ms == CONNECTION_HEARTBEAT_TIMEOUT_MS

    settings.server.keep_alive_timeout_seconds = 60
    assert connection_tuning_from_settings(settings).idle_timeout_ms == 120_000


def test_next_backoff_grows_then_caps() -> None:
    """退避指数增长并在上限封顶（抖动注入中点，便于断言确定值）。"""
    middle = lambda: 0.5  # noqa: E731 - 用例内联随机源，表达式即定义
    assert next_backoff_ms(0, rand=middle) == RECONNECT_BASE_MS
    assert next_backoff_ms(1, rand=middle) == RECONNECT_BASE_MS * 2
    assert next_backoff_ms(3, rand=middle) == RECONNECT_BASE_MS * 8
    assert next_backoff_ms(30, rand=middle) == RECONNECT_MAX_MS
    assert next_backoff_ms(999, rand=middle) == RECONNECT_MAX_MS, "指数过大按上限截断，不溢出"


def test_next_backoff_jitter_bounds() -> None:
    """抖动上下界：`rand=0` 取下界、`rand≈1` 取上界，封顶后仍保留抖动（防脉冲）。"""
    low = next_backoff_ms(0, rand=lambda: 0.0)
    high = next_backoff_ms(0, rand=lambda: 0.999)
    assert low == int(RECONNECT_BASE_MS * (1 - RECONNECT_JITTER_RATIO))
    assert high == int(RECONNECT_BASE_MS * (1 + RECONNECT_JITTER_RATIO))
    assert low != high, "同一尝试次数必须给出不同等待，否则会形成重连脉冲"

    capped_low = next_backoff_ms(20, rand=lambda: 0.0)
    capped_high = next_backoff_ms(20, rand=lambda: 0.999)
    assert capped_low < capped_high
    assert capped_high <= RECONNECT_MAX_MS * (1 + RECONNECT_JITTER_RATIO) + 1


def test_next_backoff_handles_negative_attempt() -> None:
    """`attempt < 0` 按 0 处理（脚本误传不至于给出更短的等待）。"""
    assert next_backoff_ms(-5, rand=lambda: 0.5) == RECONNECT_BASE_MS


def test_stream_session_carries_connection_key() -> None:
    """流登记带上所属连接键（03-4 给 `StreamSession` 补的字段，供连接维度聚合）。"""
    registry = StreamRegistry()
    registry.open(
        namespace="msg",
        topic="audit.operation",
        group="audit",
        consumer_id="c1",
        partition=0,
        opened_at=1.0,
        connection_key="demo|10.0.0.1:1234",
    )
    sessions = registry.sessions()
    assert len(sessions) == 1
    assert sessions[0].connection_key == "demo|10.0.0.1:1234"
