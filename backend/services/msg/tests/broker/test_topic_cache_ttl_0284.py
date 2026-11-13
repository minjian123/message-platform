"""主题缓存 TTL（02-8-4；Kiwi 2497）：跨实例最长 5 秒收敛，本实例写操作仍立即失效。"""

import pytest

from bms_msg.broker.topic_cache import TopicCache, TopicSnapshot

pytestmark = pytest.mark.kiwi_id(2497)


class _Clock:
    """可手动推进的 epoch 毫秒时钟。"""

    def __init__(self) -> None:
        self.value = 1_000_000

    def __call__(self) -> int:
        return self.value


def _snapshot(*, loaded_at_ms: int = 1_000_000) -> TopicSnapshot:
    """构造一个主题快照。"""
    return TopicSnapshot(
        id=1,
        tenant_id=7001,
        namespace="msg",
        name="audit.operation",
        partition_count=2,
        retention_ms=86_400_000,
        retention_bytes=0,
        cleanup_policy="delete",
        status="active",
        loaded_at_ms=loaded_at_ms,
    )


def test_cache_expires_after_ttl() -> None:
    """TTL 内命中；到期视为未命中并删除条目。"""
    clock = _Clock()
    cache = TopicCache(ttl_ms=5_000, clock=clock)
    cache.put(_snapshot())
    assert cache.get(tenant_id=7001, namespace="msg", name="audit.operation") is not None

    clock.value += 5_000
    assert cache.get(tenant_id=7001, namespace="msg", name="audit.operation") is None
    assert len(cache) == 0


def test_invalidate_is_immediate() -> None:
    """写操作主动失效立即生效（不等 TTL）。"""
    cache = TopicCache(ttl_ms=5_000)
    cache.put(_snapshot())
    cache.invalidate(tenant_id=7001, namespace="msg", name="audit.operation")
    assert cache.get(tenant_id=7001, namespace="msg", name="audit.operation") is None


def test_put_refreshes_loaded_at() -> None:
    """重新回填后 TTL 从新时刻重新计算。"""
    clock = _Clock()
    cache = TopicCache(ttl_ms=5_000, clock=clock)
    cache.put(_snapshot(loaded_at_ms=clock.value))
    clock.value += 4_000
    cache.put(_snapshot(loaded_at_ms=clock.value))
    clock.value += 4_000
    assert cache.get(tenant_id=7001, namespace="msg", name="audit.operation") is not None
