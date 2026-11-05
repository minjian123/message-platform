"""主题元数据缓存（任务 01-7 详细设计 §3；Kiwi 2426）。

覆盖：

- **快照字段**：`resolve()` 返回的 `TopicSnapshot` 与库内主题逐字段一致（不再返回 ORM 实例）；
- **命中不查库**：同一键第二次解析不再调用仓储（用计数仓储断言，而不是看日志）；
- **未命中回填 + 精确失效**：改库后仍读到旧快照（无 TTL），`invalidate` 后下一次解析读到新值；
- **停用**：失效后按新状态判 `350001`（错误码与判定顺序不变）；
- **跨租户隔离**：同 namespace / name、不同租户的键互不可见；
- **装配约束**：只给 `cache` 不给 `tenant_id` 直接 `ValueError`（键里没有租户即越权）。
"""

from collections.abc import Iterator
from typing import cast

import pytest
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.router import TopicRouter
from bms_msg.broker.topic_cache import TopicCache, TopicSnapshot
from bms_msg.errors import MsgErrorCode, MsgTopicNotFoundError
from bms_msg.models.topic import STATUS_DISABLED, MsgTopic
from bms_msg.repositories.topic import TopicRepository

pytestmark = pytest.mark.kiwi_id(2426)

NAMESPACE = "msg"
"""可用命名空间（与 `topic` 夹具同值）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（与 `topic` 夹具同值）。"""

TENANT_ID = 7001
"""用例租户主键（与 `conftest.TENANT_ID` 同值）。"""

OTHER_TENANT_ID = 7999
"""另一个租户主键（只用于验证缓存键隔离，不落库）。"""


class _CountingTopics(TopicRepository):
    """统计 `get_by_identity` 调用次数的主题仓储（验证「命中不查库」）。"""

    def __init__(self, session: DbSession, counter: list[int]) -> None:
        """初始化。

        Args:
            session: 会话。
            counter: 单元素计数表（跨调用累计）。
        """
        super().__init__(session)
        self._counter = counter

    async def get_by_identity(self, *, namespace: str, name: str) -> MsgTopic | None:
        """按唯一键取主题，并累加调用次数。

        Args:
            namespace: 命名空间。
            name: 主题名。

        Returns:
            MsgTopic | None: 主题；不存在返回 None。
        """
        self._counter[0] += 1
        return await super().get_by_identity(namespace=namespace, name=name)


@pytest.fixture
def counter() -> list[int]:
    """调用计数表（每用例归零）。

    Returns:
        list[int]: 单元素计数表。
    """
    return [0]


@pytest.fixture
def test_cache() -> TopicCache:
    """每用例一份空缓存（不跨用例泄漏条目）。

    Returns:
        TopicCache: 空缓存。
    """
    return TopicCache()


@pytest.fixture
def counting(db_session: AsyncSession, counter: list[int]) -> Iterator[_CountingTopics]:
    """计数仓储。

    Args:
        db_session: 内存库会话。
        counter: 调用计数表。

    Yields:
        _CountingTopics: 计数仓储。
    """
    yield _CountingTopics(cast("DbSession", db_session), counter)


async def test_resolve_returns_snapshot_of_db_row(topic: MsgTopic, db_session: AsyncSession) -> None:
    """解析结果是与库内主题逐字段一致的快照（不是 ORM 实例）。"""
    router = TopicRouter(TopicRepository(cast("DbSession", db_session)))
    resolved = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)

    assert isinstance(resolved, TopicSnapshot)
    assert (resolved.id, resolved.tenant_id, resolved.namespace, resolved.name) == (
        int(topic.id),
        int(topic.tenant_id),
        topic.namespace,
        topic.name,
    )
    assert (resolved.partition_count, resolved.retention_ms, resolved.retention_bytes) == (
        int(topic.partition_count),
        int(topic.retention_ms),
        int(topic.retention_bytes),
    )
    assert (resolved.cleanup_policy, resolved.status) == (topic.cleanup_policy, topic.status)
    assert resolved.loaded_at_ms > 0


async def test_hit_skips_repository(
    counting: _CountingTopics, topic: MsgTopic, counter: list[int], test_cache: TopicCache
) -> None:
    """同一键第二次解析命中缓存，不再查库。"""
    del topic
    router = TopicRouter(counting, cache=test_cache, tenant_id=TENANT_ID)
    first = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    second = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)

    assert counter[0] == 1
    assert first is second
    assert len(test_cache) == 1


async def test_invalidate_reloads_new_value(
    counting: _CountingTopics, topic: MsgTopic, counter: list[int], test_cache: TopicCache, db_session: AsyncSession
) -> None:
    """改库后仍读到旧快照（无 TTL），精确失效后下一次解析读到新值。"""
    router = TopicRouter(counting, cache=test_cache, tenant_id=TENANT_ID)
    snapshot = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    original = int(topic.partition_count)
    assert snapshot.partition_count == original

    await counting.update(int(topic.id), partition_count=3)
    await db_session.flush()

    stale = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    assert stale.partition_count == original

    test_cache.invalidate(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME)
    fresh = await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    assert fresh.partition_count == 3
    assert counter[0] == 2


async def test_disabled_takes_effect_after_invalidate(
    counting: _CountingTopics, topic: MsgTopic, test_cache: TopicCache, db_session: AsyncSession
) -> None:
    """停用经失效后下一请求即判 `350001`（错误码与判定顺序不变）。"""
    router = TopicRouter(counting, cache=test_cache, tenant_id=TENANT_ID)
    await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)

    await counting.update(int(topic.id), status=STATUS_DISABLED)
    await db_session.flush()
    # 停用端点归 06 域（06-2），这里直接失效；服务侧失效收口由 `expand_partitions` 用例覆盖。
    test_cache.invalidate(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME)

    with pytest.raises(MsgTopicNotFoundError) as excinfo:
        await router.resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    assert excinfo.value.code == MsgErrorCode.TOPIC_NOT_FOUND
    assert len(test_cache) == 0


def test_cache_requires_tenant_id(db_session: AsyncSession) -> None:
    """启用缓存但缺 `tenant_id`：直接拒绝（键里没有租户即跨租户取主题）。"""
    topics = TopicRepository(cast("DbSession", db_session))
    with pytest.raises(ValueError):
        TopicRouter(topics, cache=TopicCache())


async def test_cache_keys_are_tenant_scoped(topic: MsgTopic, test_cache: TopicCache) -> None:
    """同 namespace / name、不同租户的键互不可见。"""
    test_cache.put(TopicSnapshot.from_topic(topic))

    assert test_cache.get(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME) is not None
    assert test_cache.get(tenant_id=OTHER_TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME) is None
    assert len(test_cache) == 1

    test_cache.invalidate(tenant_id=OTHER_TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME)
    assert len(test_cache) == 1
    test_cache.invalidate(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME)
    assert len(test_cache) == 0
