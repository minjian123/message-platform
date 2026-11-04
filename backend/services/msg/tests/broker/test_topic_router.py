"""主题路由（msg 任务 01-2；Kiwi 2414）。

覆盖：按当前租户 + 命名空间 + 名称解析（存在 / 不存在 / 未启用）、分区号范围校验、
发布分区选择（同 key 恒同分区、复用 `core/routing.py::select_partition`）。
"""

from typing import cast

import pytest
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode, MsgPartitionCountInvalidError, MsgTopicNotFoundError
from bms_msg.models.topic import STATUS_DISABLED, MsgTopic
from bms_msg.repositories.topic import TopicRepository

pytestmark = pytest.mark.kiwi_id(2414)

NAMESPACE = "msg"
"""可用命名空间（服务目录已启用模块）。"""

TOPIC_NAME = "audit.operation"
"""用例主题名（与 `topic` 夹具同值）。"""


def _router(db_session: AsyncSession) -> TopicRouter:
    """构造请求级路由。

    Args:
        db_session: 当前会话。

    Returns:
        TopicRouter: 主题路由。
    """
    return TopicRouter(TopicRepository(cast("DbSession", db_session)))


async def test_resolve_returns_active_topic(topic: MsgTopic, db_session: AsyncSession) -> None:
    """解析命中：返回库内主题（主键与分区数一致）。"""
    resolved = await _router(db_session).resolve(namespace=NAMESPACE, name=TOPIC_NAME)
    assert (resolved.id, resolved.partition_count) == (topic.id, 2)


async def test_resolve_missing_topic_raises(topic: MsgTopic, db_session: AsyncSession) -> None:
    """主题不存在：`350001`。"""
    del topic
    with pytest.raises(MsgTopicNotFoundError) as excinfo:
        await _router(db_session).resolve(namespace=NAMESPACE, name="audit.missing")
    assert excinfo.value.code == MsgErrorCode.TOPIC_NOT_FOUND


async def test_resolve_disabled_topic_raises(topic: MsgTopic, db_session: AsyncSession) -> None:
    """主题停用：按不可用处理（同 `350001`，不区分存在性）。"""
    topics = TopicRepository(cast("DbSession", db_session))
    await topics.update(int(topic.id), status=STATUS_DISABLED)
    with pytest.raises(MsgTopicNotFoundError):
        await _router(db_session).resolve(namespace=NAMESPACE, name=TOPIC_NAME)


async def test_partition_accepts_in_range(topic: MsgTopic, db_session: AsyncSession) -> None:
    """分区号在 `0 ~ count-1` 内：原样返回。"""
    router = _router(db_session)
    assert (router.partition(topic, 0), router.partition(topic, 1)) == (0, 1)


@pytest.mark.parametrize("partition_no", [-1, 2])
async def test_partition_rejects_out_of_range(topic: MsgTopic, db_session: AsyncSession, partition_no: int) -> None:
    """分区号越界（负数 / 等于分区数）：`350009`。"""
    with pytest.raises(MsgPartitionCountInvalidError) as excinfo:
        _router(db_session).partition(topic, partition_no)
    assert excinfo.value.code == MsgErrorCode.PARTITION_COUNT_INVALID


async def test_route_is_stable_for_same_key(topic: MsgTopic, db_session: AsyncSession) -> None:
    """同 key 恒落同分区，且落在合法范围内。"""
    router = _router(db_session)
    selected = {router.route(topic, "order-1001") for _ in range(10)}
    assert len(selected) == 1
    assert 0 <= selected.pop() < int(topic.partition_count)


async def test_route_distributes_distinct_keys(topic: MsgTopic, db_session: AsyncSession) -> None:
    """不同 key 可分布到两个分区（不是恒定返回 0）。"""
    router = _router(db_session)
    selected = {router.route(topic, f"order-{index}") for index in range(20)}
    assert selected == {0, 1}
