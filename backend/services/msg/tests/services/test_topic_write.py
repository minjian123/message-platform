"""主题写操作服务（msg 任务 06-2-1；Kiwi 2465）。

覆盖：删主题受理（状态机 / 台账幂等 / 列表与计数排除 `deleting`）、清理进度查询、改保留期（部分更新 /
越界 / 状态拒绝）、扩分区状态守卫、未装配台账仓储的装配缺陷、`disabled` 状态拒绝。
"""

from collections.abc import Callable
from typing import cast

import pytest
from bms_core.core.exceptions import ConfigError
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.topic_cache import TopicCache
from bms_msg.core.config import MAX_RETENTION_BYTES
from bms_msg.errors import (
    MsgErrorCode,
    MsgPartitionCountInvalidError,
    MsgTopicNotFoundError,
    MsgTopicStateInvalidError,
)
from bms_msg.models.topic import STATUS_ACTIVE, STATUS_DELETING, STATUS_DISABLED
from bms_msg.models.topic_cleanup import CLEANUP_PENDING
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.repositories.topic_cleanup import TopicCleanupRepository
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2465)

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器（conftest `make_request` 的类型）。"""


@pytest.fixture
def write_service(db_session: AsyncSession) -> TopicService:
    """带台账仓储的主题服务（删主题 / 查进度需要）。

    Args:
        db_session: 请求级会话。

    Returns:
        TopicService: 主题服务。
    """
    session = cast("DbSession", db_session)
    return TopicService(
        TopicRepository(session),
        PartitionRepository(session),
        _uow_of(session),
        cache=TopicCache(),
        cleanups=TopicCleanupRepository(session),
    )


def _uow_of(session: DbSession) -> UnitOfWork:
    """构造绑定会话的工作单元。

    Args:
        session: 会话。

    Returns:
        UnitOfWork: 工作单元实例。
    """
    return DbUnitOfWork(session)


async def test_begin_delete_marks_deleting_and_creates_ledger(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """删主题受理：主题置 `deleting`、台账 `pending`、名称与命名空间留快照。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    updated, ledger = await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    assert (updated.status, ledger.status) == (STATUS_DELETING, CLEANUP_PENDING)
    assert (ledger.namespace, ledger.topic, int(ledger.topic_id)) == ("msg", "audit.operation", int(topic.id))
    assert (ledger.done_steps, ledger.attempts) == ([], 0)


async def test_begin_delete_is_idempotent(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """重复受理：复用同一台账行（不产生第二行、不重置已完成步骤）。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    _topic, first = await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    _topic, second = await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    assert first.id == second.id


async def test_begin_delete_unknown_topic_returns_350001(
    write_service: TopicService, current_tenant: CurrentTenant
) -> None:
    """删不存在的主题：`350001`。"""
    with pytest.raises(MsgTopicNotFoundError):
        await write_service.begin_delete(tenant=current_tenant, topic_id=999999)


async def test_begin_delete_requires_ledger_repository(
    current_tenant: CurrentTenant, db_session: AsyncSession, make_request: MakeRequest
) -> None:
    """未装配台账仓储：`ConfigError`（装配缺陷可见，不静默跳过）。"""
    session = cast("DbSession", db_session)
    service = TopicService(TopicRepository(session), PartitionRepository(session), _uow_of(session))
    topic = await service.create_topic(tenant=current_tenant, request=make_request())
    with pytest.raises(ConfigError):
        await service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    with pytest.raises(ConfigError):
        await service.get_cleanup(topic_id=int(topic.id))


async def test_deleting_topic_hidden_from_list_and_count(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """`deleting` 主题不进列表、不进计数（配额立刻释放，允许同名重建）。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    rows, total = await write_service.list_topics(tenant=current_tenant, namespace=None, keyword=None, page=1, size=10)
    assert (len(rows), total) == (0, 0)


async def test_get_cleanup_returns_ledger(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """清理进度：受理后可查；未受理过的主题返回 None。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    assert await write_service.get_cleanup(topic_id=int(topic.id)) is None
    await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    ledger = await write_service.get_cleanup(topic_id=int(topic.id))
    assert ledger is not None
    assert ledger.status == CLEANUP_PENDING


async def test_update_retention_partial_fields(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """改保留期：只给 `retention_ms` 时容量上限不变，且返回变更前值。"""
    topic = await write_service.create_topic(
        tenant=current_tenant, request=make_request(retention_ms=60_000, retention_bytes=1024)
    )
    updated, before = await write_service.update_retention(
        tenant=current_tenant, topic_id=int(topic.id), retention_ms=120_000, retention_bytes=None
    )
    assert (updated.retention_ms, updated.retention_bytes) == (120_000, 1024)
    assert (before.get("retention_ms"), before.get("retention_bytes")) == (60_000, 1024)


async def test_update_retention_bytes_only(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """改容量上限：只给 `retention_bytes` 时保留期不变。"""
    topic = await write_service.create_topic(
        tenant=current_tenant, request=make_request(retention_ms=60_000, retention_bytes=0)
    )
    updated, _before = await write_service.update_retention(
        tenant=current_tenant, topic_id=int(topic.id), retention_ms=None, retention_bytes=2048
    )
    assert (updated.retention_ms, updated.retention_bytes) == (60_000, 2048)


@pytest.mark.parametrize(
    ("retention_ms", "retention_bytes"),
    [(0, None), (None, -1), (None, MAX_RETENTION_BYTES + 1)],
)
async def test_update_retention_rejects_out_of_range(
    write_service: TopicService,
    current_tenant: CurrentTenant,
    make_request: MakeRequest,
    retention_ms: int | None,
    retention_bytes: int | None,
) -> None:
    """保留期 / 容量越界：`350009`（与建主题同码）。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    with pytest.raises(MsgPartitionCountInvalidError):
        await write_service.update_retention(
            tenant=current_tenant,
            topic_id=int(topic.id),
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
        )


async def test_update_retention_unknown_topic(write_service: TopicService, current_tenant: CurrentTenant) -> None:
    """改保留期时主题不存在：`350001`。"""
    with pytest.raises(MsgTopicNotFoundError):
        await write_service.update_retention(
            tenant=current_tenant, topic_id=999999, retention_ms=60_000, retention_bytes=None
        )


async def test_update_retention_rejected_when_deleting(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """`deleting` 状态下改保留期：`350028`。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request())
    await write_service.begin_delete(tenant=current_tenant, topic_id=int(topic.id))
    with pytest.raises(MsgTopicStateInvalidError) as excinfo:
        await write_service.update_retention(
            tenant=current_tenant, topic_id=int(topic.id), retention_ms=60_000, retention_bytes=None
        )
    assert excinfo.value.code == int(MsgErrorCode.TOPIC_STATE_INVALID)


async def test_expand_rejected_when_disabled(
    write_service: TopicService,
    current_tenant: CurrentTenant,
    make_request: MakeRequest,
    db_session: AsyncSession,
) -> None:
    """`disabled` 状态下扩分区：`350028`（停用主题不接受扩容）。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    session = cast("DbSession", db_session)
    # 必须提交：服务方法在 `begin()` 前会 `rollback()`（清掉端点鉴权读留下的隐式事务），
    # 未提交的改动会被这次回滚一并丢弃。
    stored = await TopicRepository(session).update(int(topic.id), status=STATUS_DISABLED)
    assert stored is not None
    await session.commit()
    with pytest.raises(MsgTopicStateInvalidError):
        await write_service.expand_partitions(tenant=current_tenant, topic_id=int(topic.id), partition_count=4)


async def test_active_topic_still_writable(
    write_service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest, db_session: AsyncSession
) -> None:
    """`active` 是唯一可写状态（把状态改回 active 后扩容恢复）。"""
    topic = await write_service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    session = cast("DbSession", db_session)
    await TopicRepository(session).update(int(topic.id), status=STATUS_ACTIVE)
    updated, partitions = await write_service.expand_partitions(
        tenant=current_tenant, topic_id=int(topic.id), partition_count=3
    )
    assert (updated.partition_count, len(partitions)) == (3, 3)
