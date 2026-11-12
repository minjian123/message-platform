"""主题服务（msg 任务 01-1；Kiwi 2412）。

覆盖：创建（成功 / 重名 / 分区数越界 / 保留期越界 / 命名空间非法 / 主题名非法）、查询（成功 / 不存在）、
列表（分页 / 过滤 / 参数越界）、增分区（只增不减 / 等于即幂等 / 不存在）、`tenant_id` 取自上下文。
"""

from collections.abc import Callable
from typing import cast

import pytest
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from bms_msg.core.config import MAX_PARTITION_COUNT, MAX_RETENTION_MS
from bms_msg.errors import (
    MsgErrorCode,
    MsgNamespaceInvalidError,
    MsgPartitionCountInvalidError,
    MsgTopicExistsError,
    MsgTopicNotFoundError,
)
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant, enabled_namespaces

pytestmark = pytest.mark.kiwi_id(2412)

NAMESPACE = "msg"
"""可用命名空间（夹具上下文的服务目录已启用模块）。"""

TENANT_ID = 7001
"""测试租户主键（与夹具上下文同值）。"""

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器（夹具 `make_request` 的类型）。"""


def test_test_namespace_is_enabled() -> None:
    """测试命名空间确在服务目录已启用清单内（否则 350010 用例的基准不成立）。"""
    assert NAMESPACE in enabled_namespaces()


async def test_create_topic_with_partitions(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """创建成功：主题行与 N 条分区行同事务落库，`tenant_id` 取自上下文，leader 为实例标识。"""
    topic = await service.create_topic(tenant=current_tenant, request=make_request())
    assert (topic.tenant_id, topic.partition_count, topic.status) == (TENANT_ID, 3, "active")
    detail, partitions = await service.get_topic(tenant=current_tenant, topic_id=int(topic.id))
    assert detail.id == topic.id
    assert [partition.partition_no for partition in partitions] == [0, 1, 2]
    assert {partition.leader for partition in partitions} == {"msg-0"}


async def test_create_duplicate_topic_rejected(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """同租户 / 命名空间 / 名称重复：`350008`。"""
    await service.create_topic(tenant=current_tenant, request=make_request())
    with pytest.raises(MsgTopicExistsError) as excinfo:
        await service.create_topic(tenant=current_tenant, request=make_request())
    assert excinfo.value.code == int(MsgErrorCode.TOPIC_EXISTS)


@pytest.mark.parametrize("namespace", ["nope", "UPPER", "audit.log"])
async def test_create_rejects_invalid_namespace(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest, namespace: str
) -> None:
    """命名空间非法（未登记 / 形态不符）：`350010`，且不落主题行。

    长度与必填由契约层拦（`10001`），故此处只取「形态合法但未登记」与「形态不符」两类。
    """
    with pytest.raises(MsgNamespaceInvalidError) as excinfo:
        await service.create_topic(tenant=current_tenant, request=make_request(namespace=namespace))
    assert excinfo.value.code == int(MsgErrorCode.NAMESPACE_INVALID)
    rows, total = await service.list_topics(tenant=current_tenant, namespace=None, keyword=None, page=1, size=10)
    assert (len(rows), total) == (0, 0)


@pytest.mark.parametrize("partition_count", [0, -1, MAX_PARTITION_COUNT + 1])
async def test_create_rejects_invalid_partition_count(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest, partition_count: int
) -> None:
    """分区数越界（≤0 或超上限）：`350009`。"""
    with pytest.raises(MsgPartitionCountInvalidError) as excinfo:
        await service.create_topic(tenant=current_tenant, request=make_request(partition_count=partition_count))
    assert excinfo.value.code == int(MsgErrorCode.PARTITION_COUNT_INVALID)


@pytest.mark.parametrize("retention_ms", [0, -1, MAX_RETENTION_MS + 1])
async def test_create_rejects_invalid_retention(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest, retention_ms: int
) -> None:
    """保留期越界：同 `350009`（设计口径）。"""
    with pytest.raises(MsgPartitionCountInvalidError):
        await service.create_topic(tenant=current_tenant, request=make_request(retention_ms=retention_ms))


@pytest.mark.parametrize("name", ["主题中文", "space name", "a/b"])
async def test_create_rejects_invalid_topic_name(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest, name: str
) -> None:
    """主题名形态不符：参数错误 `10001`（属请求契约，不是业务码）。"""
    with pytest.raises(ParamError):
        await service.create_topic(tenant=current_tenant, request=make_request(name=name))


async def test_get_unknown_topic_returns_350001(service: TopicService, current_tenant: CurrentTenant) -> None:
    """主题不存在：`350001` / 404。"""
    with pytest.raises(MsgTopicNotFoundError) as excinfo:
        await service.get_topic(tenant=current_tenant, topic_id=123456)
    assert excinfo.value.code == int(MsgErrorCode.TOPIC_NOT_FOUND)


async def test_list_topics_paginates_and_filters(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """列表：分页切片、命名空间与关键词过滤。"""
    for index in range(3):
        await service.create_topic(
            tenant=current_tenant, request=make_request(name=f"audit.operation.{index}", partition_count=1)
        )
    await service.create_topic(tenant=current_tenant, request=make_request(name="notify.sent", partition_count=1))

    first_page, total = await service.list_topics(tenant=current_tenant, namespace=None, keyword=None, page=1, size=2)
    assert (len(first_page), total) == (2, 4)
    second_page, _ = await service.list_topics(tenant=current_tenant, namespace=None, keyword=None, page=2, size=2)
    assert len(second_page) == 2
    audit_rows, audit_total = await service.list_topics(
        tenant=current_tenant, namespace=NAMESPACE, keyword="audit", page=1, size=10
    )
    assert (len(audit_rows), audit_total) == (3, 3)


@pytest.mark.parametrize(("page", "size"), [(0, 10), (1, 0), (1, 101)])
async def test_list_rejects_invalid_pagination(
    service: TopicService, current_tenant: CurrentTenant, page: int, size: int
) -> None:
    """分页参数越界：`10001`（页码从 1 起、每页 1~100）。"""
    with pytest.raises(ParamError):
        await service.list_topics(tenant=current_tenant, namespace=None, keyword=None, page=page, size=size)


async def test_list_rejects_invalid_namespace_filter(service: TopicService, current_tenant: CurrentTenant) -> None:
    """命名空间过滤值非法：`350010`（fail-closed，避免静默空列表）。"""
    with pytest.raises(MsgNamespaceInvalidError):
        await service.list_topics(tenant=current_tenant, namespace="nope", keyword=None, page=1, size=10)


async def test_expand_partitions_appends_only(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """增分区：追加新分区、主题分区数更新、既有分区号不变。"""
    topic = await service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    updated, partitions = await service.expand_partitions(
        tenant=current_tenant, topic_id=int(topic.id), partition_count=5
    )
    assert (updated.partition_count, [p.partition_no for p in partitions]) == (5, [0, 1, 2, 3, 4])


async def test_expand_partitions_same_count_is_idempotent(
    service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest
) -> None:
    """目标分区数等于现状：不新增、不改主题（幂等）。"""
    topic = await service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    updated, partitions = await service.expand_partitions(
        tenant=current_tenant, topic_id=int(topic.id), partition_count=2
    )
    assert (updated.partition_count, len(partitions)) == (2, 2)


async def test_expand_partitions_rejects_shrink(
    service: TopicService,
    current_tenant: CurrentTenant,
    make_request: MakeRequest,
    engine: AsyncEngine,
) -> None:
    """减少分区：`350009`，且库内主题与分区不变（用新会话读数，回避回滚后同会话的过期对象）。"""
    topic = await service.create_topic(tenant=current_tenant, request=make_request(partition_count=3))
    topic_id = int(topic.id)
    with pytest.raises(MsgPartitionCountInvalidError):
        await service.expand_partitions(tenant=current_tenant, topic_id=topic_id, partition_count=2)
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as fresh:
        session = cast("DbSession", fresh)
        stored = await TopicRepository(session).get(topic_id)
        partitions = await PartitionRepository(session).list_by_topic(topic_id)
    assert stored is not None
    assert (stored.partition_count, len(partitions)) == (3, 3)


async def test_expand_unknown_topic_returns_350001(service: TopicService, current_tenant: CurrentTenant) -> None:
    """增分区时主题不存在：`350001`。"""
    with pytest.raises(MsgTopicNotFoundError):
        await service.expand_partitions(tenant=current_tenant, topic_id=999999, partition_count=2)


async def test_create_and_expand_write_assignments(
    db_session: AsyncSession,
    current_tenant: CurrentTenant,
    make_request: MakeRequest,
) -> None:
    """02-8：装配分配器后建主题 / 增分区写副本集与 leader；旧分区不重写。"""
    from bms_core.core.concurrent import ConcurrentStableFrozenList
    from bms_core.db.unit_of_work import DbUnitOfWork

    from bms_msg.services.partition_assignment import (
        PartitionAssigner,
        PartitionAssignmentConfig,
    )

    session = cast("DbSession", db_session)
    assigner = PartitionAssigner(
        PartitionAssignmentConfig(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2", "msg-3")),
            replication_factor=2,
            min_insync_replicas=1,
        )
    )
    service = TopicService(
        TopicRepository(session), PartitionRepository(session), DbUnitOfWork(session), assigner=assigner
    )
    topic = await service.create_topic(tenant=current_tenant, request=make_request(partition_count=2))
    _detail, partitions = await service.get_topic(tenant=current_tenant, topic_id=int(topic.id))
    first = [list(partition.replicas) for partition in partitions]
    assert all(len(replicas) == 2 for replicas in first)
    assert {partition.leader for partition in partitions} == {first[0][0], first[1][0]}

    _topic, expanded = await service.expand_partitions(tenant=current_tenant, topic_id=int(topic.id), partition_count=4)
    assert [partition.partition_no for partition in expanded] == [0, 1, 2, 3]
    assert [list(partition.replicas)[:1] for partition in expanded[:2]] == [[first[0][0]], [first[1][0]]]
    assert all(len(partition.replicas) == 2 for partition in expanded[2:])
