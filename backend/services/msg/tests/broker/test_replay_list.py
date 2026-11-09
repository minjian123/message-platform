"""重放任务列表服务（任务 06-6-1；Kiwi 2468）。

覆盖（详细设计 §6）：

- 服务层分页 / 排序 / `total`（创建时间倒序、`id` 倒序）；
- 过滤条件与租户维度；
- 参数校验分支（页码、每页条数、状态取值、创建时间区间）→ `10001`；
- 命名空间非法 → `350010`。

接口层已由 `tests/api/test_admin_replay_list.py` 覆盖（HTTP 契约、ACL 过滤、租户隔离）；这里直接调
服务层，是因为 `page` / `size` 的边界在契约层已被 `Query(ge/le)` 拦下，服务层分支只能直调才可达。
"""

from datetime import UTC, datetime, timedelta

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.replay import ReplayService
from bms_msg.errors import MsgNamespaceInvalidError
from bms_msg.models.topic import MsgTopic
from bms_msg.schemas.replay import ReplayRequest
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2468)

NAMESPACE = "msg"
"""可用命名空间。"""


def _request(topic: str = "audit.operation", **overrides: object) -> ReplayRequest:
    """构造建任务请求。

    Args:
        topic: 主题名。
        **overrides: 覆盖字段。

    Returns:
        ReplayRequest: 建任务请求。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "namespace": NAMESPACE,
            "topic": topic,
            "partitions": ConcurrentStableList([0]),
            "rate_per_second": 10,
            "confirm": True,
        }
    )
    payload.update(overrides)
    return ReplayRequest.model_validate(dict(payload.items()))


async def _create(service: ReplayService, tenant: CurrentTenant, *, topic: str = "audit.operation") -> int:
    """建一个重放任务并返回主键。

    Args:
        service: 重放服务。
        tenant: 当前租户。
        topic: 主题名。

    Returns:
        int: 任务主键。
    """
    task = await service.create(tenant=tenant, request=_request(topic))
    return int(task.id)


async def test_list_orders_by_created_at_desc(
    replay_service: ReplayService,
    current_tenant: CurrentTenant,
    topic: MsgTopic,
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
) -> None:
    """列表按创建时间倒序（同刻按主键倒序），total 为过滤后总数。"""
    del db_session, message_store
    first = await _create(replay_service, current_tenant)
    second = await _create(replay_service, current_tenant)
    third = await _create(replay_service, current_tenant)

    rows, total = await replay_service.list(tenant=current_tenant, page=1, size=2)

    assert [int(row.id) for row in rows] == [third, second]
    assert total == 3
    assert int(rows[0].topic_id) == int(topic.id)
    assert first not in [int(row.id) for row in rows]


async def test_list_filters(replay_service: ReplayService, current_tenant: CurrentTenant, topic: MsgTopic) -> None:
    """状态 / 命名空间 / 主题 / 创建时间区间过滤各自生效。"""
    del topic
    task_id = await _create(replay_service, current_tenant)

    by_status = await replay_service.list(tenant=current_tenant, page=1, size=20, status="pending")
    by_namespace = await replay_service.list(tenant=current_tenant, page=1, size=20, namespace=NAMESPACE)
    by_topic = await replay_service.list(tenant=current_tenant, page=1, size=20, topic="audit.operation")
    future = await replay_service.list(
        tenant=current_tenant, page=1, size=20, created_from=datetime.now(UTC) + timedelta(days=1)
    )

    assert [int(row.id) for row in by_status[0]] == [task_id]
    assert len(by_namespace[0]) == 1
    assert len(by_topic[0]) == 1
    assert future == (ConcurrentStableList(), 0)


async def test_list_rejects_bad_pagination(
    replay_service: ReplayService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """页码 / 每页条数越界 → `10001`。"""
    del topic
    with pytest.raises(ParamError):
        await replay_service.list(tenant=current_tenant, page=0, size=20)
    with pytest.raises(ParamError):
        await replay_service.list(tenant=current_tenant, page=1, size=101)


async def test_list_rejects_unknown_status(
    replay_service: ReplayService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """状态取值不在白名单 → `10001`（不静默当「不过滤」）。"""
    del topic
    with pytest.raises(ParamError):
        await replay_service.list(tenant=current_tenant, page=1, size=20, status="unknown")


async def test_list_rejects_reversed_created_range(
    replay_service: ReplayService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """创建时间区间颠倒 → `10001`。"""
    del topic
    now = datetime.now(UTC)
    with pytest.raises(ParamError):
        await replay_service.list(tenant=current_tenant, page=1, size=20, created_from=now, created_to=now)


async def test_list_rejects_unknown_namespace(
    replay_service: ReplayService, current_tenant: CurrentTenant, topic: MsgTopic
) -> None:
    """命名空间未启用 → `350010`。"""
    del topic
    with pytest.raises(MsgNamespaceInvalidError):
        await replay_service.list(tenant=current_tenant, page=1, size=20, namespace="unknown-ns")
