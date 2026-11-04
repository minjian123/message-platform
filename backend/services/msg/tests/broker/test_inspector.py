"""消费组只读投影（msg 任务 01-4；Kiwi 2416）。

覆盖：`list_groups` 的参数护栏（`page < 1`、`size` 超上限 → `ParamError`）与过滤分支——命名空间不符的组
记录不展示（跨命名空间串味）。正常列表 / 分页 / 孤儿主题跳过与详情位点拼装由接口层用例
（`api/test_admin_groups.py`）覆盖，本文件只补接口层 Query 护栏挡掉的服务层分支。
"""

from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.group import GROUP_STATE_STABLE, ConsumerGroup, InMemoryGroupStore
from bms_msg.broker.inspector import GroupInspector
from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.repositories.topic import TopicRepository
from bms_msg.services.topic import MAX_LIST_SIZE
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2416)

NAMESPACE = "msg"
"""可用命名空间。"""


async def test_list_rejects_invalid_page(inspector: GroupInspector, current_tenant: CurrentTenant) -> None:
    """页码非法（`< 1`）：`ParamError`（10001）——接口层 Query 已挡一层，服务层仍自查。"""
    with pytest.raises(ParamError):
        await inspector.list_groups(tenant=current_tenant, namespace=None, topic=None, page=0, size=10)


async def test_list_rejects_oversized_page_size(inspector: GroupInspector, current_tenant: CurrentTenant) -> None:
    """每页条数超上限：`ParamError`（10001）。"""
    with pytest.raises(ParamError):
        await inspector.list_groups(tenant=current_tenant, namespace=None, topic=None, page=1, size=MAX_LIST_SIZE + 1)


async def test_list_skips_group_of_other_namespace(
    inspector: GroupInspector,
    current_tenant: CurrentTenant,
    group_store: InMemoryGroupStore,
    db_session: AsyncSession,
) -> None:
    """命名空间过滤：其他命名空间的组不串进结果（组记录按 `topic_id` 反查命名空间后判定）。"""
    topics = TopicRepository(cast("DbSession", db_session))
    foreign = await topics.create(
        namespace="other",
        name="audit.foreign",
        partition_count=1,
        retention_ms=DEFAULT_RETENTION_MS,
    )
    await group_store.save(
        ConsumerGroup(
            tenant_id=int(current_tenant.tenant_id),
            topic_id=int(foreign.id),
            group="foreign",
            state=GROUP_STATE_STABLE,
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    rows, total = await inspector.list_groups(tenant=current_tenant, namespace=NAMESPACE, topic=None, page=1, size=10)
    assert (list(rows), total) == ([], 0)
