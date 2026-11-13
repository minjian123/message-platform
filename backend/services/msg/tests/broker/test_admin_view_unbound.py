"""管理面无数据目录读语义（06-1-4）：新建主题未写入时返回 0 / 空视图，不 500。"""

from pathlib import Path
from typing import cast

import pytest
from bms_core.db.session import DbSession
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.broker.admin_view import PartitionInspector
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.tenant.context import CurrentTenant

pytestmark = pytest.mark.kiwi_id(2493)
"""本文件 1 例共用 Kiwi 2493（06-1-4 管理面错误映射与数据口径）。"""


async def test_describe_unbound_topic_returns_zero(
    db_session: AsyncSession,
    current_tenant: CurrentTenant,
    tmp_path: Path,
) -> None:
    """主题已建但本地无目录：分区详情返回 LEO=0 / ISR 空，不抛 `PartitionNotBoundError`。"""
    session = cast("DbSession", db_session)
    topics = TopicRepository(session)
    await topics.create(
        namespace="msg",
        name="audit.unbound",
        partition_count=2,
        retention_ms=DEFAULT_RETENTION_MS,
    )
    inspector = PartitionInspector(
        router=TopicRouter(topics),
        messages=FileMessageStore(directory=PartitionDirectory(root=tmp_path), fsync_mode="os"),
        offsets=InMemoryOffsetStore(),
        groups=InMemoryGroupStore(),
        partitions=PartitionRepository(session),
        replication=None,
    )

    detail = await inspector.describe(
        tenant=current_tenant,
        namespace="msg",
        topic="audit.unbound",
        partition=None,
        group=None,
    )

    assert detail.partition_count == 2
    assert [item.leo for item in detail.items] == [0, 0]
    assert all(item.isr == [] for item in detail.items)
