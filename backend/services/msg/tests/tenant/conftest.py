"""05-5 租户生命周期 / 清理用例夹具（内存库 + 真实进程内资源 + 临时文件根）。"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.events.base import EventEnvelope
from bms_core.models.base import Base
from bms_core.tenant.lifecycle import build_tenant_lifecycle_event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.offset import FileOffsetStore
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.tenant.ledger import DbTenantLifecycleLedger, SessionScope
from bms_msg.tenant.purge import reset_tenant_purge_metrics
from bms_msg.tenant.runtime import TenantLifecycleRuntime, reset_tenant_lifecycle_metrics

TENANT_ID = 7001
"""测试租户主键。"""


class RecordingAudit:
    """审计替身：记录事件；可切换为失败。"""

    def __init__(self) -> None:
        """初始化空记录。"""
        self.events: list[tuple[EventEnvelope, bool]] = []
        self.fail = False

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一条审计事件。

        Args:
            event: 审计事件。
            required: 是否强审计。

        Returns:
            bool: 恒 True（fail=True 时抛错）。

        Raises:
            RuntimeError: 模拟审计出口故障。
        """
        if self.fail:
            raise RuntimeError("audit down")
        self.events.append((event, required))
        return True


@pytest.fixture(autouse=True)
def _reset_metrics() -> Iterator[None]:
    """每个用例前后清零进程级生命周期 / 清扫计数。"""
    reset_tenant_lifecycle_metrics()
    reset_tenant_purge_metrics()
    yield
    reset_tenant_lifecycle_metrics()
    reset_tenant_purge_metrics()


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """内存库引擎（每用例一份；建本域四表）。

    Yields:
        AsyncEngine: 内存 SQLite 引擎。
    """
    created = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with created.begin() as connection:
        for table in ("msg_topic", "msg_partition", "msg_replay_task", "msg_tenant_lifecycle"):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
def session_scope(engine: AsyncEngine) -> SessionScope:
    """平台库短会话入口（测试用内存库）。

    Args:
        engine: 内存库引擎。

    Returns:
        SessionScope: 会话入口。
    """
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def _scope() -> AsyncGenerator[AsyncSession]:
        async with factory() as session:
            yield session

    return cast("SessionScope", _scope)


@pytest.fixture
def audit() -> RecordingAudit:
    """审计替身。

    Returns:
        RecordingAudit: 审计替身。
    """
    return RecordingAudit()


@pytest.fixture
def ledger(session_scope: SessionScope) -> DbTenantLifecycleLedger:
    """DB 生命周期账本。

    Args:
        session_scope: 会话入口。

    Returns:
        DbTenantLifecycleLedger: 账本。
    """
    return DbTenantLifecycleLedger(session_scope=session_scope)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """消息数据根。

    Args:
        tmp_path: pytest 临时目录。

    Returns:
        Path: 数据根。
    """
    return tmp_path


@pytest.fixture
def runtime(root: Path, ledger: DbTenantLifecycleLedger, audit: RecordingAudit) -> TenantLifecycleRuntime:
    """生命周期运行时（真实进程内资源 + 临时文件根）。

    Args:
        root: 数据根。
        ledger: 账本。
        audit: 审计替身。

    Returns:
        TenantLifecycleRuntime: 运行时。
    """
    directory = PartitionDirectory(root=root)
    return TenantLifecycleRuntime(
        ledger=ledger,
        audit_sink=audit,
        connections=ConnectionRegistry(),
        streams=StreamRegistry(),
        delay_store=InMemoryDelayStore(),
        delay_wheel=DelayWheel(),
        message_store=FileMessageStore(directory=directory),
        offset_store=FileOffsetStore(directory=directory, root=root, audit=audit),
        groups=InMemoryGroupStore(),
        dead_letters=InMemoryDeadLetterQueue(),
        delivery_state=DeliveryState(),
        waiter=DeliveryWaiter(),
        topic_cache=TopicCache(),
        retention_throttle=RetentionThrottle(),
        directory=directory,
    )


def lifecycle_event(
    event_type: str,
    *,
    tenant_id: int = TENANT_ID,
    version: int = 1,
    status: str = "active",
) -> EventEnvelope:
    """构造一条租户生命周期事件。

    Args:
        event_type: 事件类型。
        tenant_id: 租户主键。
        version: 版本戳。
        status: 租户状态。

    Returns:
        EventEnvelope: 事件信封。
    """
    return build_tenant_lifecycle_event(
        event_type=event_type,
        tenant_id=tenant_id,
        db_key="tenant_demo",
        status=status,
        version=version,
    )


def event_types(events: list[tuple[EventEnvelope, bool]]) -> ConcurrentStableList[str]:
    """取审计事件里的动作名（用例断言用）。

    Args:
        events: 审计记录。

    Returns:
        ConcurrentStableList[str]: 动作名。
    """
    return ConcurrentStableList(str(event.payload.get("action")) for event, _required in events)
