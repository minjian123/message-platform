"""msg 服务工程级测试夹具：内存库 + 租户上下文 + 服务 / 请求构造器（任务 01-1）。

口径（详细设计 §10「测试库」）：

- **内存 SQLite**（`sqlite+aiosqlite://` + `StaticPool`）：每用例一份、只建 `msg_topic` / `msg_partition`
  两表，用例互不依赖历史数据，也不触碰共享库与 `own_db` 白名单；
- 租户上下文由夹具按下发主键设置（仓储 `tenant_scoped` 据此注入 / 过滤），API 用例另有真实
  `TenantMiddleware` 覆盖同一上下文，验证「以解析结果为准」；
- 服务 / 请求构造器以夹具形式提供（`services/msg/tests` 不是包，跨文件 import 不可靠）。

> bms 侧通用「共享测试库准备集」已下沉 `bms_core.testing.shared_db`（本任务一并交付），
> 供后续跨服务 / 真库集成用例直接复用；本任务的平台库两表用内存库即可。
"""

import importlib
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.context import reset_tenant_context, set_tenant_context
from bms_core.db.session import DbSession
from bms_core.db.tenant import TenantContext
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from bms_core.events.base import EventEnvelope
from bms_core.models.base import Base
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from bms_msg.broker.coordinator import GroupCoordinator
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import DelayInspector, DelayService, InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.inspector import GroupInspector
from bms_msg.broker.log import InMemoryMessageStore
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner, ReplayService
from bms_msg.broker.retention import RetentionService
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.replay import ReplayTaskRepository
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.group import MemberRequest
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.replay_impact import ReplayImpactInspector
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant
from tests_support.clock import ManualClock

TENANT_ID = 7001
"""测试租户主键（雪花）。"""

TENANT_CODE = "demo"
"""测试租户编码。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录中已登记且启用的 `module_key`）。"""

GROUP = "audit"
"""测试消费组名（01-4 起消费须先加入该组）。"""

_TENANT_IDS: ConcurrentStableDict[str, int] = ConcurrentStableDict({"demo": TENANT_ID, "other": 8002})
"""租户编码 → 固定主键（`demo` 与夹具上下文同值；`other` 用于验证「以解析结果为准」）。"""

MakeRequest = Callable[..., TopicCreateRequest]
"""创建请求构造器类型（`make_request(name=..., partition_count=...)`）。"""


class FakeTenantSource:
    """租户源替身：两个启用租户（`demo` / `other`）+ 其他编码恒不存在，可切换停用。"""

    def __init__(self) -> None:
        """初始化（缺省启用）。"""
        self.suspended = False

    async def by_code(self, code: str) -> TenantContext:
        """按编码取上下文。

        Args:
            code: 租户编码（未登记编码恒不存在）。

        Returns:
            TenantContext: 租户上下文。

        Raises:
            TenantNotFoundError: 编码未登记。
            TenantSuspendedError: 替身被置为停用。
        """
        from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError

        tenant_id = _TENANT_IDS.get(code)
        if tenant_id is None:
            raise TenantNotFoundError("未知租户")
        if self.suspended:
            raise TenantSuspendedError(f"租户已停用：{code}")
        return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=tenant_id)

    async def by_domain(self, domain: str) -> TenantContext:
        """按子域名取上下文（本替身不支持域名来源）。

        Args:
            domain: 子域名。

        Returns:
            TenantContext: 永不返回。

        Raises:
            TenantNotFoundError: 恒抛。
        """
        from bms_core.core.exceptions import TenantNotFoundError

        raise TenantNotFoundError(f"未知租户域名：{domain}")

    async def by_id(self, tenant_id: str) -> TenantContext:
        """按主键取上下文。

        Args:
            tenant_id: 租户主键字符串。

        Returns:
            TenantContext: 租户上下文。

        Raises:
            TenantNotFoundError: 主键不在替身映射内。
        """
        from bms_core.core.exceptions import TenantNotFoundError

        for code, value in _TENANT_IDS.items():
            if str(value) == tenant_id:
                return TenantContext(code=code, db_key=f"tenant_{code}", name=code, tenant_id=value)
        raise TenantNotFoundError(f"未知租户：{tenant_id}")

    async def single_active(self) -> TenantContext | None:
        """唯一启用租户（返回 `demo`）。

        Returns:
            TenantContext | None: `demo` 租户上下文。
        """
        return TenantContext(code=TENANT_CODE, db_key=f"tenant_{TENANT_CODE}", name="演示租户", tenant_id=TENANT_ID)


@pytest.fixture(autouse=True)
def _service_catalog() -> None:
    """确保本产品服务行已登记（命名空间 allowlist 取自服务目录合并视图）。"""
    importlib.import_module("services.module_registry")


@pytest.fixture(autouse=True)
def _tenant_context() -> Iterator[None]:
    """设置当前租户上下文（仓储租户注入 / 过滤的输入）。

    Yields:
        None: 用例运行期。
    """
    token = set_tenant_context(
        TenantContext(code=TENANT_CODE, db_key=f"tenant_{TENANT_CODE}", name="演示租户", tenant_id=TENANT_ID)
    )
    try:
        yield
    finally:
        reset_tenant_context(token)


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """内存库引擎（每用例一份；只建本模块的元数据表）。

    Yields:
        AsyncEngine: 内存 SQLite 引擎。
    """
    created = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with created.begin() as connection:
        for table in (
            "msg_topic",
            "msg_partition",
            "msg_partition_migration",
            "msg_replay_task",
            "msg_tenant_lifecycle",
            "msg_quota",
            "msg_topic_acl",
            "msg_topic_acl_scope",
            "msg_topic_cleanup",
            "msg_dead_letter",
            "msg_consumer_group",
            "msg_group_member",
            "msg_overview_snapshot",
            "msg_ingress_counter",
        ):
            await connection.run_sync(Base.metadata.tables[table].create)
    try:
        yield created
    finally:
        await created.dispose()


@pytest.fixture
async def db_session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """请求级会话（绑定内存库）。

    Args:
        engine: 内存库引擎。

    Yields:
        AsyncSession: 异步会话。
    """
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
def tenant_source() -> FakeTenantSource:
    """租户源替身夹具。

    Returns:
        FakeTenantSource: 可切换停用状态的替身。
    """
    return FakeTenantSource()


@pytest.fixture
def current_tenant() -> CurrentTenant:
    """当前租户（含已启用命名空间快照）。

    Returns:
        CurrentTenant: 当前租户。
    """
    return CurrentTenant.from_context(
        TenantContext(code=TENANT_CODE, db_key=f"tenant_{TENANT_CODE}", name="演示租户", tenant_id=TENANT_ID)
    )


@pytest.fixture
def service(db_session: AsyncSession) -> TopicService:
    """主题服务（同会话仓储 + 工作单元）。

    Args:
        db_session: 请求级会话。

    Returns:
        TopicService: 主题服务。
    """
    session = cast("DbSession", db_session)
    return TopicService(TopicRepository(session), PartitionRepository(session), DbUnitOfWork(session))


@pytest.fixture
def make_request() -> MakeRequest:
    """创建请求构造器（缺省一个合法主题）。

    Returns:
        MakeRequest: `make_request(**overrides)`。
    """

    def _make(**overrides: object) -> TopicCreateRequest:
        """构造创建请求。

        Args:
            **overrides: 覆盖字段。

        Returns:
            TopicCreateRequest: 创建请求。
        """
        payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
            {
                "namespace": NAMESPACE,
                "name": "audit.operation",
                "partition_count": 3,
                "retention_ms": DEFAULT_RETENTION_MS,
            }
        )
        for key, value in overrides.items():
            payload.set(key, value)
        return TopicCreateRequest.model_validate(dict(payload.items()))

    return _make


@pytest.fixture
def clock() -> ManualClock:
    """可推进时钟夹具。

    Returns:
        ManualClock: 固定起点时钟。
    """
    return ManualClock()


@pytest.fixture
def advance(clock: ManualClock) -> Callable[[int], None]:
    """时钟推进器（用例里模拟可见性超时，不 sleep）。

    Args:
        clock: 可推进时钟。

    Returns:
        Callable[[int], None]: `advance(毫秒)`。
    """
    return clock.advance


@pytest.fixture
async def topic(service: TopicService, current_tenant: CurrentTenant, make_request: MakeRequest) -> MsgTopic:
    """建好一个两分区主题（数据面用例的前置元数据）。

    Args:
        service: 主题服务（同会话仓储）。
        current_tenant: 当前租户。
        make_request: 创建请求构造器。

    Returns:
        MsgTopic: 新建主题（`namespace=msg`、两分区）。
    """
    return await service.create_topic(
        tenant=current_tenant, request=make_request(name="audit.operation", partition_count=2)
    )


@pytest.fixture
def message_store() -> InMemoryMessageStore:
    """内存消息存储（每用例一份）。

    Returns:
        InMemoryMessageStore: 空存储。
    """
    return InMemoryMessageStore()


@pytest.fixture
def offset_store() -> InMemoryOffsetStore:
    """内存位点存储（每用例一份）。

    Returns:
        InMemoryOffsetStore: 空位点。
    """
    return InMemoryOffsetStore()


@pytest.fixture
def dead_letters() -> InMemoryDeadLetterQueue:
    """内存死信队列（每用例一份）。

    Returns:
        InMemoryDeadLetterQueue: 空队列。
    """
    return InMemoryDeadLetterQueue()


@pytest.fixture
def delivery_state() -> DeliveryState:
    """投递状态（每用例一份）。

    Returns:
        DeliveryState: 空状态。
    """
    return DeliveryState()


@pytest.fixture
def delivery_service(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    dead_letters: InMemoryDeadLetterQueue,
    delivery_state: DeliveryState,
    clock: ManualClock,
) -> DeliveryService:
    """投递服务（请求级仓储 + 进程级状态 + 可推进时钟）。

    Args:
        db_session: 请求级会话。
        message_store: 消息存储。
        offset_store: 位点存储。
        dead_letters: 死信队列。
        delivery_state: 投递状态。
        clock: 可推进时钟。

    Returns:
        DeliveryService: 投递服务。
    """
    session = cast("DbSession", db_session)
    return DeliveryService(
        store=message_store,
        offsets=offset_store,
        dead_letters=dead_letters,
        router=TopicRouter(TopicRepository(session)),
        state=delivery_state,
        now=clock,
    )


MakeMember = Callable[..., MemberRequest]
"""成员请求构造器类型（`make_member(consumer_id=..., topic=...)`）。"""


@pytest.fixture
def group_store() -> InMemoryGroupStore:
    """内存组存储（每用例一份）。

    Returns:
        InMemoryGroupStore: 空组存储。
    """
    return InMemoryGroupStore()


@pytest.fixture
def make_member() -> MakeMember:
    """成员请求构造器（缺省加入 `audit.operation` 组的 `c1`）。

    Returns:
        MakeMember: `make_member(**overrides)`。
    """

    def _make(**overrides: object) -> MemberRequest:
        """构造成员请求。

        Args:
            **overrides: 覆盖字段。

        Returns:
            MemberRequest: 成员请求。
        """
        payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
            {
                "group": GROUP,
                "namespace": NAMESPACE,
                "topic": "audit.operation",
                "consumer_id": "c1",
            }
        )
        for key, value in overrides.items():
            payload.set(key, value)
        return MemberRequest.model_validate(dict(payload.items()))

    return _make


@pytest.fixture
def coordinator(
    db_session: AsyncSession,
    group_store: InMemoryGroupStore,
    delivery_state: DeliveryState,
    clock: ManualClock,
) -> GroupCoordinator:
    """消费组协调器（请求级仓储 + 进程级组存储 / 投递状态 + 可推进时钟）。

    Args:
        db_session: 请求级会话。
        group_store: 组存储。
        delivery_state: 投递状态（撤销易主分区时清在途）。
        clock: 可推进时钟。

    Returns:
        GroupCoordinator: 协调器。
    """
    session = cast("DbSession", db_session)
    return GroupCoordinator(
        groups=group_store,
        router=TopicRouter(TopicRepository(session)),
        delivery=delivery_state,
        now=clock,
    )


@pytest.fixture
def inspector(
    db_session: AsyncSession,
    group_store: InMemoryGroupStore,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
) -> GroupInspector:
    """组只读投影（请求级主题仓储 + 进程级组 / 消息 / 位点存储）。

    Args:
        db_session: 请求级会话。
        group_store: 组存储。
        message_store: 消息存储（取 LEO）。
        offset_store: 位点存储（取已提交位点）。

    Returns:
        GroupInspector: 组只读投影。
    """
    session = cast("DbSession", db_session)
    topics = TopicRepository(session)
    return GroupInspector(
        groups=group_store,
        topics=topics,
        router=TopicRouter(topics),
        messages=message_store,
        offsets=offset_store,
    )


@pytest.fixture
def delay_store() -> InMemoryDelayStore:
    """内存延迟区（每用例一份；缺省软上限 10 万条）。

    Returns:
        InMemoryDelayStore: 空延迟区。
    """
    return InMemoryDelayStore()


@pytest.fixture
def delay_wheel(clock: ManualClock) -> DelayWheel:
    """时间轮（与夹具时钟同源：装填按剩余时间选层）。

    Args:
        clock: 可推进时钟。

    Returns:
        DelayWheel: 空时间轮。
    """
    return DelayWheel(now=clock)


@pytest.fixture
def delay_service(
    db_session: AsyncSession, delay_store: InMemoryDelayStore, delay_wheel: DelayWheel, clock: ManualClock
) -> DelayService:
    """延迟发布服务（请求级主题仓储 + 进程级延迟区 / 时间轮 + 可推进时钟）。

    Args:
        db_session: 请求级会话。
        delay_store: 延迟区。
        delay_wheel: 时间轮。
        clock: 可推进时钟。

    Returns:
        DelayService: 延迟发布服务。
    """
    session = cast("DbSession", db_session)
    return DelayService(store=delay_store, router=TopicRouter(TopicRepository(session)), wheel=delay_wheel, now=clock)


@pytest.fixture
def delay_inspector(db_session: AsyncSession, delay_store: InMemoryDelayStore, clock: ManualClock) -> DelayInspector:
    """延迟区只读投影（请求级主题仓储 + 进程级延迟区 + 可推进时钟）。

    Args:
        db_session: 请求级会话。
        delay_store: 延迟区。
        clock: 可推进时钟。

    Returns:
        DelayInspector: 只读投影。
    """
    session = cast("DbSession", db_session)
    return DelayInspector(store=delay_store, router=TopicRouter(TopicRepository(session)), now=clock)


@pytest.fixture
def delay_scheduler(
    delay_store: InMemoryDelayStore,
    message_store: InMemoryMessageStore,
    delay_wheel: DelayWheel,
    clock: ManualClock,
) -> DelayScheduler:
    """到期推进器（10ms 周期：生命周期用例不必等 1s；`run_once` 不受周期影响）。

    Args:
        delay_store: 延迟区。
        message_store: 消息存储。
        delay_wheel: 时间轮。
        clock: 可推进时钟。

    Returns:
        DelayScheduler: 未启动的推进器。
    """
    return DelayScheduler(
        store=delay_store,
        messages=message_store,
        wheel=delay_wheel,
        now=clock,
        interval_ms=10,
    )


class FakeAuditSink:
    """审计出口替身（满足 `AuditSink` 协议）：记录事件，可按次返回成功 / 失败。"""

    def __init__(self) -> None:
        """初始化（缺省成功、无事件）。"""
        self.events: ConcurrentStableList[EventEnvelope] = ConcurrentStableList()
        self.result = True

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录事件并返回当前设定的结果。

        Args:
            event: 审计事件信封。

        Returns:
            bool: `self.result`。
        """
        self.events.add(event)
        return self.result


class ExplodingAuditSink:
    """审计出口替身：发射即抛异常（验证记录级强审计写入失败即上抛）。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """恒抛异常。

        Args:
            event: 审计事件信封。

        Returns:
            bool: 不返回。

        Raises:
            RuntimeError: 恒抛。
        """
        del event
        raise RuntimeError("审计出口不可用")


@pytest.fixture
def audit_sink() -> FakeAuditSink:
    """审计出口替身（缺省成功）。

    Returns:
        FakeAuditSink: 空替身。
    """
    return FakeAuditSink()


@pytest.fixture
def retention_service(message_store: InMemoryMessageStore, clock: ManualClock) -> RetentionService:
    """保留期清理服务（进程级存储 + 可推进时钟）。

    Args:
        message_store: 消息存储。
        clock: 可推进时钟。

    Returns:
        RetentionService: 清理服务。
    """
    return RetentionService(store=message_store, now=clock)


@pytest.fixture
def replay_service(
    db_session: AsyncSession,
    message_store: InMemoryMessageStore,
    offset_store: InMemoryOffsetStore,
    group_store: InMemoryGroupStore,
    clock: ManualClock,
) -> ReplayService:
    """重放任务服务（请求级平台库会话 + 进程级消息 / 位点 / 组存储 + 可推进时钟）。

    Args:
        db_session: 请求级会话。
        message_store: 消息存储。
        offset_store: 位点存储（影响基线取已提交位点）。
        group_store: 组存储（影响基线解析受影响组）。
        clock: 可推进时钟。

    Returns:
        ReplayService: 重放服务。
    """
    session = cast("DbSession", db_session)
    return ReplayService(
        tasks=ReplayTaskRepository(session),
        router=TopicRouter(TopicRepository(session)),
        store=message_store,
        uow=DbUnitOfWork(session),
        impact=ReplayImpactInspector(groups=group_store, offsets=offset_store, messages=message_store, now=clock),
        now=clock,
    )


@pytest.fixture
def replay_auditor(audit_sink: FakeAuditSink) -> ReplayAuditor:
    """重放审计记录器（出口为替身）。

    Args:
        audit_sink: 审计出口替身。

    Returns:
        ReplayAuditor: 审计记录器。
    """
    return ReplayAuditor(sink=audit_sink)


@pytest.fixture
def exploding_auditor() -> ReplayAuditor:
    """重放审计记录器（出口恒抛异常：验证强审计写入失败即上抛，业务失败）。

    Returns:
        ReplayAuditor: 审计记录器。
    """
    return ReplayAuditor(sink=ExplodingAuditSink())


@pytest.fixture
def replay_uow(db_session: AsyncSession) -> Callable[[], AbstractAsyncContextManager[UnitOfWork]]:
    """执行器用工作单元工厂（复用内存会话，不依赖请求上下文）。

    Args:
        db_session: 请求级会话。

    Returns:
        Callable[[], AbstractAsyncContextManager[UnitOfWork]]: 工作单元工厂。
    """
    session = cast("DbSession", db_session)

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """产出绑定内存会话的工作单元。

        Yields:
            UnitOfWork: 工作单元。
        """
        yield DbUnitOfWork(session)

    return _uow


@pytest.fixture
def replay_runner(
    message_store: InMemoryMessageStore,
    replay_uow: Callable[[], AbstractAsyncContextManager[UnitOfWork]],
    replay_auditor: ReplayAuditor,
    clock: ManualClock,
) -> ReplayRunner:
    """重放执行器（10ms 周期：生命周期用例不必等 200ms；`run_once` 不受周期影响）。

    Args:
        message_store: 消息存储。
        replay_uow: 工作单元工厂。
        replay_auditor: 审计记录器。
        clock: 可推进时钟。

    Returns:
        ReplayRunner: 未启动的执行器。
    """
    return ReplayRunner(
        messages=message_store,
        uow_factory=replay_uow,
        auditor=replay_auditor,
        now=clock,
        interval_ms=10,
    )
