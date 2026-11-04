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
from collections.abc import AsyncIterator, Callable, Iterator
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.context import reset_tenant_context, set_tenant_context
from bms_core.db.session import DbSession
from bms_core.db.tenant import TenantContext
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.models.base import Base
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant

TENANT_ID = 7001
"""测试租户主键（雪花）。"""

TENANT_CODE = "demo"
"""测试租户编码。"""

NAMESPACE = "msg"
"""可用命名空间（服务目录中已登记且启用的 `module_key`）。"""

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
    """内存库引擎（每用例一份；只建本模块两表）。

    Yields:
        AsyncEngine: 内存 SQLite 引擎。
    """
    created = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with created.begin() as connection:
        await connection.run_sync(Base.metadata.tables["msg_topic"].create)
        await connection.run_sync(Base.metadata.tables["msg_partition"].create)
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
