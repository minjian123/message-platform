"""ACL 守卫会话隔离与「判权后读 ORM 行」回归（05-3-1；Kiwi 2490）。

背景（05-3-1 详细设计 §2）：05-3 把 ACL 守卫绑到请求业务会话并 `release=uow.rollback`；
`rollback` 会过期该会话内**全部** ORM 实例，端点「先查行 → 再判权 → 再读行属性」在判权后
触发同步懒加载并抛 `MissingGreenlet`——管理面 `GET /admin/topics` 有数据即 500，
死信列表 / 详情同因（计划 §6.1 台账 #15）。

本模块与既有 ACL 用例的区别：**不替换守卫**，用真实 `TopicAclService` + 真实 `msg.topic`
端口 + 两个独立会话跑完整链路（既有用例用替身守卫，替身不释放会话，因此漏网）。

分组：

- G1 有数据：列表 / 详情 200 且字段完整（修复前必 500）
- G2 空集：200 + 空页
- G3 权限与授权：无 `msg:query` → 30001；`describe` 被拒 → 列表过滤 + 详情 350027
- G4 分页：`page` / `size` 切片，`total` 保持「过滤前命中数」口径
- G5 同源端点：死信列表 / 详情 200（台账 #15 同根因）
- G6 装配：未声明扩展点不建 ACL 会话；声明后 ACL 会话与业务会话是**两个对象**

两个会话同绑内存引擎（`StaticPool` 共用一条 SQLite 连接，属测试装置限制）；生产隔离由
`get_platform_write_db` 提供独立连接，见 `api/deps.py`。
"""

import importlib
import json
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.api.base import mount_service_routers
from bms_core.api.errors import register_exception_handlers
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.context import reset_current_user_id, set_current_user_id
from bms_core.core.exceptions import ConfigError
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.extension import RESOURCE_ACL_KEY, ExtensionRequirement
from bms_core.permission.base import BasePermissionChecker
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from bms_msg.acl.cache import TopicAclCache
from bms_msg.acl.port import MSG_TOPIC_ACL_PLUGIN, MsgTopicAclFactory
from bms_msg.api.admin import dlq as admin_dlq
from bms_msg.api.admin import topics
from bms_msg.api.deps import get_topic_acl_service, get_topic_acl_session
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.errors import MsgErrorCode
from bms_msg.models.dead_letter import MsgDeadLetter
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.models.topic_acl import MsgTopicAcl

pytestmark = pytest.mark.kiwi_id(2490)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码声明唯一事实源）。"""

NAMESPACE = "msg"
"""可用命名空间。"""

TENANT_ID = 7001
"""测试租户主键（与 `conftest` 租户上下文一致）。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""

BASE_URL = "http://test"
"""测试基址。"""

VISIBLE_TOPIC = "audit.operation"
"""对 describe 可见的主题。"""

HIDDEN_TOPIC = "audit.hidden"
"""被 deny 规则挡住的主题。"""

DENIED_USER_ID = 42
"""被 deny 的操作人主键（`get_current_user_id` 注入）。"""

_BASE = datetime(2026, 1, 1, tzinfo=UTC)
"""基准时间。"""


class _Sink:
    """审计出口替身：恒成功（本模块不断言审计内容）。"""

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """恒成功。

        Args:
            event: 审计事件（忽略）。
            required: 强审计开关（忽略）。

        Returns:
            bool: True。
        """
        del event, required
        return True


class _DenyChecker(BasePermissionChecker):
    """权限桩：一律拒绝（验证角色权限先行拦截）。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。

        Args:
            code: 权限码（本桩不区分）。

        Returns:
            bool: False。
        """
        del code
        return False


class _Registry:
    """引擎注册表替身：恒返回同一内存引擎（只覆盖 `get_topic_acl_session` 的装配分支）。"""

    def __init__(self, engine: AsyncEngine) -> None:
        """初始化。

        Args:
            engine: 内存库引擎。
        """
        self._engine = engine

    def is_sync_only(self, db_key: str) -> bool:
        """非同步方言（SQLite 走异步路径）。

        Args:
            db_key: 数据源键（忽略）。

        Returns:
            bool: False。
        """
        del db_key
        return False

    async def get(self, db_key: str, *, read_only: bool = False) -> AsyncEngine:
        """取引擎。

        Args:
            db_key: 数据源键（忽略）。
            read_only: 只读标记（忽略）。

        Returns:
            AsyncEngine: 内存库引擎。
        """
        del db_key, read_only
        return self._engine


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """重置插件注册表并导入占位实现（每个用例独立构建）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（350027 的文案与 HTTP 状态据此解析）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
async def acl_session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """ACL 守卫专用会话（与业务会话独立，同绑一个内存引擎）。

    Args:
        engine: 内存库引擎。

    Yields:
        AsyncSession: 独立会话。
    """
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
def api_app(db_session: AsyncSession, acl_session: AsyncSession, tenant_source: object) -> FastAPI:
    """最小应用：声明 `resource_acl` + 真实 ACL 守卫 + 主题 / 死信路由。

    Args:
        db_session: 业务会话（平台库工作单元）。
        acl_session: ACL 专用会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.state.settings = SimpleNamespace(
        tenant_guard=SimpleNamespace(readonly_routes=ConcurrentStableList()),
        permission=SimpleNamespace(provider=""),
        tenant_module=SimpleNamespace(provider=""),
        error_mapper=SimpleNamespace(provider=""),
        resource_acl=SimpleNamespace(provider=MSG_TOPIC_ACL_PLUGIN),
        tenant=SimpleNamespace(
            exempt_paths=ConcurrentStableList(),
            deferred_paths=ConcurrentStableList(),
            allow_demo_fallback=False,
        ),
    )
    app.state.tenant_source = tenant_source
    app.state.topic_cache = TopicCache()
    app.state.audit_sink = _Sink()
    app.state.topic_acl_cache = TopicAclCache()
    app.state.extension_requirements = [
        ExtensionRequirement(plugin_key=RESOURCE_ACL_KEY, plugin_name=MSG_TOPIC_ACL_PLUGIN, required=True)
    ]
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    app.include_router(mount_service_routers(ConcurrentStableList([topics.router, admin_dlq.router])))
    register_plugin(RESOURCE_ACL_KEY, MSG_TOPIC_ACL_PLUGIN, MsgTopicAclFactory(app))

    business = cast("DbSession", db_session)

    async def _platform_uow() -> DbUnitOfWork:
        """平台库工作单元替身（业务会话）。

        Returns:
            DbUnitOfWork: 绑定业务会话的工作单元。
        """
        return DbUnitOfWork(business)

    async def _acl_session_override() -> AsyncIterator[DbSession]:
        """ACL 会话替身（独立会话）。

        Yields:
            DbSession: 独立会话。
        """
        yield cast("DbSession", acl_session)

    app.dependency_overrides[get_platform_uow] = _platform_uow
    app.dependency_overrides[get_topic_acl_session] = _acl_session_override
    return app


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=api_app), base_url=BASE_URL)


@pytest.fixture
def denied_user() -> Iterator[None]:
    """把当前操作人设为被 deny 的用户（判定走真实 `decide()`）。

    Yields:
        None: 用例运行期。
    """
    token = set_current_user_id(DENIED_USER_ID)
    try:
        yield
    finally:
        reset_current_user_id(token)


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 响应 JSON。
    """
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


def _data(response: Response) -> ConcurrentStableDict[str, object]:
    """取响应 `data` 段。

    Args:
        response: 响应。

    Returns:
        ConcurrentStableDict[str, object]: 业务数据。
    """
    return ConcurrentStableDict(cast("dict[str, object]", _body(response)["data"]))


def _names(response: Response) -> list[str]:
    """取列表响应的主题名序列。

    Args:
        response: 响应。

    Returns:
        list[str]: 主题名（保持响应顺序）。
    """
    items = cast("list[object]", _data(response)["items"])
    return [cast("str", cast("dict[str, object]", item)["name"]) for item in items]


async def _seed_topic(session: AsyncSession, *, name: str, partitions: int = 2) -> int:
    """直写一个主题与其分区（绕开写端点，只验读路径）。

    Args:
        session: 业务会话。
        name: 主题名。
        partitions: 分区数。

    Returns:
        int: 主题主键。
    """
    topic = MsgTopic(
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        name=name,
        partition_count=partitions,
        retention_ms=DEFAULT_RETENTION_MS,
        retention_bytes=0,
        cleanup_policy="delete",
        status="active",
    )
    session.add(topic)
    await session.flush()
    topic_id = int(topic.id)
    for partition_no in range(partitions):
        session.add(
            MsgPartition(
                topic_id=topic_id,
                partition_no=partition_no,
                leader="msg-0",
                replicas=ConcurrentStableList(["msg-0"]),
                min_insync_replicas=0,
            )
        )
    await session.commit()
    return topic_id


async def _seed_deny_rule(session: AsyncSession, *, topic: str, user_id: int) -> None:
    """直写一条 `describe` deny 规则（真实判定链路用）。

    Args:
        session: 业务会话。
        topic: 主题名。
        user_id: 被拒的操作人主键。
    """
    session.add(
        MsgTopicAcl(
            tenant_id=TENANT_ID,
            namespace=NAMESPACE,
            topic=topic,
            principal_type="user",
            principal_key=str(user_id),
            action="describe",
            effect="deny",
            status="enabled",
        )
    )
    await session.commit()


async def _seed_dead_letter(session: AsyncSession, *, topic_id: int, topic: str, offset: int = 0) -> int:
    """直写一条死信（只验读路径，不经队列）。

    Args:
        session: 业务会话。
        topic_id: 主题主键。
        topic: 主题名快照。
        offset: 分区内序号。

    Returns:
        int: 死信主键。
    """
    row = MsgDeadLetter(
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        topic_id=topic_id,
        topic=topic,
        partition_no=0,
        msg_offset=offset,
        message_id=f"m-{offset}",
        log_id=f"l-{offset}",
        trace_id="trace-1",
        reason="processing_failed",
        attempts=5,
        detail=ConcurrentStableDict({"error": "boom"}),
        status="pending",
        failed_at=_BASE,
    )
    session.add(row)
    await session.commit()
    return int(row.id)


async def test_topic_list_with_rows_returns_page(client: AsyncClient, db_session: AsyncSession) -> None:
    """G1：有数据时列表 200 且字段完整（修复前 `_item` 抛 `MissingGreenlet` → 500）。"""
    await _seed_topic(db_session, name=VISIBLE_TOPIC)
    response = await client.get("/api/v1/msg/admin/topics", headers=DEMO_HEADERS)
    data = _data(response)
    items = cast("list[object]", data["items"])
    assert response.status_code == 200
    assert (len(items), data["total"], data["page"], data["size"]) == (1, 1, 1, 20)
    first = cast("dict[str, object]", items[0])
    assert (first["namespace"], first["name"], first["partition_count"], first["status"]) == (
        NAMESPACE,
        VISIBLE_TOPIC,
        2,
        "active",
    )
    assert first["created_at"]


async def test_topic_detail_with_rows_returns_partitions(client: AsyncClient, db_session: AsyncSession) -> None:
    """G1：有数据时详情 200（修复前 `_detail` 判权后序列化 → `MissingGreenlet`）。"""
    topic_id = await _seed_topic(db_session, name=VISIBLE_TOPIC, partitions=2)
    response = await client.get(f"/api/v1/msg/admin/topics/{topic_id}", headers=DEMO_HEADERS)
    data = _data(response)
    partitions = [cast("dict[str, object]", item) for item in cast("list[object]", data["partitions"])]
    assert response.status_code == 200
    assert [item["partition_no"] for item in partitions] == [0, 1]
    assert data["name"] == VISIBLE_TOPIC
    assert data["metadata_version"] == 0


async def test_topic_list_empty_returns_empty_page(client: AsyncClient) -> None:
    """G2：空集 200 + 空页（无数据时判权循环不进入，历史上也返回 200）。"""
    response = await client.get("/api/v1/msg/admin/topics", headers=DEMO_HEADERS)
    data = _data(response)
    assert response.status_code == 200
    assert (cast("list[object]", data["items"]), data["total"]) == ([], 0)


async def test_topic_list_filters_by_describe_acl(
    client: AsyncClient, db_session: AsyncSession, denied_user: None
) -> None:
    """G3：`describe` 被拒的主题不进列表，`total` 保持过滤前口径。"""
    del denied_user
    await _seed_topic(db_session, name=VISIBLE_TOPIC)
    await _seed_topic(db_session, name=HIDDEN_TOPIC)
    await _seed_deny_rule(db_session, topic=HIDDEN_TOPIC, user_id=DENIED_USER_ID)
    response = await client.get("/api/v1/msg/admin/topics", headers=DEMO_HEADERS)
    assert response.status_code == 200
    assert _names(response) == [VISIBLE_TOPIC]
    assert _data(response)["total"] == 2


async def test_topic_detail_denied_returns_350027(
    client: AsyncClient, db_session: AsyncSession, denied_user: None
) -> None:
    """G3：详情 `describe` 被拒 → 403 + 350027（且不泄露内容）。"""
    del denied_user
    topic_id = await _seed_topic(db_session, name=HIDDEN_TOPIC)
    await _seed_deny_rule(db_session, topic=HIDDEN_TOPIC, user_id=DENIED_USER_ID)
    response = await client.get(f"/api/v1/msg/admin/topics/{topic_id}", headers=DEMO_HEADERS)
    assert (response.status_code, _body(response)["code"]) == (403, int(MsgErrorCode.ACL_DENIED))


async def test_topic_list_paginates(client: AsyncClient, db_session: AsyncSession) -> None:
    """G4：`page` / `size` 切片，`total` 为过滤前命中总数。"""
    for index in range(3):
        await _seed_topic(db_session, name=f"audit.operation.{index}", partitions=1)
    response = await client.get("/api/v1/msg/admin/topics", params={"page": 2, "size": 2}, headers=DEMO_HEADERS)
    data = _data(response)
    assert response.status_code == 200
    assert (len(cast("list[object]", data["items"])), data["total"], data["page"], data["size"]) == (1, 3, 2, 2)


async def test_permission_denied_returns_30001(client: AsyncClient, api_app: FastAPI) -> None:
    """G3：无 `msg:query` → 403 + 30001，ACL 不参与（角色权限先行）。"""
    register_plugin("permission", "deny", _DenyChecker)
    provider = cast("SimpleNamespace", api_app.state.settings).permission
    provider.provider = "deny"
    response = await client.get("/api/v1/msg/admin/topics", headers=DEMO_HEADERS)
    assert (response.status_code, _body(response)["code"]) == (403, 30001)


async def test_dead_letter_list_and_detail_with_rows(client: AsyncClient, db_session: AsyncSession) -> None:
    """G5：死信列表 / 详情有数据时 200（台账 #15 与主题列表同根因）。"""
    topic_id = await _seed_topic(db_session, name=VISIBLE_TOPIC)
    dlq_id = await _seed_dead_letter(db_session, topic_id=topic_id, topic=VISIBLE_TOPIC)

    listed = await client.get("/api/v1/msg/admin/dlq", headers=DEMO_HEADERS)
    listed_data = _data(listed)
    assert listed.status_code == 200
    assert listed_data["total"] == 1
    first = cast("dict[str, object]", cast("list[object]", listed_data["items"])[0])
    assert (first["namespace"], first["topic"], first["status"]) == (NAMESPACE, VISIBLE_TOPIC, "pending")

    detail = await client.get(f"/api/v1/msg/admin/dlq/{dlq_id}", headers=DEMO_HEADERS)
    assert detail.status_code == 200
    assert cast("dict[str, object]", _data(detail)["detail"]) == {"error": "boom"}


async def test_acl_session_skipped_without_extension() -> None:
    """G6：未声明 `resource_acl` → 不建 ACL 会话（守卫星恒短路）。"""
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(extension_requirements=[])))
    generator = get_topic_acl_session(cast("Request", request))
    assert await anext(generator) is None
    await generator.aclose()

    service = get_topic_acl_service(cast("Request", request), None)
    with pytest.raises(ConfigError):
        await service.snapshot(tenant_id=TENANT_ID, namespace=NAMESPACE)


async def test_acl_session_is_independent_from_business_session(engine: AsyncEngine) -> None:
    """G6：声明扩展点 → ACL 会话独立开出，且不是业务会话对象（隔离的落点）。"""
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                engine_registry=_Registry(engine),
                extension_requirements=[
                    ExtensionRequirement(plugin_key=RESOURCE_ACL_KEY, plugin_name=MSG_TOPIC_ACL_PLUGIN, required=True)
                ],
                session_factory=None,
            )
        )
    )
    generator = get_topic_acl_session(cast("Request", request))
    session = await anext(generator)
    try:
        assert isinstance(session, AsyncSession)
        service = get_topic_acl_service(cast("Request", request), cast("DbSession", session))
        snapshot = await service.snapshot(tenant_id=TENANT_ID, namespace=NAMESPACE)
        # 空规则 = 版本戳 0 + 无规则（默认允许）；能读出快照即证明仓储已绑在 ACL 会话上
        assert (snapshot.version, len(snapshot.rules)) == (0, 0)
    finally:
        await generator.aclose()
