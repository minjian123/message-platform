"""测试辅助：构造「最小可运行应用」——真实模块路由 + 租户中间件 + 统一错误渲染 + 内存库会话替身。

为什么放在 `backend/tests_support/`：03-1 的三个用例文件（根路径入口 / 长驻流 / 真 h2c）都要同一份装配，
而 `services/msg/tests` 不是包（跨文件 import 不可靠，见 `tests/conftest.py` 开头）。`backend/tests_support`
在 `pythonpath` 的 `.` 下可导入，且不属覆盖率统计包（`coverage_packages = ["bms_msg", "services", "ops"]`）。

装配口径与 `tests/api/test_data_messages.py` 的 `api_app` 一致，差别只在：
- 额外挂根路径聚合（`api/frame/routes.py`，`/msg/*`）；
- 额外注入 `stream_registry` / `uow_factory`（长驻流所需）与 `stream_tuning` 覆盖点。

**不启 lifespan、不连真库**：本辅助只构造应用对象，供 `ASGITransport` 或真实 hypercorn 驱动。
"""

import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from bms_core.api.base import mount_service_routers
from bms_core.api.errors import register_exception_handlers
from bms_core.api.middleware import TenantMiddleware
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from bms_msg.api.admin import topics
from bms_msg.api.data import groups as data_groups
from bms_msg.api.data import messages
from bms_msg.api.frame import connection as frame_connection
from bms_msg.api.frame import routes as frame_routes
from bms_msg.api.frame.gate import register_frame_handlers
from bms_msg.api.rpc import routes as rpc_routes
from bms_msg.api.rpc import stream as rpc_stream
from bms_msg.broker.backpressure import RateLimiter, default_rate_limit_tuning
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import InMemoryDeadLetterQueue
from bms_msg.broker.delay import InMemoryDelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.log import InMemoryMessageStore, MessageStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import InMemoryOffsetStore
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler, DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry
from bms_msg.broker.waiter import DeliveryWaiter, NotifyingMessageStore
from bms_msg.rpc.builtin import default_rpc_registry

__all__ = ["build_frame_app", "install_backpressure_state"]

_TEST_DATA_ROOT = Path(tempfile.gettempdir()) / "msg-test-store"
"""用例侧数据根（只用于目录登记表拼路径；用例用内存存储，不会真的写盘）。"""


def install_backpressure_state(app: FastAPI) -> None:
    """注入 03-5 的进程级状态（唤醒器 / 指标 / 限流器）与 01-7 的固定成本状态（缓存 / 节流），
    并把消息存储包一层「写入即唤醒」。

    各用例文件自建的应用都要调一次（生产由 `configure_service()` 注入）。幂等：已注入则跳过。
    注意调用顺序——**先注入再构造延迟推进器**，否则推进器会捕获未包装的存储，延迟转投不会唤醒等待者；
    顺序不满足时功能仍可用（长轮询退化为「等到超时再拉」），只是少了即时唤醒。

    Args:
        app: 应用实例（其 `message_store` 必须已就位）。
    """
    # 01-7 的两项与唤醒器无关，先注入（即使下面因幂等提前返回也已就位）。
    app.state.topic_cache = TopicCache()
    app.state.retention_throttle = RetentionThrottle()
    # 02-1：分区目录登记表（数据面端点声明了该依赖；用例用内存存储，故 root 不落盘即可）。
    if getattr(app.state, "partition_directory", None) is None:
        app.state.partition_directory = PartitionDirectory(root=_TEST_DATA_ROOT)
    if getattr(app.state, "waiter", None) is not None:
        return
    waiter = DeliveryWaiter()
    app.state.waiter = waiter
    app.state.message_store = NotifyingMessageStore(cast("MessageStore", app.state.message_store), waiter)
    app.state.backpressure_metrics = BackpressureMetrics(waiter=waiter)
    app.state.rate_limiter = RateLimiter(default_rate_limit_tuning())


def build_frame_app(
    *,
    session: AsyncSession | None = None,
    engine: AsyncEngine | None = None,
    tenant_source: object,
    mount_frame: bool = True,
    segment_records: int = 1_000,
) -> FastAPI:
    """构造最小应用（`/api/v1` 全量业务路由 + 可选根路径 `/msg/*` 聚合）。

    两种会话形态（二选一）：

    - `session`：所有请求 / 每轮拉取**共用**一个内存会话——绝大多数用例够用，也最快；
    - `engine`：每次请求 / 每轮拉取**现开现关**一个新会话——并发用例（真 h2c 下的流 + 三端点并发）
      必须用这一种：共用 `AsyncSession` 不支持并发操作（同一会话并发会抛
      「concurrent operations are not permitted」），而那条断言正是本任务的验收点。

    Args:
        session: 内存库会话（与 `engine` 二选一）。
        engine: 异步引擎（并发用例用；每个工作单元开独立会话）。
        tenant_source: 租户源替身（`by_code` / `by_id` / `single_active`）。
        mount_frame: 是否挂根路径聚合（`False` 时只有 `/api/v1`，用于对照既有端点）。
        segment_records: 内存消息存储的段替身条数（保留期用例注入小值）。

    Returns:
        FastAPI: 应用实例（`app.state` 已注入全部数据面进程级状态）。

    Raises:
        ValueError: `session` 与 `engine` 同时为空或同时给定。
    """
    if (session is None) == (engine is None):
        raise ValueError("build_frame_app 需要且只需要 session / engine 之一")

    app = FastAPI()
    app.state.settings = SimpleNamespace(
        permission=SimpleNamespace(provider=""),
        tenant_module=SimpleNamespace(provider=""),
        error_mapper=SimpleNamespace(provider=""),
        tenant=SimpleNamespace(
            exempt_paths=ConcurrentStableList(),
            deferred_paths=ConcurrentStableList(),
            allow_demo_fallback=False,
        ),
    )
    app.state.tenant_source = tenant_source
    app.state.message_store = InMemoryMessageStore(segment_records=segment_records)
    install_backpressure_state(app)
    app.state.offset_store = InMemoryOffsetStore()
    app.state.dead_letter_queue = InMemoryDeadLetterQueue()
    app.state.delivery_state = DeliveryState()
    app.state.group_store = InMemoryGroupStore()
    app.state.stream_registry = StreamRegistry()
    app.state.connection_registry = ConnectionRegistry()
    app.state.delay_store = InMemoryDelayStore()
    app.state.delay_scheduler = DelayScheduler(
        store=app.state.delay_store,
        messages=app.state.message_store,
        wheel=DelayWheel(),
    )
    # 04-1：RPC 方法注册表（生产由 `configure_service()` 注入同一份 `default_rpc_registry()`）。
    app.state.rpc_registry = default_rpc_registry()

    @asynccontextmanager
    async def _uow() -> AsyncGenerator[UnitOfWork]:
        """开一个工作单元（长驻流每轮拉取 / 预检用）。

        共用会话模式直接复用调用方会话；引擎模式每轮新开一个会话并关闭（并发用例必需）。

        Yields:
            UnitOfWork: 工作单元。
        """
        if session is not None:
            yield DbUnitOfWork(cast("DbSession", session))
            return
        maker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            cast("AsyncEngine", engine), expire_on_commit=False
        )
        async with maker() as fresh:
            yield DbUnitOfWork(cast("DbSession", fresh))

    app.state.uow_factory = cast("UowFactory", _uow)
    app.add_middleware(TenantMiddleware)
    register_exception_handlers(app)
    register_frame_handlers(app)
    routers = [topics.router, messages.router, data_groups.router]
    app.include_router(mount_service_routers(ConcurrentStableList(routers)))
    if mount_frame:
        app.include_router(
            mount_service_routers(
                ConcurrentStableList(
                    [
                        frame_routes.router,
                        frame_routes.connection_router,
                        frame_connection.router,
                        rpc_routes.router,
                        rpc_stream.stream_router,
                    ]
                ),
                prefix="",
            )
        )

    async def _platform_uow() -> AsyncGenerator[DbUnitOfWork]:
        """平台库工作单元替身（共用会话模式复用；引擎模式每次请求新开）。

        Yields:
            DbUnitOfWork: 工作单元。
        """
        if session is not None:
            yield DbUnitOfWork(cast("DbSession", session))
            return
        maker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            cast("AsyncEngine", engine), expire_on_commit=False
        )
        async with maker() as fresh:
            yield DbUnitOfWork(cast("DbSession", fresh))

    app.dependency_overrides[get_platform_uow] = _platform_uow
    return app
