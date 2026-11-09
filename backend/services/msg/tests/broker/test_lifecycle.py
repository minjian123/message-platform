"""生命周期接缝（任务 03-1；Kiwi 2420）：启动钩子起后台循环、停机随资源管理器回收。

口径（详细设计 §7.2）：`startup_hooks()` 在装配完成后启动延迟推进器与重放执行器，并把回收句柄
登记进 `app.state.resources`；停机时 `ResourceManager.aclose()` 逆序释放 → 两个循环停止。
钩子与 `aclose()` 都幂等（重复执行不起第二个循环、不会重复登记）。
"""

import inspect
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.plugin import default_plugin_registry
from bms_core.core.resources import ResourceManager
from fastapi import FastAPI

from bms_msg.broker.delay import DelayRecord, DelayStore, InMemoryDelayStore
from bms_msg.broker.lifecycle import BackgroundLoops, DelayCloser, OffsetFlusher, StorageCloser, start_background_tasks
from bms_msg.broker.replay import ReplayRunner
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.main import ApplicationFactory

pytestmark = pytest.mark.kiwi_id(2420)

TENANT_ID = 7001
"""用例租户主键（与夹具上下文同值）。"""

TOPIC_ID = 1
"""用例主题主键。"""


def _record(*, clock: Callable[[], datetime], span_ms: int) -> DelayRecord:
    """构造一条延迟记录（端口级直接入队）。

    Args:
        clock: 可推进时钟。
        span_ms: 延迟跨度（毫秒）。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = clock()
    return DelayRecord(
        log_id="l-1",
        message_id="m-1",
        tenant_id=TENANT_ID,
        topic_id=TOPIC_ID,
        namespace="msg",
        topic="audit.operation",
        partition_no=0,
        delay_level="1s",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "test"}),
        trace_id="trace-1",
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
    )


class _OffsetFlushStub:
    """位点刷盘替身（02-3 起启动钩子也会拉起位点刷盘循环并登记回收句柄）。

    本文件只做**最小适配**：不断言 02-3 的偏序与刷盘行为（那是 Kiwi 2429 的用例），
    只为「钩子能在只含后台任务的应用上跑通」提供一个可调用的存储。
    """

    def __init__(self) -> None:
        """初始化（记录各钩子是否被调用）。"""
        self.ensured = False
        self.stopped = False
        self.closed = False

    def ensure_started(self) -> None:
        """标记循环已启动。"""
        self.ensured = True

    async def stop(self) -> None:
        """标记循环已停止。"""
        self.stopped = True

    async def run_once(self) -> int:
        """不刷任何文件。

        Returns:
            int: 恒 0。
        """
        return 0

    async def aclose(self) -> None:
        """标记已刷盘关闭。"""
        self.closed = True


def _app(
    scheduler: DelayScheduler,
    runner: ReplayRunner,
    *,
    purge: object | None = None,
    cleanup: object | None = None,
) -> FastAPI:
    """构造只带后台任务与资源管理器的空应用。

    Args:
        scheduler: 延迟推进器。
        runner: 重放执行器。
        purge: 租户到期清扫服务替身（05-5；None 表示不装配）。
        cleanup: 删主题清理扫描替身（06-2；None 表示不装配）。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.state.delay_scheduler = scheduler
    app.state.replay_runner = runner
    app.state.offset_store = _OffsetFlushStub()
    # 02-7：启动钩子会先尝试重建延迟区；内存实现没有 `rebuild`，钩子按「无则跳过」处理
    app.state.delay_store = InMemoryDelayStore()
    if purge is not None:
        app.state.tenant_purge_service = purge
    if cleanup is not None:
        app.state.topic_cleanup_service = cleanup
    app.state.resources = ResourceManager()
    return app


class _PurgeStub:
    """租户到期清扫服务替身（05-5：只验证启动 / 停止接线）。"""

    def __init__(self) -> None:
        """初始化（未启动）。"""
        self.ensured = False
        self.stopped = False

    def ensure_started(self) -> None:
        """标记清扫循环已启动。"""
        self.ensured = True

    async def stop(self) -> None:
        """标记清扫循环已停止。"""
        self.stopped = True


async def test_start_hook_starts_and_stops_tenant_purge_service(
    delay_scheduler: DelayScheduler, replay_runner: ReplayRunner
) -> None:
    """05-5：启动钩子拉起租户到期清扫，停机随资源管理器停止。"""
    purge = _PurgeStub()
    app = _app(delay_scheduler, replay_runner, purge=purge)

    await start_background_tasks(app)

    assert purge.ensured is True
    assert purge.stopped is False

    await app.state.resources.aclose()

    assert purge.stopped is True


async def test_start_hook_starts_and_stops_topic_cleanup(
    delay_scheduler: DelayScheduler, replay_runner: ReplayRunner
) -> None:
    """06-2：启动钩子拉起删主题清理扫描，停机随资源管理器停止。"""
    cleanup = _PurgeStub()
    app = _app(delay_scheduler, replay_runner, cleanup=cleanup)

    await start_background_tasks(app)

    assert cleanup.ensured is True
    assert cleanup.stopped is False

    await app.state.resources.aclose()

    assert cleanup.stopped is True


async def test_start_hook_starts_loops_and_registers_resource(
    delay_scheduler: DelayScheduler, replay_runner: ReplayRunner
) -> None:
    """钩子启动两个循环，并登记一个可回收句柄（`resources` 里恰好一条）。"""
    app = _app(delay_scheduler, replay_runner)

    await start_background_tasks(app)

    assert delay_scheduler.running is True
    assert replay_runner.running is True
    loops = getattr(app.state, "background_loops", None)
    assert isinstance(loops, BackgroundLoops)

    await app.state.resources.aclose()

    assert delay_scheduler.running is False
    assert replay_runner.running is False


async def test_start_hook_is_idempotent(delay_scheduler: DelayScheduler, replay_runner: ReplayRunner) -> None:
    """重复执行钩子：循环不重启、句柄不重复登记（第二次直接复用）。"""
    app = _app(delay_scheduler, replay_runner)

    await start_background_tasks(app)
    first = app.state.background_loops
    await start_background_tasks(app)

    assert app.state.background_loops is first
    assert delay_scheduler.running is True
    assert replay_runner.running is True

    await app.state.resources.aclose()


async def test_background_loops_aclose_is_idempotent(
    delay_scheduler: DelayScheduler, replay_runner: ReplayRunner
) -> None:
    """`aclose()` 可重复调用（资源管理器与显式释放都走它）。"""
    app = _app(delay_scheduler, replay_runner)
    await start_background_tasks(app)
    loops = app.state.background_loops

    await loops.aclose()
    await loops.aclose()

    assert delay_scheduler.running is False
    assert replay_runner.running is False


def test_factory_exposes_startup_hook() -> None:
    """应用工厂把钩子登记在 `startup_hooks()`（不是 `configure_service()`：那时事件循环还没起）。"""
    hooks = ApplicationFactory().startup_hooks()

    assert [hook.__name__ for hook in hooks] == ["start_background_tasks"]


class _RebuildableStore(InMemoryDelayStore):
    """可重建的延迟区替身（02-7：文件实现才有 `rebuild`，这里只验证钩子的接线顺序）。"""

    def __init__(self) -> None:
        """初始化（记录重建次数）。"""
        super().__init__()
        self.rebuilt = 0

    async def rebuild(self) -> object:
        """标记已重建并返回一个哑对象（钩子只读它的日志字段）。

        Returns:
            object: 哑报告。
        """
        self.rebuilt += 1
        return object()


class _RunnerStub:
    """重放执行器替身（本用例只验证延迟区的接线顺序，不碰数据库）。"""

    def __init__(self) -> None:
        """初始化（未启动）。"""
        self.started = False
        self.stopped = False

    @property
    def running(self) -> bool:
        """是否已启动。

        Returns:
            bool: 已启动即 True。
        """
        return self.started

    def ensure_started(self) -> None:
        """标记已启动。"""
        self.started = True

    async def stop(self) -> None:
        """标记已停止。"""
        self.stopped = True


async def test_start_hook_rebuilds_delay_store_before_starting_loop(
    delay_scheduler: DelayScheduler, clock: Callable[[], datetime], advance: Callable[[int], None]
) -> None:
    """D19：启动钩子先重建延迟区并把记录装进时间轮，再启动推进器。

    判据是「钩子返回时时间轮里已有记录」——推进器任务此刻还没被调度过，所以这份记录只能来自
    钩子里的「重建 → 装填」，顺序反了就会是一个空轮（重启后存量记录要等下一次发布才动）。
    """
    app = _app(delay_scheduler, cast("ReplayRunner", _RunnerStub()))
    store = _RebuildableStore()
    record = _record(clock=clock, span_ms=1_000)
    await store.enqueue(record)
    advance(2_000)  # 记录已到期 → 装填时进待重试队列，`take_due` 首轮即可见
    app.state.delay_store = store

    await start_background_tasks(app)

    assert store.rebuilt == 1
    assert [item.log_id for item in delay_scheduler.wheel.take_due(clock())] == [record.log_id]
    await app.state.resources.aclose()


async def test_factory_registers_delay_closer_between_offsets_and_storage() -> None:
    """D18：登记顺序 `OffsetFlusher → DelayCloser → StorageCloser`。

    逆序释放即「刷日志 → 刷 `.delay` → 刷位点」——清 `.delay` 依赖日志已落盘，顺序错只在
    os / time 档下表现为「优雅关闭后仍丢几条延迟消息」。
    """
    registry = default_plugin_registry()
    registry.reset()
    try:
        app = ApplicationFactory().create(None)
        resources = list(app.state.resources._resources)  # pyright: ignore[reportPrivateUsage] - 登记顺序就是本用例的判据
    finally:
        registry.reset()

    offsets = next(index for index, item in enumerate(resources) if isinstance(item, OffsetFlusher))
    delay = next(index for index, item in enumerate(resources) if isinstance(item, DelayCloser))
    storage = next(index for index, item in enumerate(resources) if isinstance(item, StorageCloser))
    assert offsets < delay < storage


def test_delay_store_port_signature_is_unchanged() -> None:
    """D19：`DelayStore` 端口四个方法的签名未变（02-7 只换实现，不动端口）。"""
    assert list(inspect.signature(DelayStore.enqueue).parameters) == ["self", "record"]
    assert list(inspect.signature(DelayStore.count_for_tenant).parameters) == ["self", "tenant_id"]
    assert list(inspect.signature(DelayStore.list_for_tenant).parameters) == ["self", "tenant_id"]
    assert list(inspect.signature(DelayStore.list_all).parameters) == ["self"]
    assert list(inspect.signature(DelayStore.remove).parameters) == ["self", "log_id"]
