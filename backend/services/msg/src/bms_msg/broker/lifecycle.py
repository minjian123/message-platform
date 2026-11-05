"""后台任务与生命周期的接缝：装配完成后启动、停机时随资源管理器逆序回收（任务 03-1）。

**为什么要有这个模块**：01-5 的延迟推进器与 01-6 的重放执行器此前都是「首个请求惰性启动」——
进程起来后只要没人发延迟消息 / 没人建重放任务，它们就不跑；而基座 `service_lifespan` 之前没有
「产品启动钩子」的登记处，两条遗留一直挂着。基座现已支持 `startup_hooks()` 与 `app.state.resources`
（`ResourceManager`，装配后逆序 `aclose()`），本模块把两者接起来：

- `start_background_tasks(app)`：`startup_hooks()` 的执行体——启动两个循环，并把回收句柄登记进资源管理器；
- `BackgroundLoops`：`aclose()` 里停两个循环，停机时序由基座统一保证（先摘流、再 release）。

**不改惰性入口**：首个延迟发布 / 首个重放任务仍会调 `ensure_started()`（幂等），启动钩子只是让它更早起。
**不做任务恢复**：重放任务「落表但不续跑」仍是 8B_06 的活，本模块只负责循环活着。
"""

from typing import Protocol, cast

from bms_core.core.capability import BaseAsyncResource
from bms_core.core.logging import get_logger
from bms_core.core.resources import ResourceManager
from fastapi import FastAPI

from bms_msg.broker.delay import DelayStore
from bms_msg.broker.replay import ReplayRunner
from bms_msg.broker.scheduler import DelayScheduler

__all__ = [
    "BackgroundLoops",
    "CloseableStore",
    "DelayCloser",
    "FlushableOffsetStore",
    "OffsetFlushLoop",
    "OffsetFlusher",
    "RebuildableDelayStore",
    "StorageCloser",
    "start_background_tasks",
]

_LOGGER = get_logger("bms_msg.lifecycle")


class CloseableStore(Protocol):
    """可优雅关闭的存储（只要求一个 `aclose`，不依赖具体实现）。"""

    async def aclose(self) -> None:
        """刷盘、写恢复点并释放句柄。"""


class RebuildableDelayStore(Protocol):
    """可从盘上重建的延迟区（`FileDelayStore` 结构上满足；内存实现没有 `rebuild`）。"""

    async def rebuild(self) -> object:
        """全量扫 `.delay` 重建索引与计数。

        Returns:
            object: 重建报告（只用于日志字段，故不绑定具体类型）。
        """
        ...

    async def aclose(self) -> None:
        """补写待落盘的计数、刷 `.delay`、关句柄。"""
        ...


class FlushableOffsetStore(Protocol):
    """可后台刷盘、可优雅刷盘的位点存储（`FileOffsetStore` 结构上即满足）。"""

    def ensure_started(self) -> None:
        """启动刷盘循环（幂等）。"""

    async def stop(self) -> None:
        """停止刷盘循环（幂等；不刷盘）。"""

    async def run_once(self) -> int:
        """刷一轮（返回落盘文件数）。"""
        ...

    async def aclose(self) -> None:
        """停循环并无条件刷盘（幂等）。"""


class StorageCloser(BaseAsyncResource):
    """存储关闭句柄（02-2）：登记进 `ResourceManager`，停机时刷日志与恢复点。

    **注册顺序即偏序**：本资源在 `configure_service()` 里注册、后台循环在 `startup_hooks()` 里注册，
    而 `ResourceManager` 是逆序释放——所以「先停后台循环（延迟转投 / 重放）、再刷日志」由装配顺序
    天然保证，不需要额外的协调代码。
    """

    def __init__(self, *, store: CloseableStore) -> None:
        """初始化。

        Args:
            store: 消息存储（文件段实现）。
        """
        self._store = store

    async def aclose(self) -> None:
        """刷盘 + 写恢复点 + 关句柄（`aclose` 幂等，重复调用无副作用）。"""
        await self._store.aclose()
        _LOGGER.info("storage_closed")


class DelayCloser(BaseAsyncResource):
    """延迟区关闭句柄（02-7）：停机时补写待落盘的 `attempts` 并刷 `.delay`。

    **注册顺序即偏序**（与位点同源推导）：清 `.delay` 依赖日志已落盘，所以本资源必须晚于
    `StorageCloser` 释放，也就是**早于它注册**（`ResourceManager` 逆序释放）。顺序错的表现是
    「优雅关闭后重启仍丢了几条延迟消息」，而且只在 os / time 档下复现——用例 D18 直接断言注册顺序。
    """

    def __init__(self, *, store: RebuildableDelayStore) -> None:
        """初始化。

        Args:
            store: 延迟区存储（文件实现）。
        """
        self._store = store

    async def aclose(self) -> None:
        """补写 + 刷盘 + 关句柄（`aclose` 幂等，重复调用无副作用）。"""
        await self._store.aclose()
        _LOGGER.info("delay_store_closed")


class OffsetFlushLoop(BaseAsyncResource):
    """位点刷盘循环的回收句柄（02-3）：停机时先停循环，刷盘留给 `OffsetFlusher`。

    **为什么与 `OffsetFlusher` 分成两个资源**：位点需要两件事，且它们要求的释放位置相反——
    停循环要**早**（不再产生新脏），刷盘要**晚**（在日志 fsync 之后）。`ResourceManager` 逆序释放，
    所以「停循环」注册得晚（`startup_hooks`，且先于 `BackgroundLoops`）、「刷盘」注册得早
    （`configure_service`，且早于 `StorageCloser`）。
    """

    def __init__(self, *, store: FlushableOffsetStore) -> None:
        """初始化。

        Args:
            store: 位点存储（文件实现）。
        """
        self._store = store

    async def aclose(self) -> None:
        """停止刷盘循环（`stop()` 幂等，不刷盘）。"""
        await self._store.stop()
        _LOGGER.info("offset_flush_loop_stopped")


class OffsetFlusher(BaseAsyncResource):
    """位点刷盘关闭句柄（02-3）：停机时无条件刷一次位点。

    **必须比 `StorageCloser` 晚释放**（即位点句柄要更早注册）：先刷日志、再刷位点，才能保证
    「位点值 ≤ 已落盘 LEO」——反过来会让位点越过日志尾，重启后对齐把位点夹回 LEO，
    中间那段消息就被跳过了（丢消息）。
    """

    def __init__(self, *, store: FlushableOffsetStore) -> None:
        """初始化。

        Args:
            store: 位点存储（文件实现）。
        """
        self._store = store

    async def aclose(self) -> None:
        """停循环 + 无条件刷盘（`aclose` 幂等，重复调用无副作用）。"""
        await self._store.aclose()
        _LOGGER.info("offset_store_closed")


class BackgroundLoops(BaseAsyncResource):
    """两个后台循环的回收句柄（登记进 `ResourceManager`，停机时逆序释放）。"""

    def __init__(self, *, scheduler: DelayScheduler, runner: ReplayRunner) -> None:
        """初始化。

        Args:
            scheduler: 延迟到期推进器。
            runner: 重放执行器。
        """
        self._scheduler = scheduler
        self._runner = runner

    async def aclose(self) -> None:
        """停止两个循环（各自 `stop()` 幂等，重复调用无副作用）。"""
        await self._scheduler.stop()
        await self._runner.stop()
        _LOGGER.info("background_loops_stopped")


async def start_background_tasks(app: FastAPI) -> None:
    """启动延迟推进器、重放执行器与位点刷盘循环，并登记停机回收（`startup_hooks()` 执行体）。

    幂等：`ensure_started()` 内部按 `running` 判定；重复执行不会起第二个循环、不会重复登记。

    **02-7 新增重建**：先把 `.delay` 全量扫一遍装进时间轮，再 `ensure_started()`。顺序不能反——
    先起推进器会让首轮扫到一个空轮，重启后存量延迟记录要等下一次装填才动；重建放在这里也保证
    「进程起来就能投」，不必等第一个延迟发布来触发（01-5 留下的那条惰性入口仍然保留、仍然幂等）。

    **注册顺序即偏序**（逆序释放）：本函数里先登记 `OffsetFlushLoop`、后登记 `BackgroundLoops`，
    于是停机时先停延迟 / 重放、再停位点刷盘——先断掉会产生新位点的上游，再停刷盘。

    Args:
        app: 应用实例（取 `delay_store` / `delay_scheduler` / `replay_runner` / `offset_store` 与资源管理器）。
    """
    scheduler = cast("DelayScheduler", app.state.delay_scheduler)
    runner = cast("ReplayRunner", app.state.replay_runner)
    offsets = cast("FlushableOffsetStore", app.state.offset_store)
    delay_store = cast("DelayStore", app.state.delay_store)
    await _rebuild_delay_store(delay_store=delay_store, scheduler=scheduler)
    scheduler.ensure_started()
    runner.ensure_started()
    offsets.ensure_started()
    resources = cast("ResourceManager", app.state.resources)
    if getattr(app.state, "offset_flush_loop", None) is None:
        flush_loop = OffsetFlushLoop(store=offsets)
        resources.register(flush_loop)
        app.state.offset_flush_loop = flush_loop
    if getattr(app.state, "background_loops", None) is None:
        loops = BackgroundLoops(scheduler=scheduler, runner=runner)
        resources.register(loops)
        app.state.background_loops = loops
    _LOGGER.info(
        "background_loops_started",
        delay_scheduler=scheduler.running,
        replay_runner=runner.running,
        offset_flush_loop=getattr(app.state, "offset_flush_loop", None) is not None,
    )


async def _rebuild_delay_store(*, delay_store: DelayStore, scheduler: DelayScheduler) -> None:
    """重建延迟区并把记录装进时间轮（内存实现没有 `rebuild`，整段跳过）。

    Args:
        delay_store: 延迟区存储。
        scheduler: 到期推进器（提供时间轮实例）。
    """
    rebuild = getattr(delay_store, "rebuild", None)
    if rebuild is None:
        return
    report = await rebuild()
    wheel = scheduler.wheel
    records = await delay_store.list_all()
    for record in records:
        wheel.place(record)
    _LOGGER.info(
        "delay_store_rebuilt",
        records=len(records),
        expired=getattr(report, "expired", 0),
        isolated_segments=getattr(report, "isolated_segments", 0),
    )
