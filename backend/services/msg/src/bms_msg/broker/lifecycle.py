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

from typing import cast

from bms_core.core.capability import BaseAsyncResource
from bms_core.core.logging import get_logger
from bms_core.core.resources import ResourceManager
from fastapi import FastAPI

from bms_msg.broker.replay import ReplayRunner
from bms_msg.broker.scheduler import DelayScheduler

__all__ = ["BackgroundLoops", "start_background_tasks"]

_LOGGER = get_logger("bms_msg.lifecycle")


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
    """启动延迟推进器与重放执行器，并登记停机回收（`startup_hooks()` 执行体）。

    幂等：`ensure_started()` 内部按 `running` 判定；重复执行不会起第二个循环、不会重复登记。

    Args:
        app: 应用实例（取 `delay_scheduler` / `replay_runner` 与资源管理器）。
    """
    scheduler = cast("DelayScheduler", app.state.delay_scheduler)
    runner = cast("ReplayRunner", app.state.replay_runner)
    scheduler.ensure_started()
    runner.ensure_started()
    if getattr(app.state, "background_loops", None) is None:
        resources = cast("ResourceManager", app.state.resources)
        loops = BackgroundLoops(scheduler=scheduler, runner=runner)
        resources.register(loops)
        app.state.background_loops = loops
    _LOGGER.info(
        "background_loops_started",
        delay_scheduler=scheduler.running,
        replay_runner=runner.running,
    )
