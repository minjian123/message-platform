"""生命周期接缝（任务 03-1；Kiwi 2420）：启动钩子起后台循环、停机随资源管理器回收。

口径（详细设计 §7.2）：`startup_hooks()` 在装配完成后启动延迟推进器与重放执行器，并把回收句柄
登记进 `app.state.resources`；停机时 `ResourceManager.aclose()` 逆序释放 → 两个循环停止。
钩子与 `aclose()` 都幂等（重复执行不起第二个循环、不会重复登记）。
"""

import pytest
from bms_core.core.resources import ResourceManager
from fastapi import FastAPI

from bms_msg.broker.lifecycle import BackgroundLoops, start_background_tasks
from bms_msg.broker.replay import ReplayRunner
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.main import ApplicationFactory

pytestmark = pytest.mark.kiwi_id(2420)


def _app(scheduler: DelayScheduler, runner: ReplayRunner) -> FastAPI:
    """构造只带后台任务与资源管理器的空应用。

    Args:
        scheduler: 延迟推进器。
        runner: 重放执行器。

    Returns:
        FastAPI: 应用实例。
    """
    app = FastAPI()
    app.state.delay_scheduler = scheduler
    app.state.replay_runner = runner
    app.state.resources = ResourceManager()
    return app


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
