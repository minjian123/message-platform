"""RPC 预算值对象与判据（任务 04-2；Kiwi 2434）。

判据：

1. `validate_budget_ms`：缺失取缺省、`bool` 与非法类型拒、区间两侧（`0` / `600000` 合法，越界拒）；
2. `Deadline`：已用 / 剩余（夹取到 0）/ 耗尽判定 / 下游预算（减返回余量、不足归零）；
3. 两端同一判据：服务端（JSON / 帧）、客户端与用例都调 `validate_budget_ms`，不各写一套。
"""

import asyncio

import pytest
from bms_core.core.exceptions import ParamError

from bms_msg.core.config import (
    RPC_DEADLINE_MAX_MS,
    RPC_DEFAULT_TIMEOUT_MS,
    RPC_RETURN_RESERVE_MS,
)
from bms_msg.rpc.cancel import ensure_not_cancelled
from bms_msg.rpc.deadline import OUTCOME_UNKNOWN, Deadline, validate_budget_ms

pytestmark = pytest.mark.kiwi_id(2434)


def test_missing_budget_falls_back_to_default() -> None:
    """缺失取缺省；`default` 可覆盖（客户端构造期用它带自己的缺省）。"""
    assert validate_budget_ms(None) == RPC_DEFAULT_TIMEOUT_MS
    assert validate_budget_ms(None, default=1234) == 1234


@pytest.mark.parametrize("raw", [True, False])
def test_bool_is_rejected(raw: bool) -> None:
    """`bool` 是 `int` 子类：不单独排掉，`True` 会被当成 1ms 静默接受。"""
    with pytest.raises(ParamError):
        validate_budget_ms(raw)


@pytest.mark.parametrize("raw", ["1000", 1.5, object()])
def test_non_int_is_rejected(raw: object) -> None:
    """字符串 / 浮点 / 任意对象一律 `10001`。"""
    with pytest.raises(ParamError):
        validate_budget_ms(raw)


@pytest.mark.parametrize("raw", [-1, RPC_DEADLINE_MAX_MS + 1])
def test_out_of_range_is_rejected(raw: int) -> None:
    """区间外（负值 / 超上限）`10001`。"""
    with pytest.raises(ParamError):
        validate_budget_ms(raw)


@pytest.mark.parametrize("raw", [0, RPC_DEADLINE_MAX_MS])
def test_bounds_are_accepted(raw: int) -> None:
    """`0`（立即耗尽）与上限都是合法值。"""
    assert validate_budget_ms(raw) == raw


def test_spent_and_remaining() -> None:
    """已用按锚点算、剩余夹取到 0（响应头里不出现负数）。"""
    deadline = Deadline.start(1000, now=100.0)

    assert deadline.budget_ms == 1000
    assert deadline.spent_ms(100.125) == 125
    assert deadline.remaining_ms(100.125) == 875
    assert deadline.remaining_ms(200.0) == 0
    assert deadline.is_exhausted(100.125) is False


def test_exhausted_when_spent_reaches_budget() -> None:
    """已用等于预算即耗尽（边界取 `>=`，不是 `>`）。"""
    deadline = Deadline.start(125, now=0.0)

    assert deadline.is_exhausted(0.125) is True
    assert deadline.remaining_ms(0.125) == 0


def test_downstream_subtracts_return_reserve() -> None:
    """下游预算 = 剩余 − 返回余量。"""
    deadline = Deadline.start(1000, now=0.0)

    assert deadline.downstream_ms(0.0) == 1000 - RPC_RETURN_RESERVE_MS


def test_downstream_clamps_when_budget_below_reserve() -> None:
    """预算不足一个返回余量时下游预算归 0（下游预检即 350004）。"""
    deadline = Deadline.start(RPC_RETURN_RESERVE_MS - 1, now=0.0)

    assert deadline.downstream_ms(0.0) == 0


def test_start_uses_real_monotonic_clock_when_now_omitted() -> None:
    """不注入时刻时走真实单调时钟（运行期路径）。"""
    deadline = Deadline.start(None)

    assert deadline.budget_ms == RPC_DEFAULT_TIMEOUT_MS
    assert deadline.spent_ms() >= 0
    assert deadline.remaining_ms() <= RPC_DEFAULT_TIMEOUT_MS
    assert deadline.is_exhausted() is False
    assert deadline.downstream_ms() >= 0


def test_outcome_constant_is_unknown() -> None:
    """「结果未知」的取值进错误 `detail` / `data`，04-4 升为正式枚举。"""
    assert OUTCOME_UNKNOWN == "unknown"


async def test_ensure_not_cancelled_passes_on_healthy_request() -> None:
    """正常请求不抛（写响应前的复核不能误伤）。"""
    ensure_not_cancelled()


async def test_ensure_not_cancelled_raises_after_swallowed_cancel() -> None:
    """handler 吞掉取消并正常返回时，写响应前的复核必须拦住（否则会写出不该有的 200）。"""
    started = asyncio.Event()

    async def victim() -> None:
        started.set()
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            # 吞掉取消（`suppress(CancelledError)` 是常见写法），然后照常往下走。
            ensure_not_cancelled()

    task = asyncio.create_task(victim())
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
