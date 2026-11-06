"""04-4 用例之三（Kiwi 2439）：熔断三态 / 窗口 / 半开限流 / 打开快速失败 / 与重试联动。

fake clock 精确驱动「打开 30s 后转半开」，不依赖真实等待；日志断言走
`structlog.testing.capture_logs()`（console 格式下 `assert_logged` 抓不到）。
"""

from __future__ import annotations

import pytest
from bms_core.circuit.base import CircuitState
from bms_core.circuit.null import NullCircuitBreaker
from structlog.testing import capture_logs

from bms_msg.client.circuit import InProcessCircuitBreaker, rpc_dependency
from bms_msg.client.retry import CircuitPolicy, RetryPolicy
from bms_msg.client.rpc import RpcCallError, RpcClient, StaticInstanceProvider
from tests_support.rpc_retry import FakeClock, FixedJitter, error_response, result_response, scripted

pytestmark = pytest.mark.kiwi_id(2439)

BASE_URL = "http://test"
"""实例基址（脚本传输不真正解析）。"""

DEP = rpc_dependency("demo")
"""熔断依赖标识（与 bms `service_dependency` 同值）。"""


def _breaker(clock: FakeClock, **kwargs: object) -> InProcessCircuitBreaker:
    """构造注入 fake clock 的熔断器。

    Args:
        clock: 假时钟。
        kwargs: 覆盖熔断参数。

    Returns:
        InProcessCircuitBreaker: 熔断器。
    """
    return InProcessCircuitBreaker(clock=clock, policy=CircuitPolicy(**kwargs))  # type: ignore[arg-type]


def _client(transport: object, **kwargs: object) -> RpcClient:
    """构造脚本传输客户端。

    Args:
        transport: 传输替身。
        kwargs: 构造参数。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceProvider([BASE_URL]), transport=transport, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 1. 契约与依赖标识
# ---------------------------------------------------------------------------


def test_dependency_matches_bms_namespace() -> None:
    """依赖标识沿用 bms `service:<key>`，04-5 可直接换成 bms 侧实现。"""
    assert DEP == "service:demo"
    assert rpc_dependency("other") == "service:other"


async def test_null_breaker_is_always_closed() -> None:
    """缺省 `NullCircuitBreaker` 恒闭合：首期行为等价现状、零回归。"""
    breaker = NullCircuitBreaker()
    assert await breaker.allow(DEP) is True
    await breaker.record_failure(DEP)
    assert await breaker.allow(DEP) is True
    assert await breaker.state(DEP) is CircuitState.CLOSED
    await breaker.record_success(DEP)


async def test_default_construction() -> None:
    """不注入时钟 / 参数时用仓内默认常量，仍可正常放行。"""
    breaker = InProcessCircuitBreaker()
    assert await breaker.allow(DEP) is True
    assert await breaker.state(DEP) is CircuitState.CLOSED


# ---------------------------------------------------------------------------
# 2. 三态转换 / 窗口 / 半开限流
# ---------------------------------------------------------------------------


async def test_consecutive_failures_open_circuit() -> None:
    """窗口内连续失败达阈值即打开，并记状态转换日志。"""
    clock = FakeClock()
    breaker = _breaker(clock)
    with capture_logs() as logs:
        for _ in range(4):
            await breaker.record_failure(DEP)
            assert await breaker.state(DEP) is CircuitState.CLOSED
        await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.OPEN
    assert await breaker.allow(DEP) is False
    events = [entry for entry in logs if entry["event"] == "msg_rpc_circuit"]
    assert len(events) == 1
    assert (events[0]["from"], events[0]["to"], events[0]["dependency"]) == ("closed", "open", DEP)


async def test_success_clears_failure_count() -> None:
    """成功清零连续失败计数：4 次失败 + 1 次成功 + 1 次失败不会打开。"""
    clock = FakeClock()
    breaker = _breaker(clock)
    for _ in range(4):
        await breaker.record_failure(DEP)
    await breaker.record_success(DEP)
    await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.CLOSED


async def test_window_expiry_resets_count() -> None:
    """窗口内无新失败即清零连续计数。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=2, window_ms=100)
    await breaker.record_failure(DEP)
    clock.advance(101)
    await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.CLOSED


async def test_open_expires_into_half_open_and_probe_limit() -> None:
    """打开到期转半开；半开只放 1 个探测，其余快速失败。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=1, open_ms=100)
    await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.OPEN
    clock.advance(100)
    assert await breaker.state(DEP) is CircuitState.HALF_OPEN
    assert await breaker.allow(DEP) is True
    assert await breaker.allow(DEP) is False


async def test_half_open_probe_success_closes() -> None:
    """半开探测成功 → 闭合。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=1, open_ms=100)
    await breaker.record_failure(DEP)
    clock.advance(100)
    assert await breaker.allow(DEP) is True
    await breaker.record_success(DEP)
    assert await breaker.state(DEP) is CircuitState.CLOSED


async def test_half_open_probe_failure_reopens() -> None:
    """半开探测失败 → 回到打开，并重置打开计时。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=1, open_ms=100)
    await breaker.record_failure(DEP)
    clock.advance(100)
    assert await breaker.allow(DEP) is True
    await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.OPEN
    clock.advance(99)
    assert await breaker.state(DEP) is CircuitState.OPEN


async def test_state_advances_open_to_half_open() -> None:
    """`state()` 供告警读取时也会推进到期状态（与 `allow` 同一推进点）。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=1, open_ms=100)
    await breaker.record_failure(DEP)
    clock.advance(100)
    assert await breaker.state(DEP) is CircuitState.HALF_OPEN
    assert await breaker.state(DEP) is CircuitState.HALF_OPEN


# ---------------------------------------------------------------------------
# 3. 与重试层联动
# ---------------------------------------------------------------------------


async def test_open_circuit_fails_fast_without_any_attempt() -> None:
    """熔断打开时直接失败，不进入重试、零次发送。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=1)
    await breaker.record_failure(DEP)
    transport = scripted(result_response())
    client = _client(transport, circuit=breaker, clock=clock, jitter=FixedJitter([1]))
    with pytest.raises(RpcCallError) as caught:
        await client.call("demo", "echo", b"x")
    assert caught.value.code == 0
    assert caught.value.category.value == "transient"
    assert len(transport.requests) == 0
    assert clock.sleeps == []


async def test_retry_failures_count_towards_circuit() -> None:
    """重试期间的每次失败都计入连续计数，达阈值后打开。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=2)
    transport = scripted(*([error_response(350005)] * 3))
    client = _client(transport, circuit=breaker, clock=clock, jitter=FixedJitter([1, 2]))
    with pytest.raises(RpcCallError):
        await client.call("demo", "echo", b"x")
    assert await breaker.state(DEP) is CircuitState.OPEN


async def test_success_records_back_to_circuit() -> None:
    """成功调用写回 `record_success`：连续失败计数被清零。"""
    clock = FakeClock()
    breaker = _breaker(clock, failure_threshold=3)
    await breaker.record_failure(DEP)
    transport = scripted(error_response(350005), result_response())
    client = _client(transport, circuit=breaker, clock=clock, jitter=FixedJitter([1]))
    await client.call("demo", "echo", b"x")
    assert await breaker.state(DEP) is CircuitState.CLOSED
    # 再两次失败仍不足以打开（成功已清零）。
    await breaker.record_failure(DEP)
    await breaker.record_failure(DEP)
    assert await breaker.state(DEP) is CircuitState.CLOSED


async def test_default_client_uses_null_breaker() -> None:
    """未注入熔断器时走 `NullCircuitBreaker`：可重试错误正常重试，行为等价首期。"""
    transport = scripted(error_response(350005), result_response())
    client = _client(
        transport,
        clock=FakeClock(),
        jitter=FixedJitter([1]),
        retry_policy=RetryPolicy(),
    )
    assert await client.call("demo", "echo", b"x") is not None
    assert len(transport.requests) == 2
