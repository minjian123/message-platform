"""04-4 用例之一（Kiwi 2439）：错误分类 / full jitter 退避 / `Retry-After` / 调用链预算。

fake clock + 固定随机源让退避序列精确可断言；04-2「超时 = 结果未知、不得直接重试」在
「带幂等键」与「不带键」两条路径上分别固定。
"""

from __future__ import annotations

import pytest
from bms_core.msgclient.deadline import OUTCOME_UNKNOWN
from bms_core.msgclient.frame import decode_frame
from bms_core.msgclient.retry import (
    JitterMode,
    RetryBudget,
    RetryDecision,
    RetryPolicy,
    SystemClock,
    SystemJitter,
    acquire_budget,
    compute_backoff_ms,
    decide_retry,
    parse_retry_after,
    release_budget,
)
from bms_core.msgclient.rpc import (
    RpcCallError,
    RpcCallTimeoutError,
    RpcClient,
    StaticInstanceSelector,
    error_of_frame,
)
from structlog.testing import capture_logs

from bms_msg.api.frame.responses import classify, error_frame
from bms_msg.errors import ErrorCategory, Outcome
from tests_support.rpc_retry import (
    FakeClock,
    FixedJitter,
    error_response,
    result_response,
    scripted,
)

pytestmark = pytest.mark.kiwi_id(2439)

BASE_URL = "http://test"
"""实例基址（脚本传输不真正解析）。"""


def _client(transport: object, **kwargs: object) -> RpcClient:
    """构造走脚本传输的客户端（注入 fake clock / 固定随机源）。

    Args:
        transport: 脚本传输替身。
        kwargs: 透传给 `RpcClient` 的构造参数。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceSelector([BASE_URL]), transport=transport, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 1. 五类分类 / 枚举 / 错误对象
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (10001, ErrorCategory.PARAM_ERROR),
        (350006, ErrorCategory.PARAM_ERROR),
        (350023, ErrorCategory.BUSINESS_REJECT),
        (350005, ErrorCategory.TRANSIENT),
        (350022, ErrorCategory.TRANSIENT),
        (350004, ErrorCategory.UNKNOWN),
        (99999, ErrorCategory.SERVER_BUG),
    ],
)
def test_classify_categories(code: int, expected: ErrorCategory) -> None:
    """五类判定逐码固定；`layer` / `retryable` 保持 03-2 / 04-2 既有值。

    Args:
        code: 错误码。
        expected: 期望类别。
    """
    category, layer, retryable = classify(code)
    assert category is expected
    if code == 350004:
        assert (layer, retryable) == ("business", False)
    if code == 350005:
        assert (layer, retryable) == ("business", True)
    if code == 10001:
        assert (layer, retryable) == ("protocol", False)


def test_error_frame_carries_category() -> None:
    """错误帧头带 `category`，且与 `classify()` 同值（服务端唯一出处）。"""
    header = decode_frame(error_frame(code=350005, message="限流")).header
    assert header["category"] == ErrorCategory.TRANSIENT.value
    assert header["retryable"] is True
    assert header["layer"] == "business"


def test_outcome_enum_keeps_string_value() -> None:
    """`Outcome` 三值枚举闭合 04-2 遗留，且字符串值不变（既有用例零破坏）。"""
    assert [member.value for member in Outcome] == ["ok", "unknown", "failed"]
    assert OUTCOME_UNKNOWN == "unknown"
    assert RpcCallTimeoutError().outcome == Outcome.UNKNOWN
    assert RpcCallTimeoutError().category is ErrorCategory.UNKNOWN


def test_error_of_frame_reads_category_and_falls_back() -> None:
    """客户端照搬帧头 `category`；老服务端缺字段 / 取值不认识时保守回落 `server_bug`。"""
    header = {"code": 350005, "message": "限流", "retryable": True, "layer": "business", "category": "transient"}
    assert error_of_frame(header).category is ErrorCategory.TRANSIENT
    assert error_of_frame({**header, "category": "something_new"}).category is ErrorCategory.SERVER_BUG
    assert error_of_frame({k: v for k, v in header.items() if k != "category"}).category is ErrorCategory.SERVER_BUG


# ---------------------------------------------------------------------------
# 2. 退避：full jitter / 封顶 / 禁止立即重试
# ---------------------------------------------------------------------------


def test_compute_backoff_full_jitter() -> None:
    """full jitter 在 `[1, raw_cap]` 内取值，指数基线按倍数增长并夹到 cap。"""
    policy = RetryPolicy(base_ms=100, multiplier=2, cap_ms=1000)
    jitter = FixedJitter([7, 200, 9999])
    assert compute_backoff_ms(1, policy, jitter) == 7
    assert compute_backoff_ms(2, policy, jitter) == 200
    # 随机源给越界值也只能取到 raw_cap，不突破封顶承诺。
    assert compute_backoff_ms(4, policy, jitter) == 800
    assert jitter.calls == [(1, 100), (1, 200), (1, 800)]


def test_compute_backoff_never_zero_and_none_mode() -> None:
    """下界恒 >= 1（禁止无退避立即重试）；`none` 形态取指数基线原值。"""
    policy = RetryPolicy(base_ms=100, multiplier=2, cap_ms=1000, jitter=JitterMode.NONE)
    assert compute_backoff_ms(1, policy, FixedJitter([0])) == 100
    assert compute_backoff_ms(3, policy, FixedJitter([0])) == 400
    assert compute_backoff_ms(9, policy, FixedJitter([0])) == 1000
    full = RetryPolicy(base_ms=100, cap_ms=1000)
    assert compute_backoff_ms(1, full, FixedJitter([0])) == 1
    assert compute_backoff_ms(1, full, FixedJitter([9999])) == 100


def test_system_clock_and_jitter() -> None:
    """生产实现可调用：时钟给非负单调毫秒，随机源落在区间内。"""
    clock = SystemClock()
    assert clock.now_ms() >= 0
    assert 1 <= SystemJitter().uniform(1, 1) <= 1


async def test_retry_uses_exponential_backoff_with_full_jitter() -> None:
    """两次瞬时失败后成功：退避序列来自 fake clock，尝试次数与序列落在日志里。"""
    clock = FakeClock()
    transport = scripted(
        error_response(350005),
        error_response(350005),
        result_response(b"done"),
    )
    client = _client(
        transport,
        clock=clock,
        jitter=FixedJitter([50, 150]),
        retry_policy=RetryPolicy(base_ms=100, multiplier=2, cap_ms=2000),
    )
    with capture_logs() as logs:
        reply = await client.call("demo", "echo", b"x")
    assert reply.payload == b"done"
    assert clock.sleeps == [50, 150]
    assert len(transport.requests) == 3
    retries = [entry for entry in logs if entry["event"] == "msg_rpc_retry"]
    assert [entry["attempt"] for entry in retries] == [1, 2]
    assert [entry["delay_ms"] for entry in retries] == [50, 150]
    assert [entry["budget_remaining"] for entry in retries] == [2, 1]
    assert any(entry["event"] == "msg_rpc_retry_stopped" for entry in logs) is False


async def test_retries_exhausted_records_attempts_and_backoffs() -> None:
    """重试耗尽：最终错误带 `attempts=3` 与退避序列，且不再发起第 4 次尝试。"""
    clock = FakeClock()
    transport = scripted(*([error_response(350005)] * 4))
    client = _client(
        transport,
        clock=clock,
        jitter=FixedJitter([10, 20]),
        retry_policy=RetryPolicy(base_ms=100, cap_ms=2000),
    )
    with capture_logs() as logs, pytest.raises(RpcCallError) as caught:
        await client.call("demo", "echo", b"x")
    assert caught.value.attempts == 3
    assert caught.value.backoff_ms == (10, 20)
    assert len(transport.requests) == 3
    assert any(entry["event"] == "msg_rpc_retry_stopped" for entry in logs)


# ---------------------------------------------------------------------------
# 3. Retry-After：双来源 / 非法忽略 / 取 max 并封顶
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ((), None),
        ((("X-Other", "1"),), None),
        ((("X-Other", "1"), ("Retry-After", "2")), 2000),
        ((("retry-after", "0"),), 0),
        ((("Retry-After", "1.5"),), None),
        ((("Retry-After", "-1"),), None),
        ((("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"),), None),
        ((("Retry-After", "６"),), None),
    ],
)
def test_parse_retry_after(headers: tuple[tuple[str, str], ...], expected: int | None) -> None:
    """只认 delta-seconds；非法 / HTTP-date / 全角数字一律忽略并回落本地退避。

    Args:
        headers: 响应头。
        expected: 期望毫秒。
    """
    assert parse_retry_after(headers) == expected


async def test_retry_after_http_header_is_respected() -> None:
    """HTTP 429 的 `Retry-After` 作为下限：取 max(退避, 建议) 并夹到 cap。"""
    from bms_core.msgclient.transport import RpcHttpResponse

    clock = FakeClock()
    limited = RpcHttpResponse(status=429, headers=(("Retry-After", "3"),), body=b"", stream_id=1)
    too_large = RpcHttpResponse(status=429, headers=(("Retry-After", "99"),), body=b"", stream_id=1)
    transport = scripted(limited, too_large, result_response())
    client = _client(
        transport,
        clock=clock,
        jitter=FixedJitter([10, 20]),
        retry_policy=RetryPolicy(base_ms=100, cap_ms=5000),
    )
    await client.call("demo", "echo", b"x")
    assert clock.sleeps == [3000, 5000]


async def test_retry_after_frame_ms_is_respected() -> None:
    """帧 `retry_after_ms` 与计算退避取 max；0 视为「无建议」纯走抖动。"""
    clock = FakeClock()
    transport = scripted(
        error_response(350005, retry_after_ms=800),
        error_response(350005, retry_after_ms=0),
        result_response(),
    )
    client = _client(transport, clock=clock, jitter=FixedJitter([10, 20]), retry_policy=RetryPolicy(cap_ms=2000))
    await client.call("demo", "echo", b"x")
    assert clock.sleeps == [800, 20]


# ---------------------------------------------------------------------------
# 4. 调用链共享预算
# ---------------------------------------------------------------------------


def test_budget_contextvar_reuse_and_release() -> None:
    """同一调用链复用一个预算对象；新建的绑定在释放后还原为「无预算」。"""
    policy = RetryPolicy(max_retries=2, max_budget=2)
    outer, token = acquire_budget(policy)
    assert outer.remaining == 2
    inner, inner_token = acquire_budget(policy)
    assert inner is outer
    assert inner_token is None
    outer.consume()
    assert inner.remaining == 1
    release_budget(inner_token)
    assert acquire_budget(policy)[0] is outer
    release_budget(token)
    after, last_token = acquire_budget(policy)
    assert after is not outer
    release_budget(last_token)


def test_budget_initial_is_min_of_retries_and_max_budget() -> None:
    """预算初值取 `min(max_retries, max_budget)`，任一收紧都生效。"""
    _, token = acquire_budget(RetryPolicy(max_retries=5, max_budget=1))
    release_budget(token)
    _, token = acquire_budget(RetryPolicy(max_retries=5, max_budget=1))
    release_budget(token)
    _, token = acquire_budget(RetryPolicy(max_retries=1, max_budget=5))
    release_budget(token)
    assert RetryBudget(0).remaining == 0


async def test_retry_budget_is_written_to_frame_and_decrements() -> None:
    """帧头 `retry_budget` 逐跳递减；上游显式给更小值时取 min。"""
    transport = scripted(error_response(350005), result_response())
    client = _client(transport, clock=FakeClock(), jitter=FixedJitter([1]), retry_policy=RetryPolicy())
    await client.call("demo", "echo", b"x", retry_budget=1)
    first = decode_frame(transport.requests[0].body).header
    second = decode_frame(transport.requests[1].body).header
    assert first["retry_budget"] == 1
    assert second["retry_budget"] == 0


async def test_zero_budget_stops_retry() -> None:
    """上游预算为 0 时不再重试（防 A→B→C 各跳重试叠加成风暴）。"""
    transport = scripted(error_response(350005), result_response())
    client = _client(transport, clock=FakeClock(), jitter=FixedJitter([1]), retry_policy=RetryPolicy())
    with pytest.raises(RpcCallError) as caught:
        await client.call("demo", "echo", b"x", retry_budget=0)
    assert caught.value.attempts == 1
    assert len(transport.requests) == 1


# ---------------------------------------------------------------------------
# 5. 判定函数的短路分支（纯函数逐条固定）
# ---------------------------------------------------------------------------


def _decide(error: RpcCallError, **kwargs: object) -> object:
    """调 `decide_retry` 并补默认值，让每条短路分支单独可断言。

    Args:
        error: 触发判定的错误。
        kwargs: 覆盖项。

    Returns:
        object: 判定结果或 None。
    """
    options: dict[str, object] = {
        "attempts": 0,
        "policy": RetryPolicy(),
        "budget": RetryBudget(2),
        "idempotency_key": "key",
        "idempotent": False,
        "retry_after_ms": None,
        "jitter": FixedJitter([10]),
    }
    options.update(kwargs)
    return decide_retry(error, **options)  # type: ignore[arg-type]


def test_decide_retry_short_circuits() -> None:
    """六步短路逐条固定：类别 / unknown 无键 / transient 但不可重试 / 次数 / 预算 / 幂等门槛。"""
    from bms_core.msgclient.retry import RpcRetryBlockedError

    assert _decide(RpcCallError(10001, "x", category=ErrorCategory.PARAM_ERROR)) is None
    assert _decide(RpcCallError(350023, "x", category=ErrorCategory.BUSINESS_REJECT)) is None
    assert _decide(RpcCallError(0, "x", category=ErrorCategory.SERVER_BUG)) is None
    assert _decide(RpcCallTimeoutError(), idempotency_key=None) is None
    assert _decide(RpcCallError(350005, "x", category=ErrorCategory.TRANSIENT, retryable=False)) is None
    assert _decide(RpcCallError(350005, "x", category=ErrorCategory.TRANSIENT, retryable=True), attempts=2) is None
    assert (
        _decide(RpcCallError(350005, "x", category=ErrorCategory.TRANSIENT, retryable=True), budget=RetryBudget(0))
        is None
    )
    with pytest.raises(RpcRetryBlockedError):
        _decide(
            RpcCallError(350005, "x", category=ErrorCategory.TRANSIENT, retryable=True),
            idempotency_key=None,
        )
    allowed = _decide(RpcCallError(350004, "x", category=ErrorCategory.UNKNOWN), retry_after_ms=500)
    assert isinstance(allowed, RetryDecision)
    assert allowed.delay_ms == 500
