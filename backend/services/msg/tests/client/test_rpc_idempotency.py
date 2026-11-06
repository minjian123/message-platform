"""04-4 用例之二（Kiwi 2439）：幂等键生成 / 稳定复用 / 非幂等无键阻止 / 服务端去重。

服务端去重直接驱动 `invoke_rpc`（不经 HTTP），把「首次结果复用 / 处理中 / 失败释放 /
作用域隔离 / TTL 过期」五条路径在 fake clock 下逐条固定。
"""

from __future__ import annotations

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from structlog.testing import capture_logs

from bms_msg.api.rpc.idempotency import idempotency_context
from bms_msg.api.rpc.request import optional_budget, optional_text
from bms_msg.broker.backpressure import RateLimiter, default_rate_limit_tuning
from bms_msg.client.retry import RetryPolicy
from bms_msg.client.rpc import RpcCallError, RpcClient, StaticInstanceProvider
from bms_msg.errors import MsgRpcInProgressError
from bms_msg.protocol.frame import decode_frame
from bms_msg.rpc.builtin import default_rpc_registry
from bms_msg.rpc.deadline import Deadline
from bms_msg.rpc.idempotency import (
    IdempotencyContext,
    InProcessIdempotencyStore,
    scoped_idempotency_key,
)
from bms_msg.rpc.registry import RpcRegistry, RpcReply
from bms_msg.rpc.service import invoke_rpc
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
    """构造脚本传输客户端。

    Args:
        transport: 传输替身。
        kwargs: 构造参数。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceProvider([BASE_URL]), transport=transport, **kwargs)  # type: ignore[arg-type]


def _context(store: InProcessIdempotencyStore, key: str) -> IdempotencyContext:
    """构造一个去重上下文。

    Args:
        store: 去重存储。
        key: 作用域键。

    Returns:
        IdempotencyContext: 上下文。
    """
    return IdempotencyContext(store=store, key=key)


async def _invoke(
    *,
    store: InProcessIdempotencyStore | None = None,
    key: str = "scope-a",
    payload: bytes = b"one",
    handler: object = None,
    service: str = "demo",
    method: str = "echo",
) -> RpcReply:
    """直接驱动一次一元调用（可注入自定义 handler 与去重上下文）。

    Args:
        store: 去重存储；None 表示不去重。
        key: 作用域键。
        payload: 调用参数。
        handler: 自定义处理器（None 用 `demo.echo`）。
        service: 服务标识。
        method: 方法名。

    Returns:
        RpcReply: 结果。
    """
    registry = default_rpc_registry() if handler is None else handler  # type: ignore[assignment]
    return await invoke_rpc(
        registry=registry,  # type: ignore[arg-type]
        limiter=RateLimiter(default_rate_limit_tuning()),
        tenant="demo",
        service=service,
        method=method,
        payload=payload,
        headers=ConcurrentStableDict(),
        deadline=Deadline.start(None),
        idempotency=None if store is None else _context(store, key),
    )


# ---------------------------------------------------------------------------
# 1. 键生成 / 稳定复用 / 显式覆盖
# ---------------------------------------------------------------------------


async def test_generated_key_is_stable_across_retries() -> None:
    """客户端生成 `uuid4().hex`，重试全程复用同一键（`request_id` 每次尝试重新生成）。"""
    transport = scripted(error_response(350005), error_response(350005), result_response())
    client = _client(transport, clock=FakeClock(), jitter=FixedJitter([1, 2]), retry_policy=RetryPolicy())
    await client.call("demo", "echo", b"x")
    headers = [decode_frame(request.body).header for request in transport.requests]
    keys = {header.get("idempotency_key") for header in headers}
    assert len(keys) == 1
    generated = next(iter(keys))
    assert isinstance(generated, str) and len(generated) == 32
    assert len({header["request_id"] for header in headers}) == 3


async def test_explicit_key_wins() -> None:
    """调用方显式给的业务键不被自动生成的键覆盖。"""
    transport = scripted(result_response())
    client = _client(transport)
    await client.call("demo", "echo", b"x", idempotency_key="order-42")
    assert decode_frame(transport.requests[0].body).header["idempotency_key"] == "order-42"


async def test_idempotent_call_needs_no_key() -> None:
    """显式声明幂等时不生成键，仍可重试（无键也有幂等保证）。"""
    transport = scripted(error_response(350005), result_response())
    client = _client(transport, clock=FakeClock(), jitter=FixedJitter([1]), retry_policy=RetryPolicy())
    await client.call("demo", "echo", b"x", idempotent=True)
    headers = [decode_frame(request.body).header for request in transport.requests]
    assert all("idempotency_key" not in header for header in headers)
    assert len(transport.requests) == 2


async def test_non_idempotent_without_key_is_blocked() -> None:
    """关闭自动生成键后，非幂等调用遇到可重试错误被阻止，且零重试（任务 §2.4）。"""
    from bms_msg.client.retry import RpcRetryBlockedError

    transport = scripted(error_response(350005), result_response())
    client = _client(
        transport,
        retry_policy=RetryPolicy(auto_idempotency_key=False),
        clock=FakeClock(),
        jitter=FixedJitter([1]),
    )
    with pytest.raises(RpcRetryBlockedError) as caught:
        await client.call("demo", "echo", b"x")
    assert isinstance(caught.value.error, RpcCallError)
    assert len(transport.requests) == 1
    assert "idempotency_key" not in decode_frame(transport.requests[0].body).header


# ---------------------------------------------------------------------------
# 2. 注册表声明 / 契约 / 上下文构造
# ---------------------------------------------------------------------------


def test_registry_declarations_and_contract_entries() -> None:
    """注册表默认声明非幂等；内置方法声明幂等，`declarations()` 按形态分开。"""
    registry = RpcRegistry()
    registry.register("svc", "write", object())  # type: ignore[arg-type]
    assert registry.idempotent("svc", "write") is False
    assert registry.idempotent("svc", "missing") is False
    builtin = default_rpc_registry()
    assert builtin.idempotent("demo", "echo") is True
    assert builtin.idempotent("demo", "ticker") is True
    declarations = builtin.declarations()
    assert len(declarations) == 4
    assert [item.form for item in declarations] == ["一元方法", "一元方法", "流式方法", "流式方法"]


def test_idempotency_context_only_for_keyed_non_idempotent() -> None:
    """仅「带键 + 方法声明非幂等」构造上下文；键按四维作用域拼接。"""
    store = InProcessIdempotencyStore(clock=FakeClock())
    registry = default_rpc_registry()
    assert (
        idempotency_context(store=store, registry=registry, tenant="t", service="demo", method="echo", key=None) is None
    )
    # 内置方法声明幂等 → 不去重（重复执行无副作用）。
    assert (
        idempotency_context(store=store, registry=registry, tenant="t", service="demo", method="echo", key="k") is None
    )
    non_idempotent = RpcRegistry()
    non_idempotent.register("svc", "create", object())  # type: ignore[arg-type]
    ctx = idempotency_context(store=store, registry=non_idempotent, tenant="t", service="svc", method="create", key="k")
    assert ctx is not None
    assert ctx.key == "msg:t:idem:svc.create:k"
    assert scoped_idempotency_key(tenant="t", service="svc", method="create", key="k") == ctx.key


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        (0, 0),
        (3, 3),
    ],
)
def test_optional_budget_valid(raw: object, expected: int | None) -> None:
    """帧头 `retry_budget` 合法形态（缺失 / 零 / 正数）。

    Args:
        raw: 原始值。
        expected: 期望结果。
    """
    assert optional_budget({"retry_budget": raw}, "retry_budget") == expected


@pytest.mark.parametrize("raw", [True, -1, "3", 1.5])
def test_optional_budget_rejects_invalid(raw: object) -> None:
    """布尔 / 负数 / 字符串 / 浮点一律 `10001`，不静默忽略。

    Args:
        raw: 非法值。
    """
    with pytest.raises(ParamError):
        optional_budget({"retry_budget": raw}, "retry_budget")


def test_optional_text_for_idempotency_key() -> None:
    """幂等键走既有的字符串头判据（超长 / 非字符串即 `10001`）。"""
    assert optional_text({"idempotency_key": "k"}, "idempotency_key") == "k"
    with pytest.raises(ParamError):
        optional_text({"idempotency_key": "x" * 129}, "idempotency_key")
    with pytest.raises(ParamError):
        optional_text({"idempotency_key": 7}, "idempotency_key")


# ---------------------------------------------------------------------------
# 3. 服务端去重：首次结果复用 / 处理中 / 失败释放 / 作用域 / TTL
# ---------------------------------------------------------------------------


async def test_scoped_key_replays_first_result() -> None:
    """同一作用域键第二次调用不再执行 handler，原样复用首次结果（payload + headers）。"""
    calls: list[bytes] = []

    async def counting(call: object) -> RpcReply:
        calls.append(call.payload)  # type: ignore[arg-type]
        headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        headers.set("handler", "counting")
        return RpcReply(payload=b"first", headers=headers)

    registry = RpcRegistry()
    registry.register("svc", "create", counting)
    store = InProcessIdempotencyStore(clock=FakeClock())
    first = await _invoke(store=store, key="k", handler=registry, service="svc", method="create")
    second = await _invoke(store=store, key="k", payload=b"two", handler=registry, service="svc", method="create")
    assert first.payload == second.payload == b"first"
    assert dict(second.headers.items()) == {"handler": "counting"}
    assert calls == [b"one"]


async def test_in_progress_error_when_first_not_finished() -> None:
    """`begin` 已被占用且首次结果未写出 → `10007` 处理中错，带建议等待与 reason。"""
    store = InProcessIdempotencyStore(clock=FakeClock())
    assert await store.begin("k") is True
    with pytest.raises(MsgRpcInProgressError) as caught:
        await _invoke(store=store, key="k")
    assert caught.value.code == 10007
    assert caught.value.http_status == 503
    assert caught.value.retry_after_ms > 0
    assert caught.value.data == {"reason": "in_progress"}
    assert await store.load("k") is None


async def test_failed_first_call_releases_key() -> None:
    """首次执行失败 / 被取消 → `release`，带键重试能真正重跑业务。"""
    from bms_msg.errors import MsgRpcMethodNotFoundError

    store = InProcessIdempotencyStore(clock=FakeClock())
    with pytest.raises(MsgRpcMethodNotFoundError):
        await _invoke(store=store, key="k", handler=RpcRegistry())
    assert await store.load("k") is None
    assert await store.begin("k", ttl_ms=1000) is True


async def test_scope_isolates_tenant_service_method() -> None:
    """不同租户 / 服务 / 方法的同名业务键互不误判重复。"""
    store = InProcessIdempotencyStore(clock=FakeClock())
    keys = [
        scoped_idempotency_key(tenant="a", service="svc", method="create", key="k"),
        scoped_idempotency_key(tenant="b", service="svc", method="create", key="k"),
        scoped_idempotency_key(tenant="a", service="other", method="create", key="k"),
        scoped_idempotency_key(tenant="a", service="svc", method="update", key="k"),
    ]
    assert len(set(keys)) == 4
    for key in keys:
        assert await store.begin(key) is True
    assert await store.begin(keys[0]) is False


async def test_ttl_expiry_allows_reexecution() -> None:
    """TTL 到期后惰性清理，同一键可以重新执行。"""
    clock = FakeClock()
    store = InProcessIdempotencyStore(clock=clock, ttl_ms=100)
    assert await store.begin("k", ttl_ms=100) is True
    await store.save("k", RpcReply(payload=b"first"))
    assert await store.begin("k") is False
    clock.advance(101)
    assert await store.begin("k") is True
    assert await store.load("k") is None


async def test_store_save_and_release_with_explicit_ttl() -> None:
    """`save` / `release` 的显式 TTL 与键释放分支。"""
    store = InProcessIdempotencyStore(clock=FakeClock(), ttl_ms=50)
    await store.save("k", RpcReply(payload=b"v"), ttl_ms=500)
    assert (await store.load("k")).payload == b"v"  # type: ignore[union-attr]
    await store.release("k")
    await store.release("k")
    assert await store.load("k") is None
    assert await store.begin("k") is True


async def test_no_context_is_zero_overhead() -> None:
    """`idempotency=None` 时整层透传，不去重表、不写日志之外的新状态。"""
    with capture_logs() as logs:
        first = await _invoke(payload=b"one")
        second = await _invoke(payload=b"two")
    assert first.payload == b"one"
    assert second.payload == b"two"
    assert [entry["event"] for entry in logs] == ["msg_rpc", "msg_rpc"]
