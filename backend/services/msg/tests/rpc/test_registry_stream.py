"""流式方法注册表与内置流处理器（任务 04-3-1；Kiwi 2437）。

**为什么不并进 `test_registry.py`**：那个文件是 04-1 的用例（Kiwi 2433），一条用例号对应一套判据；
把流式登记塞进去只能两条 Kiwi 混标一个文件，后续按号追溯会分不清「这文件属于哪次交付」。

判据：

1. **两形态各一张表**：一元 `resolve()` 看不到流式方法、`resolve_stream()` 看不到一元方法，
   `methods()` 与 `stream_methods()` 各出各的清单；
2. **冲突即拒启**：同形态重复登记、跨形态同名登记都抛 `RpcRegistrationError`；
3. **`services()` 两形态合并去重**（配额键维度：只有流式方法的服务同样消耗配额）；
4. **内置流方法**：`demo.ticker` 按 ASCII 十进制条数推 N 帧（空 = 0 帧、非数字 / 越界 `10001`）；
   `demo.idle` 推 1 帧后不再产出。
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.rpc.builtin import (
    DEMO_IDLE,
    DEMO_SERVICE,
    DEMO_TICKER,
    TICKER_MAX_FRAMES,
    default_rpc_registry,
    idle_handler,
    ticker_handler,
)
from bms_msg.rpc.registry import RpcCall, RpcRegistrationError, RpcRegistry, RpcReply

pytestmark = pytest.mark.kiwi_id(2437)


def _call(payload: bytes = b"", *, method: str = DEMO_TICKER) -> RpcCall:
    """构造一次流式调用入参。

    Args:
        payload: 参数原始字节。
        method: 方法名。

    Returns:
        RpcCall: 调用入参。
    """
    headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    headers.set("x-biz", "audit")
    return RpcCall(service=DEMO_SERVICE, method=method, payload=payload, headers=headers, tenant="demo")


async def _noop(call: RpcCall) -> RpcReply:
    """一元处理器替身（只为占位，注册表不校验返回值）。

    Args:
        call: 调用入参（未使用）。

    Returns:
        RpcReply: 空结果。
    """
    del call
    return RpcReply()


async def _stream(call: RpcCall) -> AsyncIterator[RpcReply]:
    """流式处理器替身（产出一帧即结束）。

    Args:
        call: 调用入参（未使用）。

    Yields:
        RpcReply: 唯一一帧。
    """
    del call
    yield RpcReply(payload=b"s")


def test_register_stream_and_resolve() -> None:
    """流式登记后可解析；一元表看不到它（形态隔离）。"""
    registry = RpcRegistry()
    registry.register_stream(DEMO_SERVICE, DEMO_TICKER, _stream)

    assert registry.resolve_stream(DEMO_SERVICE, DEMO_TICKER) is _stream
    assert registry.resolve(DEMO_SERVICE, DEMO_TICKER) is None
    assert registry.resolve_stream(DEMO_SERVICE, "missing") is None
    assert len(registry) == 1


def test_unary_handler_is_not_streamable() -> None:
    """一元登记后 `resolve_stream()` 返回 None（一元方法不能被流式端点调起）。"""
    registry = RpcRegistry()
    registry.register(DEMO_SERVICE, "echo", _noop)

    assert registry.resolve_stream(DEMO_SERVICE, "echo") is None
    assert registry.methods() == ((DEMO_SERVICE, "echo"),)
    assert registry.stream_methods() == ()


def test_duplicate_stream_registration_rejected() -> None:
    """同一 `(service, method)` 二次登记流式处理器 → 拒启。"""
    registry = RpcRegistry()
    registry.register_stream(DEMO_SERVICE, DEMO_TICKER, _stream)

    with pytest.raises(RpcRegistrationError, match="重复登记"):
        registry.register_stream(DEMO_SERVICE, DEMO_TICKER, _stream)


def test_form_conflict_rejected_both_directions() -> None:
    """同一方法只能一种形态：先一元后流式、先流式后一元都拒启。"""
    unary_first = RpcRegistry()
    unary_first.register(DEMO_SERVICE, "echo", _noop)
    with pytest.raises(RpcRegistrationError, match="形态冲突"):
        unary_first.register_stream(DEMO_SERVICE, "echo", _stream)

    stream_first = RpcRegistry()
    stream_first.register_stream(DEMO_SERVICE, "echo", _stream)
    with pytest.raises(RpcRegistrationError, match="形态冲突"):
        stream_first.register(DEMO_SERVICE, "echo", _noop)


def test_stream_methods_keep_insertion_order_and_services_merge() -> None:
    """流式清单保插入序；`services()` 两形态合并去重且一元在前。"""
    registry = RpcRegistry()
    registry.register("b", "one", _noop)
    registry.register_stream("c", "two", _stream)
    registry.register_stream("b", "three", _stream)

    assert registry.methods() == (("b", "one"),)
    assert registry.stream_methods() == (("c", "two"), ("b", "three"))
    assert registry.services() == ("b", "c")
    assert len(registry) == 3


def test_default_registry_exposes_two_stream_methods() -> None:
    """缺省注册表：两个一元 + 两个流式（装配与契约导出共用同一构造入口）。"""
    registry = default_rpc_registry()

    assert registry.methods() == ((DEMO_SERVICE, "echo"), (DEMO_SERVICE, "length"))
    assert registry.stream_methods() == ((DEMO_SERVICE, DEMO_TICKER), (DEMO_SERVICE, DEMO_IDLE))


async def test_ticker_handler_emits_sequential_frames() -> None:
    """`demo.ticker` 按条数推帧：payload 为序号文本、结果头带 `index`。"""
    frames = [reply async for reply in ticker_handler(_call(b"3"))]

    assert [reply.payload for reply in frames] == [b"0", b"1", b"2"]
    assert [dict(reply.headers.items()) for reply in frames] == [
        {"handler": "demo.ticker", "index": "0"},
        {"handler": "demo.ticker", "index": "1"},
        {"handler": "demo.ticker", "index": "2"},
    ]


async def test_ticker_handler_empty_payload_emits_nothing() -> None:
    """空参数 = 0 帧（空流用例的目标）。"""
    assert [reply async for reply in ticker_handler(_call(b""))] == []


async def test_ticker_handler_rejects_non_numeric() -> None:
    """非 ASCII 十进制 → 契约层 `10001`（服务层转流内 ERROR 帧）。"""
    with pytest.raises(ParamError, match="ASCII 十进制"):
        await ticker_handler(_call(b"many")).__anext__()


async def test_ticker_handler_rejects_out_of_range() -> None:
    """负数 / 超上限 → `10001`（内置方法不该被当压测装置）。"""
    with pytest.raises(ParamError, match="超出范围"):
        await ticker_handler(_call(str(TICKER_MAX_FRAMES + 1).encode())).__anext__()


async def test_idle_handler_emits_one_frame_then_waits() -> None:
    """`demo.idle` 推 1 帧后不再产出（等取消 / 等空闲超时）。"""
    stream = idle_handler(_call(b"", method=DEMO_IDLE))

    first = await stream.__anext__()
    assert first.payload == b"idle"
    assert dict(first.headers.items()) == {"handler": "demo.idle"}
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(stream.__anext__(), timeout=0.05)
