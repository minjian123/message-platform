"""客户端流式消费（任务 04-3-2；Kiwi 2438）：句柄语义、增量切帧、序号校验、结束三态与取消。

判据（对应 04-3-2 详细设计 §6.1 第二个文件）：

1. **惰性打开**：`client.stream()` 构造时不发请求，`async with` / 首次 `__anext__` 才发；请求走
   `/rpc/{service}/{method}/stream`，预算与租户头按一元口径带上。
2. **切帧**：一帧跨两块、一次读含三帧（`_pending` 逐条吐）都正确；不完整尾包留到下一块。
3. **结束三态**：末帧 `end_of_stream` → 本帧照常产出、下一次 `StopAsyncIteration`；`ERROR` 帧按码抛
   （`350004` → `RpcCallTimeoutError`，其余照搬帧头 `layer` / `retryable`）；EOF 无收尾 → `code=0` +
   `layer=protocol`。
4. **序号与关联**：`seq` 必须从 0 单调 +1、`request_id` 必须与本次调用一致，不符即协议错（不诱导按码重试）。
5. **取消与收尾**：`async with` 中途 `break` → 退出时 `abort(cancelled)`；`aclose()` 幂等、不抛流内异常；
   单写 `break` 不发 RST（**已知行为**，用例固定）；任务被取消 → `abort(cancelled)` 且异常原样穿透。
6. **空闲兜底**：注入 `RPC_STREAM_CLIENT_IDLE_MS=20`，服务端不发也不断 → `abort(timeout)` + `RpcCallTimeoutError`。
7. **观测**：正常结束记 `msg_rpc_client_stream`（`frames` / `bytes` / `seq_max` / `duration_ms`），中止记
   `msg_rpc_client_cancel`（`reason` / `rst_sent` / `frames`），两者不合并。

**为什么大量用 `ScriptedRpcTransport`**：切帧与三态的判据要的是「第几块里有几帧」这种精确时序，
只有脚本能摆出来；真 app / 真 hypercorn 的行为分别由 ASGI 用例与 `test_rpc_stream_h2c.py` 兜住
（ASGI 面只验协议与异常映射，这是详细设计 §9 风险 2 的处置）。
"""

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame, encode_frame
from bms_core.msgclient.rpc import RpcCallError, RpcCallTimeoutError, RpcClient, StaticInstanceSelector
from bms_core.msgclient.stream import RpcStream
from bms_core.msgclient.transport import (
    RpcHttpRequest,
    RpcTransportClosedError,
    RpcTransportProtocolError,
    RpcTransportTimeoutError,
)
from bms_core.msgclient.transport.stream import CLOSE_CANCELLED
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from structlog.testing import capture_logs

from bms_msg.api.frame.responses import error_frame
from bms_msg.api.rpc.responses import rpc_stream_frame
from bms_msg.rpc.registry import RpcReply
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_transport import AsgiRpcTransport, ScriptedRpcTransport

pytestmark = pytest.mark.kiwi_id(2438)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

SERVICE = "demo"
"""服务标识（与内置 `demo.*` 处理器一致）。"""

METHOD = "ticker"
"""流式方法名。"""

BUDGET_MS = 30_000
"""用例里的预算（毫秒；只进帧头，不做本地倒计时）。"""


def _frame(
    request_id: str,
    seq: int,
    payload: bytes = b"",
    *,
    end: bool = False,
    headers: dict[str, str] | None = None,
) -> bytes:
    """用**服务端同一个编码器**编一帧流结果（帧形状与生产同源）。

    Args:
        request_id: 请求标识（逐帧回显）。
        seq: 帧序号。
        payload: 结果字节。
        end: 是否置 `end_of_stream`。
        headers: 结果头。

    Returns:
        bytes: 帧字节。
    """
    reply = RpcReply(payload=payload)
    if headers:
        for key, value in headers.items():
            reply.headers.set(key, value)
    return rpc_stream_frame(
        request_id=request_id,
        service=SERVICE,
        method=METHOD,
        reply=reply,
        seq=seq,
        used_ms=0,
        remaining_ms=BUDGET_MS,
        end_of_stream=end,
    )


def _rid_of(request: RpcHttpRequest) -> str:
    """从请求体里取本次 `request_id`（脚本块要按它编帧）。

    Args:
        request: 传输层入参。

    Returns:
        str: 请求标识。
    """
    return str(decode_frame(request.body).header["request_id"])


def _client(transport: ScriptedRpcTransport | AsgiRpcTransport) -> RpcClient:
    """构造被测客户端（生产 `RpcClient` + 注入的传输层）。

    Args:
        transport: 传输层。

    Returns:
        RpcClient: 客户端。
    """
    return RpcClient(StaticInstanceSelector(["http://msg:8000"]), tenant="demo", transport=transport)


def _scripted(*chunks: bytes, **kwargs: Any) -> ScriptedRpcTransport:
    """构造按固定块序列作答的替身。

    Args:
        *chunks: 依次交出的块。
        **kwargs: 透传给 `ScriptedRpcTransport`（`stream_error` / `stream_hold` / `open_error`）。

    Returns:
        ScriptedRpcTransport: 替身。
    """
    return ScriptedRpcTransport(stream_chunks=chunks, **kwargs)


def _events(logs: Sequence[Any], name: str) -> list[Any]:
    """过滤出指定事件名。

    Args:
        logs: `capture_logs()` 的结果。
        name: 事件名。

    Returns:
        list[Any]: 匹配的事件。
    """
    return [event for event in logs if event.get("event") == name]


# ---------------------------------------------------------------------------
# 1. 惰性打开与请求形状
# ---------------------------------------------------------------------------


async def test_lazy_open_sends_request_only_on_enter() -> None:
    """构造句柄不发请求；`async with` 进入时才发，路径 / 预算 / 租户头 / 帧头都对。"""
    transport = _scripted(_frame("placeholder", 0, end=True))
    client = _client(transport)
    stream = client.stream(SERVICE, METHOD, b"3", headers={"k": "v"}, deadline_ms=BUDGET_MS, trace_id="t-1")

    assert transport.open_requests == [], "构造句柄时不该发请求（惰性打开）"
    assert transport.requests == []

    async with stream:
        assert len(transport.open_requests) == 1
        request = transport.open_requests[0]
        assert request.target == f"/rpc/{SERVICE}/{METHOD}/stream"
        assert ("X-Tenant-ID", "demo") in request.headers
        header = decode_frame(request.body).header
        assert header["service"] == SERVICE
        assert header["method"] == METHOD
        assert header["deadline_ms"] == BUDGET_MS
        assert header["headers"] == {"k": "v"}
        assert header["trace_id"] == "t-1"

    await client.aclose()
    assert transport.closed


async def test_iterating_without_context_opens_lazily() -> None:
    """不写 `async with` 时，首次 `__anext__` 也会打开（惰性打开的另一个入口）。"""
    transport = ScriptedRpcTransport(stream_chunks=lambda request: [_frame(_rid_of(request), 0, b"only", end=True)])
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    assert transport.open_requests == []

    first = await replies.__anext__()
    assert first.payload == b"only"
    with pytest.raises(StopAsyncIteration):
        await replies.__anext__()

    await replies.aclose()
    assert len(transport.open_requests) == 1


# ---------------------------------------------------------------------------
# 2. 正常结束与切帧
# ---------------------------------------------------------------------------


async def test_normal_stream_iterates_and_stops() -> None:
    """3 帧 + 末帧 `end_of_stream`：逐帧产出、第 4 次 `StopAsyncIteration`，并记正常结束事件。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [
            _frame(_rid_of(request), 0, b"0"),
            _frame(_rid_of(request), 1, b"1"),
            _frame(_rid_of(request), 2, b"2", end=True, headers={"index": "2"}),
        ]
    )
    client = _client(transport)
    with capture_logs() as logs:
        async with client.stream(SERVICE, METHOD, b"3") as replies:
            payloads = [reply.payload async for reply in replies]
            assert replies.__aiter__() is replies, "句柄自身即迭代器（单消费者）"

    assert payloads == [b"0", b"1", b"2"]
    assert transport.aborts == [], "正常结束不该发 RST_STREAM"

    finished = _events(logs, "msg_rpc_client_stream")
    assert len(finished) == 1, f"应恰好记一条正常结束事件，实际 {finished!r}"
    event = finished[0]
    assert event["request_id"]
    assert event["service"] == SERVICE
    assert event["method"] == METHOD
    assert event["stream_id"] == 7
    assert event["frames"] == 3
    assert event["bytes"] == 3
    assert event["seq_max"] == 2
    assert isinstance(event["duration_ms"], int)
    assert not _events(logs, "msg_rpc_client_cancel"), "正常结束不该记取消事件"


async def test_frame_across_two_chunks_is_reassembled() -> None:
    """一帧横跨两块：不完整尾包留到下一块，切帧结果仍完整。"""
    transport = ScriptedRpcTransport(stream_chunks=lambda request: _split(_rid_of(request)))
    client = _client(transport)

    async with client.stream(SERVICE, METHOD) as replies:
        payloads = [reply.payload async for reply in replies]

    assert payloads == [b"alpha", b"beta"]


def _split(request_id: str) -> tuple[bytes, ...]:
    """把两帧字节切成「前 5 字节 + 其余」（第一块里连帧头都不完整）。

    Args:
        request_id: 请求标识。

    Returns:
        tuple[bytes, ...]: 两块字节。
    """
    stream = _frame(request_id, 0, b"alpha") + _frame(request_id, 1, b"beta", end=True)
    return stream[:5], stream[5:]


async def test_single_chunk_with_three_frames_is_drained_one_by_one() -> None:
    """一次 `read()` 拿到三帧：`_pending` 逐条吐出，不丢不重。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [
            b"".join(
                [
                    _frame(_rid_of(request), 0, b"x"),
                    _frame(_rid_of(request), 1, b"y"),
                    _frame(_rid_of(request), 2, b"z", end=True),
                ]
            )
        ]
    )
    client = _client(transport)

    async with client.stream(SERVICE, METHOD) as replies:
        payloads = [reply.payload async for reply in replies]

    assert payloads == [b"x", b"y", b"z"]
    assert len(transport.open_requests) == 1, "三帧同块也只该打开一次"


# ---------------------------------------------------------------------------
# 3. 结束三态与协议错
# ---------------------------------------------------------------------------


async def test_seq_gap_is_protocol_error() -> None:
    """`seq` 跳号 → `code=0` / `layer=protocol`，并记一条 `reason=error` 的中止事件。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [
            _frame(_rid_of(request), 0, b"a"),
            _frame(_rid_of(request), 5, b"b", end=True),
        ]
    )
    client = _client(transport)
    with capture_logs() as logs, pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()
            await replies.__anext__()

    assert excinfo.value.code == 0
    assert excinfo.value.layer == "protocol"
    assert "seq" in str(excinfo.value)
    assert transport.aborts == ["error"], "协议错应中止本流"
    cancelled = _events(logs, "msg_rpc_client_cancel")
    assert len(cancelled) == 1
    assert cancelled[0]["reason"] == "error"
    assert cancelled[0]["rst_sent"] is True
    assert cancelled[0]["frames"] == 1


async def test_request_id_mismatch_is_protocol_error() -> None:
    """`request_id` 与本次调用不一致 → 协议错（多路流串帧必须当场发现）。"""
    transport = _scripted(_frame("someone-else", 0, b"a", end=True))
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert (excinfo.value.code, excinfo.value.layer) == (0, "protocol")
    assert "request_id" in str(excinfo.value)


async def test_other_frame_type_is_protocol_error() -> None:
    """流里出现非 `48+response` 的帧 → 协议错。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [
            encode_frame(
                frame_type=int(FrameType.PUBLISH),
                flags=FrameFlags.RESPONSE,
                header={"request_id": _rid_of(request)},
            )
        ]
    )
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert (excinfo.value.code, excinfo.value.layer) == (0, "protocol")
    assert "帧类型不符" in str(excinfo.value)


async def test_error_frame_maps_deadline() -> None:
    """流内 `350004` → `RpcCallTimeoutError`（与服务端预算耗尽同一异常）。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [error_frame(code=350004, message="预算耗尽", request_id=_rid_of(request))]
    )
    client = _client(transport)

    with pytest.raises(RpcCallTimeoutError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert excinfo.value.code == 350004
    assert excinfo.value.budget_ms == BUDGET_MS
    assert excinfo.value.outcome == "unknown"
    assert transport.aborts == [], "ERROR 帧是终止帧：服务端已收尾，不该再发 RST"


async def test_error_frame_maps_business_code_and_keeps_server_classification() -> None:
    """流内其它错误码 → `RpcCallError`，`layer` / `retryable` / `retry_after_ms` 全部照搬帧头。"""
    payload = error_frame(code=350024, message="流空闲超时", request_id="r-1", retry_after_ms=None)
    header = decode_frame(payload).header
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [error_frame(code=350024, message="流空闲超时", request_id=_rid_of(request))]
    )
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert excinfo.value.code == 350024
    assert excinfo.value.layer == header["layer"], "错误层必须照搬服务端判定"
    assert excinfo.value.retryable is bool(header["retryable"])
    assert excinfo.value.retry_after_ms is None


async def test_eof_without_terminator_is_protocol_error() -> None:
    """EOF 但既无 `end_of_stream` 也无 `ERROR` 帧 → 协议错（不得按 EOF 猜结束）。"""
    transport = ScriptedRpcTransport(stream_chunks=lambda request: [_frame(_rid_of(request), 0, b"a")])
    client = _client(transport)
    with capture_logs() as logs, pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            assert (await replies.__anext__()).payload == b"a"
            await replies.__anext__()

    assert (excinfo.value.code, excinfo.value.layer) == (0, "protocol")
    assert "异常中断" in str(excinfo.value)
    assert transport.aborts == [], "流已 EOF：没有可中止的东西，不该再发 RST_STREAM"
    cancelled = _events(logs, "msg_rpc_client_cancel")
    assert len(cancelled) == 1
    assert cancelled[0]["reason"] == "error"
    assert cancelled[0]["rst_sent"] is False


async def test_undecodable_frame_is_protocol_error() -> None:
    """字节流不是合法帧（magic 不符）→ 协议错（不把解码异常漏给调用方）。"""
    transport = _scripted(b"not-a-frame-at-all")
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert (excinfo.value.code, excinfo.value.layer) == (0, "protocol")
    assert "不可解析" in str(excinfo.value)


# ---------------------------------------------------------------------------
# 4. 取消与收尾
# ---------------------------------------------------------------------------


async def test_async_with_break_sends_reset_on_exit() -> None:
    """`async with` 中途 `break`：退出时发 `RST_STREAM(CANCEL)`，并记 `frames` 已产出数。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [_frame(_rid_of(request), index, b"x") for index in range(5)]
    )
    client = _client(transport)
    with capture_logs() as logs:
        async with client.stream(SERVICE, METHOD) as replies:
            async for reply in replies:
                assert reply.payload == b"x"
                break

    assert transport.aborts == [CLOSE_CANCELLED], "退出上下文应发 RST_STREAM(CANCEL)"
    cancelled = _events(logs, "msg_rpc_client_cancel")
    assert len(cancelled) == 1
    assert cancelled[0]["reason"] == CLOSE_CANCELLED
    assert cancelled[0]["rst_sent"] is True
    assert cancelled[0]["frames"] == 1
    assert not _events(logs, "msg_rpc_client_stream"), "没跑完就不该记正常结束"


async def test_break_without_context_is_known_leak() -> None:
    """**已知行为**：只写 `break` 不退出上下文 → 不发 `RST_STREAM`（语言层不通知迭代器）。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [_frame(_rid_of(request), index, b"x") for index in range(3)]
    )
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    async for _reply in replies:
        break

    assert transport.aborts == [], "单写 break 不会通知迭代器，这是被固定的已知行为"

    await replies.aclose()
    assert transport.aborts == [CLOSE_CANCELLED], "显式 aclose() 才是唯一可靠的清理口"


async def test_aclose_is_idempotent() -> None:
    """`aclose()` 二次调用不发第二帧、不重复记事件。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [_frame(_rid_of(request), index, b"x") for index in range(3)]
    )
    client = _client(transport)
    with capture_logs() as logs:
        replies = client.stream(SERVICE, METHOD)
        await replies.__anext__()
        await replies.aclose()
        await replies.aclose()

    assert transport.aborts == [CLOSE_CANCELLED]
    assert len(_events(logs, "msg_rpc_client_cancel")) == 1


async def test_aclose_after_normal_end_does_nothing() -> None:
    """正常结束后再 `aclose()`：不发帧、不再记事件（幂等且无副作用）。"""
    transport = ScriptedRpcTransport(stream_chunks=lambda request: [_frame(_rid_of(request), 0, b"x", end=True)])
    client = _client(transport)
    with capture_logs() as logs:
        async with client.stream(SERVICE, METHOD) as replies:
            payloads = [reply.payload async for reply in replies]
        await replies.aclose()

    assert payloads == [b"x"]
    assert transport.aborts == []
    assert not _events(logs, "msg_rpc_client_cancel")


async def test_aclose_does_not_raise_stream_error() -> None:
    """`aclose()` 不抛流内异常：错误帧已经把异常交给调用方了，清理路径不能再抛。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [error_frame(code=350023, message="未注册", request_id=_rid_of(request))]
    )
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    with pytest.raises(RpcCallError):
        await replies.__anext__()

    await replies.aclose()
    assert transport.aborts == [], "已因错误帧收口的流不再发 RST"


async def test_aclose_before_open_is_noop() -> None:
    """从未打开就 `aclose()`：不发请求、不发帧（惰性打开的另一面）。"""
    transport = _scripted()
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    await replies.aclose()

    assert transport.open_requests == []
    assert transport.aborts == []


async def test_iterating_after_close_is_error() -> None:
    """关闭后再迭代：明确报「已关闭」，而不是静默吐空。"""
    transport = _scripted()
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    await replies.aclose()

    with pytest.raises(RpcCallError, match="已关闭"):
        await replies.__anext__()


async def test_cancel_during_read_aborts_stream() -> None:
    """读取被 `task.cancel()`：本流中止（发 RST），`CancelledError` 原样穿透。"""
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [_frame(_rid_of(request), 0, b"x")], stream_hold=True
    )
    client = _client(transport)
    replies = client.stream(SERVICE, METHOD)
    assert (await replies.__anext__()).payload == b"x"

    task = asyncio.create_task(replies.__anext__())
    await asyncio.sleep(0.01)  # 让任务卡在 read() 上
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert transport.aborts == [CLOSE_CANCELLED], "取消必须让服务端停下"
    await replies.aclose()


# ---------------------------------------------------------------------------
# 5. 空闲兜底与打开阶段异常
# ---------------------------------------------------------------------------


async def test_idle_timeout_aborts_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    """单次读空闲超 90s（用例注入 20ms）→ `abort(timeout)` + `RpcCallTimeoutError`。"""
    monkeypatch.setattr("bms_core.msgclient.stream.RPC_STREAM_CLIENT_IDLE_MS", 20)
    transport = ScriptedRpcTransport(
        stream_chunks=lambda request: [_frame(_rid_of(request), 0, b"x")], stream_hold=True
    )
    client = _client(transport)
    with capture_logs() as logs, pytest.raises(RpcCallTimeoutError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            assert (await replies.__anext__()).payload == b"x"
            await replies.__anext__()

    assert excinfo.value.code == 350004
    assert "空闲超时" in str(excinfo.value)
    assert excinfo.value.budget_ms == BUDGET_MS
    assert transport.aborts == ["timeout"], "空闲兜底应发 RST_STREAM"
    cancelled = _events(logs, "msg_rpc_client_cancel")[0]
    assert cancelled["reason"] == "timeout"
    assert cancelled["frames"] == 1


async def test_open_timeout_maps_to_call_timeout() -> None:
    """打开阶段超时 → `RpcCallTimeoutError`（`350004`）。"""
    transport = _scripted(open_error=RpcTransportTimeoutError("超时"))
    client = _client(transport)

    with pytest.raises(RpcCallTimeoutError) as excinfo:
        async with client.stream(SERVICE, METHOD):
            pass

    assert excinfo.value.code == 350004
    assert excinfo.value.budget_ms == BUDGET_MS


async def test_open_protocol_error_maps_to_protocol_layer() -> None:
    """打开阶段协议错 → `code=0` / `layer=protocol`。"""
    transport = _scripted(open_error=RpcTransportProtocolError("ALPN 未协商到 h2"))
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD):
            pass

    assert (excinfo.value.code, excinfo.value.layer) == (0, "protocol")


async def test_open_closed_maps_to_transport_layer() -> None:
    """打开阶段连接失败 → `code=0` / `layer=transport`。"""
    transport = _scripted(open_error=RpcTransportClosedError("连接不可用"))
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD):
            pass

    assert (excinfo.value.code, excinfo.value.layer) == (0, "transport")


@pytest.mark.parametrize(
    ("error", "layer"),
    [
        (RpcTransportClosedError("对端 reset"), "transport"),
        (RpcTransportProtocolError("帧不可解析"), "protocol"),
    ],
)
async def test_read_transport_error_maps_by_layer(error: Exception, layer: str) -> None:
    """读中传输层错误：按层映射（`transport` / `protocol`），并中止本流。

    Args:
        error: 脚本在读完后抛出的异常。
        layer: 期望的错误层。
    """
    transport = _scripted(stream_error=error)
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            await replies.__anext__()

    assert (excinfo.value.code, excinfo.value.layer) == (0, layer)
    assert transport.aborts == ["error"]


# ---------------------------------------------------------------------------
# 6. ASGI 面（进程内真 app：协议与异常映射）
# ---------------------------------------------------------------------------


async def test_asgi_stream_reads_real_frames(msg_app: FastAPI) -> None:
    """进程内真 app：`demo.ticker` 推 3 帧，逐帧到达且 `seq` 连续。"""
    transport = AsgiRpcTransport(msg_app)
    client = _client(transport)

    with capture_logs() as logs:
        async with client.stream(SERVICE, METHOD, b"3") as replies:
            got = [(reply.payload, reply.headers.get("index")) async for reply in replies]

    assert got == [(b"0", "0"), (b"1", "1"), (b"2", "2")]
    assert not _events(logs, "msg_rpc_client_cancel"), "真 app 正常推完不该有中止事件"
    assert _events(logs, "msg_rpc_client_stream")[0]["frames"] == 3


async def test_asgi_error_frame_before_stream_is_raised(msg_app: FastAPI) -> None:
    """非流式方法走 `/stream`：服务端回 200 + 单帧 `350023`，客户端按码抛（建流前失败也是帧）。"""
    transport = AsgiRpcTransport(msg_app)
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, "echo", b"hi") as replies:
            await replies.__anext__()

    assert excinfo.value.code == 350023


async def test_asgi_open_rejects_non_200() -> None:
    """非 200（路由 / 权限 / 网关层）在打开阶段就失败，不落到「无收尾帧」那种误导性协议错。"""
    app = FastAPI()

    @app.post(f"/rpc/{SERVICE}/{METHOD}/stream")
    async def denied() -> JSONResponse:
        """固定拒绝（模拟网关 / 权限拦截）。

        Returns:
            JSONResponse: 403。
        """
        return JSONResponse({"code": 30001}, status_code=403)

    transport = AsgiRpcTransport(app)
    client = _client(transport)

    with pytest.raises(RpcCallError) as excinfo:
        async with client.stream(SERVICE, METHOD) as replies:
            pytest.fail(f"非 200 却拿到了流：{replies!r}")

    assert (excinfo.value.code, excinfo.value.layer) == (0, "transport")
    assert "HTTP 403" in str(excinfo.value)


def test_stream_handle_type() -> None:
    """`RpcClient.stream()` 返回 `RpcStream` 句柄（不是生成器：生成器 `break` 后无法确定性取消）。"""
    assert isinstance(_client(_scripted()).stream(SERVICE, METHOD), RpcStream)
