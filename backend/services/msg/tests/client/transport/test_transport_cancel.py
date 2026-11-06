"""H2 传输层 · 取消发 RST 与观测（任务 04-7-2；Kiwi 2436）：取消 / 超时 / 异常三路中止 + `rst_sent`。

判据：

1. **取消即中止本流**：`task.cancel()` → 服务端收到 `RST_STREAM(CANCEL=8)`，同连接另一条流照常 200；
2. **超时同路**：`timeout_ms` 到点同样中止本流（否则「客户端走了、服务端还在算」）；
3. **已收口的流不再发帧**：对已关闭流发 RST 属协议违规，`rst_sent` 必须如实为 False；
4. **`close_reason` 六态**与 `abort()` 幂等（重复 `abort` 不改写原因、不吞已发事实）；
5. **`msg_rpc_client_cancel` 字段齐全**：`request_id` / `service` / `method` / `stream_id` / `reason` /
   `rst_sent` / `elapsed_ms`——这条事件要能独立回答「RST 发了没有、是哪条流、为什么不等了」。
"""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import cast

import pytest
from structlog.testing import capture_logs

from bms_msg.client.transport import (
    H2Transport,
    RpcHttpRequest,
    RpcRequestTrace,
    RpcTransportClosedError,
    RpcTransportTimeoutError,
)
from bms_msg.client.transport.connection import H2ClientConnection
from bms_msg.client.transport.stream import (
    CLOSE_CANCELLED,
    CLOSE_CONNECTION_ERROR,
    CLOSE_DONE,
    CLOSE_ERROR,
    CLOSE_PEER_RESET,
    CLOSE_TIMEOUT,
    H2Stream,
    abort_reason_of,
)
from tests_support.h2_server import H2TestServer

pytestmark = pytest.mark.kiwi_id(2436)

HEADERS: tuple[tuple[str, str], ...] = (("content-type", "application/x-msg-frame"),)
"""请求头。"""

TRACE = RpcRequestTrace(request_id="req-1", service="demo", method="slow")
"""日志关联信息（断言 `msg_rpc_client_cancel` 的关联字段）。"""


def _request(
    origin: str,
    target: str = "/ok",
    *,
    timeout_ms: int = 5000,
    trace: RpcRequestTrace | None = TRACE,
) -> RpcHttpRequest:
    """构造传输层请求。

    Args:
        origin: 实例基址。
        target: 请求路径。
        timeout_ms: 本地超时。
        trace: 日志关联信息（None 表示调用方不要求关联）。

    Returns:
        RpcHttpRequest: 请求。
    """
    return RpcHttpRequest(
        origin=origin,
        target=target,
        headers=HEADERS,
        body=b"",
        timeout_ms=timeout_ms,
        trace=trace,
    )


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    """等到条件成立。

    Args:
        predicate: 条件。
        timeout: 超时（秒）。

    Raises:
        AssertionError: 超时未成立。
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("等待超时")
        await asyncio.sleep(0.01)


def _cancel_event(events: Sequence[Mapping[str, object]]) -> Mapping[str, object]:
    """从捕获的日志里取出那条 `msg_rpc_client_cancel`。

    Args:
        events: `capture_logs()` 捕获的事件列表。

    Returns:
        Mapping[str, object]: 唯一一条取消事件。

    Raises:
        AssertionError: 事件条数不是 1。
    """
    matched = [event for event in events if event.get("event") == "msg_rpc_client_cancel"]
    assert len(matched) == 1, f"应恰好记一条取消事件，实际 {matched!r}"
    return matched[0]


async def test_cancel_sends_reset_and_logs_rst_sent() -> None:
    """取消在途调用：服务端收到 `RST_STREAM(CANCEL)`，日志给出「RST 发了 / 是哪条流」。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with capture_logs() as events:
                held = asyncio.create_task(transport.send(_request(server.url, "/gated")))
                await _wait_until(lambda: transport.stats().open_streams == 1)
                held.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await held
                await _wait_until(lambda: server.reset_recorded(1))

            assert ("reset", 1, 8) in server.frames, "取消应发 RST_STREAM(CANCEL=8)"
            event = _cancel_event(events)
            assert event["request_id"] == "req-1"
            assert event["service"] == "demo"
            assert event["method"] == "slow"
            assert event["stream_id"] == 1
            assert event["reason"] == CLOSE_CANCELLED
            assert event["rst_sent"] is True
            assert isinstance(event["elapsed_ms"], int)
            assert event["elapsed_ms"] >= 0
            # 取消不关整条连接：同连接再发一次照常成功。
            assert (await transport.send(_request(server.url, "/ok"))).status == 200
            assert transport.stats().connections == 1
            assert transport.stats().open_streams == 0
        finally:
            await transport.aclose()


async def test_cancel_without_trace_logs_null_identity() -> None:
    """调用方没给 `trace`（如健康探测）：事件照记，关联字段为 None 而不是编造。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with capture_logs() as events:
                held = asyncio.create_task(transport.send(_request(server.url, "/gated", trace=None)))
                await _wait_until(lambda: transport.stats().open_streams == 1)
                held.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await held
                await _wait_until(lambda: server.reset_recorded(1))

            event = _cancel_event(events)
            assert event["request_id"] is None
            assert event["service"] is None
            assert event["method"] is None
            assert event["rst_sent"] is True
        finally:
            await transport.aclose()


async def test_timeout_sends_reset_and_raises() -> None:
    """本地超时走同一条中止路径：发 RST、抛 `RpcTransportTimeoutError`、`reason=timeout`。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with capture_logs() as events:
                with pytest.raises(RpcTransportTimeoutError):
                    await transport.send(_request(server.url, "/slow", timeout_ms=100))
                await _wait_until(lambda: server.reset_recorded(1))

            event = _cancel_event(events)
            assert event["reason"] == CLOSE_TIMEOUT
            assert event["rst_sent"] is True
            assert transport.stats().open_streams == 0
        finally:
            await transport.aclose()


async def test_connection_failure_logs_error_without_rst() -> None:
    """连接被对端关掉：中止原因记 `error`，但流已被连接故障收口 → `rst_sent=false`（无帧可发）。"""
    async with H2TestServer() as server:
        transport = H2Transport()
        try:
            with capture_logs() as events, pytest.raises(RpcTransportClosedError):
                await transport.send(_request(server.url, "/close"))

            event = _cancel_event(events)
            assert event["reason"] == CLOSE_ERROR
            assert event["rst_sent"] is False
        finally:
            await transport.aclose()


class _RecordingConnection:
    """只记录 `abort_stream` 调用的连接替身（流语义单测不需要真 socket）。"""

    def __init__(self, *, sent: bool) -> None:
        """初始化。

        Args:
            sent: `abort_stream` 的返回值（True = 流仍在表里、真的发了帧）。
        """
        self.sent = sent
        self.aborted: list[int] = []

    def abort_stream(self, stream_id: int) -> bool:
        """记录一次中止请求。

        Args:
            stream_id: h2 流号。

        Returns:
            bool: 构造时指定的「是否发帧」。
        """
        self.aborted.append(stream_id)
        return self.sent


def _stream(connection: _RecordingConnection) -> H2Stream:
    """用替身连接造一条流。

    Args:
        connection: 连接替身。

    Returns:
        H2Stream: 流对象。
    """
    return H2Stream(cast("H2ClientConnection", connection), 7)


def test_abort_on_finished_stream_sends_no_frame() -> None:
    """已正常收口的流再中止：不发帧、不把 `done` 改写成本地原因。"""
    connection = _RecordingConnection(sent=False)
    stream = _stream(connection)
    assert stream.close_reason is None

    stream.on_end()
    assert stream.close_reason == CLOSE_DONE
    assert stream.abort(CLOSE_CANCELLED) is False
    assert stream.rst_sent is False
    assert stream.close_reason == CLOSE_DONE, "已收口的原因不该被后来的 abort 覆盖"
    assert connection.aborted == [7]


def test_abort_is_idempotent_and_keeps_first_reason() -> None:
    """首次中止发帧并记原因；二次中止不发帧，也不把「发过」改写成「没发」。"""
    connection = _RecordingConnection(sent=True)
    stream = _stream(connection)
    assert stream.abort(CLOSE_TIMEOUT) is True
    assert stream.rst_sent is True
    assert stream.close_reason == CLOSE_TIMEOUT

    connection.sent = False  # 第一次中止后流已不在连接表里
    assert stream.abort(CLOSE_CANCELLED) is False
    assert stream.rst_sent is True
    assert stream.close_reason == CLOSE_TIMEOUT


def test_abort_without_reason_keeps_close_reason_empty() -> None:
    """只做清理、不记原因：`rst_sent` 照样为真，`close_reason` 保持未定。"""
    stream = _stream(_RecordingConnection(sent=True))
    assert stream.abort(None) is True
    assert stream.rst_sent is True
    assert stream.close_reason is None


def test_close_reason_covers_peer_and_connection_paths() -> None:
    """对端 reset 与连接故障各记各的原因（`reason` 与流状态同源）。"""
    reset = _stream(_RecordingConnection(sent=False))
    reset.on_reset(8)
    assert reset.close_reason == CLOSE_PEER_RESET

    broken = _stream(_RecordingConnection(sent=False))
    broken.on_connection_error(RpcTransportClosedError("连接断了"))
    assert broken.close_reason == CLOSE_CONNECTION_ERROR


def test_abort_reason_of_maps_exception_types() -> None:
    """异常 → `reason` 的唯一映射：取消 / 超时 / 其它三档。"""
    assert abort_reason_of(asyncio.CancelledError()) == CLOSE_CANCELLED
    assert abort_reason_of(TimeoutError()) == CLOSE_TIMEOUT
    assert abort_reason_of(RpcTransportClosedError("boom")) == CLOSE_ERROR
