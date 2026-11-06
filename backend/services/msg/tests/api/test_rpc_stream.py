"""服务端流式端点与流核心（任务 04-3-1；Kiwi 2437）。

判据（对应详细设计 §6.1）：

1. **帧序与结束三态**：连续 N 帧 `seq` 从 0 单调 +1、末帧置 `end_of_stream`、payload 与处理器产出一致；
   空流只回一个空 payload 收尾帧；异常结束以 `ERROR` 帧收尾（错误帧一定是最后一帧）；无收尾帧即中断。
2. **建流前失败回单帧 `error`**：非帧请求 `10001`（JSON）、路径参数非法 `10001`、帧类型 / 响应位 /
   结束位 / 缺 `request_id` / 压缩 `350018`、版本 `350019`、租户 `350012`、方法未注册或非流式 `350023`、
   名额满 `350021`。
3. **流内错误**：业务错原码、单帧超限 `350006`、预算耗尽 `350004`（`detail.outcome=unknown`）、
   空闲 `350024`。
4. **背压**：注入 `tuning(2, 1024)` + 慢 `send`，处理器产出不会跑到「已写出 + 在途」之外（攒批留一帧）。
5. **取消**：`http.disconnect` 命中后停止产出、名额归还、处理器被取消。
6. **观测**：`msg_rpc_stream` 五态字段齐全（`capture_logs`）。

另含服务层的纯函数与分支用例（攒批判据、断开 / 取消 / 未预期故障的 outcome）——它们跑不到 HTTP 层，
但分支必须有人覆盖。

> **`read_stream` vs `httpx`**：帧序 / 三态用 `httpx.ASGITransport`（整段读完再切帧）；背压与取消必须
> 自己当服务器（`call_stream`）才能慢读与造断开。
"""

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import replace
from typing import TypedDict

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from structlog.testing import capture_logs

from bms_msg.api.deps import get_backpressure_tuning, get_connection_tuning
from bms_msg.broker.backpressure import (
    BackpressureTuning,
    RateLimiter,
    default_backpressure_tuning,
    default_rate_limit_tuning,
)
from bms_msg.broker.connection import default_connection_tuning
from bms_msg.errors import MsgTopicNotFoundError
from bms_msg.protocol.frame import FrameFlags, FrameType, encode_frame
from bms_msg.rpc.deadline import Deadline
from bms_msg.rpc.registry import RpcCall, RpcRegistry, RpcReply
from bms_msg.rpc.stream import StreamItem, open_rpc_stream
from tests_support.frame_client import DEMO_HEADERS, FRAME_TYPE, frame_bytes, header, post_frame
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_stream import FrameEntry, call_stream, read_stream

pytestmark = pytest.mark.kiwi_id(2437)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

BASE_URL = "http://test"
"""测试基址。"""

RPC_TYPE = int(FrameType.RPC)
"""RPC 帧类型编号（48）。"""


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """帧客户端（ASGI 直连；请求体整段读回后再切帧）。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL, headers=DEMO_HEADERS) as session:
        yield session


def _body(payload: bytes = b"", *, request_id: str | None = "r1", **extra: object) -> bytes:
    """编码一个 RPC 请求帧。

    Args:
        payload: 参数原始字节。
        request_id: 请求标识；None 表示故意不带。
        extra: 追加帧头字段。

    Returns:
        bytes: 帧字节。
    """
    return frame_bytes(RPC_TYPE, header(request_id=request_id, extra=dict(extra)), payload)


async def _post(client: AsyncClient, path: str, body: bytes) -> Response:
    """发一次流式请求。

    Args:
        client: 客户端。
        path: 路径。
        body: 请求帧字节。

    Returns:
        Response: 响应。
    """
    return await post_frame(client, path, body)


def _entries(response: Response) -> list[FrameEntry]:
    """取响应里的帧序列（断言 HTTP 200 + 帧媒体类型）。

    Args:
        response: 响应。

    Returns:
        list[FrameEntry]: 帧序列。
    """
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith(FRAME_TYPE)
    return read_stream(response.content)


def _single_code(response: Response) -> int:
    """断言响应恰好是一帧 `ERROR`，并取回错误码。

    Args:
        response: 响应。

    Returns:
        int: 错误码。
    """
    entries = _entries(response)
    assert len(entries) == 1, f"期望单帧 error，实际 {len(entries)} 帧"
    code = entries[0].code
    assert code is not None, "单帧不是 ERROR 帧"
    return code


def _log_events(logs: Sequence[Mapping[str, object]]) -> list[Mapping[str, object]]:
    """取 `msg_rpc_stream` 事件。

    `capture_logs()` 给的是 `list[EventDict]`（= `MutableMapping[str, Any]`），与 `list[dict[...]]`
    不变型不兼容，故收 `Sequence[Mapping[str, object]]`（04-7-2 同一处理）。

    Args:
        logs: `capture_logs()` 的结果。

    Returns:
        list[Mapping[str, object]]: 事件列表。
    """
    return [entry for entry in logs if entry.get("event") == "msg_rpc_stream"]


# ---------------------------------------------------------------------------
# 1. 帧序与结束三态
# ---------------------------------------------------------------------------


async def test_frames_are_sequential_and_terminated(client: AsyncClient) -> None:
    """连续 5 帧：`seq` 0..4、末帧 `end_of_stream`、payload 与产出一致。"""
    entries = _entries(await _post(client, "/rpc/demo/ticker/stream", _body(b"5")))

    assert [entry.seq for entry in entries] == [0, 1, 2, 3, 4]
    assert [bytes(entry.frame.payload) for entry in entries] == [b"0", b"1", b"2", b"3", b"4"]
    assert [entry.end for entry in entries] == [False, False, False, False, True]
    assert all(entry.is_result for entry in entries), "流帧必须是 48 + response 位"
    first = entries[0].frame
    assert first.header["request_id"] == "r1"
    assert first.header["service"] == "demo"
    assert first.header["method"] == "ticker"
    assert first.header["length"] == 1
    assert int(first.header["used_ms"]) >= 0
    assert int(first.header["remaining_ms"]) > 0
    assert dict(first.header["headers"].items()) == {"handler": "demo.ticker", "index": "0"}


async def test_empty_stream_returns_single_terminal_frame(client: AsyncClient) -> None:
    """空流（处理器 0 帧）→ 只剩末帧（`seq=0` + `end_of_stream`，payload 空）。"""
    entries = _entries(await _post(client, "/rpc/demo/ticker/stream", _body(b"0")))

    assert len(entries) == 1
    assert entries[0].seq == 0
    assert entries[0].end is True
    assert bytes(entries[0].frame.payload) == b""


async def test_exact_coalesce_multiple_does_not_add_empty_frame(client: AsyncClient) -> None:
    """恰好 8 帧（攒批上界）时不额外补空帧：末帧必须是真实产出帧（留一帧的实现口径）。"""
    entries = _entries(await _post(client, "/rpc/demo/ticker/stream", _body(b"8")))

    assert [entry.seq for entry in entries] == list(range(8))
    assert entries[-1].end is True
    assert bytes(entries[-1].frame.payload) == b"7"


# ---------------------------------------------------------------------------
# 2. 建流前的失败：一律 HTTP 200 + 单帧 error
# ---------------------------------------------------------------------------


async def test_json_request_is_rejected(client: AsyncClient) -> None:
    """JSON 请求 → 契约层 `10001`（基座统一错误响应，不是帧）。

    **只判码不判状态**：`10001` 是平台登记在案、对外按 HTTP 200 渲染的契约层码（与一元 JSON 路径
    同口径，`test_rpc_invoke.py` 也只判 `code`）。
    """
    response = await client.post("/rpc/demo/ticker/stream", json={"payload": ""})

    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["code"] == 10001


async def test_invalid_service_name_returns_single_error_frame(client: AsyncClient) -> None:
    """路径 `service` 形态非法 → 单帧 `10001`（路径约束不在契约里声明，端点内判）。"""
    assert _single_code(await _post(client, "/rpc/bad!/ticker/stream", _body(b"1"))) == 10001


async def test_invalid_method_name_returns_single_error_frame(client: AsyncClient) -> None:
    """路径 `method` 形态非法 → 单帧 `10001`。"""
    assert _single_code(await _post(client, "/rpc/demo/bad!/stream", _body(b"1"))) == 10001


async def test_unregistered_method_returns_350023(client: AsyncClient) -> None:
    """未注册方法 → 单帧 `350023`（business / 不重试）。"""
    response = await _post(client, "/rpc/demo/missing/stream", _body())

    entries = _entries(response)
    assert len(entries) == 1
    assert entries[0].code == 350023
    assert entries[0].frame.header["layer"] == "business"
    assert entries[0].frame.header["retryable"] is False
    assert dict(entries[0].frame.header["detail"].items()) == {"service": "demo", "method": "missing"}


async def test_unary_method_as_stream_returns_350023(client: AsyncClient) -> None:
    """一元方法当流式调 → 同一个 `350023`（对调用方而言就是「这里没有可流式的方法」）。"""
    assert _single_code(await _post(client, "/rpc/demo/echo/stream", _body(b"x"))) == 350023


async def test_wrong_frame_type_rejected(client: AsyncClient) -> None:
    """把 publish 帧发到流式端点 → `350018`。"""
    body = frame_bytes(int(FrameType.PUBLISH), header())

    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", body)) == 350018


async def test_request_frame_must_not_set_response_flag(client: AsyncClient) -> None:
    """请求帧置 response 位 → `350018`。"""
    body = frame_bytes(RPC_TYPE, header(), b"1", flags=int(FrameFlags.NEED_RESPONSE) | int(FrameFlags.RESPONSE))

    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", body)) == 350018


async def test_request_frame_must_not_set_end_of_stream(client: AsyncClient) -> None:
    """请求帧置 `end_of_stream` → `350018`（那是响应末帧的标记，请求侧没有含义）。"""
    body = frame_bytes(RPC_TYPE, header(), b"1", flags=int(FrameFlags.END_OF_STREAM))

    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", body)) == 350018


async def test_missing_request_id_rejected(client: AsyncClient) -> None:
    """缺 `request_id` → `350018`（envelope 必填）。"""
    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", _body(b"1", request_id=None))) == 350018


async def test_compressed_frame_rejected(client: AsyncClient) -> None:
    """`flags.compressed` 置位 → `350018`（RPC 的 payload 直接交给处理器）。"""
    body = frame_bytes(RPC_TYPE, header(), b"1", flags=int(FrameFlags.COMPRESSED))

    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", body)) == 350018


async def test_declared_compression_rejected(client: AsyncClient) -> None:
    """只声明 `header.compression` 也拒。"""
    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", _body(b"1", compression="gzip"))) == 350018


async def test_tenant_mismatch_rejected(client: AsyncClient) -> None:
    """帧头 `tenant` 与请求身份不一致 → `350012`。"""
    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", _body(b"1", tenant="other"))) == 350012


async def test_unsupported_version_returns_error_frame(client: AsyncClient) -> None:
    """主版本不支持 → `350019` 错误帧（客户端已在按帧说话）。"""
    body = encode_frame(frame_type=RPC_TYPE, header=header(), payload=b"1", version=(2, 0))

    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", body)) == 350019


async def test_invalid_budget_is_param_error(client: AsyncClient) -> None:
    """`deadline_ms` 形态非法 → 单帧 `10001`（与一元同码）。"""
    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", _body(b"1", deadline_ms="soon"))) == 10001


async def test_request_headers_passed_to_handler(msg_app: FastAPI, client: AsyncClient) -> None:
    """请求帧的结构化头原样交给处理器，并随结果帧回显（`map<str, str>` 的正常路径）。"""

    async def echo_headers(call: RpcCall) -> AsyncIterator[RpcReply]:
        yield RpcReply(payload=b"", headers=call.headers)

    msg_app.state.rpc_registry.register_stream("demo", "hdr", echo_headers)

    entries = _entries(await _post(client, "/rpc/demo/hdr/stream", _body(b"", headers={"k": "v"})))

    assert dict(entries[0].frame.header["headers"].items()) == {"k": "v"}


async def test_headers_must_be_string_map(client: AsyncClient) -> None:
    """帧头 `headers` 不是 map<str, str> → `10001`。"""
    assert _single_code(await _post(client, "/rpc/demo/ticker/stream", _body(b"1", headers={"k": 1}))) == 10001


async def test_stream_quota_exhausted(msg_app: FastAPI, client: AsyncClient) -> None:
    """单连接长驻流名额已满 → 单帧 `350021`，且名额未被占用（不泄漏）。"""
    exhausted = replace(default_connection_tuning(), max_streams_per_connection=0)
    msg_app.dependency_overrides[get_connection_tuning] = lambda: exhausted

    response = await _post(client, "/rpc/demo/ticker/stream", _body(b"3"))

    assert _single_code(response) == 350021
    assert sum(record.streams for record in msg_app.state.connection_registry.records()) == 0


# ---------------------------------------------------------------------------
# 3. 流内错误：ERROR 帧 + EOF
# ---------------------------------------------------------------------------


async def test_business_error_ends_stream_with_error_frame(msg_app: FastAPI, client: AsyncClient) -> None:
    """处理器一上来就抛业务错 → 单帧 `ERROR`（原码），`layer=business` / `retryable=false`。"""

    async def boom(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        raise MsgTopicNotFoundError("主题不存在")
        yield RpcReply()  # pragma: no cover - 让函数成为异步生成器

    msg_app.state.rpc_registry.register_stream("demo", "boom", boom)

    entries = _entries(await _post(client, "/rpc/demo/boom/stream", _body()))

    assert len(entries) == 1
    assert entries[0].code == 350001
    assert entries[0].frame.header["layer"] == "business"
    assert entries[0].frame.header["retryable"] is False


async def test_error_after_results_keeps_them_then_error_frame(msg_app: FastAPI, client: AsyncClient) -> None:
    """先产出再报错：已产出的结果照常交付，错误帧一定是最后一帧。"""

    async def half(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        yield RpcReply(payload=b"a")
        yield RpcReply(payload=b"b")
        raise MsgTopicNotFoundError("主题不存在")

    msg_app.state.rpc_registry.register_stream("demo", "half", half)

    entries = _entries(await _post(client, "/rpc/demo/half/stream", _body()))

    assert [bytes(entry.frame.payload) for entry in entries if entry.is_result] == [b"a", b"b"]
    assert [entry.end for entry in entries if entry.is_result] == [False, False]
    assert entries[-1].code == 350001


async def test_contract_error_in_stream_is_protocol_layer(msg_app: FastAPI, client: AsyncClient) -> None:
    """处理器抛契约层错误（`ParamError`）也以 `ERROR` 帧收尾，`layer=protocol`。"""
    entries = _entries(await _post(client, "/rpc/demo/ticker/stream", _body(b"many")))

    assert len(entries) == 1
    assert entries[0].code == 10001
    assert entries[0].frame.header["layer"] == "protocol"


async def test_oversized_frame_returns_350006(msg_app: FastAPI, client: AsyncClient) -> None:
    """单帧结果超 1 MiB → `350006`（在编码之前判，不会变成 500）。"""
    oversized = b"x" * (1024 * 1024 + 1)

    async def big(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        yield RpcReply(payload=oversized)

    msg_app.state.rpc_registry.register_stream("demo", "big", big)

    assert _single_code(await _post(client, "/rpc/demo/big/stream", _body())) == 350006


async def test_deadline_exhausted_mid_stream(msg_app: FastAPI, client: AsyncClient) -> None:
    """推流中预算耗尽 → `350004` + `detail.outcome=unknown`（整条流共享一份预算）。"""

    async def slow(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        await asyncio.sleep(0.06)
        yield RpcReply(payload=b"late")

    msg_app.state.rpc_registry.register_stream("demo", "slow", slow)

    entries = _entries(await _post(client, "/rpc/demo/slow/stream", _body(deadline_ms=20)))

    assert len(entries) == 1
    assert entries[0].code == 350004
    assert dict(entries[0].frame.header["detail"].items())["outcome"] == "unknown"


async def test_idle_timeout_emits_350024(
    msg_app: FastAPI, client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """空闲超时 → 已产出的帧照常交付 + `350024` 收尾（阈值注入到 30ms）。"""
    monkeypatch.setattr("bms_msg.rpc.stream.RPC_STREAM_IDLE_MS", 30)

    entries = _entries(await _post(client, "/rpc/demo/idle/stream", _body()))

    assert [bytes(entry.frame.payload) for entry in entries if entry.is_result] == [b"idle"]
    assert entries[-1].code == 350024
    assert dict(entries[-1].frame.header["detail"].items())["idle_ms"] == 30
    assert sum(record.streams for record in msg_app.state.connection_registry.records()) == 0


async def test_stream_releases_quota_after_normal_end(msg_app: FastAPI, client: AsyncClient) -> None:
    """正常结束后名额归还（`finally` 一处收口）。"""
    await _post(client, "/rpc/demo/ticker/stream", _body(b"3"))

    assert sum(record.streams for record in msg_app.state.connection_registry.records()) == 0


# ---------------------------------------------------------------------------
# 4. 背压与取消（ASGI 直调：慢 send / 造断开）
# ---------------------------------------------------------------------------


async def test_backpressure_pauses_producer(msg_app: FastAPI) -> None:
    """注入 `tuning(2, 1024)` + 慢 `send`：产出不会跑到「已写出 + 在途」之外。"""
    produced: list[int] = []

    async def pulser(call: RpcCall) -> AsyncIterator[RpcReply]:
        total = int(call.payload)
        for index in range(total):
            produced.append(index)
            yield RpcReply(payload=b"x")

    msg_app.state.rpc_registry.register_stream("demo", "pulser", pulser)
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=2, inflight_bytes=1024
    )
    written: list[int] = []
    samples: list[int] = []

    async def slow_send(chunk: bytes, count: int) -> None:
        del count
        written.append(len(written) + len(read_stream(chunk)))
        samples.append(len(produced) - written[-1])
        await asyncio.sleep(0.005)

    result = await call_stream(msg_app, path="/rpc/demo/pulser/stream", body=_body(b"12"), on_chunk=slow_send)

    entries = read_stream(result.body)
    assert [entry.seq for entry in entries] == list(range(12))
    assert entries[-1].end is True
    assert max(samples) <= 2, f"在途超过阈值：样本 {samples}"


async def test_disconnect_stops_stream_and_releases_quota(msg_app: FastAPI) -> None:
    """`http.disconnect` 后停止产出、名额归还、处理器被取消。"""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def idle_like(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        try:
            yield RpcReply(payload=b"first")
            started.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    msg_app.state.rpc_registry.register_stream("demo", "idle_like", idle_like)
    disconnect = asyncio.Event()

    async def hook(chunk: bytes, count: int) -> None:
        del chunk, count
        disconnect.set()

    result = await call_stream(
        msg_app, path="/rpc/demo/idle_like/stream", body=_body(), on_chunk=hook, disconnect=disconnect
    )

    for _ in range(100):
        if cancelled.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set()
    assert cancelled.is_set(), "处理器未被取消"
    assert sum(record.streams for record in msg_app.state.connection_registry.records()) == 0
    # 断开后不写收尾帧：最后一帧不能是 end_of_stream，也不该是 ERROR（取消不产生错误帧）
    entries = read_stream(result.body)
    assert entries
    assert all(not entry.end for entry in entries)
    assert all(entry.code is None for entry in entries)


# ---------------------------------------------------------------------------
# 5. 观测
# ---------------------------------------------------------------------------


async def test_log_records_ok_outcome(client: AsyncClient) -> None:
    """正常结束：`msg_rpc_stream` 字段齐全（`frames` / `bytes` / `seq_max` / 时长 / 停滞）。"""
    with capture_logs() as logs:
        await _post(client, "/rpc/demo/ticker/stream", _body(b"3"))

    events = _log_events(logs)
    assert len(events) == 1
    event = events[0]
    assert event["service"] == "demo"
    assert event["method"] == "ticker"
    assert event["request_id"] == "r1"
    assert event["outcome"] == "ok"
    assert event["frames"] == 3
    assert event["bytes"] == 3
    assert event["seq_max"] == 2
    assert isinstance(event["duration_ms"], int)
    assert event["duration_ms"] >= 0
    assert isinstance(event["stalled_ms"], int)
    assert event["stalled_ms"] >= 0


async def test_log_records_business_error_outcome(msg_app: FastAPI, client: AsyncClient) -> None:
    """业务错：`outcome=business_error`。"""

    async def boom(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        raise MsgTopicNotFoundError("主题不存在")
        yield RpcReply()  # pragma: no cover - 让函数成为异步生成器

    msg_app.state.rpc_registry.register_stream("demo", "boom", boom)

    with capture_logs() as logs:
        await _post(client, "/rpc/demo/boom/stream", _body())

    assert _log_events(logs)[0]["outcome"] == "business_error"


async def test_log_records_deadline_outcome(msg_app: FastAPI, client: AsyncClient) -> None:
    """预算耗尽：`outcome=deadline_exceeded`。"""

    async def slow(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        await asyncio.sleep(0.06)
        yield RpcReply()

    msg_app.state.rpc_registry.register_stream("demo", "slow2", slow)

    with capture_logs() as logs:
        await _post(client, "/rpc/demo/slow2/stream", _body(deadline_ms=20))

    assert _log_events(logs)[0]["outcome"] == "deadline_exceeded"


async def test_log_records_idle_outcome(msg_app: FastAPI, client: AsyncClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """空闲超时：`outcome=idle`（与连接心跳分层，日志据此区分「谁先放弃」）。"""
    monkeypatch.setattr("bms_msg.rpc.stream.RPC_STREAM_IDLE_MS", 30)

    with capture_logs() as logs:
        await _post(client, "/rpc/demo/idle/stream", _body())

    assert _log_events(logs)[0]["outcome"] == "idle"


# ---------------------------------------------------------------------------
# 6. 服务层（纯函数与跑不到 HTTP 层的分支）
# ---------------------------------------------------------------------------


async def test_coalesce_bytes_bound_flushes_batch(
    msg_app: FastAPI, client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """字节上界先到即交出一批（与帧数上界同一机制，先到者触发）。"""
    monkeypatch.setattr("bms_msg.rpc.stream.RPC_STREAM_COALESCE_BYTES", 4)
    frames = [RpcReply(payload=b"ab"), RpcReply(payload=b"cd")]

    async def pair(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        for reply in frames:
            yield reply

    msg_app.state.rpc_registry.register_stream("demo", "pair", pair)

    entries = _entries(await _post(client, "/rpc/demo/pair/stream", _body()))

    assert [bytes(entry.frame.payload) for entry in entries] == [b"ab", b"cd"]
    assert entries[-1].end is True


async def test_disabled_threshold_keeps_batching(msg_app: FastAPI) -> None:
    """阈值关闭（0 / 0）时只按攒批上界分批：8 帧 → 两批（7 + 1），不是每帧一批。"""
    msg_app.dependency_overrides[get_backpressure_tuning] = lambda: BackpressureTuning(
        inflight_messages=0, inflight_bytes=0
    )

    result = await call_stream(msg_app, path="/rpc/demo/ticker/stream", body=_body(b"8"))

    entries = read_stream(result.body)
    assert [entry.seq for entry in entries] == list(range(8))
    assert entries[-1].end is True
    assert len(result.chunks) == 2, f"阈值关闭时仍应按帧数上界分批，实际 {len(result.chunks)} 批"


class _ServiceArgs(TypedDict):
    """服务层用例的公共实参（`**` 展开要 TypedDict 才能保住类型）。"""

    registry: RpcRegistry
    limiter: RateLimiter
    tenant: str
    service: str
    tuning: BackpressureTuning
    deadline: Deadline
    headers: ConcurrentStableDict[str, str]


def _service_calls(registry: RpcRegistry) -> _ServiceArgs:
    """构造服务层调用的公共实参（不含 payload / method）。

    Args:
        registry: 方法注册表。

    Returns:
        _ServiceArgs: 关键字实参。
    """
    return {
        "registry": registry,
        "limiter": RateLimiter(default_rate_limit_tuning()),
        "tenant": "demo",
        "service": "demo",
        "tuning": default_backpressure_tuning(),
        "deadline": Deadline.start(30_000),
        "headers": ConcurrentStableDict[str, str](),
    }


async def test_service_layer_disconnect_cancels() -> None:
    """断开探测命中 → 抛 `CancelledError`、`outcome=cancelled`（服务层判据，不经 HTTP）。"""

    async def probe() -> bool:
        return True

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        async for _item in open_rpc_stream(
            **_service_calls(_ticker_registry()),
            method="ticker",
            payload=b"3",
            disconnected=probe,
        ):
            pass  # pragma: no cover - 一帧都拿不到

    assert _log_events(logs)[0]["outcome"] == "cancelled"


async def test_service_layer_preflight_error_is_logged_as_business() -> None:
    """预检失败（方法未注册）也记一条 `msg_rpc_stream`（`business_error`），便于对账。"""
    with capture_logs() as logs, pytest.raises(Exception, match="未注册"):
        async for _item in open_rpc_stream(
            **_service_calls(RpcRegistry()),
            method="unknown",
            payload=b"",
        ):
            pass  # pragma: no cover - 一帧都拿不到

    assert _log_events(logs)[0]["outcome"] == "business_error"


async def test_service_layer_mid_stream_close_marks_cancelled() -> None:
    """取到一批后被 `aclose()`（GeneratorExit）→ `outcome=cancelled`、不写收尾批。"""
    stream = open_rpc_stream(
        **_service_calls(_ticker_registry()),
        method="ticker",
        payload=b"8",
    )

    first = await stream.__anext__()
    assert isinstance(first, StreamItem)
    assert first.terminated is False
    assert len(first.replies) == 7  # 攒满 8 帧时留一帧待判末帧
    with capture_logs() as logs:
        await stream.aclose()

    assert _log_events(logs)[0]["outcome"] == "cancelled"


async def test_service_layer_unexpected_error_marks_error() -> None:
    """非 `BizError` 的处理器故障 → `outcome=error`，且不产出收尾批。"""

    async def broken(call: RpcCall) -> AsyncIterator[RpcReply]:
        del call
        raise RuntimeError("handler bug")
        yield RpcReply()  # pragma: no cover - 让函数成为异步生成器

    registry = RpcRegistry()
    registry.register_stream("demo", "broken", broken)

    with capture_logs() as logs, pytest.raises(RuntimeError):
        async for _item in open_rpc_stream(**_service_calls(registry), method="broken", payload=b""):
            pass  # pragma: no cover - 一帧都拿不到

    assert _log_events(logs)[0]["outcome"] == "error"


def _ticker_registry() -> RpcRegistry:
    """构造只含 `demo.ticker` 的注册表（服务层用例用，不依赖 app 装配）。

    Returns:
        RpcRegistry: 注册表。
    """
    from bms_msg.rpc.builtin import default_rpc_registry

    return default_rpc_registry()
