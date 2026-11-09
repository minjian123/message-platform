"""链路字段的三个出口与契约（msg 任务 06-4；Kiwi 2469）。

覆盖：

- `MessageItem` 四字段（JSON / SSE 同源）与载荷原始字节通道；
- 帧 `consume` 响应头逐条元数据、`message` 帧头四字段（与 JSON 字段名一致）；
- 管理面消息检索项四字段且拿不到正文；
- 发布事件日志绑定四字段（空值不写 key，避免顶掉自动注入的 `trace_id`）；
- 帧契约 `x-msg-frame` 的可选字段清单与 `tracing` 段；契约快照同步落盘四字段。
"""

# pyright: reportPrivateUsage=false

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.frame import FrameType, decode_frame

from bms_msg.api.admin.messages import _admin_item
from bms_msg.api.data.messages import _log_published, message_item
from bms_msg.api.frame.responses import message_frame, result_frame
from bms_msg.broker.log import MessageHeaderView, StoredMessage
from bms_msg.core.trace import EMPTY_TRACE, TraceContext
from bms_msg.protocol.contract import frame_contract
from bms_msg.schemas.message import ConsumeResult, MessageItem, PublishRequest

pytestmark = pytest.mark.kiwi_id(2469)

_NOW = datetime(2026, 11, 9, 8, 0, tzinfo=UTC)
"""固定发布时间（断言不依赖当前时刻）。"""

TRACE = TraceContext(trace_id="t-1", span_id="s-1", parent_span_id="p-1", trace_flags="01")
"""一组完整链路字段。"""

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根（`backend/` 的父目录）。"""


def _stored(*, payload: bytes = b"payload", trace: TraceContext = TRACE) -> StoredMessage:
    """构造一条存储记录。

    Args:
        payload: 消息体字节。
        trace: 链路字段。

    Returns:
        StoredMessage: 存储记录。
    """
    return StoredMessage(
        message_id="m-1",
        log_id="l-1",
        topic_id=1,
        partition_no=0,
        offset=3,
        key="k",
        payload=payload,
        headers=ConcurrentStableDict({"a": "b"}),
        trace=trace,
        published_at=_NOW,
    )


def _header_view(*, trace: TraceContext = TRACE) -> MessageHeaderView:
    """构造一条只读头视图。

    Args:
        trace: 链路字段。

    Returns:
        MessageHeaderView: 头视图（载荷只有长度与摘要）。
    """
    return MessageHeaderView(
        message_id="m-1",
        log_id="l-1",
        topic_id=1,
        partition_no=0,
        offset=3,
        key="k",
        headers=ConcurrentStableDict({"a": "b"}),
        trace=trace,
        published_at=_NOW,
        content_type="application/json",
        payload_len=7,
        payload_sha256=sha256(b"payload").hexdigest(),
    )


def test_message_item_carries_four_fields() -> None:
    """JSON / SSE 出口：四项逐条回显，载荷走原始字节通道（不 base64 往返）。"""
    item = message_item(_stored(), "audit.operation", 1)
    assert (item.trace_id, item.span_id, item.parent_span_id, item.trace_flags) == (
        TRACE.trace_id,
        TRACE.span_id,
        TRACE.parent_span_id,
        TRACE.trace_flags,
    )
    assert item.raw_payload == b"payload"

    bare = message_item(_stored(trace=EMPTY_TRACE), "audit.operation", 1)
    assert (bare.trace_id, bare.span_id, bare.parent_span_id, bare.trace_flags) == (None, None, None, None)


def test_consume_frame_meta_has_trace_per_message() -> None:
    """帧 consume 响应：`header.messages[]` 逐条四项，且两条各自独立。"""
    items = [
        message_item(_stored(trace=TRACE), "audit.operation", 1),
        message_item(
            _stored(trace=TraceContext(trace_id="t-2", span_id="s-2", trace_flags="00")), "audit.operation", 1
        ),
    ]
    result = ConsumeResult(messages=ConcurrentStableList(items))
    frame = decode_frame(result_frame(frame_type=int(FrameType.CONSUME), request_id="r1", result=result))

    metas = cast("list[dict[str, object]]", frame.header["messages"])
    assert [(meta["trace_id"], meta["span_id"], meta["parent_span_id"], meta["trace_flags"]) for meta in metas] == [
        (TRACE.trace_id, TRACE.span_id, TRACE.parent_span_id, TRACE.trace_flags),
        ("t-2", "s-2", None, "00"),
    ]
    assert "trace_id" not in frame.header


def test_message_frame_has_trace() -> None:
    """帧 `message` 帧头四字段与 JSON 同名，载荷仍是原始字节。"""
    item = message_item(_stored(payload=b"\x00\x01\xff"), "audit.operation", 2)
    frame = decode_frame(message_frame(request_id="r2", item=item))
    assert (frame.header["trace_id"], frame.header["span_id"], frame.header["trace_flags"]) == (
        TRACE.trace_id,
        TRACE.span_id,
        TRACE.trace_flags,
    )
    assert bytes(frame.payload) == b"\x00\x01\xff"


def test_admin_item_has_trace_and_no_body() -> None:
    """管理面检索项：四字段可查，正文（字段层面）不存在。"""
    mapped = _admin_item(_header_view())
    assert (mapped.trace_id, mapped.span_id, mapped.parent_span_id, mapped.trace_flags) == (
        TRACE.trace_id,
        TRACE.span_id,
        TRACE.parent_span_id,
        TRACE.trace_flags,
    )
    assert not hasattr(mapped, "payload")


class _Capture:
    """日志器替身：记录事件名与字段（不写盘、不依赖日志配置）。"""

    def __init__(self) -> None:
        """初始化（无事件）。"""
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **fields: object) -> None:
        """记录一次日志调用。

        Args:
            event: 事件名。
            fields: 结构化字段。
        """
        self.events.append((event, fields))


def test_publish_log_binds_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    """发布日志带四项；字段为空时**不写 key**（否则会顶掉日志器自动注入的 `trace_id`）。"""
    capture = _Capture()
    monkeypatch.setattr("bms_msg.api.data.messages._LOGGER", capture)

    _log_published(
        request=PublishRequest(
            namespace="msg", topic="audit.operation", trace_id="t-1", span_id="s-1", trace_flags="01"
        ),
        message_id="m-1",
        log_id="l-1",
        partition=0,
        offset=1,
        delayed=False,
    )
    _log_published(
        request=PublishRequest(namespace="msg", topic="audit.operation"),
        message_id="m-2",
        log_id="l-2",
        partition=0,
        offset=None,
        delayed=True,
    )

    full, bare = capture.events[0][1], capture.events[1][1]
    assert (capture.events[0][0], full["trace_id"], full["span_id"], full["trace_flags"]) == (
        "msg_message_published",
        "t-1",
        "s-1",
        "01",
    )
    assert "parent_span_id" not in full
    assert not {"trace_id", "span_id", "parent_span_id", "trace_flags"} & set(bare)
    assert (bare["offset"], bare["delayed"]) == (None, True)


def test_frame_contract_documents_tracing() -> None:
    """帧契约：publish 可选字段、message 头字段、`tracing` 口径段齐备。"""
    contract = frame_contract()
    publish_optional = cast("list[str]", contract["endpoint_fields"]["publish"]["optional"])
    assert {"trace_id", "span_id", "parent_span_id", "trace_flags"} <= set(publish_optional)
    assert {"span_id", "parent_span_id", "trace_flags"} <= set(
        cast("list[str]", contract["stream"]["message_header_fields"])
    )

    tracing = cast("dict[str, object]", contract["tracing"])
    assert tracing["max_length"] == 128
    assert "broker" in cast("str", tracing["who_decides"])


def test_snapshot_carries_trace_fields() -> None:
    """契约快照同步落盘四字段与 `tracing` 段（`ops.contract_snapshot` 零漂移的前置）。"""
    snapshot = cast(
        "dict[str, object]",
        json.loads((REPO_ROOT / "deploy" / "contracts" / "msg.json").read_text(encoding="utf-8")),
    )
    frame = cast("dict[str, object]", snapshot["x-msg-frame"])
    assert cast("dict[str, object]", frame["tracing"])["max_length"] == 128
    components = cast("dict[str, object]", snapshot["components"])
    schemas = cast("dict[str, dict[str, object]]", components["schemas"])
    item_props = cast("dict[str, object]", schemas["bms_msg__schemas__message__MessageItem"]["properties"])
    assert {"trace_id", "span_id", "parent_span_id", "trace_flags"} <= set(item_props)


def test_item_message_model_requires_all_four() -> None:
    """响应模型四项必填（漏填在构造期就报错，不靠人记）。"""
    with pytest.raises(ValueError):
        MessageItem(  # type: ignore[call-arg]
            message_id="m-1",
            topic="audit.operation",
            partition=0,
            offset=0,
            key=None,
            payload="",
            headers=ConcurrentStableDict(),
            delivery_count=1,
            published_at=_NOW,
        )
