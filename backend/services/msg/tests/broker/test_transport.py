"""传输基元（任务 03-1；Kiwi 2420）：连接元数据归一化 / 流登记表 / SSE 编码。

覆盖点对应详细设计 §4：
- `connection_meta()` 把 ASGI 的 `"2"` / `"1.1"` 归一成 `HTTP/2` / `HTTP/1.1`，缺字段给缺省不抛；
- `StreamRegistry` 自增 id、注销幂等、活跃数增减、快照带关闭原因；
- `sse_event()` 的 `data` 必须单行（SSE 以空行分事件），中文不转义；
- `default_stream_tuning()` 与四个常量同值。
"""

import json

import pytest

from bms_msg.broker.transport import (
    ConnectionMeta,
    StreamRegistry,
    connection_meta,
    default_stream_tuning,
    sse_comment,
    sse_event,
)
from bms_msg.core.config import (
    SUBSCRIBE_KEEPALIVE_MS,
    SUBSCRIBE_MAX_SECONDS,
    SUBSCRIBE_POLL_INTERVAL_MS,
)

pytestmark = pytest.mark.kiwi_id(2420)


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        (
            {"http_version": "2", "scheme": "http", "client": ("10.0.0.1", 1234)},
            ConnectionMeta(
                http_version="HTTP/2", scheme="http", is_tls=False, client_host="10.0.0.1", client_port=1234
            ),
        ),
        (
            {"http_version": "1.1", "scheme": "https", "client": ("10.0.0.2", 80)},
            ConnectionMeta(
                http_version="HTTP/1.1", scheme="https", is_tls=True, client_host="10.0.0.2", client_port=80
            ),
        ),
        (
            {"http_version": "1.0", "scheme": "http", "client": None},
            ConnectionMeta(http_version="HTTP/1.0", scheme="http", is_tls=False, client_host="", client_port=0),
        ),
        (
            {"client": ("10.0.0.3", 9)},
            ConnectionMeta(http_version="unknown", scheme="http", is_tls=False, client_host="10.0.0.3", client_port=9),
        ),
        (
            {"http_version": "h3", "scheme": "https", "client": "unix"},
            ConnectionMeta(http_version="h3", scheme="https", is_tls=True, client_host="", client_port=0),
        ),
    ],
)
def test_connection_meta_normalizes_scope(scope: dict[str, object], expected: ConnectionMeta) -> None:
    """协议版本归一化、TLS 由 scheme 判定、缺字段 / 异常形态给缺省（不抛）。"""
    assert connection_meta(scope) == expected


def test_stream_registry_open_close_and_snapshot() -> None:
    """流登记：自增 id、注销幂等并带原因、活跃数与快照随之变化。"""
    registry = StreamRegistry()

    first = registry.open(
        namespace="msg", topic="audit.operation", group="g", consumer_id="c1", partition=1, opened_at=1.0
    )
    second = registry.open(
        namespace="msg", topic="audit.operation", group="g", consumer_id="c2", partition=2, opened_at=2.0
    )

    assert (first, second) == ("s1", "s2")
    assert registry.active() == 2
    assert [session.stream_id for session in registry.sessions()] == ["s1", "s2"]
    assert [session.reason for session in registry.sessions()] == ["", ""]

    closed = registry.close(first, reason="client_closed")

    assert closed is not None
    assert closed.reason == "client_closed"
    assert closed.partition == 1
    assert registry.active() == 1
    assert registry.close(first, reason="deadline") is None


def test_sse_event_is_single_line_data() -> None:
    """`data` 单行（内层换行会被 SSE 当成事件分隔）、中文不转义、事件名与结尾空行齐备。"""
    encoded = sse_event("message", {"topic": "审计", "offset": 7})

    text = encoded.decode()
    assert text.startswith("event: message\ndata: ")
    assert text.endswith("\n\n")
    body = text[len("event: message\ndata: ") : -2]
    assert "\n" not in body
    assert json.loads(body) == {"topic": "审计", "offset": 7}
    assert "审计" in text, "中文必须直出（ensure_ascii=False），否则客户端看到 \\u 转义"


def test_sse_comment_format() -> None:
    """注释行以 `: ` 开头、空行结尾（客户端忽略内容，只用于保活）。"""
    assert sse_comment("keepalive") == b": keepalive\n\n"


def test_default_stream_tuning_matches_constants() -> None:
    """缺省流参数与 `core/config.py` 的常量同值（改常量即改缺省，不需要改两处）。"""
    tuning = default_stream_tuning()

    assert tuning.poll_interval_ms == SUBSCRIBE_POLL_INTERVAL_MS
    assert tuning.max_seconds == SUBSCRIBE_MAX_SECONDS
    assert tuning.keepalive_ms == SUBSCRIBE_KEEPALIVE_MS


def test_stream_registry_cancel_tenant_is_scoped() -> None:
    """按租户取消长驻流：只标记目标租户，未登记 / 已取消不重复标记。"""
    registry = StreamRegistry()
    first = registry.open(
        namespace="msg",
        topic="a",
        group="g",
        consumer_id="c1",
        partition=0,
        opened_at=1.0,
        tenant_id=1,
    )
    second = registry.open(
        namespace="msg",
        topic="b",
        group="g",
        consumer_id="c2",
        partition=1,
        opened_at=1.0,
        tenant_id=2,
    )
    assert registry.cancel_tenant(tenant_id=1) == 1
    assert registry.is_cancelled(first) is True
    assert registry.is_cancelled(second) is False
    assert registry.cancel_tenant(tenant_id=1) == 0
    registry.close(second, reason="test")
    assert registry.is_cancelled(second) is True
