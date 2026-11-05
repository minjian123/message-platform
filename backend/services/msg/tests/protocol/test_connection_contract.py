"""连接管理契约（任务 03-4；Kiwi 2424）：`x-msg-frame.connection` 段与实现常量同值。

为什么要单独一份（而不是扩 03-2 的 `test_contract.py`）：`kiwi_id` 不混标——一份文件的用例归属一个
平台用例号，扩文件会让 03-2 / 03-4 的编号在同一文件里打架（03-2 起一直按这个口径执行）。
"""

from __future__ import annotations

import pytest

from bms_msg.core.config import (
    CONNECTION_HEARTBEAT_INTERVAL_MS,
    CONNECTION_HEARTBEAT_TIMEOUT_MS,
    MAX_CONNECTIONS,
    MAX_STREAMS_PER_CONNECTION,
)
from bms_msg.protocol.contract import frame_contract
from bms_msg.protocol.frame import FrameType

pytestmark = pytest.mark.kiwi_id(2424)


def test_connection_contract_matches_constants() -> None:
    """心跳编号 / 间隔 / 超时 / 两个上限与实现常量同值（契约与实现只有一处事实源）。"""
    connection = frame_contract()["connection"]
    heartbeat = connection["heartbeat"]
    limits = connection["limits"]

    assert heartbeat["frame_type"] == int(FrameType.HEARTBEAT)
    assert heartbeat["endpoint"] == "POST /msg/connection/heartbeat"
    assert heartbeat["interval_ms"] == CONNECTION_HEARTBEAT_INTERVAL_MS
    assert heartbeat["timeout_ms"] == CONNECTION_HEARTBEAT_TIMEOUT_MS
    assert limits["max_connections"] == MAX_CONNECTIONS
    assert limits["max_streams_per_connection"] == MAX_STREAMS_PER_CONNECTION


def test_heartbeat_no_longer_in_known_unsupported() -> None:
    """心跳已落地：`known_unsupported` 只剩帧式订阅（03-5）。"""
    unsupported = frame_contract()["known_unsupported"]
    assert "heartbeat" not in unsupported
    assert "subscribe" in unsupported


def test_connection_contract_declares_drain_and_goaway_split() -> None:
    """drain 与 GOAWAY 的分工写进契约（客户端据此决定「等」还是「换实例」）。"""
    drain = frame_contract()["connection"]["drain"]
    assert drain["signal_fields"] == ["draining", "retry_after_ms"]
    assert "hypercorn" in drain["goaway"]
    assert "不发 GOAWAY" in drain["goaway"]
