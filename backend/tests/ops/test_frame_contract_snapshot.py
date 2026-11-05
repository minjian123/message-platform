"""契约快照含帧协议（任务 03-2；Kiwi 2421）：`x-msg-frame` 随导出进入 `msg.json`。

帧形态不进运行时 `/openapi.json`（否则生成式客户端会按 JSON 解析帧端点），但必须进「对外承诺的
全量契约」——本用例断言导出产物里有它，且与实现常量同值（漂移由 `check` 兜住）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from bms_core.core.plugin import default_plugin_registry
from bms_core.core.serialization import normalize_collections

from bms_msg.protocol.contract import FRAME_CONTRACT_KEY, frame_contract
from bms_msg.protocol.frame import MAX_FRAME_HEADER_BYTES, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION
from bms_msg.rpc.builtin import default_rpc_registry
from ops import contract_snapshot

pytestmark = pytest.mark.kiwi_id(2421)


@pytest.fixture(autouse=True)
def _plugins() -> None:
    """每个装配前重置插件注册表（`create()` 会冻结注册表，二次装配必须重置）。"""
    default_plugin_registry().reset()


def test_export_contains_frame_contract(tmp_path: Path) -> None:
    """导出产物顶层含 `x-msg-frame`，且版本与上限与实现常量一致。

    Args:
        tmp_path: 临时仓库根。
    """
    assert contract_snapshot.export(tmp_path) == 0
    snapshot = contract_snapshot.snapshot_path(tmp_path, "msg")
    payload = json.loads(snapshot.read_text(encoding="utf-8"))

    frame = payload[FRAME_CONTRACT_KEY]
    assert frame["protocol_version"] == PROTOCOL_VERSION
    assert frame["limits"] == {"header_bytes": MAX_FRAME_HEADER_BYTES, "payload_bytes": MAX_FRAME_PAYLOAD_BYTES}
    # 键名是 03-3 定的 `frame_media_type`（当初与「消息体内容类型」消歧时改的名；本断言原本
    # 还写着旧名 `content_type`，属存量笔误——04-1 触及本文件时一并修正）。
    assert frame["frame_media_type"] == "application/x-msg-frame"


def test_exported_contract_equals_code_table(tmp_path: Path) -> None:
    """快照里的帧契约逐字段等于代码常量表（单一事实源，无二次维护）。

    Args:
        tmp_path: 临时仓库根。
    """
    assert contract_snapshot.export(tmp_path) == 0
    payload = json.loads(contract_snapshot.snapshot_path(tmp_path, "msg").read_text(encoding="utf-8"))

    assert payload[FRAME_CONTRACT_KEY] == json.loads(json.dumps(normalize_collections(frame_contract())))


def test_export_contains_rpc_contract(tmp_path: Path) -> None:
    """`x-msg-frame.rpc` 段：帧类型 48、响应位约定、方法清单与注册表同源。

    Args:
        tmp_path: 临时仓库根。
    """
    assert contract_snapshot.export(tmp_path) == 0
    payload = json.loads(contract_snapshot.snapshot_path(tmp_path, "msg").read_text(encoding="utf-8"))

    rpc = payload[FRAME_CONTRACT_KEY]["rpc"]
    assert rpc["frame_type"] == 48
    assert rpc["request_required"] == ["request_id"]
    assert rpc["errors"] == {"method_not_found": 350023}
    assert rpc["methods"] == [
        {"method": "echo", "service": "demo"},
        {"method": "length", "service": "demo"},
    ]
    # 与代码里的注册表同源（不是手写清单）。
    assert [{"service": service, "method": method} for service, method in default_rpc_registry().methods()] == [
        {"service": item["service"], "method": item["method"]} for item in rpc["methods"]
    ]


def test_injection_only_happens_at_export() -> None:
    """注入只发生在导出层：运行时 `app.openapi()` 不含帧契约，导出后才含。

    帧是同一路径上的另一形态，混进 `/openapi.json` 会让生成式客户端按 JSON 解析帧端点。
    """
    from bms_msg.main import ApplicationFactory

    runtime_openapi = ApplicationFactory().create(None).openapi()
    assert FRAME_CONTRACT_KEY not in runtime_openapi

    default_plugin_registry().reset()
    assert FRAME_CONTRACT_KEY in contract_snapshot.build_openapi("msg")
