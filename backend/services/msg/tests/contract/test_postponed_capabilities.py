"""01-8-2 事务消息与 CDC 口子 · 占位契约与文档护栏（Kiwi 2482）。

首期不实现事务消息与 CDC，本文件只验「后置口径本身」：
占位单源结构 / 未知字段忽略 / 版本可见 / 公开快照零能力 / 不宣称已支持 / 触发条件齐全。
不新增任何业务能力用例；断言只读仓内静态资产，跨仓文件不进扫描范围（CI 只 clone msg）。
"""

from __future__ import annotations

import inspect
import json
import re
import tomllib
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher

pytestmark = pytest.mark.kiwi_id(2482)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（backend/services/msg/tests/contract/<本文件> 上溯五级）。"""

_POSTPONED = _REPO / "deploy" / "events" / "postponed_capabilities.toml"
_SNAPSHOT = _REPO / "deploy" / "contracts" / "msg.json"
_EVENT_SNAPSHOT = _REPO / "deploy" / "events" / "contracts.json"
_EVENTS_README = _REPO / "deploy" / "events" / "README.md"

_TASK = (
    "msg文档/项目/08B_消息平台/任务/01_消息模型与投递/"
    "01_消息模型与投递_08_可靠性与事务/"
    "01_消息模型与投递_08_可靠性与事务_02_事务消息与CDC口子"
)

_DOC_PATHS = ConcurrentStableFrozenList(
    [
        "msg文档/项目/08B_消息平台/需求/01_需求_消息模型与投递.md",
        "msg文档/设计/架构设计/09_架构设计_决策与风险.md",
        "msg文档/设计/架构设计/05_架构设计_接入面_协议与客户端.md",
        "msg文档/规划/01_规划_消息平台.md",
        f"{_TASK}/01_消息模型与投递_08_可靠性与事务_02_事务消息与CDC口子.md",
        f"{_TASK}/设计/01_详细设计_01-8-2_事务消息与CDC口子.md",
        f"{_TASK}/设计/02_设计评审记录_01-8-2.md",
        f"{_TASK}/实施/01_实施记录_01-8-2_事务消息与CDC口子.md",
        f"{_TASK}/测试/01_测试记录_01-8-2_事务消息与CDC口子.md",
    ]
)
"""文档断言范围：本次回写与记录落点（只扫 msg 仓内文件）。"""

_TOP_KEYS = ConcurrentStableFrozenList(
    [
        "schema_version",
        "status",
        "scope",
        "source",
        "compatibility",
        "transactional_message",
        "cdc_outbox_connector",
        "review",
        "ownership",
    ]
)
"""顶层已知键：只增不删的基线，老实现按这些键取缺省。"""

_SECTION_KEYS = ConcurrentStableFrozenList(["transactional_message", "cdc_outbox_connector"])
_TX_KEYS = ConcurrentStableFrozenList(
    [
        "port_position",
        "state_model",
        "future_components",
        "reserved_event_types",
        "failure_branches",
        "trigger_conditions",
        "consequence_if_skipped",
    ]
)
_CDC_KEYS = ConcurrentStableFrozenList(
    ["form", "excluded", "failure_branches", "trigger_conditions", "consequence_if_skipped"]
)
_SNAPSHOT_TOKENS = ConcurrentStableFrozenList(
    ["transaction", "half_message", "binlog", "outbox_connector", "cdc", "/msg/transaction", "/admin/cdc"]
)
_CLAIM_PATTERNS = ConcurrentStableFrozenList(
    [
        r"事务消息\s*已(?:支持|实现|上线|交付|启用|接入)",
        r"(?:CDC|cdc)\s*已(?:支持|实现|上线|交付|启用|接入)",
        r"已(?:支持|接入|实现)\s*(?:事务消息|CDC|cdc)",
    ]
)
_NEGATIVE_MARKERS = ConcurrentStableFrozenList(
    [
        "不出现",
        "不得",
        "禁止",
        "不宣称",
        "不承诺",
        "不构成",
        "不实现",
        "不新增",
        "不接",
        "不是",
        "不做",
        "仍后置",
        "恒为预留",
        "误导",
        "误读",
        "断言",
        "避免",
        "未满足",
        "未进入",
        "非目标",
    ]
)
"""否定 / 警示语境：跳过这些行，避免把「不得出现某描述」自身判为违规。"""


def _load() -> object:
    """读取并解析占位单源。

    Returns:
        object: TOML 解出的顶层映射。
    """
    return tomllib.loads(_POSTPONED.read_text(encoding="utf-8"))


def _with_future_fields() -> str:
    """构造含未来顶层键与未来条目的单源文本（未知字段忽略的注入样本）。

    Returns:
        str: 注入后的 TOML 文本。
    """
    original = _POSTPONED.read_text(encoding="utf-8")
    return 'future_key_2482 = "future"\n\n' + original + '\n[future_section]\nstatus = "reserved"\n'


# ---------------------------------------------------------------------------
# U1 占位单源结构：条目恒为预留，不构成已承诺能力
# ---------------------------------------------------------------------------


def test_postponed_contract_shape_is_reserved() -> None:
    """U1：顶层与两类条目的状态恒为 reserved，字段清单可被后续任务直接接手。"""
    data = cast("dict[str, object]", _load())
    for key in _TOP_KEYS:
        assert key in data, key
    assert data["status"] == "reserved"
    assert "不构成已承诺能力" in cast("str", data["scope"])

    tx = cast("dict[str, object]", data["transactional_message"])
    cdc = cast("dict[str, object]", data["cdc_outbox_connector"])
    assert tx["status"] == "reserved"
    assert cdc["status"] == "reserved"
    for key in _TX_KEYS:
        assert key in tx, key
    for key in _CDC_KEYS:
        assert key in cdc, key
    assert "不新增端口" in cast("str", tx["port_position"])
    assert "不新增扫描端点" in cast("str", cdc["form"])


# ---------------------------------------------------------------------------
# U2 只增不删 / 未知字段忽略
# ---------------------------------------------------------------------------


def test_unknown_fields_are_ignored_and_known_fields_stable() -> None:
    """U2：注入未来键与未来条目后，按已知键取出的视图逐项不变，未知键不进入结果。"""
    base = cast("dict[str, object]", _load())
    future = cast("dict[str, object]", tomllib.loads(_with_future_fields()))

    assert "future_key_2482" in future
    assert "future_section" in future

    base_view = {key: base.get(key) for key in _TOP_KEYS}
    future_view = {key: future.get(key) for key in _TOP_KEYS}
    assert future_view == base_view
    assert "future_key_2482" not in future_view
    assert "future_section" not in future_view


# ---------------------------------------------------------------------------
# U3 版本可见与帧契约兼容措辞一致
# ---------------------------------------------------------------------------


def test_version_is_visible_and_matches_frame_compatibility_rule() -> None:
    """U3：schema_version 可读且主版本稳定；兼容策略与公开帧契约版本规则同口径。"""
    data = cast("dict[str, object]", _load())
    assert cast("str", data["schema_version"]) == "1.0.0"
    compat = cast("dict[str, object]", data["compatibility"])
    assert "只增不删" in cast("str", compat["additive"])
    assert "未知字段" in cast("str", compat["unknown_fields"])
    assert "忽略" in cast("str", compat["unknown_fields"])
    assert "版本可见" in cast("str", compat["version_visible"])
    assert "评审" in cast("str", compat["major_change"])

    spec = cast("dict[str, object]", json.loads(_SNAPSHOT.read_text(encoding="utf-8")))
    frame = cast("dict[str, object]", spec["x-msg-frame"])
    rule = cast("dict[str, object]", frame["version_rule"])
    minor = cast("str", rule["minor"])
    assert "只增可选字段" in minor
    assert "未知字段" in minor
    assert "忽略" in minor


# ---------------------------------------------------------------------------
# U4 零能力护栏：公开快照 / 事件契约 / 端口委托
# ---------------------------------------------------------------------------


def test_no_committed_capability_in_public_contract() -> None:
    """U4：公开快照无相关端点与字段，预留事件类型未登记，事务发布端口仍委托普通发布。"""
    snapshot = _SNAPSHOT.read_text(encoding="utf-8").lower()
    for token in _SNAPSHOT_TOKENS:
        assert token not in snapshot, token

    data = cast("dict[str, object]", _load())
    tx = cast("dict[str, object]", data["transactional_message"])
    reserved = cast("list[object]", tx["reserved_event_types"])
    assert len(reserved) > 0
    if _EVENT_SNAPSHOT.exists():
        event_text = _EVENT_SNAPSHOT.read_text(encoding="utf-8")
        for name in reserved:
            assert cast("str", name) not in event_text
    assert "当前无事件" in _EVENTS_README.read_text(encoding="utf-8")

    source = inspect.getsource(MsgEventPublisher.publish_transactional)
    assert "self.publish(" in source


# ---------------------------------------------------------------------------
# U5 文档断言：不出现肯定式能力宣称
# ---------------------------------------------------------------------------


def test_documents_do_not_claim_postponed_capabilities() -> None:
    """U5：相关文档逐行扫描，跳过否定 / 警示语境后不出现肯定式能力描述。"""
    for rel in _DOC_PATHS:
        path = _REPO / rel
        assert path.exists(), rel
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if any(marker in line for marker in _NEGATIVE_MARKERS):
                continue
            for pattern in _CLAIM_PATTERNS:
                assert re.search(pattern, line) is None, f"{rel}:{number}: {line}"


# ---------------------------------------------------------------------------
# U6 触发条件与不做的后果登记齐全
# ---------------------------------------------------------------------------


def test_triggers_and_consequences_are_registered() -> None:
    """U6：两类条目均登记触发条件、失败分支与不做的后果，投入归口可读。"""
    data = cast("dict[str, object]", _load())
    for section in _SECTION_KEYS:
        block = cast("dict[str, object]", data[section])
        assert len(cast("list[object]", block["trigger_conditions"])) >= 3
        assert len(cast("list[object]", block["failure_branches"])) >= 3
        assert cast("str", block["consequence_if_skipped"])

    review = cast("dict[str, object]", data["review"])
    assert "评审" in cast("str", review["conclusion"])
    assert "阶段计划" in cast("str", review["effort_owner"])
