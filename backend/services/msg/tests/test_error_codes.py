"""产品自持错误码声明 ↔ 代码逐条对账（msg 任务 `01-1-1`；Kiwi 2411）。

`deploy/registry/msg.toml` 的 `[[errcodes]]` 是对外承诺的唯一声明，须与 `bms_msg/errors.py` 的
`MsgErrorCode` 枚举及异常类 `http_status_` 逐条一致：码集合相同、`http_status` 相同、
`message_key` 形如 `msg.error.<snake>`、描述与级别齐备；并验证声明式映射与装配声明已就位
（07D 任务 03 附录 A / B）。
"""

import inspect
import tomllib
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.plugin import default_plugin_registry, resolve_plugin
from bms_core.extension import SCOPE_RULE_KEY
from bms_core.resource_scope.rules import BaseScopeRule
from fastapi import FastAPI

from bms_msg import errors as errors_module
from bms_msg.errors import MsgError, MsgErrorCode
from bms_msg.main import ApplicationFactory
from bms_msg.tenant.scope_rule import MSG_NAMESPACE_RULE, MsgNamespaceRule

pytestmark = pytest.mark.kiwi_id(2411)

_REPO_ROOT = Path(__file__).resolve().parents[4]
"""msg 仓根（本文件位于 `backend/services/msg/tests/`）。"""

_MANIFEST = _REPO_ROOT / "deploy" / "registry" / "msg.toml"
"""产品自持声明（平台侧装载的同一份）。"""


def _declared() -> ConcurrentStableDict[int, ConcurrentStableDict[str, object]]:
    """读取清单 `[[errcodes]]` 段（`code → 声明`）。

    Returns:
        ConcurrentStableDict[int, ConcurrentStableDict[str, object]]: 声明映射。
    """
    payload = tomllib.loads(_MANIFEST.read_text(encoding="utf-8"))
    declared: ConcurrentStableDict[int, ConcurrentStableDict[str, object]] = ConcurrentStableDict()
    for item in payload.get("errcodes", []):
        declared.set(cast("int", item["code"]), ConcurrentStableDict(item))
    return declared


def _raised() -> ConcurrentStableDict[int, int]:
    """取代码中实际抛出的「码 → HTTP 状态」（异常类预置值）。

    Returns:
        ConcurrentStableDict[int, int]: 状态映射。
    """
    statuses: ConcurrentStableDict[int, int] = ConcurrentStableDict()
    # 04-4：MsgRpcInProgressError 复用 bms 平台码 10007（不新增产品码，见设计 A3），
    # 它不是 msg 自持声明的一员，不参与本文件的「声明 ↔ 产品码」对账。
    product_codes = {int(member) for member in MsgErrorCode}
    for _, member in inspect.getmembers(errors_module, lambda item: isinstance(item, type)):
        if not issubclass(member, MsgError) or member is MsgError:
            continue
        error = member()
        if error.code in product_codes:
            statuses.set(error.code, error.http_status)
    return statuses


def test_declared_codes_match_enum() -> None:
    """码集合：声明与 `MsgErrorCode` 枚举完全一致（不漏登、不多登）。"""
    assert set(_declared()) == {int(member) for member in MsgErrorCode}


def test_declared_status_matches_exception() -> None:
    """状态一致：声明 `http_status` 与异常类预置值逐条相等（避免渲染期状态漂移告警）。"""
    assert {code: int(cast("int", item["http_status"])) for code, item in _declared().items()} == dict(_raised())


def test_declared_message_keys_and_severity() -> None:
    """文案键与级别：`msg.error.<snake>`，级别取 info / warning / error，描述非空。"""
    for code, item in _declared().items():
        key = str(item["message_key"])
        assert key.startswith("msg.error."), code
        assert key[len("msg.error.") :].isidentifier() and key[len("msg.error.") :].islower(), code
        assert item["severity"] in {"info", "warning", "error"}, code
        assert str(item["description"]).strip(), code


def test_declared_maps_from() -> None:
    """声明式映射：两条租户码各映射到本段位产品码，状态语义与基座码一致（附录 B）。"""
    declared = _declared()
    assert list(cast("list[int]", declared[350011]["maps_from"])) == [80002]
    assert list(cast("list[int]", declared[350012]["maps_from"])) == [80001]
    assert int(cast("int", declared[350011]["http_status"])) == 403
    assert int(cast("int", declared[350012]["http_status"])) == 404


def test_extension_requirements_declares_scope_rule() -> None:
    """装配声明：只声明命名空间规则一项，且为必需（缺失即拒启，附录 A）。"""
    requirements = list(ApplicationFactory().extension_requirements())
    assert len(requirements) == 1
    assert requirements[0].plugin_key == SCOPE_RULE_KEY
    assert requirements[0].plugin_name == MSG_NAMESPACE_RULE
    assert requirements[0].required is True


def test_configure_plugins_exposes_scope_rule() -> None:
    """装配钩子导入规则模块后，自动候选可按 `scope_rule:msg.namespace` 解析。"""
    registry = default_plugin_registry()
    registry.reset()
    try:
        ApplicationFactory().configure_plugins(FastAPI(), settings=cast("object", None))  # type: ignore[arg-type]
        resolved = resolve_plugin(SCOPE_RULE_KEY, MSG_NAMESPACE_RULE, expected_version=BaseScopeRule.contract_version)
        assert isinstance(resolved, MsgNamespaceRule)
    finally:
        registry.reset()
