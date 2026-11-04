"""产品自持错误码声明 ↔ 代码逐条对账（msg 任务 `01-1-1`；Kiwi 2411）。

`deploy/registry/msg.toml` 的 `[[errcodes]]` 是对外承诺的唯一声明，须与 `bms_msg/errors.py` 的
`MsgErrorCode` 枚举及异常类 `http_status_` 逐条一致：码集合相同、`http_status` 相同、
`message_key` 形如 `msg.error.<snake>`、描述与级别齐备；并验证装配钩子登记了产品码覆盖。
"""

import inspect
import tomllib
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import BizError
from bms_core.core.plugin import default_plugin_registry, resolve_plugin
from bms_core.error_mapper.base import BaseErrorMapper
from fastapi import FastAPI

from bms_msg import errors as errors_module
from bms_msg.errors import (
    MSG_ERROR_MAPPER,
    MsgError,
    MsgErrorCode,
    MsgErrorMapper,
    MsgNamespaceInvalidError,
)
from bms_msg.main import ApplicationFactory

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
    for _, member in inspect.getmembers(errors_module, lambda item: isinstance(item, type)):
        if not issubclass(member, MsgError) or member is MsgError:
            continue
        error = member()
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


def test_mapper_keeps_other_errors() -> None:
    """覆盖只针对租户两个码：msg 自有异常与其他基座异常一律原样返回（不越权改写）。"""
    mapper = MsgErrorMapper()
    own = MsgNamespaceInvalidError()
    other = BizError(10001)
    assert mapper.map(own) is own
    assert mapper.map(other) is other


def test_configure_plugins_registers_error_mapper() -> None:
    """装配钩子登记产品码覆盖：`configure_plugins()` 后可按 `error_mapper:msg` 解析。"""
    registry = default_plugin_registry()
    registry.reset()
    try:
        ApplicationFactory().configure_plugins(FastAPI(), settings=cast("object", None))  # type: ignore[arg-type]
        resolved = resolve_plugin("error_mapper", MSG_ERROR_MAPPER, expected_version=BaseErrorMapper.contract_version)
        assert isinstance(resolved, MsgErrorMapper)
    finally:
        registry.reset()
