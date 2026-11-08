"""审计动作 / 类别 / helper 契约（05-4；Kiwi 2455）。"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import pytest
from bms_core.audit.classes import AUDIT_CLASS_RECORD_OPERATION, AUDIT_CLASS_SENSITIVE, audit_class_for
from bms_core.core.exceptions import InternalError
from bms_core.events.base import EventEnvelope

from bms_msg.audit.trail import (
    ACCESS_DENIED_ACTION,
    ACL_GRANT_ACTION,
    ACTION_CLASSES,
    DELAY_DISCARD_ACTION,
    MESSAGE_DELETE_ACTION,
    OFFSET_PURGE_ACTION,
    OFFSET_RESET_ACTION,
    QUOTA_UPDATE_ACTION,
    REPLAY_ACTION,
    TOPIC_CREATE_ACTION,
    emit_access_denied,
    emit_acl_change,
    emit_record_operation,
    emit_sensitive,
)
from bms_msg.broker.offset import OFFSET_PURGE_ACTION as EXISTING_OFFSET_PURGE
from bms_msg.broker.offset import OFFSET_RESET_ACTION as EXISTING_OFFSET_RESET
from bms_msg.broker.replay import REPLAY_ACTION as EXISTING_REPLAY

pytestmark = pytest.mark.kiwi_id(2455)


class _Sink:
    """审计替身：记录事件 / 强审计开关，可按次返回失败。"""

    def __init__(self, *, result: bool = True) -> None:
        """初始化。

        Args:
            result: `emit` 的返回值。
        """
        self.events: list[EventEnvelope] = []
        self.required: list[bool] = []
        self.result = result

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """记录一次写入。

        Args:
            event: 审计事件。
            required: 强审计开关。

        Returns:
            bool: `self.result`。
        """
        self.events.append(event)
        self.required.append(required)
        return self.result


class _ExplodingSink:
    """审计替身：发射即抛。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """恒抛异常。

        Args:
            event: 审计事件（忽略）。
            required: 强审计开关（忽略）。

        Returns:
            bool: 不返回。

        Raises:
            RuntimeError: 恒抛。
        """
        del event, required
        raise RuntimeError("audit down")


def test_action_classes_match_contract() -> None:
    """动作 → 类别：配置 / 权限强审计，记录级带定位，访问异常普通。"""
    assert audit_class_for(TOPIC_CREATE_ACTION, ACTION_CLASSES).strong is True
    assert audit_class_for(QUOTA_UPDATE_ACTION, ACTION_CLASSES).class_key == AUDIT_CLASS_SENSITIVE
    assert audit_class_for(ACL_GRANT_ACTION, ACTION_CLASSES).strong is True
    assert audit_class_for(DELAY_DISCARD_ACTION, ACTION_CLASSES).class_key == AUDIT_CLASS_RECORD_OPERATION
    assert audit_class_for(DELAY_DISCARD_ACTION, ACTION_CLASSES).record_level is True
    assert audit_class_for(MESSAGE_DELETE_ACTION, ACTION_CLASSES).record_level is True
    assert audit_class_for(ACCESS_DENIED_ACTION, ACTION_CLASSES).strong is False


def test_existing_action_constants_align() -> None:
    """既有重放 / 位点常量与 07D 03 实现一致（避免映射漂移）。"""
    assert REPLAY_ACTION == EXISTING_REPLAY
    assert OFFSET_RESET_ACTION == EXISTING_OFFSET_RESET
    assert OFFSET_PURGE_ACTION == EXISTING_OFFSET_PURGE


async def test_emit_sensitive_carries_scope_and_required() -> None:
    """配置变更：一条强审计，作用域字段与载荷正确。"""
    sink = _Sink()
    await emit_sensitive(
        sink,
        action=TOPIC_CREATE_ACTION,
        tenant_id=7001,
        params={"topic": "audit.operation"},
        method="POST",
        path="/api/v1/msg/admin/topics",
        module_key="msg",
        resource_key="audit.operation",
    )

    event = sink.events[0]
    assert (event.event_type, event.payload["action"], event.payload["module"]) == (
        "sys.operation.logged",
        TOPIC_CREATE_ACTION,
        "msg",
    )
    assert (event.payload["tenant_id"], event.payload["module_key"], event.payload["resource_key"]) == (
        "7001",
        "msg",
        "audit.operation",
    )
    assert sink.required == [True]


async def test_emit_sensitive_rejected_raises() -> None:
    """强审计被出口拒绝（False）→ `InternalError`。"""
    with pytest.raises(InternalError):
        await emit_sensitive(
            _Sink(result=False),
            action=QUOTA_UPDATE_ACTION,
            tenant_id=7001,
            params={"items": []},
            method="PUT",
            path="/api/v1/msg/admin/quotas",
        )


async def test_emit_sensitive_non_strong_uses_normal_path() -> None:
    """非强类别（访问异常）走普通路径：失败返回 False，不抛。"""
    assert (
        await emit_sensitive(
            _Sink(result=False),
            action=ACCESS_DENIED_ACTION,
            tenant_id=7001,
            params=None,
            method="GET",
            path="/api/v1/msg/admin/topics",
        )
        is False
    )


async def test_emit_sensitive_exception_raises() -> None:
    """强审计出口抛异常 → `InternalError`（不静默继续）。"""
    with pytest.raises(InternalError):
        await emit_sensitive(
            _ExplodingSink(),
            action=TOPIC_CREATE_ACTION,
            tenant_id=7001,
            params=None,
            method="POST",
            path="/api/v1/msg/admin/topics",
        )


async def test_emit_record_operation_carries_record_ref() -> None:
    """记录级：`record_ref = msg.message/log_id`，强审计。"""
    sink = _Sink()
    await emit_record_operation(
        sink,
        action=DELAY_DISCARD_ACTION,
        tenant_id=7001,
        log_id="l-123",
        params={"reason": "poison"},
        method="POST",
        path="/api/v1/msg/admin/delayed/l-123/discard",
        module_key="msg",
        resource_key="audit.operation",
    )

    event = sink.events[0]
    ref = cast("Mapping[str, object]", event.payload["record_ref"])
    assert (ref["type"], ref["id"]) == ("msg.message", "l-123")
    assert sink.required == [True]


async def test_emit_message_delete_contract() -> None:
    """删消息契约：动作 / `record_ref` / 强审计可用（实际触发归 06-2）。"""
    sink = _Sink()
    await emit_record_operation(
        sink,
        action=MESSAGE_DELETE_ACTION,
        tenant_id=7001,
        log_id="l-456",
        params={"reason": "gdpr"},
        method="DELETE",
        path="/api/v1/msg/admin/messages/l-456",
    )

    event = sink.events[0]
    ref = cast("Mapping[str, object]", event.payload["record_ref"])
    assert (event.payload["action"], ref["type"], ref["id"]) == (MESSAGE_DELETE_ACTION, "msg.message", "l-456")
    assert sink.required == [True]


async def test_emit_acl_change_contract() -> None:
    """权限变更契约：ACL 动作 / 主体 / 权限 / 效果 / 作用域，强审计。"""
    sink = _Sink()
    await emit_acl_change(
        sink,
        action=ACL_GRANT_ACTION,
        tenant_id=7001,
        principal="user:1001",
        permission="subscribe",
        effect="allow",
        module_key="msg",
        resource_key="audit.operation",
    )

    event = sink.events[0]
    params = cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))
    assert (event.payload["action"], event.payload["module_key"], event.payload["resource_key"]) == (
        ACL_GRANT_ACTION,
        "msg",
        "audit.operation",
    )
    assert (params["principal"], params["permission"], params["effect"]) == ("user:1001", "subscribe", "allow")
    assert sink.required == [True]


async def test_emit_access_denied_is_normal_and_carries_status() -> None:
    """访问异常：普通审计（required=False），带状态 / 原因 / 尝试动作。"""
    sink = _Sink()
    await emit_access_denied(
        sink,
        tenant_id=7001,
        status=403,
        method="POST",
        path="/api/v1/msg/publish",
        attempted_action="msg:publish",
        cost_time=12,
    )

    event = sink.events[0]
    params = cast("dict[str, object]", json.loads(cast("str", event.payload["params"])))
    assert event.payload["status"] == 403
    assert event.payload["error_msg"] == "HTTP 403"
    assert params == {"status": 403, "reason": "forbidden", "attempted_action": "msg:publish"}
    assert event.payload["cost_time"] == 12
    assert sink.required == [False]


async def test_emit_access_denied_failure_is_swallowed() -> None:
    """访问异常写失败：返回 False，不抛（不影响原拒绝响应）。"""
    assert (
        await emit_access_denied(
            _ExplodingSink(),
            tenant_id=None,
            status=401,
            method="GET",
            path="/api/v1/msg/admin/topics",
        )
        is False
    )
    assert (
        await emit_access_denied(
            _Sink(result=False),
            tenant_id=None,
            status=429,
            method="POST",
            path="/api/v1/msg/publish",
        )
        is False
    )
