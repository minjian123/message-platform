"""审计留痕动作、类别与发射 helper（05-4；需求 05-5）。

**单一映射**：`ACTION_CLASSES` 同时供 `ApplicationFactory.audit_classes()` 声明与运行期 helper 取
`strong` / `record_level`，避免「声明强、实际弱」。动作命名见详细设计 §5.3。

**写入出口**：全部经基座 `build_operation_event()` + `AuditSink.emit()` 写 `sys.operation.logged`；
不自建审计表、不建第二套事件总线、不删除审计记录。

**失败口径**（bms 07D `02_04`）：配置 / 权限 / 记录级操作强审计，失败抛 `InternalError`（10000）；
访问异常普通审计，失败只记日志，不影响原拒绝响应。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Protocol

from bms_core.audit.base import RecordRef
from bms_core.audit.classes import (
    AUDIT_CLASS_RECORD_OPERATION,
    AUDIT_CLASS_SENSITIVE,
    AuditClass,
    audit_class_for,
)
from bms_core.audit.events import build_operation_event
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.context import get_current_client_ip, get_current_trace_id, get_current_user_id
from bms_core.core.exceptions import InternalError
from bms_core.core.logging import get_logger
from bms_core.events.base import EventEnvelope

__all__ = [
    "ACCESS_AUDIT_CLASS",
    "ACCESS_DENIED_ACTION",
    "ACL_GRANT_ACTION",
    "ACL_REVOKE_ACTION",
    "ACL_UPDATE_ACTION",
    "ACTION_CLASSES",
    "DELAY_DISCARD_ACTION",
    "DLQ_ARCHIVE_ACTION",
    "DLQ_IGNORE_ACTION",
    "DLQ_REPLAY_ACTION",
    "LEADER_SWITCH_ACTION",
    "MESSAGE_DELETE_ACTION",
    "MESSAGE_REF_TYPE",
    "MODULE",
    "OFFSET_PURGE_ACTION",
    "OFFSET_REF_TYPE",
    "OFFSET_RESET_ACTION",
    "QUOTA_UPDATE_ACTION",
    "REBALANCE_ACTION",
    "REPLAY_ACTION",
    "REPLAY_REF_TYPE",
    "TOPIC_CREATE_ACTION",
    "TOPIC_DELETE_ACTION",
    "TOPIC_EXPAND_ACTION",
    "TOPIC_UPDATE_ACTION",
    "AuditEmitter",
    "emit_access_denied",
    "emit_acl_change",
    "emit_record_operation",
    "emit_sensitive",
]

_LOGGER = get_logger("bms_msg.audit")

MODULE = "msg"
"""审计事件所属模块（`payload.module`）。"""

TOPIC_CREATE_ACTION = "msg:topic.create"
"""建主题（配置变更，强审计）。"""

TOPIC_UPDATE_ACTION = "msg:topic.update"
"""改保留期（配置变更，强审计；06-2）。"""

TOPIC_EXPAND_ACTION = "msg:topic.expand"
"""扩分区（配置变更，强审计；06-2）。"""

TOPIC_DELETE_ACTION = "msg:topic.delete"
"""删主题受理（配置变更，强审计；06-2）。

**只在请求侧写一条**：执行结果随时间变化且可能重试多轮，写进审计会产出同动作多条记录，
破坏「动作 ↔ 留痕」单一映射；执行结果由清理台账（`msg_topic_cleanup`）与结构化日志承担。
"""

QUOTA_UPDATE_ACTION = "msg:quota.update"
"""配额调整（配置变更，强审计）。"""

ACL_GRANT_ACTION = "msg:acl.grant"
"""ACL 授予（权限变更，强审计；05-3 实现）。"""

ACL_REVOKE_ACTION = "msg:acl.revoke"
"""ACL 收回（权限变更，强审计；05-3 实现）。"""

ACL_UPDATE_ACTION = "msg:acl.update"
"""ACL 变更（权限变更，强审计；05-3 实现）。"""

REPLAY_ACTION = "msg:replay"
"""重放（既有动作；仅用于类别查询与对账，不迁移既有写入）。"""

OFFSET_RESET_ACTION = "msg:offset.reset"
"""位点重置（既有动作；仅用于类别查询与对账）。"""

OFFSET_PURGE_ACTION = "msg:offset.purge"
"""位点清理（既有动作；仅用于类别查询与对账）。"""

DELAY_DISCARD_ACTION = "msg:delay.discard"
"""延迟转投异常处置（记录级，强审计 + `record_ref`）。"""

DLQ_REPLAY_ACTION = "msg:dlq.replay"
"""死信重放（记录级，强审计 + `record_ref = msg.message/log_id`；06-5）。"""

DLQ_IGNORE_ACTION = "msg:dlq.ignore"
"""死信忽略（记录级，强审计 + `record_ref`；06-5）。"""

DLQ_ARCHIVE_ACTION = "msg:dlq.archive"
"""死信归档（记录级，强审计 + `record_ref`；06-5）。"""

MESSAGE_DELETE_ACTION = "msg:message.delete"
"""删消息（记录级契约；实际触发归 06-2）。"""

ACCESS_DENIED_ACTION = "msg:access.denied"
"""访问异常（普通审计；失败只告警）。"""

LEADER_SWITCH_ACTION = "msg:partition.failover"
"""分区切主（配置变更 / 敏感操作，强审计；02-5）。"""

REBALANCE_ACTION = "msg:partition.rebalance"
"""分区副本重平衡 / 迁移（配置变更 / 敏感操作，强审计；02-8）。"""

ACCESS_AUDIT_CLASS = "access"
"""访问异常类别键（非基座缺省类别，由 msg 自定义）。"""

MESSAGE_REF_TYPE = "msg.message"
"""消息记录的 `record_ref.type`（`id` 为 `log_id`）。"""

REPLAY_REF_TYPE = "msg.replay"
"""重放任务的 `record_ref.type`（`id` 为任务主键）。"""

OFFSET_REF_TYPE = "msg.offset"
"""位点的 `record_ref.type`（`id` 为 `topic_id:partition_no`）。"""

ACTION_CLASSES: ConcurrentStableDict[str, AuditClass] = ConcurrentStableDict(
    {
        TOPIC_CREATE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="建主题"),
        TOPIC_UPDATE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="改保留期"),
        TOPIC_EXPAND_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="扩分区"),
        TOPIC_DELETE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="删主题"),
        QUOTA_UPDATE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="配额调整"),
        ACL_GRANT_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="ACL 授予"),
        ACL_REVOKE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="ACL 收回"),
        ACL_UPDATE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="ACL 变更"),
        REPLAY_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="重放"),
        OFFSET_RESET_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="位点重置"),
        OFFSET_PURGE_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="位点清理"),
        DELAY_DISCARD_ACTION: AuditClass(
            AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="延迟转投异常处置"
        ),
        DLQ_REPLAY_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="死信重放"),
        DLQ_IGNORE_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="死信忽略"),
        DLQ_ARCHIVE_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="死信归档"),
        MESSAGE_DELETE_ACTION: AuditClass(AUDIT_CLASS_RECORD_OPERATION, strong=True, record_level=True, label="删消息"),
        LEADER_SWITCH_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="分区切主"),
        REBALANCE_ACTION: AuditClass(AUDIT_CLASS_SENSITIVE, strong=True, label="分区重平衡"),
        ACCESS_DENIED_ACTION: AuditClass(ACCESS_AUDIT_CLASS, strong=False, label="访问异常"),
    }
)
"""动作 → 留痕类别（唯一映射；`audit_classes()` 与 helper 同源）。"""

_ACCESS_REASONS: ConcurrentStableDict[int, str] = ConcurrentStableDict(
    {401: "unauthenticated", 403: "forbidden", 429: "rate_limited"}
)
"""访问异常状态 → 原因标签。"""


class AuditEmitter(Protocol):
    """审计出口最小协议（生产为基座 `AuditEventEmitter`，用例注入替身）。"""

    async def emit(self, event: EventEnvelope, *, required: bool = False) -> bool:
        """写一条审计事件。

        Args:
            event: 审计事件信封。
            required: 是否强审计（失败时实现应抛错或返回 False）。

        Returns:
            bool: 写入成功 True。
        """
        ...


def _tenant_text(tenant_id: int | str | None) -> str | None:
    """租户主键转审计字段（雪花 ID 一律十进制字符串）。

    Args:
        tenant_id: 租户主键（整数 / 字符串 / 空）。

    Returns:
        str | None: 字符串主键；空值返回 None。
    """
    if tenant_id is None:
        return None
    return str(int(tenant_id)) if isinstance(tenant_id, int) else str(tenant_id)


def _params_text(payload: Mapping[str, object] | None) -> str | None:  # bare-collections:allow（只读映射入参）
    """载荷转 JSON 串（紧凑、键排序；空映射返回 None，避免落空对象）。

    Args:
        payload: 待序列化的载荷。

    Returns:
        str | None: JSON 字符串或 None。
    """
    if not payload:
        return None
    return json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _build_event(
    *,
    action: str,
    tenant_id: int | str | None,
    method: str,
    path: str,
    params: str | None,
    status: int,
    error_msg: str | None = None,
    user_id: int | None = None,
    ip: str | None = None,
    trace_id: str | None = None,
    module_key: str | None = None,
    resource_key: str | None = None,
    record_ref: RecordRef | None = None,
) -> EventEnvelope:
    """构造操作审计事件（公共字段 + 可选作用域 / 定位）。

    Args:
        action: 审计动作。
        tenant_id: 租户主键。
        method: HTTP 方法（内部动作用规划方法）。
        path: 路径或规划路径。
        params: 载荷 JSON。
        status: 结果状态。
        error_msg: 错误摘要。
        user_id: 操作人主键（缺省取请求上下文）。
        ip: 来源 IP（缺省取请求上下文）。
        trace_id: 链路标识（缺省取请求上下文）。
        module_key: 作用域第二级。
        resource_key: 作用域第三级。
        record_ref: 记录级定位。

    Returns:
        EventEnvelope: 审计事件信封。
    """
    return build_operation_event(
        tenant_id=_tenant_text(tenant_id),
        user_id=user_id if user_id is not None else get_current_user_id(),
        username=None,
        module=MODULE,
        action=action,
        method=method,
        path=path,
        query=None,
        params=params,
        ip=ip if ip is not None else get_current_client_ip(),
        user_agent=None,
        cost_time=None,
        status=status,
        error_msg=error_msg,
        trace_id=trace_id if trace_id is not None else get_current_trace_id(),
        module_key=module_key,
        resource_key=resource_key,
        record_ref=record_ref,
    )


async def _emit_required(sink: AuditEmitter, event: EventEnvelope, *, action: str) -> bool:
    """写强审计：失败一律抛 `InternalError`（没留痕就不该生效）。

    Args:
        sink: 审计出口。
        event: 审计事件。
        action: 动作（错误信息与日志用）。

    Returns:
        bool: 恒 True。

    Raises:
        InternalError: 写入失败或被出口拒绝（10000 / 500）。
    """
    try:
        accepted = await sink.emit(event, required=True)
    except Exception as exc:
        _LOGGER.warning("msg_audit_required_failed", action=action, error=repr(exc))
        raise InternalError(f"审计写入失败：{action}") from exc
    if not accepted:
        _LOGGER.warning("msg_audit_required_rejected", action=action)
        raise InternalError(f"审计写入被拒绝：{action}")
    return True


async def _emit_normal(sink: AuditEmitter, event: EventEnvelope, *, action: str) -> bool:
    """写普通审计：失败只记日志，不影响业务。

    Args:
        sink: 审计出口。
        event: 审计事件。
        action: 动作（日志用）。

    Returns:
        bool: 写入成功 True；失败 False。
    """
    try:
        accepted = await sink.emit(event, required=False)
    except Exception as exc:
        _LOGGER.warning("msg_audit_normal_failed", action=action, error=repr(exc))
        return False
    if not accepted:
        _LOGGER.warning("msg_audit_normal_rejected", action=action)
    return accepted


async def emit_sensitive(
    sink: AuditEmitter,
    *,
    action: str,
    tenant_id: int | str | None,
    params: Mapping[str, object] | None,  # bare-collections:allow（只读映射入参）
    method: str,
    path: str,
    module_key: str | None = None,
    resource_key: str | None = None,
    user_id: int | None = None,
    ip: str | None = None,
    trace_id: str | None = None,
) -> bool:
    """写一条配置 / 权限变更审计（类别决定是否强审计）。

    Args:
        sink: 审计出口。
        action: 动作。
        tenant_id: 租户主键。
        params: 载荷。
        method: HTTP 方法。
        path: 路径。
        module_key: 作用域第二级。
        resource_key: 作用域第三级。
        user_id: 操作人主键。
        ip: 来源 IP。
        trace_id: 链路标识。

    Returns:
        bool: 写入成功 True。

    Raises:
        InternalError: 类别为强审计且写入失败。
    """
    event = _build_event(
        action=action,
        tenant_id=tenant_id,
        method=method,
        path=path,
        params=_params_text(params),
        status=200,
        user_id=user_id,
        ip=ip,
        trace_id=trace_id,
        module_key=module_key,
        resource_key=resource_key,
    )
    if audit_class_for(action, ACTION_CLASSES).strong:
        return await _emit_required(sink, event, action=action)
    return await _emit_normal(sink, event, action=action)


async def emit_acl_change(
    sink: AuditEmitter,
    *,
    action: str,
    tenant_id: int | str | None,
    principal: str,
    permission: str,
    effect: str,
    module_key: str,
    resource_key: str,
    method: str = "POST",
    path: str = "/api/v1/msg/admin/acl",
    user_id: int | None = None,
    ip: str | None = None,
    trace_id: str | None = None,
) -> bool:
    """写一条 ACL 权限变更审计（05-3 实现时调用；强审计）。

    Args:
        sink: 审计出口。
        action: `msg:acl.grant` / `revoke` / `update`。
        tenant_id: 租户主键。
        principal: 用户 / 角色标识。
        permission: `publish` / `subscribe` / `describe` / `manage`。
        effect: `allow` / `deny`。
        module_key: 命名空间。
        resource_key: 主题名。
        method: HTTP 方法。
        path: 管理端点路径。
        user_id: 操作人主键。
        ip: 来源 IP。
        trace_id: 链路标识。

    Returns:
        bool: 写入成功 True。

    Raises:
        InternalError: 强审计写入失败。
    """
    params = ConcurrentStableDict(
        {
            "principal": principal,
            "permission": permission,
            "effect": effect,
            "result": "applied",
        }
    )
    return await emit_sensitive(
        sink,
        action=action,
        tenant_id=tenant_id,
        params=params,
        method=method,
        path=path,
        module_key=module_key,
        resource_key=resource_key,
        user_id=user_id,
        ip=ip,
        trace_id=trace_id,
    )


async def emit_record_operation(
    sink: AuditEmitter,
    *,
    action: str,
    tenant_id: int | str | None,
    log_id: str,
    params: Mapping[str, object] | None,  # bare-collections:allow（只读映射入参）
    method: str,
    path: str,
    module_key: str | None = None,
    resource_key: str | None = None,
    user_id: int | None = None,
    ip: str | None = None,
    trace_id: str | None = None,
) -> bool:
    """写一条记录级操作审计（`record_ref = msg.message/log_id`，强审计）。

    Args:
        sink: 审计出口。
        action: 动作（`msg:delay.discard` / `msg:message.delete` 等）。
        tenant_id: 租户主键。
        log_id: 消息记录标识。
        params: 载荷。
        method: HTTP 方法。
        path: 路径。
        module_key: 作用域第二级。
        resource_key: 作用域第三级。
        user_id: 操作人主键。
        ip: 来源 IP。
        trace_id: 链路标识。

    Returns:
        bool: 写入成功 True。

    Raises:
        InternalError: 强审计写入失败。
    """
    event = _build_event(
        action=action,
        tenant_id=tenant_id,
        method=method,
        path=path,
        params=_params_text(params),
        status=200,
        user_id=user_id,
        ip=ip,
        trace_id=trace_id,
        module_key=module_key,
        resource_key=resource_key,
        record_ref=RecordRef(type=MESSAGE_REF_TYPE, id=log_id),
    )
    return await _emit_required(sink, event, action=action)


async def emit_access_denied(
    sink: AuditEmitter,
    *,
    tenant_id: int | str | None,
    status: int,
    method: str,
    path: str,
    attempted_action: str | None = None,
    cost_time: int | None = None,
    user_id: int | None = None,
    ip: str | None = None,
    trace_id: str | None = None,
    module_key: str | None = None,
    resource_key: str | None = None,
) -> bool:
    """写一条访问异常审计（普通审计；失败只告警）。

    Args:
        sink: 审计出口。
        tenant_id: 租户主键。
        status: 实际状态码（401 / 403 / 429）。
        method: HTTP 方法。
        path: 路由模板或原始路径。
        attempted_action: 尝试的权限动作（基座已写入时取）。
        cost_time: 请求耗时（毫秒）。
        user_id: 操作人主键。
        ip: 来源 IP。
        trace_id: 链路标识。
        module_key: 作用域第二级。
        resource_key: 作用域第三级。

    Returns:
        bool: 写入成功 True；失败 False（不影响原响应）。
    """
    params = ConcurrentStableDict({"status": int(status), "reason": _ACCESS_REASONS.get(int(status), "denied")})
    if attempted_action:
        params.set("attempted_action", attempted_action)
    event = _build_event(
        action=ACCESS_DENIED_ACTION,
        tenant_id=tenant_id,
        method=method,
        path=path,
        params=_params_text(params),
        status=status,
        error_msg=f"HTTP {status}",
        user_id=user_id,
        ip=ip,
        trace_id=trace_id,
        module_key=module_key,
        resource_key=resource_key,
    )
    if cost_time is not None:
        event.payload["cost_time"] = cost_time
    return await _emit_normal(sink, event, action=ACCESS_DENIED_ACTION)
