"""延迟转投异常处置服务原语（05-4；需求 05-5）。

**语义**：运维 / 管理员对一条长期无法转投的延迟记录做人工移除（丢弃），不是把自动重试改成丢弃。
自动重试仍按 `DelayScheduler._defer()`：`attempts + 1`、下一轮重试、永不自动丢弃。

**顺序**：先移除（延迟区 + 时间轮）→ 写强审计（`record_ref = msg.message/log_id`）→ 成功返回；
审计失败把记录重新入队并回到时间轮，再上抛使业务失败。这样不会出现「审计已记丢弃、实际没丢」的
假审计，也不会在审计不可用时静默丢弃消息。

**端点**：本期不挂 HTTP 端点；06-2 的「延迟区取消」端点复用本服务。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.db.sync import DbSession

from bms_msg.audit.trail import DELAY_DISCARD_ACTION, AuditEmitter, emit_record_operation
from bms_msg.broker.delay import DelayRecord, DelayStore
from bms_msg.broker.scheduler import DelayWheel
from bms_msg.errors import MsgMessageNotFoundError

__all__ = ["DELAY_DISCARD_PATH", "DelayDisposalService"]

_LOGGER = get_logger("bms_msg.delay_disposal")

DELAY_DISCARD_PATH = "/api/v1/msg/admin/delayed/{log_id}/discard"
"""延迟处置审计路径（规划路径；06-2 端点落地后保持不变）。"""


class DelayDisposalService:
    """延迟记录人工处置（丢弃）服务：移除 + 强审计 + 失败恢复。"""

    def __init__(self, *, store: DelayStore, wheel: DelayWheel, sink: AuditEmitter) -> None:
        """初始化。

        Args:
            store: 延迟区存储（进程级）。
            wheel: 时间轮（与推进器共用同一实例）。
            sink: 审计出口（生产为基座 `AuditEventEmitter`，用例注入替身）。
        """
        self._store = store
        self._wheel = wheel
        self._sink = sink

    async def discard(
        self,
        *,
        tenant_id: int,
        log_id: str,
        reason: str,
        actor_id: int | None = None,
        ip: str | None = None,
        trace_id: str | None = None,
        session: DbSession | None = None,
    ) -> bool:
        """丢弃一条延迟记录（强审计失败即恢复记录并让操作失败）。

        Args:
            tenant_id: 当前租户主键。
            log_id: 延迟记录标识。
            reason: 处置原因（必填，去空白后不得为空）。
            actor_id: 操作人主键。
            ip: 来源 IP。
            trace_id: 链路标识。
            session: 业务会话（给出则审计与业务同事务）。

        Returns:
            bool: 成功 True。

        Raises:
            ParamError: 原因为空（10001）。
            MsgMessageNotFoundError: 记录不存在或不属于当前租户（350003 / 404）。
            InternalError: 强审计写入失败（10000 / 500）。
        """
        reason_text = reason.strip()
        if not reason_text:
            raise ParamError("延迟处置原因必填")
        record = await self._find(tenant_id=int(tenant_id), log_id=log_id)
        if record is None:
            raise MsgMessageNotFoundError(f"延迟记录不存在：{log_id}")
        await self._store.remove(log_id=log_id)
        self._wheel.forget(log_id=log_id)
        try:
            await emit_record_operation(
                self._sink,
                action=DELAY_DISCARD_ACTION,
                tenant_id=int(tenant_id),
                log_id=log_id,
                params=self._payload(record=record, reason=reason_text),
                method="POST",
                path=DELAY_DISCARD_PATH,
                module_key=record.namespace,
                resource_key=record.topic,
                user_id=actor_id,
                ip=ip,
                trace_id=trace_id,
                session=session,
            )
        except Exception:
            await self._restore(record)
            raise
        _LOGGER.info(
            "msg_delay_discarded",
            tenant_id=int(tenant_id),
            log_id=log_id,
            namespace=record.namespace,
            topic=record.topic,
            partition=record.partition_no,
            attempts=record.attempts,
            reason=reason_text,
        )
        return True

    async def _find(self, *, tenant_id: int, log_id: str) -> DelayRecord | None:
        """按租户 + `log_id` 查找延迟记录（租户隔离；不存在返回 None）。

        Args:
            tenant_id: 租户主键。
            log_id: 记录标识。

        Returns:
            DelayRecord | None: 命中的记录。
        """
        records = await self._store.list_for_tenant(tenant_id=tenant_id)
        return next((record for record in records if record.log_id == log_id), None)

    async def _restore(self, record: DelayRecord) -> None:
        """审计失败时把记录放回延迟区与时间轮（尽力恢复，失败记 critical）。

        Args:
            record: 被移除的记录。
        """
        try:
            await self._store.enqueue(record)
            self._wheel.place(record)
        except Exception as exc:
            _LOGGER.critical(
                "msg_delay_discard_restore_failed",
                tenant_id=record.tenant_id,
                log_id=record.log_id,
                error=repr(exc),
            )

    def _payload(self, *, record: DelayRecord, reason: str) -> ConcurrentStableDict[str, object]:
        """构造审计载荷（记录定位 + 处置原因 + 便于排障的上下文）。

        Args:
            record: 被处置记录。
            reason: 处置原因。

        Returns:
            ConcurrentStableDict[str, object]: 载荷。
        """
        return ConcurrentStableDict(
            {
                "log_id": record.log_id,
                "reason": reason,
                "attempts": int(record.attempts),
                "namespace": record.namespace,
                "topic": record.topic,
                "partition": int(record.partition_no),
                "delay_level": record.delay_level,
                "deliver_at": record.deliver_at.isoformat(),
            }
        )
