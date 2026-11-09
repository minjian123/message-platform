"""死信模型（`msg_dead_letter`）：转入原因、失败上下文、五态处置与重放任务关联。

口径（详细设计 06-5-1 §4）：

- **定位三元组唯一**：`(topic_id, partition_no, msg_offset)` 是死信的天然身份，`add` 幂等由数据库兜底；
- **不软删除**：`deleted_at` 恒为 NULL；主题删除 / 租户清理走物理删除（与 06-2 清理台账同口径）；
- **两个时间**：`failed_at` 是转入死信的事件时间（取自 `DeadLetter.failed_at`），`created_at` 是落库时间；
- **状态机**：`pending` / `replaying` / `resolved` / `ignored` / `archived`，迁移只经服务层。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import BigInteger, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

DLQ_STATUS_PENDING = "pending"
"""待处置（转入死信后的初始态）。"""

DLQ_STATUS_REPLAYING = "replaying"
"""重放中（已建重放任务，等待终态回写）。"""

DLQ_STATUS_RESOLVED = "resolved"
"""已解决（重放任务 `completed`）。"""

DLQ_STATUS_IGNORED = "ignored"
"""已忽略（人工确认不再处理，原因必填）。"""

DLQ_STATUS_ARCHIVED = "archived"
"""已归档（终态；从活跃列表隐藏，`contains` 仍命中）。"""

DLQ_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        DLQ_STATUS_PENDING,
        DLQ_STATUS_REPLAYING,
        DLQ_STATUS_RESOLVED,
        DLQ_STATUS_IGNORED,
        DLQ_STATUS_ARCHIVED,
    )
)
"""五态全集（顺序即状态机自然顺序，供筛选与展示）。"""

DLQ_ACTIVE_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (DLQ_STATUS_PENDING, DLQ_STATUS_REPLAYING)
)
"""活跃态：计入 `msg_dlq_total` 存量与「待处置」口径。"""

DLQ_ARCHIVABLE_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (DLQ_STATUS_PENDING, DLQ_STATUS_IGNORED, DLQ_STATUS_RESOLVED)
)
"""允许归档的状态（`replaying` 必须先回 `pending`，`archived` 不再接受处置）。"""

DEFAULT_DLQ_STATUS = DLQ_STATUS_PENDING
"""转入死信时的缺省状态。"""

DEFAULT_DISPOSITION_REASON_MAX_LENGTH = 256
"""处置原因长度上限（忽略原因必填，重放 / 归档可选）。"""


class MsgDeadLetter(BaseModel):
    """死信（`msg_dead_letter`）：一条被隔离、等待人工处置的失败消息。"""

    __tablename__ = "msg_dead_letter"
    __table_args__ = (
        UniqueConstraint("topic_id", "partition_no", "msg_offset", name="uq_msg_dead_letter_key"),
        Index("idx_msg_dead_letter_tenant_status", "tenant_id", "status", "created_at"),
        Index("idx_msg_dead_letter_replay_task", "replay_task_id"),
        Index("idx_msg_dead_letter_topic", "topic_id"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变）")
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间（取自主题归属）")
    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="主题主键（逻辑外键 → msg_topic.id）")
    topic: Mapped[str] = mapped_column(String(200), comment="主题名快照（主题停用 / 改名后仍可排障）")
    partition_no: Mapped[int] = mapped_column(Integer, comment="分区号")
    msg_offset: Mapped[int] = mapped_column(BigInteger, comment="分区内序号（列名避开 SQL 保留字 offset）")
    message_id: Mapped[str] = mapped_column(String(64), comment="原消息 ID（重放不变）")
    log_id: Mapped[str] = mapped_column(String(64), comment="原记录 ID（记录级定位入口）")
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="链路标识（取自消息头）")
    reason: Mapped[str] = mapped_column(
        String(32), comment="转入原因（delivery_exhausted / schema_invalid / processing_failed）"
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, comment="累计投递次数")
    detail: Mapped[ConcurrentStableDict[str, str]] = mapped_column(
        StableJson,
        default=dict,
        comment="失败详情（字段名 → 说明；消费端上报时原样保存）",
    )
    status: Mapped[str] = mapped_column(String(16), default=DEFAULT_DLQ_STATUS, comment="状态（五态）")
    disposition_reason: Mapped[str | None] = mapped_column(
        String(DEFAULT_DISPOSITION_REASON_MAX_LENGTH), nullable=True, comment="处置原因（忽略必填）"
    )
    disposed_by: Mapped[int | None] = mapped_column(SnowflakeId, nullable=True, comment="处置人（雪花主键）")
    disposed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="处置时间（UTC）")
    replay_task_id: Mapped[int | None] = mapped_column(
        SnowflakeId, nullable=True, comment="关联重放任务（msg_replay_task.id；仅 replaying 有值）"
    )
    failed_at: Mapped[datetime] = mapped_column(DateTime, comment="转入死信的事件时间（UTC）")


__all__ = [
    "DEFAULT_DISPOSITION_REASON_MAX_LENGTH",
    "DEFAULT_DLQ_STATUS",
    "DLQ_ACTIVE_STATUSES",
    "DLQ_ARCHIVABLE_STATUSES",
    "DLQ_STATUSES",
    "DLQ_STATUS_ARCHIVED",
    "DLQ_STATUS_IGNORED",
    "DLQ_STATUS_PENDING",
    "DLQ_STATUS_REPLAYING",
    "DLQ_STATUS_RESOLVED",
    "MsgDeadLetter",
]
