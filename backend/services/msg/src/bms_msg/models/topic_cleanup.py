"""主题删除清理台账模型（`msg_topic_cleanup`）：删主题的异步清理进度与幂等依据。

口径（详细设计 06-2-1 §4.2、§7）：

- **一主题一行**：`topic_id` 唯一；重复受理复用同行，不重复建台账、不重复触发首轮；
- **行不随主题删除**：主题与分区行物理删除后本行保留，用于追溯「谁在什么时候删了什么」；
- **三态**：`pending` / `running` / `completed`——失败不是终态（可自愈故障恢复后由下一轮收口）；
- **步骤键即幂等依据**：固定七项闭集，只作「跳过判据」，无按步骤查询需求，故用 JSON 列而非子表；
- **不软删除**：`deleted_at` 恒为 NULL（与 `msg_topic` / `msg_replay_task` 同口径）。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

CLEANUP_PENDING = "pending"
"""待执行（已受理、首轮尚未开始，或上一轮失败等待下一轮）。"""

CLEANUP_RUNNING = "running"
"""执行中。"""

CLEANUP_COMPLETED = "completed"
"""已完成（主题与分区行已物理删除）。"""

CLEANUP_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    [CLEANUP_PENDING, CLEANUP_RUNNING, CLEANUP_COMPLETED]
)
"""台账状态闭集。"""

STEP_MESSAGES = "messages"
"""步骤：删除分区日志段目录。"""

STEP_DELAY = "delay"
"""步骤：清理延迟区记录与时间轮。"""

STEP_OFFSETS = "offsets"
"""步骤：清理该主题在各组下的位点文件。"""

STEP_GROUPS = "groups"
"""步骤：清理该主题的消费组记录。"""

STEP_DLQ = "dlq"
"""步骤：清理该主题的死信。"""

STEP_RUNTIME = "runtime"
"""步骤：清理进程内运行态（在途 / 节流窗口 / 唤醒器 / 目录登记）。"""

STEP_METADATA = "metadata"
"""步骤：物理删除分区行与主题行，并失效缓存。"""

CLEANUP_STEPS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    [
        STEP_MESSAGES,
        STEP_DELAY,
        STEP_OFFSETS,
        STEP_GROUPS,
        STEP_DLQ,
        STEP_RUNTIME,
        STEP_METADATA,
    ]
)
"""清理步骤（**顺序即执行序**，不可交换：见详细设计 §7.1）。"""

DEFAULT_STATUS = CLEANUP_PENDING
"""受理时的缺省台账状态。"""


class MsgTopicCleanup(BaseModel):
    """主题删除清理台账（`msg_topic_cleanup`）。"""

    __tablename__ = "msg_topic_cleanup"
    __table_args__ = (
        UniqueConstraint("topic_id", name="uq_msg_topic_cleanup_topic"),
        Index("idx_msg_topic_cleanup_status", "status"),
        Index("idx_msg_topic_cleanup_tenant", "tenant_id", "created_at"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变）")
    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="主题主键（逻辑外键 → msg_topic.id）")
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间快照（主题行删除后仍可读）")
    topic: Mapped[str] = mapped_column(String(200), comment="主题名快照（同上）")
    status: Mapped[str] = mapped_column(
        String(16), default=DEFAULT_STATUS, comment="状态（pending / running / completed）"
    )
    done_steps: Mapped[ConcurrentStableList[str]] = mapped_column(
        StableJson, default=list, comment='已完成步骤键（["messages","delay",…]；幂等跳过依据）'
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, comment="已执行轮次（连续失败 ≥3 告警）")
    last_error: Mapped[str | None] = mapped_column(String(512), nullable=True, comment="最后一轮失败原因（截断 512）")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="首轮开始时刻（UTC）")
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="完成时刻（UTC）")
