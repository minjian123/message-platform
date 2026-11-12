"""分区迁移台账模型（`msg_partition_migration`）：一次 rebalance 计划项的进度与结果。

口径（02-8 详细设计 §7.1 / §7.4 / §11.1）：

- **一计划一分区一行**：`(plan_id, topic_id, partition_no)` 唯一；替换类动作用同一行按
  `expand` → `shrink` 两步执行，不拆两行（避免唯一约束冲突与进度分裂）；
- **只增不改语义**：状态从 `planned` 起单向推进，`failed` / `degraded` / `cancelled` 为终态；
  `catching_up` 可被 `status` 反复刷新进度，重启后仍可读；
- **不软删除、不归档**：行极小、追溯价值高，保留策略归后续运维任务（本任务不自动清理）；
- **无物理外键**：`topic_id` 逻辑引用 `msg_topic.id`（主题删除后成为孤儿引用，属预期）。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import BigInteger, Boolean, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

PLAN_STATUS_PLANNED = "planned"
"""计划项：只生成动作，未改任何运行态。"""

PLAN_STATUS_RUNNING = "running"
"""执行中：正在切主 / 写 replicas。"""

PLAN_STATUS_CATCHING_UP = "catching_up"
"""追平中：分配已生效，目标副本在追平（写入不中断）。"""

PLAN_STATUS_SUCCEEDED = "succeeded"
"""成功：目标进 ISR 且达到 leader HW（扩容）/ 已从副本集移除（缩容）。"""

PLAN_STATUS_FAILED = "failed"
"""失败：预检 / 切主 / 写分配失败。"""

PLAN_STATUS_DEGRADED = "degraded"
"""降级：force 切主或 `isr_verified=false`；不计通过，需人工复核。"""

PLAN_STATUS_CANCELLED = "cancelled"
"""已取消：运维取消未执行项。"""

PLAN_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        PLAN_STATUS_PLANNED,
        PLAN_STATUS_RUNNING,
        PLAN_STATUS_CATCHING_UP,
        PLAN_STATUS_SUCCEEDED,
        PLAN_STATUS_FAILED,
        PLAN_STATUS_DEGRADED,
        PLAN_STATUS_CANCELLED,
    )
)
"""台账状态闭集（`planned` → 单向推进）。"""

PLAN_TERMINAL_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (PLAN_STATUS_SUCCEEDED, PLAN_STATUS_FAILED, PLAN_STATUS_DEGRADED, PLAN_STATUS_CANCELLED)
)
"""终态（不再推进；`status` 命令只刷新非终态项）。"""

ACTION_EXPAND = "expand"
"""动作：扩容（新增副本并追平）。"""

ACTION_SHRINK = "shrink"
"""动作：缩容（移除副本，必要时先切主）。"""

ACTION_REPLACE = "replace"
"""动作：替换（先扩后缩，同一行两步）。"""

ACTION_LEADER_MOVE = "leader_move"
"""动作：仅移动 leader（副本集不变）。"""

ACTION_NOOP = "noop"
"""动作：无需变更。"""

MIGRATION_ACTIONS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (ACTION_EXPAND, ACTION_SHRINK, ACTION_REPLACE, ACTION_LEADER_MOVE, ACTION_NOOP)
)
"""迁移动作闭集。"""

RESULT_TRUNCATED = "truncated"
"""切换结果：按真实 HW 截断。"""

RESULT_RETAINED = "retained"
"""切换结果：旧 leader 不可达，保留未提交尾部。"""

RESULT_NOOP = "noop"
"""切换结果：无需截断。"""

MIGRATION_RESULTS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (RESULT_TRUNCATED, RESULT_RETAINED, RESULT_NOOP)
)
"""切换结果闭集（台账非切换类动作为空串）。"""


class MsgPartitionMigration(BaseModel):
    """分区迁移台账（`msg_partition_migration`）。"""

    __tablename__ = "msg_partition_migration"
    __table_args__ = (
        UniqueConstraint("plan_id", "topic_id", "partition_no", name="uq_msg_partition_migration_item"),
        Index("idx_msg_partition_migration_plan", "plan_id"),
        Index("idx_msg_partition_migration_partition", "topic_id", "partition_no"),
        Index("idx_msg_partition_migration_status", "status"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变）")
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间快照（审计定位用）")
    topic: Mapped[str] = mapped_column(String(200), comment="主题名快照（审计定位用）")
    plan_id: Mapped[str] = mapped_column(String(64), comment="计划标识（一次 plan 一组行）")
    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="逻辑外键 → msg_topic.id（同库，不建物理外键）")
    partition_no: Mapped[int] = mapped_column(Integer, comment="分区号")
    attempt: Mapped[int] = mapped_column(Integer, default=1, comment="尝试次数（重新 plan / resume 时递增）")
    action: Mapped[str] = mapped_column(String(16), comment="动作（expand / shrink / replace / leader_move / noop）")
    step: Mapped[str] = mapped_column(
        String(16), default="", comment="执行阶段（replace 的 expand / shrink；其余动作为空）"
    )
    from_replicas: Mapped[ConcurrentStableList[str]] = mapped_column(
        StableJson, default=list, comment="动作前副本集（插入序）"
    )
    to_replicas: Mapped[ConcurrentStableList[str]] = mapped_column(
        StableJson, default=list, comment="目标副本集（插入序）"
    )
    from_leader: Mapped[str] = mapped_column(String(128), default="", comment="动作前 leader")
    to_leader: Mapped[str] = mapped_column(String(128), default="", comment="目标 leader")
    status: Mapped[str] = mapped_column(String(16), default=PLAN_STATUS_PLANNED, comment="台账状态（见 PLAN_STATUSES）")
    result: Mapped[str] = mapped_column(
        String(16), default="", comment="切主结果（truncated / retained / noop；非切换动作为空）"
    )
    isr_verified: Mapped[bool] = mapped_column(Boolean, default=True, comment="切主是否已验证目标在 ISR")
    retained_tail: Mapped[int] = mapped_column(BigInteger, default=0, comment="保留的未提交条数")
    progress_leo: Mapped[int] = mapped_column(BigInteger, default=0, comment="最近一次进度：目标副本 LEO")
    progress_hw: Mapped[int] = mapped_column(BigInteger, default=0, comment="最近一次进度：leader HW")
    remaining_records: Mapped[int] = mapped_column(BigInteger, default=0, comment="剩余待追平条数（0 = 已追平）")
    operator: Mapped[str] = mapped_column(String(128), default="", comment="执行人（审计）")
    reason: Mapped[str] = mapped_column(String(512), default="", comment="执行原因（审计）")
    error: Mapped[str] = mapped_column(String(512), default="", comment="失败原因（截断 512）")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="开始执行时刻（UTC）")
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="进入终态时刻（UTC）")
