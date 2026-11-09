"""重放任务模型（`msg_replay_task`）：重放范围、限速、进度与终态。

口径（详细设计 §6.5）：

- **范围是快照**：`partitions` 在建任务时把「缺省全部分区」解析成显式列表并存库——主题后续扩分区
  不影响已建任务（否则同一个任务会随扩分区改变语义）。
- **进度分子分母独立列**：`total` 是建任务时按条件精确计数的分母，`replayed` 是执行器累加的分子；
  两者都不由对端反推，避免「进度与写入不一致」。
- **游标存 JSON 文本**：游标是任务内部执行位置，没有独立查询需求（按任务整取整存）；拆子表只会多两次
  往返与一批无用索引。列名用 `cursor_state` 避开 SQL 保留字 `cursor`。
- **影响两列分工**（06-6-1）：`impact_groups` 是创建时的影响起点（写一次不改），`impact_snapshot` 是
  终态后保留的最后快照（首次查询已终结任务时写一次）；基线不被快照覆盖，两者都可追溯。
- **不软删除**：终态用 `status` 表达，`deleted_at` 恒为 NULL（与 `msg_topic` 同口径）。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import BigInteger, DateTime, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from bms_msg.core.config import DEFAULT_REPLAY_RATE_PER_SECOND

TASK_PENDING = "pending"
"""待执行（已建、执行器尚未拾取）。"""

TASK_RUNNING = "running"
"""执行中。"""

TASK_PAUSED = "paused"
"""已暂停（批边界生效；恢复后继续）。"""

TASK_CANCELLED = "cancelled"
"""已取消（终态）。"""

TASK_COMPLETED = "completed"
"""已完成（终态；游标全部到底）。"""

TASK_FAILED = "failed"
"""已失败（终态；单批不可恢复异常，`error_msg` 记原因）。"""

TASK_TERMINAL_STATUSES: tuple[str, ...] = (TASK_CANCELLED, TASK_COMPLETED, TASK_FAILED)
"""终态集合（终态不可逆：不提供重开，重试即按原范围重建任务）。"""

REPLAY_STATUSES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        TASK_PENDING,
        TASK_RUNNING,
        TASK_PAUSED,
        TASK_CANCELLED,
        TASK_COMPLETED,
        TASK_FAILED,
    )
)
"""全部合法状态（列表过滤的白名单；非法值按 `10001` 拒绝，不静默当「不过滤」）。"""

DEFAULT_STATUS = TASK_PENDING
"""建任务时的缺省状态。"""


class MsgReplayTask(BaseModel):
    """重放任务（`msg_replay_task`）：一次「按条件把历史再写一遍」的服务端操作。"""

    __tablename__ = "msg_replay_task"
    __table_args__ = (
        Index("idx_msg_replay_task_status", "status"),
        Index("idx_msg_replay_task_tenant", "tenant_id", "created_at"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变）")
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间（范围副本，排障可读）")
    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="主题主键（逻辑外键 → msg_topic.id）")
    topic: Mapped[str] = mapped_column(String(200), comment="主题名（展示名快照；主题停用后仍可排障）")
    partitions: Mapped[ConcurrentStableList[int]] = mapped_column(
        StableJson, default=list, comment="分区号列表（创建时固化，主题扩分区不影响已建任务）"
    )
    offset_from: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="起始 offset（含）；NULL 不限")
    offset_to: Mapped[int | None] = mapped_column(BigInteger, nullable=True, comment="结束 offset（不含）；NULL 不限")
    time_from: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="起始发布时间（含，UTC）")
    time_to: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="结束发布时间（不含，UTC）")
    filter_key: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="分区键精确过滤；NULL 不过滤（列名避开保留字 key）"
    )
    rate_per_second: Mapped[int] = mapped_column(
        Integer, default=DEFAULT_REPLAY_RATE_PER_SECOND, comment="限速（条/秒）"
    )
    status: Mapped[str] = mapped_column(String(16), default=DEFAULT_STATUS, comment="状态（五态 + 两个错误终态）")
    total: Mapped[int] = mapped_column(BigInteger, default=0, comment="选中总条数（进度分母）")
    replayed: Mapped[int] = mapped_column(BigInteger, default=0, comment="已写入条数（进度分子）")
    cursor_state: Mapped[ConcurrentStableList[ConcurrentStableDict[str, int]]] = mapped_column(
        StableJson,
        default=list,
        comment='分区游标 [{"partition":0,"next":12,"end":100}]（列名避开保留字 cursor）',
    )
    log_id_first: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="首条成功写入的记录标识")
    log_id_last: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="末条成功写入的记录标识")
    error_msg: Mapped[str | None] = mapped_column(String(512), nullable=True, comment="失败原因（截断 512）")
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="终态时刻（UTC）")
    impact_groups: Mapped[ConcurrentStableDict[str, object]] = mapped_column(
        StableJson,
        default=dict,
        comment='影响基线（创建时写一次不改）：{"captured_at":"...","truncated":false,"groups":[...]}',
    )
    impact_snapshot: Mapped[ConcurrentStableDict[str, object] | None] = mapped_column(
        StableJson,
        nullable=True,
        default=None,
        comment="终态影响快照（首次查询已终结任务时写一次）：{captured_at, truncated, groups}",
    )
