"""分区模型（`msg_partition`）：主题的物理分片与检查点。

口径（详细设计 §4.2）：分区号在主题内唯一、**只追加不重排**（重排会破坏 key → partition 映射）；
`replicas` / `checkpoint_leo` / `checkpoint_hw` / `epoch` 在本阶段就建好列，避免 8B_05 复制落地时改表
（架构设计 D13：`epoch` 用于拒绝旧 leader 的过期写入）。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import BigInteger, DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

SWITCH_STATE_IDLE = "idle"
"""切换状态：空闲（可写；leader 由 `leader` 列决定）。"""

SWITCH_STATE_SWITCHING = "switching"
"""切换状态：切换窗口（所有写被拒，等旧租约过期后由目标节点激活）。"""

SWITCH_RESULT_TRUNCATED = "truncated"
"""切换结果：按真实 HW 截断了新 leader 的未提交尾部。"""

SWITCH_RESULT_RETAINED = "retained"
"""切换结果：旧 leader 不可达，保留未提交尾部（可能转正）。"""

SWITCH_RESULT_NOOP = "noop"
"""切换结果：无需截断（目标 LEO 未超过水位）。"""


class MsgPartition(BaseModel):
    """分区（`msg_partition`）：LEO / HW 只存检查点，实时值在内存（重启后由日志扫描恢复）。"""

    __tablename__ = "msg_partition"
    __table_args__ = (
        UniqueConstraint("topic_id", "partition_no", name="uq_msg_partition_no"),
        Index("idx_msg_partition_topic", "topic_id"),
    )

    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="逻辑外键 → msg_topic.id（同库，不建物理外键）")
    partition_no: Mapped[int] = mapped_column(Integer, comment="分区号（0 起，只追加不重排）")
    leader: Mapped[str] = mapped_column(String(128), comment="当前 leader 标识（首期单机为实例标识）")
    replicas: Mapped[ConcurrentStableList[str]] = mapped_column(
        StableJson, default=list, comment="副本列表（8B_05 复制使用）"
    )
    checkpoint_leo: Mapped[int] = mapped_column(BigInteger, default=0, comment="检查点 LEO（周期落库；实时值在内存）")
    checkpoint_hw: Mapped[int] = mapped_column(BigInteger, default=0, comment="检查点 HW（提交位点）")
    epoch: Mapped[int] = mapped_column(Integer, default=0, comment="领导者任期（8B_05 防脑裂；本阶段预留）")
    min_insync_replicas: Mapped[int] = mapped_column(
        Integer, default=0, comment="acks=all 的最小同步副本数（0 = 继承全局；02-4）"
    )
    switch_state: Mapped[str] = mapped_column(
        String(16), default=SWITCH_STATE_IDLE, comment="切换状态（idle / switching；02-5）"
    )
    switch_started_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="切换开始时刻（UTC；审计与排障，不用于跨节点时钟判定）"
    )
    switch_from_leader: Mapped[str | None] = mapped_column(
        String(128), nullable=True, comment="切换前 leader（中断重试时据此探测旧 leader）"
    )
    switch_from_epoch: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="切换前 epoch（中断重试时校验同一轮切换）"
    )
    last_switch_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="最近一次成功切换时刻（UTC；02-5）"
    )
    last_switch_by: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="最近一次切换触发人")
    last_switch_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="最近一次切换后的 epoch")
    last_switch_result: Mapped[str | None] = mapped_column(
        String(16), nullable=True, comment="最近一次切换结果（truncated / retained / noop）"
    )
    last_truncate_to: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True, comment="最近一次切换截断后的 LEO；NULL = 未截断（保留）"
    )
