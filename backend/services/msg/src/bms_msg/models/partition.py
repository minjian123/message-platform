"""分区模型（`msg_partition`）：主题的物理分片与检查点。

口径（详细设计 §4.2）：分区号在主题内唯一、**只追加不重排**（重排会破坏 key → partition 映射）；
`replicas` / `checkpoint_leo` / `checkpoint_hw` / `epoch` 在本阶段就建好列，避免 8B_05 复制落地时改表
（架构设计 D13：`epoch` 用于拒绝旧 leader 的过期写入）。
"""

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import BigInteger, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column


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
