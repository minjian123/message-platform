"""消费组模型（`msg_consumer_group` / `msg_group_member`；01-4-1 落库）。

口径：

- **组身份 = `(tenant_id, topic_id, name)`**：与位点键 `(group, topic_id, partition_no)` 一一对应；
  `namespace` 只是快照（管理面过滤与排障），不参与判定。
- **列名避开保留字**：组名落 `name` 而不是 `group`（MySQL 保留字），映射回值对象 `group`。
- **成员整替**：每次保存先删该组全部成员行再插入快照，保证「库里就是一份完整快照」。
- **成员租约**：存活判据是 `last_heartbeat + 会话超时`；心跳写穿，`instance_id` 只记最后写入实例，不参与判定。
- **不做软删除**：组删除 / 主题删除 / 租户清理一律物理删除（与 06-2 清理台账同口径）。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId, StableJson
from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

GROUP_STATE_EMPTY = "empty"
"""组状态：存在但无成员（位点保留）。"""

GROUP_STATE_PREPARING_REBALANCE = "preparing_rebalance"
"""组状态：成员集合已变化、分配尚未生效。"""

GROUP_STATE_STABLE = "stable"
"""组状态：分配已生效。"""

GROUP_STATE_DEAD = "dead"
"""组状态：空组超空闲阈值被回收（行与位点保留，重新 join 即恢复）。"""

DEFAULT_GROUP_STATE = GROUP_STATE_EMPTY
"""新建组的初始状态。"""

DEFAULT_GROUP_NAME_MAX_LENGTH = 128
"""组名长度上限（契约层同值）。"""

DEFAULT_MEMBER_ID_MAX_LENGTH = 128
"""成员标识长度上限（契约层同值）。"""


class MsgConsumerGroup(BaseModel):
    """消费组（`msg_consumer_group`）：组元数据 + 世代 + 状态。"""

    __tablename__ = "msg_consumer_group"
    __table_args__ = (
        UniqueConstraint("tenant_id", "topic_id", "name", name="uq_msg_consumer_group_identity"),
        Index("idx_msg_consumer_group_tenant", "tenant_id", "name"),
        Index("idx_msg_consumer_group_topic", "topic_id"),
        Index("idx_msg_consumer_group_state", "state", "updated_at"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键）")
    topic_id: Mapped[int] = mapped_column(SnowflakeId, comment="主题主键（逻辑外键 → msg_topic.id）")
    name: Mapped[str] = mapped_column(
        String(DEFAULT_GROUP_NAME_MAX_LENGTH), comment="组名（列名避开 SQL 保留字 group）"
    )
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间快照（管理面过滤 / 排障）")
    state: Mapped[str] = mapped_column(
        String(24), default=DEFAULT_GROUP_STATE, comment="状态（empty / preparing_rebalance / stable / dead）"
    )
    generation: Mapped[int] = mapped_column(Integer, default=0, comment="世代号（成员集合变化 +1）")


class MsgGroupMember(BaseModel):
    """组成员（`msg_group_member`）：成员租约 + 当前持有分区。"""

    __tablename__ = "msg_group_member"
    __table_args__ = (
        UniqueConstraint("group_id", "member_id", name="uq_msg_group_member_identity"),
        Index("idx_msg_group_member_heartbeat", "group_id", "last_heartbeat"),
    )

    group_id: Mapped[int] = mapped_column(SnowflakeId, comment="所属组（逻辑外键 → msg_consumer_group.id）")
    member_id: Mapped[str] = mapped_column(String(DEFAULT_MEMBER_ID_MAX_LENGTH), comment="成员标识（consumer_id）")
    instance_id: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="最后写入该成员的实例标识（排障用，不参与存活判定）"
    )
    joined_at: Mapped[datetime] = mapped_column(DateTime, comment="加入时刻（UTC；重连不刷新）")
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, comment="最后心跳（UTC；存活判据）")
    partitions: Mapped[ConcurrentStableList[int]] = mapped_column(
        StableJson, default=list, comment="当前持有分区号（升序）"
    )


__all__ = [
    "DEFAULT_GROUP_NAME_MAX_LENGTH",
    "DEFAULT_GROUP_STATE",
    "DEFAULT_MEMBER_ID_MAX_LENGTH",
    "GROUP_STATE_DEAD",
    "GROUP_STATE_EMPTY",
    "GROUP_STATE_PREPARING_REBALANCE",
    "GROUP_STATE_STABLE",
    "MsgConsumerGroup",
    "MsgGroupMember",
]
