"""租户生命周期动作账本（`msg_tenant_lifecycle`）：五动作版本戳与物理清理进度。

口径（05-5 详细设计 §5）：

- **一条 `(tenant_id, action)` 一行**：`action` 为 `created` / `suspended` / `activated` /
  `deleted` / `purged`；业务级幂等键是 `(tenant_id, action, version)`。
- **不是租户状态表**：数据面不读本表做状态判定；租户状态仍以基座租户源为准。
- **两步删除**：`deleted` 只停写（写 `deleted_event_at`，**不自算清理时点**）；`purged` 由平台
  授权后才物理删除并写 `purged_at`。
- **删除进度挂在 `deleted` 行**：`deleted_event_at` / `purge_after` / `purged_at` / `failure_count` /
  `last_error`；`purge_after` 仅作 05-5 旧数据行的存量收敛位，新行恒 NULL。
- **永久保留**：本表不做软删除（`deleted_at` 恒 NULL），不随租户资源删除；它是幂等证据与
  清理进度，不是业务数据。
"""

from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.models.base import BaseModel, SnowflakeId
from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

ACTION_CREATED = "created"
"""动作：租户开通（05-1 目录初始化回归）。"""

ACTION_SUSPENDED = "suspended"
"""动作：租户停用。"""

ACTION_ACTIVATED = "activated"
"""动作：租户启用。"""

ACTION_DELETED = "deleted"
"""动作：租户注销（只停写 + 清易失资源；数据保留至平台授权清理）。"""

ACTION_PURGED = "purged"
"""动作：平台已物理清理（授权产品删数据）。"""

LIFECYCLE_ACTIONS: ConcurrentStableList[str] = ConcurrentStableList(
    (
        ACTION_CREATED,
        ACTION_SUSPENDED,
        ACTION_ACTIVATED,
        ACTION_DELETED,
        ACTION_PURGED,
    )
)
"""合法动作集合（与 bms 五类事件一一对应）。"""


class MsgTenantLifecycle(BaseModel):
    """租户生命周期动作账本（平台库；按 `tenant_id` 列区分）。"""

    __tablename__ = "msg_tenant_lifecycle"
    __table_args__ = (
        UniqueConstraint("tenant_id", "action", name="uq_msg_tenant_lifecycle_tenant_action"),
        Index("idx_msg_tenant_lifecycle_purge", "purge_after", "purged_at"),
    )

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变；取自事件载荷）")
    action: Mapped[str] = mapped_column(
        String(16), comment="生命周期动作（created / suspended / activated / deleted / purged）"
    )
    applied_version: Mapped[int] = mapped_column(
        Integer, default=0, comment="已成功处理的事件版本戳（sys_tenant.version；旧事件按 0；未处理为 -1）"
    )
    deleted_event_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="删除事件生效时刻（UTC；仅 deleted 行）"
    )
    purge_after: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="存量兼容位：两步删除下不再自算，仅 05-5 旧数据行有值（NULL = 无）"
    )
    purged_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="收到 tenant.purged 后物理清理完成时刻（UTC；NULL = 未完成；幂等判据）"
    )
    failure_count: Mapped[int] = mapped_column(Integer, default=0, comment="连续清理失败次数（成功清零）")
    last_error: Mapped[str | None] = mapped_column(
        String(512), nullable=True, comment="最近一次清理失败原因（截断 512）"
    )
    last_event_id: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="最近处理的事件 ID（排障用）")
