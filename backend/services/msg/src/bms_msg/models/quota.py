"""消息维度配额模型（`msg_quota`）：只存限额，用量实时计算。

口径（05-2 详细设计 §4）：

- **唯一键** `(tenant_id, namespace, quota_key)`：`namespace` 空串表示租户级，命名空间级行只作覆盖；
- **不存用量**：用量由 `usage_snapshot()` 实时取数，避免双写与口径漂移；
- **不做软删除**：配额行随租户到期物理清理，`deleted_at` 恒 NULL；
- **单位 / 周期落库派生值**：由 `quota_key` 决定，客户端不可自报（管理面只接收限额）。
"""

from bms_core.models.base import BaseModel, SnowflakeId
from sqlalchemy import BigInteger, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from bms_msg.core.config import (
    QUOTA_ORIGIN_DEFAULT,
    QUOTA_PERIOD_NONE,
    QUOTA_UNIT_COUNT,
)

__all__ = ["MsgQuota"]


class MsgQuota(BaseModel):
    """配额限额（六维之一；租户级或命名空间级覆盖）。"""

    __tablename__ = "msg_quota"
    __table_args__ = (UniqueConstraint("tenant_id", "namespace", "quota_key", name="uq_msg_quota_scope"),)

    tenant_id: Mapped[int] = mapped_column(
        SnowflakeId, comment="租户内部身份（雪花主键，不可变；取自基座解析链，不接受客户端自报）"
    )
    namespace: Mapped[str] = mapped_column(
        String(64),
        default="",
        comment="命名空间（对齐 bms module_key）；空串 = 租户级",
    )
    quota_key: Mapped[str] = mapped_column(
        String(32),
        comment=(
            "配额维度键（topic_count / group_count / produce_rate / storage_bytes / unacked_count / connection_count）"
        ),
    )
    limit_value: Mapped[int] = mapped_column(BigInteger, default=0, comment="硬限（0 = 不限）")
    soft_limit: Mapped[int] = mapped_column(
        BigInteger,
        default=0,
        comment="软限（0 = 未显式设置，读时按硬限 × 80% 派生）",
    )
    unit: Mapped[str] = mapped_column(String(16), default=QUOTA_UNIT_COUNT, comment="单位（count / bytes / qps）")
    period: Mapped[str] = mapped_column(String(16), default=QUOTA_PERIOD_NONE, comment="周期（none；本期六维均为瞬时）")
    origin: Mapped[str] = mapped_column(
        String(16), default=QUOTA_ORIGIN_DEFAULT, comment="来源（default 缺省行 / configured 管理面调整）"
    )
