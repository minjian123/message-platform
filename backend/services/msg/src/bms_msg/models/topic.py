"""主题模型（`msg_topic`）：消息的逻辑类别，承载分区数 / 保留期 / 清理策略与启停状态。

口径（详细设计 §4.1）：

- `tenant_id` 用雪花（`SnowflakeId`）：与基座租户主键同型，仓储租户条件可直接比较；
- 唯一键 `(tenant_id, namespace, name)` **不带 `deleted_at`**：本表不做软删除（用 `status` 表达），
  把 `deleted_at` 并入唯一键会让 MySQL 的多个 NULL 互异而使约束失效；
- `namespace` 对齐 bms `module_key`，取值合法性在服务层判定（`350010`）。
"""

from bms_core.models.base import BaseModel, SnowflakeId
from sqlalchemy import BigInteger, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

CLEANUP_DELETE = "delete"
"""清理策略：按时长删除整段（首期唯一支持值）。"""

CLEANUP_COMPACT = "compact"
"""清理策略：按 key 压缩（后置；首期不接受创建）。"""

STATUS_ACTIVE = "active"
"""主题状态：启用（可收发）。"""

STATUS_DISABLED = "disabled"
"""主题状态：停用（拒绝生产与消费，数据保留）。"""

DEFAULT_CLEANUP_POLICY = CLEANUP_DELETE
"""缺省清理策略。"""

DEFAULT_STATUS = STATUS_ACTIVE
"""缺省主题状态。"""


class MsgTopic(BaseModel):
    """主题（`msg_topic`）：分区数只增不减，减少会破坏 key → partition 映射。"""

    __tablename__ = "msg_topic"
    __table_args__ = (
        UniqueConstraint("tenant_id", "namespace", "name", name="uq_msg_topic_identity"),
        Index("idx_msg_topic_status", "status"),
    )

    tenant_id: Mapped[int] = mapped_column(
        SnowflakeId, comment="租户内部身份（雪花主键，不可变；取自基座解析链，不接受客户端自报）"
    )
    namespace: Mapped[str] = mapped_column(
        String(64), comment="命名空间（对齐 bms module_key；须为已登记且启用的模块）"
    )
    name: Mapped[str] = mapped_column(String(200), comment="主题名（建议 {域}.{对象}.{动作}）")
    partition_count: Mapped[int] = mapped_column(Integer, comment="分区数（只增不减；1~1024）")
    retention_ms: Mapped[int] = mapped_column(BigInteger, comment="保留时长（毫秒）")
    cleanup_policy: Mapped[str] = mapped_column(
        String(16), default=DEFAULT_CLEANUP_POLICY, comment="清理策略（delete / compact；首期只支持 delete）"
    )
    status: Mapped[str] = mapped_column(String(16), default=DEFAULT_STATUS, comment="状态（active / disabled）")
