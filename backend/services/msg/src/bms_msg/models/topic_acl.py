"""主题 ACL 模型（05-3）：`msg_topic_acl` 规则 + `msg_topic_acl_scope` 作用域版本戳。

口径（详细设计 §4 / §5.4）：

- **规则表**唯一键 `(tenant_id, namespace, topic, principal_type, principal_key, action)`，
  **不带 `deleted_at`**：本表不做软删除（用 `status` 表达），收回置 `disabled`；
- **版本戳表**唯一键 `(tenant_id, namespace)`：任一规则变更在同一事务内 `scope_version + 1`，
  供进程内快照按版本失效；`scope_version` 与基类乐观锁 `version` 分列，避免撞列；
- `tenant_id` 用雪花（`SnowflakeId`）：与基座租户主键同型，仓储租户条件可直接比较；
- `namespace` 对齐 bms `module_key`；ACL 只在同一 `namespace` 内生效，跨租户 / 跨模块不因 ACL 放行；
- `topic` 为原样主题名（允许点号），作为 bms 三级作用域的 `resource_key`。
"""

from bms_core.models.base import BaseModel, SnowflakeId
from sqlalchemy import BigInteger, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

__all__ = [
    "ACL_ACTION_DESCRIBE",
    "ACL_ACTION_MANAGE",
    "ACL_ACTION_PUBLISH",
    "ACL_ACTION_SUBSCRIBE",
    "ACL_EFFECT_ALLOW",
    "ACL_EFFECT_DENY",
    "ACL_PRINCIPAL_ROLE",
    "ACL_PRINCIPAL_USER",
    "ACL_STATUS_DISABLED",
    "ACL_STATUS_ENABLED",
    "MsgTopicAcl",
    "MsgTopicAclScope",
]

ACL_PRINCIPAL_USER = "user"
"""主体类型：用户。"""

ACL_PRINCIPAL_ROLE = "role"
"""主体类型：角色。"""

ACL_ACTION_PUBLISH = "publish"
"""动作：发布。"""

ACL_ACTION_SUBSCRIBE = "subscribe"
"""动作：订阅 / 消费。"""

ACL_ACTION_DESCRIBE = "describe"
"""动作：查看主题元数据。"""

ACL_ACTION_MANAGE = "manage"
"""动作：管理主题（建 / 改 / 重放 / ACL）。"""

ACL_EFFECT_ALLOW = "allow"
"""效果：允许。"""

ACL_EFFECT_DENY = "deny"
"""效果：拒绝（同一匹配集合内优先于 allow）。"""

ACL_STATUS_ENABLED = "enabled"
"""状态：启用。"""

ACL_STATUS_DISABLED = "disabled"
"""状态：停用（收回）。"""


class MsgTopicAcl(BaseModel):
    """主题 ACL 规则（同一主体对同一主题的同一动作只有一行）。"""

    __tablename__ = "msg_topic_acl"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "namespace",
            "topic",
            "principal_type",
            "principal_key",
            "action",
            name="uq_msg_topic_acl_rule",
        ),
        Index("idx_msg_topic_acl_scope", "tenant_id", "namespace", "topic"),
        Index("idx_msg_topic_acl_principal", "tenant_id", "namespace", "principal_type", "principal_key"),
    )

    tenant_id: Mapped[int] = mapped_column(
        SnowflakeId, comment="租户内部身份（雪花主键，不可变；取自基座解析链，不接受客户端自报）"
    )
    namespace: Mapped[str] = mapped_column(
        String(64), comment="命名空间（对齐 bms module_key；ACL 只在同一命名空间内生效）"
    )
    topic: Mapped[str] = mapped_column(String(200), comment="主题名（原样，允许点号；作为 resource_key）")
    principal_type: Mapped[str] = mapped_column(String(16), comment="主体类型（user / role）")
    principal_key: Mapped[str] = mapped_column(
        String(64), comment="主体标识（用户主键 / 角色主键的十进制字符串；仅数字）"
    )
    action: Mapped[str] = mapped_column(String(16), comment="动作（publish / subscribe / describe / manage）")
    effect: Mapped[str] = mapped_column(
        String(8), default=ACL_EFFECT_ALLOW, comment="效果（allow / deny；同一匹配集合内 deny 优先）"
    )
    status: Mapped[str] = mapped_column(
        String(16), default=ACL_STATUS_ENABLED, comment="状态（enabled / disabled；收回置 disabled）"
    )


class MsgTopicAclScope(BaseModel):
    """主题 ACL 作用域版本戳（`(tenant_id, namespace)` 一行）。"""

    __tablename__ = "msg_topic_acl_scope"
    __table_args__ = (UniqueConstraint("tenant_id", "namespace", name="uq_msg_topic_acl_scope"),)

    tenant_id: Mapped[int] = mapped_column(SnowflakeId, comment="租户内部身份（雪花主键，不可变；取自基座解析链）")
    namespace: Mapped[str] = mapped_column(String(64), comment="命名空间（对齐 bms module_key）")
    scope_version: Mapped[int] = mapped_column(
        BigInteger, default=0, comment="作用域版本戳（该命名空间任一 ACL 规则变更 +1；无行视为 0）"
    )
