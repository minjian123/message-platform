"""主题 ACL 常量与端口实现出口（05-3）。

- 动作 / 效果 / 状态 / 主体类型统一从模型模块派生，避免字符串在多处漂移；
- `MSG_TOPIC_ACL` 同时是 `resource_acl` 插件名与 `msg.topic` 资源类型标识；
- 判定纯函数、端口实现、异步守卫与管理服务分文件，保持「判定单点、装载归守卫」。
"""

from bms_msg.models.topic_acl import (
    ACL_ACTION_DESCRIBE,
    ACL_ACTION_MANAGE,
    ACL_ACTION_PUBLISH,
    ACL_ACTION_SUBSCRIBE,
    ACL_EFFECT_ALLOW,
    ACL_EFFECT_DENY,
    ACL_PRINCIPAL_ROLE,
    ACL_PRINCIPAL_USER,
    ACL_STATUS_DISABLED,
    ACL_STATUS_ENABLED,
)

__all__ = [
    "ACL_ACTIONS",
    "ACL_ACTION_DESCRIBE",
    "ACL_ACTION_MANAGE",
    "ACL_ACTION_PUBLISH",
    "ACL_ACTION_SUBSCRIBE",
    "ACL_EFFECTS",
    "ACL_EFFECT_ALLOW",
    "ACL_EFFECT_DENY",
    "ACL_PRINCIPAL_ROLE",
    "ACL_PRINCIPAL_TYPES",
    "ACL_PRINCIPAL_USER",
    "ACL_STATUSES",
    "ACL_STATUS_DISABLED",
    "ACL_STATUS_ENABLED",
    "DEFAULT_ACL_EFFECT",
    "MSG_TOPIC_ACL_PLUGIN",
    "MSG_TOPIC_RESOURCE_TYPE",
    "TOPIC_ACL_CACHE_MAX_ENTRIES",
]

MSG_TOPIC_RESOURCE_TYPE = "msg.topic"
"""bms 三级作用域的资源类型标识（第十类声明）。"""

MSG_TOPIC_ACL_PLUGIN = "msg.topic"
"""`resource_acl` 端口实现名（与资源类型标识同值，便于排障）。"""

ACL_ACTIONS = (ACL_ACTION_PUBLISH, ACL_ACTION_SUBSCRIBE, ACL_ACTION_DESCRIBE, ACL_ACTION_MANAGE)
"""四类 ACL 动作（不隐含：manage 不自动包含其他动作）。"""

ACL_EFFECTS = (ACL_EFFECT_ALLOW, ACL_EFFECT_DENY)
"""效果取值。"""

ACL_STATUSES = (ACL_STATUS_ENABLED, ACL_STATUS_DISABLED)
"""状态取值。"""

ACL_PRINCIPAL_TYPES = (ACL_PRINCIPAL_USER, ACL_PRINCIPAL_ROLE)
"""主体类型取值。"""

DEFAULT_ACL_EFFECT = ACL_EFFECT_ALLOW
"""无匹配规则时的默认效果（显式 deny 优先）。"""

TOPIC_ACL_CACHE_MAX_ENTRIES = 4096
"""进程内 ACL 快照条目上限（超限整体清空，防随机租户打爆内存）。"""
