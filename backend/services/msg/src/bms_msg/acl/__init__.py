"""主题 ACL 包（05-3）：常量 / 快照缓存 / 纯判定 / 端口实现 / 守卫与管理服务。"""

from bms_msg.acl.cache import AclRule, AclSnapshot, TopicAclCache
from bms_msg.acl.engine import decide
from bms_msg.acl.keys import (
    ACL_ACTION_DESCRIBE,
    ACL_ACTION_MANAGE,
    ACL_ACTION_PUBLISH,
    ACL_ACTION_SUBSCRIBE,
    ACL_ACTIONS,
    ACL_EFFECT_ALLOW,
    ACL_EFFECT_DENY,
    ACL_EFFECTS,
    ACL_PRINCIPAL_ROLE,
    ACL_PRINCIPAL_TYPES,
    ACL_PRINCIPAL_USER,
    ACL_STATUS_DISABLED,
    ACL_STATUS_ENABLED,
    ACL_STATUSES,
    DEFAULT_ACL_EFFECT,
    MSG_TOPIC_ACL_PLUGIN,
    MSG_TOPIC_RESOURCE_TYPE,
    TOPIC_ACL_CACHE_MAX_ENTRIES,
)
from bms_msg.acl.port import MsgTopicAcl, MsgTopicAclFactory
from bms_msg.acl.service import (
    AclChange,
    AclRuleSpec,
    AclRuleView,
    TopicAclAdminService,
    TopicAclService,
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
    "AclChange",
    "AclRule",
    "AclRuleSpec",
    "AclRuleView",
    "AclSnapshot",
    "MsgTopicAcl",
    "MsgTopicAclFactory",
    "TopicAclAdminService",
    "TopicAclCache",
    "TopicAclService",
    "decide",
]
