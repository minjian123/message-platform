"""msg 服务模型包：ORM 模型导出与**模型模块清单**（迁移链 / 开发库自动建表的发现入口）。

`MODEL_MODULES` 是服务包自声明的模型模块清单（`bms_core.db.migration::service_model_modules` 按
`{package_prefix}_{service}.models` 解析本包后读取）；新增模型文件必须在此登记，否则迁移链与
开发库自动建表取不到该表。

> 包名前缀注意：本包名是 `bms_msg`（`[app].package_prefix = "bms"` + `[app].service = "msg"`），
> 不是 `msg`——配 `package_prefix = "msg"` 会解析出 `msg.models` 而取到空清单（2026-11-04 修正）。
"""

from bms_msg.models.dead_letter import (
    DEFAULT_DISPOSITION_REASON_MAX_LENGTH,
    DEFAULT_DLQ_STATUS,
    DLQ_ACTIVE_STATUSES,
    DLQ_ARCHIVABLE_STATUSES,
    DLQ_STATUS_ARCHIVED,
    DLQ_STATUS_IGNORED,
    DLQ_STATUS_PENDING,
    DLQ_STATUS_REPLAYING,
    DLQ_STATUS_RESOLVED,
    DLQ_STATUSES,
    MsgDeadLetter,
)
from bms_msg.models.partition import MsgPartition
from bms_msg.models.quota import MsgQuota
from bms_msg.models.replay import (
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PAUSED,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_TERMINAL_STATUSES,
    MsgReplayTask,
)
from bms_msg.models.tenant_lifecycle import (
    ACTION_ACTIVATED,
    ACTION_CREATED,
    ACTION_DELETED,
    ACTION_SUSPENDED,
    LIFECYCLE_ACTIONS,
    MsgTenantLifecycle,
)
from bms_msg.models.topic import (
    CLEANUP_COMPACT,
    CLEANUP_DELETE,
    DEFAULT_CLEANUP_POLICY,
    DEFAULT_STATUS,
    STATUS_ACTIVE,
    STATUS_DELETING,
    STATUS_DISABLED,
    MsgTopic,
)
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
    MsgTopicAcl,
    MsgTopicAclScope,
)
from bms_msg.models.topic_cleanup import (
    CLEANUP_COMPLETED,
    CLEANUP_PENDING,
    CLEANUP_RUNNING,
    CLEANUP_STATUSES,
    CLEANUP_STEPS,
    STEP_DELAY,
    STEP_DLQ,
    STEP_GROUPS,
    STEP_MESSAGES,
    STEP_METADATA,
    STEP_OFFSETS,
    STEP_RUNTIME,
    MsgTopicCleanup,
)

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
    "ACTION_ACTIVATED",
    "ACTION_CREATED",
    "ACTION_DELETED",
    "ACTION_SUSPENDED",
    "CLEANUP_COMPACT",
    "CLEANUP_COMPLETED",
    "CLEANUP_DELETE",
    "CLEANUP_PENDING",
    "CLEANUP_RUNNING",
    "CLEANUP_STATUSES",
    "CLEANUP_STEPS",
    "DEFAULT_CLEANUP_POLICY",
    "DEFAULT_DISPOSITION_REASON_MAX_LENGTH",
    "DEFAULT_DLQ_STATUS",
    "DEFAULT_STATUS",
    "DLQ_ACTIVE_STATUSES",
    "DLQ_ARCHIVABLE_STATUSES",
    "DLQ_STATUSES",
    "DLQ_STATUS_ARCHIVED",
    "DLQ_STATUS_IGNORED",
    "DLQ_STATUS_PENDING",
    "DLQ_STATUS_REPLAYING",
    "DLQ_STATUS_RESOLVED",
    "LIFECYCLE_ACTIONS",
    "MODEL_MODULES",
    "STATUS_ACTIVE",
    "STATUS_DELETING",
    "STATUS_DISABLED",
    "STEP_DELAY",
    "STEP_DLQ",
    "STEP_GROUPS",
    "STEP_MESSAGES",
    "STEP_METADATA",
    "STEP_OFFSETS",
    "STEP_RUNTIME",
    "TASK_CANCELLED",
    "TASK_COMPLETED",
    "TASK_FAILED",
    "TASK_PAUSED",
    "TASK_PENDING",
    "TASK_RUNNING",
    "TASK_TERMINAL_STATUSES",
    "MsgDeadLetter",
    "MsgPartition",
    "MsgQuota",
    "MsgReplayTask",
    "MsgTenantLifecycle",
    "MsgTopic",
    "MsgTopicAcl",
    "MsgTopicAclScope",
    "MsgTopicCleanup",
]

MODEL_MODULES: tuple[str, ...] = (
    "bms_msg.models.topic",
    "bms_msg.models.partition",
    "bms_msg.models.quota",
    "bms_msg.models.replay",
    "bms_msg.models.tenant_lifecycle",
    "bms_msg.models.topic_acl",
    "bms_msg.models.topic_cleanup",
    "bms_msg.models.dead_letter",
)
"""本服务私有模型模块清单（导入即把表登记进 `Base.metadata`）。"""
