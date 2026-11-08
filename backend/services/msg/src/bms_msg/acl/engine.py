"""ACL 纯判定函数（05-3）：唯一判定实现，端口与过滤共用。

规则（详细设计 §5.3）：

1. 只匹配同 `tenant_id / namespace / topic / action` 的启用规则（租户与命名空间由调用方
   已带入 `AclContext`，跨租户 / 跨模块不会出现在同一快照匹配集里）；
2. 主体匹配：`user` 比 `ctx.user_id`，`role` 比 `ctx.roles`（角色主键十进制字符串）；
3. 同一匹配集合内 **deny 优先**；有 allow 放行；无匹配默认允许；
4. `manage` 不隐含其他动作（动作精确匹配）。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableSet
from bms_core.resource_scope.acl import AclContext

from bms_msg.acl.cache import AclSnapshot
from bms_msg.acl.keys import (
    ACL_EFFECT_DENY,
    ACL_PRINCIPAL_ROLE,
    ACL_PRINCIPAL_USER,
    MSG_TOPIC_RESOURCE_TYPE,
)

__all__ = ["decide"]


def decide(snapshot: AclSnapshot, ctx: AclContext, *, action: str) -> bool:
    """判定主体能否对该主题执行动作（允许 True / 拒绝 False）。

    Args:
        snapshot: 当前作用域的启用规则快照。
        ctx: 判定入参（租户 / 命名空间 / 主题 / 操作人 / 角色）。
        action: 四类 ACL 动作之一。

    Returns:
        bool: 允许 True；拒绝 False。
    """
    if ctx.resource_key is None or ctx.resource_type not in (None, MSG_TOPIC_RESOURCE_TYPE):
        return False
    user_key = None if ctx.user_id is None else str(ctx.user_id)
    role_keys: ConcurrentStableSet[str] = ConcurrentStableSet(ctx.roles)
    for rule in snapshot.rules:
        if rule.topic != ctx.resource_key or rule.action != action:
            continue
        if rule.principal_type == ACL_PRINCIPAL_USER:
            if user_key is None or rule.principal_key != user_key:
                continue
        elif rule.principal_type == ACL_PRINCIPAL_ROLE:
            if rule.principal_key not in role_keys:
                continue
        else:
            continue
        if rule.effect == ACL_EFFECT_DENY:
            return False
    return True
