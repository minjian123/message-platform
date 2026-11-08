"""ACL 规则快照缓存（05-3）：按 `(tenant_id, namespace)` 缓存版本戳 + 启用规则。

- **只缓存规则快照，不缓存判定结果**（bms 07D `02_5` 禁止 ACL 实现跨请求缓存结论）；
- 版本戳由 `msg_topic_acl_scope` 提供：每次判定先读版本，版本变化才重载；
- 条目上限固定，超限整体清空（与访问异常节流表同口径，防随机租户打爆内存）。
"""

from __future__ import annotations

from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList

from bms_msg.acl.keys import TOPIC_ACL_CACHE_MAX_ENTRIES
from bms_msg.models.topic_acl import MsgTopicAcl

__all__ = ["AclRule", "AclSnapshot", "TopicAclCache"]


@dataclass(frozen=True)
class AclRule:
    """一条启用规则的不可变快照（判定只读这些字段）。"""

    topic: str
    """主题名（`resource_key`）。"""

    principal_type: str
    """主体类型（user / role）。"""

    principal_key: str
    """主体标识（用户 / 角色主键的十进制字符串）。"""

    action: str
    """动作（publish / subscribe / describe / manage）。"""

    effect: str
    """效果（allow / deny）。"""

    @classmethod
    def from_row(cls, row: MsgTopicAcl) -> AclRule:
        """由 ORM 行构造快照规则。

        Args:
            row: ACL 规则行（仅启用行会被装载）。

        Returns:
            AclRule: 不可变规则。
        """
        return cls(
            topic=row.topic,
            principal_type=row.principal_type,
            principal_key=row.principal_key,
            action=row.action,
            effect=row.effect,
        )


@dataclass(frozen=True)
class AclSnapshot:
    """一个 `(tenant_id, namespace)` 的启用规则快照。"""

    version: int
    """作用域版本戳（与 `msg_topic_acl_scope.scope_version` 对齐）。"""

    rules: ConcurrentStableFrozenList[AclRule]
    """启用规则（不可变；空表示无规则 = 默认允许）。"""


class TopicAclCache:
    """进程级 ACL 规则快照缓存（版本戳比对；不做 TTL）。"""

    def __init__(self, *, max_entries: int = TOPIC_ACL_CACHE_MAX_ENTRIES) -> None:
        """初始化。

        Args:
            max_entries: 条目上限（超限整体清空）。
        """
        self._max_entries = max_entries
        self._entries: ConcurrentStableDict[str, AclSnapshot] = ConcurrentStableDict()

    @staticmethod
    def _key(tenant_id: int, namespace: str) -> str:
        """构造缓存键（命名空间不含 `:`，用 `:` 分隔租户与命名空间）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            str: 缓存键。
        """
        return f"{int(tenant_id)}:{namespace}"

    def get(self, tenant_id: int, namespace: str) -> AclSnapshot | None:
        """取快照（未命中返回 None）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            AclSnapshot | None: 快照；未命中 None。
        """
        return self._entries.get(self._key(tenant_id, namespace))

    def put(self, tenant_id: int, namespace: str, snapshot: AclSnapshot) -> AclSnapshot:
        """写入快照（超上限时整体清空后写入）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            snapshot: 快照。

        Returns:
            AclSnapshot: 写入的快照。
        """
        key = self._key(tenant_id, namespace)
        if len(self._entries) >= self._max_entries and key not in self._entries:
            self._entries.clear()
        self._entries.set(key, snapshot)
        return snapshot

    def invalidate(self, tenant_id: int, namespace: str) -> None:
        """失效一个作用域快照（管理面提交后调用）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
        """
        self._entries.pop(self._key(tenant_id, namespace), None)

    def invalidate_tenant(self, tenant_id: int) -> None:
        """失效某租户全部作用域快照（租户到期清理）。

        Args:
            tenant_id: 租户主键。
        """
        target = f"{int(tenant_id)}:"
        for key in tuple(self._entries):
            if key.startswith(target):
                self._entries.pop(key, None)

    def clear(self) -> None:
        """清空全部快照（用例隔离 / 运维）。"""
        self._entries.clear()
