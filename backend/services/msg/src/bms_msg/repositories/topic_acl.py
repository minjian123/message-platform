"""主题 ACL 仓储（05-3）：规则 CRUD、作用域版本戳与租户清理。

- 只 `flush` 不 `commit`：事务边界归 `TopicAclService` / 租户清理调用方；
- 读写都显式带 `tenant_id`：请求链与后台清理链行为一致，不依赖 `ContextVar`；
- `bump_scope_version` 先 `SELECT ... FOR UPDATE`；无行则插入；并发首次插入冲突由
  服务层整体重试一次（规则写幂等，重试安全）。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.repositories.base_db_repository import BaseDbRepository
from sqlalchemy import ColumnElement, delete, func, select

from bms_msg.models.topic_acl import (
    ACL_STATUS_ENABLED,
    MsgTopicAcl,
    MsgTopicAclScope,
)

__all__ = ["MsgTopicAclRepository", "MsgTopicAclScopeRepository"]


class MsgTopicAclRepository(BaseDbRepository[MsgTopicAcl]):
    """ACL 规则仓储：按唯一键判存、按作用域装载、租户到期清理。"""

    model = MsgTopicAcl
    tenant_scoped = True
    soft_delete_enabled = False

    async def get_by_key(
        self,
        *,
        tenant_id: int,
        namespace: str,
        topic: str,
        principal_type: str,
        principal_key: str,
        action: str,
    ) -> MsgTopicAcl | None:
        """按唯一键取规则（显式租户条件）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            topic: 主题名。
            principal_type: 主体类型。
            principal_key: 主体标识。
            action: 动作。

        Returns:
            MsgTopicAcl | None: 规则；不存在返回 None。
        """
        statement = select(MsgTopicAcl).where(
            MsgTopicAcl.tenant_id == int(tenant_id),
            MsgTopicAcl.namespace == namespace,
            MsgTopicAcl.topic == topic,
            MsgTopicAcl.principal_type == principal_type,
            MsgTopicAcl.principal_key == principal_key,
            MsgTopicAcl.action == action,
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def get_by_id(self, *, tenant_id: int, acl_id: int) -> MsgTopicAcl | None:
        """按主键取规则（显式租户条件，跨租户不可见）。

        Args:
            tenant_id: 租户主键。
            acl_id: 规则主键。

        Returns:
            MsgTopicAcl | None: 规则；不存在 / 不属于当前租户返回 None。
        """
        statement = select(MsgTopicAcl).where(
            MsgTopicAcl.tenant_id == int(tenant_id),
            MsgTopicAcl.id == int(acl_id),
        )
        return (await self._session.execute(statement)).scalar_one_or_none()

    async def list_enabled(self, *, tenant_id: int, namespace: str) -> ConcurrentStableList[MsgTopicAcl]:
        """取某作用域全部启用规则（ACL 快照装载的唯一查询）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            ConcurrentStableList[MsgTopicAcl]: 启用规则（按主题 / 动作 / 主体排序，便于稳定快照）。
        """
        statement = (
            select(MsgTopicAcl)
            .where(
                MsgTopicAcl.tenant_id == int(tenant_id),
                MsgTopicAcl.namespace == namespace,
                MsgTopicAcl.status == ACL_STATUS_ENABLED,
            )
            .order_by(
                MsgTopicAcl.topic.asc(),
                MsgTopicAcl.action.asc(),
                MsgTopicAcl.principal_type.asc(),
                MsgTopicAcl.principal_key.asc(),
            )
        )
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def list_rules(
        self,
        *,
        tenant_id: int,
        namespace: str | None,
        topic: str | None,
        principal_type: str | None,
        principal_key: str | None,
        action: str | None,
        status: str | None,
        offset: int,
        limit: int,
    ) -> ConcurrentStableList[MsgTopicAcl]:
        """分页列出规则（管理面）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间过滤；None 不限制。
            topic: 主题过滤；None 不限制。
            principal_type: 主体类型过滤；None 不限制。
            principal_key: 主体标识过滤；None 不限制。
            action: 动作过滤；None 不限制。
            status: 状态过滤；None 不限制。
            offset: 跳过条数。
            limit: 本页条数。

        Returns:
            ConcurrentStableList[MsgTopicAcl]: 当页规则。
        """
        statement = (
            select(MsgTopicAcl)
            .where(*self._filters(tenant_id, namespace, topic, principal_type, principal_key, action, status))
            .order_by(
                MsgTopicAcl.namespace.asc(),
                MsgTopicAcl.topic.asc(),
                MsgTopicAcl.principal_type.asc(),
                MsgTopicAcl.principal_key.asc(),
                MsgTopicAcl.action.asc(),
            )
            .limit(limit)
            .offset(offset)
        )
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def count_rules(
        self,
        *,
        tenant_id: int,
        namespace: str | None,
        topic: str | None,
        principal_type: str | None,
        principal_key: str | None,
        action: str | None,
        status: str | None,
    ) -> int:
        """统计规则总数（与 `list_rules` 同条件）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间过滤；None 不限制。
            topic: 主题过滤；None 不限制。
            principal_type: 主体类型过滤；None 不限制。
            principal_key: 主体标识过滤；None 不限制。
            action: 动作过滤；None 不限制。
            status: 状态过滤；None 不限制。

        Returns:
            int: 总数。
        """
        statement = (
            select(func.count())
            .select_from(MsgTopicAcl)
            .where(*self._filters(tenant_id, namespace, topic, principal_type, principal_key, action, status))
        )
        return int((await self._session.execute(statement)).scalar_one())

    async def delete_for_tenant(self, *, tenant_id: int) -> int:
        """物理删除某租户全部规则（租户到期清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(delete(MsgTopicAcl).where(MsgTopicAcl.tenant_id == int(tenant_id)))
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)

    async def flush(self) -> None:
        """刷新挂起变更（仓储统一只 flush，不 commit）。"""
        await self._session.flush()

    def _filters(
        self,
        tenant_id: int,
        namespace: str | None,
        topic: str | None,
        principal_type: str | None,
        principal_key: str | None,
        action: str | None,
        status: str | None,
    ) -> ConcurrentStableList[ColumnElement[bool]]:
        """构造管理面过滤条件（全部显式租户）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间过滤；None 不限制。
            topic: 主题过滤；None 不限制。
            principal_type: 主体类型过滤；None 不限制。
            principal_key: 主体标识过滤；None 不限制。
            action: 动作过滤；None 不限制。
            status: 状态过滤；None 不限制。

        Returns:
            ConcurrentStableList[ColumnElement[bool]]: 条件列表。
        """
        conditions: ConcurrentStableList[ColumnElement[bool]] = ConcurrentStableList()
        conditions.add(MsgTopicAcl.tenant_id == int(tenant_id))
        if namespace is not None:
            conditions.add(MsgTopicAcl.namespace == namespace)
        if topic is not None:
            conditions.add(MsgTopicAcl.topic == topic)
        if principal_type is not None:
            conditions.add(MsgTopicAcl.principal_type == principal_type)
        if principal_key is not None:
            conditions.add(MsgTopicAcl.principal_key == principal_key)
        if action is not None:
            conditions.add(MsgTopicAcl.action == action)
        if status is not None:
            conditions.add(MsgTopicAcl.status == status)
        return conditions


class MsgTopicAclScopeRepository(BaseDbRepository[MsgTopicAclScope]):
    """ACL 作用域版本戳仓储：取 / 自增 / 租户清理。"""

    model = MsgTopicAclScope
    tenant_scoped = True
    soft_delete_enabled = False

    async def get_scope_version(self, *, tenant_id: int, namespace: str) -> int:
        """取作用域版本戳（无行返回 0，不写库）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            int: 版本戳；无行 0。
        """
        statement = select(MsgTopicAclScope.scope_version).where(
            MsgTopicAclScope.tenant_id == int(tenant_id),
            MsgTopicAclScope.namespace == namespace,
        )
        value = (await self._session.execute(statement)).scalar_one_or_none()
        return int(value) if value is not None else 0

    async def bump_scope_version(self, *, tenant_id: int, namespace: str) -> int:
        """作用域版本 +1（无行则插入 1；`SELECT ... FOR UPDATE` 串行化同作用域写）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            int: 自增后的版本戳。
        """
        statement = (
            select(MsgTopicAclScope)
            .where(
                MsgTopicAclScope.tenant_id == int(tenant_id),
                MsgTopicAclScope.namespace == namespace,
            )
            .with_for_update()
        )
        row = (await self._session.execute(statement)).scalar_one_or_none()
        if row is None:
            created = await self.create(
                tenant_id=int(tenant_id),
                namespace=namespace,
                scope_version=1,
            )
            return int(created.scope_version)
        row.scope_version = int(row.scope_version) + 1
        await self._session.flush()
        return int(row.scope_version)

    async def delete_for_tenant(self, *, tenant_id: int) -> int:
        """物理删除某租户全部作用域版本行（租户到期清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除行数。
        """
        result = await self._session.execute(
            delete(MsgTopicAclScope).where(MsgTopicAclScope.tenant_id == int(tenant_id))
        )
        await self._session.flush()
        return int(getattr(result, "rowcount", 0) or 0)
