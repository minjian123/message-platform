"""主题 ACL 服务（05-3）：异步守卫 + 管理面写入。

- **守卫**（`TopicAclService`）：按版本戳装载快照 → 构造 `AclContext` → 调基座
  `resource_acl` 端口 `check`；判定逻辑不在本类；
- **管理服务**（`TopicAclAdminService`）：批量授予 / 更新 / 收回、主题存在性校验、
  版本戳自增、提交后缓存失效与强审计；
- **失败口径**：ACL 读取失败 → `ServiceUnavailableError` 10007 / 503（fail-closed）；
  端口拒绝 → `ResourceAccessDeniedError` 82013 → 产品码 350027 / 403。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.context import get_current_user_id
from bms_core.core.exceptions import ParamError, ResourceAccessDeniedError, ServiceUnavailableError
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.resource_scope.acl import AclContext, BaseResourceAcl, current_roles
from sqlalchemy.exc import IntegrityError

from bms_msg.acl.cache import AclRule, AclSnapshot, TopicAclCache
from bms_msg.acl.keys import (
    ACL_ACTION_MANAGE,
    ACL_ACTIONS,
    ACL_EFFECT_DENY,
    ACL_EFFECTS,
    ACL_PRINCIPAL_TYPES,
    MSG_TOPIC_RESOURCE_TYPE,
)
from bms_msg.audit.trail import (
    ACL_GRANT_ACTION,
    ACL_REVOKE_ACTION,
    ACL_UPDATE_ACTION,
    AuditEmitter,
    emit_acl_change,
)
from bms_msg.errors import MsgTopicNotFoundError
from bms_msg.models.topic_acl import (
    ACL_STATUS_DISABLED,
    ACL_STATUS_ENABLED,
    MsgTopicAcl,
)
from bms_msg.repositories.topic import TopicRepository
from bms_msg.repositories.topic_acl import MsgTopicAclRepository, MsgTopicAclScopeRepository
from bms_msg.tenant.context import CurrentTenant

__all__ = [
    "AclChange",
    "AclRuleSpec",
    "AclRuleView",
    "TopicAclAdminService",
    "TopicAclService",
]


@dataclass(frozen=True)
class AclRuleSpec:
    """一条待写入的 ACL 规则（API 层校验后传入）。"""

    namespace: str
    topic: str
    principal_type: str
    principal_key: str
    action: str
    effect: str


@dataclass(frozen=True)
class AclRuleView:
    """规则响应 / 审计快照（提交后不依赖 ORM 会话）。"""

    acl_id: int
    namespace: str
    topic: str
    principal_type: str
    principal_key: str
    action: str
    effect: str
    status: str
    updated_at: datetime | None

    @classmethod
    def from_row(cls, row: MsgTopicAcl) -> AclRuleView:
        """由 ORM 行构造视图。

        Args:
            row: ACL 规则行。

        Returns:
            AclRuleView: 不可变视图。
        """
        return cls(
            acl_id=int(row.id),
            namespace=row.namespace,
            topic=row.topic,
            principal_type=row.principal_type,
            principal_key=row.principal_key,
            action=row.action,
            effect=row.effect,
            status=row.status,
            updated_at=row.updated_at,
        )


@dataclass(frozen=True)
class AclChange:
    """一次写入结果（`audit_action=None` 表示幂等未变更）。"""

    view: AclRuleView
    audit_action: str | None
    effect: str
    changed: bool


class TopicAclService:
    """ACL 异步守卫：装载快照 + 调端口（判定单点在 `MsgTopicAcl`）。"""

    def __init__(
        self,
        *,
        rules: MsgTopicAclRepository,
        scopes: MsgTopicAclScopeRepository,
        cache: TopicAclCache,
        acl_provider: Callable[[], BaseResourceAcl | None],
        user_id_provider: Callable[[], int | None] = get_current_user_id,
        roles_provider: Callable[[], ConcurrentStableFrozenList[str]] = current_roles,
        release: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """初始化。

        Args:
            rules: 规则仓储（请求级会话）。
            scopes: 版本戳仓储（同会话）。
            cache: 进程级快照缓存。
            acl_provider: `resource_acl` 端口读取器（未声明返回 None）。
            user_id_provider: 操作人读取器。
            roles_provider: 角色主键读取器。
            release: 读取后释放会话事务（避免后续 `uow.begin()` 撞「事务已开启」）；
                None 表示不释放（单元测试直连）。
        """
        self._rules = rules
        self._scopes = scopes
        self._cache = cache
        self._acl_provider = acl_provider
        self._user_id_provider = user_id_provider
        self._roles_provider = roles_provider
        self._release = release

    async def authorize(self, *, tenant_id: int, namespace: str, topic: str, action: str) -> None:
        """判定并拒绝（允许即返回）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            topic: 主题名。
            action: 四类 ACL 动作之一。

        Raises:
            ServiceUnavailableError: ACL 规则读取失败（10007 / 503）。
            ResourceAccessDeniedError: 端口拒绝（82013 / 403）。
        """
        acl = self._acl_provider()
        if acl is None:
            return
        try:
            await self.snapshot(tenant_id=tenant_id, namespace=namespace)
            acl.check(self._context(tenant_id, namespace, topic), action=action)
        finally:
            await self._release_session()

    async def filter_allowed(
        self,
        *,
        tenant_id: int,
        namespace: str,
        topics: ConcurrentStableList[str],
        action: str,
    ) -> ConcurrentStableList[str]:
        """过滤出允许的主题（列表接口用；同一作用域只装载一次快照）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            topics: 候选主题。
            action: 四类 ACL 动作之一。

        Returns:
            ConcurrentStableList[str]: 允许的主题（保持入参顺序）。

        Raises:
            ServiceUnavailableError: ACL 规则读取失败（10007 / 503）。
        """
        acl = self._acl_provider()
        if acl is None:
            return ConcurrentStableList(topics)
        allowed: ConcurrentStableList[str] = ConcurrentStableList()
        try:
            await self.snapshot(tenant_id=tenant_id, namespace=namespace)
            for topic in topics:
                try:
                    acl.check(self._context(tenant_id, namespace, topic), action=action)
                except ResourceAccessDeniedError:
                    continue
                allowed.add(topic)
        finally:
            await self._release_session()
        return allowed

    async def snapshot(self, *, tenant_id: int, namespace: str) -> AclSnapshot:
        """取作用域快照（版本一致命中缓存，否则重载）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。

        Returns:
            AclSnapshot: 启用规则快照（空快照 = 默认允许）。

        Raises:
            ServiceUnavailableError: 版本 / 规则读取失败（10007 / 503）。
        """
        try:
            version = await self._scopes.get_scope_version(tenant_id=tenant_id, namespace=namespace)
            cached = self._cache.get(tenant_id, namespace)
            if cached is not None and cached.version == version:
                return cached
            rows = await self._rules.list_enabled(tenant_id=tenant_id, namespace=namespace)
            snapshot = AclSnapshot(version=version, rules=_frozen_rules(rows))
            return self._cache.put(tenant_id, namespace, snapshot)
        except Exception as exc:
            raise ServiceUnavailableError("主题 ACL 规则不可用") from exc

    async def _release_session(self) -> None:
        """释放读取产生的会话事务（未注入 release 时为空操作）。"""
        if self._release is not None:
            await self._release()

    def _context(self, tenant_id: int, namespace: str, topic: str) -> AclContext:
        """构造端口判定入参。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            topic: 主题名。

        Returns:
            AclContext: 判定入参。
        """
        return AclContext(
            tenant_id=int(tenant_id),
            module_key=namespace,
            resource_type=MSG_TOPIC_RESOURCE_TYPE,
            resource_key=topic,
            user_id=self._user_id_provider(),
            roles=ConcurrentStableFrozenList(self._roles_provider()),
        )


class TopicAclAdminService:
    """ACL 管理面写入服务（批量授予 / 更新 / 收回）。"""

    def __init__(
        self,
        *,
        rules: MsgTopicAclRepository,
        scopes: MsgTopicAclScopeRepository,
        topics: TopicRepository,
        uow: UnitOfWork,
        cache: TopicAclCache,
        authorizer: TopicAclService,
        sink: AuditEmitter,
    ) -> None:
        """初始化。

        Args:
            rules: 规则仓储。
            scopes: 版本戳仓储。
            topics: 主题仓储（授予前校验存在与归属）。
            uow: 工作单元（事务边界）。
            cache: 进程级快照缓存（提交后失效）。
            authorizer: ACL 守卫（写前判目标主题 `manage`）。
            sink: 审计出口。
        """
        self._rules = rules
        self._scopes = scopes
        self._topics = topics
        self._uow = uow
        self._cache = cache
        self._authorizer = authorizer
        self._sink = sink

    async def list_rules(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str | None,
        topic: str | None,
        principal_type: str | None,
        principal_key: str | None,
        action: str | None,
        status: str | None,
        page: int,
        size: int,
    ) -> tuple[ConcurrentStableList[MsgTopicAcl], int]:
        """分页列出 ACL 规则。

        Args:
            tenant: 当前租户。
            namespace: 命名空间过滤。
            topic: 主题过滤。
            principal_type: 主体类型过滤。
            principal_key: 主体标识过滤。
            action: 动作过滤。
            status: 状态过滤。
            page: 页码（从 1 起）。
            size: 每页条数。

        Returns:
            tuple[ConcurrentStableList[MsgTopicAcl], int]: 当页规则 + 总数。
        """
        if namespace is not None:
            tenant.ensure_namespace(namespace)
        rows = await self._rules.list_rules(
            tenant_id=int(tenant.tenant_id),
            namespace=namespace,
            topic=topic,
            principal_type=principal_type,
            principal_key=principal_key,
            action=action,
            status=status,
            offset=(page - 1) * size,
            limit=size,
        )
        total = await self._rules.count_rules(
            tenant_id=int(tenant.tenant_id),
            namespace=namespace,
            topic=topic,
            principal_type=principal_type,
            principal_key=principal_key,
            action=action,
            status=status,
        )
        return rows, total

    async def upsert(
        self, *, tenant: CurrentTenant, specs: ConcurrentStableList[AclRuleSpec]
    ) -> ConcurrentStableList[AclChange]:
        """批量授予 / 更新（整批校验、整批写入）。

        Args:
            tenant: 当前租户。
            specs: 规则规格（已由 API 转成不可变对象）。

        Returns:
            ConcurrentStableList[AclChange]: 每条规格的结果（保持入参顺序）。

        Raises:
            ServiceUnavailableError: ACL 守卫读取失败（10007 / 503）。
            ResourceAccessDeniedError: 目标主题 `manage` 被拒（82013 / 403）。
            MsgNamespaceInvalidError: 命名空间非法（350010）。
            MsgTopicNotFoundError: 主题不存在 / 不属于当前租户（350001）。
            ParamError: 参数形态非法（10001）。
        """
        _ensure_no_duplicates(specs)
        for spec in specs:
            _validate_spec(spec)
            await self._authorizer.authorize(
                tenant_id=int(tenant.tenant_id),
                namespace=spec.namespace,
                topic=spec.topic,
                action=ACL_ACTION_MANAGE,
            )
            tenant.ensure_namespace(spec.namespace)

        views: ConcurrentStableList[AclChange] = ConcurrentStableList()
        await self._uow.rollback()
        try:
            async with self._uow.begin():
                await self._ensure_topics(specs=specs)
                views = await self._apply(tenant_id=int(tenant.tenant_id), specs=specs)
        except IntegrityError:
            # 版本戳首次建行并发冲突：规则写幂等，整批重试一次。
            await self._uow.rollback()
            async with self._uow.begin():
                await self._ensure_topics(specs=specs)
                views = await self._apply(tenant_id=int(tenant.tenant_id), specs=specs)
        await self._after_commit(tenant_id=int(tenant.tenant_id), changes=views, method="PUT")
        return views

    async def revoke(self, *, tenant: CurrentTenant, acl_id: int) -> AclChange | None:
        """收回一条规则（置 `status=disabled`，幂等）。

        Args:
            tenant: 当前租户。
            acl_id: 规则主键。

        Returns:
            AclChange | None: 变更结果；规则不存在 / 不属于当前租户返回 None。

        Raises:
            ServiceUnavailableError: ACL 守卫读取失败（10007 / 503）。
            ResourceAccessDeniedError: 目标主题 `manage` 被拒（82013 / 403）。
        """
        row = await self._rules.get_by_id(tenant_id=int(tenant.tenant_id), acl_id=acl_id)
        if row is None:
            return None
        await self._authorizer.authorize(
            tenant_id=int(tenant.tenant_id),
            namespace=row.namespace,
            topic=row.topic,
            action=ACL_ACTION_MANAGE,
        )
        if row.status == ACL_STATUS_DISABLED:
            return AclChange(view=AclRuleView.from_row(row), audit_action=None, effect=row.effect, changed=False)

        await self._uow.rollback()
        try:
            async with self._uow.begin():
                current = await self._rules.get_by_id(tenant_id=int(tenant.tenant_id), acl_id=acl_id)
                if current is None:  # pragma: no cover - 事务前已取到；并发删除的防御分支
                    return None
                current.status = ACL_STATUS_DISABLED
                await self._scopes.bump_scope_version(tenant_id=int(tenant.tenant_id), namespace=current.namespace)
                view = AclRuleView.from_row(current)
        except IntegrityError:
            async with self._uow.begin():
                current = await self._rules.get_by_id(tenant_id=int(tenant.tenant_id), acl_id=acl_id)
                if current is None:  # pragma: no cover - 同上
                    return None
                current.status = ACL_STATUS_DISABLED
                await self._scopes.bump_scope_version(tenant_id=int(tenant.tenant_id), namespace=current.namespace)
                view = AclRuleView.from_row(current)
        change = AclChange(view=view, audit_action=ACL_REVOKE_ACTION, effect=ACL_EFFECT_DENY, changed=True)
        await self._after_commit(
            tenant_id=int(tenant.tenant_id), changes=ConcurrentStableList([change]), method="DELETE"
        )
        return change

    async def _ensure_topics(self, *, specs: ConcurrentStableList[AclRuleSpec]) -> None:
        """校验每条规格的主题存在且属于当前租户（事务内读）。

        Args:
            specs: 规则规格。

        Raises:
            MsgTopicNotFoundError: 主题不存在 / 不属于当前租户（350001）。
        """
        for spec in specs:
            if await self._topics.get_by_identity(namespace=spec.namespace, name=spec.topic) is None:
                raise MsgTopicNotFoundError(f"主题不存在：{spec.namespace}/{spec.topic}")

    async def _apply(
        self, *, tenant_id: int, specs: ConcurrentStableList[AclRuleSpec]
    ) -> ConcurrentStableList[AclChange]:
        """在事务内应用一批规格（调用方已校验）。

        Args:
            tenant_id: 租户主键。
            specs: 规则规格。

        Returns:
            ConcurrentStableList[AclChange]: 每条规格的结果。
        """
        changes: ConcurrentStableList[AclChange] = ConcurrentStableList()
        touched: ConcurrentStableSet[str] = ConcurrentStableSet()
        for spec in specs:
            existing = await self._rules.get_by_key(
                tenant_id=tenant_id,
                namespace=spec.namespace,
                topic=spec.topic,
                principal_type=spec.principal_type,
                principal_key=spec.principal_key,
                action=spec.action,
            )
            if existing is None:
                row = await self._rules.create(
                    tenant_id=tenant_id,
                    namespace=spec.namespace,
                    topic=spec.topic,
                    principal_type=spec.principal_type,
                    principal_key=spec.principal_key,
                    action=spec.action,
                    effect=spec.effect,
                    status=ACL_STATUS_ENABLED,
                )
                changes.add(
                    AclChange(
                        view=AclRuleView.from_row(row),
                        audit_action=ACL_GRANT_ACTION,
                        effect=row.effect,
                        changed=True,
                    )
                )
                touched.add(spec.namespace)
                continue
            if existing.status == ACL_STATUS_DISABLED:
                existing.status = ACL_STATUS_ENABLED
                existing.effect = spec.effect
                await self._rules.flush()
                changes.add(
                    AclChange(
                        view=AclRuleView.from_row(existing),
                        audit_action=ACL_GRANT_ACTION,
                        effect=existing.effect,
                        changed=True,
                    )
                )
                touched.add(spec.namespace)
                continue
            if existing.effect != spec.effect:
                existing.effect = spec.effect
                await self._rules.flush()
                changes.add(
                    AclChange(
                        view=AclRuleView.from_row(existing),
                        audit_action=ACL_UPDATE_ACTION,
                        effect=existing.effect,
                        changed=True,
                    )
                )
                touched.add(spec.namespace)
                continue
            changes.add(
                AclChange(
                    view=AclRuleView.from_row(existing),
                    audit_action=None,
                    effect=existing.effect,
                    changed=False,
                )
            )
        for namespace in touched:
            await self._scopes.bump_scope_version(tenant_id=tenant_id, namespace=namespace)
        return changes

    async def _after_commit(
        self, *, tenant_id: int, changes: ConcurrentStableList[AclChange], method: str
    ) -> ConcurrentStableList[bool]:
        """提交后失效缓存并写 ACL 强审计。

        Args:
            tenant_id: 租户主键。
            changes: 写入结果。
            method: HTTP 方法（PUT / DELETE）。

        Returns:
            ConcurrentStableList[bool]: 每条变更的审计写入结果（未变更不写）。

        Raises:
            InternalError: 强审计写入失败（10000 / 500）。
        """
        for namespace in ConcurrentStableSet(change.view.namespace for change in changes if change.changed):
            self._cache.invalidate(tenant_id, namespace)
        results: ConcurrentStableList[bool] = ConcurrentStableList()
        for change in changes:
            if change.audit_action is None:
                continue
            await emit_acl_change(
                self._sink,
                action=change.audit_action,
                tenant_id=tenant_id,
                principal=f"{change.view.principal_type}:{change.view.principal_key}",
                permission=change.view.action,
                effect=change.effect,
                module_key=change.view.namespace,
                resource_key=change.view.topic,
                method=method,
                path="/api/v1/msg/admin/acls",
            )
            results.add(True)
        return results


def _frozen_rules(rows: ConcurrentStableList[MsgTopicAcl]) -> ConcurrentStableFrozenList[AclRule]:
    """把 ORM 行转成不可变规则列表（快照构造用）。

    Args:
        rows: 启用规则行。

    Returns:
        ConcurrentStableFrozenList[AclRule]: 规则列表。
    """
    return ConcurrentStableFrozenList(AclRule.from_row(row) for row in rows)


def _validate_spec(spec: AclRuleSpec) -> None:
    """校验一条规则规格的形态（业务存在性另判）。

    Args:
        spec: 规则规格。

    Raises:
        ParamError: 动作 / 效果 / 主体类型 / 主体标识非法（10001）。
    """
    if spec.action not in ACL_ACTIONS:
        raise ParamError(f"ACL 动作非法：{spec.action}")
    if spec.effect not in ACL_EFFECTS:
        raise ParamError(f"ACL 效果非法：{spec.effect}")
    if spec.principal_type not in ACL_PRINCIPAL_TYPES:
        raise ParamError(f"ACL 主体类型非法：{spec.principal_type}")
    if not spec.principal_key.isdigit() or not 1 <= len(spec.principal_key) <= 64:
        raise ParamError(f"ACL 主体标识须为 1~64 位十进制字符串：{spec.principal_key}")
    if not spec.namespace or not spec.topic:
        raise ParamError("ACL 命名空间 / 主题不能为空")


def _ensure_no_duplicates(specs: ConcurrentStableList[AclRuleSpec]) -> None:
    """拒绝同批重复键（避免同一规则两次写入产生歧义审计）。

    Args:
        specs: 规则规格。

    Raises:
        ParamError: 同批出现相同唯一键（10001）。
    """
    seen: ConcurrentStableSet[tuple[str, str, str, str, str]] = ConcurrentStableSet()
    for spec in specs:
        key = (spec.namespace, spec.topic, spec.principal_type, spec.principal_key, spec.action)
        if key in seen:
            raise ParamError(
                "ACL 同批重复规则："
                f"{spec.namespace}/{spec.topic}/{spec.principal_type}:{spec.principal_key}/{spec.action}"
            )
        seen.add(key)
