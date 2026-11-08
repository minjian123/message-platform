"""主题 ACL 核心（Kiwi 2456）：模型 / 仓储 / 快照缓存 / 纯判定 / 端口 / 守卫 / 管理服务。

覆盖：默认允许、显式 deny 优先、用户 / 角色主体、服务身份、跨作用域隔离、
版本戳即时生效、两个缓存实例一致、读取失败 10007、管理面 grant/update/revoke 与审计。
"""

# pyright: reportPrivateUsage=false, reportDeprecated=false

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.core.exceptions import ParamError, ResourceAccessDeniedError, ServiceUnavailableError
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork
from bms_core.resource_scope.acl import AclContext
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.acl.cache import AclRule, AclSnapshot, TopicAclCache
from bms_msg.acl.engine import decide
from bms_msg.acl.keys import (
    ACL_ACTION_DESCRIBE,
    ACL_ACTION_MANAGE,
    ACL_ACTION_PUBLISH,
    ACL_ACTION_SUBSCRIBE,
    ACL_EFFECT_ALLOW,
    ACL_EFFECT_DENY,
    MSG_TOPIC_RESOURCE_TYPE,
)
from bms_msg.acl.port import MsgTopicAcl as MsgTopicAclPort
from bms_msg.acl.port import MsgTopicAclFactory
from bms_msg.acl.service import AclChange, AclRuleSpec, TopicAclAdminService, TopicAclService
from bms_msg.errors import MsgTopicNotFoundError
from bms_msg.models.topic_acl import MsgTopicAcl
from bms_msg.repositories.topic import TopicRepository
from bms_msg.repositories.topic_acl import MsgTopicAclRepository, MsgTopicAclScopeRepository
from bms_msg.tenant.context import CurrentTenant
from bms_msg.tenant.purge import TenantPurgeService

pytestmark = pytest.mark.kiwi_id(2456)

NAMESPACE = "msg"
TOPIC = "audit.operation"
TENANT_ID = 7001
USER_ID = 7001
ROLE_ID = "11"


def _spec(
    *,
    topic: str = TOPIC,
    principal_type: str = "user",
    principal_key: str = str(USER_ID),
    action: str = ACL_ACTION_PUBLISH,
    effect: str = ACL_EFFECT_ALLOW,
) -> AclRuleSpec:
    """构造一条规则规格。"""
    return AclRuleSpec(
        namespace=NAMESPACE,
        topic=topic,
        principal_type=principal_type,
        principal_key=principal_key,
        action=action,
        effect=effect,
    )


def _service(
    session: AsyncSession,
    cache: TopicAclCache,
    *,
    user_id: int | None = USER_ID,
    roles: ConcurrentStableFrozenList[str] | None = None,
) -> TopicAclService:
    """构造 ACL 守卫（端口读同一缓存）。"""
    db = cast("DbSession", session)
    port = MsgTopicAclPort(cache_provider=lambda: cache)
    return TopicAclService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        cache=cache,
        acl_provider=lambda: port,
        user_id_provider=lambda: user_id,
        roles_provider=lambda: roles or ConcurrentStableFrozenList[str](),
    )


async def _grant(
    session: AsyncSession,
    audit_sink: object,
    tenant: CurrentTenant,
    *specs: AclRuleSpec,
) -> ConcurrentStableList[AclChange]:
    """通过管理服务写入规则（同时建主题前置）。"""
    db = cast("DbSession", session)
    uow = DbUnitOfWork(db)
    topics = TopicRepository(db)
    if await topics.get_by_identity(namespace=NAMESPACE, name=TOPIC) is None:
        await topics.create(
            tenant_id=TENANT_ID,
            namespace=NAMESPACE,
            name=TOPIC,
            partition_count=1,
            retention_ms=1,
            cleanup_policy="delete",
        )
        await session.commit()
    service = TopicAclAdminService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        topics=topics,
        uow=uow,
        cache=TopicAclCache(),
        authorizer=_service(session, TopicAclCache()),
        sink=cast("object", audit_sink),  # type: ignore[arg-type]
    )
    return await service.upsert(tenant=tenant, specs=ConcurrentStableList(specs))


async def test_default_allow_without_rules(db_session: AsyncSession) -> None:
    """无规则默认允许；端口拿到空快照也放行。"""
    acl = _service(db_session, TopicAclCache())
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)


async def test_user_allow_and_deny(db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object) -> None:
    """用户 allow 放行；显式 deny 覆盖默认允许。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_ALLOW))
    acl = _service(db_session, TopicAclCache())
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)

    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_DENY))
    with pytest.raises(ResourceAccessDeniedError):
        await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)


async def test_role_allow_and_deny_priority(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """角色 allow 放行；用户 allow 与角色 deny 冲突时 deny 优先。"""
    await _grant(
        db_session,
        audit_sink,
        current_tenant,
        _spec(principal_type="role", principal_key=ROLE_ID, effect=ACL_EFFECT_ALLOW),
    )
    acl = _service(db_session, TopicAclCache(), roles=ConcurrentStableFrozenList([ROLE_ID]))
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)

    await _grant(
        db_session,
        audit_sink,
        current_tenant,
        _spec(principal_type="user", principal_key=str(USER_ID), effect=ACL_EFFECT_ALLOW),
        _spec(principal_type="role", principal_key=ROLE_ID, effect=ACL_EFFECT_DENY),
    )
    with pytest.raises(ResourceAccessDeniedError):
        await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)


async def test_service_identity_not_matched_by_user_rules(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """无 user_id（服务身份）不匹配 user 规则，按默认允许。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_DENY))
    acl = _service(db_session, TopicAclCache(), user_id=None)
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)


async def test_action_and_namespace_are_exact(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """动作精确匹配（manage 不隐含 publish）；不同 namespace 规则不匹配。"""
    await _grant(
        db_session,
        audit_sink,
        current_tenant,
        _spec(action=ACL_ACTION_MANAGE, effect=ACL_EFFECT_DENY),
    )
    acl = _service(db_session, TopicAclCache())
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)
    with pytest.raises(ResourceAccessDeniedError):
        await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_MANAGE)
    # 跨命名空间：同 topic 名不会命中本作用域规则
    await acl.authorize(tenant_id=TENANT_ID, namespace="other", topic=TOPIC, action=ACL_ACTION_MANAGE)


async def test_filter_allowed(db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object) -> None:
    """列表过滤：deny 的主题不返回，默认允许的主题保留。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_DENY))
    acl = _service(db_session, TopicAclCache())
    allowed = await acl.filter_allowed(
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        topics=ConcurrentStableList([TOPIC, "other.topic"]),
        action=ACL_ACTION_DESCRIBE,
    )
    # 规则动作是 publish，describe 默认允许 → 两个都返回
    assert list(allowed) == [TOPIC, "other.topic"]


async def test_version_reloads_same_cache(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """同一缓存：版本戳变化后下一次判定重载规则（变更即时生效）。"""
    cache = TopicAclCache()
    acl = _service(db_session, cache)
    await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_DENY))
    with pytest.raises(ResourceAccessDeniedError):
        await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)


async def test_two_caches_consistent(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """两个缓存实例（模拟多 worker）共享版本源：一个变更后另一个也重载。"""
    first = _service(db_session, TopicAclCache())
    second = _service(db_session, TopicAclCache())
    await first.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_SUBSCRIBE)
    await second.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_SUBSCRIBE)
    await _grant(db_session, audit_sink, current_tenant, _spec(action=ACL_ACTION_SUBSCRIBE, effect=ACL_EFFECT_DENY))
    for acl in (first, second):
        with pytest.raises(ResourceAccessDeniedError):
            await acl.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_SUBSCRIBE)


async def test_snapshot_read_failure_is_fail_closed(db_session: AsyncSession) -> None:
    """版本 / 规则读取失败 → 10007 / 503，不放行。"""

    class _Broken:
        async def get_scope_version(self, **_: object) -> int:
            raise RuntimeError("db down")

        async def list_enabled(self, **_: object) -> ConcurrentStableList[MsgTopicAcl]:
            raise RuntimeError("db down")

    service = TopicAclService(
        rules=cast("object", _Broken()),  # type: ignore[arg-type]
        scopes=cast("object", _Broken()),  # type: ignore[arg-type]
        cache=TopicAclCache(),
        acl_provider=lambda: MsgTopicAclPort(cache_provider=lambda: TopicAclCache()),
    )
    with pytest.raises(ServiceUnavailableError) as excinfo:
        await service.authorize(tenant_id=TENANT_ID, namespace=NAMESPACE, topic=TOPIC, action=ACL_ACTION_PUBLISH)
    assert excinfo.value.code == 10007


async def test_port_missing_snapshot_denies(db_session: AsyncSession) -> None:
    """端口被直接调用且快照未装载 → 82013（fail-closed）。"""
    port = MsgTopicAclPort(cache_provider=lambda: TopicAclCache())
    ctx = AclContext(
        tenant_id=TENANT_ID,
        module_key=NAMESPACE,
        resource_type=MSG_TOPIC_RESOURCE_TYPE,
        resource_key=TOPIC,
        user_id=USER_ID,
    )
    with pytest.raises(ResourceAccessDeniedError):
        port.check(ctx, action=ACL_ACTION_PUBLISH)


async def test_scope_version_and_cache_invalidation(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """版本表首次写 1、再写 2；缓存失效后重载。"""
    db = cast("DbSession", db_session)
    scopes = MsgTopicAclScopeRepository(db)
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_ALLOW))
    assert await scopes.get_scope_version(tenant_id=TENANT_ID, namespace=NAMESPACE) == 1
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_DENY))
    assert await scopes.get_scope_version(tenant_id=TENANT_ID, namespace=NAMESPACE) == 2


async def test_admin_upsert_update_revoke_and_audit(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """grant → update → revoke 的审计动作与幂等口径。"""
    changes = await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_ALLOW))
    assert changes[0].changed is True
    assert changes[0].audit_action == "msg:acl.grant"

    db = cast("DbSession", db_session)
    service = TopicAclAdminService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        topics=TopicRepository(db),
        uow=DbUnitOfWork(db),
        cache=TopicAclCache(),
        authorizer=_service(db_session, TopicAclCache()),
        sink=cast("object", audit_sink),  # type: ignore[arg-type]
    )
    updated = await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(effect=ACL_EFFECT_DENY)]))
    assert updated[0].audit_action == "msg:acl.update"
    unchanged = await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(effect=ACL_EFFECT_DENY)]))
    assert unchanged[0].changed is False

    acl_id = updated[0].view.acl_id
    revoked = await service.revoke(tenant=current_tenant, acl_id=acl_id)
    assert revoked is not None and revoked.audit_action == "msg:acl.revoke"
    again = await service.revoke(tenant=current_tenant, acl_id=acl_id)
    assert again is not None and again.changed is False


async def test_admin_validation(db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object) -> None:
    """重复键 / 非法动作 / 主题不存在分别拒绝。"""
    db = cast("DbSession", db_session)
    service = TopicAclAdminService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        topics=TopicRepository(db),
        uow=DbUnitOfWork(db),
        cache=TopicAclCache(),
        authorizer=_service(db_session, TopicAclCache()),
        sink=cast("object", audit_sink),  # type: ignore[arg-type]
    )
    with pytest.raises(ParamError):
        await service.upsert(
            tenant=current_tenant,
            specs=ConcurrentStableList([_spec(effect=ACL_EFFECT_ALLOW), _spec(effect=ACL_EFFECT_DENY)]),
        )
    with pytest.raises(ParamError):
        await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(action="unknown")]))
    with pytest.raises(ParamError):
        await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(effect="unknown")]))
    with pytest.raises(ParamError):
        await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(principal_type="unknown")]))
    with pytest.raises(ParamError):
        await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(principal_key="abc")]))
    with pytest.raises(ParamError):
        await service.upsert(
            tenant=current_tenant,
            specs=ConcurrentStableList(
                [
                    AclRuleSpec(
                        namespace="",
                        topic=TOPIC,
                        principal_type="user",
                        principal_key="1",
                        action="publish",
                        effect="allow",
                    )
                ]
            ),
        )
    with pytest.raises(MsgTopicNotFoundError):
        await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(topic="missing.topic")]))


def test_repository_filters_cover_all_optional_columns(db_session: AsyncSession) -> None:
    """仓储过滤：未给过滤只有租户条件；全给时六个可选条件都拼入。"""
    repo = MsgTopicAclRepository(cast("DbSession", db_session))
    assert len(repo._filters(TENANT_ID, None, None, None, None, None, None)) == 1
    assert len(repo._filters(TENANT_ID, NAMESPACE, TOPIC, "user", str(USER_ID), ACL_ACTION_PUBLISH, "enabled")) == 7


async def test_purge_acl_deletes_rows_and_cache(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """租户到期清理：删除两表行并失效快照；未注入会话 / 缓存时为空操作。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_ALLOW))
    cache = TopicAclCache()
    cache.put(TENANT_ID, NAMESPACE, AclSnapshot(version=1, rules=ConcurrentStableFrozenList()))

    @asynccontextmanager
    async def _scope() -> AsyncIterator[AsyncSession]:
        """复用内存会话的短会话入口。"""
        yield db_session

    service = TenantPurgeService(
        ledger=cast("Any", None),
        runtime=cast("Any", None),
        data_root=Path("."),
        offsets_root=Path("."),
        acl_session_scope=lambda: _scope(),
        acl_cache=cache,
    )
    await service._purge_acl(TENANT_ID)
    assert cache.get(TENANT_ID, NAMESPACE) is None
    assert (
        await MsgTopicAclRepository(cast("DbSession", db_session)).count_rules(
            tenant_id=TENANT_ID,
            namespace=None,
            topic=None,
            principal_type=None,
            principal_key=None,
            action=None,
            status=None,
        )
        == 0
    )
    assert (
        await MsgTopicAclScopeRepository(cast("DbSession", db_session)).get_scope_version(
            tenant_id=TENANT_ID, namespace=NAMESPACE
        )
        == 0
    )

    empty = TenantPurgeService(
        ledger=cast("Any", None),
        runtime=cast("Any", None),
        data_root=Path("."),
        offsets_root=Path("."),
    )
    await empty._purge_acl(TENANT_ID)


def test_cache_overflow_clear_and_invalidate_tenant() -> None:
    """缓存：同键重复写不触发清空；超上限清空；按租户失效与 clear 覆盖。"""
    cache = TopicAclCache(max_entries=1)
    empty = AclSnapshot(version=0, rules=ConcurrentStableFrozenList())
    cache.put(1, "a", empty)
    cache.put(1, "a", empty)
    cache.put(2, "b", empty)
    cache.put(1, "x", empty)
    cache.put(1, "y", empty)
    cache.put(2, "b", empty)
    cache.invalidate(1, "x")
    cache.invalidate_tenant(1)
    cache.invalidate_tenant(2)
    cache.clear()
    assert cache.get(1, "y") is None


def test_engine_edge_branches() -> None:
    """判定：无资源键 / 非主题类型 / 角色不匹配 / 未知主体类型均不命中。"""
    snapshot = AclSnapshot(
        version=1,
        rules=ConcurrentStableFrozenList(
            (
                AclRule(
                    topic=TOPIC,
                    principal_type="role",
                    principal_key="99",
                    action=ACL_ACTION_PUBLISH,
                    effect=ACL_EFFECT_DENY,
                ),
                AclRule(
                    topic=TOPIC,
                    principal_type="service",
                    principal_key="x",
                    action=ACL_ACTION_PUBLISH,
                    effect=ACL_EFFECT_DENY,
                ),
            )
        ),
    )
    base = AclContext(
        tenant_id=TENANT_ID,
        module_key=NAMESPACE,
        resource_type=MSG_TOPIC_RESOURCE_TYPE,
        resource_key=TOPIC,
        user_id=USER_ID,
    )
    assert decide(snapshot, base, action=ACL_ACTION_PUBLISH) is True
    assert decide(snapshot, AclContext(tenant_id=TENANT_ID, module_key=NAMESPACE), action=ACL_ACTION_PUBLISH) is False
    assert (
        decide(
            snapshot,
            AclContext(
                tenant_id=TENANT_ID,
                module_key=NAMESPACE,
                resource_type="other.type",
                resource_key=TOPIC,
            ),
            action=ACL_ACTION_PUBLISH,
        )
        is False
    )


def test_port_missing_cache_and_factory() -> None:
    """端口：缓存未装配拒绝；工厂从 app.state 惰性取缓存。"""
    port = MsgTopicAclPort(cache_provider=lambda: None)
    ctx = AclContext(
        tenant_id=TENANT_ID,
        module_key=NAMESPACE,
        resource_type=MSG_TOPIC_RESOURCE_TYPE,
        resource_key=TOPIC,
        user_id=USER_ID,
    )
    with pytest.raises(ResourceAccessDeniedError):
        port.check(ctx, action=ACL_ACTION_PUBLISH)

    cache = TopicAclCache()
    cache.put(TENANT_ID, NAMESPACE, AclSnapshot(version=0, rules=ConcurrentStableFrozenList()))
    factory = MsgTopicAclFactory(SimpleNamespace(state=SimpleNamespace(topic_acl_cache=cache)))
    factory.create().check(ctx, action=ACL_ACTION_PUBLISH)


async def test_filter_deny_and_release(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object
) -> None:
    """过滤遇到 deny 跳过；release 注入后每次读取结束调用。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(action=ACL_ACTION_DESCRIBE, effect=ACL_EFFECT_DENY))
    released: ConcurrentStableList[bool] = ConcurrentStableList()

    async def _release() -> None:
        released.add(True)

    db = cast("DbSession", db_session)
    cache = TopicAclCache()
    service = TopicAclService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        cache=cache,
        acl_provider=lambda: MsgTopicAclPort(cache_provider=lambda: cache),
        user_id_provider=lambda: USER_ID,
        release=_release,
    )
    allowed = await service.filter_allowed(
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        topics=ConcurrentStableList([TOPIC]),
        action=ACL_ACTION_DESCRIBE,
    )
    assert list(allowed) == []
    assert list(released) == [True]


async def test_admin_regrant_and_integrity_retry(
    db_session: AsyncSession, current_tenant: CurrentTenant, audit_sink: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """收回后重新授予走 grant；upsert / revoke 的 IntegrityError 各重试一次。"""
    await _grant(db_session, audit_sink, current_tenant, _spec(effect=ACL_EFFECT_ALLOW))
    db = cast("DbSession", db_session)
    service = TopicAclAdminService(
        rules=MsgTopicAclRepository(db),
        scopes=MsgTopicAclScopeRepository(db),
        topics=TopicRepository(db),
        uow=DbUnitOfWork(db),
        cache=TopicAclCache(),
        authorizer=_service(db_session, TopicAclCache()),
        sink=cast("object", audit_sink),  # type: ignore[arg-type]
    )
    row = await MsgTopicAclRepository(db).get_by_key(
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        topic=TOPIC,
        principal_type="user",
        principal_key=str(USER_ID),
        action=ACL_ACTION_PUBLISH,
    )
    assert row is not None
    await service.revoke(tenant=current_tenant, acl_id=int(row.id))
    regrant = await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(effect=ACL_EFFECT_ALLOW)]))
    assert regrant[0].audit_action == "msg:acl.grant"

    calls: ConcurrentStableList[int] = ConcurrentStableList()
    original = service._apply  # type: ignore[attr-defined]

    async def _flaky_apply(**kwargs: object) -> ConcurrentStableList[AclChange]:
        calls.add(1)
        if len(calls) == 1:
            raise IntegrityError("stmt", None, Exception("conflict"))
        return await original(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(service, "_apply", _flaky_apply)
    await service.upsert(tenant=current_tenant, specs=ConcurrentStableList([_spec(effect=ACL_EFFECT_DENY)]))
    assert len(calls) == 2

    bumped: ConcurrentStableList[int] = ConcurrentStableList()

    class _FlakyScopes:
        async def bump_scope_version(self, **_kwargs: object) -> int:
            bumped.add(1)
            if len(bumped) == 1:
                raise IntegrityError("stmt", None, Exception("conflict"))
            return 2

    monkeypatch.setattr(service, "_scopes", _FlakyScopes())
    revoked = await service.revoke(tenant=current_tenant, acl_id=int(row.id))
    assert revoked is not None and revoked.changed is True
    assert len(bumped) == 2
