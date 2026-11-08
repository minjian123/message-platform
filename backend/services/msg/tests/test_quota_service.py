"""05-2 消息维度配额：定义 / 缓存 / 服务（Kiwi 2454）。

覆盖：六维定义与环境变量覆盖、软限派生与校验、缓存 TTL / 失效、缺省行懒初始化与幂等、
命名空间覆盖回落、判定状态、管理面批量调整与非法分支、用量快照取数。
"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork, UnitOfWork
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.deps import get_quota_service_cached
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import InMemoryGroupStore
from bms_msg.broker.transport import ConnectionMeta
from bms_msg.models.quota import MsgQuota
from bms_msg.models.topic import MsgTopic
from bms_msg.quota.cache import QuotaCache, QuotaLimit
from bms_msg.quota.keys import QUOTA_DEFINITIONS, definition, effective_soft, next_soft_limit
from bms_msg.quota.service import (
    STATE_HARD_EXCEEDED,
    STATE_OK,
    STATE_SOFT_EXCEEDED,
    STATE_UNAVAILABLE,
    STATE_UNCONFIGURED,
    QuotaService,
    QuotaUpdate,
    judge,
)
from bms_msg.repositories.quota import QuotaRepository
from bms_msg.usage.counter import DeliveryCounter, default_delivery_counter, reset_default_delivery_counter
from bms_msg.usage.snapshot import StorageUsageSampler, UsageSnapshot, UsageSnapshotService

pytestmark = pytest.mark.kiwi_id(2454)

TENANT_ID = 7001


def _limit(**overrides: object) -> QuotaLimit:
    """构造测试限额。"""
    base: dict[str, object] = {
        "key": "topic_count",
        "namespace": "",
        "limit": 100,
        "soft_limit": 0,
        "unit": "count",
        "period": "none",
        "origin": "default",
        "updated_at": None,
    }
    base.update(overrides)
    return QuotaLimit(**base)  # type: ignore[arg-type]


def _service(db_session: AsyncSession) -> tuple[QuotaService, QuotaRepository, DbUnitOfWork]:
    """组装请求级配额服务。"""
    session = cast("DbSession", db_session)
    repo = QuotaRepository(session)
    uow = DbUnitOfWork(session)
    return QuotaService(cache=QuotaCache(), repo=repo, uow=uow), repo, uow


class _Usage:
    """用量快照替身。"""

    def __init__(self, snapshot: UsageSnapshot | None) -> None:
        self._snapshot = snapshot

    async def snapshot(self, tenant_id: int) -> UsageSnapshot | None:
        del tenant_id
        return self._snapshot


class _BrokenRepo(QuotaRepository):
    """读取恒失败的仓储（验证降级分支）。"""

    def __init__(self, session: DbSession) -> None:  # pragma: no cover - 不触真实会话
        super().__init__(session)

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[MsgQuota]:
        del tenant_id
        raise RuntimeError("boom")


class _RaisingUpsertRepo(QuotaRepository):
    """缺省写入触发唯一冲突的仓储。"""

    def __init__(self, session: DbSession) -> None:
        super().__init__(session)
        self.calls = 0

    async def list_for_tenant(self, *, tenant_id: int) -> ConcurrentStableList[MsgQuota]:
        self.calls += 1
        if self.calls == 1:
            return ConcurrentStableList()
        statement = select(MsgQuota).where(MsgQuota.tenant_id == int(tenant_id))
        return ConcurrentStableList((await self._session.execute(statement)).scalars().all())

    async def upsert_default(self, **values: object) -> tuple[MsgQuota, bool]:
        del values
        from sqlalchemy.exc import IntegrityError

        raise IntegrityError("insert", {}, Exception("duplicate"))


class _FakeUow(UnitOfWork):
    """工作单元替身（记录提交 / 回滚）。"""

    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def begin(self) -> AbstractAsyncContextManager[object]:
        return nullcontext()

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def test_definitions_are_complete_and_unique() -> None:
    """六维定义键唯一、单位 / 周期 / 缺省值合法。"""
    assert [item.key for item in QUOTA_DEFINITIONS] == [
        "topic_count",
        "group_count",
        "produce_rate",
        "storage_bytes",
        "unacked_count",
        "connection_count",
    ]
    assert {item.unit for item in QUOTA_DEFINITIONS} == {"count", "bytes", "qps"}
    assert all(item.period == "none" and item.default >= 0 for item in QUOTA_DEFINITIONS)
    with pytest.raises(KeyError):
        definition("unknown")


def test_default_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量覆盖缺省值；非法 / 空值回落模块常量。"""
    item = definition("topic_count")
    monkeypatch.setenv(item.default_env, "7")
    assert item.default == 7
    monkeypatch.setenv(item.default_env, "-1")
    assert item.default == item.default_value
    monkeypatch.setenv(item.default_env, "abc")
    assert item.default == item.default_value
    monkeypatch.setenv(item.default_env, "")
    assert item.default == item.default_value


def test_soft_limit_rules() -> None:
    """软限：显式值优先、缺省按 80% 派生、硬限不限则无软限。"""
    assert effective_soft(0, 0) == 0
    assert effective_soft(100, 0) == 80
    assert effective_soft(100, 50) == 50
    assert effective_soft(1, 0) == 1
    assert next_soft_limit(0, None) == 0
    assert next_soft_limit(100, 0) == 0
    assert next_soft_limit(100, 50) == 50
    with pytest.raises(ValueError):
        next_soft_limit(100, -1)
    with pytest.raises(ValueError):
        next_soft_limit(0, 1)
    with pytest.raises(ValueError):
        next_soft_limit(100, 101)


def test_judge_states() -> None:
    """判定：不可用 / 硬限 / 软限 / 未配置 / 已配置。"""
    assert judge(usage=None, limit=_limit()) == STATE_UNAVAILABLE
    assert judge(usage=100, limit=_limit()) == STATE_HARD_EXCEEDED
    assert judge(usage=80, limit=_limit()) == STATE_SOFT_EXCEEDED
    assert judge(usage=10, limit=_limit()) == STATE_UNCONFIGURED
    assert judge(usage=10, limit=_limit(origin="configured")) == STATE_OK


def test_cache_defaults_and_namespace_override() -> None:
    """缓存：缺省回落、租户级作用域忽略命名空间行、命名空间覆盖优先。"""
    cache = QuotaCache()
    default = cache.resolve(TENANT_ID, "msg", "topic_count")
    assert default.namespace == "" and default.origin == "default"
    assert default.hard_limit == default.limit
    assert default.soft_limit_or_none == default.effective_soft

    cache.put(
        TENANT_ID,
        ConcurrentStableList(
            [
                MsgQuota(
                    tenant_id=TENANT_ID,
                    namespace="",
                    quota_key="topic_count",
                    limit_value=10,
                    soft_limit=5,
                    unit="count",
                    period="none",
                    origin="configured",
                ),
                MsgQuota(
                    tenant_id=TENANT_ID,
                    namespace="msg",
                    quota_key="topic_count",
                    limit_value=3,
                    soft_limit=0,
                    unit="count",
                    period="none",
                    origin="configured",
                ),
                MsgQuota(
                    tenant_id=TENANT_ID,
                    namespace="msg",
                    quota_key="group_count",
                    limit_value=2,
                    soft_limit=0,
                    unit="count",
                    period="none",
                    origin="configured",
                ),
            ]
        ),
    )
    assert cache.resolve(TENANT_ID, "msg", "topic_count").limit == 3
    assert cache.resolve(TENANT_ID, "other", "topic_count").limit == 10
    # 租户级作用域不取命名空间行（group_count 为 tenant 作用域）
    assert cache.resolve(TENANT_ID, "msg", "group_count").limit == definition("group_count").default
    assert cache.resolve(TENANT_ID, "", "group_count").limit == definition("group_count").default
    assert cache.resolve(TENANT_ID, "", "storage_bytes").origin == "default"
    assert len(cache.resolve_all(TENANT_ID)) == 6
    assert cache.is_initialized(TENANT_ID) is False
    cache.mark_initialized(TENANT_ID)
    assert cache.is_initialized(TENANT_ID) is True
    cache.invalidate(TENANT_ID)
    assert cache.get(TENANT_ID) is None and cache.is_initialized(TENANT_ID) is False


def test_cache_ttl_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    """缓存超过 TTL 即失效。"""
    now = [1000.0]
    monkeypatch.setattr("bms_msg.quota.cache.time.monotonic", lambda: now[0])
    cache = QuotaCache(ttl_ms=10)
    cache.put(TENANT_ID, ConcurrentStableList())
    assert cache.get(TENANT_ID) is not None
    now[0] += 0.02
    assert cache.get(TENANT_ID) is None


async def test_ensure_initialized_is_idempotent(db_session: AsyncSession) -> None:
    """懒初始化写六条租户级缺省行；重复执行不新增。"""
    service, repo, _uow = _service(db_session)
    await service.ensure_initialized(TENANT_ID)
    rows = await repo.list_for_tenant(tenant_id=TENANT_ID)
    assert {row.quota_key for row in rows} == {item.key for item in QUOTA_DEFINITIONS}
    assert all(row.namespace == "" and row.origin == "default" for row in rows)
    await service.ensure_initialized(TENANT_ID)
    assert len(await repo.list_for_tenant(tenant_id=TENANT_ID)) == 6


async def test_ensure_initialized_integrity_conflict_falls_back(db_session: AsyncSession) -> None:
    """唯一约束冲突（并发）后重查，不报错。"""
    repo = _RaisingUpsertRepo(cast("DbSession", db_session))
    uow = _FakeUow()
    service = QuotaService(cache=QuotaCache(), repo=repo, uow=uow)
    await service.ensure_initialized(TENANT_ID)
    assert uow.rollbacks == 1
    assert service.cache.is_initialized(TENANT_ID) is True


async def test_load_failure_falls_back_to_defaults(db_session: AsyncSession) -> None:
    """回源失败时按缺省值生效，不抛错。"""
    repo = _BrokenRepo(cast("DbSession", db_session))
    service = QuotaService(cache=QuotaCache(), repo=repo, uow=DbUnitOfWork(cast("DbSession", db_session)))
    limit = await service.effective(TENANT_ID, "msg", "topic_count")
    assert limit.origin == "default"
    assert service.cached_limit(TENANT_ID, "msg", "topic_count").origin == "default"


async def test_update_writes_configured_rows(db_session: AsyncSession) -> None:
    """批量调整写入 `origin=configured` 并派生软限。"""
    service, repo, uow = _service(db_session)
    async with uow.begin():
        await service.update(
            TENANT_ID,
            [
                QuotaUpdate(quota_key="topic_count", namespace="msg", limit=10, soft_limit=None),
                QuotaUpdate(quota_key="storage_bytes", namespace="", limit=0, soft_limit=None),
            ],
        )
    rows = {row.quota_key: row for row in await repo.list_for_tenant(tenant_id=TENANT_ID)}
    assert rows["topic_count"].origin == "configured"
    assert rows["topic_count"].soft_limit == 0
    assert effective_soft(rows["topic_count"].limit_value, rows["topic_count"].soft_limit) == 8
    assert rows["storage_bytes"].limit_value == 0


async def test_update_rejects_invalid(db_session: AsyncSession) -> None:
    """非法调整：未知维度 / 重复作用域 / 负值。"""
    service, _repo, _uow = _service(db_session)
    with pytest.raises(KeyError):
        await service.update(TENANT_ID, [QuotaUpdate("unknown", "", 1, None)])
    with pytest.raises(ValueError):
        await service.update(
            TENANT_ID,
            [QuotaUpdate("topic_count", "", 1, None), QuotaUpdate("topic_count", "", 2, None)],
        )
    with pytest.raises(ValueError):
        await service.update(TENANT_ID, [QuotaUpdate("topic_count", "", -1, None)])
    with pytest.raises(RuntimeError):
        await QuotaService(cache=QuotaCache()).update(TENANT_ID, [])


async def test_assess_all_uses_usage_snapshot(db_session: AsyncSession) -> None:
    """判定视图合并限额与用量（含存储不可用）。"""
    cache = QuotaCache()
    service = QuotaService(
        cache=cache,
        repo=QuotaRepository(cast("DbSession", db_session)),
        uow=DbUnitOfWork(cast("DbSession", db_session)),
        usage=cast(
            "object",
            _Usage(
                UsageSnapshot(
                    topic_count=100,
                    partition_count=1,
                    storage_bytes=None,
                    delivered_today=0,
                    group_count=0,
                    unacked_count=0,
                    connection_count=0,
                )
            ),
        ),  # type: ignore[arg-type]
    )
    assessments = await service.assess_all(TENANT_ID)
    by_key = {item.limit.key: item for item in assessments}
    assert by_key["topic_count"].state == STATE_HARD_EXCEEDED
    assert by_key["storage_bytes"].state == STATE_UNAVAILABLE
    one = await service.assess(TENANT_ID, "", "group_count")
    assert one.usage == 0


async def test_snapshot_service_counts(db_session: AsyncSession, tmp_path: Path) -> None:
    """用量快照：主题 / 分区 / 组 / 未确认 / 连接 / 存储 / 今日投递。"""
    session = cast("DbSession", db_session)
    topic = MsgTopic(
        tenant_id=TENANT_ID,
        namespace="msg",
        name="audit.operation",
        partition_count=2,
        retention_ms=1000,
        retention_bytes=0,
        cleanup_policy="delete",
        status="active",
    )
    session.add(topic)
    await session.flush()
    groups = InMemoryGroupStore()
    from bms_msg.broker.group import ConsumerGroup

    await groups.save(
        ConsumerGroup(
            tenant_id=TENANT_ID,
            topic_id=int(topic.id),
            group="g1",
            state="stable",
            generation=1,
            members=ConcurrentStableList(),
        )
    )
    state = DeliveryState()
    from datetime import timedelta

    from bms_msg.broker.delivery import InflightDelivery

    state.track(
        ("g1", int(topic.id), 0),
        1,
        InflightDelivery(attempts=1, deadline=datetime.now(UTC) + timedelta(seconds=1)),
    )
    connections = ConnectionRegistry()
    connections.touch(
        key="demo|h:1",
        meta=ConnectionMeta(client_host="h", client_port=1, http_version="2", scheme="http", is_tls=False),
        tenant="demo",
        now=0.0,
        wall=0.0,
        heartbeat=False,
        tenant_id=TENANT_ID,
    )
    counter = DeliveryCounter(today=lambda: date(2026, 1, 1))
    counter.increment(3)
    directory = tmp_path / "data" / str(TENANT_ID)
    directory.mkdir(parents=True)
    (directory / "a.log").write_bytes(b"x" * 5)
    sampler = StorageUsageSampler(root=tmp_path, clock=lambda: 1.0)
    service = UsageSnapshotService(
        session=session,
        groups=groups,
        delivery_state=state,
        connections=connections,
        counter=counter,
        sampler=sampler,
    )
    snapshot = await service.snapshot(TENANT_ID)
    assert (snapshot.topic_count, snapshot.partition_count) == (1, 0)
    assert snapshot.group_count == 1
    assert snapshot.unacked_count == 1
    assert snapshot.connection_count == 1
    assert snapshot.storage_bytes == 5
    assert snapshot.delivered_today == 3
    assert await service.storage_bytes(TENANT_ID) == 5
    sampler.invalidate(TENANT_ID)
    assert await service.unacked_count(TENANT_ID) == 1


def test_snapshot_value_of_unknown_key() -> None:
    """未知维度 / 速率不可用返回 None。"""
    snapshot = UsageSnapshot(1, 1, 1, 1, 1, 1, 1)
    assert snapshot.value_of("produce_rate") is None
    assert snapshot.value_of("unknown") is None


async def test_purge_removes_quota_rows_and_invalidates_cache(db_session: AsyncSession, tmp_path: Path) -> None:
    """租户到期清理：删配额行并失效缓存 / 存储采样。"""
    from contextlib import asynccontextmanager

    from bms_msg.tenant.purge import TenantPurgeService

    session = cast("DbSession", db_session)
    repo = QuotaRepository(session)
    await repo.create(
        tenant_id=TENANT_ID,
        namespace="",
        quota_key="topic_count",
        limit_value=10,
        soft_limit=0,
        unit="count",
        period="none",
        origin="configured",
    )
    await session.commit()

    @asynccontextmanager  # pyright: ignore[reportDeprecated]
    async def scope() -> AsyncIterator[DbSession]:
        yield session

    cache = QuotaCache()
    cache.put(TENANT_ID, ConcurrentStableList())
    sampler = StorageUsageSampler(root=tmp_path)
    service = TenantPurgeService(
        ledger=cast("object", None),  # type: ignore[arg-type]
        runtime=cast("object", None),  # type: ignore[arg-type]
        data_root=tmp_path,
        offsets_root=tmp_path,
        quota_session_scope=scope,
        quota_cache=cache,
        storage_sampler=sampler,
    )
    await service._purge_quota(TENANT_ID)
    assert await repo.list_for_tenant(tenant_id=TENANT_ID) == []
    assert cache.get(TENANT_ID) is None

    # 未装配清理入口：空转不报错
    bare = TenantPurgeService(
        ledger=cast("object", None),  # type: ignore[arg-type]
        runtime=cast("object", None),  # type: ignore[arg-type]
        data_root=tmp_path,
        offsets_root=tmp_path,
    )
    await bare._purge_quota(TENANT_ID)


def test_cache_unknown_key_raises() -> None:
    """缓存取未知维度即 `KeyError`（参数错误口径）。"""
    with pytest.raises(KeyError):
        QuotaCache().resolve(TENANT_ID, "", "unknown")


def test_assessment_exceeded_flag() -> None:
    """判定视图 `exceeded()` 与状态同源。"""
    from bms_msg.quota.service import QuotaAssessment

    assert QuotaAssessment(limit=_limit(), usage=100, state=STATE_HARD_EXCEEDED).exceeded() is True
    assert QuotaAssessment(limit=_limit(), usage=1, state=STATE_OK).exceeded() is False


async def test_ensure_initialized_skips_existing_rows(db_session: AsyncSession) -> None:
    """缓存失效但缺省行已在库：重复初始化命中 `continue`，不重复写。"""
    service, repo, _uow = _service(db_session)
    await service.ensure_initialized(TENANT_ID)
    service.cache.invalidate(TENANT_ID)
    await service.ensure_initialized(TENANT_ID)
    assert len(await repo.list_for_tenant(tenant_id=TENANT_ID)) == 6


async def test_reload_quietly_without_repo_and_with_broken_repo(db_session: AsyncSession) -> None:
    """并发兜底：无仓储直接返回；仓储仍失败只记日志。"""
    await QuotaService(cache=QuotaCache())._reload_quietly(TENANT_ID)
    broken = QuotaService(
        cache=QuotaCache(),
        repo=_BrokenRepo(cast("DbSession", db_session)),
        uow=DbUnitOfWork(cast("DbSession", db_session)),
    )
    await broken._reload_quietly(TENANT_ID)
    assert broken.cache.is_initialized(TENANT_ID) is False


async def test_repository_upsert_branches(db_session: AsyncSession) -> None:
    """仓储：缺省行幂等（已存在返回 False）；配置行已存在走更新分支。"""
    repo = QuotaRepository(cast("DbSession", db_session))
    first, created = await repo.upsert_default(
        tenant_id=TENANT_ID,
        namespace="",
        quota_key="topic_count",
        limit_value=10,
        soft_limit=0,
        unit="count",
        period="none",
    )
    assert created is True and first.limit_value == 10
    again, created_again = await repo.upsert_default(
        tenant_id=TENANT_ID,
        namespace="",
        quota_key="topic_count",
        limit_value=99,
        soft_limit=0,
        unit="count",
        period="none",
    )
    assert created_again is False and again.limit_value == 10
    configured = await repo.upsert_configured(
        tenant_id=TENANT_ID,
        namespace="",
        quota_key="topic_count",
        limit_value=5,
        soft_limit=4,
        unit="count",
        period="none",
    )
    assert configured.limit_value == 5
    await db_session.commit()


def test_delivery_counter_roll_reset_and_default() -> None:
    """今日投递计数：跨日归零、负增量按 0、重置与进程级缺省实例。"""
    today = [date(2026, 1, 1)]
    counter = DeliveryCounter(today=lambda: today[0])
    counter.increment(3)
    counter.increment(-5)
    assert counter.value() == 3
    today[0] = date(2026, 1, 2)
    assert counter.value() == 0
    counter.increment(2)
    counter.reset()
    assert counter.value() == 0
    reset_default_delivery_counter()
    assert default_delivery_counter().value() == 0


def test_storage_sampler_skips_subdirectories(tmp_path: Path) -> None:
    """目录扫描只累计文件：`rglob` 会连目录一起返回，目录项必须跳过。"""
    nested = tmp_path / "data" / str(TENANT_ID) / "audit.operation" / "0"
    nested.mkdir(parents=True)
    (nested / "segment.log").write_bytes(b"x" * 10)

    sampler = StorageUsageSampler(root=tmp_path)

    assert sampler.sample(TENANT_ID) == 10, "只算文件大小，目录本身不累计"


def test_storage_sampler_handles_scan_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """目录扫描失败返回 None（投影判不可用，不拒绝写入）。"""
    directory = tmp_path / "data" / str(TENANT_ID)
    directory.mkdir(parents=True)

    def _boom(self: Path, pattern: str) -> object:
        del self, pattern
        raise OSError("boom")

    monkeypatch.setattr(Path, "rglob", _boom)
    sampler = StorageUsageSampler(root=tmp_path)
    assert sampler.sample(TENANT_ID) is None


def test_cached_quota_service_dependency() -> None:
    """连接守卫用只读配额服务：只带缓存、无仓储。"""
    cache = QuotaCache()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(quota_cache=cache)))
    service = get_quota_service_cached(cast("object", request))  # type: ignore[arg-type]
    assert service.cache is cache
