"""配额服务（05-2）：限额合并 / 判定 / 懒初始化 / 管理面调整。

- **缺省兜底**：缓存未命中按内置缺省（`quota/keys.py`）生效，配额行缺失不会让业务停摆。
- **懒初始化**：首次发布时把六条租户级缺省行落库（`origin=default`），唯一约束兜底并发。
- **调整**：管理面批量写入（`origin=configured`）；调用方提交成功后失效缓存。
- **判定**：`judge()` 只按硬限拒绝，软限仅告警；用量不可用（`None`）判 `unavailable`、不拒绝。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.logging import get_logger
from bms_core.db.unit_of_work import UnitOfWork
from sqlalchemy.exc import IntegrityError

from bms_msg.core.config import QUOTA_ORIGIN_DEFAULT
from bms_msg.models.quota import MsgQuota
from bms_msg.quota.cache import QuotaCache, QuotaLimit
from bms_msg.quota.keys import QUOTA_DEFINITIONS, definition, next_soft_limit
from bms_msg.repositories.quota import QuotaRepository
from bms_msg.usage.snapshot import UsageSnapshot, UsageSnapshotService

__all__ = [
    "STATE_HARD_EXCEEDED",
    "STATE_OK",
    "STATE_SOFT_EXCEEDED",
    "STATE_UNAVAILABLE",
    "STATE_UNCONFIGURED",
    "QuotaAssessment",
    "QuotaService",
    "QuotaUpdate",
]

_LOGGER = get_logger("bms_msg.quota")

STATE_OK = "ok"
"""状态：未超软限且已配置。"""

STATE_UNCONFIGURED = "unconfigured"
"""状态：未显式配置（按缺省限额生效，且未触软限）。"""

STATE_SOFT_EXCEEDED = "soft_exceeded"
"""状态：已达软限（只告警）。"""

STATE_HARD_EXCEEDED = "hard_exceeded"
"""状态：已达硬限（业务拒绝）。"""

STATE_UNAVAILABLE = "unavailable"
"""状态：用量不可用（如存储扫描失败）。"""


@dataclass(frozen=True)
class QuotaUpdate:
    """一条管理面调整请求（已过契约层的形态校验）。"""

    quota_key: str
    """维度键。"""

    namespace: str
    """作用域（空串 = 租户级）。"""

    limit: int
    """硬限（0 = 不限）。"""

    soft_limit: int | None
    """软限（None / 0 = 未设置）。"""


@dataclass(frozen=True)
class _NormalizedUpdate:
    """归一后的调整项（软限已校验）。"""

    item: QuotaUpdate
    soft_limit: int


@dataclass(frozen=True)
class QuotaAssessment:
    """一维配额的判定视图（管理面 / 投影 / 执行点共用）。"""

    limit: QuotaLimit
    """生效限额。"""

    usage: int | None
    """当前用量；不可用为 None。"""

    state: str
    """状态（unconfigured / ok / soft_exceeded / hard_exceeded / unavailable）。"""

    @property
    def source(self) -> str:
        """来源（default / configured）。

        Returns:
            str: 来源。
        """
        return self.limit.origin

    def exceeded(self) -> bool:
        """是否已达硬限（业务应拒绝）。

        Returns:
            bool: 已达硬限返回 True。
        """
        return self.state == STATE_HARD_EXCEEDED


def judge(*, usage: int | None, limit: QuotaLimit) -> str:
    """按用量与限额判定状态。

    Args:
        usage: 当前用量；不可用为 None。
        limit: 生效限额。

    Returns:
        str: 状态。
    """
    if usage is None:
        return STATE_UNAVAILABLE
    if limit.limit > 0 and usage >= limit.limit:
        return STATE_HARD_EXCEEDED
    if limit.effective_soft > 0 and usage >= limit.effective_soft:
        return STATE_SOFT_EXCEEDED
    if limit.origin == QUOTA_ORIGIN_DEFAULT:
        return STATE_UNCONFIGURED
    return STATE_OK


class QuotaService:
    """配额服务（请求级仓储 + 进程级缓存 / 用量）。"""

    def __init__(
        self,
        *,
        cache: QuotaCache,
        repo: QuotaRepository | None = None,
        uow: UnitOfWork | None = None,
        usage: UsageSnapshotService | None = None,
    ) -> None:
        """初始化。

        Args:
            cache: 进程级配额快照缓存。
            repo: 配额仓储（无请求会话时为 None，只走缺省值）。
            uow: 平台库工作单元（懒初始化提交用）。
            usage: 用量快照服务（None 表示不取用量）。
        """
        self._cache = cache
        self._repo = repo
        self._uow = uow
        self._usage = usage

    @property
    def cache(self) -> QuotaCache:
        """配额快照缓存（管理面调整后显式失效）。

        Returns:
            QuotaCache: 缓存。
        """
        return self._cache

    async def ensure_initialized(self, tenant_id: int) -> None:
        """首次发布时补齐六条租户级缺省行（幂等；失败只记日志，不阻断发布）。

        Args:
            tenant_id: 租户主键。
        """
        if self._cache.is_initialized(tenant_id) or self._repo is None or self._uow is None:
            return
        try:
            rows = await self._repo.list_for_tenant(tenant_id=tenant_id)
            existing = {row.quota_key for row in rows if row.namespace == ""}
            created = False
            for item in QUOTA_DEFINITIONS:
                if item.key in existing:
                    continue
                await self._repo.upsert_default(
                    tenant_id=tenant_id,
                    namespace="",
                    quota_key=item.key,
                    limit_value=item.default,
                    soft_limit=0,
                    unit=item.unit,
                    period=item.period,
                )
                created = True
            if created:
                await self._uow.commit()
                rows = await self._repo.list_for_tenant(tenant_id=tenant_id)
            self._cache.put(tenant_id, rows)
            self._cache.mark_initialized(tenant_id)
        except IntegrityError:
            await self._uow.rollback()
            await self._reload_quietly(tenant_id)
        except Exception as exc:
            await self._uow.rollback()
            _LOGGER.warning("msg_quota_init_failed", tenant_id=int(tenant_id), error=repr(exc))

    async def _reload_quietly(self, tenant_id: int) -> None:
        """并发兜底：唯一约束冲突后重查并写缓存。

        Args:
            tenant_id: 租户主键。
        """
        if self._repo is None:
            return
        try:
            rows = await self._repo.list_for_tenant(tenant_id=tenant_id)
        except Exception as exc:
            _LOGGER.warning("msg_quota_reload_failed", tenant_id=int(tenant_id), error=repr(exc))
            return
        self._cache.put(tenant_id, rows)
        self._cache.mark_initialized(tenant_id)

    async def _load(self, tenant_id: int) -> ConcurrentStableFrozenList[QuotaLimit] | None:
        """取租户快照（缓存优先；仓储可用时回源）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableFrozenList[QuotaLimit] | None: 快照；无仓储且未缓存返回 None。
        """
        snapshot = self._cache.get(tenant_id)
        if snapshot is not None:
            return snapshot
        if self._repo is None:
            return None
        try:
            rows = await self._repo.list_for_tenant(tenant_id=tenant_id)
        except Exception as exc:
            _LOGGER.warning("msg_quota_load_failed", tenant_id=int(tenant_id), error=repr(exc))
            return None
        return self._cache.put(tenant_id, rows)

    async def effective(self, tenant_id: int, namespace: str, quota_key: str) -> QuotaLimit:
        """取一维生效限额（命名空间覆盖 → 租户级 → 内置缺省）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 维度键。

        Returns:
            QuotaLimit: 生效限额。

        Raises:
            KeyError: 未知维度键。
        """
        await self._load(tenant_id)
        return self._cache.resolve(tenant_id, namespace, quota_key)

    def cached_limit(self, tenant_id: int, namespace: str, quota_key: str) -> QuotaLimit:
        """只读缓存取限额（连接守卫等不查库的路径用）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 维度键。

        Returns:
            QuotaLimit: 生效限额（未缓存即内置缺省）。
        """
        return self._cache.resolve(tenant_id, namespace, quota_key)

    async def assess_all(self, tenant_id: int, *, namespace: str = "") -> ConcurrentStableList[QuotaAssessment]:
        """取六维判定视图（管理面 / 投影）。

        Args:
            tenant_id: 租户主键。
            namespace: 作用域（空串 = 租户级）。

        Returns:
            ConcurrentStableList[QuotaAssessment]: 六维判定（固定定义顺序）。
        """
        await self._load(tenant_id)
        snapshot = await self._snapshot(tenant_id)
        return ConcurrentStableList(
            self._assess(tenant_id, namespace, item.key, snapshot) for item in QUOTA_DEFINITIONS
        )

    async def assess(
        self, tenant_id: int, namespace: str, quota_key: str, *, snapshot: UsageSnapshot | None = None
    ) -> QuotaAssessment:
        """取一维判定视图。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            quota_key: 维度键。
            snapshot: 已取到的用量快照（避免重复取数）；None 时自行取。

        Returns:
            QuotaAssessment: 判定。
        """
        limit = await self.effective(tenant_id, namespace, quota_key)
        current = snapshot if snapshot is not None else await self._snapshot(tenant_id)
        usage = None if current is None else current.value_of(quota_key)
        return QuotaAssessment(limit=limit, usage=usage, state=judge(usage=usage, limit=limit))

    def _assess(
        self, tenant_id: int, namespace: str, quota_key: str, snapshot: UsageSnapshot | None
    ) -> QuotaAssessment:
        """同步判定（调用方已加载缓存 / 用量）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            quota_key: 维度键。
            snapshot: 用量快照。

        Returns:
            QuotaAssessment: 判定。
        """
        limit = self._cache.resolve(tenant_id, namespace, quota_key)
        usage = None if snapshot is None else snapshot.value_of(quota_key)
        return QuotaAssessment(limit=limit, usage=usage, state=judge(usage=usage, limit=limit))

    async def _snapshot(self, tenant_id: int) -> UsageSnapshot | None:
        """取用量快照（未装配用量服务返回 None）。

        Args:
            tenant_id: 租户主键。

        Returns:
            UsageSnapshot | None: 用量快照。
        """
        if self._usage is None:
            return None
        return await self._usage.snapshot(tenant_id)

    async def update(self, tenant_id: int, items: Iterable[QuotaUpdate]) -> ConcurrentStableList[MsgQuota]:
        """批量写入调整（整批校验通过才写；调用方负责提交与失效缓存）。

        Args:
            tenant_id: 租户主键。
            items: 调整项。

        Returns:
            ConcurrentStableList[MsgQuota]: 写入后的行。

        Raises:
            ValueError: 未知维度 / 重复作用域 / 软限非法。
            RuntimeError: 无请求会话。
        """
        if self._repo is None:
            raise RuntimeError("配额调整需要请求级会话")
        payload = list(items)
        seen: ConcurrentStableSet[str] = ConcurrentStableSet()
        normalized: ConcurrentStableList[_NormalizedUpdate] = ConcurrentStableList()
        for item in payload:
            definition(item.quota_key)
            identity = f"{item.namespace}\x00{item.quota_key}"
            if identity in seen:
                raise ValueError(f"重复的配额作用域：{item.namespace}/{item.quota_key}")
            seen.add(identity)
            if item.limit < 0:
                raise ValueError(f"硬限不得为负：{item.limit}")
            normalized.add(_NormalizedUpdate(item=item, soft_limit=next_soft_limit(item.limit, item.soft_limit)))
        written: ConcurrentStableList[MsgQuota] = ConcurrentStableList()
        for entry in normalized:
            item = entry.item
            dimension = definition(item.quota_key)
            written.add(
                await self._repo.upsert_configured(
                    tenant_id=tenant_id,
                    namespace=item.namespace,
                    quota_key=item.quota_key,
                    limit_value=item.limit,
                    soft_limit=entry.soft_limit,
                    unit=dimension.unit,
                    period=dimension.period,
                )
            )
        return written
