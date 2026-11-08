"""配额快照缓存（进程级）：TTL + 显式失效，缺省值兜底。

- 键为 `tenant_id`，值为不可变 `QuotaLimit` 元组；缓存未命中 / 过期时按缺省值回落，
  **不因取数失败而拒绝业务**（配额行缺失按内置缺省生效）。
- 多实例一致性归 8B_05：直改库最多滞后 TTL（60s），管理面调整走显式失效。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from bms_msg.core.config import (
    QUOTA_CACHE_TTL_MS,
    QUOTA_ORIGIN_DEFAULT,
)
from bms_msg.models.quota import MsgQuota
from bms_msg.quota.keys import QUOTA_DEFINITIONS, QuotaDefinition, effective_soft

__all__ = ["QuotaCache", "QuotaLimit"]


@dataclass(frozen=True)
class QuotaLimit:
    """一维配额的生效限额（合并缺省 / 覆盖后的结果）。"""

    key: str
    """配额维度键。"""

    namespace: str
    """生效作用域（空串 = 租户级）。"""

    limit: int
    """硬限（0 = 不限）。"""

    soft_limit: int
    """显式软限（0 = 未设置）。"""

    unit: str
    """单位（count / bytes / qps）。"""

    period: str
    """周期（none）。"""

    origin: str
    """来源（default / configured）。"""

    updated_at: datetime | None
    """最近更新时间（缺省行 / 未落库为 None）。"""

    @property
    def effective_soft(self) -> int:
        """生效软限（显式值优先，否则硬限 × 80%）。

        Returns:
            int: 软限；0 表示无软限。
        """
        return effective_soft(self.limit, self.soft_limit)

    @property
    def hard_limit(self) -> int | None:
        """硬限（0 归一为 None，表示不限）。

        Returns:
            int | None: 硬限。
        """
        return self.limit or None

    @property
    def soft_limit_or_none(self) -> int | None:
        """软限（0 归一为 None）。

        Returns:
            int | None: 软限。
        """
        soft = self.effective_soft
        return soft or None


def _from_row(row: MsgQuota) -> QuotaLimit:
    """ORM 行 → 不可变快照。

    Args:
        row: 配额行。

    Returns:
        QuotaLimit: 快照。
    """
    return QuotaLimit(
        key=row.quota_key,
        namespace=row.namespace,
        limit=int(row.limit_value),
        soft_limit=int(row.soft_limit),
        unit=row.unit,
        period=row.period,
        origin=row.origin,
        updated_at=row.updated_at,
    )


@dataclass(frozen=True)
class _CacheEntry:
    """租户快照缓存条目。"""

    loaded_at: float
    """载入时刻（`monotonic` 秒）。"""

    limits: ConcurrentStableFrozenList[QuotaLimit]
    """配额快照（不可变）。"""


def _default_of(definition: QuotaDefinition) -> QuotaLimit:
    """缺省限额（未落库 / 未配置）。

    Args:
        definition: 维度定义。

    Returns:
        QuotaLimit: 缺省快照。
    """
    return QuotaLimit(
        key=definition.key,
        namespace="",
        limit=definition.default,
        soft_limit=0,
        unit=definition.unit,
        period=definition.period,
        origin=QUOTA_ORIGIN_DEFAULT,
        updated_at=None,
    )


class QuotaCache:
    """进程级配额快照缓存（按租户；TTL + 显式失效）。"""

    def __init__(self, *, ttl_ms: int = QUOTA_CACHE_TTL_MS) -> None:
        """初始化。

        Args:
            ttl_ms: 快照有效期（毫秒）。
        """
        self._ttl_ms = ttl_ms
        self._entries: ConcurrentStableDict[int, _CacheEntry] = ConcurrentStableDict()
        self._initialized: ConcurrentStableSet[int] = ConcurrentStableSet()

    def get(self, tenant_id: int) -> ConcurrentStableFrozenList[QuotaLimit] | None:
        """取未过期的租户快照。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableFrozenList[QuotaLimit] | None: 快照；未命中 / 过期返回 None。
        """
        entry = self._entries.get(int(tenant_id))
        if entry is None:
            return None
        if (time.monotonic() - entry.loaded_at) * 1000 >= self._ttl_ms:
            self._entries.pop(int(tenant_id), None)
            return None
        return entry.limits

    def put(self, tenant_id: int, limits: ConcurrentStableList[MsgQuota]) -> ConcurrentStableFrozenList[QuotaLimit]:
        """写入租户快照。

        Args:
            tenant_id: 租户主键。
            limits: 配额行。

        Returns:
            ConcurrentStableFrozenList[QuotaLimit]: 写入后的快照。
        """
        snapshot = ConcurrentStableFrozenList(_from_row(row) for row in limits)
        self._entries[int(tenant_id)] = _CacheEntry(loaded_at=time.monotonic(), limits=snapshot)
        return snapshot

    def invalidate(self, tenant_id: int) -> None:
        """失效某租户快照（管理面调整 / 租户清理）。

        Args:
            tenant_id: 租户主键。
        """
        self._entries.pop(int(tenant_id), None)
        self._initialized.discard(int(tenant_id))

    def is_initialized(self, tenant_id: int) -> bool:
        """该租户缺省行是否已确认齐全。

        Args:
            tenant_id: 租户主键。

        Returns:
            bool: 已确认返回 True。
        """
        return int(tenant_id) in self._initialized

    def mark_initialized(self, tenant_id: int) -> None:
        """标记该租户缺省行已齐全。

        Args:
            tenant_id: 租户主键。
        """
        self._initialized.add(int(tenant_id))

    def resolve(self, tenant_id: int, namespace: str, key: str) -> QuotaLimit:
        """取一维生效限额（命名空间覆盖 → 租户级 → 内置缺省）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。
            key: 配额维度键。

        Returns:
            QuotaLimit: 生效限额。

        Raises:
            KeyError: 未知维度键。
        """
        definition = _definition_of(key)
        snapshot = self.get(tenant_id)
        if snapshot is None:
            return _default_of(definition)
        if definition.scope == "tenant":
            candidates = [item for item in snapshot if item.key == key and item.namespace == ""]
        else:
            candidates = [item for item in snapshot if item.key == key and item.namespace == namespace]
            if not candidates:
                candidates = [item for item in snapshot if item.key == key and item.namespace == ""]
        if not candidates:
            return _default_of(definition)
        return candidates[0]

    def resolve_all(self, tenant_id: int, namespace: str = "") -> ConcurrentStableList[QuotaLimit]:
        """取六维生效限额（固定定义顺序）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间（空串 = 租户级）。

        Returns:
            ConcurrentStableList[QuotaLimit]: 六维限额。
        """
        return ConcurrentStableList(
            self.resolve(tenant_id, namespace, definition.key) for definition in QUOTA_DEFINITIONS
        )


def _definition_of(key: str) -> QuotaDefinition:
    """按维度键取定义（避免与 `keys.definition` 的异常口径分叉）。

    Args:
        key: 维度键。

    Returns:
        QuotaDefinition: 定义。

    Raises:
        KeyError: 未知维度键。
    """
    for definition in QUOTA_DEFINITIONS:
        if definition.key == key:
            return definition
    raise KeyError(key)
