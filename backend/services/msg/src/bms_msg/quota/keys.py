"""消息维度配额键与缺省口径（05-2）：六维定义、单位 / 周期、缺省值与环境变量覆盖。

**唯一事实源**：配额键、展示名、单位、周期、作用域与缺省值只在本模块定义；表模型、服务、
管理面端点、平台投影声明与用例全部引用这里，不在别处复制六维清单。

**作用域**：`tenant` 只查租户级行；`namespace` / `tenant+namespace` 先查命名空间级行，
未命中回落租户级行，再未命中用缺省值。`namespace=""` 表示租户级。
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableFrozenList

from bms_msg.core.config import (
    QUOTA_DEFAULT_CONNECTION_COUNT,
    QUOTA_DEFAULT_CONNECTION_COUNT_ENV,
    QUOTA_DEFAULT_GROUP_COUNT,
    QUOTA_DEFAULT_GROUP_COUNT_ENV,
    QUOTA_DEFAULT_PRODUCE_RATE,
    QUOTA_DEFAULT_PRODUCE_RATE_ENV,
    QUOTA_DEFAULT_STORAGE_BYTES,
    QUOTA_DEFAULT_STORAGE_BYTES_ENV,
    QUOTA_DEFAULT_TOPIC_COUNT,
    QUOTA_DEFAULT_TOPIC_COUNT_ENV,
    QUOTA_DEFAULT_UNACKED_COUNT,
    QUOTA_DEFAULT_UNACKED_COUNT_ENV,
    QUOTA_SOFT_RATIO_PERCENT,
)

__all__ = [
    "QUOTA_DEFINITIONS",
    "QuotaDefinition",
    "definition",
    "effective_soft",
    "next_soft_limit",
]

SCOPE_TENANT = "tenant"
"""作用域：仅租户级。"""

SCOPE_NAMESPACE = "namespace"
"""作用域：命名空间级覆盖 + 租户级回落。"""


@dataclass(frozen=True)
class QuotaDefinition:
    """一个配额维度的静态定义。"""

    key: str
    """维度键（表列 / 投影 / 管理面共用）。"""

    label: str
    """中文展示名（管理台与投影文案）。"""

    unit: str
    """单位：`count` / `bytes` / `qps`。"""

    period: str
    """周期：本期六维均为 `none`（瞬时）。"""

    scope: str
    """作用域：`tenant` / `namespace`。"""

    default_env: str
    """缺省值环境变量名。"""

    default_value: int
    """缺省值（硬限；`0` = 不限）。"""

    @property
    def default(self) -> int:
        """取当前生效缺省值（环境变量 > 模块常量）。

        Returns:
            int: 非负整数；非法 / 空值回落模块常量。
        """
        raw = os.environ.get(self.default_env)
        if raw is None or raw == "":
            return self.default_value
        try:
            value = int(raw)
        except TypeError, ValueError:
            return self.default_value
        return value if value >= 0 else self.default_value


QUOTA_DEFINITIONS: ConcurrentStableFrozenList[QuotaDefinition] = ConcurrentStableFrozenList(
    (
        QuotaDefinition(
            key="topic_count",
            label="主题数",
            unit="count",
            period="none",
            scope=SCOPE_NAMESPACE,
            default_env=QUOTA_DEFAULT_TOPIC_COUNT_ENV,
            default_value=QUOTA_DEFAULT_TOPIC_COUNT,
        ),
        QuotaDefinition(
            key="group_count",
            label="消费组数",
            unit="count",
            period="none",
            scope=SCOPE_TENANT,
            default_env=QUOTA_DEFAULT_GROUP_COUNT_ENV,
            default_value=QUOTA_DEFAULT_GROUP_COUNT,
        ),
        QuotaDefinition(
            key="produce_rate",
            label="生产速率",
            unit="qps",
            period="none",
            scope=SCOPE_NAMESPACE,
            default_env=QUOTA_DEFAULT_PRODUCE_RATE_ENV,
            default_value=QUOTA_DEFAULT_PRODUCE_RATE,
        ),
        QuotaDefinition(
            key="storage_bytes",
            label="消息存储",
            unit="bytes",
            period="none",
            scope=SCOPE_TENANT,
            default_env=QUOTA_DEFAULT_STORAGE_BYTES_ENV,
            default_value=QUOTA_DEFAULT_STORAGE_BYTES,
        ),
        QuotaDefinition(
            key="unacked_count",
            label="未确认数",
            unit="count",
            period="none",
            scope=SCOPE_TENANT,
            default_env=QUOTA_DEFAULT_UNACKED_COUNT_ENV,
            default_value=QUOTA_DEFAULT_UNACKED_COUNT,
        ),
        QuotaDefinition(
            key="connection_count",
            label="连接数",
            unit="count",
            period="none",
            scope=SCOPE_TENANT,
            default_env=QUOTA_DEFAULT_CONNECTION_COUNT_ENV,
            default_value=QUOTA_DEFAULT_CONNECTION_COUNT,
        ),
    )
)
"""六维配额定义（固定顺序：与表、投影、管理面一致）。"""


def definition(key: str) -> QuotaDefinition:
    """按维度键取定义。

    Args:
        key: 维度键。

    Returns:
        QuotaDefinition: 定义。

    Raises:
        KeyError: 未知维度键（调用方按参数错误处理）。
    """
    for item in QUOTA_DEFINITIONS:
        if item.key == key:
            return item
    raise KeyError(key)


def effective_soft(limit: int, soft_limit: int) -> int:
    """取生效软限：显式值优先；未设置时按硬限 × 80%（向下取整）；硬限不限则为 0。

    Args:
        limit: 硬限（`0` = 不限）。
        soft_limit: 显式软限（`0` = 未设置）。

    Returns:
        int: 生效软限；`0` 表示无软限。
    """
    if soft_limit > 0:
        return soft_limit
    if limit > 0:
        return max(limit * QUOTA_SOFT_RATIO_PERCENT // 100, 1)
    return 0


def next_soft_limit(limit: int, soft_limit: int | None) -> int:
    """校验并归一软限（管理面调整用）。

    Args:
        limit: 硬限（`0` = 不限）。
        soft_limit: 请求给的软限；`None` 表示不设置。

    Returns:
        int: 归一后的软限（`0` = 未设置）。

    Raises:
        ValueError: 软限为负、或硬限不限时给了软限、或软限大于硬限。
    """
    if soft_limit is None or soft_limit == 0:
        return 0
    if soft_limit < 0:
        raise ValueError(f"软限不得为负：{soft_limit}")
    if limit == 0:
        raise ValueError("硬限不限时不允许设置软限")
    if soft_limit > limit:
        raise ValueError(f"软限不得大于硬限：{soft_limit} > {limit}")
    return soft_limit
