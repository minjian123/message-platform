"""配额子包（05-2）：维度定义 / 快照缓存 / 服务。"""

from bms_msg.quota.keys import QUOTA_DEFINITIONS, QuotaDefinition, definition, effective_soft, next_soft_limit

__all__ = [
    "QUOTA_DEFINITIONS",
    "QuotaDefinition",
    "definition",
    "effective_soft",
    "next_soft_limit",
]
