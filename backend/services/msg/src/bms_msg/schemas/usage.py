"""租户用量管理面契约（06-8 详细设计 §4）：四列元数据 + 当前租户一行用量。

**同源约束**：`columns` 顺序与 `label` 取自 `USAGE_COLUMNS`（与 05-1 投影响应同一常量），
`items` 的四键与 `USAGE_COLUMNS` 的键集合闭合（由 `test_usage_columns.py` 断言），
本契约不引入任何新的统计口径。

**为什么是固定四字段而非动态字典**：契约可生成前端类型、字段集合可被断言、
`storage_bytes` 的「采集失败即不可用」语义能落成 `int | None`。
"""

from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_LIST, BaseSchema
from pydantic import Field

__all__ = ["TenantUsageRow", "TenantUsageView", "UsageColumnView"]


class UsageColumnView(BaseSchema):
    """一列的元数据（键与展示名）。"""

    key: str = Field(description="列键（＝ `sys_field.field_key`）")
    label: str = Field(description="展示名（＝ `sys_field.name`）")


class TenantUsageRow(BaseSchema):
    """当前租户的一行用量（四列值）。"""

    topic_count: int = Field(description="主题数（含停用主题）")
    partition_count: int = Field(description="分区数（全部主题合计）")
    storage_bytes: int | None = Field(description="消息存储字节数；目录扫描失败为 null")
    delivered_today: int = Field(description="今日投递条数（进程内 UTC 日计数）")


class TenantUsageView(BaseSchema):
    """租户用量响应（列元数据 + 一行值）。"""

    columns: Annotated[ConcurrentStableList[UsageColumnView], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="列元数据（顺序即展示顺序）"
    )
    items: Annotated[ConcurrentStableList[TenantUsageRow], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="用量行（恒为一行）"
    )
    total: int = Field(description="总行数（恒定 1）")
