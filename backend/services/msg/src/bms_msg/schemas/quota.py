"""配额管理面契约（05-2 详细设计 §8）：查询 / 批量调整。

集合字段一律插入序集合类 + 契约元数据内联标注；`limit` / `hard_limit` 同值（`0` 归一为 `null`
表示不限），`soft_limit` 为生效软限（`null` 表示无软限）。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_LIST, BaseSchema
from pydantic import Field

__all__ = ["QuotaItem", "QuotaUpdateItem", "QuotaUpdateRequest", "QuotaView"]


class QuotaItem(BaseSchema):
    """一维配额的管理面视图。"""

    quota_key: str = Field(description="配额维度键")
    namespace: str = Field(description="作用域（空串 = 租户级）")
    limit: int | None = Field(description="硬限（null = 不限）")
    soft_limit: int | None = Field(description="生效软限（null = 无软限）")
    hard_limit: int | None = Field(description="硬限（与 limit 同值，平台投影字段口径）")
    unit: str = Field(description="单位（count / bytes / qps）")
    period: str = Field(description="周期（none）")
    usage: int | None = Field(description="当前用量（null = 不可用）")
    state: str = Field(description="状态（unconfigured / ok / soft_exceeded / hard_exceeded / unavailable）")
    source: str = Field(description="来源（default / configured）")
    updated_at: datetime | None = Field(description="最近更新时间（UTC；缺省行为 null）")


class QuotaView(BaseSchema):
    """配额查询响应。"""

    items: Annotated[ConcurrentStableList[QuotaItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="六维配额（固定顺序）"
    )


class QuotaUpdateItem(BaseSchema):
    """一条配额调整。"""

    quota_key: str = Field(min_length=1, max_length=32, description="配额维度键")
    namespace: str = Field(default="", max_length=64, description="作用域（空串 = 租户级）")
    limit: int = Field(ge=0, description="硬限（0 = 不限）")
    soft_limit: int | None = Field(default=None, ge=0, description="软限（null / 0 = 未设置；不得大于硬限）")


class QuotaUpdateRequest(BaseSchema):
    """批量调整请求（整批校验、整批失败）。"""

    items: Annotated[ConcurrentStableList[QuotaUpdateItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="调整项"
    )
