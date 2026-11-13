"""延迟中消息的只读视图契约（01-5 详细设计 §7.3）：待投列表 + 分页。

**只读**：不做改期 / 取消 / 删除；06 域在此基础上扩筛选与指标看板。
`remaining_ms` 由服务端计算（客户端不必比对本地时钟，避免时区与时钟漂移）。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, BaseSchema
from pydantic import Field

__all__ = ["DelayDiscardRequest", "DelayDiscardResult", "DelayedItem", "DelayedPage"]


class DelayedItem(BaseSchema):
    """一条延迟中消息（尚未进入日志：没有 offset，只有到期时刻）。"""

    log_id: str = Field(description="记录 ID（发布时分配；转投沿用，审计条目可凭它定位）")
    message_id: str = Field(description="消息 ID（重投 / 转投 / 重放不变）")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名（主题删除后仍可展示，便于排障）")
    partition: int = Field(description="发布时锁定的分区号（扩分区后不漂移）")
    level: str = Field(description="延迟档位（1s / 5s / 10s / 30s / 1m / 5m / 10m / 30m / 60m）")
    deliver_at: datetime = Field(description="到期时刻（UTC）")
    remaining_ms: int = Field(description="剩余时间（毫秒；已到期未转投为 0）")


class DelayDiscardRequest(BaseSchema):
    """丢弃延迟记录请求。"""

    reason: str = Field(min_length=1, max_length=256, description="处置原因（必填）")


class DelayDiscardResult(BaseSchema):
    """丢弃延迟记录结果。"""

    log_id: str = Field(description="记录 ID")
    discarded: bool = Field(description="是否丢弃成功")


class DelayedPage(BaseSchema):
    """待投列表分页（`total` 为过滤前总数）。"""

    items: Annotated[ConcurrentStableList[DelayedItem], CONTRACT_COLLECTION] = Field(
        description="当页延迟消息（到期时刻升序，最急的在前）"
    )
    total: int = Field(description="过滤前总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")
