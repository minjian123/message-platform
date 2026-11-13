"""死信管理面契约（06-5-2 §3）：列表 / 详情 / 重放 / 忽略 / 归档请求与响应。

口径：

- 列表与详情字段对齐 `msg_dead_letter` 表；`offset` 在契约里叫 `offset`（列名 `msg_offset` 避开保留字）；
- `confirm` 为高危写操作显式确认，缺省 `false` 由端点统一拒绝（`10001`）；
- 忽略原因去空白后必须非空、长度 1~256（形态问题由契约层转 `10001`）。
"""

from datetime import datetime
from typing import Annotated, Self

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_DICT, BaseSchema, SnowflakeIdStr
from pydantic import Field, model_validator

from bms_msg.core.config import DEFAULT_REPLAY_RATE_PER_SECOND, MAX_REPLAY_RATE_PER_SECOND
from bms_msg.models.dead_letter import DEFAULT_DISPOSITION_REASON_MAX_LENGTH

__all__ = [
    "DlqArchiveRequest",
    "DlqDetail",
    "DlqDisposeResult",
    "DlqIgnoreRequest",
    "DlqItem",
    "DlqList",
    "DlqReplayRequest",
]

_DISPOSITION_REASON_MAX = DEFAULT_DISPOSITION_REASON_MAX_LENGTH


class DlqItem(BaseSchema):
    """死信摘要（列表行）。"""

    id: SnowflakeIdStr = Field(description="死信 ID（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    partition_no: int = Field(description="分区号")
    offset: int = Field(description="原 offset（分区内序号）")
    log_id: str = Field(description="原记录 ID")
    message_id: str = Field(description="原消息 ID")
    trace_id: str | None = Field(default=None, description="链路标识")
    reason: str = Field(description="转入原因（delivery_exhausted / schema_invalid / processing_failed）")
    attempts: int = Field(description="累计投递次数")
    status: str = Field(description="状态（pending / replaying / resolved / ignored / archived）")
    failed_at: datetime = Field(description="失败时间（UTC）")
    replay_task_id: SnowflakeIdStr | None = Field(default=None, description="重放任务 ID（仅 replaying 有值）")
    disposition_reason: str | None = Field(default=None, description="处置原因")
    disposed_at: datetime | None = Field(default=None, description="处置时间（UTC）")
    created_at: datetime = Field(description="落库时间（UTC）")


class DlqDetail(DlqItem):
    """死信详情（摘要 + 失败上下文）。"""

    detail: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT,
        description="失败上下文（字段名 → 说明；消费端上报时原样保存）",
    )


class DlqList(BaseSchema):
    """死信分页响应。"""

    items: Annotated[ConcurrentStableList[DlqItem], CONTRACT_COLLECTION] = Field(description="当页死信")
    total: int = Field(description="过滤前总数")
    page: int = Field(description="页码")
    size: int = Field(description="每页条数")


class DlqReplayRequest(BaseSchema):
    """重放请求（`confirm` 必填 true；速率缺省 100 条/秒）。"""

    confirm: bool = Field(default=False, description="显式确认（高危写操作；必须为 true）")
    rate_per_second: int = Field(
        default=DEFAULT_REPLAY_RATE_PER_SECOND,
        ge=1,
        le=MAX_REPLAY_RATE_PER_SECOND,
        description=f"重放限速（条/秒，1~{MAX_REPLAY_RATE_PER_SECOND}）",
    )


class DlqIgnoreRequest(BaseSchema):
    """忽略请求（原因必填、去空白后 1~256）。"""

    confirm: bool = Field(default=False, description="显式确认（高危写操作；必须为 true）")
    reason: str = Field(min_length=1, max_length=_DISPOSITION_REASON_MAX, description="忽略原因（必填）")


class DlqArchiveRequest(BaseSchema):
    """归档请求（原因可选）。"""

    confirm: bool = Field(default=False, description="显式确认（高危写操作；必须为 true）")
    reason: str | None = Field(default=None, max_length=_DISPOSITION_REASON_MAX, description="归档原因（可选）")

    @model_validator(mode="after")
    def _normalize_reason(self) -> Self:
        """原因去空白；空白串归一为 None。

        Returns:
            Self: 处理后的请求。
        """
        if self.reason is not None:
            cleaned = self.reason.strip()
            self.reason = cleaned or None
        return self


class DlqDisposeResult(BaseSchema):
    """处置响应（状态与重放任务 ID）。"""

    id: SnowflakeIdStr = Field(description="死信 ID")
    status: str = Field(description="处置后的状态")
    replay_task_id: SnowflakeIdStr | None = Field(default=None, description="重放任务 ID（仅重放返回）")
