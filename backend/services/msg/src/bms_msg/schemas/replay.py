"""重放任务契约（01-6 详细设计 §8.1）：建任务请求 + 任务快照。

**范围条件是扁平字段**（已确认）：`offset_from` / `offset_to` / `time_from` / `time_to` / `key` 一层平铺，
新增条件只加可选字段、不改既有结构（契约只增不改）。区间统一**左闭右开**，跨类条件相与。

**校验分工**：形态（长度 / 上下界 / 区间次序）在契约层（`10001`）；业务口径（命名空间是否已启用、
主题是否存在、分区号是否越界）在服务层（`3500xx`）——错误码不能由请求校验统一转成 `10001`。
"""

from datetime import datetime
from typing import Annotated, Self

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, BaseSchema, SnowflakeIdStr
from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from bms_msg.core.config import (
    DEFAULT_REPLAY_RATE_PER_SECOND,
    MAX_REPLAY_RATE_PER_SECOND,
    MESSAGE_KEY_MAX_LENGTH,
)
from bms_msg.schemas.fields import NAMESPACE_FIELD, TOPIC_FIELD

__all__ = ["ReplayRequest", "ReplayTaskItem"]


class ReplayRequest(BaseSchema):
    """建重放任务请求（`tenant` 不在请求体：由基座解析链注入）。"""

    namespace: str = NAMESPACE_FIELD
    topic: str = TOPIC_FIELD
    partitions: Annotated[ConcurrentStableList[int], CONTRACT_COLLECTION] | None = Field(
        default=None,
        min_length=1,
        description="分区号列表（缺省 null = 全部分区；创建时固化为快照，主题扩分区不影响已建任务）",
    )
    offset_from: int | None = Field(default=None, ge=0, description="起始 offset（含）；null 不限")
    offset_to: int | None = Field(default=None, ge=0, description="结束 offset（不含）；null 不限")
    time_from: datetime | None = Field(default=None, description="起始发布时间（含，UTC）；null 不限")
    time_to: datetime | None = Field(default=None, description="结束发布时间（不含，UTC）；null 不限")
    key: str | None = Field(default=None, max_length=MESSAGE_KEY_MAX_LENGTH, description="分区键精确匹配；null 不过滤")
    rate_per_second: int = Field(
        default=DEFAULT_REPLAY_RATE_PER_SECOND,
        ge=1,
        le=MAX_REPLAY_RATE_PER_SECOND,
        description=f"限速（条/秒，1~{MAX_REPLAY_RATE_PER_SECOND}）",
    )

    @model_validator(mode="after")
    def _ensure_ranges(self) -> Self:
        """校验区间次序（左闭右开，结束必须大于起始）。

        用 `PydanticCustomError` 而不是裸 `ValueError`：裸异常会把 `ValueError` 实例塞进错误的 `ctx`，
        统一错误响应序列化时报 `Unable to serialize unknown type`（实测）。

        Returns:
            Self: 校验通过的请求。

        Raises:
            PydanticCustomError: 区间次序非法（由契约层转 `10001`）。
        """
        if self.offset_from is not None and self.offset_to is not None and self.offset_to <= self.offset_from:
            raise PydanticCustomError(
                "replay_offset_range",
                "offset 区间非法（结束须大于起始）：[{start}, {end})",
                {"start": self.offset_from, "end": self.offset_to},
            )
        if self.time_from is not None and self.time_to is not None and self.time_to <= self.time_from:
            raise PydanticCustomError(
                "replay_time_range",
                "时间区间非法（结束须大于起始）：[{start}, {end})",
                {"start": self.time_from.isoformat(), "end": self.time_to.isoformat()},
            )
        return self


class ReplayTaskItem(BaseSchema):
    """重放任务快照（建任务 / 进度 / 暂停 / 恢复 / 取消的响应主体）。"""

    task_id: SnowflakeIdStr = Field(description="任务 ID（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名（创建时快照，主题停用后仍可展示）")
    partitions: Annotated[ConcurrentStableList[int], CONTRACT_COLLECTION] = Field(
        description="分区号列表（创建时固化的解析结果）"
    )
    offset_from: int | None = Field(description="起始 offset（含）；null 不限")
    offset_to: int | None = Field(description="结束 offset（不含）；null 不限")
    time_from: datetime | None = Field(description="起始发布时间（含，UTC）；null 不限")
    time_to: datetime | None = Field(description="结束发布时间（不含，UTC）；null 不限")
    key: str | None = Field(description="分区键精确匹配条件；null 不过滤")
    rate_per_second: int = Field(description="限速（条/秒）")
    status: str = Field(description="状态（pending / running / paused / cancelled / completed / failed）")
    total: int = Field(description="选中总条数（进度分母；建任务时按条件精确计数）")
    replayed: int = Field(description="已写入条数（进度分子）")
    log_id_first: str | None = Field(description="首条成功写入的记录 ID（审计可凭它定位）")
    log_id_last: str | None = Field(description="末条成功写入的记录 ID（审计可凭它定位）")
    created_at: datetime = Field(description="创建时间（UTC）")
    finished_at: datetime | None = Field(description="终态时刻（UTC）；未终结为 null")
    error_msg: str | None = Field(description="失败原因；非 failed 为 null")
