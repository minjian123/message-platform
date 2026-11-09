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

__all__ = ["AffectedGroupItem", "ReplayRequest", "ReplayTaskItem", "ReplayTaskPage"]


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
    confirm: bool = Field(
        default=False,
        description="显式确认（批量重放属高危写操作；必须为 true，缺省 / false 一律拒绝，防脚本绕过）",
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


class AffectedGroupItem(BaseSchema):
    """受影响消费组的影响视图（基线 / 当前 / 变化）。

    口径与 01-4 组详情同源：`lag = leo - committed`，**不夹取负值**；度量固定按基线时刻的分区集合，
    主题事后扩分区不影响可比性。
    """

    group: str = Field(description="消费组名")
    baseline_lag: int = Field(description="创建任务时的组级积压（基线）")
    current_lag: int = Field(description="当前组级积压（同一分区集合；可负）")
    delta: int = Field(description="变化量（当前 - 基线；可负）")
    captured_at: datetime = Field(description="该值所属时刻（UTC；未终结为基线时刻，已终结为快照时刻）")


class ReplayTaskItem(BaseSchema):
    """重放任务快照（建任务 / 进度 / 暂停 / 恢复 / 取消 / 列表的响应主体）。"""

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
    affected_groups: Annotated[ConcurrentStableList[AffectedGroupItem], CONTRACT_COLLECTION] | None = Field(
        default=None,
        description="受影响消费组的影响视图（列表端点为 null，仅详情返回；无订阅组为空数组）",
    )
    affected_truncated: bool = Field(
        default=False, description="影响基线是否因组数上限被截断（true 表示只展示前若干个组）"
    )


class ReplayTaskPage(BaseSchema):
    """重放任务列表分页（`total` 为 ACL 过滤前的 SQL 计数）。"""

    items: Annotated[ConcurrentStableList[ReplayTaskItem], CONTRACT_COLLECTION] = Field(
        description="当页任务（创建时间倒序；已按主题 describe 权限过滤）"
    )
    total: int = Field(description="过滤前总数（ACL 过滤只影响 items，不影响分页基数）")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")
