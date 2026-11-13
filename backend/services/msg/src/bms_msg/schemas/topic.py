"""主题请求 / 响应契约（详细设计 §8）。

集合字段一律插入序集合类 + 契约元数据内联标注（`Annotated[ConcurrentStableList[X], CONTRACT_COLLECTION]`），
禁命名泛型别名（防契约 `$defs` 漂移）；雪花 ID 由 `BaseSchema` 统一序列化为字符串。

**校验分工**：形态（长度 / 正则）在契约层；业务口径（分区数 1~1024、保留期上限、命名空间属已启用
`module_key`）在服务层——错误码要落 `350009` / `350010`，不能由请求校验统一转成 `10001`。
"""

from datetime import datetime
from typing import Annotated, Literal

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_LIST, BaseSchema, SnowflakeIdStr
from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from bms_msg.core.config import DEFAULT_RETENTION_MS
from bms_msg.models.topic import CLEANUP_DELETE


class TopicCreateRequest(BaseSchema):
    """创建主题请求（`tenant_id` **不在请求体**：由基座解析链注入，防客户端自报冒充）。"""

    namespace: str = Field(
        min_length=1, max_length=64, description="命名空间（对齐 bms module_key；须为已登记且启用的模块）"
    )
    name: str = Field(min_length=1, max_length=200, description="主题名（建议 {域}.{对象}.{动作}）")
    partition_count: int = Field(description="分区数（1~1024；只增不减）")
    retention_ms: int = Field(default=DEFAULT_RETENTION_MS, description="保留时长（毫秒；默认 30 天，上限 366 天）")
    retention_bytes: int = Field(
        default=0, ge=0, description="容量上限（字节；0 = 不限，缺省不限）。超限从最老段整段删除"
    )
    cleanup_policy: Literal["delete"] = Field(
        default=CLEANUP_DELETE, description="清理策略（首期只支持 delete；其它值按参数非法拒绝）"
    )


class PartitionItem(BaseSchema):
    """分区明细（详情接口的 `partitions` 元素）。"""

    partition_no: int = Field(description="分区号（0 起）")
    leader: str = Field(description="当前 leader 标识")
    checkpoint_leo: int = Field(description="检查点 LEO")
    checkpoint_hw: int = Field(description="检查点 HW")
    epoch: int = Field(description="领导者任期（复制阶段使用）")
    switch_state: str = Field(default="idle", description="切换状态（idle / switching；02-5）")
    last_switch_at: datetime | None = Field(default=None, description="最近一次成功切换时刻（UTC）")
    last_switch_by: str | None = Field(default=None, description="最近一次切换触发人")
    last_switch_epoch: int | None = Field(default=None, description="最近一次切换后的 epoch")
    last_switch_result: str | None = Field(default=None, description="最近一次切换结果（truncated / retained / noop）")
    last_truncate_to: int | None = Field(default=None, description="最近一次切换截断后的 LEO；null = 未截断")


class TopicItem(BaseSchema):
    """主题明细（列表行 / 创建与查询响应的主体）。"""

    id: SnowflakeIdStr = Field(description="主题 ID（雪花，JSON 输出为字符串）")
    tenant_id: SnowflakeIdStr = Field(description="租户内部身份（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    name: str = Field(description="主题名")
    partition_count: int = Field(description="分区数")
    retention_ms: int = Field(description="保留时长（毫秒）")
    retention_bytes: int = Field(description="容量上限（字节；0 = 不限）")
    cleanup_policy: str = Field(description="清理策略")
    status: str = Field(description="状态（active / disabled / deleting）")
    created_at: datetime = Field(description="创建时间（UTC）")


class CleanupProgress(BaseSchema):
    """删主题清理进度（`deleting` 主题的详情里出现；其余状态为 null）。"""

    status: str = Field(description="台账状态（pending / running / completed）")
    done_steps: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST,
        description="已完成步骤键（messages / delay / offsets / groups / dlq / runtime / metadata）",
    )
    attempts: int = Field(description="已执行轮次（连续失败 ≥3 会输出告警日志）")
    started_at: datetime | None = Field(description="首轮开始时刻（UTC；未开始为 null）")
    finished_at: datetime | None = Field(description="完成时刻（UTC；未完成为 null）")
    last_error: str | None = Field(description="最后一轮失败原因（未失败为 null）")


class TopicDetail(TopicItem):
    """主题详情（含分区列表；`deleting` 时含清理进度）。"""

    metadata_version: int = Field(default=0, description="元数据版本（= max(partition.epoch)）；客户端刷新判据")
    partitions: Annotated[ConcurrentStableList[PartitionItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="分区明细（按分区号升序）"
    )
    cleanup: CleanupProgress | None = Field(
        default=None, description="删主题清理进度（仅 deleting 状态有值，其余为 null）"
    )


class TopicPage(BaseSchema):
    """主题列表响应（页码分页：`page` 从 1 起，`size` 默认 20）。"""

    items: Annotated[ConcurrentStableList[TopicItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="当前页主题"
    )
    total: int = Field(description="符合条件的总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")


class DeleteImpact(BaseSchema):
    """删主题影响预检（二次确认弹窗的「影响范围」唯一数据源）。"""

    partition_count: int = Field(description="分区数")
    message_total: int = Field(description="可读消息总量（Σ(LEO − 起点)）")
    group_count: int = Field(description="该主题的消费组数")
    delayed_total: int = Field(description="延迟待投条数")
    dlq_total: int = Field(description="死信条数")


class TopicCleanupView(BaseSchema):
    """删主题受理结果（**不是**「已删除」：主题进入 `deleting`，清理异步进行）。"""

    topic_id: SnowflakeIdStr = Field(description="主题 ID（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    status: str = Field(description="主题状态（受理后恒为 deleting）")
    cleanup: CleanupProgress = Field(description="清理进度")


class TopicRetentionUpdate(BaseSchema):
    """改保留期请求（两项均可选、至少给一项；范围校验在服务层，越界为 `350009`）。"""

    retention_ms: int | None = Field(default=None, description="保留时长（毫秒；null 表示不改）")
    retention_bytes: int | None = Field(default=None, description="容量上限（字节；0 = 不限；null 表示不改）")

    @model_validator(mode="after")
    def _ensure_any_field(self) -> TopicRetentionUpdate:
        """校验至少给出一项（空 PATCH 会静默成功，属契约层问题）。

        Returns:
            TopicRetentionUpdate: 校验通过的请求。

        Raises:
            PydanticCustomError: 两项均为空（由契约层转 `10001`）。
        """
        if self.retention_ms is None and self.retention_bytes is None:
            raise PydanticCustomError("retention_empty", "至少给出 retention_ms 与 retention_bytes 之一")
        return self


class TopicPartitionExpandRequest(BaseSchema):
    """扩分区请求（**只增不减**：目标值不大于现状即拒绝，`350009`）。"""

    partition_count: int = Field(ge=1, description="目标分区数（须大于当前分区数且在上界内）")
