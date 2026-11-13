"""消费组契约（01-4 详细设计 §7）：成员请求 / 分配结果 / 离开结果 / 组只读视图。

集合字段一律插入序集合类 + 契约元数据内联标注（`Annotated[ConcurrentStable*, CONTRACT_COLLECTION]`），
禁命名泛型别名（防契约 `$defs` 漂移）。

**字段口径**：`generation` 是客户端判断「手上分配是否过期」的唯一依据；`partitions` 为该成员的当前分配
（空闲成员为空数组）；`heartbeat_interval_ms` / `session_timeout_ms` 由服务端下发，避免客户端硬编码节奏
与服务端调参漂移。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, BaseSchema
from pydantic import Field

from bms_msg.schemas.fields import CONSUMER_ID_FIELD, GROUP_FIELD, NAMESPACE_FIELD, TOPIC_FIELD

__all__ = [
    "GroupDeleteResult",
    "GroupDetail",
    "GroupItem",
    "GroupMemberItem",
    "GroupPage",
    "GroupPartitionItem",
    "LeaveResult",
    "MemberAssignment",
    "MemberRequest",
]


class MemberRequest(BaseSchema):
    """成员请求（`join` / `heartbeat` / `leave` 共用：组按主题维度建）。"""

    group: str = GROUP_FIELD
    namespace: str = NAMESPACE_FIELD
    topic: str = TOPIC_FIELD
    consumer_id: str = CONSUMER_ID_FIELD


class MemberAssignment(BaseSchema):
    """成员分配（`join` / `heartbeat` 响应：客户端按 `partitions` 拉取，`generation` 变了即换分配）。"""

    group: str = Field(description="消费组名")
    topic: str = Field(description="主题名")
    consumer_id: str = Field(description="成员标识")
    generation: int = Field(description="世代号（成员集合每变化一次 +1；重连不递增）")
    state: str = Field(description="组状态（empty / preparing_rebalance / stable / dead）")
    partitions: Annotated[ConcurrentStableList[int], CONTRACT_COLLECTION] = Field(
        description="本成员当前持有的分区号（升序；空闲成员为空数组）"
    )
    member_count: int = Field(description="组内当前活跃成员数")
    heartbeat_interval_ms: int = Field(description="建议心跳间隔（毫秒；服务端声明，客户端据此保活）")
    session_timeout_ms: int = Field(description="会话超时（毫秒；超时未心跳即判定成员失效）")


class LeaveResult(BaseSchema):
    """离开结果（`leave` 幂等：未注册成员也返回成功）。"""

    group: str = Field(description="消费组名")
    topic: str = Field(description="主题名")
    consumer_id: str = Field(description="成员标识")
    generation: int = Field(description="离开后的世代号")
    state: str = Field(description="离开后的组状态（无成员即 empty）")


class GroupDeleteResult(BaseSchema):
    """组删除结果（默认只删组与成员，位点保留）。"""

    group: str = Field(description="消费组名")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    deleted: bool = Field(description="组与成员是否已删除（组不存在为 false）")
    offsets_purged: int = Field(description="本次清理的位点条目数（默认 0 = 位点保留）")


class GroupItem(BaseSchema):
    """组列表行（管理面只读视图；积压只进详情）。"""

    group: str = Field(description="消费组名")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    state: str = Field(description="组状态（empty / preparing_rebalance / stable / dead）")
    generation: int = Field(description="世代号")
    member_count: int = Field(description="活跃成员数")
    assigned_partitions: int = Field(description="全组已分配的分区总数")


class GroupPage(BaseSchema):
    """组列表分页（`total` 为过滤前总数）。"""

    items: Annotated[ConcurrentStableList[GroupItem], CONTRACT_COLLECTION] = Field(description="当页组（组名升序）")
    total: int = Field(description="过滤前总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")


class GroupMemberItem(BaseSchema):
    """组成员明细。"""

    consumer_id: str = Field(description="成员标识")
    joined_at: datetime = Field(description="加入时间（UTC；重连不刷新）")
    last_seen: datetime = Field(description="最后心跳时间（UTC）")
    partitions: Annotated[ConcurrentStableList[int], CONTRACT_COLLECTION] = Field(
        description="当前持有的分区号（升序）"
    )


class GroupPartitionItem(BaseSchema):
    """组内分区位点与积压。"""

    partition: int = Field(description="分区号")
    owner: str | None = Field(description="当前持有者（无成员或未分配为 null）")
    committed: int = Field(description="已提交位点（下一条待消费的 offset）")
    leo: int = Field(description="分区日志末端位点")
    lag: int = Field(description="积压（LEO - 已提交位点）")


class GroupDetail(BaseSchema):
    """组详情（状态 / 世代 / 成员 / 分配 / 各分区位点与积压）。"""

    group: str = Field(description="消费组名")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    state: str = Field(description="组状态（empty / preparing_rebalance / stable / dead）")
    generation: int = Field(description="世代号")
    partition_count: int = Field(description="主题分区数")
    members: Annotated[ConcurrentStableList[GroupMemberItem], CONTRACT_COLLECTION] = Field(
        description="成员列表（按 consumer_id 升序）"
    )
    partitions: Annotated[ConcurrentStableList[GroupPartitionItem], CONTRACT_COLLECTION] = Field(
        description="各分区位点与积压（按分区号升序）"
    )
