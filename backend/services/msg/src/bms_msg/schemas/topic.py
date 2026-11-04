"""主题请求 / 响应契约（详细设计 §8）。

集合字段一律插入序集合类 + 契约元数据内联标注（`Annotated[ConcurrentStableList[X], CONTRACT_COLLECTION]`），
禁命名泛型别名（防契约 `$defs` 漂移）；雪花 ID 由 `BaseSchema` 统一序列化为字符串。

**校验分工**：形态（长度 / 正则）在契约层；业务口径（分区数 1~1024、保留期上限、命名空间属已启用
`module_key`）在服务层——错误码要落 `350009` / `350010`，不能由请求校验统一转成 `10001`。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_LIST, BaseSchema
from pydantic import Field

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
    cleanup_policy: str = Field(default=CLEANUP_DELETE, max_length=16, description="清理策略（首期只支持 delete）")


class PartitionItem(BaseSchema):
    """分区明细（详情接口的 `partitions` 元素）。"""

    partition_no: int = Field(description="分区号（0 起）")
    leader: str = Field(description="当前 leader 标识")
    checkpoint_leo: int = Field(description="检查点 LEO")
    checkpoint_hw: int = Field(description="检查点 HW")
    epoch: int = Field(description="领导者任期（复制阶段使用）")


class TopicItem(BaseSchema):
    """主题明细（列表行 / 创建与查询响应的主体）。"""

    id: int = Field(description="主题 ID（雪花，JSON 输出为字符串）")
    tenant_id: int = Field(description="租户内部身份（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    name: str = Field(description="主题名")
    partition_count: int = Field(description="分区数")
    retention_ms: int = Field(description="保留时长（毫秒）")
    cleanup_policy: str = Field(description="清理策略")
    status: str = Field(description="状态（active / disabled）")
    created_at: datetime = Field(description="创建时间（UTC）")


class TopicDetail(TopicItem):
    """主题详情（含分区列表）。"""

    partitions: Annotated[ConcurrentStableList[PartitionItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="分区明细（按分区号升序）"
    )


class TopicPage(BaseSchema):
    """主题列表响应（页码分页：`page` 从 1 起，`size` 默认 20）。"""

    items: Annotated[ConcurrentStableList[TopicItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="当前页主题"
    )
    total: int = Field(description="符合条件的总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")
