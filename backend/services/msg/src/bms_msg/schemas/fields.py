"""契约共享字段（命名空间 / 主题 / 组 / 成员 + 分区号）。

数据面契约族（消息 / 消费组）共用同一批字段定义，**只在此处维护一次**：形态（长度 / 正则 / 上下界）
在契约层拒绝（`10001`），业务口径（命名空间是否已启用、主题是否存在、分区号是否越界、组是否存在）在服务层
判定（`3500xx`）。字段的 `description` 是公开契约的一部分，迁移时**逐字保留**（否则契约快照漂移）。
"""

from pydantic import Field

from bms_msg.core.config import CONSUMER_ID_PATTERN, GROUP_NAME_PATTERN

__all__ = [
    "CONSUMER_ID_FIELD",
    "GROUP_FIELD",
    "NAMESPACE_FIELD",
    "PARTITION_FIELD",
    "TOPIC_FIELD",
]

NAMESPACE_FIELD = Field(
    min_length=1, max_length=64, description="命名空间（对齐 bms module_key；须为已登记且启用的模块）"
)
"""命名空间字段（数据面请求共用；形态校验的宽度留量由服务层给 `350010`）。"""

TOPIC_FIELD = Field(min_length=1, max_length=200, description="主题名（与创建主题时的 name 同值）")
"""主题名字段（数据面请求共用）。"""

GROUP_FIELD = Field(
    min_length=1, max_length=128, pattern=GROUP_NAME_PATTERN, description="消费组名（组元数据与再均衡归 01-4）"
)
"""消费组字段（拉取 / 确认 / 成员请求共用）。"""

PARTITION_FIELD = Field(ge=0, description="分区号（必带；组内分摊与再均衡归 01-4）")
"""分区号字段（拉取 / 确认共用）。"""

CONSUMER_ID_FIELD = Field(
    min_length=1,
    max_length=128,
    pattern=CONSUMER_ID_PATTERN,
    description="成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连）",
)
"""成员标识字段（成员请求 / 拉取 / 确认共用）。"""
