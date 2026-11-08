"""主题 ACL 请求 / 响应契约（05-3）。

- 请求体不接收 `tenant_id`（由基座解析链注入，防客户端自报冒充）；
- 形态校验在契约层（长度 / 非空），业务口径（命名空间已启用、主题存在、动作 / 效果 / 主体合法）
  在服务层，错误码落 `350010` / `350001` / `10001`；
- 雪花 ID 由 `BaseSchema` 统一序列化为字符串。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, CONTRACT_STABLE_LIST, BaseSchema, SnowflakeIdStr
from pydantic import Field


class AclRuleRequest(BaseSchema):
    """一条 ACL 规则（授予 / 更新）。"""

    namespace: str = Field(min_length=1, max_length=64, description="命名空间（对齐 bms module_key）")
    topic: str = Field(min_length=1, max_length=200, description="主题名（须已存在且属于当前租户）")
    principal_type: str = Field(max_length=16, description="主体类型（user / role）")
    principal_key: str = Field(max_length=64, description="主体标识（用户 / 角色主键的十进制字符串）")
    action: str = Field(max_length=16, description="动作（publish / subscribe / describe / manage）")
    effect: str = Field(max_length=8, description="效果（allow / deny）")


class AclBatchRequest(BaseSchema):
    """批量授予 / 更新请求（整批校验、整批成功 / 失败）。"""

    items: Annotated[ConcurrentStableList[AclRuleRequest], CONTRACT_COLLECTION] = Field(
        min_length=1, default_factory=CONTRACT_STABLE_LIST, description="规则清单"
    )


class AclItem(BaseSchema):
    """ACL 规则明细。"""

    acl_id: SnowflakeIdStr = Field(description="规则 ID（雪花，JSON 输出为字符串）")
    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    principal_type: str = Field(description="主体类型")
    principal_key: str = Field(description="主体标识")
    action: str = Field(description="动作")
    effect: str = Field(description="效果")
    status: str = Field(description="状态（enabled / disabled）")
    updated_at: datetime | None = Field(description="最近更新时间（UTC）")


class AclPage(BaseSchema):
    """ACL 列表响应（页码分页）。"""

    items: Annotated[ConcurrentStableList[AclItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="当页规则"
    )
    total: int = Field(description="符合条件的总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")


class AclBatchResult(BaseSchema):
    """批量授予 / 更新结果。"""

    items: Annotated[ConcurrentStableList[AclItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="写入后的规则（与入参同序）"
    )
    changed: Annotated[ConcurrentStableList[SnowflakeIdStr], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="实际发生变更的规则 ID（幂等未变更为空）"
    )


class AclRevokeResult(BaseSchema):
    """收回结果（幂等）。"""

    acl_id: SnowflakeIdStr = Field(description="规则 ID（雪花，JSON 输出为字符串）")
    changed: bool = Field(description="是否实际发生停用（已停用 / 不存在为 false）")
