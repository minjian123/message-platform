"""位点重置契约（06-2，复用 02-3 `OffsetResetter` 端口）。

**定位用命名空间 + 主题名而不是 `topic_id`**：管理面前端与深链都拿不到雪花 ID 的稳定来源，
且「按名字解析主题」天然带上租户归属校验（`350001`），不会出现「传了别人家的 `topic_id`」。

`confirm` 必填且必须为 true：高危操作的前端确认只作提示，**后端口径才是判据**（防脚本绕过）。
"""

from bms_core.schemas.base import BaseSchema
from pydantic import Field

from bms_msg.schemas.fields import GROUP_FIELD, NAMESPACE_FIELD, PARTITION_FIELD, TOPIC_FIELD

__all__ = ["OffsetResetRequest", "OffsetResetResult"]


class OffsetResetRequest(BaseSchema):
    """位点重置请求。"""

    namespace: str = NAMESPACE_FIELD
    topic: str = TOPIC_FIELD
    group: str = GROUP_FIELD
    partition: int = PARTITION_FIELD
    offset: int = Field(ge=0, description="目标位点（不早于分区起点，且不超过 LEO）")
    confirm: bool = Field(description="显式确认（必须为 true；缺省 / false 一律拒绝，防脚本绕过）")


class OffsetResetResult(BaseSchema):
    """位点重置结果（含重置前值，便于前端与审计对账）。"""

    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    group: str = Field(description="消费组名")
    partition: int = Field(description="分区号")
    previous_offset: int = Field(description="重置前位点（该组该分区无已提交位点时为 0）")
    offset: int = Field(description="重置后位点")
