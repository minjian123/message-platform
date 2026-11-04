"""消息投递契约（详细设计 §7）：发布 / 拉取 / 确认的请求与响应主体。

集合字段一律插入序集合类 + 契约元数据内联标注（`Annotated[ConcurrentStable*, CONTRACT_COLLECTION]`），
禁命名泛型别名（防契约 `$defs` 漂移）。

**校验分工**：形态（长度 / 正则 / 数值上下界 / 必填）在契约层（`10001`）；业务口径（命名空间是否已启用、
主题是否存在、分区号是否在范围内、位点是否越界）在服务层（`3500xx`）。
载荷以 **base64 字符串**传输（二进制安全），解码在服务层（非法 base64 → `10001`）。

**延迟是发布的一个维度**（01-5）：`delay_level` 给值即延迟发布，不另开端点；延迟发布此刻还没有
offset（消息尚未进入 offset 空间），故 `offset` 放宽为可空、另给 `deliver_at`。
"""

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.schemas.base import (
    CONTRACT_COLLECTION,
    CONTRACT_STABLE_DICT,
    CONTRACT_STABLE_LIST,
    BaseSchema,
)
from pydantic import Field

from bms_msg.core.config import (
    ACKS_AT_LEAST_ONCE,
    ACKS_FIRE_AND_FORGET,
    DEFAULT_FETCH_MESSAGES,
    MAX_FETCH_MESSAGES,
    MESSAGE_KEY_MAX_LENGTH,
)
from bms_msg.schemas.fields import CONSUMER_ID_FIELD, GROUP_FIELD, NAMESPACE_FIELD, PARTITION_FIELD, TOPIC_FIELD

__all__ = [
    "AckRequest",
    "AckResult",
    "ConsumeRequest",
    "ConsumeResult",
    "DelayLevel",
    "MessageItem",
    "PublishRequest",
    "PublishResult",
]

_NAMESPACE_FIELD = NAMESPACE_FIELD
"""命名空间字段（三个请求共用；形态校验的宽度留量由服务层给 `350010`）。"""

_TOPIC_FIELD = TOPIC_FIELD
"""主题名字段（三个请求共用）。"""

_GROUP_FIELD = GROUP_FIELD
"""消费组字段（拉取 / 确认共用）。"""

_CONSUMER_ID_FIELD = CONSUMER_ID_FIELD
"""成员标识字段（拉取 / 确认共用；成员制强约束见 01-4 详细设计 §6.1）。"""

_PARTITION_FIELD = PARTITION_FIELD
"""分区号字段（拉取 / 确认共用）。"""


class DelayLevel(StrEnum):
    """延迟档位（固定 9 档；与 `core/config.DELAY_LEVEL_MS` 一一对应，非法值由契约层拒绝）。

    不做任意精度：档位是容量与可测性的边界（任意毫秒会让时间轮与用例同时失控）。
    """

    SECOND_1 = "1s"
    """1 秒（退避重试的最短档）。"""

    SECOND_5 = "5s"
    """5 秒。"""

    SECOND_10 = "10s"
    """10 秒。"""

    SECOND_30 = "30s"
    """30 秒。"""

    MINUTE_1 = "1m"
    """1 分钟。"""

    MINUTE_5 = "5m"
    """5 分钟。"""

    MINUTE_10 = "10m"
    """10 分钟。"""

    MINUTE_30 = "30m"
    """30 分钟。"""

    MINUTE_60 = "60m"
    """60 分钟（首期最长档位；更长的等待应由业务用状态机表达）。"""


class PublishRequest(BaseSchema):
    """发布请求（`tenant` 不在请求体：由数据面解析链注入，防客户端自报冒充）。"""

    namespace: str = _NAMESPACE_FIELD
    topic: str = _TOPIC_FIELD
    key: str | None = Field(default=None, max_length=MESSAGE_KEY_MAX_LENGTH, description="分区键（同 key 恒同分区）")
    payload: str = Field(default="", description="消息体（base64 编码；空串表示空消息体）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT, description="结构化头（trace / 业务标记）"
    )
    trace_id: str | None = Field(default=None, max_length=128, description="链路标识")
    delay_level: DelayLevel | None = Field(
        default=None, description="延迟档位（缺省 null = 普通发布；给值即延迟发布，到期才进入 offset 空间）"
    )


class PublishResult(BaseSchema):
    """发布结果（`message_id` 与 `offset` 解耦：前者全局唯一，后者分区内序号）。"""

    message_id: str = Field(description="消息 ID（雪花，JSON 输出为字符串；重投 / 重放不变）")
    log_id: str = Field(description="记录 ID（发布即分配；延迟发布到期转投沿用，审计可凭它定位记录）")
    topic: str = Field(description="主题名")
    partition: int = Field(description="落到的分区号")
    offset: int | None = Field(description="分区内序号（延迟发布尚未进入 offset 空间，为 null）")
    deliver_at: datetime | None = Field(description="到期时刻（UTC；普通发布为 null）")


class ConsumeRequest(BaseSchema):
    """拉取请求（成员制：须先 `join`，且 `partition` 必须在该成员的分配内）。"""

    group: str = _GROUP_FIELD
    consumer_id: str = _CONSUMER_ID_FIELD
    namespace: str = _NAMESPACE_FIELD
    topic: str = _TOPIC_FIELD
    partition: int = _PARTITION_FIELD
    max_messages: int = Field(
        default=DEFAULT_FETCH_MESSAGES, ge=1, le=MAX_FETCH_MESSAGES, description="本次最多拉取条数（1~100）"
    )
    acks: int = Field(
        default=ACKS_AT_LEAST_ONCE,
        ge=ACKS_FIRE_AND_FORGET,
        le=ACKS_AT_LEAST_ONCE,
        description="投递档位：1 = 确认后才算成功（至少一次，缺省）；0 = 不等确认（可能丢）",
    )


class MessageItem(BaseSchema):
    """一条投递的消息（`delivery_count` 供消费端识别重复；幂等键由业务自选）。"""

    message_id: str = Field(description="消息 ID（重投保持不变）")
    topic: str = Field(description="主题名")
    partition: int = Field(description="分区号")
    offset: int = Field(description="分区内序号")
    key: str | None = Field(description="分区键")
    payload: str = Field(description="消息体（base64 编码）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT, description="结构化头"
    )
    trace_id: str | None = Field(description="链路标识")
    delivery_count: int = Field(description="投递次数（首次 1，重投递增）")
    published_at: datetime = Field(description="发布时间（UTC）")


class ConsumeResult(BaseSchema):
    """拉取结果（可能为空：位点已到 LEO，或剩余消息均在可见性窗口内）。"""

    messages: Annotated[ConcurrentStableList[MessageItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="本次投递的消息（按 offset 升序）"
    )


class AckRequest(BaseSchema):
    """确认请求（位点提交值 = `offset + 1`；成员制：分区须仍在该成员分配内）。"""

    group: str = _GROUP_FIELD
    consumer_id: str = _CONSUMER_ID_FIELD
    namespace: str = _NAMESPACE_FIELD
    topic: str = _TOPIC_FIELD
    partition: int = _PARTITION_FIELD
    offset: int = Field(ge=0, description="待确认消息的分区内序号")


class AckResult(BaseSchema):
    """确认结果（`committed_offset` 为下一条待消费的 offset）。"""

    committed_offset: int = Field(description="已提交位点（已处理完的最后一条 + 1）")
