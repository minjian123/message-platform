"""消息投递契约（详细设计 §7）：发布 / 拉取 / 确认的请求与响应主体。

集合字段一律插入序集合类 + 契约元数据内联标注（`Annotated[ConcurrentStable*, CONTRACT_COLLECTION]`），
禁命名泛型别名（防契约 `$defs` 漂移）。

**校验分工**：形态（长度 / 正则 / 数值上下界 / 必填）在契约层（`10001`）；业务口径（命名空间是否已启用、
主题是否存在、分区号是否在范围内、位点是否越界）在服务层（`3500xx`）。
载荷以 **base64 字符串**传输（二进制安全），解码在服务层（非法 base64 → `10001`）。

**内容类型与压缩（03-3）**：`content_type` / `compression` / `uncompressed_len` 随消息记录落库并回显，
取值口径见 `protocol.payload`。帧路径的 payload 原始字节另走**私有通道**（`raw_payload`，不进 JSON Schema、
不进 `model_dump()`），避免 base64 往返；JSON 路径仍用 `payload` 的 base64（文本载体必需）。

**延迟是发布的一个维度**（01-5）：`delay_level` 给值即延迟发布，不另开端点；延迟发布此刻还没有
offset（消息尚未进入 offset 空间），故 `offset` 放宽为可空、另给 `deliver_at`。
"""

import base64
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE
from bms_core.schemas.base import (
    CONTRACT_COLLECTION,
    CONTRACT_STABLE_DICT,
    CONTRACT_STABLE_LIST,
    BaseSchema,
)
from pydantic import Field, PrivateAttr, field_serializer

from bms_msg.core.config import (
    ACKS_AT_LEAST_ONCE,
    ACKS_FIRE_AND_FORGET,
    ACKS_PUBLISH_LOCAL,
    DEFAULT_FETCH_MESSAGES,
    MAX_FETCH_MESSAGES,
    MESSAGE_KEY_MAX_LENGTH,
    SUBSCRIBE_MAX_WAIT_LIMIT_MS,
    SUBSCRIBE_MAX_WAIT_MS,
)
from bms_msg.core.trace import TRACE_FIELD_MAX_LENGTH
from bms_msg.schemas.fields import CONSUMER_ID_FIELD, GROUP_FIELD, NAMESPACE_FIELD, PARTITION_FIELD, TOPIC_FIELD

__all__ = [
    "AckRequest",
    "AckResult",
    "ConsumeRequest",
    "ConsumeResult",
    "DelayLevel",
    "MessageItem",
    "NackReason",
    "NackRequest",
    "NackResult",
    "PublishRequest",
    "PublishResult",
]

CONTENT_TYPE_FIELD_MAX_LENGTH = 255
"""内容类型字段长度上限（两个 RFC 6838 token 各 128 位 + 分隔符，与 `protocol.payload` 同口径）。"""

COMPRESSION_FIELD_MAX_LENGTH = 16
"""压缩算法名长度上限（白名单最长 6 位，留余量）。"""

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

TRACE_ID_DESCRIPTION = "链路标识"
"""链路标识字段描述（请求与响应同一句，防契约描述漂移）。"""

SPAN_ID_DESCRIPTION = "生产者本次 span 标识（消费端据它建子 span）"
"""span 标识字段描述。"""

PARENT_SPAN_ID_DESCRIPTION = "生产 span 的父 span（链路起点可为空）"
"""父 span 字段描述。"""

TRACE_FLAGS_DESCRIPTION = "采样标记（约定 W3C 8 位十六进制；broker 不解释，原样透传）"
"""采样标记字段描述（broker 只透传，不据此丢弃消息）。"""


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
    trace_id: str | None = Field(default=None, max_length=TRACE_FIELD_MAX_LENGTH, description=TRACE_ID_DESCRIPTION)
    span_id: str | None = Field(default=None, max_length=TRACE_FIELD_MAX_LENGTH, description=SPAN_ID_DESCRIPTION)
    parent_span_id: str | None = Field(
        default=None, max_length=TRACE_FIELD_MAX_LENGTH, description=PARENT_SPAN_ID_DESCRIPTION
    )
    trace_flags: str | None = Field(
        default=None, max_length=TRACE_FIELD_MAX_LENGTH, description=TRACE_FLAGS_DESCRIPTION
    )
    delay_level: DelayLevel | None = Field(
        default=None, description="延迟档位（缺省 null = 普通发布；给值即延迟发布，到期才进入 offset 空间）"
    )
    content_type: str | None = Field(
        default=None,
        max_length=CONTENT_TYPE_FIELD_MAX_LENGTH,
        description="消息体内容类型（缺省按 application/msgpack；见协议契约 payload_content_types）",
    )
    compression: str | None = Field(
        default=None,
        max_length=COMPRESSION_FIELD_MAX_LENGTH,
        description="压缩算法（缺省 none；帧路径由 flags.compressed 与它共同判定，两者必须自洽）",
    )
    uncompressed_len: int | None = Field(
        default=None, ge=0, description="解压后字节数（可选声明；broker 只透传，不参与上限判定）"
    )
    acks: Literal["1", "all"] = Field(
        default=ACKS_PUBLISH_LOCAL,
        description="写入确认档位：1 = leader 本地落盘即返回；all = ISR 确认并推进 HW 后返回（复制关闭时与 1 等价）",
    )

    _raw_payload: bytes | None = PrivateAttr(default=None)
    """原始字节通道（帧路径专用；不进 JSON Schema、不进 `model_dump()`）。"""

    @property
    def raw_payload(self) -> bytes | None:
        """原始消息体字节（未设置时为 None，由 `payload` 的 base64 解码）。

        Returns:
            bytes | None: 帧路径的原始字节；JSON 路径为 None。
        """
        return self._raw_payload

    def set_raw_payload(self, payload: bytes) -> None:
        """设置原始消息体字节（仅帧闸门使用；不改变 `payload` 字段）。

        Args:
            payload: 消息体原始字节。
        """
        self._raw_payload = payload


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
    max_wait_ms: int = Field(
        default=SUBSCRIBE_MAX_WAIT_MS,
        ge=0,
        le=SUBSCRIBE_MAX_WAIT_LIMIT_MS,
        description="长轮询挂起上限（毫秒；0 = 不挂起立即返回；越界 10001）",
    )


class MessageItem(BaseSchema):
    """一条投递的消息（`delivery_count` 供消费端识别重复；幂等键由业务自选）。"""

    message_id: str = Field(description="消息 ID（重投保持不变）")
    topic: str = Field(description="主题名")
    partition: int = Field(description="分区号")
    offset: int = Field(description="分区内序号")
    key: str | None = Field(description="分区键")
    payload: str = Field(description="消息体（base64 编码；帧路径由私有原始字节通道替代）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT, description="结构化头"
    )
    trace_id: str | None = Field(description=TRACE_ID_DESCRIPTION)
    span_id: str | None = Field(description=SPAN_ID_DESCRIPTION)
    parent_span_id: str | None = Field(description=PARENT_SPAN_ID_DESCRIPTION)
    trace_flags: str | None = Field(description=TRACE_FLAGS_DESCRIPTION)
    delivery_count: int = Field(description="投递次数（首次 1，重投递增）")
    published_at: datetime = Field(description="发布时间（UTC）")
    content_type: str = Field(
        default=DEFAULT_CONTENT_TYPE, description="消息体内容类型（缺省 application/msgpack，恒有值）"
    )
    compression: str = Field(default=COMPRESSION_NONE, description="压缩算法（none 表示未压缩，恒有值）")
    uncompressed_len: int | None = Field(default=None, description="解压后字节数（生产者可选声明）")

    _raw_payload: bytes | None = PrivateAttr(default=None)
    """原始字节通道（帧装配用；不进 JSON Schema、不进 `model_dump()`）。"""

    @property
    def raw_payload(self) -> bytes | None:
        """原始消息体字节（未设置时为 None：调用方应改用 `payload` 的 base64）。

        Returns:
            bytes | None: 原始字节；非帧装配构造的模型为 None。
        """
        return self._raw_payload

    def set_raw_payload(self, payload: bytes) -> None:
        """设置原始消息体字节（装配期用；`payload` 字段保持空串由序列化器补位）。

        Args:
            payload: 消息体原始字节。
        """
        self._raw_payload = payload

    @field_serializer("payload")
    def _payload_text(self, value: str) -> str:
        """序列化补位：`payload` 为空而原始字节存在时输出 base64（JSON / SSE 路径需要）。

        Args:
            value: 字段当前值。

        Returns:
            str: base64 文本（已有值时原样返回）。
        """
        if value or self._raw_payload is None:
            return value
        return base64.b64encode(self._raw_payload).decode("ascii")


class ConsumeResult(BaseSchema):
    """拉取结果（可能为空：位点已到 LEO、剩余消息均在可见性窗口内，或 inflight 已达阈值）。

    `paused` / 两个用量字段是 03-5 的背压自报：**暂停不是错误**，客户端据此退避（或按 `max_wait_ms`
    继续等），而不是按错误码重试。
    """

    messages: Annotated[ConcurrentStableList[MessageItem], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_LIST, description="本次投递的消息（按 offset 升序）"
    )
    paused: bool = Field(default=False, description="是否因 inflight 达阈值而暂停推送（不是错误）")
    inflight_messages: int = Field(default=0, description="本分区当前在途条数（已投出未确认）")
    inflight_bytes: int = Field(default=0, description="本分区当前在途字节合计（按 payload 线上字节）")


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


class NackReason(StrEnum):
    """拒绝原因（消费端上报；`delivery_exhausted` 由 broker 独占，不接受外部指定）。"""

    SCHEMA_INVALID = "schema_invalid"
    """消息体不符合 schema（字段缺失 / 类型错误 / 顶层不是 Map）。"""

    PROCESSING_FAILED = "processing_failed"
    """业务处理失败且确认不可重试（重试也只会再失败）。"""


class NackRequest(BaseSchema):
    """拒绝一条消息（转死信 + 位点前移；03-3 详细设计 §5.2）。

    成员制与 `ack` 同口径：组须存在、成员须活跃、分区须在其分配内。
    """

    namespace: str = _NAMESPACE_FIELD
    topic: str = _TOPIC_FIELD
    group: str = _GROUP_FIELD
    consumer_id: str = _CONSUMER_ID_FIELD
    partition: int = _PARTITION_FIELD
    offset: int = Field(ge=0, description="待拒绝消息的分区内序号")
    reason: NackReason = Field(description="拒绝原因（schema_invalid / processing_failed）")
    detail: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=CONTRACT_STABLE_DICT,
        description="失败详情（字段名 → 说明，如 field / expected / actual）",
    )


class NackResult(BaseSchema):
    """拒绝结果（与 `AckResult` 同口径：`committed_offset` 为下一条待消费的 offset）。"""

    committed_offset: int = Field(description="已提交位点（被拒消息之后的第一条）")
