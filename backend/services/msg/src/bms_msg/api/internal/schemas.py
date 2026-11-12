"""内部复制 / 控制通道的请求与响应契约（02-8）。

**不进产品契约快照**：本组模型只服务 8001 内部端口（平台网络 + 服务令牌），不注册网关、
不写产品路由；`protocol_version` 为唯一版本锚点，未知版本按不可达处理。

**编码**：JSON；`payload` 用 base64（与 `/msg/publish` 既有口径一致），`headers` 原样字典，
`published_at` UTC ISO8601；记录全字段与 `StoredMessage` 一一对应。
"""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.msgclient.payload import COMPRESSION_NONE, DEFAULT_CONTENT_TYPE
from bms_core.schemas.base import CONTRACT_COLLECTION
from pydantic import BaseModel, ConfigDict, Field

from bms_msg.broker.log import StoredMessage
from bms_msg.core.config import REPLICATION_INTERNAL_PROTOCOL_VERSION
from bms_msg.core.trace import TraceContext

__all__ = [
    "DemoteRequest",
    "FetchRequest",
    "FetchResponse",
    "HealthResponse",
    "InternalMessage",
    "SnapshotResponse",
    "SwitchApplyRequest",
    "SwitchApplyResponse",
]

_INTERNAL_CONFIG = ConfigDict(extra="forbid")


class InternalMessage(BaseModel):
    """内部通道的一条记录（`StoredMessage` 的 JSON 投影）。"""

    model_config = _INTERNAL_CONFIG

    message_id: str = Field(description="全局唯一标识")
    log_id: str = Field(description="记录标识")
    topic_id: int = Field(description="主题主键")
    partition_no: int = Field(description="分区号")
    offset: int = Field(description="分区内序号")
    key: str | None = Field(default=None, description="分区键")
    payload: str = Field(description="业务字节（base64）")
    headers: Annotated[ConcurrentStableDict[str, str], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableDict[str, str](), description="结构化头"
    )
    trace_id: str | None = Field(default=None, description="链路标识")
    span_id: str | None = Field(default=None, description="span 标识")
    parent_span_id: str | None = Field(default=None, description="父 span 标识")
    trace_flags: str | None = Field(default=None, description="采样标记")
    published_at: datetime = Field(description="发布时间（UTC）")
    content_type: str = Field(default=DEFAULT_CONTENT_TYPE, description="内容类型")
    compression: str = Field(default=COMPRESSION_NONE, description="压缩算法")
    uncompressed_len: int | None = Field(default=None, description="解压后字节数")
    epoch: int = Field(default=0, description="leader 任期")

    @classmethod
    def from_stored(cls, message: StoredMessage) -> InternalMessage:
        """把存储记录转成内部契约。

        Args:
            message: 存储记录。

        Returns:
            InternalMessage: 内部契约实例。
        """
        return cls(
            message_id=message.message_id,
            log_id=message.log_id,
            topic_id=message.topic_id,
            partition_no=message.partition_no,
            offset=message.offset,
            key=message.key,
            payload=base64.b64encode(message.payload).decode("ascii"),
            headers=ConcurrentStableDict(message.headers),
            trace_id=message.trace.trace_id,
            span_id=message.trace.span_id,
            parent_span_id=message.trace.parent_span_id,
            trace_flags=message.trace.trace_flags,
            published_at=message.published_at,
            content_type=message.content_type,
            compression=message.compression,
            uncompressed_len=message.uncompressed_len,
            epoch=message.epoch,
        )

    def to_stored(self) -> StoredMessage:
        """还原成存储记录（follower 落盘用）。

        Returns:
            StoredMessage: 存储记录。
        """
        return StoredMessage(
            message_id=self.message_id,
            log_id=self.log_id,
            topic_id=self.topic_id,
            partition_no=self.partition_no,
            offset=self.offset,
            key=self.key,
            payload=base64.b64decode(self.payload.encode("ascii")),
            headers=ConcurrentStableDict(self.headers),
            trace=TraceContext(
                trace_id=self.trace_id,
                span_id=self.span_id,
                parent_span_id=self.parent_span_id,
                trace_flags=self.trace_flags,
            ),
            published_at=self.published_at,
            content_type=self.content_type,
            compression=self.compression,
            uncompressed_len=self.uncompressed_len,
            epoch=self.epoch,
        )


class HealthResponse(BaseModel):
    """内部健康探针响应（不鉴权）。"""

    model_config = _INTERNAL_CONFIG

    status: str = Field(default="ok", description="固定 ok")
    node_id: str = Field(description="本节点标识")
    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")


class FetchRequest(BaseModel):
    """follower → leader 的拉取请求。"""

    model_config = _INTERNAL_CONFIG

    leader: str = Field(description="目标 leader 节点")
    follower: str = Field(description="发起方节点")
    topic_id: int = Field(description="主题主键")
    partition_no: int = Field(description="分区号")
    from_offset: int = Field(ge=0, description="follower 当前 LEO")
    max_messages: int = Field(gt=0, description="单次上限（条）")
    epoch: int = Field(ge=0, description="follower 已知 leader 任期")
    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")


class FetchResponse(BaseModel):
    """leader → follower 的拉取结果。"""

    model_config = _INTERNAL_CONFIG

    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")
    leader_epoch: int = Field(description="leader 任期")
    from_offset: int = Field(description="响应起点")
    records: Annotated[ConcurrentStableList[InternalMessage], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableList[InternalMessage](), description="记录（offset 升序）"
    )
    leader_leo: int = Field(description="leader 末端")
    high_watermark: int = Field(description="leader 高水位")
    leader_isr: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableList[str](), description="leader 当前 ISR"
    )


class SnapshotResponse(BaseModel):
    """分区复制运行态快照（控制通道）。"""

    model_config = _INTERNAL_CONFIG

    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")
    leader: str = Field(description="当前 leader")
    epoch: int = Field(description="当前任期")
    leo: int = Field(description="本节点末端")
    hw: int = Field(description="高水位")
    isr: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableList[str](), description="当前 ISR"
    )
    replicas: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableList[str](), description="副本集"
    )
    switch_state: str = Field(default="idle", description="切换状态")
    writable: bool = Field(default=False, description="本节点是否可写")
    last_known_hw: int = Field(default=0, description="最后已知 leader HW")
    last_known_isr: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        default_factory=lambda: ConcurrentStableList[str](), description="最后已知 ISR"
    )
    last_known_at: float = Field(default=0.0, description="最后已知采集时刻（单调毫秒）")
    start_offset: int = Field(default=0, description="日志可读起点（迁移预检）")


class DemoteRequest(BaseModel):
    """旧 leader 降级请求。"""

    model_config = _INTERNAL_CONFIG

    to_leader: str = Field(description="新 leader")
    to_epoch: int = Field(ge=0, description="新任期")


class SwitchApplyRequest(BaseModel):
    """目标节点激活请求（`SwitchApplyCommand` 的 JSON 投影）。"""

    model_config = _INTERNAL_CONFIG

    topic_id: int = Field(description="主题主键")
    partition_no: int = Field(description="分区号")
    from_leader: str = Field(description="切换前 leader")
    to_leader: str = Field(description="新 leader")
    to_epoch: int = Field(ge=0, description="新任期")
    watermark: int | None = Field(default=None, description="切换水位（null = 未知，走保留）")
    truncate: bool = Field(default=False, description="是否按水位截断")
    grace_ms: int = Field(default=0, ge=0, description="停写宽限（毫秒）")
    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")


class SwitchApplyResponse(BaseModel):
    """目标节点激活结果。"""

    model_config = _INTERNAL_CONFIG

    protocol_version: int = Field(default=REPLICATION_INTERNAL_PROTOCOL_VERSION, description="内部协议版本")
    to_leo: int = Field(description="激活后的 LEO")
    truncated_to: int | None = Field(default=None, description="截断后的 LEO；null = 未截断")
    retained_tail: int = Field(default=0, description="保留的未提交条数")
    already_applied: bool = Field(default=False, description="是否幂等重复激活")
