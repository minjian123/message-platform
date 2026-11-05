"""传输层契约（任务 03-1 / 03-4）：连接元数据自报 + 长驻流查询参数。

- `ConnectionInfo`：`GET /msg/connection`、`POST /msg/connection/heartbeat` 与心跳帧三处的共同主体——
  协议版本 / 承载方式 / 客户端地址 + 连接键 / 活跃流 / 三类阈值 / 两个上限 / drain 标记。
  只描述**当前这条连接**，不含租户业务数据，故不加权限码（05 域认证落地后再评估收紧）。
- `SubscribeQuery`：`GET /msg/subscribe` 的查询参数。**复用数据面共享字段**（命名空间 / 主题 / 组 /
  成员 / 分区），形态校验口径与 `/msg/consume` 完全一致（越界一律 `10001`），避免两套形态规则。

`max_seconds` 上界落在契约层（`10001`）而不是产品码：与 01-2「位点负值归契约」同一分工——参数越界不是
业务判定，占产品码会让码表被参数校验污染。
"""

from datetime import UTC, datetime

from bms_core.schemas.base import BaseSchema
from pydantic import Field

from bms_msg.core.config import (
    DEFAULT_FETCH_MESSAGES,
    MAX_FETCH_MESSAGES,
    SUBSCRIBE_MAX_SECONDS,
    SUBSCRIBE_MAX_SECONDS_LIMIT,
)
from bms_msg.schemas.fields import (
    CONSUMER_ID_FIELD,
    GROUP_FIELD,
    NAMESPACE_FIELD,
    PARTITION_FIELD,
    TOPIC_FIELD,
)

__all__ = [
    "ConnectionInfo",
    "SubscribeQuery",
]


class ConnectionInfo(BaseSchema):
    """连接元数据（`GET /msg/connection`、`POST /msg/connection/heartbeat`、心跳帧三处共用）。

    一次把客户端自适应需要的参数给全（间隔 / 超时 / 两个上限 / drain），SDK 不必再查常量
    （03-4 详细设计 §6.2）。字段**只增不删**：新增仍走本模型，客户端按未知字段忽略处理。
    """

    http_version: str = Field(description="协商到的 HTTP 版本（HTTP/2 / HTTP/1.1）")
    scheme: str = Field(description="承载协议（http / https）")
    is_tls: bool = Field(description="是否 TLS 承载（TLS 下由 ALPN 协商 h2）")
    client_host: str = Field(description="客户端地址（无地址场景为空串）")
    client_port: int = Field(description="客户端端口（无地址场景为 0）")
    protocol_version: str | None = Field(
        default=None, description="本请求所用帧协议版本（帧形态非空；JSON 请求为 null）"
    )
    supported_protocol_version: str = Field(default="1.0", description="服务端当前帧协议版本（恒有值）")
    connection_key: str = Field(default="", description="派生连接键（tenant|host:port；与日志同值）")
    tenant: str = Field(default="anonymous", description="租户编码；无租户上下文为 anonymous")
    active_streams: int = Field(default=0, description="该连接上的长驻流数")
    requests: int = Field(default=0, description="该连接累计请求数")
    uptime_ms: int = Field(default=0, description="连接存活时长（毫秒）")
    idle_ms: int = Field(default=0, description="距最近一次请求的时长（毫秒；判半开的可见值）")
    last_heartbeat_at: datetime | None = Field(default=None, description="最近一次心跳时刻；从未心跳为 null")
    heartbeat_interval_ms: int = Field(default=0, description="服务端建议心跳间隔（毫秒）")
    heartbeat_timeout_ms: int = Field(default=0, description="服务端判定半开的阈值（毫秒）")
    idle_timeout_ms: int = Field(default=0, description="连接名额回收阈值（毫秒）")
    max_streams_per_connection: int = Field(default=0, description="单连接长驻流上限（超限 350021）")
    max_connections: int = Field(default=0, description="单副本连接数上限（超限 350020）")
    draining: bool = Field(default=False, description="本实例是否在摘流（true 时客户端应换实例重连）")
    retry_after_ms: int = Field(default=0, description="建议重试间隔（毫秒；drain / 限流时用）")
    server_time: datetime = Field(default_factory=lambda: datetime.now(UTC), description="服务端当前时间")


class SubscribeQuery(BaseSchema):
    """长驻流查询参数（`GET /msg/subscribe`）。

    字段与 `/msg/consume` 请求体同口径：形态在契约层拒绝（`10001`），业务判定在服务层（`3500xx`）。
    """

    namespace: str = NAMESPACE_FIELD
    topic: str = TOPIC_FIELD
    group: str = GROUP_FIELD
    consumer_id: str = CONSUMER_ID_FIELD
    partition: int = PARTITION_FIELD
    max_messages: int = Field(
        default=DEFAULT_FETCH_MESSAGES,
        ge=1,
        le=MAX_FETCH_MESSAGES,
        description="单批拉取条数（与 consume 同口径）",
    )
    max_seconds: int = Field(
        default=SUBSCRIBE_MAX_SECONDS,
        ge=1,
        le=SUBSCRIBE_MAX_SECONDS_LIMIT,
        description="单条流时长上限（秒）；到期以 close(deadline) 正常结束，由客户端重连",
    )
