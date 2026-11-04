"""传输层契约（任务 03-1）：连接元数据自报 + 长驻流查询参数。

- `ConnectionInfo`：`GET /msg/connection` 的响应主体——协议版本 / 承载方式 / 客户端地址。
  只描述**当前这条连接**，不含租户数据，故不加权限码（收紧归 03-4，05 域认证落地后）。
- `SubscribeQuery`：`GET /msg/subscribe` 的查询参数。**复用数据面共享字段**（命名空间 / 主题 / 组 /
  成员 / 分区），形态校验口径与 `/msg/consume` 完全一致（越界一律 `10001`），避免两套形态规则。

`max_seconds` 上界落在契约层（`10001`）而不是产品码：与 01-2「位点负值归契约」同一分工——参数越界不是
业务判定，占产品码会让码表被参数校验污染。
"""

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
    """连接元数据（`GET /msg/connection`；排障第一问的机器可读出口）。"""

    http_version: str = Field(description="协商到的 HTTP 版本（HTTP/2 / HTTP/1.1）")
    scheme: str = Field(description="承载协议（http / https）")
    is_tls: bool = Field(description="是否 TLS 承载（TLS 下由 ALPN 协商 h2）")
    client_host: str = Field(description="客户端地址（无地址场景为空串）")
    client_port: int = Field(description="客户端端口（无地址场景为 0）")


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
