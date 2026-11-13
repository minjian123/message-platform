"""连接列表只读契约（06-1-4）。"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, BaseSchema, SnowflakeIdStr
from pydantic import Field

__all__ = ["ConnectionItem", "ConnectionPage"]


class ConnectionItem(BaseSchema):
    """一条连接的只读视图（地址已脱敏）。"""

    connection_id: str = Field(description="连接标识（连接键的短哈希，不回传原始键）")
    tenant_id: SnowflakeIdStr | None = Field(default=None, description="租户内部身份；匿名连接为 null")
    remote_prefix: str = Field(description="脱敏地址（IPv4 只留前两段，IPv6 只留前 64 位）")
    http_version: str = Field(description="协商到的 HTTP 版本")
    scheme: str = Field(description="承载协议（http / https）")
    opened_at: datetime = Field(description="建立时刻（UTC）")
    last_seen_at: datetime = Field(description="最近请求时刻（UTC）")
    last_heartbeat_at: datetime | None = Field(default=None, description="最近心跳时刻（UTC）")
    streams: int = Field(description="长驻流数")
    requests: int = Field(description="累计请求数")
    state: str = Field(description="状态：active / idle / draining / stale")


class ConnectionPage(BaseSchema):
    """连接列表分页（单实例视图，跨实例聚合归 02-8-4）。"""

    items: Annotated[ConcurrentStableList[ConnectionItem], CONTRACT_COLLECTION]
    total: int = Field(description="当前租户的连接总数（过滤后）")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")
