"""管理面只读视图响应契约（06-1-1）：分区 / 消息检索 / 概览 / 延迟汇总。

**只读约束**：响应不含消息正文——`MessageItem` 只有载荷长度与 SHA-256 摘要；
动态字典字段一律用普通 `dict`（`ApiResponse.data` 走 pydantic 序列化，自定义 Mapping 会退化成 500）。
"""

from datetime import datetime
from typing import Annotated

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.base import CONTRACT_COLLECTION, BaseSchema
from pydantic import Field

__all__ = [
    "DelaySummary",
    "LevelBucket",
    "MessageDeleteBatchRequest",
    "MessageDeleteItem",
    "MessageDeleteResult",
    "MessageItem",
    "MessagePage",
    "Overview",
    "PartitionItem",
    "PartitionPage",
]


class PartitionItem(BaseSchema):
    """单个分区的实时运行态（LEO 来自 `MessageStore`，非落库检查点）。"""

    partition: int = Field(description="分区号（0 起）")
    leader: str | None = Field(description="当前 leader 实例标识；元数据缺行时为 null")
    epoch: int = Field(default=0, description="领导者任期（复制快照优先；复制关闭时取元数据行）")
    leo: int = Field(description="日志末端位点（下一条待分配 offset）")
    hw: int | None = Field(description="高水位；02-4 交付前恒 null，不伪造单副本数值")
    isr: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(
        description="同步副本集；02-5 交付前恒空集合"
    )
    committed: int | None = Field(description="已提交位点；未指定消费组时为 null")
    lag: int | None = Field(description="积压（leo - committed）；未指定消费组时为 null")
    owner: str | None = Field(description="分区持有者；未指定消费组或组无分配时为 null")


class PartitionPage(BaseSchema):
    """分区详情响应。"""

    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    group: str | None = Field(description="消费组；未指定时为 null")
    partition_count: int = Field(description="主题分区总数")
    items: Annotated[ConcurrentStableList[PartitionItem], CONTRACT_COLLECTION] = Field(
        description="分区明细（按分区号升序）"
    )


class MessageItem(BaseSchema):
    """一条消息的头信息与载荷摘要（**不含正文**）。"""

    log_id: str = Field(description="记录 ID（精确检索入口）")
    message_id: str = Field(description="消息 ID（重投 / 重放不变）")
    partition: int = Field(description="分区号")
    offset: int = Field(description="分区内序号")
    published_at: datetime = Field(description="发布时间（UTC）")
    headers: dict[str, str] = Field(  # bare-collections:allow pydantic 响应字段须为内置 dict，否则序列化退化
        description="结构化头（不含载荷）"
    )
    trace_id: str | None = Field(description="链路标识（06-4；排障时据此串联生产与消费）")
    span_id: str | None = Field(description="生产者本次 span 标识")
    parent_span_id: str | None = Field(description="生产 span 的父 span")
    trace_flags: str | None = Field(description="采样标记（broker 原样透传，不解释）")
    payload_len: int = Field(description="落盘载荷字节数")
    payload_sha256: str = Field(description="落盘载荷 SHA-256（64 位小写十六进制）")


class MessagePage(BaseSchema):
    """消息检索分页（`total` 为过滤前总数）。"""

    namespace: str = Field(description="命名空间")
    topic: str = Field(description="主题名")
    total: int = Field(description="过滤前总数")
    page: int = Field(description="页码（从 1 起）")
    size: int = Field(description="每页条数")
    items: Annotated[ConcurrentStableList[MessageItem], CONTRACT_COLLECTION] = Field(
        description="当页消息头（按分区号、offset 升序）"
    )


class MessageDeleteItem(BaseSchema):
    """单条删消息结果。"""

    log_id: str = Field(description="记录 ID")
    result: str = Field(description="结果：deleted / not_found / already_deleted")


class MessageDeleteResult(BaseSchema):
    """删消息结果（单条与批量共用）。"""

    items: Annotated[ConcurrentStableList[MessageDeleteItem], CONTRACT_COLLECTION]
    deleted: int = Field(description="本次删除条数")
    not_found: int = Field(description="不存在或已删除条数")


class MessageDeleteBatchRequest(BaseSchema):
    """批量删消息请求。"""

    log_ids: Annotated[ConcurrentStableList[str], CONTRACT_COLLECTION] = Field(description="记录 ID 列表（≤ 100）")
    confirm: bool = Field(default=False, description="显式确认（必须为 true）")


class Overview(BaseSchema):
    """平台概览四张数字卡 + 快照可用性。"""

    messages_in_per_second: float = Field(description="入口吞吐（本采样周期条数 / 周期秒数）")
    consumer_lag: int = Field(description="当前租户积压条数")
    dlq_total: int = Field(description="当前租户死信存量")
    disk_used_ratio: float = Field(description="磁盘使用率（多卷取最大值）")
    snapshot_available: bool = Field(description="采样快照是否可用；进程刚启动时为 false，四值返回 0")


class LevelBucket(BaseSchema):
    """一个延迟档位的汇总。"""

    level: str = Field(description="延迟档位（1s / 5s / … / 60m）")
    count: int = Field(description="该档待投条数（无记录也输出 0）")
    min_remaining_ms: int = Field(description="该档最短剩余毫秒数；无记录为 0")


class DelaySummary(BaseSchema):
    """延迟汇总（与 `/admin/delayed` 同一份当前租户快照）。"""

    namespace: str | None = Field(description="命名空间过滤值")
    topic: str | None = Field(description="主题过滤值")
    total: int = Field(description="过滤前待投总条数")
    levels: Annotated[ConcurrentStableList[LevelBucket], CONTRACT_COLLECTION] = Field(
        description="九档分布（按档位声明序）"
    )
    stale_count: int = Field(description="已到期但未转投的条数")
    quota_used_ratio: float = Field(description="当前租户待投量 / 软上限（夹到 0~1）")
    hint: str = Field(description="结构化堆积提示：normal / stale / near_quota")
