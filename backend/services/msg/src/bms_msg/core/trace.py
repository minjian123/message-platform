"""链路上下文的报文侧承载（任务 06-4）：四个 trace 字段的**唯一载体**。

为什么要有这个值对象：`trace_id` 原本是一条显式参数，从发布一路手传到存储、投递、延迟转投、
重放、三个出口（32 处签名）。再加 `span_id` / `parent_span_id` / `trace_flags` 三个字段后，
「逐层四个关键字」既容易漏传（漏一处就是静默丢字段、排障时才暴露），也会把下次扩展（baggage /
tracestate）再放大一次。故收成一个不可变对象，一处承载、一处换算。

三条不变量（任何一层都不得违反）：

1. **不可变**：谁都不许就地改 trace 字段；要变就换新值——重放同样**不重新生成**（`message_id`
   不变、trace 也不变，只有 `log_id` 新分配）。
2. **四字段各自可空**：允许「只有 `trace_id`」的存量形态；不为了凑齐而填假值（broker 不伪造 span）。
3. **空即 `EMPTY_TRACE`**：端口缺省值用单例，避免 `None` 与「四项全空的实例」两种空态并存。

**与基座 `bms_core.tracing.SpanContext` 的关系**：不复用。基座那个是**进程内 span 上下文**
（`name` 必填、无采样位、带 attributes），报文头里没有 span 名，硬用会逼出假值；本对象只描述
「随消息走的那几个字符串」，语义与 `x-msg-frame` 契约一致。

**broker 不解释 `trace_flags`**：位含义归 SDK / OTel，本服务只存、搬、回显——一旦在这里判定
「采样位为 0 就丢弃」，链路会变成半截，且标志位演进会被 broker 锁死。
"""

from dataclasses import dataclass
from typing import Protocol

from bms_core.core.objects import BaseValueObject

__all__ = [
    "EMPTY_TRACE",
    "TRACE_FIELD_MAX_LENGTH",
    "TraceCarrier",
    "TraceContext",
    "trace_of",
]

TRACE_FIELD_MAX_LENGTH = 128
"""四个 trace 字段的统一长度上限（与 03-2 帧头 `optional_text` 同尺）。

**只判长度、不判形态**：`trace_id` 一直只限长度，`span_id` / `trace_flags` 沿用同一口径——
按 OTel 形态强校验会拒绝 SDK 的合法变体（带前缀 / 自定义 span 名），且与既有字段不一致。
"""


class TraceCarrier(Protocol):
    """能把四个 trace 字段读出来的载体（请求模型 / 记录头 / 存储记录 / 延迟记录）。

    只为 `trace_of()` 的入参定形：这些类型各自持有四个可选字符串字段，但互不继承，
    用协议比 union 更稳（新增载体不必改 union）。
    """

    @property
    def trace_id(self) -> str | None:
        """链路标识。"""
        ...

    @property
    def span_id(self) -> str | None:
        """本次 span 标识。"""
        ...

    @property
    def parent_span_id(self) -> str | None:
        """父 span 标识。"""
        ...

    @property
    def trace_flags(self) -> str | None:
        """采样标记（原样透传，不解释）。"""
        ...


@dataclass(frozen=True)
class TraceContext(BaseValueObject):
    """随消息流转的链路字段集合（不可变；四项除 `trace_id` 外均可不全）。"""

    trace_id: str | None = None
    """链路标识（跨生产与消费共享；可空 = 生产者未注入）。"""

    span_id: str | None = None
    """生产者本次 span 标识（消费端据此建子 span）。"""

    parent_span_id: str | None = None
    """生产 span 的父 span（链路起点可空）。"""

    trace_flags: str | None = None
    """采样标记（约定 W3C 8 位十六进制；broker 不解释、不改写、不据此丢弃）。"""

    @property
    def is_empty(self) -> bool:
        """四个字段是否全空（空链路：不写盘、不回显非空值）。

        Returns:
            bool: 全空为 True。
        """
        return (
            self.trace_id is None and self.span_id is None and self.parent_span_id is None and self.trace_flags is None
        )

    @property
    def has_span(self) -> bool:
        """是否带 span 侧信息（决定记录头要不要写 `trace` 子结构）。

        Returns:
            bool: `span_id` / `parent_span_id` / `trace_flags` 任一非空为 True。
        """
        return self.span_id is not None or self.parent_span_id is not None or self.trace_flags is not None

    def with_trace_id(self, trace_id: str | None) -> TraceContext:
        """替换链路标识（服务端补 trace 时用；不改 span 侧字段）。

        Args:
            trace_id: 新的链路标识。

        Returns:
            TraceContext: 新实例（原实例不变）。
        """
        return TraceContext(
            trace_id=trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            trace_flags=self.trace_flags,
        )


EMPTY_TRACE = TraceContext()
"""空链路单例（端口缺省值；避免 `None` 与全空实例两种空态并存）。"""


def trace_of(source: TraceCarrier | None) -> TraceContext:
    """从任一带四个字段的载体取值（各层唯一的换算入口）。

    Args:
        source: 承载四个 trace 字段的对象；`None` 视为空链路。

    Returns:
        TraceContext: 链路上下文（缺字段为 None）。
    """
    if source is None:
        return EMPTY_TRACE
    return TraceContext(
        trace_id=source.trace_id,
        span_id=source.span_id,
        parent_span_id=source.parent_span_id,
        trace_flags=source.trace_flags,
    )
