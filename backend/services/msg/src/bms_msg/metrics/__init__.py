"""msg 指标域（06-3-1）：指标清单 / 事件点采集器 / 采样快照。

**单一出口**：收集器（`bms_msg.metrics.collector`）注册进基座 `PrometheusMetrics`，
根路径 `/metrics` 由基座提供；本域不自建 HTTP 端点、不第二次渲染 Prometheus 文本。
**导入边界**：本包只导出无 broker 依赖的部分（采集器 / 快照），避免存储层埋点导入时成环；
采样器与收集器由应用装配与测试直接按子模块导入。
"""

from bms_msg.metrics.lag import LagSnapshot, MetricsSnapshotStore
from bms_msg.metrics.traffic import (
    HistogramReading,
    TrafficMetrics,
    get_traffic_metrics,
    reason_of,
    reset_traffic_metrics,
)

__all__ = [
    "HistogramReading",
    "LagSnapshot",
    "MetricsSnapshotStore",
    "TrafficMetrics",
    "get_traffic_metrics",
    "reason_of",
    "reset_traffic_metrics",
]
