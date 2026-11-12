"""指标名 / 类型 / 标签 / 文案的单一事实源（06-3-1）。

**为什么集中**：06-3-2 的告警规则与 06-1 的看板只引用这里登记的名字；改一个指标名必须只改一处。
**标签纪律**：业务标签只允许 `tenant` / `topic`；`reason` / `scope` / `result` / `status` / `method`
为闭集枚举，取值在本模块定义，禁止把用户输入直接作标签。
"""

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

__all__ = [
    "COUNTER_NAMES",
    "GAUGE_NAMES",
    "HISTOGRAM_NAMES",
    "LATENCY_BUCKETS_MS",
    "METRIC_HELP",
    "PUBLISH_REASONS",
    "TENANT_TOPIC_NAMES",
]

LATENCY_BUCKETS_MS: ConcurrentStableList[float] = ConcurrentStableList(
    (5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0)
)
"""发布 / 消费延迟直方图桶（毫秒；上限外落 `+Inf`）。"""

PUBLISH_REASONS: ConcurrentStableList[str] = ConcurrentStableList(
    ("watermark", "quota", "rate_limit", "acl", "param", "unavailable", "internal")
)
"""发布 / 消费错误原因（闭集；标签只放 `reason`）。"""

TENANT_TOPIC_NAMES: ConcurrentStableList[str] = ConcurrentStableList(
    (
        "msg_messages_in_total",
        "msg_bytes_in_total",
        "msg_messages_out_total",
        "msg_publish_latency_ms",
        "msg_consume_latency_ms",
        "msg_consumer_lag",
        "msg_inflight_messages",
        "msg_inflight_bytes",
        "msg_window_stall_total",
        "msg_rate_limit_rejected_total",
        "msg_dlq_total",
        "msg_dlq_growth_total",
    )
)
"""带 `tenant,topic` 业务标签的指标。"""

COUNTER_NAMES: ConcurrentStableList[str] = ConcurrentStableList(
    (
        "msg_messages_in_total",
        "msg_bytes_in_total",
        "msg_messages_out_total",
        "msg_publish_errors_total",
        "msg_consume_errors_total",
        "msg_delay_deliver_failed_total",
        "msg_rpc_calls_total",
        "msg_long_poll_changed_total",
        "msg_long_poll_timeout_total",
        "msg_long_poll_rejected_total",
        "msg_window_stall_total",
        "msg_rate_limit_rejected_total",
        "msg_dlq_growth_total",
        "msg_isr_shrink_total",
        "msg_leader_switch_total",
        "msg_auto_election_total",
        "msg_replication_transport_errors_total",
        "msg_storage_rejected_total",
        "msg_delay_rejected_total",
        "msg_replay_replayed_total",
        "msg_replay_failed_total",
        "msg_tenant_lifecycle_events_total",
        "msg_tenant_purge_total",
    )
)
"""计数器指标（单调递增）。"""

GAUGE_NAMES: ConcurrentStableList[str] = ConcurrentStableList(
    (
        "msg_consumer_lag",
        "msg_inflight_messages",
        "msg_inflight_bytes",
        "msg_long_poll_waiters",
        "msg_dlq_total",
        "msg_disk_used_ratio",
        "msg_delay_pending",
        "msg_delay_segments",
        "msg_replay_tasks",
        "msg_isr_size",
        "msg_replication_lag_records",
        "msg_partition_migration_tasks",
        "msg_partition_migration_remaining_records",
    )
)
"""瞬时值指标（可增可减）。"""

HISTOGRAM_NAMES: ConcurrentStableList[str] = ConcurrentStableList(
    (
        "msg_publish_latency_ms",
        "msg_consume_latency_ms",
        "msg_rpc_duration_ms",
        "msg_fsync_duration_ms",
    )
)
"""直方图指标（毫秒桶）。"""

METRIC_HELP: ConcurrentStableDict[str, str] = ConcurrentStableDict(
    {
        "msg_messages_in_total": "发布进入消息数（含延迟发布）。",
        "msg_bytes_in_total": "发布进入消息载荷字节数。",
        "msg_messages_out_total": "消费投递给客户端的消息数（超时为 0）。",
        "msg_publish_latency_ms": "发布处理到追加落盘耗时（毫秒）。",
        "msg_consume_latency_ms": "消费批次读取耗时（毫秒；不含长轮询等待）。",
        "msg_publish_errors_total": "发布失败次数（按原因）。",
        "msg_consume_errors_total": "消费失败次数（按原因）。",
        "msg_delay_deliver_failed_total": "延迟消息到期转投失败次数（按原因）。",
        "msg_rpc_calls_total": "远程调用次数（按方法与结果）。",
        "msg_rpc_duration_ms": "远程调用服务端处理耗时（毫秒）。",
        "msg_fsync_duration_ms": "刷盘耗时（毫秒；按日志 / 延迟区）。",
        "msg_consumer_lag": "消费积压条数（按租户与主题求和）。",
        "msg_inflight_messages": "已投递未确认消息数。",
        "msg_inflight_bytes": "已投递未确认消息字节数。",
        "msg_long_poll_waiters": "长轮询等待器数量（进程内）。",
        "msg_long_poll_changed_total": "长轮询被新消息唤醒次数。",
        "msg_long_poll_timeout_total": "长轮询超时次数。",
        "msg_long_poll_rejected_total": "长轮询因上限被拒次数。",
        "msg_window_stall_total": "传输层暂停次数（按租户与主题）。",
        "msg_rate_limit_rejected_total": "限流拒绝次数（按级别 / 租户 / 主题）。",
        "msg_dlq_total": "当前死信存量。",
        "msg_dlq_growth_total": "累计转入死信条数。",
        "msg_isr_shrink_total": "ISR 收缩次数（02-4 起为真实计数）。",
        "msg_leader_switch_total": "分区切主结果计数（按 result；degraded 表示 isr_verified=false）。",
        "msg_auto_election_total": (
            "自动选举结果计数（按 result：triggered / succeeded / blocked / conflict / exhausted）。"
        ),
        "msg_replication_transport_errors_total": "副本复制 / 控制通道传输失败次数（按操作与原因）。",
        "msg_partition_migration_tasks": "分区迁移台账任务数（按状态）。",
        "msg_partition_migration_remaining_records": "追平中迁移任务的剩余条数之和。",
        "msg_isr_size": "分区当前 ISR 大小（含 leader；标签 topic_id 为数值主键）。",
        "msg_replication_lag_records": "分区副本延迟条数（leader LEO 与 ISR follower LEO 的差值）。",
        "msg_disk_used_ratio": "磁盘使用率（多卷取最大值）。",
        "msg_storage_rejected_total": "存储水位拒绝次数（按原因）。",
        "msg_delay_pending": "延迟区待投条数（按租户）。",
        "msg_delay_segments": "延迟区段数。",
        "msg_delay_rejected_total": "延迟区容量拒绝次数（按范围）。",
        "msg_replay_tasks": "重放任务数（按状态）。",
        "msg_replay_replayed_total": "重放累计写入条数。",
        "msg_replay_failed_total": "重放任务失败次数（按原因）。",
        "msg_tenant_lifecycle_events_total": "租户生命周期事件处理数（按结果）。",
        "msg_tenant_purge_total": "租户到期清理数（按结果）。",
    }
)
"""指标 HELP 文案（未登记名不应出现；测试断言清单与文案一一对应）。"""
