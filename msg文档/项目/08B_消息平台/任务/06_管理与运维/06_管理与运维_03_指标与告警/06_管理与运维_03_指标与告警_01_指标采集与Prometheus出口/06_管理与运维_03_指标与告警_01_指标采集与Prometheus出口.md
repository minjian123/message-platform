# 06-3-1 指标采集与 Prometheus 出口

> 08B · 06 管理与运维 · 06-3 指标与告警 · 子任务 01（需求 06-3）

[文档首页](../../../../../../文档首页.md) › [消息平台规划](../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../计划/01_计划_消息平台.md) › [06 管理与运维](../../06_管理与运维.md) › [06-3 指标与告警](../06_管理与运维_03_指标与告警.md) › 06-3-1

## 1. 任务信息 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 编号 | 06-3-1 |
| 父任务 | [06-3 指标与告警](../06_管理与运维_03_指标与告警.md) |
| 对应需求 | [06-3 指标与告警](../../../../需求/06_需求_管理与运维.md#r06-3) |
| 依赖 | 02-6（存储快照）、02-7（延迟快照）、03-5（背压快照）、04-1 / 04-2 / 04-7-2（远程调用事件）、05-1（生命周期）、01-6（重放）、01-2（现有死信队列指标源） |
| 负责人 | minjian |

## 2. 任务内容 <a id="content"></a>

1. **指标注册与导出**：新增统一指标模块，把既有采集器快照转成 Prometheus 文本格式；`GET /metrics` 输出 `text/plain; version=0.0.4`；端点只读、无副作用、仅内部网络暴露（公网 / 网关不挂载）。
2. **指标清单**：至少覆盖——
   - 流量：`msg_messages_in_total`、`msg_bytes_in_total`、`msg_messages_out_total`；
   - 延迟：`msg_publish_latency_ms`、`msg_consume_latency_ms`（Histogram，P50 / P95 / P99 由 Prometheus 计算）；
   - 错误：`msg_publish_errors_total{reason}`、`msg_consume_errors_total{reason}`、`msg_delay_deliver_failed_total`；
   - 积压与在途：`msg_consumer_lag`、`msg_inflight_messages`、`msg_inflight_bytes`、`msg_long_poll_waiters`；
   - 死信：`msg_dlq_total`、`msg_dlq_growth_total`；
   - 复制与存储：`msg_isr_shrink_total`（02-5 交付前为 0）、`msg_fsync_duration_ms`、`msg_disk_used_ratio`、`msg_storage_rejected_total`；
   - 延迟区：`msg_delay_pending`、`msg_delay_segments`、`msg_delay_rejected_total`；
   - 重放：`msg_replay_tasks`、`msg_replay_replayed_total`、`msg_replay_failed_total`；
   - 限流 / 背压：`msg_window_stall_total`、`msg_rate_limit_rejected_total`、`msg_long_poll_{changed,timeout,rejected}_total`。
3. **标签纪律**：只允许 `service` / `tenant` / `topic` 作为业务标签；`partition` / `group` 不进标签；`(tenant, topic)` 超过 `METRICS_TOPIC_KEY_LIMIT` 归入 `__other__`；`reason` / `scope` 等枚举标签取值收敛，禁止用户输入直接作标签。
4. **数据来源**：直接消费既有 `snapshot()` 与事件；重放 / 死信指标从服务层计数读取（死信先取 01-2 现有队列，06-5 持久化后接口不变）；不新增第二套采集器、不在每次请求内全量扫描日志或组。
5. **性能**：`/metrics` 单次响应耗时有上限；快照读取不做 I/O 重活；采集开销基准（有 / 无指标端点）留痕，不显著影响吞吐。
6. **契约与配置**：指标名 / 标签 / 类型集中定义，供 06-3-2 规则与 06-1-2 看板引用；不新增业务配置项，端口 / 路径固定。
7. **测试**：指标名 / 类型 / 标签 / 数值断言；`/metrics` 文本可被 Prometheus 解析；高基数归并；空数据 / 异常快照降级；采集开销基准；变更模块行 + 分支覆盖率 100%。

## 3. 完成标准 <a id="accept"></a>

- `/metrics` 可被 Prometheus 直接抓取，核心指标齐全；
- 标签只到 `service` / `tenant` / `topic`，无 `partition` / `group` 标签；
- 指标值与既有采集器快照一致，无第二套计数；
- 高基数场景归入 `__other__`，内存有界；
- 采集不显著影响吞吐，基准结果留痕；
- 变更模块行 + 分支覆盖率 100%，静态检查全绿。

## 4. 参考文档 <a id="ref"></a>

- [需求 06-3 指标与告警](../../../../需求/06_需求_管理与运维.md#r06-3)
- 《[架构设计 · 控制面：元数据与运维](../../../../../../设计/架构设计/06_架构设计_控制面_元数据与运维.md)》第 5 节
- bms《[可观测性](../../../../../../../bms文档/设计/架构设计/27_架构设计_子系统_可观测性.md)》第 3 节指标采集
- bms《[运维与可观测](../../../../../../../bms文档/资料/知识档案/消息与事件驱动/10_事件驱动_运维与可观测.md)》第 2~6 节
- [06-3-2 告警规则与分级抑制恢复](../06_管理与运维_03_指标与告警_02_告警规则与分级抑制恢复/06_管理与运维_03_指标与告警_02_告警规则与分级抑制恢复.md)（规则引用同一指标清单）

## 5. 前置契约（已交付） <a id="prereq"></a>

| 前置 | 已交付契约 | 对本任务的约束 |
| --- | --- | --- |
| [02-6 存储水位与拒绝策略](../../../02_存储与复制/02_存储与复制_06_存储水位与拒绝策略/02_存储与复制_06_存储水位与拒绝策略.md) | `StorageMetrics.snapshot()` | 直接转格式，不改字段 |
| [02-7 延迟消息持久化与到期恢复](../../../02_存储与复制/02_存储与复制_07_延迟消息持久化与到期恢复/02_存储与复制_07_延迟消息持久化与到期恢复.md) | `DelayMetrics.snapshot()`、`stale_count()` | 延迟区指标同源 |
| [03-5 背压与流控](../../../03_传输与协议/03_传输与协议_05_背压与流控/03_传输与协议_05_背压与流控.md) | `BackpressureMetrics.snapshot()` | inflight / 限流 / 等待指标同源 |
| [05-1 租户用量板块与表单登记](../../../05_多租户与安全/05_多租户与安全_01_租户用量板块与表单登记/05_多租户与安全_01_租户用量板块与表单登记.md) | 生命周期指标快照 | 指标出口统一，不单独暴露 |
