# 发版前故障演练检查清单（生成物）

> 本文件由 `python -m ops.drill checklist` 从 `deploy/drill/scenarios.toml` 生成，**勿手改**。
> 重新生成：`cd backend && python -m ops.drill checklist`。

## 1. 前置门禁

- [ ] 环境不是 `prod` / `production`（硬拒绝，无逃生开关）
- [ ] 已显式传 `--confirm release`（逐条执行时为 `--confirm <场景>`）
- [ ] 非隔离环境已给 `MSG_DRILL_BACKUP_EVIDENCE`（隔离实例记不适用 + 理由）
- [ ] `MSG_DRILL_WORK_DIR` 指向隔离临时目录，不是真实数据目录
- [ ] `MSG_DRILL_REPORT_DIR` 已归档上一轮记录（本轮记录不覆盖历史证据）

## 2. 执行命令

```bash
cd backend
python -m ops.drill release --confirm release --rerun-reason "发版前复跑"
```

失败不中断：六类全部跑完再汇总，退出码取最差（失败 1 > 降级通过 2 > 通过 0）。

## 3. 六类场景（按声明序）

| # | 场景 | 命令 | 期望观察 | 记录 |
| --- | --- | --- | --- | --- |
| 1 | `process_crash` | `python -m ops.drill run process_crash --confirm process_crash` | 两档各一轮：杀进程后以同一数据根重启，已确认不丢、未确认重投、offset 连续 | `MSG_DRILL_REPORT_DIR/process_crash.json` |
| 2 | `disk_full` | `python -m ops.drill run disk_full --confirm disk_full` | 注入期拒绝写且就绪转 503，清理后自动恢复；进程全程不退出 | `MSG_DRILL_REPORT_DIR/disk_full.json` |
| 3 | `slow_disk` | `python -m ops.drill run slow_disk --confirm slow_disk` | 压测写入期间发布确认仍正确；延迟上升只作方向性判定，绝对阈值归部署阶段 | `MSG_DRILL_REPORT_DIR/slow_disk.json` |
| 4 | `network_partition` | `python -m ops.drill run network_partition --confirm network_partition` | 代理切 drop 后发布失败、切回 pass 后成功；实例自身探针全程正常 | `MSG_DRILL_REPORT_DIR/network_partition.json` |
| 5 | `stuck_consumer` | `python -m ops.drill run stuck_consumer --confirm stuck_consumer` | 在途达上限即暂停推送；逐条确认后立即恢复，offset 唯一 | `MSG_DRILL_REPORT_DIR/stuck_consumer.json` |
| 6 | `poison_message` | `python -m ops.drill run poison_message --confirm poison_message` | 三段分别取证：入口拒绝、重投有界转死信、主动拒绝即时终结；均不阻塞后续 | `MSG_DRILL_REPORT_DIR/poison_message.json` |

## 4. 判定口径

- 结论三态：**通过 0 / 降级通过 2 / 失败 1**，与 07-3 / 07-4 / 07-6-1 同口径。
- **降级不计通过**：可降级项无样本或未闭环时记「降级通过」，须人工复核并在签核栏写明。
- `data_loss=true` 或任一硬判据非 `ok` 直接判失败，**阻断发版**。
- 未通过项必须登记归口（`issues[]` / `leftovers[]` 的 owner），否则矩阵判失败。

## 5. 版本与环境记录

| 项 | 值 |
| --- | --- |
| 服务版本 | |
| 契约版本 | |
| git 提交 | |
| 环境 / 目标 | |
| 配置摘要 | |
| 执行人 / 日期 | |

## 6. 签核

| 场景 | 结论 | 记录 | 复核人 |
| --- | --- | --- | --- |
| `process_crash` | | `process_crash.json` | |
| `disk_full` | | `disk_full.json` | |
| `slow_disk` | | `slow_disk.json` | |
| `network_partition` | | `network_partition.json` | |
| `stuck_consumer` | | `stuck_consumer.json` | |
| `poison_message` | | `poison_message.json` | |

## 7. 判据速查

| 场景 | 注入方式 | 观测指标 | 恢复判据 |
| --- | --- | --- | --- |
| `process_crash` | `sigkill_sigterm` | `msg_messages_in_total`、`msg_consumer_lag`、`msg_publish_errors_total` | 已确认消息不回退（恢复后不重投已确认 offset）；未确认消息重新投递（至少一次语义）；重启不重排 offset（新消息 offset 连续）；实例确实被信号终止（恢复耗时由 G5 判） |
| `disk_full` | `fill_volume` | `msg_disk_used_ratio`、`msg_storage_rejected_total`、`msg_publish_errors_total` | 满盘期发布被拒（HTTP 507 + 业务码 350022）；满盘期 /readyz 转 503；用量指标随填充上升且进程存活；清理填充后恢复可写、/readyz 回 200 |
| `slow_disk` | `controlled_write` | `msg_fsync_duration_ms`、`msg_publish_latency_ms`、`msg_consumer_lag` | 无假确认（已确认消息停压后不再投递）；位点不越尾（已确认位点与发布条数一致）；停压后积压归零；fsync / 发布延迟相对基线上涨（方向性） |
| `network_partition` | `tcp_proxy` | `msg_publish_errors_total`、`msg_rpc_duration_ms`、`msg_consumer_lag` | 断连档发布失败（客户端可见的连接错误）；断连期实例本身健康（故障只在链路）；恢复后客户端可重连并发布成功；已确认消息不重复投递、积压追平 |
| `stuck_consumer` | `unacked_inflight` | `msg_inflight_messages`、`msg_inflight_bytes`、`msg_consumer_lag` | 在途填满后 consume 返回 paused=true（背压生效）；在途条数不超过上限（内存有界）；积压可观测（msg_consumer_lag 上升）；确认后恢复消费、积压归零且不重复 |
| `poison_message` | `invalid_and_exhausted` | `msg_dlq_total`、`msg_consume_errors_total`、`msg_consumer_lag` | 非法载荷被入口拒绝（HTTP 4xx 或业务码非 0）；重投次数不超过上限（5）；重投超限后进死信（msg_dlq_total 增长）；死信管理面可查（按 message_id 命中）；毒消息终结后同分区后续消息可消费（不阻塞）；主动 nack 即时转死信；主动 nack 的死信管理面可查 |
