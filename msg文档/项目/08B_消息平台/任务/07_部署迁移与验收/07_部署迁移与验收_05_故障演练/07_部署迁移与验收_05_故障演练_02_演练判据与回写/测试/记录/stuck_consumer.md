# 故障演练记录 · 消费者卡死（只拉不确认填满 inflight）

- 场景：`stuck_consumer`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T05:52:05.676483+00:00` → `2026-11-11T05:52:16.449497+00:00`
- 恢复耗时：`0 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：通过**（退出码 0）

## 1. 注入

- method：unacked_inflight
- detail：发布 105 条后只拉不确认，填满在途上限 100
- artifact：D:\Develop\bizs\msg\tmp\drill-final
- artifact_rel：.

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_inflight_messages` | `msg_inflight_bytes` | `msg_consumer_lag` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T05:52:15.315900+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 注入中 | 2026-11-11T05:52:15.932178+00:00 | 200（就绪） | 200 | 100 | 790 | 0 |
| 恢复后 | 2026-11-11T05:52:16.441980+00:00 | 200（就绪） | 200 | 5 | 45 | 0 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `backpressure_applied` | 在途填满后 consume 返回 `paused=true`（背压生效） | paused=True、inflight_messages=100（上限 100） | 通过 |
| `inflight_bounded` | 在途条数不超过上限（内存有界） | inflight_messages=100 ≤ 100 | 通过 |
| `backlog_visible` | 积压可观测（在途条数 > 0 或 `msg_consumer_lag` 上升） | 在途 100 条、lag 0 → 0 | 通过 |
| `resumed_after_ack` | 确认后恢复消费且积压归零（不重复消费） | 确认 105 条，残留 0 条 | 通过 |
| `no_duplicate` | 恢复期投递不重复（offset 唯一且单调） | 投出 100 条，唯一 100 条 | 通过 |

## 4. 偏离项

- 单分区单消费者（多成员分配与再平衡不在本类判据内）

## 5. 执行期问题

- 无

## 6. 遗留

- 无

## 7. 复现线索

- 配置摘要：`be71921bb5213e6054f54150af42a16f4ef7b61ff42acab275375842ca0178fb`
- 复跑：第 3 次，前次 `2026-11-11T05:04:21.373565+00:00`（通过）；原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- 动作轨迹：201 条
- 产物 `var`（data_root，None 字节，sha256 `None`）
- 产物 `instance.log`（log，71107 字节，sha256 `2f7fc91cf56b6cd804c4438b8d77b8b5baf2d52da44476f088d4c6153783429b`）
- 产物 `slowdisk/pressure.bin`（slowdisk，134217728 字节，sha256 `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`）
- 日志：`instance.log` 共 217 行、71107 字节，sha256 `2f7fc91cf56b6cd804c4438b8d77b8b5baf2d52da44476f088d4c6153783429b`
