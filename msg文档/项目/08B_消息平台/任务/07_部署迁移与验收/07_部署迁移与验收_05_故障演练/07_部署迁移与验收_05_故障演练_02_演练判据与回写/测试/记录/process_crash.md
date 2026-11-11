# 故障演练记录 · 进程崩溃（SIGKILL / SIGTERM 两档）

- 场景：`process_crash`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T05:50:44.198451+00:00` → `2026-11-11T05:51:25.190162+00:00`
- 恢复耗时：`13616 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：通过**（退出码 0）

## 1. 注入

- method：sigkill_sigterm
- detail：两档各发布 6 条、确认 4 条后杀进程并以同一数据根重启
- artifact：D:\Develop\bizs\msg\tmp\drill-final
- artifact_rel：.

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_messages_in_total` | `msg_consumer_lag` | `msg_publish_errors_total` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T05:50:55.909740+00:00 | 200（就绪） | 200 | 6 | 0 | 0 |
| 注入中 | 2026-11-11T05:50:57.938695+00:00 | 0（未就绪） | 不可达 | 0 | 0 | 0 |
| 恢复后 | 2026-11-11T05:51:09.508989+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 注入前 | 2026-11-11T05:51:11.622504+00:00 | 200（就绪） | 200 | 1 | 0 | 0 |
| 注入中 | 2026-11-11T05:51:13.657112+00:00 | 0（未就绪） | 不可达 | 0 | 0 | 0 |
| 恢复后 | 2026-11-11T05:51:25.172150+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `SIGKILL.confirmed_not_rewound` | 已确认消息不回退（恢复后不重投 offset < 已确认位点） | 已确认 4 条，恢复后投出 [4, 5] | 通过 |
| `SIGKILL.unacked_redelivered` | 未确认 2 条重新投递（至少一次语义） | 恢复后投出 [4, 5]，期望含 [4, 5] | 通过 |
| `SIGKILL.leo_not_rewound` | 重启不重排 offset（新消息 offset 连续） | 新消息 offset=6，期望 6 | 通过 |
| `SIGKILL.killed_by_signal` | 实例确实被信号终止（非正常退出） | 信号 SIGKILL，退出码 1 | 通过 |
| `SIGKILL.rto_within_budget` | 恢复耗时在预算内 | 13616 ms（预算 240000 ms） | 通过 |
| `SIGTERM.confirmed_not_rewound` | 已确认消息不回退（恢复后不重投 offset < 已确认位点） | 已确认 4 条，恢复后投出 [4, 5] | 通过 |
| `SIGTERM.unacked_redelivered` | 未确认 2 条重新投递（至少一次语义） | 恢复后投出 [4, 5]，期望含 [4, 5] | 通过 |
| `SIGTERM.leo_not_rewound` | 重启不重排 offset（新消息 offset 连续） | 新消息 offset=6，期望 6 | 通过 |
| `SIGTERM.killed_by_signal` | 实例确实被信号终止（非正常退出） | 信号 SIGTERM，退出码 1 | 通过 |
| `SIGTERM.rto_within_budget` | 恢复耗时在预算内 | 13565 ms（预算 240000 ms） | 通过 |

## 4. 偏离项

- 位点提交是「标脏 + 定期落盘」，确认后等一个刷盘窗口再杀进程（否则脏页丢失会整段重投）
- 在途状态为进程内存，重启后 delivery_count 从 1 重新计；未确认消息靠位点重投保证不丢
- 成员会话同为进程内存：重启后脚本按客户端真实动作重新 join，未沿用旧会话
- 元数据库用 SQLite（生产为平台 MySQL），数据面与文件存储为真实现

## 5. 执行期问题

- 无

## 6. 遗留

- 无

## 7. 复现线索

- 配置摘要：`b57dac463e3c723844c614c36ccfb22a23d2ad43a33dd56ac6e8599b4f169bcb`
- 复跑：第 3 次，前次 `2026-11-11T05:02:56.710350+00:00`（通过）；原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- 动作轨迹：34 条
- 产物 `var`（data_root，None 字节，sha256 `None`）
- 产物 `instance.log`（log，12352 字节，sha256 `88aa74cecc49dca04af3c44fab5b5611a1a439125e8f4db7ecaa43462ceab280`）
- 产物 `slowdisk/pressure.bin`（slowdisk，134217728 字节，sha256 `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`）
- 日志：`instance.log` 共 40 行、12352 字节，sha256 `88aa74cecc49dca04af3c44fab5b5611a1a439125e8f4db7ecaa43462ceab280`
