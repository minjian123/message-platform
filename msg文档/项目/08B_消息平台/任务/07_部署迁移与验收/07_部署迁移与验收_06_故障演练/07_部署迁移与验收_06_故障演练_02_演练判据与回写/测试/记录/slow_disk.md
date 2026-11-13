# 故障演练记录 · 慢盘（受控写入压同一卷）

- 场景：`slow_disk`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T05:51:39.398880+00:00` → `2026-11-11T05:51:50.314487+00:00`
- 恢复耗时：`1285 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：降级通过**（退出码 2）

## 未通过原因

- latency_visible：fsync 0.00 → 0.00 ms；publish 0.00 → 0.00 ms（基线为 0：该指标本次无样本，无法比较）

## 1. 注入

- method：controlled_write
- detail：后台线程循环写 128 MiB 至 slowdisk/pressure.bin 并 fsync
- artifact：D:\Develop\bizs\msg\tmp\drill-final\slowdisk\pressure.bin
- artifact_rel：slowdisk/pressure.bin
- artifact_sha256：254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_fsync_duration_ms` | `msg_publish_latency_ms` | `msg_consumer_lag` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T05:51:49.020742+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 注入中 | 2026-11-11T05:51:49.255826+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 恢复后 | 2026-11-11T05:51:50.305844+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `no_fake_ack` | 无假确认（已确认消息停压后不再投递） | 停压后重投 0 条（期望 0） | 通过 |
| `offset_within_leo` | 位点不越尾（已确认条数 = 发布条数） | 发布 20 条、确认 20 条 | 通过 |
| `lag_drained` | 停压后积压归零（可拉取为空且无在途） | 拉取 0 条、/readyz=200 | 通过 |
| `latency_visible` | `msg_fsync_duration_ms` / 发布延迟相对基线上涨（方向性；绝对阈值归部署阶段） | fsync 0.00 → 0.00 ms；publish 0.00 → 0.00 ms（基线为 0：该指标本次无样本，无法比较） | 降级 |
| `pressure_written` | 受控写入确实执行（压测线程无 I/O 错误） | 写入 128 MiB，错误=无 | 通过 |

## 4. 偏离项

- 慢盘由「受控写入 + fsync 争抢同卷」等价实现，未使用 dm-delay（需 root 与设备操作）
- 本机 SSD 上 I/O 争抢可能不足以抬升 fsync 延迟：该项判「降级」而非「失败」

## 5. 执行期问题

- 无

## 6. 遗留

- item=慢盘延迟绝对阈值未取证（本机 SSD 无样本）；owner=07-6 部署阶段（真盘 I/O 延迟注入，平台级生产部署架构）

## 7. 复现线索

- 配置摘要：`ebe8246a2537cc04c2e181244950b1d4e10f7fae0866d8b3eff43c35cb5331ef`
- 复跑：第 3 次，前次 `2026-11-11T05:03:52.900719+00:00`（降级通过）；原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- 动作轨迹：44 条
- 产物 `var`（data_root，None 字节，sha256 `None`）
- 产物 `instance.log`（log，14857 字节，sha256 `62927e830435b8c4831f8b0438a9ee9a4e7c90faa4c7044893e4c407302465cd`）
- 产物 `slowdisk/pressure.bin`（slowdisk，134217728 字节，sha256 `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`）
- 日志：`instance.log` 共 46 行、14857 字节，sha256 `62927e830435b8c4831f8b0438a9ee9a4e7c90faa4c7044893e4c407302465cd`
