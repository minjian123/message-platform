# 故障演练记录 · 磁盘满（受控填充 + 收紧拒绝水位）

- 场景：`disk_full`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T05:51:25.319835+00:00` → `2026-11-11T05:51:39.301640+00:00`
- 恢复耗时：`4346 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：通过**（退出码 0）

## 1. 注入

- method：fill_volume
- detail：填充 1527 MiB 至 fill/volume.bin，拒绝水位收紧至 0.3574
- artifact：D:\Develop\bizs\msg\tmp\drill-final\fill\volume.bin
- artifact_rel：fill/volume.bin

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_disk_used_ratio` | `msg_storage_rejected_total` | `msg_publish_errors_total` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T05:51:34.952674+00:00 | 200（就绪） | 200 | 0.3570 | 0 | 0 |
| 注入中 | 2026-11-11T05:51:37.974921+00:00 | 503（未就绪） | 200 | 0.3574 | 0 | 0 |
| 恢复后 | 2026-11-11T05:51:39.289287+00:00 | 200（就绪） | 200 | 0.3570 | 1 | 1 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `publish_rejected` | 满盘期发布被拒（HTTP 507 + 业务码 350022） | HTTP 507、业务码 350022 | 通过 |
| `readyz_degraded` | 满盘期 /readyz 转 503（不摘实例但不再接写） | /readyz=503 | 通过 |
| `ratio_risen` | `msg_disk_used_ratio` 随填充上升 | 0.3570 → 0.3574 | 通过 |
| `process_alive` | 进程存活（/healthz 仍 200，磁盘满不致死） | /healthz=200 | 通过 |
| `recovered` | 清理填充后恢复可写、/readyz 回 200 | 发布 HTTP 200、/readyz=200 | 通过 |
| `rto_within_budget` | 恢复耗时在预算内 | 4346 ms（预算 240000 ms） | 通过 |

## 4. 偏离项

- 本机无可配额卷：用「收紧拒绝水位 + 受控填充」等价「临时卷被填满」，容器通道为 tmpfs 限额卷
- 填充文件落在演练工作根（与数据根同卷），清理即删除，不触碰其他目录

## 5. 执行期问题

- 无

## 6. 遗留

- 无

## 7. 复现线索

- 配置摘要：`db98dbb5294657b3630a562b621a96e3b8d1a0bd09344e4bfd2438708d32cd51`
- 复跑：第 3 次，前次 `2026-11-11T05:03:38.890304+00:00`（通过）；原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- 动作轨迹：5 条
- 产物 `var`（data_root，None 字节，sha256 `None`）
- 产物 `instance.log`（log，2379 字节，sha256 `1b08da15d49be44c65b73c30f7bcfe806eeb049e2457cb29eb53503a7573d276`）
- 产物 `slowdisk/pressure.bin`（slowdisk，134217728 字节，sha256 `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`）
- 日志：`instance.log` 共 8 行、2379 字节，sha256 `1b08da15d49be44c65b73c30f7bcfe806eeb049e2457cb29eb53503a7573d276`
