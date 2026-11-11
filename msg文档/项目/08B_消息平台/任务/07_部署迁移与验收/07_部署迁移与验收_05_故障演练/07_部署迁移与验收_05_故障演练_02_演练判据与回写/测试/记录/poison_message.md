# 故障演练记录 · 毒消息（入口拒绝 / 重投超限 / 主动拒绝）

- 场景：`poison_message`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T05:52:16.552755+00:00` → `2026-11-11T05:56:17.728249+00:00`
- 恢复耗时：`231534 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：降级通过**（退出码 2）

## 未通过原因

- dead_letter_queryable：死信列表不可查（HTTP 500，{"code":10000,"message":"error.10000","data":null}）
- not_blocking_followers：毒消息在 220s 内未终结（重投已达 5 次），后续消息按停等被阻塞
- nack_queryable：死信列表不可查（HTTP 500，{"code":10000,"message":"error.10000","data":null}）

## 1. 注入

- method：invalid_and_exhausted
- detail：非法载荷 + 重投超限（上限 5）+ 主动 nack
- artifact：D:\Develop\bizs\msg\tmp\drill-final
- artifact_rel：.

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_dlq_total` | `msg_consume_errors_total` | `msg_consumer_lag` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T05:52:26.189024+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 注入中 | 2026-11-11T05:52:26.240032+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 恢复后 | 2026-11-11T05:56:17.725758+00:00 | 200（就绪） | 200 | 2 | 0 | 0 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `invalid_payload_rejected` | 非法载荷被入口拒绝（HTTP 4xx 或业务码非 0） | HTTP 200、业务码 10001 | 通过 |
| `redelivery_bounded` | 重投次数不超过上限（5） | 最大 delivery_count=5 | 通过 |
| `dead_letter_created` | 重投超限后进死信（msg_dlq_total 增长） | msg_dlq_total 0 → 2 | 通过 |
| `dead_letter_queryable` | 死信管理面可查（按 message_id 命中） | 死信列表不可查（HTTP 500，{"code":10000,"message":"error.10000","data":null}） | 降级 |
| `not_blocking_followers` | 毒消息终结后同分区后续消息可消费（不阻塞） | 毒消息在 220s 内未终结（重投已达 5 次），后续消息按停等被阻塞 | 降级 |
| `nack_to_dlq` | 主动 nack 即时转死信 | nack HTTP 200；msg_dlq_total 2 → 3 | 通过 |
| `nack_queryable` | 主动 nack 的死信管理面可查 | 死信列表不可查（HTTP 500，{"code":10000,"message":"error.10000","data":null}） | 降级 |

## 4. 偏离项

- 可见性超时为常量 30s、重投上限 5 次，本场景按预算等待；提速需 02-2/02-3 开放旋钮
- 死信库为 SQLite（隔离实例），元数据结构与生产一致

## 5. 执行期问题

- 无

## 6. 遗留

- item=管理面死信列表在有死信行时返回 500（MissingGreenlet：ACL 释放后读 ORM 属性）；owner=06-5 死信管理（生产同样触发，需 ACL 释放与 ORM 过期解耦后复验本场景）
- item=毒消息终结后同分区后续消息未在预算内证明可消费（停等阻塞）；owner=02-3 消费位点管理 / 02-2 崩溃恢复（定位位点推进与死信转移的先后）

## 7. 复现线索

- 配置摘要：`de82c431f1946f0b4b7e93187a355ddbee8c68ec7ba4a1eb5cfaf736af45b291`
- 复跑：第 6 次，前次 `2026-11-11T05:21:12.231645+00:00`（降级通过）；原因：07-6-2 最终取证复跑（判据拆分 + 记录增强）
- 动作轨迹：100 条
- 产物 `var`（data_root，None 字节，sha256 `None`）
- 产物 `instance.log`（log，56441 字节，sha256 `d825e17cd0faa1e0479b6320eccfc66da097b0bbaa99c86fb9b522f602d46556`）
- 产物 `slowdisk/pressure.bin`（slowdisk，134217728 字节，sha256 `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`）
- 日志：`instance.log` 共 442 行、56441 字节，sha256 `d825e17cd0faa1e0479b6320eccfc66da097b0bbaa99c86fb9b522f602d46556`
