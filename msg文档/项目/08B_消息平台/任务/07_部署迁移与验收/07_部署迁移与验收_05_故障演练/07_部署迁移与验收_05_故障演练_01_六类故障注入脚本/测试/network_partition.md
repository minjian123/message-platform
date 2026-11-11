# 故障演练记录 · 网络分区（客户端侧 TCP 代理）

- 场景：`network_partition`
- 模式：`run`
- 环境：`test` / 目标 `local`
- 服务版本：`0.1.0`
- 起止：`2026-11-11T02:46:14.256097+00:00` → `2026-11-11T02:46:31.171435+00:00`
- 恢复耗时：`5308 ms`
- 已确认消息丢失：`否`
- 演练前备份：`not_applicable`（隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据）
- **结论：通过**（退出码 0）

## 1. 注入

- method：tcp_proxy
- detail：代理端口 56699 → 实例端口 18200，档位 pass/drop/loss/delay
- artifact：D:\Develop\bizs\msg\tmp\drill

## 2. 三段观测

| 阶段 | 时刻 | /readyz | /healthz | `msg_publish_errors_total` | `msg_rpc_duration_ms` | `msg_consumer_lag` |
| --- | --- | --- | --- | --- | --- | --- |
| 注入前 | 2026-11-11T02:46:25.598455+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 注入中 | 2026-11-11T02:46:25.848521+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |
| 恢复后 | 2026-11-11T02:46:31.126277+00:00 | 200（就绪） | 200 | 0 | 0 | 0 |

## 3. 判据

| 判据 | 期望 | 实测 | 结果 |
| --- | --- | --- | --- |
| `proxy_passthrough` | 代理直通档可用（经代理发布 2xx） | HTTP 200 | 通过 |
| `drop_blocks_publish` | 断连档发布失败（客户端可见的连接错误） | 发布被拒（连接错误） | 通过 |
| `instance_alive_during_partition` | 断连期实例本身健康（故障只在客户端链路） | /readyz=200、/healthz=200 | 通过 |
| `reconnected` | 恢复后经代理发布成功（客户端重连） | HTTP 200 | 通过 |
| `no_duplicate_delivery` | 已确认消息不重复投递（位点单调不回退） | 已确认 3 条，恢复后投出 [] | 通过 |
| `lag_drained` | 恢复后积压追平（确认完可拉取为空） | 残留 0 条 | 通过 |
| `modes_switchable` | 三档模式可切换（loss 后仍可发、delay 档发布成功） | loss 后继续发布，delay 档 HTTP 200 | 通过 |

## 4. 偏离项

- 分区由客户端侧本地 TCP 代理实现（drop / loss / delay），未断容器网络
- 客户端重连由本脚本驱动（每次调用新建/复用连接），未覆盖 SDK 内建退避策略

## 5. 执行期问题

- 无

## 6. 遗留

- 无
