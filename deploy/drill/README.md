# 故障演练（六类注入）

> 任务 07-6-1 的落地说明。场景定义（注入方式 / 观测指标 / 判据锚点 / 恢复预算）唯一来源是
> 本目录的 [`scenarios.toml`](scenarios.toml)；键位模板见 [`drill.env.example`](drill.env.example)。

## 1. 它做什么

用**真装配的隔离实例**（真路由、真文件存储、真水位、真探针、真指标）复现六类故障，逐类产出可机器判读的
记录（JSON + Markdown 同源）。判定结论三态与进程退出码一一对应：**通过 0 / 降级通过 2 / 失败 1**。

## 2. 三条命令

```bash
cd backend
python -m ops.drill plan                 # 校验场景单源并渲染六类表（不启动实例）
python -m ops.drill run <场景> --confirm <场景> [--dry-run]
python -m ops.drill cleanup              # 清理工作根（幂等）
```

- `--dry-run` 只渲染注入与恢复动作：零执行、零写盘。
- 任何注入前都必须 `--confirm <场景名>`；`--env prod|production` 一律**硬拒绝**（无逃生开关）。
- 非 `test` / `dev` / `local` 环境必须给 `MSG_DRILL_BACKUP_EVIDENCE`，否则拒绝执行。

## 3. 隔离实例的形态

| 项 | 取值 | 说明 |
| --- | --- | --- |
| 进程 | `python -m ops.drill.instance` 子进程 | 崩溃场景要「真进程被杀」，进程内证明不了重启后行为 |
| 数据根 | `$MSG_DRILL_WORK_DIR/var` | 每次运行前清空（LEO / 位点 / 死信从零开始） |
| 元数据库 | 该工作根下的 SQLite | 不碰平台 MySQL；用完随目录清理 |
| 监听 | `127.0.0.1:<空闲端口>` | 不对外暴露 |
| 三处替代 | 租户源替身 / `[permission] provider=null` / SQLite | 逐条写入记录的 `deviations[]`，不隐去 |

## 4. 六类场景与判据

| 场景 | 注入 | 观测 | 恢复判据 |
| --- | --- | --- | --- |
| `process_crash` | SIGKILL / SIGTERM 两档，同数据根重启 | 入站量、积压、发布错误 | 已确认不回退、未确认重投、offset 连续、退出确为信号 |
| `disk_full` | 受控填充 + 成对收紧水位（warn < reject） | 磁盘用量、拒绝计数、发布错误 | 507 + `350022`、`/readyz` 503、进程存活、清理后回 200 |
| `slow_disk` | 后台线程持续写同一卷并 fsync | fsync 延迟、发布延迟、积压 | 无假确认、位点不越尾、停压后积压归零 |
| `network_partition` | 客户端侧 TCP 代理：pass / drop / loss / delay | 发布错误、RPC 延迟、积压 | 断连期失败、实例自身健康、恢复可重连、无重复投递 |
| `stuck_consumer` | 只拉不确认直到在途上限 | 在途条数 / 字节、积压 | `paused=true`、在途不超上限、确认后积压归零且不重复 |
| `poison_message` | 非法载荷 + 重投超限 + 主动 nack | 死信总数、消费错误、积压 | 入口拒绝、重投有界、死信可查、不阻塞后续 |

恢复预算与更细的动作清单以 `scenarios.toml` 为准（`plan` 打印的就是它）。

## 5. 记录字段

```text
schema_version / task / scenario / mode / environment{env,target,os,python} / service{version,contract_version}
started_at / finished_at / injection{method,detail,artifact}
samples{before,during,after}（各带 readyz / healthz / 指标子集 / 明细）
criteria[{key,expect,actual,result,passed}] / rto_ms / data_loss / backup{mode,reason}
conclusion{result,exit_code,reasons} / deviations[] / issues[] / leftovers[]
```

- `data_loss` 只有「已确认消息丢失」才为 `true`。
- 失败同样落两份记录（不出现「失败无产物」）。
- 下游按 `criteria[]` + `samples` 组「场景 × 判据 × 证据」表，字段只增不改语义。

## 6. 禁止项

- **不要**在生产环境执行注入；`--env prod` 直接拒绝，不提供覆盖开关。
- **不要**把 `MSG_DRILL_WORK_DIR` 指到真实数据目录：每次运行前会清空其下的 `var/` 与元数据库。
- **不要**在共享环境跳过 `--confirm`：二次确认是硬前置，不是可选参数。
- **不要**把真实地址与凭据写进仓库：演练键位只从进程环境变量读取。
