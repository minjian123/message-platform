# 故障演练（六类注入与判据）

> 任务 07-6-1 落地六类注入；任务 07-6-2 补判据表、验收矩阵与发版前复跑。
> 场景定义（注入方式 / 观测指标 / 判据 / 恢复预算）唯一来源是本目录的
> [`scenarios.toml`](scenarios.toml)；键位模板见 [`drill.env.example`](drill.env.example)；
> 发版前清单是生成物 [`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md)。

## 1. 它做什么

用**真装配的隔离实例**（真路由、真文件存储、真水位、真探针、真指标）复现六类故障，逐类产出可机器判读的
记录（JSON + Markdown 同源），再按单源判据对账成「场景 × 判据 × 证据」矩阵。结论三态与进程退出码一一对应：
**通过 0 / 降级通过 2 / 失败 1**；降级不计通过，`data_loss=true` 或硬判据非 `ok` 直接判失败。

## 2. 命令

```bash
cd backend
python -m ops.drill plan                 # 校验场景单源并渲染六类表（不启动实例）
python -m ops.drill criteria [--json]    # 渲染判据表（级别 / 记录键 / 证据要求）
python -m ops.drill checklist [--check]  # 生成或校验发版前检查清单
python -m ops.drill run <场景> --confirm <场景> [--dry-run] [--rerun-reason <原因>]
python -m ops.drill verify --records <目录> [--records <目录>] [--include <记录>] \
    [--out <目录>] [--archive <目录>] [--check-doc <md>]
python -m ops.drill release --confirm release [--rerun-reason <原因>] [--dry-run]
python -m ops.drill cleanup              # 清理工作根（幂等）
```

- 任何注入前都必须 `--confirm <场景名>`（发版复跑为 `--confirm release`）；`--env prod|production` 一律**硬拒绝**。
- 非 `test` / `dev` / `local` 环境必须给 `MSG_DRILL_BACKUP_EVIDENCE`，否则拒绝执行。
- `verify` 的 `--records` 可重复；同场景多份记录取 `started_at` 最新一份，历史非通过记录必须有后继复跑。
- `--archive` 把参与对账的记录复制到 `<目录>/记录/` 并写 sha256；`--check-doc` 检查指定 Markdown 的相对链接。

## 3. 隔离实例的形态

| 项 | 取值 | 说明 |
| --- | --- | --- |
| 进程 | `python -m ops.drill.instance` 子进程 | 崩溃场景要「真进程被杀」，进程内证明不了重启后行为 |
| 数据根 | `$MSG_DRILL_WORK_DIR/var` | 每次运行前清空（LEO / 位点 / 死信从零开始） |
| 元数据库 | 该工作根下的 SQLite | 不碰平台 MySQL；用完随目录清理 |
| 监听 | `127.0.0.1:<空闲端口>` | 不对外暴露 |
| 三处替代 | 租户源替身 / `[permission] provider=null` / SQLite | 逐条写入记录的 `deviations[]`，不隐去 |

## 4. 六类场景

| 场景 | 注入 | 观测 | 恢复判据（摘要） |
| --- | --- | --- | --- |
| `process_crash` | SIGKILL / SIGTERM 两档，同数据根重启 | 入站量、积压、发布错误 | 已确认不回退、未确认重投、offset 连续、退出确为信号 |
| `disk_full` | 受控填充 + 成对收紧水位（warn < reject） | 磁盘用量、拒绝计数、发布错误 | 507 + `350022`、`/readyz` 503、进程存活、清理后回 200 |
| `slow_disk` | 后台线程持续写同一卷并 fsync | fsync 延迟、发布延迟、积压 | 无假确认、位点不越尾、停压后积压归零 |
| `network_partition` | 客户端侧 TCP 代理：pass / drop / loss / delay | 发布错误、RPC 延迟、积压 | 断连期失败、实例自身健康、恢复可重连、无重复投递 |
| `stuck_consumer` | 只拉不确认直到在途上限 | 在途条数 / 字节、积压 | `paused=true`、在途不超上限、确认后积压归零且不重复 |
| `poison_message` | 非法载荷 + 重投超限 + 主动 nack | 死信总数、消费错误、积压 | 入口拒绝、重投有界、进死信、可查、不阻塞后续、nack 转死信 |

逐条判据（含级别与记录键）以 `python -m ops.drill criteria` 为准；`scenarios.toml` 是唯一事实源。

## 5. 记录字段（schema 2，只增不改语义）

```text
schema_version / task / scenario / mode
environment{env,target,os,python,config_digest}
service{name,version,contract_version}
started_at / finished_at / injection{method,detail,artifact,artifact_rel,artifact_sha256}
commands[{seq,action,detail}] / artifacts[{path,role,bytes,sha256}] / logs{path,lines,bytes,sha256,excerpt}
samples{before,during,after}（各带 readyz / healthz / 指标子集 / 明细）
criteria[{key,expect,actual,result,passed}] / rto_ms / data_loss / backup{mode,reason}
rerun{attempt,previous_started_at,previous_conclusion,reason}
conclusion{result,exit_code,reasons} / deviations[] / issues[] / leftovers[]
```

- `data_loss` 只有「已确认消息丢失」才为 `true`。
- 失败同样落两份记录（不出现「失败无产物」）。
- `commands[]` / `logs` 只作复现线索，不进入通过判据；判据只认 `criteria[]` 与 `samples`。
- v1 记录仍可判读，缺增强字段在矩阵里记降级并提示复跑。

## 6. 发版前复跑

```bash
cd backend
# 工作区根 tmp/msg/ 是唯一合法临时落点；两个变量的相对基准不同，勿混用
export MSG_DRILL_WORK_DIR=../../tmp/msg/drill          # 相对 cwd（backend/）解析
export MSG_DRILL_REPORT_DIR=../tmp/msg/drill/reports   # 相对仓根（msg/）解析
python -m ops.drill checklist --check            # 清单未漂移
python -m ops.drill release --confirm release    # 六类全跑 → 记录 → 矩阵 → release.json
```

- 失败不中断：六类全部跑完再汇总，退出码取最差。
- `release.json` / `release.md` 记录版本、配置摘要、git 提交与每份记录的 sha256，便于跨版本比较。
- 清单由 `scenarios.toml` 生成；判据或顺序调整后重新生成并提交。

## 7. 禁止项

- **不要**在生产环境执行注入；`--env prod` 直接拒绝，不提供覆盖开关。
- **不要**把 `MSG_DRILL_WORK_DIR` 指到真实数据目录：每次运行前会清空其下的 `var/` 与元数据库。
- **不要**在共享环境跳过 `--confirm`：二次确认是硬前置，不是可选参数。
- **不要**手改 `RELEASE_CHECKLIST.md`：它是生成物，`checklist --check` 会判漂移。
- **不要**把真实地址与凭据写进仓库：演练键位只从进程环境变量读取。

## 9. 多实例场景（02-8）

`scenarios.toml` 新增可选 `target`（缺省 `local`）：本机六类保持 `target=local`；多实例三类为
`replica_crash` / `network_partition_multi` / `split_brain_multi`（`target=docker`），由
`python -m ops.cluster_drill` 在开发服务器上执行（独立项目 / 容器名 / 卷 / 演练库前缀）。

```bash
cd backend
python -m ops.cluster_drill plan
python -m ops.cluster_drill run replica_crash --confirm replica_crash [--skip-provision] [--topic-id N]
python -m ops.cluster_drill run network_partition_multi --confirm network_partition_multi
python -m ops.cluster_drill run split_brain_multi --confirm split_brain_multi
python -m ops.cluster_drill verify --records <记录目录>
python -m ops.cluster_drill cleanup
```

- 演练环境键位见 `cluster.env.example`（真实主机 / 凭据只落 `cluster.env`，不入库）。
- `--env prod|production` 硬拒绝；任何注入前必须 `--confirm <场景>`；退出码 `0` 通过 / `1` 失败 / `2` 降级。
- `force` 或 `isr_verified=false` 只记「降级」，不计通过；`data_loss=true` 或硬判据非 `ok` 直接失败。
- `ops.drill run` 只跑本机六类；对 `target=docker` 场景会提示改用 `ops.cluster_drill`。
