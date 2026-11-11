# msg 恢复演练单元说明

> 恢复顺序、两种执行模式与记录口径（任务 07-3-2）；备份侧清单见 `../backup/README.md`，镜像与卷口径见 `../compose/README.md`

## 1. 定位与边界

- 唯一实现是 `backend/ops/recovery.py`，在**部署宿主检出仓**运行；镜像不含 `ops/`，服务容器不参与恢复。
- 恢复顺序固定为「元数据库 → 位点文件 → 消息日志 → 校验对齐」，由清单的 `recovery_order` 逐项核对，不跳步。
- 两种模式：
  - `--mode drill`（缺省）：把数据根复制到工作根，在副本上完成恢复与全部校验；源数据根只读。
  - `--mode live`：停服后就地恢复，再启动并轮询就绪；必须显式 `--yes`，`prod` / `production` 一律拒绝。
- 不实现平台库备份与导入工具本身：只按宿主侧命令模板执行，不拼 SQL、不持有连接串。
- 不复制消息日志全量：日志来自仍存活的数据卷，清单只登记段范围与 LEO。

## 2. 键位（只从进程环境变量读取）

| 键 | 必填 | 缺省 | 用途 |
| --- | --- | --- | --- |
| `MSG_RECOVERY_WORK_DIR` | drill 是 | — | 演练工作根（与数据根分盘，不得等于或位于数据根之下） |
| `MSG_RECOVERY_STOP_CMD` | live 是 | — | 停止服务命令模板 |
| `MSG_RECOVERY_START_CMD` | live 是 | — | 启动服务命令模板 |
| `MSG_RECOVERY_METADATA_CMD` | live 是 | — | 元数据库导入命令模板，占位 `{dump}` / `{backup_id}` / `{target_env}` |
| `MSG_RECOVERY_READY_URL` | live 是 | — | 就绪地址 |
| `MSG_RECOVERY_TIMEOUT_SEC` | 否 | `120` | 命令与就绪轮询超时（秒） |

- `MSG_BACKUP_DIR` / `MSG_BACKUP_PLATFORM_DUMP_DIR` / `MSG_DATA_DIR` 与 07-3-1 复用，不新增同义键。
- 六键是**宿主侧运行旋钮**，不属于 07-2 的六类敏感项注册表，不写进 `compose/msg.yml` 的 `x-service-env`，不注入容器。
- 脚本不读取 `.env` 明文；部署侧把键位写入不入库的宿主环境文件（已 gitignore）后由命令注入。

## 3. 命令与模板

```bash
# 列出可用快照（含当前与回退候选）
python -m ops.recovery --list-backups

# 只读校验：清单 / 快照校验和 / dump 表集合 / 段缺口，不建目录、不写位点
python -m ops.recovery --dry-run

# 离线完整演练（源数据根只读；结束清理工作根）
python -m ops.recovery --mode drill --work-dir <工作根>

# 回退到上一份快照重跑
python -m ops.recovery --mode drill --work-dir <工作根> --backup-id <备份标识>

# 隔离部署环境真实演练（停服 → 导入 → 恢复 → 启动 → 就绪）
python -m ops.recovery --mode live --yes --work-dir <工作根>
```

命令模板在宿主 shell 中执行，仅在 `live` 模式触发；`drill` 只把模板原文（脱敏后）写进记录。模板内的口令材料（`--password=` / `MYSQL_PWD=` / `PGPASSWORD=` / `-p<值>` / URL 内嵌口令）在入记录与标准输出前统一替换为 `***`。

## 4. 工作根与产物

```text
<MSG_RECOVERY_WORK_DIR>/
└── run-<备份标识>-<UTC 时间>/
    ├── backup/       # 演练前自动备份（drill 结束删除；live 恒保留为回退点）
    └── target/       # drill 的数据根副本（结束删除；--keep 保留）
<工作区根>/tmp/msg/recovery/<备份标识>-<模式>-<UTC 时间>.json   # 记录（同目录同名 .md）
```

- 记录固定字段：环境、服务与契约版本、备份时 / 当前配置摘要、备份标识与时间、恢复起止、RTO / RPO、步骤表、七块校验结论、结论与原因、失败注入、问题与处置、遗留与归口。
- `drill` 默认清理工作根；`live` 不删工作根（演练前备份是唯一回退点）。`--keep` 只影响 drill 的副本。
- `--max-partitions` 可限制演练副本纳入的分区数；被排除的分区不参与延迟判定，并在记录中登记。

## 5. 退出码与结论

| 退出码 | 结论 | 含义 |
| --- | --- | --- |
| 0 | 通过 | 无硬失败、无降级项 |
| 2 | 降级通过 | 无硬失败，但有段间空洞 / 段尾缺失 / 分区缺失 / 位点夹取 / 位点低于起点 / 延迟损坏；需人工确认后再接流量 |
| 1 | 失败 | 清单、快照、dump、位点、抽样、命令或就绪任一硬失败 |

- 失败记录末行固定为 `[ALERT] recovery.fallback --backup-id <上一份>`，可直接复制重跑。
- `live` 模式结论为降级时，未给 `--allow-degraded` 会**停在启动前**（不允许部分恢复后直接接流量）。
- 「保留期裁剪」（清单里的旧段已被保留期合法删除）只登记 `pruned`，不判降级；段间空洞与段尾缺失才降级。

## 6. 禁止项

- 不在生产环境执行破坏性恢复（`live` 遇到 `prod` / `production` 直接拒绝，无覆盖开关）；生产灾难恢复走平台人工流程。
- 不在共享环境直接改数据根：先用 `drill` 演练，确认结论与回退路径后再考虑 `live`。
- 不把备份根、工作根或记录放进仓库；记录默认落 `tmp/`（已忽略），入库的人工记录不含真实路径与凭据。
- 不用「复制全量日志」替代恢复：日志不在备份里，复制日志等于把备份成本转嫁给恢复。
- 不把 `drill` 的降级结论当作「可以接流量」：`requires_review` 为真时必须人工确认缺口范围。

---

> 依 bms《文档生成规范》编写 · 恢复语义见《架构设计 · 部署与运维》「备份与恢复」节
