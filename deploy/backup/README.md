# msg 备份单元说明

> 位点日快照的调度形态与键位口径（任务 07-3-1）；镜像与卷口径见 `../compose/README.md`

## 1. 定位与边界

- 唯一实现是 `backend/ops/backup.py`，在**部署宿主检出仓**运行；镜像不含 `ops/`，服务容器不参与备份。
- 每次执行做三件事：核对平台库备份是否覆盖 `msg_*` 表、把 `offsets/` 复制到独立备份卷、生成与快照同目录的 `manifest.json`。消息日志只登记段范围与 LEO 摘要，不复制。
- 调度用**用户级 systemd timer**：`msg-backup.timer` 每日 13:00 触发（平台库备份 12:00 之后，留余量读当日 dump），`Persistent=true` 让挂起期错过的触发点补跑——这是 cron 不具备的能力。
- 本目录只放**模板与说明**；真机安装与启用归部署阶段，仓内不出现真实磁盘路径、服务器地址、端口与账号。

## 2. 键位（只从进程环境变量读取）

| 键 | 必填 | 缺省 | 用途 |
| --- | --- | --- | --- |
| `MSG_BACKUP_DIR` | 是 | — | 备份根目录，放宿主独立卷 / 数据盘，不与 `MSG_DATA_DIR` 同盘 |
| `MSG_BACKUP_PLATFORM_DUMP_DIR` | 是 | — | 平台库备份产物目录，只读扫描 |
| `MSG_DATA_DIR` | 否 | `var/lib/msg` | 源数据根，`data/` 与 `offsets/` 的父目录 |
| `MSG_BACKUP_KEEP_DAYS` | 否 | `7` | 快照保留天数 |
| `MSG_BACKUP_SPACE_FACTOR` | 否 | `1.2` | 可用空间估算系数 |

- 键位与缺省值的单一事实源是 `bms_msg/core/config.py` 的「07-3-1 备份」段；`deploy/.env.example` 列出同样的键，值全为空。
- 这些是**宿主侧运行旋钮**，不属于 07-2 的「六类敏感项」注册表，因此不写进 `compose/msg.yml` 的 `x-service-env`，不注入服务容器。
- 部署侧把上表四个键写入 `msg-backup.env`（不入库、已 gitignore），由 service 的 `EnvironmentFile` 注入进程环境；脚本自身不读取 `.env` 明文。

## 3. 模板安装

1. 把 `msg-backup.service` / `msg-backup.timer` 复制到用户级单元目录。
2. 替换 service 里的三个占位：`<检出根>` → msg 仓库检出根、`<仓内解释器>` → 仓内 3.14 解释器、`<备份日志>` → 备份卷下的日志文件路径；`msg-backup.env` 放同一目录或把 `EnvironmentFile` 指到实际位置。
3. 确认 `MSG_BACKUP_DIR` 指向的目录已建好且属主为运行用户；脚本只在其中写 `.staging-*`、快照目录与 `current.json`。
4. 校验与启用（部署阶段执行，本任务不执行）：

```bash
systemd-analyze verify ~/.config/systemd/user/msg-backup.service
systemctl --user daemon-reload
systemctl --user enable --now msg-backup.timer
systemctl --user list-timers msg-backup.timer
```

排障看 service 状态与备份日志；手工补跑用 `systemctl --user start msg-backup.service`，或在 `backend/` 下执行 `python -m ops.backup --dry-run`。

## 4. 备份目录结构

```text
<MSG_BACKUP_DIR>/
├── current.json                     # 最新可用备份指针（原子替换）
├── <备份标识>/                      # 已发布快照，标识为 UTC 时间戳
│   ├── manifest.json                # 清单（与快照同目录）
│   └── offsets/                     # 位点文件相对路径原样保留
│       └── {tenant_id}/{group}-{hash8}/{topic_id}-{partition}.offset
└── .staging-<备份标识>/             # 仅写入期存在，失败即删
```

发布只认「临时目录校验通过 → 原子改名」：失败时只删 `.staging-*`，上一份快照与 `current.json` 原样不动。清理只删过期且含清单的目录，**最新一份与指针所指目录永不删除**，即使全部过期也至少留一份可用快照。

## 5. 清单字段（恢复侧按此核对）

| 字段 | 含义 |
| --- | --- |
| `schema_version` / `backup_id` / `created_at` | 格式版本、标识与 UTC 时间 |
| `service` / `config_version` | 服务与契约版本、四份 `config*.toml` 的内容摘要 |
| `tenants` | `offsets/` 与 `data/` 一级目录名并集 |
| `metadata` | 平台 dump 文件名、期望表集合、dump 中的表集合、覆盖结论 |
| `offsets.files` | 每个位点文件的相对路径 / 字节数 / sha256 |
| `log_segments` | `backed_up = false`、各分区段范围与 LEO、`needs_alignment` |
| `recovery_order` | 元数据库 → 位点文件 → 消息日志 → 校验对齐 |
| `files` | 快照目录逐文件清单（不含清单自身） |
| `retention_days` | 本次生效的保留天数 |

LEO 取 `recovery-point.json`；恢复点缺失或损坏时取段名最大值并标 `leo_source = segments_only`，该分区进 `needs_alignment`，从不静默填 0。

## 6. 退出码与告警

| 结果 | 表现 |
| --- | --- |
| 成功 | 标准输出 `[OK] backup.published …`，退出 0 |
| 失败 | 标准错误 `[FAIL] backup.<类别>: …` 与 `[ALERT] backup.<类别>: …`，退出 1；原因带对象路径或键名，不打印环境变量值 |
| 空间不足 | `[ALERT] backup.space: …` 提前失败，未建临时目录 |
| 清理单份失败 | `[ALERT] backup.purge …`，本次仍返回 0，下一轮重试 |
| dry-run | 标准输出清单预览，退出 0，不建目录、不写文件、不动指针 |

告警通道（规则 / webhook）归 06-3 指标告警体系；本单元只产结构化日志行。

## 7. 禁止项

- 不把备份根目录放进仓库或容器可写层；不把备份文件、快照或 `msg-backup.env` 提交入库。
- 不在模板里写真实磁盘路径、服务器地址、内网地址与账号；凭据不进任何仓内文件。
- 不用 `cron` 替代 timer（挂起期任务会被跳过且不补跑）；不改平台库备份总控脚本。
- 不用「删最新一份」来腾空间；保留期只能靠 `MSG_BACKUP_KEEP_DAYS` 调整。
- 不在生产环境手工执行恢复动作；恢复演练流程见任务 07-3-2。

---

> 依 bms《文档生成规范》编写 · 备份语义见《架构设计 · 部署与运维》「备份与恢复」节
