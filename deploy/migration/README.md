# 迁移编排与切换开关（07-4-1 / 07-4-2）

> 操作手册 · 需求 07-4 · 任务 07-4-1 / 07-4-2 · 设计见任务目录 `设计/01_详细设计_07-4-1_切换开关与分域迁移编排.md`、`设计/01_详细设计_07-4-2_回滚演练与记录.md`

本目录回答四个问题：**现在能不能迁**（前置清单）、**迁哪个域按什么顺序**（切换矩阵）、**配错了会怎样**（开关契约）、**切过去怎么退回来**（回滚演练）。生产环境执行迁移归 bms `8B_08`；本站只负责编排、演练与阻止「前置未满足就迁」。

## 1. 三条命令

```bash
# 在 backend/ 目录执行（与 ops 其他命令一致）
python -m ops.migration check --env prod     # 生产闸门：前置未满足即退出 1
python -m ops.migration check --env test     # 隔离环境：阻断项只记录
python -m ops.migration matrix               # 渲染切换矩阵（文档表由此粘贴）
python -m ops.migration drill --msg-url http://msg:8000 --env test   # 隔离环境演练
```

回滚演练（07-4-2）：

```bash
python -m ops.rollback check --env prod      # 场景单源 / 判据可追溯 / 生产闸门
python -m ops.rollback matrix                # 渲染注入场景表（设计文档表由此粘贴）
python -m ops.rollback drill --env test      # 切 msg → 三类注入 → 回滚 → 恢复 + 一致性
```

- 两条 `drill` 都只允许 `test` / `dev`（`prod` / `production` 直接拒绝，无覆盖开关）；
- 缺省路径：`deploy/migration/preconditions.toml`、`deploy/migration/switch_matrix.toml`、`deploy/migration/rollback_scenarios.toml`；
- 记录落工作区根 `tmp/msg/migration/` 与 `tmp/msg/rollback/`（已忽略），JSON 是机器对接契约、Markdown 供人阅读；仓内 `tmp/` 不是合法临时落点（工作区卫生规范只放行工作区根 `tmp/` 与各仓 `.cache/`）。

## 2. 前置清单（`preconditions.toml`）

- 唯一状态源：**不在任务 / 计划文档另抄状态**；
- `status` 只允许 `satisfied` / `not_ready`；`required_in` 写生效环境（`prod` 为硬闸门）；
- `evidence` 必须写证据锚点（文档 / 任务编号 / 演练记录），空证据视为未核对；
- 当前仓内缺省：`copy_replication`（8B_05）为 `not_ready`，`rollback_drill`（07-4-2）已 `satisfied`；**生产迁移仍被 8B_05 阻断**；
- 前置交付后只改 `status` + `evidence`，不改代码、不重新发布业务镜像。

## 3. 切换矩阵（`switch_matrix.toml`）

| 批次 | 范围 | 进入条件 |
| --- | --- | --- |
| 1 | 一条低风险事件 + 一个只读远程调用 | 生产前置全绿 + 切换前备份成功 |
| 2 | 审计事件 | 批次 1 观察窗口通过 |
| 3 | 业务事件逐域 + 写远程调用 | 批次 2 观察窗口通过 |

每个域必须写：开关键、域级键（8B_08 待交付，当前只允许 `pending_8b08`）、默认值（原实现）、观察指标（必须已登记）、回滚判据、责任人。指标名取自 `bms_msg.metrics.names`，阈值数值归 8B_12。

**首期只有全局开关**：同一时刻切换的是整个端口；「分域」体现为「先切哪个端口、先看哪类流量」，真正的域级分流要等 bms `8B_08` 落地域级键。

## 4. 开关契约

| 端口 | 键 | 合法值 | 缺省 / 原实现 |
| --- | --- | --- | --- |
| 事件发布 | `[event].provider` | `rocketmq` / `msg` / `""`（→ `null`） | `rocketmq`（未部署 broker 时 `""`） |
| 服务调用 | `[service_client].provider` | `http` / `msg` / `""`（`null` 占位） | `http` |

- **缺省 / 空串 = 原实现**（回原链路）；
- **非法值 / `provider = "msg"` 但地址或命名空间缺失 = 装配期拒启**（`PluginError` / `ConfigError`），**不静默回落**——静默回落会造成「以为切了 msg、实际还在 http」的假切换；
- 切换只改配置 + **重启调用方服务**，不发布业务代码、不重建业务镜像（插件装配在启动期完成，运行期不监听文件）；
- 每次切换记录 `config_fingerprint`（两个 provider + 命名空间 + 实例地址 + 策略的摘要）用于对账。

## 5. 演练做什么

`drill` 用真 msg 应用（真路由 / 真帧 / 真数据面）跑完：

1. 前置清单与矩阵校验（不通过即失败）；
2. 两个端口都解析到 `msg`（原实现 / 未注册 / 配置不全都会失败）；
3. 一条事件链：建主题 → 发布 → 消费，载荷 / 属性 / 分区键逐项比对；
4. 一个只读远程调用（进程内用真实业务函数 `fetch_catalog_snapshot`；网络演练用内置 `demo.echo`）；
5. 在途：首跳失败重试两次，幂等键相同、服务端只执行一次；
6. 未确认事件：不可达发布必须抛 `EventPublishError`；恢复后恰好投递一次；
7. 双写：两次调用 = 恰好两条消息（端口只解析出一个实现）；
8. fallback：原链路可继续调用（未提供地址时结论降级，不冒充通过）；
9. 业务零改动：真实调用点签名与端口方法集不变。

结论三态：`通过`（退出 0）/ `降级通过`（退出 2）/ `失败`（退出 1）。失败也写记录。

## 6. 回滚演练（07-4-2）

- 判据唯一来源：`rollback_scenarios.toml`（模式 / 目标 / 矩阵判据原文 / 阈值占位 / 采样 / 恢复预算）；
- 三类注入**必须齐备**：`unavailable`（两端口不可达）、`error_rate`（发布失败率）、`latency`（只读延迟）；
- 闭环：切到 `msg` → 每条场景注入 → 把两个 `provider` 改回原实现（只改配置 + 重启调用方）→ 验证 outbox 补发恰好一次、再投零次、只读调用同构；
- 四类一致性全验：效果一次、无丢失（`outbox_enqueued = delivered + pending`、`published = consumed`）、位点 / 死信 / 重放、审计链；
- 记录字段：环境、版本、配置版本、切换域、注入观测、回滚起止、`config_change_ms`、`recovery_ms`、数据完整性、问题与遗留；JSON / Markdown 同源，失败也落盘；
- 判据阈值是**占位值**（`threshold_source = "placeholder_8b12"`），真实数值归 8B_12；
- 原链路事件侧 broker 未部署时 `recover.*` 记 `degraded`、结论 `降级通过` 并登记遗留，**不冒充通过**。

## 7. 禁止项

- 不在生产环境执行 `drill`；生产切换、观察窗口与 PromQL 取值归 bms `8B_08`；
- 回滚只改 `[event].provider` / `[service_client].provider` + 重启调用方：不重新发布业务代码、不执行数据库降级、不删除原链路；
- 不为了让演练通过而改判据或放宽矩阵（改判据要先改设计并重新确认）；
- 不在前置未满足时把 `check --env prod` 当「参考」——它是闸门，不是提示；
- 不删除 RocketMQ / outbox / HTTP 原链路，不改业务端口契约；
- 不把凭据、真实地址写进记录或本目录（记录只写相对路径与脱敏命令）。
