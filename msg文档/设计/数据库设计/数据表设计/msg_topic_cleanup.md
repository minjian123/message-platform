# msg_topic_cleanup（主题删除清理台账）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_topic_cleanup

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 06-管理与运维 · 06-2 管理模块写操作 |
| 上游依据 | 《[架构设计 · 控制面：元数据与运维](../../架构设计/06_架构设计_控制面_元数据与运维.md)》第 4.2 节、[任务 06-2-1](../../../项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_02_管理模块写操作/06_管理与运维_02_管理模块写操作_01_写操作后端与资源清理/06_管理与运维_02_管理模块写操作_01_写操作后端与资源清理.md)、[详细设计](../../../项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_02_管理模块写操作/06_管理与运维_02_管理模块写操作_01_写操作后端与资源清理/设计/01_详细设计_06-2-1_写操作后端与资源清理.md) §4.2 |
| ORM 模型 | `services/msg/src/bms_msg/models/topic_cleanup.py::MsgTopicCleanup`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-09；随任务 06-2-1，幂等建表落地，无迁移脚本） |
| 相关节点 | [msg_topic](msg_topic.md)、[msg_partition](msg_partition.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 4 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**不做软删除**（`deleted_at` 恒为 NULL），且**行不随主题删除**——主题与分区行物理删除后本行保留，用于追溯「谁在什么时候删了什么、清了多少步」。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 台账 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `topic_id` 复合索引 | 租户内部身份（雪花主键，不可变；取自基座解析链） |
| `topic_id` | DECIMAL(38,0) | 否 | **唯一** | 主题主键（一个主题一行；重复受理复用同行） |
| `namespace` | VARCHAR(64) | 否 | — | 命名空间快照（主题行删除后仍可读） |
| `topic` | VARCHAR(200) | 否 | — | 主题名快照（同上） |
| `status` | VARCHAR(16) | 否 | 默认 `pending` | 台账状态：`pending` / `running` / `completed` |
| `done_steps` | JSON | 否 | 默认 `[]` | 已完成步骤键（`messages` / `delay` / `offsets` / `groups` / `dlq` / `runtime` / `metadata`），**幂等跳过依据** |
| `attempts` | INT | 否 | 默认 0 | 已执行轮次（每轮 +1；用于「连续失败 ≥3 告警」判据） |
| `last_error` | VARCHAR(512) | 是 | — | 最后一轮失败原因（截断 512） |
| `started_at` | DATETIME | 是 | — | 首轮开始时刻（UTC） |
| `finished_at` | DATETIME | 是 | — | 完成时刻（UTC） |
| `created_at` | DATETIME | 否 | 审计 | 受理时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 发起删除的操作人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表恒 NULL | 软删除时间（不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

**为什么 `done_steps` 用 JSON 而不是子表**：步骤是固定七项的闭集、只作「跳过判据」，没有按步骤查询的需求；拆子表只会多一轮往返与一批无用索引（与 `msg_replay_task.cursor_state` 同口径）。

**为什么状态只有三态**：清理失败不是终态——任务书要求「失败保持 `deleting` 并在下一轮扫描重试」，可自愈故障（磁盘满 / 句柄占用）恢复后应自动收口；不可自愈情形由 `attempts` + `last_error` + 告警暴露，人工介入后仍由下一轮收口。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_topic_cleanup_topic` | 唯一 | `(topic_id)` | 一个主题只有一份台账；重复受理复用同行 |
| `idx_msg_topic_cleanup_status` | 普通 | `status` | 周期扫描按状态拾取待办 |
| `idx_msg_topic_cleanup_tenant` | 普通 | `(tenant_id, created_at)` | 按租户追溯删除记录 |

- 无物理外键；`topic_id` 是逻辑外键（主题行删除后成为孤儿引用，属预期）。
- 公共软删除索引 `idx_msg_topic_cleanup_deleted_at` 由基类命名约定生成，此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（单行极小，长期保留以便追溯）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-09 | v1 | 新建表结构（平台库；随任务 06-2-1，删主题异步清理台账：三步状态 + 七步完成键 + 尝试次数与最后错误） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)「数据表文件规范」节配套
