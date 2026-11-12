# msg_partition_migration（分区迁移台账）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_partition_migration

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 02-存储与复制 · 02-8 多实例部署与故障切换验收 |
| 上游依据 | [任务 02-8](../../../项目/08B_消息平台/任务/02_存储与复制/02_存储与复制_08_多实例部署与故障切换验收/02_存储与复制_08_多实例部署与故障切换验收.md)、[详细设计](../../../项目/08B_消息平台/任务/02_存储与复制/02_存储与复制_08_多实例部署与故障切换验收/设计/01_详细设计_02-8_多实例部署与故障切换验收.md) §7.1 / §7.4 / §11.1 |
| ORM 模型 | `services/msg/src/bms_msg/models/partition_migration.py::MsgPartitionMigration`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-12；随任务 02-8，幂等建表落地，无迁移脚本） |
| 相关节点 | [msg_partition](msg_partition.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 4 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**不做软删除**（`deleted_at` 恒为 NULL），也**不自动清理**（保留策略归后续运维任务）。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 台账行 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | — | 租户内部身份（雪花主键，不可变；取自主题行） |
| `namespace` | VARCHAR(64) | 否 | — | 命名空间快照（审计定位用） |
| `topic` | VARCHAR(200) | 否 | — | 主题名快照（审计定位用） |
| `plan_id` | VARCHAR(64) | 否 | 与 `topic_id` / `partition_no` 唯一 | 计划标识（`rb-<UTC 时间>-<短随机>`） |
| `topic_id` | DECIMAL(38,0) | 否 | 复合索引 | 逻辑外键 → `msg_topic.id`（不建物理外键） |
| `partition_no` | INT | 否 | 与 `topic_id` 复合索引 | 分区号 |
| `attempt` | INT | 否 | 默认 1 | 尝试次数（重新 plan / resume 时递增） |
| `action` | VARCHAR(16) | 否 | — | 动作：`expand` / `shrink` / `replace` / `leader_move` / `noop` |
| `step` | VARCHAR(16) | 否 | 默认空串 | 执行阶段（`replace` 的 `expand` / `shrink`；其余为空） |
| `from_replicas` | JSON | 否 | 默认 `[]` | 动作前副本集（插入序） |
| `to_replicas` | JSON | 否 | 默认 `[]` | 目标副本集（插入序） |
| `from_leader` | VARCHAR(128) | 否 | 默认空串 | 动作前 leader |
| `to_leader` | VARCHAR(128) | 否 | 默认空串 | 目标 leader |
| `status` | VARCHAR(16) | 否 | 默认 `planned` | `planned` / `running` / `catching_up` / `succeeded` / `failed` / `degraded` / `cancelled` |
| `result` | VARCHAR(16) | 否 | 默认空串 | 切主结果：`truncated` / `retained` / `noop`（非切换动作为空） |
| `isr_verified` | BOOLEAN | 否 | 默认 true | 切主是否验证目标在 ISR（false 记降级、不计通过） |
| `retained_tail` | BIGINT | 否 | 默认 0 | 保留的未提交条数 |
| `progress_leo` | BIGINT | 否 | 默认 0 | 最近一次进度：目标副本 LEO |
| `progress_hw` | BIGINT | 否 | 默认 0 | 最近一次进度：leader HW |
| `remaining_records` | BIGINT | 否 | 默认 0 | 剩余待追平条数（0 = 已追平） |
| `operator` | VARCHAR(128) | 否 | 默认空串 | 执行人（审计） |
| `reason` | VARCHAR(512) | 否 | 默认空串 | 执行原因（审计） |
| `error` | VARCHAR(512) | 否 | 默认空串 | 失败 / 降级说明（截断 512） |
| `started_at` | DATETIME | 是 | — | 开始执行时刻（UTC） |
| `finished_at` | DATETIME | 是 | — | 进入终态时刻（UTC） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表恒 NULL | 软删除时间（不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本（CAS 更新时 +1） |

**为什么一计划一分区一行**：替换类动作需要「先扩后缩」两步，但进度与结果必须能一眼回答「这个分区迁到哪了」；拆两行会让唯一约束与进度读模型分裂，`step` 列足以表达阶段。

**为什么 `step` 不进状态机**：`catching_up` 期间仍属同一状态，阶段只是 `replace` 的内部步骤；把它做成状态会让「追平中」出现两种语义，`status` 命令难以判定。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_partition_migration_item` | 唯一 | `(plan_id, topic_id, partition_no)` | 防重复计划项；同一计划内一分区一行 |
| `idx_msg_partition_migration_plan` | 普通 | `plan_id` | 按计划读全量动作 |
| `idx_msg_partition_migration_partition` | 普通 | `(topic_id, partition_no)` | `has_active` / 按分区追溯 |
| `idx_msg_partition_migration_status` | 普通 | `status` | 指标采样与待办拾取 |

- 无物理外键；`topic_id` 是逻辑外键（主题删除后成为孤儿引用，属预期）。
- 公共软删除索引 `idx_msg_partition_migration_deleted_at` 由基类命名约定生成，此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（单行极小，长期保留以便追溯）；自动清理策略归后续运维任务（02-8 不做）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-12 | v1 | 新建表结构（平台库；随任务 02-8：计划项 / 动作与阶段 / 副本集前后值 / 切主结果与 ISR 验证 / 追平进度） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)「数据表文件规范」节配套
