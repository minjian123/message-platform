# msg_dead_letter（死信）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_dead_letter

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 06-管理与运维 · 06-5 死信处置 |
| 上游依据 | 《[架构设计 · 控制面：元数据与运维](../../架构设计/06_架构设计_控制面_元数据与运维.md)》第 7 节、[任务 06-5](../../../项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_05_死信处置/06_管理与运维_05_死信处置.md)、[详细设计](../../../项目/08B_消息平台/任务/06_管理与运维/06_管理与运维_05_死信处置/06_管理与运维_05_死信处置_01_死信持久化与状态模型/设计/01_详细设计_06-5-1_死信持久化与状态模型.md) |
| ORM 模型 | `services/msg/src/bms_msg/models/dead_letter.py::MsgDeadLetter`（继承 `BaseModel`） |
| 状态 | 已落地（2026-11-09，06-5-1；模型 / 仓储 / 持久化队列 / 幂等建表随任务交付） |
| 相关节点 | [msg_topic](msg_topic.md)、[msg_replay_task](msg_replay_task.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**不做软删除**（`deleted_at` 恒为 NULL，主题删除 / 租户清理走物理删除）。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 死信记录 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `status`、`created_at` 复合索引 | 租户内部身份（雪花主键，不可变；取自基座解析链） |
| `namespace` | VARCHAR(64) | 否 | — | 命名空间（取自主题归属，排障可读） |
| `topic_id` | DECIMAL(38,0) | 否 | 与定位三元组唯一 | 主题主键（逻辑外键 → `msg_topic.id`，不建物理外键） |
| `topic` | VARCHAR(200) | 否 | — | 主题名快照（主题停用 / 改名后仍可排障） |
| `partition_no` | INT | 否 | 与定位三元组唯一 | 分区号 |
| `msg_offset` | BIGINT | 否 | 与定位三元组唯一 | 分区内序号（列名避开 SQL 保留字 `offset`） |
| `message_id` | VARCHAR(64) | 否 | — | 原消息 ID（重放不变） |
| `log_id` | VARCHAR(64) | 否 | — | 原记录 ID（记录级定位入口） |
| `trace_id` | VARCHAR(64) | 是 | — | 链路标识（取自消息头；无则 NULL） |
| `reason` | VARCHAR(32) | 否 | — | 转入原因：`delivery_exhausted` / `schema_invalid` / `processing_failed` |
| `attempts` | INT | 否 | 默认 0 | 累计投递次数（broker 判定为上限值；消费端上报为首投次数） |
| `detail` | JSON | 否 | 默认 `{}` | 失败详情（字段名 → 说明；消费端上报时原样保存） |
| `status` | VARCHAR(16) | 否 | 默认 `pending` | 状态：`pending` / `replaying` / `resolved` / `ignored` / `archived` |
| `disposition_reason` | VARCHAR(256) | 是 | — | 处置原因（忽略必填；重放 / 归档可选） |
| `disposed_by` | DECIMAL(38,0) | 是 | — | 处置人（雪花主键；终态写入） |
| `disposed_at` | DATETIME | 是 | — | 处置时间（UTC；终态写入） |
| `replay_task_id` | DECIMAL(38,0) | 是 | 普通索引 | 关联重放任务（`msg_replay_task.id`；仅 `replaying` 有值） |
| `failed_at` | DATETIME | 否 | — | 转入死信的事件时间（UTC；取自 `DeadLetter.failed_at`） |
| `created_at` | DATETIME | 否 | 审计 | 落库时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人（转入路径无操作人，通常 NULL） |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人（状态迁移时写操作人；系统迁移可为空） |
| `deleted_at` | DATETIME | 是 | 基类提供；本表恒 NULL | 软删除时间（不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

**为什么 `reason` 用 VARCHAR 而不是枚举列**：原因集合是登记枚举（`DeadLetterReason`），但数据库不需要为三值建枚举类型；跨库（MySQL / PostgreSQL / 达梦）枚举类型落点不一致，VARCHAR + 应用层枚举校验更稳。

**为什么同时保留 `failed_at` 与 `created_at`**：延迟消费 / 补登时「何时失败」与「何时落库」可能不同；列表默认按 `failed_at` 展示，指标与保留期判断可用落库时间。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_dead_letter_key` | 唯一 | `(topic_id, partition_no, msg_offset)` | 死信定位三元组唯一；`add` 幂等由数据库兜底 |
| `idx_msg_dead_letter_tenant_status` | 普通 | `(tenant_id, status, created_at)` | 列表按租户 + 状态 + 时间筛选 |
| `idx_msg_dead_letter_replay_task` | 普通 | `(replay_task_id)` | 反查某重放任务关联的死信 |
| `idx_msg_dead_letter_topic` | 普通 | `(topic_id)` | 主题删除 / 租户清理按主题物理删除 |

- 无物理外键；`topic_id` / `replay_task_id` 为逻辑外键（同库）。
- 公共软删除索引 `idx_msg_dead_letter_deleted_at` 由基类命名约定生成，此处不重复登记。
- **不建 `log_id` 唯一约束**：重放会合法地产生同一 `message_id` 的多条记录（新 `log_id`），若重放后的新记录再次失败，会产生新的死信行（新 offset）。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不自动归档，`archived` 行长期保留；后续归档阶段另议。**主题删除 / 租户清理时物理删除**对应行（与 06-2 清理台账 `done_steps.dlq` 语义一致）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-09 | v1 | 新建表结构（平台库；随任务 06-5-1 定稿：定位三元组唯一、五态状态机、失败事件时间与落库时间分列、处置人与重放任务关联） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
