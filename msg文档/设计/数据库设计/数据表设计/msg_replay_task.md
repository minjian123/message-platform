# msg_replay_task（重放任务）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_replay_task

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 01-消息模型与投递 · 01-6 消息保留与重放 |
| 上游依据 | 《[架构设计 · 控制面：元数据与运维](../../架构设计/06_架构设计_控制面_元数据与运维.md)》第 4.2 节 / 第 7 节、《[架构设计 · 数据面：存储与复制](../../架构设计/04_架构设计_数据面_存储与复制.md)》第 6 节、[任务 01-6](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_06_消息保留与重放/01_消息模型与投递_06_消息保留与重放.md)、[详细设计](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_06_消息保留与重放/设计/01_详细设计_01_消息保留与重放.md) §6.5 |
| ORM 模型 | `services/msg/src/bms_msg/models/replay.py::MsgReplayTask`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-04；随任务 01-6，幂等建表落地，无迁移脚本；2026-11-09 随 06-6-1 additive 扩影响两列） |
| 相关节点 | [msg_topic](msg_topic.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**不做软删除**（用 `status` 表达终态），`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 任务 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 普通索引（与 `created_at` 复合） | 租户内部身份（雪花主键，不可变；取自基座解析链） |
| `namespace` | VARCHAR(64) | 否 | — | 命名空间（重放范围副本，排障可读） |
| `topic_id` | DECIMAL(38,0) | 否 | — | 主题主键（逻辑外键 → `msg_topic.id`，不建物理外键） |
| `topic` | VARCHAR(200) | 否 | — | 主题名（展示名快照；主题停用 / 改名后仍可排障） |
| `partitions` | JSON | 否 | 默认 `[]` | 分区号列表（**创建时固化的解析结果**，主题扩分区不影响已建任务） |
| `offset_from` | BIGINT | 是 | — | 起始 offset（含）；NULL 不限 |
| `offset_to` | BIGINT | 是 | — | 结束 offset（不含）；NULL 不限 |
| `time_from` | DATETIME | 是 | — | 起始发布时间（含，UTC）；NULL 不限 |
| `time_to` | DATETIME | 是 | — | 结束发布时间（不含，UTC）；NULL 不限 |
| `filter_key` | VARCHAR(256) | 是 | — | 分区键精确过滤；NULL 不过滤（列名避开 SQL 保留字 `key`） |
| `rate_per_second` | INT | 否 | — | 限速（条/秒，1 ~ 1000） |
| `status` | VARCHAR(16) | 否 | 默认 `pending` | 状态：`pending` / `running` / `paused` / `cancelled` / `completed` / `failed` |
| `total` | BIGINT | 否 | 默认 0 | 选中总条数（建任务时按条件精确计数，进度分母） |
| `replayed` | BIGINT | 否 | 默认 0 | 已写入条数（进度分子） |
| `cursor_state` | JSON | 否 | 默认 `[]` | 分区游标：`[{"partition":0,"next":12,"end":100}]`（列名避开 SQL 保留字 `cursor`） |
| `log_id_first` | VARCHAR(64) | 是 | — | 首条成功写入的记录标识（审计载荷来源之一） |
| `log_id_last` | VARCHAR(64) | 是 | — | 末条成功写入的记录标识（审计载荷来源之一） |
| `error_msg` | VARCHAR(512) | 是 | — | `failed` 的原因（截断 512） |
| `finished_at` | DATETIME | 是 | — | 终态时刻（UTC；`completed` / `cancelled` / `failed` 置值） |
| `impact_groups` | JSON | 否 | 默认 `{}` | 影响基线（创建时写一次，此后不改）：`{"captured_at":"...","truncated":false,"groups":[{"group":"g1","baseline_lag":120,"partitions":[{"partition":0,"committed":10,"leo":60}]}]}` |
| `impact_snapshot` | JSON | 是 | 默认 NULL | 终态影响快照（首次查询已终结任务时写一次）：`{"captured_at":"...","truncated":false,"groups":[{"group":"g1","baseline_lag":120,"current_lag":200,"delta":80}]}` |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人（执行器写终态审计时作为操作人） |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `idx_msg_replay_task_status` | 普通 | `status` | 执行器按状态捞任务（只捞 `running`） |
| `idx_msg_replay_task_tenant` | 普通 | `tenant_id`, `created_at` | 租户维度按时间追溯重放历史 |

- 无物理外键；`topic_id` 为逻辑外键（同库）。
- 公共软删除索引 `idx_msg_replay_task_deleted_at` 由基类命名约定生成（`deleted_at` 恒建索引），此处不重复登记。
- 不建唯一约束：同一范围允许建多个任务（重放是「至少一次」的生产行为，幂等责任在消费端）。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档；终态任务行长期保留（单行数据量小，是「谁重放过什么」的唯一索引）。行数增长慢（每次重放一行），不需要定期清理。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-04 | v1 | 新建表结构（平台库；随任务 01-6 定稿：条件列扁平化（`offset_*` / `time_*` / `filter_key`）、进度分子分母独立列、游标存 JSON 文本而非子表） | minjian |
| 2026-11-09 | v2 | additive 扩两列（随任务 06-6-1 影响可见）：`impact_groups`（创建时影响基线与分区级积压明细，写一次不改）、`impact_snapshot`（终态影响快照，首次查询已终结任务时写一次）；既有列语义与索引不变 | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
