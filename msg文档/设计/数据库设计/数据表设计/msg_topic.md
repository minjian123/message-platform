# msg_topic（主题定义）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_topic

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 01-消息模型与投递 · 01-1 主题与分区模型 |
| 上游依据 | 《[架构设计 · 数据面：存储与复制](../../架构设计/04_架构设计_数据面_存储与复制.md)》第 2 节、《[架构设计 · 多租户与安全](../../架构设计/07_架构设计_多租户与安全.md)》第 2~3 节、[任务 01-1](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_01_主题与分区模型/01_消息模型与投递_01_主题与分区模型.md)、[详细设计](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_01_主题与分区模型/设计/01_详细设计_01_主题与分区模型.md) §4.1 |
| ORM 模型 | `services/msg/src/bms_msg/models/topic.py::MsgTopic`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-04；随任务 01-1，幂等建表落地，无迁移脚本） |
| 相关节点 | [msg_partition](msg_partition.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 2~3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**业务上不做软删除**（用 `status` 表达），`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 主题 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `namespace`、`name` 复合唯一 | 租户内部身份（雪花主键，不可变；取自基座解析链，**不接受客户端自报**） |
| `namespace` | VARCHAR(64) | 否 | 同上 | 命名空间（对齐 bms `module_key`，须为服务目录中已登记且启用的模块） |
| `name` | VARCHAR(200) | 否 | 同上 | 主题名（建议 `{域}.{对象}.{动作}`，字符集 `[a-zA-Z0-9._-]`） |
| `partition_count` | INT | 否 | — | 分区数（**只增不减**；1~1024） |
| `retention_ms` | BIGINT | 否 | — | 保留时长（毫秒；首期默认 30 天、上限 366 天，常量在 `bms_msg/core/config.py`） |
| `retention_bytes` | BIGINT | 否 | 默认 0 | 容量上限（字节；`0` = 不限，上限常量 `MAX_RETENTION_BYTES`）。超限从最老段整段删除，**最多删到只剩最后一段**（不把刚写入的数据删空），随 01-6 |
| `cleanup_policy` | VARCHAR(16) | 否 | 默认 `delete` | 清理策略（`delete` / `compact`；首期只支持 `delete`） |
| `status` | VARCHAR(16) | 否 | 默认 `active` | 状态（`active` / `disabled` / `deleting`）。`deleting` = 已受理删除、正在清理：数据面按 `350001` 拒绝、管理面写操作按 `350028` 拒绝、列表与计数不展示，清理完成即物理删除本行（06-2） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用，见上） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_topic_identity` | 唯一 | `(tenant_id, namespace, name)` | 主题标识唯一；**不带 `deleted_at`**（本表不软删，带上会让 MySQL 多个 NULL 互异而失效） |
| `idx_msg_topic_status` | 普通 | `status` | 列表按状态过滤 |

- 无物理外键；`msg_partition.topic_id` 为逻辑外键（同库）。
- 公共软删除索引 `idx_msg_topic_deleted_at` 由基类命名约定生成（`deleted_at` 恒建索引），此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（元数据，随主题停用保留）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-04 | v1 | 新建表结构（平台库；随任务 01-1 定稿：`tenant_id` 由设计初稿 `String(64)` 改为雪花 `DECIMAL(38,0)`，与基座《数据规范》同口径） | minjian |
| 2026-11-04 | v2 | 增 `retention_bytes`（容量上限，0 = 不限；随任务 01-6「按时间与容量双维度保留」，与 `retention_ms` 同码校验 `350009`） | minjian |
| 2026-11-09 | v3 | `status` 扩第三个取值 `deleting`（删主题受理后的清理中态；随任务 06-2，配套新表 [msg_topic_cleanup](msg_topic_cleanup.md)）。列定义与默认值不变，属语义扩展，**不改已有行** | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
