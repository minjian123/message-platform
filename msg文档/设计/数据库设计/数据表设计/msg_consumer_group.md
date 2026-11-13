# msg_consumer_group（消费组）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_consumer_group

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 01-消息模型与投递 · 01-4-1 消费组落库 |
| 上游依据 | [任务 01-4](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_04_消费组/01_消息模型与投递_04_消费组.md)、[详细设计 01-4](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_04_消费组/设计/01_详细设计_01_消费组.md)、[01-4-1 详细设计](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_04_消费组/01_消息模型与投递_04_消费组_01_消费组落库/设计/01_详细设计_01-4-1_消费组落库.md) |
| ORM 模型 | `services/msg/src/bms_msg/models/consumer_group.py::MsgConsumerGroup`（继承 `BaseModel`） |
| 状态 | 已落地（2026-11-13，01-4-1；模型 / 仓储 / DB 存储 / 空组回收随任务交付） |
| 相关节点 | [msg_group_member](msg_group_member.md)、[msg_topic](msg_topic.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**不做软删除**（组删除 / 主题删除 / 租户清理走物理删除）。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 组主键（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `name` 复合索引 | 租户内部身份（雪花主键，不可变） |
| `topic_id` | DECIMAL(38,0) | 否 | 组身份唯一键 + 普通索引 | 主题主键（逻辑外键 → `msg_topic.id`） |
| `name` | VARCHAR(128) | 否 | 组身份唯一键 | 组名（**列名避开 SQL 保留字 `group`**） |
| `namespace` | VARCHAR(64) | 否 | — | 命名空间快照（管理面过滤 / 排障） |
| `state` | VARCHAR(24) | 否 | 默认 `empty` | 状态：`empty` / `preparing_rebalance` / `stable` / `dead` |
| `generation` | INT | 否 | 默认 0 | 世代号（成员集合变化 +1，重连不递增） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人（数据面路径通常 NULL） |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC；空组回收按它判空闲） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人（数据面路径通常 NULL） |
| `deleted_at` | DATETIME | 是 | 基类提供；本表恒 NULL | 软删除时间（不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

**为什么组名落 `name` 而不是 `group`**：`GROUP` 是 MySQL 保留字，直接做列名会让建表 / 查询在所有方言上都要额外转义；`name` 与数据库设计总览的规划列一致。

**为什么 `namespace` 只是快照**：`topic_id` 已隐含 `(tenant, namespace, name)`，组身份判定不依赖该列；它只用于管理面过滤与排障展示，主题改名（不允许）也不会改变组身份。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_consumer_group_identity` | 唯一 | `(tenant_id, topic_id, name)` | 组身份唯一；并发 `join` 由数据库兜底 |
| `idx_msg_consumer_group_tenant` | 普通 | `(tenant_id, name)` | 管理面按租户列表（组名升序） |
| `idx_msg_consumer_group_topic` | 普通 | `(topic_id)` | 主题删除 / 租户清理按主题物理删除 |
| `idx_msg_consumer_group_state` | 普通 | `(state, updated_at)` | 空组回收扫描（`empty` + 空闲超阈值） |

- 无物理外键；`topic_id` 为逻辑外键（同库）。
- 公共软删除索引由基类命名约定生成，此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不自动归档；空组超 7 天只置 `dead`（行与位点保留），显式删除才物理删除（`msg:group.delete` 强审计）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-13 | v1 | 新建表结构（平台库；随任务 01-4-1 定稿：组身份三元组、命名空间快照、四态、世代与空闲回收） | minjian |
