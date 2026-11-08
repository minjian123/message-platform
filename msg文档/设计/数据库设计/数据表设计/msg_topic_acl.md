# msg_topic_acl（主题授权规则）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_topic_acl

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 05-多租户与安全 · 05-3 主题授权 |
| 上游依据 | 《[架构设计 · 多租户与安全](../../架构设计/07_架构设计_多租户与安全.md)》第 7 节、bms 07D 需求 `02-5`（资源级授权接入约束）、[任务 05-3](../../../项目/08B_消息平台/任务/05_多租户与安全/05_多租户与安全_03_主题授权/05_多租户与安全_03_主题授权.md)、[详细设计](../../../项目/08B_消息平台/任务/05_多租户与安全/05_多租户与安全_03_主题授权/设计/01_详细设计_05_多租户与安全_03_主题授权.md) §4 |
| ORM 模型 | `services/msg/src/bms_msg/models/topic_acl.py::MsgTopicAcl`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-08；随任务 05-3，幂等建表落地，无迁移脚本） |
| 相关节点 | [msg_topic_acl_scope](msg_topic_acl_scope.md)、[msg_topic](msg_topic.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 2~5 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**业务上不做软删除**（用 `status` 表达），`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 规则 ID（应用侧生成，禁库自增） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与后五列复合唯一 | 租户内部身份（雪花主键，不可变；取自基座解析链，不接受客户端自报） |
| `namespace` | VARCHAR(64) | 否 | 同上 | 命名空间（对齐 bms `module_key`；授权只在同一命名空间内生效） |
| `topic` | VARCHAR(200) | 否 | 同上 | 主题名（原样，允许 `[a-zA-Z0-9._-]`；作为 `resource_key`） |
| `principal_type` | VARCHAR(16) | 否 | 同上 | 主体类型（`user` / `role`） |
| `principal_key` | VARCHAR(64) | 否 | 同上 | 主体标识（用户主键 / 角色主键的十进制字符串；仅数字） |
| `action` | VARCHAR(16) | 否 | 同上 | 动作（`publish` / `subscribe` / `describe` / `manage`） |
| `effect` | VARCHAR(8) | 否 | 默认 `allow` | 效果（`allow` / `deny`；同一匹配集合内 deny 优先） |
| `status` | VARCHAR(16) | 否 | 默认 `enabled` | 状态（`enabled` / `disabled`；收回置 `disabled`） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_topic_acl_rule` | 唯一 | `(tenant_id, namespace, topic, principal_type, principal_key, action)` | 同一主体对同一主题的同一动作只有一行；**不带 `deleted_at`**（本表不软删，带上会让 MySQL 多个 NULL 互异而失效） |
| `idx_msg_topic_acl_scope` | 普通 | `(tenant_id, namespace, topic)` | 授权快照装载（某作用域全部 enabled 规则） |
| `idx_msg_topic_acl_principal` | 普通 | `(tenant_id, namespace, principal_type, principal_key)` | 管理面按主体过滤 |

- 无物理外键；`topic` 为逻辑引用 `msg_topic (tenant_id, namespace, name)`（同库）；
- 公共软删除索引 `idx_msg_topic_acl_deleted_at` 由基类命名约定生成（`deleted_at` 恒建索引），此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（授权是当前生效状态，历史由基座审计链承载）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-08 | v1 | 新建表结构（平台库；规则表 + 版本戳表两表方案，收回用 `status=disabled`） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
