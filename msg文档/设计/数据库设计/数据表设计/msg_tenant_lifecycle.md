# msg_tenant_lifecycle（租户生命周期动作账本）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_tenant_lifecycle

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 05-多租户与安全 · 05-5 租户生命周期资源清理 |
| 上游依据 | 《[架构设计 · 多租户与安全](../../架构设计/07_架构设计_多租户与安全.md)》第 5 节、[需求 05-6](../../../项目/08B_消息平台/需求/05_需求_多租户与安全.md#r05-6)、[任务 05-5](../../../项目/08B_消息平台/任务/05_多租户与安全/05_多租户与安全_05_租户生命周期资源清理/05_多租户与安全_05_租户生命周期资源清理.md)、bms 07D `01-4` |
| ORM 模型 | `services/msg/src/bms_msg/models/tenant_lifecycle.py::MsgTenantLifecycle`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-08；随 05-5，幂等建表落地，无迁移脚本） |
| 相关节点 | [数据库设计总览](../01_数据库设计_总览.md) 第 3 / 4 / 9 节、[msg_topic](msg_topic.md) |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**业务上不做软删除**（生命周期账本永久保留），`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 主键（ID） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `action` 复合唯一 | 租户内部身份（雪花主键，不可变；取自事件载荷） |
| `action` | VARCHAR(16) | 否 | 同上 | 生命周期动作（`created` / `suspended` / `activated` / `deleted`） |
| `applied_version` | INT | 否 | 默认 0 | 已成功处理的事件版本戳（`sys_tenant.version`；旧事件按 0） |
| `deleted_event_at` | DATETIME | 是 | — | 删除事件生效时刻（UTC；仅 `deleted` 行） |
| `purge_after` | DATETIME | 是 | — | 下一次到期清理时刻（UTC；取该租户未清理主题的最早到期点；NULL = 无待清理） |
| `purged_at` | DATETIME | 是 | — | 该租户全部主题清理完成时刻（UTC；NULL = 未完成） |
| `failure_count` | INT | 否 | 默认 0 | 连续清理失败次数（成功清零） |
| `last_error` | VARCHAR(512) | 是 | — | 最近一次清理失败原因（截断 512） |
| `last_event_id` | VARCHAR(128) | 是 | — | 最近处理的事件 ID（排障用） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_tenant_lifecycle_tenant_action` | 唯一 | `(tenant_id, action)` | 同一租户同一动作只有一条账本行；**不带 `deleted_at`**（本表不软删） |
| `idx_msg_tenant_lifecycle_purge` | 普通 | `(purge_after, purged_at)` | 到期清扫按 `purge_after <= now` 且 `purged_at IS NULL` 取待清理行 |

- 无物理外键；`tenant_id` 为逻辑引用（基座租户主键同库不同表，不建外键）。
- 公共软删除索引 `idx_msg_tenant_lifecycle_deleted_at` 由基类命名约定生成（`deleted_at` 恒建索引），此处不重复登记。
- 每表业务索引 2 个，未超每表 ≤5 个的口径。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（生命周期账本是幂等证据与清理进度，保留至人工归档；不随租户资源删除）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。
- **不是租户状态表**：只记录「某租户某动作处理到哪个版本 + 删除清理进度」，不承载租户状态机、不替代基座租户状态、不被数据面读取做状态判定。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-07 | v1 | 新建表结构（平台库；05-5 生命周期动作账本，替代 05-1 的租户目录 `.lifecycle.json` 生产路径，后者仅保留兼容读） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
