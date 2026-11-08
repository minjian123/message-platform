# msg_quota（消息维度配额）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_quota

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 05-多租户与安全 · 05-2 消息维度配额 |
| 上游依据 | 《[架构设计 · 多租户与安全](../../架构设计/07_架构设计_多租户与安全.md)》第 6 节、[需求 05-3](../../../项目/08B_消息平台/需求/05_需求_多租户与安全.md#r05-3)、bms 07D `02-3` |
| ORM 模型 | `services/msg/src/bms_msg/models/quota.py::MsgQuota`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-08；随 05-2，幂等建表落地，无迁移脚本） |
| 相关节点 | [数据库设计总览](../01_数据库设计_总览.md) 第 3 / 4 / 9 节、[msg_topic](msg_topic.md) |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**业务上不做软删除**（配额行用物理删除表达生命周期），`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 主键（ID） |
| `tenant_id` | DECIMAL(38,0) | 否 | 与 `namespace` / `quota_key` 复合唯一 | 租户内部身份（雪花主键，不可变） |
| `namespace` | VARCHAR(64) | 否 | 同上；默认空串 | 命名空间（对齐 bms `module_key`）；**空串 = 租户级** |
| `quota_key` | VARCHAR(32) | 否 | 同上 | 配额维度键（六维之一） |
| `limit_value` | BIGINT | 否 | 默认 0 | 硬限（`0` = 不限） |
| `soft_limit` | BIGINT | 否 | 默认 0 | 软限（告警线；`0` = 未显式设置，读时按硬限 × 80% 派生） |
| `unit` | VARCHAR(16) | 否 | 由维度派生 | 单位（`count` / `bytes` / `qps`） |
| `period` | VARCHAR(16) | 否 | 默认 `none` | 周期（`none`；本期六维均为瞬时） |
| `origin` | VARCHAR(16) | 否 | 默认 `default` | 来源（`default` 懒初始化缺省行 / `configured` 管理面调整） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 修改人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

- **不存用量**：用量由 `usage_snapshot()` 实时计算（主题 / 分区 / 存储 / 今日投递 / 组 / 未确认 / 连接），避免双写与口径漂移。
- `limit_value` / `soft_limit` 用 `BIGINT`（字节与条数共用）；不设 FLOAT。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_quota_scope` | 唯一 | `(tenant_id, namespace, quota_key)` | 同一租户同一作用域同一维度只有一行；**不带 `deleted_at`**（本表不软删） |

- 无物理外键；`tenant_id` 为逻辑引用（基座租户主键同库不同表，不建外键）。
- 公共软删除索引 `idx_msg_quota_deleted_at` 由基类命名约定生成（`deleted_at` 恒建索引），此处不重复登记。
- 每表业务索引 1 个，未超每表 ≤5 个的口径；租户级列表 / 合并查询走唯一索引前缀 `tenant_id`。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；跨租户按 `tenant_id` 列区分）。
- **归档**：不归档（配额是租户配置，随租户到期物理清理删除）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地（开发库 `[database].auto_create`、运维经基座 `ops.init_schema` 口径）；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。开发 / 演示库结构不符时删库重建。
- **不是租户状态表**：只存配额限额，不承载租户状态、不参与数据面状态判定。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-08 | v1 | 新建表结构（平台库；六维配额限额，含作用域 / 软硬限 / 单位 / 周期 / 来源；不存用量） | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
