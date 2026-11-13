# msg_group_member（组成员）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_group_member

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 01-消息模型与投递 · 01-4-1 消费组落库 |
| 上游依据 | [任务 01-4](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_04_消费组/01_消息模型与投递_04_消费组.md)、[01-4-1 详细设计](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_04_消费组/01_消息模型与投递_04_消费组_01_消费组落库/设计/01_详细设计_01-4-1_消费组落库.md) |
| ORM 模型 | `services/msg/src/bms_msg/models/consumer_group.py::MsgGroupMember`（继承 `BaseModel`） |
| 状态 | 已落地（2026-11-13，01-4-1；模型 / 成员整替 / 心跳写穿随任务交付） |
| 相关节点 | [msg_consumer_group](msg_consumer_group.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`；本表**不做软删除**（成员随组整替，组删除时物理删除）。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 成员行主键 |
| `group_id` | DECIMAL(38,0) | 否 | 成员身份唯一键 + 心跳索引 | 所属组（逻辑外键 → `msg_consumer_group.id`） |
| `member_id` | VARCHAR(128) | 否 | 成员身份唯一键 | 成员标识（`consumer_id`；组内唯一由客户端保证） |
| `instance_id` | VARCHAR(64) | 是 | — | 最后写入该成员的实例标识（排障用，不参与存活判定） |
| `joined_at` | DATETIME | 否 | — | 加入时刻（UTC；重连不刷新） |
| `last_heartbeat` | DATETIME | 否 | 与 `group_id` 复合索引 | 最后心跳（UTC；存活判据 = 心跳 + 会话超时） |
| `partitions` | JSON | 否 | 默认 `[]` | 当前持有分区号（升序；空闲成员为空数组） |
| `created_at` | DATETIME | 否 | 审计 | 落库时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人（数据面路径通常 NULL） |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人（数据面路径通常 NULL） |
| `deleted_at` | DATETIME | 是 | 基类提供；本表恒 NULL | 软删除时间（不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

**为什么成员整替而不是差异更新**：每次再均衡都可能大改分配，差异更新要算集合差且容易漏删；「先删该组全部成员行再插入快照」让「库里就是一份完整快照」永远成立，代价只是几条 DELETE/INSERT。

**为什么 `instance_id` 不参与判定**：成员存活只看 `last_heartbeat`；实例标识仅用于排障（「这台成员最后是哪台实例接的」），避免把「实例迁移」误判成「成员失效」。

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_group_member_identity` | 唯一 | `(group_id, member_id)` | 同组成员唯一（同 id 再 `join` 即重连） |
| `idx_msg_group_member_heartbeat` | 普通 | `(group_id, last_heartbeat)` | 按组判定存活 / 清理超时成员 |

- 无物理外键；`group_id` 为逻辑外键（同库）。
- 成员行随组删除级联清理（应用层先删成员再删组，同一事务）。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻；随组按 `tenant_id` 区分）。
- **归档**：不归档；组删除 / 主题删除 / 租户清理时物理删除。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地；**本阶段无迁移脚本**（结构以模型为唯一事实源，《数据库开发规范》§5）。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-13 | v1 | 新建表结构（平台库；随任务 01-4-1 定稿：成员唯一、心跳租约、持有分区快照、实例标识） | minjian |
