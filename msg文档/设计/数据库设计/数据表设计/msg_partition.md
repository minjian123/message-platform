# msg_partition（分区定义与检查点）

> msg · 数据库设计 · 数据表设计

[文档首页](../../../文档首页.md) › [数据库设计总览](../01_数据库设计_总览.md) › [已设计数据表登记](../01_数据库设计_总览.md#tables-registry) › msg_partition

## 1. 归属与依据 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 归属库 | msg 平台库（`datasource = platform`；库名由 `[database]` 派生，本地 SQLite 落 `database/msg/`） |
| 归属业务域 | `msg`（装载注册：清单 `[[domains]]`） |
| 归属服务 | `msg` |
| 覆盖模块 | 01-消息模型与投递 · 01-1 主题与分区模型；02-存储与复制 · 02-4 副本与 ISR 复制、02-5 选举、epoch 与故障切换 |
| 上游依据 | 《[架构设计 · 数据面：存储与复制](../../架构设计/04_架构设计_数据面_存储与复制.md)》第 2 / 7 节（`epoch` 与检查点须在 8B_02 预留）、[任务 01-1](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_01_主题与分区模型/01_消息模型与投递_01_主题与分区模型.md)、[详细设计](../../../项目/08B_消息平台/任务/01_消息模型与投递/01_消息模型与投递_01_主题与分区模型/设计/01_详细设计_01_主题与分区模型.md) §4.2 |
| ORM 模型 | `services/msg/src/bms_msg/models/partition.py::MsgPartition`（继承 `BaseModel`） |
| 状态 | 已落库（2026-11-04；随任务 01-1，幂等建表落地，无迁移脚本） |
| 相关节点 | [msg_topic](msg_topic.md)、[数据库设计总览](../01_数据库设计_总览.md) 第 2~3 节 |

## 2. 字段 <a id="fields"></a>

公共字段继承 `BaseModel`（雪花 ID / 审计 / 软删除列 / 乐观锁），语义见 bms《数据库设计 · 数据规范》§2；本表**业务上不做软删除**，`deleted_at` 恒为 NULL。

| 字段 | 类型 | 可空 | 约束 / 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `id` | DECIMAL(38,0) | 否 | 主键，雪花 ID | 分区记录 ID（应用侧生成） |
| `topic_id` | DECIMAL(38,0) | 否 | 与 `partition_no` 复合唯一 | 逻辑外键 → `msg_topic.id`（同库，不建物理外键） |
| `partition_no` | INT | 否 | 同上 | 分区号（0 起，**只追加不重排**） |
| `leader` | VARCHAR(128) | 否 | — | 当前 leader 标识（首期单机为实例标识） |
| `replicas` | JSON | 否 | 默认 `[]` | 副本列表（02-8 写分区分配结果；空 = 未分配，协调器按配置兜底） |
| `checkpoint_leo` | BIGINT | 否 | 默认 0 | 检查点 LEO（周期落库；实时值在内存，重启后由日志扫描恢复） |
| `checkpoint_hw` | BIGINT | 否 | 默认 0 | 检查点 HW（提交位点） |
| `epoch` | INT | 否 | 默认 0 | 领导者任期（8B_05 防脑裂；02-4 起写入记录头并由 follower 校验） |
| `min_insync_replicas` | INT | 否 | 默认 0 | `acks=all` 的最小同步副本数（02-4；0 = 继承全局配置） |
| `switch_state` | VARCHAR(16) | 否 | 默认 `idle` | 切换状态（`idle` / `switching`；02-5 两阶段停写） |
| `switch_started_at` | DATETIME | 是 | NULL | 切换开始时刻（UTC；审计与排障，不用于跨节点时钟判定） |
| `switch_from_leader` | VARCHAR(128) | 是 | NULL | 切换前 leader（中断重试时据此探测旧 leader） |
| `switch_from_epoch` | INT | 是 | NULL | 切换前 epoch（中断重试时校验同一轮切换） |
| `last_switch_at` | DATETIME | 是 | NULL | 最近一次成功切换时刻（UTC） |
| `last_switch_by` | VARCHAR(128) | 是 | NULL | 最近一次切换触发人（内部服务传当前用户 / `system:drill`） |
| `last_switch_epoch` | INT | 是 | NULL | 最近一次切换后的 epoch |
| `last_switch_result` | VARCHAR(16) | 是 | NULL | 最近一次切换结果（`truncated` / `retained` / `noop`） |
| `last_truncate_to` | BIGINT | 是 | NULL | 最近一次切换截断后的 LEO；NULL = 未截断（保留尾部） |
| `created_at` | DATETIME | 否 | 审计 | 创建时间（UTC） |
| `created_by` | DECIMAL(38,0) | 是 | 审计 | 创建人 |
| `updated_at` | DATETIME | 否 | 审计 | 更新时间（UTC） |
| `updated_by` | DECIMAL(38,0) | 是 | 审计 | 更新人 |
| `deleted_at` | DATETIME | 是 | 基类提供；本表业务恒 NULL | 软删除时间（本表不使用） |
| `version` | INT | 否 | 默认 1 | 乐观锁版本 |

## 3. 索引与约束 <a id="index"></a>

| 名称 | 类型 | 列 | 说明 |
| --- | --- | --- | --- |
| `uq_msg_partition_no` | 唯一 | `(topic_id, partition_no)` | 分区号在主题内唯一（防重复建分区） |
| `idx_msg_partition_topic` | 普通 | `topic_id` | 按主题取全部分区（详情 / 迁移 / 复制） |

- 无物理外键；`topic_id` 未设级联删除——主题只停用不删除，分区行随主题保留。
- 公共软删除索引 `idx_msg_partition_deleted_at` 由基类命名约定生成，此处不重复登记。

## 4. 分片 / 归档 / 建表 <a id="storage"></a>

- **分片**：不分片（平台库常驻）。
- **归档**：不归档；分区随主题保留，消息本体在文件系统（见总览第 2 节）。
- **建表**：随 **`msg:platform` 清单**表集派生，经幂等建表落地；**本阶段无迁移脚本**（结构以模型为唯一事实源）。

## 5. 变更记录 <a id="revlog"></a>

| 日期 | 版本 | 变更 | 作者 |
| --- | --- | --- | --- |
| 2026-11-04 | v1 | 新建表结构（平台库；随任务 01-1 定稿：`replicas` / `checkpoint_leo` / `checkpoint_hw` / `epoch` 一并预留，避免 8B_05 复制落地时改表） | minjian |
| 2026-11-11 | v2 | 增 `min_insync_replicas`（02-4：每分区可覆盖的最小同步副本数，0 = 继承全局；`replicas` 由 02-8 写分区分配结果） | minjian |
| 2026-11-11 | v3 | 增 9 个切换列：`switch_state` / `switch_started_at` / `switch_from_leader` / `switch_from_epoch` / `last_switch_at` / `last_switch_by` / `last_switch_epoch` / `last_switch_result` / `last_truncate_to`（02-5：两阶段停写、epoch 递增与切换结果可查；全部有缺省 / 可空，旧行不需回填） | minjian |
| 2026-11-12 | v4 | `replicas` 语义变更（02-8）：从「只读 / 配置兜底」变为「分配结果写入」；建主题 / 增分区 / `ops.rebalance` 共用同一分配器，结构不变 | minjian |

> 数据表设计 · 与 bms《[数据库开发规范](../../../../bms文档/规范/数据库开发规范.md)》「数据表文件规范」节配套
