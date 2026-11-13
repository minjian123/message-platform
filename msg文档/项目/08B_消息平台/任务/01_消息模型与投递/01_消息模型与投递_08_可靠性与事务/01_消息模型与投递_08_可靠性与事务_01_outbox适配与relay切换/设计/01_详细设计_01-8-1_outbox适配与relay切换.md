# 01-8-1 outbox 适配与 relay 切换 · 详细设计

> 08B · 01 消息模型与投递 · 01-8 可靠性与事务 · 子任务 01（需求 01-8）

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../../计划/01_计划_消息平台.md) › [01 消息模型与投递](../../../01_消息模型与投递.md) › [01-8 可靠性与事务](../../01_消息模型与投递_08_可靠性与事务.md) › [01-8-1 outbox 适配与 relay 切换](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) › 详细设计

> **状态（2026-11-11 定稿）**：四项待拍板事项已逐项确认（§15），设计定稿；本轮完成定稿标注与冲突字面回写（§13）。**仍未改代码、未登记 Kiwi、未提交、未推送**；实现须另行确认后按 §12 的跨仓口径分别提交。

## 0. 分章导航 <a id="toc"></a>

| 章 | 内容 |
| --- | --- |
| 1 | 任务级边界与输入基线 |
| 2 | 交付物清单与零变更清单 |
| 3 | 现状核对结论 |
| 4 | 形态选型定稿（含失败重试 / 游标 / 重复投递 / 清理责任） |
| 5 | 接口与数据契约 |
| 6 | 配置切换与回滚契约 |
| 7 | 与 07-4 / 平台级生产部署架构的同口径对齐 |
| 8 | 审计事件不丢与效果一次 |
| 9 | 事务消息 / CDC 后置边界 |
| 10 | 失败分支与处置 |
| 11 | 测试设计与验收映射 |
| 12 | 实现约束与跨仓提交口径 |
| 13 | 登记落点与文档同步（含冲突回写清单） |
| 14 | 边界与开放项 |
| 15 | 对齐记录 |

## 1. 任务级边界与输入基线 <a id="scope"></a>

**做什么**：把「业务提交 ⇒ 事件可投递」这条链的最后一跳从原实现切到 msg，并使其**可在配置层切过去、切回来**——在 bms 仓交付 `[outbox].provider = "msg"` 的 relay 实现与装配，在 msg 仓交付任务契约、跨仓联调用例与验收证据；明确失败重试、游标、重复投递与清理责任，以及切换 / 回滚配置与 07-4 / 平台级生产部署架构的同口径。

**不做什么**：

- 不做 msg 侧 outbox connector（扫 `sys_outbox`）——本形态只保留接口口子，实现归 01-8-2 的「CDC 口子」评估，且规划 §11 已定「不做通用 CDC 平台」；
- 不实现事务消息（D17 后置）、不改 `publish_transactional()` 的「首期委托 `publish()`」口径；
- 不换 outbox 表、不改表结构 / 字段 / 索引，不改 `BaseOutboxStore` / `BaseOutboxDispatcher` 端口契约；
- 不改 `[event].provider` / `[service_client].provider` 的键位与语义，不新增配置节（`Settings` 为 `extra="forbid"`）；
- 不改 bms 业务代码、不改调用方函数签名；不执行生产迁移（8B_05 复制能力仍为硬前置，D24 阻断）；
- 不替代 07-4 的编排 / 闸门 / 矩阵，不替代平台级生产部署架构的分域切换与真实链路联调，不替代 01-8-3 的可靠性验收；
- 不在本任务内推送远程，不触发 CI。

**输入基线**：

| 输入 | 事实 |
| --- | --- |
| 需求 / 任务 | [需求 01-8](../../../../../需求/01_需求_消息模型与投递.md#r01-8)、[01-8 父任务](../../01_消息模型与投递_08_可靠性与事务.md)、[01-8-1 任务](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) |
| 阶段 | 归 **8B_04**；计划表 §3 的 01-8 行与 §5 门禁 9 当前均记为未开始；进度只在计划表回写 |
| bms 发件箱现状 | `bms_core/outbox/base.py`：`BaseOutboxStore`（`outbox_store`）与 `BaseOutboxDispatcher`（`outbox_dispatcher`）；`store.py` 的 `SqlOutboxStore`、`dispatcher.py` 的 `PollOutboxDispatcher`（`plugin_name = "poll"`） |
| 最后一跳现状 | `PollOutboxDispatcher.dispatch_once()` 已把逐条投递交给 `EventPublisher.publish(record.to_envelope())`；`MsgEventPublisher`（`plugin_name = "msg"`，数据面 `POST /api/v1/msg/publish`）已由 04-5 + bms 07B `06_01` 交付 |
| 插件装配 | `core/assembly.py`：`register_plugin("outbox_dispatcher", "poll", PollOutboxDispatcherFactory(settings, app))`；`PluginWiring("outbox_dispatcher", BaseOutboxDispatcher, "outbox", "outbox_dispatcher")`；`MSG_PROVIDER_NAME = "msg"` 已定义 |
| 配置 | `[outbox_store].provider = "sql"`、`[outbox].provider = "poll"`、`[event].provider = ""`、`[service_client].provider = "http"`；`[outbox]` 另含 `enabled` / `poll_interval_seconds` / `batch_size` / `max_retries` / `retry_backoff_seconds` / `db_keys` |
| 切换证据 | bms `tests/outbox/test_provider_switch.py`（Kiwi 2442）：未确认事件不丢、换可用发布器后每条只成功一次、切回不重投 |
| 开关契约 | 原 07-4-1 §6 三态 / §8 窗口、07-4-2 §4 / §8（该两文档已随部署域整体剥离，口径按本任务 §6.2 沿用） |
| 跨仓夹具 | msg `tests_support/bms_port_contract.py`（沿用 04-5 跨仓契约测试，不复制协议实现） |
| 覆盖率口径 | 见 bms《[AI开发规范](../../../../../../../../bms文档/规范/AI开发规范.md)》「覆盖率要求」节（本设计不复述数字） |
| 用例号 | **待设计定稿后先在 Kiwi 登记取得编号**，本轮不登记、不编造；口径见 §11 |

## 2. 交付物清单 <a id="deliverables"></a>

> 本轮只交付「详细设计文档」一行；其余为设计定稿后实施阶段的交付物，列出以便验收映射与跨仓分工。

| 仓 | 类别 | 交付物 | 本轮 |
| --- | --- | --- | --- |
| msg | 详细设计 | 本文件 `01-8-1/设计/01_详细设计_01-8-1_outbox适配与relay切换.md` | **是** |
| bms | 适配实现 | `bms_core/outbox/dispatcher.py` 追加 `MsgOutboxDispatcher`（沿 `PollOutboxDispatcher` 派生，`plugin_name = MSG_PROVIDER_NAME`） | 否 |
| bms | 装配 | `bms_core/core/assembly.py`：`PollOutboxDispatcherFactory` 抽出可覆写的 publisher 解析钩子；新增 `MsgOutboxDispatcherFactory` 并 `register_plugin("outbox_dispatcher", MSG_PROVIDER_NAME, ...)` | 否 |
| bms | CLI 通道 | `backend/ops/outbox.py` 的 `_build_dispatcher()` 改为按 `[outbox].provider` 解析，不再硬编码 `PollOutboxDispatcher`（§5.3） | 否 |
| bms | 配置说明 | `backend/config.toml` 的 `[outbox]` 注释补 `msg` 取值语义；**缺省值仍为 `poll`** | 否 |
| bms | 用例 | `libs/bms_core/tests/outbox/test_msg_outbox_switch.py`（T1~T9，§11） | 否 |
| msg | 任务契约 | 01-8-1 任务文档补「前置契约（已交付）」/「协同契约」；01-8-2 / 01-8-3 / 平台级生产部署架构补消费方标注（§13） | 否 |
| msg | 跨仓联调用例 | `services/msg/tests/client/` 下复用 `tests_support/bms_port_contract.py` 的联调用例（真 msg 应用 + 真数据面） | 否 |
| msg | 验收证据 | `01-8-1/实施/`、`01-8-1/测试/` 各一份记录（命令 / 原始输出 / 结论 / 偏差与遗留） | 否 |
| 两仓 | 记录回写 | 计划表 §3 01-8 行 + §5 门禁 9；冲突字面回写（§13） | 否 |

**零变更清单**：`BaseOutboxStore` / `BaseOutboxDispatcher` 端口方法集与契约版本；`SqlOutboxStore` 与 `sys_outbox` 表结构；`PollOutboxDispatcher` 既有行为（重试 / 退避 / 死信 / 轮询参数）；`MsgEventPublisher` 与 `bms_core.msgclient` 全链；`[event].provider` / `[service_client].provider` 键位与语义；`MsgServiceClient`；07-4 的 `preconditions.toml` / `switch_matrix.toml` / `rollback_scenarios.toml` 单源文件字段；RocketMQ / outbox / HTTP 原链路；契约快照；错误码表；msg 仓服务端业务代码；`deploy/compose/msg.yml` / `Dockerfile`；《架构设计 · 接入面》§9 语义节点（只写语义，不写实现细节，见《[文档生成规范](../../../../../../../../bms文档/规范/文档生成规范.md)》§8.6）。

## 3. 现状核对结论 <a id="current"></a>

| # | 现状 | 处置 |
| --- | --- | --- |
| 1 | relay 已把最后一跳交给 `EventPublisher` 端口；`MsgEventPublisher` 已交付并注册 | 形态 A 的实质是「让 `[outbox].provider` 也能选中 msg 发布实现」，**不重写轮询与重试**（§4） |
| 2 | 需求 01-8 背景写「适配层只换了 `EventPublisher` / `BaseServiceClient`，没换 `BaseOutboxStore` / relay」 | 本设计不换 `BaseOutboxStore`（表与事务边界不动），只交付可切的 relay 实现；背景里的「没换 relay」由本任务补齐，不改需求文档字面 |
| 3 | `[outbox].provider` 已是 `PluginSelection` 键（`poll` / 空串），`Settings` 为 `extra="forbid"` | 复用该键，不新增配置节（§6.1） |
| 4 | `MsgEventPublisherFactory.create()` 在地址 / 命名空间缺失时抛 `ConfigError`（不静默回落） | msg 版 relay 复用同一入口，非法配置在**装配期**拒启（§6.2） |
| 5 | `ops/outbox.py` 的 `_build_dispatcher()` 硬编码 `PollOutboxDispatcher.from_settings(...)` | 不改则 CLI 通道与运行期不一致，切换不闭环；列为必交付项与用例 T8（§5.3） |
| 6 | `PollOutboxDispatcherFactory.create()` 读 `settings.event.provider` 取发布器 | 抽出可覆写钩子，msg 版固定解析 `MSG_PROVIDER_NAME`；默认行为不变（§5.2） |
| 7 | 01-8-1 §2-3、07-4 父任务 §3-2 写「非法值 fail-safe 回原链路」，与 07-4-1 §6.2 / 07-4-2 输入基线的「非法拒启」冲突 | 按拍板沿用已定稿「非法拒启」；冲突字面登记回写，本轮不改既有文档（§13、§15） |
| 8 | `switch_matrix.toml` 的 `config_fingerprint` 口径为「两个 provider 值 + 命名空间 + msg 实例地址 + 策略」 | 本任务新增第三个 provider，需扩为三个；登记为 07-4-1 协同回写项（§6.4、§13） |
| 9 | 知识档案 [14](../../../../../../../../bms文档/资料/知识档案/消息与事件驱动/14_事件驱动_事务性发件箱.md) §8 把「outbox 表保留 + relay 换 msg 客户端」列为 first step；[16](../../../../../../../../bms文档/资料/知识档案/消息与事件驱动/16_事件驱动_CDC与日志尾随.md) §7 把 connector 列为「8B_04 评估」且首期不做业务表 CDC | 与拍板形态一致；connector 只留口子（§9） |
| 10 | 本地无 Docker / 无真实 RocketMQ broker | 联调主证据为进程内真 msg 应用 + 真 `SqlOutboxStore`；原链路事件侧真实集群归平台级生产部署架构（§14） |

## 4. 形态选型定稿 <a id="form"></a>

**定稿：形态 A —— bms 侧 relay 的 msg 版本**。在 bms 仓交付 `plugin_name = "msg"` 的 `BaseOutboxDispatcher` 派生实现，复用真 `SqlOutboxStore` 与 `bms_core.msgclient` / `MsgEventPublisher`；**形态 B（msg 侧 connector 扫 outbox）只保留接口口子，不在本任务实现**。msg 仓不放第二套客户端实现。

依据：① 需求 01-8 内容 1 与任务 01-8-1 §2-1 要求「定一种」；② 硬约束「落点以 bms 仓为主」；③ 知识档案 14 §8 的 first step 与「只换 `EventPublisher` 实现」；④ 规划 §11「不做通用 CDC 平台」与 01-8-2 的「CDC 口子」边界；⑤ 架构 [05 §9](../../../../../../../设计/架构设计/05_架构设计_接入面_协议与客户端.md#adapter) 的「可选」在本设计收口为定稿，但架构节点只写语义、不回写实现细节。

### 4.1 实现形态 <a id="form-shape"></a>

- `MsgOutboxDispatcher` 沿 `BaseOutboxDispatcher → PollOutboxDispatcher` 链派生（继承链固定前三段不变，符合架构铁律「主体内建必须继承」），**只覆写 `plugin_name = MSG_PROVIDER_NAME`**；轮询、`dispatch_once()`、`dispatch_due()`、`setup()` / `aclose()`、指标上报全部继承，零逻辑复制。
- 与 `poll` 版的**唯一实质差异**：工厂注入的 `EventPublisher` 固定为 `MSG_PROVIDER_NAME` 解析出的 `MsgEventPublisher`（§5.2）。这正是「relay 换成 msg 客户端」的落点。
- `from_settings()` 为 `classmethod`，子类调用时构造 `MsgOutboxDispatcher` 实例，轮询参数仍取 `[outbox]` 既有字段。

### 4.2 四项责任（任务 §2-1 要求逐项说明） <a id="form-responsibilities"></a>

| 责任 | 定稿口径 | 与形态 B 的差异 |
| --- | --- | --- |
| **失败重试** | `MsgEventPublisher` 自身不重试（短超时 `DEFAULT_PUBLISH_TIMEOUT`，失败抛 `EventPublishError`）；退避重试、计次、超限转死信全部由继承的 `dispatch_once()` 调 `mark_failed()` 承担（`[outbox].max_retries` / `retry_backoff_seconds`）。**两处都重试会让失败次数相乘**，故本层不加重试 | 形态 B 需在 connector 侧另建重试与死信，且要跨库回写状态 |
| **位点 / 游标** | **不新增游标**。沿用 `claim_pending(session, now, limit)` 按 `(aggregate_key, id)` 同聚合取队首；`status`（`pending` / `delivered` / `dead`）+ `retry_count` + `next_retry_at` + `delivered_at` 即既有游标，全在 bms 业务库同事务内 | 形态 B 需自持扫描位点（binlog offset / 高水位），崩溃恢复与三库方言差异见知识档案 16 §3 |
| **重复投递** | 至少一次；`OutboxRecord.to_envelope()` 保真 `event_id` 作幂等键，`mark_delivered()` 后不再取；消费端按 `event_id` 幂等（[04-4](../../../../04_远程调用/04_远程调用_04_错误语义与幂等重试/04_远程调用_04_错误语义与幂等重试.md)）；切回不重投（Kiwi 2442 证据）。**不引入第二套去重** | 形态 B 需自带去重台账，与消费端幂等形成两套 |
| **清理责任** | **归 bms**：`sys_outbox` 行的状态流转、`replay()`、死信看板与既有归档口径均不变；msg 侧不删行、不托管生命周期、不写回业务库 | 形态 B 要把「谁标记已投递、谁清理」移到 msg 侧，跨库写权限与表归属须另定 |

### 4.3 两条底线 <a id="form-baseline"></a>

「至少一次 + 消费端幂等」不变；「业务提交 ⇒ 事件一定可投递」不变。变的只是「最后一跳由哪个发布实现驱动」。

## 5. 接口与数据契约 <a id="contract"></a>

### 5.1 端口契约（零改动） <a id="contract-port"></a>

- `BaseOutboxStore`：`enqueue` / `claim_pending` / `mark_delivered` / `mark_failed` / `replay` / `list_dead_letters` / `get_dead_letter` / `set_dead_letter_status` / `backlog`；`plugin_key = "outbox_store"`。
- `BaseOutboxDispatcher`：`enabled` / `dispatch_once(db_key)` / `dispatch_due()`；`plugin_key = "outbox_dispatcher"`，`contract_version` 沿用 `DEFAULT_CONTRACT_VERSION`，不升版本。
- `EventPublisher.publish(event)`：msg 版经 `MsgEventPublisher` 转 `POST /api/v1/msg/publish`（JSON 数据面，**首期不走 `/msg/publish` 帧入口**，与 `PUBLISH_PATH` 注释一致）。
- `OutboxRecord.to_envelope()`：`event_id` / `event_type` / `event_version` / `aggregate_key` / `tenant_id` / `trace_id` / `occurred_at` / `payload` 保真透传，不新增字段。

### 5.2 工厂与装配 <a id="contract-factory"></a>

| 项 | 定稿 |
| --- | --- |
| `MsgOutboxDispatcherFactory` | `plugin_key = "outbox_dispatcher"`、`plugin_name = MSG_PROVIDER_NAME`；构造参数与 `PollOutboxDispatcherFactory` 同（`settings` + `app`） |
| publisher 解析钩子 | 在 `PollOutboxDispatcherFactory` 抽出 `_resolve_publisher()`（默认 `resolve_plugin("event", settings.event.provider, expected_version=EventPublisher.contract_version)`）；`MsgOutboxDispatcherFactory` 覆写为 `resolve_plugin("event", MSG_PROVIDER_NAME, ...)`。**默认行为逐字不变** |
| 其余依赖 | `store` 取 `resolve_plugin("outbox_store", settings.outbox_store.provider, ...)`；`metrics` / `engine_registry` / `session_factory` 与 `from_settings()` 参数全部沿用父类口径，不复制装配逻辑 |
| 注册 | `register_plugin("outbox_dispatcher", MSG_PROVIDER_NAME, MsgOutboxDispatcherFactory(settings, app))`，与既有 `"poll"` 登记并列；`resolve_plugin` 单值解析，装配只产出一个实现 |
| 配置校验来源 | `MsgEventPublisherFactory.create()` 内的 `msg_base_url(settings)` / `msg_namespace(settings)`：地址不可用或命名空间为空 → `ConfigError`（指名键名），复用 07-4-1 T8 同一入口 |

**两键职责分离（设计定稿口径）**：`[outbox].provider = "msg"` 表示「relay 的最后一跳走 msg」，其工厂**固定**以 `MSG_PROVIDER_NAME` 解析事件发布实现，**不读 `[event].provider`**；`[event].provider` 仍按 07-4-1 原语义管「直连发布端口」，本任务不改。理由：避免运维漏改一个键造成「relay 切了、发布器还在原实现」的假切换；避免两个键必须联动才能让切换生效；同时不产生第二条通道（`resolve_plugin` 单实例）。若评审要求两键联动，须先回写本设计（§14 开放项）。

### 5.3 CLI 通道一致性 <a id="contract-cli"></a>

`ops/outbox.py` 的 `_build_dispatcher()` 现状硬编码 `PollOutboxDispatcher.from_settings(...)`，且 `_resolve()` 只在 provider 非空时才建注册表。若不同步，`python -m ops.outbox dispatch` 在 `[outbox].provider = "msg"` 时仍按 `poll` 装配，CLI 与运行期不一致，切换 / 回滚演练不闭环。

定稿：抽出**与 `FastAPI` 应用无关的公共构造入口**（`bms_core` 内，供应用装配与 CLI 共用同一份判据），CLI 按 `settings.outbox.provider` 解析投递器；实现时确定函数签名并写入实施记录。用例 T8 断言两通道解析出同一实现。**不新增第二套装配判据**。

### 5.4 集合声明 <a id="contract-collections"></a>

新增 / 改动的 bms 代码中，凡集合声明一律使用 `ConcurrentStable*` / `ConcurrentStableFrozen*`（`ConcurrentStableList` / `ConcurrentStableFrozenList` / `ConcurrentStableDict` / `ConcurrentStableFrozenDict` / `ConcurrentStableSet` / `ConcurrentStableFrozenSet`），**不裸用 `dict` / `list` / `set` 作声明**；与调用点需要内建容器的转换沿用既有 `.items()` / `dict(...)` 惯例（`MsgEventPublisher._request_body()` 现状即如此）。护栏用例 T9 静态断言。

## 6. 配置切换与回滚契约 <a id="switch"></a>

### 6.1 键位 <a id="switch-keys"></a>

复用既有 `[outbox].provider`，**不新增配置节、不改 `Settings` 模型**（`extra="forbid"`）。它成为与 `[event].provider` / `[service_client].provider` 并列的第三个 provider 键；三者取值口径一致、缺省各自为原实现。

| 键 | 缺省 | 切到 msg | 切回 |
| --- | --- | --- | --- |
| `[outbox].provider` | `poll`（原 relay） | `msg` | 改回 `poll` |
| `[event].provider` | `""`（null 占位）/ `rocketmq` | `msg`（07-4 口径，本任务不改） | 07-4 口径 |
| `[service_client].provider` | `http` | `msg`（07-4 口径，本任务不改） | 07-4 口径 |

`backend/config.toml` 的 `[outbox]` 注释补 `msg` 取值语义；**缺省值保持 `poll` 不变**。

### 6.2 三态与 fail-safe（沿用 07-4 定稿） <a id="switch-states"></a>

| 态 | 判据 | 行为 |
| --- | --- | --- |
| 缺省 / 原实现 | `provider` 未配置或 `poll` | 解析 `PollOutboxDispatcher`，与切换前逐字段一致 |
| 合法-msg | `provider = "msg"` 且 msg 地址 / 命名空间齐备 | 解析 `MsgOutboxDispatcher`，最后一跳走 `MsgEventPublisher`；业务代码零改动 |
| 空串 | `provider = ""` | **bms 既有 `null` 占位语义（`NullOutboxDispatcher`，不投递）**，不产生静默投递 |
| 非法-未注册 | 任意未注册实现名 | 装配期 `PluginError`，**不回落** |
| 非法-配置不全 | `provider = "msg"` 但实例地址 / 命名空间缺失或非法 | 装配期 `ConfigError` 并指名键名，**不回落**（由 `MsgEventPublisherFactory` 抛出） |

> **差异登记（不臆造）**：07-4-1 §6.2 的「缺省 / 空串 = 原实现」是 `[event]` / `[service_client]` 口径；bms `[outbox]` 的空串既有语义是 `null` 占位（不投递），**不是**回 `poll`。本设计如实沿用 bms 既有语义，并在 §13 登记该差异说明，不把它写成「回原实现」。

**fail-safe 口径**：「缺省回原实现」是缺省语义；「非法值拒启」是安全语义。静默回落会让「以为切到 msg、实际还在原链路」的假切换进入观察窗口，长期比启动失败更贵——这是 07-4-1 §6.2 的定稿理由，本任务沿用。

### 6.3 生效方式 <a id="switch-effect"></a>

- 只改配置文件 / 环境变量 + **重启调用方服务**；不发布业务代码、不重建业务镜像；插件装配在启动期完成，运行期不监听配置文件（bms 07B `06_01` 口径）。
- 切换 / 回滚不删除原链路，`poll` 实现与 RocketMQ / outbox / HTTP fallback 保留一个版本。
- 本任务只提供**可切的键与证据**；生产环境的真实切换由平台级生产部署架构执行，当前仍被 8B_05 复制能力硬阻断（D24 / `check --env prod` 退出 1）。

### 6.4 `config_fingerprint` 扩展 <a id="switch-fingerprint"></a>

07-4-1 §6.3 定义为「两个 `provider` 值 + 命名空间 + msg 实例地址 + 策略的规范化摘要」。本任务新增第三个 provider 键，指纹须扩为 **`[event].provider` + `[service_client].provider` + `[outbox].provider` + 命名空间 + msg 实例地址 + 策略**，规范化摘要算法不变。该扩展属 07-4-1 的协同契约回写（§13），本轮只登记，不改 `ops.migration` 与矩阵单源文件。

## 7. 与 07-4 / 平台级生产部署架构的同口径对齐 <a id="alignment-74"></a>

| 口径 | 07-4 / 平台级生产部署架构既有结论 | 本任务对齐方式 |
| --- | --- | --- |
| 开关三态 | 缺省原实现 / 合法 msg / 非法拒启（07-4-1 §6.2） | §6.2 逐条沿用；`config_fingerprint` 扩三键（§6.4） |
| 切换只改配置 | 改 `provider` + 重启调用方，不发布业务代码（07-4-1 §6.3 / D23） | §6.3 一致 |
| 在途请求 | 先 drain；无法 drain 时同一幂等键 + 服务端去重（07-4-1 §8 / 07-4-2 §8） | 沿用，不新增机制；用例 T6 |
| 未确认事件 | 事件侧**不双写**；未确认行留在 outbox，由既有投递器在 provider 生效后补发；发布失败必须抛 `EventPublishError`（07-4-1 §8） | `MsgEventPublisher` 不吞异常；`mark_failed` 保行计次；用例 T5 / T6 |
| outbox 重投 | 按 `event_id` 标记已投递，切回不重投（bms Kiwi 2442 证据） | 直接复用该证据，不重写 outbox；用例 T6 |
| 双写窗口 | 首期**禁止双写**：`resolve_plugin` 单值解析，一次调用只产生一条消息（07-4-1 §8 T11） | 装配只产出一个 dispatcher；用例 T7 |
| 回滚语义 | 只改配置 + 重启；原链路保留；记录必备字段；无法确认的残留显式记 `degraded`（[09 决策与风险](../../../../../../../设计/架构设计/09_架构设计_决策与风险.md) §2.6） | §6.3 一致；回滚完整闭环归 07-4-2，本任务只验「可切回 + 行为恢复」 |
| 分域顺序 | `switch_matrix.toml`：一条低风险事件 + 只读 RPC → 审计事件 → 业务事件逐域；每域观察窗口与回退判据（平台级生产部署架构） | 本任务不改矩阵、不新增域；`[outbox].provider` 作为第三个可切键供平台级生产部署架构消费（§13 协同契约） |
| 生产闸门 | 8B_05 未交付 → `check --env prod` 拒绝对生产放行（07-4-1 §4） | 本任务不碰闸门；真实集群联调 / 真盘演练归平台级生产部署架构 |
| 指标与阈值 | 矩阵指标名来自 `bms_msg.metrics.names`，阈值数值归平台级容量实测 | 本任务沿用 bms `bms_outbox_delivery_total` / `bms_outbox_backlog` 既有指标，不新增指标名、不定阈值 |

## 8. 审计事件不丢与效果一次 <a id="reliability"></a>

- **链路**：业务事务内 `enqueue`（同库同事务）→ `claim_pending` → `MsgOutboxDispatcher` → `MsgEventPublisher` → `POST /api/v1/msg/publish` → msg 数据面 → 消费端。审计链 `sys.operation.logged` 经同一条 outbox 通道，切换后仍可达、可查。
- **不丢**：发布失败抛 `EventPublishError` → `mark_failed()` 保留行并计次（`error_msg` 落最近失败原因）→ 退避后重试 → 超 `max_retries` 转死信，死信看板可查；`DispatchResult`（`published` / `failed` / `dead` / `backlog`）与 `bms_outbox_delivery_total` / `bms_outbox_backlog` 提供可定位记录。**不静默丢弃**。
- **效果一次**：`event_id` 幂等键 + 消费端幂等（04-4）；重放与迁移在途不引入第二套去重；重复消费不产生重复副作用。
- **记录级定位**：复用 [05-4](../../../../05_多租户与安全/05_多租户与安全_04_审计留痕与记录级定位/05_多租户与安全_04_审计留痕与记录级定位.md) 已交付的四类留痕与 `record_ref`；本任务不自建审计表、不绕过基座写入、不改审计字段。
- **验收映射**：审计事件全链路不丢与记录级定位的端到端用例归 **01-8-3**；本任务只交付「切换后可达 + 失败可重试 + 有可定位记录」的单元 / 跨仓用例（§11），不重复取证。

## 9. 事务消息 / CDC 后置边界 <a id="postponed"></a>

- 事务消息首期**不实现**（D17 / [规划 §11](../../../../../../../规划/01_规划_消息平台.md)）：`publish_transactional()` 保持「委托 `publish()`」；半消息 / 回查 / 提交或回滚的端口位置、事件类型与失败分支由 **01-8-2** 承接，本任务不写占位实现。
- CDC 只保留「outbox connector」形态的**接口口子**：不做通用 CDC 平台、不接 binlog / WAL、不尾随业务表、不承诺多源同步；msg 仓服务端不新增扫描端点。
- 契约与文档中不得出现「事务消息已支持」「CDC 已接入」等误导性描述；文档断言归 01-8-2，本任务只做「零变更」断言（T9：未引入事务消息 / connector 实现）。

## 10. 失败分支与处置 <a id="failures"></a>

| # | 失败 | 触发 | 表现 | 处置 |
| --- | --- | --- | --- | --- |
| 1 | msg 实例不可达 | 发布连接失败 / 非 200 | 单条 `mark_failed()`，行保留、计次递增、退避 | 修地址 / 网络；恢复后由投递器补发，不改业务代码 |
| 2 | 发布失败被吞 | 不可达未抛 `EventPublishError` | 事件被标记已投递却未到达 | 属实现缺陷：回写设计并报 bms；用例 T5 拦截 |
| 3 | 重试超上限 | `retry_count > max_retries` | 转死信（`dead`），死信看板可查 | 显式失败，不静默丢弃；重放入口用既有 `replay()` |
| 4 | 配置不全 | `provider = "msg"` 但地址 / 命名空间缺失 | 装配期 `ConfigError`，指名键名 | 补配置后重启；不回落、不带病启动 |
| 5 | 未注册实现名 | 写错 provider | 装配期 `PluginError` | 修正取值；不回落 |
| 6 | 两键漏改 | `[event].provider` 仍原实现 | 不影响本任务切换（§5.2 职责分离） | 记录 `config_fingerprint` 三值用于对账 |
| 7 | CLI 未同步 | `_build_dispatcher()` 仍硬编码 `poll` | CLI 与运行期实现不一致 | 按 §5.3 抽公共入口；T8 拦截 |
| 8 | 双写 / 重复副作用 | 一次调用产生两条消息 | 用例 T7 失败 | 停止切换，查是否存在第二条通道 |
| 9 | 切回后有重投 | 已 `delivered` 行被再投 | 用例 T6 失败 | 查 `claim_pending` 状态条件；复用 Kiwi 2442 判据 |
| 10 | 提交失败 | 发布成功但事务提交失败 | 下轮重投（至少一次），消费端幂等收敛 | 既有语义，不新增补偿 |
| 11 | 多实例重复抢批 | `claim_pending` 无 `SKIP LOCKED` / 租约列 | 无效投递放大，消费端幂等兜底 | 知识档案 14 §4 已知开放项，归 02-8 / 平台级容量实测（§14） |
| 12 | 集合裸声明 | 新增代码出现裸 `dict/list/set` 声明 | T9 静态断言失败 | 改 `ConcurrentStable*` |

## 11. 测试设计与验收映射 <a id="tests"></a>

**Kiwi 登记口径（设计定稿后、编码前执行，本轮不登记）**：按《[KiwiTCMS部署使用说明](../../../../../../../../bms文档/资料/开发服务器/linux/KiwiTCMS部署使用说明.md)》§5.2/§5.3——Category「平台骨架」、Priority `P2`、Status `CONFIRMED`、Tag「自动化」；一个任务登记**一条**用例，多条自动化断言共用同一 `kiwi_id`；平台自增主键为准，先登记取得编号再回填代码 `@pytest.mark.kiwi_id(<编号>)` 与任务记录，不复用 / 不改派既有编号（含 2442）。

| 编号 | 用例 | 覆盖 | 对应验收 |
| --- | --- | --- | --- |
| T1 | 三态解析：`poll` / `msg` / `""`（null 占位）；未注册名 → `PluginError`；`msg` 配置不全 → `ConfigError` 且指名键名 | §6.2 | 配置层可切、非法拒启 |
| T2 | 切换正反：`[outbox].provider` 由 `poll` → `msg` → `poll`，解析出预期实现；三键 `config_fingerprint` 可复算且与切换点一致 | §6.1 / §6.4 | 切换 / 回滚配置可复现 |
| T3 | 业务代码零改动：真实业务函数签名、`BaseOutboxStore` / `BaseOutboxDispatcher` 端口方法集、契约版本、`to_envelope()` 字段与切换前逐项一致 | §5.1 | 业务代码零改动 |
| T4 | 审计事件不丢（跨仓）：真 `SqlOutboxStore` + 真 msg 应用 / `MsgEventPublisher`，切换后事件可达、载荷 / 属性 / 租户 / `trace_id` 一致 | §8 | 切换后审计事件不丢 |
| T5 | 失败重试：不可达发布抛 `EventPublishError`，行保留、`retry_count` 递增、`error_msg` 可定位；恢复后补发一次；超上限转死信可查 | §4.2 / §8 / §10 #1~#3 | 失败可重试且有可定位记录 |
| T6 | 效果一次：重复投递 / 重放 / 切回三类场景，已 `delivered` 行不重投，消费端按 `event_id` 幂等（复用 Kiwi 2442 判据） | §4.2 / §7 | 重复消费效果一次 |
| T7 | 双写窗口：端口单实现；一次调用恰好一条消息、两次调用恰好两条 | §7 | 无双写路径 |
| T8 | CLI 通道一致：`ops.outbox dispatch` 与运行期按同一 `provider` 解析出同一实现；`provider` 为空串时不投递 | §5.3 | 切换 / 回滚全通道闭环 |
| T9 | 实现约束护栏：变更模块无裸 `dict/list/set` 集合声明；未引入事务消息 / connector 实现；msg 仓无第二套客户端 | §5.4 / §9 / §12 | 硬约束与后置边界 |

- 覆盖率按 bms《[AI开发规范](../../../../../../../../bms文档/规范/AI开发规范.md)》「覆盖率要求」节判定；本地只跑单文件 / 单节点定向用例，全量测试由 CI 复核。
- 跨仓用例在 msg 仓复用 `tests_support/bms_port_contract.py` 构造真应用，断言与 bms 侧共用同一判据，不复制协议实现。
- 真机 / 生产窗口不在本任务：真实集群联调、PromQL 取值、审计表行级对账登记为遗留并明确归口（§14）。

## 12. 实现约束与跨仓提交口径 <a id="constraints"></a>

| 项 | 强制口径 |
| --- | --- |
| 集合声明 | 一律 `ConcurrentStable*` / `ConcurrentStableFrozen*`，不裸 `dict` / `list` / `set`（§5.4） |
| 解释器 | 只用 `D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe`（3.14）；**禁止 PATH 上的 `python`**。命令示例：`D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m pytest backend/libs/bms_core/tests/outbox/test_msg_outbox_switch.py -k "switch or rollback"`；CLI：`D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m ops.outbox dispatch` |
| 配置唯一来源 | 连接串 / 实例地址 / 命名空间只经 `Settings`（含环境变量）读取；代码、脚本、CI 不写死连接串或库路径 |
| 用例登记 | 设计定稿后**先登记 Kiwi 用例再编码**（Category 平台骨架 / P2 / CONFIRMED / Tag 自动化）；本轮不登记、不编码 |
| 跨仓分工 | **bms 仓为主**：适配实现 / 装配 / CLI / 配置注释 / 用例；**msg 仓**：任务契约、跨仓联调用例、验收证据。两仓**分别提交** |
| 提交 / 推送 | 说「提交」只做 `commit`，说「推送」才走 `engkit push`；推送不触发 CI。本轮**不提交、不推送** |
| 提交粒度 | 按《AI开发规范》§5「提交次数与推送时点」：一个任务最多两次提交（`feat(scope)` 一次、`docs(scope)` 一次）；链外任务先询问是否提交 |
| 进度落点 | 进度类字段的唯一落点是**计划表**，需求与任务文档不承载（口径见《计划文档规范》） |
| 文档校验 | 文档三项校验（links / docs-scope / eol）通过；本设计链接均为相对链接 |

## 13. 登记落点与文档同步 <a id="registry"></a>

| 落点 | 内容 | 本轮 |
| --- | --- | --- |
| 本设计 | 任务目录 `设计/` 落详细设计（交付物清单 / 接口与字段边界及失败分支 / 测试设计与验收映射 / 登记落点 / 边界与开放项 / 对齐记录） | **是** |
| 计划表 §3 / §5 | 01-8 行与门禁 9 现状（实施完成时回写；本轮不动） | 否 |
| 01-8-1 任务文档 | 补「前置契约（已交付）」：`[outbox].provider` 三态、`MsgOutboxDispatcher`、三键 `config_fingerprint`、Kiwi 用例号 | 否 |
| 01-8-2 / 01-8-3 | 「协同契约」：connector 口子边界（§9）、可靠性验收入口与可复用证据（§8） | 否 |
| 07-4-1 详细设计 | §6.3 `config_fingerprint` 扩为三个 provider 值；§6.1 键位表补 `[outbox].provider`（协同契约，不改开关语义） | 否 |
| 07-4-2 详细设计 | 输入基线补 `[outbox].provider` 与 msg 版 relay 语义 | 否 |
| 平台级生产部署架构任务文档 | 「前置契约」补：第三个可切键与分域切换时的消费方式（不新增域） | 否 |
| **冲突回写** | 01-8-1 任务 §2-3、07-4 父任务 §3-2 的「非法值 fail-safe 回原链路」字面 → 改为「缺省回原实现 / 非法值装配期拒启」，与 07-4-1 §6.2、07-4-2 输入基线、T7/T8 用例一致（对齐记录见 §15 #3） | 否（本轮只登记） |
| 差异登记 | `[outbox]` 空串 = `null` 占位 ≠ 07-4-1 的「空串 = 原实现」（§6.2），在 01-8-1 任务契约中写明，避免验收误读 | 否 |
| 架构 / 规范 | **不新增架构节点、不回写架构 05 §9**（架构节点只写语义，实现细节落本设计，见《文档生成规范》§8.6）；不修改规范 | 否 |

### 13.1 定稿收尾执行清单（2026-11-11） <a id="registry-done"></a>

四项拍板已逐项确认，冲突字面回写获授权，本轮执行以下动作（**全部为 msg 仓文档，零代码改动**）：

| # | 动作 | 落点 | 状态 |
| --- | --- | --- | --- |
| 1 | 设计标注定稿、记录本清单 | 本文件头部 / §13.1 / §15 #13 | 已执行 |
| 2 | 冲突字面回写：缺省 `poll`、非法值装配期拒启 | [01-8-1 任务文档 §2-3](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) | 已执行 |
| 3 | 冲突字面回写：不静默回落、不删原链路 | 原 07-4 父任务 §3-2（该文档已随部署域整体剥离） | 已执行 |
| 4 | 本任务补「设计定稿与协同契约」 | [01-8-1 任务文档 §5](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) | 已执行 |
| 5 | 消费方标注「协同契约（待交付）」 | [01-8-2](../../01_消息模型与投递_08_可靠性与事务_02_事务消息与CDC口子/01_消息模型与投递_08_可靠性与事务_02_事务消息与CDC口子.md) / [01-8-3](../../01_消息模型与投递_08_可靠性与事务_03_可靠性验收/01_消息模型与投递_08_可靠性与事务_03_可靠性验收.md) | 已执行 |

**明确未执行（须在代码交付后或另行确认后处理）**：

- 计划表 §3 的 01-8 行与 §5 门禁 9——进度只在计划表回写，且须等实施完成，本轮不动；
- 07-4-1 §6.1 / §6.3 与 07-4-2 输入基线的**详细设计正文**协同回写（三键 `config_fingerprint` / `[outbox].provider` 语义）——已交付任务的详细设计，待本任务代码交付后按「前置契约（已交付）」口径回写，避免现在写「已交付」造成不实描述；
- 01-8-1 任务文档的「前置契约（已交付）」——同上，代码未交付前不写「已交付」；
- 架构 05 §9、规范、bms 仓任何文件——按 §2 零变更清单与《文档生成规范》§8.6 不动。

**实施后回写（2026-11-11，代码已交付）**：上述后两项中，07-4-1 §6.1 键位表已补 `[outbox].provider`、§6.3 指纹已扩为三值，07-4-2 输入基线已补 msg 版 relay 语义；01-8-1 任务文档 §5 与 01-8-2 / 01-8-3 / 平台级生产部署架构的标注统一改为「前置契约（已交付）」，写入 Kiwi **2481** 与 bms 提交 `7df9ae318`。计划表 §3 的 01-8 行与 §5 门禁 9 仍按 §7 遗留，待 01-8-2 / 01-8-3 完成后统一回写。

> 「协同契约」与「前置契约」的措辞按交付状态区分：设计定稿阶段只登记**待交付**契约，代码交付后才改标**已交付**（《AI开发规范》§7「不臆造」）。

## 14. 边界与开放项 <a id="open"></a>

| # | 开放项 | 影响 | 归口 |
| --- | --- | --- | --- |
| 1 | 8B_05 复制能力未交付（02-4 / 02-5 / 02-8 搁置） | 生产迁移硬阻断，本任务只做可切性与隔离环境证据 | 8B_05 / 平台级生产部署架构 |
| 2 | 真实 RocketMQ 集群 / 真实容器 / 真盘未联调 | 原链路事件侧证据为 `degraded`，不默认通过 | 平台级生产部署架构 |
| 3 | 域级键（平台级生产部署架构）未落地 | 只能整端口切换，回滚粒度粗 | bms 平台级生产部署架构 |
| 4 | `[event].provider` 与 `[outbox].provider` 是否联动 | 本设计定稿为职责分离（§5.2）；如需联动须先回写本设计 | 设计评审 |
| 5 | `config_fingerprint` 三键扩展需 07-4-1 协同回写 | 未回写前切换点摘要缺 `[outbox].provider` | 07-4-1 消费方 |
| 6 | 公共构造入口（CLI 与应用装配共用）的函数签名 | 实现阶段确定并写入实施记录 | 本任务实施 |
| 7 | 指标阈值 / 容量数值多为判据形状 | 不给出「超阈值」真实数值 | 平台级容量实测 |
| 8 | `claim_pending` 多实例重复抢批（无 `SKIP LOCKED` / 租约列） | 无效投递放大，消费端幂等兜底 | 02-8 / 平台级容量实测（知识档案 14 §4） |
| 9 | 死信表在真实 MySQL / PG / DM8 的 DDL 与时区行为 | 待部署窗口验证 | 平台级生产部署架构（计划 §6.1 #13） |
| 10 | 帧入口 `/msg/publish`（HTTP/2）未走 | 本任务沿用 JSON 数据面，不新开通道 | 后续性能治理 / 平台级容量实测 |
| 11 | 事务消息与 CDC 口子的触发条件与预算 | 首期不实现，须避免「已支持」误读 | 01-8-2 |

## 15. 对齐记录 <a id="alignment"></a>

| # | 事项 | 定稿（2026-11-11 逐项确认） |
| --- | --- | --- |
| 1 | **形态取舍** | 形态 A：bms 侧 relay 的 msg 版本（`MsgOutboxDispatcher` + 装配），复用真 `SqlOutboxStore` 与 `bms_core.msgclient`；形态 B（msg 侧 connector 扫 outbox）只留接口口子，不实现 |
| 2 | **切换 / 回滚键位** | 复用既有 `[outbox].provider`，不新增配置节；缺省 `poll` → `msg` → 切回 `poll`；与 `[event].provider` / `[service_client].provider` 同口径 |
| 3 | **口径冲突** | 沿用 07-4-1 §6.2 / 07-4-2 已定稿「缺省回原实现、非法值装配期拒启（PluginError / ConfigError）」，**不静默回落**；01-8-1 §2-3 与 07-4 父任务 §3-2 的冲突字面登记回写（§13），本轮不改既有文档 |
| 4 | **范围划分 / 是否改动某仓** | 本轮只落 msg 仓详细设计；后续实施 bms 仓为主（适配 / 装配 / CLI / 配置 / 用例），msg 仓只交任务契约、跨仓联调用例与验收证据，两仓分别提交；本轮**不改代码、不提交、不推送** |
| 5 | 实现形态 | `MsgOutboxDispatcher` 沿 `PollOutboxDispatcher` 派生、只覆写 `plugin_name`；与 `poll` 版唯一差异是工厂注入的发布实现，零逻辑复制（§4.1） |
| 6 | 两键职责 | `[outbox].provider` 的工厂固定解析 `MSG_PROVIDER_NAME`，不读 `[event].provider`；职责分离、避免假切换（§5.2） |
| 7 | 空串语义差异 | `[outbox]` 空串沿用 bms 既有 `null` 占位（不投递），不等同 07-4-1 的「空串 = 原实现」，差异登记（§6.2） |
| 8 | CLI 通道 | `ops/outbox.py` 必须同步按 provider 解析，否则切换不闭环（§5.3 / T8） |
| 9 | 四项责任 | 失败重试归投递器、游标沿用 outbox 状态列、重复投递靠 `event_id` + 消费端幂等、清理责任归 bms（§4.2） |
| 10 | 后置边界 | 事务消息与 CDC 首期不实现，只留口子，不宣称已支持（§9） |
| 11 | 硬约束 | 集合声明 `ConcurrentStable*`、解释器 `.venv/Scripts/python.exe`、Kiwi 先登记后编码、跨仓分别提交、「提交」只 commit /「推送」才 `engkit push` 且不触发 CI（§12） |
| 12 | 本轮状态 | 设计文档已产出；**未改代码、未登记 Kiwi、未提交、未推送**；设计定稿后再谈实现 |
| 13 | **设计定稿（2026-11-11）** | 四项拍板口径不变；本轮执行 §13.1 定稿收尾清单（定稿标注 + 两处冲突字面回写 + 消费方「协同契约（待交付）」标注），全部为 msg 仓文档；计划表 / 已交付任务详细设计 / 「前置契约（已交付）」按交付时点延后；**仍未改代码、未登记 Kiwi、未提交、未推送** |

> 依《[文档生成规范](../../../../../../../../bms文档/规范/文档生成规范.md)》编写 · 交付链条见 bms《[AI开发规范](../../../../../../../../bms文档/规范/AI开发规范.md)》§5 · 覆盖率口径见同规范「覆盖率要求」节 · 迁移语义见《[架构设计 · 决策与风险](../../../../../../../设计/架构设计/09_架构设计_决策与风险.md)》§2.6（D23 / D24）· 事务消息后置见同文档 D17
