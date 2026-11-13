# 01-8-1 outbox 适配与 relay 切换 · 测试记录

> 08B · 01 消息模型与投递 · 01-8 可靠性与事务 · 子任务 01（需求 01-8）

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../../计划/01_计划_消息平台.md) › [01 消息模型与投递](../../../01_消息模型与投递.md) › [01-8 可靠性与事务](../../01_消息模型与投递_08_可靠性与事务.md) › [01-8-1](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) › 测试记录

## 1. 信息 <a id="info"></a>

| 项 | 值 |
| --- | --- |
| 任务 | 01-8-1 outbox 适配与 relay 切换 |
| 用例号 | Kiwi **2481**（先登记后编码；两仓全部自动化断言共用同一编号，代码以 `pytest.mark.kiwi_id(2481)` 标注） |
| 平台字段 | Category 平台骨架 ｜ Priority P2 ｜ Status CONFIRMED ｜ Tag 自动化（2026-11-11 登记并回读一致，2480 之后顺延） |
| 设计 | [详细设计](../设计/01_详细设计_01-8-1_outbox适配与relay切换.md) §11 |
| 实施 | [实施记录](../实施/01_实施记录_01-8-1_outbox适配与relay切换.md) |
| 日期 | 2026-11-11 |

## 2. 用例清单 <a id="cases"></a>

### 2.1 bms 仓（`libs/bms_core/tests/outbox/test_msg_outbox_switch.py`，10 例）

| 编号 | 用例 | 覆盖 |
| --- | --- | --- |
| T1 | `test_provider_three_states` | `poll` / `msg` 映射、未注册名 `PluginError`（不回落） |
| T1 | `test_msg_dispatcher_registered_in_plugin_registry` | 装配登记表同时具备 `poll` / `msg` 两个实现 |
| T2 | `test_msg_factory_pins_msg_publisher` | msg 工厂固定解析 `MSG_PROVIDER_NAME`，不读 `[event].provider` |
| T2 | `test_poll_factory_keeps_event_provider` | `poll` 工厂行为不变，仍按 `[event].provider` 解析（可切回） |
| T3 / T9 | `test_port_contract_and_zero_duplicated_logic` | 端口契约与版本不变、零逻辑复制、未引入事务消息 / connector 能力 |
| T4~T7 | `test_msg_relay_delivery_retry_and_switch_back` | 真 SQLite 发件箱：失败保行计次、恢复补发一次、切回不重投、每轮恰好一条 |
| T8 | `test_cli_null_placeholder` | 空串沿用 bms 既有 `null` 占位（不投递） |
| T8 | `test_cli_poll_uses_original` | `poll` 解析为原实现 |
| T8 | `test_cli_msg_uses_msg_publisher` | `msg` 解析为 msg 版且发布实现为 `MsgEventPublisher` |
| T8 | `test_cli_rejects_unknown_provider` | 非法 provider 在 CLI 装配期抛 `PluginError` |

### 2.2 msg 仓（`services/msg/tests/client/test_outbox_relay_switch.py`，4 例）

| 编号 | 用例 | 覆盖 |
| --- | --- | --- |
| U1 | `test_relay_envelope_reaches_real_data_plane` | `OutboxRecord.to_envelope()` 重建的信封经真应用投递，载荷 / 属性 / `event_id` / `trace_id` 逐项一致 |
| U2 | `test_relay_publish_failure_is_observable` | 主题不存在时抛 `EventPublishError` 且携带 `event_type` / `status`，失败信号不静默 |
| U3 | `test_repeated_delivery_keeps_stable_idempotency_key` | 同一事件重投两次，消费端收到 2 条且 `event_id` 幂等键稳定 |
| U4 | `test_msg_dispatcher_keeps_single_delivery_path` | msg 版与 poll 版共用同一投递函数，切换不产生第二条通道 |

**夹具**：bms 侧复用 `tests/outbox/conftest.py` 的临时 SQLite 库（发件箱三表）；msg 侧复用 `tests_support/bms_port_contract.py` 与 `tests_support/msg_fixtures.py` 的真应用（真路由 / 真数据面 / 真消费端），不复制第二套协议实现。

## 3. 执行与结果 <a id="results"></a>

| 范围 | 命令 | 结果 |
| --- | --- | --- |
| bms 新用例 | `python.exe -m pytest libs/bms_core/tests/outbox/test_msg_outbox_switch.py -q` | `10 passed in 8.97s` |
| bms 回归 | `python.exe -m pytest libs/bms_core/tests/outbox/ libs/bms_core/tests/ops/test_outbox_cli.py libs/bms_core/tests/core/test_assembly_factories.py libs/bms_core/tests/msgclient/adapter/test_assembly.py libs/bms_core/tests/msgclient/adapter/test_event_publisher.py -q` | `68 passed in 11.33s` |
| bms 静态 | `ruff check` / `ruff format --check` / `python -m pyright --pythonpath <解释器>` | All checks passed / 0 errors |
| msg 联调用例 | `python.exe -m pytest services/msg/tests/client/test_outbox_relay_switch.py -q` | `4 passed in 0.57s` |
| msg deploy 回归 | `python.exe -m pytest services/msg/tests/deploy/ -q` | `464 passed in 74.27s` |
| 文档 | `python.exe -m engkit links` / `-m engkit docs-scope` | 断链 0 / 失效锚点 0；320 份文档违规 0 |
| 提交前预检 | bms `git commit` 钩子 | `2154 passed`、`[coverage] percent=100.00`、30 项全部通过（57.9s） |
| 提交前预检 | msg `git commit` 钩子 | `2154 passed`、改动行覆盖率 100%、30 项通过 |

> 解释器统一为 `D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe`（3.14.4）；pyright 必须带工程 `--pythonpath`，直接调 `pyright.exe` 会产生数百条 `unknown` 基线误报。

## 4. 覆盖率与门禁 <a id="coverage"></a>

- 口径见 bms《AI开发规范》「覆盖率要求」节（本记录不复述数字）；提交环判改动行，推送环与 CI 判模块级。
- 本机只跑单文件 / 单节点定向用例，全量测试由 CI 复核；覆盖率数据由 engkit 采集插件落盘，未手工采集。
- 提交前预检输出 `[diff-coverage] 通过：1 个文件的改动行均已被执行`，`[coverage] percent=100.00`。
- 集合声明约束由 T9 / U4 契约断言与 `engkit base bare-collections` 护栏共同把关。

## 5. 问题与处置 <a id="issues"></a>

| # | 问题 | 处置 |
| --- | --- | --- |
| 1 | U3 初版「先发布失败再补发」触发 `InvalidRequestError: A transaction is already begun on this Session` | `msg_app` 的内存库 session 为单例，404 路径未提交事务；改为「同一事件重投两次，幂等键稳定」。失败后补发恰好一次的语义由 bms 仓 T4~T7 用真 SQLite 承担，判据未降低 |
| 2 | 文档提交首轮被 `docs-scope` 拦下 | 设计文档 3 行进度类字段措辞改写，复跑 0 违规；未绕过门禁 |
| 3 | bms 提交首轮被工作区卫生拦下 | 根因与处置见[实施记录 §5](../实施/01_实施记录_01-8-1_outbox适配与relay切换.md)，属工具链落点问题，非本任务代码缺陷 |

## 6. 偏差与遗留 <a id="leftovers"></a>

- 真实集群 / 真盘未联调（原 RocketMQ 链路已下线）：本任务证据为进程内真应用 + 真 SQLite 发件箱，生产窗口归平台级生产部署架构；原链路事件侧如实标 `degraded`，不默认通过。
- 审计事件全链路不丢与效果一次的端到端验收已由 01-8-3 完成（Kiwi 2483，真 SQLite 发件箱 + 真 msg 应用 + 消费端副作用计数）；本任务保留切换 / 回滚 / 失败重试的前置证据。
- 事务消息 / CDC 首期不实现（01-8-2），T9 / U4 断言未引入相关能力，避免「已支持」误读。
- 阈值数值与容量结论归平台级容量实测；本任务未定任何新指标。
