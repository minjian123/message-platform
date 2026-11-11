# 01-8-1 outbox 适配与 relay 切换 · 实施记录

> 08B · 01 消息模型与投递 · 01-8 可靠性与事务 · 子任务 01（需求 01-8）

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../../计划/01_计划_消息平台.md) › [01 消息模型与投递](../../../01_消息模型与投递.md) › [01-8 可靠性与事务](../../01_消息模型与投递_08_可靠性与事务.md) › [01-8-1](../01_消息模型与投递_08_可靠性与事务_01_outbox适配与relay切换.md) › 实施记录

## 1. 信息 <a id="info"></a>

| 项 | 值 |
| --- | --- |
| 任务 | 01-8-1 outbox 适配与 relay 切换（需求 [01-8](../../../../../需求/01_需求_消息模型与投递.md#r01-8)） |
| 用例 | Kiwi **2481**（平台骨架 / P2 / CONFIRMED / 自动化；两仓全部自动化断言共用） |
| 设计 | [详细设计](../设计/01_详细设计_01-8-1_outbox适配与relay切换.md) |
| 实施日期 | 2026-11-11 |
| 范围 | **bms 仓为主**：relay 适配 / 装配 / CLI / 配置 / 用例；**msg 仓**：任务契约、跨仓联调用例、验收证据 |
| 提交 | bms `7df9ae318` ｜ msg 文档 `fc61604`、落点修复 `a5ee581`（本记录随后续提交） |

## 2. 交付物 <a id="deliverables"></a>

| 仓 | 类别 | 文件 | 说明 |
| --- | --- | --- | --- |
| bms | 常量 | `libs/bms_core/src/bms_core/msgclient/constants.py` | `MSG_PROVIDER_NAME` 上移为三键共用单一事实源 |
| bms | 实现 | `libs/bms_core/src/bms_core/outbox/dispatcher.py` | `MsgOutboxDispatcher`（沿 `PollOutboxDispatcher` 派生）+ `outbox_dispatcher_class()` 单一判据 |
| bms | 装配 | `libs/bms_core/src/bms_core/core/assembly.py` | `_resolve_publisher()` / `_dispatcher_type` 钩子、`MsgOutboxDispatcherFactory`、注册 `msg` |
| bms | CLI | `ops/outbox.py` | `_build_dispatcher()` 按 `[outbox].provider` 解析，不再硬编码 `poll` |
| bms | 配置 | `config.toml` | `[outbox]` 注释补 `msg` 取值；缺省仍为 `poll` |
| bms | 用例 | `libs/bms_core/tests/outbox/test_msg_outbox_switch.py` | T1~T9，10 例，Kiwi 2481 |
| msg | 用例 | `services/msg/tests/client/test_outbox_relay_switch.py` | U1~U4，4 例，真应用 / 真数据面 / 真消费端 |
| msg | 文档 | 本记录 + [测试记录](../测试/01_测试记录_01-8-1_outbox适配与relay切换.md) | 证据链 |

**零变更**：`BaseOutboxStore` / `BaseOutboxDispatcher` 端口契约与版本、`SqlOutboxStore` 与 `sys_outbox` 表结构、`PollOutboxDispatcher` 既有行为、`MsgEventPublisher` 与 `bms_core.msgclient` 全链、`[event].provider` / `[service_client].provider` 语义、07-4 的矩阵 / 清单 / 场景单源、契约快照、错误码表、RocketMQ / outbox / HTTP 原链路、msg 仓服务端业务代码、事务消息与 CDC 实现。

## 3. 关键实现决策 <a id="decisions"></a>

| # | 决策 | 理由 |
| --- | --- | --- |
| 1 | 形态 A：bms relay 的 msg 版本，connector 只留口子 | 硬约束「bms 仓为主」+ 知识档案 14 §8 的 first step + 规划 §11「不做通用 CDC 平台」 |
| 2 | `MsgOutboxDispatcher` 只覆写 `plugin_name`，不重写投递逻辑 | 与 `poll` 版差异仅最后一跳的发布实现；复制重试 / 游标 / 死信语义会产生两套判定 |
| 3 | 复用既有 `[outbox].provider`，不新增配置节 | 基座 `Settings` 为 `extra="forbid"`；与另两个 provider 键同口径，回滚是纯配置切换 |
| 4 | 非法值装配期拒启，不静默回落 | 沿用 07-4-1 §6.2 / 07-4-2 输入基线定稿；静默回落会造成「以为切到 msg」的假切换 |
| 5 | `[outbox].provider` 工厂固定解析 `msg` 发布实现，不读 `[event].provider` | 避免运维漏改一键造成假切换；`[event].provider` 仍管直连发布端口，职责不重叠 |
| 6 | `[outbox]` 空串沿用 bms 既有 `null` 占位（不投递） | 与 07-4-1 的「空串 = 原实现」有差异，如实登记而非套用（设计 §6.2） |
| 7 | CLI 必须同步按 provider 解析 | 原 `_build_dispatcher()` 硬编码 `PollOutboxDispatcher`，不同步则切换 / 回滚演练不闭环 |
| 8 | CLI 的 `msg` 分支直接构造 `MsgEventPublisher` | CLI 无 FastAPI 应用上下文，无法复用需 `app` 的工厂；配置校验仍走 `msg_base_url()` / `msg_namespace()` 同一入口，`ConfigError` 同源 |
| 9 | 两仓用例分工：bms 真发件箱表、msg 真数据面 | bms 仓 CI 不 clone msg；只有 msg 仓能同时 import 两侧，避免复制第二套协议实现 |
| 10 | 集合声明一律 `ConcurrentStable*` | 项目硬约束；护栏由 T9 / U4 契约断言与静态检查共同把关 |

## 4. 实施过程 <a id="process"></a>

1. 按《AI开发规范》§2 顺序读齐规范 / 规划 / 计划 / 需求 / 任务，补读 07-4-1 / 07-4-2 详细设计与 bms outbox 源码，核对出「relay 已把最后一跳交给 `EventPublisher` 端口」这一现状。
2. 四项待拍板事项逐项确认（形态 A、键位复用、非法拒启、跨仓范围），产出[详细设计](../设计/01_详细设计_01-8-1_outbox适配与relay切换.md)并定稿；设计先于编码。
3. 提交前在 Kiwi 登记用例 **2481** 并回读核对（Category 平台骨架 / P2 / CONFIRMED / Tag 自动化），先登记后编码。
4. bms 仓落实现与 T1~T9；本地单文件跑 ruff / format / pyright（工程 `--pythonpath` 口径）、单节点定向 pytest。
5. msg 仓落 U1~U4 跨仓联调用例，复用 `tests_support/bms_port_contract.py` 构造真应用。
6. 两仓分别提交（只 commit，不推送、不触发 CI）。

**关键命令**（解释器统一 `D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe`）：

```bash
# bms 仓
D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m pytest libs/bms_core/tests/outbox/test_msg_outbox_switch.py -q
D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m pyright --pythonpath <同一解释器> <改动文件>
# msg 仓
D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m pytest services/msg/tests/client/test_outbox_relay_switch.py -q
D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m engkit links
D:/Develop/bizs/bms/backend/.venv/Scripts/python.exe -m engkit docs-scope
```

## 5. 问题与处置 <a id="issues"></a>

| # | 问题 | 处置 |
| --- | --- | --- |
| 1 | 文档提交被 `engkit docs-scope` 拦下（设计文档出现进度类字段字样） | 按「进度类字段唯一落点」口径改写 3 行措辞，不绕过门禁；复跑 0 违规 |
| 2 | bms 仓提交被「工作区卫生」步骤拦下 | 追根因：`msg/tmp/` 的历史日志由 `*_REPORT_DIR` 默认落点（仓内 `tmp/`）与手工重定向生成，而规范只放行工作区根 `tmp/`；检查器 `REPO_NAMES` 漏 `msg` 导致其被当普通子目录扫。按用户拍板方案 1，把 5 个默认落点与 recovery 工作根改到工作区根 `tmp/msg/`，同步 README 与设计文档（msg 仓提交 `a5ee581`） |
| 3 | 单文件直接调 `pyright.exe` 报数百条 `unknown` 基线错误 | 工程口径是 `python -m pyright --pythonpath <解释器>`；按工程口径复跑 0 errors，属调用方式问题而非代码缺陷 |
| 4 | U3 初版「先发布失败再补发」触发 `InvalidRequestError: A transaction is already begun` | `msg_app` 的内存库 session 是单例，404 路径未提交事务；改为「同一事件重投两次，幂等键稳定」，失败后补发的语义由 bms 仓真 SQLite 用例承担 |
| 5 | 01-8-1 §2-3、07-4 父任务 §3-2 的「非法值回原链路」与 07-4 定稿冲突 | 按拍板沿用「非法拒启」，两处字面已回写；差异登记在设计 §6.2 / §15 |

## 6. 验证结论 <a id="verification"></a>

| 项 | 结果 |
| --- | --- |
| bms 新用例 | 10 passed |
| bms 回归（outbox / CLI / 装配 / msgclient adapter） | 68 passed |
| msg 跨仓联调用例 | 4 passed |
| msg deploy 目录回归 | 464 passed |
| bms 提交前预检 | 30 项全部通过（57.9s） |
| msg 文档 / 落点修复提交前预检 | 2154 passed、改动行覆盖率 100%、30 项通过 |
| 文档三项校验 | links 断链 0 / 失效锚点 0；docs-scope 违规 0；改动文件全 LF |
| 覆盖率 | 按 bms《AI开发规范》「覆盖率要求」节判定，本地提交环判改动行，推送环与 CI 判模块级 |

## 7. 偏差与遗留 <a id="leftovers"></a>

| # | 事项 | 归口 |
| --- | --- | --- |
| 1 | 8B_05 复制能力未交付，生产迁移仍被闸门阻断 | 02-4 / 02-5 / 02-8、07-8 |
| 2 | 真实 RocketMQ 集群 / 真实容器 / 真盘联调未执行 | 07-8（证据为进程内真应用 + 真 SQLite 发件箱） |
| 3 | `config_fingerprint` 三键扩展需 07-4-1 §6.1 / §6.3 协同回写 | 本任务消费方回写（随本记录提交） |
| 4 | 域级键与白名单 / 灰度未落地，只能整端口切换 | bms 8B_08 |
| 5 | `claim_pending` 多实例重复抢批（无 `SKIP LOCKED` / 租约列） | 02-8 / 07-5（知识档案 14 §4） |
| 6 | 事务消息 / CDC 端口、事件类型与失败分支占位 | 01-8-2（首期不实现） |
| 7 | 审计事件全链路不丢、效果一次的端到端验收 | 01-8-3（复用本任务 T4~T7 / U1~U3 证据） |
| 8 | 计划表 §3 的 01-8 行与 §5 门禁 9 现状 | 待 01-8-2 / 01-8-3 完成后统一回写（父任务未整体收口） |
| 9 | 帧入口 `/msg/publish` 未走 | 后续性能治理 / 8B_12 |
