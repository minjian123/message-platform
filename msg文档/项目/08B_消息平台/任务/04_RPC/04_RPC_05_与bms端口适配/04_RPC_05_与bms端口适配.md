# 04-5 与 bms 端口适配

## 1. 任务信息 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 编号 | 04-5 |
| 父任务 | [04 RPC](../04_RPC.md) |
| 对应需求 | [04-5 与 bms 端口适配](../../../需求/04_需求_RPC.md#r04-5) |
| 依赖 | 04-1（RPC 端点 / stub）、04-4（错误语义与重试）；bms `EventPublisher` / `BaseServiceClient` / `ServiceCallPolicy` / 服务目录已交付；**bms 适配任务（跨仓）已立项：bms 07B `06_01`「协议链上移与消息平台端口适配」** |
| 负责人 | minjian |

## 2. 任务内容 <a id="content"></a>

1. **分工口径（已拍板）**：**msg 仓**交付 RPC 协议库 / stub（04-1~04-4 的客户端能力）与事件发布协议客户端；**bms 仓另立任务**交付 `EventPublisher` / `BaseServiceClient` 的 msg 实现、插件装配、配置切换与实例选择。本任务负责 msg 侧协议库、契约测试与两仓联调；bms 侧任务完成装配与切换，不把 bms 插件代码塞进 msg 仓。
   - **依赖方向补注（bms 07B `06_01` 立项时定稿）**：msg 仓以路径依赖 `bms-core`，若 bms 适配层直接 `import bms_msg.client` 会形成仓级循环依赖并违反 07B 架构 38 §8 红线。故「协议链」整条上移到 `bms_core/msgclient`（帧编解码 / 客户端 / 五类分类 / 退避与预算 / 幂等 / 熔断），`bms_core` 不引用任何 `bms_msg` 符号；**msg 仓删除本地客户端副本并统一 `from bms_core.msgclient import …`**，只保留 broker 服务端业务与契约快照导出。这样各产品仓经路径依赖**天生附带消息客户端**，无需额外部署或跨仓装配，且协议实现唯一。`provider` 切换、`category` 映射与回滚三态由 bms 侧任务承担；本任务的两仓契约测试覆盖上移前后行为等价。
2. **EventPublisher 映射**：`publish(event)` → `/msg/publish`；`event_type` → topic / namespace / module 的映射规则在详细设计定稿（首期同域映射）；`payload` → 消息 payload；`event_id` → 幂等键 / `message_id` 的映射；`aggregate_key` → 分区键；`trace_id` / `tenant_id` / `occurred_at` / `event_version` 透传；`publish_transactional()` 首期委托 `publish()`（事务消息归 8B_04）。
3. **BaseServiceClient 映射**：`call(ServiceRequest)` → `/rpc/{service}/{method}`；`service` 取 `request.service`；`method` 由 `request.path` 归一化（如 `/api/v1/foo/bar` → `foo.bar`），保留 bms `/api/v1` 公开契约校验；`json_body` / `content` → RPC payload；`ServiceCallPolicy.timeout` → `deadline_ms`；`max_retries` / `retry_backoff` / `circuit_breaker` / `rate_limit` → 04-4 的韧性能力；`ServiceResponse` ← RPC 响应；非 2xx / 错误码映射到 bms 异常。
4. **配置切换**：bms `[event].provider` / `[service_client].provider` 取 `msg` / 原实现；**业务代码不改**；切回原实现行为恢复；切换与回滚需明确在途请求、未确认事件、双写窗口的处置，详细设计定稿并演练。
5. **trace / 租户 / 身份**：透传 `trace_id` / `tenant_id`；服务 JWT 由 bms 客户端按现有开关附加；不新增认证协议，不绕过 bms 租户解析；错误与超时映射保留原始码与 `retryable`。
6. **契约测试**：msg 侧 wire format 契约；bms 侧 adapter 映射；跨仓共享 fixture（请求 / 响应 / 错误 / timeout / cancel / 幂等键）；真实 msg 服务联调；业务代码零改动的切换用例。
7. **退出条件**：**替换一个真实 `HttpServiceClient` 调用**（8B_03 退出条件）；切回原实现后行为恢复；事件发布链路可切换；错误码与超时映射正确。
8. **边界**：不做 bms 业务迁移（8B_08）、不做事务消息（8B_04）、不做 outbox relay 替换（8B_04）、不做多实例选主（04-6 / 8B_05）、不改 bms 端口契约。
9. **风险**：事件切换的双写 / 回滚窗口、topic 映射、错误码映射、跨仓版本错配；处置登记在阶段计划 §7。

## 3. 完成标准 <a id="accept"></a>

- 业务代码不改即可在 msg / 原实现之间切换；切回原实现后行为恢复。
- 错误码、超时、trace、租户映射正确；非幂等操作按 04-4 的幂等键与重试规则处理。
- 一个真实 `HttpServiceClient` 调用被 RPC 替换（8B_03 退出条件），事件发布链路可切换。
- msg 侧变更模块行 + 分支覆盖率 100%，静态检查全绿；bms 侧适配任务按其门禁验收，两仓契约测试全绿。

## 4. 参考文档 <a id="ref"></a>

- [需求 04-5 与 bms 端口适配](../../../需求/04_需求_RPC.md#r04-5)
- [架构设计 · 接入面：协议与客户端](../../../../../设计/架构设计/05_架构设计_接入面_协议与客户端.md#adapter) 第 8.1 节、第 9 节
- [消息平台规划](../../../../../规划/01_规划_消息平台.md) 第 8 节
- bms《[RPC 模型](../../../../../../bms文档/资料/知识档案/消息与事件驱动/09_事件驱动_RPC模型.md)》第 11 节、第 14 节
- bms《[后端开发规范](../../../../../../bms文档/规范/后端开发规范.md)》「服务间调用必须走基座」节
- [04-1 RPC 一元调用](../04_RPC_01_一元调用/04_RPC_01_一元调用.md)、[04-4 错误语义与幂等重试](../04_RPC_04_错误语义与幂等重试/04_RPC_04_错误语义与幂等重试.md)

## 5. 前置契约（已交付 / 待交付） <a id="prereq"></a>

| 前置 | 状态 | 契约 | 对本任务的约束 |
| --- | --- | --- | --- |
| [04-1 RPC 一元调用](../04_RPC_01_一元调用/04_RPC_01_一元调用.md) | 待交付（本域前一任务） | `/rpc/{service}/{method}`、最小 stub、错误骨架 | 适配器只映射到该端点，不另开 RPC 路径 |
| [04-4 错误语义与幂等重试](../04_RPC_04_错误语义与幂等重试/04_RPC_04_错误语义与幂等重试.md) | **已交付（2026-11-06，Kiwi 2439）** | `ErrorCategory` 五类 + `classify()` 三元组（唯一出处）、`RetryPolicy`（2 次 / 100ms / ×2 / 封顶 2s + full jitter）、`RetryBudget`（帧头 `retry_budget` + `ContextVar`）、幂等键 `idempotency_key` 与进程内去重、复用 bms `BaseCircuitBreaker` 契约的三态实现、`Retry-After` 双来源；错误帧增 `category`；帧契约重导 | bms `ServiceCallPolicy` 的韧性字段映射到同一套语义，不重复实现；适配层可整体注入 `RetryPolicy` / 熔断器，并决定生产是否默认启用三态（缺省为 `NullCircuitBreaker` 恒闭合）；`RpcCallError` 已带 `category` / `attempts` / `backoff_ms` 供错误码映射消费 |
| bms `EventPublisher` / `BaseServiceClient` / `ServiceCallPolicy` | 已交付 | `publish` / `publish_transactional` / `call(ServiceRequest)`；超时 / 重试 / 熔断 / 限流 | 不改端口契约；业务代码只依赖端口 |
| bms 服务目录 / 配置 | 已交付 | 服务登记、`/api/v1` 公开契约、`service_endpoints` | `BaseServiceClient` 适配沿用 bms 服务目录；04-6 提供实例选择抽象 |
| bms 适配任务（跨仓） | **步 1 ~ 步 3 均已交付（2026-11-06，bms 07B `06_01` 收口）** | 协议链上移 `bms_core/msgclient`（消除仓级循环依赖、各产品仓天生附带客户端）、msg 实现与插件装配、`provider = "msg"` 全局切换、按 `category` 的错误映射、回滚三态演练、真实调用点（`fetch_catalog_snapshot`）替换；Kiwi 2440 / 2441 / 2442 | 「业务代码不改切换」与「替换真实调用」需两仓联合验收——已按进程内真 msg 应用（真路由 → 真帧）+ 真实业务函数达成，生产集群联调归部署阶段；msg 侧契约用例见[跨仓契约测试实施记录](实施/02_实施记录_04_RPC_05_跨仓契约测试.md)。任务文档路径（跨仓纯文本）：`bms/bms文档/项目/07B_跨产品协同/任务/06_消息平台端口适配/06_消息平台端口适配_01_消息平台端口适配/06_消息平台端口适配_01_消息平台端口适配.md` |
