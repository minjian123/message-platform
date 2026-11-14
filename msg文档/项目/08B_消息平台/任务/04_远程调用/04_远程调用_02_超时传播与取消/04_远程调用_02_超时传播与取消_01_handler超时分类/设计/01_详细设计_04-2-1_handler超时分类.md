# handler 超时分类详细设计

> `350034 RPC_HANDLER_TIMEOUT`（504 / business / 不重试 / `data.outcome=unknown` + `source=handler`）× service 层就地转换（HTTP 与帧同源）× 指标 `msg_rpc{outcome}` 五态 × 契约 / 登记 / 前端文案三处齐备

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../../计划/01_计划_消息平台.md) › [04-2 超时传播与取消](../../04_远程调用_02_超时传播与取消.md) › [04-2-1 handler 超时分类](../04_远程调用_02_超时传播与取消_01_handler超时分类.md) › 详细设计

---

## 1. 概述 <a id="overview"></a>

### 1.1 目标

让「handler 自己抛出的 `TimeoutError`（通常是调下游超时）」成为**一等错误语义**：调用方能拿到确定的码、明确的「结果未知」标记与「这是 handler 侧超时」的来源，而不是基座兜底的 500 / `10000`。

### 1.2 现状核对

| # | 现状 | 处置 |
| --- | --- | --- |
| 1 | `rpc/service.py::_execute_with_deadline` 用 `Timeout.expired()` 区分「预算到点」与「handler 自抛」，自抛时 `raise` 原异常 | 保留判据，把「自抛」分支改为转 `MsgRpcHandlerTimeoutError`（`from exc` 保留链） |
| 2 | `api/rpc/routes.py` 只捕获 `BizError`；`TimeoutError` 不在其列 → 基座兜底 500 / `10000` | 转换落在 service 层后，端点层零改动 |
| 3 | `api/frame/responses.py` 的 `_BUSINESS_CODES` 不含新码 → 帧形态会被判 `server_bug` | 加入 `_BUSINESS_CODES` |
| 4 | `RPC_ERROR_CODES`（端点 `x-error-codes`）不含新码 | 追加 `350034` |
| 5 | 指标 `_rpc_outcome()` 只有四态（`ok` / `deadline_exceeded` / `rate_limited` / `method_not_found` / `business_error`） | 增加第五态 `handler_timeout` |
| 6 | 产品错误码 33 个（`350001`~`350033`），`350034` 未占用 | 新增登记 |

## 2. 决策（已逐项确认 2026-11-14） <a id="decisions"></a>

| # | 决策 | 结论 | 依据 |
| --- | --- | --- | --- |
| 1 | 对外错误码 | **新增 `350034 RPC_HANDLER_TIMEOUT`**，与 `350004` 语义分离 | 排障与指标能区分「本跳没时间」与「下游超时」，日后拆分不返工 |
| 2 | 客户端分类 | **business / 不重试**（入 `_BUSINESS_CODES`，不入 `_RETRYABLE_SYSTEM`） | 超时 = 结果未知，是否重试由幂等键决定（与 `350004` 同口径） |
| 3 | 补齐范围 | **同轮全补齐**：枚举 + registry + 契约重导 + 前端文案 + `x-error-codes` | 一次到位，不留半成品 |
| 4 | 指标标签 | **新增 `handler_timeout`**（`msg_rpc{outcome}` 五态） | 超时类问题在指标上可分辨 |
| 5 | 异常转换落点 | **service 层就地转换** | HTTP 与帧两形态同源，避免装配路径分叉（CI 已因跨路径分叉踩过坑） |
| 6 | `data` 字段 | **与 `350004` 同构 + `source=handler`** | 客户端复用「结果未知」处理，现场多一层归因 |
| 7 | 用例落点 | **扩 `tests/api/test_rpc_deadline.py`**（已含「自抛不被误报」用例） | 同主题同文件，不新增文件 |

## 3. 接口与字段 <a id="contract"></a>

### 3.1 错误码 350034

| 项 | 值 |
| --- | --- |
| 枚举 | `MsgErrorCode.RPC_HANDLER_TIMEOUT = 350034` |
| 异常类 | `MsgRpcHandlerTimeoutError(MsgError)` |
| HTTP 状态 | `504` |
| 层 / 可重试 | `business` / `retryable=false` |
| `message_key` | `msg.error.rpc_handler_timeout` |
| 触发点 | handler 内抛 `TimeoutError` 且**预算未到点**（`Timeout.expired()` 为假） |
| 与相邻码分工 | `350004` = 本跳预算耗尽（没时间）；`350024` = 流空闲（半天没吐数据）；`350023` = 方法未注册；本码 = **handled 了但等下游超时** |

### 3.2 `data` 字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `outcome` | `"unknown"` | 结果未知（可能已生效，不得直接重试） |
| `source` | `"handler"` | 标明是 handler 自抛，而非本跳预算耗尽 |
| `used_ms` | int | 本次调用已消耗（`Deadline.spent_ms()`） |
| `remaining_ms` | int | 剩余预算（`Deadline.remaining_ms()`；自抛时通常仍 > 0） |

> JSON 形态进响应 `data`，帧形态进 error 帧 `detail`（与 `350004` 同一出口）。

### 3.3 转换点

```text
rpc/service.py::_execute_with_deadline
  except TimeoutError as exc:
      if timeout.expired():            # 预算到点
          await _cancel_and_wait(task)
          raise exhausted_error(deadline) from exc      # 350004（不变）
      await _cancel_and_wait(task)     # handler 自抛：同样先收尾
      raise MsgRpcHandlerTimeoutError(...) from exc     # 350034（新增）
```

- 自抛分支**同样要 `await` 任务收尾**：handler 已抛，任务可能仍在清理，不能只转换异常就走；
- `MsgRpcHandlerTimeoutError` 携带 §3.2 的四个字段（service 层能拿到 `Deadline`）。

## 4. 失败分支与边界 <a id="failures"></a>

| # | 场景 | 行为 |
| --- | --- | --- |
| 1 | handler 抛 `TimeoutError`，预算同时到点 | 判据用 `timeout.expired()`：为真即按 `350004`，不产生 `350034`（两码互斥） |
| 2 | handler 抛 `TimeoutError` 且客户端已断开 | 取消优先：`CancelledError` 路径先命中，不写响应 |
| 3 | handler 抛 `asyncio.TimeoutError`（3.11 起即 `TimeoutError` 别名） | 同一分支，行为一致 |
| 4 | 帧形态 | 同异常 → 闸门 `_BUSINESS_CODES` → error 帧，`detail` 带四字段 |
| 5 | 流式调用（`/stream`） | 不在本任务范围：流内预算与空闲超时归 04-3 |

## 5. 指标与观测 <a id="metrics"></a>

| 项 | 变化 |
| --- | --- |
| `msg_rpc{outcome}` | 新增 `handler_timeout`；其余四态不变 |
| 日志 | `_log_outcome("handler_timeout", ..., code=350034)`，与 `deadline_exceeded` 并列 |
| 文档 | 06-3 指标清单与 `protocol/contract.py` 的 outcome 闭集同步 |

## 6. 契约与登记 <a id="registry"></a>

| 落点 | 动作 |
| --- | --- |
| `errors.py` | 枚举 + 异常类（`data` 四字段）+ 文件头码清单追加 `350034` |
| `deploy/registry/msg.toml` | 追加 `[[errcodes]]`（`code/domain/http_status/message_key/severity/description`） |
| `deploy/contracts/msg.json` | `python -m ops.contract_snapshot export` 重导 |
| `api/rpc/routes.py` | `RPC_ERROR_CODES` 追加 `350034` |
| `api/frame/responses.py` | `_BUSINESS_CODES` 追加 `350034` |
| `frontend/modules/msg/src/i18n/messages.ts` | 新增 `error.350034`（中 / 英） |
| `protocol/contract.py` / 06-3 指标清单 | outcome 闭集加 `handler_timeout` |

## 7. 测试设计与验收映射 <a id="tests"></a>

| # | 用例（`services/msg/tests/api/test_rpc_deadline.py`） | 断言 |
| --- | --- | --- |
| 1 | 升级 `test_handler_own_timeout_is_not_misreported` | `invoke_rpc` 抛 `MsgRpcHandlerTimeoutError`；`code == 350034`；`data` 四字段齐全且 `source == "handler"`、`outcome == "unknown"` |
| 2 | 新增端点用例 | `POST /rpc/budget/boom` → 504、`code == 350034`、`data.outcome == "unknown"`、`data.source == "handler"` |
| 3 | 新增帧形态用例 | 帧请求 → error 帧 `code == 350034`，`detail` 带四字段 |
| 4 | 回归：预算到点仍 `350004` | 既有 `test_exhausted_*` 全绿，且不出现 `350034` |
| 5 | 回归：handler 抛其他异常仍走兜底 | 非 `TimeoutError` 不转换（保持原行为） |

**覆盖率**：变更模块 `bms_msg.rpc.service` / `bms_msg.errors` / `bms_msg.api.rpc.routes` / `bms_msg.api.frame.responses` 行 + 分支 100%。

## 8. 登记回写 <a id="writeback"></a>

- 计划表：04-2 行状态「已完成」→「部分完成」并补记本子任务，子任务完工后回迁 §2 并在行内补交付说明（数值只在计划表维护）；
- 任务文档：04-2 子任务表追加 04-2-1；
- 台账：计划 §6.1 新增一行（B6 后半）并在交付后闭环；
- 06-3 指标清单：outcome 五态；
- 前端基类 / 契约文档：无需改动（无继承链变化）。

## 9. 边界与开放项 <a id="open"></a>

| # | 项 | 归口 |
| --- | --- | --- |
| 1 | bms `msgclient` 是否把 `350034` 映射为 `RpcCallTimeoutError` | 04-5 适配面 |
| 2 | 流式调用的 handler 自抛超时 | 04-3 边界（流内已定 `350024` / `350004`） |
| 3 | 多跳链路上「哪一跳超时」的聚合视图 | 平台级可观测（06-3 已交底座） |

## 10. 对齐记录 <a id="alignment"></a>

| 日期 | 变更 |
| --- | --- |
| 2026-11-14 | 立项与定稿：新增 `350034`，business / 不重试；service 层转换；`data` 同构 + `source`；指标五态；用例扩 `test_rpc_deadline.py` |
