# 04-2-1 handler 超时分类 · 测试记录

> 消息平台 · 04 远程调用 · 04-2 超时传播与取消 · 子任务 01 · 测试记录

[文档首页](../../../../../../../文档首页.md) › 消息平台任务 › [04 远程调用](../../../04_远程调用.md) › [04-2 超时传播与取消](../../04_远程调用_02_超时传播与取消.md) › [04-2-1 handler 超时分类](../04_远程调用_02_超时传播与取消_01_handler超时分类.md) › 测试记录

## 1. 用例登记 <a id="kiwi"></a>

| 项 | 值 |
| --- | --- |
| 平台 | Kiwi TCMS（容器 `bms-kiwi`，Django shell 批量登记） |
| 编号 | **2499**（登记后回读核对：2499 / 2498 / 2497） |
| 分类 / 优先级 / 状态 / 标签 | 平台骨架 / P2 / CONFIRMED / 自动化 |
| 标注 | `backend/services/msg/tests/api/test_rpc_handler_timeout_0421.py` 模块级 `pytestmark = pytest.mark.kiwi_id(2499)` |

## 2. 用例清单 <a id="cases"></a>

| # | 用例 | 断言 |
| --- | --- | --- |
| 1 | `test_invoke_rpc_converts_handler_timeout` | `invoke_rpc` 抛 `MsgRpcHandlerTimeoutError`；`code=350034`、`http_status=504`、`__cause__` 为 `TimeoutError`；`data` 四字段 |
| 2 | `test_http_form_returns_350034` | `POST /rpc/budget/boom` → 504、`code=350034`、`data` 四字段 |
| 3 | `test_frame_form_returns_350034_error_frame` | 帧请求 → error 帧 `code=350034`、`detail` 四字段、`layer=business`、`retryable=false` |
| 4 | `test_budget_exhaustion_stays_350004` | 预算到点仍 `MsgRpcDeadlineExceededError`（`350004`），两码互斥 |

## 3. 回归面 <a id="regression"></a>

| 文件 | 结果 |
| --- | --- |
| `services/msg/tests/api/test_rpc_deadline.py`（含升级后的 `test_handler_own_timeout_is_not_misreported`） | 15 passed |
| `services/msg/tests/api/test_frame_stream.py`（错误分类闭集） | 17 passed |
| `services/msg/tests/metrics/test_collector.py` | 14 passed |
| `services/msg/tests/test_error_codes.py`（registry ↔ 枚举对账） | 6 passed |

## 4. 验收映射 <a id="accept"></a>

| 完成标准 | 证据 |
| --- | --- |
| handler 自抛超时不再 500 / `10000` | 用例 2 / 3 |
| 与 `350004` 在码、指标、`data.source` 三处可区分 | 用例 1（`source`）、用例 4（互斥）、实施记录 §2 第 3 条（`outcome=handler_timeout`） |
| 契约 / 登记 / 前端文案 / 分类集合四处一致 | `contract_snapshot check` 零漂移；`test_error_codes.py` 6 例；`error.350034` 已入文案包 |
| 变更模块行 + 分支 100% | 推送环复判 |

## 5. 结论 <a id="conclusion"></a>

定向 4 例与回归 52 例全绿；契约零漂移；覆盖率交推送环复判。
