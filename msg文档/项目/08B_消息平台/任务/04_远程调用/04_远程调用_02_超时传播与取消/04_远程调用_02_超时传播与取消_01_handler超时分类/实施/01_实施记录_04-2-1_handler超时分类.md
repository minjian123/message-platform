# 04-2-1 handler 超时分类 · 实施记录

> 消息平台 · 04 远程调用 · 04-2 超时传播与取消 · 子任务 01 · 实施记录

[文档首页](../../../../../../../文档首页.md) › 消息平台任务 › [04 远程调用](../../../04_远程调用.md) › [04-2 超时传播与取消](../../04_远程调用_02_超时传播与取消.md) › [04-2-1 handler 超时分类](../04_远程调用_02_超时传播与取消_01_handler超时分类.md) › 实施记录

## 1. 基本信息 <a id="info"></a>

| 项 | 值 |
| --- | --- |
| 任务 | 04-2-1 handler 超时分类（04-2 嵌套子任务） |
| 实施日期 | 2026-11-14 |
| Kiwi 用例 | 2499 |
| 依据 | [详细设计](../设计/01_详细设计_04-2-1_handler超时分类.md)；质量盘点 B6 |

## 2. 实施过程 <a id="process"></a>

1. **错误码**：`errors.py` 新增枚举 `RPC_HANDLER_TIMEOUT = 350034` 与异常类 `MsgRpcHandlerTimeoutError`（`data` 四字段：`outcome=unknown` / `source=handler` / `used_ms` / `remaining_ms`）。
2. **转换点**：`rpc/service.py::_execute_with_deadline` 的 `except TimeoutError` 分支——`Timeout.expired()` 为真仍抛 `350004`；为假（handler 自抛）先 `_cancel_and_wait(task)` 收尾，再抛 `handler_timeout_error(deadline)`（`from exc` 保链）。新增构造函数 `handler_timeout_error()` 与 `__all__` 导出。
3. **指标**：`_invoke_once` 的异常链新增 `except MsgRpcHandlerTimeoutError` 分支，`_log_outcome("handler_timeout", ...)`；`api/rpc/routes.py::_rpc_outcome` 同一码映射到 `handler_timeout` 标签；两处 docstring 的 outcome 闭集同步五态。
4. **契约与登记**：`deploy/registry/msg.toml` 追加 `[[errcodes]] code = 350034`；`api/rpc/routes.py::RPC_ERROR_CODES`、`api/frame/responses.py::_BUSINESS_CODES` 各追加；`rpc/contract.py` 一元 `errors` 段追加 `handler_timeout`；`deploy/contracts/msg.json` 重导（`python -m ops.contract_snapshot export`）。
5. **前端文案**：`frontend/modules/msg/src/i18n/messages.ts` 新增 `error.350034`（中 / 英）。
6. **用例**：新建 `backend/services/msg/tests/api/test_rpc_handler_timeout_0421.py`（Kiwi 2499，4 例）；`test_rpc_deadline.py::test_handler_own_timeout_is_not_misreported` 断言升级为 `MsgRpcHandlerTimeoutError` + `code=350034`。

## 3. 问题与处置 <a id="issues"></a>

| # | 问题 | 处置 |
| --- | --- | --- |
| 1 | 详细设计原定「扩 `test_rpc_deadline.py`」，但该文件带模块级 `pytestmark = kiwi_id(2434)`，新用例会被双重标注 | 改为**独立用例文件**（`test_rpc_handler_timeout_0421.py`，`kiwi_id(2499)`）；设计 §2 决策 7 / §7 已回写，符合「一条用例一个编号」 |
| 2 | 首次断言把 `ConcurrentStableDict` 当作 `dict` 判断，帧形态用例失败 | 辅助函数改为按 `object` 收参 + `isinstance(detail, dict)` 后按键取用（与 `test_rpc_deadline.py` 既有写法一致），pyright strict 亦通过 |
| 3 | `_error_frame` 返回类型若标注为 `ConcurrentStableDict`，pyright 报 `reportUnknownMemberType` | 改为返回 `tuple[int, object]`，由断言函数收窄 |

## 4. 验证 <a id="verify"></a>

| 项 | 命令 | 结果 |
| --- | --- | --- |
| 静态 | `ruff check` / `ruff format --check`（8 个改动文件） | 通过 / 已格式化 |
| 类型 | `uv run pyright`（8 个改动文件） | 0 error |
| 定向用例 | `pytest services/msg/tests/api/test_rpc_handler_timeout_0421.py` | 4 passed |
| 回归 | `pytest .../test_rpc_deadline.py`、`.../test_frame_stream.py`、`.../metrics/test_collector.py`、`.../test_error_codes.py` | 19 passed / 17 passed / 14 passed / 6 passed |
| 契约 | `python -m ops.contract_snapshot check` | 零漂移 |
| 前端类型 | `node packages/api-types/scripts/generate.mjs` | 生成物无变化（新增码不进 TS 面） |

## 5. 覆盖率 <a id="coverage"></a>

变更模块：`bms_msg.rpc.service`、`bms_msg.errors`、`bms_msg.api.rpc.routes`、`bms_msg.api.frame.responses`、`bms_msg.rpc.contract`。行 + 分支 100% 由推送环复判（提交环判改动行）。

## 6. 偏差与遗留 <a id="leftovers"></a>

| # | 项 | 归口 |
| --- | --- | --- |
| 1 | 用例从「扩现有文件」改为「独立文件」（Kiwi 双重标注约束） | 已回写设计 §2 / §7 |
| 2 | bms `msgclient` 是否把 `350034` 映射为 `RpcCallTimeoutError` | 04-5 适配面（本任务只保证服务端码与 `layer` / `retryable` 正确） |
| 3 | 流式调用中的 handler 自抛超时 | 04-3 边界 |
