"""msg 后端运维脚本包（`python -m ops.<脚本>`，与 bms / mdm 同构）。

当前提供：

- `contract_snapshot`：公开契约（OpenAPI）快照的导出与零漂移校验；
- `rebalance` / `failover` / `cluster_runtime`：分区分配迁移与切主的运维命令（02-8）。

`check_modules` / `event_contracts` / 建库与迁移脚本按后续任务补（msg 无事件消费声明，事件契约脚本暂缺）。
"""
