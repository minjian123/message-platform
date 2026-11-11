"""六类故障演练（任务 07-6-1）：宿主侧离线命令，与 `ops.backup` / `ops.recovery` 同形态。

分层：`env`（键位与场景单源）、`runtime`（隔离实例与 HTTP 观测）、`context`（场景上下文）、
`scenarios.*`（六类注入与判据）、`engine`（注册与编排）、`record`（JSON / Markdown 同源记录）、
`__main__`（命令行）。入口：`python -m ops.drill <plan|run|cleanup>`。
"""
