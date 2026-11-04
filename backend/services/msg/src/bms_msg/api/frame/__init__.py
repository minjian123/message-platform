"""msg 根路径数据面入口包（`/msg/*`，HTTP/2 帧入口；任务 03-1）。

与 `api/data/` 的区别只在**挂载位置**：`api/data/` 的端点是 `/api/v1/msg/*` 下的既有 HTTP/1.1 数据面
（保留作基准对照与回退路径），本包把它们原样挂到根路径 `/msg/*`，并追加两个只属于帧入口的能力
（SSE 长驻流 `subscribe`、连接元数据 `connection`）。

两者共用 `api/data/messages.py` 的 `DATA_PLANE_ROUTES` 规格——路径 / 方法 / 权限 / 错误码只有一份，
03-2 引入二进制帧时只替换 body 编解码，不动路由。
"""
