# 真容器复跑记录 · 05-3-1 ACL 守卫会话隔离

> 隔离演练集群（三实例 + MySQL）换新镜像后逐项取证；不含凭据与内网地址。

## 1. 装置

| 项 | 值 |
| --- | --- |
| 镜像 | `drill/msg:drill-1347595`（由 `1347595` 源码构建，镜像 ID `sha256:ac5b16f4…`） |
| 集群 | 隔离项目 `msg-drill`，三实例 `msg-1` / `msg-2` / `msg-3`（复制开启、自动选举开启、`min.insync=2`） |
| 数据 | 沿用上轮演练库与宿主卷（库内已有 7 条 `drill.replica.probe.*` 历史主题） |
| 客户端 | 宿主侧 `curl`，经实例映射端口；租户头 `X-Tenant-ID: demo` |

## 2. 步骤与结果

| # | 动作 | 关键输出 | 结论 |
| --- | --- | --- | --- |
| 1 | 三实例健康检查 | `/healthz` 200、`/readyz` 200 | 集群可用 |
| 2 | `GET /api/v1/msg/admin/topics?page=1&size=5` | 200；`total=7`，首条 `drill.replica.probe.replica_crash.*` | **本次报告场景恢复**（修复前有数据即 500） |
| 3 | `POST /api/v1/msg/admin/topics`（`probe.acl.session`，2 分区） | 200；返回雪花 id / `status=active` | 写路径正常 |
| 4 | 再查 `GET /admin/topics` | 200；`total=8`，首条为新建主题 | 列表含新数据仍 200 |
| 5 | `GET /admin/topics/{id}` | 200；两分区，leader `msg-3` / `msg-1`，`metadata_version=0` | **详情恢复**（同源破面） |
| 6 | `GET /admin/dlq` | 200 | 死信列表正常（计划 §6.1 #15） |
| 7 | `GET /admin/replay` | 200 | 重放列表正常 |
| 8 | `GET /admin/acls` | 200 | ACL 管理面读正常（含判权后读行） |
| 9 | `GET /admin/overview` | 200 | 概览正常 |
| 10 | `GET /admin/partitions?namespace=msg&topic=probe.acl.session` | 首次 **500**；约 5s 后 **200**（两分区 `leo=0`、`hw=0`、`isr` 三副本齐全、leader `msg-3` / `msg-1`） | 首次失败为**新建主题挂载轮次未到**（`PartitionNotBoundError: 主题目录未登记`），与 ACL 无关；已登记遗留 |

## 3. 结论

- 本次报告的 `GET /api/v1/msg/admin/topics`（有数据）与详情端点均恢复 200；死信 / 重放 / ACL / 概览 smoke 无 500。
- 分区详情的首次 500 属「新建主题 + 挂载周期」竞态，需在 02-10 挂载或 06-1-1 错误映射侧闭环，不阻塞本次修复。
- 演练脚本 `ops/cluster_drill/chain.py` 仍直读库（按拍板不回切）；接口已可用，后续需要时可回切。
