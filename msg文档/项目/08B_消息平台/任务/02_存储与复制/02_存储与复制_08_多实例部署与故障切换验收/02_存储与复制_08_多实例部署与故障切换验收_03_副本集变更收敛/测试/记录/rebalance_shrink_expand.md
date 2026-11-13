# 02-8-3 真机复跑记录 · 副本集变更收敛（缩容 → 扩容）

> `ops.rebalance` 3 → 2 → 3 × 被移出实例卸载与重挂载 × ISR 收敛 × 卸载不删数据

[文档首页](../../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../../规划/01_规划_消息平台.md) › [阶段计划](../../../../../../计划/01_计划_消息平台.md) › [02 存储与复制](../../../../02_存储与复制.md) › [02-8 多实例部署与故障切换验收](../../../02_存储与复制_08_多实例部署与故障切换验收.md) › [02-8-3 副本集变更收敛](../../02_存储与复制_08_多实例部署与故障切换验收_03_副本集变更收敛.md) › 测试记录 › 真机复跑

---

## 1. 环境与前置 <a id="env"></a>

| 项 | 值 |
| --- | --- |
| 执行日期 | 2026-11-13 |
| 集群 | 隔离演练集群 `msg-drill-1..3`（三实例健康，`/readyz` 200） |
| 镜像 | `drill/msg:drill-5fc781d`（源码 `5fc781d`，含 02-8-3 收敛与本次缺陷修复） |
| 演练库 | `msgdrill_msg` |
| 演练主题 | `drill.membership.probe.1794530890`（`topic_id=58793353962844977940558998600155136`，3 分区） |
| 基线 | 重启三实例 → 等就绪 → 等挂载轮一轮：三节点各 `msg_replication_mounted_partitions=29`、三分区 ISR=3 |
| 写入 | **全程零业务写入**（不发布消息，验证「无写请求也能挂载 / 收敛」） |

> 前两轮缺陷暴露使用另一主题 `drill.membership.probe.1794529802`（见 §4）；两主题均留在演练库，按拍板**不执行 cleanup**（05-3-1 等演练现场同在此环境）。

## 2. 执行命令 <a id="commands"></a>

容器内直跑运维 CLI（`BMS_APP__SERVICE=msg` 由镜像外注入，缺它会因数据源模板缺服务标识而失败）：

```bash
# 缩容：候选节点去掉 msg-3
docker exec -e BMS_APP__SERVICE=msg msg-drill-1 python -m ops.rebalance plan \
  --nodes msg-1,msg-2 --factor 2 --min-insync 2 --topic-id 58793353962844977940558998600155136 --json
docker exec -e BMS_APP__SERVICE=msg msg-drill-1 python -m ops.rebalance apply \
  --plan-id rb-20261113T015313Z-b1ba2a --operator minjian --reason "02-8-3 真机 shrink 复跑" --wait-ms 120000 --json

# 扩容：候选节点恢复三节点
docker exec -e BMS_APP__SERVICE=msg msg-drill-1 python -m ops.rebalance plan \
  --nodes msg-1,msg-2,msg-3 --factor 3 --min-insync 2 --topic-id 58793353962844977940558998600155136 --json
docker exec -e BMS_APP__SERVICE=msg msg-drill-1 python -m ops.rebalance apply \
  --plan-id rb-20261113T015418Z-f87506 --operator minjian --reason "02-8-3 真机 expand 复跑" --wait-ms 120000 --json
```

取证：每 0.5s 抓三节点 `/metrics` 的 `msg_replication_mounted_partitions` 与该主题各分区 `msg_isr_size`；按时间窗抓 `docker logs`；`ops.rebalance status --json` 收口；平台库直读 `msg_partition`。

## 3. 时间线与判据 <a id="criteria"></a>

### 3.1 缩容（3 → 2，`rb-20261113T015313Z-b1ba2a`）

| 时刻（UTC+8） | 事件 |
| --- | --- |
| 09:53:14 | `plan` rc=0；三条 `shrink` 动作（`to_replicas` 均为 `msg-1/msg-2`） |
| 09:53:20 | `apply` rc=0（6s）；三行 `succeeded`，无 `force` / `degraded` |
| 09:53:20 | 平台库 `replicas` 3 → 2；分区 2 leader `msg-3` → `msg-1`、epoch 0 → 1（先切主再移出） |
| 09:53:20 | `msg-drill-3` 三条 `msg_replication_partition_detached`（`reason=not_in_replicas`）；挂载数 29 → 26 |
| 09:53:20 | leader 侧三条 `msg_replication_membership_changed`（`removed=['msg-3']`、`isr_size=2`）；ISR 3 → 2 |

### 3.2 扩容（2 → 3，`rb-20261113T015418Z-f87506`）

| 时刻（UTC+8） | 事件 |
| --- | --- |
| 09:54:19 | `plan` rc=0；三条 `expand` 动作 |
| 09:54:25 | `msg-drill-3` 对分区 0 收敛 `added=['msg-3']`（非承载占位，尚未挂载） |
| 09:54:48 | 挂载轮挂上分区 0（26 → 27）；leader 侧 `msg_replication_isr_expand`（`isr_size=3`） |
| 09:55:18 | 挂载轮挂上分区 1（27 → 28）；该分区 ISR 2 → 3 |
| 09:55:48 | 挂载轮挂上分区 2（28 → 29）；该分区 ISR 2 → 3 |
| 09:55:49 | `apply` rc=0（90s，逐分区等待真实追平）；三行 `succeeded`，`degraded=0` / `failed=0` |
| 09:57:04 | 收口采样：三节点挂载数均 29；三 leader 行 ISR 均 3 |

> 扩容是**逐分区串行**的：每个分区写完目标副本集后，等该分区的目标真正挂载、拉取过（`last_known_isr` 含自身）且 leader ISR 回升，才处理下一个分区——因此总耗时约 90s，而不是一次性 30s。

### 3.3 判据

| # | 判据 | 期望 | 实测 | 结论 |
| --- | --- | --- | --- | --- |
| H1 | 平台库 `msg_partition.replicas` | 3 → 2 → 3 | 三分区均 3 → 2 → 3 | 通过 |
| H2 | leader 侧 ISR | 3 → 2，刷新周期内（≤5s） | 缩容 `apply` 完成即 2（09:53:20，日志与采样同秒） | 通过 |
| H3 | 被移出节点卸载 | 挂载 −3、该主题 ISR 指标行删除、`partition_detached` 日志 | 29 → 26；`msg-drill-3` 该主题 `msg_isr_size` / `lag_records` 行消失；三条 detached | 通过 |
| H4 | 被移出节点停止拉取 | 不再为该主题拉取 | 卸载后仅见挂载轮 `mount_skipped(not_in_replica_set)`；该主题拉取失败计数 **0**；运行态与指标行已移除（拉取由挂载驱动） | 通过 |
| H5 | 扩容收敛 | 新成员无写请求即挂载、追平、重入 ISR（≤120s） | 逐分区 09:54:48 / 09:55:18 / 09:55:48 挂载并 `isr_expand`，总 90s | 通过 |
| S1 | 台账终态 | 全 `succeeded`、无 `force` / `degraded` | 缩容 / 扩容各 3 行 `succeeded` | 通过 |
| S2 | 卸载不删本地数据 | `msg-3` 数据目录字节数不变 | 105770 → 105770（缩容、扩容两段一致） | 通过 |

> **H4 口径修正**：`/internal/replication/fetch` 是**全局 POST**（分区在请求体里），日志没有「按分区 fetch」的 URL，因此不能沿用「日志无该分区 fetch」判据。改为「卸载日志 + 挂载轮跳过 + 该主题指标行消失 + 拉取失败计数 0」四证，机制上由 `hosted=False` / 移出挂载表保证不再调度拉取。

## 4. 缺陷暴露与修复（本轮新增，均已提交） <a id="defects"></a>

| # | 暴露轮次 | 现象 | 根因 | 修复 |
| --- | --- | --- | --- | --- |
| 1 | 第 1 轮 | `plan` 输出 3 条动作、`plan_id` 正常，但台账表 0 行；`apply` 返回 `succeeded=0, items=[]` | `DbMigrationLedger.create` / `delete_plan` / `_cas` 只 `flush` 未 `commit`，CLI 进程退出即回滚 | `b01ddf7 fix(02-8-1)`：三处补 `await uow.commit()` + 提交次数回归用例（沿用 2486） |
| 2 | 第 2 轮 | `apply` 三行 `succeeded`，但平台库 `replicas` / `leader` 未变 | `DbMigrationTopology.write_assignment` 同样只 `flush` 未 `commit`（切主存储 `DbLeaderSwitchStore` 有提交，故会出现「leader 变了、副本集没变」） | `e21bdf6 fix(02-8-1)`：补 `commit` + 回归用例 |
| 3 | 第 3 轮 | 扩容后 `msg-3` 永不挂载；`mount_once` 每轮 `skipped`，挂载数停在 26 | `plan` 预检对目标调 `snapshot` → 运行态留下**非承载占位**；`mount_once` 见「键已存在」直接跳过，占位永不被替换 | `6d27537 feat(02-8-3)`：`mount_once` 仅在 `hosted=True` 时跳过，非承载占位按分配事实丢弃后重挂载 + 回归用例 |
| 4 | 第 3 轮 | 上述第 3 条同时，扩容 `apply` 在目标未挂载时即返回成功（09:02:35 成功 / 09:02:54 才挂载） | 空分区下 `_probe_caught_up` 只看「目标在 leader ISR + LEO ≥ HW」；占位成员可被瞬时计入 ISR | `72e5df2 fix(02-8-1)`：目标判据追加「`node in snapshot.last_known_isr`」（真正拉取过）+ 回归用例 |
| 5 | 第 4 轮 | 第 4 条修复后仍能在挂载前看到 leader ISR 瞬时 3（`09:42:39`，挂载在 `09:43:01`） | 新成员进度以 `last_fetch_ms=now` 初始化，`_scan_partition` 把「从未拉取」误判为「新鲜」，空分区 LEO 0 ≥ HW 0 即入 ISR | `5fc781d fix(02-8-3)`：新成员进度 `last_fetch_ms=-inf`，拉取一次后才可能入 ISR + G2 断言 |

最终轮（`5fc781d`）时间线确认：leader ISR 只在**挂载发生的同一秒**由 2 升 3（`isr_expand` 与挂载轮同步），不再提前。

## 5. 证据文件 <a id="evidence"></a>

- [结构化证据（plan / apply / status / 库快照 / 采样序列）](raw/rebalance_shrink_expand.json)
- 指标采样：[缩容](raw/metrics_timeline_shrink.txt) ｜ [扩容](raw/metrics_timeline_expand.txt)
- 日志摘录：[log_excerpts.txt](raw/log_excerpts.txt)（卸载 / 成员变更 / 挂载轮 / ISR 扩张 / 拉取失败计数）

## 6. 遗留与说明 <a id="leftovers"></a>

| # | 项 | 处置 |
| --- | --- | --- |
| 1 | 扩容逐分区串行，3 分区约 90s（每分区等真实追平） | **预期行为**（迁移安全优先）；批量扩缩容耗时随分区数线性，容量观测归平台级容量实测 |
| 2 | `/internal/replication/fetch` 无按分区路径，日志无法逐分区证明「无 fetch」 | 判据改为「卸载 + 挂载跳过 + 指标行 + 失败计数」四证（§3.3 H4），已记录口径 |
| 3 | 演练环境未 cleanup（按 2026-11-13 拍板保留现场） | 两轮演练主题与三实例留在 `msgdrill_*`，后续复跑可直接使用；需要回收时执行 `ops.cluster_drill cleanup` |
| 4 | 镜像按提交短号另建（`drill-1347595` → `drill-b01ddf7` → `drill-e21bdf6` → `drill-6d27537` → `drill-5fc781d`） | 未覆盖 05-3-1 已验证的 `drill/msg:drill-1347595` |

---

> 结论以本记录 §3 判据为准，不默认通过；进度以阶段计划为唯一落点
