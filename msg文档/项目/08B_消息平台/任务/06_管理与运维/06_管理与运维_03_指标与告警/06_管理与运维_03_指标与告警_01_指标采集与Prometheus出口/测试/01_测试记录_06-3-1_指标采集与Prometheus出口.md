# 06-3-1 指标采集与 Prometheus 出口 · 测试记录

> 08B · 06 管理与运维 · 06-3 指标与告警 · 子任务 01（Kiwi 2460）

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [06 管理与运维](../../../06_管理与运维.md) › [06-3](../../06_管理与运维_03_指标与告警.md) › [06-3-1](../06_管理与运维_03_指标与告警_01_指标采集与Prometheus出口.md) › 测试记录

## 1. 测试范围 <a id="scope"></a>

依据「详细设计 + 实施记录」，覆盖指标清单与类型、快照转换、标签与基数、采样与降级、埋点、端点可解析六类：

| 层 | 文件 | 重点 |
| --- | --- | --- |
| 事件点采集 | `tests/metrics/test_traffic.py` | 发布 / 消费计数与字节、直方图桶、分键归并、reason 映射、RPC / fsync、空标签、单例重置 |
| 收集器 | `tests/metrics/test_collector.py` | 指标名 / 类型 / 标签、`__total__` 跳过、`__other__` 保留、磁盘取最大值、延迟 / 生命周期 / 重放 / 死信、空快照与异常值、`describe()`、`/metrics` 文本可解析 |
| 采样器 | `tests/metrics/test_sampler.py` | 积压 / 死信 / 重放写入、增长正差值、维度失败隔离、主题已删跳过、租户源回退、启动 / 停止幂等、快照 reset |
| 装配与埋点 | `tests/metrics/test_assembly.py` | 组存储默认 / 内存 `list_all`、停机先停采样器、启动钩子拉起采样器、基座不支持时降级、埋点异常吞掉 |
| 回归 | `tests/api/`、`tests/broker/`、`tests/test_skeleton.py` 等 | 发布 / 消费 / RPC / 帧 / 长轮询 / 限流 / 文件段 / 生命周期 / 装配 |

## 2. 用例与验收映射 <a id="cases"></a>

| # | 用例 | 断言 | 结果 |
| --- | --- | --- | --- |
| T1 | 发布 / 消费计数 | `messages_in` / `bytes_in` / `messages_out` 与条数、字节一致 | 通过 |
| T2 | 空批不记出流量 | 长轮询超时 count=0 不产生 `messages_out`，仍记延迟 | 通过 |
| T3 | 直方图桶 | 7ms 落第 2 桶、15s 落 `+Inf`、累计桶单调 | 通过 |
| T4 | 分键归并 | 超 `METRICS_TOPIC_KEY_LIMIT` 后 topic 归 `__other__`，租户保留 | 通过 |
| T5 | reason 映射 | 七类闭集（含 `data.reason` 细分）与未知异常回退 `internal` | 通过 |
| T6 | RPC / fsync 埋点 | 方法 / 结果 / scope 标签与耗时 | 通过 |
| T7 | 收集器标签 | `service` / `tenant` / `topic` / `reason` / `scope` / `status` 齐全 | 通过 |
| T8 | `__total__` / `__other__` | total 不产出序列、other 保留为标签值 | 通过 |
| T9 | 磁盘多卷 | 取最大值且无卷标签 | 通过 |
| T10 | 生命周期 / 清理 | `result` 标签与三 / 四计数 | 通过 |
| T11 | 重放 / 死信快照 | 任务状态、累计重放、失败计数、存量与增长 | 通过 |
| T12 | ISR 常量 | 02-5 前恒 0 且可被规则引用 | 通过 |
| T13 | `/metrics` 可解析 | 注册后 `render()` 文本可被 Prometheus parser 解析 | 通过 |
| T14 | HELP 清单 | 每个 family 在 `METRIC_HELP` 内（`_total` 归一） | 通过 |
| T15 | `describe()` | 与 `collect()` 同名集合 | 通过 |
| T16 | 采样写入 | 积压（LEO−committed 求和）/ 死信 / 重放 / 水位刷新 | 通过 |
| T17 | 死信增长 | 只累加正差值；存量下降不产生负值 | 通过 |
| T18 | 维度失败隔离 | 死信 / 存储失败不影响积压；DB 不可用时三维保留上次值 | 通过 |
| T19 | 主题已删 | 积压与死信跳过、不抛错 | 通过 |
| T20 | 租户源回退 | 查不到时用主键字符串作标签 | 通过 |
| T21 | 启停幂等 | `ensure_started` / `stop` 可重复 | 通过 |
| T22 | 组存储 `list_all` | 默认空实现与内存稳定排序 | 通过 |
| T23 | 停机偏序 | `BackgroundLoops` 先停采样器 | 通过 |
| T24 | 启动钩子 | 拉起采样器并登记回收句柄 | 通过 |
| T25 | 基座降级 | 无 `register_collector` 支持时不报错、仍装配采样器 | 通过 |
| T26 | 埋点异常 | 采集抛错不影响响应；业务异常仍上抛 | 通过 |

## 3. 执行与结果 <a id="run"></a>

```text
pytest services/msg/tests/metrics -q            → 38 passed
pytest services/msg/tests/observability -q      → 6 passed
受影响回归（api / broker / skeleton / registry） → 174 passed
ruff check / ruff format --check / pyright      → 全绿
提交前预检（改动行覆盖率）                       → 通过（15 个文件改动行全部执行）
bms bms_core/tests/metrics                      → 13 passed
```

## 4. 覆盖率 <a id="coverage"></a>

- 提交环：改动行全部被执行（预检输出「通过：15 个文件的改动行均已被执行」，行覆盖率 100%）。
- 推送环 / CI：模块级行 + 分支 100% 由推送预检与 CI 判定；本记录不预判。
- 两个空 series 守卫为不可达防御分支，已标注 `# pragma: no cover` 并写理由。

## 5. 问题与偏差 <a id="issues"></a>

| # | 问题 | 处置 |
| --- | --- | --- |
| 1 | 同名指标两套标签集（事件点 vs 采样器） | 重放 / 死信增长统一由采样器输出，`TrafficMetrics` 不再记同名指标 |
| 2 | 基座注册期冲突检测未生效 | Collector 提供 `describe()`；设计 §6.1 已回写 |
| 3 | 测试中 `CounterMetricFamily.name` 去掉 `_total` | 测试辅助同时索引 `name` 与 `name + "_total"` |

## 6. 遗留 <a id="leftovers"></a>

真机 `/metrics` 抓取（服务器尚无 msg 服务）、模块级分支覆盖率由推送环复核、多实例聚合归 8B_05。与[实施记录 §7](../实施/01_实施记录_06-3-1_指标采集与Prometheus出口.md#leftovers)一致。
