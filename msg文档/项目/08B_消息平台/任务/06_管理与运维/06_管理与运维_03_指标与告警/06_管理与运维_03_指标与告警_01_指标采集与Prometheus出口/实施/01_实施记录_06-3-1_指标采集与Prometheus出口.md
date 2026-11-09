# 06-3-1 指标采集与 Prometheus 出口 · 实施记录

> 08B · 06 管理与运维 · 06-3 指标与告警 · 子任务 01（Kiwi 2460）

[文档首页](../../../../../../../文档首页.md) › [消息平台规划](../../../../../../../规划/01_规划_消息平台.md) › [06 管理与运维](../../../06_管理与运维.md) › [06-3](../../06_管理与运维_03_指标与告警.md) › [06-3-1](../06_管理与运维_03_指标与告警_01_指标采集与Prometheus出口.md) › 实施记录

## 1. 任务信息 <a id="meta"></a>

| 项 | 值 |
| --- | --- |
| 任务 | 06-3-1 指标采集与 Prometheus 出口 |
| 对应需求 | [06-3 指标与告警](../../../../../需求/06_需求_管理与运维.md#r06-3) |
| 详细设计 | [01 详细设计](../设计/01_详细设计_06-3-1_指标采集与Prometheus出口.md) |
| Kiwi | 2460 |
| 计划行 | msg 计划 §3「06-3」（本记录不承载进度） |

## 2. 交付物与落点 <a id="deliverables"></a>

| # | 交付物 | 落点 |
| --- | --- | --- |
| 1 | 基座注册通道 | bms `backend/libs/bms_core/src/bms_core/metrics/base.py`、`prometheus.py`、`tests/metrics/test_prometheus_metrics.py` |
| 2 | 指标清单 / 文案 / 桶 / 枚举 | `backend/services/msg/src/bms_msg/metrics/names.py` |
| 3 | 事件点采集器 | `backend/services/msg/src/bms_msg/metrics/traffic.py` |
| 4 | 采样快照 | `backend/services/msg/src/bms_msg/metrics/lag.py` |
| 5 | 后台采样器 | `backend/services/msg/src/bms_msg/metrics/sampler.py` |
| 6 | Prometheus 收集器 | `backend/services/msg/src/bms_msg/metrics/collector.py` |
| 7 | 数据面埋点 | `api/data/messages.py`（发布 / 消费装饰器） |
| 8 | RPC 埋点 | `api/rpc/routes.py` |
| 9 | 刷盘埋点 | `broker/segment.py`、`broker/delay_segment.py` |
| 10 | 采样支撑 | `broker/group.py`（`list_all`）、`repositories/topic.py`（`get_by_id_unscoped`） |
| 11 | 装配与关闭偏序 | `bms_msg/main.py`、`broker/lifecycle.py`、`core/config.py`、`backend/config.toml` |
| 12 | 测试 | `backend/services/msg/tests/metrics/`（4 份） |
| 13 | 记录 | 本记录、[测试记录](../测试/01_测试记录_06-3-1_指标采集与Prometheus出口.md)、计划回写、架构 06 §5.1 |

## 3. 实施过程 <a id="process"></a>

1. **上下文**：读 AI 开发规范、需求 06-3、架构 06 §5 / §8、bms 可观测性设计与部署规范、msg 既有三套采集器与投递 / RPC / 存储代码。
2. **决策确认**：25 项待拍板用 question 逐项确认（落地方式 / 标签口径 / 采样周期 / 直方图桶 / reason 枚举 / RPC 纳入 / 死信语义 / 基座清单 / 部署聚合等）。
3. **Kiwi 登记**：06-3-1 取 2460（06-3-2 取 2461），先登记后编码。
4. **基座扩展**：`BaseMetrics` 加非抽象 `register_collector()`（默认 False），`PrometheusMetrics` 覆盖；补 3 条用例（注册渲染 / Null 返回 False / 渲染可解析）。
5. **msg 指标域**：新增 `names` / `traffic` / `lag` / `sampler` / `collector` 五个模块；采集器只读、Collector 纯内存转换、采样器后台写入。
6. **埋点**：发布 / 消费用装饰器统一记成功与 `BizError`；RPC 用装饰器；fsync 在段刷盘处计时；全部埋点吞掉指标异常。
7. **装配**：`config.toml` 开 `[metrics] provider=prometheus`；`configure_service()` 末尾注册 Collector 与构造采样器；启动钩子拉起、`BackgroundLoops` 停机先停采样器。
8. **验证**：单文件测试、ruff、pyright、提交前预检；`promtool` / `amtool` 在服务器容器内复核。

## 4. 关键实现说明与偏差 <a id="key"></a>

### 4.1 指标落地

- 自定义 `MsgMetricsCollector` 注册进基座 `CollectorRegistry`，`/metrics` 仍由基座提供；不自建端点、不第二次 `generate_latest`。
- Collector 显式带 `service` 标签（基座的自动注入只覆盖写入式指标）；`describe()` 返回与 `collect()` 同名的家族，供注册期冲突检测。
- 直方图用毫秒 10 桶（5~10000），`+Inf` 兜底；`HistogramReading.cumulative()` 生成累计桶。

### 4.2 偏差（回写设计）

| # | 设计原写 | 实际 | 原因 |
| --- | --- | --- | --- |
| 1 | 重放 / 死信增长由 `TrafficMetrics` 事件点计数 | 改由采样器读任务表 / 死信存量差值计算 | 事件点无法拿到租户 / 主题名；且与采样器同名指标冲突（一个 family 两套标签集） |
| 2 | 死信增长按「转入时 +1」 | 按相邻两次存量正差值累加 | 同上；存量下降不产生负值，重启归零登记开放项 |
| 3 | 基座注册同名冲突「重复注册即抛错」 | 只有提供 `describe()` 才在注册期检测；同一实例重复注册按对象去重 | `CollectorRegistry` 默认 `auto_describe=False` 的实测行为 |
| 4 | `/metrics` 内可读取实时积压 | 积压 / 死信 / 重放由 15s 采样器写入 | 同步端点不能做异步查询，且任务书禁止请求内全量扫描 |

### 4.3 采样与降级

- 采样器每维度独立 `try`：一个维度失败只 warning，保留上一轮快照（避免把「采样失败」表现成「积压归零」）。
- `_tenant_code()` 查租户源失败时回退主键字符串，不中断整轮。
- 主题已删 / 组记录损坏直接跳过该条，不中断同维度其它记录。
- 磁盘使用率多卷取最大值、不带卷标签（已拍板）。

### 4.4 埋点

- 发布 / 消费装饰器只在 `BizError` 时记错误；成功时记录条数、字节、延迟；指标异常一律吞掉。
- `msg_publish_latency_ms` 覆盖鉴权后到 `append` / 延迟入区；`msg_consume_latency_ms` 只覆盖批读取（不含长轮询等待）。
- `reason` 七类映射按错误码 / `data.reason` 判定，不做文案匹配。

## 5. 验证结果 <a id="verify"></a>

| 项 | 结果 |
| --- | --- |
| `pytest services/msg/tests/metrics` | 38 passed |
| `pytest services/msg/tests/observability` | 6 passed |
| 受影响回归（api / broker / skeleton / registry） | 174 passed |
| ruff / ruff format / pyright | 全绿 |
| 提交前预检（改动行覆盖率） | 通过（15 个文件改动行全部执行，行 100%） |
| 基座 bms 测试 | `bms_core/tests/metrics` 13 passed |
| `/metrics` 文本解析 | `text_string_to_metric_families` 通过 |

## 6. 覆盖率 <a id="coverage"></a>

提交前预检按改动行判定通过；模块级 100%（行 + 分支）以推送环与 CI 为准，本记录不预判。两个「空 series」守卫为不可达防御分支，已按规范标注 `# pragma: no cover` 并写理由。

## 7. 偏差与遗留 <a id="leftovers"></a>

| # | 遗留 | 归口 |
| --- | --- | --- |
| 1 | 多实例指标聚合与采样去重（进程内） | 8B_05 |
| 2 | `msg_isr_shrink_total` 恒 0 | 02-5 |
| 3 | 桶边界与采样周期按真实容量复核 | 8B_12 |
| 4 | 指标名登记进基座候选清单（本轮不改基座清单） | 后续基座能力项 |
| 5 | 死信增长按存量差值、重启归零 | 8B_05 / 8B_12 |
| 6 | 采样开销与组规模实测 | 8B_12 |
| 7 | 真机 `/metrics` 抓取（服务器尚无 msg 服务） | 部署窗口 |
