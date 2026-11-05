"""msg 口径常量：分区数边界、保留期、命名空间 / 主题 / 组 / 成员形态、投递语义、消费组、延迟与保留 / 重放参数。

**为什么不放配置**：基座 `Settings` 为 `extra="forbid"`，产品无法新增配置段；保留期与投递参数
「按配置可调」已登记为遗留（需基座扩具名字段）。故本阶段用模块常量表达，改口径即改此处（一次发版）。
"""

import re
from collections.abc import Mapping
from types import MappingProxyType

MIN_PARTITION_COUNT = 1
"""分区数下界（0 个分区无法承载消息）。"""

MAX_PARTITION_COUNT = 1024
"""分区数上界（单主题分区过多会放大文件句柄与元数据压力）。"""

SECONDS_PER_DAY = 86_400
"""一天秒数（保留期换算用）。"""

MS_PER_SECOND = 1_000
"""一秒毫秒数（保留期以毫秒传输）。"""

DEFAULT_RETENTION_MS = 30 * SECONDS_PER_DAY * MS_PER_SECOND
"""缺省保留期：30 天（毫秒）。"""

MAX_RETENTION_MS = 366 * SECONDS_PER_DAY * MS_PER_SECOND
"""保留期上限：366 天（毫秒；防止误设超长保留打爆磁盘）。"""

NAMESPACE_PATTERN = re.compile(r"^[a-z0-9_-]{1,64}$")
"""命名空间形态（小写字母 / 数字 / 下划线 / 连字符，1~64 字符）。"""

TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,200}$")
"""主题名形态（字母 / 数字 / 点 / 下划线 / 连字符，1~200 字符）。"""

GROUP_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,128}$")
"""消费组名形态（字母 / 数字 / 点 / 下划线 / 连字符，1~128 字符；组元数据归 01-4）。"""

MESSAGE_KEY_MAX_LENGTH = 256
"""分区键最大长度（超长即契约层拒绝，避免把路由键当业务大字段用）。"""

DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000
"""可见性超时：投递后 30s 内未 ack 则该消息可被重投（详细设计 §6.3）。"""

MAX_DELIVERY_ATTEMPTS = 5
"""重投上限：同一条消息最多投递 5 次，超限即转死信（详细设计 §6.3）。"""

MAX_FETCH_MESSAGES = 100
"""单次拉取上限（条）。请求的 `max_messages` 不得越过该值。"""

DEFAULT_FETCH_MESSAGES = 20
"""单次拉取缺省条数（请求未给 `max_messages` 时）。"""

ACKS_AT_LEAST_ONCE = 1
"""投递档位：确认后才算成功（至少一次；缺省）。"""

ACKS_FIRE_AND_FORGET = 0
"""投递档位：不等确认（位点返回前直接前移；可能丢，仅用于可丢场景）。"""

CONSUMER_ID_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]{1,128}$")
"""成员标识形态（字母 / 数字 / 点 / 下划线 / 冒号 / 连字符，1~128 字符；允许 `host:pid`）。

组内唯一性由客户端保证（同 id 再 `join` 视为重连），服务端不校验唯一性（01-4 详细设计 §4.1）。
"""

HEARTBEAT_INTERVAL_MS = 10_000
"""心跳间隔建议值（毫秒）：客户端据此定保活周期；服务端随 `join` / `heartbeat` 响应回传。"""

SESSION_TIMEOUT_MS = 30_000
"""会话超时（毫秒）：`last_seen` 超过该值即判定成员失效（心跳间隔的 3 倍，容忍两次丢包）。

与可见性超时（`DEFAULT_VISIBILITY_TIMEOUT_MS`）解耦：前者判成员存活、后者判消息重投。
"""

ASSIGNMENT_STRATEGY_RANGE = "range"
"""分区分配策略：按成员 `consumer_id` 升序、分区按号连续切段（其余策略按此位扩展）。"""

DELAY_LEVEL_MS: Mapping[str, int] = MappingProxyType(
    {
        "1s": 1_000,
        "5s": 5_000,
        "10s": 10_000,
        "30s": 30_000,
        "1m": 60_000,
        "5m": 300_000,
        "10m": 600_000,
        "30m": 1_800_000,
        "60m": 3_600_000,
    }
)
"""延迟档位（档位名 → 毫秒；只读映射）。

**只增不删**：删档位会让存量延迟记录失去映射（`delay_level` 存的是档位名，不是毫秒值）。
不加任意精度：档位是容量与可测性的边界（详细设计 §4.1）。
"""

DELAY_TICK_MS = 1_000
"""推进周期（毫秒）：与 L0 秒桶同粒度，1s 档位偏差 0~1s 且只晚不早（详细设计 §5.4）。"""

DELAY_MAX_CATCHUP_SECONDS = 60
"""单轮追赶窗口（秒）：进程长停摆 / 时钟前跳后单轮最多推进 60s，其余留到下一轮（防扫桶风暴）。"""

DELAY_BATCH_LIMIT = 500
"""单轮转投上限（条）：一批投不完下一轮继续，防单轮卡死事件循环。"""

DELAY_QUOTA_PER_TENANT = 100_000
"""延迟区软上限（每租户条数）：超限 `350015`（429），不静默丢弃（详细设计 §4.5）。"""

DELAY_MAX_SPAN_MS = 86_400 * MS_PER_SECOND
"""延迟跨度上限（毫秒）：超出即 `10001`——延迟区不是定时任务系统，超长跨度应由业务用状态机表达。"""

MAX_RETENTION_BYTES = 1_099_511_627_776
"""主题容量上限上界（字节，1 TiB）：防止误设超长保留把磁盘一次打满（与 `MAX_RETENTION_MS` 同职责）。"""

RETENTION_SEGMENT_RECORDS = 1_000
"""段替身条数（内存实现每 N 条切一段；真实段的字节 / 时间粒度归 8B_02）。

**为什么按条数切段**：真实段删除是「整段」语义（段内只要还有未过期记录，整段都保留），
按条数切段的替身与之一致；逐条段会让内存实现比真实实现更精细，8B_02 换实现后用例期望值必须重写。
用例注入小值（如 2）即可验证段边界，不必造大数据。
"""

RETENTION_SCAN_WINDOW_MS = 1_000
"""保留清理的节流窗口（毫秒，01-7）：同一（主题, 分区）每窗口最多执行一次清理。

**为什么按分区**：清理本就按分区执行（`RetentionService.apply` 只处理一个分区），按主题分键
要么改变扫描范围、要么让单请求耗时随分区数上升。
**为什么是 1s**：保留期以天计，窗口只决定「清理时刻偏移」与「扫描频率」；降三个数量级已足够，
取 5s / 30s 只会同比拉长 `350003` 的出现时延。
**与后台清理的关系**：8B_02 的后台清理复用同一常量作采样窗口，但**自持键表**——共享键表会让
请求把主题「占」掉窗口，后台到点即跳过，正好漏扫。
"""

DEFAULT_REPLAY_RATE_PER_SECOND = 100
"""重放缺省速率（条/秒）：架构 06 §11 把默认值列为 8B_06 待定项，本阶段先定此值（常量一处改）。"""

MAX_REPLAY_RATE_PER_SECOND = 1_000
"""重放速率上界（条/秒）：契约层拒绝越界（`10001`），避免「限速」被配成实质不限速。"""

REPLAY_TICK_MS = 200
"""重放推进周期（毫秒）：1 秒 5 轮，速率 100 时每轮约 20 条；暂停 / 取消最迟一个周期后生效。"""

REPLAY_SCAN_BATCH = 200
"""单批扫描上限（条）：逐批 `fetch`，不物化全量选中列表（百万条会吃爆内存）。"""

REPLAY_BURST_SECONDS = 1
"""令牌上限折算秒数：进程长停摆后最多攒「1 秒的量」，不会一次性爆发。"""

REPLAY_TASK_BATCH = 5
"""单轮最多推进的任务数：多任务并行推进，单轮不因一个任务卡死。"""

REPLAY_ERROR_MSG_MAX_LENGTH = 512
"""任务失败原因落库截断长度（对齐列宽）。"""

SUBSCRIBE_POLL_INTERVAL_MS = 200
"""长驻流无消息时的重试拉取间隔（毫秒）。

真正的长轮询挂起归 03-5：本阶段用固定短间隔轮询，不忙等也不永久挂起；
换成事件唤醒时只改这一处（`StreamTuning` 是唯一的等待策略出口）。
"""

SUBSCRIBE_MAX_SECONDS = 60
"""单条长驻流时长上限（秒）：到期发 `close(deadline)` 正常结束，由客户端重连。

与 03-4 的连接空闲回收解耦：前者限制**单条流**活多久，后者判**连接**是否还活着。
"""

SUBSCRIBE_MAX_SECONDS_LIMIT = 600
"""请求参数 `max_seconds` 的上界（秒）：越界由契约层拒绝（`10001`）。

不设上界等于允许「无限流」——没有心跳（03-4）之前，无限流就是资源泄漏的入口。
"""

SUBSCRIBE_KEEPALIVE_MS = 15_000
"""空闲期 SSE 注释行心跳间隔（毫秒）。

只是给代理 / 网关保活用（SSE 规范内的空注释），**不是**应用层心跳——应用层心跳（PING）归 03-4。
"""

CONNECTION_HEARTBEAT_INTERVAL_MS = 10_000
"""连接心跳建议间隔（毫秒；与 01-4 成员心跳同数值、**独立常量**）。

成员心跳判「成员还活着」（会话与再均衡），连接心跳判「连接还活着」（名额与 drain 协商）——
同数值是为运维只记一个数，独立常量是为两者将来能分别调。
"""

CONNECTION_HEARTBEAT_TIMEOUT_MS = 30_000
"""连接无心跳即判半开的阈值（毫秒；间隔 3 倍，容忍两次丢包）。

判定只回收**连接名额**，不关流、不丢消息（详细设计 §5.1 三层分工）。
"""

MAX_CONNECTIONS = 1_024
"""单副本连接数上限（进程级；超限 `350020`）。

量级取「单副本 + 网关一跳」：反向代理后 `client_port` 是代理侧端口，上限统计的是入口连接数。
"""

MAX_STREAMS_PER_CONNECTION = 100
"""单连接长驻流上限（与 hypercorn `h2_max_concurrent_streams` 缺省同值；超限 `350021`）。

应用层只计长驻流（唯一有精确开 / 关时机的流类型）；短请求并发由协议层 SETTINGS 约束。
"""

SUBSCRIBE_MAX_WAIT_MS = 30_000
"""长轮询单次无消息等待上限（毫秒）：有消息立即返回，到期返空批。

与 `SUBSCRIBE_MAX_SECONDS`（一条流的总时长）解耦：前者是「一次等待多长」，后者是「这条流活多久」。
30s 小于常见网关的 60s 空闲超时，且挂起请求本身是活跃 stream（不会被连接空闲回收）。
"""

SUBSCRIBE_MAX_WAIT_LIMIT_MS = 60_000
"""请求参数 `max_wait_ms` 的上界（毫秒）：越界由契约层拒绝（`10001`）。

不设上界等于允许「挂起十分钟」，挂起数 × 时长会放大内存；`350004`（超时）仍留给 8B_03 RPC deadline。
"""

MAX_LONG_POLL_WAITERS = 1_024
"""全局长轮询等待者上限：超限**立即返空批**（不排队、不报错）。

挂起是延迟优化不是契约承诺；上限防慢客户端用挂起请求堆协程。按租户配额归 05 域。
"""

INFLIGHT_MAX_MESSAGES = 100
"""单分区持有者在途条数上限（与 `MAX_FETCH_MESSAGES` 同值：一批最多 100 条，便于解释）。"""

INFLIGHT_MAX_BYTES = 16 * 1024 * 1024
"""单分区持有者在途字节上限（16 MiB）。

按 `len(payload)` 计：broker 不解压，这是真实内存占用，也是 1 MiB 上限的同一把尺；
`uncompressed_len` 是生产者可选声明（可谎报），用它等于把硬背压降成软约束。
"""

SUBSCRIBE_PREFETCH = 0
"""长驻流预取条数（缺省 0 = 零缓冲）。

`0` 表示「拉到即发、发完再拉」；`>0` 表示发当前批的同时先拉下一批（`min(prefetch, max_messages)` 条）。
预取的消息**立即计入 inflight**（与已发出的消息同一本账），故 `prefetch <= INFLIGHT_MAX_MESSAGES`。
"""

SUBSCRIBE_PREFETCH_LIMIT = 100
"""预取条数上界（与 `MAX_FETCH_MESSAGES` 同口径，防「预取自己把 inflight 阈值占满」）。"""

RATE_LIMIT_TENANT_PER_SECOND = 0
"""租户级生产速率（条/秒；0 = 不限）。缺省关闭：不改变既有行为，数值由 05-3 配额给。"""

RATE_LIMIT_TOPIC_PER_SECOND = 0
"""主题级生产速率（条/秒；0 = 不限）。与租户级同时判、同时扣，任一不足即拒（`350005`）。"""

RATE_LIMIT_BURST_SECONDS = 1
"""令牌桶容量折算秒数：容量 = 速率 × 本值。进程长停摆最多攒「1 秒的量」，不会一次性爆发。"""

RATE_LIMIT_BUCKET_LIMIT = 4_096
"""令牌桶数量上限：超限先清「已满且无负债」的桶，仍超则按插入序淘汰最旧（防高基数打爆内存）。"""

RATE_LIMIT_RETRY_AFTER_MS = 1_000
"""限流拒绝的缺省建议重试间隔（毫秒；桶给出的等待更有依据时取桶值）。"""

METRICS_TOPIC_KEY_LIMIT = 500
"""背压指标的分键上限（`(tenant, topic)`）：超出归 `__other__`，防高基数。"""

WINDOW_STALL_THRESHOLD_MS = 50
"""窗口暂停判定阈值（毫秒）：流两次推进间隔超过该值即记一次 `msg_window_stall_total`。

应用侧看不到 h2 窗口数值，只能观测「写/推进等了多久」；阈值取 50ms 以避开正常调度抖动。
"""

CONNECTION_EVICT_SCAN_LIMIT = 64
"""单次惰性清扫最多检查的条目数（请求驱动，空载零唤醒）。"""

CONNECTION_RETRY_AFTER_MS = 1_000
"""建议重试间隔（毫秒）：drain 提示与限流拒绝（`350020` / `350021`）共用。"""

RECONNECT_BASE_MS = 200
"""重连退避基数（毫秒；最小测试客户端与 SDK 同口径）。"""

RECONNECT_MAX_MS = 30_000
"""重连退避上限（毫秒；指数增长到此封顶）。"""

RECONNECT_JITTER_RATIO = 0.2
"""重连抖动比例（±20%）：防「同一时刻断开的客户端同时重连」形成脉冲。"""
