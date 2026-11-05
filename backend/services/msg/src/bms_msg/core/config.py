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
