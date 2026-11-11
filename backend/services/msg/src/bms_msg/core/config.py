"""msg 口径常量：分区数边界、保留期、命名空间 / 主题 / 组 / 成员形态、投递语义、消费组、延迟与保留 / 重放参数。

**为什么不放配置**：基座 `Settings` 为 `extra="forbid"`，产品无法新增配置段；保留期与投递参数
「按配置可调」已登记为遗留（需基座扩具名字段）。故本阶段用模块常量表达，改口径即改此处（一次发版）。
"""

import re
from collections.abc import Mapping
from types import MappingProxyType

# 07B 06_01 步 1：RPC 协议常量整体上移 bms_core.msgclient.constants（单一事实源在基座）。
# 用显式赋值转发（而非 `from ... import`），既让 `from bms_msg.core.config import RPC_*` 零改动，
# 也不会被 ruff 当作未使用的 re-export import 删掉。
from bms_core.msgclient import constants as _rpc_constants

RATE_LIMIT_RPC_PER_SECOND = _rpc_constants.RATE_LIMIT_RPC_PER_SECOND
RPC_CALLER_HEADER = _rpc_constants.RPC_CALLER_HEADER
RPC_CALLER_MAX_LENGTH = _rpc_constants.RPC_CALLER_MAX_LENGTH
RPC_CANCEL_POLL_MS = _rpc_constants.RPC_CANCEL_POLL_MS
RPC_CIRCUIT_FAILURE_THRESHOLD = _rpc_constants.RPC_CIRCUIT_FAILURE_THRESHOLD
RPC_CIRCUIT_HALF_OPEN_MAX_CALLS = _rpc_constants.RPC_CIRCUIT_HALF_OPEN_MAX_CALLS
RPC_CIRCUIT_OPEN_MS = _rpc_constants.RPC_CIRCUIT_OPEN_MS
RPC_CIRCUIT_WINDOW_MS = _rpc_constants.RPC_CIRCUIT_WINDOW_MS
RPC_CLIENT_TIMEOUT_SLACK_MS = _rpc_constants.RPC_CLIENT_TIMEOUT_SLACK_MS
RPC_DEADLINE_HEADER = _rpc_constants.RPC_DEADLINE_HEADER
RPC_DEADLINE_MAX_MS = _rpc_constants.RPC_DEADLINE_MAX_MS
RPC_DEFAULT_TIMEOUT_MS = _rpc_constants.RPC_DEFAULT_TIMEOUT_MS
RPC_IDEMPOTENCY_IN_PROGRESS_MS = _rpc_constants.RPC_IDEMPOTENCY_IN_PROGRESS_MS
RPC_IDEMPOTENCY_TTL_MS = _rpc_constants.RPC_IDEMPOTENCY_TTL_MS
RPC_METHOD_MAX_LENGTH = _rpc_constants.RPC_METHOD_MAX_LENGTH
RPC_METHOD_PATTERN = _rpc_constants.RPC_METHOD_PATTERN
RPC_REMAINING_MS_HEADER = _rpc_constants.RPC_REMAINING_MS_HEADER
RPC_RETURN_RESERVE_MS = _rpc_constants.RPC_RETURN_RESERVE_MS
RPC_RETRY_BASE_MS = _rpc_constants.RPC_RETRY_BASE_MS
RPC_RETRY_CAP_MS = _rpc_constants.RPC_RETRY_CAP_MS
RPC_RETRY_MAX_ATTEMPTS = _rpc_constants.RPC_RETRY_MAX_ATTEMPTS
RPC_RETRY_MAX_BUDGET = _rpc_constants.RPC_RETRY_MAX_BUDGET
RPC_RETRY_MULTIPLIER = _rpc_constants.RPC_RETRY_MULTIPLIER
RPC_SERVICE_MAX_LENGTH = _rpc_constants.RPC_SERVICE_MAX_LENGTH
RPC_SERVICE_PATTERN = _rpc_constants.RPC_SERVICE_PATTERN
RPC_STREAM_CLIENT_IDLE_MS = _rpc_constants.RPC_STREAM_CLIENT_IDLE_MS
RPC_STREAM_COALESCE_BYTES = _rpc_constants.RPC_STREAM_COALESCE_BYTES
RPC_STREAM_COALESCE_FRAMES = _rpc_constants.RPC_STREAM_COALESCE_FRAMES
RPC_STREAM_IDLE_MS = _rpc_constants.RPC_STREAM_IDLE_MS
RPC_TENANT_HEADER = _rpc_constants.RPC_TENANT_HEADER
RPC_TRACE_HEADER = _rpc_constants.RPC_TRACE_HEADER
RPC_USED_MS_HEADER = _rpc_constants.RPC_USED_MS_HEADER

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

REPLAY_IMPACT_MAX_GROUPS = 100
"""影响基线最多采集的消费组数（组名升序截断；超出置 `truncated` 并回 `affected_truncated`）。

**为什么设上限**：基线在**建任务请求内同步采集**（每组每分区两次取值），异常租户的上千个组会把建任务
拖成数十秒；100 组足够覆盖真实排障场景（单主题的消费组通常个位数），超出时页面可提示「仅展示前 100 组」。
"""

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

不设上界等于允许「挂起十分钟」，挂起数 × 时长会放大内存；`350004` 是 RPC 预算耗尽（04-2 启用），与长轮询无关。
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

METRICS_SAMPLE_INTERVAL_SECONDS = 15.0
"""指标后台采样周期（秒）：与 Prometheus 缺省抓取间隔同频；不新增业务配置项（06-3-1）。"""

TOPIC_CLEANUP_SCAN_INTERVAL_SECONDS = 30.0
"""删主题清理的周期扫描间隔（秒）：兜底「首轮未跑完 / 进程重启 / 上一轮失败」，受理时另有即时 kick（06-2）。"""

TOPIC_CLEANUP_FAILURE_ALERT_ATTEMPTS = 3
"""清理连续失败告警阈值（轮次）：达到即每轮输出 warning 级日志，供 06-3 告警规则消费（06-2）。"""

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

# ---------------------------------------------------------------------------
# 02-1 分区追加日志（文件段）：数据根 / 记录格式 / 段与索引 / fsync / log_id 索引
# ---------------------------------------------------------------------------

DATA_DIR_ENV = "MSG_DATA_DIR"
"""消息数据根的环境变量名（部署注入；代码与脚本不写死绝对路径）。"""

DEFAULT_DATA_DIR = "var/lib/msg"
"""数据根缺省值（相对进程工作目录）：开发态从 `backend/` 启动即落 `backend/var/lib/msg`。"""

DATA_ROOT_SUBDIR = "data"
"""数据根下的分区目录子目录名（`{root}/data/{tenant}/{namespace}/{topic}.{topic_id}/{partition}`）。"""

RECORD_FORMAT_VERSION = 1
"""记录格式版本（写入时写当前值；读取到更高版本即拒绝，不做猜测解析）。"""

RECORD_FRAME_BYTES = 12
"""记录框架固定前缀字节数：`length` + `crc` + `header_len` 各 4 字节。"""

MAX_RECORD_HEADER_BYTES = 1024 * 1024
"""记录头上限（1 MiB）：超出即判损坏，防伪造长度导致巨量内存分配。"""

MAX_RECORD_BYTES = 1024 * 1024 + MAX_RECORD_HEADER_BYTES + RECORD_FRAME_BYTES
"""记录总长上限（载荷 1 MiB + 头 + 框架）：与帧契约的载荷上限同尺。"""

SEGMENT_MAX_BYTES = 64 * 1024 * 1024
"""段滚动阈值（字节）：下一批写入会越过它即先开新段（单条记录不跨段）。"""

SEGMENT_ROLL_MS = 24 * 60 * 60 * MS_PER_SECOND
"""段滚动时长（毫秒，一天）：按**段首记录的发布时间**与本条之差判定，不按墙钟。"""

INDEX_INTERVAL_BYTES = 4096
"""稀疏 offset 索引间隔（字节）：段内首条必写，其后每越过该距离追加一条。

与 Kafka 的 `index.interval.bytes` 同量级：用「少量顺序扫描」换「索引体积远小于数据体积」。
"""

TIME_INDEX_INTERVAL_MS = 1_000
"""时间索引间隔（毫秒）：条目时间戳单调不减，同时是「段内最大发布时间」的取值来源。

误差上界即本值（保留期以天计，可忽略）；取 1s 而不是每条都记，是为了让时间索引体积约为数据的 1.6%。
"""

LOG_ID_INDEX_LIMIT = 100_000
"""`log_id` → 记录位置的进程内 LRU 上限（条）：未命中走「扫段头」兜底，故内存有界不影响正确性。"""

FSYNC_MODE_ENV = "MSG_FSYNC_MODE"
"""fsync 档位环境变量名（`batch` / `time` / `each` / `os`）。"""

FSYNC_INTERVAL_MS_ENV = "MSG_FSYNC_INTERVAL_MS"
"""fsync 时间窗环境变量名（毫秒；`batch` 与 `time` 档使用）。"""

FSYNC_MODE_BATCH = "batch"
"""fsync 档位：按批（缺省）——累计 N 条或超过时间窗即刷盘。"""

FSYNC_MODE_TIME = "time"
"""fsync 档位：按时间——只看距上次刷盘的时间窗。"""

FSYNC_MODE_EACH = "each"
"""fsync 档位：每条——写完即刷盘（单机崩溃窗口 0）。"""

FSYNC_MODE_OS = "os"
"""fsync 档位：依赖 OS——只写页缓存，不主动刷盘。"""

DEFAULT_FSYNC_MODE = FSYNC_MODE_BATCH
"""缺省 fsync 档位（任务口径：默认按批，其余三档可切）。"""

FSYNC_BATCH_RECORDS = 64
"""按批档的单批条数阈值（批次内合并一次刷盘）。"""

FSYNC_INTERVAL_MS = 200
"""按批 / 按时间档的刷盘时间窗（毫秒）。"""

RETENTION_THROTTLE_KEY_LIMIT = 8_192
"""保留清理节流键表上限（01-7 交接：键表只增不减，本任务加上限 + 淘汰）。

超限按「窗口序号最旧」淘汰；被淘汰的分区下一次请求重扫一次清理（幂等，只多一次扫描）。
"""

# ---------------------------------------------------------------------------
# 02-2 崩溃恢复：恢复点 / 截断取证
# ---------------------------------------------------------------------------

RECOVERY_POINT_NAME = "recovery-point.json"
"""恢复点文件名（每分区一个，放分区目录内）。

后缀不匹配段名正则 `^(\\d{20})\\.log$`，不会被段枚举误读成段。
"""

RECOVERY_POINT_TEMP_SUFFIX = ".tmp"
"""恢复点写入的临时文件后缀（同目录同卷，`Path.replace` 才是原子替换）。"""

RECOVERY_POINT_FORMAT_VERSION = 1
"""恢复点格式版本（读取到更高版本即拒绝加载，与记录 `format_version` 同口径）。"""

CORRUPT_TAIL_INFIX = ".log.corrupt-"
"""截断取证文件的中缀：`{base:020d}.log.corrupt-{ts}`。

刻意不采用 `.log` 结尾：段枚举只认 20 位零填充的 `.log`，取证副本不得参与恢复与保留判定。
"""

CORRUPT_TAIL_KEEP = 8
"""截断取证文件的保留份数上限（超出按时间删最早）。

设上限的理由：持续损坏时每次重启都会产出一份接近全量的副本，不清理会把磁盘再吃满一遍。
"""

# ---------------------------------------------------------------------------
# 02-3 消费位点：目录 / 文件格式 / 刷盘
# ---------------------------------------------------------------------------

OFFSETS_SUBDIR = "offsets"
"""数据根下的位点子目录名（`{root}/offsets/{tenant_id}/{group_dir}/{topic_id}-{partition}.offset`）。

**与 `data/` 平级而非塞进 `data/{tenant}/`**：位点与日志的备份口径不同（位点每日快照、日志不备份），
分目录让备份只搬 `offsets/`；且命名空间模式允许 `offsets` 作名字，塞进 `data/{tenant}/` 会撞名。
"""

OFFSET_FILE_SUFFIX = ".offset"
"""位点文件后缀（`{topic_id}-{partition}.offset`）。"""

OFFSET_FILE_TEMP_SUFFIX = ".tmp"
"""位点写入的临时文件后缀（同目录同卷，`Path.replace` 才是原子替换）。"""

OFFSET_FILE_MAGIC = b"MOFS"
"""位点文件魔数（msg offset file store）：前 4 字节不符即判「不是位点文件」。"""

OFFSET_FILE_FORMAT_VERSION = 2
"""位点文件格式版本（读到更高版本即拒绝加载，与记录 / 恢复点同口径）。"""

OFFSET_HEADER_FIXED_BYTES = 51
"""文件头定长部分：magic 4 + version 2 + group_len 1 + topic_id 8 + partition_no 4（组名变长另计）。"""

OFFSET_HEADER_FIXED_BYTES_V1 = 19
"""v1 文件头的固定字节数（读旧文件用）：magic 4 + 版本 2 + 组名长 1 + topic_id 8 + 分区 4。

v2 把 `topic_id` 从 int64 换成 40 字节 ASCII：基座雪花是 126 位（48+40+38），按 `>q` 写入必然
`struct.error: 'q' format requires ...`（07-6-1 故障演练实测：刷盘每轮失败、位点永不落盘）。
"""

OFFSET_TOPIC_ID_ASCII_BYTES = 40
"""v2 头里 `topic_id` 的 ASCII 定宽（十进制、右对齐空格填充；126 位雪花最多 38 位十进制）。"""

OFFSET_SLOT_BYTES = 24
"""单槽字节数：`seq` 4 + `offset` 8 + `updated_at_us` 8 + `crc` 4。"""

OFFSET_MAX_GROUP_BYTES = 128
"""组名 UTF-8 字节上限（与 `GROUP_NAME_PATTERN` 的 128 字符上限同尺）。"""

OFFSET_GROUP_DIR_MAX_CHARS = 64
"""组目录名保留的可读字符数：超出截断，唯一性由哈希后缀保证。

截断的理由是 Windows 路径长度上限（租户雪花 + 组名 128 + 文件名后接近 260），
不影响正确性——组名以文件头 `group` 字段为准，不从目录名反推。
"""

OFFSET_GROUP_HASH_CHARS = 8
"""组目录名哈希后缀位数（sha1 前 8 位十六进制）。

存在的唯一理由：`GROUP_NAME_PATTERN` 允许大小写混用，而 Windows / macOS 大小写不敏感——
直接用组名作目录名会让 `Audit` 与 `audit` 共享一个文件（前者位点丢失、后者跳消息）。
"""

OFFSET_FLUSH_INTERVAL_MS_ENV = "MSG_OFFSET_FLUSH_INTERVAL_MS"
"""位点刷盘时间窗环境变量名（毫秒）。"""

OFFSET_FLUSH_INTERVAL_MS = 1_000
"""位点刷盘时间窗缺省值（毫秒）：崩溃最多重复 1 秒内的提交。

取 1s 而非日志 fsync 的 200ms：位点丢失的后果是**重复消费**（业务侧幂等兜底），
而日志丢失的后果是丢消息——两者代价不对称，位点可以更松。实测结论回填架构 04 §11 #5。
"""

OFFSET_DIRTY_LIMIT_ENV = "MSG_OFFSET_DIRTY_LIMIT"
"""位点单次刷盘文件数上限的环境变量名。"""

OFFSET_DIRTY_LIMIT = 256
"""位点单次刷盘的文件数上限：既是「脏到多少就提前刷」的触发阈值，也是单轮 fsync 次数的上界。

一个组订 100 分区就是 100 个位点文件；不设上界时一次刷盘可能卡住事件循环数百毫秒。
"""

# ---------------------------------------------------------------------------
# 02-6 存储水位与拒绝策略：阈值 / 采样 / 就绪探测 / 日志抑制
# ---------------------------------------------------------------------------

STORAGE_WARN_RATIO_ENV = "MSG_STORAGE_WARN_RATIO"
"""告警水位环境变量名（比例 0~1，不含端点）。"""

STORAGE_WARN_RATIO = 0.70
"""告警水位缺省值：只记指标、不拒绝写（需求 02-6 §1「如 70% 告警」）。

它与拒绝阈值构成滞回区间：进入拒绝后必须降到**该值以下**才解除。
"""

STORAGE_REJECT_RATIO_ENV = "MSG_STORAGE_REJECT_RATIO"
"""拒绝水位环境变量名（比例 0~1，不含端点）。"""

STORAGE_REJECT_RATIO = 0.85
"""拒绝水位缺省值：达到即拒绝新消息（`350022` / 507）。

**预留空间 = 1 − 该值**（缺省 15%，落在架构 08 §4 要求的 10%~15%）：不单设预留比例参数，
否则两个参数冲突时还要额外定义「听谁的」，长期必然误配。
"""

STORAGE_RATIO_DIGITS = 4
"""使用率比较前保留的小数位：避免浮点尾差让「正好 85%」在阈值两侧来回翻。"""

STORAGE_SAMPLE_TTL_MS_ENV = "MSG_STORAGE_SAMPLE_TTL_MS"
"""水位采样缓存有效期环境变量名（毫秒）。"""

STORAGE_SAMPLE_TTL_MS = 1_000
"""水位采样缓存缺省有效期（毫秒）：publish 与 `/readyz` 共用同一份读数。

1s 内的滞后对「磁盘满」没有业务意义，换来的是热路径只做一次时间比较。
"""

STORAGE_PROBE_NAME = ".readyz-probe"
"""就绪探测写盘文件名：固定单文件、写完即删，不会累积（后缀不匹配段名正则，不被段枚举误读）。"""

STORAGE_CHECK_KEY = "msg_storage"
"""存储就绪检查项键。

**不取 `DEPENDENCIES` 里的名字**：那份清单是「可降级依赖」（redis / database 等），
磁盘满不可降级、只能拒写；`msg_` 前缀同时避免与基座依赖名撞车。
"""

RECOVERY_CHECK_KEY = "msg_recovery"
"""恢复状态检查项键（非必需项：数据有损不摘实例，只做降级可见）。"""

STORAGE_VOLUME_KEY_LIMIT = 8
"""水位指标的分键上限（卷数极难超过；越界归 `__other__`，与 03-5 的分键有界同口径）。"""

DELAY_FAILURE_LOG_WINDOW_MS = 1_000
"""延迟转投失败日志的抑制窗口（毫秒）：缺省一个推进 tick。

高水位期间转投每 tick 都会失败，不抑制会在几秒内刷出成千上万行同样原因。
"""

# ---------------------------------------------------------------------------
# 02-7 延迟消息持久化：`.delay` 段 / 两级软上限 / 超期告警 / attempts 落盘节流
# ---------------------------------------------------------------------------

DELAY_SEGMENT_MAX_BYTES = 8 * 1024 * 1024
"""`.delay` 段滚动阈值（字节）：比 `.log` 的 64 MiB 小一个量级。

延迟区写入稀疏，段小才便于「段内有效条目归零即整段回收」；单条记录不跨段。
"""

DELAY_SEGMENT_MAX_RECORDS = 10_000
"""`.delay` 段滚动阈值（条）：兜住「小记录写满大段」的实际形态（8 MiB 在 400 字节/条时约 20 000 条）。

与字节阈值同时生效，先到者触发轮转。
"""

DELAY_QUOTA_GLOBAL = 500_000
"""延迟区软上限（全局条数，02-7 补）：超限同样 `350015`，`data.scope = "global"`。

取值口径：每租户上限 100 000 的 5 倍（支持 5 个租户同时打满）；全量驻留下这是内存上界可控的量级，
字节级配额与容量实测归 05-3 / 8B_12。判定在存储实现侧 `enqueue()`（端口不新增方法），
且只拦「新 `log_id`」——覆盖写不增总量，重试永不被容量拒绝。
"""

DELAY_STUCK_TTL_MS = 7 * 24 * 60 * 60 * MS_PER_SECOND
"""超期未投的告警线（毫秒，缺省 7 天）：远大于延迟跨度上限（一天），不会误判正常长延迟。

**只告警、不删、不提前投**（架构 04 §2.6 #1「只能拒进、不能静默清」）：删掉一条卡住的延迟记录
等于平台单方面撕毁「某时刻一定投递」的承诺，提前投则违反「不早投」。
"""

DELAY_ATTEMPT_FLUSH_MS = 30_000
"""`attempts` 落盘的时间窗（毫秒）：窗口内只更新内存，到点（或下一次同记录失败）才追加落盘。

转投失败每 tick 重试一次，逐次落盘会让「1 KiB 记录卡一天」放大到几十 MB；节流后降到 MB 级。
与 02-3 位点「内存为准 + 后台刷 + 关闭强刷」同一口径。
"""

DELAY_ATTEMPT_FLUSH_ATTEMPTS = 10
"""`attempts` 落盘的次数阈值：距上次落盘累计增长达到该值即强制写一次（长窗口下也能留下进度）。"""

DELAY_METRICS_KEY_LIMIT = 200
"""延迟区指标按租户分键的上限：越界归 `__other__`（与 02-6 的卷分键、03-5 的主题分键同口径）。"""

# ---------------------------------------------------------------------------
# 05-5 租户生命周期：到期清扫
# ---------------------------------------------------------------------------

TENANT_PURGE_SCAN_INTERVAL_MS = 60_000
"""租户到期清扫周期（毫秒）：保留期以天计，1 分钟足够及时；查询只走账本 `purge_after` 索引。"""

TENANT_PURGE_BATCH_LIMIT = 100
"""单轮最多处理的到期待清理租户数：防一次清扫长时间占用事件循环。"""

# ---------------------------------------------------------------------------
# 04-1 RPC 一元调用：路径形态 / 调用方标识 / 客户端兜底超时 / RPC 速率
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 05-2 消息维度配额：六维缺省 / 软限 / 缓存 / 存储采样
# ---------------------------------------------------------------------------

QUOTA_DEFAULT_TOPIC_COUNT_ENV = "MSG_QUOTA_DEFAULT_TOPIC_COUNT"
"""主题数缺省值环境变量名。"""

QUOTA_DEFAULT_TOPIC_COUNT = 100
"""主题数缺省硬限（每租户 / 命名空间；`0` = 不限）。"""

QUOTA_DEFAULT_GROUP_COUNT_ENV = "MSG_QUOTA_DEFAULT_GROUP_COUNT"
"""消费组数缺省值环境变量名。"""

QUOTA_DEFAULT_GROUP_COUNT = 100
"""消费组数缺省硬限（每租户）。"""

QUOTA_DEFAULT_PRODUCE_RATE_ENV = "MSG_QUOTA_DEFAULT_PRODUCE_RATE"
"""生产速率缺省值环境变量名。"""

QUOTA_DEFAULT_PRODUCE_RATE = 1_000
"""生产速率缺省硬限（条/秒；每租户 / 命名空间）。"""

QUOTA_DEFAULT_STORAGE_BYTES_ENV = "MSG_QUOTA_DEFAULT_STORAGE_BYTES"
"""消息存储缺省值环境变量名。"""

QUOTA_DEFAULT_STORAGE_BYTES = 10 * 1024 * 1024 * 1024
"""消息存储缺省硬限（字节，10 GiB；每租户）。"""

QUOTA_DEFAULT_UNACKED_COUNT_ENV = "MSG_QUOTA_DEFAULT_UNACKED_COUNT"
"""未确认数缺省值环境变量名。"""

QUOTA_DEFAULT_UNACKED_COUNT = 10_000
"""未确认数缺省硬限（条；每租户聚合）。"""

QUOTA_DEFAULT_CONNECTION_COUNT_ENV = "MSG_QUOTA_DEFAULT_CONNECTION_COUNT"
"""连接数缺省值环境变量名。"""

QUOTA_DEFAULT_CONNECTION_COUNT = 1_000
"""连接数缺省硬限（每租户；仅新连接判定）。"""

QUOTA_SOFT_RATIO_PERCENT = 80
"""软限缺省比例（硬限的百分比）：只告警、不拒绝。"""

QUOTA_CACHE_TTL_MS_ENV = "MSG_QUOTA_CACHE_TTL_MS"
"""配额快照缓存有效期环境变量名（毫秒）。"""

QUOTA_CACHE_TTL_MS = 60_000
"""配额快照缓存缺省有效期（毫秒）：直改库最多滞后该值，管理面调整显式失效。"""

QUOTA_STORAGE_SAMPLE_TTL_MS_ENV = "MSG_QUOTA_STORAGE_SAMPLE_TTL_MS"
"""存储用量采样有效期环境变量名（毫秒）。"""

QUOTA_STORAGE_SAMPLE_TTL_MS = 30_000
"""存储用量采样缺省有效期（毫秒）：扫描是目录遍历，不宜每次发布都做。"""

QUOTA_UNIT_COUNT = "count"
"""单位：条数。"""

QUOTA_UNIT_BYTES = "bytes"
"""单位：字节数。"""

QUOTA_UNIT_QPS = "qps"
"""单位：每秒条数。"""

QUOTA_PERIOD_NONE = "none"
"""周期：瞬时（本期六维均为瞬时值）。"""

QUOTA_ORIGIN_DEFAULT = "default"
"""配额来源：懒初始化写入的缺省行。"""

QUOTA_ORIGIN_CONFIGURED = "configured"
"""配额来源：管理面调整写入。"""

ACCESS_ANOMALY_WINDOW_MS = 60_000
"""访问异常审计的节流窗口（毫秒）：同一 `(租户, 路径, 状态)` 窗口内只留首条。"""

ACCESS_ANOMALY_MAX_KEYS = 1_024
"""访问异常节流键数量上限：超过即清空，防止随机路径打爆内存。"""

# ---------------------------------------------------------------------------
# 07-3-1 备份脚本与调度：宿主侧运行旋钮（不进 Settings，基座 extra="forbid"）
# ---------------------------------------------------------------------------

BACKUP_DIR_ENV = "MSG_BACKUP_DIR"
"""备份根目录的环境变量名（必填；宿主独立卷，不与消息数据根同盘）。"""

BACKUP_PLATFORM_DUMP_DIR_ENV = "MSG_BACKUP_PLATFORM_DUMP_DIR"
"""平台库备份产物目录的环境变量名（必填；只读扫描，不复制平台库）。"""

BACKUP_KEEP_DAYS_ENV = "MSG_BACKUP_KEEP_DAYS"
"""位点快照保留天数的环境变量名。"""

BACKUP_SPACE_FACTOR_ENV = "MSG_BACKUP_SPACE_FACTOR"
"""空间估算系数的环境变量名（乘位点文件总字节）。"""

BACKUP_KEEP_DAYS = 7
"""快照保留天数缺省值：与平台库备份的本地保留口径一致。"""

BACKUP_SPACE_FACTOR = 1.2
"""空间估算系数缺省值：覆盖副本与清单之外的写放大。"""

BACKUP_SPACE_RESERVE_BYTES = 64 * 1024 * 1024
"""空间固定余量（64 MiB）：位点目录为空时也留出清单与指针的写入空间。

不把余量做成环境变量：它只兜住清单自身的体量，按备份体量缩放没有意义，
多一个可配项只会让「空间不足」的判据更难解释。
"""

BACKUP_SNAPSHOT_RETRIES = 3
"""单文件一致性校验重试次数：写入期间内容变化最多重试三轮。

位点文件是双槽 + 原子替换，正常写入不会读到半文件；重试是并发写入边界的
兜底证据，不是正确性的前提。
"""

BACKUP_STAGING_PREFIX = ".staging-"
"""写入期临时目录前缀（失败只删这类目录，绝不触碰已发布快照）。"""

BACKUP_MANIFEST_NAME = "manifest.json"
"""备份清单文件名（与快照同目录）。"""

BACKUP_POINTER_NAME = "current.json"
"""最新可用备份指针文件名（普通文件，不用符号链接）。"""

BACKUP_POINTER_TEMP_NAME = ".current.json.tmp"
"""指针原子替换用的同目录临时文件名。"""

BACKUP_ID_FORMAT = "%Y%m%dT%H%M%SZ"
"""备份标识时间格式（UTC，字典序即时间序，清理无需读目录元数据）。"""

BACKUP_MANIFEST_SCHEMA_VERSION = 1
"""清单格式版本（恢复侧读到更高版本即拒，与记录 / 恢复点同口径）。"""

# ---------------------------------------------------------------------------
# 07-3-2 恢复演练与记录：宿主侧运行旋钮（不进 Settings；恢复是宿主离线动作）
# ---------------------------------------------------------------------------

RECOVERY_WORK_DIR_ENV = "MSG_RECOVERY_WORK_DIR"
"""演练工作根的环境变量名（drill 必填；目标副本与临时备份根都落在这里）。"""

RECOVERY_STOP_CMD_ENV = "MSG_RECOVERY_STOP_CMD"
"""停止服务命令模板的环境变量名（live 必填；按平台 shell 语义执行）。"""

RECOVERY_START_CMD_ENV = "MSG_RECOVERY_START_CMD"
"""启动服务命令模板的环境变量名（live 必填）。"""

RECOVERY_METADATA_CMD_ENV = "MSG_RECOVERY_METADATA_CMD"
"""元数据库导入命令模板的环境变量名（live 必填；可用 `{dump}` / `{backup_id}` / `{target_env}`）。"""

RECOVERY_READY_URL_ENV = "MSG_RECOVERY_READY_URL"
"""就绪探针地址的环境变量名（live 必填）。"""

RECOVERY_TIMEOUT_SEC_ENV = "MSG_RECOVERY_TIMEOUT_SEC"
"""命令与就绪轮询超时的环境变量名（正整数秒）。"""

RECOVERY_DEFAULT_TIMEOUT_SEC = 120
"""超时缺省值（秒）：导入大库可上调，但不允许无限等待。"""

RECOVERY_PROBE_INTERVAL_MS = 500
"""就绪轮询间隔（毫秒）；也是 live 模式等待服务可见的步进。"""

RECOVERY_SAMPLE_PER_PARTITION = 20
"""每分区抽样读回的缺省条数（已确认消息可读的最小证据）。"""

RECOVERY_SAMPLE_LIMIT = 200
"""抽样总量缺省上限：避免分区多时把演练拖成全量扫描。"""

RECOVERY_REPORT_DIR = "tmp/recovery"
"""演练记录缺省落点（相对仓根；`tmp/` 已忽略，不进库）。"""

RECOVERY_REPORT_SCHEMA_VERSION = 1
"""演练记录格式版本（后续只增字段，不删不改语义）。"""

RECOVERY_WORK_TARGET_SUBDIR = "target"
"""演练副本数据根在工作根下的子目录名。"""

RECOVERY_WORK_BACKUP_SUBDIR = "backup"
"""演练前自动备份目录在工作根下的子目录名。"""

RECOVERY_OUTPUT_TAIL_CHARS = 2_048
"""命令输出摘要保留的末尾字符数（只做排障线索，不作判据）。"""

RECOVERY_PROD_ENVS = ("prod", "production")
"""禁止执行 live 恢复的环境名：生产恢复走人工流程，不提供覆盖开关。"""

# ---------------------------------------------------------------------------
# 07-4-1 切换开关与分域迁移编排：前置清单 / 矩阵 / 演练记录（不进 Settings）
# ---------------------------------------------------------------------------

MIGRATION_PRECONDITIONS_PATH = "deploy/migration/preconditions.toml"
"""迁移前置清单相对仓根的缺省路径（唯一状态源；8B_05 交付后只改状态）。"""

MIGRATION_MATRIX_PATH = "deploy/migration/switch_matrix.toml"
"""切换矩阵相对仓根的缺省路径（机器可读单源；文档表由渲染结果粘贴）。"""

MIGRATION_REPORT_DIR = "tmp/migration"
"""演练记录缺省落点（相对仓根；`tmp/` 已忽略，不进库）。"""

MIGRATION_RECORD_SCHEMA_VERSION = 1
"""演练记录格式版本（后续只增字段，不删不改语义）。"""

MIGRATION_DEFAULT_ENV = "test"
"""`check` / `drill` 的缺省环境（隔离环境；生产闸门须显式 `--env prod`）。"""

MIGRATION_PROD_ENVS = ("prod", "production")
"""生产环境名：`check` 走硬闸门，`drill` 一律拒绝（生产切换归 bms 8B_08）。"""

MIGRATION_DRILL_ENVS = ("test", "testing", "dev", "development")
"""允许执行演练的环境名（隔离环境）；其它取值按参数错误处理，不猜语义。"""

MIGRATION_OBSERVE_WINDOW_MINUTES = 30
"""每域切换后的观察窗口缺省时长（分钟；生产窗口由 bms 8B_08 取值）。"""

MIGRATION_DEFAULT_EVENT_TYPE = "demo.order.created"
"""演练用事件类型（`topic_for` 据此派生 `bms-demo-order`，与跨仓契约夹具同源）。"""

MIGRATION_DEFAULT_GROUP = "migration-drill"
"""演练用消费组（与业务组隔离，避免污染真实位点）。"""

MIGRATION_DEFAULT_CONSUMER_ID = "drill-1"
"""演练用消费成员标识（组内唯一即可）。"""

MIGRATION_DEFAULT_READ_ONLY_SERVICE = "demo"
"""网络演练的只读调用目标服务（msg 内置 `demo` 方法恒可调用）。"""

MIGRATION_DEFAULT_READ_ONLY_METHOD = "echo"
"""网络演练的只读调用目标方法（`demo.echo` 原样回传，幂等且无副作用）。"""

MIGRATION_DEFAULT_TENANT = "demo"
"""演练缺省租户编码（数据面与管理面共用同一租户口径）。"""

# ---------------------------------------------------------------------------
# 07-4-2 回滚演练与记录：场景单源 / 恢复预算 / 记录（不进 Settings）
# ---------------------------------------------------------------------------

ROLLBACK_SCENARIOS_PATH = "deploy/migration/rollback_scenarios.toml"
"""回滚场景相对仓根的缺省路径（注入方式 / 判据锚点 / 阈值占位 / 恢复预算唯一来源）。"""

ROLLBACK_REPORT_DIR = "tmp/rollback"
"""回滚演练记录缺省落点（相对仓根；`tmp/` 已忽略，不进库）。"""

ROLLBACK_RECORD_SCHEMA_VERSION = 1
"""回滚演练记录格式版本（后续只增字段，不删不改语义）。"""

ROLLBACK_RECOVERY_BUDGET_MS = 300_000
"""恢复时间缺省预算（毫秒）：回滚开始到事件链与只读调用均恢复；场景表可覆盖。"""

ROLLBACK_ERROR_RATE_THRESHOLD = 1.0
"""错误率判据缺省阈值（百分比，1.0 = 1%）；真实数值归 8B_12 压测结论。"""

ROLLBACK_LATENCY_THRESHOLD_MS = 200.0
"""延迟判据缺省阈值（毫秒，P95 形状）；真实数值归 8B_12 压测结论。"""

ROLLBACK_DEFAULT_SAMPLES = 20
"""错误率场景缺省采样次数（够算出百分比，又不把演练拖长）。"""

# ---------------------------------------------------------------------------
# 07-6-1 六类故障注入：演练工作根 / 场景单源 / 预算 / 判定阈值（不进 Settings）
# ---------------------------------------------------------------------------

DRILL_WORK_DIR_ENV = "MSG_DRILL_WORK_DIR"
"""演练工作根的环境变量名（必填；临时数据根、填充文件与实例日志都在其下）。"""

DRILL_INSTANCE_PORT_ENV = "MSG_DRILL_INSTANCE_PORT"
"""隔离实例端口的环境变量名（父进程选空闲端口后传给 `ops.drill.instance`）。"""

DRILL_INSTANCE_ROOT_ENV = "MSG_DRILL_INSTANCE_DATA_ROOT"
"""隔离实例工作根的环境变量名（父进程传给 `ops.drill.instance`）。"""

DRILL_BASE_PORT_ENV = "MSG_DRILL_BASE_PORT"
"""隔离实例起始端口的环境变量名（占用则递增探测）。"""

DRILL_FILL_MB_ENV = "MSG_DRILL_FILL_MB"
"""磁盘满场景受控填充量的环境变量名（MiB）。"""

DRILL_SLOW_WRITE_MB_ENV = "MSG_DRILL_SLOW_WRITE_MB"
"""慢盘场景受控写入量的环境变量名（MiB）。"""

DRILL_REJECT_RATIO_ENV = "MSG_DRILL_REJECT_RATIO"
"""磁盘满场景拒绝水位的环境变量名（0~1）。"""

DRILL_REPORT_DIR_ENV = "MSG_DRILL_REPORT_DIR"
"""演练记录落点的环境变量名（相对仓根）。"""

DRILL_EVIDENCE_DIR_ENV = "MSG_DRILL_EVIDENCE_DIR"
"""验收矩阵落点的环境变量名（相对仓根；缺省 `tmp/drill/evidence`）。"""

DRILL_SCENARIOS_PATH = "deploy/drill/scenarios.toml"
"""场景单源相对仓根的缺省路径（注入方式 / 观测指标 / 判据锚点 / 预算唯一来源）。"""

DRILL_REPORT_DIR = "tmp/drill"
"""演练记录缺省落点（相对仓根；`tmp/` 已忽略，不进库）。"""

DRILL_SCENARIO_SCHEMA_VERSION = 2
"""场景单源格式版本（判据结构化后升 2；记录格式版本单独维护）。"""

DRILL_RECORD_SCHEMA_VERSION = 2
"""演练记录格式版本（只增字段；v1 记录仍可判读，缺增强字段记降级）。"""

DRILL_EVIDENCE_SCHEMA_VERSION = 1
"""验收矩阵（场景 × 判据 × 证据）格式版本。"""

DRILL_EVIDENCE_DIR = "tmp/drill/evidence"
"""验收矩阵缺省落点（相对仓根；`tmp/` 已忽略，不进库）。"""

DRILL_CHECKLIST_PATH = "deploy/drill/RELEASE_CHECKLIST.md"
"""发版前检查清单（生成物）相对仓根的路径。"""

DRILL_COMMAND_LIMIT = 200
"""记录 `commands[]` 的动作条数上限（超出保留首尾并标 `truncated`）。"""

DRILL_DEFAULT_ENV = "test"
"""演练缺省环境（隔离实例）；生产环境一律拒绝，无逃生开关。"""

DRILL_PROD_ENVS = ("prod", "production")
"""禁止执行注入的环境名：生产演练走人工流程，不提供覆盖开关。"""

DRILL_RECOVERY_BUDGET_MS = 240_000
"""单场景恢复预算缺省值（毫秒）：覆盖「重启实例 + 等可见性超时后重投」的最长路径。"""

DRILL_FILL_MB = 64
"""磁盘满场景受控填充量缺省值（MiB）：够触发水位判定，又不把本机盘写满。"""

DRILL_SLOW_WRITE_MB = 128
"""慢盘场景受控写入量缺省值（MiB）。"""

DRILL_REJECT_RATIO = 0.0001
"""磁盘满场景拒绝水位缺省值：本机无隔离卷配额时，用极低水位等价「卷将满」。"""

DRILL_BASE_PORT = 18_080
"""隔离实例起始端口缺省值（只绑 127.0.0.1）。"""

DRILL_READY_TIMEOUT_SEC = 60
"""实例就绪轮询超时（秒）：真装配含建表与插件装配，冷启动比探针慢。"""

DRILL_PROBE_INTERVAL_MS = 500
"""探针轮询间隔（毫秒）。"""

DRILL_HTTP_TIMEOUT_SEC = 10
"""演练 HTTP 调用超时（秒）。"""

DRILL_NAMESPACE = "msg"
"""演练用命名空间（须为已启用 module_key）。"""

DRILL_TOPIC_PREFIX = "drill."
"""演练主题名前缀（与业务主题隔离，便于清理识别）。"""

DRILL_GROUP_PREFIX = "drill-group-"
"""演练消费组名前缀（与业务组隔离，避免污染真实位点）。"""

DRILL_CONSUMER_PREFIX = "drill-consumer-"
"""演练成员标识前缀（按主题唯一：同一进程内多主题演练不会互相判死）。"""

DRILL_TENANT_CODE = "demo"
"""演练租户口径（隔离实例的租户源替身固定返回该编码）。"""

DRILL_TENANT_ID = 7001
"""演练租户主键（隔离实例替身固定值；与 bms 演示租户夹具同口径）。"""

DRILL_OUTPUT_TAIL_CHARS = 2_048
"""实例日志摘要保留的末尾字符数（只做排障线索，不作判据）。"""

DRILL_PARTITION_NO = 0
"""演练默认分区号（单分区主题，避免分区分配噪声）。"""
