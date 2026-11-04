"""msg 口径常量：分区数边界、保留期、命名空间 / 主题 / 组 / 成员形态、投递语义、消费组与延迟参数。

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
