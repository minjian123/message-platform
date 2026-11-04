"""msg 口径常量：分区数边界、保留期、命名空间与主题名形态（详细设计 §8.2）。

**为什么不放配置**：基座 `Settings` 为 `extra="forbid"`，产品无法新增配置段；保留期「按配置可调」
已登记为遗留（需基座扩具名字段）。故本阶段用模块常量表达，改口径即改此处（一次发版）。
"""

import re

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
