"""分区键路由（详细设计 §6）：同 key 恒落同分区，无 key 进程内轮询。

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 哈希 | `zlib.crc32` | 标准库、跨进程稳定、开销低 |
| 不用 `hash()` | — | 进程间随机（`PYTHONHASHSEED`），重启后路由漂移 |
| 不用 md5 / sha256 | — | 开销大、均匀性无明显收益 |
| 无 key | 进程内轮询计数器 | 分布均匀、实现简单 |

**路由稳定性前提**：分区数不变即结果不变——这正是「分区只能增不减」的根本原因。增分区后
`% new_count` 会让部分 key 重映射（Kafka 同样如此），同键有序只在新旧交替点之后成立。
"""

import itertools
import zlib

from bms_msg.errors import MsgPartitionCountInvalidError

__all__ = ["next_partition", "reset_round_robin", "select_partition"]


class _RoundRobin:
    """进程内轮询计数器（`itertools.count` 的 `__next__` 为 C 实现，无需额外加锁）。"""

    def __init__(self) -> None:
        """初始化计数器（从 0 起）。"""
        self._counter = itertools.count()

    def next(self, partition_count: int) -> int:
        """取下一个分区号。

        Args:
            partition_count: 分区数（须为正）。

        Returns:
            int: 分区号（`0 ~ partition_count-1`）。
        """
        return next(self._counter) % partition_count

    def reset(self) -> None:
        """复位计数器（用例隔离用）。"""
        self._counter = itertools.count()


_ROUND_ROBIN = _RoundRobin()
"""无 key 轮询计数器（进程内单例）。"""


def select_partition(key: str | None, partition_count: int) -> int:
    """按分区键选分区（纯函数，可单测）。

    Args:
        key: 分区键；`None` / 空串表示未提供，走轮询。
        partition_count: 分区数（须为正）。

    Returns:
        int: 分区号（`0 ~ partition_count-1`）。

    Raises:
        MsgPartitionCountInvalidError: 分区数非正（`350009`）。
    """
    _ensure_partition_count(partition_count)
    if key is None or key == "":
        return next_partition(partition_count)
    return zlib.crc32(key.encode("utf-8")) % partition_count


def next_partition(partition_count: int) -> int:
    """轮询取下一个分区号（进程内计数器，跨调用单调递增）。

    Args:
        partition_count: 分区数（须为正）。

    Returns:
        int: 分区号（`0 ~ partition_count-1`）。

    Raises:
        MsgPartitionCountInvalidError: 分区数非正（`350009`）。
    """
    _ensure_partition_count(partition_count)
    return _ROUND_ROBIN.next(partition_count)


def reset_round_robin() -> None:
    """复位轮询计数器（用例隔离用；生产路径不调用）。"""
    _ROUND_ROBIN.reset()


def _ensure_partition_count(partition_count: int) -> None:
    """校验分区数为正（路由前置；越界同 `350009`）。

    Args:
        partition_count: 分区数。

    Raises:
        MsgPartitionCountInvalidError: 分区数非正（`350009`）。
    """
    if partition_count <= 0:
        raise MsgPartitionCountInvalidError(f"分区数必须为正：{partition_count}")
