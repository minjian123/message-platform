"""保留清理节流窗口表（任务 01-7 详细设计 §4；Kiwi 2426）。

覆盖：同窗口只放行首个请求、跨窗口再放行、键按（主题, 分区）互不影响、窗口常量取模块口径。
用例注入假时钟，窗口边界确定性由用例掌握（不靠 `sleep`）。
"""

from collections.abc import Callable

import pytest

from bms_msg.broker.retention import RetentionThrottle
from bms_msg.core.config import RETENTION_SCAN_WINDOW_MS

pytestmark = pytest.mark.kiwi_id(2426)

WINDOW_MS = 1_000
"""用例窗口宽度（与模块常量同值）。"""

TOPIC_ID = 7
"""用例主题主键。"""

PARTITION = 0
"""用例分区号。"""


@pytest.fixture
def now() -> list[float]:
    """可推进的假时钟（毫秒）。

    Returns:
        list[float]: 单元素时钟表。
    """
    return [0.0]


@pytest.fixture
def clock(now: list[float]) -> Callable[[], float]:
    """假时钟函数。

    Args:
        now: 时钟表。

    Returns:
        Callable[[], float]: 取当前毫秒。
    """
    return lambda: now[0]


def test_window_constant_is_module_scope() -> None:
    """窗口宽度缺省取模块常量（不进 `Settings`：基座 `extra="forbid"`）。"""
    assert RETENTION_SCAN_WINDOW_MS == WINDOW_MS
    throttle = RetentionThrottle()
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True


def test_first_request_in_window_is_admitted(clock: Callable[[], float]) -> None:
    """新窗口第一个请求必扫（到达即扫，并记下窗口序号）。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock)
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True


def test_second_request_in_same_window_is_skipped(clock: Callable[[], float], now: list[float]) -> None:
    """同窗口第二个请求跳过（同一窗口内最多扫一次）。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock)
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True
    now[0] = WINDOW_MS - 1
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is False


def test_next_window_is_admitted_again(clock: Callable[[], float], now: list[float]) -> None:
    """跨窗口再放行（固定时间片：`now // W` 变化即新窗口）。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock)
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True
    now[0] = WINDOW_MS
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True
    now[0] = 3 * WINDOW_MS
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True


def test_partitions_and_topics_are_independent(clock: Callable[[], float]) -> None:
    """键按（主题, 分区）分：别的分区 / 别的主题独立占窗口。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock)
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is True
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION + 1) is True
    assert throttle.admit(topic_id=TOPIC_ID + 1, partition_no=PARTITION) is True
    assert throttle.admit(topic_id=TOPIC_ID, partition_no=PARTITION) is False
