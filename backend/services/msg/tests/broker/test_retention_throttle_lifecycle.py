"""保留清理节流键表的生命周期（msg 任务 02-1 承接 01-7 的移交项；Kiwi 2427）。

01-7 的窗口表只存「每个（主题, 分区）当前窗口序号」，键集合只增不减（分区数只增不减）。
02-1 引入段管理时一并处置，**不另建第二套窗口表**：

- 上限 + 按窗口序号最旧淘汰（内存封顶，不依赖 06 域的主题删除入口）；
- `forget(topic_id, partition_no)`：主题删除 / 分区目录整体移除时精确清理（本期无调用方，为 06 域预留）。

用例注入假时钟，窗口边界确定性由用例掌握（不靠 `sleep`）。
"""

from collections.abc import Callable

import pytest

from bms_msg.broker.retention import RetentionThrottle

pytestmark = pytest.mark.kiwi_id(2427)

WINDOW_MS = 1_000
"""用例窗口宽度（与模块常量同值）。"""


@pytest.fixture
def clock() -> Callable[[], float]:
    """固定时钟（同一窗口内的判定；窗口推进由用例自行换时钟）。

    Returns:
        Callable[[], float]: 恒返回 0 的时钟。
    """
    return lambda: 0.0


async def test_key_table_is_capped_and_evicts_oldest(clock: Callable[[], float]) -> None:
    """T1：键表超上限 → 按窗口序号最旧淘汰，条目数回到上限。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock, key_limit=2)

    for topic_id in (1, 2, 3):
        assert throttle.admit(topic_id=topic_id, partition_no=0) is True

    assert len(throttle) == 2
    assert throttle.window_of(topic_id=1, partition_no=0) is None
    assert throttle.window_of(topic_id=3, partition_no=0) is not None


async def test_evicted_key_rescans_once(clock: Callable[[], float]) -> None:
    """T2：被淘汰的键下一次 `admit` 返回 True（重扫一次，清理幂等）。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock, key_limit=2)
    for topic_id in (1, 2, 3):
        throttle.admit(topic_id=topic_id, partition_no=0)

    assert throttle.admit(topic_id=1, partition_no=0) is True
    assert len(throttle) == 2
    assert throttle.admit(topic_id=1, partition_no=0) is False


async def test_forget_allows_immediate_rescan(clock: Callable[[], float]) -> None:
    """T3：`forget` 后同窗口内 `admit` 立即返回 True。"""
    throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=clock, key_limit=8)
    assert throttle.admit(topic_id=7, partition_no=0) is True
    assert throttle.admit(topic_id=7, partition_no=0) is False

    throttle.forget(topic_id=7, partition_no=0)

    assert len(throttle) == 0
    assert throttle.admit(topic_id=7, partition_no=0) is True
