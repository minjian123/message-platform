"""range 分区分配（msg 任务 01-4；Kiwi 2416）。

`assign_range` 是纯函数：结果只取决于「成员 id 集合 + 分区数」。本文件断言三条不变量——
**不重复**（区间连续切段）、**不全漏**（成员非空时覆盖全部分区）、**确定性**（同输入同输出），
并覆盖余数、成员多于分区（空闲成员）、空组三支。
"""

import pytest
from bms_core.core.concurrent import ConcurrentStableList

from bms_msg.broker.group import assign_range

pytestmark = pytest.mark.kiwi_id(2416)


def _members(*consumer_ids: str) -> ConcurrentStableList[str]:
    """构造成员列表（调用方保证升序）。

    Args:
        *consumer_ids: 成员标识。

    Returns:
        ConcurrentStableList[str]: 成员列表。
    """
    return ConcurrentStableList(consumer_ids)


def _as_dict(assignment: object) -> dict[str, list[int]]:
    """把分配结果转成普通字典便于断言。

    Args:
        assignment: 分配结果。

    Returns:
        dict[str, list[int]]: `consumer_id → 分区号`。
    """
    return {key: list(value) for key, value in assignment.items()}  # type: ignore[attr-defined]


def test_range_even_split() -> None:
    """整除：4 分区 2 成员 → 各 2 个连续区间，并集完整、无重复。"""
    result = _as_dict(assign_range(_members("a", "b"), 4))
    assert result == {"a": [0, 1], "b": [2, 3]}


def test_range_remainder_goes_to_earlier_members() -> None:
    """有余数：5 分区 2 成员 → 字典序靠前者多分一个（结果确定，便于断言）。"""
    result = _as_dict(assign_range(_members("a", "b"), 5))
    assert result == {"a": [0, 1, 2], "b": [3, 4]}


def test_idle_members_get_empty_assignment() -> None:
    """成员多于分区：靠后的成员拿到空列表（空闲成员不报错，需求 01-4 内容 3）。"""
    result = _as_dict(assign_range(_members("a", "b", "c"), 2))
    assert result == {"a": [0], "b": [1], "c": []}


def test_no_members_yields_empty_mapping() -> None:
    """空组：不产生任何分配（组会转 `empty`，不重算）。"""
    assert _as_dict(assign_range(_members(), 4)) == {}
