"""分区副本分配（02-8）：确定性轮转 / 关闭空副本集 / leader 保留。"""

from __future__ import annotations

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList

from bms_msg.services.partition_assignment import (
    PartitionAssigner,
    PartitionAssignmentConfig,
)

pytestmark = pytest.mark.kiwi_id(2486)

NODES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("msg-1", "msg-2", "msg-3"))


def _assigner(*, enabled: bool = True, factor: int = 3, min_insync: int = 2) -> PartitionAssigner:
    """构造分配器。

    pytestmark = pytest.mark.kiwi_id(2486)

        Args:
            enabled: 是否开启复制。
            factor: 副本数。
            min_insync: 最小同步副本数。

        Returns:
            PartitionAssigner: 分配器。
    """
    return PartitionAssigner(
        PartitionAssignmentConfig(
            enabled=enabled,
            node_id="msg-1",
            nodes=NODES,
            replication_factor=factor,
            min_insync_replicas=min_insync,
        )
    )


def test_assignment_is_deterministic_rotation() -> None:
    """分配：同一输入结果稳定，且按 `(topic*31+partition)%nodes` 轮转。"""
    assigner = _assigner()
    first = assigner.assign(topic_id=7, partition_no=0)
    second = assigner.assign(topic_id=7, partition_no=0)

    assert first == second
    assert list(first.replicas) == ["msg-2", "msg-3", "msg-1"]
    assert first.leader == "msg-2"
    assert first.min_insync_replicas == 2


def test_assignment_keeps_current_leader_when_still_replica() -> None:
    """leader：已有 leader 仍在副本集内时保留。"""
    result = _assigner().assign(
        topic_id=7,
        partition_no=0,
        current_replicas=ConcurrentStableFrozenList(("msg-1", "msg-2", "msg-3")),
        current_leader="msg-3",
    )

    assert result.leader == "msg-3"


def test_assignment_replaces_leader_when_removed() -> None:
    """leader：原 leader 不在副本集时取副本集首个。"""
    result = _assigner(factor=2).assign(
        topic_id=7,
        partition_no=0,
        current_replicas=ConcurrentStableFrozenList(("msg-1",)),
        current_leader="msg-1",
    )

    assert result.leader == result.replicas[0]


def test_assignment_disabled_has_empty_replicas() -> None:
    """复制关闭：`replicas=[]`、leader=本节点，与单实例行为一致。"""
    result = _assigner(enabled=False).assign(topic_id=1, partition_no=2)

    assert list(result.replicas) == []
    assert result.leader == "msg-1"
    assert result.min_insync_replicas == 1


def test_assignment_clamps_min_insync_to_factor() -> None:
    """min_insync：超过副本数时按副本数生效。"""
    result = _assigner(factor=2, min_insync=9).assign(topic_id=1, partition_no=0)

    assert result.min_insync_replicas == 2


def test_assign_range_returns_all_partitions() -> None:
    """区间分配：返回连续分区号且顺序稳定。"""
    mapping = _assigner().assign_range(topic_id=9, start_no=2, count=3)

    assert list(mapping.keys()) == [2, 3, 4]


@pytest.mark.parametrize("factor", [1, 2, 3])
def test_assignment_factor_bounds(factor: int) -> None:
    """副本数：1~节点数均有效且长度一致。"""
    result = _assigner(factor=factor).assign(topic_id=3, partition_no=1)

    assert len(result.replicas) == factor
