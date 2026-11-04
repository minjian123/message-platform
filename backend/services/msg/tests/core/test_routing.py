"""分区键路由与轮询（msg 任务 01-1；Kiwi 2412）。

覆盖：同 key 结果稳定；不同 key 分布均匀；无 key（`None` / 空串）轮询；复位；分区数变化后映射按新
模数重算；分区数非正即 `350009`。
"""

import zlib

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.core.routing import next_partition, reset_round_robin, select_partition
from bms_msg.errors import MsgErrorCode, MsgPartitionCountInvalidError

pytestmark = pytest.mark.kiwi_id(2412)


def test_same_key_maps_to_same_partition() -> None:
    """同 key 100 次结果一致（路由可复现是「同键有序」的前提）。"""
    partitions = {select_partition("order-42", 7) for _ in range(100)}
    assert len(partitions) == 1
    assert 0 <= partitions.pop() < 7


def test_different_keys_spread_over_partitions() -> None:
    """1000 个 key 落 4 分区：每分区占比不偏（阈值 15%~35%，crc32 实测远优于该区间）。"""
    counts: ConcurrentStableDict[int, int] = ConcurrentStableDict()
    for index in range(1000):
        partition = select_partition(f"key-{index}", 4)
        counts.set(partition, counts.get(partition, 0) + 1)
    assert set(counts.keys()) == {0, 1, 2, 3}
    assert all(150 <= count <= 350 for count in counts.values())


def test_partition_changes_after_count_growth() -> None:
    """分区数变化后映射按新模数重算（增分区会重映射部分 key，属已知且可接受行为）。"""
    key = "rebalance-probe"
    residue = zlib.crc32(key.encode("utf-8"))
    assert residue % 4 != residue % 5
    assert select_partition(key, 4) == residue % 4
    assert select_partition(key, 5) == residue % 5


def test_missing_key_uses_round_robin() -> None:
    """无 key（`None` / 空串）按轮询分配：连续调用覆盖全部分区且循环。"""
    reset_round_robin()
    sequence = [select_partition(None, 3) for _ in range(6)]
    assert sequence == [0, 1, 2, 0, 1, 2]
    assert select_partition("", 3) == 0


def test_reset_round_robin_restarts_sequence() -> None:
    """复位后轮询从 0 重新开始（用例隔离用）。"""
    reset_round_robin()
    assert next_partition(5) == 0
    assert next_partition(5) == 1
    reset_round_robin()
    assert next_partition(5) == 0


@pytest.mark.parametrize("partition_count", [0, -1])
def test_non_positive_partition_count_is_rejected(partition_count: int) -> None:
    """分区数非正：`350009`（有 key 与轮询两条路径都拒绝）。"""
    with pytest.raises(MsgPartitionCountInvalidError) as direct:
        select_partition("k", partition_count)
    assert direct.value.code == int(MsgErrorCode.PARTITION_COUNT_INVALID)
    with pytest.raises(MsgPartitionCountInvalidError):
        next_partition(partition_count)


def test_routing_error_is_business_error() -> None:
    """路由拒绝复用 msg 段位码（不是参数错误 10001）。"""
    with pytest.raises(MsgPartitionCountInvalidError) as excinfo:
        select_partition(None, 0)
    assert not isinstance(excinfo.value, ParamError)
