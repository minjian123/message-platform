"""消费位点端口与内存实现（msg 任务 01-2；Kiwi 2414）。

覆盖：无记录即 0、提交后可见、只进不退（小值幂等）、组 / 主题 / 分区三维隔离。
"""

import pytest

from bms_msg.broker.offset import InMemoryOffsetStore

pytestmark = pytest.mark.kiwi_id(2414)


async def test_missing_offset_is_zero(offset_store: InMemoryOffsetStore) -> None:
    """未提交过即 0（从头消费）。"""
    assert await offset_store.get(group="audit", topic_id=1, partition_no=0) == 0


async def test_commit_is_visible(offset_store: InMemoryOffsetStore) -> None:
    """提交后读到新值（提交值 = 下一条待消费的 offset）。"""
    await offset_store.commit(group="audit", topic_id=1, partition_no=0, offset=3)
    assert await offset_store.get(group="audit", topic_id=1, partition_no=0) == 3


async def test_commit_never_goes_backwards(offset_store: InMemoryOffsetStore) -> None:
    """小值提交幂等无操作（位点不回退）。"""
    await offset_store.commit(group="audit", topic_id=1, partition_no=0, offset=5)
    await offset_store.commit(group="audit", topic_id=1, partition_no=0, offset=2)
    await offset_store.commit(group="audit", topic_id=1, partition_no=0, offset=5)
    assert await offset_store.get(group="audit", topic_id=1, partition_no=0) == 5


async def test_offsets_are_scoped_by_group_topic_partition(offset_store: InMemoryOffsetStore) -> None:
    """位点按 (group, topic, partition) 三维隔离（组间互不影响）。"""
    await offset_store.commit(group="audit", topic_id=1, partition_no=0, offset=4)
    assert await offset_store.get(group="audit", topic_id=1, partition_no=1) == 0
    assert await offset_store.get(group="audit", topic_id=2, partition_no=0) == 0
    assert await offset_store.get(group="notify", topic_id=1, partition_no=0) == 0
