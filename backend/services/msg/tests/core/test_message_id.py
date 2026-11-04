"""message_id 生成（msg 任务 01-1；Kiwi 2412）。

覆盖：输出为字符串、十进制雪花、连续 / 高并发批量不重复、与 offset 无绑定关系（长度与雪花位宽一致）。
"""

import asyncio

import pytest

from bms_msg.core.message_id import new_message_id

pytestmark = pytest.mark.kiwi_id(2412)

MAX_ID_DIGITS = 38
"""雪花十进制位数上限（`2^126-1` 为 38 位）。"""


def test_message_id_is_decimal_string() -> None:
    """输出为十进制字符串（超出 JS 安全整数，接口一律字符串）。"""
    message_id = new_message_id()
    assert isinstance(message_id, str)
    assert message_id.isdigit()
    assert 15 <= len(message_id) <= MAX_ID_DIGITS


def test_message_id_is_unique_across_many_calls() -> None:
    """10 万个连续生成不重复（全局唯一是幂等与排障的前提）。"""
    ids = {new_message_id() for _ in range(100_000)}
    assert len(ids) == 100_000


async def test_message_id_unique_under_concurrency() -> None:
    """并发批量生成不重复（雪花生成器线程 / 协程安全）。"""

    async def _batch() -> set[str]:
        await asyncio.sleep(0)
        return {new_message_id() for _ in range(2000)}

    batches = await asyncio.gather(*(_batch() for _ in range(5)))
    merged = {value for batch in batches for value in batch}
    assert len(merged) == 5 * 2000


def test_message_id_monotonic_per_process() -> None:
    """同进程连续生成单调递增且互不相同（与分区内 offset 无绑定：offset 由 broker 按分区分配）。"""
    ids = [int(new_message_id()) for _ in range(50)]
    assert ids == sorted(ids)
    assert len(set(ids)) == 50
