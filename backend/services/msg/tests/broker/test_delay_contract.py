"""延迟区契约（msg 任务 02-7；Kiwi 2432）。

同一组断言对**内存实现**与**文件实现**参数化：02-7 只换实现、不动端口，两者行为必须一致——
容量（每租户 + 全局）、后写胜出、计数、排序、幂等删除、跨度上限都在这里当场对账。

覆盖（详细设计 §12 用例 D13）。
"""

from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ParamError

from bms_msg.broker.delay import DelayRecord, DelayStore, InMemoryDelayStore
from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.core.config import DELAY_MAX_SPAN_MS
from bms_msg.errors import MsgDelayQuotaExceededError

pytestmark = pytest.mark.kiwi_id(2432)

TENANT_ID = 9_001
"""用例租户主键。"""

OTHER_TENANT = 9_002
"""第二个租户（全局上限用例）。"""

TOPIC_ID = 11
"""用例主题主键。"""

NAMESPACE = "orders"
"""用例命名空间。"""

TOPIC_NAME = "orders.events"
"""用例主题名。"""


@pytest.fixture(params=["memory", "file"])
def make_store(
    request: pytest.FixtureRequest, tmp_path: Path, clock: Callable[[], datetime]
) -> Callable[..., DelayStore]:
    """按参数产出两种实现的构造器（容量阈值由各用例给，避免为配额专门造数据）。

    Args:
        request: pytest 参数（`memory` / `file`）。
        tmp_path: 用例临时目录（文件实现的数据根）。
        clock: 可推进时钟。

    Returns:
        Callable[..., DelayStore]: `make_store(quota=…, global_quota=…)`。
    """

    def _build(*, quota: int = 10, global_quota: int = 20) -> DelayStore:
        """构造一种实现。

        Args:
            quota: 每租户软上限。
            global_quota: 全局软上限。

        Returns:
            DelayStore: 延迟区实现。
        """
        if request.param == "memory":
            return InMemoryDelayStore(quota=quota, global_quota=global_quota)
        directory = PartitionDirectory(root=tmp_path)
        directory.bind(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME, topic_id=TOPIC_ID)
        return FileDelayStore(
            directory=directory,
            quota=quota,
            global_quota=global_quota,
            fsync_mode="each",
            clock=lambda: 0.0,
            now=clock,
        )

    return _build


def _record(
    clock: Callable[[], datetime],
    *,
    log_id: str = "l-1",
    span_ms: int = 60_000,
    attempts: int = 0,
    tenant_id: int = TENANT_ID,
    partition_no: int = 0,
) -> DelayRecord:
    """构造一条延迟记录。

    Args:
        clock: 可推进时钟。
        log_id: 记录标识。
        span_ms: 延迟跨度（毫秒）。
        attempts: 转投失败次数。
        tenant_id: 租户主键。
        partition_no: 分区号。

    Returns:
        DelayRecord: 延迟记录。
    """
    published_at = clock()
    return DelayRecord(
        log_id=log_id,
        message_id=f"m-{log_id}",
        tenant_id=tenant_id,
        topic_id=TOPIC_ID,
        namespace=NAMESPACE,
        topic=TOPIC_NAME,
        partition_no=partition_no,
        delay_level="1m",
        key="order-1",
        payload=b"payload",
        headers=ConcurrentStableDict({"source": "contract"}),
        trace_id="trace-1",
        published_at=published_at,
        deliver_at=published_at + timedelta(milliseconds=span_ms),
        attempts=attempts,
    )


async def test_enqueue_then_list_and_count(
    make_store: Callable[..., DelayStore], clock: Callable[[], datetime]
) -> None:
    """写入后可按租户 / 全量列出，计数与排序一致（`deliver_at` 升序、同刻按 `log_id`）。"""
    store = make_store()
    await store.enqueue(_record(clock, log_id="late", span_ms=600_000))
    await store.enqueue(_record(clock, log_id="b", span_ms=60_000))
    await store.enqueue(_record(clock, log_id="a", span_ms=60_000, partition_no=1))

    assert [item.log_id for item in await store.list_all()] == ["a", "b", "late"]
    assert [item.log_id for item in await store.list_for_tenant(tenant_id=TENANT_ID)] == ["a", "b", "late"]
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 3
    assert await store.count_for_tenant(tenant_id=OTHER_TENANT) == 0
    assert await store.list_for_tenant(tenant_id=OTHER_TENANT) == []


async def test_same_log_id_overwrites(make_store: Callable[..., DelayStore], clock: Callable[[], datetime]) -> None:
    """同 `log_id` 再入队即覆盖（`attempts` 以最后一次为准，总量不增）。"""
    store = make_store()
    record = _record(clock, log_id="l-1")
    await store.enqueue(record)
    await store.enqueue(replace(record, attempts=4))

    assert [item.attempts for item in await store.list_all()] == [4]
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 1


async def test_quota_scopes(make_store: Callable[..., DelayStore], clock: Callable[[], datetime]) -> None:
    """租户满与全局满都抛 `350015`，`data.scope` 区分两者。"""
    store = make_store(quota=2, global_quota=3)
    await store.enqueue(_record(clock, log_id="t-0"))
    await store.enqueue(_record(clock, log_id="t-1"))

    with pytest.raises(MsgDelayQuotaExceededError) as tenant_full:
        await store.enqueue(_record(clock, log_id="t-2"))
    assert tenant_full.value.data == {"scope": "tenant", "limit": 2, "count": 2, "tenant_limit": 2}

    await store.enqueue(_record(clock, log_id="o-0", tenant_id=OTHER_TENANT))
    with pytest.raises(MsgDelayQuotaExceededError) as global_full:
        await store.enqueue(_record(clock, log_id="o-1", tenant_id=OTHER_TENANT))
    assert global_full.value.data == {"scope": "global", "limit": 3, "count": 3, "tenant_limit": 2}


async def test_remove_is_idempotent(make_store: Callable[..., DelayStore], clock: Callable[[], datetime]) -> None:
    """`remove` 删一次即生效，重复调用 / 删除不存在都不报错。"""
    store = make_store()
    await store.enqueue(_record(clock, log_id="l-1"))
    await store.remove(log_id="l-1")

    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0
    assert list(await store.list_all()) == []
    await store.remove(log_id="l-1")
    await store.remove(log_id="missing")
    assert await store.count_for_tenant(tenant_id=TENANT_ID) == 0


async def test_span_limit(make_store: Callable[..., DelayStore], clock: Callable[[], datetime]) -> None:
    """跨度超上限由端口层拒绝（`10001`），边界值放行。"""
    store = make_store()
    with pytest.raises(ParamError) as rejected:
        await store.enqueue(_record(clock, log_id="over", span_ms=DELAY_MAX_SPAN_MS + 1))
    assert rejected.value.code == 10001

    await store.enqueue(_record(clock, log_id="exact", span_ms=DELAY_MAX_SPAN_MS))
    assert [item.log_id for item in await store.list_all()] == ["exact"]
