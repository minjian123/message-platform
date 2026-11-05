"""应用固定成本治理（任务 01-7；Kiwi 2426）。

覆盖（完成标准逐条对应）：

- **同窗口最多一次**：同窗口内两次 `publish` 只触发一次 `RetentionService.apply`（记录式实现计数，
  不是看日志）；跨窗口后再次触发；
- **配置变更下一次请求即生效**：经 `TopicService.expand_partitions`（唯一写入口）改分区数后，
  下一次 `consume` 立刻按新分区数判定（此前同一请求报 `350009`）；
- **停用下一请求即生效**：直改库不失效（无 TTL，已登记遗留），精确失效后下一请求即 `350001`；
- **冷热两态逐字段一致**：缓存未预热与预热后的 `consume` 响应体完全相等、`publish` 响应键集相同。
"""

from collections.abc import AsyncIterator, Iterator
from typing import cast

import pytest
from bms_core.db.session import DbSession
from bms_core.db.unit_of_work import DbUnitOfWork
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.api.data import messages
from bms_msg.broker.retention import RetentionOutcome, RetentionService, RetentionThrottle
from bms_msg.errors import MsgErrorCode
from bms_msg.models.topic import STATUS_DISABLED
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.services.topic import TopicService
from bms_msg.tenant.context import CurrentTenant
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins

pytestmark = pytest.mark.kiwi_id(2426)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

NAMESPACE = "msg"
"""可用命名空间。"""

TOPIC_NAME = "audit.fixed"
"""用例主题名（单分区起步，增分区用例再改）。"""

GROUP = "audit"
"""用例消费组。"""

CONSUMER = "c1"
"""用例成员。"""

TENANT_ID = 7001
"""用例租户主键（与 `conftest.TENANT_ID` 同值）。"""

WINDOW_MS = 1_000
"""节流窗口（与模块常量同值；用例注入假时钟推进）。"""

BASE_URL = "http://test"
"""测试基址。"""

HEADERS = {"X-Tenant-ID": "demo"}
"""缺省租户头。"""

_SCANS: list[tuple[int, int]] = []
"""本用例的清理扫描记录（`(topic_id, partition_no)`）。"""


class _RecordingRetention(RetentionService):
    """记录每次实际清理的 `RetentionService`（转发原实现，语义不变）。"""

    async def apply(
        self, *, topic_id: int, partition_no: int, retention_ms: int, retention_bytes: int
    ) -> RetentionOutcome:
        """记一次扫描后转发。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            retention_ms: 保留时长。
            retention_bytes: 容量上限。

        Returns:
            RetentionOutcome: 清理结果。
        """
        _SCANS.append((topic_id, partition_no))
        return await super().apply(
            topic_id=topic_id, partition_no=partition_no, retention_ms=retention_ms, retention_bytes=retention_bytes
        )


@pytest.fixture(autouse=True)
def recordings(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[int, int]]]:
    """把清理实现换成记录式，并保证每用例从零开始。

    Args:
        monkeypatch: 替换 `api.data.messages` 模块里的实现名。

    Yields:
        list[tuple[int, int]]: 扫描记录。
    """
    _SCANS.clear()
    monkeypatch.setattr(messages, "RetentionService", _RecordingRetention)
    yield _SCANS
    _SCANS.clear()


@pytest.fixture
def now(msg_app: FastAPI) -> list[float]:
    """假时钟 + 该应用专属的节流窗口表（窗口边界由用例掌握）。

    Args:
        msg_app: 最小应用。

    Returns:
        list[float]: 单元素时钟表（毫秒）。
    """
    moment = [0.0]
    msg_app.state.retention_throttle = RetentionThrottle(window_ms=WINDOW_MS, clock=lambda: moment[0])
    return moment


@pytest.fixture
async def client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """最小应用上的异步客户端。

    Args:
        msg_app: 最小应用。

    Yields:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url=BASE_URL) as opened:
        yield opened


def _body(response: Response) -> dict[str, object]:
    """取响应 JSON。

    Args:
        response: 响应。

    Returns:
        dict[str, object]: 响应体。
    """
    return cast("dict[str, object]", response.json())


def _data(response: Response) -> dict[str, object]:
    """取响应体里的 `data`。

    Args:
        response: 响应。

    Returns:
        dict[str, object]: 业务数据。
    """
    return cast("dict[str, object]", _body(response)["data"])


async def _create_topic(client: AsyncClient, *, partition_count: int = 1) -> None:
    """建主题。

    Args:
        client: 客户端。
        partition_count: 分区数。
    """
    created = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": partition_count},
        headers=HEADERS,
    )
    assert created.status_code == 200, created.text


async def _join(client: AsyncClient) -> None:
    """成员加入组。

    Args:
        client: 客户端。
    """
    joined = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": TOPIC_NAME, "consumer_id": CONSUMER},
        headers=HEADERS,
    )
    assert joined.status_code == 200, joined.text


async def _publish(client: AsyncClient) -> Response:
    """发布一条消息。

    Args:
        client: 客户端。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "eA=="},
        headers=HEADERS,
    )


async def _consume(client: AsyncClient, *, partition: int = 0) -> Response:
    """拉取一批（不等待，避免用例卡在长轮询）。

    Args:
        client: 客户端。
        partition: 分区号。

    Returns:
        Response: 响应。
    """
    return await client.post(
        "/api/v1/msg/consume",
        json={
            "group": GROUP,
            "consumer_id": CONSUMER,
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "partition": partition,
            "max_wait_ms": 0,
        },
        headers=HEADERS,
    )


async def _topic_id(db_session: AsyncSession) -> int:
    """取用例主题主键。

    Args:
        db_session: 内存库会话。

    Returns:
        int: 主题主键。
    """
    topic = await TopicRepository(cast("DbSession", db_session)).get_by_identity(namespace=NAMESPACE, name=TOPIC_NAME)
    assert topic is not None
    return int(topic.id)


async def test_same_window_scans_once(
    client: AsyncClient, now: list[float], recordings: list[tuple[int, int]], msg_app: FastAPI
) -> None:
    """同窗口内两次发布只扫一次；缓存首次请求后即回填。"""
    del now
    await _create_topic(client)
    await _join(client)

    assert (_body(await _publish(client))["code"], _body(await _publish(client))["code"]) == (0, 0)

    assert len(recordings) == 1
    assert len(msg_app.state.topic_cache) == 1


async def test_next_window_scans_again(
    client: AsyncClient, now: list[float], recordings: list[tuple[int, int]]
) -> None:
    """跨窗口后再次扫描（固定时间片）。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)
    now[0] = WINDOW_MS
    await _publish(client)

    assert len(recordings) == 2
    assert recordings[0] == recordings[1]


async def test_config_change_applies_on_next_request(
    client: AsyncClient, msg_app: FastAPI, db_session: AsyncSession, current_tenant: CurrentTenant
) -> None:
    """经唯一写入口改分区数后，下一次请求立刻按新值判定。"""
    await _create_topic(client, partition_count=1)
    await _join(client)

    before = await _consume(client, partition=1)
    assert (before.status_code, _body(before)["code"]) == (400, int(MsgErrorCode.PARTITION_COUNT_INVALID))

    session = cast("DbSession", db_session)
    topic_id = await _topic_id(db_session)
    # 前面的 HTTP 请求共用这一个内存会话：先收尾其自动事务，再让服务层自己开事务。
    await db_session.commit()
    service = TopicService(
        TopicRepository(session),
        PartitionRepository(session),
        DbUnitOfWork(session),
        cache=msg_app.state.topic_cache,
    )
    await service.expand_partitions(tenant=current_tenant, topic_id=topic_id, partition_count=2)

    # 分组按新分区数重算（01-4 口径：重连触发重分配），随后新分区即可拉取。
    await _join(client)
    after = await _consume(client, partition=1)
    assert (after.status_code, _body(after)["code"]) == (200, 0)


async def test_disabled_takes_effect_after_invalidate(
    client: AsyncClient, msg_app: FastAPI, db_session: AsyncSession
) -> None:
    """直改库不失效（无 TTL），精确失效后下一请求即 `350001`。"""
    await _create_topic(client)
    await _join(client)
    await _publish(client)

    session = cast("DbSession", db_session)
    await TopicRepository(session).update(await _topic_id(db_session), status=STATUS_DISABLED)
    await db_session.flush()
    # 直改库没有写入口可挂失效（停用端点归 06 域）：本请求仍读旧快照——这正是「不加 TTL」的边界。
    assert _body(await _publish(client))["code"] == 0

    msg_app.state.topic_cache.invalidate(tenant_id=TENANT_ID, namespace=NAMESPACE, name=TOPIC_NAME)
    denied = await _publish(client)
    assert (denied.status_code, _body(denied)["code"]) == (404, int(MsgErrorCode.TOPIC_NOT_FOUND))


async def test_cold_and_warm_responses_are_identical(client: AsyncClient) -> None:
    """缓存冷 / 热两态的响应逐字段一致（结构、字段值、错误码）。"""
    await _create_topic(client)
    await _join(client)

    cold = _data(await _consume(client))
    warm = _data(await _consume(client))
    assert cold == warm

    cold_publish = _data(await _publish(client))
    warm_publish = _data(await _publish(client))
    assert set(cold_publish) == set(warm_publish)
    assert (cold_publish["topic"], cold_publish["partition"]) == (warm_publish["topic"], warm_publish["partition"])
    assert cold_publish["deliver_at"] is warm_publish["deliver_at"] is None
