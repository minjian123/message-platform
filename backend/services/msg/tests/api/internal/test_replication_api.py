"""内部复制端点（02-8）：鉴权 / fetch / snapshot / demote / apply / 协议版本。"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from bms_core.api.deps import get_token_verifier
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.oauth.verify import VerifiedToken
from fastapi import FastAPI

from bms_msg.api.internal.app import create_internal_app
from bms_msg.broker.log import StoredMessage
from bms_msg.broker.replication import (
    ReplicaFetchRequest,
    ReplicaFetchResult,
    ReplicationDivergedError,
    ReplicationSnapshot,
    ReplicaUnavailableError,
    StaleLeaderError,
    SwitchApplyCommand,
    SwitchApplyResult,
)
from bms_msg.core.trace import EMPTY_TRACE

pytestmark = pytest.mark.kiwi_id(2486)


class FakeCoordinator:
    """复制协调器替身（按 `from_offset` 触发不同错误）。"""

    def __init__(self) -> None:
        self.config = SimpleNamespace(node_id="msg-1")
        self.demoted: ConcurrentStableList[tuple[str, int]] = ConcurrentStableList()
        self.demote_error: Exception | None = None
        self.apply_error: Exception | None = None

    async def serve_fetch(self, request: ReplicaFetchRequest) -> ReplicaFetchResult:
        if request.from_offset == 99:
            raise StaleLeaderError("stale")
        if request.from_offset == 100:
            raise ReplicationDivergedError("diverged", leader_leo=5)
        if request.from_offset == 101:
            raise ReplicaUnavailableError("switching", reason="switching")
        message = StoredMessage(
            message_id="m-1",
            log_id="l-1",
            topic_id=request.topic_id,
            partition_no=request.partition_no,
            offset=request.from_offset,
            key=None,
            payload=b"hi",
            headers=ConcurrentStableDict({"h": "v"}),
            trace=EMPTY_TRACE,
            published_at=datetime(2026, 11, 12, tzinfo=UTC),
            epoch=1,
        )
        return ReplicaFetchResult(
            leader_epoch=1,
            from_offset=request.from_offset,
            records=ConcurrentStableList((message,)),
            leader_leo=request.from_offset + 1,
            high_watermark=request.from_offset + 1,
            leader_isr=ConcurrentStableFrozenList(("msg-1",)),
        )

    async def snapshot(self, *, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        if partition_no == 99:
            return None
        return ReplicationSnapshot(
            leader="msg-1",
            epoch=2,
            leo=5,
            hw=4,
            isr=ConcurrentStableFrozenList(("msg-1",)),
            replicas=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            start_offset=1,
        )

    async def demote(self, *, topic_id: int, partition_no: int, to_leader: str, to_epoch: int) -> None:
        if self.demote_error is not None:
            raise self.demote_error
        self.demoted.add((to_leader, to_epoch))

    async def apply_switch(self, command: SwitchApplyCommand) -> SwitchApplyResult:
        if self.apply_error is not None:
            raise self.apply_error
        return SwitchApplyResult(to_leo=5, truncated_to=None, retained_tail=0, already_applied=False)


class FakeVerifier:
    """令牌校验替身（恒返回服务身份）。"""

    async def verify(self, token: str, *, audience: object = None) -> VerifiedToken:
        del token, audience
        return VerifiedToken(subject="msg", service="msg")


@pytest.fixture
def internal_app() -> FastAPI:
    """内部应用（主应用 state 带协调器）。"""
    main = FastAPI()
    main.state.replication = FakeCoordinator()
    app = create_internal_app(main)
    app.dependency_overrides[get_token_verifier] = lambda: FakeVerifier()
    return app


@pytest.fixture
def client(internal_app: FastAPI) -> httpx.AsyncClient:
    """内部应用客户端（带服务令牌头）。"""
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=internal_app),
        base_url="http://internal",
        headers={"Authorization": "Bearer test"},
    )


async def test_healthz_needs_no_auth(internal_app: FastAPI) -> None:
    """健康探针：不鉴权、返回节点标识与协议版本。"""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=internal_app), base_url="http://i") as client:
        response = await client.get("/internal/replication/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_fetch_success_and_errors(client: httpx.AsyncClient) -> None:
    """拉取：200 返回记录；409 / 410 / 503 返回 reason 闭集。"""
    payload = {
        "leader": "msg-1",
        "follower": "msg-2",
        "topic_id": 9,
        "partition_no": 0,
        "from_offset": 3,
        "max_messages": 5,
        "epoch": 1,
    }
    ok = await client.post("/internal/replication/fetch", json=payload)
    assert ok.status_code == 200
    assert ok.json()["records"][0]["payload"] == "aGk="

    for offset, status, reason in ((99, 409, "stale_epoch"), (100, 410, "diverged"), (101, 503, "switching")):
        response = await client.post("/internal/replication/fetch", json={**payload, "from_offset": offset})
        assert response.status_code == status
        assert response.json()["detail"]["reason"] == reason


async def test_fetch_protocol_mismatch(client: httpx.AsyncClient) -> None:
    """协议版本未知：400 protocol（不做尽力解析）。"""
    response = await client.post(
        "/internal/replication/fetch",
        json={
            "leader": "msg-1",
            "follower": "msg-2",
            "topic_id": 9,
            "partition_no": 0,
            "from_offset": 0,
            "max_messages": 1,
            "epoch": 1,
            "protocol_version": 9,
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"]["reason"] == "protocol"


async def test_snapshot_and_demote_and_apply(client: httpx.AsyncClient) -> None:
    """快照 / 降级 / 激活：404 未挂载、204 降级、200 激活且版本一致。"""
    snapshot = await client.get("/internal/replication/partitions/9/0/snapshot")
    assert snapshot.status_code == 200
    assert snapshot.json()["start_offset"] == 1

    missing = await client.get("/internal/replication/partitions/9/99/snapshot")
    assert missing.status_code == 404

    demote = await client.post(
        "/internal/replication/partitions/9/0/demote",
        json={"to_leader": "msg-2", "to_epoch": 3},
    )
    assert demote.status_code == 204

    applied = await client.post(
        "/internal/replication/partitions/9/0/apply-switch",
        json={
            "topic_id": 9,
            "partition_no": 0,
            "from_leader": "msg-1",
            "to_leader": "msg-2",
            "to_epoch": 3,
            "watermark": 4,
            "truncate": True,
            "grace_ms": 10,
        },
    )
    assert applied.status_code == 200
    assert applied.json()["to_leo"] == 5


async def test_apply_path_mismatch(client: httpx.AsyncClient) -> None:
    """激活：路径与命令体分区不一致 → 409 switch_conflict。"""
    response = await client.post(
        "/internal/replication/partitions/9/0/apply-switch",
        json={
            "topic_id": 9,
            "partition_no": 1,
            "from_leader": "msg-1",
            "to_leader": "msg-2",
            "to_epoch": 3,
        },
    )
    assert response.status_code == 409
    assert response.json()["detail"]["reason"] == "switch_conflict"


async def test_missing_token_is_auth_error(internal_app: FastAPI) -> None:
    """鉴权：缺令牌 → 401 且 reason=auth。"""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=internal_app), base_url="http://i") as client:
        response = await client.post(
            "/internal/replication/fetch",
            json={
                "leader": "msg-1",
                "follower": "msg-2",
                "topic_id": 9,
                "partition_no": 0,
                "from_offset": 0,
                "max_messages": 1,
                "epoch": 1,
            },
        )
    assert response.status_code == 401
    assert cast("dict[str, object]", response.json()["detail"])["reason"] == "auth"


async def test_healthz_without_replication_state() -> None:
    """健康探针：未装配复制时节点标识回 unknown。"""
    main = FastAPI()
    app = create_internal_app(main)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://i") as client:
        response = await client.get("/internal/replication/healthz")
    assert response.json()["node_id"] == "unknown"


async def test_demote_failure_and_apply_failure(internal_app: FastAPI) -> None:
    """降级失败 → 503；激活失败按原因映射 404 / 503。"""
    coordinator = cast("FakeCoordinator", internal_app.state.replication)
    coordinator.demote_error = RuntimeError("boom")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=internal_app),
        base_url="http://i",
        headers={"Authorization": "Bearer test"},
    ) as client:
        demote = await client.post(
            "/internal/replication/partitions/9/0/demote", json={"to_leader": "msg-2", "to_epoch": 3}
        )
        assert demote.status_code == 503
        coordinator.demote_error = None

        coordinator.apply_error = ReplicaUnavailableError("未挂载", reason="unavailable")
        missing = await client.post(
            "/internal/replication/partitions/9/0/apply-switch",
            json={
                "topic_id": 9,
                "partition_no": 0,
                "from_leader": "msg-1",
                "to_leader": "msg-2",
                "to_epoch": 3,
            },
        )
        assert missing.status_code == 404

        coordinator.apply_error = RuntimeError("boom")
        failed = await client.post(
            "/internal/replication/partitions/9/0/apply-switch",
            json={
                "topic_id": 9,
                "partition_no": 0,
                "from_leader": "msg-1",
                "to_leader": "msg-2",
                "to_epoch": 3,
            },
        )
        assert failed.status_code == 503
