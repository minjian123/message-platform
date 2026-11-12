"""网络复制 / 控制通道（02-8）：端点解析、错误映射、令牌缓存、本地快路径。"""

from __future__ import annotations

from typing import cast

import httpx
import pytest
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict, ConcurrentStableFrozenList
from bms_core.core.exceptions import ConfigError
from bms_core.oauth.base import OAuthToken

from bms_msg.broker.network import (
    HttpLeaderControlTransport,
    HttpReplicaTransport,
    InternalChannelConfig,
    ServiceTokenProvider,
    TransportErrorMetrics,
    parse_node_endpoints,
)
from bms_msg.broker.replication import (
    ReplicaFetchRequest,
    ReplicationDivergedError,
    ReplicaUnavailableError,
    StaleLeaderError,
    SwitchApplyCommand,
    SwitchApplyResult,
)

pytestmark = pytest.mark.kiwi_id(2486)

ENDPOINTS = ConcurrentStableFrozenDict({"msg-2": "http://msg-2:8001", "msg-3": "http://msg-3:8001"})


class FakeIssuer:
    """服务令牌签发替身（记录调用次数）。"""

    def __init__(self) -> None:
        self.calls = 0

    async def issue(self, spec: object) -> OAuthToken:
        """签发固定令牌。"""
        del spec
        self.calls += 1
        return OAuthToken(access_token=f"token-{self.calls}", expires_in=60)


def _config(**overrides: object) -> InternalChannelConfig:
    """构造内部通道配置。"""
    values: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "host": "127.0.0.1",
            "port": 8001,
            "endpoints": ENDPOINTS,
            "timeout_ms": 1000,
            "connect_timeout_ms": 100,
            "fetch_max_bytes": 1024,
        }
    )
    for key, value in overrides.items():
        values.set(key, value)
    return InternalChannelConfig(**dict(values.items()))  # type: ignore[arg-type]


def _fetch_request() -> ReplicaFetchRequest:
    """构造拉取请求。"""
    return ReplicaFetchRequest(
        leader="msg-2",
        follower="msg-1",
        topic_id=9,
        partition_no=0,
        from_offset=3,
        max_messages=2,
        epoch=1,
    )


def _record(offset: int) -> dict[str, object]:
    """构造内部记录 JSON。"""
    return {
        "message_id": f"m-{offset}",
        "log_id": f"l-{offset}",
        "topic_id": 9,
        "partition_no": 0,
        "offset": offset,
        "key": None,
        "payload": "aGk=",
        "headers": {},
        "trace_id": None,
        "span_id": None,
        "parent_span_id": None,
        "trace_flags": None,
        "published_at": "2026-11-12T00:00:00+00:00",
        "content_type": "application/json",
        "compression": "none",
        "uncompressed_len": None,
        "epoch": 1,
    }


def test_parse_node_endpoints_valid_and_invalid() -> None:
    """端点解析：合法映射去尾斜杠；非法条目 / 重复 / 非 http / 带路径即拒。"""
    parsed = parse_node_endpoints("msg-1=http://msg-1:8001/, msg-2=https://msg-2:8001")
    assert parsed["msg-1"] == "http://msg-1:8001"
    assert parsed["msg-2"] == "https://msg-2:8001"

    for raw in ("bad", "msg-1=http://a:1,msg-1=http://b:1", "msg-1=ftp://a:1", "msg-1=http://a:1/path"):
        with pytest.raises(ConfigError):
            parse_node_endpoints(raw)


def test_internal_channel_requires_multi_instance_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """配置校验：多实例必须有端口与完整端点；单实例忽略。"""
    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_PORT", "0")
    with pytest.raises(ConfigError, match="多实例"):
        InternalChannelConfig.from_env(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            business_port=8000,
        )

    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_PORT", "8000")
    with pytest.raises(ConfigError, match="业务端口"):
        InternalChannelConfig.from_env(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            business_port=8000,
        )

    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_PORT", "8001")
    monkeypatch.setenv("MSG_REPLICATION_NODE_ENDPOINTS", "msg-1=http://msg-1:8001")
    with pytest.raises(ConfigError, match="未覆盖"):
        InternalChannelConfig.from_env(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            business_port=8000,
        )

    single = InternalChannelConfig.from_env(
        enabled=False,
        node_id="msg-1",
        nodes=ConcurrentStableFrozenList(("msg-1",)),
        business_port=8000,
    )
    assert single.port == 0


async def test_token_provider_caches() -> None:
    """令牌：TTL 内复用，不重复签发。"""
    issuer = FakeIssuer()
    provider = ServiceTokenProvider(cast("object", issuer))  # type: ignore[arg-type]

    first = await provider.authorization()
    second = await provider.authorization()

    assert first == second
    assert issuer.calls == 1


async def test_fetch_success_and_errors() -> None:
    """拉取：成功解析记录；409 / 410 / 503 / 401 映射到领域异常与错误计数。"""
    body = {
        "protocol_version": 1,
        "leader_epoch": 2,
        "from_offset": 3,
        "records": [_record(3), _record(4)],
        "leader_leo": 5,
        "high_watermark": 4,
        "leader_isr": ["msg-2"],
    }
    metrics = TransportErrorMetrics()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/fetch"):
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    transport = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=metrics,
        transport=httpx.MockTransport(handler),
    )
    result = await transport.fetch(_fetch_request())
    assert [message.offset for message in result.records] == [3, 4]
    assert result.high_watermark == 4
    await transport.aclose()

    status_cases = [
        (409, {"reason": "stale_epoch"}, StaleLeaderError),
        (410, {"reason": "diverged", "leader_leo": 7}, ReplicationDivergedError),
        (503, {"reason": "switching"}, ReplicaUnavailableError),
        (401, {"reason": "auth"}, ReplicaUnavailableError),
    ]
    for status, detail, expected in status_cases:
        failing = HttpReplicaTransport(
            config=_config(),
            tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
            metrics=metrics,
            transport=httpx.MockTransport(
                lambda _request, status=status, detail=detail: httpx.Response(status, json={"detail": detail})
            ),
        )
        with pytest.raises(expected):
            await failing.fetch(_fetch_request())
        await failing.aclose()


async def test_fetch_transport_failures() -> None:
    """拉取：超时 / 连接失败 / 未知节点都按不可达处理并计数。"""
    metrics = TransportErrorMetrics()

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout", request=request)

    def connect(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    for handler, reason in ((timeout, "timeout"), (connect, "connect")):
        transport = HttpReplicaTransport(
            config=_config(),
            tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
            metrics=metrics,
            transport=httpx.MockTransport(handler),
        )
        with pytest.raises(ReplicaUnavailableError) as exc:
            await transport.fetch(_fetch_request())
        assert exc.value.reason == reason
        await transport.aclose()

    missing = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=metrics,
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
    )
    request = _fetch_request()
    request = ReplicaFetchRequest(
        leader="missing",
        follower=request.follower,
        topic_id=request.topic_id,
        partition_no=request.partition_no,
        from_offset=request.from_offset,
        max_messages=request.max_messages,
        epoch=request.epoch,
    )
    with pytest.raises(ReplicaUnavailableError, match="地址未配置"):
        await missing.fetch(request)
    await missing.aclose()
    assert metrics.snapshot()


async def test_control_transport_snapshot_demote_apply() -> None:
    """控制通道：快照 404 返回 None、成功映射；demote / apply 走 HTTP；本地快路径直连协调器。"""
    snapshot_body = {
        "protocol_version": 1,
        "leader": "msg-2",
        "epoch": 2,
        "leo": 10,
        "hw": 8,
        "isr": ["msg-2"],
        "replicas": ["msg-2", "msg-3"],
        "switch_state": "idle",
        "writable": True,
        "last_known_hw": 8,
        "last_known_isr": ["msg-2"],
        "last_known_at": 1.0,
        "start_offset": 0,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/snapshot"):
            return httpx.Response(200, json=snapshot_body)
        if path.endswith("/demote"):
            return httpx.Response(204)
        if path.endswith("/apply-switch"):
            return httpx.Response(
                200,
                json={
                    "protocol_version": 1,
                    "to_leo": 10,
                    "truncated_to": None,
                    "retained_tail": 0,
                    "already_applied": False,
                },
            )
        return httpx.Response(404)

    transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    snapshot = await transport.snapshot(node_id="msg-2", topic_id=9, partition_no=0)
    assert snapshot is not None and snapshot.start_offset == 0
    await transport.demote(node_id="msg-2", topic_id=9, partition_no=0, to_leader="msg-3", to_epoch=3)
    applied = await transport.apply_switch(
        node_id="msg-2",
        command=SwitchApplyCommand(
            topic_id=9,
            partition_no=0,
            from_leader="msg-2",
            to_leader="msg-3",
            to_epoch=3,
            watermark=8,
            truncate=True,
            grace_ms=10,
        ),
    )
    assert applied.to_leo == 10
    await transport.aclose()

    missing = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda _request: httpx.Response(404)),
    )
    assert await missing.snapshot(node_id="msg-2", topic_id=9, partition_no=0) is None
    await missing.aclose()


async def test_control_local_fast_path() -> None:
    """本地快路径：登记自身后不再发 HTTP。"""

    class FakeCoordinator:
        async def snapshot(self, *, topic_id: int, partition_no: int) -> object:
            del topic_id, partition_no
            return None

        async def demote(self, **kwargs: object) -> None:
            del kwargs

        async def apply_switch(self, command: object) -> object:
            del command
            raise AssertionError("不应调用")

    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("本地路径不应发 HTTP")

    transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(handler),
    )
    transport.register_local("msg-1", cast("object", FakeCoordinator()))  # type: ignore[arg-type]
    assert await transport.snapshot(node_id="msg-1", topic_id=9, partition_no=0) is None
    await transport.demote(node_id="msg-1", topic_id=9, partition_no=0, to_leader="msg-2", to_epoch=2)
    await transport.aclose()


async def test_non_json_error_body_maps_to_unavailable() -> None:
    """错误体解析：非 JSON 响应按不可达处理（不抛解析错误）。"""
    failing = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda _request: httpx.Response(500, text="oops")),
    )
    with pytest.raises(ReplicaUnavailableError):
        await failing.fetch(_fetch_request())
    await failing.aclose()


async def test_channel_config_rejects_bad_numbers(monkeypatch: pytest.MonkeyPatch) -> None:
    """配置校验：非法整数 / 非正上限都拒。"""
    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_PORT", "8001")
    monkeypatch.setenv("MSG_REPLICATION_NODE_ENDPOINTS", "msg-2=http://msg-2:8001")
    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_TIMEOUT_MS", "abc")
    with pytest.raises(ConfigError, match="取值非法"):
        InternalChannelConfig.from_env(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            business_port=8000,
        )

    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_TIMEOUT_MS", "0")
    with pytest.raises(ConfigError, match="必须为正"):
        InternalChannelConfig.from_env(
            enabled=True,
            node_id="msg-1",
            nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
            business_port=8000,
        )


async def test_token_refresh_and_issue_failure() -> None:
    """令牌：过期后重新签发；签发失败按鉴权错误映射。"""
    issuer = FakeIssuer()
    provider = ServiceTokenProvider(cast("object", issuer))  # type: ignore[arg-type]
    assert await provider.authorization() == "Bearer token-1"
    provider._refresh_at = 0  # pyright: ignore[reportPrivateUsage]
    assert await provider.authorization() == "Bearer token-2"

    class BrokenIssuer:
        async def issue(self, spec: object) -> OAuthToken:
            del spec
            raise RuntimeError("no key")

    transport = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", BrokenIssuer())),  # type: ignore[arg-type]
    )
    with pytest.raises(ReplicaUnavailableError) as exc:
        await transport.fetch(_fetch_request())
    assert exc.value.reason == "auth"
    assert transport._client.fetch_max_bytes == 1024  # pyright: ignore[reportPrivateUsage]
    await transport.aclose()


async def test_transport_without_metrics_and_log_suppression() -> None:
    """无指标装配时错误仍映射；同节点同操作日志 60s 内只记一次。"""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    transport = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=None,
        transport=httpx.MockTransport(handler),
    )
    for _ in range(2):
        with pytest.raises(ReplicaUnavailableError):
            await transport.fetch(_fetch_request())
    await transport.aclose()


async def test_fetch_protocol_version_mismatch() -> None:
    """拉取：响应协议版本未知按不可达处理。"""
    metrics = TransportErrorMetrics()
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "protocol_version": 9,
            "leader_epoch": 1,
            "from_offset": 3,
            "records": [],
            "leader_leo": 3,
            "high_watermark": 3,
            "leader_isr": [],
        }
    )
    transport = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=metrics,
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=dict(body.items()))),
    )
    with pytest.raises(ReplicaUnavailableError) as exc:
        await transport.fetch(_fetch_request())
    assert exc.value.reason == "protocol"
    await transport.aclose()


async def test_control_snapshot_failures_and_protocol() -> None:
    """控制通道：传输失败 / 非 2xx / 协议未知都返回 None。"""
    cases = [
        httpx.MockTransport(lambda _request: (_ for _ in ()).throw(httpx.ConnectError("x"))),
        httpx.MockTransport(lambda _request: httpx.Response(500, text="oops")),
        httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "protocol_version": 9,
                    "leader": "msg-2",
                    "epoch": 1,
                    "leo": 0,
                    "hw": 0,
                    "isr": [],
                    "replicas": [],
                },
            )
        ),
    ]
    for mock in cases:
        transport = HttpLeaderControlTransport(
            config=_config(),
            tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
            transport=mock,
        )
        assert await transport.snapshot(node_id="msg-2", topic_id=9, partition_no=0) is None
        await transport.aclose()


async def test_control_demote_and_apply_errors() -> None:
    """控制通道：demote 失败抛不可达；apply 协议未知抛不可达。"""
    demote_transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda _request: httpx.Response(503, json={"detail": {"reason": "unavailable"}})),
    )
    with pytest.raises(ReplicaUnavailableError):
        await demote_transport.demote(node_id="msg-2", topic_id=9, partition_no=0, to_leader="msg-3", to_epoch=2)
    await demote_transport.aclose()

    apply_transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "protocol_version": 9,
                    "to_leo": 0,
                    "truncated_to": None,
                    "retained_tail": 0,
                    "already_applied": False,
                },
            )
        ),
    )
    with pytest.raises(ReplicaUnavailableError) as exc:
        await apply_transport.apply_switch(
            node_id="msg-2",
            command=SwitchApplyCommand(
                topic_id=9,
                partition_no=0,
                from_leader="msg-2",
                to_leader="msg-3",
                to_epoch=3,
                watermark=None,
                truncate=False,
                grace_ms=0,
            ),
        )
    assert exc.value.reason == "protocol"
    await apply_transport.aclose()


async def test_control_local_apply_switch_fast_path() -> None:
    """本地快路径：apply_switch 直连协调器。"""

    class FakeCoordinator:
        async def apply_switch(self, command: object) -> SwitchApplyResult:
            del command
            return SwitchApplyResult(to_leo=1, truncated_to=None, retained_tail=0, already_applied=False)

    transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda _request: httpx.Response(500)),
    )
    transport.register_local("msg-1", cast("object", FakeCoordinator()))  # type: ignore[arg-type]
    result = await transport.apply_switch(
        node_id="msg-1",
        command=SwitchApplyCommand(
            topic_id=9,
            partition_no=0,
            from_leader="msg-1",
            to_leader="msg-2",
            to_epoch=2,
            watermark=None,
            truncate=False,
            grace_ms=0,
        ),
    )
    assert result.to_leo == 1
    await transport.aclose()


async def test_token_returns_cached_without_issuing() -> None:
    """令牌：命中缓存直接返回（不再签发）。"""
    issuer = FakeIssuer()
    provider = ServiceTokenProvider(cast("object", issuer))  # type: ignore[arg-type]
    provider._token = "cached"  # pyright: ignore[reportPrivateUsage]
    provider._refresh_at = __import__("time").monotonic() + 100  # pyright: ignore[reportPrivateUsage]
    assert await provider.authorization() == "Bearer cached"
    assert issuer.calls == 0


async def test_token_second_waiter_reuses_issued_token() -> None:
    """令牌：并发等待者进入锁后看到已签发令牌即复用（不重复签发）。"""
    import asyncio

    class SlowIssuer:
        def __init__(self) -> None:
            self.calls = 0

        async def issue(self, spec: object) -> OAuthToken:
            del spec
            self.calls += 1
            await asyncio.sleep(0)
            return OAuthToken(access_token="issued", expires_in=60)

    issuer = SlowIssuer()
    provider = ServiceTokenProvider(cast("object", issuer))  # type: ignore[arg-type]
    results = await asyncio.gather(provider.authorization(), provider.authorization())
    assert results == ["Bearer issued", "Bearer issued"]
    assert issuer.calls == 1


async def test_error_detail_non_mapping_json() -> None:
    """错误体：`detail` 非映射时按空明细处理。"""
    failing = HttpReplicaTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        transport=httpx.MockTransport(lambda _request: httpx.Response(500, json={"detail": "boom"})),
    )
    with pytest.raises(ReplicaUnavailableError):
        await failing.fetch(_fetch_request())
    await failing.aclose()


async def test_snapshot_and_apply_protocol_mismatch() -> None:
    """控制通道：快照 / 激活响应协议版本未知都按不可达处理。"""
    snapshot_body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "protocol_version": 9,
            "leader": "msg-2",
            "epoch": 1,
            "leo": 0,
            "hw": 0,
            "isr": [],
            "replicas": [],
            "switch_state": "idle",
            "writable": False,
            "last_known_hw": 0,
            "last_known_isr": [],
            "last_known_at": 0.0,
            "start_offset": 0,
        }
    )
    snapshot_transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=TransportErrorMetrics(),
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=dict(snapshot_body.items()))),
    )
    assert await snapshot_transport.snapshot(node_id="msg-2", topic_id=9, partition_no=0) is None
    await snapshot_transport.aclose()

    apply_body = {
        "protocol_version": 9,
        "to_leo": 0,
        "truncated_to": None,
        "retained_tail": 0,
        "already_applied": False,
    }
    apply_transport = HttpLeaderControlTransport(
        config=_config(),
        tokens=ServiceTokenProvider(cast("object", FakeIssuer())),  # type: ignore[arg-type]
        metrics=TransportErrorMetrics(),
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=apply_body)),
    )
    with pytest.raises(ReplicaUnavailableError):
        await apply_transport.apply_switch(
            node_id="msg-2",
            command=SwitchApplyCommand(
                topic_id=9,
                partition_no=0,
                from_leader="msg-2",
                to_leader="msg-3",
                to_epoch=3,
                watermark=None,
                truncate=False,
                grace_ms=0,
            ),
        )
    await apply_transport.aclose()


def test_internal_channel_multi_instance_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """配置：多实例端口 / 端点齐备即通过校验。"""
    monkeypatch.setenv("MSG_REPLICATION_INTERNAL_PORT", "8001")
    monkeypatch.setenv("MSG_REPLICATION_NODE_ENDPOINTS", "msg-2=http://msg-2:8001")
    config = InternalChannelConfig.from_env(
        enabled=True,
        node_id="msg-1",
        nodes=ConcurrentStableFrozenList(("msg-1", "msg-2")),
        business_port=8000,
    )
    assert config.port == 8001
    assert config.endpoints["msg-2"] == "http://msg-2:8001"
