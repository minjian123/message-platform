"""主题 ACL 执行点（Kiwi 2456）：数据面 / 管理面 / 重放 / 顺序与三处易漏点。

装配用 `tests_support.frame_app.build_frame_app`（真实模块路由 + 内存库），只把
`get_topic_acl_service` 换成可控假守卫，验证「ACL 拒绝 / 放行」对调用链的影响。
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from typing import Any, cast

import pytest
from bms_core.api.base import mount_service_routers
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.exceptions import ResourceAccessDeniedError
from bms_core.core.plugin import default_plugin_registry, register_plugin
from bms_core.msgclient.frame import FrameType, decode_frames
from bms_core.permission.base import BasePermissionChecker
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.acl.keys import ACL_ACTION_DESCRIBE, ACL_ACTION_MANAGE, ACL_ACTION_PUBLISH, ACL_ACTION_SUBSCRIBE
from bms_msg.api.admin import replay as admin_replay
from bms_msg.api.admin.delayed import _filter_rows as filter_delayed
from bms_msg.api.admin.groups import _filter_rows as filter_groups
from bms_msg.api.deps import get_topic_acl_service
from bms_msg.broker.replay import ReplayAuditor, ReplayRunner
from bms_msg.tenant.context import CurrentTenant
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import frame_bytes, post_frame
from tests_support.frame_client import header as frame_header

pytestmark = pytest.mark.kiwi_id(2456)

NAMESPACE = "msg"
TOPIC = "audit.operation"
BASE_URL = "http://test"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}


class _DenyChecker(BasePermissionChecker):
    """RBAC 桩：一律拒绝。"""

    def check(self, code: str) -> bool:
        """恒定拒绝。"""
        del code
        return False


class _FakeAcl:
    """可控 ACL 守卫：记录调用、按动作拒绝、按名单过滤。"""

    def __init__(self) -> None:
        self.calls: ConcurrentStableList[tuple[str, str]] = ConcurrentStableList()
        self.deny: ConcurrentStableSet[str] = ConcurrentStableSet()
        self.allowed_names: ConcurrentStableList[str] | None = None

    async def authorize(self, *, tenant_id: int, namespace: str, topic: str, action: str) -> None:
        """记录调用并按 `deny` 集合拒绝。"""
        del tenant_id
        self.calls.add((topic, action))
        if action in self.deny:
            raise ResourceAccessDeniedError("denied")

    async def filter_allowed(
        self,
        *,
        tenant_id: int,
        namespace: str,
        topics: ConcurrentStableList[str],
        action: str,
    ) -> ConcurrentStableList[str]:
        """记录过滤并返回配置的名单（None = 全放行）。"""
        del tenant_id, namespace
        self.calls.add((topics[0] if topics else "", action))
        if self.allowed_names is not None:
            return ConcurrentStableList(name for name in topics if name in self.allowed_names)
        return topics


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """隔离插件注册表并导入占位实现。"""
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品错误码声明（350027 / 350001 等）。"""
    from pathlib import Path

    from bms_core.registry.products import load_product_manifest

    manifest_path = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
    reset_error_code_records()
    manifest = load_product_manifest(manifest_path)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def fake_acl() -> _FakeAcl:
    """可控 ACL 守卫。"""
    return _FakeAcl()


@pytest.fixture
def app(db_session: AsyncSession, tenant_source: object, fake_acl: _FakeAcl) -> FastAPI:
    """最小应用：数据面 + 主题管理 + 重放路由；ACL 守卫换假件。"""
    built = build_frame_app(session=db_session, tenant_source=tenant_source, mount_frame=True)
    built.state.replay_runner = ReplayRunner(
        messages=built.state.message_store,
        uow_factory=built.state.uow_factory,
        auditor=ReplayAuditor(sink=built.state.audit_sink),
        interval_ms=10_000,
    )
    built.include_router(mount_service_routers(ConcurrentStableList([admin_replay.router])))
    built.dependency_overrides[get_topic_acl_service] = lambda: fake_acl
    return built


@pytest.fixture
async def client(app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。"""
    return AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL)


def _body(response: Response) -> ConcurrentStableDict[str, object]:
    """解析统一响应体。"""
    return ConcurrentStableDict(cast("dict[str, object]", json.loads(response.text)))


def _data(response: Response) -> ConcurrentStableDict[str, object]:
    """取响应 `data` 段。"""
    return ConcurrentStableDict(cast("dict[str, object]", _body(response)["data"]))


def _code(response: Response) -> int:
    """取响应业务码（JSON 数字）。"""
    return int(cast("int", _body(response)["code"]))


async def _create_topic(client: AsyncClient, *, name: str = TOPIC, tenant: str = "demo") -> Response:
    """经管理面创建主题。"""
    return await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": 1},
        headers={"X-Tenant-ID": tenant},
    )


async def _publish(client: AsyncClient, *, tenant: str = "demo") -> Response:
    """发布一条消息。"""
    return await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC, "payload": "eA=="},
        headers={"X-Tenant-ID": tenant},
    )


async def test_publish_denied_before_quota(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """publish ACL 拒绝 → 350027 / 403，且在配额 / 主题解析之前。"""
    fake_acl.deny.add(ACL_ACTION_PUBLISH)
    response = await _publish(client)
    assert (response.status_code, _code(response)) == (403, 350027)
    assert ("audit.operation", ACL_ACTION_PUBLISH) in fake_acl.calls


async def test_consume_ack_nack_join_denied(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """consume / ack / nack / join 的 subscribe ACL 拒绝。"""
    fake_acl.deny.add(ACL_ACTION_SUBSCRIBE)
    consume = await client.post(
        "/api/v1/msg/consume",
        json={"namespace": NAMESPACE, "topic": TOPIC, "partition": 0, "group": "g", "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )
    join = await client.post(
        "/api/v1/msg/join",
        json={"namespace": NAMESPACE, "topic": TOPIC, "group": "g", "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )
    ack = await client.post(
        "/api/v1/msg/ack",
        json={
            "namespace": NAMESPACE,
            "topic": TOPIC,
            "partition": 0,
            "group": "g",
            "consumer_id": "c1",
            "offset": 0,
        },
        headers=DEMO_HEADERS,
    )
    nack = await client.post(
        "/api/v1/msg/nack",
        json={
            "namespace": NAMESPACE,
            "topic": TOPIC,
            "partition": 0,
            "group": "g",
            "consumer_id": "c1",
            "offset": 0,
            "reason": "processing_failed",
        },
        headers=DEMO_HEADERS,
    )
    for response in (consume, join, ack, nack):
        assert (response.status_code, _code(response)) == (403, 350027)


async def test_heartbeat_and_leave_do_not_call_acl(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """heartbeat / leave 只走 RBAC，不调用 ACL（撤销订阅后仍可退出）。"""
    fake_acl.deny.add(ACL_ACTION_SUBSCRIBE)
    heartbeat = await client.post(
        "/api/v1/msg/heartbeat",
        json={"namespace": NAMESPACE, "topic": TOPIC, "group": "g", "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )
    leave = await client.post(
        "/api/v1/msg/leave",
        json={"namespace": NAMESPACE, "topic": TOPIC, "group": "g", "consumer_id": "c1"},
        headers=DEMO_HEADERS,
    )
    assert heartbeat.status_code not in (403,)
    assert leave.status_code not in (403,)
    assert list(fake_acl.calls) == []


async def test_create_topic_denied_is_before_ownership(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """管理面 create=manage：ACL 拒绝先于主题存在性（不返回 350001）。"""
    fake_acl.deny.add(ACL_ACTION_MANAGE)
    response = await _create_topic(client, name="not.exists")
    assert (response.status_code, _code(response)) == (403, 350027)


async def test_topic_list_filters_and_detail_denied(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """列表按 describe 过滤；详情 describe 拒绝 403。"""
    created = await _create_topic(client, name="audit.operation")
    await _create_topic(client, name="audit.hidden")
    assert created.status_code == 200
    fake_acl.allowed_names = ConcurrentStableList(["audit.operation"])
    listed = await client.get("/api/v1/msg/admin/topics", headers=DEMO_HEADERS)
    names = [
        item["name"]
        for item in cast(
            "ConcurrentStableList[ConcurrentStableDict[str, object]]",
            _data(listed)["items"],
        )
    ]
    assert names == ["audit.operation"]

    topic_id = _data(created)["id"]
    fake_acl.deny.add(ACL_ACTION_DESCRIBE)
    detail = await client.get(f"/api/v1/msg/admin/topics/{topic_id}", headers=DEMO_HEADERS)
    assert (detail.status_code, _code(detail)) == (403, 350027)


async def test_cross_tenant_subscribe_rejected(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """ACL 放行也不跨租户：other 订阅 demo 的主题 → 350001。"""
    fake_acl.deny.clear()
    assert (await _create_topic(client, tenant="demo")).status_code == 200
    response = await _publish(client, tenant="other")
    assert (response.status_code, _code(response)) == (404, 350001)


async def test_replay_denied_before_task(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """重放任意主题：manage ACL 拒绝 → 403，且不落任务。"""
    fake_acl.deny.add(ACL_ACTION_MANAGE)
    response = await client.post(
        "/api/v1/msg/admin/replay",
        json={"namespace": NAMESPACE, "topic": "any.topic", "partitions": [0]},
        headers=DEMO_HEADERS,
    )
    assert (response.status_code, _code(response)) == (403, 350027)


async def test_rbac_rejection_skips_acl(client: AsyncClient, app: FastAPI, fake_acl: _FakeAcl) -> None:
    """RBAC 拒绝时 ACL 零调用（顺序不可绕过）。"""
    register_plugin("permission", "deny", _DenyChecker)
    app.state.settings.permission.provider = "deny"
    response = await _publish(client)
    assert (response.status_code, _code(response)) == (403, 30001)
    assert list(fake_acl.calls) == []


class _DenyAllAcl:
    """过滤替身：所有主题都拒绝。"""

    async def filter_allowed(self, **_kwargs: object) -> ConcurrentStableList[str]:
        """返回空名单。"""
        return ConcurrentStableList()


async def test_admin_list_filter_helpers_skip_denied(current_tenant: CurrentTenant) -> None:
    """组 / 延迟列表过滤：ACL 拒绝的主题不返回。"""
    from types import SimpleNamespace

    group_row = SimpleNamespace(namespace=NAMESPACE, topic=TOPIC)
    delay_row = SimpleNamespace(record=SimpleNamespace(namespace=NAMESPACE, topic=TOPIC))
    groups = await filter_groups(
        cast("Any", _DenyAllAcl()),
        current_tenant,
        ConcurrentStableList([group_row]),  # type: ignore[arg-type]
    )
    delayed = await filter_delayed(
        cast("Any", _DenyAllAcl()),
        current_tenant,
        ConcurrentStableList([delay_row]),  # type: ignore[arg-type]
    )
    assert list(groups) == []
    assert list(delayed) == []


async def test_frame_publish_and_subscribe_denied(client: AsyncClient, fake_acl: _FakeAcl) -> None:
    """帧路径 ACL 拒绝：返回 error 帧（82013），不落到 JSON 4xx。"""
    fake_acl.deny.update({ACL_ACTION_PUBLISH, ACL_ACTION_SUBSCRIBE})
    publish_header = frame_header(extra={"namespace": NAMESPACE, "topic": TOPIC})
    published = await post_frame(client, "/msg/publish", frame_bytes(int(FrameType.PUBLISH), publish_header, b"x"))
    assert decode_frames(published.content).frames[0].header["code"] == 82013

    subscribe_header = frame_header(
        extra={
            "namespace": NAMESPACE,
            "topic": TOPIC,
            "group": "g",
            "consumer_id": "c1",
            "partition": 0,
        }
    )
    subscribed = await post_frame(client, "/msg/subscribe", frame_bytes(int(FrameType.SUBSCRIBE), subscribe_header))
    assert decode_frames(subscribed.content).frames[0].header["code"] == 82013


async def test_stream_frames_encodes_error_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """帧流编码器：ERROR 事件 → ERROR 帧（覆盖错误分支）。"""
    from bms_msg.api.frame import routes as frame_routes
    from bms_msg.schemas.transport import error_event

    class _Core:
        def __init__(self) -> None:
            self._done = False

        def __aiter__(self) -> _Core:
            return self

        async def __anext__(self) -> object:
            if self._done:
                raise StopAsyncIteration
            self._done = True
            return error_event(350013, "member_lost")

        async def aclose(self) -> None:
            return None

    def _core(**_kwargs: object) -> _Core:
        return _Core()

    monkeypatch.setattr(frame_routes, "_stream_core", _core)
    frames = [
        frame
        async for frame in frame_routes.stream_frames(
            request_id="r1",
            uow_factory=cast("Any", None),
            registry=cast("Any", None),
            connections=cast("Any", None),
            store=cast("Any", None),
            offsets=cast("Any", None),
            dead_letters=cast("Any", None),
            state=cast("Any", None),
            groups=cast("Any", None),
            tenant=cast("Any", None),
            query=cast("Any", None),
            tuning=cast("Any", None),
            connection_key="demo|peer",
            waiter=cast("Any", None),
            backpressure=cast("Any", None),
            metrics=cast("Any", None),
            cache=cast("Any", None),
        )
    ]
    assert decode_frames(frames[0]).frames[0].header["code"] == 350013
