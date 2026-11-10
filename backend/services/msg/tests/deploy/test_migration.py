"""切换开关与分域迁移编排（Kiwi 2477；任务 07-4-1 §12）。

覆盖策略：

- **编排与判据走真实对象**：前置清单 / 矩阵用仓内真实 TOML；演练用真 msg 应用（真路由 → 真帧 → 真数据面）
  + bms 适配层 + 真实业务函数（`fetch_catalog_snapshot`），判据只有一份（`ops.migration.run_drill`）。
- **失败分支走真实损坏**：改清单字段、删域、写未登记指标、把环境写成生产、让发布指向 503，
  脚本按真实读取 / 调用判失败。
- **网络实现也跑真应用**：`HttpTargets` 用 `ASGITransport` + `AsgiRpcTransport` 驱动同一个 app，
  与生产只差「字节往哪送」，不另写一套判据。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import inspect
import json
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from bms_core.catalog.base import fetch_catalog_snapshot
from bms_core.core import assembly
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.config import Settings
from bms_core.core.exceptions import ConfigError
from bms_core.core.plugin import PluginError, default_plugin_registry, resolve_plugin
from bms_core.core.resources import ResourceManager
from bms_core.events.base import EventEnvelope, EventPublishError
from bms_core.events.topic import encode_event, event_attributes, topic_for
from bms_core.fallback.null import NullFallbackPolicy
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher
from bms_core.msgclient.adapter.policy import default_circuit
from bms_core.msgclient.retry import RetryPolicy
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.msgclient.values import RpcCall
from bms_core.ratelimit.null import NullRateLimiter
from bms_core.servicecall.base import BaseServiceClient, ServiceCallPolicy, ServiceRequest
from bms_core.servicecall.http import HttpServiceClient
from fastapi import FastAPI, Response
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from bms_msg.rpc.registry import RpcRegistrationError
from ops import migration
from tests_support.bms_port_contract import (
    BASE_URL,
    EVENT_NAMESPACE,
    EVENT_TYPE,
    SNAPSHOT_PATH,
    TARGET_METHOD,
    TARGET_SERVICE,
    TrackingHandler,
    msg_client,
    msg_event_publisher,
    snapshot_body,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_transport import AsgiRpcTransport, RpcHttpRequest, RpcHttpResponse

pytestmark = pytest.mark.kiwi_id(2477)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]
"""夹具经 import 暴露给 pytest（`tests_support` 是辅助包，不是本文件的实现依赖）。"""

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_PRECONDITIONS = _REPO / "deploy" / "migration" / "preconditions.toml"
_MATRIX = _REPO / "deploy" / "migration" / "switch_matrix.toml"
_DESIGN = (
    _REPO
    / "msg文档"
    / "项目"
    / "08B_消息平台"
    / "任务"
    / "07_部署迁移与验收"
    / "07_部署迁移与验收_04_bms迁移与回滚"
    / "07_部署迁移与验收_04_bms迁移与回滚_01_切换开关与分域迁移编排"
    / "设计"
    / "01_详细设计_07-4-1_切换开关与分域迁移编排.md"
)

_NOW = datetime(2026, 11, 10, 8, 0, 0, tzinfo=UTC)
"""固定时刻（记录逐字节可比）。"""


class _StepClock:
    """每次调用推进 5ms 的假时钟（让 `elapsed_ms` 可断言且不真的等待）。"""

    def __init__(self) -> None:
        """初始化。"""
        self._value = 0.0

    def __call__(self) -> float:
        """返回当前值并推进。"""
        current = self._value
        self._value += 0.005
        return current


class _FailOnceTransport:
    """首跳抛传输错误、次跳委托真 ASGI 传输（在途重试的注入面）。"""

    def __init__(self, inner: AsgiRpcTransport) -> None:
        """初始化。

        Args:
            inner: 真 ASGI 传输。
        """
        self._inner = inner
        self._remaining = 1
        self.keys: ConcurrentStableList[str] = ConcurrentStableList()

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """记录幂等键；首跳失败，次跳委托。

        Args:
            request: 传输层入参。

        Returns:
            RpcHttpResponse: 响应。

        Raises:
            RpcTransportError: 首跳注入失败。
        """
        from bms_core.msgclient.frame import decode_frame
        from bms_core.msgclient.transport import RpcTransportError

        self.keys.add(decode_frame(request.body).header.get("idempotency_key") or "")
        if self._remaining:
            self._remaining -= 1
            raise RpcTransportError("首跳失败（注入）")
        return await self._inner.send(request)

    def open_stream(self, request: RpcHttpRequest) -> Any:
        """流式面直接委托（本用例不用流）。"""
        return self._inner.open_stream(request)

    async def aclose(self) -> None:
        """释放内部传输。"""
        await self._inner.aclose()

    def stats(self) -> Any:
        """统计委托。"""
        return self._inner.stats()


def _settings(
    *,
    event: str = "msg",
    client: str = "msg",
    namespace: str = EVENT_NAMESPACE,
    service: str = "msg",
    base_url: str = BASE_URL,
) -> Settings:
    """构造一份可控配置（只改开关与实例地址）。

    Args:
        event: `[event].provider`。
        client: `[service_client].provider`。
        namespace: 事件命名空间。
        service: `[app].service`。
        base_url: msg 实例基址。

    Returns:
        Settings: 应用配置。
    """
    settings = Settings()
    settings.app.service = service
    settings.event.provider = event
    settings.event.options.set("namespace", namespace)
    settings.service_client.provider = client
    settings.service_client.instances.set("msg", ConcurrentStableList([base_url]))
    return settings


@pytest.fixture
def switch_settings() -> Iterator[Settings]:
    """登记平台插件后的配置（`resolve_plugin` 能解析 event / service_client）。

    Yields:
        Settings: 已装配的配置。
    """
    registry = default_plugin_registry()
    registry.reset()
    settings = _settings()
    assembly.register_platform_plugins(settings, FastAPI(), ResourceManager())
    try:
        yield settings
    finally:
        registry.reset()


_write_seq = 0
"""非幂等写方法序号（同一 app 上多次演练不撞名）。"""


def _register_method(app: FastAPI, method: str, handler: TrackingHandler, *, idempotent: bool) -> None:
    """登记目标方法（重复登记视为幂等：同一 app 上多次演练共用首个处理器）。

    Args:
        app: 真 msg 应用。
        method: 方法名。
        handler: 处理器。
        idempotent: 是否幂等。

    Raises:
        RpcRegistrationError: 同方法被不同处理器占用（用例里的真实配置错误）。
    """
    with suppress(RpcRegistrationError):
        # 同一 app 上重复演练：处理器已存在即复用，不因「再登记一次」判失败。
        app.state.rpc_registry.register(TARGET_SERVICE, method, handler, idempotent=idempotent)


class _InProcessTargets:
    """进程内演练目标：真 msg 应用 + ASGI 传输 + 真业务函数。"""

    def __init__(
        self,
        *,
        app: FastAPI,
        settings: Settings,
        fallback: bool = True,
        event_type: str = EVENT_TYPE,
    ) -> None:
        """初始化。

        Args:
            app: 真 msg 应用。
            settings: 已装配配置（切换判定用）。
            fallback: 是否提供原链路只读调用。
            event_type: 本次演练的事件类型（每次演练一个新主题，避免状态串场）。
        """
        self._app = app
        self._settings = settings
        self._event_type = event_type
        self._topic = topic_for(event_type)
        self._http = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        self._publisher = msg_event_publisher(app, tenant="demo")
        self._unavailable = MsgEventPublisher(
            base_url=BASE_URL,
            namespace=EVENT_NAMESPACE,
            tenant="demo",
            transport=httpx.MockTransport(lambda request: httpx.Response(503)),
        )
        self._fallback = fallback
        self._closed = False
        self._read_only_handler = TrackingHandler()
        _register_method(app, TARGET_METHOD, self._read_only_handler, idempotent=True)

    async def prepare_event(self) -> None:
        """建主题并加入消费组。"""
        response = await self._http.post(
            "/api/v1/msg/admin/topics",
            json={"namespace": EVENT_NAMESPACE, "name": self._topic, "partition_count": 1},
            headers={"X-Tenant-ID": "demo"},
        )
        assert response.status_code in (200, 409), response.text
        joined = await self._http.post(
            "/api/v1/msg/join",
            json={
                "group": migration.MIGRATION_DEFAULT_GROUP,
                "namespace": EVENT_NAMESPACE,
                "topic": self._topic,
                "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
            },
            headers={"X-Tenant-ID": "demo"},
        )
        assert joined.status_code == 200, joined.text

    async def publish(self, event: EventEnvelope) -> None:
        """经真数据面发布。"""
        await self._publisher.publish(event)

    async def publish_unavailable(self, event: EventEnvelope) -> None:
        """经 503 替身发布（必须抛 `EventPublishError`）。"""
        await self._unavailable.publish(event)

    async def consume_events(self) -> ConcurrentStableFrozenList[migration.DrillMessage]:
        """拉取并确认当前批。"""
        response = await self._http.post(
            "/api/v1/msg/consume",
            json={
                "group": migration.MIGRATION_DEFAULT_GROUP,
                "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
                "namespace": EVENT_NAMESPACE,
                "topic": self._topic,
                "partition": 0,
                "max_messages": 100,
                "max_wait_ms": 0,
            },
            headers={"X-Tenant-ID": "demo"},
        )
        assert response.status_code == 200, response.text
        body = cast("dict[str, object]", response.json())
        data = cast("dict[str, object]", body["data"])
        raw_items = cast("list[dict[str, object]]", data["messages"])
        messages: ConcurrentStableList[migration.DrillMessage] = ConcurrentStableList()
        for item in raw_items:
            offset = int(cast("int", item["offset"]))
            messages.add(
                migration.DrillMessage(
                    message_id=str(item["message_id"]),
                    offset=offset,
                    payload=base64.b64decode(cast("str", item["payload"])),
                    headers=ConcurrentStableFrozenDict(cast("dict[str, str]", item["headers"])),
                    key=cast("str | None", item["key"]),
                )
            )
            acked = await self._http.post(
                "/api/v1/msg/ack",
                json={
                    "group": migration.MIGRATION_DEFAULT_GROUP,
                    "consumer_id": migration.MIGRATION_DEFAULT_CONSUMER_ID,
                    "namespace": EVENT_NAMESPACE,
                    "topic": self._topic,
                    "partition": 0,
                    "offset": offset,
                },
                headers={"X-Tenant-ID": "demo"},
            )
            assert acked.status_code == 200, acked.text
        return ConcurrentStableFrozenList(messages)

    async def read_only(self) -> migration.ReadOnlyResult:
        """只读调用：真实业务函数 `fetch_catalog_snapshot`。"""
        client = msg_client(AsgiRpcTransport(self._app))
        try:
            records = await fetch_catalog_snapshot(client)
        finally:
            await client.aclose()
        return migration.ReadOnlyResult(
            target=f"{TARGET_SERVICE}.{TARGET_METHOD}",
            digest=migration.digest_json([asdict(record) for record in records]),
            count=len(records),
        )

    async def fallback_read_only(self) -> migration.ReadOnlyResult | None:
        """原链路只读调用：真 `HttpServiceClient` + 同一份响应体。"""
        if not self._fallback:
            return None
        client = HttpServiceClient(
            circuit_breaker=default_circuit(),
            fallback_policy=NullFallbackPolicy(),
            rate_limiter=NullRateLimiter(),
            base_url_template="http://{service}:8000",
            caller="platform",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, content=snapshot_body())),
        )
        try:
            records = await fetch_catalog_snapshot(client)
        finally:
            await client.aclose()
        return migration.ReadOnlyResult(
            target="http:snapshot",
            digest=migration.digest_json([asdict(record) for record in records]),
            count=len(records),
        )

    async def write_attempt(self, key: str) -> migration.WriteAttemptResult | None:
        """非幂等写调用：首跳失败、次跳成功，服务端按幂等键只执行一次。"""
        global _write_seq
        handler = TrackingHandler()
        method = f"modules.create_{_write_seq}"
        _write_seq += 1
        _register_method(self._app, method, handler, idempotent=False)
        transport = _FailOnceTransport(AsgiRpcTransport(self._app))
        client = RpcClient(
            StaticInstanceSelector([BASE_URL]),
            tenant="demo",
            transport=transport,
            circuit=default_circuit(),
        )
        try:
            reply = await client.call(
                TARGET_SERVICE,
                method,
                b"{}",
                idempotency_key=key,
                policy=RetryPolicy(max_retries=1, base_ms=0, cap_ms=0),
            )
        finally:
            await client.aclose()
        return migration.WriteAttemptResult(
            executions=len(handler.calls),
            keys=ConcurrentStableFrozenList(transport.keys),
            digest=migration.digest_bytes(reply.payload),
        )

    def switch(self) -> migration.SwitchReport:
        """切换判定（走真实插件解析）。"""
        return migration.resolve_switch(self._settings)

    async def aclose(self) -> None:
        """释放连接（幂等）。"""
        if self._closed:
            return
        self._closed = True
        await self._http.aclose()
        await self._publisher.aclose()
        await self._unavailable.aclose()


_run_seq = 0
"""演练序号（每次演练用独立主题，避免同一 app 上状态串场）。"""


async def _drill(*, msg_app: FastAPI, switch_settings: Settings, fallback: bool = True) -> migration.DrillReport:
    """跑一次完整演练（所有编排用例共用）。

    Args:
        msg_app: 真 msg 应用。
        switch_settings: 已装配配置。
        fallback: 是否提供 fallback。

    Returns:
        migration.DrillReport: 演练报告。
    """
    global _run_seq
    _run_seq += 1
    event_type = f"{EVENT_TYPE}_{_run_seq}"
    matrix, matrix_errors = migration.load_matrix(_MATRIX)
    preconditions, precondition_errors = migration.load_preconditions(_PRECONDITIONS)
    assert not matrix_errors
    assert not precondition_errors
    targets = _InProcessTargets(app=msg_app, settings=switch_settings, fallback=fallback, event_type=event_type)
    return await migration.run_drill(
        matrix=matrix,
        preconditions=preconditions,
        targets=targets,
        env="test",
        event_type=event_type,
        now=lambda: _NOW,
        clock=_StepClock(),
    )


# ---------------------------------------------------------------------------
# M1 前置清单
# ---------------------------------------------------------------------------


def test_preconditions_parse_and_validate() -> None:
    """真实清单可解析、字段齐备；非法状态与空证据会被指名。"""
    preconditions, errors = migration.load_preconditions(_PRECONDITIONS)
    assert list(errors) == []
    assert migration.validate_preconditions(preconditions) == ()
    assert len(preconditions.items) == 8

    first = preconditions.items[0]
    broken = replace(
        preconditions,
        items=ConcurrentStableFrozenList(
            [
                replace(first, status="done", evidence=""),
                replace(first, required_in=ConcurrentStableFrozenList(["staging"])),
                replace(first, label=""),
            ]
        ),
    )
    messages = "；".join(migration.validate_preconditions(broken))
    assert "status = 'done'" in messages
    assert "evidence 为空" in messages
    assert "required_in 含未登记环境" in messages
    assert "缺少 label" in messages


def test_preconditions_missing_file(tmp_path: Path) -> None:
    """清单缺失按失败处理，不冒充「全绿」。"""
    preconditions, errors = migration.load_preconditions(tmp_path / "absent.toml")
    assert list(errors)
    assert preconditions.items == ()


# ---------------------------------------------------------------------------
# M2 生产闸门
# ---------------------------------------------------------------------------


def test_gate_blocks_production_but_allows_test() -> None:
    """生产闸门拒绝 8B_05；07-4-2 交付后 `rollback_drill` 已满足，隔离环境只记录阻断项。"""
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    prod_code, prod_lines = migration.run_check(preconditions_path=_PRECONDITIONS, matrix_path=_MATRIX, env="prod")
    assert prod_code == 1
    text = "\n".join(prod_lines)
    assert "copy_replication" in text
    assert "rollback_drill" not in text

    test_code, test_lines = migration.run_check(preconditions_path=_PRECONDITIONS, matrix_path=_MATRIX, env="test")
    assert test_code == 0
    assert "阻断" in "\n".join(test_lines)
    assert migration.evaluate_gate(preconditions, "test").allowed is True


def test_check_rejects_unknown_env_and_missing_matrix(tmp_path: Path) -> None:
    """未登记环境按参数错误；矩阵缺失按校验失败。"""
    assert migration.main(["check", "--env", "staging"]) == 2
    code, lines = migration.run_check(
        preconditions_path=_PRECONDITIONS, matrix_path=tmp_path / "absent.toml", env="test"
    )
    assert code == 1
    assert "切换矩阵不存在" in "\n".join(lines)


def test_check_fails_on_broken_preconditions(tmp_path: Path) -> None:
    """清单字段非法时 `check` 失败并列出明细。"""
    broken = tmp_path / "preconditions.toml"
    broken.write_text(
        """
schema_version = 1
[meta]
updated_at = "2026-11-10"
owner = "tester"

[[items]]
id = "x"
label = "x"
status = "done"
required_in = ["prod"]
evidence = ""
""".strip(),
        encoding="utf-8",
    )
    code, lines = migration.run_check(preconditions_path=broken, matrix_path=_MATRIX, env="test")
    assert code == 1
    assert "status = 'done'" in "\n".join(lines)


# ---------------------------------------------------------------------------
# M3 / M4 矩阵
# ---------------------------------------------------------------------------


def test_matrix_covers_ports_and_fields() -> None:
    """真实矩阵五域、两类端口、批次 1 双端口、指标已登记。"""
    matrix, errors = migration.load_matrix(_MATRIX)
    assert list(errors) == []
    assert migration.validate_matrix(matrix) == ()
    assert {domain.port for domain in matrix.domains} == {"event", "service_client"}
    phase_one = {domain.port for domain in matrix.domains if domain.phase == 1}
    assert phase_one == {"event", "service_client"}


def test_matrix_rejects_bad_fields() -> None:
    """缺端口 / 未登记指标 / 非法域级键 / 空判据都会被指名。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    first = matrix.domains[0]
    broken_domain = replace(
        first,
        switch_key="[wrong].provider",
        domain_level_key="[event].domains.low-risk",
        observe_metrics=ConcurrentStableFrozenList(["msg_not_registered"]),
        rollback_criteria=ConcurrentStableFrozenList([""]),
        prerequisites=ConcurrentStableFrozenList(["absent"]),
        target_value="http",
        default_value="",
        owner="",
        category="readonly",
    )
    broken = replace(
        matrix,
        domains=ConcurrentStableFrozenList([broken_domain]),
        phases=ConcurrentStableFrozenList([replace(matrix.phases[0], id=9)]),
    )
    messages = "；".join(migration.validate_matrix(broken, known_preconditions=ConcurrentStableFrozenList(["x"])))
    assert "switch_key" in messages
    assert "domain_level_key" in messages
    assert "指标未登记" in messages
    assert "rollback_criteria 含空判据" in messages
    assert "未登记前置" in messages
    assert "target_value" in messages
    assert "default_value" in messages
    assert "owner 为空" in messages
    assert "category = 'readonly' 与 port = 'event' 不匹配" in messages
    assert "phase = 1 未在 [[phases]] 中声明" in messages
    assert "矩阵未覆盖端口 'service_client'" in messages


def test_matrix_rows_match_design_document() -> None:
    """设计文档的表与 TOML 渲染逐行一致（单源不漂移）。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    rendered = list(migration.render_matrix_rows(matrix))
    text = _DESIGN.read_text(encoding="utf-8")
    block = text.split("<!-- matrix-table:begin -->")[1].split("<!-- matrix-table:end -->")[0]
    documented = [line.strip() for line in block.strip().splitlines() if line.strip()]
    assert documented == rendered
    assert migration.main(["matrix"]) == 0


# ---------------------------------------------------------------------------
# M5 ~ M8 开关契约
# ---------------------------------------------------------------------------


def test_default_and_original_providers(switch_settings: Settings) -> None:
    """缺省空串解析到 null（原链路占位）；`http` 解析到原实现。"""
    settings = switch_settings.model_copy(deep=True)
    settings.event.provider = ""
    settings.service_client.provider = "http"
    report = migration.resolve_switch(settings)
    assert report.event.implementation == "null"
    assert report.event.mode == "original"
    assert report.service_client.implementation == "http"
    assert report.service_client.mode == "original"
    assert report.fingerprint
    assert migration.validate_switch(report)  # 未切 msg，演练前提不满足


def test_msg_providers_resolve_to_adapter(switch_settings: Settings) -> None:
    """`provider = "msg"` 解析出适配层实现，演练前提成立。"""
    report = migration.resolve_switch(switch_settings)
    assert report.event.implementation == "msg"
    assert report.service_client.implementation == "msg"
    assert report.event.mode == "msg"
    assert report.service_client.mode == "msg"
    assert migration.validate_switch(report) == ()


def test_unknown_provider_is_rejected(switch_settings: Settings) -> None:
    """未注册实现拒启（不静默回落）。"""
    settings = switch_settings.model_copy(deep=True)
    settings.service_client.provider = "bogus"
    with pytest.raises(PluginError):
        resolve_plugin(
            "service_client",
            settings.service_client.provider,
            expected_version=BaseServiceClient.contract_version,
        )
    report = migration.resolve_switch(settings)
    assert report.service_client.error_kind == "plugin"
    assert "未注册" in report.service_client.error
    assert migration.validate_switch(report)


def test_msg_provider_without_config_is_rejected(switch_settings: Settings) -> None:
    """`provider = "msg"` 但命名空间 / 地址不全：装配期拒启并指名键名。"""
    del switch_settings
    settings = _settings(service="", namespace="")
    settings.event.options.set("namespace", "")
    with pytest.raises(ConfigError) as caught:
        assembly.msg_namespace(settings)
    assert "namespace" in str(caught.value)

    settings = _settings()
    settings.service_client.instances.clear()
    settings.service_client.options.set("base_url_template", "http://msg:8000")
    with pytest.raises(ConfigError) as caught_address:
        assembly.msg_base_url(settings)
    assert "base_url_template" in str(caught_address.value)

    registry = default_plugin_registry()
    registry.reset()
    assembly.register_platform_plugins(settings, FastAPI(), ResourceManager())
    try:
        report = migration.resolve_switch(settings)
    finally:
        registry.reset()
    assert report.event.error_kind == "config"
    assert report.service_client.error_kind == "config"
    assert "base_url_template" in report.service_client.error
    assert migration.validate_switch(report)


# ---------------------------------------------------------------------------
# M9 ~ M11 在途 / 未确认 / 双写
# ---------------------------------------------------------------------------


async def test_inflight_retry_keeps_same_key(msg_app: FastAPI, switch_settings: Settings) -> None:
    """在途：首跳失败重试两次、幂等键相同、服务端只执行一次。"""
    targets = _InProcessTargets(app=msg_app, settings=switch_settings)
    try:
        write = await targets.write_attempt("drill-inflight")
    finally:
        await targets.aclose()
    assert write is not None
    assert write.executions == 1
    assert len(set(write.keys)) == 1
    assert len(write.keys) == 2


async def test_unavailable_publish_raises_and_recovers(msg_app: FastAPI, switch_settings: Settings) -> None:
    """未确认事件：不可达发布必须抛错；恢复后发布一次、消费一次。"""
    targets = _InProcessTargets(app=msg_app, settings=switch_settings)
    try:
        await targets.prepare_event()
        event = _fixed_event(2)
        with pytest.raises(EventPublishError):
            await targets.publish_unavailable(event)
        assert await targets.consume_events() == ()
        await targets.publish(_fixed_event(3))
        delivered = await targets.consume_events()
    finally:
        await targets.aclose()
    assert len(delivered) == 1


async def test_dual_write_single_channel(msg_app: FastAPI, switch_settings: Settings) -> None:
    """双写：两次调用恰好两条消息（端口只解析出一个实现）。"""
    targets = _InProcessTargets(app=msg_app, settings=switch_settings)
    try:
        await targets.prepare_event()
        await targets.publish(_fixed_event(4))
        await targets.publish(_fixed_event(5))
        messages = await targets.consume_events()
        report = targets.switch()
    finally:
        await targets.aclose()
    assert len(messages) == 2
    assert report.event.mode == "msg"
    assert report.service_client.mode == "msg"


def _fixed_event(index: int) -> EventEnvelope:
    """构造固定事件（与 `ops.migration` 同形，用于单元级断言）。

    Args:
        index: 序号。

    Returns:
        EventEnvelope: 事件信封。
    """
    return EventEnvelope(
        event_type=EVENT_TYPE,
        payload=ConcurrentStableDict({"drill_index": index, "run": "07-4-1"}),
        tenant_id="demo",
        trace_id=f"drill-{index}",
        event_id=f"drill-{index:04d}",
        occurred_at=_NOW,
        aggregate_key=f"drill-{index % 4}",
    )


# ---------------------------------------------------------------------------
# M12 事件链 + 只读调用 + fallback
# ---------------------------------------------------------------------------


async def test_full_drill_passes_with_event_chain_and_readonly_call(
    msg_app: FastAPI, switch_settings: Settings
) -> None:
    """完整演练：事件链 / 只读调用 / 在途 / 未确认 / 双写 / fallback 全绿。"""
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings)
    results = {step.step: step.result for step in report.steps}
    assert report.conclusion == "通过"
    assert results == {
        "preconditions": "ok",
        "matrix": "ok",
        "switch_semantics": "ok",
        "event_chain": "ok",
        "readonly_call": "ok",
        "inflight_idempotency": "ok",
        "unconfirmed_event": "ok",
        "dual_write": "ok",
        "fallback": "ok",
        "business_zero_change": "ok",
    }
    assert report.gate.allowed is True
    assert [item for item in report.gate.blocked_by] == ["copy_replication"]
    assert report.counts["published"] == 4
    assert report.counts["consumed"] == 4
    assert report.counts["duplicates"] == 0
    assert report.counts["executions"] == 1
    assert report.reasons == ()


async def test_drill_rerun_same_result(msg_app: FastAPI, switch_settings: Settings) -> None:
    """独立会话再跑一次结论一致（同命令可重复执行，不依赖上一轮残留）。"""
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings)
    assert report.conclusion == "通过"
    assert report.counts["messages"] == 2
    assert [step.step for step in report.steps] == [
        "preconditions",
        "matrix",
        "switch_semantics",
        "event_chain",
        "readonly_call",
        "inflight_idempotency",
        "unconfirmed_event",
        "dual_write",
        "fallback",
        "business_zero_change",
    ]


async def test_drill_without_fallback_is_degraded(msg_app: FastAPI, switch_settings: Settings) -> None:
    """未提供 fallback：结论降级并写明原因（没验回滚可用性不算全绿）。"""
    report = await _drill(msg_app=msg_app, switch_settings=switch_settings, fallback=False)
    assert report.conclusion == "降级通过"
    assert any(step.step == "fallback" and step.result == "degraded" for step in report.steps)
    assert any("fallback" in reason for reason in report.reasons)


async def test_drill_rejects_production(msg_app: FastAPI, switch_settings: Settings) -> None:
    """生产环境禁止演练（无覆盖开关）。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    targets = _InProcessTargets(app=msg_app, settings=switch_settings)
    try:
        with pytest.raises(ValueError, match="生产环境"):
            await migration.run_drill(matrix=matrix, preconditions=preconditions, targets=targets, env="prod")
    finally:
        await targets.aclose()


# ---------------------------------------------------------------------------
# M13 记录与 CLI
# ---------------------------------------------------------------------------


async def test_record_contract_and_repeatability(msg_app: FastAPI, switch_settings: Settings, tmp_path: Path) -> None:
    """记录字段齐备、JSON 与 Markdown 同源；重复执行计数一致。"""
    first = await _drill(msg_app=msg_app, switch_settings=switch_settings)
    assert first.conclusion == "通过"
    assert first.counts["published"] == first.counts["consumed"]
    assert first.counts["duplicates"] == 0
    assert migration.record_dict(first) == migration.record_dict(first)

    json_path, markdown_path = migration.write_record(first, tmp_path / "drill")
    assert json_path.exists()
    assert markdown_path.exists()
    record = cast("dict[str, object]", json.loads(json_path.read_text(encoding="utf-8")))
    assert record["schema_version"] == 1
    assert record["task"] == "07-4-1"
    assert record["mode"] == "drill"
    assert record["counts"] == {"published": 4, "consumed": 4, "executions": 1, "messages": 2, "duplicates": 0}
    conclusion = cast("dict[str, object]", record["conclusion"])
    assert conclusion["result"] == "通过"
    gate = cast("dict[str, object]", record["production_gate"])
    assert gate["allowed"] is True
    assert "copy_replication" in cast("list[str]", gate["blocked_by"])
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "结论：**通过**" in markdown
    assert "`published` | 4" in markdown
    artifacts = cast("dict[str, str]", record["artifacts"])
    assert artifacts["report_json"].endswith("drill.json")


async def test_cli_drill_runs_with_injected_targets(
    msg_app: FastAPI,
    switch_settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI 的 `drill` 走同一份编排（目标工厂可注入，离线可跑）。"""

    def _inproc_factory(settings: Settings, args: argparse.Namespace) -> migration.DrillTargets:
        """返回进程内目标（忽略 CLI 构造的网络参数）。"""
        del settings, args
        return _InProcessTargets(app=msg_app, settings=switch_settings)

    monkeypatch.setattr(migration, "_TARGETS_FACTORY", _inproc_factory)
    report = tmp_path / "cli"
    code = await asyncio.to_thread(
        migration.main,
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--report",
            str(report),
            "--event-provider",
            "msg",
            "--service-client-provider",
            "msg",
        ],
    )
    assert code == 0
    assert report.with_suffix(".json").exists()
    assert report.with_suffix(".md").exists()

    # 前置全绿时不再打印「生产闸门阻断项」（CLI 的另一条分支；用假目标避免复用同一会话）。
    satisfied = _satisfied_preconditions(tmp_path)

    def _fake_factory(settings: Settings, args: argparse.Namespace) -> migration.DrillTargets:
        """返回假目标（不触达消息应用）。"""
        del settings, args
        return _FakeTargets()

    monkeypatch.setattr(migration, "_TARGETS_FACTORY", _fake_factory)
    second = tmp_path / "cli-satisfied"
    code = await asyncio.to_thread(
        migration.main,
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--preconditions",
            str(satisfied),
            "--report",
            str(second),
        ],
    )
    assert code == 0
    assert second.with_suffix(".json").exists()


def test_cli_drill_rejects_production() -> None:
    """`drill --env prod` 参数级拒绝（退出 2）。"""
    assert migration.main(["drill", "--env", "prod", "--msg-url", BASE_URL]) == 2


def test_cli_drill_rejects_unknown_env() -> None:
    """`drill --env staging` 参数级拒绝（退出 2）。"""
    assert migration.main(["drill", "--env", "staging", "--msg-url", BASE_URL]) == 2


def test_cli_drill_rejects_broken_files(tmp_path: Path) -> None:
    """清单 / 矩阵缺失时 `drill` 失败（退出 1），不猜状态。"""
    code = migration.main(
        [
            "drill",
            "--env",
            "test",
            "--msg-url",
            BASE_URL,
            "--preconditions",
            str(tmp_path / "absent.toml"),
        ]
    )
    assert code == 1


# ---------------------------------------------------------------------------
# M14 业务代码零改动
# ---------------------------------------------------------------------------


def test_business_zero_change() -> None:
    """真实业务函数签名与端口方法集不变（切换只换注入实例）。"""
    errors, evidence = migration.business_zero_change()
    assert errors == ()
    assert "client:POSITIONAL_OR_KEYWORD" in evidence["fetch_catalog_snapshot"]
    assert "MsgEventPublisher.publish" in evidence
    assert "MsgServiceClient.call" in evidence


# ---------------------------------------------------------------------------
# 网络实现（同一份判据，只换传输）
# ---------------------------------------------------------------------------


async def test_http_targets_round_trip(msg_app: FastAPI, switch_settings: Settings) -> None:
    """网络演练目标用 ASGI 传输跑通：建主题 / 发布 / 消费 / 只读 / fallback / 关闭幂等。"""
    fallback_app = FastAPI()

    async def _snapshot() -> Response:
        """原链路只读响应。"""
        return Response(content=snapshot_body(), media_type="application/json")

    fallback_app.add_api_route(SNAPSHOT_PATH, _snapshot, methods=["GET"])
    _register_method(msg_app, TARGET_METHOD, TrackingHandler(), idempotent=True)
    unavailable = httpx.MockTransport(lambda request: httpx.Response(503))
    targets = migration.build_http_targets(
        switch_settings,
        base_url=BASE_URL,
        namespace=EVENT_NAMESPACE,
        read_only_service=TARGET_SERVICE,
        read_only_method=TARGET_METHOD,
        http_transport=ASGITransport(app=msg_app),
        unavailable_transport=unavailable,
        fallback_base_url="http://fallback",
        fallback_path=SNAPSHOT_PATH,
        fallback_transport=ASGITransport(app=fallback_app),
        rpc_transport=AsgiRpcTransport(app=msg_app),
    )
    try:
        await targets.prepare_event()
        await targets.publish(_fixed_event(6))
        messages = await targets.consume_events()
        read_only = await targets.read_only()
        fallback = await targets.fallback_read_only()
        write = await targets.write_attempt("ignored")
        report = targets.switch()
        with pytest.raises(EventPublishError):
            await targets.publish_unavailable(_fixed_event(7))
    finally:
        await targets.aclose()
        await targets.aclose()
    assert len(messages) == 1
    assert read_only.target == f"{TARGET_SERVICE}.{TARGET_METHOD}"
    assert read_only.count == 1
    assert fallback is not None
    assert fallback.count == 1
    assert write is None
    assert report.event.mode == "msg"


async def test_http_targets_without_fallback(switch_settings: Settings) -> None:
    """未给原链路地址时 fallback 返回 None（不默认判通过）。"""
    targets = migration.build_http_targets(switch_settings, base_url=BASE_URL, namespace=EVENT_NAMESPACE)
    try:
        assert await targets.fallback_read_only() is None
    finally:
        await targets.aclose()


def test_http_targets_switch_report(switch_settings: Settings) -> None:
    """网络目标的切换判定取真实配置（不另造口径）。"""
    targets = migration.build_http_targets(switch_settings, base_url=BASE_URL, namespace=EVENT_NAMESPACE)
    report = targets.switch()
    assert report.event.implementation == "msg"
    assert report.service_client.implementation == "msg"
    assert report.fingerprint


async def test_http_targets_fail_when_endpoint_down(switch_settings: Settings) -> None:
    """下游不可达时发布与只读调用都上抛（不被演练吞成通过）。"""
    targets = migration.build_http_targets(
        switch_settings,
        base_url="http://127.0.0.1:1",
        namespace=EVENT_NAMESPACE,
        http_transport=httpx.MockTransport(lambda request: httpx.Response(503)),
        unavailable_transport=httpx.MockTransport(lambda request: httpx.Response(503)),
        rpc_transport=None,
    )
    try:
        with pytest.raises(EventPublishError):
            await targets.publish(_fixed_event(8))
        with pytest.raises(httpx.HTTPStatusError):
            await targets.prepare_event()
    finally:
        await targets.aclose()


# ---------------------------------------------------------------------------
# 记录：check 模式的报告（无步骤，但字段齐备）
# ---------------------------------------------------------------------------


def test_record_dict_check_mode() -> None:
    """`check` 模式记录也带全字段（失败 / 通过都能留痕）。"""
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    report = migration.DrillReport(
        mode="check",
        env="test",
        started_at=_NOW,
        finished_at=_NOW,
        switch=ConcurrentStableFrozenDict({"event_provider": "", "service_client_provider": ""}),
        gate=migration.evaluate_gate(preconditions, "test"),
        matrix=ConcurrentStableFrozenDict({"domains": 5, "phases": 3, "metrics_checked": []}),
        steps=ConcurrentStableFrozenList(),
        counts=ConcurrentStableFrozenDict(),
        conclusion=migration.CONCLUSION_PASS,
        reasons=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
        artifacts=ConcurrentStableFrozenDict(),
    )
    record = migration.record_dict(report)
    assert record["mode"] == "check"
    markdown = migration.render_markdown(record)
    assert "# 迁移演练记录（07-4-1）" in markdown
    assert "| `event_provider` |  |" in markdown


async def _noop() -> None:
    """占位协程（保留给未来的异步断言扩展）。"""


def test_module_reexports() -> None:
    """模块出口保持可导入（用例 / CLI / 07-6-2 复用同一入口）。"""
    for name in migration.__all__:
        assert hasattr(migration, name)
    assert inspect.iscoroutinefunction(_noop) is True


@pytest.fixture
async def app_client(msg_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """ASGI 客户端（供需要直接管理主题的扩展用例使用）。

    Args:
        msg_app: 真 msg 应用。

    Yields:
        AsyncClient: ASGI 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=msg_app), base_url="http://test") as client:
        yield client


def test_helpers_exist(app_client: AsyncClient, db_session: AsyncSession) -> None:
    """夹具可用性护栏（避免后续扩展用例误以为夹具缺失）。"""
    del db_session
    assert app_client is not None


def test_rpc_reply_helper_type() -> None:
    """`RpcCall` 与 `ServiceRequest` 的契约面可直接构造（跨仓复用的最小断言）。"""
    assert ServiceRequest(service=TARGET_SERVICE, method="GET", path=SNAPSHOT_PATH).policy.max_retries >= 0
    assert ServiceCallPolicy().timeout > 0
    call = RpcCall(
        service=TARGET_SERVICE,
        method=TARGET_METHOD,
        payload=b"",
        headers=ConcurrentStableDict(),
        tenant="demo",
    )
    assert call.service == TARGET_SERVICE


# ---------------------------------------------------------------------------
# 解析 / 校验的失败分支（清单与矩阵的每一条守卫都要有反例）
# ---------------------------------------------------------------------------


def test_string_list_rejects_non_list(tmp_path: Path) -> None:
    """TOML 取值不是数组时折成空列表（交给校验器报「为空」，不是崩在解析）。"""
    path = tmp_path / "scalar.toml"
    path.write_text(
        'schema_version = 1\n[[items]]\nid = "x"\nlabel = "x"\nstatus = "satisfied"\n'
        'required_in = "prod"\nevidence = "e"\n',
        encoding="utf-8",
    )
    preconditions, errors = migration.load_preconditions(path)
    assert errors == ()
    assert preconditions.items[0].required_in == ()
    assert "required_in 为空" in "；".join(migration.validate_preconditions(preconditions))


def test_preconditions_parse_failures(tmp_path: Path) -> None:
    """坏 TOML / 缺 items / items 非表：都返回错误，不崩。"""
    bad = tmp_path / "bad.toml"
    bad.write_text("schema_version = [", encoding="utf-8")
    preconditions, errors = migration.load_preconditions(bad)
    assert preconditions.schema_version == 0
    assert any("解析失败" in item for item in errors)

    empty = tmp_path / "empty.toml"
    empty.write_text('schema_version = 1\nitems = ["x"]\n', encoding="utf-8")
    _, errors = migration.load_preconditions(empty)
    assert any("缺少 [[items]]" in item or "不是表" in item for item in errors)

    no_items = tmp_path / "none.toml"
    no_items.write_text("schema_version = 1\n", encoding="utf-8")
    _, errors = migration.load_preconditions(no_items)
    assert any("缺少 [[items]]" in item for item in errors)


def test_validate_preconditions_negative() -> None:
    """清单结构反例：版本 / 空清单 / 缺 id / 空 required_in / 重复项。"""
    base, _ = migration.load_preconditions(_PRECONDITIONS)
    first = base.items[0]
    broken = replace(base, schema_version=2, items=ConcurrentStableFrozenList())
    messages = "；".join(migration.validate_preconditions(broken))
    assert "schema_version" in messages
    assert "清单为空" in messages

    items = ConcurrentStableFrozenList(
        [
            replace(first, id="", label=""),
            replace(first, required_in=ConcurrentStableFrozenList()),
            first,
            first,
        ]
    )
    messages = "；".join(migration.validate_preconditions(replace(base, items=items)))
    assert "缺少 id" in messages
    assert "required_in 为空" in messages
    assert "前置 id 重复" in messages


def test_matrix_parse_failures(tmp_path: Path) -> None:
    """坏 TOML / 缺 domains / phases 与 domains 非表：都返回错误。"""
    bad = tmp_path / "bad.toml"
    bad.write_text("schema_version = [", encoding="utf-8")
    matrix, errors = migration.load_matrix(bad)
    assert matrix.schema_version == 0
    assert any("解析失败" in item for item in errors)

    no_domains = tmp_path / "none.toml"
    no_domains.write_text('schema_version = 1\nphases = ["x"]\ndomains = ["x"]\n', encoding="utf-8")
    _, errors = migration.load_matrix(no_domains)
    assert any("phases[0] 不是表" in item for item in errors)
    assert any("domains[0] 不是表" in item for item in errors)

    empty_domains = tmp_path / "empty.toml"
    empty_domains.write_text("schema_version = 1\n", encoding="utf-8")
    _, errors = migration.load_matrix(empty_domains)
    assert any("缺少 [[domains]]" in item for item in errors)


def test_validate_matrix_negative_fields() -> None:
    """矩阵结构反例：窗口 / 缺 key / 重复 key / 端口 / 状态 / 默认值 / 前置 / 指标 / 判据。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    first = matrix.domains[0]
    broken_domains = ConcurrentStableFrozenList(
        [
            replace(
                first,
                key="",
                port="wrong",
                category="wrong",
                switch_key="bad",
                domain_level_key="bad",
                domain_level_status="active",
                default_value="http",
                prerequisites=ConcurrentStableFrozenList(),
                observe_metrics=ConcurrentStableFrozenList(),
                rollback_criteria=ConcurrentStableFrozenList(),
            ),
            replace(first, key="low_risk_event"),
            replace(first, key="other_event", default_value="http"),
            replace(first, key="other_event"),
            replace(first, key="dup_switch", switch_key="[service_client].provider"),
            replace(first, key="dup_domain", domain_level_key="[service_client].domains.other"),
        ]
    )
    broken = replace(matrix, observe_window_minutes=0, domains=broken_domains)
    messages = "；".join(migration.validate_matrix(broken))
    assert "观察窗口非法" in messages
    assert "缺少 key" in messages
    assert "port = 'wrong' 非法" in messages
    assert "category = 'wrong' 非法" in messages
    assert "switch_key" in messages
    assert "domain_level_key" in messages
    assert "domain_level_status" in messages
    assert "default_value 须以原实现 'rocketmq' 开头" in messages
    assert "prerequisites 为空" in messages
    assert "observe_metrics 为空" in messages
    assert "rollback_criteria 为空" in messages


def test_gate_requires_env_specific_items() -> None:
    """环境专属前置（dev）未满足时拦 dev；生产阻断项仍单独列出。"""
    base, _ = migration.load_preconditions(_PRECONDITIONS)
    first = base.items[0]
    custom = replace(
        base,
        items=ConcurrentStableFrozenList(
            [replace(first, required_in=ConcurrentStableFrozenList(["dev"]), status="not_ready")]
        ),
    )
    gate = migration.evaluate_gate(custom, "dev")
    assert gate.allowed is False
    assert tuple(gate.blocked_by) == (first.id,)

    # 生产环境看这条 dev 专属前置：不阻断、也不列进生产阻断项（两个条件都走假分支）。
    prod_gate = migration.evaluate_gate(custom, "prod")
    assert prod_gate.allowed is True
    assert prod_gate.blocked_by == ()


def _satisfied_preconditions(tmp_path: Path) -> Path:
    """复制真实清单并把未满足项改成 satisfied（验证「全绿」路径）。"""
    text = _PRECONDITIONS.read_text(encoding="utf-8").replace('status = "not_ready"', 'status = "satisfied"')
    path = tmp_path / "preconditions-satisfied.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_gate_allows_when_all_satisfied(tmp_path: Path) -> None:
    """前置全满足：生产闸门放行，不再列阻断项。"""
    path = _satisfied_preconditions(tmp_path)
    code, lines = migration.run_check(preconditions_path=path, matrix_path=_MATRIX, env="prod")
    assert code == 0
    assert "阻断项" not in "\n".join(lines)


# ---------------------------------------------------------------------------
# 事件链 / 业务零改动的反例
# ---------------------------------------------------------------------------


def _msg(
    index: int, *, payload: bytes = b"x", headers: ConcurrentStableDict[str, str] | None = None, key: str | None = "k"
) -> migration.DrillMessage:
    """构造一条演练消息。"""
    return migration.DrillMessage(
        message_id=f"m-{index}",
        offset=index,
        payload=payload,
        headers=ConcurrentStableFrozenDict(headers or {}),
        key=key,
    )


def test_event_chain_check_negative() -> None:
    """事件链判据：条数 / 载荷 / 属性 / 分区键任一不符都要报出来。"""
    event = _fixed_event(1)
    assert migration.event_chain_errors(event, ConcurrentStableFrozenList()) != ()
    assert migration.event_chain_errors(event, ConcurrentStableFrozenList([_msg(1, payload=b"bad")])) != ()
    bad_headers = ConcurrentStableDict({"event_type": "wrong"})
    assert (
        migration.event_chain_errors(
            event, ConcurrentStableFrozenList([_msg(1, payload=encode_event(event), headers=bad_headers)])
        )
        != ()
    )
    assert (
        migration.event_chain_errors(
            event,
            ConcurrentStableFrozenList(
                [
                    _msg(
                        1,
                        payload=encode_event(event),
                        headers=ConcurrentStableDict(event_attributes(event)),
                        key="wrong",
                    )
                ]
            ),
        )
        != ()
    )


def test_business_zero_change_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    """真实调用点多出必填参数 / 适配层签名漂移都要判失败。"""

    def _with_extra_param(client: object, extra: object) -> None:
        """模拟真实调用点多出必填参数。"""
        del client, extra

    monkeypatch.setattr(migration, "fetch_catalog_snapshot", _with_extra_param)

    class _BadPublisher:
        """签名与端口不一致的发布器。"""

        def publish(self, event: object, extra: object) -> None:
            """签名多一个必填参数。"""
            del event, extra

    monkeypatch.setattr(migration, "MsgEventPublisher", _BadPublisher)
    errors, _ = migration.business_zero_change()
    assert any("新增了必填参数" in item for item in errors)
    assert any("MsgEventPublisher.publish" in item for item in errors)


# ---------------------------------------------------------------------------
# 演练失败分支（步骤级：失败即记原因，不向上抛）
# ---------------------------------------------------------------------------


class _FakeTargets:
    """可编排失败行为的演练目标（只用于步骤级反例）。"""

    def __init__(
        self,
        *,
        switch: migration.SwitchReport | None = None,
        batches: list[list[migration.DrillMessage]] | None = None,
        fail_publish_at: set[int] | None = None,
        prepare_error: bool = False,
        unavailable: str = "event_error",
        read_only: str = "ok",
        write: str = "ok",
        fallback: str = "ok",
    ) -> None:
        """初始化各步行为。"""
        self._switch = switch or _switch_report()
        self._batches = list(batches) if batches is not None else None
        self._published: list[EventEnvelope] = []
        self._fail_publish_at = fail_publish_at or set()
        self._prepare_error = prepare_error
        self._unavailable = unavailable
        self._read_only = read_only
        self._write = write
        self._fallback = fallback
        self.publish_calls = 0

    async def prepare_event(self) -> None:
        """建主题（可注入失败）。"""
        if self._prepare_error:
            raise RuntimeError("prepare 失败")

    async def publish(self, event: EventEnvelope) -> None:
        """发布（可指定第几次失败）。"""
        index = self.publish_calls
        self.publish_calls += 1
        if index in self._fail_publish_at:
            raise RuntimeError("publish 失败")
        self._published.append(event)

    async def publish_unavailable(self, event: EventEnvelope) -> None:
        """不可达发布（按模式抛错）。"""
        del event
        if self._unavailable == "swallow":
            return
        if self._unavailable == "other":
            raise RuntimeError("别的错")
        raise EventPublishError("不可达")

    async def consume_events(self) -> ConcurrentStableFrozenList[migration.DrillMessage]:
        """返回下一批消息（缺省把「已发布未消费」的事件折成消息）。"""
        if self._batches is not None:
            batch = self._batches.pop(0) if self._batches else []
            return ConcurrentStableFrozenList(batch)
        published = self._published
        self._published = []
        return ConcurrentStableFrozenList(
            [
                migration.DrillMessage(
                    message_id=f"m-{event.event_id}",
                    offset=index,
                    payload=encode_event(event),
                    headers=ConcurrentStableFrozenDict(event_attributes(event)),
                    key=event.aggregate_key,
                )
                for index, event in enumerate(published)
            ]
        )

    async def read_only(self) -> migration.ReadOnlyResult:
        """只读调用（可注入零条 / 异常）。"""
        if self._read_only == "error":
            raise RuntimeError("readonly 失败")
        count = 0 if self._read_only == "empty" else 1
        return migration.ReadOnlyResult(target="fake", digest="d", count=count)

    async def fallback_read_only(self) -> migration.ReadOnlyResult | None:
        """原链路（可注入异常 / 未提供）。"""
        if self._fallback == "error":
            raise RuntimeError("fallback 失败")
        if self._fallback == "none":
            return None
        return migration.ReadOnlyResult(target="http", digest="d", count=1)

    async def write_attempt(self, key: str) -> migration.WriteAttemptResult | None:
        """在途写调用（按模式返回）。"""
        del key
        if self._write == "error":
            raise RuntimeError("write 失败")
        if self._write == "none":
            return None
        if self._write == "bad_exec":
            return migration.WriteAttemptResult(executions=2, keys=ConcurrentStableFrozenList(["k", "k"]), digest="d")
        if self._write == "bad_keys":
            return migration.WriteAttemptResult(executions=1, keys=ConcurrentStableFrozenList(["a", "b"]), digest="d")
        return migration.WriteAttemptResult(executions=1, keys=ConcurrentStableFrozenList(["k", "k"]), digest="d")

    def switch(self) -> migration.SwitchReport:
        """切换判定。"""
        return self._switch

    async def aclose(self) -> None:
        """无连接可释放。"""


def _switch_report(*, event_mode: str = "msg", client_mode: str = "msg", error: str = "") -> migration.SwitchReport:
    """构造切换判定（默认双端口 msg）。"""
    return migration.SwitchReport(
        event=migration.SwitchDecision(
            plugin_key="event",
            configured="msg",
            implementation="" if error else event_mode,
            mode=event_mode,
            error_kind="config" if error else "",
            error=error,
        ),
        service_client=migration.SwitchDecision(
            plugin_key="service_client",
            configured="msg",
            implementation="" if error else client_mode,
            mode=client_mode,
            error_kind="config" if error else "",
            error=error,
        ),
        fingerprint="fp",
    )


async def _run_fake(**kwargs: Any) -> migration.DrillReport:
    """用可控目标跑一次演练（清单 / 矩阵取仓内真实文件）。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    targets = _FakeTargets(**kwargs)
    return await migration.run_drill(
        matrix=matrix,
        preconditions=preconditions,
        targets=targets,
        env="test",
        now=lambda: _NOW,
        clock=_StepClock(),
    )


async def test_drill_fails_on_bad_preconditions_and_matrix() -> None:
    """清单 / 矩阵校验不过：对应步失败，后续步骤仍完整记录。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    report = await migration.run_drill(
        matrix=matrix,
        preconditions=replace(preconditions, schema_version=9),
        targets=_FakeTargets(),
        env="test",
        now=lambda: _NOW,
        clock=_StepClock(),
    )
    assert report.conclusion == "失败"
    assert any("preconditions" in reason for reason in report.reasons)

    report = await migration.run_drill(
        matrix=replace(matrix, observe_window_minutes=0),
        preconditions=preconditions,
        targets=_FakeTargets(),
        env="test",
        now=lambda: _NOW,
        clock=_StepClock(),
    )
    assert any("matrix" in reason for reason in report.reasons)


async def test_drill_fails_on_bad_switch() -> None:
    """未切到 msg / 解析失败：switch 步失败。"""
    report = await _run_fake(switch=_switch_report(event_mode="original"))
    assert report.conclusion == "失败"
    assert any("switch_semantics" in reason for reason in report.reasons)


async def test_drill_step_failures() -> None:
    """事件链 / 只读 / 在途 / 未确认 / 双写 / fallback 的失败分支。"""
    cases: list[dict[str, Any]] = [
        {"prepare_error": True},
        {"fail_publish_at": {0}},
        {"batches": [[_msg(1), _msg(1)]]},
        {"read_only": "empty"},
        {"read_only": "error"},
        {"write": "error"},
        {"write": "none"},
        {"write": "bad_exec"},
        {"write": "bad_keys"},
        {"unavailable": "other"},
        {"unavailable": "swallow"},
        {"batches": [[_msg(1)], []], "fail_publish_at": {1}},
        {"batches": [[_msg(1)], [_msg(2), _msg(2)]]},
        {"batches": [[_msg(1)], []], "fail_publish_at": {2}},
        {"batches": [[_msg(1)], [], []], "fail_publish_at": {3}},
        {"fallback": "error"},
        {"fallback": "none"},
        {"batches": [[_msg(1)], [_msg(2)], [_msg(3), _msg(3)]]},
    ]
    for kwargs in cases:
        report = await _run_fake(**kwargs)
        assert report.conclusion in ("失败", "降级通过"), kwargs
        assert report.steps


async def test_drill_business_zero_change_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """业务零改动失败：该步失败并让结论为失败。"""

    def _with_extra_param(client: object, extra: object) -> None:
        """模拟真实调用点多出必填参数。"""
        del client, extra

    monkeypatch.setattr(migration, "fetch_catalog_snapshot", _with_extra_param)
    report = await _run_fake()
    assert report.conclusion == "失败"
    assert any("business_zero_change" in reason for reason in report.reasons)


async def test_drill_all_preconditions_satisfied_has_no_production_issue() -> None:
    """前置全绿：记录里不再登记「生产迁移被阻断」。"""
    matrix, _ = migration.load_matrix(_MATRIX)
    preconditions, _ = migration.load_preconditions(_PRECONDITIONS)
    satisfied = replace(
        preconditions,
        items=ConcurrentStableFrozenList([replace(item, status="satisfied") for item in preconditions.items]),
    )
    report = await migration.run_drill(
        matrix=matrix,
        preconditions=satisfied,
        targets=_FakeTargets(),
        env="test",
        now=lambda: _NOW,
        clock=_StepClock(),
    )
    assert report.gate.blocked_by == ()
    assert report.issues == ()


# ---------------------------------------------------------------------------
# CLI 剩余路径
# ---------------------------------------------------------------------------


def test_main_matrix_rejects_broken_file(tmp_path: Path) -> None:
    """`matrix` 子命令遇坏矩阵：退出 1 并打印错误。"""
    broken = tmp_path / "matrix.toml"
    broken.write_text("schema_version = 1\n", encoding="utf-8")
    assert migration.main(["matrix", "--matrix", str(broken)]) == 1


def test_main_check_paths() -> None:
    """`check` 子命令成功 / 失败两条路径。"""
    assert migration.main(["check", "--env", "test"]) == 0
    assert migration.main(["check", "--env", "prod"]) == 1


def test_main_module_entry(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """`python -m ops.migration` 入口（`__main__` 守卫）可执行。"""
    import runpy
    import sys

    monkeypatch.setattr(sys, "argv", ["ops.migration", "check", "--env", "test"])
    with pytest.raises(SystemExit) as caught:
        runpy.run_module("ops.migration", run_name="__main__")
    assert caught.value.code == 0
    assert "前置清单" in capsys.readouterr().out


def test_build_targets_default_factory(switch_settings: Settings) -> None:
    """缺省工厂按命令行参数构造网络目标（用例注入点未生效时的路径）。"""
    args = migration.build_parser().parse_args(["drill", "--msg-url", BASE_URL])
    targets = migration.build_targets(switch_settings, args)
    assert isinstance(targets, migration.HttpTargets)
    asyncio.run(targets.aclose())
