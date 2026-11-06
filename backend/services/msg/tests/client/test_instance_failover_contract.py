"""04-6 跨仓契约用例（Kiwi 2444）：真 msg 应用 + 多 origin 路由，验证换实例与不重复提交。

**为什么在 msg 仓**：`bms-core` 是 msg 的路径依赖，只有这边能同时 import 真实 msg 应用与
`bms_core.msgclient`。bms 仓用例负责选择 / 摘除的单元级覆盖；这里补「真路由 → 真帧 → 真处理器」
的端到端证据。

**不 mock 帧编解码、不 mock msg 路由**：失败实例用脚本传输注入连接错误，成功实例走
`AsgiRpcTransport` 驱动真应用；处理器调用次数直接回答「有没有重复提交」。
"""

from __future__ import annotations

import pytest
from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.core.exceptions import ServiceUnavailableError
from bms_core.msgclient.instances import InstanceSelector, InstanceSnapshot, StaticInstanceSelector
from bms_core.msgclient.rpc import (
    InstanceUnavailableError,
    RpcCallError,
    RpcClient,
)
from bms_core.msgclient.transport import (
    PHASE_CONNECT,
    RpcHttpRequest,
    RpcHttpResponse,
    RpcTransport,
    RpcTransportClosedError,
    TransportStats,
)
from fastapi import FastAPI

from tests_support.bms_port_contract import (
    TARGET_METHOD,
    TARGET_SERVICE,
    ErrorCase,
    TrackingHandler,
    error_response,
    snapshot_body,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_retry import FakeClock
from tests_support.rpc_transport import AsgiRpcTransport, ScriptedRpcTransport

pytestmark = pytest.mark.kiwi_id(2444)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]

A = "http://msg-a:8000"
"""失败实例。"""

B = "http://msg-b:8000"
"""健康实例。"""


class _RoutingTransport:
    """按 `request.origin` 把请求路由到不同传输（模拟两个 msg 节点）。"""

    def __init__(self, routes: dict[str, RpcTransport]) -> None:
        """初始化。

        Args:
            routes: origin → 传输实现。
        """
        self._routes = routes
        self.origins: list[str] = []

    async def send(self, request: RpcHttpRequest) -> RpcHttpResponse:
        """按 origin 转发一元请求。

        Args:
            request: 传输层请求。

        Returns:
            RpcHttpResponse: 目标实例的响应。
        """
        self.origins.append(request.origin)
        return await self._routes[request.origin].send(request)

    def open_stream(self, request: RpcHttpRequest):
        """按 origin 打开流（本用例不覆盖流式，保持契约完整）。

        Args:
            request: 传输层请求。

        Returns:
            AbstractAsyncContextManager: 目标实例的流打开器。
        """
        return self._routes[request.origin].open_stream(request)

    async def aclose(self) -> None:
        """关闭全部底层传输。"""
        for transport in self._routes.values():
            await transport.aclose()

    def stats(self) -> TransportStats:
        """进程内路由没有连接概念。

        Returns:
            TransportStats: 恒零。
        """
        return TransportStats(connections=0, open_streams=0)


class _EmptyProvider:
    """空来源替身（目录不可用）。"""

    def resolve(self, service: str) -> InstanceSnapshot:
        """返回空快照。

        Args:
            service: 逻辑服务标识（未使用）。

        Returns:
            InstanceSnapshot: 空快照。
        """
        del service
        return InstanceSnapshot(
            pool_key="msg",
            addresses=ConcurrentStableFrozenList(),
            revision=0,
            fetched_at_ms=0,
        )


def _register_target(app: FastAPI) -> TrackingHandler:
    """把目标方法登记进真 msg 应用。

    Args:
        app: 真 msg 应用。

    Returns:
        TrackingHandler: 记录调用次数的处理器。
    """
    handler = TrackingHandler()
    app.state.rpc_registry.register(TARGET_SERVICE, TARGET_METHOD, handler, idempotent=True)
    return handler


async def test_connect_failure_switches_to_real_msg_instance(msg_app: FastAPI) -> None:
    """失败实例连接不上 → 切到真 msg 应用，处理器只执行一次。"""
    handler = _register_target(msg_app)
    clock = FakeClock()

    async def fail_connect(request: RpcHttpRequest) -> RpcHttpResponse:
        del request
        raise RpcTransportClosedError("connect failed", phase=PHASE_CONNECT)

    routing = _RoutingTransport(
        {
            A: ScriptedRpcTransport(handler=fail_connect),
            B: AsgiRpcTransport(msg_app),
        }
    )
    client = RpcClient(StaticInstanceSelector([A, B], clock=clock), tenant="demo", transport=routing, clock=clock)
    try:
        reply = await client.call(TARGET_SERVICE, TARGET_METHOD, idempotent=True)
    finally:
        await client.aclose()

    assert reply.payload == snapshot_body()
    assert routing.origins == [A, B]
    assert len(handler.calls) == 1


async def test_unknown_result_stays_on_same_instance(msg_app: FastAPI) -> None:
    """结果未知（350004 / unknown）：不换实例，真应用处理器从未被调用。"""
    handler = _register_target(msg_app)
    clock = FakeClock()
    unknown = error_response(ErrorCase(350004, "unknown", False, ServiceUnavailableError, 10007))
    routing = _RoutingTransport(
        {
            A: ScriptedRpcTransport(handler=lambda request: _respond(unknown)),
            B: AsgiRpcTransport(msg_app),
        }
    )
    client = RpcClient(StaticInstanceSelector([A, B], clock=clock), tenant="demo", transport=routing, clock=clock)
    try:
        with pytest.raises(RpcCallError):
            await client.call(TARGET_SERVICE, TARGET_METHOD, idempotent=True)
    finally:
        await client.aclose()

    assert routing.origins
    assert all(origin == A for origin in routing.origins)
    assert handler.calls == []


async def test_empty_catalog_is_instance_unavailable() -> None:
    """目录为空：快速失败，抛 `InstanceUnavailableError`（不回落写死地址）。"""
    clock = FakeClock()
    selector = InstanceSelector(_EmptyProvider(), clock=clock)
    client = RpcClient(selector, tenant="demo", transport=ScriptedRpcTransport(handler=lambda request: _respond(None)))
    try:
        with pytest.raises(InstanceUnavailableError) as caught:
            await client.call(TARGET_SERVICE, TARGET_METHOD)
    finally:
        await client.aclose()

    assert caught.value.retryable is True
    assert caught.value.reason == "empty_snapshot"


async def test_single_instance_uses_same_abstraction(msg_app: FastAPI) -> None:
    """单实例也走 `InstanceSelector`，行为与多实例一致。"""
    handler = _register_target(msg_app)
    clock = FakeClock()
    client = RpcClient(
        StaticInstanceSelector([B], clock=clock),
        tenant="demo",
        transport=AsgiRpcTransport(msg_app),
        clock=clock,
    )
    try:
        reply = await client.call(TARGET_SERVICE, TARGET_METHOD, idempotent=True)
    finally:
        await client.aclose()

    assert reply.payload == snapshot_body()
    assert len(handler.calls) == 1


async def _respond(response: RpcHttpResponse | None) -> RpcHttpResponse:
    """脚本处理器：返回固定响应。

    Args:
        response: 响应；None 用空结果（本用例不会走到）。

    Returns:
        RpcHttpResponse: 响应。
    """
    assert response is not None
    return response
