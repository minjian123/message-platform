"""bms 端口适配的两仓契约用例（07B `06_01` 步 3 / 04-5 §2.6；Kiwi 2442）。

**这条用例为什么在 msg 仓**：`bms-core` 是 msg 的路径依赖，只有这边能同时 import 两侧——真 msg
应用（真路由 → 真帧 → 真处理器）+ bms 适配层 + bms 真实业务函数。bms 仓 CI 不 clone msg，那边
只能做替身级演练（`tests/msgclient/adapter/test_rollback.py`）。

六类契约（夹具在 `tests_support/bms_port_contract.py`）：
请求 / 响应走真应用；错误五类走脚本帧（要精确控制 `category`）；timeout 与 cancel 走脚本传输
（真应用的这两条路径由 04-2 / 04-3 用例覆盖，这里只验「到 bms 端口后不外泄」）。

**目标方法的处理器是测试替身**：`platform` 服务本体不在场，`platform.modules.snapshot` 的语义由
`TrackingHandler` 等价给出（返回同一份统一响应包裹）。验收的是「真实调用点经真帧往返仍成立」，
不是「platform 服务正确」。
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import cast

import pytest
from bms_core.catalog.base import fetch_catalog_snapshot
from bms_core.core.exceptions import BizError, ServiceUnavailableError
from bms_core.msgclient.rpc import RpcClient, StaticInstanceProvider
from bms_core.msgclient.transport import RpcHttpRequest, RpcHttpResponse
from bms_core.servicecall.base import ServiceRequest
from fastapi import FastAPI

from tests_support.bms_port_contract import (
    BASE_URL,
    ERROR_CASES,
    TARGET_METHOD,
    TARGET_SERVICE,
    ErrorCase,
    TrackingHandler,
    cancel_transport,
    error_response,
    msg_client,
    snapshot_request,
    timeout_transport,
)
from tests_support.msg_fixtures import msg_app, msg_declared_codes, msg_plugins
from tests_support.rpc_transport import AsgiRpcTransport, ScriptedRpcTransport, TransportHandler

pytestmark = pytest.mark.kiwi_id(2442)

__all__ = ["msg_app", "msg_declared_codes", "msg_plugins"]


def _register_target(app: FastAPI, *, method: str = TARGET_METHOD, idempotent: bool = True) -> TrackingHandler:
    """把目标方法登记进应用注册表。

    Args:
        app: 最小应用（`build_frame_app` 产物）。
        method: 方法名（缺省为真实调用点）。
        idempotent: 幂等声明（服务端只对「非幂等 + 带键」启用去重）。

    Returns:
        TrackingHandler: 记录调用次数的处理器。
    """
    handler = TrackingHandler()
    app.state.rpc_registry.register(TARGET_SERVICE, method, handler, idempotent=idempotent)
    return handler


async def test_real_call_point_over_real_frames(msg_app: FastAPI) -> None:
    """真实调用点：`fetch_catalog_snapshot` 经真 msg 应用（真帧）拿到与 http 同构的结果。"""
    handler = _register_target(msg_app)
    client = msg_client(AsgiRpcTransport(msg_app))
    try:
        records = await fetch_catalog_snapshot(client)
    finally:
        await client.aclose()

    assert [record.module_key for record in records] == ["platform"]
    assert handler.calls == [(TARGET_SERVICE, TARGET_METHOD, b"")]


async def test_same_idempotency_key_executes_once(msg_app: FastAPI) -> None:
    """幂等键：非幂等方法带同一个键重复调用，服务端只执行一次处理器（重试不产生第二次副作用）。

    **为什么另立一个写方法**：真实调用点 `modules.snapshot` 是 GET、注册时声明幂等——服务端对
    幂等方法**不去重**（重复执行无副作用，去重表没必要留记录，见 `api/rpc/idempotency.py`）；
    要验去重，必须用「非幂等 + 带键」这一条真实路径。
    """
    write_method = "modules.create"
    handler = _register_target(msg_app, method=write_method, idempotent=False)
    async with RpcClient(
        StaticInstanceProvider([BASE_URL]),
        tenant="demo",
        transport=AsgiRpcTransport(msg_app),
    ) as rpc:
        first = await rpc.call(TARGET_SERVICE, write_method, b"{}", idempotency_key="k-2442")
        second = await rpc.call(TARGET_SERVICE, write_method, b"{}", idempotency_key="k-2442")

    assert len(handler.calls) == 1
    assert first.payload == second.payload


@pytest.mark.parametrize("case", ERROR_CASES, ids=[f"{case.code}-{case.category}" for case in ERROR_CASES])
async def test_error_frame_maps_to_expected_bms_exception(case: ErrorCase) -> None:
    """错误帧按 `category` 分派到 bms 异常，原始码进 `data`（类别决定类，码值不改语义）。"""
    client = msg_client(ScriptedRpcTransport(handler=_fixed(error_response(case))))
    with pytest.raises(case.expected) as caught:
        await client.call(snapshot_request())

    error = cast("BizError", caught.value)
    detail = cast("Mapping[str, object]", error.data)
    assert detail["code"] == case.code
    assert detail["category"] == case.category
    assert error.code == case.bms_code


async def test_timeout_is_unknown_outcome_not_failure() -> None:
    """timeout：映射为 `ServiceUnavailableError` 且 `data.outcome=unknown`（「结果未知」不是「失败」）。"""
    client = msg_client(timeout_transport())
    with pytest.raises(ServiceUnavailableError) as caught:
        await client.call(snapshot_request())

    detail = cast("Mapping[str, object]", caught.value.data)
    assert detail["code"] == 350004
    assert detail["outcome"] == "unknown"


async def test_cancel_passes_through_untouched() -> None:
    """cancel：`CancelledError` 原样穿透（不重试、不映射成业务异常）。"""
    client = msg_client(cancel_transport())
    with pytest.raises(asyncio.CancelledError):
        await client.call(snapshot_request())


async def test_unregistered_method_over_real_app_is_business_reject(msg_app: FastAPI) -> None:
    """真实应用上打到未注册方法：`350023` 业务拒绝 → `BizError`（保留原码，不吞成不可用）。"""
    client = msg_client(AsgiRpcTransport(msg_app))
    absent = ServiceRequest(service=TARGET_SERVICE, method="GET", path="/api/v1/modules/absent")
    try:
        with pytest.raises(BizError) as caught:
            await client.call(absent)
    finally:
        await client.aclose()

    assert caught.value.code == 350023


def _fixed(response: RpcHttpResponse) -> TransportHandler:
    """构造「恒定返回给定响应」的一元处理器。

    Args:
        response: 要返回的传输响应。

    Returns:
        一元处理器（`RpcHttpRequest` → `RpcHttpResponse`）。
    """

    async def handler(request: RpcHttpRequest) -> RpcHttpResponse:
        """忽略请求，返回固定响应。"""
        del request
        return response

    return handler
