"""跨仓契约夹具（bms 07B `06_01` 步 3 与 msg 04-5 §2.6 共用）：六类 wire 期望的单一来源。

**为什么落在 msg 仓**：`bms-core` 是 msg 的路径依赖，反向不成立——只有这边能同时 import 两侧
（`bms_core.msgclient.adapter` 与 `bms_msg`）。bms 仓 CI 不 clone msg，故 bms 侧用自有替身；
那边的期望值**不复制本文件**（复制必然漂移，两仓各改一半最难看）。

六类：请求（`ServiceRequest` → 帧）/ 响应（帧 → `ServiceResponse`）/ 错误（五类逐码）/
timeout / cancel / 幂等键。前四类给出「同一份期望」，后两类的判据是「不外泄、不重试、去重一次」。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import httpx
from bms_core.catalog.base import CATALOG_SNAPSHOT_PATH
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import BizError, ParamError, RateLimitError, ServiceUnavailableError
from bms_core.events.topic import topic_for
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher
from bms_core.msgclient.adapter.service_client import MsgServiceClient
from bms_core.msgclient.frame import FrameFlags, FrameType, encode_frame
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.msgclient.transport import RpcHttpResponse, RpcTransportTimeoutError
from bms_core.msgclient.values import RpcCall, RpcReply
from bms_core.ratelimit.null import NullRateLimiter
from bms_core.servicecall.base import ServiceCallPolicy, ServiceRequest
from fastapi import FastAPI

from tests_support.rpc_transport import ScriptedRpcTransport

__all__ = [
    "BASE_URL",
    "ERROR_CASES",
    "EVENT_NAMESPACE",
    "EVENT_TOPIC",
    "EVENT_TYPE",
    "SNAPSHOT_PATH",
    "TARGET_METHOD",
    "TARGET_SERVICE",
    "ErrorCase",
    "TrackingHandler",
    "cancel_transport",
    "error_response",
    "msg_client",
    "msg_event_publisher",
    "snapshot_body",
    "snapshot_request",
    "timeout_transport",
]

BASE_URL = "http://msg"
"""实例基址（ASGI 直连，主机名仅占位）。"""

EVENT_NAMESPACE = "msg"
"""事件发布命名空间（msg 侧按已启用 `module_key` 校验；用例应用只启用该模块）。"""

EVENT_TYPE = "demo.order.created"
"""事件类型（`topic_for` 据此派生 `bms-demo-order`）。"""

EVENT_TOPIC = topic_for(EVENT_TYPE)
"""事件主题（与 bms 发布器同一派生函数，避免两处写字面量）。"""

TARGET_SERVICE = "platform"
"""真实调用点的目标服务（`fetch_catalog_snapshot` 的缺省）。"""

TARGET_METHOD = "modules.snapshot"
"""目标方法（`/api/v1/modules/snapshot` 去前缀后点分）。"""

SNAPSHOT_PATH = CATALOG_SNAPSHOT_PATH
"""真实调用点的路径常量（与 bms 业务函数共用同一份，避免两处写字面量）。"""

_MODULE_ROW: ConcurrentStableDict[str, object] = ConcurrentStableDict(
    {
        "module_key": "platform",
        "name": "平台",
        "table_prefix": "sys_",
        "event_domain": "platform",
        "errcode_segment": "10",
        "service_key": "platform",
        "business_code": "platform",
        "service_group": "foundation",
        "build_batch": 1,
        "service_version": "0.1.0",
        "contract_version": "0.1.0",
        "product_key": "bms",
        "status": "enabled",
    }
)
"""快照一行的必填字段（与 `ModuleRecord` 对齐；`tenant_base` 缺省 False）。"""


def snapshot_body() -> bytes:
    """统一响应包裹的快照体（真实调用点的期望载荷）。

    Returns:
        bytes: `{"code":0,"message":"ok","data":[<一行>]}` 的 JSON 字节。
    """
    body: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {"code": 0, "message": "ok", "data": [dict(_MODULE_ROW.items())]}
    )
    return json.dumps(dict(body.items())).encode()


def snapshot_request(*, max_retries: int = 0) -> ServiceRequest:
    """真实调用点的请求（与 `fetch_catalog_snapshot` 内部同形）。

    Args:
        max_retries: 最大重试次数（契约用例缺省 0，避免把「重试」混进错误映射断言）。

    Returns:
        ServiceRequest: 调用请求。
    """
    return ServiceRequest(
        service=TARGET_SERVICE,
        method="GET",
        path=SNAPSHOT_PATH,
        policy=ServiceCallPolicy(max_retries=max_retries, retry_backoff=0.0),
    )


@dataclass(frozen=True)
class ErrorCase:
    """一条错误帧的契约期望。"""

    code: int
    """产品码 / 平台码。"""

    category: str
    """帧头 `category`（04-4 五类）。"""

    retryable: bool
    """帧头 `retryable`。"""

    expected: type[Exception]
    """bms 侧应抛出的异常类型。"""

    bms_code: int
    """bms 异常自身的码（只有业务拒绝保留原码，其余按类别归一为平台码）。"""


ERROR_CASES: tuple[ErrorCase, ...] = (
    ErrorCase(10001, "param_error", False, ParamError, 10001),
    ErrorCase(350023, "business_reject", False, BizError, 350023),
    ErrorCase(350005, "transient", True, RateLimitError, 10005),
    ErrorCase(350016, "transient", True, ServiceUnavailableError, 10007),
    ErrorCase(350004, "unknown", False, ServiceUnavailableError, 10007),
    ErrorCase(350002, "server_bug", False, ServiceUnavailableError, 10007),
)
"""五类（`server_bug` 单列一条）逐码期望：类别决定异常类与平台码，原始码只进 `data`。

**为什么只有业务拒绝保留原码**：参数错误 / 限流 / 不可用对调用方的处置是统一的（改参 / 退避 /
降级），原码进 `data` 供排障即可；业务拒绝的码是调用方要分支的语义（如 `350023` = 方法未注册），
归一就把「对端没这个方法」和「下游挂了」混成了一件事。
"""


def error_response(case: ErrorCase) -> RpcHttpResponse:
    """按契约期望编码一个错误帧响应。

    Args:
        case: 错误码契约。

    Returns:
        RpcHttpResponse: 携带错误帧的传输响应。
    """
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict(
        {
            "request_id": "r-contract",
            "code": case.code,
            "message": "contract",
            "retryable": case.retryable,
            "layer": "business",
            "category": case.category,
            "detail": None,
            "retry_after_ms": None,
        }
    )
    body = encode_frame(frame_type=int(FrameType.ERROR), flags=FrameFlags.RESPONSE, header=dict(header.items()))
    return RpcHttpResponse(status=200, headers=(), body=body, stream_id=1)


def timeout_transport() -> ScriptedRpcTransport:
    """构造「传输层超时」的脚本传输（对应 `ServiceUnavailableError` 的未知结果分支）。

    Returns:
        ScriptedRpcTransport: 首跳即超时的传输。
    """

    async def handler(request: object) -> RpcHttpResponse:
        """恒定抛传输超时。"""
        del request
        raise RpcTransportTimeoutError("下游未在预算内响应（注入）")

    return ScriptedRpcTransport(handler=handler)


def cancel_transport() -> ScriptedRpcTransport:
    """构造「调用方取消」的脚本传输（`CancelledError` 必须原样穿透，不映射成 bms 异常）。

    Returns:
        ScriptedRpcTransport: 首跳即取消的传输。
    """

    async def handler(request: object) -> RpcHttpResponse:
        """抛 `CancelledError`（取消不是失败）。"""
        del request
        raise asyncio.CancelledError("调用方取消（注入）")

    return ScriptedRpcTransport(handler=handler)


class TrackingHandler:
    """记录调用次数与入参的目标方法处理器（`platform.modules.snapshot` 的等价替身）。"""

    def __init__(self) -> None:
        """初始化。"""
        self.calls: list[tuple[str, str, bytes]] = []

    async def __call__(self, call: RpcCall) -> RpcReply:
        """记录一次调用并返回快照载荷。

        Args:
            call: 调用入参。

        Returns:
            RpcReply: 结果（载荷为统一响应包裹的快照体）。
        """
        self.calls.append((call.service, call.method, call.payload))
        return RpcReply(payload=snapshot_body())


def msg_client(transport: object, *, tenant: str | None = "demo") -> MsgServiceClient:
    """构造走给定传输的 `MsgServiceClient`（bms 侧适配层，原样复用）。

    Args:
        transport: `RpcTransport` 实现（`AsgiRpcTransport` 或 `ScriptedRpcTransport`）。
        tenant: 缺省租户（帧头 `tenant` 来源）。

    Returns:
        MsgServiceClient: 适配层实例。
    """
    return MsgServiceClient(
        rpc_client=RpcClient(
            StaticInstanceSelector([BASE_URL]),
            transport=transport,  # pyright: ignore[reportArgumentType]
        ),
        rate_limiter=NullRateLimiter(),
        caller=TARGET_SERVICE,
        tenant=tenant,
    )


def msg_event_publisher(
    app: FastAPI, *, namespace: str = EVENT_NAMESPACE, tenant: str | None = None
) -> MsgEventPublisher:
    """构造走真实 msg 应用的事件发布器（ASGI 直连，不启网络）。

    Args:
        app: 真实 msg 应用（`build_frame_app` 产物，含 `/api/v1` 数据面）。
        namespace: 事件命名空间（缺省为用例应用已启用的模块键）。
        tenant: 装配期缺省租户编码（事件自带租户时优先）。

    Returns:
        MsgEventPublisher: bms 侧发布器（生产实现原样复用）。
    """
    return MsgEventPublisher(
        base_url=BASE_URL,
        namespace=namespace,
        tenant=tenant,
        transport=httpx.ASGITransport(app=app),
    )
