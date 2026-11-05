"""数据面帧闸门（任务 03-2）：同一路径上的帧形态入口。

**为什么是「依赖闸门」而不是第二条路由**（详细设计 §6.1）：

- 再加一条 `include_in_schema=False` 的同路径路由，会被基座 `hidden_route_violations()` 判
  「隐藏路由超白名单」（它按**路径集合**判定，业务路径不得进白名单）；
- 再加一条可见路由，`app.openapi()` 的 `paths` 以路径为键，后者覆盖前者——帧路由没有 `requestBody`，
  公开契约里的请求体 schema 会被抹掉。

FastAPI 的依赖先于请求体校验执行，因此「在依赖里判定帧形态、自己处理、用异常短路端点」既保住单路由，
也让 JSON 分支零改动。

**职责边界**：只做「解码 → 组装入参模型 → 调 `api/data/messages.py` 的同一批函数 → 编码结果」。
业务语义（保留清理 / 成员守卫 / 投递 / 停等 / 延迟）仍然只有一份，闸门里一行不写。
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from bms_core.api.errors import build_error_response
from bms_core.core.exceptions import ParamError
from bms_core.core.logging import get_logger
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.schemas.common import ApiResponse
from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, ValidationError

from bms_msg.api.data.messages import (
    PlatformUowDep,
    ack,
    consume,
    nack,
    publish,
)
from bms_msg.api.deps import (
    BackpressureMetricsDep,
    BackpressureTuningDep,
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DelaySchedulerDep,
    DelayStoreDep,
    DeliveryStateDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    RateLimiterDep,
    RetentionThrottleDep,
    TopicCacheDep,
    WaiterDep,
)
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame, result_frame
from bms_msg.broker.backpressure import BackpressureTuning, RateLimiter
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayStore
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import connection_meta
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.core.config import MS_PER_SECOND
from bms_msg.errors import (
    MsgError,
    MsgFrameInvalidError,
    MsgRateLimitedError,
    MsgTenantNotFoundError,
    MsgVersionUnsupportedError,
)
from bms_msg.protocol.frame import (
    PROTOCOL_VERSION,
    Frame,
    FrameFlags,
    FrameType,
    decode_frame,
)
from bms_msg.protocol.payload import (
    KNOWN_CONTENT_TYPES,
    ensure_compression_flag,
    normalize_compression,
    normalize_content_type,
)
from bms_msg.schemas.message import AckRequest, ConsumeRequest, NackRequest, PublishRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = ["FrameHandled", "build_frame_gate", "is_frame_request", "register_frame_handlers"]

_LOGGER = get_logger("bms_msg.frame")

_FRAME_MISMATCH_HINT = "帧类型与请求路径不匹配"
"""类型不匹配的提示（客户端最常见错误：把 consume 发到 /msg/publish）。"""


class FrameHandled(Exception):
    """帧请求已被闸门处理完毕（携带待返回的响应；由注册的异常处理器直出）。

    用异常短路是因为 FastAPI 的依赖无法「返回响应」——它只能在进入端点前抛异常；
    这不是错误分支，而是「请求已被另一条通道完整处理」的控制流。
    """

    def __init__(self, response: Response) -> None:
        """初始化。

        Args:
            response: 待返回的响应（帧形态，HTTP 200）。
        """
        super().__init__("frame request handled")
        self.response = response


def register_frame_handlers(app: FastAPI) -> None:
    """登记帧响应与限流异常处理器（装配期调用；测试最小装配同样需要）。

    两个处理器：

    - `FrameHandled`：直出闸门准备好的帧响应；
    - `MsgRateLimitedError`：JSON 429 **带 `Retry-After` 头**（基座统一错误响应已支持该参数，
      但全局 `BizError` 处理器不传；限流是这个头最需要的地方，故产品侧单独登记）。

    Args:
        app: 应用实例。
    """

    async def _render(request: Request, exc: Exception) -> Response:
        """直出闸门准备好的响应。

        Args:
            request: 当前请求（未使用，签名由处理器约定决定）。
            exc: `FrameHandled` 实例。

        Returns:
            Response: 帧响应。
        """
        del request
        return cast("FrameHandled", exc).response

    async def _rate_limited(request: Request, exc: Exception) -> Response:
        """限流拒绝 → 统一错误体 + `Retry-After`（秒，向上取整到至少 1）。

        Args:
            request: 当前请求（取请求 id / 链路 id / 应用装配）。
            exc: `MsgRateLimitedError`。

        Returns:
            Response: `429` 统一错误响应。
        """
        refuse = cast("MsgRateLimitedError", exc)
        seconds = max(math.ceil(refuse.retry_after_ms / MS_PER_SECOND), 1)
        return await build_error_response(request.scope, refuse, retry_after=seconds)

    app.add_exception_handler(FrameHandled, _render)
    app.add_exception_handler(MsgRateLimitedError, _rate_limited)


def _response(body: bytes) -> Response:
    """包装一个帧响应（HTTP 200；错误也在帧里，见详细设计 §6.5）。

    Args:
        body: 帧字节。

    Returns:
        Response: 响应对象。
    """
    return Response(content=body, media_type=FRAME_MEDIA_TYPE, status_code=200)


def _log(*, frame_type: int, request_id: str | None, request: Request, outcome: str) -> None:
    """记一条帧处理日志（版本可见的第三处落点：排障时先看这里）。

    Args:
        frame_type: 帧类型编号。
        request_id: 请求标识。
        request: 当前请求（取 HTTP 版本）。
        outcome: 结果（`ok` / `protocol_error` / `business_error` / `transport_error` / `rejected`）。
    """
    meta = connection_meta(request.scope)
    _LOGGER.info(
        "msg_frame",
        protocol_version=PROTOCOL_VERSION,
        frame_type=frame_type,
        request_id=request_id,
        http_version=meta.http_version,
        outcome=outcome,
    )


def _framed(*, code: int, message: str, request_id: str | None, frame_type: int, request: Request) -> FrameHandled:
    """构造「错误帧已就绪」的短路异常，并记日志。

    Args:
        code: 错误码。
        message: 提示。
        request_id: 请求标识。
        frame_type: 请求帧类型。
        request: 当前请求。

    Returns:
        FrameHandled: 待抛出的短路异常。
    """
    _log(frame_type=frame_type, request_id=request_id, request=request, outcome="protocol_error")
    return FrameHandled(_response(error_frame(code=code, message=message, request_id=request_id)))


def _request_id(header: Mapping[str, Any]) -> str | None:
    """取 `request_id`（缺失或非字符串时返回 None，由必填校验给出 `350018`）。

    Args:
        header: 帧头。

    Returns:
        str | None: 请求标识。
    """
    value = header.get("request_id")
    return value if isinstance(value, str) and value else None


_KNOWN_TYPES = frozenset(int(member) for member in FrameType)
"""已登记的类型编号（不在其中即「未知类型」）。"""


def _ensure_frame_type(frame: Frame, expected: int) -> None:
    """校验帧类型与路径匹配、且本任务确实服务该类型。

    Args:
        frame: 解码结果。
        expected: 该路径期望的类型。

    Raises:
        MsgFrameInvalidError: 类型不匹配（`350018`）。
    """
    if frame.frame_type == expected:
        return
    if frame.frame_type == int(FrameType.SUBSCRIBE):
        raise MsgFrameInvalidError(f"帧类型 {frame.frame_type} 暂未开放（帧式订阅推送归 03-5）；{_FRAME_MISMATCH_HINT}")
    if frame.frame_type == int(FrameType.HEARTBEAT):
        raise MsgFrameInvalidError(f"心跳帧请发到 /msg/connection/heartbeat；{_FRAME_MISMATCH_HINT}")
    if frame.frame_type == int(FrameType.NACK):
        raise MsgFrameInvalidError(f"nack 帧请发到 /msg/nack；{_FRAME_MISMATCH_HINT}")
    raise MsgFrameInvalidError(f"{_FRAME_MISMATCH_HINT}（收到 {frame.frame_type}，期望 {expected}）")


def _ensure_required(header: Mapping[str, Any], required: tuple[str, ...]) -> None:
    """校验必填头（缺**键**属协议层 `350018`；键在而值非法属契约层 `10001`）。

    Args:
        header: 帧头。
        required: 必填键集合（取自请求模型的必填字段）。

    Raises:
        MsgFrameInvalidError: 缺必填键（`350018`）。
    """
    missing = tuple(name for name in required if name not in header)
    if missing:
        raise MsgFrameInvalidError(f"缺少必填 frame header 字段：{', '.join(missing)}")


def _ensure_tenant(header: Mapping[str, Any], tenant: CurrentTenant) -> None:
    """校验帧头 `tenant` 与中间件解析结果一致（帧不作为鉴权第二入口）。

    Args:
        header: 帧头。
        tenant: 当前租户（中间件 + `frame_tenant()` 解析结果）。

    Raises:
        MsgTenantNotFoundError: 声明不一致（`350012`，不泄露租户是否存在）。
    """
    declared = header.get("tenant")
    if declared is None or declared == tenant.code:
        return
    raise MsgTenantNotFoundError("frame header 的 tenant 与请求身份不一致")


def _payload_of(frame: Frame) -> bytes:
    """取帧 payload（消费 / 确认不允许携带消息体）。

    Args:
        frame: 解码结果。

    Returns:
        bytes: 消息体（publish 专用；其余类型为空）。

    Raises:
        MsgFrameInvalidError: 非 publish 帧携带了 payload（`350018`）。
    """
    payload = bytes(frame.payload)
    if frame.frame_type != int(FrameType.PUBLISH) and payload:
        raise MsgFrameInvalidError("consume / ack 帧不得携带 payload")
    return payload


def _model_input(header: Mapping[str, Any], allowed: frozenset[str], payload: bytes) -> dict[str, Any]:
    """按「已知键白名单」把帧头转成模型入参（未知键忽略 = 铁律 2）。

    **payload 段不进模型**：原始字节走模型的私有通道（`set_raw_payload`），`payload` 字段保持空串，
    避免帧路径多一次 base64 编码（03-3 详细设计 §4.7）。

    Args:
        header: 帧头。
        allowed: 请求模型声明的字段名。
        payload: 帧 payload（publish 的消息体；本函数不使用，由调用方走私有通道）。

    Returns:
        dict[str, Any]: 模型入参。

    Raises:
        MsgFrameInvalidError: 头里出现 `payload` 键（消息体必须走 body，`350018`）。
    """
    del payload
    if "payload" in header:
        raise MsgFrameInvalidError("消息体必须放在 payload 段，不得写进 header")
    values = {key: header[key] for key in header if key in allowed and key != "payload"}
    if "payload" in allowed:
        values["payload"] = ""
    return values


def _normalize_payload_marks(frame: Frame, model: BaseModel) -> None:
    """规范化内容类型与压缩标记，并校验「是否压缩」与「用什么算法」自洽。

    只对发布请求生效（其余端点没有消息体）。未登记的内容类型**透传但打点**：broker 不解 payload，
    无从判断格式支不支持，拒收会把「新增格式」变成「先改 broker 再上线」（详细设计 §4.1）。

    Args:
        frame: 解码结果（提供 `flags.compressed`）。
        model: 已校验的请求模型。

    Raises:
        MsgFrameInvalidError: 压缩标记矛盾（`350018`）。
        ParamError: 内容类型 / 算法名形态非法（`10001`）。
    """
    if not isinstance(model, PublishRequest):
        return
    content_type = normalize_content_type(model.content_type)
    compression = normalize_compression(model.compression)
    ensure_compression_flag(compressed=bool(frame.flags & FrameFlags.COMPRESSED), compression=compression)
    model.content_type = content_type
    model.compression = compression
    if content_type not in KNOWN_CONTENT_TYPES:
        _LOGGER.info("msg_payload", content_type=content_type, outcome="unknown_content_type")


async def _invoke(
    *,
    frame_type: int,
    model: BaseModel,
    tenant: CurrentTenant,
    uow: UnitOfWork,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    delay_store: DelayStore,
    delay_scheduler: DelayScheduler,
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    limiter: RateLimiter,
    metrics: BackpressureMetrics,
    cache: TopicCache,
    throttle: RetentionThrottle,
) -> object:
    """按帧类型调用数据面同一批函数（业务语义的唯一出处）。

    Args:
        frame_type: 帧类型。
        model: 请求模型。
        tenant: 当前租户。
        uow: 平台库工作单元。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        delay_store: 延迟区。
        delay_scheduler: 到期推进器。
        waiter: 分区唤醒器（长轮询 / 额度归还后的唤醒）。
        backpressure: inflight 双阈值。
        limiter: 生产限流器。
        metrics: 背压指标收集器。
        cache: 进程内主题快照缓存（01-7）。
        throttle: 保留清理节流窗口表（01-7）。

    Returns:
        object: 结果模型。

    Raises:
        TypeError: 帧类型未知（装配错误）。
    """
    if frame_type == int(FrameType.PUBLISH):
        response = await publish(
            req=cast("PublishRequest", model),
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            delay_store=delay_store,
            delay_scheduler=delay_scheduler,
            limiter=limiter,
            metrics=metrics,
            cache=cache,
            throttle=throttle,
        )
    elif frame_type == int(FrameType.CONSUME):
        response = await consume(
            req=cast("ConsumeRequest", model),
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            waiter=waiter,
            backpressure=backpressure,
            metrics=metrics,
            cache=cache,
            throttle=throttle,
        )
    elif frame_type == int(FrameType.ACK):
        response = await ack(
            req=cast("AckRequest", model),
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            waiter=waiter,
            cache=cache,
        )
    elif frame_type == int(FrameType.NACK):
        response = await nack(
            req=cast("NackRequest", model),
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            waiter=waiter,
            cache=cache,
        )
    else:  # pragma: no cover - 类型在挂载期已固定为三种之一
        raise TypeError(f"未支持的帧类型：{frame_type}")
    return cast("ApiResponse[Any]", response).data


async def _handle_frame(
    *,
    request: Request,
    frame_type: int,
    request_model: type[BaseModel],
    required: tuple[str, ...],
    allowed: frozenset[str],
    tenant: CurrentTenant,
    uow: UnitOfWork,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    delay_store: DelayStore,
    delay_scheduler: DelayScheduler,
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    limiter: RateLimiter,
    metrics: BackpressureMetrics,
    cache: TopicCache,
    throttle: RetentionThrottle,
) -> Response:
    """处理一个帧请求（解码 → 校验 → 调用 → 编码）。

    Args:
        request: 当前请求。
        frame_type: 该路径期望的帧类型。
        request_model: 请求模型。
        required: 必填头键（模型必填字段）。
        allowed: 模型字段白名单。
        tenant: 当前租户。
        uow: 平台库工作单元。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        delay_store: 延迟区。
        delay_scheduler: 到期推进器。
        waiter: 分区唤醒器。
        backpressure: inflight 双阈值。
        limiter: 生产限流器。
        metrics: 背压指标收集器。
        cache: 进程内主题快照缓存（01-7）。
        throttle: 保留清理节流窗口表（01-7）。

    Returns:
        Response: 帧响应（成功或错误帧，均为 200）。

    Raises:
        MsgError: 帧不可解析（magic / 长度 / 截断）——由基座统一渲染为 4xx JSON。
    """
    body = await request.body()
    try:
        frame = decode_frame(body)
    except MsgVersionUnsupportedError as exc:
        _log(frame_type=frame_type, request_id=None, request=request, outcome="transport_error")
        raise FrameHandled(_response(error_frame(code=exc.code, message=str(exc)))) from exc
    request_id = _request_id(frame.header)
    if frame.frame_type not in _KNOWN_TYPES and not frame.flags & FrameFlags.NEED_RESPONSE:
        # 未知类型且不要求响应：按铁律「忽略」——不回错误帧，回一个空 200（客户端据此判定「已被忽略」）
        _log(frame_type=frame_type, request_id=request_id, request=request, outcome="ignored")
        return Response(status_code=200, content=b"", media_type=FRAME_MEDIA_TYPE)
    try:
        _ensure_frame_type(frame, frame_type)
        _ensure_required(frame.header, required)
        _ensure_tenant(frame.header, tenant)
        payload = _payload_of(frame)
        model = request_model.model_validate(_model_input(frame.header, allowed, payload))
        if isinstance(model, PublishRequest):
            model.set_raw_payload(payload)
        _normalize_payload_marks(frame, model)
    except ParamError as exc:
        # 内容类型 / 算法名形态非法：契约层（键在、值形态错），与 JSON 路径同码。
        raise _framed(
            code=10001,
            message=str(exc),
            request_id=request_id,
            frame_type=frame_type,
            request=request,
        ) from exc
    except ValidationError as exc:
        raise _framed(
            code=10001,  # 契约层：键在但值形态非法
            message="frame header 字段形态非法",
            request_id=request_id,
            frame_type=frame_type,
            request=request,
        ) from exc
    except MsgError as exc:
        raise _framed(
            code=exc.code,
            message=exc.message or str(exc),
            request_id=request_id,
            frame_type=frame_type,
            request=request,
        ) from exc
    try:
        result = await _invoke(
            frame_type=frame_type,
            model=model,
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            delay_store=delay_store,
            delay_scheduler=delay_scheduler,
            waiter=waiter,
            backpressure=backpressure,
            limiter=limiter,
            metrics=metrics,
            cache=cache,
            throttle=throttle,
        )
    except MsgError as exc:
        _log(frame_type=frame_type, request_id=request_id, request=request, outcome="business_error")
        message = exc.message or str(exc)
        retry_ms = exc.retry_after_ms if isinstance(exc, MsgRateLimitedError) else None
        raise FrameHandled(
            _response(error_frame(code=exc.code, message=message, request_id=request_id, retry_after_ms=retry_ms))
        ) from exc
    _log(frame_type=frame_type, request_id=request_id, request=request, outcome="ok")
    return _response(result_frame(frame_type=frame_type, request_id=request_id, result=result))


def build_frame_gate(*, frame_type: int, request_model: type[BaseModel]) -> Callable[..., Awaitable[None]]:
    """构造某端点专用的帧闸门依赖（必填键与白名单取自请求模型，不另维护一份字段表）。

    Args:
        frame_type: 该路径期望的帧类型。
        request_model: 请求模型（`PublishRequest` / `ConsumeRequest` / `AckRequest`）。

    Returns:
        Callable[..., Awaitable[None]]: 可直接 `Depends(...)` 的闸门函数。
    """
    required = ("request_id", *(name for name, field in request_model.model_fields.items() if field.is_required()))
    allowed = frozenset(request_model.model_fields)

    async def gate(
        request: Request,
        tenant: DataPlaneTenantDep,
        uow: PlatformUowDep,
        store: MessageStoreDep,
        offsets: OffsetStoreDep,
        dead_letters: DeadLetterQueueDep,
        state: DeliveryStateDep,
        groups: GroupStoreDep,
        delay_store: DelayStoreDep,
        delay_scheduler: DelaySchedulerDep,
        waiter: WaiterDep,
        backpressure: BackpressureTuningDep,
        limiter: RateLimiterDep,
        metrics: BackpressureMetricsDep,
        cache: TopicCacheDep,
        throttle: RetentionThrottleDep,
    ) -> None:
        """帧形态则整单处理并短路端点；非帧形态直接放行（JSON 路径零改动）。

        Args:
            request: 当前请求。
            tenant: 当前租户。
            uow: 平台库工作单元。
            store: 消息存储。
            offsets: 位点存储。
            dead_letters: 死信队列。
            state: 投递状态。
            groups: 组存储。
            delay_store: 延迟区。
            delay_scheduler: 到期推进器。
            waiter: 分区唤醒器。
            backpressure: inflight 双阈值。
            limiter: 生产限流器。
            metrics: 背压指标收集器。
            cache: 进程内主题快照缓存（01-7）。
            throttle: 保留清理节流窗口表（01-7）。

        Raises:
            FrameHandled: 帧请求已处理完毕（携带响应）。
            MsgError: 帧不可解析（4xx JSON）。
        """
        if not is_frame_request(request):
            return
        response = await _handle_frame(
            request=request,
            frame_type=frame_type,
            request_model=request_model,
            required=required,
            allowed=allowed,
            tenant=tenant,
            uow=uow,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            delay_store=delay_store,
            delay_scheduler=delay_scheduler,
            waiter=waiter,
            backpressure=backpressure,
            limiter=limiter,
            metrics=metrics,
            cache=cache,
            throttle=throttle,
        )
        raise FrameHandled(response)

    return gate
