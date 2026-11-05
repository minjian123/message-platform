"""根路径数据面入口（HTTP/2 帧入口，任务 03-1）。

三件事：

1. **复用数据面三端点**：`/msg/publish`、`/msg/consume`、`/msg/ack` 直接调 `mount_data_plane()` 挂载，
   与 `/api/v1/msg/*` **同一个实现、同一份错误码声明**——HTTP/2 下先跑通同一套语义，03-2 再把 body
   换成二进制帧（路径与网关口径此后不再动）。
2. **`GET /msg/subscribe`**：SSE 长驻流（server streaming）。内部逐批调用 `DeliveryService.consume`，
   成员制 / inflight / 可见性超时重投 / 分区内停等与拉取端点**完全同口径**——流只是把「多次拉取」
   串起来，不是第二套投递语义。
3. **`GET /msg/connection`**：连接元数据只读自报（协议版本 / 承载方式 / 客户端地址），排障第一问。

**流的边界（写死在这里，别在实现里漂）**：

- 预检（保留期清理 + 主题 / 分区 / 成员）在**进流之前**做完——失败是普通 JSON 错误响应（状态码与错误码
  与 `/msg/consume` 一致）；一旦发出 `open` 事件，状态码已是 200，后续错误只能走 `error` 事件。
- 无消息时按 `StreamTuning.poll_interval_ms` 短间隔重试（缺省 200ms），不忙等、不永久挂起；
  真正的长轮询挂起归 03-5，换实现只换 `StreamTuning`。
- 结束条件三选一：客户端断开 / `RST_STREAM`、时长上限（`close(deadline)`）、成员撤销或分区易主
  （`error` + `close(member_lost)`）。
- 取消时**只释放流级资源**（登记表登记项、协程、每轮短会话）；`DeliveryState.inflight` 属消息语义，
  不随流清理——已投未 ack 的消息必须仍可 ack / 超时重投。
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Annotated

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query, Request, params
from fastapi.responses import StreamingResponse

from bms_msg.api.data.messages import (
    CONSUME_ERROR_CODES,
    CONSUME_PERMISSION,
    DataPlaneRoute,
    apply_retention,
    delivery_service,
    ensure_member,
    message_item,
    mount_data_plane,
)
from bms_msg.api.deps import (
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DeliveryStateDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    StreamRegistryDep,
    StreamTuningDep,
    UowFactoryDep,
)
from bms_msg.api.frame.gate import build_frame_gate
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.transport import (
    StreamRegistry,
    StreamTuning,
    connection_meta,
    sse_comment,
    sse_event,
)
from bms_msg.core.config import ACKS_AT_LEAST_ONCE
from bms_msg.errors import MsgError
from bms_msg.protocol.frame import PROTOCOL_VERSION
from bms_msg.schemas.message import ConsumeRequest, MessageItem
from bms_msg.schemas.transport import ConnectionInfo, SubscribeQuery
from bms_msg.tenant.context import CurrentTenant

__all__ = ["router", "stream_events"]

router = BaseRouter(
    key="msg_frame",
    prefix="/msg",
    tags=["msg-frame"],
)
"""根路径数据面聚合路由（挂载前缀为空 → 最终为 `/msg/*`）。"""


def _frame_gate(spec: DataPlaneRoute) -> params.Depends:
    """把帧闸门适配成路由依赖（只有根路径 `/msg/*` 挂；`/api/v1` 保持纯 JSON）。

    Args:
        spec: 数据面端点规格（提供帧类型与请求模型）。

    Returns:
        params.Depends: 该端点的帧闸门依赖。
    """
    return Depends(build_frame_gate(frame_type=spec.frame_type, request_model=spec.request_model))


mount_data_plane(router, frame_gate=_frame_gate)
"""复用数据面三端点（与 `/api/v1/msg/*` 同一规格），并挂 `application/x-msg-frame` 帧形态。"""

_CONSUME = Depends(require_permission(CONSUME_PERMISSION))
"""长驻流权限依赖（与拉取 / 确认同码：消费即持有 `msg:consume`）。"""

_MEMBER_LOST_CODES = frozenset({350013, 350014})
"""流内「成员消失」类码（组不存在 / 成员失效或分区易主）：关闭原因记 `member_lost`。

码值直接写字面量而非引枚举：`frozenset` 与枚举混用会让「是不是同一码」的判定分散在两处；
这两码的语义在本文件只用一次，写值与注释足够（码表唯一出处仍是 `errors.py`）。
"""


def _consume_request(query: SubscribeQuery) -> ConsumeRequest:
    """长驻流查询参数 → 拉取请求（语义与 `/msg/consume` 完全一致）。

    Args:
        query: 长驻流查询参数。

    Returns:
        ConsumeRequest: 拉取请求（`acks=1`，缺省至少一次语义）。
    """
    return ConsumeRequest(
        namespace=query.namespace,
        topic=query.topic,
        group=query.group,
        consumer_id=query.consumer_id,
        partition=query.partition,
        max_messages=query.max_messages,
        acks=ACKS_AT_LEAST_ONCE,
    )


async def _pull(
    *,
    uow_factory: UowFactory,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    tenant: CurrentTenant,
    request: ConsumeRequest,
) -> ConcurrentStableList[MessageItem]:
    """开一个短会话拉一批（每轮现开现关，不占请求会话）。

    每轮都重跑成员守卫：成员可能在流存续期间被撤销或分区易主——不重跑就会出现「流还在推、
    但这条流已无权消费」的静默越权。

    Args:
        uow_factory: 工作单元工厂（无请求上下文也能开平台库会话）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        tenant: 当前租户。
        request: 拉取请求。

    Returns:
        ConcurrentStableList[MessageItem]: 本批消息（可能为空）。

    Raises:
        MsgError: 主题不存在 / 组不存在 / 成员失效 / 分区号越界等（由调用方转 `error` 事件）。
    """
    async with uow_factory() as uow:
        await ensure_member(tenant=tenant, uow=uow, groups=groups, state=state, request=request)
        delivered = await delivery_service(uow, store, offsets, dead_letters, state).consume(
            tenant=tenant, request=request
        )
    return ConcurrentStableList(
        message_item(delivery.message, request.topic, delivery.delivery_count) for delivery in delivered
    )


async def stream_events(
    *,
    uow_factory: UowFactory,
    registry: StreamRegistry,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    tenant: CurrentTenant,
    query: SubscribeQuery,
    tuning: StreamTuning,
) -> AsyncGenerator[bytes]:
    """长驻流事件生成器（SSE 字节流；登记 / 注销在此收口）。

    生命周期见模块文档：`open` → 逐批 `message`（无消息发注释行保活）→ `close`；
    客户端断开时 `close(client_closed)` 后原样抛出 `CancelledError`（取消语义不能被吞）。

    Args:
        uow_factory: 工作单元工厂（每轮短会话）。
        registry: 流登记表（`finally` 里注销，用例据此断言「断开即释放」）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        tenant: 当前租户。
        query: 长驻流查询参数。
        tuning: 流参数。

    Yields:
        bytes: SSE 事件（`event: <名>\\ndata: <单行 JSON>\\n\\n`）或注释行。
    """
    request = _consume_request(query)
    started = time.monotonic()
    deadline = started + query.max_seconds
    last_emit = started
    stream_id = registry.open(
        namespace=query.namespace,
        topic=query.topic,
        group=query.group,
        consumer_id=query.consumer_id,
        partition=query.partition,
        opened_at=started,
    )
    reason = "error"
    try:
        yield sse_event(
            "open",
            {
                "stream_id": stream_id,
                "max_seconds": query.max_seconds,
                "poll_interval_ms": tuning.poll_interval_ms,
            },
        )
        while True:
            if time.monotonic() >= deadline:
                reason = "deadline"
                yield sse_event("close", {"reason": reason})
                return
            try:
                batch = await _pull(
                    uow_factory=uow_factory,
                    store=store,
                    offsets=offsets,
                    dead_letters=dead_letters,
                    state=state,
                    groups=groups,
                    tenant=tenant,
                    request=request,
                )
            except MsgError as exc:
                reason = "member_lost" if exc.code in _MEMBER_LOST_CODES else "error"
                yield sse_event("error", {"code": exc.code, "message": str(exc)})
                yield sse_event("close", {"reason": reason})
                return
            if batch:
                for item in batch:
                    yield sse_event("message", item.model_dump(mode="json"))
                last_emit = time.monotonic()
                continue
            if (time.monotonic() - last_emit) * 1000 >= tuning.keepalive_ms:
                yield sse_comment("keepalive")
                last_emit = time.monotonic()
            await asyncio.sleep(tuning.poll_interval_ms / 1000)
    except asyncio.CancelledError, GeneratorExit:
        # `CancelledError`：客户端 RST_STREAM / 连接断开；`GeneratorExit`：服务端关闭生成器（如停机）。
        # 两者对外都是「客户端走了」，关闭原因同为 client_closed；这里只登记原因并**原样抛出**，
        # 取消语义不能被吞（吞了会让上层以为流是正常结束的）。
        reason = "client_closed"
        raise
    finally:
        registry.close(stream_id, reason=reason)


@router.get(
    "/subscribe",
    dependencies=[_CONSUME],
    openapi_extra={"x-error-codes": list(CONSUME_ERROR_CODES)},
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "SSE 长驻流（event: open / message / error / close）",
            "content": {"text/event-stream": {}},
        }
    },
)
async def subscribe(
    query: Annotated[SubscribeQuery, Query()],
    tenant: DataPlaneTenantDep,
    uow_factory: UowFactoryDep,
    registry: StreamRegistryDep,
    tuning: StreamTuningDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
) -> StreamingResponse:
    """订阅一条分区（SSE 长驻流，server streaming）。

    需要 msg:consume 权限；须先 `join` 且分区在该成员分配内（组不存在 350013，成员失效 350014）。
    预检与 `/msg/consume` 同口径：命名空间 350010、主题 350001、分区号 350009；
    `max_seconds` 越界（> 600）与其它参数形态问题由契约层拒绝（`10001`）。
    进流之后：拉到消息逐条 `message`；无消息按轮询间隔重试（带注释行保活）；
    到期 `close(deadline)`；成员被撤销 / 分区易主 → `error`（同码）+ `close(member_lost)`。
    客户端断开或 `RST_STREAM` 后流被注销（`close(client_closed)`），已投未 ack 的消息仍可 ack。
    """
    consume_request = _consume_request(query)
    # 预检在本请求的作用域内完成：失败即普通 JSON 错误（状态码与 /msg/consume 一致）
    async with uow_factory() as preflight_uow:
        await apply_retention(
            tenant=tenant,
            uow=preflight_uow,
            store=store,
            namespace=query.namespace,
            topic=query.topic,
            partition_no=query.partition,
        )
        await ensure_member(
            tenant=tenant,
            uow=preflight_uow,
            groups=groups,
            state=state,
            request=consume_request,
        )
    return StreamingResponse(
        stream_events(
            uow_factory=uow_factory,
            registry=registry,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            tenant=tenant,
            query=query,
            tuning=tuning,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/connection")
async def connection(request: Request) -> ApiResponse[ConnectionInfo]:
    """回本连接元数据（协议版本 / 承载方式 / 客户端地址）。

    只读自报，不含租户数据，故不加权限码；05 域认证落地后由 03-4 决定是否收紧。
    用途：排障第一问（协商到 h2 还是退回 1.1）、用例断言、03-4 连接视图的底座。
    版本可见（03-2）：`supported_protocol_version` 恒有值（服务端当前帧版本），`protocol_version` 为本
    请求所用帧版本——本端点是 JSON 请求，故恒为 null；帧形态的版本在响应帧 header 与日志里。
    **不声明 `x-error-codes`**：本端点不触达任何业务判定，没有产品码可声明（与探针端点同性质，
    装载校验只对它给软提示，不影响通过）。
    """
    meta = connection_meta(request.scope)
    return ApiResponse.ok(
        ConnectionInfo(
            http_version=meta.http_version,
            scheme=meta.scheme,
            is_tls=meta.is_tls,
            client_host=meta.client_host,
            client_port=meta.client_port,
            protocol_version=None,
            supported_protocol_version=PROTOCOL_VERSION,
        )
    )
