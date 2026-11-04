"""数据面消息接口（`/api/v1/msg/publish`、`/msg/consume`、`/msg/ack`；详细设计 §7）。

- **权限码**：`msg:publish`（发布）、`msg:consume`（拉取与确认）——生产与消费分权（最小权限）。
- **租户**：数据面唯一出口 `get_data_plane_tenant()`（内部 `frame_tenant()`）；请求体不接收 `tenant`。
- **数据源**：主题元数据在平台库，会话取 `get_platform_uow`；消息 / 位点 / 死信是进程级内存实现
  （`app.state` 注入）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

import base64
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends

from bms_msg.api.deps import (
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DeliveryStateDep,
    MessageStoreDep,
    OffsetStoreDep,
)
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgErrorCode
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import (
    AckRequest,
    AckResult,
    ConsumeRequest,
    ConsumeResult,
    MessageItem,
    PublishRequest,
    PublishResult,
)

__all__ = ["router"]

_PUBLISH = Depends(require_permission("msg:publish"))
_CONSUME = Depends(require_permission("msg:consume"))

_PUBLISH_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""发布端点声明的错误码（`x-error-codes`）。"""

_CONSUME_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.MESSAGE_NOT_FOUND),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""拉取端点声明的错误码（`x-error-codes`）。"""

_ACK_ERRORS = [
    int(MsgErrorCode.TOPIC_NOT_FOUND),
    int(MsgErrorCode.OFFSET_INVALID),
    int(MsgErrorCode.MESSAGE_NOT_FOUND),
    int(MsgErrorCode.DELIVERY_EXHAUSTED),
    int(MsgErrorCode.PARTITION_COUNT_INVALID),
    int(MsgErrorCode.NAMESPACE_INVALID),
    int(MsgErrorCode.TENANT_SUSPENDED),
    int(MsgErrorCode.TENANT_NOT_FOUND),
]
"""确认端点声明的错误码（`x-error-codes`）。"""

router = BaseRouter(
    key="msg_data_messages",
    prefix="/msg",
    tags=["msg-data"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def _service(
    uow: UnitOfWork,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
) -> DeliveryService:
    """组装投递服务（请求级主题仓储 + 进程级存储 / 位点 / 死信 / 投递状态）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。

    Returns:
        DeliveryService: 投递服务。
    """
    session = cast("DbSession", uow.session)
    return DeliveryService(
        store=store,
        offsets=offsets,
        dead_letters=dead_letters,
        router=TopicRouter(TopicRepository(session)),
        state=state,
    )


def _item(message: StoredMessage, topic: str, delivery_count: int) -> MessageItem:
    """存储记录 + 投递次数 → 响应明细。

    Args:
        message: 存储记录。
        topic: 主题名。
        delivery_count: 投递次数。

    Returns:
        MessageItem: 响应明细（载荷 base64 编码）。
    """
    return MessageItem(
        message_id=message.message_id,
        topic=topic,
        partition=message.partition_no,
        offset=message.offset,
        key=message.key,
        payload=base64.b64encode(message.payload).decode("ascii"),
        headers=message.headers,
        trace_id=message.trace_id,
        delivery_count=delivery_count,
        published_at=message.published_at,
    )


@router.post("/publish", dependencies=[_PUBLISH], openapi_extra={"x-error-codes": _PUBLISH_ERRORS})
async def publish(
    req: PublishRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
) -> ApiResponse[PublishResult]:
    """发布一条消息（按分区键选分区，返回 `message_id` / 分区 / offset）。

    需要 msg:publish 权限；`tenant` 取自数据面解析链；主题不存在或未启用返回 350001，
    命名空间非已启用 module_key 返回 350010，载荷非合法 base64 返回 10001。
    """
    message = await _service(uow, store, offsets, dead_letters, state).publish(tenant=tenant, request=req)
    return ApiResponse.ok(
        PublishResult(
            message_id=message.message_id,
            topic=req.topic,
            partition=message.partition_no,
            offset=message.offset,
        )
    )


@router.post("/consume", dependencies=[_CONSUME], openapi_extra={"x-error-codes": _CONSUME_ERRORS})
async def consume(
    req: ConsumeRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
) -> ApiResponse[ConsumeResult]:
    """拉取一批消息（缺省至少一次：超时未 ack 会重投且 `delivery_count` 递增）。

    需要 msg:consume 权限；`acks=0` 表示不等确认（返回前位点直接前移，可能丢）。
    主题不存在 350001，分区号越界 350009，位点已被保留期清理 350003。
    """
    delivered = await _service(uow, store, offsets, dead_letters, state).consume(tenant=tenant, request=req)
    return ApiResponse.ok(
        ConsumeResult(
            messages=ConcurrentStableList(
                _item(delivery.message, req.topic, delivery.delivery_count) for delivery in delivered
            )
        )
    )


@router.post("/ack", dependencies=[_CONSUME], openapi_extra={"x-error-codes": _ACK_ERRORS})
async def ack(
    req: AckRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
) -> ApiResponse[AckResult]:
    """确认一条消息（位点提交值 = `offset + 1`，提交失败不前移）。

    需要 msg:consume 权限；位点越界 350002，消息不在投递中 350003，
    消息已因重投超限转入死信 350007。
    """
    committed = await _service(uow, store, offsets, dead_letters, state).ack(tenant=tenant, request=req)
    return ApiResponse.ok(AckResult(committed_offset=committed))
