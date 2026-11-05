"""数据面消息接口（`/api/v1/msg/publish`、`/msg/consume`、`/msg/ack`；详细设计 §7）。

- **权限码**：`msg:publish`（发布）、`msg:consume`（拉取与确认）——生产与消费分权（最小权限）。
- **租户**：数据面唯一出口 `get_data_plane_tenant()`（内部 `frame_tenant()`）；请求体不接收 `tenant`。
- **成员制**（01-4）：`consume` / `ack` 必带 `consumer_id`，且组须存在、成员须活跃、分区须在其分配内；
  校验在入口（`ensure_member`）完成，`DeliveryService` 的投递语义与停等规则一行不改。
- **延迟是发布的一个维度**（01-5）：`publish` 按 `delay_level` 分派——给值走 `DelayService`（入延迟区、
  无 offset、惰性启动推进器），缺省走原投递链路；两条分支共用同一份租户与主题校验口径。
- **保留清理是请求驱动**（01-6）：`publish` 与 `consume` 入口各按主题 `retention_ms` / `retention_bytes`
  幂等判定一次并前移可读区间起点。**放在 API 层而非 `DeliveryService` 内部**：01-2 / 01-3 的投递与停等
  语义（尤其中 `_collect`）一行不改；代价是请求内主题解析多一次（与 01-4 同类遗留，8B_01 帧入口统一收敛）。
- **两处挂载、一份规格**（03-1）：端点实现只定义一次，由 `DATA_PLANE_ROUTES` + `mount_data_plane()` 挂到
  两条聚合路由——`/api/v1/msg/*`（既有 HTTP/1.1 数据面，保留作基准对照与回退）与根路径 `/msg/*`
  （HTTP/2 帧入口，03-2 只换 body 编解码）。路径 / 方法 / 权限依赖 / `x-error-codes` **各只写一处**，
  避免两套路径的语义与错误码漂移。
- **数据源**：主题元数据在平台库，会话取 `get_platform_uow`；消息 / 位点 / 死信 / 组 / 延迟区是进程级内存
  实现（`app.state` 注入）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

import base64
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, params
from pydantic import BaseModel

from bms_msg.api.deps import (
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DelaySchedulerDep,
    DelayStoreDep,
    DeliveryStateDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
)
from bms_msg.broker.coordinator import GroupCoordinator
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayService, DelayStore
from bms_msg.broker.delivery import DeliveryService, DeliveryState
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.retention import RetentionService
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.errors import MsgErrorCode
from bms_msg.protocol.frame import FrameType
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
from bms_msg.tenant.context import CurrentTenant

__all__ = [
    "ACK_ERROR_CODES",
    "CONSUME_ERROR_CODES",
    "CONSUME_PERMISSION",
    "DATA_PLANE_ROUTES",
    "PUBLISH_ERROR_CODES",
    "PUBLISH_PERMISSION",
    "DataPlaneRoute",
    "FrameGateFactory",
    "apply_retention",
    "ensure_member",
    "message_item",
    "mount_data_plane",
    "router",
]

PUBLISH_PERMISSION = "msg:publish"
"""发布权限码（根路径 `/msg/*` 与 `/api/v1/msg/*` 共用一处声明）。"""

CONSUME_PERMISSION = "msg:consume"
"""拉取 / 确认 / 长驻流权限码（同一处声明，两处挂载共用）。"""

_PUBLISH = Depends(require_permission(PUBLISH_PERMISSION))
"""发布端点权限依赖（两条聚合路由共用同一实例）。"""

_CONSUME = Depends(require_permission(CONSUME_PERMISSION))
"""拉取 / 确认端点权限依赖（两条聚合路由共用同一实例）。"""

PUBLISH_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.TOPIC_NOT_FOUND),
        int(MsgErrorCode.NAMESPACE_INVALID),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
        int(MsgErrorCode.DELAY_QUOTA_EXCEEDED),
        int(MsgErrorCode.PAYLOAD_TOO_LARGE),
    ]
)
"""发布端点声明的错误码（`x-error-codes`；01-5 追加延迟区已满 `350015`，03-2 追加超限 `350006`）。"""

CONSUME_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.TOPIC_NOT_FOUND),
        int(MsgErrorCode.GROUP_NOT_FOUND),
        int(MsgErrorCode.MEMBER_INVALID),
        int(MsgErrorCode.MESSAGE_NOT_FOUND),
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
        int(MsgErrorCode.NAMESPACE_INVALID),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
    ]
)
"""拉取端点声明的错误码（`x-error-codes`；01-4 追加组 / 成员两项）。"""

ACK_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.TOPIC_NOT_FOUND),
        int(MsgErrorCode.GROUP_NOT_FOUND),
        int(MsgErrorCode.MEMBER_INVALID),
        int(MsgErrorCode.OFFSET_INVALID),
        int(MsgErrorCode.MESSAGE_NOT_FOUND),
        int(MsgErrorCode.DELIVERY_EXHAUSTED),
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
        int(MsgErrorCode.NAMESPACE_INVALID),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
    ]
)
"""确认端点声明的错误码（`x-error-codes`；01-4 追加组 / 成员两项）。"""

router = BaseRouter(
    key="msg_data_messages",
    prefix="/msg",
    tags=["msg-data"],
)

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""


def delivery_service(
    uow: UnitOfWork,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
) -> DeliveryService:
    """组装投递服务（请求级主题仓储 + 进程级存储 / 位点 / 死信 / 投递状态）。

    跨模块出口：帧入口与长驻流复用同一装配口径（不各自 new 一遍，避免语义分叉）。

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


def _delay_service(uow: UnitOfWork, store: DelayStore, scheduler: DelayScheduler) -> DelayService:
    """组装延迟发布服务（请求级主题仓储 + 进程级延迟区 / 时间轮）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        store: 延迟区存储。
        scheduler: 推进器（时间轮与其共用同一实例，发布侧装填后才能被推进）。

    Returns:
        DelayService: 延迟发布服务。
    """
    session = cast("DbSession", uow.session)
    return DelayService(store=store, router=TopicRouter(TopicRepository(session)), wheel=scheduler.wheel)


async def apply_retention(
    *,
    tenant: CurrentTenant,
    uow: UnitOfWork,
    store: MessageStore,
    namespace: str,
    topic: str,
    partition_no: int | None = None,
    key: str | None = None,
) -> None:
    """按主题保留口径清理目标分区到期段（请求驱动，幂等）。

    判定顺序与既有链路一致：命名空间（`350010`）→ 主题（`350001`）→ 分区号（`350009`）→ 清理；
    因此**非法请求不会触发清理**，且错误码与投递链路同源（排障时两侧结论一致）。

    分区号的两种给法（与两条链路对应）：`consume` 直接给 `partition_no`；`publish` 只给 `key`，
    由 `route()` 算出目标分区（同 key 恒同分区，与随后的 `append` 落点一致）。

    Args:
        tenant: 当前租户。
        uow: 平台库工作单元（取主题仓储会话）。
        store: 消息存储（段元数据与起点前移）。
        namespace: 命名空间。
        topic: 主题名。
        partition_no: 目标分区号（`consume` 用）；None 时按 `key` 路由。
        key: 分区键（`publish` 用；可空）。

    Raises:
        MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
        MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        MsgPartitionCountInvalidError: 分区号越界（350009）。
    """
    tenant.ensure_namespace(namespace)
    session = cast("DbSession", uow.session)
    router = TopicRouter(TopicRepository(session))
    resolved = await router.resolve(namespace=namespace, name=topic)
    target = router.partition(resolved, partition_no) if partition_no is not None else router.route(resolved, key)
    await RetentionService(store=store).apply(
        topic_id=int(resolved.id),
        partition_no=target,
        retention_ms=int(resolved.retention_ms),
        retention_bytes=int(resolved.retention_bytes),
    )


async def ensure_member(
    *,
    tenant: CurrentTenant,
    uow: UnitOfWork,
    groups: GroupStore,
    state: DeliveryState,
    request: ConsumeRequest | AckRequest,
) -> None:
    """成员制校验：组存在、成员活跃、分区在该成员分配内（01-4 详细设计 §6.1）。

    校验放在**数据面入口**而非 `DeliveryService`：01-2 / 01-3 的投递语义与停等规则保持不变，
    8B_01 帧入口复用同一个 `GroupCoordinator.ensure_owner()`。

    Args:
        tenant: 当前租户。
        uow: 平台库工作单元（取主题仓储会话）。
        groups: 组存储。
        state: 投递状态（协调器撤销易主分区时清在途）。
        request: 拉取 / 确认请求（两者都带 `group` / `consumer_id` / `partition`）。

    Raises:
        MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
        MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        MsgGroupNotFoundError: 消费组不存在（350013）。
        MsgMemberInvalidError: 成员不在组内、已失效，或分区不在其分配内（350014）。
    """
    session = cast("DbSession", uow.session)
    coordinator = GroupCoordinator(groups=groups, router=TopicRouter(TopicRepository(session)), delivery=state)
    await coordinator.ensure_owner(
        tenant=tenant,
        namespace=request.namespace,
        topic=request.topic,
        group=request.group,
        consumer_id=request.consumer_id,
        partition=request.partition,
    )


def message_item(message: StoredMessage, topic: str, delivery_count: int) -> MessageItem:
    """存储记录 + 投递次数 → 响应明细（跨模块出口：长驻流的 `message` 事件复用同一结构）。

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


async def publish(
    req: PublishRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    delay_store: DelayStoreDep,
    delay_scheduler: DelaySchedulerDep,
) -> ApiResponse[PublishResult]:
    """发布一条消息（缺省立即入日志；带 `delay_level` 则先进延迟区、到期才入日志）。

    需要 msg:publish 权限；`tenant` 取自数据面解析链；主题不存在或未启用返回 350001，
    命名空间非已启用 module_key 返回 350010，载荷非合法 base64 返回 10001。
    延迟发布：`offset` 为 null、返回 `deliver_at` 与 `log_id`；档位非法 10001，延迟区已满 350015。
    首个延迟发布会惰性启动到期推进任务（无延迟消息的进程不起后台任务）。
    入口按主题保留口径清理本分区到期段（幂等；见 01-6）。
    """
    if req.delay_level is not None:
        order = await _delay_service(uow, delay_store, delay_scheduler).publish(tenant=tenant, request=req)
        delay_scheduler.ensure_started()
        return ApiResponse.ok(
            PublishResult(
                message_id=order.message_id,
                log_id=order.log_id,
                topic=req.topic,
                partition=order.partition_no,
                offset=None,
                deliver_at=order.deliver_at,
            )
        )
    await apply_retention(
        tenant=tenant,
        uow=uow,
        store=store,
        namespace=req.namespace,
        topic=req.topic,
        key=req.key,
    )
    message = await delivery_service(uow, store, offsets, dead_letters, state).publish(tenant=tenant, request=req)
    return ApiResponse.ok(
        PublishResult(
            message_id=message.message_id,
            log_id=message.log_id,
            topic=req.topic,
            partition=message.partition_no,
            offset=message.offset,
            deliver_at=None,
        )
    )


async def consume(
    req: ConsumeRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
) -> ApiResponse[ConsumeResult]:
    """拉取一批消息（成员制 + 分区内停等：队头未 ack 时下一次拉取为空，ack 后继续）。

    需要 msg:consume 权限；须先 `join` 并只在分配到的分区上拉取（组内不重复分配）。
    `acks=0` 表示不等确认（返回前位点直接前移，可能丢），同样受成员制约束。
    返回消息在分区内按 offset 升序；超时未 ack 会重投同一条消息（`delivery_count` 递增）；
    死信消息不再投递，其后的消息可继续（顺序性唯一破例）。
    主题不存在 350001，组不存在 350013，成员失效或分区不在其分配内 350014，
    分区号越界 350009，位点已被保留期清理 350003。
    入口先按主题保留口径清理本分区到期段（幂等；见 01-6），因此「位点早于起点」会在同一次请求里显式报 350003。
    """
    await apply_retention(
        tenant=tenant,
        uow=uow,
        store=store,
        namespace=req.namespace,
        topic=req.topic,
        partition_no=req.partition,
    )
    await ensure_member(tenant=tenant, uow=uow, groups=groups, state=state, request=req)
    delivered = await delivery_service(uow, store, offsets, dead_letters, state).consume(tenant=tenant, request=req)
    return ApiResponse.ok(
        ConsumeResult(
            messages=ConcurrentStableList(
                message_item(delivery.message, req.topic, delivery.delivery_count) for delivery in delivered
            )
        )
    )


async def ack(
    req: AckRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
) -> ApiResponse[AckResult]:
    """确认一条消息（位点提交值 = `offset + 1`，提交失败不前移）。

    需要 msg:consume 权限；须是该分区的当前持有者（组不存在 350013，成员失效或分区已易主 350014）。
    位点越界 350002，消息不在投递中 350003，消息已因重投超限转入死信 350007。
    """
    await ensure_member(tenant=tenant, uow=uow, groups=groups, state=state, request=req)
    committed = await delivery_service(uow, store, offsets, dead_letters, state).ack(tenant=tenant, request=req)
    return ApiResponse.ok(AckResult(committed_offset=committed))


@dataclass(frozen=True)
class DataPlaneRoute:
    """一个数据面端点的挂载规格（两条聚合路由共用，防语义与错误码漂移）。"""

    path: str
    """端点相对路径（含前导 `/`，相对聚合路由前缀）。"""

    endpoint: Callable[..., Any]
    """端点实现（同一函数对象挂两处，OpenAPI 生成两份但行为唯一）。"""

    dependencies: Sequence[params.Depends]
    """权限依赖（发布 / 拉取确认分权）。"""

    error_codes: ConcurrentStableList[int]
    """`x-error-codes` 声明（跨仓错误码反向对账）。"""

    frame_type: int
    """帧形态的帧类型编号（03-2；与路径一一对应，闸门据此判类型匹配）。"""

    request_model: type[BaseModel]
    """请求模型（帧形态的必填键与字段白名单直接取自它，不另维护一份字段表）。"""


FrameGateFactory = Callable[[DataPlaneRoute], params.Depends]
"""帧闸门依赖工厂（03-2：`api/frame/gate.py::build_frame_gate` 适配成 `Depends`）。

放在本模块声明、由根路径聚合注入：数据面模块**不反向依赖**帧入口模块（否则形成循环导入）。
"""


DATA_PLANE_ROUTES: ConcurrentStableList[DataPlaneRoute] = ConcurrentStableList(
    [
        DataPlaneRoute(
            path="/publish",
            endpoint=publish,
            dependencies=[_PUBLISH],
            error_codes=PUBLISH_ERROR_CODES,
            frame_type=int(FrameType.PUBLISH),
            request_model=PublishRequest,
        ),
        DataPlaneRoute(
            path="/consume",
            endpoint=consume,
            dependencies=[_CONSUME],
            error_codes=CONSUME_ERROR_CODES,
            frame_type=int(FrameType.CONSUME),
            request_model=ConsumeRequest,
        ),
        DataPlaneRoute(
            path="/ack",
            endpoint=ack,
            dependencies=[_CONSUME],
            error_codes=ACK_ERROR_CODES,
            frame_type=int(FrameType.ACK),
            request_model=AckRequest,
        ),
    ]
)
"""数据面端点规格（**唯一出处**：路径 / 方法 / 权限 / 错误码只在这里写一次）。

`/api/v1/msg/*` 与根路径 `/msg/*` 两处挂载都读它；新增数据面端点或调整错误码时只改这里。
"""


def mount_data_plane(target: BaseRouter, *, frame_gate: FrameGateFactory | None = None) -> None:
    """把数据面三端点挂到给定聚合路由（`/api/v1/msg/*` 与根路径 `/msg/*` 共用）。

    Args:
        target: 目标聚合路由（其 `prefix` 决定最终路径）。
        frame_gate: 帧闸门依赖工厂（只有根路径 `/msg/*` 传；`/api/v1` 保持纯 JSON）。
            传 None 时行为与 03-1 完全一致。
    """
    for spec in DATA_PLANE_ROUTES:
        dependencies: list[params.Depends] = list(spec.dependencies)
        if frame_gate is not None:
            dependencies.append(frame_gate(spec))
        target.add_api_route(
            spec.path,
            spec.endpoint,
            methods=["POST"],
            dependencies=dependencies,
            openapi_extra={"x-error-codes": list(spec.error_codes)},
        )


mount_data_plane(router)
"""本模块路由（挂 `/api/v1` 后即 `/api/v1/msg/{publish,consume,ack}`；纯 JSON，无帧闸门）。"""
