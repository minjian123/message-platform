"""数据面消息接口（`/api/v1/msg/publish`、`/msg/consume`、`/msg/ack`；详细设计 §7）。

- **权限码**：`msg:publish`（发布）、`msg:consume`（拉取与确认）——生产与消费分权（最小权限）。
- **租户**：数据面唯一出口 `get_data_plane_tenant()`（内部 `frame_tenant()`）；请求体不接收 `tenant`。
- **成员制**（01-4）：`consume` / `ack` 必带 `consumer_id`，且组须存在、成员须活跃、分区须在其分配内；
  校验在入口（`ensure_member`）完成，`DeliveryService` 的投递语义与停等规则一行不改。
- **延迟是发布的一个维度**（01-5）：`publish` 按 `delay_level` 分派——给值走 `DelayService`（入延迟区、
  无 offset、惰性启动推进器），缺省走原投递链路；两条分支共用同一份租户与主题校验口径。
- **保留清理是请求驱动**（01-6）：`publish` 与 `consume` 入口各按主题 `retention_ms` / `retention_bytes`
  幂等判定一次并前移可读区间起点（**01-7 起每窗口每（主题, 分区）最多一次**，判定逻辑不变）。
  **放在 API 层而非 `DeliveryService` 内部**：01-2 / 01-3 的投递与停等语义（尤其中 `_collect`）一行不改；
  代价是请求内主题解析多一次（与 01-4 同类遗留）——**该项已由 01-7 的进程内主题快照缓存收敛**。
- **固定成本**（01-7）：主题解析统一经 `topic_router()` 组装（可选接入 `TopicCache`），
  一次请求内的 2~3 次解析收敛为「首次查库、其余命中」。缓存与节流都不改变响应结构与错误码，
  冷热两态逐字段一致（详细设计 §6）。
- **两处挂载、一份规格**（03-1）：端点实现只定义一次，由 `DATA_PLANE_ROUTES` + `mount_data_plane()` 挂到
  两条聚合路由——`/api/v1/msg/*`（既有 HTTP/1.1 数据面，保留作基准对照与回退）与根路径 `/msg/*`
  （HTTP/2 帧入口，03-2 只换 body 编解码）。路径 / 方法 / 权限依赖 / `x-error-codes` **各只写一处**，
  避免两套路径的语义与错误码漂移。
- **数据源**：主题元数据在平台库，会话取 `get_platform_uow`；消息 / 位点 / 死信 / 组 / 延迟区是进程级内存
  实现（`app.state` 注入）。
- **契约**：端点声明 `x-error-codes`，供跨仓 `check_modules` 的错误码反向对账。
"""

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import wraps
from typing import Annotated, Any, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import BizError
from bms_core.core.logging import get_logger
from bms_core.db.session import DbSession, get_platform_uow
from bms_core.db.unit_of_work import UnitOfWork
from bms_core.msgclient.frame import FrameType
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, params
from pydantic import BaseModel

from bms_msg.acl.keys import ACL_ACTION_PUBLISH, ACL_ACTION_SUBSCRIBE
from bms_msg.api.deps import (
    BackpressureMetricsDep,
    BackpressureTuningDep,
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DelaySchedulerDep,
    DelayStoreDep,
    DeliveryCounterDep,
    DeliveryStateDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    PartitionDirectoryDep,
    QuotaServiceDep,
    RateLimiterDep,
    ReplicationDep,
    RetentionThrottleDep,
    StorageGateDep,
    TopicAclServiceDep,
    TopicCacheDep,
    UsageSnapshotDep,
    WaiterDep,
)
from bms_msg.api.frame.guard import CONNECTION_GUARD
from bms_msg.broker.backpressure import BackpressureTuning, InflightUsage, RateLimiter, retry_after_ms
from bms_msg.broker.coordinator import GroupCoordinator
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delay import DelayService, DelayStore
from bms_msg.broker.delivery import Delivery, DeliveryService, DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replication import ReplicationView
from bms_msg.broker.retention import RetentionService, RetentionThrottle
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.scheduler import DelayScheduler
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.waiter import DeliveryWaiter, WaitOutcome
from bms_msg.broker.watermark import StorageGate
from bms_msg.core.config import MS_PER_SECOND
from bms_msg.errors import MsgErrorCode, MsgRateLimitedError, MsgStorageWatermarkExceededError
from bms_msg.metrics import get_traffic_metrics
from bms_msg.quota.service import QuotaService
from bms_msg.repositories.topic import TopicRepository
from bms_msg.schemas.message import (
    AckRequest,
    AckResult,
    ConsumeRequest,
    ConsumeResult,
    MessageItem,
    NackRequest,
    NackResult,
    PublishRequest,
    PublishResult,
)
from bms_msg.tenant.context import CurrentTenant
from bms_msg.usage.counter import DeliveryCounter

__all__ = [
    "ACK_ERROR_CODES",
    "CONSUME_ERROR_CODES",
    "CONSUME_PERMISSION",
    "DATA_PLANE_ROUTES",
    "NACK_ERROR_CODES",
    "PUBLISH_ERROR_CODES",
    "PUBLISH_PERMISSION",
    "DataPlaneRoute",
    "FrameGateFactory",
    "apply_retention",
    "ensure_member",
    "message_item",
    "mount_data_plane",
    "nack",
    "router",
    "topic_router",
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
        int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED),
        int(MsgErrorCode.INSUFFICIENT_REPLICAS),
    ]
)
"""发布端点声明的错误码（`x-error-codes`；01-5 追加延迟区已满 `350015`，03-2 追加超限 `350006`，
02-6 追加存储水位拒绝 `350022`，02-4 追加 ISR 不足 / 提交超时 `350032`）。"""

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

NACK_ERROR_CODES = ConcurrentStableList(
    [
        int(MsgErrorCode.TOPIC_NOT_FOUND),
        int(MsgErrorCode.GROUP_NOT_FOUND),
        int(MsgErrorCode.MEMBER_INVALID),
        int(MsgErrorCode.OFFSET_INVALID),
        int(MsgErrorCode.MESSAGE_NOT_FOUND),
        int(MsgErrorCode.PARTITION_COUNT_INVALID),
        int(MsgErrorCode.NAMESPACE_INVALID),
        int(MsgErrorCode.TENANT_SUSPENDED),
        int(MsgErrorCode.TENANT_NOT_FOUND),
    ]
)
"""拒绝端点声明的错误码（`x-error-codes`；03-3 新增；无「已死信」项——重复上报幂等返回）。"""

router = BaseRouter(
    key="msg_data_messages",
    prefix="/msg",
    tags=["msg-data"],
    dependencies=[Depends(CONNECTION_GUARD)],
)
"""数据面路由（`/api/v1` 挂载时带连接守卫：连接数上限与半开清扫对两处挂载同口径，网关路径不构成绕过口）。

守卫是**路由级依赖**（而非逐路由追加）：`DATA_PLANE_ROUTES` 的两处挂载各自复制同一份依赖集合，
新增端点不必记得补守卫。帧形态的超限拒绝由守卫自己编 error 帧（03-4 详细设计 §5.2）。
"""

PlatformUowDep = Annotated[UnitOfWork, Depends(get_platform_uow)]
"""平台库工作单元依赖（主题元数据归平台库）。"""

_LOGGER = get_logger("bms_msg.data")
"""数据面日志（只出结构化事件：`msg_retention`）。"""


def topic_router(
    uow: UnitOfWork,
    *,
    cache: TopicCache | None,
    tenant_id: int | None,
    directory: PartitionDirectory | None = None,
) -> TopicRouter:
    """组装主题路由（请求级仓储 + 可选进程内快照缓存 + 可选分区目录登记表）。

    跨模块出口：数据面 / 帧入口 / 长驻流复用同一装配口径（缓存键含租户，两者必须同给或同不给）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        cache: 进程内主题快照缓存（None = 不缓存、直接查库）。
        tenant_id: 当前租户主键（缓存键的一部分）。
        directory: 分区目录登记表（02-1；给出即登记，文件存储靠它拼落盘路径）。

    Returns:
        TopicRouter: 主题路由。
    """
    session = cast("DbSession", uow.session)
    return TopicRouter(TopicRepository(session), cache=cache, tenant_id=tenant_id, directory=directory)


def delivery_service(
    uow: UnitOfWork,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    *,
    cache: TopicCache | None,
    tenant_id: int | None,
    backpressure: BackpressureTuning | None = None,
    waiter: DeliveryWaiter | None = None,
    directory: PartitionDirectory | None = None,
    gate: StorageGate | None = None,
    counter: DeliveryCounter | None = None,
    replication: ReplicationView | None = None,
) -> DeliveryService:
    """组装投递服务（请求级主题仓储 + 进程级存储 / 位点 / 死信 / 投递状态 / 背压参数）。

    跨模块出口：帧入口与长驻流复用同一装配口径（不各自 new 一遍，避免语义分叉）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        cache: 进程内主题快照缓存（None = 不缓存）。
        tenant_id: 当前租户主键（缓存键的一部分）。
        backpressure: inflight 双阈值（None 表示不限）。
        waiter: 分区唤醒器（None 表示不唤醒，仅单测直连时出现）。
        directory: 分区目录登记表（02-1）。
        gate: 存储水位闸门（02-6；None 表示不判，内存实现直连与既有用例不受影响）。
        counter: 今日投递计数（05-2；None 表示不计数）。
        replication: 复制运行态视图（02-4；None 表示复制关闭、不夹取）。

    Returns:
        DeliveryService: 投递服务。
    """
    return DeliveryService(
        store=store,
        offsets=offsets,
        dead_letters=dead_letters,
        router=topic_router(uow, cache=cache, tenant_id=tenant_id, directory=directory),
        state=state,
        backpressure=backpressure,
        waiter=waiter,
        gate=gate,
        counter=counter,
        replication=replication,
    )


def _delay_service(
    uow: UnitOfWork,
    store: DelayStore,
    scheduler: DelayScheduler,
    *,
    cache: TopicCache | None,
    tenant_id: int | None,
    directory: PartitionDirectory | None = None,
    gate: StorageGate | None = None,
) -> DelayService:
    """组装延迟发布服务（请求级主题仓储 + 进程级延迟区 / 时间轮）。

    Args:
        uow: 平台库工作单元（取主题仓储会话）。
        store: 延迟区存储。
        scheduler: 推进器（时间轮与其共用同一实例，发布侧装填后才能被推进）。
        cache: 进程内主题快照缓存（None = 不缓存）。
        tenant_id: 当前租户主键（缓存键的一部分）。
        directory: 分区目录登记表（02-1）。
        gate: 存储水位闸门（02-6；None 表示不判）。

    Returns:
        DelayService: 延迟发布服务。
    """
    return DelayService(
        store=store,
        router=topic_router(uow, cache=cache, tenant_id=tenant_id, directory=directory),
        wheel=scheduler.wheel,
        gate=gate,
    )


async def apply_retention(
    *,
    tenant: CurrentTenant,
    uow: UnitOfWork,
    store: MessageStore,
    namespace: str,
    topic: str,
    cache: TopicCache,
    throttle: RetentionThrottle,
    partition_no: int | None = None,
    key: str | None = None,
    directory: PartitionDirectory | None = None,
) -> tuple[int, int]:
    """按主题保留口径清理目标分区到期段（请求驱动，幂等），并回传解析结果。

    判定顺序与既有链路一致：命名空间（`350010`）→ 主题（`350001`）→ 分区号（`350009`）→ 清理；
    因此**非法请求不会触发清理**，且错误码与投递链路同源（排障时两侧结论一致）。

    分区号的两种给法（与两条链路对应）：`consume` 直接给 `partition_no`；`publish` 只给 `key`，
    由 `route()` 算出目标分区（同 key 恒同分区，与随后的 `append` 落点一致）。

    **节流（01-7）**：`throttle.admit()` 判定本窗口是否轮到扫描——同一（主题, 分区）每窗口最多
    执行一次 `RetentionService.apply`；判定与副作用不变，只改频率。节流在判定之后，
    因此非法请求照旧先报错（不会被「本窗口跳过」掩盖）。

    **回传 `(topic_id, partition_no)`**（03-5）：长轮询与长驻流的等待键是 `(topic_id, partition_no)`，
    复用这次解析结果，避免为了拿主键再解析一遍主题。

    Args:
        tenant: 当前租户。
        uow: 平台库工作单元（取主题仓储会话）。
        store: 消息存储（段元数据与起点前移）。
        namespace: 命名空间。
        topic: 主题名。
        cache: 进程内主题快照缓存（本次解析的唯一数据来源）。
        throttle: 清理节流窗口表（进程级单例）。
        partition_no: 目标分区号（`consume` 用）；None 时按 `key` 路由。
        key: 分区键（`publish` 用；可空）。
        directory: 分区目录登记表（02-1；解析即登记，文件存储靠它拼落盘路径）。

    Returns:
        tuple[int, int]: `(主题主键, 目标分区号)`。

    Raises:
        MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
        MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        MsgPartitionCountInvalidError: 分区号越界（350009）。
    """
    tenant.ensure_namespace(namespace)
    router = topic_router(uow, cache=cache, tenant_id=int(tenant.tenant_id), directory=directory)
    resolved = await router.resolve(namespace=namespace, name=topic)
    target = router.partition(resolved, partition_no) if partition_no is not None else router.route(resolved, key)
    topic_id = int(resolved.id)
    started = time.perf_counter()
    if throttle.admit(topic_id=topic_id, partition_no=target):
        outcome = await RetentionService(store=store).apply(
            topic_id=topic_id,
            partition_no=target,
            retention_ms=int(resolved.retention_ms),
            retention_bytes=int(resolved.retention_bytes),
        )
        _LOGGER.info(
            "msg_retention",
            outcome="applied",
            topic_id=topic_id,
            partition=target,
            deleted_segments=outcome.deleted_segments,
            elapsed_ms=round((time.perf_counter() - started) * 1_000, 3),
        )
    else:
        _LOGGER.info(
            "msg_retention",
            outcome="throttled",
            topic_id=topic_id,
            partition=target,
            elapsed_ms=round((time.perf_counter() - started) * 1_000, 3),
        )
    return topic_id, target


async def ensure_member(
    *,
    tenant: CurrentTenant,
    uow: UnitOfWork,
    groups: GroupStore,
    state: DeliveryState,
    cache: TopicCache | None,
    request: ConsumeRequest | AckRequest | NackRequest,
    directory: PartitionDirectory | None = None,
) -> None:
    """成员制校验：组存在、成员活跃、分区在该成员分配内（01-4 详细设计 §6.1）。

    校验放在**数据面入口**而非 `DeliveryService`：01-2 / 01-3 的投递语义与停等规则保持不变，
    8B_01 帧入口复用同一个 `GroupCoordinator.ensure_owner()`。

    Args:
        tenant: 当前租户。
        uow: 平台库工作单元（取主题仓储会话）。
        groups: 组存储。
        state: 投递状态（协调器撤销易主分区时清在途）。
        cache: 进程内主题快照缓存（None = 不缓存）。
        request: 拉取 / 确认请求（两者都带 `group` / `consumer_id` / `partition`）。
        directory: 分区目录登记表（02-1）。

    Raises:
        MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
        MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        MsgGroupNotFoundError: 消费组不存在（350013）。
        MsgMemberInvalidError: 成员不在组内、已失效，或分区不在其分配内（350014）。
    """
    coordinator = GroupCoordinator(
        groups=groups,
        router=topic_router(uow, cache=cache, tenant_id=int(tenant.tenant_id), directory=directory),
        delivery=state,
    )
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

    **载荷不在这里编码 base64**：原始字节放进私有通道，JSON / SSE 序列化时由
    `MessageItem._payload_text` 补位、帧装配直接取原始字节——帧路径因此不再往返一次
    base64（03-3 详细设计 §4.7）。

    Args:
        message: 存储记录。
        topic: 主题名。
        delivery_count: 投递次数。

    Returns:
        MessageItem: 响应明细（含内容类型与压缩标记；载荷走原始字节通道）。
    """
    item = MessageItem(
        message_id=message.message_id,
        topic=topic,
        partition=message.partition_no,
        offset=message.offset,
        key=message.key,
        payload="",
        headers=message.headers,
        trace_id=message.trace.trace_id,
        span_id=message.trace.span_id,
        parent_span_id=message.trace.parent_span_id,
        trace_flags=message.trace.trace_flags,
        delivery_count=delivery_count,
        published_at=message.published_at,
        content_type=message.content_type,
        compression=message.compression,
        uncompressed_len=message.uncompressed_len,
    )
    item.set_raw_payload(message.payload)
    return item


async def _ensure_quota_rate(
    *,
    quota: QuotaService,
    limiter: RateLimiter,
    metrics: BackpressureMetrics,
    tenant: CurrentTenant,
    namespace: str,
    topic: str,
) -> None:
    """生产速率双闸门：静态两级桶（既有）+ 租户配额速率（05-2）。

    Args:
        quota: 配额服务（取当前生效 `produce_rate`）。
        limiter: 生产限流器。
        metrics: 背压指标（记录拒绝）。
        tenant: 当前租户。
        namespace: 命名空间。
        topic: 主题名（静态主题桶键用）。

    Raises:
        MsgRateLimitedError: 任一闸门拒绝（350005 / 429）。
    """
    verdict = limiter.check(tenant=tenant.code, topic=(tenant.code, namespace, topic))
    if not verdict.allowed:
        metrics.record_rate_rejected(scope=verdict.scope, tenant=tenant.code, topic=topic)
        raise MsgRateLimitedError(
            f"生产速率超出配额（{verdict.scope}），请稍后重试",
            retry_after_ms=retry_after_ms(verdict),
        )
    limit = await quota.effective(int(tenant.tenant_id), namespace, "produce_rate")
    quota_verdict = limiter.check_quota(
        tenant=tenant.code,
        namespace=namespace,
        rate_per_second=limit.limit,
    )
    if not quota_verdict.allowed:
        metrics.record_rate_rejected(scope=quota_verdict.scope, tenant=tenant.code, topic=topic)
        raise MsgRateLimitedError(
            f"生产速率超出配额（{namespace or '租户级'}），请稍后重试",
            retry_after_ms=retry_after_ms(quota_verdict),
        )


def _payload_bytes(request: PublishRequest) -> int:
    """取本次发布的消息体字节数（帧走原始字节，JSON 走 base64 估算）。

    Args:
        request: 发布请求。

    Returns:
        int: 字节数。
    """
    raw = request.raw_payload
    if raw is not None:
        return len(raw)
    return len(request.payload) * 3 // 4


async def _ensure_storage_quota(
    *,
    quota: QuotaService,
    usage: UsageSnapshotDep,
    tenant_id: int,
    namespace: str,
    incoming: int,
) -> None:
    """存储配额闸门（在存储水位闸门之前判；硬限已到即 `350022`）。

    Args:
        quota: 配额服务。
        usage: 用量快照（存储字节数）。
        tenant_id: 租户主键。
        namespace: 命名空间。
        incoming: 本次新增字节数。

    Raises:
        MsgStorageWatermarkExceededError: 当前用量 + 本次新增超过硬限（507）。
    """
    limit = await quota.effective(tenant_id, namespace, "storage_bytes")
    if limit.limit <= 0:
        return
    stored = await usage.storage_bytes(tenant_id)
    if stored is None:
        return
    if stored + incoming <= limit.limit:
        return
    raise MsgStorageWatermarkExceededError(
        f"消息存储已达租户配额上限：{stored + incoming}/{limit.limit}",
        data={
            "used_ratio": round((stored + incoming) / limit.limit, 4),
            "threshold": 1.0,
            "volume": "tenant_quota",
            "free_bytes": max(limit.limit - stored, 0),
            "reason": "tenant_quota",
        },
    )


async def _ensure_unacked_quota(
    *,
    quota: QuotaService,
    usage: UsageSnapshotDep,
    tenant_id: int,
) -> bool:
    """未确认配额闸门（已到硬限返回 True，调用方返空批 + `paused=true`）。

    Args:
        quota: 配额服务。
        usage: 用量快照。
        tenant_id: 租户主键。

    Returns:
        bool: 已达硬限返回 True。
    """
    limit = await quota.effective(tenant_id, "", "unacked_count")
    if limit.limit <= 0:
        return False
    return await usage.unacked_count(tenant_id) >= limit.limit


def _track_data_plane(operation: str) -> Callable[[Any], Any]:
    """数据面埋点装饰器（06-3-1）：成功记流量 / 延迟，失败按原因记错误。

    指标异常必须吞掉（绝不因采集失败影响请求）；错误只捕获 `BizError`，
    不把取消 / 系统异常算成业务失败。

    Args:
        operation: `publish` 或 `consume`。

    Returns:
        Callable[[Any], Any]: 函数装饰器。
    """

    def decorator(func: Any) -> Any:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            started = time.monotonic()
            try:
                response = await func(*args, **kwargs)
            except BizError as exc:
                try:
                    traffic = get_traffic_metrics()
                    if operation == "publish":
                        traffic.record_publish_error(exc=exc)
                    else:
                        traffic.record_consume_error(exc=exc)
                except Exception:
                    pass
                raise
            try:
                tenant = kwargs.get("tenant")
                req = kwargs.get("req")
                if tenant is not None and req is not None:
                    duration_ms = (time.monotonic() - started) * 1000.0
                    traffic = get_traffic_metrics()
                    if operation == "publish":
                        traffic.record_publish(
                            tenant=tenant.code,
                            topic=req.topic,
                            size=_payload_bytes(req),
                            duration_ms=duration_ms,
                        )
                    else:
                        data = getattr(response, "data", None)
                        count = len(getattr(data, "messages", ()) or ())
                        traffic.record_consume(
                            tenant=tenant.code,
                            topic=req.topic,
                            count=count,
                            duration_ms=duration_ms,
                        )
            except Exception:
                pass
            return response

        return wrapper

    return decorator


def _log_published(
    *,
    request: PublishRequest,
    message_id: str,
    log_id: str,
    partition: int,
    offset: int | None,
    delayed: bool,
) -> None:
    """写一条发布事件日志，带上消息自带的 trace 字段（06-4）。

    **为什么只在发布路径打**：一条消息一个 trace，日志量恒定；消费是批量、批内可能多 trace，
    逐条打会把日志量放大到「条数」，故消费侧只回契约字段、由 SDK 在 OTel 侧串链。

    **为什么空值不写 key**：日志器对上下文字段用 `setdefault` 注入，显式传 `None` 会把
    自动注入的 `trace_id` 顶掉；故只在字段有值时放入（`span_id` 等新字段同理，避免噪声键）。

    Args:
        request: 发布请求（取 trace 四字段）。
        message_id: 消息身份。
        log_id: 记录身份。
        partition: 落到的分区号。
        offset: 分区内序号（延迟发布为 None）。
        delayed: 是否延迟发布（先进延迟区）。
    """
    fields: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    fields.set("message_id", message_id)
    fields.set("log_id", log_id)
    fields.set("partition", partition)
    fields.set("offset", offset)
    fields.set("delayed", delayed)
    for name, value in (
        ("trace_id", request.trace_id),
        ("span_id", request.span_id),
        ("parent_span_id", request.parent_span_id),
        ("trace_flags", request.trace_flags),
    ):
        if value is not None:
            fields.set(name, value)
    _LOGGER.info("msg_message_published", **dict(fields.items()))


@_track_data_plane("publish")
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
    limiter: RateLimiterDep,
    metrics: BackpressureMetricsDep,
    cache: TopicCacheDep,
    throttle: RetentionThrottleDep,
    directory: PartitionDirectoryDep,
    gate: StorageGateDep,
    quota: QuotaServiceDep,
    usage: UsageSnapshotDep,
    counter: DeliveryCounterDep,
    acl: TopicAclServiceDep,
    replication: ReplicationDep,
) -> ApiResponse[PublishResult]:
    """发布一条消息（缺省立即入日志；带 `delay_level` 则先进延迟区、到期才入日志）。

    需要 msg:publish 权限；`tenant` 取自数据面解析链；主题不存在或未启用返回 350001，
    命名空间非已启用 module_key 返回 350010，载荷非合法 base64 返回 10001。
    延迟发布：`offset` 为 null、返回 `deliver_at` 与 `log_id`；档位非法 10001，延迟区已满 350015。
    首个延迟发布会惰性启动到期推进任务（无延迟消息的进程不起后台任务）。
    入口按主题保留口径清理本分区到期段（幂等；见 01-6）。
    **首次发布懒初始化配额缺省行**（05-2）；**限流在两条分支之前**（03-5 + 05-2）：
    延迟发布同样是生产，不限会让「延迟档」成为绕过限流的旁路；超出速率返回 350005（429 + `Retry-After`）。
    **存储配额在存储水位之前判**（05-2 / 02-6）：超租户存储硬限返回 350022（507，
    `data.reason=tenant_quota`）；盘满同样 350022（`data.reason=watermark`）。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_PUBLISH,
    )
    await quota.ensure_initialized(int(tenant.tenant_id))
    await _ensure_quota_rate(
        quota=quota,
        limiter=limiter,
        metrics=metrics,
        tenant=tenant,
        namespace=req.namespace,
        topic=req.topic,
    )
    await _ensure_storage_quota(
        quota=quota,
        usage=usage,
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        incoming=_payload_bytes(req),
    )
    if req.delay_level is not None:
        order = await _delay_service(
            uow,
            delay_store,
            delay_scheduler,
            cache=cache,
            tenant_id=int(tenant.tenant_id),
            directory=directory,
            gate=gate,
        ).publish(tenant=tenant, request=req)
        delay_scheduler.ensure_started()
        _log_published(
            request=req,
            message_id=order.message_id,
            log_id=order.log_id,
            partition=order.partition_no,
            offset=None,
            delayed=True,
        )
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
        cache=cache,
        throttle=throttle,
        key=req.key,
        directory=directory,
    )
    message = await delivery_service(
        uow,
        store,
        offsets,
        dead_letters,
        state,
        cache=cache,
        tenant_id=int(tenant.tenant_id),
        directory=directory,
        gate=gate,
        counter=counter,
        replication=replication,
    ).publish(tenant=tenant, request=req)
    _log_published(
        request=req,
        message_id=message.message_id,
        log_id=message.log_id,
        partition=message.partition_no,
        offset=message.offset,
        delayed=False,
    )
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


def _consume_result(
    delivered: ConcurrentStableList[Delivery],
    req: ConsumeRequest,
    *,
    paused: bool,
    usage: InflightUsage,
) -> ApiResponse[ConsumeResult]:
    """把投递结果装配成响应（拉取端点与长轮询共用一处，字段口径不分叉）。

    Args:
        delivered: 本次投递（按 offset 升序）。
        req: 拉取请求（取主题名回显）。
        paused: 是否因 inflight 达阈值而暂停。
        usage: 当前在途用量。

    Returns:
        ApiResponse[ConsumeResult]: 拉取响应。
    """
    return ApiResponse.ok(
        ConsumeResult(
            messages=ConcurrentStableList(
                message_item(delivery.message, req.topic, delivery.delivery_count) for delivery in delivered
            ),
            paused=paused,
            inflight_messages=usage.messages,
            inflight_bytes=usage.bytes,
        )
    )


@_track_data_plane("consume")
async def consume(
    req: ConsumeRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
    waiter: WaiterDep,
    backpressure: BackpressureTuningDep,
    metrics: BackpressureMetricsDep,
    cache: TopicCacheDep,
    throttle: RetentionThrottleDep,
    directory: PartitionDirectoryDep,
    quota: QuotaServiceDep,
    usage: UsageSnapshotDep,
    counter: DeliveryCounterDep,
    acl: TopicAclServiceDep,
    replication: ReplicationDep,
) -> ApiResponse[ConsumeResult]:
    """拉取一批消息（成员制 + 分区内停等 + 长轮询 + inflight 双阈值）。

    需要 msg:consume 权限；须先 `join` 并只在分配到的分区上拉取（组内不重复分配）。
    `acks=0` 表示不等确认（返回前位点直接前移，可能丢），同样受成员制约束。
    返回消息在分区内按 offset 升序；超时未 ack 会重投同一条消息（`delivery_count` 递增）；
    死信消息不再投递，其后的消息可继续（顺序性唯一破例）。
    主题不存在 350001，组不存在 350013，成员失效或分区不在其分配内 350014，
    分区号越界 350009，位点已被保留期清理 350003。
    入口先按主题保留口径清理本分区到期段（幂等；见 01-6），因此「位点早于起点」会在同一次请求里显式报 350003。

    **长轮询（03-5）**：拉空后挂起到「有消息」或 `max_wait_ms` 到期；每轮都重跑成员校验
    （成员可能在挂起期间超时）。挂起数到全局上限时立即返空（不报错）。
    **inflight 阈值（03-5）**：已达阈值时不拉、直接返空 + `paused=true`；ack / nack / 死信归还后立即恢复。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_SUBSCRIBE,
    )
    topic_id, partition_no = await apply_retention(
        tenant=tenant,
        uow=uow,
        store=store,
        namespace=req.namespace,
        topic=req.topic,
        cache=cache,
        throttle=throttle,
        partition_no=req.partition,
        directory=directory,
    )
    state_key = (req.group, topic_id, partition_no)
    wait_key = (topic_id, partition_no)
    limit = backpressure.limit
    measured = state.usage(state_key)
    if backpressure.enabled and measured.exhausted(limit):
        metrics.observe_inflight(tenant=tenant.code, topic=req.topic, usage=measured)
        return _consume_result(ConcurrentStableList(), req, paused=True, usage=measured)
    deadline = time.monotonic() + req.max_wait_ms / MS_PER_SECOND
    while True:
        # 版本号必须在拉取**之前**取：拉取期间到达的消息靠版本差判定，不必睡满超时。
        version = waiter.version(wait_key)
        await ensure_member(
            tenant=tenant, uow=uow, groups=groups, state=state, cache=cache, request=req, directory=directory
        )
        if await _ensure_unacked_quota(quota=quota, usage=usage, tenant_id=int(tenant.tenant_id)):
            measured = state.usage(state_key)
            metrics.observe_inflight(tenant=tenant.code, topic=req.topic, usage=measured)
            return _consume_result(ConcurrentStableList(), req, paused=True, usage=measured)
        delivered = await delivery_service(
            uow,
            store,
            offsets,
            dead_letters,
            state,
            cache=cache,
            tenant_id=int(tenant.tenant_id),
            backpressure=backpressure,
            waiter=waiter,
            directory=directory,
            counter=counter,
            replication=replication,
        ).consume(tenant=tenant, request=req)
        measured = state.usage(state_key)
        metrics.observe_inflight(tenant=tenant.code, topic=req.topic, usage=measured)
        paused = backpressure.enabled and measured.exhausted(limit)
        if delivered:
            return _consume_result(delivered, req, paused=paused, usage=measured)
        remaining_ms = int((deadline - time.monotonic()) * MS_PER_SECOND)
        if req.max_wait_ms <= 0 or remaining_ms <= 0:
            return _consume_result(ConcurrentStableList(), req, paused=paused, usage=measured)
        outcome = await waiter.wait(wait_key, version=version, timeout_ms=remaining_ms)
        metrics.record_wait(outcome)
        if outcome is WaitOutcome.REJECTED:
            return _consume_result(ConcurrentStableList(), req, paused=paused, usage=measured)


async def ack(
    req: AckRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
    waiter: WaiterDep,
    cache: TopicCacheDep,
    directory: PartitionDirectoryDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[AckResult]:
    """确认一条消息（位点提交值 = `offset + 1`，提交失败不前移）。

    需要 msg:consume 权限；须是该分区的当前持有者（组不存在 350013，成员失效或分区已易主 350014）。
    位点越界 350002，消息不在投递中 350003，消息已因重投超限转入死信 350007。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_SUBSCRIBE,
    )
    await ensure_member(
        tenant=tenant, uow=uow, groups=groups, state=state, cache=cache, request=req, directory=directory
    )
    committed = await delivery_service(
        uow,
        store,
        offsets,
        dead_letters,
        state,
        cache=cache,
        tenant_id=int(tenant.tenant_id),
        waiter=waiter,
        directory=directory,
    ).ack(tenant=tenant, request=req)
    return ApiResponse.ok(AckResult(committed_offset=committed))


async def nack(
    req: NackRequest,
    tenant: DataPlaneTenantDep,
    uow: PlatformUowDep,
    store: MessageStoreDep,
    offsets: OffsetStoreDep,
    dead_letters: DeadLetterQueueDep,
    state: DeliveryStateDep,
    groups: GroupStoreDep,
    waiter: WaiterDep,
    cache: TopicCacheDep,
    directory: PartitionDirectoryDep,
    acl: TopicAclServiceDep,
) -> ApiResponse[NackResult]:
    """拒绝一条消息（转死信 + 位点前移；03-3 详细设计 §5.2）。

    需要 msg:consume 权限（拿消息的人才能拒消息）；须是该分区的当前持有者（组不存在 350013，
    成员失效或分区已易主 350014）。位点越界 350002，消息不在投递中 350003。
    已死信的 offset 重复上报**幂等**（返回当前位点，不重复入队），因此本端点不声明 `350007`。
    """
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=req.namespace,
        topic=req.topic,
        action=ACL_ACTION_SUBSCRIBE,
    )
    await ensure_member(
        tenant=tenant, uow=uow, groups=groups, state=state, cache=cache, request=req, directory=directory
    )
    committed = await delivery_service(
        uow,
        store,
        offsets,
        dead_letters,
        state,
        cache=cache,
        tenant_id=int(tenant.tenant_id),
        waiter=waiter,
        directory=directory,
    ).nack(tenant=tenant, request=req)
    return ApiResponse.ok(NackResult(committed_offset=committed))


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
        DataPlaneRoute(
            path="/nack",
            endpoint=nack,
            dependencies=[_CONSUME],
            error_codes=NACK_ERROR_CODES,
            frame_type=int(FrameType.NACK),
            request_model=NackRequest,
        ),
    ]
)
"""数据面端点规格（**唯一出处**：路径 / 方法 / 权限 / 错误码只在这里写一次）。

`/api/v1/msg/*` 与根路径 `/msg/*` 两处挂载都读它；新增数据面端点或调整错误码时只改这里。
"""


def mount_data_plane(target: BaseRouter, *, frame_gate: FrameGateFactory | None = None) -> None:
    """把数据面端点挂到给定聚合路由（`/api/v1/msg/*` 与根路径 `/msg/*` 共用）。

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
"""本模块路由（挂 `/api/v1` 后即 `/api/v1/msg/{publish,consume,ack,nack}`；纯 JSON，无帧闸门）。"""
