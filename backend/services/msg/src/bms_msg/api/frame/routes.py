"""根路径数据面入口（HTTP/2 帧入口，任务 03-1 / 03-5）。

四件事：

1. **复用数据面端点**：`/msg/publish`、`/msg/consume`、`/msg/ack`、`/msg/nack` 直接调 `mount_data_plane()` 挂载，
   与 `/api/v1/msg/*` **同一个实现、同一份错误码声明**——HTTP/2 下先跑通同一套语义，03-2 再把 body
   换成二进制帧（路径与网关口径此后不再动）。
2. **`GET /msg/subscribe`**：SSE 长驻流（server streaming）。内部逐批调用 `DeliveryService.consume`，
   成员制 / inflight / 可见性超时重投 / 分区内停等与拉取端点**完全同口径**——流只是把「多次拉取」
   串起来，不是第二套投递语义。
3. **`POST /msg/subscribe`**：同一条订阅的**帧流形态**（03-5）：请求帧带参数，响应是 `open` 帧 + 逐条
   `message` 帧 + 正文 EOF，供 SDK 走纯二进制通道。
4. **`GET /msg/connection`**：连接元数据只读自报（协议版本 / 承载方式 / 客户端地址），排障第一问。

**流的边界（写死在这里，别在实现里漂）**：

- 预检（保留期清理 + 主题 / 分区 / 成员）在**进流之前**做完——失败是普通错误响应（状态码与错误码
  与 `/msg/consume` 一致）；一旦发出 `open` 事件 / 帧，状态码已是 200，后续错误只能走 `error`。
- 无消息时按**事件唤醒**等待（03-5）：`waiter.wait(...)` 到「有新消息」或超时（`min(max_wait_ms, 保活间隔,
  剩余时长)`）；`max_wait_ms == 0` 时回落固定短间隔（`poll_interval_ms`），保持 03-1 的口径可用。
- 结束条件三选一：客户端断开 / `RST_STREAM`、时长上限（`close(deadline)`）、成员撤销或分区易主
  （`error` + `close(member_lost)`）。
- 取消时**只释放流级资源**（登记表登记项、协程、每轮短会话、未完成的预取任务）；`DeliveryState.inflight`
  属消息语义，不随流清理——已投未 ack 的消息必须仍可 ack / 超时重投。

**两种编码共用一条核心**（`_stream_core`）：等待 / 背压 / 预取 / 指标只在核心写一遍，SSE 与帧流各自
只做编码，避免两条路径的等待语义分叉。
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Annotated, cast

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import BizError, ParamError
from bms_core.msgclient.frame import FrameType, decode_frame
from bms_core.permission.base import require_permission
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Query, Request, params
from fastapi.responses import StreamingResponse

from bms_msg.acl.keys import ACL_ACTION_SUBSCRIBE
from bms_msg.api.data.messages import (
    CONSUME_ERROR_CODES,
    CONSUME_PERMISSION,
    DataPlaneRoute,
    apply_retention,
    delivery_service,
    ensure_member,
    message_item,
    mount_data_plane,
    topic_router,
)
from bms_msg.api.deps import (
    BackpressureMetricsDep,
    BackpressureTuningDep,
    ConnectionRegistryDep,
    ConnectionTuningDep,
    DataPlaneTenantDep,
    DeadLetterQueueDep,
    DeliveryStateDep,
    GroupStoreDep,
    MessageStoreDep,
    OffsetStoreDep,
    PartitionDirectoryDep,
    ReplicationDep,
    RetentionThrottleDep,
    StreamRegistryDep,
    StreamTuningDep,
    TopicAclServiceDep,
    TopicCacheDep,
    UowFactoryDep,
    WaiterDep,
)
from bms_msg.api.frame.gate import build_frame_gate
from bms_msg.api.frame.guard import (
    CONNECTION_GUARD,
    CONNECTION_INFO_GUARD,
    ConnectionInfoRecordDep,
    connection_info,
)
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame, message_frame, stream_open_frame
from bms_msg.broker.backpressure import BackpressureTuning
from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.metrics import BackpressureMetrics
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.replay import UowFactory
from bms_msg.broker.replication import ReplicationView
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import (
    StreamRegistry,
    StreamTuning,
    sse_comment,
    sse_event,
)
from bms_msg.broker.waiter import DeliveryWaiter, WaitOutcome
from bms_msg.core.config import (
    ACKS_AT_LEAST_ONCE,
    MS_PER_SECOND,
    WINDOW_STALL_THRESHOLD_MS,
)
from bms_msg.errors import (
    MsgError,
    MsgErrorCode,
    MsgFrameInvalidError,
    MsgTenantNotFoundError,
)
from bms_msg.schemas.message import ConsumeRequest, MessageItem
from bms_msg.schemas.transport import (
    ConnectionInfo,
    StreamEvent,
    StreamEventKind,
    StreamOpen,
    SubscribeQuery,
    close_event,
    error_event,
    keepalive_event,
    message_event,
    open_event,
)
from bms_msg.tenant.context import CurrentTenant

__all__ = ["router", "stream_events", "stream_frames"]

router = BaseRouter(
    key="msg_frame",
    prefix="/msg",
    tags=["msg-frame"],
    dependencies=[Depends(CONNECTION_GUARD)],
)
"""根路径数据面聚合路由（挂载前缀为空 → 最终为 `/msg/*`）。

连接守卫是**路由级依赖**：数据面四端点（复用规格挂上来的）、`/msg/subscribe` 的两种形态
全部先登记连接、判连接数上限、顺带清扫过期连接（03-4 详细设计 §5.2）。
`GET /msg/connection` 不在此路由内——它是「打满时也要能查」的排障入口，见 `connection_router`。
"""

connection_router = BaseRouter(
    key="msg_connection",
    prefix="/msg",
    tags=["msg-frame"],
    dependencies=[Depends(CONNECTION_INFO_GUARD)],
)
"""连接自报路由（`GET /msg/connection`）：登记与清扫照旧，**不判**连接数上限（排障入口）。"""


def _frame_gate(spec: DataPlaneRoute) -> params.Depends:
    """把帧闸门适配成路由依赖（只有根路径 `/msg/*` 挂；`/api/v1` 保持纯 JSON）。

    Args:
        spec: 数据面端点规格（提供帧类型与请求模型）。

    Returns:
        params.Depends: 该端点的帧闸门依赖。
    """
    return Depends(build_frame_gate(frame_type=spec.frame_type, request_model=spec.request_model))


mount_data_plane(router, frame_gate=_frame_gate)
"""复用数据面端点（与 `/api/v1/msg/*` 同一规格），并挂 `application/x-msg-frame` 帧形态。"""

_CONSUME = Depends(require_permission(CONSUME_PERMISSION))
"""长驻流权限依赖（与拉取 / 确认同码：消费即持有 `msg:consume`）。"""

_SUBSCRIBE_ERROR_CODES = ConcurrentStableList([*CONSUME_ERROR_CODES, int(MsgErrorCode.STREAM_LIMIT_EXCEEDED)])
"""长驻流错误码声明：与拉取同码 + 追加 `350021`（单连接长驻流超限；拉取端点不会抛它，故不共用清单）。"""

_MEMBER_LOST_CODES = frozenset({350013, 350014})
"""流内「成员消失」类码（组不存在 / 成员失效或分区易主）：关闭原因记 `member_lost`。

码值直接写字面量而非引枚举：`frozenset` 与枚举混用会让「是不是同一码」的判定分散在两处；
这两码的语义在本文件只用一次，写值与注释足够（码表唯一出处仍是 `errors.py`）。
"""

_STREAM_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
"""长驻流的公共响应头：禁缓存、禁代理缓冲（否则 SSE / 帧流会被网关攒批）。"""

_FRAME_REQUIRED_HEADERS = ("request_id", "namespace", "topic", "group", "consumer_id", "partition")
"""帧流请求的必填头（缺键属协议层 `350018`；键在而值形态非法属契约层 `10001`）。"""


def _consume_request(query: SubscribeQuery, *, max_messages: int | None = None) -> ConsumeRequest:
    """长驻流参数 → 拉取请求（语义与 `/msg/consume` 完全一致）。

    `max_wait_ms` 传 0：**等待由流自己在本层做**（`waiter.wait`），不叠加拉取端点的长轮询——
    否则一次「拉空」会先在流里等 30s 再返回，保活注释行与到期判定都会被一起挂住。

    Args:
        query: 长驻流参数。
        max_messages: 本轮条数（缺省取查询参数；预取时按 `prefetch` 收窄）。

    Returns:
        ConsumeRequest: 拉取请求（`acks=1`，缺省至少一次语义）。
    """
    return ConsumeRequest(
        namespace=query.namespace,
        topic=query.topic,
        group=query.group,
        consumer_id=query.consumer_id,
        partition=query.partition,
        max_messages=query.max_messages if max_messages is None else max_messages,
        acks=ACKS_AT_LEAST_ONCE,
        max_wait_ms=0,
    )


async def _resolve_ids(
    *,
    uow_factory: UowFactory,
    tenant: CurrentTenant,
    query: SubscribeQuery,
    cache: TopicCache,
    directory: PartitionDirectory | None = None,
) -> tuple[int, int]:
    """解析 `(topic_id, partition_no)`（等待键需要）。

    HTTP 端点一律用预检结果（`apply_retention` 已解析过），此函数只服务「直调生成器」的调用方
    （用例 / 将来的嵌入式调用）——不重复解析是常态，兜底解析是例外。

    Args:
        uow_factory: 工作单元工厂。
        tenant: 当前租户。
        query: 长驻流参数。
        cache: 进程内主题快照缓存（解析结果与预检共用同一份，命中不查库）。
        directory: 分区目录登记表（02-1；解析即登记）。

    Returns:
        tuple[int, int]: 主题主键与分区号。

    Raises:
        MsgError: 命名空间 / 主题 / 分区号非法（与拉取端点同码）。
    """
    tenant.ensure_namespace(query.namespace)
    async with uow_factory() as uow:
        router = topic_router(uow, cache=cache, tenant_id=int(tenant.tenant_id), directory=directory)
        resolved = await router.resolve(namespace=query.namespace, name=query.topic)
        return int(resolved.id), router.partition(resolved, query.partition)


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
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    cache: TopicCache,
    directory: PartitionDirectory | None = None,
    replication: ReplicationView | None = None,
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
        waiter: 分区唤醒器（额度归还后唤醒本流）。
        backpressure: inflight 双阈值。
        cache: 进程内主题快照缓存（每轮成员制与拉取共用，命中不查库）。
        directory: 分区目录登记表（02-1）。
        replication: 复制运行态视图（02-4；None 表示复制关闭）。

    Returns:
        ConcurrentStableList[MessageItem]: 本批消息（可能为空）。

    Raises:
        MsgError: 主题不存在 / 组不存在 / 成员失效 / 分区号越界等（由调用方转 `error` 事件）。
    """
    async with uow_factory() as uow:
        await ensure_member(
            tenant=tenant, uow=uow, groups=groups, state=state, cache=cache, request=request, directory=directory
        )
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
            replication=replication,
        ).consume(tenant=tenant, request=request)
    return ConcurrentStableList(
        message_item(delivery.message, request.topic, delivery.delivery_count) for delivery in delivered
    )


def wait_timeout_ms(*, now: float, deadline: float, last_emit: float, tuning: StreamTuning) -> int:
    """算本次等待时长（取三者最小值，至少 1ms）。

    三者都要管：`max_wait_ms`（长轮询语义）、剩余总时长（到期要发 close）、距上次发送的保活间隔
    （否则保活会被长等待吞掉，代理会先把连接掐了）。

    Args:
        now: 当前 `monotonic` 秒。
        deadline: 流到期时刻（`monotonic` 秒）。
        last_emit: 上次发送时刻（`monotonic` 秒）。
        tuning: 流参数。

    Returns:
        int: 等待毫秒数（≥1）。
    """
    total_ms = int((deadline - now) * MS_PER_SECOND)
    keepalive_ms = int(tuning.keepalive_ms - (now - last_emit) * MS_PER_SECOND)
    return max(1, min(tuning.max_wait_ms, total_ms, max(keepalive_ms, 1)))


async def _stream_core(
    *,
    uow_factory: UowFactory,
    registry: StreamRegistry,
    connections: ConnectionRegistry,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    tenant: CurrentTenant,
    query: SubscribeQuery,
    tuning: StreamTuning,
    connection_key: str,
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    metrics: BackpressureMetrics,
    cache: TopicCache,
    directory: PartitionDirectory | None = None,
    topic_id: int | None = None,
    partition_no: int | None = None,
    replication: ReplicationView | None = None,
) -> AsyncGenerator[StreamEvent]:
    """长驻流核心：等待 / 背压 / 预取 / 指标只在这里写一遍（SSE 与帧流只做编码）。

    生命周期：`open` → 逐批 `message`（空闲时 `keepalive`）→ `close`；客户端断开时
    `close(client_closed)` 后原样抛出取消（取消语义不能被吞）。

    Args:
        uow_factory: 工作单元工厂（每轮短会话）。
        registry: 流登记表（`finally` 里注销，用例据此断言「断开即释放」）。
        connections: 连接登记表（`finally` 里归还长驻流名额）。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        tenant: 当前租户。
        query: 长驻流参数。
        tuning: 流参数。
        connection_key: 所属连接的派生键（名额归还与此同键）。
        waiter: 分区唤醒器。
        backpressure: inflight 双阈值。
        metrics: 背压指标收集器。
        cache: 进程内主题快照缓存（预检 / 兜底解析 / 每轮拉取共用）。
        directory: 分区目录登记表（02-1）。
        topic_id: 主题主键（预检已解析时给值）。
        partition_no: 分区号（预检已解析时给值）。
        replication: 复制运行态视图（02-4；None 表示复制关闭）。

    Yields:
        StreamEvent: 流事件（编码由调用方决定）。
    """
    request = _consume_request(query)
    if topic_id is None or partition_no is None:
        topic_id, partition_no = await _resolve_ids(
            uow_factory=uow_factory, tenant=tenant, query=query, cache=cache, directory=directory
        )
    wait_key = (topic_id, partition_no)
    budget_key = (query.group, topic_id, partition_no)
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
        connection_key=connection_key,
        tenant_id=int(tenant.tenant_id),
    )
    reason = "error"
    pending: asyncio.Task[ConcurrentStableList[MessageItem]] | None = None
    try:
        yield open_event(
            StreamOpen(
                stream_id=stream_id,
                max_seconds=query.max_seconds,
                max_wait_ms=tuning.max_wait_ms,
                poll_interval_ms=tuning.poll_interval_ms,
                prefetch=tuning.prefetch,
                inflight_messages=backpressure.inflight_messages,
                inflight_bytes=backpressure.inflight_bytes,
            )
        )
        while True:
            if registry.is_cancelled(stream_id):
                reason = "tenant_cancelled"
                yield close_event(reason)
                return
            if time.monotonic() >= deadline:
                reason = "deadline"
                yield close_event(reason)
                return
            # 版本号必须在拉取**之前**取：拉取期间到达的消息靠版本差判定，不必睡满超时。
            version = waiter.version(wait_key)
            try:
                if pending is not None:
                    batch = await pending
                    pending = None
                else:
                    batch = await _pull(
                        uow_factory=uow_factory,
                        store=store,
                        offsets=offsets,
                        dead_letters=dead_letters,
                        state=state,
                        groups=groups,
                        tenant=tenant,
                        request=request,
                        waiter=waiter,
                        backpressure=backpressure,
                        cache=cache,
                        directory=directory,
                        replication=replication,
                    )
            except MsgError as exc:
                reason = "member_lost" if exc.code in _MEMBER_LOST_CODES else "error"
                yield error_event(exc.code, str(exc))
                yield close_event(reason)
                return
            usage = state.usage(budget_key)
            metrics.observe_inflight(tenant=tenant.code, topic=query.topic, usage=usage)
            if batch:
                if tuning.prefetch > 0:
                    pending = asyncio.create_task(
                        _pull(
                            uow_factory=uow_factory,
                            store=store,
                            offsets=offsets,
                            dead_letters=dead_letters,
                            state=state,
                            groups=groups,
                            tenant=tenant,
                            request=_consume_request(query, max_messages=min(tuning.prefetch, query.max_messages)),
                            waiter=waiter,
                            backpressure=backpressure,
                            cache=cache,
                            directory=directory,
                            replication=replication,
                        )
                    )
                mark = time.monotonic()
                for item in batch:
                    yield message_event(item)
                    resumed = time.monotonic()
                    if (resumed - mark) * MS_PER_SECOND >= WINDOW_STALL_THRESHOLD_MS:
                        # 两次 yield 之间等了很久 ⇒ 传输层在等客户端读（窗口耗尽）；不忙等，只记账。
                        metrics.record_window_stall(tenant=tenant.code, topic=query.topic)
                    mark = resumed
                    last_emit = resumed
                continue
            timeout_ms = wait_timeout_ms(now=time.monotonic(), deadline=deadline, last_emit=last_emit, tuning=tuning)
            if tuning.max_wait_ms <= 0:
                await asyncio.sleep(tuning.poll_interval_ms / MS_PER_SECOND)
            else:
                outcome = await waiter.wait(wait_key, version=version, timeout_ms=timeout_ms)
                metrics.record_wait(outcome)
                if outcome is WaitOutcome.REJECTED:
                    # 挂起数到上限：不排队，退一个兜底间隔再来（不把「等的人太多」变成错误）。
                    await asyncio.sleep(tuning.poll_interval_ms / MS_PER_SECOND)
            if (time.monotonic() - last_emit) * MS_PER_SECOND >= tuning.keepalive_ms:
                last_emit = time.monotonic()
                yield keepalive_event()
    except asyncio.CancelledError, GeneratorExit:
        # `CancelledError`：客户端 RST_STREAM / 连接断开；`GeneratorExit`：服务端关闭生成器（如停机）。
        # 两者对外都是「客户端走了」，关闭原因同为 client_closed；这里只登记原因并**原样抛出**，
        # 取消语义不能被吞（吞了会让上层以为流是正常结束的）。
        reason = "client_closed"
        raise
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        registry.close(stream_id, reason=reason)
        connections.close_stream(connection_key)


async def stream_events(
    *,
    uow_factory: UowFactory,
    registry: StreamRegistry,
    connections: ConnectionRegistry,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    tenant: CurrentTenant,
    query: SubscribeQuery,
    tuning: StreamTuning,
    connection_key: str,
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    metrics: BackpressureMetrics,
    cache: TopicCache,
    directory: PartitionDirectory | None = None,
    topic_id: int | None = None,
    partition_no: int | None = None,
    replication: ReplicationView | None = None,
) -> AsyncGenerator[bytes]:
    """SSE 编码器（`text/event-stream`；核心见 `_stream_core`）。

    Args:
        uow_factory: 工作单元工厂。
        registry: 流登记表。
        connections: 连接登记表。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        tenant: 当前租户。
        query: 长驻流参数。
        tuning: 流参数。
        connection_key: 所属连接的派生键。
        waiter: 分区唤醒器。
        backpressure: inflight 双阈值。
        metrics: 背压指标收集器。
        cache: 进程内主题快照缓存（每轮成员制与拉取共用）。
        directory: 分区目录登记表（02-1）。
        topic_id: 主题主键（预检已解析时给值）。
        partition_no: 分区号（预检已解析时给值）。
        replication: 复制运行态视图（02-4；None 表示复制关闭）。

    Yields:
        bytes: SSE 事件（`event: <名>\\ndata: <单行 JSON>\\n\\n`）或注释行。
    """
    core = _stream_core(
        uow_factory=uow_factory,
        registry=registry,
        connections=connections,
        store=store,
        offsets=offsets,
        dead_letters=dead_letters,
        state=state,
        groups=groups,
        tenant=tenant,
        query=query,
        tuning=tuning,
        connection_key=connection_key,
        waiter=waiter,
        backpressure=backpressure,
        metrics=metrics,
        cache=cache,
        directory=directory,
        topic_id=topic_id,
        partition_no=partition_no,
        replication=replication,
    )
    try:
        async for event in core:
            if event.kind is StreamEventKind.OPEN:
                opened = cast("StreamOpen", event.opened)
                yield sse_event(
                    "open",
                    {
                        "stream_id": opened.stream_id,
                        "max_seconds": opened.max_seconds,
                        "max_wait_ms": opened.max_wait_ms,
                        "poll_interval_ms": opened.poll_interval_ms,
                        "prefetch": opened.prefetch,
                        "inflight_messages": opened.inflight_messages,
                        "inflight_bytes": opened.inflight_bytes,
                    },
                )
            elif event.kind is StreamEventKind.MESSAGE:
                yield sse_event("message", cast("MessageItem", event.item).model_dump(mode="json"))
            elif event.kind is StreamEventKind.ERROR:
                yield sse_event("error", {"code": event.error_code, "message": event.error_message})
            elif event.kind is StreamEventKind.CLOSE:
                yield sse_event("close", {"reason": event.reason})
            else:
                yield sse_comment("keepalive")
    finally:
        # 外层生成器被关闭时内层不会自动收尾（`async for` 只结束循环）：显式关闭，
        # 否则流登记与连接名额要等 GC 才归还，用例的「断开即释放」断言会失败。
        await core.aclose()


async def stream_frames(
    *,
    request_id: str,
    uow_factory: UowFactory,
    registry: StreamRegistry,
    connections: ConnectionRegistry,
    store: MessageStore,
    offsets: OffsetStore,
    dead_letters: DeadLetterQueue,
    state: DeliveryState,
    groups: GroupStore,
    tenant: CurrentTenant,
    query: SubscribeQuery,
    tuning: StreamTuning,
    connection_key: str,
    waiter: DeliveryWaiter,
    backpressure: BackpressureTuning,
    metrics: BackpressureMetrics,
    cache: TopicCache,
    directory: PartitionDirectory | None = None,
    topic_id: int | None = None,
    partition_no: int | None = None,
    replication: ReplicationView | None = None,
) -> AsyncGenerator[bytes]:
    """帧流编码器（`application/x-msg-frame`；核心见 `_stream_core`）。

    `open` → `SUBSCRIBE`+RESPONSE 帧；`message` → `MESSAGE` 帧（payload 为原始字节）；
    `error` → `ERROR` 帧；`close` 与 `keepalive` **不发帧**（EOF 即正常结束，保活由 h2 心跳承担）。

    Args:
        request_id: 请求标识（逐帧回显，客户端据此对齐多路流）。
        uow_factory: 工作单元工厂。
        registry: 流登记表。
        connections: 连接登记表。
        store: 消息存储。
        offsets: 位点存储。
        dead_letters: 死信队列。
        state: 投递状态。
        groups: 组存储。
        tenant: 当前租户。
        query: 长驻流参数。
        tuning: 流参数。
        connection_key: 所属连接的派生键。
        waiter: 分区唤醒器。
        backpressure: inflight 双阈值。
        metrics: 背压指标收集器。
        cache: 进程内主题快照缓存（每轮成员制与拉取共用）。
        directory: 分区目录登记表（02-1）。
        topic_id: 主题主键（预检已解析时给值）。
        partition_no: 分区号（预检已解析时给值）。

    Yields:
        bytes: 连续帧字节（客户端按 `header_len` / `payload_len` 自行切分）。
    """
    core = _stream_core(
        uow_factory=uow_factory,
        registry=registry,
        connections=connections,
        store=store,
        offsets=offsets,
        dead_letters=dead_letters,
        state=state,
        groups=groups,
        tenant=tenant,
        query=query,
        tuning=tuning,
        connection_key=connection_key,
        waiter=waiter,
        backpressure=backpressure,
        metrics=metrics,
        cache=cache,
        directory=directory,
        topic_id=topic_id,
        partition_no=partition_no,
        replication=replication,
    )
    try:
        async for event in core:
            if event.kind is StreamEventKind.OPEN:
                yield stream_open_frame(request_id=request_id, opened=cast("StreamOpen", event.opened))
            elif event.kind is StreamEventKind.MESSAGE:
                yield message_frame(request_id=request_id, item=cast("MessageItem", event.item))
            elif event.kind is StreamEventKind.ERROR:
                yield error_frame(code=event.error_code or 0, message=event.error_message, request_id=request_id)
            # close / keepalive 不发帧：EOF 即正常结束，保活由连接心跳承担。
    finally:
        await core.aclose()


async def _preflight(
    *,
    request: SubscribeQuery,
    tenant: CurrentTenant,
    uow_factory: UowFactory,
    store: MessageStore,
    groups: GroupStore,
    state: DeliveryState,
    cache: TopicCache,
    throttle: RetentionThrottle,
    directory: PartitionDirectory | None = None,
) -> tuple[int, int]:
    """进流前的预检（保留期清理 + 成员制），并回传 `(topic_id, partition_no)`。

    Args:
        request: 长驻流参数。
        tenant: 当前租户。
        uow_factory: 工作单元工厂。
        store: 消息存储。
        groups: 组存储。
        state: 投递状态。
        cache: 进程内主题快照缓存。
        throttle: 保留清理节流（与 publish / consume 共用同一窗口表）。
        directory: 分区目录登记表（02-1）。

    Returns:
        tuple[int, int]: 主题主键与分区号。

    Raises:
        MsgError: 命名空间 / 主题 / 分区号 / 组 / 成员类错误（与拉取端点同码）。
    """
    async with uow_factory() as preflight_uow:
        topic_id, partition_no = await apply_retention(
            tenant=tenant,
            uow=preflight_uow,
            store=store,
            namespace=request.namespace,
            topic=request.topic,
            cache=cache,
            throttle=throttle,
            partition_no=request.partition,
            directory=directory,
        )
        await ensure_member(
            tenant=tenant,
            uow=preflight_uow,
            groups=groups,
            state=state,
            cache=cache,
            request=_consume_request(request),
            directory=directory,
        )
    return topic_id, partition_no


@router.get(
    "/subscribe",
    dependencies=[_CONSUME],
    openapi_extra={"x-error-codes": list(_SUBSCRIBE_ERROR_CODES)},
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "SSE 长驻流（event: open / message / error / close）",
            "content": {"text/event-stream": {}},
        }
    },
)
async def subscribe(
    request: Request,
    query: Annotated[SubscribeQuery, Query()],
    tenant: DataPlaneTenantDep,
    uow_factory: UowFactoryDep,
    registry: StreamRegistryDep,
    tuning: StreamTuningDep,
    connections: ConnectionRegistryDep,
    connection_tuning: ConnectionTuningDep,
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
    acl: TopicAclServiceDep,
    replication: ReplicationDep,
) -> StreamingResponse:
    """订阅一条分区（SSE 长驻流，server streaming）。

    需要 msg:consume 权限；须先 `join` 且分区在该成员分配内（组不存在 350013，成员失效 350014）。
    预检与 `/msg/consume` 同口径：命名空间 350010、主题 350001、分区号 350009；
    `max_seconds` 越界（> 600）与其它参数形态问题由契约层拒绝（`10001`）。
    单连接长驻流超上限返回 350021（普通 JSON 错误，尚未进流；名额随即归还）。
    进流之后：拉到消息逐条 `message`；无消息按事件唤醒等待（保活期发注释行）；
    到期 `close(deadline)`；成员被撤销 / 分区易主 → `error`（同码）+ `close(member_lost)`。
    客户端断开或 `RST_STREAM` 后流被注销（`close(client_closed)`），已投未 ack 的消息仍可 ack。
    """
    connection_key = cast("str", request.state.connection_key)
    await acl.authorize(
        tenant_id=int(tenant.tenant_id),
        namespace=query.namespace,
        topic=query.topic,
        action=ACL_ACTION_SUBSCRIBE,
    )
    # 先占名额：预检失败必须归还，否则一次失败会永久占一个长驻流名额（03-4 详细设计 §5.3）。
    connections.open_stream(connection_key, limit=connection_tuning.max_streams_per_connection)
    try:
        topic_id, partition_no = await _preflight(
            request=query,
            tenant=tenant,
            uow_factory=uow_factory,
            store=store,
            groups=groups,
            state=state,
            cache=cache,
            throttle=throttle,
            directory=directory,
        )
    except BaseException:
        connections.close_stream(connection_key)
        raise
    return StreamingResponse(
        stream_events(
            uow_factory=uow_factory,
            registry=registry,
            connections=connections,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            tenant=tenant,
            query=query,
            tuning=tuning,
            connection_key=connection_key,
            waiter=waiter,
            backpressure=backpressure,
            metrics=metrics,
            cache=cache,
            topic_id=topic_id,
            partition_no=partition_no,
            replication=replication,
        ),
        media_type="text/event-stream",
        headers=_STREAM_HEADERS,
    )


def _frame_subscribe_query(body: bytes, tenant: CurrentTenant) -> tuple[str, SubscribeQuery]:
    """解码帧流请求（一帧 = 一次订阅），校验类型 / payload / 必填头 / 租户一致性。

    Args:
        body: 请求体字节。
        tenant: 当前租户（帧头 `tenant` 只能与之相同，不得作为第二个鉴权入口）。

    Returns:
        tuple[str, SubscribeQuery]: 请求标识与订阅参数。

    Raises:
        MsgVersionUnsupportedError: 主版本不支持（350019）。
        MsgFrameInvalidError: 类型不是 4 / 带 payload / 缺必填头 / 头值形态非法（350018）。
        MsgTenantNotFoundError: 帧头租户与请求身份不一致（350012）。
    """
    frame = decode_frame(body)
    if frame.frame_type != int(FrameType.SUBSCRIBE):
        raise MsgFrameInvalidError(f"该端点只接受订阅帧（4），收到 {frame.frame_type}")
    if bytes(frame.payload):
        raise MsgFrameInvalidError("订阅帧不得携带 payload（订阅参数走 header）")
    missing = tuple(name for name in _FRAME_REQUIRED_HEADERS if name not in frame.header)
    if missing:
        raise MsgFrameInvalidError(f"缺少必填 frame header 字段：{', '.join(missing)}")
    declared = frame.header.get("tenant")
    if declared is not None and declared != tenant.code:
        raise MsgTenantNotFoundError("frame header 的 tenant 与请求身份不一致")
    values = {name: frame.header[name] for name in SubscribeQuery.model_fields if name in frame.header}
    try:
        query = SubscribeQuery.model_validate(values)
    except ValueError as exc:
        # 键在而值形态非法属契约层（`10001`），与数据面闸门口径一致；缺键才是协议层（`350018`）。
        raise ParamError(f"订阅帧头字段形态非法：{exc}") from exc
    request_id = frame.header["request_id"]
    if not isinstance(request_id, str) or not request_id:
        raise MsgFrameInvalidError("缺少必填 frame header 字段：request_id")
    return request_id, query


@router.post(
    "/subscribe",
    dependencies=[_CONSUME],
    openapi_extra={"x-error-codes": list(_SUBSCRIBE_ERROR_CODES)},
    response_class=StreamingResponse,
    responses={
        200: {
            "description": "帧流（open 帧 4+RESPONSE / message 帧 6 / error 帧 16；正文 EOF 即正常结束）",
            "content": {FRAME_MEDIA_TYPE: {}},
        }
    },
)
async def subscribe_frames(
    request: Request,
    tenant: DataPlaneTenantDep,
    uow_factory: UowFactoryDep,
    registry: StreamRegistryDep,
    tuning: StreamTuningDep,
    connections: ConnectionRegistryDep,
    connection_tuning: ConnectionTuningDep,
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
    acl: TopicAclServiceDep,
    replication: ReplicationDep,
) -> StreamingResponse:
    """订阅一条分区（帧流形态，`application/x-msg-frame`）。

    与 `GET /msg/subscribe` **同一套语义**（权限码 / 预检 / 背压 / 结束条件全同），差别只在承载：
    参数在帧 header、响应是连续帧。帧不可解析 / 类型不符 / 缺必填头 → `200` + 单帧 `error`（协议内失败）；
    预检失败同理（尚未进流，用 error 帧而**不是** JSON，客户端只按帧解析）。
    单连接长驻流超上限仍返回 350021（由连接守卫按帧形态给出）。
    """
    body = await request.body()
    try:
        request_id, query = _frame_subscribe_query(body, tenant)
    except BizError as exc:
        return StreamingResponse(
            _single_frame(error_frame(code=exc.code, message=exc.message or str(exc))),
            media_type=FRAME_MEDIA_TYPE,
            headers=_STREAM_HEADERS,
        )
    connection_key = cast("str", request.state.connection_key)
    try:
        await acl.authorize(
            tenant_id=int(tenant.tenant_id),
            namespace=query.namespace,
            topic=query.topic,
            action=ACL_ACTION_SUBSCRIBE,
        )
    except BizError as exc:
        return StreamingResponse(
            _single_frame(error_frame(code=exc.code, message=exc.message or str(exc), request_id=request_id)),
            media_type=FRAME_MEDIA_TYPE,
            headers=_STREAM_HEADERS,
        )
    connections.open_stream(connection_key, limit=connection_tuning.max_streams_per_connection)
    try:
        topic_id, partition_no = await _preflight(
            request=query,
            tenant=tenant,
            uow_factory=uow_factory,
            store=store,
            groups=groups,
            state=state,
            cache=cache,
            throttle=throttle,
            directory=directory,
        )
    except MsgError as exc:
        connections.close_stream(connection_key)
        return StreamingResponse(
            _single_frame(error_frame(code=exc.code, message=exc.message or str(exc), request_id=request_id)),
            media_type=FRAME_MEDIA_TYPE,
            headers=_STREAM_HEADERS,
        )
    except BaseException:
        connections.close_stream(connection_key)
        raise
    return StreamingResponse(
        stream_frames(
            request_id=request_id,
            uow_factory=uow_factory,
            registry=registry,
            connections=connections,
            store=store,
            offsets=offsets,
            dead_letters=dead_letters,
            state=state,
            groups=groups,
            tenant=tenant,
            query=query,
            tuning=tuning,
            connection_key=connection_key,
            waiter=waiter,
            backpressure=backpressure,
            metrics=metrics,
            cache=cache,
            directory=directory,
            topic_id=topic_id,
            partition_no=partition_no,
            replication=replication,
        ),
        media_type=FRAME_MEDIA_TYPE,
        headers=_STREAM_HEADERS,
    )


async def _single_frame(payload: bytes) -> AsyncGenerator[bytes]:
    """把一帧包装成「只有一帧就结束」的响应体（预检失败用）。

    Args:
        payload: 帧字节。

    Yields:
        bytes: 该帧（随后 EOF）。
    """
    yield payload


@connection_router.get("/connection", openapi_extra={"x-error-codes": []})
async def connection(
    request: Request,
    record: ConnectionInfoRecordDep,
    tuning: ConnectionTuningDep,
) -> ApiResponse[ConnectionInfo]:
    """回本连接元数据（协议版本 / 承载方式 / 客户端地址 + 连接键 / 活跃流 / 阈值 / drain 标记）。

    只读自报，不含租户业务数据，故不加权限码（05 域认证落地后再评估收紧）。
    用途：排障第一问（协商到 h2 还是退回 1.1）、用例断言、客户端取服务端口径（心跳间隔 / 上限 / drain）。
    **兼作保活**：本端点每被调用一次就刷新连接 `last_seen`——无帧能力的客户端（curl / EventSource）
    用它当心跳，与 `POST /msg/connection/heartbeat` 共用同一份登记（03-4 详细设计 §6.1）。
    **不判连接数上限**（守卫豁免该判定）：它是打满时的排障入口，判上限会让「满」变成「连查都查不了」。
    版本可见（03-2）：`supported_protocol_version` 恒有值；本端点是 JSON 请求，`protocol_version` 恒为 null。
    **显式声明 `x-error-codes: []`**：不触达业务判定，没有产品码可声明（与探针端点同性质；
    空数组是「已确认无码」，区别于漏声明——跨仓契约反向对账据此区分两者）。
    """
    return ApiResponse.ok(connection_info(request, record, tuning))
