"""投递语义（01-2 详细设计 §6）与顺序性（01-3 详细设计 §4）：分区内停等、重投不越队头、死信例外。

五条不可动摇的口径：

1. **先处理后提交**：consume 只投递、不改位点；位点只由 ack（或 `acks=0` 的批量前移）推进；
2. **重投保留原记录**：`message_id` / `offset` / `payload` 不变，只有 `delivery_count` 递增；
3. **提交失败不前移**：先 `OffsetStore.commit()` 成功、再改内存状态——最坏结果是重复 ack，而重复 ack 幂等；
4. **毒消息不阻塞分区**：重投达上限即转死信并把位点前移（不删日志，重放与排障仍需原始记录）；
5. **分区内停等（顺序性）**：扫描遇未过期在途队头即停止本批，其后的 offset 一条不投；重投回原 offset，
   队头 ack 后才继续；本批一旦含重投，新消息留到下一批（失败重试不与主链路同批）。死信是唯一破例——
   消息已终结，其后 offset 可继续投，已死信 offset 后续跳过不重投。

顺序单位是**分区**（同 key 同分区由 `core/routing` 保证，跨分区不承诺顺序）；`acks` 档位只决定投出后
是否登记在途、何时推进位点，不影响停等判定本身。

**两类 ID**：`publish` 同时生成 `message_id`（消息身份）与 `log_id`（记录身份，01-5 起随记录下发）；
延迟转投沿用发布时分配的 `log_id`，重放新分配（01-6）——本模块只负责普通发布这一处。

`DeliveryState` 是**进程级**状态（跨请求存续，装配时落 `app.state`）；`DeliveryService` 是**请求级**对象
（绑定平台库主题仓储与请求租户），与 01-1 的 `TopicService` 同构。
"""

import asyncio
import base64
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList, ConcurrentStableSet
from bms_core.core.exceptions import ParamError
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.backpressure import BackpressureTuning, InflightUsage
from bms_msg.broker.deadletter import (
    DEAD_LETTER_EXHAUSTED,
    DeadLetter,
    DeadLetterQueue,
    DeadLetterReason,
)
from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.broker.watermark import StorageGate
from bms_msg.core.config import ACKS_FIRE_AND_FORGET, DEFAULT_VISIBILITY_TIMEOUT_MS, MAX_DELIVERY_ATTEMPTS
from bms_msg.core.message_id import new_message_id
from bms_msg.errors import (
    MsgDeliveryExhaustedError,
    MsgMessageNotFoundError,
    MsgOffsetInvalidError,
    MsgPayloadTooLargeError,
)
from bms_msg.protocol.frame import MAX_FRAME_PAYLOAD_BYTES
from bms_msg.protocol.payload import normalize_compression, normalize_content_type
from bms_msg.schemas.message import AckRequest, ConsumeRequest, NackRequest, PublishRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = [
    "Delivery",
    "DeliveryService",
    "DeliveryState",
    "InflightDelivery",
    "PartitionKey",
    "decode_payload",
    "ensure_payload_size",
    "resolve_payload",
]

PartitionKey = tuple[str, int, int]
"""投递状态键：`(group, topic_id, partition_no)`。"""


def _utcnow() -> datetime:
    """当前 UTC 时间（可见性超时与死信时间的判据；用例可注入固定时钟）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


@dataclass(frozen=True)
class InflightDelivery(BaseValueObject):
    """一次在途投递（已投出、尚未确认）。"""

    attempts: int
    """本次是第几次投递（首次为 1）。"""

    deadline: datetime
    """可见性超时到期时刻（到点后可重投）。"""


@dataclass(frozen=True)
class Delivery(BaseValueObject):
    """一次投递结果（消息记录 + 本组本分区的投递次数）。"""

    message: StoredMessage
    """消息记录（重投时与首次完全相同）。"""

    delivery_count: int
    """投递次数（首次 1，重投递增；消费端据此识别重复消息）。"""


class DeliveryState:
    """进程级投递状态：在途投递 + 字节账 + 乱序已确认 offset + 状态临界区锁。"""

    def __init__(self) -> None:
        """初始化（无在途、无暂存确认、字节账为空）。"""
        self._inflight: dict[PartitionKey, dict[int, InflightDelivery]] = {}
        self._sizes: dict[PartitionKey, dict[int, int]] = {}
        self._acked: dict[PartitionKey, ConcurrentStableSet[int]] = {}
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        """状态临界区锁（consume / ack 互斥，避免同分区并发推进位点）。

        Returns:
            asyncio.Lock: 进程内锁。
        """
        return self._lock

    def lookup(self, key: PartitionKey, offset: int) -> InflightDelivery | None:
        """取某 offset 的在途投递记录。

        Args:
            key: 投递状态键。
            offset: 分区内序号。

        Returns:
            InflightDelivery | None: 在途记录；未投递或已确认返回 None。
        """
        return self._inflight.get(key, {}).get(offset)

    def usage(self, key: PartitionKey) -> InflightUsage:
        """取某分区的在途用量（条数 + 字节；03-5 双阈值的唯一读数）。

        Args:
            key: 投递状态键。

        Returns:
            InflightUsage: 当前用量。
        """
        return InflightUsage(
            messages=len(self._inflight.get(key, {})),
            bytes=sum(self._sizes.get(key, {}).values()),
        )

    def track(self, key: PartitionKey, offset: int, delivery: InflightDelivery, *, size: int = 0) -> None:
        """登记一次在途投递（覆盖同 offset 的旧记录，并同步字节账）。

        **重投覆盖时先减旧字节**：否则同一条消息每重投一次就多算一份，阈值会被虚增的字节提前打满，
        表现为「消息还没几条就停止推送」。

        Args:
            key: 投递状态键。
            offset: 分区内序号。
            delivery: 在途投递记录。
            size: 该消息的 payload 字节数（03-5；缺省 0 表示不记账，兼容既有调用）。
        """
        bucket = self._inflight.setdefault(key, {})
        sizes = self._sizes.setdefault(key, {})
        sizes.pop(offset, None)
        bucket[offset] = delivery
        if size:
            sizes[offset] = size

    def forget(self, key: PartitionKey, offset: int) -> None:
        """移除在途记录与字节账（确认 / 死信 / 重投超限后）。

        Args:
            key: 投递状态键。
            offset: 分区内序号。
        """
        self._inflight.get(key, {}).pop(offset, None)
        self._sizes.get(key, {}).pop(offset, None)

    def mark_acked(self, key: PartitionKey, offset: int) -> None:
        """记录一个已确认 offset（用于乱序确认的成段推进）。

        Args:
            key: 投递状态键。
            offset: 分区内序号。
        """
        self._acked.setdefault(key, ConcurrentStableSet[int]()).add(offset)

    def pending(self, key: PartitionKey) -> ConcurrentStableSet[int]:
        """取已确认但尚未并入位点的 offset 快照。

        Args:
            key: 投递状态键。

        Returns:
            ConcurrentStableSet[int]: offset 快照（调用方可安全修改）。
        """
        return ConcurrentStableSet(self._acked.get(key, ConcurrentStableSet[int]()))

    def prune(self, key: PartitionKey, committed: int) -> None:
        """清理已并入位点的确认记录（`< committed` 的部分；更高位的暂存保留）。

        Args:
            key: 投递状态键。
            committed: 已提交位点。
        """
        acked = self._acked.get(key, ConcurrentStableSet[int]())
        for offset in acked.to_list():
            if offset < committed:
                acked.discard(offset)

    def drop_partition(self, key: PartitionKey) -> None:
        """清掉某分区全部在途登记与字节账（分区易主时用；位点与暂存确认**不动**）。

        01-4 再均衡撤销易主分区时调用：不清则新 owner 会被「他人未过期在途」挡住整批拉取
        （01-3 停等），最长空转一个可见性超时；清掉后新 owner 立即可从已提交位点重投，
        代价是未 ack 消息多投一次（至少一次语义允许）。`acked` 集合保留——已确认但受缺口阻塞的
        offset 仍应参与连续前缀推进，清掉只会多一次重复投递。

        03-5 起**字节账一并清**：不清则新持有者一上来就被「前持有者的字节」判成超限，
        恢复要等一个可见性超时（与在途账同一类问题）。

        Args:
            key: 投递状态键。
        """
        self._inflight.pop(key, None)
        self._sizes.pop(key, None)


class DeliveryService:
    """投递服务（请求级）：发布 / 拉取 / 确认，语义见模块文档与详细设计 §6。"""

    def __init__(
        self,
        *,
        store: MessageStore,
        offsets: OffsetStore,
        dead_letters: DeadLetterQueue,
        router: TopicRouter,
        state: DeliveryState,
        now: Callable[[], datetime] = _utcnow,
        backpressure: BackpressureTuning | None = None,
        waiter: DeliveryWaiter | None = None,
        gate: StorageGate | None = None,
    ) -> None:
        """初始化。

        Args:
            store: 消息存储（进程级）。
            offsets: 位点存储（进程级）。
            dead_letters: 死信队列（进程级）。
            router: 主题路由（请求级，绑定平台库主题仓储）。
            state: 投递状态（进程级）。
            now: 时钟（缺省 UTC 当前时间；用例注入固定时钟以模拟可见性超时）。
            backpressure: inflight 双阈值（03-5；None 表示不限，保持 01-2 / 01-3 用例原行为）。
            waiter: 分区唤醒器（03-5；确认 / 死信归还额度后唤醒被暂停的流）。
            gate: 存储水位闸门（02-6；None 表示不判，内存实现直连与既有用例不受影响）。
        """
        self._store = store
        self._offsets = offsets
        self._dead_letters = dead_letters
        self._router = router
        self._state = state
        self._now = now
        self._backpressure = backpressure
        self._waiter = waiter
        self._gate = gate

    def _notify(self, key: PartitionKey) -> None:
        """唤醒该分区上的长轮询 / 长驻流（额度归还或消息写入后）。

        Args:
            key: 投递状态键（`(group, topic_id, partition_no)`；等待键省去 group）。
        """
        if self._waiter is not None:
            self._waiter.notify((key[1], key[2]))

    def _budget(self, key: PartitionKey, request: ConsumeRequest) -> InflightUsage | None:
        """算本轮可用额度（None 表示不限）。

        三条短路：未配置阈值 / 阈值为 0 / `acks=0`（该档位不登记在途，用量恒不增长）。

        Args:
            key: 投递状态键。
            request: 拉取请求。

        Returns:
            InflightUsage | None: 剩余额度；None 表示不限。
        """
        if self._backpressure is None or not self._backpressure.enabled:
            return None
        if request.acks == ACKS_FIRE_AND_FORGET:
            return None
        return self._state.usage(key).remaining(self._backpressure.limit)

    async def publish(self, *, tenant: CurrentTenant, request: PublishRequest) -> StoredMessage:
        """发布一条消息（按分区键选分区、分配 offset、生成 `message_id` 与 `log_id`）。

        Args:
            tenant: 当前租户（数据面经 `frame_tenant()` 解析）。
            request: 发布请求（`tenant` 不在请求体；`delay_level` 为空才走本路径）。

        Returns:
            StoredMessage: 含 `message_id` / `log_id` / `partition_no` / `offset` 的记录。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 主题分区数非正（350009）。
            MsgStorageWatermarkExceededError: 存储水位超拒绝阈值或数据卷不可用（350022 / 507，02-6）。
            ParamError: 载荷非合法 base64（10001）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.route(topic, request.key)
        if self._gate is not None:
            # 02-6：水位判在**主题与分区判定之后、写入之前**——非法请求的错误码优先级不变，
            # 且只约束「外部生产」（延迟转投与重放直接走 store.append，不经本闸门）。
            self._gate.ensure_writable()
        return await self._store.append(
            topic_id=int(topic.id),
            partition_no=partition_no,
            message_id=new_message_id(),
            log_id=new_message_id(),
            key=request.key,
            payload=resolve_payload(request),
            headers=ConcurrentStableDict(request.headers.items()),
            trace_id=request.trace_id,
            published_at=self._now(),
            content_type=normalize_content_type(request.content_type),
            compression=normalize_compression(request.compression),
            uncompressed_len=request.uncompressed_len,
        )

    async def consume(self, *, tenant: CurrentTenant, request: ConsumeRequest) -> ConcurrentStableList[Delivery]:
        """拉取一批消息（缺省至少一次：登记在途、等 ack 才推进位点）。

        Args:
            tenant: 当前租户。
            request: 拉取请求（必带 `partition`）。

        Returns:
            ConcurrentStableList[Delivery]: 本次投递的消息（可能为空）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 分区号越界（350009）。
            MsgMessageNotFoundError: 位点已被保留期清理（350003）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.partition(topic, request.partition)
        topic_id = int(topic.id)
        key: PartitionKey = (request.group, topic_id, partition_no)
        async with self._state.lock:
            committed = await self._offsets.get(group=request.group, topic_id=topic_id, partition_no=partition_no)
            start = await self._store.start_offset(topic_id=topic_id, partition_no=partition_no)
            if committed < start:
                raise MsgMessageNotFoundError(f"位点已被保留期清理：{committed} < {start}")
            return await self._collect(
                request=request,
                key=key,
                topic_id=topic_id,
                partition_no=partition_no,
                committed=committed,
                budget=self._budget(key, request),
            )

    async def ack(self, *, tenant: CurrentTenant, request: AckRequest) -> int:
        """确认一条消息（推进连续已确认前缀；提交失败不改变任何状态）。

        Args:
            tenant: 当前租户。
            request: 确认请求（必带 `partition` 与 `offset`）。

        Returns:
            int: 已提交位点（下一条待消费的 offset）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 分区号越界（350009）。
            MsgOffsetInvalidError: 位点越界（`>= LEO`；350002）。
            MsgDeliveryExhaustedError: 消息已转死信、不可再确认（350007）。
            MsgMessageNotFoundError: 该 offset 不在投递中（350003）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.partition(topic, request.partition)
        topic_id = int(topic.id)
        key: PartitionKey = (request.group, topic_id, partition_no)
        async with self._state.lock:
            leo = await self._store.leo(topic_id=topic_id, partition_no=partition_no)
            if request.offset >= leo:
                raise MsgOffsetInvalidError(f"位点越界：{request.offset} >= LEO {leo}")
            if await self._dead_letters.contains(topic_id=topic_id, partition_no=partition_no, offset=request.offset):
                raise MsgDeliveryExhaustedError(f"消息重投超限、已转入死信，不可再确认：offset={request.offset}")
            committed = await self._offsets.get(group=request.group, topic_id=topic_id, partition_no=partition_no)
            if request.offset < committed:
                return committed
            if self._state.lookup(key, request.offset) is None:
                raise MsgMessageNotFoundError(f"消息不在投递中：offset={request.offset}")
            advanced = await self._commit(key=key, offset=request.offset)
            self._state.forget(key, request.offset)
            self._notify(key)
            return advanced

    async def nack(self, *, tenant: CurrentTenant, request: NackRequest) -> int:
        """拒绝一条消息（转死信 + 位点前移；幂等）。

        与 `ack` 的区别只在「消息是终结还是成功」：两者都推进位点，`nack` 额外把消息转入死信并记原因。
        顺序性因此与「重投超限转死信」完全一致——死信是唯一能越过队头的破例（01-3 §4）。

        Args:
            tenant: 当前租户。
            request: 拒绝请求（必带 `partition` / `offset` / `reason`）。

        Returns:
            int: 已提交位点（下一条待消费的 offset）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgPartitionCountInvalidError: 分区号越界（350009）。
            MsgOffsetInvalidError: 位点越界（`>= LEO`；350002）。
            MsgMessageNotFoundError: 该 offset 不在投递中（350003）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.partition(topic, request.partition)
        topic_id = int(topic.id)
        key: PartitionKey = (request.group, topic_id, partition_no)
        async with self._state.lock:
            leo = await self._store.leo(topic_id=topic_id, partition_no=partition_no)
            if request.offset >= leo:
                raise MsgOffsetInvalidError(f"位点越界：{request.offset} >= LEO {leo}")
            committed = await self._offsets.get(group=request.group, topic_id=topic_id, partition_no=partition_no)
            if await self._dead_letters.contains(topic_id=topic_id, partition_no=partition_no, offset=request.offset):
                # 幂等：重复上报（重试 / 超时重发）不重复入队，也不改变位点。
                return committed
            existing = self._state.lookup(key, request.offset)
            if existing is None:
                raise MsgMessageNotFoundError(f"消息不在投递中，无法拒绝：offset={request.offset}")
            batch = await self._store.fetch(
                topic_id=topic_id, partition_no=partition_no, from_offset=request.offset, max_messages=1
            )
            if not batch or batch[0].offset != request.offset:
                raise MsgMessageNotFoundError(f"消息不存在：offset={request.offset}")
            return await self._dead_letter(
                key=key,
                message=batch[0],
                attempts=existing.attempts,
                reason=DeadLetterReason(request.reason),
                detail=ConcurrentStableDict(request.detail.items()),
            )

    async def _collect(
        self,
        *,
        request: ConsumeRequest,
        key: PartitionKey,
        topic_id: int,
        partition_no: int,
        committed: int,
        budget: InflightUsage | None = None,
    ) -> ConcurrentStableList[Delivery]:
        """从已提交位点起按 offset 升序收集一批投递（分区内停等）。

        扫描规则（优先级从高到低；详细设计 §4.2）：

        1. **已死信** → 跳过（顺序性唯一破例：消息已终结，不投、不阻塞）；
        2. **未过期在途** → 停止本批（未确认队头不越过其后的任何 offset）；
        3. **本批已含重投且当前是新消息** → 停止本批（新消息留到下一批，重试不与主链路同批）；
        4. 在途已过期且 `attempts + 1 > 上限` → 转死信 → 继续看下一条；
        5. 其余 → 投出（`acks=1` 登记在途；`acks=0` 不登记）。

        **03-5 的字节额度只限制「投出前的条数」，不参与上述停等判定**：额度不足时停批，
        但绝不越过队头、绝不改变「谁先谁后」——顺序性与背压是两件事。

        Args:
            request: 拉取请求。
            key: 投递状态键。
            topic_id: 主题主键。
            partition_no: 分区号。
            committed: 已提交位点。
            budget: 剩余额度（None 表示不限；条数为 0 即一条不投）。

        Returns:
            ConcurrentStableList[Delivery]: 本次投递（可能为空；按 offset 升序）。

        Raises:
            MsgDeliveryExhaustedError: 无（死信在此路径静默跳过，仅 ack 路径报 350007）。
        """
        delivered: ConcurrentStableList[Delivery] = ConcurrentStableList()
        cursor = committed
        contiguous = committed
        retried = False
        limit = request.max_messages if budget is None else min(request.max_messages, budget.messages)
        bytes_left = None if budget is None else budget.bytes
        while len(delivered) < limit:
            batch = await self._store.fetch(
                topic_id=topic_id,
                partition_no=partition_no,
                from_offset=cursor,
                max_messages=limit - len(delivered),
            )
            if not batch:
                break
            stopped = False
            for message in batch:
                cursor = message.offset + 1
                if await self._dead_letters.contains(
                    topic_id=topic_id, partition_no=partition_no, offset=message.offset
                ):
                    # 已死信：消息已终结，跳过且不阻塞后续（顺序性唯一破例）。
                    # 补记已确认暂存：死信那次位点提交若失败（或尚未推进），后续 ack 才能成段越过它。
                    # 顺序扫描保证跳过时它必是当前连续前缀（前面的消息都已投出或同样已死信）。
                    self._state.mark_acked(key, message.offset)
                    contiguous = message.offset + 1
                    continue
                existing = self._state.lookup(key, message.offset)
                if existing is not None and existing.deadline > self._now():
                    # 停等：队头未确认，其后的 offset 一条都不投。
                    stopped = True
                    break
                if retried and existing is None:
                    # 本批已含重投：新消息留到下一批，失败重试不与主链路同批下发。
                    stopped = True
                    break
                size = len(message.payload)
                if bytes_left is not None and size > bytes_left:
                    # 字节额度不足：停批。与「队头未确认」是两种停法——这里不改变顺序判定。
                    stopped = True
                    break
                delivery = await self._deliver(key=key, message=message, acks=request.acks, existing=existing)
                if delivery is None:
                    # 本次刚转死信：与「已死信」同处置，继续看下一条。
                    continue
                delivered.add(delivery)
                if bytes_left is not None:
                    bytes_left -= size
                if existing is not None:
                    retried = True
                contiguous = message.offset + 1
                if len(delivered) == limit:
                    break
            if stopped:
                break
        if request.acks == ACKS_FIRE_AND_FORGET and contiguous > committed:
            await self._offsets.commit(
                group=request.group, topic_id=topic_id, partition_no=partition_no, offset=contiguous
            )
        return delivered

    async def _deliver(
        self, *, key: PartitionKey, message: StoredMessage, acks: int, existing: InflightDelivery | None
    ) -> Delivery | None:
        """决定一条消息本次是否投出、投出时的投递次数。

        Args:
            key: 投递状态键。
            message: 存储中的消息。
            acks: 投递档位（1 = 至少一次；0 = 不等确认）。
            existing: 该 offset 的在途记录（未投过或已确认即 `None`；未过期的在途已由调用方停等拦下）。

        Returns:
            Delivery | None: 需投出时的投递；`None` 仅表示本次刚转死信（调用方应继续看下一条）。
        """
        if acks == ACKS_FIRE_AND_FORGET:
            return Delivery(message=message, delivery_count=1)
        attempts = 1 if existing is None else existing.attempts + 1
        if attempts > MAX_DELIVERY_ATTEMPTS:
            await self._dead_letter(key=key, message=message)
            return None
        self._state.track(
            key,
            message.offset,
            InflightDelivery(
                attempts=attempts,
                deadline=self._now() + timedelta(milliseconds=DEFAULT_VISIBILITY_TIMEOUT_MS),
            ),
            size=len(message.payload),
        )
        return Delivery(message=message, delivery_count=attempts)

    async def _dead_letter(
        self,
        *,
        key: PartitionKey,
        message: StoredMessage,
        attempts: int | None = None,
        reason: DeadLetterReason = DEAD_LETTER_EXHAUSTED,
        detail: ConcurrentStableDict[str, str] | None = None,
    ) -> int:
        """把消息转入死信并前移位点（不删日志）。

        Args:
            key: 投递状态键。
            message: 被放弃的消息。
            attempts: 累计投递次数（缺省按重投上限填；消费端上报时给实际次数）。
            reason: 转入原因（broker 判定为重投超限，消费端上报见 `DeadLetterReason`）。
            detail: 失败详情（消费端上报时原样保存）。

        Returns:
            int: 推进后的位点。
        """
        _group, topic_id, partition_no = key
        await self._dead_letters.add(
            DeadLetter(
                message_id=message.message_id,
                topic_id=topic_id,
                partition_no=partition_no,
                offset=message.offset,
                attempts=attempts if attempts is not None else MAX_DELIVERY_ATTEMPTS,
                failed_at=self._now(),
                reason=reason,
                detail=detail if detail is not None else ConcurrentStableDict(),
            )
        )
        self._state.forget(key, message.offset)
        advanced = await self._commit(key=key, offset=message.offset)
        self._notify(key)
        return advanced

    async def _commit(self, *, key: PartitionKey, offset: int) -> int:
        """按「连续已确认前缀」推进位点（先落存储、成功后才改内存状态）。

        Args:
            key: 投递状态键。
            offset: 本次确认的 offset。

        Returns:
            int: 推进后的位点（无推进即现值）。

        Raises:
            Exception: `OffsetStore.commit()` 的任何异常原样冒泡（状态不变，可重试）。
        """
        group, topic_id, partition_no = key
        current = await self._offsets.get(group=group, topic_id=topic_id, partition_no=partition_no)
        candidate = self._next_committed(key, offset, current)
        if candidate > current:
            await self._offsets.commit(group=group, topic_id=topic_id, partition_no=partition_no, offset=candidate)
            self._state.mark_acked(key, offset)
            self._state.prune(key, candidate)
        else:
            # 前面还有未确认的消息：先暂存，等缺口补齐时再成段推进。
            self._state.mark_acked(key, offset)
        return candidate

    def _next_committed(self, key: PartitionKey, offset: int, current: int) -> int:
        """计算加上本次确认后的连续前缀（纯函数，不改状态）。

        Args:
            key: 投递状态键。
            offset: 本次确认的 offset。
            current: 已提交位点。

        Returns:
            int: 新的连续前缀位点。
        """
        pending = self._state.pending(key)
        pending.add(offset)
        candidate = current
        while candidate in pending:
            candidate += 1
        return candidate


def ensure_payload_size(data: bytes) -> bytes:
    """判单条消息上限（`350006` / 413）。

    **唯一判据**：帧路径（原始字节）与 JSON 路径（base64 解码后）都走这里——两条路径一个上限，
    改阈值只改 `MAX_FRAME_PAYLOAD_BYTES` 一处。

    Args:
        data: 消息体字节（压缩后的线上字节）。

    Returns:
        bytes: 原样返回（便于链式调用）。

    Raises:
        MsgPayloadTooLargeError: 超过 1 MiB 上限（350006 / 413）。
    """
    if len(data) > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"消息体超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(data)} 字节）")
    return data


def decode_payload(payload: str) -> bytes:
    """把 base64 载荷解码为业务字节（形态属请求契约，非法即 `10001`）。

    解码后交给 ``ensure_payload_size`` 判上限：普通发布与延迟发布共用本函数，因此**两条分支同一判据**；
    帧路径不经本函数（它的原始字节直接走 ``ensure_payload_size``）。

    Args:
        payload: base64 字符串（空串表示空消息体）。

    Returns:
        bytes: 业务字节。

    Raises:
        ParamError: 非法 base64（10001）。
        MsgPayloadTooLargeError: 超过 1 MiB 上限（350006 / 413）。
    """
    try:
        decoded = base64.b64decode(payload, validate=True)
    except ValueError as exc:
        raise ParamError(f"消息体须为合法的 base64 编码：{exc}") from exc
    return ensure_payload_size(decoded)


def resolve_payload(request: PublishRequest) -> bytes:
    """取发布请求的消息体字节（帧路径直通、JSON 路径 base64 解码，上限同一处判）。

    Args:
        request: 发布请求。

    Returns:
        bytes: 业务字节。

    Raises:
        ParamError: 非法 base64（10001）。
        MsgPayloadTooLargeError: 超过 1 MiB 上限（350006 / 413）。
    """
    raw = request.raw_payload
    if raw is not None:
        return ensure_payload_size(raw)
    return decode_payload(request.payload)
