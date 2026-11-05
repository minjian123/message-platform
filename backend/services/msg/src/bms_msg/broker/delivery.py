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

from bms_msg.broker.deadletter import DEAD_LETTER_EXHAUSTED, DeadLetter, DeadLetterQueue
from bms_msg.broker.log import MessageStore, StoredMessage
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.core.config import ACKS_FIRE_AND_FORGET, DEFAULT_VISIBILITY_TIMEOUT_MS, MAX_DELIVERY_ATTEMPTS
from bms_msg.core.message_id import new_message_id
from bms_msg.errors import (
    MsgDeliveryExhaustedError,
    MsgMessageNotFoundError,
    MsgOffsetInvalidError,
    MsgPayloadTooLargeError,
)
from bms_msg.protocol.frame import MAX_FRAME_PAYLOAD_BYTES
from bms_msg.schemas.message import AckRequest, ConsumeRequest, PublishRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = ["Delivery", "DeliveryService", "DeliveryState", "InflightDelivery", "PartitionKey", "decode_payload"]

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
    """进程级投递状态：在途投递 + 乱序已确认 offset + 状态临界区锁。"""

    def __init__(self) -> None:
        """初始化（无在途、无暂存确认）。"""
        self._inflight: dict[PartitionKey, dict[int, InflightDelivery]] = {}
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

    def track(self, key: PartitionKey, offset: int, delivery: InflightDelivery) -> None:
        """登记一次在途投递（覆盖同 offset 的旧记录）。

        Args:
            key: 投递状态键。
            offset: 分区内序号。
            delivery: 在途投递记录。
        """
        self._inflight.setdefault(key, {})[offset] = delivery

    def forget(self, key: PartitionKey, offset: int) -> None:
        """移除在途记录（确认或转死信后）。

        Args:
            key: 投递状态键。
            offset: 分区内序号。
        """
        self._inflight.get(key, {}).pop(offset, None)

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
        """清掉某分区全部在途登记（分区易主时用；位点与暂存确认**不动**）。

        01-4 再均衡撤销易主分区时调用：不清则新 owner 会被「他人未过期在途」挡住整批拉取
        （01-3 停等），最长空转一个可见性超时；清掉后新 owner 立即可从已提交位点重投，
        代价是未 ack 消息多投一次（至少一次语义允许）。`acked` 集合保留——已确认但受缺口阻塞的
        offset 仍应参与连续前缀推进，清掉只会多一次重复投递。

        Args:
            key: 投递状态键。
        """
        self._inflight.pop(key, None)


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
    ) -> None:
        """初始化。

        Args:
            store: 消息存储（进程级）。
            offsets: 位点存储（进程级）。
            dead_letters: 死信队列（进程级）。
            router: 主题路由（请求级，绑定平台库主题仓储）。
            state: 投递状态（进程级）。
            now: 时钟（缺省 UTC 当前时间；用例注入固定时钟以模拟可见性超时）。
        """
        self._store = store
        self._offsets = offsets
        self._dead_letters = dead_letters
        self._router = router
        self._state = state
        self._now = now

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
            ParamError: 载荷非合法 base64（10001）。
        """
        tenant.ensure_namespace(request.namespace)
        topic = await self._router.resolve(namespace=request.namespace, name=request.topic)
        partition_no = self._router.route(topic, request.key)
        return await self._store.append(
            topic_id=int(topic.id),
            partition_no=partition_no,
            message_id=new_message_id(),
            log_id=new_message_id(),
            key=request.key,
            payload=decode_payload(request.payload),
            headers=ConcurrentStableDict(request.headers.items()),
            trace_id=request.trace_id,
            published_at=self._now(),
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
                request=request, key=key, topic_id=topic_id, partition_no=partition_no, committed=committed
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
            return advanced

    async def _collect(
        self,
        *,
        request: ConsumeRequest,
        key: PartitionKey,
        topic_id: int,
        partition_no: int,
        committed: int,
    ) -> ConcurrentStableList[Delivery]:
        """从已提交位点起按 offset 升序收集一批投递（分区内停等）。

        扫描规则（优先级从高到低；详细设计 §4.2）：

        1. **已死信** → 跳过（顺序性唯一破例：消息已终结，不投、不阻塞）；
        2. **未过期在途** → 停止本批（未确认队头不越过其后的任何 offset）；
        3. **本批已含重投且当前是新消息** → 停止本批（新消息留到下一批，重试不与主链路同批）；
        4. 在途已过期且 `attempts + 1 > 上限` → 转死信 → 继续看下一条；
        5. 其余 → 投出（`acks=1` 登记在途；`acks=0` 不登记）。

        Args:
            request: 拉取请求。
            key: 投递状态键。
            topic_id: 主题主键。
            partition_no: 分区号。
            committed: 已提交位点。

        Returns:
            ConcurrentStableList[Delivery]: 本次投递（可能为空；按 offset 升序）。

        Raises:
            MsgDeliveryExhaustedError: 无（死信在此路径静默跳过，仅 ack 路径报 350007）。
        """
        delivered: ConcurrentStableList[Delivery] = ConcurrentStableList()
        cursor = committed
        contiguous = committed
        retried = False
        while len(delivered) < request.max_messages:
            batch = await self._store.fetch(
                topic_id=topic_id,
                partition_no=partition_no,
                from_offset=cursor,
                max_messages=request.max_messages - len(delivered),
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
                delivery = await self._deliver(key=key, message=message, acks=request.acks, existing=existing)
                if delivery is None:
                    # 本次刚转死信：与「已死信」同处置，继续看下一条。
                    continue
                delivered.add(delivery)
                if existing is not None:
                    retried = True
                contiguous = message.offset + 1
                if len(delivered) == request.max_messages:
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
        )
        return Delivery(message=message, delivery_count=attempts)

    async def _dead_letter(self, *, key: PartitionKey, message: StoredMessage) -> None:
        """把毒消息转入死信并前移位点（不删日志）。

        Args:
            key: 投递状态键。
            message: 被放弃的消息。
        """
        _group, topic_id, partition_no = key
        await self._dead_letters.add(
            DeadLetter(
                message_id=message.message_id,
                topic_id=topic_id,
                partition_no=partition_no,
                offset=message.offset,
                attempts=MAX_DELIVERY_ATTEMPTS,
                failed_at=self._now(),
                reason=DEAD_LETTER_EXHAUSTED,
            )
        )
        self._state.forget(key, message.offset)
        await self._commit(key=key, offset=message.offset)

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


def decode_payload(payload: str) -> bytes:
    """把 base64 载荷解码为业务字节（形态属请求契约，非法即 `10001`）。

    解码后判 1 MiB 上限（`350006`）：普通发布与延迟发布共用本函数，因此**两条分支同一判据**；
    帧路径的消息体也经此进入（先 base64 再由本函数还原），上限只在这一点判。

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
    if len(decoded) > MAX_FRAME_PAYLOAD_BYTES:
        raise MsgPayloadTooLargeError(f"消息体超过上限 {MAX_FRAME_PAYLOAD_BYTES} 字节（实际 {len(decoded)} 字节）")
    return decoded
