"""消费组协议编排（01-4 详细设计 §5、§8.3）：加入 / 心跳 / 离开 / 分区归属校验。

**协议形态（已定口径）**：服务端主导 + eager 全量再均衡 + 单段下发——`join` / `heartbeat` 直接返回
`generation + partitions`，客户端只接收（无 Kafka 式 `sync` 段）；成员失效由服务端 `last_seen` + 会话超时
判定，显式 `leave` 只是正常退出的快速路径（口径 2、7、10）。

**四条不可动摇的口径**：

1. **只动组、不动位点**：位点唯一写入方仍是 `ack` 与 `acks=0` 的批量前移；撤销易主分区时只清在途
   （新 owner 立即可重投），位点与暂存确认一律不碰（口径 10、详细设计 §5.4）。
2. **重连不抖**：同 `consumer_id` 再 `join` 只刷新 `last_seen`，不递增世代、不重算分配（口径 4）。
3. **被动清理**：无后台定时器——`join` / `heartbeat` 进入时清理同组超时成员，全部成员同时死亡时组自然
   静默、位点不动（口径 7）。
4. **分配不完整即重算**：`Σ 成员分配分区数 ≠ partition_count` 时重算，覆盖 `expand_partitions()` 后
   新分区自动纳入（详细设计 §5.5、对齐记录 15）。

`GroupCoordinator` 是**请求级**对象（绑定平台库主题仓储与进程级组存储 / 投递状态），与 `DeliveryService`
同构；数据面入口（HTTP 端点 / 8B_01 帧入口）共用同一个 `ensure_owner()`，组校验只有一套实现。
"""

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import (
    GROUP_STATE_EMPTY,
    GROUP_STATE_PREPARING_REBALANCE,
    GROUP_STATE_STABLE,
    ConsumerGroup,
    GroupMember,
    GroupStore,
    assign_range,
)
from bms_msg.broker.router import TopicRouter
from bms_msg.broker.topic_cache import TopicSnapshot
from bms_msg.core.config import SESSION_TIMEOUT_MS
from bms_msg.errors import MsgGroupCountQuotaExceededError, MsgGroupNotFoundError, MsgMemberInvalidError
from bms_msg.quota.service import QuotaService
from bms_msg.schemas.group import MemberRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = ["GroupCoordinator", "MemberState"]


def _utcnow() -> datetime:
    """当前 UTC 时间（会话超时的判据；用例注入固定时钟后不 sleep）。

    Returns:
        datetime: 带时区的当前时间。
    """
    return datetime.now(UTC)


@dataclass(frozen=True)
class MemberState(BaseValueObject):
    """一次成员操作的落点（组快照 + 该成员，避免调用方再取一次可能为空的成员）。"""

    group: ConsumerGroup
    """操作后的组快照。"""

    member: GroupMember
    """本次操作的成员（`join` / `heartbeat` 返回时必在组内）。"""


class GroupCoordinator:
    """消费组协议编排（请求级：平台库主题解析 + 进程级组存储与投递状态）。"""

    def __init__(
        self,
        *,
        groups: GroupStore,
        router: TopicRouter,
        delivery: DeliveryState,
        now: Callable[[], datetime] = _utcnow,
        quota: QuotaService | None = None,
    ) -> None:
        """初始化。

        Args:
            groups: 组存储（进程级）。
            router: 主题路由（请求级，绑定平台库主题仓储）。
            delivery: 投递状态（进程级；撤销易主分区时清在途）。
            now: 时钟（缺省 UTC 当前时间；用例注入固定时钟以模拟会话超时）。
            quota: 配额服务（None = 不判组数配额，兼容既有单测）。
        """
        self._groups = groups
        self._router = router
        self._delivery = delivery
        self._now = now
        self._quota = quota

    async def join(self, *, tenant: CurrentTenant, request: MemberRequest) -> MemberState:
        """加入组（组不存在则建；同 `consumer_id` 即重连）。

        Args:
            tenant: 当前租户（数据面经 `frame_tenant()` 解析）。
            request: 成员请求（`group` / `namespace` / `topic` / `consumer_id`）。

        Returns:
            MemberState: 组快照 + 该成员（含本次分配与世代）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        """
        topic = await self._resolve(tenant=tenant, request=request)
        partition_count = int(topic.partition_count)
        record = await self._load(tenant=tenant, topic_id=int(topic.id), group=request.group)
        if record is None:
            await self._ensure_group_quota(tenant=tenant)
            record = ConsumerGroup(
                tenant_id=int(tenant.tenant_id),
                topic_id=int(topic.id),
                group=request.group,
                state=GROUP_STATE_EMPTY,
                generation=0,
                members=ConcurrentStableList(),
            )
        record, pruned = self._prune(record)
        if record.member(request.consumer_id) is None:
            record = self._add(record=record, consumer_id=request.consumer_id)
            record = await self._rebalance(record=record, partition_count=partition_count)
            return MemberState(group=record, member=self._member_of(record, request.consumer_id))
        record = self._touch(record=record, consumer_id=request.consumer_id)
        if pruned or self._stale(record, partition_count):
            record = await self._rebalance(record=record, partition_count=partition_count)
        else:
            await self._groups.save(record)
        return MemberState(group=record, member=self._member_of(record, request.consumer_id))

    async def heartbeat(self, *, tenant: CurrentTenant, request: MemberRequest) -> MemberState:
        """刷新存活并取回当前分配（顺带清理同组超时成员，触发必要的再均衡）。

        Args:
            tenant: 当前租户。
            request: 成员请求。

        Returns:
            MemberState: 组快照 + 该成员（`generation` 变了即须按新 `partitions` 执行）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgGroupNotFoundError: 组不存在（350013）。
            MsgMemberInvalidError: 成员不在组内或已超时失效（350014）。
        """
        topic = await self._resolve(tenant=tenant, request=request)
        partition_count = int(topic.partition_count)
        record = await self._load(tenant=tenant, topic_id=int(topic.id), group=request.group)
        if record is None:
            raise MsgGroupNotFoundError(f"消费组不存在或尚未加入：{request.group}")
        record, pruned = self._prune(record)
        member = self._member_of(record, request.consumer_id)
        record = self._touch(record=record, consumer_id=member.consumer_id)
        if pruned or self._stale(record, partition_count):
            record = await self._rebalance(record=record, partition_count=partition_count)
        else:
            await self._groups.save(record)
        return MemberState(group=record, member=self._member_of(record, request.consumer_id))

    async def leave(self, *, tenant: CurrentTenant, request: MemberRequest) -> ConsumerGroup:
        """主动离开（幂等：成员未注册也返回成功，网络重试友好）。

        Args:
            tenant: 当前租户。
            request: 成员请求。

        Returns:
            ConsumerGroup: 离开后的组快照（无成员即 `empty`；仍有他人则已重算分配）。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgGroupNotFoundError: 组不存在（350013）。
        """
        topic = await self._resolve(tenant=tenant, request=request)
        partition_count = int(topic.partition_count)
        record = await self._load(tenant=tenant, topic_id=int(topic.id), group=request.group)
        if record is None:
            raise MsgGroupNotFoundError(f"消费组不存在或尚未加入：{request.group}")
        remaining = ConcurrentStableList(
            member for member in record.members if member.consumer_id != request.consumer_id
        )
        if len(remaining) == len(record.members):
            return record
        after = replace(record, members=remaining)
        if not remaining:
            empty = replace(after, state=GROUP_STATE_EMPTY, generation=record.generation + 1)
            await self._groups.save(empty)
            await self._revoke(before=record, after=empty, partition_count=partition_count)
            return empty
        return await self._rebalance(record=after, partition_count=partition_count)

    async def ensure_owner(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str,
        topic: str,
        group: str,
        consumer_id: str,
        partition: int,
    ) -> None:
        """校验「成员活跃 + 该分区属于它」（`consume` / `ack` 的唯一入口校验）。

        Args:
            tenant: 当前租户。
            namespace: 命名空间。
            topic: 主题名。
            group: 组名。
            consumer_id: 成员标识。
            partition: 请求的分区号。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgGroupNotFoundError: 组不存在（350013）。
            MsgPartitionCountInvalidError: 分区号越界（350009）。
            MsgMemberInvalidError: 成员不在组内、已失效，或分区不在其分配内（350014）。
        """
        tenant.ensure_namespace(namespace)
        resolved = await self._router.resolve(namespace=namespace, name=topic)
        record = await self._load(tenant=tenant, topic_id=int(resolved.id), group=group)
        if record is None:
            raise MsgGroupNotFoundError(f"消费组不存在或尚未加入：{group}")
        self._router.partition(resolved, partition)
        member = record.member(consumer_id)
        if member is None or self._expired(member) or partition not in member.partitions:
            raise MsgMemberInvalidError(f"分区不在该成员的有效分配内：{group}/{consumer_id} × 分区 {partition}")

    async def _resolve(self, *, tenant: CurrentTenant, request: MemberRequest) -> TopicSnapshot:
        """校验命名空间并解析主题（判定顺序：租户 → 命名空间 → 主题）。

        Args:
            tenant: 当前租户。
            request: 成员请求。

        Returns:
            TopicSnapshot: 主题快照。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
        """
        tenant.ensure_namespace(request.namespace)
        return await self._router.resolve(namespace=request.namespace, name=request.topic)

    async def _load(self, *, tenant: CurrentTenant, topic_id: int, group: str) -> ConsumerGroup | None:
        """取组记录（按 `(tenant_id, topic_id, group)`）。

        Args:
            tenant: 当前租户。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            ConsumerGroup | None: 组记录；未 `join` 过返回 None。
        """
        return await self._groups.load(tenant_id=int(tenant.tenant_id), topic_id=topic_id, group=group)

    async def _ensure_group_quota(self, *, tenant: CurrentTenant) -> None:
        """校验消费组数配额（仅新建组时调用；硬限已到即 `350026`）。

        Args:
            tenant: 当前租户。

        Raises:
            MsgGroupCountQuotaExceededError: 组数已达硬限（350026 / 429）。
        """
        if self._quota is None:
            return
        limit = await self._quota.effective(int(tenant.tenant_id), "", "group_count")
        if limit.limit <= 0:
            return
        used = len(await self._groups.list_for_tenant(tenant_id=int(tenant.tenant_id)))
        if used >= limit.limit:
            raise MsgGroupCountQuotaExceededError(
                f"消费组数已达配额上限：{used}/{limit.limit}",
                limit=limit.limit,
                usage=used,
            )

    def _member_of(self, record: ConsumerGroup, consumer_id: str) -> GroupMember:
        """取成员，不存在即 `350014`（`heartbeat` 的成员判定与 `join` 的返回取值共用）。

        Args:
            record: 组快照。
            consumer_id: 成员标识。

        Returns:
            GroupMember: 成员。

        Raises:
            MsgMemberInvalidError: 成员不在组内（含已超时被清理、已离开，350014）。
        """
        member = record.member(consumer_id)
        if member is None:
            raise MsgMemberInvalidError(f"成员不在组内（须重新 join）：{record.group}/{consumer_id}")
        return member

    def _expired(self, member: GroupMember) -> bool:
        """判定成员是否已超时会话（`last_seen` 距当前 ≥ 会话超时）。

        Args:
            member: 成员。

        Returns:
            bool: 已失效返回 True。
        """
        return self._now() - member.last_seen >= timedelta(milliseconds=SESSION_TIMEOUT_MS)

    def _prune(self, record: ConsumerGroup) -> tuple[ConsumerGroup, bool]:
        """清理超时成员（被动清理：无后台定时器，进入 `join` / `heartbeat` 时执行）。

        Args:
            record: 组快照。

        Returns:
            tuple[ConsumerGroup, bool]: 清理后的快照 + 是否有成员被清理。
        """
        alive = ConcurrentStableList(member for member in record.members if not self._expired(member))
        if len(alive) == len(record.members):
            return record, False
        return replace(record, members=alive), True

    def _stale(self, record: ConsumerGroup, partition_count: int) -> bool:
        """判定分配是否已不完整（成员非空且持有分区总数 ≠ 主题分区数，如扩分区后）。

        Args:
            record: 组快照。
            partition_count: 主题分区数。

        Returns:
            bool: 需要重算返回 True。
        """
        return bool(record.members) and record.assigned_count() != partition_count

    def _add(self, record: ConsumerGroup, *, consumer_id: str) -> ConsumerGroup:
        """加入新成员（保持成员按 `consumer_id` 升序，分配结果确定）。

        Args:
            record: 组快照。
            consumer_id: 成员标识。

        Returns:
            ConsumerGroup: 加入后的快照（尚未分配）。
        """
        now = self._now()
        member = GroupMember(
            consumer_id=consumer_id,
            joined_at=now,
            last_seen=now,
            partitions=ConcurrentStableList(),
        )
        members = ConcurrentStableList(sorted([*record.members, member], key=lambda item: item.consumer_id))
        return replace(record, members=members)

    def _touch(self, record: ConsumerGroup, *, consumer_id: str) -> ConsumerGroup:
        """刷新成员 `last_seen`（重连与心跳共用）。

        Args:
            record: 组快照。
            consumer_id: 成员标识。

        Returns:
            ConsumerGroup: 刷新后的快照。
        """
        now = self._now()
        members = ConcurrentStableList(
            replace(member, last_seen=now) if member.consumer_id == consumer_id else member for member in record.members
        )
        return replace(record, members=members)

    async def _rebalance(self, *, record: ConsumerGroup, partition_count: int) -> ConsumerGroup:
        """eager 全量再均衡：标记 `preparing_rebalance` → 重算分配 → 撤销易主分区在途 → `stable`。

        两次 `save` 让中间态可观测（进程在两步之间中断时，排障能看到组停在哪一步）。

        Args:
            record: 组快照（成员集合已是最新）。
            partition_count: 主题分区数。

        Returns:
            ConsumerGroup: 分配生效后的快照（`generation` 已 +1）。
        """
        await self._groups.save(replace(record, state=GROUP_STATE_PREPARING_REBALANCE))
        assignment = assign_range(
            ConcurrentStableList(member.consumer_id for member in record.members), partition_count
        )
        members = ConcurrentStableList(
            replace(
                member,
                partitions=ConcurrentStableList(assignment.get(member.consumer_id, ConcurrentStableList[int]())),
            )
            for member in record.members
        )
        stable = replace(
            record,
            state=GROUP_STATE_STABLE,
            generation=record.generation + 1,
            members=members,
        )
        await self._groups.save(stable)
        await self._revoke(before=record, after=stable, partition_count=partition_count)
        return stable

    async def _revoke(self, *, before: ConsumerGroup, after: ConsumerGroup, partition_count: int) -> None:
        """撤销易主分区的在途登记（位点与暂存确认不动；新 owner 从已提交位点续读）。

        Args:
            before: 再均衡前的组快照。
            after: 再均衡后的组快照。
            partition_count: 主题分区数。
        """
        moved = [
            partition_no
            for partition_no in range(partition_count)
            if before.owner_of(partition_no) != after.owner_of(partition_no)
        ]
        if not moved:
            return
        async with self._delivery.lock:
            for partition_no in moved:
                self._delivery.drop_partition((after.group, after.topic_id, partition_no))
