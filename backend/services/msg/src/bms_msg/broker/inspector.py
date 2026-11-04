"""消费组只读投影（01-4 详细设计 §7.4、§8.5）：列表 / 详情，拼装位点与积压。

**只读**：不写组、不动位点、不投递；`/admin/groups` 的最小集由本任务落地，06 域在此基础上扩字段
（积压趋势、筛选、多租户聚合）与指标（架构 06 §5.1 的 `msg_consumer_lag`）。

**两个取舍**：

- **`lag` 只进详情**：列表要遍历全租户所有组的所有分区，成本随分区数线性增长；详情是排障入口，
  单组分区数有限，逐分区取 `committed` / `LEO` 可接受。
- **主题缺失即跳过**：组记录按 `topic_id` 反查主题表，主题已删 / 不属于当前租户的记录直接跳过，
  避免管理面把孤儿组记录当成有效组展示。
"""

from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ParamError
from bms_core.core.objects import BaseValueObject

from bms_msg.broker.group import ConsumerGroup, GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.broker.router import TopicRouter
from bms_msg.errors import MsgGroupNotFoundError
from bms_msg.models.topic import MsgTopic
from bms_msg.repositories.topic import TopicRepository
from bms_msg.services.topic import MAX_LIST_SIZE
from bms_msg.tenant.context import CurrentTenant

__all__ = ["GroupDetailView", "GroupInspector", "GroupRow", "PartitionLag"]


@dataclass(frozen=True)
class GroupRow(BaseValueObject):
    """组列表行（组记录 + 主题展示名）。"""

    record: ConsumerGroup
    """组记录快照。"""

    namespace: str
    """命名空间（取自主题记录）。"""

    topic: str
    """主题名（取自主题记录）。"""

    assigned_partitions: int
    """全组已分配的分区总数（判「分配是否覆盖全部分区」）。"""


@dataclass(frozen=True)
class PartitionLag(BaseValueObject):
    """单分区位点与积压。"""

    partition_no: int
    """分区号。"""

    owner: str | None
    """当前持有者（无成员或未分配为 None）。"""

    committed: int
    """已提交位点（下一条待消费的 offset）。"""

    leo: int
    """分区日志末端位点。"""

    lag: int
    """积压（`leo - committed`）。"""


@dataclass(frozen=True)
class GroupDetailView(BaseValueObject):
    """组详情（列表行 + 分区明细）。"""

    row: GroupRow
    """组列表行（状态 / 世代 / 成员数）。"""

    partition_count: int
    """主题分区数。"""

    partitions: ConcurrentStableList[PartitionLag]
    """各分区位点与积压（按分区号升序）。"""


class GroupInspector:
    """组只读投影（请求级：平台库主题仓储 + 进程级组存储 / 消息存储 / 位点存储）。"""

    def __init__(
        self,
        *,
        groups: GroupStore,
        topics: TopicRepository,
        router: TopicRouter,
        messages: MessageStore,
        offsets: OffsetStore,
    ) -> None:
        """初始化。

        Args:
            groups: 组存储（进程级）。
            topics: 主题仓储（请求级；按主题主键反查展示名）。
            router: 主题路由（按 `namespace` + `name` 解析主题）。
            messages: 消息存储（取 `LEO`）。
            offsets: 位点存储（取已提交位点）。
        """
        self._groups = groups
        self._topics = topics
        self._router = router
        self._messages = messages
        self._offsets = offsets

    async def list_groups(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str | None,
        topic: str | None,
        page: int,
        size: int,
    ) -> tuple[ConcurrentStableList[GroupRow], int]:
        """分页列出组（按组名升序；可按命名空间 / 主题名过滤）。

        Args:
            tenant: 当前租户（列表按租户隔离）。
            namespace: 命名空间过滤；None 不限制（给定时同样校验合法性）。
            topic: 主题名过滤；None 不限制。
            page: 页码（从 1 起）。
            size: 每页条数（1~100）。

        Returns:
            tuple[ConcurrentStableList[GroupRow], int]: 当页行 + 过滤后总数。

        Raises:
            MsgNamespaceInvalidError: 命名空间过滤值非法（350010）。
            ParamError: 分页参数越界（10001）。
        """
        if namespace is not None:
            tenant.ensure_namespace(namespace)
        if page < 1:
            raise ParamError(f"页码须从 1 起：{page}")
        if not 1 <= size <= MAX_LIST_SIZE:
            raise ParamError(f"每页条数须在 1~{MAX_LIST_SIZE}：{size}")
        records = await self._groups.list_for_tenant(tenant_id=int(tenant.tenant_id))
        rows: ConcurrentStableList[GroupRow] = ConcurrentStableList()
        for record in records:
            resolved = await self._topics.get(record.topic_id)
            if resolved is None:
                continue
            if namespace is not None and resolved.namespace != namespace:
                continue
            if topic is not None and resolved.name != topic:
                continue
            rows.add(self._row(record=record, topic=resolved))
        total = len(rows)
        return ConcurrentStableList(list(rows)[(page - 1) * size : page * size]), total

    async def describe(self, *, tenant: CurrentTenant, namespace: str, topic: str, group: str) -> GroupDetailView:
        """取组详情（含各分区位点与积压）。

        Args:
            tenant: 当前租户。
            namespace: 命名空间。
            topic: 主题名。
            group: 组名。

        Returns:
            GroupDetailView: 组详情。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgTopicNotFoundError: 主题不存在、不属于当前租户或未启用（350001）。
            MsgGroupNotFoundError: 组不存在（350013）。
        """
        tenant.ensure_namespace(namespace)
        resolved = await self._router.resolve(namespace=namespace, name=topic)
        record = await self._groups.load(tenant_id=int(tenant.tenant_id), topic_id=int(resolved.id), group=group)
        if record is None:
            raise MsgGroupNotFoundError(f"消费组不存在：{group}")
        partition_count = int(resolved.partition_count)
        partitions: ConcurrentStableList[PartitionLag] = ConcurrentStableList()
        for partition_no in range(partition_count):
            committed = await self._offsets.get(group=group, topic_id=int(resolved.id), partition_no=partition_no)
            leo = await self._messages.leo(topic_id=int(resolved.id), partition_no=partition_no)
            partitions.add(
                PartitionLag(
                    partition_no=partition_no,
                    owner=record.owner_of(partition_no),
                    committed=committed,
                    leo=leo,
                    lag=leo - committed,
                )
            )
        return GroupDetailView(
            row=self._row(record=record, topic=resolved),
            partition_count=partition_count,
            partitions=partitions,
        )

    def _row(self, *, record: ConsumerGroup, topic: MsgTopic) -> GroupRow:
        """由组记录 + 主题记录拼一行。

        Args:
            record: 组记录。
            topic: 主题记录。

        Returns:
            GroupRow: 列表行。
        """
        return GroupRow(
            record=record,
            namespace=topic.namespace,
            topic=topic.name,
            assigned_partitions=record.assigned_count(),
        )
