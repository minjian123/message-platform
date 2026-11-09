"""消费组模型与存储端口（01-4 详细设计 §4、§5.3、§8.2）。

**三条结构性口径**：

1. **组身份 = `(tenant_id, topic_id, group)`**：与位点键 `(group, topic_id, partition_no)` 一一对应，
   组间位点天然隔离；`tenant_id` 冗余存一份只为管理面按租户列表（`topic_id` 雪花已隐含租户）。
2. **快照读写**：端口只有 `load` / `save`（整条覆盖）/ `list_for_tenant`，协调器不感知存储形态；
   8B_02 落 `msg_group` / `msg_group_member` 两表时端口签名不变（口径 1）。
3. **分配器是纯函数**：`assign_range()` 只依赖「成员 id 集合 + 分区数」，同样输入同样输出；
   换 sticky / cooperative 时新增函数与策略位，不动协议与状态机（口径 3、§4.4）。

**不落库、不共享**：进程内实现随进程重启清空（组与分配全丢，客户端重新 `join`），与消息 / 位点 / 死信
的现状一致；多副本下的协调者归属归 8B_02 / 8B_05（详细设计 §12 风险 1、2）。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from bms_msg.core.config import ASSIGNMENT_STRATEGY_RANGE

__all__ = [
    "ASSIGNMENT_STRATEGY_RANGE",
    "GROUP_STATE_DEAD",
    "GROUP_STATE_EMPTY",
    "GROUP_STATE_PREPARING_REBALANCE",
    "GROUP_STATE_STABLE",
    "ActivePartitions",
    "ConsumerGroup",
    "GroupKey",
    "GroupMember",
    "GroupStore",
    "InMemoryGroupStore",
    "assign_range",
]

GroupKey = tuple[int, int, str]
"""组定位键：`(tenant_id, topic_id, group)`。"""

ActivePartitions = ConcurrentStableDict[str, ConcurrentStableList[int]]
"""分配结果：`consumer_id → 分区号（升序）`。"""

GROUP_STATE_EMPTY = "empty"
"""组状态：存在但无成员（全部离开）；位点保留，消费停摆属正常。"""

GROUP_STATE_PREPARING_REBALANCE = "preparing_rebalance"
"""组状态：成员集合已变化、分配尚未生效（排障时卡在此态说明分配未完成或进程中断）。"""

GROUP_STATE_STABLE = "stable"
"""组状态：分配已生效，成员按 `generation` 拉取。"""

GROUP_STATE_DEAD = "dead"
"""组状态：组已被回收（组删除归 06 域，本任务只定义常量与迁移条件，不产生该态）。"""


@dataclass(frozen=True)
class GroupMember(BaseValueObject):
    """组内成员（存活判据只看 `last_seen`，与业务处理耗时无关）。"""

    consumer_id: str
    """成员标识（客户端自带；组内唯一由客户端保证，同 id 再 `join` 即重连）。"""

    joined_at: datetime
    """加入时间（UTC；重连不刷新，用于排障看「这个成员什么时候进来的」）。"""

    last_seen: datetime
    """最后一次心跳 / 加入时间（UTC；超过会话超时即判定失效）。"""

    partitions: ConcurrentStableList[int]
    """当前持有的分区号（升序；空闲成员为空列表）。"""


@dataclass(frozen=True)
class ConsumerGroup(BaseValueObject):
    """消费组记录（不可变快照：状态变化即整条替换并保存）。"""

    tenant_id: int
    """租户内部身份（雪花；取自基座解析链）。"""

    topic_id: int
    """主题主键（组按主题维度建，口径 5）。"""

    group: str
    """组名（1~128，`GROUP_NAME_PATTERN`）。"""

    state: str
    """组状态（`empty` / `preparing_rebalance` / `stable` / `dead`）。"""

    generation: int
    """世代号：成员集合每变化一次 +1，重连不递增；客户端凭它判断分配是否过期。"""

    members: ConcurrentStableList[GroupMember]
    """成员列表（按 `consumer_id` 升序，分配结果确定）。"""

    def member(self, consumer_id: str) -> GroupMember | None:
        """取指定成员。

        Args:
            consumer_id: 成员标识。

        Returns:
            GroupMember | None: 成员；不在组内返回 None。
        """
        for item in self.members:
            if item.consumer_id == consumer_id:
                return item
        return None

    def owner_of(self, partition_no: int) -> str | None:
        """取某分区的当前持有者。

        Args:
            partition_no: 分区号。

        Returns:
            str | None: 持有者 `consumer_id`；无成员或该分区未分配返回 None。
        """
        for item in self.members:
            if partition_no in item.partitions:
                return item.consumer_id
        return None

    def assigned_count(self) -> int:
        """取全组成员持有的分区总数（判定分配是否覆盖全部分区）。

        Returns:
            int: 分区总数。
        """
        return sum(len(item.partitions) for item in self.members)


def assign_range(members: ConcurrentStableList[str], partition_count: int) -> ActivePartitions:
    """range 分配：成员按 `consumer_id` 升序、分区按号连续切段（纯函数，口径 3）。

    余数分给字典序靠前的成员；成员数大于分区数时，靠后的成员拿到空列表（空闲成员，口径 11）。

    Args:
        members: 成员标识（调用方保证升序且不重复）。
        partition_count: 主题分区数。

    Returns:
        ActivePartitions: `consumer_id → 分区号（升序）`；无成员返回空映射。
    """
    assignment: ActivePartitions = ConcurrentStableDict()
    if not members:
        return assignment
    base, extra = divmod(partition_count, len(members))
    cursor = 0
    for index, consumer_id in enumerate(members):
        size = base + (1 if index < extra else 0)
        assignment.set(consumer_id, ConcurrentStableList(range(cursor, cursor + size)))
        cursor += size
    return assignment


class GroupStore(BaseFrameworkObject, ABC):
    """消费组存储端口（快照读写；8B_02 换 DB 实现，签名不变）。"""

    @abstractmethod
    async def load(self, *, tenant_id: int, topic_id: int, group: str) -> ConsumerGroup | None:
        """取组记录。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            ConsumerGroup | None: 组记录；未 `join` 过返回 None。
        """

    @abstractmethod
    async def save(self, record: ConsumerGroup) -> None:
        """整条覆盖保存（新建与更新同一入口）。

        Args:
            record: 组记录快照。
        """

    @abstractmethod
    async def list_for_tenant(
        self, *, tenant_id: int, topic_id: int | None = None
    ) -> ConcurrentStableList[ConsumerGroup]:
        """列出某租户的组记录（管理面只读视图）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题过滤；None 表示不限。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 组记录（组名升序）。
        """

    async def list_all(self) -> ConcurrentStableList[ConsumerGroup]:
        """列出全部组记录（后台指标采样用）。

        默认实现返回空清单（未知存储形态不提供全量遍历）；内存实现覆盖，
        DB 实现接管时按环境表补齐。新增非抽象方法不破坏既有实现。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 全部组记录。
        """
        return ConcurrentStableList()

    @abstractmethod
    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的全部组记录（租户删除时的易失资源清理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的组记录数。
        """


class InMemoryGroupStore(GroupStore):
    """内存组存储（首期实现；8B_02 换 `msg_group` / `msg_group_member`，端口与装配点不变）。"""

    def __init__(self) -> None:
        """初始化（空表）。"""
        self._groups: dict[GroupKey, ConsumerGroup] = {}

    async def load(self, *, tenant_id: int, topic_id: int, group: str) -> ConsumerGroup | None:
        """取组记录（未 `join` 过即 None）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题主键。
            group: 组名。

        Returns:
            ConsumerGroup | None: 组记录。
        """
        return self._groups.get((tenant_id, topic_id, group))

    async def save(self, record: ConsumerGroup) -> None:
        """整条覆盖保存。

        Args:
            record: 组记录快照。
        """
        self._groups[(record.tenant_id, record.topic_id, record.group)] = record

    async def list_for_tenant(
        self, *, tenant_id: int, topic_id: int | None = None
    ) -> ConcurrentStableList[ConsumerGroup]:
        """列出某租户的组记录（组名升序，组名相同按主题主键兜底）。

        Args:
            tenant_id: 租户主键。
            topic_id: 主题过滤；None 表示不限。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 组记录。
        """
        matched = [
            record
            for (owner, topic, _name), record in self._groups.items()
            if owner == tenant_id and (topic_id is None or topic == topic_id)
        ]
        return ConcurrentStableList(sorted(matched, key=lambda record: (record.group, record.topic_id)))

    async def list_all(self) -> ConcurrentStableList[ConsumerGroup]:
        """列出全部组记录（后台指标采样用；按租户 / 主题 / 组名稳定排序）。

        Returns:
            ConcurrentStableList[ConsumerGroup]: 全部组记录。
        """
        return ConcurrentStableList(
            sorted(
                self._groups.values(),
                key=lambda record: (record.tenant_id, record.topic_id, record.group),
            )
        )

    async def purge_tenant(self, *, tenant_id: int) -> int:
        """删除某租户的全部组记录（幂等）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 删除的组记录数。
        """
        keys = [key for key in self._groups if key[0] == int(tenant_id)]
        for key in keys:
            self._groups.pop(key, None)
        return len(keys)
