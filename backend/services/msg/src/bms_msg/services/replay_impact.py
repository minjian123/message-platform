"""重放影响投影（06-6-1 详细设计 §5）：受影响组解析、积压基线与终态快照。

**口径三条**：

1. **组范围 = 组订阅关系**：按 01-4 的 `GroupStore.list_for_tenant` 取租户内组，再按 `topic_id` 过滤，
   不另建「谁订阅了这个主题」的第二套定义。
2. **积压算式与 01-4 同源**：分区 `lag = leo - committed`，组级为各分区求和，**不夹取负值**——位点被
   重置到 `LEO` 之后出现负值是真实状态，夹取会让重放详情与组详情页显示不一致。
3. **基线冻结分区集合**：基线记下创建时刻主题的全部分区及各自位点，之后度量只按**同一分区集合**重取，
   主题扩分区不会让 baseline 与 current 口径分叉；基线之后新建的组不回填（组落库归 8B_02）。

本模块只做只读投影：不认任务状态机、不写任务行，也不碰审计；状态判断与落库时机在 `ReplayService`。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject
from bms_core.core.roots import BaseFrameworkObject

from bms_msg.broker.group import GroupStore
from bms_msg.broker.log import MessageStore
from bms_msg.broker.offset import OffsetStore
from bms_msg.core.config import REPLAY_IMPACT_MAX_GROUPS

__all__ = [
    "AffectedGroup",
    "ImpactBaseline",
    "ImpactGroupBaseline",
    "ImpactPartition",
    "ReplayImpactInspector",
    "build_impact_inspector",
]

_LOGGER = get_logger("bms_msg.replay_impact")


def _utcnow() -> datetime:
    """当前 UTC naive 时间（与任务行 `finished_at` 等列口径一致）。

    Returns:
        datetime: UTC naive 当前时间。
    """
    return datetime.now(UTC).replace(tzinfo=None)


def _utc_naive(value: datetime) -> datetime:
    """把时间规整为 UTC naive（列与契约的统一口径：带时区的入参在采集 / 快照前先归整）。

    Args:
        value: 带时区或 naive 的时间（naive 按 UTC 解释）。

    Returns:
        datetime: UTC naive 时间。
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _int_of(value: object) -> int:
    """把 JSON 解出的数值规整为 `int`（strict 类型检查下 `int(object)` 不合法）。

    Args:
        value: JSON 解出的值。

    Returns:
        int: 整数值。
    """
    return cast("int", value)


def _str_of(value: object) -> str:
    """把 JSON 解出的值规整为 `str`。

    Args:
        value: JSON 解出的值。

    Returns:
        str: 字符串值。
    """
    return cast("str", value)


@dataclass(frozen=True)
class ImpactPartition(BaseValueObject):
    """基线里的单分区位点（度量时按同一分区号重取 committed / leo）。"""

    partition_no: int
    """分区号。"""

    committed: int
    """基线时刻已提交位点（下一条待消费的 offset）。"""

    leo: int
    """基线时刻分区日志末端位点。"""

    @property
    def lag(self) -> int:
        """分区积压（`leo - committed`，不夹取负值）。

        Returns:
            int: 积压条数（可负）。
        """
        return self.leo - self.committed

    def payload(self) -> ConcurrentStableDict[str, object]:
        """序列化为落库形态（列名 `partition` 与表文件一致）。

        Returns:
            ConcurrentStableDict[str, object]: `{"partition","committed","leo"}`。
        """
        return ConcurrentStableDict({"partition": self.partition_no, "committed": self.committed, "leo": self.leo})

    @classmethod
    def from_payload(cls, raw: ConcurrentStableDict[str, object]) -> ImpactPartition:
        """由落库形态还原。

        Args:
            raw: `{"partition","committed","leo"}`。

        Returns:
            ImpactPartition: 分区位点。
        """
        return cls(
            partition_no=_int_of(raw["partition"]),
            committed=_int_of(raw["committed"]),
            leo=_int_of(raw["leo"]),
        )


@dataclass(frozen=True)
class ImpactGroupBaseline(BaseValueObject):
    """基线里的单个消费组（组级积压 + 分区明细）。"""

    group: str
    """消费组名。"""

    baseline_lag: int
    """基线时刻组级积压（各分区求和）。"""

    partitions: ConcurrentStableList[ImpactPartition]
    """基线时刻各分区位点（度量只按这些分区重取）。"""

    def payload(self) -> ConcurrentStableDict[str, object]:
        """序列化为落库形态。

        Returns:
            ConcurrentStableDict[str, object]: `{"group","baseline_lag","partitions"}`。
        """
        return ConcurrentStableDict(
            {
                "group": self.group,
                "baseline_lag": self.baseline_lag,
                "partitions": [item.payload() for item in self.partitions],
            }
        )

    @classmethod
    def from_payload(cls, raw: ConcurrentStableDict[str, object]) -> ImpactGroupBaseline:
        """由落库形态还原。

        Args:
            raw: `{"group","baseline_lag","partitions"}`。

        Returns:
            ImpactGroupBaseline: 组基线。
        """
        partitions = cast("ConcurrentStableList[ConcurrentStableDict[str, object]]", raw["partitions"])
        return cls(
            group=_str_of(raw["group"]),
            baseline_lag=_int_of(raw["baseline_lag"]),
            partitions=ConcurrentStableList(ImpactPartition.from_payload(item) for item in partitions),
        )


@dataclass(frozen=True)
class ImpactBaseline(BaseValueObject):
    """影响基线（创建任务时的受影响组与积压起点）。"""

    captured_at: datetime
    """采集时刻（UTC naive）。"""

    truncated: bool
    """是否因组数上限截断（`REPLAY_IMPACT_MAX_GROUPS`）。"""

    groups: ConcurrentStableList[ImpactGroupBaseline]
    """受影响组（组名升序）。"""

    def payload(self) -> ConcurrentStableDict[str, object]:
        """序列化为落库形态。

        Returns:
            ConcurrentStableDict[str, object]: `{"captured_at","truncated","groups"}`。
        """
        return ConcurrentStableDict(
            {
                "captured_at": self.captured_at.isoformat(),
                "truncated": self.truncated,
                "groups": [item.payload() for item in self.groups],
            }
        )


@dataclass(frozen=True)
class AffectedGroup(BaseValueObject):
    """影响视图的单个组（基线 / 当前 / 变化，供响应与快照共用）。"""

    group: str
    """消费组名。"""

    baseline_lag: int
    """基线时刻组级积压。"""

    current_lag: int
    """当前组级积压（同一分区集合）。"""

    delta: int
    """变化量（`current_lag - baseline_lag`，可负）。"""

    captured_at: datetime
    """该值所属时刻：未终结任务为基线时刻，已终结任务为快照时刻。"""


class ReplayImpactInspector(BaseFrameworkObject):
    """影响投影（请求级：组存储 + 位点存储 + 消息存储，不持有数据库会话）。"""

    def __init__(
        self,
        *,
        groups: GroupStore,
        offsets: OffsetStore,
        messages: MessageStore,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """初始化。

        Args:
            groups: 组存储（解析受影响组）。
            offsets: 位点存储（取已提交位点）。
            messages: 消息存储（取分区 `LEO`）。
            now: 时钟（缺省 UTC naive 当前时间；用例注入固定时钟）。
        """
        self._groups = groups
        self._offsets = offsets
        self._messages = messages
        self._now = now or _utcnow

    async def capture(self, *, tenant_id: int, topic_id: int, partition_count: int) -> ImpactBaseline:
        """采集影响基线（建任务时调用一次）。

        Args:
            tenant_id: 租户主键（组记录按租户隔离）。
            topic_id: 主题主键。
            partition_count: 主题分区数（基线冻结此刻的全部分区）。

        Returns:
            ImpactBaseline: 基线（无订阅组时 `groups` 为空，`captured_at` 照写）。
        """
        records = await self._groups.list_for_tenant(tenant_id=tenant_id)
        matched = ConcurrentStableList(
            sorted((record for record in records if int(record.topic_id) == topic_id), key=lambda item: item.group)
        )
        truncated = len(matched) > REPLAY_IMPACT_MAX_GROUPS
        if truncated:
            _LOGGER.warning(
                "msg_replay_impact_truncated",
                topic_id=topic_id,
                total=len(matched),
                kept=REPLAY_IMPACT_MAX_GROUPS,
            )
        groups: ConcurrentStableList[ImpactGroupBaseline] = ConcurrentStableList()
        for record in list(matched)[:REPLAY_IMPACT_MAX_GROUPS]:
            groups.add(
                await self._baseline_group(group=record.group, topic_id=topic_id, partition_count=partition_count)
            )
        return ImpactBaseline(captured_at=_utc_naive(self._now()), truncated=truncated, groups=groups)

    async def measure(
        self, *, topic_id: int, baseline: ImpactBaseline, captured_at: datetime | None = None
    ) -> ConcurrentStableList[AffectedGroup]:
        """按基线的分区集合重取位点，算出当前积压与变化量。

        Args:
            topic_id: 主题主键。
            baseline: 影响基线（提供组名与分区集合）。
            captured_at: 结果的时刻标签；缺省为基线时刻（未终结任务）。

        Returns:
            ConcurrentStableList[AffectedGroup]: 各组影响（顺序同基线）。
        """
        moment = baseline.captured_at if captured_at is None else _utc_naive(captured_at)
        affected: ConcurrentStableList[AffectedGroup] = ConcurrentStableList()
        for group in baseline.groups:
            current = await self._current_lag(group=group, topic_id=topic_id)
            affected.add(
                AffectedGroup(
                    group=group.group,
                    baseline_lag=group.baseline_lag,
                    current_lag=current,
                    delta=current - group.baseline_lag,
                    captured_at=moment,
                )
            )
        return affected

    def snapshot_payload(
        self, *, affected: ConcurrentStableList[AffectedGroup], truncated: bool, captured_at: datetime
    ) -> ConcurrentStableDict[str, object]:
        """组装终态快照的落库形态（只留基线 / 当前 / 变化，不留分区明细）。

        Args:
            affected: 度量结果。
            truncated: 基线是否被截断。
            captured_at: 快照时刻（UTC naive）。

        Returns:
            ConcurrentStableDict[str, object]: `{"captured_at","truncated","groups"}`。
        """
        return ConcurrentStableDict(
            {
                "captured_at": _utc_naive(captured_at).isoformat(),
                "truncated": truncated,
                "groups": [
                    {
                        "group": item.group,
                        "baseline_lag": item.baseline_lag,
                        "current_lag": item.current_lag,
                        "delta": item.delta,
                    }
                    for item in affected
                ],
            }
        )

    @staticmethod
    def decode_baseline(raw: ConcurrentStableDict[str, object] | None) -> ImpactBaseline | None:
        """解码基线列（历史任务或列缺省时返回 None，不抛异常）。

        Args:
            raw: `impact_groups` 列值。

        Returns:
            ImpactBaseline | None: 基线；缺基线为 None。
        """
        if not raw or "groups" not in raw or "captured_at" not in raw:
            return None
        groups = cast("ConcurrentStableList[ConcurrentStableDict[str, object]]", raw["groups"])
        return ImpactBaseline(
            captured_at=datetime.fromisoformat(_str_of(raw["captured_at"])),
            truncated=bool(raw.get("truncated", False)),
            groups=ConcurrentStableList(ImpactGroupBaseline.from_payload(item) for item in groups),
        )

    @staticmethod
    def decode_snapshot(
        raw: ConcurrentStableDict[str, object] | None,
    ) -> tuple[ConcurrentStableList[AffectedGroup], bool] | None:
        """解码终态快照列（缺快照或结构不可解时返回 None）。

        Args:
            raw: `impact_snapshot` 列值。

        Returns:
            tuple[ConcurrentStableList[AffectedGroup], bool] | None: 影响清单 + 截断标记；无快照为 None。
        """
        if not raw or "groups" not in raw or "captured_at" not in raw:
            return None
        captured_at = datetime.fromisoformat(_str_of(raw["captured_at"]))
        groups = cast("ConcurrentStableList[ConcurrentStableDict[str, object]]", raw["groups"])
        affected: ConcurrentStableList[AffectedGroup] = ConcurrentStableList()
        for item in groups:
            affected.add(
                AffectedGroup(
                    group=_str_of(item["group"]),
                    baseline_lag=_int_of(item["baseline_lag"]),
                    current_lag=_int_of(item["current_lag"]),
                    delta=_int_of(item["delta"]),
                    captured_at=captured_at,
                )
            )
        return affected, bool(raw.get("truncated", False))

    async def _baseline_group(self, *, group: str, topic_id: int, partition_count: int) -> ImpactGroupBaseline:
        """采集单个组的基线（逐分区取 committed / leo）。

        Args:
            group: 组名。
            topic_id: 主题主键。
            partition_count: 主题分区数。

        Returns:
            ImpactGroupBaseline: 组基线。
        """
        partitions: ConcurrentStableList[ImpactPartition] = ConcurrentStableList()
        total = 0
        for partition_no in range(partition_count):
            committed = await self._offsets.get(group=group, topic_id=topic_id, partition_no=partition_no)
            leo = await self._messages.leo(topic_id=topic_id, partition_no=partition_no)
            partitions.add(ImpactPartition(partition_no=partition_no, committed=committed, leo=leo))
            total += leo - committed
        return ImpactGroupBaseline(group=group, baseline_lag=total, partitions=partitions)

    async def _current_lag(self, *, group: ImpactGroupBaseline, topic_id: int) -> int:
        """按基线分区集合重算组级当前积压。

        Args:
            group: 组基线（提供组名与分区集合）。
            topic_id: 主题主键。

        Returns:
            int: 当前组级积压（可负）。
        """
        total = 0
        for partition in group.partitions:
            committed = await self._offsets.get(
                group=group.group, topic_id=topic_id, partition_no=partition.partition_no
            )
            leo = await self._messages.leo(topic_id=topic_id, partition_no=partition.partition_no)
            total += leo - committed
        return total


def build_impact_inspector(
    *, groups: GroupStore, offsets: OffsetStore, messages: MessageStore
) -> ReplayImpactInspector:
    """按三个进程级端口组装影响投影（两个调用点共用同一装配口径：重放端点与死信重放端点）。

    Args:
        groups: 组存储（解析受影响组）。
        offsets: 位点存储（取已提交位点）。
        messages: 消息存储（取分区 `LEO`）。

    Returns:
        ReplayImpactInspector: 影响投影。
    """
    return ReplayImpactInspector(groups=groups, offsets=offsets, messages=messages)
