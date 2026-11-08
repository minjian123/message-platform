"""用量快照（05-2）：配额判定与平台投影的**唯一取数口径**。

七项指标分源取数：

| 指标 | 来源 | 失败口径 |
| --- | --- | --- |
| 主题数 / 分区数 | 平台库元数据表 | DB 失败上抛（管理面 500；商业面不因配额误拒） |
| 消息存储 | 数据目录扫描（TTL 缓存） | 返回 `None`（投影 `unavailable`，配额不拒绝） |
| 今日投递 | 进程内 UTC 日计数 | 重启清零（8B_05 持久化） |
| 组数 / 未确认 | 进程内组记录 + 投递状态 | 实时（进程内，读不到即 0） |
| 连接数 | 连接登记表按 `tenant_id` 归集 | 实时（匿名连接不计） |

**不落库**：用量是实时读数，配额表只存限额，避免双写与口径漂移。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.logging import get_logger
from bms_core.db.session import DbSession
from sqlalchemy import func, select

from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.group import GroupStore
from bms_msg.core.config import DATA_ROOT_SUBDIR, QUOTA_STORAGE_SAMPLE_TTL_MS
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import MsgTopic
from bms_msg.usage.counter import DeliveryCounter

__all__ = ["StorageUsageSampler", "UsageSnapshot", "UsageSnapshotService"]

_LOGGER = get_logger("bms_msg.usage")


@dataclass(frozen=True)
class UsageSnapshot:
    """一次用量快照（七项；不可用值为 `None`）。"""

    topic_count: int
    """主题数（含停用主题）。"""

    partition_count: int
    """分区数。"""

    storage_bytes: int | None
    """消息存储字节数；目录扫描失败为 None。"""

    delivered_today: int
    """今日投递条数（进程内 UTC 日计数）。"""

    group_count: int
    """消费组数（含空组）。"""

    unacked_count: int
    """未确认条数（按租户聚合）。"""

    connection_count: int
    """有效连接数（按租户；匿名连接不计）。"""

    def value_of(self, key: str) -> int | None:
        """按配额维度键取用量。

        Args:
            key: 配额维度键。

        Returns:
            int | None: 用量；未知键 / 不可用返回 None。
        """
        mapping: ConcurrentStableDict[str, int | None] = ConcurrentStableDict(
            {
                "topic_count": self.topic_count,
                "group_count": self.group_count,
                "produce_rate": None,
                "storage_bytes": self.storage_bytes,
                "unacked_count": self.unacked_count,
                "connection_count": self.connection_count,
            }
        )
        return mapping.get(key)


@dataclass(frozen=True)
class _SampleEntry:
    """存储采样缓存条目。"""

    sampled_at: float
    """采样时刻（`monotonic` 秒）。"""

    value: int | None
    """字节数；扫描失败为 None。"""


class StorageUsageSampler:
    """租户存储用量采样器（目录扫描 + TTL 缓存 + 失败降级）。"""

    def __init__(
        self,
        *,
        root: Path,
        ttl_ms: int = QUOTA_STORAGE_SAMPLE_TTL_MS,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """初始化。

        Args:
            root: 消息数据根（其下 `data/{tenant_id}` 为租户目录）。
            ttl_ms: 采样有效期（毫秒）。
            clock: 单调时钟（缺省 `time.monotonic`；用例注入固定值）。
        """
        self._root = Path(root)
        self._ttl_ms = ttl_ms
        self._clock = clock if clock is not None else time.monotonic
        self._entries: ConcurrentStableDict[int, _SampleEntry] = ConcurrentStableDict()

    def sample(self, tenant_id: int) -> int | None:
        """取某租户当前存储字节数（TTL 内复用上次结果）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int | None: 字节数；目录不存在为 0，扫描失败为 None。
        """
        key = int(tenant_id)
        entry = self._entries.get(key)
        now = self._clock()
        if entry is not None and (now - entry.sampled_at) * 1000 < self._ttl_ms:
            return entry.value
        value = self._scan(key)
        self._entries[key] = _SampleEntry(sampled_at=now, value=value)
        return value

    def invalidate(self, tenant_id: int) -> None:
        """失效某租户缓存（租户清理后）。

        Args:
            tenant_id: 租户主键。
        """
        self._entries.pop(int(tenant_id), None)

    def _scan(self, tenant_id: int) -> int | None:
        """扫描租户目录下全部文件大小。

        Args:
            tenant_id: 租户主键。

        Returns:
            int | None: 字节数；目录不存在为 0，扫描失败为 None。
        """
        directory = self._root / DATA_ROOT_SUBDIR / str(int(tenant_id))
        if not directory.is_dir():
            return 0
        total = 0
        try:
            for path in directory.rglob("*"):
                if path.is_file():
                    total += path.stat().st_size
        except OSError as exc:
            _LOGGER.warning("msg_quota_storage_sample_failed", tenant_id=int(tenant_id), error=repr(exc))
            return None
        return total


class UsageSnapshotService:
    """用量快照服务（请求级会话 + 进程级运行态）。"""

    def __init__(
        self,
        *,
        session: DbSession,
        groups: GroupStore,
        delivery_state: DeliveryState,
        connections: ConnectionRegistry,
        counter: DeliveryCounter,
        sampler: StorageUsageSampler,
    ) -> None:
        """初始化。

        Args:
            session: 平台库会话（元数据计数）。
            groups: 组存储（组数 / 未确认）。
            delivery_state: 投递状态（未确认）。
            connections: 连接登记表（按租户连接数）。
            counter: 今日投递计数。
            sampler: 存储用量采样器。
        """
        self._session = session
        self._groups = groups
        self._delivery_state = delivery_state
        self._connections = connections
        self._counter = counter
        self._sampler = sampler

    async def snapshot(self, tenant_id: int) -> UsageSnapshot:
        """取完整用量快照。

        Args:
            tenant_id: 租户主键。

        Returns:
            UsageSnapshot: 七项用量。
        """
        topic_count, partition_count = await self._topic_counts(tenant_id)
        return UsageSnapshot(
            topic_count=topic_count,
            partition_count=partition_count,
            storage_bytes=self._sampler.sample(tenant_id),
            delivered_today=self._counter.value(),
            group_count=len(await self._groups.list_for_tenant(tenant_id=int(tenant_id))),
            unacked_count=await self.unacked_count(tenant_id),
            connection_count=self._connections.count_for_tenant(tenant_id=int(tenant_id)),
        )

    async def storage_bytes(self, tenant_id: int) -> int | None:
        """取存储字节数（只走采样器，不查库）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int | None: 字节数；扫描失败为 None。
        """
        return self._sampler.sample(tenant_id)

    async def unacked_count(self, tenant_id: int) -> int:
        """取租户未确认条数（按组记录 + 分区投递状态汇总）。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 未确认条数。
        """
        counts = await self._partition_counts(tenant_id)
        records = await self._groups.list_for_tenant(tenant_id=int(tenant_id))
        total = 0
        for record in records:
            partitions = counts.get(int(record.topic_id), 0)
            for partition_no in range(partitions):
                total += self._delivery_state.usage((record.group, int(record.topic_id), partition_no)).messages
        return total

    async def _topic_counts(self, tenant_id: int) -> tuple[int, int]:
        """取主题数与分区数。

        Args:
            tenant_id: 租户主键。

        Returns:
            tuple[int, int]: 主题数 + 分区数。
        """
        topics = await self._session.execute(
            select(func.count()).select_from(MsgTopic).where(MsgTopic.tenant_id == int(tenant_id))
        )
        partitions = await self._session.execute(
            select(func.count())
            .select_from(MsgPartition)
            .join(MsgTopic, MsgPartition.topic_id == MsgTopic.id)
            .where(MsgTopic.tenant_id == int(tenant_id))
        )
        return int(topics.scalar_one()), int(partitions.scalar_one())

    async def _partition_counts(self, tenant_id: int) -> ConcurrentStableDict[int, int]:
        """取主题主键 → 分区数映射。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableDict[int, int]: 映射。
        """
        statement = select(MsgTopic.id, MsgTopic.partition_count).where(MsgTopic.tenant_id == int(tenant_id))
        rows = (await self._session.execute(statement)).all()
        return ConcurrentStableDict({int(row[0]): int(row[1]) for row in rows})
