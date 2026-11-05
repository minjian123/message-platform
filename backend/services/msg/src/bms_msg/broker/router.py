"""主题路由：主题解析（存在 / 归属 / 可用）与分区选择（详细设计 §6.5、概要设计 §2 `broker/router.py`）。

- **主题解析**：按 `(当前租户, namespace, name)` 取主题（仓储 `tenant_scoped` 保证归属），
  不存在、不属于当前租户或 `status != active` 一律 `350001`（不区分「不存在」与「停用」，
  以免向调用方泄露主题是否存在；细分码登记为遗留，归 06 域管理操作）。
- **缓存（01-7）**：`cache` 与 `tenant_id` 同时给出时，解析先查进程内快照缓存，未命中查库并回填；
  两者都不给即「不缓存、直接查库」（与 01-6 及之前完全一致）。**返回类型恒为 `TopicSnapshot`**，
  命中与未命中共用同一条产出路径（详细设计 §3.1、§6）。
- **分区选择**：物理分区由调用方指定（本任务单分区投递，组内分摊归 01-4）；**发布**走
  `core/routing.py::select_partition`（同 key 恒同分区，**不另写哈希**）。
- **目录登记（02-1）**：给出 `directory` 时，解析成功（命中缓存或查库）后把 `topic_id → 三段名字` 登记进
  进程内登记表——文件存储只收 `topic_id`，落盘目录要靠这三段。命中缓存也要登记：重启后首个请求可能直接
  命中缓存（同一进程内缓存与登记表同生命周期，但重放 / 长驻流等内部路径依赖这份登记）。
"""

import time

from bms_core.core.logging import get_logger

from bms_msg.broker.file_store import PartitionDirectory
from bms_msg.broker.topic_cache import TopicCache, TopicSnapshot
from bms_msg.core.routing import select_partition
from bms_msg.errors import MsgPartitionCountInvalidError, MsgTopicNotFoundError
from bms_msg.models.topic import STATUS_ACTIVE
from bms_msg.repositories.topic import TopicRepository

__all__ = ["TopicRouter"]

_LOGGER = get_logger("bms_msg.topic")


class TopicRouter:
    """主题解析与分区选择（请求级；绑定平台库主题仓储，可选挂进程内快照缓存）。"""

    def __init__(
        self,
        topics: TopicRepository,
        *,
        cache: TopicCache | None = None,
        tenant_id: int | None = None,
        directory: PartitionDirectory | None = None,
    ) -> None:
        """初始化。

        Args:
            topics: 主题仓储（请求级会话，租户由上下文注入）。
            cache: 进程内快照缓存（None = 不缓存）；给出时必须同时给出 `tenant_id`。
            tenant_id: 当前租户主键（缓存键的一部分）。
            directory: 分区目录登记表（02-1；None = 不登记，供管理面或不落盘的直调方使用）。

        Raises:
            ValueError: 只给 `cache` 不给 `tenant_id`（键里没有租户即跨租户取主题，属越权）。
        """
        if cache is not None and tenant_id is None:
            raise ValueError("启用主题缓存时必须给出 tenant_id（缓存键含租户）")
        self._topics = topics
        self._cache = cache
        self._tenant_id = 0 if tenant_id is None else int(tenant_id)
        self._directory = directory

    async def resolve(self, *, namespace: str, name: str) -> TopicSnapshot:
        """取当前租户下的可用主题（命中缓存则不查库），并登记分区目录身份。

        Args:
            namespace: 命名空间（合法性由服务层先行校验）。
            name: 主题名。

        Returns:
            TopicSnapshot: 主题快照（`status == active`）。

        Raises:
            MsgTopicNotFoundError: 不存在、不属于当前租户，或未启用（350001）。
        """
        snapshot = await self._resolve_snapshot(namespace=namespace, name=name)
        self._bind_directory(snapshot)
        return snapshot

    async def _resolve_snapshot(self, *, namespace: str, name: str) -> TopicSnapshot:
        """解析主题快照（缓存优先；不含目录登记）。

        Args:
            namespace: 命名空间。
            name: 主题名。

        Returns:
            TopicSnapshot: 主题快照。

        Raises:
            MsgTopicNotFoundError: 不存在、不属于当前租户，或未启用（350001）。
        """
        cache = self._cache
        if cache is not None:
            cached = cache.get(tenant_id=self._tenant_id, namespace=namespace, name=name)
            if cached is not None:
                _LOGGER.info(
                    "msg_topic_cache",
                    outcome="hit",
                    tenant_id=self._tenant_id,
                    namespace=namespace,
                    topic=name,
                )
                return cached
        started = time.perf_counter()
        topic = await self._topics.get_by_identity(namespace=namespace, name=name)
        if topic is None or topic.status != STATUS_ACTIVE:
            raise MsgTopicNotFoundError(f"主题不存在或不可用：{namespace}/{name}")
        snapshot = TopicSnapshot.from_topic(topic)
        if cache is not None:
            cache.put(snapshot)
            _LOGGER.info(
                "msg_topic_cache",
                outcome="miss",
                tenant_id=self._tenant_id,
                namespace=namespace,
                topic=name,
                elapsed_ms=round((time.perf_counter() - started) * 1_000, 3),
            )
        return snapshot

    def _bind_directory(self, snapshot: TopicSnapshot) -> None:
        """把主题的三段名字登记进分区目录表（幂等；冲突由登记表记日志并保留首次值）。

        Args:
            snapshot: 主题快照。
        """
        if self._directory is None:
            return
        self._directory.bind(
            tenant_id=int(snapshot.tenant_id),
            namespace=snapshot.namespace,
            name=snapshot.name,
            topic_id=int(snapshot.id),
        )

    def partition(self, topic: TopicSnapshot, partition_no: int) -> int:
        """校验并返回指定分区号。

        Args:
            topic: 主题快照。
            partition_no: 请求的分区号。

        Returns:
            int: 分区号（原值）。

        Raises:
            MsgPartitionCountInvalidError: 分区号越界（350009）。
        """
        count = int(topic.partition_count)
        if not 0 <= partition_no < count:
            raise MsgPartitionCountInvalidError(f"分区号须在 0~{count - 1}：{partition_no}")
        return partition_no

    def route(self, topic: TopicSnapshot, key: str | None) -> int:
        """按分区键选分区（同 key 恒同分区；无 key 进程内轮询）。

        Args:
            topic: 主题快照。
            key: 分区键（可空）。

        Returns:
            int: 分区号。

        Raises:
            MsgPartitionCountInvalidError: 主题分区数非正（350009）。
        """
        return select_partition(key, int(topic.partition_count))
