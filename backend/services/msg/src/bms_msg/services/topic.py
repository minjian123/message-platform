"""主题领域服务（详细设计 §5、§8）：判定顺序「租户 → 命名空间 → 主题」与事务边界。

- **租户**：由统一依赖 `get_current_tenant_context()` 注入（`350012` / `350011` 在中间件与依赖层已判），
  本层只取主键，不再解析、不接受请求体自报；
- **命名空间**：形态 + 服务目录已启用 `module_key`（`350010`，单点复用 `CurrentTenant.ensure_namespace`）；
- **主题**：不存在 `350001`、重名 `350008`、分区数 / 保留期非法 `350009`；
- **事务**：主题行与 N 条分区行**同一事务**提交，避免「有主题无分区」的半成品（校验读也在事务内，
  SQLAlchemy 2.0 先读后 `begin` 会因自动开启事务失败）。
- **失效收口（01-7）**：所有写方法在**提交成功之后**按精确键失效进程内主题快照缓存。
  06 域将来的更新 / 停用 / 删除**必须**经本类写方法，禁止端点层直接改库——那是「改了配置仍按旧值投递」
  的唯一来源（详细设计 §3.3）。
"""

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.config import get_settings
from bms_core.core.exceptions import ConfigError, ParamError
from bms_core.core.logging import get_logger
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.broker.topic_cache import TopicCache
from bms_msg.core.config import (
    MAX_PARTITION_COUNT,
    MAX_RETENTION_BYTES,
    MAX_RETENTION_MS,
    MIN_PARTITION_COUNT,
    NAMESPACE_PATTERN,
    TOPIC_NAME_PATTERN,
)
from bms_msg.errors import (
    MsgNamespaceInvalidError,
    MsgPartitionCountInvalidError,
    MsgTopicExistsError,
    MsgTopicNotFoundError,
    MsgTopicQuotaExceededError,
    MsgTopicStateInvalidError,
)
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import CLEANUP_DELETE, STATUS_ACTIVE, STATUS_DELETING, MsgTopic
from bms_msg.models.topic_cleanup import MsgTopicCleanup
from bms_msg.quota.service import QuotaService
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.repositories.topic_cleanup import TopicCleanupRepository
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = ["MAX_LIST_SIZE", "TopicService"]

MAX_LIST_SIZE = 100
"""列表每页上限（与接口 `size` 上界一致）。"""

_LOGGER = get_logger("bms_msg.topic")
"""主题服务日志（只出结构化事件：`msg_topic_cache` 的失效记录）。"""


class TopicService:
    """主题服务：创建 / 查询 / 列表 / 增分区（只增不减）。"""

    def __init__(
        self,
        topics: TopicRepository,
        partitions: PartitionRepository,
        uow: UnitOfWork,
        *,
        cache: TopicCache | None = None,
        quota: QuotaService | None = None,
        cleanups: TopicCleanupRepository | None = None,
    ) -> None:
        """初始化。

        Args:
            topics: 主题仓储（请求级会话）。
            partitions: 分区仓储（同会话）。
            uow: 工作单元（事务边界）。
            cache: 进程内主题快照缓存（None = 不失效；数据面读最新值靠它）。
            quota: 配额服务（None = 不判主题数配额，兼容既有单测）。
            cleanups: 清理台账仓储（None = 不具备删除受理能力，兼容既有单测）。
        """
        self._topics = topics
        self._partitions = partitions
        self._uow = uow
        self._cache = cache
        self._quota = quota
        self._cleanups = cleanups

    async def create_topic(self, *, tenant: CurrentTenant, request: TopicCreateRequest) -> MsgTopic:
        """创建主题（同事务写主题行 + N 条分区行）。

        Args:
            tenant: 当前租户（统一依赖注入）。
            request: 创建请求（`tenant_id` 不在请求体）。

        Returns:
            MsgTopic: 新建主题。

        Raises:
            MsgNamespaceInvalidError: 命名空间未登记 / 未启用（350010）。
            MsgPartitionCountInvalidError: 分区数越界（350009）。
            MsgTopicExistsError: 同租户同名主题已存在（350008）。
        """
        self._ensure_namespace(tenant, request.namespace)
        self._ensure_topic_name(request.name)
        self._ensure_partition_count(request.partition_count)
        self._ensure_retention(request.retention_ms)
        self._ensure_retention_bytes(request.retention_bytes)
        async with self._uow.begin():
            if await self._topics.get_by_identity(namespace=request.namespace, name=request.name) is not None:
                raise MsgTopicExistsError(f"主题已存在：{request.namespace}/{request.name}")
            await self._ensure_quota(tenant=tenant, namespace=request.namespace)
            topic = await self._topics.create(
                tenant_id=int(tenant.tenant_id),
                namespace=request.namespace,
                name=request.name,
                partition_count=request.partition_count,
                retention_ms=request.retention_ms,
                retention_bytes=request.retention_bytes,
                cleanup_policy=request.cleanup_policy or CLEANUP_DELETE,
            )
            await self._partitions.add_partitions(
                topic_id=int(topic.id),
                start_no=0,
                count=request.partition_count,
                leader=_leader_id(),
            )
        self._invalidate(tenant_id=int(tenant.tenant_id), namespace=request.namespace, name=request.name)
        return topic

    async def get_topic(
        self, *, tenant: CurrentTenant, topic_id: int
    ) -> tuple[MsgTopic, ConcurrentStableList[MsgPartition]]:
        """取主题详情（含分区明细，按分区号升序）。

        Args:
            tenant: 当前租户（查询按租户隔离）。
            topic_id: 主题 ID。

        Returns:
            tuple[MsgTopic, ConcurrentStableList[MsgPartition]]: 主题 + 分区列表。

        Raises:
            MsgTopicNotFoundError: 主题不存在或不属于当前租户（350001）。
        """
        del tenant
        topic = await self._topics.get(topic_id)
        if topic is None:
            raise MsgTopicNotFoundError(f"主题不存在：{topic_id}")
        partitions = await self._partitions.list_by_topic(topic_id)
        return topic, partitions

    async def list_topics(
        self,
        *,
        tenant: CurrentTenant,
        namespace: str | None,
        keyword: str | None,
        page: int,
        size: int,
    ) -> tuple[ConcurrentStableList[MsgTopic], int]:
        """分页列出主题（不含分区明细）。

        Args:
            tenant: 当前租户（列表按租户隔离）。
            namespace: 命名空间过滤；None 不限制（给定时同样校验合法性，非法即 350010）。
            keyword: 名称模糊匹配；None / 空串不限制。
            page: 页码（从 1 起）。
            size: 每页条数（1~100）。

        Returns:
            tuple[ConcurrentStableList[MsgTopic], int]: 当页主题 + 总数。

        Raises:
            MsgNamespaceInvalidError: 命名空间过滤值非法（350010）。
            ParamError: 分页参数越界（10001）。
        """
        if namespace is not None:
            self._ensure_namespace(tenant, namespace)
        if page < 1:
            raise ParamError(f"页码须从 1 起：{page}")
        if not 1 <= size <= MAX_LIST_SIZE:
            raise ParamError(f"每页条数须在 1~{MAX_LIST_SIZE}：{size}")
        items = await self._topics.list_topics(
            namespace=namespace, keyword=keyword, offset=(page - 1) * size, limit=size
        )
        total = await self._topics.count_topics(namespace=namespace, keyword=keyword)
        return items, total

    async def expand_partitions(
        self, *, tenant: CurrentTenant, topic_id: int, partition_count: int
    ) -> tuple[MsgTopic, ConcurrentStableList[MsgPartition]]:
        """增分区（**只增不减**；等于现状即幂等返回）。

        Args:
            tenant: 当前租户。
            topic_id: 主题 ID。
            partition_count: 目标分区数（须 ≥ 现分区数且在上界内）。

        Returns:
            tuple[MsgTopic, ConcurrentStableList[MsgPartition]]: 更新后的主题 + 全部分区。

        Raises:
            MsgTopicNotFoundError: 主题不存在（350001）。
            MsgPartitionCountInvalidError: 目标分区数越界或小于现状（350009）。
            MsgTopicStateInvalidError: 主题处于 `disabled` / `deleting`（350028）。
        """
        self._ensure_partition_count(partition_count)
        # 端点可能已为鉴权读过主题（会话由此自动开启事务），先回滚再显式 begin（同 05-3 ACL 服务口径）。
        await self._uow.rollback()
        async with self._uow.begin():
            topic = await self._topics.get(topic_id)
            if topic is None:
                raise MsgTopicNotFoundError(f"主题不存在：{topic_id}")
            # 事务内先取标识：提交后再读 ORM 属性会触发刷新（回滚分支更会直接失效）。
            identity = (topic.namespace, topic.name)
            self._ensure_writable_state(topic)
            current = int(topic.partition_count)
            if partition_count < current:
                raise MsgPartitionCountInvalidError(f"分区数只能增不能减：现 {current}，目标 {partition_count}")
            if partition_count > current:
                await self._partitions.add_partitions(
                    topic_id=topic_id,
                    start_no=current,
                    count=partition_count - current,
                    leader=_leader_id(),
                )
                topic = await self._topics.update(topic_id, partition_count=partition_count) or topic
            partitions = await self._partitions.list_by_topic(topic_id)
        self._invalidate(tenant_id=int(tenant.tenant_id), namespace=identity[0], name=identity[1])
        return topic, partitions

    async def update_retention(
        self,
        *,
        tenant: CurrentTenant,
        topic_id: int,
        retention_ms: int | None,
        retention_bytes: int | None,
    ) -> tuple[MsgTopic, ConcurrentStableDict[str, int]]:
        """改保留期 / 容量上限（**部分更新**：只为给出的项写库）。

        **不回溯删除已保留记录**：本次只改元数据，后续清理按新值执行（需求 06-2 第 4 条）。

        Args:
            tenant: 当前租户。
            topic_id: 主题 ID。
            retention_ms: 新保留时长（None = 不改）。
            retention_bytes: 新容量上限（None = 不改）。

        Returns:
            tuple[MsgTopic, ConcurrentStableDict[str, int]]: 更新后的主题 + 变更前值（审计对账用）。

        Raises:
            MsgTopicNotFoundError: 主题不存在（350001）。
            MsgTopicStateInvalidError: 主题处于 `deleting`（350028）。
            MsgPartitionCountInvalidError: 取值越界（350009，与建主题同码）。
        """
        if retention_ms is not None:
            self._ensure_retention(retention_ms)
        if retention_bytes is not None:
            self._ensure_retention_bytes(retention_bytes)
        await self._uow.rollback()
        async with self._uow.begin():
            topic = await self._topics.get(topic_id)
            if topic is None:
                raise MsgTopicNotFoundError(f"主题不存在：{topic_id}")
            identity = (topic.namespace, topic.name)
            self._ensure_writable_state(topic)
            before = ConcurrentStableDict(
                {"retention_ms": int(topic.retention_ms), "retention_bytes": int(topic.retention_bytes)}
            )
            values: ConcurrentStableDict[str, int] = ConcurrentStableDict()
            if retention_ms is not None:
                values.set("retention_ms", int(retention_ms))
            if retention_bytes is not None:
                values.set("retention_bytes", int(retention_bytes))
            updated = await self._topics.update(topic_id, **dict(values))
        self._invalidate(tenant_id=int(tenant.tenant_id), namespace=identity[0], name=identity[1])
        return updated or topic, before

    async def begin_delete(self, *, tenant: CurrentTenant, topic_id: int) -> tuple[MsgTopic, MsgTopicCleanup]:
        """受理删主题（**幂等**：重复受理复用台账，不重复建行、不重复触发首轮）。

        只做三件事：置 `status=deleting`（数据面据此停读写）、建 / 取台账行、提交后失效主题缓存。
        真正的资源清理在 `TopicCleanupService`，请求不阻塞等待。

        Args:
            tenant: 当前租户。
            topic_id: 主题 ID。

        Returns:
            tuple[MsgTopic, MsgTopicCleanup]: 更新后的主题 + 清理台账。

        Raises:
            MsgTopicNotFoundError: 主题不存在（350001）。
            ConfigError: 服务未装配台账仓储（装配缺陷，不静默跳过）。
        """
        if self._cleanups is None:
            raise ConfigError("删主题需要清理台账仓储（TopicCleanupRepository）")
        await self._uow.rollback()
        async with self._uow.begin():
            topic = await self._topics.get(topic_id)
            if topic is None:
                raise MsgTopicNotFoundError(f"主题不存在：{topic_id}")
            identity = (topic.namespace, topic.name)
            if topic.status != STATUS_DELETING:
                topic = await self._topics.update(topic_id, status=STATUS_DELETING) or topic
            cleanup = await self._cleanups.get_by_topic(topic_id)
            if cleanup is None:
                cleanup = await self._cleanups.create(
                    tenant_id=int(tenant.tenant_id),
                    topic_id=int(topic_id),
                    namespace=identity[0],
                    topic=identity[1],
                )
        self._invalidate(tenant_id=int(tenant.tenant_id), namespace=identity[0], name=identity[1])
        return topic, cleanup

    async def get_cleanup(self, *, topic_id: int) -> MsgTopicCleanup | None:
        """取某主题的清理台账（详情端点展示进度用；无台账返回 None）。

        Args:
            topic_id: 主题 ID。

        Returns:
            MsgTopicCleanup | None: 台账。

        Raises:
            ConfigError: 服务未装配台账仓储（装配缺陷）。
        """
        if self._cleanups is None:
            raise ConfigError("查清理进度需要清理台账仓储（TopicCleanupRepository）")
        return await self._cleanups.get_by_topic(topic_id)

    def _ensure_writable_state(self, topic: MsgTopic) -> None:
        """校验主题处于可写状态（`active` 才允许写；`disabled` / `deleting` 一律拒绝）。

        Args:
            topic: 主题记录。

        Raises:
            MsgTopicStateInvalidError: 状态为 `disabled` / `deleting`（350028）。
        """
        if topic.status == STATUS_ACTIVE:
            return
        raise MsgTopicStateInvalidError(f"主题状态不允许该操作：{topic.namespace}/{topic.name} 当前为 {topic.status}")

    def _invalidate(self, *, tenant_id: int, namespace: str, name: str) -> None:
        """失效一条主题快照（**提交成功之后**调用；未装配缓存则空转）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。
        """
        if self._cache is None:
            return
        self._cache.invalidate(tenant_id=tenant_id, namespace=namespace, name=name)
        _LOGGER.info("msg_topic_cache", outcome="invalidate", tenant_id=tenant_id, namespace=namespace, topic=name)

    async def _ensure_quota(self, *, tenant: CurrentTenant, namespace: str) -> None:
        """校验主题数配额（事务内；硬限已到即 `350025`）。

        Args:
            tenant: 当前租户。
            namespace: 命名空间（主题数按租户 / 命名空间维度计）。

        Raises:
            MsgTopicQuotaExceededError: 主题数已达硬限（350025 / 429）。
        """
        if self._quota is None:
            return
        limit = await self._quota.effective(int(tenant.tenant_id), namespace, "topic_count")
        if limit.limit <= 0:
            return
        used = await self._topics.count_topics(namespace=namespace, keyword=None)
        if used >= limit.limit:
            raise MsgTopicQuotaExceededError(
                f"主题数已达配额上限（{namespace or '租户级'}）：{used}/{limit.limit}",
                limit=limit.limit,
                usage=used,
            )

    def _ensure_namespace(self, tenant: CurrentTenant, namespace: str) -> None:
        """校验命名空间形态与可用性（`350010`；单点复用 `CurrentTenant.ensure_namespace`）。

        Args:
            tenant: 当前租户（含已启用命名空间快照）。
            namespace: 命名空间。

        Raises:
            MsgNamespaceInvalidError: 形态不符或未登记 / 未启用（350010）。
        """
        if not NAMESPACE_PATTERN.match(namespace):
            raise MsgNamespaceInvalidError(f"命名空间形态非法：{namespace}")
        tenant.ensure_namespace(namespace)

    def _ensure_topic_name(self, name: str) -> None:
        """校验主题名形态（不合法按参数错误 10001：属请求契约而非业务口径）。

        Args:
            name: 主题名。

        Raises:
            ParamError: 形态不符（10001）。
        """
        if not TOPIC_NAME_PATTERN.match(name):
            raise ParamError(f"主题名形态非法：{name}")

    def _ensure_partition_count(self, partition_count: int) -> None:
        """校验分区数上下界（`350009`）。

        Args:
            partition_count: 分区数。

        Raises:
            MsgPartitionCountInvalidError: 越界（350009）。
        """
        if not MIN_PARTITION_COUNT <= partition_count <= MAX_PARTITION_COUNT:
            raise MsgPartitionCountInvalidError(
                f"分区数须在 {MIN_PARTITION_COUNT}~{MAX_PARTITION_COUNT}：{partition_count}"
            )

    def _ensure_retention(self, retention_ms: int) -> None:
        """校验保留期（正数且不超上限；越界同 `350009`）。

        Args:
            retention_ms: 保留时长（毫秒）。

        Raises:
            MsgPartitionCountInvalidError: 越界（350009）。
        """
        if not 0 < retention_ms <= MAX_RETENTION_MS:
            raise MsgPartitionCountInvalidError(f"保留期须在 1~{MAX_RETENTION_MS} 毫秒：{retention_ms}")

    def _ensure_retention_bytes(self, retention_bytes: int) -> None:
        """校验容量上限（`0` = 不限；上限内；越界同 `350009`，与保留期同码）。

        Args:
            retention_bytes: 容量上限（字节）。

        Raises:
            MsgPartitionCountInvalidError: 越界（350009）。
        """
        if not 0 <= retention_bytes <= MAX_RETENTION_BYTES:
            raise MsgPartitionCountInvalidError(
                f"容量上限须在 0~{MAX_RETENTION_BYTES} 字节（0 = 不限）：{retention_bytes}"
            )


def _leader_id() -> str:
    """当前实例的 leader 标识（首期单机：服务标识 + worker_id）。

    Returns:
        str: leader 标识（如 `msg-0`）。
    """
    settings = get_settings()
    return f"{settings.app.service or 'msg'}-{settings.app.worker_id}"
