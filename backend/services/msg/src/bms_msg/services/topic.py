"""主题领域服务（详细设计 §5、§8）：判定顺序「租户 → 命名空间 → 主题」与事务边界。

- **租户**：由统一依赖 `get_current_tenant_context()` 注入（`350012` / `350011` 在中间件与依赖层已判），
  本层只取主键，不再解析、不接受请求体自报；
- **命名空间**：形态 + 服务目录已启用 `module_key`（`350010`，单点复用 `CurrentTenant.ensure_namespace`）；
- **主题**：不存在 `350001`、重名 `350008`、分区数 / 保留期非法 `350009`；
- **事务**：主题行与 N 条分区行**同一事务**提交，避免「有主题无分区」的半成品（校验读也在事务内，
  SQLAlchemy 2.0 先读后 `begin` 会因自动开启事务失败）。
"""

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.config import get_settings
from bms_core.core.exceptions import ParamError
from bms_core.db.unit_of_work import UnitOfWork

from bms_msg.core.config import (
    MAX_PARTITION_COUNT,
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
)
from bms_msg.models.partition import MsgPartition
from bms_msg.models.topic import CLEANUP_DELETE, MsgTopic
from bms_msg.repositories.topic import PartitionRepository, TopicRepository
from bms_msg.schemas.topic import TopicCreateRequest
from bms_msg.tenant.context import CurrentTenant

__all__ = ["MAX_LIST_SIZE", "TopicService"]

MAX_LIST_SIZE = 100
"""列表每页上限（与接口 `size` 上界一致）。"""


class TopicService:
    """主题服务：创建 / 查询 / 列表 / 增分区（只增不减）。"""

    def __init__(self, topics: TopicRepository, partitions: PartitionRepository, uow: UnitOfWork) -> None:
        """初始化。

        Args:
            topics: 主题仓储（请求级会话）。
            partitions: 分区仓储（同会话）。
            uow: 工作单元（事务边界）。
        """
        self._topics = topics
        self._partitions = partitions
        self._uow = uow

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
        async with self._uow.begin():
            if await self._topics.get_by_identity(namespace=request.namespace, name=request.name) is not None:
                raise MsgTopicExistsError(f"主题已存在：{request.namespace}/{request.name}")
            topic = await self._topics.create(
                tenant_id=int(tenant.tenant_id),
                namespace=request.namespace,
                name=request.name,
                partition_count=request.partition_count,
                retention_ms=request.retention_ms,
                cleanup_policy=request.cleanup_policy or CLEANUP_DELETE,
            )
            await self._partitions.add_partitions(
                topic_id=int(topic.id),
                start_no=0,
                count=request.partition_count,
                leader=_leader_id(),
            )
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
        """
        del tenant
        self._ensure_partition_count(partition_count)
        async with self._uow.begin():
            topic = await self._topics.get(topic_id)
            if topic is None:
                raise MsgTopicNotFoundError(f"主题不存在：{topic_id}")
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
        return topic, partitions

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


def _leader_id() -> str:
    """当前实例的 leader 标识（首期单机：服务标识 + worker_id）。

    Returns:
        str: leader 标识（如 `msg-0`）。
    """
    settings = get_settings()
    return f"{settings.app.service or 'msg'}-{settings.app.worker_id}"
