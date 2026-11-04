"""主题路由：主题解析（存在 / 归属 / 可用）与分区选择（详细设计 §6.5、概要设计 §2 `broker/router.py`）。

- **主题解析**：按 `(当前租户, namespace, name)` 取主题（仓储 `tenant_scoped` 保证归属），
  不存在、不属于当前租户或 `status != active` 一律 `350001`（不区分「不存在」与「停用」，
  以免向调用方泄露主题是否存在；细分码登记为遗留，归 06 域管理操作）。
- **分区选择**：物理分区由调用方指定（本任务单分区投递，组内分摊归 01-4）；**发布**走
  `core/routing.py::select_partition`（同 key 恒同分区，**不另写哈希**）。
"""

from bms_msg.core.routing import select_partition
from bms_msg.errors import MsgPartitionCountInvalidError, MsgTopicNotFoundError
from bms_msg.models.topic import STATUS_ACTIVE, MsgTopic
from bms_msg.repositories.topic import TopicRepository

__all__ = ["TopicRouter"]


class TopicRouter:
    """主题解析与分区选择（请求级；绑定平台库主题仓储）。"""

    def __init__(self, topics: TopicRepository) -> None:
        """初始化。

        Args:
            topics: 主题仓储（请求级会话，租户由上下文注入）。
        """
        self._topics = topics

    async def resolve(self, *, namespace: str, name: str) -> MsgTopic:
        """取当前租户下的可用主题。

        Args:
            namespace: 命名空间（合法性由服务层先行校验）。
            name: 主题名。

        Returns:
            MsgTopic: 主题记录（`status == active`）。

        Raises:
            MsgTopicNotFoundError: 不存在、不属于当前租户，或未启用（350001）。
        """
        topic = await self._topics.get_by_identity(namespace=namespace, name=name)
        if topic is None or topic.status != STATUS_ACTIVE:
            raise MsgTopicNotFoundError(f"主题不存在或不可用：{namespace}/{name}")
        return topic

    def partition(self, topic: MsgTopic, partition_no: int) -> int:
        """校验并返回指定分区号。

        Args:
            topic: 主题记录。
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

    def route(self, topic: MsgTopic, key: str | None) -> int:
        """按分区键选分区（同 key 恒同分区；无 key 进程内轮询）。

        Args:
            topic: 主题记录。
            key: 分区键（可空）。

        Returns:
            int: 分区号。

        Raises:
            MsgPartitionCountInvalidError: 主题分区数非正（350009）。
        """
        return select_partition(key, int(topic.partition_count))
