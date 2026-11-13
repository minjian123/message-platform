"""主题元数据缓存（01-7 详细设计 §3；02-8-4 增 TTL）：不可变快照 + 进程内键值表 + 写时精确失效。

三条结构性口径：

1. **存值快照，不存 ORM 实例**：请求结束、会话关闭后读 ORM 属性会触发懒加载（`MissingGreenlet`），
   同一实例被并发请求的多个会话共享也会打架；快照是判定的唯一输入（详细设计 §3.1）。
2. **键带租户**：平台库是「同库多租户」，键不含 `tenant_id` 就会跨租户取到别人的主题（越权）。
3. **TTL 5 秒 + 写时精确失效**（02-8-4）：本实例写操作仍立即失效（下一次请求即生效）；其他实例没有
   失效广播，靠 TTL 在最长 5 秒内收敛——避免「已停用 / 已删除的主题在别的实例永久可写」。
   负结果仍不缓存（刚建的主题不能有 5 秒空窗）。

缓存是**进程级**的（`app.state.topic_cache`）：多 worker 各持一份，跨实例收敛靠 TTL。
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

from bms_core.core.objects import BaseValueObject

from bms_msg.core.config import TOPIC_CACHE_TTL_MS
from bms_msg.models.topic import MsgTopic

__all__ = ["TopicCache", "TopicKey", "TopicSnapshot"]

TopicKey = tuple[int, str, str]
"""缓存键：`(tenant_id, namespace, name)`（与主题唯一键同构）。"""


def _epoch_ms() -> int:
    """当前 epoch 毫秒（快照载入时刻；日志与 8B_05 的失效协议用）。

    Returns:
        int: 当前时间（毫秒）。
    """
    return int(time.time() * 1_000)


@dataclass(frozen=True)
class TopicSnapshot(BaseValueObject):
    """主题元数据的不可变快照（只含判定需要的列，不留关系字段）。

    字段名与 `MsgTopic` 对齐：`TopicRouter` 由「返回 ORM 实例」改为「返回快照」时，
    调用点的属性读取（`id` / `namespace` / `name` / `partition_count` / `retention_ms` /
    `retention_bytes` / `status`）无需改名，改动是机械的。
    """

    id: int
    """主题主键（雪花）。"""

    tenant_id: int
    """租户主键（缓存键的一部分）。"""

    namespace: str
    """命名空间（对齐 bms `module_key`）。"""

    name: str
    """主题名。"""

    partition_count: int
    """分区数（只增不减）。"""

    retention_ms: int
    """保留时长（毫秒）。"""

    retention_bytes: int
    """容量上限（字节；`0` = 不限）。"""

    cleanup_policy: str
    """清理策略（首期唯一支持 `delete`）。"""

    status: str
    """状态（`active` / `disabled`）。"""

    loaded_at_ms: int
    """载入时刻（epoch 毫秒）：排障用，也是 8B_05 跨实例失效协议的预留位。"""

    @classmethod
    def from_topic(cls, topic: MsgTopic) -> TopicSnapshot:
        """ORM 主题记录 → 快照（唯一构造入口，避免两处字段映射漂移）。

        Args:
            topic: 主题记录（须已从库中读出，属性均可直接访问）。

        Returns:
            TopicSnapshot: 不可变快照。
        """
        return cls(
            id=int(topic.id),
            tenant_id=int(topic.tenant_id),
            namespace=topic.namespace,
            name=topic.name,
            partition_count=int(topic.partition_count),
            retention_ms=int(topic.retention_ms),
            retention_bytes=int(topic.retention_bytes),
            cleanup_policy=topic.cleanup_policy,
            status=topic.status,
            loaded_at_ms=_epoch_ms(),
        )


class TopicCache:
    """进程内主题快照缓存（无锁：`get` / `put` / `invalidate` 全程无 `await`，单事件循环内原子）。"""

    def __init__(self, *, ttl_ms: int = TOPIC_CACHE_TTL_MS, clock: Callable[[], int] = _epoch_ms) -> None:
        """初始化（空表；条目随首次解析回填）。

        Args:
            ttl_ms: 条目存活时长（毫秒）；到期视为未命中并删除。
            clock: epoch 毫秒时钟（用例注入以确定性驱动过期）。
        """
        self._entries: dict[TopicKey, TopicSnapshot] = {}
        self._ttl_ms = ttl_ms
        self._clock = clock

    def get(self, *, tenant_id: int, namespace: str, name: str) -> TopicSnapshot | None:
        """按键取快照（过期条目视为未命中）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。

        Returns:
            TopicSnapshot | None: 命中返回快照；未命中 / 已过期返回 None（调用方负责查库并回填）。
        """
        key: TopicKey = (tenant_id, namespace, name)
        snapshot = self._entries.get(key)
        if snapshot is None:
            return None
        if self._clock() - snapshot.loaded_at_ms >= self._ttl_ms:
            self._entries.pop(key, None)
            return None
        return snapshot

    def put(self, snapshot: TopicSnapshot) -> None:
        """回填快照（键取自快照自身：租户 / 命名空间 / 名称三元组唯一确定主题）。

        Args:
            snapshot: 主题快照。
        """
        self._entries[(snapshot.tenant_id, snapshot.namespace, snapshot.name)] = snapshot

    def invalidate(self, *, tenant_id: int, namespace: str, name: str) -> None:
        """按精确键失效（不做全清：全清会让失效后的首批请求同时打库）。

        Args:
            tenant_id: 租户主键。
            namespace: 命名空间。
            name: 主题名。
        """
        self._entries.pop((tenant_id, namespace, name), None)

    def __len__(self) -> int:
        """当前条目数（排障与用例断言用）。

        Returns:
            int: 条目数。
        """
        return len(self._entries)
