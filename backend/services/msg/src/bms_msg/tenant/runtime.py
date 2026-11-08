"""租户生命周期运行时依赖：账本、审计出口与各进程内 / 文件态资源句柄。

**为什么需要运行时对象**：事件消费在应用启动期由基座消费组装载，处理器没有请求态；它需要的
连接表 / 流表 / 延迟区 / 消息存储等是 `app.state` 里的进程级单例。`configure_service()` 在消费组
`load()` 之前执行，因此把 `app.state` 的引用收进本对象并登记为进程级运行时，处理器即可在后台
安全取用。

**不建租户状态**：本对象只持有资源句柄，不保存「某租户是否停用」；状态判定一律走基座租户守卫 /
租户源。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from bms_core.audit.events import build_operation_event
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import TenantNotFoundError, TenantSuspendedError
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseValueObject
from bms_core.tenant.guard import BaseTenantGuard, TenantTarget
from bms_core.tenant.lifecycle import TenantLifecycleContext

from bms_msg.broker.connection import ConnectionRegistry
from bms_msg.broker.deadletter import DeadLetterQueue
from bms_msg.broker.delivery import DeliveryState
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.group import GroupStore
from bms_msg.broker.offset import FileOffsetStore
from bms_msg.broker.replay import AuditSink
from bms_msg.broker.retention import RetentionThrottle
from bms_msg.broker.scheduler import DelayWheel
from bms_msg.broker.topic_cache import TopicCache
from bms_msg.broker.transport import StreamRegistry
from bms_msg.broker.waiter import DeliveryWaiter
from bms_msg.tenant.ledger import TenantLifecycleLedger

__all__ = [
    "ActivateOutcome",
    "DeleteOutcome",
    "GuardTenantAvailability",
    "SuspendOutcome",
    "TenantLifecycleAuditError",
    "TenantLifecycleMetrics",
    "TenantLifecycleRuntime",
    "get_tenant_lifecycle_metrics",
    "get_tenant_lifecycle_runtime",
    "reset_tenant_lifecycle_metrics",
    "reset_tenant_lifecycle_runtime",
    "set_tenant_lifecycle_runtime",
]

_LOGGER = get_logger("bms_msg.tenant_runtime")


class TenantLifecycleAuditError(Exception):
    """生命周期强审计失败（删除 / 清理必须留痕，失败即不推进动作）。"""


class GuardTenantAvailability:
    """延迟转投的租户可用性适配器（复用基座 `BaseTenantGuard`，不建本地状态）。

    与数据面同源：`guard.require()` 走租户源与全局版本戳；停用 / 不存在返回 False，租户源不可达
    也按 False（fail-closed），由调用方保留记录、下一轮再判。
    """

    def __init__(self, *, guard: BaseTenantGuard) -> None:
        """初始化。

        Args:
            guard: 基座租户可用性守卫。
        """
        self._guard = guard

    async def is_active(self, tenant_id: int) -> bool:
        """租户当前是否可收发。

        Args:
            tenant_id: 租户主键。

        Returns:
            bool: 可用 True；停用 / 不存在 / 其他异常按 False（fail-closed）。
        """
        try:
            await self._guard.require(TenantTarget(tenant_id=str(int(tenant_id))))
        except TenantSuspendedError, TenantNotFoundError:
            return False
        except Exception as exc:  # 租户源不可达等：fail-closed，保留记录下一轮再判
            _LOGGER.warning("msg_tenant_availability_unavailable", tenant_id=tenant_id, error=repr(exc))
            return False
        return True


@dataclass
class TenantLifecycleMetrics:
    """生命周期消费的进程内计数（外部指标出口归 06-3）。"""

    applied: int = 0
    """成功执行数。"""

    skipped: int = 0
    """版本不新 / 重复投递跳过数。"""

    failed: int = 0
    """处理失败数（已上抛，等待重试 / 死信）。"""

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """返回当前计数快照。

        Returns:
            ConcurrentStableDict[str, int]: 三计数。
        """
        return ConcurrentStableDict({"applied": self.applied, "skipped": self.skipped, "failed": self.failed})


_METRICS = TenantLifecycleMetrics()
"""进程级计数（测试经 `reset_tenant_lifecycle_metrics()` 隔离）。"""


def reset_tenant_lifecycle_metrics() -> None:
    """清零进程级计数（测试隔离用；生产路径不调用）。"""
    _METRICS.applied = 0
    _METRICS.skipped = 0
    _METRICS.failed = 0


def get_tenant_lifecycle_metrics() -> TenantLifecycleMetrics:
    """取进程级生命周期计数。

    Returns:
        TenantLifecycleMetrics: 进程级计数容器。
    """
    return _METRICS


@dataclass(frozen=True)
class SuspendOutcome(BaseValueObject):
    """停用动作结果（审计载荷用）。"""

    connections: int
    """取消的连接数。"""

    streams: int
    """取消的长驻流数。"""


@dataclass(frozen=True)
class ActivateOutcome(BaseValueObject):
    """启用动作结果。"""

    cancel_purge: bool
    """是否取消了待清理。"""


@dataclass(frozen=True)
class DeleteOutcome(BaseValueObject):
    """删除动作结果（易失资源清理 + 排期）。"""

    topics: int
    """主题数。"""

    connections: int
    """取消的连接数。"""

    streams: int
    """取消的长驻流数。"""

    delay_records: int
    """清空的延迟记录数。"""

    group_records: int
    """清空的组记录数。"""

    dead_letters: int
    """清空的死信数。"""

    replay_tasks_cancelled: int
    """取消的重放任务数。"""

    purge_after: datetime
    """最早到期点（UTC naive）。"""


class DelayTenantStore(Protocol):
    """延迟区按租户清理所需的最小端口（内存 / 文件实现均满足）。"""

    def drop_tenant(self, *, tenant_id: int) -> int:
        """清空某租户的延迟记录。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 清掉的记录数。
        """
        ...

    async def count_for_tenant(self, *, tenant_id: int) -> int:
        """取某租户的延迟记录数。

        Args:
            tenant_id: 租户主键。

        Returns:
            int: 记录数。
        """
        ...


class TenantLifecycleRuntime:
    """生命周期动作所需的进程级依赖集合。"""

    def __init__(
        self,
        *,
        ledger: TenantLifecycleLedger,
        audit_sink: AuditSink,
        connections: ConnectionRegistry,
        streams: StreamRegistry,
        delay_store: DelayTenantStore,
        delay_wheel: DelayWheel,
        message_store: FileMessageStore,
        offset_store: FileOffsetStore,
        groups: GroupStore,
        dead_letters: DeadLetterQueue,
        delivery_state: DeliveryState,
        waiter: DeliveryWaiter,
        topic_cache: TopicCache,
        retention_throttle: RetentionThrottle,
        directory: PartitionDirectory,
        metrics: TenantLifecycleMetrics | None = None,
    ) -> None:
        """初始化。

        Args:
            ledger: 幂等账本。
            audit_sink: 基座审计出口。
            connections: 连接登记表。
            streams: 长驻流登记表。
            delay_store: 延迟区（文件实现）。
            delay_wheel: 延迟时间轮（含 `.delay` 记录引用）。
            message_store: 消息存储（文件实现；清扫时关句柄）。
            offset_store: 位点存储（文件实现；清扫时删位点）。
            groups: 组存储。
            dead_letters: 死信队列。
            delivery_state: 投递状态。
            waiter: 分区唤醒器。
            topic_cache: 主题快照缓存。
            retention_throttle: 保留节流窗口。
            directory: 分区目录登记表。
            metrics: 生命周期计数（缺省进程级）。
        """
        self.ledger = ledger
        self.audit_sink = audit_sink
        self.connections = connections
        self.streams = streams
        self.delay_store = delay_store
        self.delay_wheel = delay_wheel
        self.message_store = message_store
        self.offset_store = offset_store
        self.groups = groups
        self.dead_letters = dead_letters
        self.delivery_state = delivery_state
        self.waiter = waiter
        self.topic_cache = topic_cache
        self.retention_throttle = retention_throttle
        self.directory = directory
        self.metrics = metrics if metrics is not None else _METRICS

    async def suspend(self, *, tenant_id: int, version: int, context: TenantLifecycleContext) -> SuspendOutcome:
        """停用：取消已建立连接 / 长驻流；不复制租户状态、不动数据。

        Args:
            tenant_id: 租户主键。
            version: 事件版本。
            context: 生命周期上下文。

        Returns:
            SuspendOutcome: 取消计数。
        """
        connections = self.connections.purge_tenant(tenant_id=tenant_id, reason="tenant_suspended")
        streams = self.streams.cancel_tenant(tenant_id=tenant_id)
        await self.record_audit(
            action="msg:tenant.suspend",
            path=f"/api/v1/msg/admin/tenants/{tenant_id}/suspend",
            tenant_id=tenant_id,
            payload=ConcurrentStableDict({"version": version, "connections": connections, "streams": streams}),
            strong=False,
            context=context,
        )
        _LOGGER.info(
            "msg_tenant_suspended",
            tenant_id=tenant_id,
            version=version,
            connections=connections,
            streams=streams,
        )
        return SuspendOutcome(connections=connections, streams=streams)

    async def activate(self, *, tenant_id: int, version: int, context: TenantLifecycleContext) -> ActivateOutcome:
        """启用：取消待清理；已物理删除的主题不恢复。

        Args:
            tenant_id: 租户主键。
            version: 事件版本。
            context: 生命周期上下文。

        Returns:
            ActivateOutcome: 是否取消待清理。
        """
        cancelled = await self.ledger.cancel_purge(tenant_id=tenant_id, event_id=context.event_id)
        await self.record_audit(
            action="msg:tenant.activate",
            path=f"/api/v1/msg/admin/tenants/{tenant_id}/activate",
            tenant_id=tenant_id,
            payload=ConcurrentStableDict({"version": version, "cancel_purge": cancelled}),
            strong=False,
            context=context,
        )
        _LOGGER.info("msg_tenant_activated", tenant_id=tenant_id, version=version, cancel_purge=cancelled)
        return ActivateOutcome(cancel_purge=cancelled)

    async def delete(self, *, tenant_id: int, version: int, context: TenantLifecycleContext) -> DeleteOutcome:
        """删除：停写 + 易失资源清理 + 按主题保留期排定清理。

        Args:
            tenant_id: 租户主键。
            version: 事件版本。
            context: 生命周期上下文。

        Returns:
            DeleteOutcome: 清理计数与最早到期点。
        """
        topics = await self.ledger.list_topics(tenant_id=tenant_id)
        base_at = _utc_naive(context.occurred_at) or _utc_now_naive()
        deadlines = [base_at + timedelta(milliseconds=topic.retention_ms) for topic in topics]
        purge_after = min(deadlines) if deadlines else base_at
        await self.ledger.mark_deleted(
            tenant_id=tenant_id,
            version=version,
            deleted_event_at=base_at,
            purge_after=purge_after,
        )
        replay_tasks_cancelled = await self.ledger.cancel_active_replay_tasks(tenant_id=tenant_id, at=base_at)

        connections = self.connections.purge_tenant(tenant_id=tenant_id, reason="tenant_deleted")
        streams = self.streams.cancel_tenant(tenant_id=tenant_id)
        delay_records = self.delay_store.drop_tenant(tenant_id=tenant_id)
        self.delay_wheel.forget_tenant(tenant_id=tenant_id)
        group_records = await self.groups.purge_tenant(tenant_id=tenant_id)
        topic_ids = tuple(topic.topic_id for topic in topics)
        dead_letters = await self.dead_letters.purge_topics(topic_ids=topic_ids)
        for topic in topics:
            self.delivery_state.drop_topic(topic_id=topic.topic_id)
            self.waiter.forget_topic(topic_id=topic.topic_id)
            self.topic_cache.invalidate(tenant_id=tenant_id, namespace=topic.namespace, name=topic.name)
            for partition_no in range(topic.partition_count):
                self.retention_throttle.forget(topic_id=topic.topic_id, partition_no=partition_no)

        outcome = DeleteOutcome(
            topics=len(topics),
            connections=connections,
            streams=streams,
            delay_records=delay_records,
            group_records=group_records,
            dead_letters=dead_letters,
            replay_tasks_cancelled=replay_tasks_cancelled,
            purge_after=purge_after,
        )
        await self.record_audit(
            action="msg:tenant.delete",
            path=f"/api/v1/msg/admin/tenants/{tenant_id}/delete",
            tenant_id=tenant_id,
            payload=ConcurrentStableDict(
                {
                    "version": version,
                    "topics": outcome.topics,
                    "connections": outcome.connections,
                    "streams": outcome.streams,
                    "delay_records": outcome.delay_records,
                    "group_records": outcome.group_records,
                    "dead_letters": outcome.dead_letters,
                    "replay_tasks_cancelled": outcome.replay_tasks_cancelled,
                    "purge_after": purge_after.isoformat(),
                }
            ),
            strong=True,
            context=context,
        )
        _LOGGER.info(
            "msg_tenant_deleted_scheduled",
            tenant_id=tenant_id,
            version=version,
            topics=outcome.topics,
            purge_after=purge_after.isoformat(),
            connections=outcome.connections,
            streams=outcome.streams,
            delay_records=outcome.delay_records,
            group_records=outcome.group_records,
            dead_letters=outcome.dead_letters,
            replay_tasks_cancelled=outcome.replay_tasks_cancelled,
        )
        return outcome

    async def record_audit(
        self,
        *,
        action: str,
        path: str,
        tenant_id: int,
        payload: ConcurrentStableDict[str, object],
        strong: bool,
        context: TenantLifecycleContext | None = None,
    ) -> bool:
        """写一条生命周期操作审计。

        Args:
            action: 审计动作。
            path: 审计路径（不新增真实路由）。
            tenant_id: 租户主键。
            payload: 载荷（JSON 可序列化）。
            strong: 是否强审计（失败即抛）。
            context: 生命周期上下文。

        Returns:
            bool: 审计是否写入成功。

        Raises:
            TenantLifecycleAuditError: 强审计失败（未写入 / 被拒 / 异常）。
        """
        event = build_operation_event(
            tenant_id=str(int(tenant_id)),
            user_id=None,
            username=None,
            module="msg",
            action=action,
            method="POST",
            path=path,
            query=None,
            params=json.dumps(dict(payload), ensure_ascii=False),
            ip=None,
            user_agent=None,
            cost_time=None,
            status=200,
            error_msg=None,
            trace_id=None,
        )
        try:
            accepted = await self.audit_sink.emit(event, required=strong)
        except Exception as exc:
            _LOGGER.warning("msg_tenant_audit_failed", action=action, tenant_id=tenant_id, error=repr(exc))
            if strong:
                raise TenantLifecycleAuditError(f"租户生命周期审计写入失败：{action}") from exc
            return False
        if not accepted:
            _LOGGER.warning("msg_tenant_audit_rejected", action=action, tenant_id=tenant_id)
            if strong:
                raise TenantLifecycleAuditError(f"租户生命周期审计被拒绝：{action}")
            return False
        return True


_runtime_holder: TenantLifecycleRuntime | None = None
"""进程级运行时（应用装配时登记；测试可重置）。"""


def set_tenant_lifecycle_runtime(runtime: TenantLifecycleRuntime) -> None:
    """登记进程级运行时（应用 `configure_service()` 调用）。

    Args:
        runtime: 生命周期运行时。
    """
    global _runtime_holder
    _runtime_holder = runtime


def get_tenant_lifecycle_runtime() -> TenantLifecycleRuntime | None:
    """取进程级运行时；未装配返回 None。

    Returns:
        TenantLifecycleRuntime | None: 运行时。
    """
    return _runtime_holder


def reset_tenant_lifecycle_runtime() -> None:
    """清空进程级运行时（测试隔离用）。"""
    global _runtime_holder
    _runtime_holder = None


def _utc_now_naive() -> datetime:
    """当前 UTC naive 时间（与库列口径一致）。

    Returns:
        datetime: UTC naive 当前时间。
    """
    return datetime.now(UTC).replace(tzinfo=None)


def _utc_naive(value: datetime | None) -> datetime | None:
    """把事件时间规整为 UTC naive。

    Args:
        value: 事件时间（可能带时区）。

    Returns:
        datetime | None: UTC naive；None 原样返回。
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
