"""租户生命周期运行时依赖：账本、审计出口与各进程内 / 文件态资源句柄。

**为什么需要运行时对象**：事件消费在应用启动期由基座消费组装载，处理器没有请求态；它需要的
连接表 / 流表 / 延迟区 / 消息存储等是 `app.state` 里的进程级单例。`configure_service()` 在消费组
`load()` 之前执行，因此把 `app.state` 的引用收进本对象并登记为进程级运行时，处理器即可在后台
安全取用。

**物理清理入口由外部挂载**：`TenantPurgeService` 依赖本对象（要拿到文件 / 进程内资源句柄），
不能在构造期反向注入。装配期先建运行时、再建清扫服务，然后 `attach_purge()` 挂上；
`on_purged` 只经运行时入口调用，未挂载即 fail-closed（不静默跳过清理）。

**不建租户状态**：本对象只持有资源句柄，不保存「某租户是否停用」；状态判定一律走基座租户守卫 /
租户源。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from bms_core.audit.events import build_operation_event
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.exceptions import ConfigError, TenantNotFoundError, TenantSuspendedError
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
    "PurgeOutcome",
    "SuspendOutcome",
    "TenantLifecycleAuditError",
    "TenantLifecycleMetrics",
    "TenantLifecycleRuntime",
    "TenantPurgeEntry",
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
    """删除动作结果（只停写 + 易失资源清理；**不排定数据清理**）。"""

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

    deleted_event_at: datetime
    """删除事件生效时刻（UTC naive；保留期与清理时点由平台给）。"""


@dataclass(frozen=True)
class PurgeOutcome(BaseValueObject):
    """物理清理结果（`tenant.purged` 触发；跨模块传递用，避免运行时反向依赖清扫服务）。"""

    tenant_id: int
    """租户主键。"""

    purged: bool
    """是否完成该租户全部清理。"""

    deleted_topics: int = 0
    """删除的主题数。"""

    deleted_data_dirs: int = 0
    """删除的数据目录数。"""

    deleted_offset_files: int = 0
    """删除的位点文件数。"""

    deleted_db_rows: int = 0
    """删除的 DB 行数。"""

    skipped: bool = False
    """是否因「此前已清理」（`purged_at` 已有值）跳过。"""


class TenantPurgeEntry(Protocol):
    """物理清理入口契约（实现为 `TenantPurgeService`；装配期由 `attach_purge()` 挂载）。"""

    async def purge_tenant(self, *, tenant_id: int, at: datetime) -> PurgeOutcome:
        """按 `tenant_id` 一次性物理删除该租户全部消息资源（幂等可重入）。

        Args:
            tenant_id: 租户主键。
            at: 清理时刻（UTC naive）。

        Returns:
            PurgeOutcome: 清理结果。
        """
        ...


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
        purge_entry: TenantPurgeEntry | None = None,
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
            purge_entry: 物理清理入口（装配期 `attach_purge()` 也可后挂）。
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
        self._purge_entry = purge_entry

    def attach_purge(self, entry: TenantPurgeEntry) -> None:
        """挂载物理清理入口（装配期在清扫服务构造后调用；测试可注入替身）。

        Args:
            entry: 物理清理入口实现。
        """
        self._purge_entry = entry

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
        """删除（第一步）：停写 + 易失资源清理，**不排定数据清理**。

        消息数据 / 位点 / 元数据保留到平台发 `tenant.purged`（保留期与清理时点归平台，本仓不自算）。
        延迟区立即清空：它属「尚未投递的生产」，留着会在重启重建后继续投。

        Args:
            tenant_id: 租户主键。
            version: 事件版本。
            context: 生命周期上下文。

        Returns:
            DeleteOutcome: 清理计数与删除事件时刻。
        """
        topics = await self.ledger.list_topics(tenant_id=tenant_id)
        deleted_event_at = _utc_naive(context.occurred_at) or _utc_now_naive()
        await self.ledger.mark_deleted(
            tenant_id=tenant_id,
            version=version,
            deleted_event_at=deleted_event_at,
        )
        replay_tasks_cancelled = await self.ledger.cancel_active_replay_tasks(tenant_id=tenant_id, at=deleted_event_at)

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
            deleted_event_at=deleted_event_at,
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
                    "deleted_event_at": deleted_event_at.isoformat(),
                }
            ),
            strong=True,
            context=context,
        )
        _LOGGER.info(
            "msg_tenant_deleted",
            tenant_id=tenant_id,
            version=version,
            topics=outcome.topics,
            deleted_event_at=deleted_event_at.isoformat(),
            connections=outcome.connections,
            streams=outcome.streams,
            delay_records=outcome.delay_records,
            group_records=outcome.group_records,
            dead_letters=outcome.dead_letters,
            replay_tasks_cancelled=outcome.replay_tasks_cancelled,
        )
        return outcome

    async def purge(self, *, tenant_id: int, version: int, context: TenantLifecycleContext) -> PurgeOutcome:
        """物理清理（第二步）：由平台 `tenant.purged` 授权后一次性删除该租户消息资源。

        只做转发与日志：删除顺序、审计与 `purged_at` 由清扫服务统一负责（`msg:tenant.purge` 强审计），
        避免同一段删除逻辑出现两份。重复消费的幂等由清扫服务按 `purged_at` 判定。

        Args:
            tenant_id: 租户主键。
            version: 事件版本（`tenant.purged` 无 `version` 字段，按 0 处理）。
            context: 生命周期上下文。

        Returns:
            PurgeOutcome: 清理结果。

        Raises:
            ConfigError: 物理清理入口未挂载（装配缺漏，fail-closed 不静默跳过）。
        """
        entry = self._purge_entry
        if entry is None:
            raise ConfigError("租户物理清理入口未挂载（TenantPurgeService 未登记）")
        at = _utc_naive(context.occurred_at) or _utc_now_naive()
        outcome = await entry.purge_tenant(tenant_id=tenant_id, at=at)
        _LOGGER.info(
            "msg_tenant_purged",
            tenant_id=tenant_id,
            version=version,
            purged=outcome.purged,
            skipped=outcome.skipped,
            deleted_topics=outcome.deleted_topics,
            deleted_data_dirs=outcome.deleted_data_dirs,
            deleted_offset_files=outcome.deleted_offset_files,
            deleted_db_rows=outcome.deleted_db_rows,
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
