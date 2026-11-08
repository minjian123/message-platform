"""消息平台租户生命周期消费（05-1 第 7 条 + 05-5 四动作）。

- **四类事件**：`tenant.created` / `tenant.suspended` / `tenant.activated` / `tenant.deleted`；
  同一消费组 `msg-tenant-lifecycle`，`ordered=True` 保证同租户事件串行。
- **动作**：`created` 只建租户目录（不建命名空间 / 配额，仍按首次发布懒创建）；`suspended`
  取消已建立连接 / 长驻流；`activated` 取消待清理；`deleted` 停写 + 易失资源清理 + 按主题
  保留期排定清理。
- **幂等**：业务级 `(tenant_id, action, version)` 落 `msg_tenant_lifecycle`（生产）或
  `.lifecycle.json`（仅 05-1 兼容读 / 测试替身）；投递级沿用基座 `sys_event_consumed`。
- **失败语义**：动作抛错**不记账、原样上抛**，由基座消费组退避重试 / 死信；租户开通事务已由
  基座提交，事件消费不阻塞开通。
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ConfigError
from bms_core.core.logging import get_logger
from bms_core.events.base import EventConsumer, EventEnvelope
from bms_core.events.consumer_records import EventConsumerRecord
from bms_core.tenant.lifecycle import (
    LIFECYCLE_APPLIED,
    TENANT_ACTIVATED,
    TENANT_CREATED,
    TENANT_DELETED,
    TENANT_LIFECYCLE_EVENT_TYPES,
    TENANT_SUSPENDED,
    BaseTenantResourceHandler,
    TenantLifecycleContext,
    TenantLifecycleDispatcher,
)

from bms_msg.broker.file_store import data_root
from bms_msg.core.config import DATA_ROOT_SUBDIR
from bms_msg.tenant.ledger import (
    LEDGER_NAME,
    FileTenantLifecycleLedger,
    TenantLifecycleLedger,
)
from bms_msg.tenant.runtime import (
    TenantLifecycleMetrics,
    TenantLifecycleRuntime,
    get_tenant_lifecycle_metrics,
    get_tenant_lifecycle_runtime,
    reset_tenant_lifecycle_metrics,
)

__all__ = [
    "LEDGER_NAME",
    "MSG_TENANT_LIFECYCLE_GROUP",
    "MsgTenantResourceHandler",
    "TenantLifecycleConsumer",
    "TenantLifecycleMetrics",
    "event_consumer_records",
    "reset_tenant_lifecycle_metrics",
    "tenant_lifecycle_consumer",
]

_LOGGER = get_logger("bms_msg.tenant_lifecycle")

MSG_TENANT_LIFECYCLE_GROUP = "msg-tenant-lifecycle"
"""消费组名（装载时补 `[event_consumer].group_prefix` 前缀，如 `bms-msg-tenant-lifecycle`）。"""

_EVENT_DESCRIPTIONS: ConcurrentStableDict[str, str] = ConcurrentStableDict(
    {
        TENANT_CREATED: "租户开通初始化：tenant.created → 租户数据目录 + 幂等账本",
        TENANT_SUSPENDED: "租户停用：tenant.suspended → 取消已建立连接 / 长驻流",
        TENANT_ACTIVATED: "租户启用：tenant.activated → 取消待清理",
        TENANT_DELETED: "租户删除：tenant.deleted → 易失资源清理 + 按保留期排定清理",
    }
)
"""四条事件声明的说明文案（顺序与 `_LIFECYCLE_EVENTS` 一致）。"""

_LIFECYCLE_EVENTS: ConcurrentStableList[str] = ConcurrentStableList(
    (
        TENANT_CREATED,
        TENANT_SUSPENDED,
        TENANT_ACTIVATED,
        TENANT_DELETED,
    )
)
"""本消费组订阅的四类事件（与 bms 契约一一对应）。"""


class MsgTenantResourceHandler(BaseTenantResourceHandler):
    """msg 产品资源动作实现：目录 / 连接 / 延迟 / 删除排期。

    `root` / `ledger` 可注入用于测试；生产由 `tenant_lifecycle_consumer()` 取进程级运行时，
    运行时持有 DB 账本与各资源句柄。`on_created` 不依赖运行时（05-1 回归）。
    """

    plugin_name = "msg.file"

    def __init__(
        self,
        *,
        root: Path | None = None,
        ledger: TenantLifecycleLedger | None = None,
        runtime: TenantLifecycleRuntime | None = None,
    ) -> None:
        """初始化。

        Args:
            root: 数据根目录（测试注入；缺省 `data_root()`）。
            ledger: 幂等账本（缺省文件账本，供 05-1 回归 / 测试）。
            runtime: 生命周期运行时（停用 / 启用 / 删除必需；缺省取进程级）。
        """
        self._root = Path(root) if root is not None else data_root() / DATA_ROOT_SUBDIR
        self._ledger = ledger if ledger is not None else FileTenantLifecycleLedger(root=self._root)
        self._runtime = runtime if runtime is not None else get_tenant_lifecycle_runtime()

    async def applied_version(self, *, tenant_id: int, action: str) -> int:
        """读取已处理版本（业务级幂等判定依据）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。

        Returns:
            int: 已处理版本；无记录返回 `-1`。
        """
        return await self._ledger.applied_version(tenant_id=tenant_id, action=action)

    async def mark_applied(self, *, tenant_id: int, action: str, version: int, event_id: str | None = None) -> None:
        """记账（动作成功后由分发器调用）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。
            version: 已处理版本。
            event_id: 事件 ID（排障用；可空）。
        """
        await self._ledger.mark_applied(tenant_id=tenant_id, action=action, version=version, event_id=event_id)

    async def on_created(self, context: TenantLifecycleContext) -> None:
        """预创建租户数据目录（幂等；不建命名空间、不建配额）。

        Args:
            context: 生命周期上下文（动作名恒为 `created`）。
        """
        (self._root / str(context.tenant_id)).mkdir(parents=True, exist_ok=True)
        _LOGGER.info(
            "msg_tenant_created_initialized",
            tenant_id=context.tenant_id,
            db_key=context.db_key,
            version=context.version,
        )

    async def on_suspended(self, context: TenantLifecycleContext) -> None:
        """停用：取消已建立连接 / 长驻流（数据保留）。

        Args:
            context: 生命周期上下文。

        Raises:
            ConfigError: 未装配运行时（后台消费必需）。
        """
        await self._require_runtime().suspend(tenant_id=context.tenant_id, version=context.version, context=context)

    async def on_activated(self, context: TenantLifecycleContext) -> None:
        """启用：取消待清理（已物理删除的主题不恢复）。

        Args:
            context: 生命周期上下文。

        Raises:
            ConfigError: 未装配运行时（后台消费必需）。
        """
        await self._require_runtime().activate(tenant_id=context.tenant_id, version=context.version, context=context)

    async def on_deleted(self, context: TenantLifecycleContext) -> None:
        """删除：停写 + 易失资源清理 + 按主题保留期排定清理。

        Args:
            context: 生命周期上下文。

        Raises:
            ConfigError: 未装配运行时（后台消费必需）。
        """
        await self._require_runtime().delete(tenant_id=context.tenant_id, version=context.version, context=context)

    def _require_runtime(self) -> TenantLifecycleRuntime:
        """取运行时；未装配即 fail-closed。

        Returns:
            TenantLifecycleRuntime: 运行时。

        Raises:
            ConfigError: 未装配运行时。
        """
        if self._runtime is None:
            raise ConfigError("租户生命周期运行时未装配（configure_service 未登记）")
        return self._runtime


class TenantLifecycleConsumer(EventConsumer):
    """单事件处理器：一条租户生命周期事件 → 基座生命周期分发器。"""

    def __init__(
        self,
        *,
        event_type: str = TENANT_CREATED,
        handler: BaseTenantResourceHandler | None = None,
        metrics: TenantLifecycleMetrics | None = None,
    ) -> None:
        """初始化。

        Args:
            event_type: 订阅的事件类型。
            handler: 资源动作实现（缺省 `MsgTenantResourceHandler()`；测试可注入）。
            metrics: 计数容器（缺省进程级）。
        """
        self._event_type = event_type
        self._handler = handler if handler is not None else MsgTenantResourceHandler()
        self._dispatcher = TenantLifecycleDispatcher(handler=self._handler)
        self._metrics = metrics if metrics is not None else get_tenant_lifecycle_metrics()

    @property
    def event_type(self) -> str:
        """订阅的事件类型。

        Returns:
            str: 事件类型。
        """
        return self._event_type

    async def consume(self, event: EventEnvelope) -> None:
        """消费一条租户生命周期事件（失败原样上抛，由消费组重试 / 死信）。

        Args:
            event: 事件信封。
        """
        try:
            outcome = await self._dispatcher.dispatch(event)
        except Exception as exc:
            self._metrics.failed += 1
            _LOGGER.warning(
                "msg_tenant_lifecycle_failed",
                event_type=event.event_type,
                event_id=event.event_id,
                tenant_id=event.tenant_id,
                error=type(exc).__name__,
            )
            raise
        if outcome == LIFECYCLE_APPLIED:
            self._metrics.applied += 1
        else:
            self._metrics.skipped += 1


def tenant_lifecycle_consumer() -> EventConsumer:
    """构造 `tenant.created` 处理器工厂（05-1 兼容零参入口）。

    Returns:
        EventConsumer: 租户生命周期处理器。
    """
    return _factory(TENANT_CREATED)()


def event_consumer_records() -> ConcurrentStableList[EventConsumerRecord]:
    """msg 自持的租户生命周期消费组声明（四类事件各一条）。

    Returns:
        ConcurrentStableList[EventConsumerRecord]: 消费组声明。
    """
    records: ConcurrentStableList[EventConsumerRecord] = ConcurrentStableList()
    for event_type in _LIFECYCLE_EVENTS:
        records.add(
            EventConsumerRecord(
                consumer=MSG_TENANT_LIFECYCLE_GROUP,
                event_type=event_type,
                handler_factory=_factory(event_type),
                ordered=True,
                description=_EVENT_DESCRIPTIONS[event_type],
            )
        )
    return records


def _factory(event_type: str) -> Callable[[], EventConsumer]:
    """构造指定事件类型的处理器工厂。

    Args:
        event_type: 事件类型。

    Returns:
        Callable[[], EventConsumer]: 零参工厂。
    """

    def _build() -> EventConsumer:
        """构造处理器实例。

        Returns:
            EventConsumer: 生命周期处理器。
        """
        return TenantLifecycleConsumer(event_type=event_type)

    return _build


# 事件名 → 动作名的映射由基座维护；此处仅做导入期契约可见性声明。
if set(_LIFECYCLE_EVENTS) != set(TENANT_LIFECYCLE_EVENT_TYPES):  # pragma: no cover - 导入期契约断言
    raise ConfigError("msg 生命周期事件集合与 bms 契约不一致")
