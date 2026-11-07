"""消息平台租户生命周期消费（05-1 第 7 条；需求 `05-6` 归口）。

- **只做非库类资源**：`tenant.created` 到达时预创建 `data/{tenant_id}/` 并写幂等账本
  `.lifecycle.json`；**不建命名空间、不建默认配额**——命名空间按架构 07 §5.1 在首次发布时
  懒创建，配额归 05-2。
- **基座契约**：实现 `BaseTenantResourceHandler`（四动作 + 版本账本）并经
  `TenantLifecycleDispatcher` 分发；本期只声明 `tenant.created`，05-5 直接补其余三动作。
- **幂等**：业务级 `(tenant_id, created, version)`；同版本重复投递 `skipped`，新版本重跑
  （目录创建天然幂等）；投递级幂等沿用基座 `sys_event_consumed` 账本，不另建表。
- **失败语义**：动作抛错**不记账、原样上抛**，由基座消费组退避重试 / 死信；租户开通事务已由
  基座提交，事件消费不阻塞开通。
- **可观测**：结构化日志 + 进程内三计数（`applied` / `skipped` / `failed`）；外部指标出口归 06-3。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.events.base import EventConsumer, EventEnvelope
from bms_core.events.consumer_records import EventConsumerRecord
from bms_core.tenant.lifecycle import (
    LIFECYCLE_APPLIED,
    TENANT_CREATED,
    BaseTenantResourceHandler,
    TenantLifecycleContext,
    TenantLifecycleDispatcher,
)

from bms_msg.broker.file_store import data_root

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

LEDGER_NAME = ".lifecycle.json"
"""租户目录下的幂等账本文件名（记录 `动作 → 已处理版本`）。"""

MSG_TENANT_LIFECYCLE_GROUP = "msg-tenant-lifecycle"
"""消费组名（装载时补 `[event_consumer].group_prefix` 前缀，如 `bms-msg-tenant-lifecycle`）。"""


@dataclass
class TenantLifecycleMetrics:
    """进程内生命周期计数（可观测的最小实现；外部指标出口归 06-3）。"""

    applied: int = 0
    """成功执行数。"""

    skipped: int = 0
    """版本不新 / 重复投递跳过数。"""

    failed: int = 0
    """处理失败数（已上抛，等待重试 / 死信）。"""

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """返回当前计数快照。

        Returns:
            ConcurrentStableDict[str, int]: `applied` / `skipped` / `failed` 三计数。
        """
        return ConcurrentStableDict({"applied": self.applied, "skipped": self.skipped, "failed": self.failed})


_METRICS = TenantLifecycleMetrics()
"""进程级计数（测试经 `reset_tenant_lifecycle_metrics()` 隔离）。"""


def reset_tenant_lifecycle_metrics() -> None:
    """清零进程级计数（测试隔离用；生产路径不调用）。"""
    _METRICS.applied = 0
    _METRICS.skipped = 0
    _METRICS.failed = 0


class MsgTenantResourceHandler(BaseTenantResourceHandler):
    """msg 产品资源动作实现：租户目录 + 文件账本。

    账本与资源同在租户目录下：目录创建幂等，账本用「临时文件 + `os.replace`」原子替换；
    本实现不触碰命名空间 / 配额（那两类归首次发布与 05-2）。
    """

    plugin_name = "msg.file"

    def __init__(self, root: Path | None = None) -> None:
        """初始化。

        Args:
            root: 数据根目录（测试注入；缺省 `data_root()`）。
        """
        self._root = Path(root) if root is not None else data_root()

    async def applied_version(self, *, tenant_id: int, action: str) -> int:
        """读取已处理版本（业务级幂等判定依据）。

        Args:
            tenant_id: 租户主键。
            action: 动作名（本期 `created`）。

        Returns:
            int: 已处理版本；账本不存在 / 损坏 / 该动作无记录返回 `-1`。
        """
        value = self._read_ledger(tenant_id).get(action)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return -1

    async def mark_applied(self, *, tenant_id: int, action: str, version: int) -> None:
        """记账（与资源动作同在租户目录内；原子替换）。

        Args:
            tenant_id: 租户主键。
            action: 动作名。
            version: 已处理版本。
        """
        directory = self._tenant_dir(tenant_id)
        directory.mkdir(parents=True, exist_ok=True)
        payload = self._read_ledger(tenant_id)
        payload[action] = version
        path = self._ledger_path(tenant_id)
        temporary = path.with_name(f"{LEDGER_NAME}.tmp")
        temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temporary, path)

    async def on_created(self, context: TenantLifecycleContext) -> None:
        """预创建租户数据目录（幂等；不建命名空间、不建配额）。

        Args:
            context: 生命周期上下文（动作名恒为 `created`）。
        """
        self._tenant_dir(context.tenant_id).mkdir(parents=True, exist_ok=True)
        _LOGGER.info(
            "msg_tenant_created_initialized",
            tenant_id=context.tenant_id,
            db_key=context.db_key,
            version=context.version,
        )

    def _tenant_dir(self, tenant_id: int) -> Path:
        """租户数据目录。

        Args:
            tenant_id: 租户主键。

        Returns:
            Path: `data_root()/{tenant_id}`。
        """
        return self._root / str(tenant_id)

    def _ledger_path(self, tenant_id: int) -> Path:
        """租户账本路径。

        Args:
            tenant_id: 租户主键。

        Returns:
            Path: `data_root()/{tenant_id}/.lifecycle.json`。
        """
        return self._tenant_dir(tenant_id) / LEDGER_NAME

    def _read_ledger(self, tenant_id: int) -> ConcurrentStableDict[str, int]:
        """读取账本（缺失 / 损坏 / 非对象均按空账本处理）。

        Args:
            tenant_id: 租户主键。

        Returns:
            ConcurrentStableDict[str, int]: 动作名 → 已处理版本（只保留整数版本）。
        """
        path = self._ledger_path(tenant_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return ConcurrentStableDict()
        except OSError, ValueError:
            _LOGGER.warning("msg_tenant_lifecycle_ledger_corrupt", tenant_id=tenant_id)
            return ConcurrentStableDict()
        if not isinstance(raw, dict):
            _LOGGER.warning("msg_tenant_lifecycle_ledger_corrupt", tenant_id=tenant_id)
            return ConcurrentStableDict()
        payload: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for key, value in cast("dict[str, object]", raw).items():
            if isinstance(value, int) and not isinstance(value, bool):
                payload[key] = value
        return payload


class TenantLifecycleConsumer(EventConsumer):
    """单事件处理器：`tenant.created` → 基座生命周期分发器。"""

    def __init__(
        self,
        *,
        handler: BaseTenantResourceHandler | None = None,
        metrics: TenantLifecycleMetrics | None = None,
    ) -> None:
        """初始化。

        Args:
            handler: 资源动作实现（缺省 `MsgTenantResourceHandler()`；测试可注入）。
            metrics: 计数容器（缺省进程级 `_METRICS`）。
        """
        self._handler = handler if handler is not None else MsgTenantResourceHandler()
        self._dispatcher = TenantLifecycleDispatcher(handler=self._handler)
        self._metrics = metrics if metrics is not None else _METRICS

    @property
    def event_type(self) -> str:
        """订阅的事件类型。

        Returns:
            str: `tenant.created`。
        """
        return TENANT_CREATED

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
    """构造处理器工厂（每次返回独立实例）。

    Returns:
        EventConsumer: 租户生命周期处理器。
    """
    return TenantLifecycleConsumer()


def event_consumer_records() -> ConcurrentStableList[EventConsumerRecord]:
    """msg 自持的租户生命周期消费组声明（本期只订阅 `tenant.created`）。

    Returns:
        ConcurrentStableList[EventConsumerRecord]: 消费组声明。
    """
    return ConcurrentStableList(
        (
            EventConsumerRecord(
                consumer=MSG_TENANT_LIFECYCLE_GROUP,
                event_type=TENANT_CREATED,
                handler_factory=tenant_lifecycle_consumer,
                description="消息平台租户开通初始化：tenant.created → 租户数据目录 + 幂等账本",
            ),
        )
    )
