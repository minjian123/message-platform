"""选举、epoch 与故障切换（任务 02-5）：手动切主的两阶段编排。

**职责边界**：本模块只做「怎么换 leader」——预检、phase 1 CAS（`switching` + epoch+1）、
等待旧租约、目标节点截断 / 保留尾部并激活、phase 2 收口与审计；**不做**自动选举、
**不做**真实跨实例网络控制通道与多实例编排（02-8）、**不做**管理端点与二次确认（06-2）。

**为什么服务只认两个协议**：`LeaderSwitchStore`（元数据 CAS）与 `LeaderControlTransport`
（节点控制）把「生产实现」与「判定逻辑」分开；生产实现见 `services/leader_switch.py` 与
`main.py` 的进程内控制传输，02-8 换网络实现时本模块零改动。

**两阶段与幂等**：phase 1 先停写（`switch_state='switching'`）并推进 epoch，等旧租约过期后
才让目标节点截断并激活；中断后同 target / 同 `switch_from_epoch` 的重试从等待 / apply 续跑，
不同 target 的请求被拒绝，避免来回切换制造二次分叉。
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from bms_msg.audit.trail import LEADER_SWITCH_ACTION, AuditEmitter, emit_sensitive
from bms_msg.broker.replication import (
    ReplicationCoordinator,
    ReplicationSnapshot,
    SwitchApplyCommand,
    SwitchApplyResult,
)
from bms_msg.core.config import MS_PER_SECOND

__all__ = [
    "InProcessLeaderControlTransport",
    "LeaderControlTransport",
    "LeaderSwitchError",
    "LeaderSwitchMetrics",
    "LeaderSwitchService",
    "LeaderSwitchStore",
    "SwitchApplyCommand",
    "SwitchApplyResult",
    "SwitchBatchResult",
    "SwitchFailure",
    "SwitchPartitionRequest",
    "SwitchPartitionState",
    "SwitchResult",
    "SwitchTopicRequest",
]

_LOGGER = get_logger("bms_msg.election")

LEADER_SWITCH_METRIC_NAME = "msg_leader_switch_total"
"""切主结果计数器名（02-8；标签 `result`）。"""

LEADER_SWITCH_RESULT_DEGRADED = "degraded"
"""切主结果：`isr_verified=false`（force 或无法验证 ISR），不计通过。"""

LEADER_SWITCH_RESULT_FAILED = "failed"
"""切主结果：预检 / CAS / 控制通道失败。"""

SWITCH_STATE_IDLE = "idle"
"""切换状态：空闲（可写）。"""

SWITCH_STATE_SWITCHING = "switching"
"""切换状态：切换窗口（所有写被拒）。"""

SWITCH_RESULT_TRUNCATED = "truncated"
"""切换结果：按真实 HW 截断了新 leader 的未提交尾部。"""

SWITCH_RESULT_RETAINED = "retained"
"""切换结果：旧 leader 不可达，保留未提交尾部。"""

SWITCH_RESULT_NOOP = "noop"
"""切换结果：无需截断。"""

_SWITCH_AUDIT_PATH = "/internal/msg/partition/failover"
"""内部切换动作的审计路径（不是 HTTP 端点，只用于审计定位）。"""


class LeaderSwitchError(RuntimeError):
    """切换被拒绝 / 失败（内部领域错误；管理面 06-2 据此给 409 / 503）。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        """初始化。

        Args:
            reason: 结构化原因（预检 / CAS / 控制通道）。
            detail: 排障详情。
        """
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


@dataclass(frozen=True)
class SwitchPartitionState(BaseValueObject):
    """分区切换所需的元数据快照（来自 `msg_partition`，含主题身份用于审计）。"""

    topic_id: int
    partition_no: int
    tenant_id: int
    namespace: str
    topic: str
    leader: str
    epoch: int
    replicas: ConcurrentStableFrozenList[str]
    switch_state: str = SWITCH_STATE_IDLE
    switch_from_leader: str | None = None
    switch_from_epoch: int | None = None


@dataclass(frozen=True)
class SwitchPartitionRequest(BaseValueObject):
    """单分区切换请求。"""

    topic_id: int
    partition_no: int
    target: str | None = None
    """新 leader；None = 在 ISR 内选 LEO 最大（并列按副本集顺序）。"""

    expected_epoch: int | None = None
    """预期任期（None = 以读取到的当前 epoch 为预期值；仍走 CAS）。"""

    force: bool = False
    """旧 leader 不可达且无法验证 ISR 时是否允许降级继续。"""

    operator: str = "system"
    """触发人（06-2 传当前用户；演练传 `system:drill`）。"""

    reason: str = ""
    """切换原因（审计与排障）。"""


@dataclass(frozen=True)
class SwitchTopicRequest(BaseValueObject):
    """主题批量切换请求（逐分区串行，独立 CAS）。"""

    topic_id: int
    targets: ConcurrentStableFrozenDict[int, str] | None = None
    """分区号 → 目标 leader；None = 每个分区自动选。"""

    expected_epochs: ConcurrentStableFrozenDict[int, int] | None = None
    """分区号 → 预期 epoch；缺省分区以当前 epoch 为预期值。"""

    force: bool = False
    operator: str = "system"
    reason: str = ""


@dataclass(frozen=True)
class SwitchResult(BaseValueObject):
    """单分区切换结果（成功；管理面与审计共用）。"""

    topic_id: int
    partition_no: int
    from_leader: str
    to_leader: str
    from_epoch: int
    to_epoch: int
    applied: bool
    """False = 已是目标 leader（幂等 no-op）。"""

    watermark: int | None
    """切换水位（None = 未知，走保留）。"""

    truncated_to: int | None
    """截断后的 LEO；None = 未截断。"""

    retained_tail: int
    """保留的未提交条数（0 = 未保留）。"""

    isr_verified: bool
    """是否已验证目标在旧 leader 的 ISR（或最后已知 ISR）内。"""

    duration_ms: int


@dataclass(frozen=True)
class SwitchFailure(BaseValueObject):
    """单分区切换失败（批量结果里的失败项）。"""

    topic_id: int
    partition_no: int
    reason: str
    detail: str


@dataclass(frozen=True)
class SwitchBatchResult(BaseValueObject):
    """主题批量切换结果（成功项 + 失败项；不回滚已成功分区）。"""

    items: ConcurrentStableFrozenList[SwitchResult] = field(
        default_factory=lambda: ConcurrentStableFrozenList[SwitchResult]()
    )
    failures: ConcurrentStableFrozenList[SwitchFailure] = field(
        default_factory=lambda: ConcurrentStableFrozenList[SwitchFailure]()
    )


class LeaderSwitchStore(BaseFrameworkObject, ABC):
    """切换元数据存储端口（生产实现 `DbLeaderSwitchStore`；用例注入内存实现）。"""

    @abstractmethod
    async def load_partition(self, *, topic_id: int, partition_no: int) -> SwitchPartitionState | None:
        """读分区切换状态（含主题身份；行缺失返回 None）。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            SwitchPartitionState | None: 分区状态。
        """

    @abstractmethod
    async def load_topic_partitions(self, *, topic_id: int) -> ConcurrentStableList[SwitchPartitionState]:
        """读主题全部分区（按分区号升序）。

        Args:
            topic_id: 主题主键。

        Returns:
            ConcurrentStableList[SwitchPartitionState]: 分区状态。
        """

    @abstractmethod
    async def begin_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        expected_epoch: int,
        target: str,
        from_leader: str,
        from_epoch: int,
    ) -> bool:
        """phase 1 CAS：`switch_state='switching'` + `leader=target` + `epoch+1`。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            expected_epoch: 预期 epoch（条件更新，防并发覆盖）。
            target: 新 leader。
            from_leader: 切换前 leader。
            from_epoch: 切换前 epoch。

        Returns:
            bool: 更新成功 True；0 行（冲突 / 已推进）False。
        """

    @abstractmethod
    async def finish_switch(
        self,
        *,
        topic_id: int,
        partition_no: int,
        to_epoch: int,
        watermark: int | None,
        result: str,
        truncated_to: int | None,
        operator: str,
    ) -> bool:
        """phase 2 CAS：`switch_state='idle'` + 最近切换结果 + checkpoint。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            to_epoch: 切换后的 epoch。
            watermark: 切换水位（写入 checkpoint_hw；None 时回落到截断位点）。
            result: `truncated` / `retained` / `noop`。
            truncated_to: 截断后的 LEO（None = 未截断）。
            operator: 触发人。

        Returns:
            bool: 更新成功 True；0 行（已 idle / epoch 不符）False。
        """


class LeaderControlTransport(BaseFrameworkObject, ABC):
    """节点控制端口（进程内实现用于用例；02-8 换网络实现）。"""

    @abstractmethod
    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """读目标节点分区运行态（不可达返回 None）。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 运行态快照。
        """

    @abstractmethod
    async def demote(
        self,
        *,
        node_id: str,
        topic_id: int,
        partition_no: int,
        to_leader: str,
        to_epoch: int,
    ) -> None:
        """通知旧 leader 立即降级（不可达时由调用方忽略，靠租约兜底）。

        Args:
            node_id: 旧 leader 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader。
            to_epoch: 新任期。
        """

    @abstractmethod
    async def apply_switch(self, *, node_id: str, command: SwitchApplyCommand) -> SwitchApplyResult:
        """让目标节点截断 / 保留尾部并激活。

        Args:
            node_id: 新 leader 节点标识。
            command: 切换命令。

        Returns:
            SwitchApplyResult: 激活结果。
        """


class InProcessLeaderControlTransport(LeaderControlTransport):
    """进程内控制传输：节点标识 → 复制协调器（用例与同进程多节点）。"""

    def __init__(self) -> None:
        """初始化（无节点）。"""
        self._nodes: ConcurrentStableDict[str, ReplicationCoordinator] = ConcurrentStableDict()

    def register(self, node_id: str, coordinator: ReplicationCoordinator) -> None:
        """登记节点（重复登记即替换）。

        Args:
            node_id: 节点标识。
            coordinator: 该节点的复制协调器。
        """
        self._nodes.set(node_id, coordinator)

    def unregister(self, node_id: str) -> None:
        """移除节点。

        Args:
            node_id: 节点标识。
        """
        self._nodes.pop(node_id, None)

    async def snapshot(self, *, node_id: str, topic_id: int, partition_no: int) -> ReplicationSnapshot | None:
        """读节点分区运行态（未登记 / 异常返回 None）。

        Args:
            node_id: 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。

        Returns:
            ReplicationSnapshot | None: 运行态快照。
        """
        coordinator = self._nodes.get(node_id)
        if coordinator is None:
            return None
        try:
            return await coordinator.snapshot(topic_id=topic_id, partition_no=partition_no)
        except Exception as exc:
            _LOGGER.warning("msg_leader_switch_probe_failed", node_id=node_id, reason=type(exc).__name__)
            return None

    async def demote(
        self,
        *,
        node_id: str,
        topic_id: int,
        partition_no: int,
        to_leader: str,
        to_epoch: int,
    ) -> None:
        """通知旧 leader 降级（未登记即视为不可达）。

        Args:
            node_id: 旧 leader 节点标识。
            topic_id: 主题主键。
            partition_no: 分区号。
            to_leader: 新 leader。
            to_epoch: 新任期。
        """
        coordinator = self._nodes.get(node_id)
        if coordinator is None:
            return
        await coordinator.demote(
            topic_id=topic_id,
            partition_no=partition_no,
            to_leader=to_leader,
            to_epoch=to_epoch,
        )

    async def apply_switch(self, *, node_id: str, command: SwitchApplyCommand) -> SwitchApplyResult:
        """让目标节点激活（未登记即抛错，由服务停在 switching）。

        Args:
            node_id: 新 leader 节点标识。
            command: 切换命令。

        Returns:
            SwitchApplyResult: 激活结果。

        Raises:
            LeaderSwitchError: 目标节点不可达（切换停在 switching）。
        """
        coordinator = self._nodes.get(node_id)
        if coordinator is None:
            raise LeaderSwitchError("target_unreachable", f"目标节点未登记：{node_id}")
        return await coordinator.apply_switch(command)


class LeaderSwitchMetrics(BaseFrameworkObject):
    """切主结果计数（02-8）：`result` 为闭集标签，不按分区 / 节点分键（防基数爆炸）。"""

    def __init__(self) -> None:
        """初始化（全零）。"""
        self._counts: ConcurrentStableDict[str, int] = ConcurrentStableDict()

    def record(self, result: str) -> None:
        """记一次切主结果。

        Args:
            result: `truncated` / `retained` / `noop` / `degraded` / `failed`。
        """
        self._counts.set(result, self._counts.get(result, 0) + 1)

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """取当前计数快照（插入序）。

        Returns:
            ConcurrentStableDict[str, int]: 结果 → 次数。
        """
        return ConcurrentStableDict(self._counts)


class LeaderSwitchService(BaseFrameworkObject):
    """手动切换领域服务（单分区 / 主题批量；首期只被内部流程调用）。"""

    def __init__(
        self,
        *,
        enabled: bool,
        store: LeaderSwitchStore,
        control: LeaderControlTransport,
        audit: AuditEmitter,
        grace_ms: int,
        follower_fresh_ms: int,
        metrics: LeaderSwitchMetrics | None = None,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """初始化。

        Args:
            enabled: 复制是否开启（关闭时拒绝切换，与单实例零变化一致）。
            store: 元数据 CAS 存储。
            control: 节点控制传输。
            audit: 审计出口（begin fail-closed；finished fail-open）。
            grace_ms: 停写宽限（毫秒）。
            follower_fresh_ms: 最后已知 ISR 的新鲜窗口（毫秒；通常取 `lag_time_max_ms`）。
            metrics: 切主结果计数（可选；None 表示不采集）。
            clock: 单调毫秒时钟（用例注入）。
            sleep: 异步等待函数（用例注入零等待）。
        """
        self._enabled = enabled
        self._store = store
        self._control = control
        self._audit = audit
        self._grace_ms = int(grace_ms)
        self._follower_fresh_ms = int(follower_fresh_ms)
        self._metrics = metrics
        self._clock = clock if clock is not None else _monotonic_ms
        self._sleep = sleep if sleep is not None else asyncio.sleep

    async def switch_partition(self, request: SwitchPartitionRequest) -> SwitchResult:
        """切换单个分区的 leader（幂等；可中断后重试）。

        Args:
            request: 切换请求。

        Returns:
            SwitchResult: 切换结果。

        Raises:
            LeaderSwitchError: 预检 / CAS / 控制通道失败（`reason` 区分）。
        """
        try:
            return await self._switch_partition(request)
        except Exception:
            if self._metrics is not None:
                self._metrics.record(LEADER_SWITCH_RESULT_FAILED)
            raise

    async def _switch_partition(self, request: SwitchPartitionRequest) -> SwitchResult:
        started = self._clock()
        state = await self._store.load_partition(topic_id=request.topic_id, partition_no=request.partition_no)
        if state is None:
            raise LeaderSwitchError("partition_not_found", f"分区不存在：{request.topic_id}/{request.partition_no}")
        if not self._enabled:
            raise LeaderSwitchError("replication_disabled", "复制未开启，不执行切换")
        if state.switch_state == SWITCH_STATE_SWITCHING:
            return await self._resume(state=state, request=request, started=started)

        expected = state.epoch if request.expected_epoch is None else int(request.expected_epoch)
        if expected != state.epoch:
            raise LeaderSwitchError("epoch_conflict", f"预期 epoch={expected} 与当前 epoch={state.epoch} 不一致")
        if request.target is not None and request.target == state.leader:
            return self._noop(state=state, started=started)

        old_snapshot = await self._control.snapshot(
            node_id=state.leader, topic_id=state.topic_id, partition_no=state.partition_no
        )
        target = request.target
        if target is None:
            target = await self._pick_target(state=state, old_snapshot=old_snapshot)
        if target not in state.replicas:
            raise LeaderSwitchError("target_not_in_replicas", f"目标 {target!r} 不在副本集 {list(state.replicas)} 内")
        target_snapshot = await self._control.snapshot(
            node_id=target, topic_id=state.topic_id, partition_no=state.partition_no
        )
        if target_snapshot is None:
            raise LeaderSwitchError("target_unreachable", f"目标节点不可达：{target}")

        watermark: int | None
        isr_verified: bool
        truncate: bool
        if old_snapshot is not None:
            isr_verified = target in old_snapshot.isr
            if not isr_verified:
                raise LeaderSwitchError(
                    "target_not_in_isr", f"目标 {target!r} 不在旧 leader 的 ISR {list(old_snapshot.isr)} 内"
                )
            watermark = old_snapshot.hw
            truncate = True
            if target_snapshot.leo < watermark:
                raise LeaderSwitchError(
                    "target_behind_watermark",
                    f"目标 LEO={target_snapshot.leo} 落后于切换水位 {watermark}",
                )
        else:
            fresh = self._clock() - target_snapshot.last_known_at <= self._follower_fresh_ms
            isr_verified = state.leader in target_snapshot.last_known_isr and fresh
            watermark = await self._last_known_watermark(state=state, primary=target_snapshot)
            truncate = False
            if not isr_verified and not request.force:
                raise LeaderSwitchError(
                    "isr_unverified",
                    "旧 leader 不可达且无法验证目标在最后已知 ISR 内（需 force=True 降级继续）",
                )

        to_epoch = state.epoch + 1
        await self._audit_begin(
            state=state,
            request=request,
            target=target,
            to_epoch=to_epoch,
        )
        begun = await self._store.begin_switch(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            expected_epoch=state.epoch,
            target=target,
            from_leader=state.leader,
            from_epoch=state.epoch,
        )
        if not begun:
            current = await self._store.load_partition(topic_id=state.topic_id, partition_no=state.partition_no)
            if current is not None and current.leader == target and current.epoch > state.epoch:
                return self._noop(
                    state=current,
                    started=started,
                    from_leader=state.leader,
                    from_epoch=state.epoch,
                    isr_verified=isr_verified,
                )
            raise LeaderSwitchError("switch_conflict", "并发切换已推进 epoch 或更换目标")
        return await self._finish(
            state=state,
            request=request,
            from_leader=state.leader,
            from_epoch=state.epoch,
            target=target,
            to_epoch=to_epoch,
            watermark=watermark,
            truncate=truncate,
            isr_verified=isr_verified,
            started=started,
        )

    async def switch_topic(self, request: SwitchTopicRequest) -> SwitchBatchResult:
        """切换主题全部分区（逐分区串行；单分区失败不影响其他分区）。

        Args:
            request: 批量切换请求。

        Returns:
            SwitchBatchResult: 成功项与失败项（不回滚已成功分区）。
        """
        partitions = await self._store.load_topic_partitions(topic_id=request.topic_id)
        items: ConcurrentStableList[SwitchResult] = ConcurrentStableList()
        failures: ConcurrentStableList[SwitchFailure] = ConcurrentStableList()
        for partition in partitions:
            target = request.targets.get(partition.partition_no) if request.targets is not None else None
            expected = (
                request.expected_epochs.get(partition.partition_no) if request.expected_epochs is not None else None
            )
            try:
                result = await self.switch_partition(
                    SwitchPartitionRequest(
                        topic_id=request.topic_id,
                        partition_no=partition.partition_no,
                        target=target,
                        expected_epoch=expected,
                        force=request.force,
                        operator=request.operator,
                        reason=request.reason,
                    )
                )
                items.add(result)
            except LeaderSwitchError as exc:
                failures.add(
                    SwitchFailure(
                        topic_id=request.topic_id,
                        partition_no=partition.partition_no,
                        reason=exc.reason,
                        detail=exc.detail,
                    )
                )
            except Exception as exc:  # pragma: no cover - 非预期异常也要落进失败项，不能中断整批
                failures.add(
                    SwitchFailure(
                        topic_id=request.topic_id,
                        partition_no=partition.partition_no,
                        reason=type(exc).__name__,
                        detail=str(exc),
                    )
                )
        return SwitchBatchResult(
            items=ConcurrentStableFrozenList(items),
            failures=ConcurrentStableFrozenList(failures),
        )

    async def _resume(
        self,
        *,
        state: SwitchPartitionState,
        request: SwitchPartitionRequest,
        started: float,
    ) -> SwitchResult:
        """从中断的 switching 状态续跑（同 target / 同 from_epoch）。

        Args:
            state: 当前分区状态（switching）。
            request: 重试请求。
            started: 本次服务入口时刻。

        Returns:
            SwitchResult: 切换结果。

        Raises:
            LeaderSwitchError: 目标不同 / 预期 epoch 不符 / 控制通道失败。
        """
        if request.target is not None and request.target != state.leader:
            raise LeaderSwitchError(
                "switch_in_progress", f"分区切换中，目标已是 {state.leader}，不允许改为 {request.target}"
            )
        from_epoch = state.switch_from_epoch if state.switch_from_epoch is not None else max(state.epoch - 1, 0)
        if request.expected_epoch is not None and int(request.expected_epoch) not in (from_epoch, state.epoch):
            raise LeaderSwitchError(
                "epoch_conflict", f"预期 epoch={request.expected_epoch} 与切换前 epoch={from_epoch} 不一致"
            )
        from_leader = state.switch_from_leader or state.leader
        old_snapshot = await self._control.snapshot(
            node_id=from_leader, topic_id=state.topic_id, partition_no=state.partition_no
        )
        target_snapshot = await self._control.snapshot(
            node_id=state.leader, topic_id=state.topic_id, partition_no=state.partition_no
        )
        if old_snapshot is not None:
            watermark: int | None = old_snapshot.hw
            truncate = True
            isr_verified = state.leader in old_snapshot.isr
        else:
            watermark = (
                target_snapshot.last_known_hw if target_snapshot is not None and target_snapshot.last_known_hw else None
            )
            truncate = False
            isr_verified = bool(target_snapshot is not None and from_leader in target_snapshot.last_known_isr)
        return await self._finish(
            state=state,
            request=request,
            from_leader=from_leader,
            from_epoch=from_epoch,
            target=state.leader,
            to_epoch=state.epoch,
            watermark=watermark,
            truncate=truncate,
            isr_verified=isr_verified,
            started=started,
        )

    async def _finish(
        self,
        *,
        state: SwitchPartitionState,
        request: SwitchPartitionRequest,
        from_leader: str,
        from_epoch: int,
        target: str,
        to_epoch: int,
        watermark: int | None,
        truncate: bool,
        isr_verified: bool,
        started: float,
    ) -> SwitchResult:
        """等待旧租约 → 目标激活 → phase 2 收口 → 完成审计。

        Args:
            state: 切换前分区状态。
            request: 切换请求。
            from_leader: 切换前 leader。
            from_epoch: 切换前 epoch。
            target: 新 leader。
            to_epoch: 新 epoch。
            watermark: 切换水位。
            truncate: 是否按水位截断。
            isr_verified: 是否已验证 ISR。
            started: 服务入口时刻。

        Returns:
            SwitchResult: 切换结果。

        Raises:
            LeaderSwitchError: 控制通道 / phase 2 失败（DB 停在 switching，可重试）。
        """
        try:
            await self._control.demote(
                node_id=from_leader,
                topic_id=state.topic_id,
                partition_no=state.partition_no,
                to_leader=target,
                to_epoch=to_epoch,
            )
        except Exception as exc:
            _LOGGER.warning(
                "msg_leader_switch_demote_failed",
                topic_id=state.topic_id,
                partition=state.partition_no,
                reason=type(exc).__name__,
            )
        if self._grace_ms > 0:
            await self._sleep(self._grace_ms / MS_PER_SECOND)
        command = SwitchApplyCommand(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            from_leader=from_leader,
            to_leader=target,
            to_epoch=to_epoch,
            watermark=watermark,
            truncate=truncate,
            grace_ms=self._grace_ms,
        )
        try:
            applied = await self._control.apply_switch(node_id=target, command=command)
        except Exception as exc:
            raise LeaderSwitchError("apply_failed", f"目标节点激活失败：{type(exc).__name__}: {exc}") from exc
        if applied.truncated_to is not None:
            result_text = SWITCH_RESULT_TRUNCATED
        elif applied.retained_tail > 0:
            result_text = SWITCH_RESULT_RETAINED
        else:
            result_text = SWITCH_RESULT_NOOP
        if self._metrics is not None:
            self._metrics.record(result_text if isr_verified else LEADER_SWITCH_RESULT_DEGRADED)
        effective_watermark = watermark if watermark is not None else applied.to_leo
        finished = await self._store.finish_switch(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            to_epoch=to_epoch,
            watermark=effective_watermark,
            result=result_text,
            truncated_to=applied.truncated_to,
            operator=request.operator,
        )
        if not finished:
            current = await self._store.load_partition(topic_id=state.topic_id, partition_no=state.partition_no)
            if current is None or current.switch_state != SWITCH_STATE_IDLE or current.epoch != to_epoch:
                raise LeaderSwitchError("finish_failed", "phase 2 收口失败（分区仍停在 switching，可重试）")
        duration_ms = int(self._clock() - started)
        result = SwitchResult(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            from_leader=from_leader,
            to_leader=target,
            from_epoch=from_epoch,
            to_epoch=to_epoch,
            applied=True,
            watermark=watermark,
            truncated_to=applied.truncated_to,
            retained_tail=applied.retained_tail,
            isr_verified=isr_verified,
            duration_ms=duration_ms,
        )
        _LOGGER.info(
            "msg_leader_switch_finished",
            topic_id=result.topic_id,
            partition=result.partition_no,
            from_leader=result.from_leader,
            to_leader=result.to_leader,
            from_epoch=result.from_epoch,
            to_epoch=result.to_epoch,
            watermark=result.watermark,
            truncated_to=result.truncated_to,
            retained_tail=result.retained_tail,
            duration_ms=result.duration_ms,
            isr_verified=result.isr_verified,
        )
        await self._audit_finished(state=state, request=request, result=result, result_text=result_text)
        return result

    async def _pick_target(self, *, state: SwitchPartitionState, old_snapshot: ReplicationSnapshot | None) -> str:
        """在 ISR（或最后已知 ISR）内选 LEO 最大者；并列按副本集顺序。

        Args:
            state: 分区状态。
            old_snapshot: 旧 leader 运行态（None 表示不可达）。

        Returns:
            str: 目标节点标识。

        Raises:
            LeaderSwitchError: 没有任何可验证的候选。
        """
        candidates: ConcurrentStableList[str] = ConcurrentStableList()
        if old_snapshot is not None:
            candidates.extend(old_snapshot.isr)
        else:
            for replica in state.replicas:
                snapshot = await self._control.snapshot(
                    node_id=replica, topic_id=state.topic_id, partition_no=state.partition_no
                )
                if snapshot is not None and state.leader in snapshot.last_known_isr:
                    candidates.add(replica)
        best: str | None = None
        best_leo = -1
        for replica in state.replicas:
            if replica not in candidates or replica == state.leader:
                continue
            snapshot = await self._control.snapshot(
                node_id=replica, topic_id=state.topic_id, partition_no=state.partition_no
            )
            leo = snapshot.leo if snapshot is not None else -1
            if leo > best_leo:
                best = replica
                best_leo = leo
        if best is None:
            raise LeaderSwitchError("no_candidate", "ISR 内没有可选的候选 leader")
        return best

    async def _last_known_watermark(self, *, state: SwitchPartitionState, primary: ReplicationSnapshot) -> int | None:
        """取「最后已知 HW」的最大值（旧 leader 不可达时用于记录，不用于截断）。

        Args:
            state: 分区状态。
            primary: 目标节点快照。

        Returns:
            int | None: 最大最后已知 HW；全为 0 时 None。
        """
        watermark = primary.last_known_hw
        for replica in state.replicas:
            if replica == primary.leader:
                continue
            snapshot = await self._control.snapshot(
                node_id=replica, topic_id=state.topic_id, partition_no=state.partition_no
            )
            if snapshot is not None:
                watermark = max(watermark, snapshot.last_known_hw)
        return watermark or None

    def _noop(
        self,
        *,
        state: SwitchPartitionState,
        started: float,
        from_leader: str | None = None,
        from_epoch: int | None = None,
        isr_verified: bool = True,
    ) -> SwitchResult:
        """构造幂等 no-op 结果（已是目标 leader，不重复递增 epoch）。

        Args:
            state: 当前分区状态。
            started: 服务入口时刻。
            from_leader: 切换前 leader（缺省同当前 leader）。
            from_epoch: 切换前 epoch（缺省同当前 epoch）。
            isr_verified: 是否已验证 ISR。

        Returns:
            SwitchResult: no-op 结果。
        """
        if self._metrics is not None:
            self._metrics.record(SWITCH_RESULT_NOOP)
        return SwitchResult(
            topic_id=state.topic_id,
            partition_no=state.partition_no,
            from_leader=from_leader if from_leader is not None else state.leader,
            to_leader=state.leader,
            from_epoch=from_epoch if from_epoch is not None else state.epoch,
            to_epoch=state.epoch,
            applied=False,
            watermark=None,
            truncated_to=None,
            retained_tail=0,
            isr_verified=isr_verified,
            duration_ms=int(self._clock() - started),
        )

    async def _audit_begin(
        self,
        *,
        state: SwitchPartitionState,
        request: SwitchPartitionRequest,
        target: str,
        to_epoch: int,
    ) -> None:
        """写 phase=begin 审计（强审计，失败即拒绝切换）。

        Args:
            state: 切换前分区状态。
            request: 切换请求。
            target: 新 leader。
            to_epoch: 新 epoch。

        Raises:
            InternalError: 审计写入失败（没留痕不生效）。
        """
        await emit_sensitive(
            self._audit,
            action=LEADER_SWITCH_ACTION,
            tenant_id=state.tenant_id,
            params=ConcurrentStableDict(
                {
                    "phase": "begin",
                    "topic_id": str(state.topic_id),
                    "partition": state.partition_no,
                    "from_leader": state.leader,
                    "to_leader": target,
                    "from_epoch": state.epoch,
                    "to_epoch": to_epoch,
                    "operator": request.operator,
                    "reason": request.reason,
                }
            ),
            method="POST",
            path=_SWITCH_AUDIT_PATH,
            module_key=state.namespace,
            resource_key=state.topic,
        )
        _LOGGER.info(
            "msg_leader_switch_begin",
            topic_id=state.topic_id,
            partition=state.partition_no,
            from_leader=state.leader,
            to_leader=target,
            from_epoch=state.epoch,
            to_epoch=to_epoch,
            operator=request.operator,
        )

    async def _audit_finished(
        self,
        *,
        state: SwitchPartitionState,
        request: SwitchPartitionRequest,
        result: SwitchResult,
        result_text: str,
    ) -> None:
        """写 phase=finished 审计（fail-open：失败只记错误日志，不回滚切换）。

        Args:
            state: 切换前分区状态。
            request: 切换请求。
            result: 切换结果。
            result_text: `truncated` / `retained` / `noop`。
        """
        try:
            await emit_sensitive(
                self._audit,
                action=LEADER_SWITCH_ACTION,
                tenant_id=state.tenant_id,
                params=ConcurrentStableDict(
                    {
                        "phase": "finished",
                        "topic_id": str(result.topic_id),
                        "partition": result.partition_no,
                        "from_leader": result.from_leader,
                        "to_leader": result.to_leader,
                        "from_epoch": result.from_epoch,
                        "to_epoch": result.to_epoch,
                        "watermark": result.watermark,
                        "truncated_to": result.truncated_to,
                        "retained_tail": result.retained_tail,
                        "isr_verified": result.isr_verified,
                        "duration_ms": result.duration_ms,
                        "operator": request.operator,
                        "reason": request.reason,
                        "result": result_text,
                    }
                ),
                method="POST",
                path=_SWITCH_AUDIT_PATH,
                module_key=state.namespace,
                resource_key=state.topic,
            )
        except Exception as exc:
            _LOGGER.error(
                "msg_leader_switch_audit_failed",
                phase="finished",
                action=LEADER_SWITCH_ACTION,
                error=repr(exc),
            )


def _monotonic_ms() -> float:
    """单调毫秒时钟。

    Returns:
        float: 单调递增毫秒数。
    """
    import time

    return time.monotonic() * MS_PER_SECOND
