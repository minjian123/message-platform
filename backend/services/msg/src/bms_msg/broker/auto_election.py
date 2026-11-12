"""自动选举（任务 02-9）：leader 失效后自动发起 02-5 的两阶段切主。

**职责边界**：本模块只做「判活 + 何时发起」——探活控制通道、维护每分区的失败计数 / 退避 / 冷却、
调 `LeaderSwitchService.switch_partition`；**不做**切主（两阶段、候选选择、尾部截断、审计全在 02-5），
**不做**客户端改投（SDK 归 8B_03），**不做**心跳表与多数派投票（02-9 定稿：共识点只有 `msg_partition`）。

**四条硬口径**：

1. **判活不看数据库**：DB 抖动 / 慢查询不得被解释成 leader 失效；只有判死之后才读一次元数据，
   读不到即本轮不动作（没有权威 `epoch` 就无法证明自己不是旧 leader）。
2. **永不降级**：`force` 硬编码 `False`；无法验证「候选在最后已知 ISR 内且视图新鲜」时停在 `blocked` 交人工。
3. **不放大故障**：候选不可达 / 无候选 / ISR 不可验证都在 `begin_switch` 之前失败，
   DB 不会被改成 `switching`，写路径不受影响。
4. **不失控**：尝试上限 + 指数退避 + 跨节点冷却（`msg_partition.last_switch_at`）+ 单轮并发上限 +
   按死 leader 后继副本序错峰；不做自动回切。
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenList, ConcurrentStableList
from bms_core.core.logging import get_logger
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

from bms_msg.broker.election import (
    SWITCH_STATE_IDLE,
    SWITCH_STATE_SWITCHING,
    LeaderControlTransport,
    LeaderSwitchError,
    LeaderSwitchService,
    LeaderSwitchStore,
    SwitchPartitionRequest,
    SwitchPartitionState,
)
from bms_msg.broker.replication import ReplicaKey, ReplicationCoordinator
from bms_msg.core.config import (
    AUTO_ELECTION_BACKOFF_BASE_MS_ENV,
    AUTO_ELECTION_BACKOFF_CAP_MS_ENV,
    AUTO_ELECTION_CONCURRENCY_ENV,
    AUTO_ELECTION_COOLDOWN_MS_ENV,
    AUTO_ELECTION_DETECT_INTERVAL_MS_ENV,
    AUTO_ELECTION_ENABLED_ENV,
    AUTO_ELECTION_FAILURE_THRESHOLD_ENV,
    AUTO_ELECTION_MAX_ATTEMPTS_ENV,
    AUTO_ELECTION_PROBE_TIMEOUT_MS_ENV,
    AUTO_ELECTION_STAGGER_MS_ENV,
    DEFAULT_AUTO_ELECTION_BACKOFF_BASE_MS,
    DEFAULT_AUTO_ELECTION_BACKOFF_CAP_MS,
    DEFAULT_AUTO_ELECTION_CONCURRENCY,
    DEFAULT_AUTO_ELECTION_COOLDOWN_MS,
    DEFAULT_AUTO_ELECTION_DETECT_INTERVAL_MS,
    DEFAULT_AUTO_ELECTION_ENABLED,
    DEFAULT_AUTO_ELECTION_FAILURE_THRESHOLD,
    DEFAULT_AUTO_ELECTION_MAX_ATTEMPTS,
    DEFAULT_AUTO_ELECTION_PROBE_TIMEOUT_MS,
    DEFAULT_AUTO_ELECTION_STAGGER_MS,
    MS_PER_SECOND,
)

__all__ = [
    "AUTO_ELECTION_OPERATOR",
    "AUTO_ELECTION_RESULT_BLOCKED",
    "AUTO_ELECTION_RESULT_CONFLICT",
    "AUTO_ELECTION_RESULT_EXHAUSTED",
    "AUTO_ELECTION_RESULT_SUCCEEDED",
    "AUTO_ELECTION_RESULT_TRIGGERED",
    "AutoElectionConfig",
    "AutoElectionMetrics",
    "AutoElectionTracker",
    "AutoElectionWatchdog",
]

_LOGGER = get_logger("bms_msg.auto_election")

AUTO_ELECTION_OPERATOR = "system:auto-election"
"""自动选举的触发人标识（写进审计 `operator` 与 `msg_partition.last_switch_by`）。"""

AUTO_ELECTION_RESULT_TRIGGERED = "triggered"
"""结果：发起了一次切主（无论成败）。"""

AUTO_ELECTION_RESULT_SUCCEEDED = "succeeded"
"""结果：条件更新成功且两阶段收口完成。"""

AUTO_ELECTION_RESULT_BLOCKED = "blocked"
"""结果：预检 / 激活 / 冷却把本次发起挡住（细分原因见日志）。"""

AUTO_ELECTION_RESULT_CONFLICT = "conflict"
"""结果：他人已推进（CAS 落败或已是目标 leader 的 no-op）。"""

AUTO_ELECTION_RESULT_EXHAUSTED = "exhausted"
"""结果：一轮内尝试次数达上限，进入冷却并交人工。"""

_CONFLICT_REASONS = ("switch_conflict", "epoch_conflict", "switch_in_progress")
"""视为「别人赢了」的切换错误原因：清零、不计退避。"""

_GONE_REASONS = ("partition_not_found", "replication_disabled")
"""装配 / 数据已变的错误原因：不再跟踪该分区。"""

_RESUME_REASONS = ("apply_failed", "finish_failed")
"""DB 已停在 `switching` 的失败：保留 `pending_epoch` 以便续跑。"""

_SUSPECT_LOG_WINDOW_MS = 60_000
"""判死告警的日志抑制窗口（毫秒）：同一分区 60s 内只记一条。"""


@dataclass(frozen=True)
class AutoElectionConfig(BaseValueObject):
    """自动选举配置（环境变量 > 常量缺省；关闭时只保留数值校验）。"""

    enabled: bool = DEFAULT_AUTO_ELECTION_ENABLED
    """总开关：缺省关闭，复制开启也需显式开启（灰度期可一键回退）。"""

    detect_interval_ms: int = DEFAULT_AUTO_ELECTION_DETECT_INTERVAL_MS
    """判活节拍（毫秒）。"""

    probe_timeout_ms: int = DEFAULT_AUTO_ELECTION_PROBE_TIMEOUT_MS
    """单次探活超时（毫秒）：须小于节拍。"""

    failure_threshold: int = DEFAULT_AUTO_ELECTION_FAILURE_THRESHOLD
    """连续失败判死阈值（次）。"""

    max_attempts: int = DEFAULT_AUTO_ELECTION_MAX_ATTEMPTS
    """一轮内发起次数上限（次）。"""

    backoff_base_ms: int = DEFAULT_AUTO_ELECTION_BACKOFF_BASE_MS
    """失败退避基数（毫秒）。"""

    backoff_cap_ms: int = DEFAULT_AUTO_ELECTION_BACKOFF_CAP_MS
    """失败退避上限（毫秒）。"""

    cooldown_ms: int = DEFAULT_AUTO_ELECTION_COOLDOWN_MS
    """成功切换后的冷却（毫秒；跨节点按 `last_switch_at` 判定）。"""

    concurrency: int = DEFAULT_AUTO_ELECTION_CONCURRENCY
    """单轮并发上限（探活与发起同时最多多少个分区在飞）。"""

    stagger_ms: int = DEFAULT_AUTO_ELECTION_STAGGER_MS
    """按副本序错峰的步长（毫秒）。"""

    @classmethod
    def from_env(cls) -> AutoElectionConfig:
        """从环境变量构造配置。

        Returns:
            AutoElectionConfig: 未校验的配置（`validate` 需要复制侧宽限与新鲜窗口）。
        """
        return cls(
            enabled=_env_bool(AUTO_ELECTION_ENABLED_ENV, DEFAULT_AUTO_ELECTION_ENABLED),
            detect_interval_ms=_env_int(AUTO_ELECTION_DETECT_INTERVAL_MS_ENV, DEFAULT_AUTO_ELECTION_DETECT_INTERVAL_MS),
            probe_timeout_ms=_env_int(AUTO_ELECTION_PROBE_TIMEOUT_MS_ENV, DEFAULT_AUTO_ELECTION_PROBE_TIMEOUT_MS),
            failure_threshold=_env_int(AUTO_ELECTION_FAILURE_THRESHOLD_ENV, DEFAULT_AUTO_ELECTION_FAILURE_THRESHOLD),
            max_attempts=_env_int(AUTO_ELECTION_MAX_ATTEMPTS_ENV, DEFAULT_AUTO_ELECTION_MAX_ATTEMPTS),
            backoff_base_ms=_env_int(AUTO_ELECTION_BACKOFF_BASE_MS_ENV, DEFAULT_AUTO_ELECTION_BACKOFF_BASE_MS),
            backoff_cap_ms=_env_int(AUTO_ELECTION_BACKOFF_CAP_MS_ENV, DEFAULT_AUTO_ELECTION_BACKOFF_CAP_MS),
            cooldown_ms=_env_int(AUTO_ELECTION_COOLDOWN_MS_ENV, DEFAULT_AUTO_ELECTION_COOLDOWN_MS),
            concurrency=_env_int(AUTO_ELECTION_CONCURRENCY_ENV, DEFAULT_AUTO_ELECTION_CONCURRENCY),
            stagger_ms=_env_int(AUTO_ELECTION_STAGGER_MS_ENV, DEFAULT_AUTO_ELECTION_STAGGER_MS),
        )

    def validate(self, *, switch_grace_ms: int, freshness_ms: int) -> None:
        """校验配置组合（开启时额外校验「判死 + 宽限 ≤ 视图新鲜窗口」）。

        Args:
            switch_grace_ms: 02-5 的停写宽限（毫秒），用于约束冷却下限与预算上限。
            freshness_ms: 最后已知 ISR 的新鲜窗口（毫秒；通常取 `lag_time_max_ms`）。

        Raises:
            ValueError: 任一约束不满足。
        """
        for name, value in (
            ("detect_interval_ms", self.detect_interval_ms),
            ("probe_timeout_ms", self.probe_timeout_ms),
            ("failure_threshold", self.failure_threshold),
            ("max_attempts", self.max_attempts),
            ("backoff_base_ms", self.backoff_base_ms),
            ("backoff_cap_ms", self.backoff_cap_ms),
            ("cooldown_ms", self.cooldown_ms),
            ("concurrency", self.concurrency),
            ("stagger_ms", self.stagger_ms),
        ):
            if value <= 0:
                raise ValueError(f"自动选举配置非法：{name} 必须为正（当前 {value}）")
        if self.probe_timeout_ms >= self.detect_interval_ms:
            raise ValueError(
                f"自动选举配置非法：probe_timeout_ms={self.probe_timeout_ms} 不小于 "
                f"detect_interval_ms={self.detect_interval_ms}（单次探活跨节拍会让失败计数抖动）"
            )
        if self.backoff_base_ms > self.backoff_cap_ms:
            raise ValueError(
                f"自动选举配置非法：backoff_base_ms={self.backoff_base_ms} 大于 backoff_cap_ms={self.backoff_cap_ms}"
            )
        if self.cooldown_ms < switch_grace_ms:
            raise ValueError(
                f"自动选举配置非法：cooldown_ms={self.cooldown_ms} 小于 switch_grace_ms={switch_grace_ms}"
                "（冷却比切换窗口还短，等于没有冷却）"
            )
        if not self.enabled:
            return
        budget = self.detect_interval_ms * self.failure_threshold + switch_grace_ms
        if budget > freshness_ms:
            raise ValueError(
                f"自动选举配置非法：判死 + 宽限预算 {budget}ms 超过视图新鲜窗口 {freshness_ms}ms，"
                "自动路径会稳定失败在 isr_unverified（调小节拍 / 阈值或调大 MSG_REPLICATION_LAG_TIME_MS）"
            )


class AutoElectionTracker:
    """单个分区的自动选举跟踪态（进程内；不跨节点共享、不落盘）。"""

    def __init__(self) -> None:
        """初始化（观察中：无失败、无退避、无冷却）。"""
        self.failures = 0
        """连续探活失败次数。"""

        self.attempts = 0
        """当前一轮内的发起次数。"""

        self.next_attempt_at = 0.0
        """允许下一次发起的单调毫秒（退避）。"""

        self.cooldown_until = 0.0
        """冷却截止的单调毫秒（成功切换后设置，`reset` 不清）。"""

        self.pending_epoch = 0
        """本进程上一手发起的切换目标 epoch（0 = 无；用于停在 `switching` 时续跑）。"""

        self.cooldown_reported = False
        """冷却期内的 `blocked` 是否已记过（防每轮刷计数）。"""

        self.exhausted_reported = False
        """`exhausted` 是否已记过。"""

        self.suspect_logged_at: float | None = None
        """判死告警的最近记录时刻（单调毫秒；60s 窗口抑制）。"""

    def reset(self) -> None:
        """清零一轮的跟踪态（保留冷却截止，避免成功后立刻再切）。"""
        self.failures = 0
        self.attempts = 0
        self.next_attempt_at = 0.0
        self.pending_epoch = 0
        self.cooldown_reported = False
        self.exhausted_reported = False


class AutoElectionMetrics(BaseFrameworkObject):
    """自动选举结果计数（`result` 为闭集标签，不按分区 / 节点分键，防基数爆炸）。"""

    def __init__(self) -> None:
        """初始化（全零）。"""
        self._counts: ConcurrentStableDict[str, int] = ConcurrentStableDict()

    def record(self, result: str) -> None:
        """记一次结果。

        Args:
            result: `triggered` / `succeeded` / `blocked` / `conflict` / `exhausted`。
        """
        self._counts.set(result, self._counts.get(result, 0) + 1)

    def snapshot(self) -> ConcurrentStableDict[str, int]:
        """取当前计数快照（插入序）。

        Returns:
            ConcurrentStableDict[str, int]: 结果 → 次数。
        """
        return ConcurrentStableDict(self._counts)


class AutoElectionWatchdog(BaseFrameworkObject):
    """自动选举看护循环：判活 → 读权威 → 抢主 → 退避 / 冷却（每分区一份跟踪态）。"""

    def __init__(
        self,
        *,
        config: AutoElectionConfig,
        node_id: str,
        replication: ReplicationCoordinator,
        store: LeaderSwitchStore,
        control: LeaderControlTransport,
        switch: LeaderSwitchService,
        metrics: AutoElectionMetrics | None = None,
        clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        """初始化（不启动循环）。

        Args:
            config: 已校验的自动选举配置。
            node_id: 本节点标识（与 `ReplicaConfig.node_id` 一致）。
            replication: 复制协调器（只读视图：已挂载分区与本地快照）。
            store: 切换元数据存储（读 `leader` / `epoch` / `switch_state` / `last_switch_at`）。
            control: 节点控制传输（探活；与切换服务同一通道）。
            switch: 02-5 切换服务（唯一执行体，本类只作触发方）。
            metrics: 结果计数（可选；None 表示不采集）。
            clock: 单调毫秒时钟（用例注入）。
            wall_clock: 墙钟（用于与 `last_switch_at` 比较冷却；用例注入）。
            sleep: 异步等待函数（用例注入零等待）。
        """
        self._config = config
        self._node_id = node_id
        self._replication = replication
        self._store = store
        self._control = control
        self._switch = switch
        self._metrics = metrics
        self._clock = clock if clock is not None else _monotonic_ms
        self._wall_clock = wall_clock if wall_clock is not None else _now_utc
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._trackers: ConcurrentStableDict[ReplicaKey, AutoElectionTracker] = ConcurrentStableDict()
        self._tasks: ConcurrentStableList[asyncio.Task[None]] = ConcurrentStableList()
        self._stopped = False

    @property
    def running(self) -> bool:
        """看护循环是否在跑（排障与用例断言）。

        Returns:
            bool: 有存活任务即为 True。
        """
        return bool(self._tasks)

    def ensure_started(self) -> None:
        """启动看护循环（幂等；关闭 / 已停 / 已启动时不重复启动）。"""
        if not self._config.enabled or self._stopped or self._tasks:
            return
        self._tasks.add(asyncio.create_task(self._loop()))

    async def stop(self) -> None:
        """停止看护循环（幂等；取消并等待任务退出）。"""
        self._stopped = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

    async def run_once(self) -> None:
        """跑一轮判活与抢主（用例可直接驱动，不依赖后台循环）。"""
        if not self._config.enabled:
            return
        keys = self._replication.mounted_partition_keys()
        if not keys:
            return
        semaphore = asyncio.Semaphore(self._config.concurrency)
        await asyncio.gather(*(self._inspect_guarded(key=key, semaphore=semaphore) for key in keys))

    async def _inspect_guarded(self, *, key: ReplicaKey, semaphore: asyncio.Semaphore) -> None:
        """带并发闸门与单分区异常隔离地检查一个分区。

        Args:
            key: 分区键。
            semaphore: 单轮并发闸门。
        """
        async with semaphore:
            try:
                await self._inspect(key=key)
            except Exception as exc:
                _LOGGER.warning(
                    "msg_auto_election_failed",
                    topic_id=key[0],
                    partition=key[1],
                    reason=type(exc).__name__,
                    detail=str(exc),
                )

    async def _inspect(self, *, key: ReplicaKey) -> None:
        """判活一个分区，必要时发起切换。

        Args:
            key: 分区键。
        """
        view = await self._replication.snapshot(topic_id=key[0], partition_no=key[1])
        if view is None:
            self._trackers.pop(key, None)
            return
        tracker = self._tracker(key)
        if view.leader == self._node_id:
            tracker.reset()
            return
        if await self._probe(node_id=view.leader, key=key):
            tracker.reset()
            return
        tracker.failures += 1
        if tracker.failures < self._config.failure_threshold:
            return
        if tracker.exhausted_reported:
            # 已达上限：进入冷却后不再自动重来（只有探活成功或 leader 变更才清零）。
            return
        if tracker.failures == self._config.failure_threshold:
            self._report_suspect(tracker=tracker, key=key, leader=view.leader)
        now = self._clock()
        if now < tracker.next_attempt_at:
            return
        if now < tracker.cooldown_until:
            self._report_cooldown(tracker=tracker, key=key)
            return
        if tracker.attempts >= self._config.max_attempts:
            self._report_exhausted(tracker=tracker, key=key, now=now)
            return
        await self._claim_if_eligible(key=key, local_leader=view.leader, tracker=tracker, now=now)

    async def _claim_if_eligible(
        self,
        *,
        key: ReplicaKey,
        local_leader: str,
        tracker: AutoElectionTracker,
        now: float,
    ) -> None:
        """读元数据权威并决定是否由本节点发起抢主。

        Args:
            key: 分区键。
            local_leader: 本地视图里的 leader（用于识别「DB 已换人」）。
            tracker: 分区跟踪态。
            now: 当前单调毫秒。
        """
        try:
            partition = await self._store.load_partition(topic_id=key[0], partition_no=key[1])
        except Exception as exc:
            _LOGGER.warning(
                "msg_auto_election_metadata_failed",
                topic_id=key[0],
                partition=key[1],
                reason=type(exc).__name__,
            )
            return
        if partition is None:
            self._trackers.pop(key, None)
            return
        if partition.leader == self._node_id or partition.leader != local_leader:
            tracker.reset()
            return
        resume = partition.switch_state == SWITCH_STATE_SWITCHING and tracker.pending_epoch == partition.epoch
        if partition.switch_state != SWITCH_STATE_IDLE and not resume:
            return
        if self._switched_recently(partition):
            self._report_cooldown(tracker=tracker, key=key)
            return
        if self._node_id not in partition.replicas:
            tracker.reset()
            return
        if not await self._is_first_reachable(partition=partition, key=key, tracker=tracker):
            return
        await self._claim(partition=partition, tracker=tracker, now=now)

    async def _claim(self, *, partition: SwitchPartitionState, tracker: AutoElectionTracker, now: float) -> None:
        """调 02-5 切换服务发起一次抢主，并按结果分类计数。

        Args:
            partition: 元数据权威快照。
            tracker: 分区跟踪态。
            now: 当前单调毫秒。
        """
        expected = int(partition.epoch)
        tracker.attempts += 1
        tracker.pending_epoch = expected + 1
        tracker.next_attempt_at = 0.0
        self._record(AUTO_ELECTION_RESULT_TRIGGERED)
        _LOGGER.info(
            "msg_auto_election_triggered",
            topic_id=partition.topic_id,
            partition=partition.partition_no,
            from_leader=partition.leader,
            proposer=self._node_id,
            epoch=expected,
            failures=tracker.failures,
        )
        request = SwitchPartitionRequest(
            topic_id=partition.topic_id,
            partition_no=partition.partition_no,
            target=None,
            expected_epoch=expected,
            force=False,
            operator=AUTO_ELECTION_OPERATOR,
            reason=f"leader_unreachable:{tracker.failures}",
        )
        try:
            result = await self._switch.switch_partition(request)
        except LeaderSwitchError as exc:
            self._on_switch_error(exc=exc, tracker=tracker, key=(partition.topic_id, partition.partition_no), now=now)
            return
        if not result.applied:
            self._record(AUTO_ELECTION_RESULT_CONFLICT)
            _LOGGER.info(
                "msg_auto_election_conflict",
                topic_id=partition.topic_id,
                partition=partition.partition_no,
                reason="already_switched",
            )
            tracker.reset()
            return
        self._record(AUTO_ELECTION_RESULT_SUCCEEDED)
        tracker.reset()
        tracker.cooldown_until = self._clock() + self._config.cooldown_ms
        _LOGGER.info(
            "msg_auto_election_succeeded",
            topic_id=result.topic_id,
            partition=result.partition_no,
            from_leader=result.from_leader,
            to_leader=result.to_leader,
            from_epoch=result.from_epoch,
            to_epoch=result.to_epoch,
            retained_tail=result.retained_tail,
            isr_verified=result.isr_verified,
            duration_ms=result.duration_ms,
        )

    def _on_switch_error(
        self,
        *,
        exc: LeaderSwitchError,
        tracker: AutoElectionTracker,
        key: ReplicaKey,
        now: float,
    ) -> None:
        """按错误原因分类：他人已赢 / 数据已变 / 可续跑 / 预检拒绝。

        Args:
            exc: 切换错误。
            tracker: 分区跟踪态。
            key: 分区键。
            now: 当前单调毫秒。
        """
        reason = exc.reason
        if reason in _CONFLICT_REASONS:
            self._record(AUTO_ELECTION_RESULT_CONFLICT)
            _LOGGER.info("msg_auto_election_conflict", topic_id=key[0], partition=key[1], reason=reason)
            tracker.reset()
            return
        if reason in _GONE_REASONS:
            _LOGGER.info("msg_auto_election_gone", topic_id=key[0], partition=key[1], reason=reason)
            tracker.reset()
            self._trackers.pop(key, None)
            return
        self._record(AUTO_ELECTION_RESULT_BLOCKED)
        if reason in _RESUME_REASONS:
            _LOGGER.warning(
                "msg_auto_election_blocked",
                topic_id=key[0],
                partition=key[1],
                reason=reason,
                attempts=tracker.attempts,
                detail=exc.detail,
            )
            tracker.next_attempt_at = now + self._backoff_ms(tracker)
            return
        tracker.pending_epoch = 0
        tracker.next_attempt_at = now + self._backoff_ms(tracker)
        _LOGGER.warning(
            "msg_auto_election_blocked",
            topic_id=key[0],
            partition=key[1],
            reason=reason,
            attempts=tracker.attempts,
            detail=exc.detail,
        )

    def _backoff_ms(self, tracker: AutoElectionTracker) -> float:
        """按已尝试次数计算指数退避（封顶）。

        Args:
            tracker: 分区跟踪态。

        Returns:
            float: 本次退避毫秒。
        """
        exponent = max(tracker.attempts - 1, 0)
        return float(min(self._config.backoff_base_ms * (2**exponent), self._config.backoff_cap_ms))

    async def _is_first_reachable(
        self,
        *,
        partition: SwitchPartitionState,
        key: ReplicaKey,
        tracker: AutoElectionTracker,
    ) -> bool:
        """判断本节点是否「死 leader 后继副本序中第一个可达的发起人」。

        Args:
            partition: 元数据权威快照。
            key: 分区键。
            tracker: 分区跟踪态（非第一个时写入错峰等待时刻）。

        Returns:
            bool: 轮到本节点发起时为 True。
        """
        order = _successor_order(partition)
        ahead: ConcurrentStableList[str] = ConcurrentStableList()
        for member in order:
            if member == self._node_id:
                break
            ahead.add(member)
        for rank, member in enumerate(ahead):
            if await self._probe(node_id=member, key=key):
                tracker.next_attempt_at = self._clock() + (rank + 1) * self._config.stagger_ms
                return False
        return True

    async def _probe(self, *, node_id: str, key: ReplicaKey) -> bool:
        """探活一个节点的分区控制端点。

        Args:
            node_id: 目标节点标识。
            key: 分区键。

        Returns:
            bool: 能读到运行态（未超时、非 None）即为 True。
        """
        try:
            async with asyncio.timeout(self._config.probe_timeout_ms / MS_PER_SECOND):
                view = await self._control.snapshot(node_id=node_id, topic_id=key[0], partition_no=key[1])
        except TimeoutError:
            return False
        except Exception as exc:
            _LOGGER.warning(
                "msg_auto_election_probe_failed",
                topic_id=key[0],
                partition=key[1],
                node_id=node_id,
                reason=type(exc).__name__,
            )
            return False
        return view is not None

    def _switched_recently(self, partition: SwitchPartitionState) -> bool:
        """按 DB 的 `last_switch_at` 判断是否仍在冷却（跨节点生效）。

        Args:
            partition: 元数据权威快照。

        Returns:
            bool: 冷却内为 True。
        """
        last = partition.last_switch_at
        if last is None:
            return False
        moment = last if last.tzinfo is not None else last.replace(tzinfo=UTC)
        elapsed_ms = (self._wall_clock() - moment).total_seconds() * MS_PER_SECOND
        return elapsed_ms < self._config.cooldown_ms

    def _report_suspect(self, *, tracker: AutoElectionTracker, key: ReplicaKey, leader: str) -> None:
        """记录「leader 判死」告警（60s 窗口抑制）。

        Args:
            tracker: 分区跟踪态。
            key: 分区键。
            leader: 被判死的 leader。
        """
        now = self._clock()
        last = tracker.suspect_logged_at
        if last is not None and now - last < _SUSPECT_LOG_WINDOW_MS:
            return
        tracker.suspect_logged_at = now
        _LOGGER.warning(
            "msg_auto_election_suspect",
            topic_id=key[0],
            partition=key[1],
            leader=leader,
            failures=tracker.failures,
        )

    def _report_cooldown(self, *, tracker: AutoElectionTracker, key: ReplicaKey) -> None:
        """记录「冷却期挡住一次发起」（每个冷却窗口只记一次）。

        Args:
            tracker: 分区跟踪态。
            key: 分区键。
        """
        if tracker.cooldown_reported:
            return
        tracker.cooldown_reported = True
        self._record(AUTO_ELECTION_RESULT_BLOCKED)
        _LOGGER.warning(
            "msg_auto_election_blocked",
            topic_id=key[0],
            partition=key[1],
            reason="cooldown",
            attempts=tracker.attempts,
        )

    def _report_exhausted(self, *, tracker: AutoElectionTracker, key: ReplicaKey, now: float) -> None:
        """记录「尝试次数达上限」并进入冷却。

        Args:
            tracker: 分区跟踪态。
            key: 分区键。
            now: 当前单调毫秒。
        """
        if tracker.exhausted_reported:
            return
        tracker.exhausted_reported = True
        tracker.cooldown_until = now + self._config.cooldown_ms
        self._record(AUTO_ELECTION_RESULT_EXHAUSTED)
        _LOGGER.error(
            "msg_auto_election_exhausted",
            topic_id=key[0],
            partition=key[1],
            attempts=tracker.attempts,
            cooldown_ms=self._config.cooldown_ms,
        )

    def _record(self, result: str) -> None:
        """记一次结果计数（未装配指标时静默）。

        Args:
            result: 结果闭集取值。
        """
        if self._metrics is not None:
            self._metrics.record(result)

    def _tracker(self, key: ReplicaKey) -> AutoElectionTracker:
        """取（必要时创建）分区跟踪态。

        Args:
            key: 分区键。

        Returns:
            AutoElectionTracker: 该分区的跟踪态。
        """
        existing = self._trackers.get(key)
        if existing is not None:
            return existing
        created = AutoElectionTracker()
        self._trackers.set(key, created)
        return created

    async def _loop(self) -> None:
        """看护循环（异常不中断；由 `stop` 取消）。"""
        interval = self._config.detect_interval_ms / MS_PER_SECOND
        while True:
            await self._sleep(interval)
            try:
                await self.run_once()
            except Exception as exc:  # pragma: no cover - 单轮异常已逐分区隔离，这里只防循环停摆
                _LOGGER.warning("msg_auto_election_round_failed", reason=type(exc).__name__)


def _successor_order(partition: SwitchPartitionState) -> ConcurrentStableFrozenList[str]:
    """按「死 leader 之后」的环形顺序输出候选发起人（排除死 leader）。

    Args:
        partition: 元数据权威快照。

    Returns:
        ConcurrentStableFrozenList[str]: 候选顺序（插入序）。
    """
    members: ConcurrentStableList[str] = ConcurrentStableList(partition.replicas)
    rotate = 0
    for position, member in enumerate(members):
        if member == partition.leader:
            rotate = position + 1
    ordered: ConcurrentStableList[str] = ConcurrentStableList()
    for offset in range(len(members)):
        member = members[(rotate + offset) % len(members)]
        if member != partition.leader:
            ordered.add(member)
    return ConcurrentStableFrozenList(ordered)


def _env_bool(name: str, default: bool) -> bool:
    """读布尔环境变量（`1` / `true` / `yes` / `on` 为真，空值为缺省）。

    Args:
        name: 环境变量名。
        default: 缺省值。

    Returns:
        bool: 解析结果。
    """
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    """读整型环境变量（空值 / 非法值回落缺省，由 `validate` 统一报错）。

    Args:
        name: 环境变量名。
        default: 缺省值。

    Returns:
        int: 解析结果。
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _monotonic_ms() -> float:
    """单调毫秒时钟。

    Returns:
        float: 单调递增毫秒数。
    """
    import time

    return time.monotonic() * MS_PER_SECOND


def _now_utc() -> datetime:
    """当前 UTC 墙钟（只用于与 `last_switch_at` 比较冷却）。

    Returns:
        datetime: 带时区的当前时刻。
    """
    return datetime.now(UTC)
