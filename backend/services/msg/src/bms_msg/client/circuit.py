"""进程内熔断器（04-4）：复用 bms `BaseCircuitBreaker` 契约的三态实现。

**为什么 msg 自己实现**：bms 侧只交付了契约 + `NullCircuitBreaker`（恒定闭合），真实三态计数
明确归「性能与安全阶段」回补（见 `bms_core/circuit/base.py` 模块说明）。本任务继承同一契约、
复用 `CircuitState` 与 `service_dependency()` 的 `service:<key>` 依赖标识，不改 bms、不另造语义；
04-5 适配时可整体换成 bms 侧的真实实现。

**手动实例化、不经插件注册表**：`BasePluggable` 带来的 `key` / `contract_version` 类属性只是
契约标识，msg 是独立进程，装配由 `RpcClient` 构造注入完成，不需要 `resolve_plugin`。
"""

from __future__ import annotations

from dataclasses import dataclass

from bms_core.circuit.base import BaseCircuitBreaker, CircuitState
from bms_core.core.logging import get_logger
from bms_core.servicecall.base import service_dependency

from bms_msg.client.retry import CircuitPolicy, Clock

__all__ = ["InProcessCircuitBreaker", "rpc_dependency"]

_LOGGER = get_logger("bms_msg.rpc.circuit")


def rpc_dependency(service: str) -> str:
    """RPC 调用的熔断依赖标识（与 bms 服务间调用同一命名空间）。

    Args:
        service: 目标服务标识。

    Returns:
        str: `service:<service>`。
    """
    return service_dependency(service)


@dataclass
class _Entry:
    """单个依赖的熔断状态（首期进程内，不跨实例）。"""

    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_ms: int = 0
    opened_at_ms: int = 0
    probes: int = 0


class InProcessCircuitBreaker(BaseCircuitBreaker):
    """三态熔断器：连续失败阈值 + 窗口清零 + 打开计时 + 半开并发限流。

    时钟经 `Clock` 注入，用例用 fake clock 精确驱动「打开 30s 后转半开」，不依赖真实等待。
    """

    def __init__(self, *, policy: CircuitPolicy | None = None, clock: Clock | None = None) -> None:
        """初始化。

        Args:
            policy: 熔断参数（缺省取 `CircuitPolicy()` 的仓内常量默认值）。
            clock: 时钟（缺省 `SystemClock()`）。
        """
        from bms_msg.client.retry import SystemClock

        self._policy = policy if policy is not None else CircuitPolicy()
        self._clock = clock if clock is not None else SystemClock()
        self._entries: dict[str, _Entry] = {}

    def _entry(self, dependency: str) -> _Entry:
        """取（或建）依赖的状态条目。

        Args:
            dependency: 依赖标识。

        Returns:
            _Entry: 状态条目。
        """
        if dependency not in self._entries:
            self._entries[dependency] = _Entry()
        return self._entries[dependency]

    def _advance(self, dependency: str, entry: _Entry, now_ms: int) -> None:
        """打开态到期即转半开（唯一的状态推进点，`allow` 与 `state` 共用）。

        Args:
            dependency: 依赖标识（日志用）。
            entry: 状态条目。
            now_ms: 当前单调毫秒。
        """
        if entry.state is CircuitState.OPEN and now_ms - entry.opened_at_ms >= self._policy.open_ms:
            self._transition(dependency, entry, CircuitState.HALF_OPEN)
            entry.probes = 0

    def _transition(self, dependency: str, entry: _Entry, target: CircuitState) -> None:
        """记录一次状态转换（观测事件唯一出口）。

        Args:
            dependency: 依赖标识。
            entry: 状态条目。
            target: 目标状态。
        """
        previous = entry.state
        entry.state = target
        _LOGGER.info("msg_rpc_circuit", dependency=dependency, **{"from": previous.value, "to": target.value})

    async def allow(self, dependency: str) -> bool:
        """是否放行本次调用（打开态快速失败；半开态限流探测）。

        Args:
            dependency: 依赖标识。

        Returns:
            bool: 放行 True。
        """
        entry = self._entry(dependency)
        self._advance(dependency, entry, self._clock.now_ms())
        if entry.state is CircuitState.OPEN:
            return False
        if entry.state is CircuitState.HALF_OPEN:
            if entry.probes >= self._policy.half_open_max_calls:
                return False
            entry.probes += 1
            return True
        return True

    async def record_success(self, dependency: str) -> None:
        """记录成功：清零失败计数；半开探测成功则闭合。

        Args:
            dependency: 依赖标识。
        """
        entry = self._entry(dependency)
        entry.failures = 0
        entry.probes = max(0, entry.probes - 1)
        if entry.state is CircuitState.HALF_OPEN:
            self._transition(dependency, entry, CircuitState.CLOSED)

    async def record_failure(self, dependency: str) -> None:
        """记录失败：半开探测失败立即回到打开；闭合态按窗口累计连续失败。

        Args:
            dependency: 依赖标识。
        """
        entry = self._entry(dependency)
        now_ms = self._clock.now_ms()
        entry.probes = max(0, entry.probes - 1)
        if entry.state is CircuitState.HALF_OPEN:
            entry.opened_at_ms = now_ms
            self._transition(dependency, entry, CircuitState.OPEN)
            return
        if now_ms - entry.last_failure_ms >= self._policy.window_ms:
            entry.failures = 0
        entry.last_failure_ms = now_ms
        entry.failures += 1
        if entry.failures >= self._policy.failure_threshold:
            entry.opened_at_ms = now_ms
            self._transition(dependency, entry, CircuitState.OPEN)

    async def state(self, dependency: str) -> CircuitState:
        """当前状态（供告警 / 可观测性读取；到期自动推进到半开）。

        Args:
            dependency: 依赖标识。

        Returns:
            CircuitState: 三态之一。
        """
        entry = self._entry(dependency)
        self._advance(dependency, entry, self._clock.now_ms())
        return entry.state
