"""故障注入动作（02-8）：容器终止 / 断网 / 恢复的命令构造。

**纯函数**：本模块只产出命令与步骤，不执行；执行走 `RemoteRunner`，便于用例断言「注入了什么」。
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.objects import BaseValueObject

from ops.cluster_drill.config import ClusterDrillConfig
from ops.cluster_drill.remote import RemoteRunner

__all__ = [
    "InjectionStep",
    "connect_command",
    "disconnect_command",
    "kill_command",
    "network_name",
    "start_command",
    "steps_for",
    "stop_command",
]

_NETWORK_FALLBACK_PREFIX = "msg-drill-net"
"""演练网络缺省名（环境文件未给 `PLATFORM_NETWORK` 时使用）。"""


def network_name(config: ClusterDrillConfig) -> str:
    """取演练网络名（环境文件 `PLATFORM_NETWORK` 优先）。

    Args:
        config: 演练配置。

    Returns:
        str: 网络名。
    """
    return (config.variables.get("PLATFORM_NETWORK") or _NETWORK_FALLBACK_PREFIX).strip()


def kill_command(container: str, *, signal: str = "SIGKILL") -> str:
    """构造容器信号终止命令。

    Args:
        container: 容器名。
        signal: 信号（`SIGKILL` / `SIGTERM`）。

    Returns:
        str: 命令。
    """
    return f"docker kill -s {shlex.quote(signal)} {shlex.quote(container)}"


def stop_command(container: str) -> str:
    """构造容器停止命令（模拟进程崩溃后的不可达）。

    Args:
        container: 容器名。

    Returns:
        str: 命令。
    """
    return f"docker stop -t 1 {shlex.quote(container)}"


def start_command(container: str) -> str:
    """构造容器重启命令（同一数据卷恢复）。

    Args:
        container: 容器名。

    Returns:
        str: 命令。
    """
    return f"docker start {shlex.quote(container)}"


def disconnect_command(container: str, network: str) -> str:
    """构造整容器断网命令。

    Args:
        container: 容器名。
        network: 网络名。

    Returns:
        str: 命令。
    """
    return f"docker network disconnect {shlex.quote(network)} {shlex.quote(container)}"


def connect_command(container: str, network: str) -> str:
    """构造整容器恢复网络命令。

    Args:
        container: 容器名。
        network: 网络名。

    Returns:
        str: 命令。
    """
    return f"docker network connect {shlex.quote(network)} {shlex.quote(container)}"


@dataclass(frozen=True)
class InjectionStep(BaseValueObject):
    """一步注入 / 恢复动作。"""

    phase: str
    """`inject` / `recover`。"""

    name: str
    """动作名（记录用）。"""

    command: str
    """远端 shell 命令。"""


def steps_for(scenario: str, config: ClusterDrillConfig) -> ConcurrentStableList[InjectionStep]:
    """按场景给出注入与恢复步骤（不执行）。

    Args:
        scenario: 场景标识。
        config: 演练配置。

    Returns:
        ConcurrentStableList[InjectionStep]: 步骤（注入在前、恢复在后）。

    Raises:
        KeyError: 未知场景。
    """
    names = config.container_names
    network = network_name(config)
    steps: ConcurrentStableList[InjectionStep] = ConcurrentStableList()
    if scenario == "replica_crash":
        steps.add(InjectionStep(phase="inject", name="kill-leader", command=kill_command(names[0])))
        steps.add(InjectionStep(phase="recover", name="start-leader", command=start_command(names[0])))
    elif scenario == "network_partition_multi":
        steps.add(
            InjectionStep(phase="inject", name="disconnect-leader", command=disconnect_command(names[0], network))
        )
        steps.add(
            InjectionStep(phase="inject", name="disconnect-follower", command=disconnect_command(names[1], network))
        )
        steps.add(InjectionStep(phase="recover", name="connect-leader", command=connect_command(names[0], network)))
        steps.add(InjectionStep(phase="recover", name="connect-follower", command=connect_command(names[1], network)))
    elif scenario == "split_brain_multi":
        steps.add(
            InjectionStep(phase="inject", name="isolate-old-leader", command=disconnect_command(names[0], network))
        )
        steps.add(InjectionStep(phase="recover", name="rejoin-old-leader", command=connect_command(names[0], network)))
    else:
        raise KeyError(f"未知的多实例场景：{scenario}")
    return steps


def apply_steps(runner: RemoteRunner, steps: ConcurrentStableList[InjectionStep], *, phase: str) -> None:
    """执行指定阶段步骤（任一失败即抛，不静默继续）。

    Args:
        runner: 远程执行器。
        steps: 步骤。
        phase: `inject` / `recover`。

    Raises:
        ClusterDrillError: 任一步骤失败。
    """
    from ops.cluster_drill.config import ClusterDrillError

    for step in steps:
        if step.phase != phase:
            continue
        result = runner.run(step.command)
        if not result.ok:
            raise ClusterDrillError(f"cluster-drill.inject {step.name} 失败：{result.stderr.strip()[:200]}")
