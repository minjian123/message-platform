"""客户端刷新验收（02-8）：`350033` → 读元数据 → 改投新 leader。

**只验服务端语义**（真实 HTTP 客户端脚本），不实现 SDK（归 8B_03）。判定与网络解耦：
发布 / 读元数据都以可注入的异步函数传入，用例无需起服务。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from bms_core.core.concurrent import ConcurrentStableFrozenList
from bms_core.core.objects import BaseValueObject

__all__ = [
    "NOT_LEADER_CODE",
    "MetadataView",
    "PublishOutcome",
    "RefreshOutcome",
    "needs_refresh",
    "refresh_and_retry",
]

NOT_LEADER_CODE = 350033
"""分区写被拒的业务码（`not_leader` / `lease_expired` / `stale_epoch`）。"""

_REFRESHABLE_REASONS = ("not_leader", "lease_expired", "stale_epoch", "switching")
"""可刷新后改投的原因闭集。"""


@dataclass(frozen=True)
class PublishOutcome(BaseValueObject):
    """一次发布结果。"""

    ok: bool
    code: int = 0
    reason: str = ""
    leader: str = ""
    epoch: int = 0
    offset: int = -1


@dataclass(frozen=True)
class MetadataView(BaseValueObject):
    """读到的分区元数据。"""

    metadata_version: int
    leader: str
    epoch: int


@dataclass(frozen=True)
class RefreshOutcome(BaseValueObject):
    """刷新验收结果。"""

    first: PublishOutcome
    metadata: MetadataView | None
    second: PublishOutcome | None
    refreshed: bool = False
    version_advanced: bool = False
    reasons: ConcurrentStableFrozenList[str] = field(default_factory=lambda: ConcurrentStableFrozenList[str]())


def needs_refresh(outcome: PublishOutcome) -> bool:
    """写旧 leader 是否需要刷新元数据后改投。

    Args:
        outcome: 发布结果。

    Returns:
        bool: `350033` 且原因在可刷新闭集内。
    """
    return outcome.code == NOT_LEADER_CODE and outcome.reason in _REFRESHABLE_REASONS


async def refresh_and_retry(
    *,
    publish: Callable[[str], Awaitable[PublishOutcome]],
    fetch_metadata: Callable[[], Awaitable[MetadataView]],
    old_base: str,
    metadata_version_before: int,
) -> RefreshOutcome:
    """执行「写旧 leader → 读元数据 → 改投新 leader」。

    Args:
        publish: 按基地址发布（返回结果）。
        fetch_metadata: 读分区元数据。
        old_base: 旧 leader 基地址。
        metadata_version_before: 切换前的 `metadata_version`。

    Returns:
        RefreshOutcome: 验收结果（`reasons` 非空即不通过）。
    """
    first = await publish(old_base)
    if not needs_refresh(first):
        return RefreshOutcome(
            first=first,
            metadata=None,
            second=None,
            reasons=ConcurrentStableFrozenList(
                (f"旧 leader 未返回 350033（code={first.code}, reason={first.reason or '-'}）",)
            ),
        )
    metadata = await fetch_metadata()
    reasons = ConcurrentStableFrozenList(
        (f"元数据 leader={metadata.leader} 与拒写提示 leader={first.leader or '-'} 不一致",)
        if first.leader and metadata.leader != first.leader
        else ()
    )
    new_base = _base_for(metadata.leader)
    second = await publish(new_base) if new_base else None
    if second is None:
        reasons = ConcurrentStableFrozenList([*reasons, "无法从节点标识推导新 leader 地址"])
    elif not second.ok:
        reasons = ConcurrentStableFrozenList([*reasons, f"改投后发布仍失败（code={second.code}）"])
    return RefreshOutcome(
        first=first,
        metadata=metadata,
        second=second,
        refreshed=True,
        version_advanced=metadata.metadata_version > int(metadata_version_before),
        reasons=reasons,
    )


def _base_for(node_id: str) -> str:
    """把节点标识推导成演练基地址（`msg-drill-1` → `http://127.0.0.1:18800`）。

    **只用于记录与本地隧道**：端口映射来自演练配置，生产客户端由网关 / 服务发现给出地址。

    Args:
        node_id: 节点标识。

    Returns:
        str: 基地址；无法推导时为空串。
    """
    suffix = node_id.rsplit("-", 1)[-1]
    if not suffix.isdigit():
        return ""
    index = int(suffix)
    return f"http://127.0.0.1:{18_800 + index - 1}"
