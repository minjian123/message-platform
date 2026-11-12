"""多实例演练判据组装（02-8）：把采集事实映射成 `Criterion`。

**不默认通过**：场景单源的每条判据都要有对应事实；缺失记 `skipped`（结论降级），
`force` / `isr_verified=false` 记 `degraded`，绝不把「没采到」写成 `ok`。
"""

from __future__ import annotations

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList

from ops.drill.model import (
    RESULT_DEGRADED,
    RESULT_SKIPPED,
    Criterion,
    DrillScenario,
)

__all__ = ["build_criteria", "missing_keys"]


def build_criteria(
    scenario: DrillScenario,
    facts: ConcurrentStableDict[str, tuple[str, str]],
) -> ConcurrentStableList[Criterion]:
    """按场景声明组装判据结果。

    Args:
        scenario: 场景定义（`target=docker`）。
        facts: 记录键 → `(result, actual)`；`result` 取 `ok` / `degraded` / `failed`。

    Returns:
        ConcurrentStableList[Criterion]: 判据结果（声明序）。
    """
    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    for spec in scenario.criterion:
        chosen: tuple[str, str] | None = None
        for key in spec.record_keys:
            value = facts.get(key)
            if value is not None:
                chosen = value
                break
        if chosen is None:
            criteria.add(
                Criterion(
                    key=spec.key,
                    expect=spec.text,
                    actual="未采集到证据（按降级记录，不计通过）",
                    result=RESULT_SKIPPED,
                )
            )
            continue
        result, actual = chosen
        criteria.add(Criterion(key=spec.key, expect=spec.text, actual=actual, result=result))
    return criteria


def missing_keys(
    scenario: DrillScenario, facts: ConcurrentStableDict[str, tuple[str, str]]
) -> ConcurrentStableList[str]:
    """列出场景声明中缺少证据的记录键（排障用）。

    Args:
        scenario: 场景定义。
        facts: 已采集事实。

    Returns:
        ConcurrentStableList[str]: 缺失键（声明序）。
    """
    missing: ConcurrentStableList[str] = ConcurrentStableList()
    for spec in scenario.criterion:
        if not any(key in facts for key in spec.record_keys):
            missing.add(spec.key)
    return missing


def degraded_note(isr_verified: bool) -> tuple[str, str]:
    """把 `isr_verified` 转成判据结果（false 记降级）。

    Args:
        isr_verified: 切主是否验证 ISR。

    Returns:
        tuple[str, str]: `(result, actual)`。
    """
    if not isr_verified:
        return RESULT_DEGRADED, "isr_verified=false（force / 无法验证 ISR，不计通过）"
    return "ok", "isr_verified=true"
