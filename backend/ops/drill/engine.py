"""演练编排（任务 07-6-1）：场景注册、隔离实例起停、报告组装。

编排只做三件事：按场景需要收紧水位并起实例、调对应场景模块跑注入、把结果装成 `DrillReport`。
失败也要有报告（前置失败记一条 `execution` 判据），否则「失败无产物」会让复跑无从比对。
"""

from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from ops.drill.capture import (
    collect_artifacts,
    command_trail,
    config_digest,
    log_snapshot,
    previous_record,
    relative_path,
    rerun_block,
    sha256_file,
)
from ops.drill.context import DrillContext, ScenarioModule, ScenarioOutcome, check
from ops.drill.env import DrillConfig, DrillError, load_scenarios, validate_scenarios
from ops.drill.model import RESULT_FAILED, DrillReport, DrillScenario, PhaseSample
from ops.drill.runtime import DrillHttp, DrillInstance, tight_watermark
from ops.drill.scenarios import (
    disk_full,
    network_partition,
    poison_message,
    process_crash,
    slow_disk,
    stuck_consumer,
)

__all__ = ["REGISTRY", "execute", "load_module", "load_scenario", "plan_lines", "reset_work_dir", "scenario_names"]

REGISTRY: ConcurrentStableFrozenDict[str, ScenarioModule] = ConcurrentStableFrozenDict(
    {
        "process_crash": cast("ScenarioModule", process_crash),
        "disk_full": cast("ScenarioModule", disk_full),
        "slow_disk": cast("ScenarioModule", slow_disk),
        "network_partition": cast("ScenarioModule", network_partition),
        "stuck_consumer": cast("ScenarioModule", stuck_consumer),
        "poison_message": cast("ScenarioModule", poison_message),
    }
)
"""场景标识 → 模块（标识与 `deploy/drill/scenarios.toml` 的 `name` 必须一致）。"""

_RUNTIME_FILES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ("platform.db", "tenant.db", "archive.db", "instance.log")
)
"""每次运行前要清掉的运行时产物（都在演练工作根下）。"""

_NO_SAMPLES: ConcurrentStableFrozenList[PhaseSample] = ConcurrentStableFrozenList()
"""空样本（实例都没起来时用）。"""


def scenario_names() -> ConcurrentStableFrozenList[str]:
    """已注册的场景标识（声明序）。

    Returns:
        ConcurrentStableFrozenList[str]: 场景标识。
    """
    return ConcurrentStableFrozenList(REGISTRY.keys())


def load_module(name: str) -> ScenarioModule:
    """按场景标识取模块。

    Args:
        name: 场景标识。

    Returns:
        ScenarioModule: 场景模块。

    Raises:
        DrillError: 未注册。
    """
    module = REGISTRY.get(name)
    if module is None:
        raise DrillError(f"drill.scenario 未注册的场景 {name!r}（可选 {', '.join(scenario_names())}）")
    return module


def reset_work_dir(config: DrillConfig) -> None:
    """清掉上一次运行的数据根与运行时产物（保证 LEO / 位点 / 死信从零开始）。

    Args:
        config: 演练配置。
    """
    config.work_dir.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(config.work_dir / "var", ignore_errors=True)
    for name in _RUNTIME_FILES:
        (config.work_dir / name).unlink(missing_ok=True)


def load_scenario(config: DrillConfig, name: str) -> DrillScenario:
    """从场景单源取一条场景定义（先校验六类齐备，再取该条）。

    Args:
        config: 演练配置。
        name: 场景标识。

    Returns:
        DrillScenario: 场景定义。

    Raises:
        DrillError: 场景单源非法或该场景不存在。
    """
    scenario_set, errors = load_scenarios(config.repo_root / "deploy" / "drill" / "scenarios.toml")
    problems: ConcurrentStableList[str] = ConcurrentStableList(errors)
    problems.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if problems:
        raise DrillError("drill.scenario 场景单源非法：" + "；".join(problems))
    for scenario in scenario_set.scenarios:
        if scenario.name == name:
            return scenario
    raise DrillError(f"drill.scenario 场景单源里没有 {name!r}")


def _outcome_from_failure(exc: DrillError, samples: ConcurrentStableFrozenList[PhaseSample]) -> ScenarioOutcome:
    """把前置 / 执行异常装成「一条失败判据」的结果（失败也留痕）。

    Args:
        exc: 异常。
        samples: 已采集到的样本（可能为空）。

    Returns:
        ScenarioOutcome: 失败结果。
    """
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict({"method": "aborted", "detail": str(exc), "artifact": ""}),
        samples=samples,
        criteria=ConcurrentStableFrozenList([check("execution", "场景执行无异常", str(exc), RESULT_FAILED)]),
        rto_ms=0,
        data_loss=False,
    )


@dataclass(frozen=True)
class _Prepared:
    """一次运行的实例与客户端（保证 `finally` 里能按同一对象清理）。"""

    instance: DrillInstance
    """隔离实例。"""

    http: DrillHttp
    """实例客户端。"""

    fill_mb: int
    """本次填充量（MiB；非磁盘满场景为 0）。"""

    reject_ratio: float
    """本次拒绝水位（非磁盘满场景为 0，表示用运行时缺省）。"""


def _prepare(config: DrillConfig, module: ScenarioModule) -> _Prepared:
    """起隔离实例并构造客户端（磁盘满场景额外收紧水位）。

    Args:
        config: 演练配置。
        module: 场景模块。

    Returns:
        _Prepared: 实例与客户端。

    Raises:
        DrillError: 实例未在预算内就绪。
    """
    reset_work_dir(config)
    ratio: float | None = None
    warn: float | None = None
    fill_mb = 0
    if module.needs_tight_watermark:
        warn, ratio, fill_mb = tight_watermark(config.work_dir, config.fill_mb)
    instance = DrillInstance(config, reject_ratio=ratio, warn_ratio=warn)
    http = DrillHttp(instance.base_url)
    instance.start()
    instance.wait_ready()
    return _Prepared(instance=instance, http=http, fill_mb=fill_mb, reject_ratio=ratio or 0.0)


def _injection(config: DrillConfig, outcome: ScenarioOutcome) -> ConcurrentStableFrozenDict[str, str]:
    """把注入明细补上相对路径与产物摘要（旧 `artifact` 保留作兼容）。"""
    payload: ConcurrentStableDict[str, str] = ConcurrentStableDict(dict(outcome.injection))
    artifact = outcome.injection.get("artifact") or ""
    if artifact:
        path = Path(artifact)
        payload.set("artifact_rel", relative_path(config.work_dir, path))
        if path.is_file():
            payload.set("artifact_sha256", sha256_file(path))
    return ConcurrentStableFrozenDict(payload)


def _report(
    scenario: DrillScenario,
    config: DrillConfig,
    started_at: datetime,
    outcome: ScenarioOutcome,
    *,
    http: DrillHttp | None,
    rerun: ConcurrentStableFrozenDict[str, str],
) -> DrillReport:
    """把场景产物装成报告。

    Args:
        scenario: 场景定义。
        config: 演练配置。
        started_at: 开始时刻。
        outcome: 场景产物。
        http: 演练客户端（未起来时为 None，动作轨迹为空）。
        rerun: 复跑标识（首跑为空）。

    Returns:
        DrillReport: 演练报告。
    """
    return DrillReport(
        scenario=scenario.name,
        mode="run",
        env=config.env,
        target="local",
        started_at=started_at,
        finished_at=datetime.now(UTC),
        injection=_injection(config, outcome),
        samples=outcome.samples,
        criteria=outcome.criteria,
        rto_ms=outcome.rto_ms,
        data_loss=outcome.data_loss,
        deviations=outcome.deviations,
        issues=outcome.issues,
        leftovers=outcome.leftovers,
        commands=command_trail(http)
        if http is not None
        else ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]](),
        artifacts=collect_artifacts(config),
        logs=log_snapshot(config),
        config_digest=config_digest(config, scenario.name),
        rerun=rerun,
    )


def execute(config: DrillConfig, scenario: DrillScenario, *, rerun_reason: str = "") -> DrillReport:
    """执行一个场景并返回报告（实例总会停掉，失败也返回报告）。

    Args:
        config: 演练配置。
        scenario: 场景定义（来自场景单源）。
        rerun_reason: 复跑原因（写入记录 `rerun.reason`；首跑忽略）。

    Returns:
        DrillReport: 演练报告。
    """
    module = load_module(scenario.name)
    started_at = datetime.now(UTC)
    rerun = rerun_block(previous_record(config, scenario.name), rerun_reason)
    prepared: _Prepared | None = None
    ctx: DrillContext | None = None
    outcome: ScenarioOutcome | None = None
    try:
        prepared = _prepare(config, module)
        ctx = DrillContext(
            config=config,
            scenario=scenario,
            instance=prepared.instance,
            http=prepared.http,
            started_at=time.monotonic(),
            fill_mb=prepared.fill_mb,
            reject_ratio=prepared.reject_ratio,
        )
        outcome = module.run(ctx)
    except DrillError as exc:
        outcome = _outcome_from_failure(exc, ctx.samples if ctx is not None else _NO_SAMPLES)
    finally:
        if prepared is not None:
            prepared.http.close()
            prepared.instance.stop()
    assert outcome is not None
    return _report(
        scenario,
        config,
        started_at,
        outcome,
        http=prepared.http if prepared is not None else None,
        rerun=rerun,
    )


def plan_lines(config: DrillConfig, scenario: DrillScenario) -> ConcurrentStableList[str]:
    """渲染某场景的注入与恢复动作（dry-run，零执行）。

    Args:
        config: 演练配置。
        scenario: 场景定义。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    module = load_module(scenario.name)
    config.work_dir.mkdir(parents=True, exist_ok=True)
    instance = DrillInstance(config)
    http = DrillHttp(instance.base_url)
    try:
        ctx = DrillContext(config=config, scenario=scenario, instance=instance, http=http, started_at=0.0)
        return module.plan(ctx)
    finally:
        http.close()
