"""多实例故障切换演练编排（02-8）：`plan` / `run` / `verify` / `cleanup`。

**为什么单独入口**：07-6 的 `ops.drill` 只跑本机隔离实例；多实例场景要经 SSH 调 Docker、开隧道、
注入容器故障，放在同一命令里会让「本机演练」也背上远程依赖。

退出码与 07-6 同口径：`0` 通过、`1` 失败、`2` 降级（含 `force` / 可降级项）。
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from ops.cluster_drill import inject, scenarios
from ops.cluster_drill.config import (
    ClusterDrillConfig,
    ClusterDrillError,
    load_config,
    reject_production,
    require_confirmation,
)
from ops.cluster_drill.probe import ProbeClient, metric_value
from ops.cluster_drill.remote import RemoteRunner, build_image_script, compose_down_script, compose_up_script
from ops.cluster_drill.report import write_cluster_report
from ops.drill.env import TARGET_DOCKER, load_scenarios, validate_scenarios
from ops.drill.model import (
    RESULT_FAILED,
    RESULT_OK,
    RESULT_SKIPPED,
    Criterion,
    DrillReport,
    DrillScenario,
    PhaseSample,
    ScenarioSet,
)

__all__ = ["main", "run_plan", "run_scenario"]

_EXIT_PARAM = 2
"""参数 / 生产拒绝的退出码（与「降级通过」同码）。"""

_PROBE_INTERVAL_MS = 2_000
"""就绪轮询间隔（毫秒）。"""


class _Outcome:
    """一次场景执行的中间结果（记录组装用）。"""

    def __init__(self) -> None:
        self.criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
        self.commands: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
        self.issues: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
        self.started_at = datetime.now(UTC)
        self.finished_at = datetime.now(UTC)
        self.rto_ms = 0
        self.data_loss = False


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.cluster_drill", description="msg 多实例故障演练（02-8）")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("plan", help="渲染三类多实例场景与注入步骤（不执行）")

    run = sub.add_parser("run", help="执行一个多实例场景")
    run.add_argument("scenario", help="场景标识（replica_crash / network_partition_multi / split_brain_multi）")
    run.add_argument("--confirm", dest="confirm", default=None, help="二次确认：必须等于场景标识")
    run.add_argument("--dry-run", action="store_true", help="只渲染注入与恢复动作，零执行")
    run.add_argument(
        "--skip-provision", action="store_true", dest="skip_provision", help="跳过源码同步与镜像构建（复用已起实例）"
    )
    run.add_argument("--topic-id", type=int, dest="topic_id", default=None, help="演练主题主键（切主用）")
    run.add_argument("--rerun-reason", dest="rerun_reason", default="", help="复跑原因")

    verify = sub.add_parser("verify", help="对账多实例记录并生成矩阵")
    verify.add_argument("--records", action="append", required=True, help="记录目录（可重复）")

    sub.add_parser("cleanup", help="停止演练容器并删除演练卷（幂等）")
    return parser


def _scenario_set(config: ClusterDrillConfig) -> ScenarioSet:
    """读取并校验场景单源（只保留多实例场景）。

    Args:
        config: 演练配置。

    Returns:
        ScenarioSet: 多实例场景集。

    Raises:
        ClusterDrillError: 单源非法。
    """
    root = Path(__file__).resolve().parents[3]
    scenario_set, errors = load_scenarios(root / "deploy" / "drill" / "scenarios.toml")
    problems: ConcurrentStableList[str] = ConcurrentStableList(errors)
    problems.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if problems:
        raise ClusterDrillError("cluster-drill.scenario 场景单源非法：" + "；".join(problems))
    return ScenarioSet(
        schema_version=scenario_set.schema_version,
        updated_at=scenario_set.updated_at,
        owner=scenario_set.owner,
        scenarios=ConcurrentStableFrozenList(item for item in scenario_set.scenarios if item.target == TARGET_DOCKER),
    )


def run_plan(config: ClusterDrillConfig) -> int:
    """渲染三类场景与注入步骤。

    Args:
        config: 演练配置。

    Returns:
        int: 退出码（恒 0）。
    """
    scenario_set = _scenario_set(config)
    print(f"多实例场景：{len(scenario_set.scenarios)} 类（env={config.env}、project={config.project}）")
    for scenario in scenario_set.scenarios:
        print(f"[{scenario.name}] {scenario.title}")
        for step in inject.steps_for(scenario.name, config):
            print(f"  [{step.phase}] {step.name}: {step.command}")
    return 0


def _wait_ready(client: ProbeClient, timeout_sec: int) -> bool:
    """轮询三实例就绪。

    Args:
        client: 探针。
        timeout_sec: 超时（秒）。

    Returns:
        bool: 超时前全部就绪为 True。
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if asyncio.run(client.all_ready()):
            return True
        time.sleep(_PROBE_INTERVAL_MS / 1000)
    return False


async def _samples(client: ProbeClient, phase: str) -> ConcurrentStableList[PhaseSample]:
    """采集一段观测（三实例聚合：任一就绪即 ok，指标取首实例合计）。

    Args:
        client: 探针。
        phase: 阶段（`before` / `during` / `after`）。

    Returns:
        ConcurrentStableList[PhaseSample]: 三实例样本。
    """
    result: ConcurrentStableList[PhaseSample] = ConcurrentStableList()
    for index in range(3):
        reading = await client.readyz(index)
        metrics = await client.metrics(index)
        result.add(
            PhaseSample(
                phase=phase,
                captured_at=datetime.now(UTC).isoformat(),
                readyz_status=reading.status,
                readyz_ok=reading.ok,
                healthz_ok=reading.status == 200,
                metrics=ConcurrentStableFrozenDict(
                    {
                        "msg_leader_switch_total": metric_value(metrics, "msg_leader_switch_total"),
                        "msg_isr_size": metric_value(metrics, "msg_isr_size"),
                        "msg_publish_errors_total": metric_value(metrics, "msg_publish_errors_total"),
                    }
                ),
                detail="",
            )
        )
    return result


def _criteria_from_samples(samples: ConcurrentStableList[PhaseSample]) -> ConcurrentStableDict[str, tuple[str, str]]:
    """由三段样本推导可观测判据（不可观测项留空由 `scenarios.build_criteria` 记 skipped）。

    Args:
        samples: 三段样本。

    Returns:
        ConcurrentStableDict[str, tuple[str, str]]: 记录键 → `(result, actual)`。
    """
    facts: ConcurrentStableDict[str, tuple[str, str]] = ConcurrentStableDict()
    after_ok = any(sample.readyz_ok for sample in samples if sample.phase == "after")
    facts.set(
        "rto_within_budget",
        (RESULT_OK, "恢复后三段样本均可采集") if after_ok else (RESULT_FAILED, "恢复后三实例均不可达"),
    )
    return facts


async def run_scenario(
    config: ClusterDrillConfig,
    scenario: DrillScenario,
    *,
    confirm: str | None,
    dry_run: bool,
    skip_provision: bool,
    topic_id: int | None,
    rerun_reason: str,
) -> int:
    """执行一个多实例场景。

    Args:
        config: 演练配置。
        scenario: 场景定义（`target=docker`）。
        confirm: 二次确认值。
        dry_run: 只渲染动作。
        skip_provision: 跳过同步 / 构建。
        topic_id: 演练主题主键（切主用；None 记录为遗留）。
        rerun_reason: 复跑原因。

    Returns:
        int: 退出码。
    """
    require_confirmation(scenario.name, confirm)
    steps = inject.steps_for(scenario.name, config)
    if dry_run:
        print(f"[PLAN] {scenario.name} · {scenario.title}（dry-run，零执行）")
        for step in steps:
            print(f"  [{step.phase}] {step.name}: {step.command}")
        return 0
    outcome = _Outcome()
    runner = RemoteRunner(config)
    root = Path(__file__).resolve().parents[3]
    started = time.monotonic()
    if not skip_provision:
        sync = runner.sync_source(local_root=root)
        if not sync.ok:
            raise ClusterDrillError(f"cluster-drill.remote 源码同步失败：{sync.stderr.strip()[:200]}")
        commit = _commit(root)
        build = runner.run(build_image_script(config, commit=commit))
        outcome.commands.add(ConcurrentStableDict({"step": "build", "result": "ok" if build.ok else "failed"}))
        if not build.ok:
            raise ClusterDrillError(f"cluster-drill.remote 镜像构建失败：{build.stderr.strip()[:200]}")
        up = runner.run(compose_up_script(config))
        outcome.commands.add(ConcurrentStableDict({"step": "compose_up", "result": "ok" if up.ok else "failed"}))
        if not up.ok:
            raise ClusterDrillError(f"cluster-drill.remote 起三实例失败：{up.stderr.strip()[:200]}")
    bases: ConcurrentStableList[str] = ConcurrentStableList(f"http://127.0.0.1:{port}" for port in config.ports)
    client = ProbeClient(bases)
    try:
        if not _wait_ready(client, config.ready_timeout_sec):
            raise ClusterDrillError("cluster-drill.guard 三实例未在预算内就绪")
        before = asyncio.run(_samples(client, "before"))
        inject.apply_steps(runner, steps, phase="inject")
        during = asyncio.run(_samples(client, "during"))
        if topic_id is not None:
            switch = runner.docker_exec(
                config.container_names[0],
                f"python -m ops.failover switch --topic-id {int(topic_id)} --all-partitions "
                f"--operator system:drill --reason {scenario.name}",
            )
            outcome.commands.add(ConcurrentStableDict({"step": "failover", "result": "ok" if switch.ok else "failed"}))
        else:
            outcome.issues.add(
                ConcurrentStableDict(
                    {"step": "failover", "reason": "未提供 --topic-id，切主与数据完整性证据未采集", "owner": "02-8"}
                )
            )
        inject.apply_steps(runner, steps, phase="recover")
        if not _wait_ready(client, config.ready_timeout_sec):
            outcome.issues.add(
                ConcurrentStableDict({"step": "recover", "reason": "恢复后未在预算内全部就绪", "owner": "02-8"})
            )
        after = asyncio.run(_samples(client, "after"))
    finally:
        await client.aclose()
    outcome.rto_ms = int((time.monotonic() - started) * 1000)
    samples = ConcurrentStableList([*before, *during, *after])
    facts = _criteria_from_samples(samples)
    if topic_id is None:
        for key in (
            "committed_not_lost",
            "no_double_write",
            "client_refresh",
            "leader_switched",
            "old_leader_fenced",
            "isr_shrink_rejoin",
            "leader_lease_expired",
            "follower_isr_shrink",
            "retained_tail_documented",
            "stale_leader_rejected",
            "rejoin_demotes",
        ):
            facts.set(key, (RESULT_SKIPPED, "未提供演练主题，无法采集"))
    outcome.criteria.extend(scenarios.build_criteria(scenario, facts))
    report = _report(config, scenario, outcome, samples, rerun_reason=rerun_reason)
    json_path, markdown_path = write_cluster_report(report, config.report_dir, title=scenario.title)
    print(f"[{report.conclusion}] {scenario.title}（退出码 {report.exit_code}，恢复 {report.rto_ms} ms）")
    for reason in report.reasons:
        print(f"  - {reason}")
    print(f"  记录：{json_path} ｜ {markdown_path}")
    return report.exit_code


def _report(
    config: ClusterDrillConfig,
    scenario: DrillScenario,
    outcome: _Outcome,
    samples: ConcurrentStableList[PhaseSample],
    *,
    rerun_reason: str,
) -> DrillReport:
    """组装演练报告（`target=docker`）。

    Args:
        config: 演练配置。
        scenario: 场景定义。
        outcome: 中间结果。
        samples: 三段样本。
        rerun_reason: 复跑原因。

    Returns:
        DrillReport: 报告。
    """
    return DrillReport(
        scenario=scenario.name,
        mode="run",
        env=config.env,
        target=TARGET_DOCKER,
        started_at=outcome.started_at,
        finished_at=datetime.now(UTC),
        injection=ConcurrentStableFrozenDict(
            {"method": scenario.inject, "detail": scenario.expect, "artifact": config.project}
        ),
        samples=ConcurrentStableFrozenList(samples),
        criteria=ConcurrentStableFrozenList(outcome.criteria),
        rto_ms=outcome.rto_ms,
        data_loss=outcome.data_loss,
        deviations=ConcurrentStableFrozenList(
            (
                "演练使用独立项目 / 容器名 / 派生库前缀，不等同生产链路",
                "未启用网关别名，客户端经本机隧道直连实例",
                "未做真盘故障注入（归 07-8）",
            )
        ),
        issues=ConcurrentStableFrozenList(ConcurrentStableFrozenDict(item) for item in outcome.issues),
        leftovers=ConcurrentStableFrozenList(),
        commands=ConcurrentStableFrozenList(ConcurrentStableFrozenDict(item) for item in outcome.commands),
        rerun=ConcurrentStableFrozenDict({"reason": rerun_reason} if rerun_reason else {}),
    )


def _commit(root: Path) -> str:
    """取本地提交短号（失败回 `unknown`，不因 git 缺失阻断）。

    Args:
        root: 仓库根。

    Returns:
        str: 提交短号。
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return "unknown"
    return (result.stdout or "").strip() or "unknown"


def run_cleanup(config: ClusterDrillConfig) -> int:
    """停止演练容器并删除演练卷（幂等）。

    Args:
        config: 演练配置。

    Returns:
        int: 退出码（0 成功；远程失败 1）。
    """
    result = RemoteRunner(config).run(compose_down_script(config))
    if not result.ok:
        print(f"[FAIL] cluster-drill.cleanup 清理失败：{result.stderr.strip()[:200]}")
        return 1
    print(f"[OK] 已清理演练项目 {config.project}")
    return 0


def run_verify(scenario_set: ScenarioSet, records: ConcurrentStableList[Path]) -> int:
    """对账多实例记录并写矩阵。

    Args:
        scenario_set: 多实例场景集。
        records: 记录目录。

    Returns:
        int: 退出码。
    """
    from ops.drill import evidence

    report = evidence.verify(scenario_set, ConcurrentStableFrozenList(records), ConcurrentStableFrozenList())
    print(f"[{report.conclusion}] 多实例场景 × 判据 × 证据（退出码 {report.exit_code}）")
    for reason in report.reasons:
        print(f"  - {reason}")
    return report.exit_code


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    args = _parser().parse_args(sys.argv[1:] if argv is None else list(argv))
    try:
        config = load_config()
        reject_production(config.env)
        if args.command == "plan":
            return run_plan(config)
        if args.command == "cleanup":
            return run_cleanup(config)
        if args.command == "verify":
            scenario_set = _scenario_set(config)
            return run_verify(
                scenario_set,
                ConcurrentStableList(Path(item).expanduser().resolve() for item in args.records),
            )
        scenario_set = _scenario_set(config)
        target = next((item for item in scenario_set.scenarios if item.name == args.scenario), None)
        if target is None:
            raise ClusterDrillError(f"cluster-drill.scenario 未注册的多实例场景 {args.scenario!r}")
        return asyncio.run(
            run_scenario(
                config,
                target,
                confirm=args.confirm,
                dry_run=bool(args.dry_run),
                skip_provision=bool(args.skip_provision),
                topic_id=args.topic_id,
                rerun_reason=str(args.rerun_reason),
            )
        )
    except ClusterDrillError as exc:
        print(f"[FAIL] {exc}")
        return _EXIT_PARAM


if __name__ == "__main__":  # pragma: no cover - 命令行入口
    raise SystemExit(main())
