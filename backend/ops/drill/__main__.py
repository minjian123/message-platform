"""演练命令行（任务 07-6-1 起；07-6-2 增判据 / 矩阵 / 发版编排）。

子命令：`run` / `plan` / `cleanup`（注入）· `criteria` / `checklist` / `verify` / `release`（判据与回写）。

退出码与结论同口径：`0` 通过、`2` 降级通过（或参数 / 生产拒绝）、`1` 失败。
`--env prod|production` 一律硬拒绝（无逃生开关）；任何注入前都要 `--confirm <场景名>`。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import DRILL_EVIDENCE_DIR, DRILL_EVIDENCE_DIR_ENV
from ops.drill import checklist as checklist_mod
from ops.drill import evidence as evidence_mod
from ops.drill.engine import execute, load_scenario, plan_lines, reset_work_dir, scenario_names
from ops.drill.env import (
    DrillConfig,
    DrillError,
    backup_evidence,
    load_config,
    load_scenarios,
    reject_production,
    render_criteria_rows,
    render_scenario_rows,
    require_confirmation,
    validate_scenarios,
)
from ops.drill.model import ScenarioSet
from ops.drill.record import write_reports
from ops.drill.release import run_release
from ops.drill.runtime import now_iso

__all__ = ["main"]

_EXIT_PARAM = 2
"""参数 / 生产拒绝的退出码（与「降级通过」同码：都没产生有效演练）。"""


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.drill", description="msg 六类故障注入演练与判据对账")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="执行一个场景")
    run.add_argument("scenario", help=f"场景标识（{' / '.join(scenario_names())}）")
    run.add_argument("--confirm", dest="confirm", default=None, help="二次确认：必须等于场景标识")
    run.add_argument("--dry-run", action="store_true", help="只渲染注入与恢复动作，零执行零写盘")
    run.add_argument("--rerun-reason", dest="rerun_reason", default="", help="复跑原因（写入记录 rerun.reason）")

    sub.add_parser("plan", help="渲染场景单源与六类场景表（不启动实例）")
    sub.add_parser("cleanup", help="清理演练工作根（幂等）")

    criteria = sub.add_parser("criteria", help="渲染判据表（六类逐条：级别 / 记录键 / 证据要求）")
    criteria.add_argument("--json", action="store_true", help="输出 JSON（机器对账用）")

    checklist = sub.add_parser("checklist", help="生成或校验发版前检查清单")
    checklist.add_argument("--check", action="store_true", help="只校验仓内清单是否与单源一致")

    verify = sub.add_parser("verify", help="对账六类记录并生成「场景 × 判据 × 证据」矩阵")
    verify.add_argument("--records", action="append", required=True, help="记录目录（可重复）")
    verify.add_argument("--include", action="append", default=[], help="补充证据记录（07-3 / 07-4，可重复）")
    verify.add_argument("--out", default=None, help="矩阵输出目录（缺省 MSG_DRILL_EVIDENCE_DIR）")
    verify.add_argument("--archive", default=None, help="把参与对账的记录复制到该目录（其下建 记录/）")
    verify.add_argument("--check-doc", default=None, help="额外检查该 Markdown 的相对链接")

    release = sub.add_parser("release", help="发版前复跑六类场景并汇总")
    release.add_argument("--confirm", dest="confirm", default=None, help="二次确认：必须等于 release")
    release.add_argument("--rerun-reason", dest="rerun_reason", default="发版前复跑", help="复跑原因")
    release.add_argument("--dry-run", action="store_true", help="只渲染六类动作，零执行零写盘")
    return parser


def _service_versions() -> tuple[str, str]:
    """取服务版本与契约版本。

    Returns:
        tuple[str, str]: 服务版本、契约版本。
    """
    from bms_msg import CONTRACT_VERSION, __version__

    return __version__, CONTRACT_VERSION


def _backup_record(config: DrillConfig) -> ConcurrentStableFrozenDict[str, str]:
    """演练前备份证据（兼容旧调用点；实现收在 `env.backup_evidence`）。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 备份证据。

    Raises:
        DrillError: 非隔离环境且未给备份证据。
    """
    return backup_evidence(config)


def _scenario_set(config: DrillConfig) -> ScenarioSet:
    """读取并校验场景单源。

    Args:
        config: 演练配置。

    Returns:
        ScenarioSet: 已校验场景集。

    Raises:
        DrillError: 单源非法。
    """
    scenario_set, errors = load_scenarios(config.repo_root / "deploy" / "drill" / "scenarios.toml")
    problems: ConcurrentStableList[str] = ConcurrentStableList(errors)
    problems.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if problems:
        raise DrillError("drill.scenario 场景单源非法：" + "；".join(problems))
    return scenario_set


def _run(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `run` 子命令。"""
    scenario = load_scenario(config, args.scenario)
    require_confirmation(scenario.name, args.confirm)
    if args.dry_run:
        print(f"[PLAN] {scenario.name} · {scenario.title}（dry-run，零执行）")
        for line in plan_lines(config, scenario):
            print(f"  {line}")
        return 0
    backup = _backup_record(config)
    report = replace(execute(config, scenario, rerun_reason=args.rerun_reason), backup=backup)
    service_version, contract_version = _service_versions()
    json_path, markdown_path = write_reports(
        report,
        config.report_dir,
        title=scenario.title,
        service_version=service_version,
        contract_version=contract_version,
    )
    print(f"[{report.conclusion}] {scenario.title}（退出码 {report.exit_code}，恢复 {report.rto_ms} ms）")
    for reason in report.reasons:
        print(f"  - {reason}")
    print(f"  记录：{json_path} ｜ {markdown_path}")
    return report.exit_code


def _plan(config: DrillConfig) -> int:
    """执行 `plan` 子命令：校验场景单源并渲染六类表。"""
    scenario_set, errors = load_scenarios(config.repo_root / "deploy" / "drill" / "scenarios.toml")
    problems: ConcurrentStableList[str] = ConcurrentStableList(errors)
    problems.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if problems:
        for item in problems:
            print(f"[FAIL] {item}")
        return 1
    print(
        f"场景单源：schema={scenario_set.schema_version}、核对 {scenario_set.updated_at}、责任人 {scenario_set.owner}"
    )
    for row in render_scenario_rows(scenario_set):
        print(row)
    return 0


def _criteria(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `criteria` 子命令：渲染判据表。"""
    scenario_set = _scenario_set(config)
    if args.json:
        rows = [
            {
                "scenario": scenario.name,
                "key": spec.key,
                "text": spec.text,
                "severity": spec.severity,
                "record_keys": list(spec.record_keys),
                "evidence": spec.evidence,
            }
            for scenario in scenario_set.scenarios
            for spec in scenario.criterion
        ]
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0
    print(f"判据表：schema={scenario_set.schema_version}、核对 {scenario_set.updated_at}")
    for row in render_criteria_rows(scenario_set):
        print(row)
    return 0


def _checklist(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `checklist` 子命令：生成或校验发版前检查清单。"""
    scenario_set = _scenario_set(config)
    path = checklist_mod.checklist_path(config.repo_root)
    if args.check:
        errors = checklist_mod.check_checklist(scenario_set, path)
        for item in errors:
            print(f"[FAIL] {item}")
        if errors:
            return 1
        print(f"[OK] 检查清单与场景单源一致：{path}")
        return 0
    checklist_mod.write_checklist(scenario_set, path)
    print(f"[OK] 已生成检查清单：{path}")
    return 0


def _evidence_dir(config: DrillConfig, raw: str | None) -> Path:
    """取矩阵输出目录（显式参数 > 环境变量 > 缺省）。"""
    value = (raw or os.environ.get(DRILL_EVIDENCE_DIR_ENV) or DRILL_EVIDENCE_DIR).strip()
    return (config.repo_root / value).resolve()


def _verify(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `verify` 子命令：对账记录并写矩阵。"""
    scenario_set = _scenario_set(config)
    records = [Path(item).expanduser().resolve() for item in args.records]
    includes = [Path(item).expanduser().resolve() for item in args.include]
    report = evidence_mod.verify(
        scenario_set,
        ConcurrentStableFrozenList(records),
        ConcurrentStableFrozenList(includes),
    )
    out_dir = _evidence_dir(config, args.out)
    report = replace(
        report,
        record_dirs=ConcurrentStableFrozenList(
            Path(os.path.relpath(Path(item), out_dir)).as_posix() for item in report.record_dirs
        ),
    )
    links = ConcurrentStableFrozenDict(
        {
            item.scenario: Path(os.path.relpath(item.record, out_dir)).as_posix()
            for item in report.scenarios
            if item.record != Path("")
        }
    )
    json_path, markdown_path = evidence_mod.write_outputs(report, out_dir, record_links=links)
    print(f"[{report.conclusion}] 场景 × 判据 × 证据（退出码 {report.exit_code}）")
    for reason in report.reasons:
        print(f"  - {reason}")
    print(f"  矩阵：{markdown_path} ｜ {json_path}")
    if args.archive:
        try:
            archived = evidence_mod.archive_records(report, Path(args.archive).expanduser().resolve())
        except OSError as exc:
            print(f"[FAIL] drill.evidence 归档失败：{exc}")
            return 1
        print(f"  已归档 {len(archived)} 份记录：{Path(args.archive).expanduser().resolve() / '记录'}")
    if args.check_doc:
        doc = Path(args.check_doc).expanduser().resolve()
        missing = evidence_mod.check_doc_links(doc)
        for item in missing:
            print(f"[FAIL] drill.evidence 链接失效：{item}（{doc}）")
        if missing:
            return 1
        print(f"  链接校验通过：{doc}")
    return report.exit_code


def _release(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `release` 子命令：发版前复跑六类并汇总。"""
    scenario_set = _scenario_set(config)
    require_confirmation("release", args.confirm)
    if args.dry_run:
        print("[PLAN] release（dry-run，零执行）")
        for scenario in scenario_set.scenarios:
            print(f"  [{scenario.name}] {scenario.title}")
            for line in plan_lines(config, scenario):
                print(f"    {line}")
        return 0
    report = run_release(
        config,
        scenario_set,
        rerun_reason=args.rerun_reason,
        backup=backup_evidence(config),
        service_version=_service_versions()[0],
        contract_version=_service_versions()[1],
    )
    for step in report.steps:
        print(f"  [{step.conclusion}] {step.scenario}（退出码 {step.exit_code}，恢复 {step.rto_ms} ms）")
    print(f"[{report.conclusion}] 发版前复跑（退出码 {report.exit_code}）")
    for reason in report.reasons:
        print(f"  - {reason}")
    print(f"  记录：{config.report_dir / 'release.json'} ｜ {config.report_dir / 'release.md'}")
    return report.exit_code


def _cleanup(config: DrillConfig) -> int:
    """执行 `cleanup` 子命令：删工作根下的运行时产物与填充文件（幂等）。"""
    reset_work_dir(config)
    for name in ("fill", "slowdisk"):
        shutil.rmtree(config.work_dir / name, ignore_errors=True)
    print(f"[OK] 已清理 {config.work_dir}（{now_iso()}）")
    return 0


def main(argv: list[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    args = _parser().parse_args(argv)
    try:
        config = load_config()
        reject_production(config.env)
        if args.command == "plan":
            return _plan(config)
        if args.command == "cleanup":
            return _cleanup(config)
        if args.command == "criteria":
            return _criteria(config, args)
        if args.command == "checklist":
            return _checklist(config, args)
        if args.command == "verify":
            return _verify(config, args)
        if args.command == "release":
            return _release(config, args)
        return _run(config, args)
    except DrillError as exc:
        print(f"[FAIL] {exc}")
        return _EXIT_PARAM


if __name__ == "__main__":
    sys.exit(main())
