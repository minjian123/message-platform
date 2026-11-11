"""演练命令行（任务 07-6-1）：`run` / `plan` / `cleanup` 三个子命令。

退出码与结论同口径：`0` 通过、`2` 降级通过（或参数 / 生产拒绝）、`1` 失败。
`--env prod|production` 一律硬拒绝（无逃生开关）；任何注入前都要 `--confirm <场景名>`。
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from dataclasses import replace

from bms_core.core.concurrent import ConcurrentStableFrozenDict

from ops.drill.engine import execute, load_scenario, plan_lines, reset_work_dir, scenario_names
from ops.drill.env import (
    DrillConfig,
    DrillError,
    load_config,
    load_scenarios,
    reject_production,
    render_scenario_rows,
    require_confirmation,
    validate_scenarios,
)
from ops.drill.record import write_reports
from ops.drill.runtime import now_iso

__all__ = ["main"]

_EXIT_PARAM = 2
"""参数 / 生产拒绝的退出码（与「降级通过」同码：都没产生有效演练）。"""

_ISOLATED_ENVS = ("test", "dev", "local")
"""视为隔离环境的标识（演练前备份记 `not_applicable`）。"""


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.drill", description="msg 六类故障注入演练")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="执行一个场景")
    run.add_argument("scenario", help=f"场景标识（{' / '.join(scenario_names())}）")
    run.add_argument("--confirm", dest="confirm", default=None, help="二次确认：必须等于场景标识")
    run.add_argument("--dry-run", action="store_true", help="只渲染注入与恢复动作，零执行零写盘")

    sub.add_parser("plan", help="渲染场景单源与六类场景表（不启动实例）")
    sub.add_parser("cleanup", help="清理演练工作根（幂等）")
    return parser


def _service_versions() -> tuple[str, str]:
    """取服务版本与契约版本。

    Returns:
        tuple[str, str]: 服务版本、契约版本。
    """
    from bms_msg import CONTRACT_VERSION, __version__

    return __version__, CONTRACT_VERSION


def _backup_record(config: DrillConfig) -> ConcurrentStableFrozenDict[str, str]:
    """演练前备份的证据口径：隔离实例记不适用，非隔离必须给证据。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 备份证据（`mode` / `reason`）。

    Raises:
        DrillError: 非隔离环境且未给备份证据。
    """
    evidence = (os.environ.get("MSG_DRILL_BACKUP_EVIDENCE") or "").strip()
    if config.env in _ISOLATED_ENVS:
        return ConcurrentStableFrozenDict(
            {
                "mode": "not_applicable",
                "reason": "隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据",
            }
        )
    if not evidence:
        raise DrillError(f"drill.guard 非隔离环境（{config.env}）必须给备份证据：MSG_DRILL_BACKUP_EVIDENCE")
    return ConcurrentStableFrozenDict({"mode": "provided", "reason": evidence})


def _run(config: DrillConfig, args: argparse.Namespace) -> int:
    """执行 `run` 子命令。

    Args:
        config: 演练配置。
        args: 命令行参数。

    Returns:
        int: 退出码。
    """
    scenario = load_scenario(config, args.scenario)
    require_confirmation(scenario.name, args.confirm)
    if args.dry_run:
        print(f"[PLAN] {scenario.name} · {scenario.title}（dry-run，零执行）")
        for line in plan_lines(config, scenario):
            print(f"  {line}")
        return 0
    backup = _backup_record(config)
    report = replace(execute(config, scenario), backup=backup)
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
    """执行 `plan` 子命令：校验场景单源并渲染六类表。

    Args:
        config: 演练配置。

    Returns:
        int: 退出码。
    """
    scenario_set, errors = load_scenarios(config.repo_root / "deploy" / "drill" / "scenarios.toml")
    all_errors: list[str] = list(errors)
    all_errors.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if all_errors:
        for item in all_errors:
            print(f"[FAIL] {item}")
        return 1
    print(
        f"场景单源：schema={scenario_set.schema_version}、核对 {scenario_set.updated_at}、责任人 {scenario_set.owner}"
    )
    for row in render_scenario_rows(scenario_set):
        print(row)
    return 0


def _cleanup(config: DrillConfig) -> int:
    """执行 `cleanup` 子命令：删工作根下的运行时产物与填充文件（幂等）。

    Args:
        config: 演练配置。

    Returns:
        int: 退出码。
    """
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
        return _run(config, args)
    except DrillError as exc:
        print(f"[FAIL] {exc}")
        return _EXIT_PARAM


if __name__ == "__main__":
    sys.exit(main())
