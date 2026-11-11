"""发版前复跑（任务 07-6-2）：六类场景顺序执行 → 记录 → 汇总验证 → 发版记录。

失败不中断：任一场景异常都要留下 `aborted` 记录，六类跑完再按最差结论退出；
`--env prod` 与缺 `--confirm release` 在进入本模块前已被拒绝。
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import DRILL_EVIDENCE_SCHEMA_VERSION
from ops.drill.capture import config_digest, sha256_file
from ops.drill.engine import execute
from ops.drill.env import DrillConfig
from ops.drill.evidence import EvidenceReport, plain, verify, write_outputs
from ops.drill.model import (
    CONCLUSION_DEGRADED,
    CONCLUSION_FAIL,
    CONCLUSION_PASS,
    DrillReport,
    ScenarioSet,
)
from ops.drill.record import write_reports

__all__ = ["ReleaseReport", "ReleaseStep", "git_commit", "run_release", "write_release"]

_RANK: ConcurrentStableFrozenDict[str, int] = ConcurrentStableFrozenDict(
    {CONCLUSION_PASS: 0, CONCLUSION_DEGRADED: 1, CONCLUSION_FAIL: 2}
)
"""结论严重度（用于取最差）。"""


@dataclass(frozen=True)
class ReleaseStep:
    """一次发版复跑中的单个场景。"""

    scenario: str
    """场景标识。"""

    title: str
    """场景标题。"""

    conclusion: str
    """场景结论。"""

    exit_code: int
    """场景退出码。"""

    rto_ms: int
    """恢复耗时。"""

    record: Path
    """记录 JSON 路径。"""

    sha256: str
    """记录 JSON 的 sha256。"""


@dataclass(frozen=True)
class ReleaseReport:
    """发版前复跑记录（JSON / Markdown 同源）。"""

    schema_version: int
    """格式版本。"""

    task: str
    """任务标识（固定 07-6-2）。"""

    mode: str
    """固定 `release`。"""

    environment: ConcurrentStableFrozenDict[str, object]
    """环境与配置摘要（env / target / os / python / config_digest / git_commit）。"""

    service: ConcurrentStableFrozenDict[str, str]
    """服务版本与契约版本。"""

    started_at: str
    """开始时刻（UTC ISO8601）。"""

    finished_at: str
    """结束时刻（UTC ISO8601）。"""

    rerun_reason: str
    """复跑原因。"""

    steps: ConcurrentStableFrozenList[ReleaseStep]
    """六类场景结果。"""

    verification: EvidenceReport
    """矩阵验证结果。"""

    conclusion: str
    """汇总结论。"""

    exit_code: int
    """退出码 0 / 2 / 1。"""

    requires_review: bool
    """是否需要人工复核。"""

    reasons: ConcurrentStableFrozenList[str]
    """未通过 / 降级原因。"""


def git_commit(repo_root: Path) -> str:
    """取当前提交（git 不可用时返回空串，不阻断演练）。

    Args:
        repo_root: 仓库根。

    Returns:
        str: 提交哈希（取不到为空）。
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except OSError, subprocess.SubprocessError:
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _write_release_record(
    config: DrillConfig,
    scenario_set: ScenarioSet,
    *,
    started_at: datetime,
    rerun_reason: str,
    backup: ConcurrentStableFrozenDict[str, str],
    service_version: str,
    contract_version: str,
) -> tuple[ConcurrentStableList[ReleaseStep], ConcurrentStableList[DrillReport]]:
    """顺序执行六类场景并写记录（失败不中断）。"""
    steps: ConcurrentStableList[ReleaseStep] = ConcurrentStableList()
    reports: ConcurrentStableList[DrillReport] = ConcurrentStableList()
    for scenario in scenario_set.scenarios:
        report = replace(execute(config, scenario, rerun_reason=rerun_reason), backup=backup)
        reports.add(report)
        json_path, _ = write_reports(
            report,
            config.report_dir,
            title=scenario.title,
            service_version=service_version,
            contract_version=contract_version,
        )
        steps.add(
            ReleaseStep(
                scenario=scenario.name,
                title=scenario.title,
                conclusion=report.conclusion,
                exit_code=report.exit_code,
                rto_ms=report.rto_ms,
                record=json_path,
                sha256=sha256_file(json_path),
            )
        )
    return steps, reports


def _worst(steps: ConcurrentStableFrozenList[ReleaseStep], verification: EvidenceReport) -> str:
    """取场景与验证结论中的最差者。"""
    worst = verification.conclusion
    for step in steps:
        if _RANK.get(step.conclusion, 2) > _RANK.get(worst, 2):
            worst = step.conclusion
    return worst


def run_release(
    config: DrillConfig,
    scenario_set: ScenarioSet,
    *,
    rerun_reason: str,
    backup: ConcurrentStableFrozenDict[str, str],
    service_version: str,
    contract_version: str,
) -> ReleaseReport:
    """发版前复跑六类场景并写发版记录。

    Args:
        config: 演练配置。
        scenario_set: 已校验场景集。
        rerun_reason: 复跑原因。
        backup: 演练前备份证据。
        service_version: 服务版本。
        contract_version: 契约版本。

    Returns:
        ReleaseReport: 发版复跑记录。
    """
    started_at = datetime.now(UTC)
    steps, _ = _write_release_record(
        config,
        scenario_set,
        started_at=started_at,
        rerun_reason=rerun_reason,
        backup=backup,
        service_version=service_version,
        contract_version=contract_version,
    )
    verification = verify(
        scenario_set,
        ConcurrentStableFrozenList((config.report_dir,)),
        ConcurrentStableFrozenList(),
    )
    out_dir = config.report_dir
    links = ConcurrentStableFrozenDict(
        {
            item.scenario: Path(os.path.relpath(item.record, out_dir)).as_posix()
            for item in verification.scenarios
            if item.record != Path("")
        }
    )
    write_outputs(verification, out_dir, record_links=links)
    conclusion = _worst(ConcurrentStableFrozenList(steps), verification)
    exit_code = 1 if conclusion == CONCLUSION_FAIL else (2 if conclusion == CONCLUSION_DEGRADED else 0)
    report = ReleaseReport(
        schema_version=DRILL_EVIDENCE_SCHEMA_VERSION,
        task="07-6-2",
        mode="release",
        environment=ConcurrentStableFrozenDict(
            {
                "env": config.env,
                "target": "local",
                "os": os.name,
                "python": platform.python_version(),
                "config_digest": config_digest(config, "release"),
                "git_commit": git_commit(config.repo_root),
            }
        ),
        service=ConcurrentStableFrozenDict(
            {"name": "msg", "version": service_version, "contract_version": contract_version}
        ),
        started_at=started_at.isoformat(),
        finished_at=datetime.now(UTC).isoformat(),
        rerun_reason=rerun_reason,
        steps=ConcurrentStableFrozenList(steps),
        verification=verification,
        conclusion=conclusion,
        exit_code=exit_code,
        requires_review=conclusion != CONCLUSION_PASS,
        reasons=verification.reasons,
    )
    write_release(report, config.report_dir)
    return report


def _document(report: ReleaseReport) -> ConcurrentStableDict[str, object]:
    """构造发版记录 JSON 文档。"""
    steps: ConcurrentStableList[object] = ConcurrentStableList()
    for step in report.steps:
        steps.add(
            ConcurrentStableDict(
                {
                    "scenario": step.scenario,
                    "title": step.title,
                    "conclusion": step.conclusion,
                    "exit_code": step.exit_code,
                    "rto_ms": step.rto_ms,
                    "record": step.record.name,
                    "sha256": step.sha256,
                }
            )
        )
    return ConcurrentStableDict(
        {
            "schema_version": report.schema_version,
            "task": report.task,
            "mode": report.mode,
            "environment": ConcurrentStableDict(dict(report.environment)),
            "service": ConcurrentStableDict(dict(report.service)),
            "started_at": report.started_at,
            "finished_at": report.finished_at,
            "rerun_reason": report.rerun_reason,
            "steps": steps,
            "verification": ConcurrentStableDict(
                {
                    "result": report.verification.conclusion,
                    "exit_code": report.verification.exit_code,
                    "requires_review": report.verification.requires_review,
                    "reasons": ConcurrentStableList(report.verification.reasons),
                }
            ),
            "conclusion": ConcurrentStableDict(
                {
                    "result": report.conclusion,
                    "exit_code": report.exit_code,
                    "requires_review": report.requires_review,
                    "reasons": ConcurrentStableList(report.reasons),
                }
            ),
        }
    )


def write_release(report: ReleaseReport, directory: Path) -> tuple[Path, Path]:
    """写发版记录 JSON 与 Markdown（同源）。

    Args:
        report: 发版复跑记录。
        directory: 输出目录。

    Returns:
        tuple[Path, Path]: JSON 路径与 Markdown 路径。
    """
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "release.json"
    markdown_path = directory / "release.md"
    json_path.write_text(json.dumps(plain(_document(report)), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("# 发版前故障演练记录")
    rows.add("")
    rows.add(f"- 任务：`{report.task}` / 模式 `{report.mode}`")
    rows.add(f"- 起止：`{report.started_at}` → `{report.finished_at}`")
    rows.add(f"- 环境：`{report.environment.get('env')}` / 目标 `{report.environment.get('target')}`")
    rows.add(f"- 配置摘要：`{report.environment.get('config_digest')}`")
    rows.add(f"- git 提交：`{report.environment.get('git_commit') or '未取到'}`")
    rows.add(f"- 复跑原因：{report.rerun_reason}")
    rows.add(f"- **汇总结论：{report.conclusion}**（退出码 {report.exit_code}）")
    rows.add("")
    rows.add("## 1. 场景结果")
    rows.add("")
    rows.add("| 场景 | 结论 | 退出码 | 恢复耗时 | 记录 | sha256 |")
    rows.add("| --- | --- | --- | --- | --- | --- |")
    for step in report.steps:
        rows.add(
            f"| `{step.scenario}` | {step.conclusion} | {step.exit_code} | {step.rto_ms} ms | "
            f"`{step.record.name}` | `{step.sha256}` |"
        )
    rows.add("")
    rows.add("## 2. 验证结论")
    rows.add("")
    rows.add(f"- 矩阵结论：{report.verification.conclusion}（退出码 {report.verification.exit_code}）")
    rows.add(f"- 需人工复核：{'是' if report.verification.requires_review else '否'}")
    if report.reasons:
        rows.add("")
        rows.add("未通过 / 降级原因：")
        rows.add("")
        for reason in report.reasons:
            rows.add(f"- {reason}")
    rows.add("")
    markdown_path.write_text("\n".join(rows), encoding="utf-8")
    return json_path, markdown_path
