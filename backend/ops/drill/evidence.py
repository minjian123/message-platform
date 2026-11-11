"""验收矩阵（任务 07-6-2）：记录对账、硬 / 可降级判定、复跑闭环与同源渲染。

一条判据是否通过，只认两处事实：`deploy/drill/scenarios.toml` 的判据声明，以及
07-6-1 记录里的 `criteria[]` / `samples`。本模块不做注入、不猜结论；缺证据就记降级，
硬判据不过就记不通过，绝不默认放行。

结论三态与 07-3-2 / 07-4-1 / 07-4-2 / 07-6-1 同口径：通过 0 / 降级通过 2 / 失败 1。
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from bms_msg.core.config import DRILL_EVIDENCE_SCHEMA_VERSION
from ops.drill.model import (
    CONCLUSION_DEGRADED,
    CONCLUSION_FAIL,
    CONCLUSION_PASS,
    RESULT_DEGRADED,
    RESULT_FAILED,
    RESULT_OK,
    RESULT_SKIPPED,
    SEVERITY_DEGRADABLE,
    DrillCriterionSpec,
    DrillScenario,
    ScenarioSet,
)

__all__ = [
    "CHECK_DEGRADED",
    "CHECK_FAILED",
    "CHECK_OK",
    "VERDICT_DEGRADED",
    "VERDICT_FAIL",
    "VERDICT_PASS",
    "Check",
    "CriterionVerdict",
    "EvidenceReport",
    "ScenarioEvidence",
    "SupplementaryEvidence",
    "archive_records",
    "check_doc_links",
    "plain",
    "render_json",
    "render_markdown",
    "verify",
    "write_outputs",
]

CHECK_OK = "ok"
"""校验结果：通过。"""

CHECK_DEGRADED = "degraded"
"""校验结果：降级（证据不全 / 已归口但未闭环）。"""

CHECK_FAILED = "failed"
"""校验结果：失败。"""

VERDICT_PASS = "通过"
"""场景 / 判据判定：通过。"""

VERDICT_DEGRADED = "降级"
"""场景 / 判据判定：降级（不判失败，但不计通过）。"""

VERDICT_FAIL = "不通过"
"""场景 / 判据判定：不通过。"""

_GLOBAL_KEYS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("rto_within_budget",))
"""全局判据的等价记录键（任何场景都允许出现）。"""

_ISOLATED_ENVS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("test", "dev", "local"))
"""隔离环境标识（备份证据记 `not_applicable`）。"""

_REQUIRED_TOP: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        "schema_version",
        "task",
        "scenario",
        "mode",
        "environment",
        "service",
        "started_at",
        "finished_at",
        "injection",
        "samples",
        "criteria",
        "rto_ms",
        "data_loss",
        "backup",
        "conclusion",
        "deviations",
        "issues",
        "leftovers",
    )
)
"""记录必须出现的顶层字段。"""

_REQUIRED_ENV: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("env", "target", "os", "python"))
"""记录 `environment` 必须出现的字段。"""

_REQUIRED_SERVICE: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("name", "version", "contract_version"))
"""记录 `service` 必须出现的字段。"""

_PHASES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("before", "during", "after"))
"""三段观测的固定阶段名。"""

_ENHANCED_FIELDS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ("commands", "artifacts", "logs", "rerun")
)
"""记录 schema 2 新增字段（v1 记录缺这些只记降级）。"""


@dataclass(frozen=True)
class Check:
    """一条校验结果。"""

    code: str
    """校验编号（`G1` / `G2` … 或 `criteria.<key>`）。"""

    level: str
    """`ok` / `degraded` / `failed`。"""

    message: str
    """可读说明。"""

    owner: str = ""
    """未通过时的归口（可空）。"""


@dataclass(frozen=True)
class CriterionVerdict:
    """一条判据的判定。"""

    key: str
    """判据标识（场景单源）。"""

    text: str
    """判据原文。"""

    severity: str
    """`hard` / `degradable`。"""

    verdict: str
    """`通过` / `降级` / `不通过`。"""

    record_keys: ConcurrentStableFrozenList[str]
    """记录键。"""

    actual: str
    """实测摘要（逐键拼接）。"""

    evidence: str
    """证据要求。"""


@dataclass(frozen=True)
class ScenarioEvidence:
    """一个场景的证据与判定。"""

    scenario: str
    """场景标识。"""

    title: str
    """场景标题。"""

    record: Path
    """最新记录路径。"""

    conclusion: str
    """记录自带结论。"""

    exit_code: int
    """记录退出码。"""

    rto_ms: int
    """恢复耗时。"""

    budget_ms: int
    """恢复预算。"""

    data_loss: bool
    """是否丢失已确认消息。"""

    verdict: str
    """判定结论。"""

    criteria: ConcurrentStableFrozenList[CriterionVerdict]
    """逐条判据。"""

    checks: ConcurrentStableFrozenList[Check]
    """全局 / 结构校验。"""


@dataclass(frozen=True)
class SupplementaryEvidence:
    """补充证据（07-3 / 07-4 记录，单列不合并统计）。"""

    task: str
    """任务标识。"""

    mode: str
    """`drill` / `live` / `check`。"""

    result: str
    """记录结论。"""

    exit_code: int
    """记录退出码。"""

    data_integrity: str
    """数据完整性（无该字段时为空）。"""

    isolated: bool
    """是否隔离环境。"""

    issues: int
    """问题条数。"""

    leftovers: int
    """遗留条数。"""

    record: Path
    """记录路径。"""


@dataclass(frozen=True)
class EvidenceReport:
    """验收矩阵的完整结果（JSON / Markdown 同源）。"""

    schema_version: int
    """矩阵格式版本。"""

    task: str
    """任务标识（固定 07-6-2）。"""

    generated_at: str
    """生成时刻（UTC ISO8601）。"""

    record_dirs: ConcurrentStableFrozenList[str]
    """参与对账的记录目录（相对仓根或绝对路径）。"""

    scenarios: ConcurrentStableFrozenList[ScenarioEvidence]
    """六类场景证据。"""

    supplementary: ConcurrentStableFrozenList[SupplementaryEvidence]
    """补充证据。"""

    checks: ConcurrentStableFrozenList[Check]
    """全局校验（G1~G12）。"""

    conclusion: str
    """汇总结论。"""

    exit_code: int
    """退出码 0 / 2 / 1。"""

    requires_review: bool
    """是否需要人工复核（降级通过为真）。"""

    reasons: ConcurrentStableFrozenList[str]
    """未通过 / 降级原因。"""

    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """未闭环与遗留（含归口）。"""


def _text(value: object) -> str:
    """取字符串（非字符串记空串）。"""
    return value if isinstance(value, str) else ""


def _number(value: object) -> int:
    """取整数（非数值记 0）。"""
    if isinstance(value, bool):
        return 0
    return int(value) if isinstance(value, int | float) else 0


def _flag(value: object) -> bool:
    """取布尔（非布尔记 False）。"""
    return value if isinstance(value, bool) else False


def _mapping(value: object) -> ConcurrentStableFrozenDict[str, object]:
    """把 JSON 子结构收窄成冻结映射（非映射记空）。"""
    if not isinstance(value, Mapping):
        return ConcurrentStableFrozenDict()
    return ConcurrentStableFrozenDict(dict(cast("Mapping[str, object]", value)))


def _items(value: object) -> ConcurrentStableFrozenList[object]:
    """把 JSON 数组收窄成冻结列表（非数组记空）。"""
    if not isinstance(value, list):
        return ConcurrentStableFrozenList()
    return ConcurrentStableFrozenList(cast("list[object]", value))


def _check(code: str, level: str, message: str, owner: str = "") -> Check:
    """构造一条校验结果。"""
    return Check(code=code, level=level, message=message, owner=owner)


def _result_verdict(result: str, severity: str) -> str:
    """把单条记录结果转成判据判定（硬判据非 ok 即不通过）。"""
    if result == RESULT_OK:
        return VERDICT_PASS
    if result in (RESULT_DEGRADED, RESULT_SKIPPED):
        return VERDICT_DEGRADED if severity == SEVERITY_DEGRADABLE else VERDICT_FAIL
    return VERDICT_FAIL


def _worst(verdicts: ConcurrentStableFrozenList[str]) -> str:
    """取最差判定（不通过 > 降级 > 通过）。"""
    if VERDICT_FAIL in verdicts:
        return VERDICT_FAIL
    if VERDICT_DEGRADED in verdicts:
        return VERDICT_DEGRADED
    return VERDICT_PASS


def _record_paths(directory: Path) -> ConcurrentStableFrozenList[Path]:
    """列出目录下的候选记录（只取 `*.json`，忽略矩阵 / 发版记录）。"""
    if not directory.is_dir():
        return ConcurrentStableFrozenList()
    items: ConcurrentStableList[Path] = ConcurrentStableList()
    for path in sorted(directory.glob("*.json")):
        if path.name.startswith(("evidence", "release")):
            continue
        items.add(path)
    return ConcurrentStableFrozenList(items)


def _load(path: Path) -> ConcurrentStableFrozenDict[str, object] | None:
    """读取记录 JSON（不可解析返回 None，由调用方记失败）。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return None
    if not isinstance(raw, dict):
        return None
    return ConcurrentStableFrozenDict(dict(cast("dict[str, object]", raw)))


def _pick_records(
    scenarios: ScenarioSet, record_dirs: ConcurrentStableFrozenList[Path]
) -> tuple[ConcurrentStableDict[str, ConcurrentStableFrozenList[Path]], ConcurrentStableList[Check]]:
    """按场景归集记录（同场景多份按 `started_at` 升序；解析失败也留痕）。"""
    names: ConcurrentStableDict[str, ConcurrentStableList[Path]] = ConcurrentStableDict()
    for scenario in scenarios.scenarios:
        names.set(scenario.name, ConcurrentStableList())
    checks: ConcurrentStableList[Check] = ConcurrentStableList()
    for directory in record_dirs:
        if not directory.is_dir():
            checks.add(_check("G1", CHECK_FAILED, f"记录目录不存在：{directory}"))
            continue
        for path in _record_paths(directory):
            record = _load(path)
            if record is None:
                checks.add(_check("G1", CHECK_FAILED, f"记录不可解析：{path.name}"))
                continue
            name = _text(record.get("scenario"))
            bucket = names.get(name)
            if bucket is None:
                continue
            bucket.add(path)
    ordered: ConcurrentStableDict[str, ConcurrentStableFrozenList[Path]] = ConcurrentStableDict()
    for name, paths in names.items():
        ordered.set(name, ConcurrentStableFrozenList(sorted(paths, key=lambda item: _sort_key(item))))
    return ordered, checks


def _sort_key(path: Path) -> str:
    """按记录 `started_at` 排序（不可解析时用文件名兜底）。"""
    record = _load(path)
    if record is None:
        return path.name
    return _text(record.get("started_at")) or path.name


def _shape_checks(
    scenario: DrillScenario, record: ConcurrentStableFrozenDict[str, object]
) -> ConcurrentStableList[Check]:
    """校验记录结构与全局口径（G2~G8、G6）。"""
    checks: ConcurrentStableList[Check] = ConcurrentStableList()
    missing = [key for key in _REQUIRED_TOP if key not in record]
    checks.add(
        _check(
            "G2",
            CHECK_FAILED if missing else CHECK_OK,
            f"记录字段缺失：{'、'.join(missing)}" if missing else "记录字段完整",
        )
    )
    environment = _mapping(record.get("environment"))
    env_missing = [key for key in _REQUIRED_ENV if not _text(environment.get(key))]
    checks.add(
        _check(
            "G8",
            CHECK_FAILED if env_missing else CHECK_OK,
            f"environment 缺字段：{'、'.join(env_missing)}" if env_missing else "environment 完整",
        )
    )
    service = _mapping(record.get("service"))
    service_missing = [key for key in _REQUIRED_SERVICE if not _text(service.get(key))]
    checks.add(
        _check(
            "G8",
            CHECK_FAILED if service_missing else CHECK_OK,
            f"service 缺字段：{'、'.join(service_missing)}" if service_missing else "service 完整",
        )
    )
    started = _text(record.get("started_at"))
    finished = _text(record.get("finished_at"))
    checks.add(_check("G3", *_time_check(started, finished)))
    samples = _mapping(record.get("samples"))
    phase_missing = [phase for phase in _PHASES if not isinstance(samples.get(phase), Mapping)]
    checks.add(
        _check(
            "G3",
            CHECK_FAILED if phase_missing else CHECK_OK,
            f"三段样本缺失：{'、'.join(phase_missing)}" if phase_missing else "三段样本齐备",
        )
    )
    injection = _mapping(record.get("injection"))
    method = _text(injection.get("method"))
    checks.add(
        _check(
            "G2",
            CHECK_OK if method == scenario.inject else CHECK_FAILED,
            f"注入方式 {method or '空'}（期望 {scenario.inject}）",
        )
    )
    backup = _mapping(record.get("backup"))
    env = _text(environment.get("env"))
    backup_mode = _text(backup.get("mode"))
    backup_ok = backup_mode == "provided" if env not in _ISOLATED_ENVS else backup_mode == "not_applicable"
    checks.add(
        _check(
            "G7",
            CHECK_OK if backup_ok and _text(backup.get("reason")) else CHECK_FAILED,
            f"备份证据 {backup_mode or '空'}（{_text(backup.get('reason')) or '无理由'}）",
        )
    )
    rto = _number(record.get("rto_ms"))
    budget = scenario.recovery_budget_ms
    checks.add(
        _check(
            "G5",
            CHECK_OK if 0 <= rto <= budget else CHECK_FAILED,
            f"恢复 {rto} ms（预算 {budget} ms）",
        )
    )
    checks.add(
        _check(
            "G6",
            CHECK_FAILED if _flag(record.get("data_loss")) else CHECK_OK,
            "已确认消息丢失" if _flag(record.get("data_loss")) else "无已确认消息丢失",
        )
    )
    schema_version = _number(record.get("schema_version"))
    if schema_version < 2:
        checks.add(_check("G2", CHECK_DEGRADED, "记录为 v1（缺命令 / 日志 / 配置摘要），需复跑"))
    elif not _text(environment.get("config_digest")):
        checks.add(_check("G2", CHECK_DEGRADED, "记录缺配置摘要（config_digest）"))
    for field in _ENHANCED_FIELDS:
        if field not in record:
            checks.add(_check("G2", CHECK_DEGRADED, f"记录缺增强字段：{field}"))
    if _text(record.get("task")) != "07-6-1":
        checks.add(_check("G2", CHECK_FAILED, f"记录 task 非法：{_text(record.get('task'))}"))
    return checks


def _time_check(started: str, finished: str) -> tuple[str, str]:
    """校验起止时间（可解析且先后有序）。"""
    try:
        start = datetime.fromisoformat(started)
        end = datetime.fromisoformat(finished)
    except ValueError:
        return CHECK_FAILED, "起止时间不可解析"
    return (CHECK_OK, "起止时间有效") if end >= start else (CHECK_FAILED, "结束时间早于开始时间")


def _criteria_verdicts(
    scenario: DrillScenario, record: ConcurrentStableFrozenDict[str, object]
) -> tuple[ConcurrentStableFrozenList[CriterionVerdict], ConcurrentStableList[Check]]:
    """把记录 `criteria[]` 与单源判据双向对账并逐条判定。"""
    items = _items(record.get("criteria"))
    by_key: ConcurrentStableDict[str, ConcurrentStableFrozenDict[str, object]] = ConcurrentStableDict()
    for item in items:
        entry = _mapping(item)
        key = _text(entry.get("key"))
        if key:
            by_key.set(key, entry)
    rows: ConcurrentStableList[CriterionVerdict] = ConcurrentStableList()
    checks: ConcurrentStableList[Check] = ConcurrentStableList()
    used: ConcurrentStableSet[str] = ConcurrentStableSet()
    for spec in scenario.criterion:
        rows.add(_criterion_row(spec, by_key, used, checks))
    allowed: ConcurrentStableSet[str] = ConcurrentStableSet(_GLOBAL_KEYS)
    for spec in scenario.criterion:
        for key in spec.record_keys:
            allowed.add(key)
    for key in scenario.aux_record_keys:
        allowed.add(key)
    orphans = [key for key in by_key if key not in allowed and not key.endswith("rto_within_budget")]
    checks.add(
        _check(
            "G9",
            CHECK_FAILED if orphans else CHECK_OK,
            f"记录出现未登记判据键：{'、'.join(orphans)}" if orphans else "判据键均可归属单源",
        )
    )
    return ConcurrentStableFrozenList(rows), checks


def _criterion_row(
    spec: DrillCriterionSpec,
    by_key: ConcurrentStableDict[str, ConcurrentStableFrozenDict[str, object]],
    used: ConcurrentStableSet[str],
    checks: ConcurrentStableList[Check],
) -> CriterionVerdict:
    """判定单条判据（缺键即不通过；硬判据非 ok 即不通过）。"""
    verdicts: ConcurrentStableList[str] = ConcurrentStableList()
    actuals: ConcurrentStableList[str] = ConcurrentStableList()
    missing: ConcurrentStableList[str] = ConcurrentStableList()
    for key in spec.record_keys:
        used.add(key)
        entry = by_key.get(key)
        if entry is None:
            missing.add(key)
            verdicts.add(VERDICT_FAIL)
            continue
        result = _text(entry.get("result"))
        verdicts.add(_result_verdict(result, spec.severity))
        actuals.add(f"{key}={result or '空'}（{_text(entry.get('actual')) or '无实测'}）")
    if missing:
        checks.add(
            _check(
                f"criteria.{spec.key}",
                CHECK_FAILED,
                f"判据 {spec.key} 缺少记录键：{'、'.join(missing)}",
            )
        )
    return CriterionVerdict(
        key=spec.key,
        text=spec.text,
        severity=spec.severity,
        verdict=_worst(ConcurrentStableFrozenList(verdicts)),
        record_keys=spec.record_keys,
        actual="；".join(actuals) if actuals else "无实测",
        evidence=spec.evidence,
    )


def _conclusion_check(record: ConcurrentStableFrozenDict[str, object]) -> Check:
    """校验记录结论 / 退出码 / 判据汇总三者自洽（G4）。"""
    items = _items(record.get("criteria"))
    results: ConcurrentStableList[str] = ConcurrentStableList()
    for item in items:
        entry = _mapping(item)
        result = _text(entry.get("result"))
        results.add(result)
        if _flag(entry.get("passed")) != (result == RESULT_OK):
            return _check("G4", CHECK_FAILED, f"判据 {_text(entry.get('key')) or '?'} 的 passed 与 result 不一致")
    expected = (
        CONCLUSION_FAIL
        if RESULT_FAILED in results
        else (CONCLUSION_DEGRADED if (RESULT_DEGRADED in results or RESULT_SKIPPED in results) else CONCLUSION_PASS)
    )
    conclusion = _mapping(record.get("conclusion"))
    actual = _text(conclusion.get("result"))
    exit_code = _number(conclusion.get("exit_code"))
    expected_exit = 1 if expected == CONCLUSION_FAIL else (2 if expected == CONCLUSION_DEGRADED else 0)
    if actual != expected:
        return _check("G4", CHECK_FAILED, f"结论 {actual or '空'} 与判据汇总 {expected} 不一致")
    if exit_code != expected_exit:
        return _check("G4", CHECK_FAILED, f"退出码 {exit_code} 与结论 {actual} 不一致（期望 {expected_exit}）")
    return _check("G4", CHECK_OK, f"结论自洽：{actual}（退出码 {exit_code}）")


def _owner(record: ConcurrentStableFrozenDict[str, object]) -> str:
    """从 issues / leftovers 取第一条归口（owner 优先）。"""
    for field in ("issues", "leftovers"):
        for item in _items(record.get(field)):
            owner = _text(_mapping(item).get("owner"))
            if owner:
                return owner
    return ""


def _closure_checks(
    scenario: str,
    records: ConcurrentStableFrozenList[Path],
    checks: ConcurrentStableList[Check],
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]],
) -> None:
    """校验复跑闭环（G11）：历史非通过要有后继记录，最新非通过要有归口。"""
    loaded: ConcurrentStableList[ConcurrentStableFrozenDict[str, object]] = ConcurrentStableList()
    for path in records:
        record = _load(path)
        if record is not None:
            loaded.add(record)
    for index, record in enumerate(loaded):
        conclusion = _text(_mapping(record.get("conclusion")).get("result"))
        if conclusion == CONCLUSION_PASS:
            continue
        owner = _owner(record)
        if index + 1 < len(loaded):
            successor = _mapping(loaded[index + 1].get("rerun"))
            expected = _text(record.get("started_at"))
            if _text(successor.get("previous_started_at")) != expected:
                checks.add(
                    _check(
                        "G11",
                        CHECK_FAILED,
                        f"{scenario} 的 {conclusion} 记录没有后继复跑（期望 rerun.previous_started_at={expected}）",
                    )
                )
                continue
            checks.add(_check("G11", CHECK_OK, f"{scenario} 的 {conclusion} 记录已复跑"))
            continue
        if not owner:
            checks.add(_check("G11", CHECK_FAILED, f"{scenario} 最新记录为 {conclusion} 且未登记归口"))
            continue
        checks.add(_check("G11", CHECK_DEGRADED, f"{scenario} 最新记录为 {conclusion}，已归口：{owner}", owner))
        leftovers.add(
            ConcurrentStableFrozenDict(
                {"scenario": scenario, "item": f"最新记录结论为 {conclusion}，待修复后复跑", "owner": owner}
            )
        )


def _evaluate(
    scenario: DrillScenario, records: ConcurrentStableFrozenList[Path]
) -> tuple[ScenarioEvidence, ConcurrentStableList[Check], ConcurrentStableList[ConcurrentStableFrozenDict[str, str]]]:
    """判定一个场景（取最新记录；历史记录参与复跑闭环校验）。"""
    checks: ConcurrentStableList[Check] = ConcurrentStableList()
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    if not records:
        checks.add(_check("G1", CHECK_FAILED, f"缺少场景记录：{scenario.name}"))
        rows: ConcurrentStableList[CriterionVerdict] = ConcurrentStableList()
        for spec in scenario.criterion:
            rows.add(
                CriterionVerdict(
                    key=spec.key,
                    text=spec.text,
                    severity=spec.severity,
                    verdict=VERDICT_FAIL,
                    record_keys=spec.record_keys,
                    actual="无记录",
                    evidence=spec.evidence,
                )
            )
        evidence = ScenarioEvidence(
            scenario=scenario.name,
            title=scenario.title,
            record=Path(""),
            conclusion="",
            exit_code=0,
            rto_ms=0,
            budget_ms=scenario.recovery_budget_ms,
            data_loss=False,
            verdict=VERDICT_FAIL,
            criteria=ConcurrentStableFrozenList(rows),
            checks=ConcurrentStableFrozenList(checks),
        )
        return evidence, checks, leftovers
    latest_path = records[-1]
    record = _load(latest_path)
    if record is None:
        checks.add(_check("G1", CHECK_FAILED, f"记录不可解析：{latest_path.name}"))
        record = ConcurrentStableFrozenDict[str, object]()
    checks.extend(_shape_checks(scenario, record))
    rows, mapping_checks = _criteria_verdicts(scenario, record)
    checks.extend(mapping_checks)
    checks.add(_conclusion_check(record))
    _closure_checks(scenario.name, records, checks, leftovers)
    verdicts = ConcurrentStableFrozenList([row.verdict for row in rows])
    checks_failed = any(item.level == CHECK_FAILED for item in checks)
    checks_degraded = any(item.level == CHECK_DEGRADED for item in checks)
    verdict = VERDICT_FAIL if checks_failed else _worst(verdicts)
    if verdict == VERDICT_PASS and checks_degraded:
        verdict = VERDICT_DEGRADED
    return (
        ScenarioEvidence(
            scenario=scenario.name,
            title=scenario.title,
            record=latest_path,
            conclusion=_text(_mapping(record.get("conclusion")).get("result")),
            exit_code=_number(_mapping(record.get("conclusion")).get("exit_code")),
            rto_ms=_number(record.get("rto_ms")),
            budget_ms=scenario.recovery_budget_ms,
            data_loss=_flag(record.get("data_loss")),
            verdict=verdict,
            criteria=rows,
            checks=ConcurrentStableFrozenList(checks),
        ),
        checks,
        leftovers,
    )


def _supplementary(path: Path) -> SupplementaryEvidence | None:
    """读取一条补充证据（07-3 / 07-4 记录）。"""
    record = _load(path)
    if record is None:
        return None
    conclusion = _mapping(record.get("conclusion"))
    environment = _mapping(record.get("environment"))
    return SupplementaryEvidence(
        task=_text(record.get("task")),
        mode=_text(record.get("mode")),
        result=_text(conclusion.get("result")),
        exit_code=_number(conclusion.get("exit_code")),
        data_integrity=_text(conclusion.get("data_integrity")),
        isolated=_flag(environment.get("isolated")),
        issues=len(_items(record.get("issues"))),
        leftovers=len(_items(record.get("leftovers"))),
        record=path,
    )


def verify(
    scenarios: ScenarioSet,
    record_dirs: ConcurrentStableFrozenList[Path],
    includes: ConcurrentStableFrozenList[Path],
) -> EvidenceReport:
    """对账记录并生成验收矩阵。

    Args:
        scenarios: 场景单源（已校验）。
        record_dirs: 记录目录（可多个；同场景取 `started_at` 最新一份）。
        includes: 补充证据记录（07-3 / 07-4）。

    Returns:
        EvidenceReport: 验收矩阵结果。
    """
    records, pick_checks = _pick_records(scenarios, record_dirs)
    checks: ConcurrentStableList[Check] = ConcurrentStableList(pick_checks)
    scenarios_out: ConcurrentStableList[ScenarioEvidence] = ConcurrentStableList()
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    for scenario in scenarios.scenarios:
        scenario_records = records.get(scenario.name)
        evidence, scenario_checks, scenario_leftovers = _evaluate(
            scenario,
            scenario_records if scenario_records is not None else ConcurrentStableFrozenList[Path](),
        )
        scenarios_out.add(evidence)
        checks.extend(scenario_checks)
        leftovers.extend(scenario_leftovers)
    checks.extend(_version_checks(ConcurrentStableFrozenList(scenarios_out)))
    supplementary: ConcurrentStableList[SupplementaryEvidence] = ConcurrentStableList()
    for path in includes:
        item = _supplementary(path)
        if item is None:
            checks.add(_check("G1", CHECK_FAILED, f"补充证据不可解析：{path.name}"))
            continue
        supplementary.add(item)
    verdicts = ConcurrentStableFrozenList([item.verdict for item in scenarios_out])
    failed = any(item.level == CHECK_FAILED for item in checks) or VERDICT_FAIL in verdicts
    degraded = (
        any(item.level == CHECK_DEGRADED for item in checks)
        or VERDICT_DEGRADED in verdicts
        or any(item.result not in (CONCLUSION_PASS,) for item in supplementary)
    )
    conclusion = CONCLUSION_FAIL if failed else (CONCLUSION_DEGRADED if degraded else CONCLUSION_PASS)
    exit_code = 1 if conclusion == CONCLUSION_FAIL else (2 if conclusion == CONCLUSION_DEGRADED else 0)
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    for check in checks:
        if check.level != CHECK_OK:
            reasons.add(f"{check.code}：{check.message}")
    for item in scenarios_out:
        if item.verdict != VERDICT_PASS:
            reasons.add(f"{item.scenario}：{item.verdict}")
    return EvidenceReport(
        schema_version=DRILL_EVIDENCE_SCHEMA_VERSION,
        task="07-6-2",
        generated_at=datetime.now(UTC).isoformat(),
        record_dirs=ConcurrentStableFrozenList(str(path) for path in record_dirs),
        scenarios=ConcurrentStableFrozenList(scenarios_out),
        supplementary=ConcurrentStableFrozenList(supplementary),
        checks=ConcurrentStableFrozenList(checks),
        conclusion=conclusion,
        exit_code=exit_code,
        requires_review=conclusion != CONCLUSION_PASS,
        reasons=ConcurrentStableFrozenList(reasons),
        leftovers=ConcurrentStableFrozenList(leftovers),
    )


def _version_checks(scenarios: ConcurrentStableFrozenList[ScenarioEvidence]) -> ConcurrentStableList[Check]:
    """校验六份记录的版本一致（G8，跨版本比较的前提）。"""
    versions: ConcurrentStableSet[str] = ConcurrentStableSet()
    for item in scenarios:
        record = _load(item.record) if item.record != Path("") else None
        if record is None:
            continue
        service = _mapping(record.get("service"))
        versions.add(f"{_text(service.get('version'))}/{_text(service.get('contract_version'))}")
    checks: ConcurrentStableList[Check] = ConcurrentStableList()
    checks.add(
        _check(
            "G8",
            CHECK_OK if len(versions) <= 1 else CHECK_FAILED,
            f"记录版本：{'、'.join(versions)}" if versions else "无记录版本",
        )
    )
    return checks


def _md_row(cells: ConcurrentStableFrozenList[str]) -> str:
    """拼一行 Markdown 表格。"""
    return "| " + " | ".join(cells) + " |"


def render_markdown(report: EvidenceReport, *, record_links: ConcurrentStableFrozenDict[str, str]) -> str:
    """渲染矩阵 Markdown（与 JSON 同源）。

    Args:
        report: 验收矩阵。
        record_links: 场景 → 记录链接文本（相对输出目录）。

    Returns:
        str: Markdown 文本（结尾带换行）。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("# 场景 × 判据 × 证据（07-6-2）")
    rows.add("")
    rows.add("> 生成物：由 `python -m ops.drill verify` 渲染，勿手改；判据单源为 `deploy/drill/scenarios.toml`。")
    rows.add("")
    rows.add(f"- 生成时间：`{report.generated_at}`")
    rows.add(f"- 记录目录：{'、'.join(f'`{item}`' for item in report.record_dirs) or '无'}")
    review = "是" if report.requires_review else "否"
    rows.add(f"- **汇总结论：{report.conclusion}**（退出码 {report.exit_code}，需人工复核：{review}）")
    rows.add("")
    rows.add("## 1. 结论总览")
    rows.add("")
    overview = ConcurrentStableFrozenList(["场景", "结论", "退出码", "恢复耗时", "预算", "已确认消息丢失", "记录"])
    rows.add(_md_row(overview))
    rows.add("| --- | --- | --- | --- | --- | --- | --- |")
    for item in report.scenarios:
        link = record_links.get(item.scenario, "")
        rows.add(
            _md_row(
                ConcurrentStableFrozenList(
                    [
                        f"`{item.scenario}`",
                        item.verdict,
                        str(item.exit_code),
                        f"{item.rto_ms} ms",
                        f"{item.budget_ms} ms",
                        "是" if item.data_loss else "否",
                        f"[记录]({link})" if link else "无",
                    ]
                )
            )
        )
    rows.add("")
    rows.add("## 2. 逐条判据")
    rows.add("")
    criteria_header = ConcurrentStableFrozenList(["场景", "#", "判据", "级别", "记录键", "实测", "结果", "证据要求"])
    rows.add(_md_row(criteria_header))
    rows.add("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for item in report.scenarios:
        for index, row in enumerate(item.criteria, start=1):
            level = "硬" if row.severity == "hard" else "可降级"
            keys = "、".join(f"`{key}`" for key in row.record_keys)
            rows.add(
                _md_row(
                    ConcurrentStableFrozenList(
                        [
                            f"`{item.scenario}`",
                            str(index),
                            row.text,
                            level,
                            keys,
                            row.actual,
                            row.verdict,
                            row.evidence,
                        ]
                    )
                )
            )
    rows.add("")
    rows.add("## 3. 全局判据")
    rows.add("")
    rows.add(_md_row(ConcurrentStableFrozenList(["编号", "结果", "说明", "归口"])))
    rows.add("| --- | --- | --- | --- |")
    for check in report.checks:
        rows.add(_md_row(ConcurrentStableFrozenList([check.code, check.level, check.message, check.owner or "—"])))
    rows.add("")
    rows.add("## 4. 补充证据（迁移相关故障）")
    rows.add("")
    if report.supplementary:
        supp_header = ConcurrentStableFrozenList(
            ["任务", "模式", "结论", "退出码", "数据完整性", "隔离", "问题", "遗留", "记录"]
        )
        rows.add(_md_row(supp_header))
        rows.add("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for item in report.supplementary:
            rows.add(
                _md_row(
                    ConcurrentStableFrozenList(
                        [
                            item.task or "—",
                            item.mode or "—",
                            item.result or "—",
                            str(item.exit_code),
                            item.data_integrity or "—",
                            "是" if item.isolated else "否",
                            str(item.issues),
                            str(item.leftovers),
                            item.record.name,
                        ]
                    )
                )
            )
    else:
        rows.add("- 无（未传 `--include`）")
    rows.add("")
    rows.add("## 5. 未闭环与遗留")
    rows.add("")
    if report.leftovers:
        rows.add(_md_row(ConcurrentStableFrozenList(["场景", "事项", "归口"])))
        rows.add("| --- | --- | --- |")
        for item in report.leftovers:
            rows.add(
                _md_row(
                    ConcurrentStableFrozenList(
                        [item.get("scenario", "—"), item.get("item", "—"), item.get("owner", "—")]
                    )
                )
            )
    else:
        rows.add("- 无")
    rows.add("")
    rows.add("## 6. 未通过原因")
    rows.add("")
    if report.reasons:
        for reason in report.reasons:
            rows.add(f"- {reason}")
    else:
        rows.add("- 无")
    rows.add("")
    return "\n".join(rows)


def plain(value: object) -> object:
    """把并发集合递归转为 JSON 可序列化的原生结构（仅在渲染边界使用）。"""
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in cast("Mapping[object, object]", value).items()}
    if isinstance(value, str | bytes):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Sequence):
        return [plain(item) for item in cast("Sequence[object]", value)]
    return value


def render_json(report: EvidenceReport, *, record_links: ConcurrentStableFrozenDict[str, str]) -> str:
    """渲染矩阵 JSON（与 Markdown 同源）。

    Args:
        report: 验收矩阵。
        record_links: 场景 → 记录相对路径。

    Returns:
        str: JSON 文本（结尾带换行）。
    """
    scenarios: ConcurrentStableList[object] = ConcurrentStableList()
    for item in report.scenarios:
        criteria: ConcurrentStableList[object] = ConcurrentStableList()
        for row in item.criteria:
            criteria.add(
                ConcurrentStableDict(
                    {
                        "key": row.key,
                        "text": row.text,
                        "severity": row.severity,
                        "verdict": row.verdict,
                        "record_keys": ConcurrentStableList(row.record_keys),
                        "actual": row.actual,
                        "evidence": row.evidence,
                    }
                )
            )
        checks: ConcurrentStableList[object] = ConcurrentStableList()
        for check in item.checks:
            checks.add(
                ConcurrentStableDict(
                    {"code": check.code, "level": check.level, "message": check.message, "owner": check.owner}
                )
            )
        scenarios.add(
            ConcurrentStableDict(
                {
                    "scenario": item.scenario,
                    "title": item.title,
                    "verdict": item.verdict,
                    "conclusion": item.conclusion,
                    "exit_code": item.exit_code,
                    "rto_ms": item.rto_ms,
                    "budget_ms": item.budget_ms,
                    "data_loss": item.data_loss,
                    "record": record_links.get(item.scenario, ""),
                    "criteria": criteria,
                    "checks": checks,
                }
            )
        )
    supplementary: ConcurrentStableList[object] = ConcurrentStableList()
    for item in report.supplementary:
        supplementary.add(
            ConcurrentStableDict(
                {
                    "task": item.task,
                    "mode": item.mode,
                    "result": item.result,
                    "exit_code": item.exit_code,
                    "data_integrity": item.data_integrity,
                    "isolated": item.isolated,
                    "issues": item.issues,
                    "leftovers": item.leftovers,
                    "record": item.record.name,
                }
            )
        )
    checks_all: ConcurrentStableList[object] = ConcurrentStableList()
    for check in report.checks:
        checks_all.add(
            ConcurrentStableDict(
                {"code": check.code, "level": check.level, "message": check.message, "owner": check.owner}
            )
        )
    document = ConcurrentStableDict(
        {
            "schema_version": report.schema_version,
            "task": report.task,
            "generated_at": report.generated_at,
            "record_dirs": ConcurrentStableList(report.record_dirs),
            "conclusion": ConcurrentStableDict(
                {
                    "result": report.conclusion,
                    "exit_code": report.exit_code,
                    "requires_review": report.requires_review,
                    "reasons": ConcurrentStableList(report.reasons),
                }
            ),
            "scenarios": scenarios,
            "supplementary": supplementary,
            "checks": checks_all,
            "leftovers": ConcurrentStableList(report.leftovers),
        }
    )
    return json.dumps(plain(document), ensure_ascii=False, indent=2) + "\n"


def write_outputs(
    report: EvidenceReport, out_dir: Path, *, record_links: ConcurrentStableFrozenDict[str, str]
) -> tuple[Path, Path]:
    """把矩阵 JSON 与 Markdown 写到同一目录（同名不同后缀）。

    Args:
        report: 验收矩阵。
        out_dir: 输出目录。
        record_links: 场景 → 记录相对路径。

    Returns:
        tuple[Path, Path]: JSON 路径与 Markdown 路径。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "02_场景×判据×证据对照表_07-6-2.json"
    markdown_path = out_dir / "02_场景×判据×证据对照表_07-6-2.md"
    json_path.write_text(render_json(report, record_links=record_links), encoding="utf-8")
    markdown_path.write_text(render_markdown(report, record_links=record_links), encoding="utf-8")
    return json_path, markdown_path


def archive_records(
    report: EvidenceReport, archive_dir: Path
) -> ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]:
    """把参与对账的记录复制到归档目录（同名 JSON + Markdown），并返回 sha256 清单。

    Args:
        report: 验收矩阵。
        archive_dir: 归档目录（其下建 `记录/`）。

    Returns:
        ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]: 归档清单。

    Raises:
        OSError: 归档目录不可写或复制失败（不静默）。
    """
    target_dir = archive_dir / "记录"
    target_dir.mkdir(parents=True, exist_ok=True)
    items: ConcurrentStableList[ConcurrentStableFrozenDict[str, object]] = ConcurrentStableList()
    for scenario in report.scenarios:
        if scenario.record == Path(""):
            continue
        for path in (scenario.record, scenario.record.with_suffix(".md")):
            if not path.is_file():
                continue
            target = target_dir / path.name
            shutil.copyfile(path, target)
            items.add(
                ConcurrentStableFrozenDict(
                    {"path": f"记录/{target.name}", "bytes": target.stat().st_size, "sha256": _sha256(target)}
                )
            )
    for directory in report.record_dirs:
        for name in ("release.json", "release.md"):
            path = Path(directory) / name
            if not path.is_file():
                continue
            target = target_dir / name
            shutil.copyfile(path, target)
            items.add(
                ConcurrentStableFrozenDict(
                    {"path": f"记录/{target.name}", "bytes": target.stat().st_size, "sha256": _sha256(target)}
                )
            )
    return ConcurrentStableFrozenList(items)


def _sha256(path: Path) -> str:
    """计算文件 sha256（归档清单用）。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_doc_links(path: Path) -> ConcurrentStableFrozenList[str]:
    """检查 Markdown 文档里的相对链接是否存在（外链 / 锚点跳过）。

    Args:
        path: 待检查的 Markdown 文件。

    Returns:
        ConcurrentStableFrozenList[str]: 失效链接列表（空表示通过）。
    """
    text = path.read_text(encoding="utf-8")
    missing: ConcurrentStableList[str] = ConcurrentStableList()
    for match in re.finditer(r"\]\(([^)]+)\)", text):
        target = match.group(1).strip()
        if not target or target.startswith(("http://", "https://", "#", "mailto:")):
            continue
        clean = target.split("#", 1)[0]
        if not (path.parent / clean).exists():
            missing.add(target)
    return ConcurrentStableFrozenList(missing)
