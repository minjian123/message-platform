# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
"""演练判据与回写（任务 07-6-2）：单源结构、双向对账、判定、矩阵、发版编排与清单。

用例不启动真实实例、不连网络：记录用合成 JSON，执行用回放式替身；真实注入由
`python -m ops.drill release` 执行，证据落任务 `测试/记录/`。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
)

from ops.drill import __main__ as drill_cli
from ops.drill import capture, checklist, evidence, release, runtime
from ops.drill.context import DrillContext
from ops.drill.env import (
    DrillConfig,
    load_scenarios,
    render_criteria_rows,
    validate_scenarios,
)
from ops.drill.model import (
    CONCLUSION_DEGRADED,
    CONCLUSION_FAIL,
    CONCLUSION_PASS,
    RESULT_DEGRADED,
    RESULT_FAILED,
    RESULT_OK,
    Criterion,
    DrillCriterionSpec,
    DrillReport,
    DrillScenario,
    PhaseSample,
    ScenarioSet,
)
from ops.drill.scenarios import poison_message

pytestmark = pytest.mark.kiwi_id(2480)

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根。"""

DESIGN = (
    REPO_ROOT
    / "msg文档/项目/08B_消息平台/任务/07_部署迁移与验收/07_部署迁移与验收_05_故障演练"
    / "07_部署迁移与验收_05_故障演练_02_演练判据与回写/设计/01_详细设计_07-6-2_演练判据与回写.md"
)
"""本任务详细设计（判据表一致性断言用）。"""


def _verify_one(tmp_path: Path, scenario: DrillScenario) -> evidence.EvidenceReport:
    """对单个场景的记录目录做一次对账（用例共用）。"""
    return evidence.verify(
        _scenario_set(scenario),
        ConcurrentStableFrozenList((tmp_path,)),
        ConcurrentStableFrozenList(),
    )


def _spec(key: str, keys: tuple[str, ...], severity: str = "hard") -> DrillCriterionSpec:
    """构造一条判据声明。"""
    return DrillCriterionSpec(
        key=key,
        text=f"{key} 文本",
        record_keys=ConcurrentStableFrozenList(keys),
        severity=severity,
        evidence="记录 criteria[]",
    )


def _scenario(
    name: str = "demo",
    *,
    specs: tuple[DrillCriterionSpec, ...] | None = None,
    aux: tuple[str, ...] = (),
) -> DrillScenario:
    """构造单场景定义。"""
    return DrillScenario(
        name=name,
        title=name,
        inject="sigkill_sigterm",
        observe=ConcurrentStableFrozenList(("msg_consumer_lag",)),
        criterion=ConcurrentStableFrozenList(specs or (_spec("k", ("k",)),)),
        aux_record_keys=ConcurrentStableFrozenList(aux),
        recovery_budget_ms=240_000,
        expect="期望",
    )


def _scenario_set(*scenarios: DrillScenario) -> ScenarioSet:
    """构造场景集。"""
    return ScenarioSet(
        schema_version=2,
        updated_at="2026-11-11",
        owner="msg 服务端",
        scenarios=ConcurrentStableFrozenList(scenarios or (_scenario(),)),
    )


def _record(
    scenario: DrillScenario,
    *,
    results: dict[str, str] | None = None,
    conclusion: str | None = None,
    exit_code: int | None = None,
    rto_ms: int = 100,
    data_loss: bool = False,
    env: str = "test",
    schema: int = 2,
    started_at: str = "2026-11-11T00:00:00+00:00",
    rerun: dict[str, str] | None = None,
    issues: list[dict[str, str]] | None = None,
    leftovers: list[dict[str, str]] | None = None,
    extra_criteria: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    """构造一份合成记录（字段与 07-6-1 契约一致）。"""
    values = results or {key: RESULT_OK for spec in scenario.criterion for key in spec.record_keys}
    criteria: list[dict[str, object]] = []
    for spec in scenario.criterion:
        for key in spec.record_keys:
            result = values.get(key, RESULT_OK)
            criteria.append(
                {
                    "key": key,
                    "expect": spec.text,
                    "actual": f"{key} 实测",
                    "result": result,
                    "passed": result == RESULT_OK,
                }
            )
    criteria.extend(extra_criteria or [])
    if conclusion is None:
        conclusion = (
            CONCLUSION_FAIL
            if any(item["result"] == RESULT_FAILED for item in criteria)
            else (
                CONCLUSION_DEGRADED
                if any(item["result"] in (RESULT_DEGRADED, "skipped") for item in criteria)
                else CONCLUSION_PASS
            )
        )
    if exit_code is None:
        exit_code = 1 if conclusion == CONCLUSION_FAIL else (2 if conclusion == CONCLUSION_DEGRADED else 0)
    samples = {
        phase: {
            "captured_at": started_at,
            "readyz_status": 200,
            "readyz_ok": True,
            "healthz_ok": True,
            "metrics": {"msg_consumer_lag": 0.0},
            "detail": "{}",
        }
        for phase in ("before", "during", "after")
    }
    record: dict[str, object] = {
        "schema_version": schema,
        "task": "07-6-1",
        "scenario": scenario.name,
        "mode": "run",
        "environment": {
            "env": env,
            "target": "local",
            "os": "test",
            "python": "3.14.4",
            "config_digest": "digest",
        },
        "service": {"name": "msg", "version": "0.1.0", "contract_version": "0.1.0"},
        "started_at": started_at,
        "finished_at": started_at,
        "injection": {"method": scenario.inject, "detail": "注入", "artifact": ""},
        "samples": samples,
        "criteria": criteria,
        "rto_ms": rto_ms,
        "data_loss": data_loss,
        "backup": {
            "mode": "not_applicable" if env in ("test", "dev", "local") else "provided",
            "reason": "隔离实例",
        },
        "conclusion": {"result": conclusion, "exit_code": exit_code, "reasons": []},
        "deviations": [],
        "issues": issues or [],
        "leftovers": leftovers or [],
        "commands": [],
        "artifacts": [],
        "logs": {},
        "rerun": rerun or {},
    }
    if schema < 2:
        for field in ("commands", "artifacts", "logs", "rerun"):
            record.pop(field, None)
        cast("dict[str, object]", record["environment"]).pop("config_digest", None)
    return record


def _write(tmp_path: Path, record: dict[str, object], name: str | None = None) -> Path:
    """把记录写到临时目录。"""
    path = tmp_path / f"{name or record.get('scenario')}.json"
    path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    return path


def _config(tmp_path: Path) -> DrillConfig:
    """构造演练配置。"""
    return DrillConfig(
        work_dir=tmp_path / "work",
        env="test",
        base_port=18_500,
        report_dir=tmp_path / "reports",
        recovery_budget_ms=240_000,
        fill_mb=1,
        slow_write_mb=1,
        reject_ratio=0.5,
        repo_root=REPO_ROOT,
    )


def _report_for(scenario: DrillScenario, result: str = RESULT_OK) -> DrillReport:
    """按场景构造可被 verify 判读的报告（发版编排替身）。"""
    samples = ConcurrentStableFrozenList(
        PhaseSample(
            phase=phase,
            captured_at="2026-11-11T00:00:00+00:00",
            readyz_status=200,
            readyz_ok=True,
            healthz_ok=True,
            metrics=ConcurrentStableFrozenDict(),
            detail="{}",
        )
        for phase in ("before", "during", "after")
    )
    criteria = ConcurrentStableFrozenList(
        Criterion(key=key, expect="e", actual="a", result=result)
        for spec in scenario.criterion
        for key in spec.record_keys
    )
    return DrillReport(
        scenario=scenario.name,
        mode="run",
        env="test",
        target="local",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        injection=ConcurrentStableFrozenDict({"method": scenario.inject, "detail": "d", "artifact": ""}),
        samples=samples,
        criteria=criteria,
        rto_ms=1,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
        config_digest="digest",
    )


# ---------------------------------------------------------------------------
# T1 单源结构
# ---------------------------------------------------------------------------


def test_real_scenarios_source_is_valid() -> None:
    """仓库内场景单源：schema 2、六类齐备、判据结构化。"""
    scenario_set, errors = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")

    assert not errors
    assert validate_scenarios(scenario_set, 240_000) == []
    assert scenario_set.schema_version == 2
    assert all(spec.record_keys and spec.evidence for scenario in scenario_set.scenarios for spec in scenario.criterion)


def test_criteria_rendering_matches_design() -> None:
    """判据表渲染与详细设计 §5 逐行一致（防两处漂移）。"""
    scenario_set, _ = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")
    rendered = list(render_criteria_rows(scenario_set))

    text = DESIGN.read_text(encoding="utf-8")
    start = text.index("| 场景 | # | 判据 | 级别 | 记录键 | 证据要求 |")
    block = text[start:].split("\n\n", 1)[0].splitlines()
    assert rendered == block


def test_parse_criteria_rejects_non_array_and_non_table(tmp_path: Path) -> None:
    """判据解析：字符串 / 非表条目都登记错误（不静默降级）。"""
    path = tmp_path / "a.toml"
    path.write_text(
        'schema_version = 2\n[[scenarios]]\nname = "a"\ninject = "sigkill_sigterm"\n'
        'observe = ["m"]\ncriterion = "x"\nrecovery_budget_ms = 1\nexpect = "e"\n',
        encoding="utf-8",
    )
    scenario_set, errors = load_scenarios(path)
    assert any("criterion 须为" in item for item in errors)
    assert scenario_set.scenarios[0].criterion == []

    path.write_text(
        'schema_version = 2\n[[scenarios]]\nname = "a"\ninject = "sigkill_sigterm"\n'
        'observe = ["m"]\ncriterion = ["x"]\nrecovery_budget_ms = 1\nexpect = "e"\n',
        encoding="utf-8",
    )
    _, errors = load_scenarios(path)
    assert any("criterion[0] 不是表" in item for item in errors)


def test_validate_criteria_reports_each_problem() -> None:
    """判据校验：键重复 / 文本空 / 级别非法 / 记录键空或重复 / 证据空 / 前置键相交。"""
    spec_bad = DrillCriterionSpec(
        key="k",
        text="",
        record_keys=ConcurrentStableFrozenList(),
        severity="nope",
        evidence="",
    )
    scenario = _scenario(
        specs=(
            spec_bad,
            _spec("k", ("x",)),
            _spec("k2", ("x",)),
        ),
        aux=("x",),
    )
    errors = validate_scenarios(_scenario_set(scenario), 240_000)
    joined = "；".join(errors)

    assert "缺少 text" in joined
    assert "severity = 'nope' 非法" in joined
    assert "record_keys 为空" in joined
    assert "evidence 为空" in joined
    assert "key 重复" in joined
    assert "record_keys 重复" in joined
    assert "aux_record_keys 与判据记录键重复" in joined


# ---------------------------------------------------------------------------
# T2 记录装载与结构
# ---------------------------------------------------------------------------


def test_verify_accepts_v2_record(tmp_path: Path) -> None:
    """v2 记录：结构完整、判据全过、结论通过。"""
    scenario = _scenario()
    _write(tmp_path, _record(scenario))

    report = _verify_one(tmp_path, scenario)

    assert report.conclusion == CONCLUSION_PASS
    assert report.exit_code == 0
    assert report.scenarios[0].verdict == evidence.VERDICT_PASS


def test_verify_degrades_v1_record(tmp_path: Path) -> None:
    """v1 记录：缺增强字段只记降级，不判失败。"""
    scenario = _scenario()
    _write(tmp_path, _record(scenario, schema=1))

    report = _verify_one(tmp_path, scenario)

    assert report.conclusion == CONCLUSION_DEGRADED
    assert report.exit_code == 2
    assert any("记录为 v1" in item for item in report.reasons)


def test_verify_reports_missing_bad_and_mismatched_records(tmp_path: Path) -> None:
    """记录缺失 / 不可解析 / 场景不匹配都记失败。"""
    scenario = _scenario()
    (tmp_path / "broken.json").write_text("{", encoding="utf-8")
    (tmp_path / "other.json").write_text(json.dumps({"scenario": "other"}), encoding="utf-8")
    missing_dir = tmp_path / "nope"

    report = evidence.verify(
        _scenario_set(scenario),
        ConcurrentStableFrozenList((tmp_path, missing_dir)),
        ConcurrentStableFrozenList(),
    )

    joined = "；".join(report.reasons)
    assert "记录不可解析" in joined
    assert "记录目录不存在" in joined
    assert "缺少场景记录" in joined
    assert report.exit_code == 1


def test_verify_reports_missing_fields_and_method(tmp_path: Path) -> None:
    """结构判据：缺字段、注入方式不符、时间不可解析、三段样本缺失都记失败。"""
    scenario = _scenario()
    record = _record(scenario)
    record.pop("criteria")
    record["injection"] = {"method": "other", "detail": "", "artifact": ""}
    record["started_at"] = "bad"
    cast("dict[str, object]", record["samples"]).pop("during")
    _write(tmp_path, record)

    report = _verify_one(tmp_path, scenario)
    joined = "；".join(report.reasons)

    assert "记录字段缺失" in joined
    assert "注入方式 other" in joined
    assert "起止时间不可解析" in joined
    assert "三段样本缺失" in joined


def test_verify_reports_backup_rto_loss_and_version(tmp_path: Path) -> None:
    """全局判据：备份证据、恢复预算、数据丢失、版本一致性。"""
    scenario = _scenario()
    record = _record(scenario, rto_ms=999_999, data_loss=True, env="prod")
    record["backup"] = {"mode": "provided", "reason": ""}
    _write(tmp_path, record)
    other = _scenario("demo2", specs=(_spec("k2", ("k2",)),))
    record2 = _record(other, started_at="2026-11-11T00:00:01+00:00")
    cast("dict[str, object]", record2["service"])["version"] = "9.9.9"
    _write(tmp_path, record2)

    report = evidence.verify(
        _scenario_set(scenario, other),
        ConcurrentStableFrozenList((tmp_path,)),
        ConcurrentStableFrozenList(),
    )
    joined = "；".join(report.reasons)

    assert "备份证据 provided" in joined
    assert "恢复 999999 ms" in joined
    assert "已确认消息丢失" in joined
    assert "记录版本" in joined


def test_verify_reports_config_digest_and_task(tmp_path: Path) -> None:
    """v2 记录缺配置摘要记降级；task 非 07-6-1 记失败。"""
    scenario = _scenario()
    record = _record(scenario)
    cast("dict[str, object]", record["environment"]).pop("config_digest")
    record["task"] = "other"
    _write(tmp_path, record)

    report = _verify_one(tmp_path, scenario)
    joined = "；".join(report.reasons)

    assert "记录缺配置摘要" in joined
    assert "记录 task 非法" in joined


# ---------------------------------------------------------------------------
# T3 双向对账与硬 / 可降级
# ---------------------------------------------------------------------------


def test_verify_reports_missing_and_orphan_keys(tmp_path: Path) -> None:
    """对账：单源键缺记录（漏判）与记录出现孤儿键都记失败。"""
    scenario = _scenario(specs=(_spec("a", ("a", "b")),), aux=("aux",))
    record = _record(scenario, results={"a": RESULT_OK})
    record["criteria"] = [{"key": "a", "result": RESULT_OK, "passed": True, "actual": "x"}]
    cast("list[object]", record["criteria"]).append({"key": "orphan", "result": RESULT_OK, "passed": True})
    cast("list[object]", record["criteria"]).append({"key": "", "result": RESULT_OK, "passed": True})
    _write(tmp_path, record)

    report = _verify_one(tmp_path, scenario)
    joined = "；".join(report.reasons)

    assert "缺少记录键：b" in joined
    assert "未登记判据键：orphan" in joined


def test_verify_allows_aux_and_global_keys(tmp_path: Path) -> None:
    """对账：aux 键与 rto_within_budget 允许出现，不算孤儿。"""
    scenario = _scenario(aux=("aux",))
    record = _record(scenario)
    cast("list[object]", record["criteria"]).extend(
        [
            {"key": "aux", "result": RESULT_OK, "passed": True},
            {"key": "SIGKILL.rto_within_budget", "result": RESULT_OK, "passed": True},
        ]
    )
    _write(tmp_path, record)

    report = _verify_one(tmp_path, scenario)

    assert report.conclusion == CONCLUSION_PASS


def test_verify_hard_degraded_is_failed(tmp_path: Path) -> None:
    """硬判据 degraded 即场景不通过。"""
    scenario = _scenario(specs=(_spec("hard", ("hard",)),))
    _write(tmp_path, _record(scenario, results={"hard": RESULT_DEGRADED}))

    report = _verify_one(tmp_path, scenario)

    assert report.scenarios[0].verdict == evidence.VERDICT_FAIL
    assert report.conclusion == CONCLUSION_FAIL


def test_verify_degradable_degraded_is_degraded(tmp_path: Path) -> None:
    """可降级判据 degraded → 场景降级，不判失败。"""
    scenario = _scenario(specs=(_spec("soft", ("soft",), severity="degradable"),))
    _write(
        tmp_path,
        _record(
            scenario,
            results={"soft": RESULT_DEGRADED},
            leftovers=[{"item": "待修", "owner": "02-3"}],
        ),
    )

    report = _verify_one(tmp_path, scenario)

    assert report.scenarios[0].verdict == evidence.VERDICT_DEGRADED
    assert report.conclusion == CONCLUSION_DEGRADED


def test_verify_detects_passed_flag_and_conclusion_mismatch(tmp_path: Path) -> None:
    """记录自洽：passed 与 result 不一致、结论高报都记失败。"""
    scenario = _scenario()
    record = _record(scenario)
    cast("list[dict[str, object]]", record["criteria"])[0]["passed"] = False
    _write(tmp_path, record)

    report = _verify_one(tmp_path, scenario)
    assert any("passed 与 result 不一致" in item for item in report.reasons)

    record = _record(scenario, conclusion=CONCLUSION_PASS, exit_code=0)
    cast("list[dict[str, object]]", record["criteria"])[0]["result"] = RESULT_FAILED
    cast("list[dict[str, object]]", record["criteria"])[0]["passed"] = False
    _write(tmp_path, record, name="mismatch")
    report = _verify_one(tmp_path, scenario)
    assert any("与判据汇总" in item for item in report.reasons)


# ---------------------------------------------------------------------------
# T4 复跑闭环
# ---------------------------------------------------------------------------


def test_verify_closure_with_successor(tmp_path: Path) -> None:
    """历史非通过记录有后继且 rerun 指向前次 → 闭环。"""
    scenario = _scenario()
    old = _record(scenario, results={"k": RESULT_DEGRADED}, started_at="2026-11-11T00:00:00+00:00")
    _write(tmp_path, old, name="old")
    new = _record(
        scenario,
        started_at="2026-11-11T00:00:01+00:00",
        rerun={"attempt": "2", "previous_started_at": "2026-11-11T00:00:00+00:00", "previous_conclusion": "降级通过"},
    )
    _write(tmp_path, new, name="new")

    report = _verify_one(tmp_path, scenario)

    assert report.conclusion == CONCLUSION_PASS
    assert any("已复跑" in item for item in report.reasons) is False


def test_verify_closure_fails_without_successor(tmp_path: Path) -> None:
    """历史非通过记录没有后继 → 未闭环（失败）。"""
    scenario = _scenario()
    old = _record(scenario, results={"k": RESULT_DEGRADED}, started_at="2026-11-11T00:00:00+00:00")
    _write(tmp_path, old, name="old")
    latest = _record(scenario, started_at="2026-11-11T00:00:01+00:00")
    _write(tmp_path, latest, name="new")

    report = _verify_one(tmp_path, scenario)

    assert report.conclusion == CONCLUSION_FAIL
    assert any("没有后继复跑" in item for item in report.reasons)


def test_verify_latest_degraded_requires_owner(tmp_path: Path) -> None:
    """最新记录非通过：有归口记降级，无归口记失败。"""
    scenario = _scenario(specs=(_spec("soft", ("soft",), severity="degradable"),))
    _write(tmp_path, _record(scenario, results={"soft": RESULT_DEGRADED}))
    report = _verify_one(tmp_path, scenario)
    assert report.conclusion == CONCLUSION_FAIL
    assert any("未登记归口" in item for item in report.reasons)

    _write(
        tmp_path,
        _record(
            scenario,
            results={"soft": RESULT_DEGRADED},
            started_at="2026-11-11T00:00:01+00:00",
            rerun={
                "attempt": "2",
                "previous_started_at": "2026-11-11T00:00:00+00:00",
                "previous_conclusion": "降级通过",
            },
            leftovers=[{"item": "待修", "owner": "02-3"}],
            issues=[{"summary": "无归口问题"}],
        ),
        name="owned",
    )
    report = _verify_one(tmp_path, scenario)
    assert report.conclusion == CONCLUSION_DEGRADED
    assert report.leftovers


# ---------------------------------------------------------------------------
# T5 矩阵与归档
# ---------------------------------------------------------------------------


def test_render_and_write_matrix(tmp_path: Path) -> None:
    """矩阵：Markdown / JSON 同源、记录链接可点、JSON 可解析。"""
    scenario = _scenario()
    record_path = _write(tmp_path, _record(scenario))
    report = _verify_one(tmp_path, scenario)
    links = ConcurrentStableFrozenDict({scenario.name: "record.json"})
    out = tmp_path / "out"

    json_path, markdown_path = evidence.write_outputs(report, out, record_links=links)

    markdown = markdown_path.read_text(encoding="utf-8")
    assert "## 1. 结论总览" in markdown
    assert "[记录](record.json)" in markdown
    assert "## 2. 逐条判据" in markdown
    document = json.loads(json_path.read_text(encoding="utf-8"))
    assert document["conclusion"]["result"] == CONCLUSION_PASS
    assert document["scenarios"][0]["criteria"][0]["key"] == "k"
    assert document["scenarios"][0]["record"] == "record.json"
    assert record_path.exists()


def test_archive_records_copies_and_hashes(tmp_path: Path) -> None:
    """归档：复制记录与同名 Markdown，返回 sha256。"""
    scenario = _scenario()
    record_path = _write(tmp_path, _record(scenario))
    record_path.with_suffix(".md").write_text("# 记录\n", encoding="utf-8")
    (tmp_path / "release.json").write_text("{}", encoding="utf-8")
    report = _verify_one(tmp_path, scenario)

    archived = evidence.archive_records(report, tmp_path / "archive")

    assert len(archived) == 3
    assert (tmp_path / "archive" / "记录" / "demo.json").exists()
    assert (tmp_path / "archive" / "记录" / "release.json").exists()
    assert all(len(str(item["sha256"])) == 64 for item in archived)


def test_check_doc_links(tmp_path: Path) -> None:
    """链接校验：缺失报错，外链 / 锚点跳过。"""
    doc = tmp_path / "a.md"
    doc.write_text("[ok](b.md) [bad](nope.md) [web](https://x) [anchor](#a)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("b", encoding="utf-8")

    missing = evidence.check_doc_links(doc)

    assert list(missing) == ["nope.md"]


def test_supplementary_evidence(tmp_path: Path) -> None:
    """补充证据：只取结论 / 模式 / 完整性 / 问题数，不并入六类统计。"""
    path = tmp_path / "recovery.json"
    path.write_text(
        json.dumps(
            {
                "task": "07-3-2",
                "mode": "drill",
                "environment": {"isolated": True},
                "conclusion": {"result": "降级通过", "exit_code": 2, "data_integrity": "有缺口"},
                "issues": [{"summary": "x"}],
                "leftovers": [{"item": "y"}],
            }
        ),
        encoding="utf-8",
    )
    scenario = _scenario()
    _write(tmp_path, _record(scenario))

    report = evidence.verify(
        _scenario_set(scenario),
        ConcurrentStableFrozenList((tmp_path,)),
        ConcurrentStableFrozenList((path,)),
    )

    assert report.supplementary[0].task == "07-3-2"
    assert report.supplementary[0].isolated is True
    assert report.supplementary[0].data_integrity == "有缺口"
    assert report.conclusion == CONCLUSION_DEGRADED


def test_supplementary_bad_file_is_failed(tmp_path: Path) -> None:
    """补充证据不可解析记失败。"""
    bad = tmp_path / "bad.json"
    bad.write_text("{", encoding="utf-8")
    scenario = _scenario()
    _write(tmp_path, _record(scenario))

    report = evidence.verify(
        _scenario_set(scenario),
        ConcurrentStableFrozenList((tmp_path,)),
        ConcurrentStableFrozenList((bad,)),
    )

    assert any("补充证据不可解析" in item for item in report.reasons)


# ---------------------------------------------------------------------------
# T6 发版编排
# ---------------------------------------------------------------------------


def test_run_release_writes_records_and_summary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """发版复跑：六类顺序、写记录与发版记录、退出码取最差。"""
    config = _config(tmp_path)
    scenarios = _scenario_set(_scenario("a"), _scenario("b"))
    monkeypatch.setattr(release, "execute", lambda _config, scenario, **_kw: _report_for(scenario))
    monkeypatch.setattr(release, "git_commit", lambda _root: "abc123")

    report = release.run_release(
        config,
        scenarios,
        rerun_reason="发版前复跑",
        backup=ConcurrentStableFrozenDict({"mode": "not_applicable", "reason": "r"}),
        service_version="0.1.0",
        contract_version="0.1.0",
    )

    assert [step.scenario for step in report.steps] == ["a", "b"]
    assert report.conclusion == CONCLUSION_PASS
    assert (config.report_dir / "release.json").exists()
    assert (config.report_dir / "release.md").exists()
    document = json.loads((config.report_dir / "release.json").read_text(encoding="utf-8"))
    assert document["environment"]["git_commit"] == "abc123"
    assert document["verification"]["result"] == CONCLUSION_PASS


def test_run_release_failure_and_degraded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """发版复跑：场景失败 → 退出 1；降级 → 退出 2（不中断）。"""
    config = _config(tmp_path)
    scenarios = _scenario_set(_scenario("a"), _scenario("b"))

    def _bad(_config: DrillConfig, scenario: DrillScenario, **_kw: object) -> DrillReport:
        result = RESULT_FAILED if scenario.name == "a" else RESULT_DEGRADED
        return _report_for(scenario, result=result)

    monkeypatch.setattr(release, "execute", _bad)
    report = release.run_release(
        config,
        scenarios,
        rerun_reason="r",
        backup=ConcurrentStableFrozenDict({"mode": "not_applicable", "reason": "r"}),
        service_version="0.1.0",
        contract_version="0.1.0",
    )

    assert report.conclusion == CONCLUSION_FAIL
    assert report.exit_code == 1


def test_git_commit_handles_non_repo(tmp_path: Path) -> None:
    """git 不可用 / 非仓库时返回空串，不阻断。"""
    assert release.git_commit(tmp_path) == ""


# ---------------------------------------------------------------------------
# T7 检查清单
# ---------------------------------------------------------------------------


def test_checklist_generate_and_check(tmp_path: Path) -> None:
    """清单：生成含六类与门禁；一致通过、漂移与缺失报错。"""
    scenario_set, _ = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")
    path = tmp_path / "RELEASE_CHECKLIST.md"

    checklist.write_checklist(scenario_set, path)

    text = path.read_text(encoding="utf-8")
    assert "前置门禁" in text
    assert "process_crash" in text
    assert "poison_message" in text
    assert checklist.check_checklist(scenario_set, path) == []

    path.write_text(text + "\n漂移", encoding="utf-8")
    assert checklist.check_checklist(scenario_set, path)
    missing = tmp_path / "missing.md"
    assert checklist.check_checklist(scenario_set, missing)


def test_normalize_strips_trailing_and_newlines() -> None:
    """归一化：CRLF / 行尾空白 / 末尾空行。"""
    assert checklist.normalize("a  \r\nb\n\n") == "a\nb"


# ---------------------------------------------------------------------------
# T8 采集、记录增强与运行时
# ---------------------------------------------------------------------------


def test_capture_artifacts_and_logs(tmp_path: Path) -> None:
    """采集：产物清单、日志摘要、配置摘要、复跑标识。"""
    config = _config(tmp_path)
    config.work_dir.mkdir(parents=True)
    (config.work_dir / "var").mkdir()
    (config.work_dir / "instance.log").write_text("line1\nline2\n", encoding="utf-8")
    (config.work_dir / "fill").mkdir()
    (config.work_dir / "fill" / "volume.bin").write_bytes(b"x")
    (config.work_dir / "slowdisk").mkdir()
    (config.work_dir / "slowdisk" / "pressure.bin").write_bytes(b"y")

    artifacts = capture.collect_artifacts(config)
    logs = capture.log_snapshot(config)

    assert {item["role"] for item in artifacts} == {"data_root", "log", "fill", "slowdisk"}
    assert logs["lines"] == 2
    assert len(str(logs["sha256"])) == 64
    assert len(capture.config_digest(config, "demo")) == 64
    assert capture.config_digest(config, "demo") == capture.config_digest(config, "demo")


def test_capture_missing_log_and_relative(tmp_path: Path) -> None:
    """采集：无日志返回空表；工作根外路径只留文件名。"""
    config = _config(tmp_path)
    assert capture.log_snapshot(config) == {}
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"z")
    assert capture.relative_path(config.work_dir, outside) == "outside.bin"


def test_capture_rerun_and_previous(tmp_path: Path) -> None:
    """复跑：首跑为空；有前次则 attempt+1 并带前次结论。"""
    config = _config(tmp_path)
    config.report_dir.mkdir(parents=True)
    assert capture.rerun_block(None, "r") == {}
    previous = _record(_scenario())
    (config.report_dir / "demo.json").write_text(json.dumps(previous), encoding="utf-8")

    block = capture.rerun_block(capture.previous_record(config, "demo"), "r")

    assert block["attempt"] == "2"
    assert block["previous_conclusion"] == CONCLUSION_PASS
    assert block["reason"] == "r"


def test_capture_previous_record_handles_bad_json(tmp_path: Path) -> None:
    """复跑：坏 JSON / 非对象视为首跑。"""
    config = _config(tmp_path)
    config.report_dir.mkdir(parents=True)
    (config.report_dir / "demo.json").write_text("{", encoding="utf-8")
    assert capture.previous_record(config, "demo") is None
    (config.report_dir / "demo.json").write_text("[]", encoding="utf-8")
    assert capture.previous_record(config, "demo") is None


def test_command_trail_truncates(tmp_path: Path) -> None:
    """动作轨迹：超过上限保留首尾并标 truncated。"""
    http = runtime.DrillHttp("http://127.0.0.1:1")
    for index in range(205):
        http.commands.add(ConcurrentStableFrozenDict({"seq": str(index), "action": "a", "detail": ""}))
    trail = capture.command_trail(http)
    http.close()

    assert len(trail) == 201
    assert any(item["action"] == "truncated" for item in trail)


def test_drill_http_command_cap() -> None:
    """动作轨迹：客户端侧 10000 条安全上限。"""
    http = runtime.DrillHttp("http://127.0.0.1:1")
    for _ in range(10_001):
        http._record("a")  # pyright: ignore[reportPrivateUsage]
    http.close()

    assert len(http.commands) == 10_000


def test_parse_metric_sum_sums_labels() -> None:
    """指标求和：同名多标签相加，坏行跳过。"""
    text = 'msg_dlq_total{topic="a"} 1\nmsg_dlq_total{topic="b"} 2\nbad line\n'
    assert runtime.parse_metric_sum(text, "msg_dlq_total") == 3


def test_context_metric_sum(tmp_path: Path) -> None:
    """上下文：metric_sum 走客户端指标文本。"""

    class _Http:
        def metrics(self) -> str:
            return 'msg_dlq_total{topic="a"} 1\nmsg_dlq_total{topic="b"} 2\n'

    ctx = DrillContext(
        config=_config(tmp_path),
        scenario=_scenario(),
        instance=cast("runtime.DrillInstance", SimpleNamespace()),
        http=cast("runtime.DrillHttp", _Http()),
        started_at=0.0,
    )
    assert ctx.metric_sum("msg_dlq_total") == 3


def test_record_document_includes_enhancements(tmp_path: Path) -> None:
    """记录：JSON 含 commands / artifacts / logs / config_digest / rerun，Markdown 有复现线索。"""
    from ops.drill import record as record_mod

    report = DrillReport(
        scenario="demo",
        mode="run",
        env="test",
        target="local",
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        injection=ConcurrentStableFrozenDict({"method": "m", "detail": "d", "artifact": "a"}),
        samples=ConcurrentStableFrozenList(),
        criteria=ConcurrentStableFrozenList((Criterion(key="k", expect="e", actual="a", result=RESULT_OK),)),
        rto_ms=1,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
        commands=ConcurrentStableFrozenList(
            (ConcurrentStableFrozenDict({"seq": "1", "action": "POST /x", "detail": ""}),)
        ),
        artifacts=ConcurrentStableFrozenList(
            (ConcurrentStableFrozenDict({"path": "instance.log", "role": "log", "bytes": 1, "sha256": "h"}),)
        ),
        logs=ConcurrentStableFrozenDict(
            {"path": "instance.log", "lines": 1, "bytes": 1, "sha256": "h", "excerpt": "x"}
        ),
        config_digest="digest",
        rerun=ConcurrentStableFrozenDict({"attempt": "2", "previous_started_at": "t", "previous_conclusion": "通过"}),
    )

    document = record_mod.build_document(report, service_version="0.1.0", contract_version="0.1.0")
    markdown = record_mod.render_markdown(report, title="demo", service_version="0.1.0")

    assert document["commands"]
    assert document["artifacts"]
    assert document["logs"]
    environment = cast("dict[str, object]", document["environment"])
    assert environment["config_digest"] == "digest"
    assert cast("dict[str, object]", document["rerun"])["attempt"] == "2"
    assert "## 7. 复现线索" in markdown


# ---------------------------------------------------------------------------
# T9 命令行
# ---------------------------------------------------------------------------


def _env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """设置演练键位（隔离工作根与记录目录）。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setenv("MSG_DRILL_REPORT_DIR", "tmp/drill-test/reports")
    monkeypatch.setenv("MSG_DRILL_EVIDENCE_DIR", "tmp/drill-test/evidence")


def test_cli_criteria_and_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """命令行：criteria 文本 / JSON、plan 渲染。"""
    _env(tmp_path, monkeypatch)

    assert drill_cli.main(["criteria"]) == 0
    assert "记录键" in capsys.readouterr().out
    assert drill_cli.main(["criteria", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["scenario"]
    assert drill_cli.main(["plan"]) == 0
    assert "process_crash" in capsys.readouterr().out


def test_cli_checklist_write_and_check(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：checklist 生成与校验。"""
    _env(tmp_path, monkeypatch)
    path = checklist.checklist_path(REPO_ROOT)
    original = path.read_text(encoding="utf-8")
    try:
        assert drill_cli.main(["checklist"]) == 0
        assert drill_cli.main(["checklist", "--check"]) == 0
        path.write_text(original + "\n漂移", encoding="utf-8")
        assert drill_cli.main(["checklist", "--check"]) == 1
    finally:
        path.write_text(original, encoding="utf-8")


def test_cli_verify_pass_and_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：verify 通过、归档、链接校验。"""
    _env(tmp_path, monkeypatch)
    scenario_set, _ = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")
    records = tmp_path / "records"
    records.mkdir()
    for scenario in scenario_set.scenarios:
        _write(records, _record(scenario))
    doc = tmp_path / "doc.md"
    doc.write_text("[m](evidence.md)\n", encoding="utf-8")
    (tmp_path / "evidence.md").write_text("x", encoding="utf-8")

    code = drill_cli.main(
        [
            "verify",
            "--records",
            str(records),
            "--out",
            str(tmp_path / "out"),
            "--archive",
            str(tmp_path / "archive"),
            "--check-doc",
            str(doc),
        ]
    )

    assert code == 0
    assert (tmp_path / "out" / "02_场景×判据×证据对照表_07-6-2.md").exists()
    assert (tmp_path / "archive" / "记录").is_dir()


def test_cli_verify_bad_doc_link(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：verify 的 --check-doc 失效链接退 1。"""
    _env(tmp_path, monkeypatch)
    scenario_set, _ = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")
    records = tmp_path / "records"
    records.mkdir()
    for scenario in scenario_set.scenarios:
        _write(records, _record(scenario))
    doc = tmp_path / "doc.md"
    doc.write_text("[m](nope.md)\n", encoding="utf-8")

    code = drill_cli.main(
        ["verify", "--records", str(records), "--out", str(tmp_path / "out"), "--check-doc", str(doc)]
    )

    assert code == 1


def test_cli_release_dry_run_and_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：release dry-run 零执行；缺 confirm 退 2；生产拒绝。"""
    _env(tmp_path, monkeypatch)

    assert drill_cli.main(["release", "--confirm", "release", "--dry-run"]) == 0
    assert drill_cli.main(["release"]) == 2
    monkeypatch.setenv("MSG_DRILL_ENV", "prod")
    assert drill_cli.main(["release", "--confirm", "release"]) == 2


def test_cli_unknown_source_is_param_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：单源非法时 criteria / verify 退 2（plan 退 1）。"""
    _env(tmp_path, monkeypatch)
    bad = tmp_path / "deploy" / "drill" / "scenarios.toml"
    bad.parent.mkdir(parents=True)
    bad.write_text("schema_version = 2\n", encoding="utf-8")
    monkeypatch.setattr(
        drill_cli,
        "load_scenarios",
        lambda _path: (ScenarioSet(2, "d", "o", ConcurrentStableFrozenList()), []),
    )

    assert drill_cli.main(["criteria"]) == 2


# ---------------------------------------------------------------------------
# T10 毒消息辅助
# ---------------------------------------------------------------------------


def test_poison_consume_one_and_wait_growth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """毒消息：短轮询拉到消息；死信增长等待超时返回当前值。"""

    class _Http:
        def __init__(self) -> None:
            self.batches = [[], [{"offset": 3}]]
            self.metrics_calls = 0

        def consume(self, topic: str, group: str, consumer: str, **kwargs: object):
            import httpx

            batch = self.batches.pop(0) if self.batches else []
            return httpx.Response(200, json={"code": 0, "data": {"messages": batch}})

        def heartbeat(self, topic: str, group: str, consumer: str) -> int:
            return 200

        def metrics(self) -> str:
            self.metrics_calls += 1
            value = 1.0 if self.metrics_calls == 1 else 2.0
            return f'msg_dlq_total{{topic="a"}} {value}\n'

    http = _Http()
    ctx = DrillContext(
        config=_config(tmp_path),
        scenario=_scenario("poison_message"),
        instance=cast("runtime.DrillInstance", SimpleNamespace()),
        http=cast("runtime.DrillHttp", http),
        started_at=0.0,
    )
    monkeypatch.setattr(poison_message.time, "sleep", lambda _s: None)

    assert poison_message._consume_one(ctx, "t") == 3  # pyright: ignore[reportPrivateUsage]
    assert poison_message._wait_dlq_growth(ctx, 1.0, timeout_sec=0.0) == 1.0  # pyright: ignore[reportPrivateUsage]
    http.metrics_calls = 0
    assert poison_message._wait_dlq_growth(ctx, 1.0, timeout_sec=1.0) == 2.0  # pyright: ignore[reportPrivateUsage]


def test_poison_consume_one_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """毒消息：短轮询超时返回 None。"""

    class _Http:
        def consume(self, topic: str, group: str, consumer: str, **kwargs: object):
            import httpx

            return httpx.Response(200, json={"code": 0, "data": {"messages": []}})

        def heartbeat(self, topic: str, group: str, consumer: str) -> int:
            return 200

    ctx = DrillContext(
        config=_config(tmp_path),
        scenario=_scenario("poison_message"),
        instance=cast("runtime.DrillInstance", SimpleNamespace()),
        http=cast("runtime.DrillHttp", _Http()),
        started_at=0.0,
    )
    monkeypatch.setattr(poison_message.time, "sleep", lambda _s: None)

    assert poison_message._consume_one(ctx, "t", timeout_sec=0.0) is None  # pyright: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# T11 边界与内部出口（覆盖率收口）
# ---------------------------------------------------------------------------


def test_internal_helpers_edges(tmp_path: Path) -> None:
    """内部出口：布尔不当数值、非映射收窄、非目录、忽略矩阵 / 发版记录、坏 JSON 排序。"""
    assert evidence._number(True) == 0  # pyright: ignore[reportPrivateUsage]
    assert evidence._mapping(1) == {}  # pyright: ignore[reportPrivateUsage]
    assert evidence._record_paths(tmp_path / "nope") == []  # pyright: ignore[reportPrivateUsage]
    (tmp_path / "evidence-x.json").write_text("{}", encoding="utf-8")
    (tmp_path / "release-x.json").write_text("{}", encoding="utf-8")
    assert evidence._record_paths(tmp_path) == []  # pyright: ignore[reportPrivateUsage]
    broken = tmp_path / "arr.json"
    broken.write_text("[]", encoding="utf-8")
    assert evidence._load(broken) is None  # pyright: ignore[reportPrivateUsage]
    assert evidence._sort_key(broken) == "arr.json"  # pyright: ignore[reportPrivateUsage]
    assert evidence.plain(Path("a/b")) == "a/b"


def test_conclusion_exit_code_mismatch(tmp_path: Path) -> None:
    """记录自洽：退出码与结论不一致记失败。"""
    scenario = _scenario()
    _write(tmp_path, _record(scenario, conclusion=CONCLUSION_PASS, exit_code=1))

    report = _verify_one(tmp_path, scenario)

    assert any("退出码 1 与结论" in item for item in report.reasons)


def test_evaluate_broken_latest(tmp_path: Path) -> None:
    """判定：最新记录不可解析也产出失败证据（不抛）。"""
    broken = tmp_path / "broken.json"
    broken.write_text("{", encoding="utf-8")

    result, checks, _ = evidence._evaluate(  # pyright: ignore[reportPrivateUsage]
        _scenario(), ConcurrentStableFrozenList((broken,))
    )

    assert result.verdict == evidence.VERDICT_FAIL
    assert any("记录不可解析" in item.message for item in checks)


def test_render_markdown_with_supplementary_and_leftovers(tmp_path: Path) -> None:
    """矩阵：补充证据与未闭环遗留两节都要渲染。"""
    scenario = _scenario(specs=(_spec("soft", ("soft",), severity="degradable"),))
    _write(
        tmp_path,
        _record(
            scenario,
            results={"soft": RESULT_DEGRADED},
            leftovers=[{"item": "待修", "owner": "02-3"}],
        ),
    )
    supplement = tmp_path / "recovery.json"
    supplement.write_text(
        json.dumps(
            {
                "task": "07-3-2",
                "mode": "drill",
                "environment": {"isolated": True},
                "conclusion": {"result": "降级通过", "exit_code": 2, "data_integrity": "有缺口"},
                "issues": [],
                "leftovers": [],
            }
        ),
        encoding="utf-8",
    )
    report = evidence.verify(
        _scenario_set(scenario),
        ConcurrentStableFrozenList((tmp_path,)),
        ConcurrentStableFrozenList((supplement,)),
    )

    markdown = evidence.render_markdown(report, record_links=ConcurrentStableFrozenDict())

    assert "## 4. 补充证据（迁移相关故障）" in markdown
    assert "## 5. 未闭环与遗留" in markdown
    assert "02-3" in markdown
    document = json.loads(evidence.render_json(report, record_links=ConcurrentStableFrozenDict()))
    assert document["supplementary"][0]["task"] == "07-3-2"


def test_archive_skips_missing_record(tmp_path: Path) -> None:
    """归档：无记录的缺失场景跳过，不抛。"""
    report = _verify_one(tmp_path, _scenario())

    assert evidence.archive_records(report, tmp_path / "archive") == []


def test_rerun_block_non_mapping(tmp_path: Path) -> None:
    """复跑：前次 rerun / conclusion 非映射时按首跑计数兜底。"""
    block = capture.rerun_block(
        {"started_at": "t", "rerun": "x", "conclusion": 1},
        "r",
    )

    assert block["attempt"] == "2"
    assert block["previous_conclusion"] == ""


def test_engine_injection_artifact_file(tmp_path: Path) -> None:
    """注入明细：文件产物补相对路径与 sha256。"""
    from ops.drill import engine
    from ops.drill.context import ScenarioOutcome

    config = _config(tmp_path)
    config.work_dir.mkdir(parents=True)
    artifact = config.work_dir / "fill" / "volume.bin"
    artifact.parent.mkdir()
    artifact.write_bytes(b"x")
    outcome = ScenarioOutcome(
        injection=ConcurrentStableFrozenDict({"method": "fill_volume", "detail": "d", "artifact": str(artifact)}),
        samples=ConcurrentStableFrozenList(),
        criteria=ConcurrentStableFrozenList(),
        rto_ms=0,
        data_loss=False,
    )

    payload = engine._injection(config, outcome)  # pyright: ignore[reportPrivateUsage]

    assert payload["artifact_rel"] == "fill/volume.bin"
    assert len(str(payload["artifact_sha256"])) == 64

    directory_outcome = ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {"method": "sigkill_sigterm", "detail": "d", "artifact": str(config.work_dir)}
        ),
        samples=ConcurrentStableFrozenList(),
        criteria=ConcurrentStableFrozenList(),
        rto_ms=0,
        data_loss=False,
    )
    directory_payload = engine._injection(config, directory_outcome)  # pyright: ignore[reportPrivateUsage]
    assert directory_payload["artifact_rel"] == "."
    assert "artifact_sha256" not in directory_payload


def test_env_reports_missing_criterion_key() -> None:
    """判据校验：缺 key 单独报错。"""
    bad = DrillCriterionSpec(
        key="",
        text="t",
        record_keys=ConcurrentStableFrozenList(("x",)),
        severity="hard",
        evidence="e",
    )
    errors = validate_scenarios(_scenario_set(_scenario(specs=(bad,))), 240_000)

    assert any("缺少 key" in item for item in errors)


def test_release_git_oserror(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """发版记录：git 命令不可执行时返回空串。"""

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("no git")

    monkeypatch.setattr(release.subprocess, "run", boom)

    assert release.git_commit(tmp_path) == ""


def test_release_worst_prefers_step() -> None:
    """发版汇总：场景结论比验证结论更差时取场景。"""
    steps = ConcurrentStableFrozenList((SimpleNamespace(conclusion=CONCLUSION_DEGRADED),))
    verification = SimpleNamespace(conclusion=CONCLUSION_PASS)

    worst = release._worst(  # pyright: ignore[reportPrivateUsage]
        cast("ConcurrentStableFrozenList[release.ReleaseStep]", steps),
        cast("evidence.EvidenceReport", verification),
    )
    assert worst == CONCLUSION_DEGRADED


def test_parse_metric_sum_edges() -> None:
    """指标求和：空行 / 注释 / 坏值跳过。"""
    text = '# c\n\nmsg_dlq_total{topic="a"} bad\nmsg_dlq_total{topic="b"} 2\n'
    assert runtime.parse_metric_sum(text, "msg_dlq_total") == 2


def test_cli_verify_reasons_and_archive_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：verify 打印原因；归档失败退 1（不静默）。"""
    _env(tmp_path, monkeypatch)
    scenario = _scenario()
    records = tmp_path / "records"
    records.mkdir()
    _write(records, _record(scenario, results={"k": RESULT_DEGRADED}))
    scenario_set = _scenario_set(scenario)
    monkeypatch.setattr(drill_cli, "_scenario_set", lambda _config: scenario_set)
    monkeypatch.setattr(
        drill_cli.evidence_mod,
        "archive_records",
        lambda _report, _dir: (_ for _ in ()).throw(OSError("readonly")),
    )

    code = drill_cli.main(
        [
            "verify",
            "--records",
            str(records),
            "--out",
            str(tmp_path / "out"),
            "--archive",
            str(tmp_path / "archive"),
        ]
    )

    assert code == 1


def test_cli_verify_without_check_doc(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：verify 不带 --check-doc 时直接返回矩阵结论。"""
    _env(tmp_path, monkeypatch)
    scenario = _scenario()
    records = tmp_path / "records"
    records.mkdir()
    _write(records, _record(scenario))
    scenario_set = _scenario_set(scenario)
    monkeypatch.setattr(drill_cli, "_scenario_set", lambda _config: scenario_set)

    assert drill_cli.main(["verify", "--records", str(records), "--out", str(tmp_path / "out")]) == 0


def test_cli_release_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：release 非 dry-run 走编排并打印六类结果。"""
    _env(tmp_path, monkeypatch)
    fake = SimpleNamespace(
        steps=[
            SimpleNamespace(scenario="a", conclusion="通过", exit_code=0, rto_ms=1),
        ],
        conclusion="通过",
        exit_code=0,
        reasons=["提示"],
    )
    monkeypatch.setattr(drill_cli, "run_release", lambda *_args, **_kwargs: fake)

    assert drill_cli.main(["release", "--confirm", "release"]) == 0
