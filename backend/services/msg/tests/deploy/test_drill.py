# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
# 回放式替身（FakeHttp / FakeInstance / lambda 回调）故意不回放内部类型：本文件只证明判据取值
# 与调用序列，类型精确性由被测模块自身（ops/drill，strict 全绿）保证。
"""六类故障注入脚本（Kiwi 2479；任务 07-6-1）。

覆盖（设计 §9 T1~T9）：键位与场景单源、生产拒绝与二次确认、观测解析、判据正反例、
记录三态与 JSON/Markdown 同源、清理幂等、命令行退出码。用例不启动真实实例（除进程级
小工具外），实例层用替身端口与假传输，真注入由 `python -m ops.drill run <场景>` 执行。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
)

from bms_msg.core.config import DRILL_REJECT_RATIO, DRILL_REPORT_DIR
from ops.drill import engine, record, runtime
from ops.drill.context import (
    as_mapping,
    check,
    items_of,
    offset_of,
    require_ok,
    result_of,
)
from ops.drill.env import (
    DrillError,
    load_config,
    load_scenarios,
    normalize_env,
    reject_production,
    render_scenario_rows,
    require_confirmation,
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
)

pytestmark = pytest.mark.kiwi_id(2479)

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根（用于定位场景单源）。"""


def _hard_spec(key: str = "c") -> DrillCriterionSpec:
    """构造一条硬判据声明（用例只关心结构与取值）。"""
    return DrillCriterionSpec(
        key=key,
        text=key,
        record_keys=ConcurrentStableFrozenList((key,)),
        severity="hard",
        evidence="e",
    )


def _sample(phase: str, *, status: int = 200, ok: bool = True) -> PhaseSample:
    """构造一段观测样本。

    Args:
        phase: 阶段。
        status: /readyz 状态码。
        ok: 是否就绪。

    Returns:
        PhaseSample: 样本。
    """
    return PhaseSample(
        phase=phase,
        captured_at="2026-11-11T00:00:00+00:00",
        readyz_status=status,
        readyz_ok=ok,
        healthz_ok=True,
        metrics=ConcurrentStableFrozenDict({"msg_messages_in_total": 3.0}),
        detail="{}",
    )


def _report(*criteria: Criterion, **overrides: object) -> DrillReport:
    """构造演练报告。

    Args:
        criteria: 判据。
        overrides: 覆盖字段。

    Returns:
        DrillReport: 报告。
    """
    now = datetime(2026, 11, 11, tzinfo=UTC)
    base = DrillReport(
        scenario="process_crash",
        mode="run",
        env="test",
        target="local",
        started_at=now,
        finished_at=now,
        injection=ConcurrentStableFrozenDict({"method": "sigkill_sigterm", "detail": "d", "artifact": "a"}),
        samples=ConcurrentStableFrozenList(
            [_sample("before"), _sample("during", status=0, ok=False), _sample("after")]
        ),
        criteria=ConcurrentStableFrozenList(criteria or [check("k", "e", "a", RESULT_OK)]),
        rto_ms=1234,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(["偏离一"]),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
    )
    if not overrides:
        return base
    merged = cast("Any", {**base.__dict__, **overrides})
    return DrillReport(**merged)


# ---------------------------------------------------------------------------
# T2：键位与场景单源
# ---------------------------------------------------------------------------


def test_normalize_env_trims_and_lowers() -> None:
    """键位解析：环境标识去空白并小写。"""
    assert normalize_env("  PROD ") == "prod"


def test_load_config_requires_work_dir() -> None:
    """键位解析：缺必填键时指名键并失败。"""
    with pytest.raises(DrillError, match="MSG_DRILL_WORK_DIR"):
        load_config({}, repo_root=REPO_ROOT)


def test_load_config_rejects_bad_int(tmp_path: Path) -> None:
    """键位解析：非法整数指名键。"""
    with pytest.raises(DrillError, match="MSG_DRILL_BASE_PORT"):
        load_config({"MSG_DRILL_WORK_DIR": str(tmp_path), "MSG_DRILL_BASE_PORT": "abc"}, repo_root=REPO_ROOT)
    with pytest.raises(DrillError, match="MSG_DRILL_BASE_PORT"):
        load_config({"MSG_DRILL_WORK_DIR": str(tmp_path), "MSG_DRILL_BASE_PORT": "0"}, repo_root=REPO_ROOT)


def test_load_config_rejects_bad_ratio(tmp_path: Path) -> None:
    """键位解析：拒绝水位须为 0~1 的小数。"""
    with pytest.raises(DrillError, match="MSG_DRILL_REJECT_RATIO"):
        load_config({"MSG_DRILL_WORK_DIR": str(tmp_path), "MSG_DRILL_REJECT_RATIO": "x"}, repo_root=REPO_ROOT)
    with pytest.raises(DrillError, match="MSG_DRILL_REJECT_RATIO"):
        load_config({"MSG_DRILL_WORK_DIR": str(tmp_path), "MSG_DRILL_REJECT_RATIO": "1.5"}, repo_root=REPO_ROOT)


def test_load_config_defaults(tmp_path: Path) -> None:
    """键位解析：只给必填键时其余取缺省。"""
    config = load_config({"MSG_DRILL_WORK_DIR": str(tmp_path)}, repo_root=REPO_ROOT)

    assert config.env == "test"
    assert config.base_port == 18_080
    assert config.report_dir == (REPO_ROOT / DRILL_REPORT_DIR).resolve()
    assert config.reject_ratio == DRILL_REJECT_RATIO


def test_load_config_reads_overrides(tmp_path: Path) -> None:
    """键位解析：显式键生效（含相对记录目录按仓根解析）。"""
    config = load_config(
        {
            "MSG_DRILL_WORK_DIR": str(tmp_path),
            "MSG_DRILL_ENV": " DEV ",
            "MSG_DRILL_BASE_PORT": "19100",
            "MSG_DRILL_REPORT_DIR": "out",
            "MSG_DRILL_RECOVERY_BUDGET_MS": "1000",
            "MSG_DRILL_FILL_MB": "16",
            "MSG_DRILL_SLOW_WRITE_MB": "32",
            "MSG_DRILL_REJECT_RATIO": "0.5",
        },
        repo_root=REPO_ROOT,
    )

    assert (config.env, config.base_port, config.fill_mb, config.slow_write_mb) == ("dev", 19100, 16, 32)
    assert config.report_dir == (REPO_ROOT / "out").resolve()
    assert config.recovery_budget_ms == 1000
    assert config.reject_ratio == 0.5


def test_reject_production_blocks() -> None:
    """生产防护：prod / production 一律拒绝。"""
    for name in ("prod", "production"):
        with pytest.raises(DrillError, match="生产环境禁止"):
            reject_production(name)
    reject_production("test")


def test_require_confirmation_matches_scenario() -> None:
    """生产防护：二次确认必须与场景名一致。"""
    require_confirmation("process_crash", "process_crash")
    with pytest.raises(DrillError, match="--confirm process_crash"):
        require_confirmation("process_crash", None)
    with pytest.raises(DrillError, match="--confirm process_crash"):
        require_confirmation("process_crash", "disk_full")


def test_load_scenarios_missing_file(tmp_path: Path) -> None:
    """场景单源：文件不存在时返回错误而不是抛异常。"""
    _, errors = load_scenarios(tmp_path / "nope.toml")

    assert list(errors) == [f"场景单源不存在：{tmp_path / 'nope.toml'}"]


def test_load_scenarios_broken_toml(tmp_path: Path) -> None:
    """场景单源：TOML 语法错误被转成可读错误。"""
    path = tmp_path / "broken.toml"
    path.write_text("schema_version = ", encoding="utf-8")

    scenario_set, errors = load_scenarios(path)

    assert scenario_set.schema_version == 0
    assert any("解析失败" in item for item in errors)


def test_load_scenarios_structure_errors(tmp_path: Path) -> None:
    """场景单源：缺 [[scenarios]] 与条目非表都被指出。"""
    path = tmp_path / "empty.toml"
    path.write_text(
        'schema_version = 2\n[[scenarios]]\nname = "a"\n'
        '[[scenarios.criterion]]\nkey = "c"\ntext = "c"\nrecord_keys = ["c"]\n'
        'severity = "hard"\nevidence = "e"\n',
        encoding="utf-8",
    )
    scenario_set, errors = load_scenarios(path)
    assert not errors
    assert any("inject" in item for item in validate_scenarios(scenario_set, 240_000))

    path.write_text("schema_version = 2\nscenarios = [1]\n", encoding="utf-8")
    _, errors = load_scenarios(path)
    assert any("不是表" in item for item in errors)


def test_load_scenarios_real_file() -> None:
    """场景单源：仓库内的六类齐备且通过校验。"""
    scenario_set, errors = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")

    assert not errors
    assert validate_scenarios(scenario_set, 240_000) == []
    local = [item.name for item in scenario_set.scenarios if item.target == "local"]
    assert local == list(engine.scenario_names())
    assert [item.name for item in scenario_set.scenarios if item.target == "docker"] == [
        "replica_crash",
        "network_partition_multi",
        "split_brain_multi",
    ]


def test_validate_scenarios_reports_each_problem() -> None:
    """场景单源校验：版本 / 元信息 / 重复 / 注入闭集 / 预算逐项报错。"""
    scenario_set = SimpleNamespace(
        schema_version=99,
        updated_at="",
        owner="",
        scenarios=ConcurrentStableFrozenList(
            [
                DrillScenario(
                    name="",
                    title="",
                    inject="nope",
                    observe=ConcurrentStableFrozenList(),
                    criterion=ConcurrentStableFrozenList(),
                    aux_record_keys=ConcurrentStableFrozenList(),
                    recovery_budget_ms=0,
                    expect="",
                ),
                DrillScenario(
                    name="process_crash",
                    title="t",
                    inject="sigkill_sigterm",
                    observe=ConcurrentStableFrozenList(["m"]),
                    criterion=ConcurrentStableFrozenList((_hard_spec(),)),
                    aux_record_keys=ConcurrentStableFrozenList(),
                    recovery_budget_ms=10_000_000,
                    expect="e",
                ),
                DrillScenario(
                    name="process_crash",
                    title="t",
                    inject="sigkill_sigterm",
                    observe=ConcurrentStableFrozenList(["m"]),
                    criterion=ConcurrentStableFrozenList((_hard_spec(),)),
                    aux_record_keys=ConcurrentStableFrozenList(),
                    recovery_budget_ms=1000,
                    expect="e",
                ),
            ]
        ),
    )

    errors = validate_scenarios(cast("object", scenario_set), 240_000)  # type: ignore[arg-type]
    joined = "；".join(errors)

    assert "schema_version 非法" in joined
    assert "meta.updated_at" in joined
    assert "meta.owner" in joined
    assert "缺少 name" in joined
    assert "inject = 'nope' 非法" in joined
    assert "observe 为空" in joined
    assert "criterion 为空" in joined
    assert "expect 为空" in joined
    assert "recovery_budget_ms 须为正整数" in joined
    assert "超出配置预算一个量级" in joined
    assert "name 重复" in joined
    assert "缺少注入方式" in joined


def test_render_scenario_rows_matches_source() -> None:
    """场景单源：渲染表行数与场景数一致（文档表由此粘贴）。"""
    scenario_set, _ = load_scenarios(REPO_ROOT / "deploy" / "drill" / "scenarios.toml")

    rows = render_scenario_rows(scenario_set)

    assert rows[0].startswith("| 场景")
    assert len(rows) == 2 + len(scenario_set.scenarios)


# ---------------------------------------------------------------------------
# T7：记录契约
# ---------------------------------------------------------------------------


def test_report_conclusion_three_states() -> None:
    """记录三态：全 ok → 通过；含 degraded → 降级；含 failed → 失败。"""
    assert _report(check("k", "e", "a", RESULT_OK)).conclusion == CONCLUSION_PASS
    assert _report(check("k", "e", "a", RESULT_DEGRADED)).conclusion == CONCLUSION_DEGRADED
    failed = _report(check("k", "e", "a", RESULT_FAILED))

    assert failed.conclusion == CONCLUSION_FAIL
    assert failed.exit_code == 1
    assert list(failed.reasons) == ["k：a"]
    assert _report(check("k", "e", "a", RESULT_OK)).reasons == []


def test_render_json_keeps_contract_fields() -> None:
    """记录契约：JSON 字段与设计一致（含 backup / conclusion.exit_code）。"""
    text = record.render_json(_report(), service_version="0.1.0", contract_version="0.1.0")
    document = json.loads(text)

    assert document["task"] == "07-6-1"
    assert document["conclusion"] == {"result": CONCLUSION_PASS, "exit_code": 0, "reasons": []}
    assert document["environment"]["target"] == "local"
    assert document["service"] == {"name": "msg", "version": "0.1.0", "contract_version": "0.1.0"}
    assert set(document["samples"]) == {"before", "during", "after"}
    assert document["criteria"][0]["passed"] is True


def test_render_markdown_lists_everything() -> None:
    """记录契约：Markdown 覆盖注入 / 三段观测 / 判据 / 偏离 / 问题 / 遗留。"""
    text = record.render_markdown(_report(), title="进程崩溃", service_version="0.1.0")

    assert "# 故障演练记录 · 进程崩溃" in text
    assert "**结论：通过**（退出码 0）" in text
    assert "| 注入前 |" in text
    assert "`k`" in text
    assert "偏离一" in text
    assert "- 无" in text  # issues / leftovers 为空时写「无」


def test_render_markdown_failure_lists_reasons() -> None:
    """记录契约：失败记录带「未通过原因」小节与降级/失败标签。"""
    report = _report(check("k", "e", "a", RESULT_FAILED), check("d", "e", "b", RESULT_DEGRADED))

    text = record.render_markdown(report, title="磁盘满", service_version="0.1.0")

    assert "## 未通过原因" in text
    assert "| 失败 |" in text
    assert "| 降级 |" in text


def test_render_metric_precision() -> None:
    """记录契约：整数指标不带小数，浮点保留四位（与水位比较精度一致）。"""
    assert record._format_metric(3.0) == "3"  # pyright: ignore[reportPrivateUsage]
    assert record._format_metric(0.35694) == "0.3569"  # pyright: ignore[reportPrivateUsage]


def test_write_reports_writes_both(tmp_path: Path) -> None:
    """记录契约：JSON 与 Markdown 同源落盘，失败也写。"""
    json_path, markdown_path = record.write_reports(
        _report(check("k", "e", "a", RESULT_FAILED)),
        tmp_path / "reports",
        title="进程崩溃",
        service_version="0.1.0",
        contract_version="0.1.0",
    )

    assert json_path.is_file() and markdown_path.is_file()
    assert json.loads(json_path.read_text(encoding="utf-8"))["conclusion"]["result"] == CONCLUSION_FAIL


# ---------------------------------------------------------------------------
# T5：观测解析与上下文助手
# ---------------------------------------------------------------------------


def test_parse_metrics_handles_labels_and_histograms() -> None:
    """观测解析：普通指标取首条、直方图按 sum/count 合成、坏行跳过。"""
    text = "\n".join(
        [
            "# HELP msg_x",
            'msg_messages_in_total{tenant="t",topic="a"} 7',
            'msg_messages_in_total{tenant="u"} 9',
            'msg_fsync_duration_ms_sum{tenant="t"} 30',
            'msg_fsync_duration_ms_count{tenant="t"} 4',
            "msg_broken not-a-number",
            "",
        ]
    )

    keys = ConcurrentStableFrozenDict({"msg_messages_in_total": "", "msg_fsync_duration_ms": ""})

    values = runtime.parse_metrics(text, keys)

    assert values["msg_messages_in_total"] == 7.0
    assert values["msg_fsync_duration_ms"] == 7.5
    assert "msg_broken" not in values


def test_parse_metrics_without_filter_keeps_all() -> None:
    """观测解析：不给关心集合时返回全部指标。"""
    values = runtime.parse_metrics("msg_a 1\nmsg_b 2\n")

    assert values["msg_a"] == 1.0 and values["msg_b"] == 2.0


def test_payload_of_handles_non_json() -> None:
    """观测解析：非 JSON 响应体返回空表。"""
    assert runtime.payload_of(httpx.Response(200, text="<html>")) == {}
    assert runtime.payload_of(httpx.Response(200, json={"a": 1})) == {"a": 1}
    assert runtime.payload_of(httpx.Response(200, json=[1, 2])) == {}


def test_as_mapping_and_items() -> None:
    """上下文助手：映射收窄与数组条目解析。"""
    assert as_mapping({"a": 1})["a"] == 1
    assert as_mapping([1]) == {}
    body: ConcurrentStableFrozenDict[str, object] = ConcurrentStableFrozenDict(
        {"messages": [{"offset": 1}, {"offset": 2}, 3]}
    )

    rows = items_of(body)

    assert [offset_of(item) for item in rows] == [1, 2, 0]
    assert items_of(body, "missing") == []


def test_require_ok_rejects_status_and_business_code() -> None:
    """上下文助手：非 2xx 与业务码非 0 都抛错并带摘要。"""
    with pytest.raises(DrillError, match="HTTP 500"):
        require_ok(httpx.Response(500, text="boom"), "发布")
    with pytest.raises(DrillError, match="业务码 350022"):
        require_ok(httpx.Response(200, json={"code": 350022, "message": "满"}), "发布")

    data = require_ok(httpx.Response(200, json={"code": 0, "data": {"offset": 4}}), "发布")
    assert data["offset"] == 4


def test_result_of_maps_boolean() -> None:
    """上下文助手：布尔判定映射成判据结果。"""
    assert result_of(True) == RESULT_OK
    assert result_of(False) == RESULT_FAILED


def test_sample_phase_reads_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    """观测采样：三段快照带上探针状态与指标子集。"""
    http = SimpleNamespace(
        readyz=lambda: (503, False, '{"status":"degraded"}'),
        metrics=lambda: "msg_consumer_lag 5\n",
        healthz=lambda: True,
    )

    sample = runtime.sample_phase("during", cast("runtime.DrillHttp", http), ("msg_consumer_lag", "msg_missing"))

    assert (sample.phase, sample.readyz_status, sample.readyz_ok, sample.healthz_ok) == ("during", 503, False, True)
    assert sample.metrics["msg_consumer_lag"] == 5.0
    assert sample.metrics["msg_missing"] == 0.0
