"""告警规则与看板静态断言（06-3-2 / Kiwi 2461）。

**不解析 YAML**：规则语法由服务器容器内 `promtool check rules` 校验；本用例只做
「指标引用不悬空 / 分级映射 / 注解齐全 / 占位标记」的结构断言，避免在本地引入新依赖。
"""

import json
import re
from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableList
from pytest import mark

from bms_msg.metrics.names import COUNTER_NAMES, GAUGE_NAMES, HISTOGRAM_NAMES

_DEPLOY = Path(__file__).resolve().parents[5] / "deploy" / "observability"
_RULES = _DEPLOY / "rules" / "msg.yml"
_DASHBOARD = _DEPLOY / "grafana" / "dashboards" / "msg.json"
_RULE_TEST = _DEPLOY / "tests" / "msg-rules.test.yml"

_METRIC_PATTERN = re.compile(r"\bmsg_[a-z0-9_]+\b")
_KNOWN: ConcurrentStableList[str] = ConcurrentStableList(
    [*COUNTER_NAMES, *GAUGE_NAMES, *HISTOGRAM_NAMES, "msg_dlq_growth_total"]
)


def _base_name(name: str) -> str:
    """去掉 Prometheus 派生后缀（`_bucket` / `_sum` / `_count` / `_created`）。

    Args:
        name: 指标名。

    Returns:
        str: 基础指标名。
    """
    for suffix in ("_bucket", "_sum", "_count", "_created"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


@mark.kiwi_id(2461)
def test_rules_reference_only_known_metrics() -> None:
    """规则表达式只引用 06-3-1 登记的指标名。"""
    text = _RULES.read_text(encoding="utf-8")
    for name in _METRIC_PATTERN.findall(text):
        if name not in _KNOWN:
            raise AssertionError(f"规则引用了未登记指标：{name}")


@mark.kiwi_id(2461)
def test_rules_levels_and_annotations() -> None:
    """每条告警都有 severity / priority，且 P0-P1 为 critical、P2-P3 为 warning。"""
    text = _RULES.read_text(encoding="utf-8")
    blocks = re.split(r"\n\s+- alert: ", text)[1:]
    assert len(blocks) >= 13
    for block in blocks:
        alert_name = block.splitlines()[0].strip()
        severity = re.search(r"severity:\s*(\w+)", block)
        priority = re.search(r"priority:\s*(P\d)", block)
        assert severity is not None, f"{alert_name} 缺 severity"
        assert priority is not None, f"{alert_name} 缺 priority"
        expected = "critical" if priority.group(1) in ("P0", "P1") else "warning"
        assert severity.group(1) == expected, f"{alert_name} 分级映射错误"
        for key in ("summary:", "description:", "runbook_url:", "dashboard_url:"):
            assert key in block, f"{alert_name} 缺 {key}"


@mark.kiwi_id(2461)
def test_placeholder_thresholds_are_marked() -> None:
    """未实测阈值均带 8B_12 待回填标记。"""
    text = _RULES.read_text(encoding="utf-8")
    assert text.count("TODO(8B_12)") >= 8


@mark.kiwi_id(2461)
def test_rule_test_file_covers_all_alerts() -> None:
    """promtool 回归文件覆盖全部告警（低值不触发）。"""
    rules = _RULES.read_text(encoding="utf-8")
    tests = _RULE_TEST.read_text(encoding="utf-8")
    for alert_name in re.findall(r"- alert:\s*(\w+)", rules):
        assert f"alertname: {alert_name}" in tests, f"回归文件缺 {alert_name}"
    assert "exp_alerts: []" in tests


@mark.kiwi_id(2461)
def test_dashboard_structure_and_metric_refs() -> None:
    """看板 JSON 合法、UID / 标题 / 面板齐全，表达式只引用已登记指标。"""
    payload = json.loads(_DASHBOARD.read_text(encoding="utf-8"))
    assert payload["uid"] == "msg-overview"
    assert payload["title"]
    panels = payload["panels"]
    assert len(panels) >= 8
    for panel in panels:
        assert panel["datasource"]["uid"] == "prometheus"
        for target in panel["targets"]:
            assert target["expr"]
            for name in _METRIC_PATTERN.findall(target["expr"]):
                assert _base_name(name) in _KNOWN, f"看板引用未登记指标：{name}"


@mark.kiwi_id(2461)
def test_runbook_path_exists() -> None:
    """告警 runbook 指向的设计文档确实存在。"""
    text = _RULES.read_text(encoding="utf-8")
    match = re.search(r'runbook_url: "([^"]+)"', text)
    assert match is not None
    repo_root = Path(__file__).resolve().parents[5]
    target = repo_root / match.group(1).split("#", 1)[0]
    assert target.exists(), f"runbook 文档不存在：{target}"
