"""演练记录渲染（任务 07-6-1）：JSON 与 Markdown **同源**，一次渲染两种载体。

为什么同源：记录被两方消费——07-6-2 机器判读 `criteria[]` / `samples`，人看 Markdown 结论表。
两份各写一遍必然漂移；此处只允许「同一 `DrillReport` → 两种文本」，字段口径写在 `build_document`。

失败也落盘：结论失败时同样产出两份文件，`conclusion.result` 与退出码一致（0 / 2 / 1）。
"""

from __future__ import annotations

import json
import platform
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableList,
)

from bms_msg.core.config import DRILL_RECORD_SCHEMA_VERSION
from ops.drill.model import (
    CONCLUSION_PASS,
    RESULT_OK,
    DrillReport,
    PhaseSample,
)

__all__ = ["build_document", "render_json", "render_markdown", "write_reports"]

_TASK_ID = "07-6-1"
"""任务标识（记录顶层，供 07-6-2 归集）。"""

_SAMPLE_PHASES: ConcurrentStableFrozenDict[str, str] = ConcurrentStableFrozenDict(
    {"before": "注入前", "during": "注入中", "after": "恢复后"}
)
"""三段样本的固定顺序与中文标题（记录与控制台一致）。"""

_RESULT_LABELS: ConcurrentStableFrozenDict[str, str] = ConcurrentStableFrozenDict(
    {"ok": "通过", "degraded": "降级", "failed": "失败", "skipped": "未执行"}
)
"""判据结果的中文标签。"""


def _plain(value: object) -> object:
    """把并发集合递归转为 JSON 可序列化的原生结构（仅在渲染边界使用）。

    Args:
        value: 任意嵌套值（并发集合 / 序列 / 标量）。

    Returns:
        object: 原生结构（Mapping → dict、Sequence → list）。
    """
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in cast("Mapping[object, object]", value).items()}
    if isinstance(value, str | bytes):
        return value
    if isinstance(value, Sequence):
        return [_plain(item) for item in cast("Sequence[object]", value)]
    return value


def _sample_document(sample: PhaseSample) -> ConcurrentStableDict[str, object]:
    """把一段观测快照转成记录结构。

    Args:
        sample: 快照。

    Returns:
        ConcurrentStableDict[str, object]: 记录字段。
    """
    metrics: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    for name, value in sample.metrics.items():
        metrics.set(name, round(value, 6))
    return ConcurrentStableDict(
        {
            "captured_at": sample.captured_at,
            "readyz_status": sample.readyz_status,
            "readyz_ok": sample.readyz_ok,
            "healthz_ok": sample.healthz_ok,
            "metrics": metrics,
            "detail": sample.detail,
        }
    )


def build_document(
    report: DrillReport, *, service_version: str, contract_version: str
) -> ConcurrentStableDict[str, object]:
    """构造记录文档（JSON 字段的唯一口径）。

    Args:
        report: 演练报告。
        service_version: 服务版本。
        contract_version: 契约版本。

    Returns:
        ConcurrentStableDict[str, object]: 记录结构。
    """
    samples: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    for sample in report.samples:
        samples.set(sample.phase, _sample_document(sample))
    criteria: ConcurrentStableList[object] = ConcurrentStableList()
    for item in report.criteria:
        criteria.append(
            ConcurrentStableDict(
                {
                    "key": item.key,
                    "expect": item.expect,
                    "actual": item.actual,
                    "result": item.result,
                    "passed": item.result == RESULT_OK,
                }
            )
        )
    reasons: ConcurrentStableList[object] = ConcurrentStableList(report.reasons)
    return ConcurrentStableDict(
        {
            "schema_version": DRILL_RECORD_SCHEMA_VERSION,
            "task": _TASK_ID,
            "scenario": report.scenario,
            "mode": report.mode,
            "environment": ConcurrentStableDict(
                {
                    "env": report.env,
                    "target": report.target,
                    "os": platform.platform(),
                    "python": platform.python_version(),
                    "config_digest": report.config_digest,
                }
            ),
            "service": ConcurrentStableDict(
                {"name": "msg", "version": service_version, "contract_version": contract_version}
            ),
            "started_at": _iso(report.started_at),
            "finished_at": _iso(report.finished_at),
            "injection": ConcurrentStableDict(dict(report.injection)),
            "commands": ConcurrentStableList(dict(item) for item in report.commands),
            "artifacts": ConcurrentStableList(dict(item) for item in report.artifacts),
            "logs": ConcurrentStableDict(dict(report.logs)),
            "samples": samples,
            "criteria": criteria,
            "rto_ms": report.rto_ms,
            "data_loss": report.data_loss,
            "backup": ConcurrentStableDict(dict(report.backup)),
            "rerun": ConcurrentStableDict(dict(report.rerun)),
            "conclusion": ConcurrentStableDict(
                {"result": report.conclusion, "exit_code": report.exit_code, "reasons": reasons}
            ),
            "deviations": ConcurrentStableList(report.deviations),
            "issues": ConcurrentStableList(report.issues),
            "leftovers": ConcurrentStableList(report.leftovers),
        }
    )


def _iso(value: datetime) -> str:
    """把时刻渲染成 ISO8601。

    Args:
        value: 时刻。

    Returns:
        str: ISO8601 文本。
    """
    return value.isoformat()


def render_json(report: DrillReport, *, service_version: str, contract_version: str) -> str:
    """渲染 JSON 记录（缩进 2 空格、保留中文、键序固定）。

    Args:
        report: 演练报告。
        service_version: 服务版本。
        contract_version: 契约版本。

    Returns:
        str: JSON 文本（结尾带换行）。
    """
    document = build_document(report, service_version=service_version, contract_version=contract_version)
    return json.dumps(_plain(document), ensure_ascii=False, indent=2) + "\n"


def _sample_table(report: DrillReport) -> ConcurrentStableList[str]:
    """渲染三段观测表（指标列取三段并集）。

    Args:
        report: 演练报告。

    Returns:
        ConcurrentStableList[str]: Markdown 行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    columns: ConcurrentStableList[str] = ConcurrentStableList()
    for sample in report.samples:
        for name in sample.metrics:
            if name not in columns:
                columns.append(name)
    header: ConcurrentStableList[str] = ConcurrentStableList(["阶段", "时刻", "/readyz", "/healthz"])
    for name in columns:
        header.append(f"`{name}`")
    rows.append("| " + " | ".join(header) + " |")
    rows.append("| --- | --- | --- | --- |" + " --- |" * len(columns))
    for sample in report.samples:
        cells: ConcurrentStableList[str] = ConcurrentStableList(
            [
                _SAMPLE_PHASES.get(sample.phase, sample.phase),
                sample.captured_at,
                f"{sample.readyz_status}{'（就绪）' if sample.readyz_ok else '（未就绪）'}",
                "200" if sample.healthz_ok else "不可达",
            ]
        )
        for name in columns:
            cells.append(_format_metric(sample.metrics.get(name, 0.0)))
        rows.append("| " + " | ".join(cells) + " |")
    return rows


def _format_metric(value: float) -> str:
    """渲染指标值（整数不带小数位，浮点保留四位——水位判定的比较精度就是四位）。

    Args:
        value: 指标值。

    Returns:
        str: 文本。
    """
    return str(int(value)) if value == int(value) else f"{value:.4f}"


def _criteria_table(report: DrillReport) -> ConcurrentStableList[str]:
    """渲染判据表。

    Args:
        report: 演练报告。

    Returns:
        ConcurrentStableList[str]: Markdown 行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList(
        ["| 判据 | 期望 | 实测 | 结果 |", "| --- | --- | --- | --- |"]
    )
    for item in report.criteria:
        label = _RESULT_LABELS.get(item.result, item.result)
        rows.append(f"| `{item.key}` | {item.expect} | {item.actual} | {label} |")
    return rows


def render_markdown(report: DrillReport, *, title: str, service_version: str) -> str:
    """渲染 Markdown 记录（与 JSON 同源；人工复核用）。

    Args:
        report: 演练报告。
        title: 场景中文标题。
        service_version: 服务版本。

    Returns:
        str: Markdown 文本（结尾带换行）。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"# 故障演练记录 · {title}")
    rows.append("")
    rows.append(f"- 场景：`{report.scenario}`")
    rows.append(f"- 模式：`{report.mode}`")
    rows.append(f"- 环境：`{report.env}` / 目标 `{report.target}`")
    rows.append(f"- 服务版本：`{service_version}`")
    rows.append(f"- 起止：`{_iso(report.started_at)}` → `{_iso(report.finished_at)}`")
    rows.append(f"- 恢复耗时：`{report.rto_ms} ms`")
    rows.append(f"- 已确认消息丢失：`{'是' if report.data_loss else '否'}`")
    rows.append(f"- 演练前备份：`{report.backup.get('mode', '未记录')}`（{report.backup.get('reason', '无')}）")
    rows.append(f"- **结论：{report.conclusion}**（退出码 {report.exit_code}）")
    rows.append("")
    if report.conclusion != CONCLUSION_PASS:
        rows.append("## 未通过原因")
        rows.append("")
        for reason in report.reasons:
            rows.append(f"- {reason}")
        rows.append("")
    rows.append("## 1. 注入")
    rows.append("")
    for key, value in report.injection.items():
        rows.append(f"- {key}：{value}")
    rows.append("")
    rows.append("## 2. 三段观测")
    rows.append("")
    rows.extend(_sample_table(report))
    rows.append("")
    rows.append("## 3. 判据")
    rows.append("")
    rows.extend(_criteria_table(report))
    rows.append("")
    rows.append("## 4. 偏离项")
    rows.append("")
    if report.deviations:
        for item in report.deviations:
            rows.append(f"- {item}")
    else:
        rows.append("- 无")
    rows.append("")
    rows.append("## 5. 执行期问题")
    rows.append("")
    if report.issues:
        for item in report.issues:
            rows.append("- " + "；".join(f"{key}={value}" for key, value in item.items()))
    else:
        rows.append("- 无")
    rows.append("")
    rows.append("## 6. 遗留")
    rows.append("")
    if report.leftovers:
        for item in report.leftovers:
            rows.append("- " + "；".join(f"{key}={value}" for key, value in item.items()))
    else:
        rows.append("- 无")
    rows.append("")
    rows.append("## 7. 复现线索")
    rows.append("")
    rows.append(f"- 配置摘要：`{report.config_digest or '未记录'}`")
    if report.rerun:
        rows.append(
            "- 复跑：第 "
            f"{report.rerun.get('attempt', '?')} 次，前次 `{report.rerun.get('previous_started_at', '无')}`"
            f"（{report.rerun.get('previous_conclusion', '无')}）；原因：{report.rerun.get('reason', '未记录')}"
        )
    else:
        rows.append("- 复跑：首跑")
    rows.append(f"- 动作轨迹：{len(report.commands)} 条")
    if report.artifacts:
        for item in report.artifacts:
            rows.append(
                f"- 产物 `{item.get('path', '')}`（{item.get('role', '')}，"
                f"{item.get('bytes', '')} 字节，sha256 `{item.get('sha256', '')}`）"
            )
    else:
        rows.append("- 产物：无")
    if report.logs:
        rows.append(
            f"- 日志：`{report.logs.get('path', '')}` 共 {report.logs.get('lines', 0)} 行、"
            f"{report.logs.get('bytes', 0)} 字节，sha256 `{report.logs.get('sha256', '')}`"
        )
    else:
        rows.append("- 日志：未采集")
    rows.append("")
    return "\n".join(rows)


def write_reports(
    report: DrillReport, directory: Path, *, title: str, service_version: str, contract_version: str
) -> tuple[Path, Path]:
    """把 JSON 与 Markdown 两份记录写到同一目录（同名不同后缀）。

    Args:
        report: 演练报告。
        directory: 记录目录（不存在则创建）。
        title: 场景中文标题。
        service_version: 服务版本。
        contract_version: 契约版本。

    Returns:
        tuple[Path, Path]: JSON 路径与 Markdown 路径。
    """
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / f"{report.scenario}.json"
    markdown_path = directory / f"{report.scenario}.md"
    json_path.write_text(
        render_json(report, service_version=service_version, contract_version=contract_version), encoding="utf-8"
    )
    markdown_path.write_text(render_markdown(report, title=title, service_version=service_version), encoding="utf-8")
    return json_path, markdown_path
