"""集群演练记录（02-8）：复用 07-6 记录契约写 JSON / Markdown。

**为什么复用**：记录格式版本、字段语义与 07-6 完全一致，证据矩阵（`ops.drill verify`）无需分叉；
`target=docker` 已由 `DrillReport.target` 表达。
"""

from __future__ import annotations

from pathlib import Path

from ops.drill.model import DrillReport
from ops.drill.record import write_reports

__all__ = ["write_cluster_report"]


def write_cluster_report(report: DrillReport, report_dir: Path, *, title: str) -> tuple[Path, Path]:
    """写集群演练记录（JSON + Markdown）。

    Args:
        report: 演练报告。
        report_dir: 记录目录。
        title: 场景标题。

    Returns:
        tuple[Path, Path]: JSON 与 Markdown 路径。
    """
    from bms_msg import CONTRACT_VERSION, __version__

    return write_reports(
        report,
        report_dir,
        title=title,
        service_version=__version__,
        contract_version=CONTRACT_VERSION,
    )
