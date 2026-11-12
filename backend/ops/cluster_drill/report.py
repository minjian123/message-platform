"""集群演练记录（02-8）：复用 07-6 记录契约写 JSON / Markdown。

**为什么复用**：记录格式版本、字段语义与 07-6 完全一致，证据矩阵（`ops.drill verify`）无需分叉；
`target=docker` 已由 `DrillReport.target` 表达。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableFrozenDict

from ops.cluster_drill.config import ClusterDrillConfig
from ops.drill.model import DrillReport
from ops.drill.record import write_reports

__all__ = ["cluster_config_digest", "write_cluster_report"]

_TARGET_DOCKER = "docker"
"""记录里的 `target` 取值（与 `ops.drill` 的 `local` 区分）。"""


def cluster_config_digest(config: ClusterDrillConfig, scenario: str) -> str:
    """计算生效配置摘要（跨版本比较用；不含凭据与主机地址）。

    Args:
        config: 演练配置。
        scenario: 场景标识。

    Returns:
        str: 十六进制摘要。
    """
    payload = {
        "env": config.env,
        "target": _TARGET_DOCKER,
        "project": config.project,
        "container_prefix": config.container_prefix,
        "db_prefix": config.db_prefix,
        "ports": list(config.ports),
        "ready_timeout_sec": config.ready_timeout_sec,
        "recovery_budget_ms": config.recovery_budget_ms,
        "scenario": scenario,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def backup_evidence(config: ClusterDrillConfig) -> ConcurrentStableFrozenDict[str, str]:
    """演练前备份证据：隔离演练环境记 `not_applicable` + 理由（证据矩阵 G7 要求非空理由）。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableFrozenDict[str, str]: `mode` / `reason`。
    """
    return ConcurrentStableFrozenDict(
        {
            "mode": "not_applicable",
            "reason": (f"隔离演练环境（{config.env}）：独立容器 / 派生库前缀 / 独立 Redis 库，不写生产数据，无需备份"),
        }
    )


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
