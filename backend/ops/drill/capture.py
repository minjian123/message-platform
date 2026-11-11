"""记录增强（任务 07-6-2）：动作轨迹、原始产物、日志摘要、配置摘要与复跑标识。

为什么单独一个模块：这些字段只服务「可复现 / 可比较」，与判据无关；把它们从 `engine` /
`record` 里拆出来，既方便单测，也避免判据逻辑被取证细节污染。

所有路径一律相对演练工作根，记录里不出现宿主盘符。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import DRILL_COMMAND_LIMIT, DRILL_OUTPUT_TAIL_CHARS
from ops.drill.env import DrillConfig
from ops.drill.runtime import DrillHttp

__all__ = [
    "collect_artifacts",
    "command_trail",
    "config_digest",
    "log_snapshot",
    "previous_record",
    "relative_path",
    "rerun_block",
    "sha256_file",
]


def sha256_file(path: Path) -> str:
    """计算文件 sha256（分块读，避免大文件占内存）。

    Args:
        path: 文件路径。

    Returns:
        str: 十六进制摘要。
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative(work_dir: Path, path: Path) -> str:
    """把路径转成相对工作根的文本（不在其下时只留文件名）。

    Args:
        work_dir: 演练工作根。
        path: 目标路径。

    Returns:
        str: 相对路径文本。
    """
    try:
        return path.resolve().relative_to(work_dir.resolve()).as_posix()
    except ValueError:
        return path.name


def relative_path(work_dir: Path, path: Path) -> str:
    """公开的相对路径入口（记录渲染与产物清单共用）。

    Args:
        work_dir: 演练工作根。
        path: 目标路径。

    Returns:
        str: 相对路径文本。
    """
    return _relative(work_dir, path)


def _file_artifact(work_dir: Path, path: Path, role: str) -> ConcurrentStableFrozenDict[str, object]:
    """构造一个文件产物条目。

    Args:
        work_dir: 演练工作根。
        path: 文件路径。
        role: 产物角色（`log` / `fill` / `slowdisk`）。

    Returns:
        ConcurrentStableFrozenDict[str, object]: 产物条目。
    """
    return ConcurrentStableFrozenDict(
        {
            "path": _relative(work_dir, path),
            "role": role,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    )


def _directory_artifact(work_dir: Path, path: Path, role: str) -> ConcurrentStableFrozenDict[str, object]:
    """构造一个目录产物条目（目录只登记存在性与路径，不递归哈希）。

    Args:
        work_dir: 演练工作根。
        path: 目录路径。
        role: 产物角色。

    Returns:
        ConcurrentStableFrozenDict[str, object]: 产物条目。
    """
    return ConcurrentStableFrozenDict({"path": _relative(work_dir, path), "role": role, "bytes": None, "sha256": None})


def collect_artifacts(config: DrillConfig) -> ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]:
    """收集本次运行的原始产物（存在才登记）。

    Args:
        config: 演练配置（工作根即产物根）。

    Returns:
        ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]: 产物清单。
    """
    work_dir = config.work_dir
    items: ConcurrentStableList[ConcurrentStableFrozenDict[str, object]] = ConcurrentStableList()
    data_root = work_dir / "var"
    if data_root.is_dir():
        items.add(_directory_artifact(work_dir, data_root, "data_root"))
    for role, path in (
        ("log", work_dir / "instance.log"),
        ("fill", work_dir / "fill" / "volume.bin"),
        ("slowdisk", work_dir / "slowdisk" / "pressure.bin"),
    ):
        if path.is_file():
            items.add(_file_artifact(work_dir, path, role))
    return ConcurrentStableFrozenList(items)


def log_snapshot(config: DrillConfig) -> ConcurrentStableFrozenDict[str, object]:
    """采集实例日志摘要（总行数 / 字节 / sha256 + 尾部文本）。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableFrozenDict[str, object]: 日志摘要（无日志时为空）。
    """
    path = config.work_dir / "instance.log"
    if not path.is_file():
        return ConcurrentStableFrozenDict()
    text = path.read_text(encoding="utf-8", errors="replace")
    return ConcurrentStableFrozenDict(
        {
            "path": _relative(config.work_dir, path),
            "lines": len(text.splitlines()),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "excerpt": text[-DRILL_OUTPUT_TAIL_CHARS:],
        }
    )


def command_trail(http: DrillHttp) -> ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]:
    """把客户端的动作轨迹截成记录可承载的长度（超限保留首尾并标 `truncated`）。

    Args:
        http: 演练客户端（调用点已记录动作）。

    Returns:
        ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]: 动作条目。
    """
    raw = list(http.commands)
    if len(raw) <= DRILL_COMMAND_LIMIT:
        return ConcurrentStableFrozenList(raw)
    half = DRILL_COMMAND_LIMIT // 2
    items: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList(raw[:half])
    items.add(
        ConcurrentStableFrozenDict(
            {"seq": "0", "action": "truncated", "detail": f"省略 {len(raw) - DRILL_COMMAND_LIMIT} 条"}
        )
    )
    items.extend(raw[-half:])
    return ConcurrentStableFrozenList(items)


def config_digest(config: DrillConfig, scenario: str) -> str:
    """计算生效配置摘要（跨版本比较用）。

    Args:
        config: 演练配置。
        scenario: 场景标识。

    Returns:
        str: 十六进制摘要。
    """
    payload = {
        "env": config.env,
        "target": "local",
        "recovery_budget_ms": config.recovery_budget_ms,
        "fill_mb": config.fill_mb,
        "slow_write_mb": config.slow_write_mb,
        "reject_ratio": config.reject_ratio,
        "scenario": scenario,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def rerun_block(previous: Mapping[str, object] | None, reason: str) -> ConcurrentStableFrozenDict[str, str]:
    """按上一份记录构造复跑标识（首跑为空）。

    Args:
        previous: 上一份记录（解析后的 JSON；None 表示首跑）。
        reason: 复跑原因。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 复跑标识。
    """
    if previous is None:
        return ConcurrentStableFrozenDict()
    raw_rerun = previous.get("rerun")
    attempt = 1
    if isinstance(raw_rerun, Mapping):
        attempt = int(cast("int", cast("Mapping[str, object]", raw_rerun).get("attempt") or 1))
    raw_conclusion = previous.get("conclusion")
    conclusion = ""
    if isinstance(raw_conclusion, Mapping):
        conclusion = str(cast("Mapping[str, object]", raw_conclusion).get("result") or "")
    return ConcurrentStableFrozenDict(
        {
            "attempt": str(attempt + 1),
            "previous_started_at": str(previous.get("started_at") or ""),
            "previous_conclusion": conclusion,
            "reason": reason,
        }
    )


def previous_record(config: DrillConfig, scenario: str) -> Mapping[str, object] | None:
    """读取同目录上一份记录（解析失败视为首跑，不阻断本次演练）。

    Args:
        config: 演练配置。
        scenario: 场景标识。

    Returns:
        Mapping[str, object] | None: 上一份记录（不存在或不可解析时 None）。
    """
    path = config.report_dir / f"{scenario}.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError, json.JSONDecodeError, OSError:
        return None
    if not isinstance(raw, dict):
        return None
    return cast("Mapping[str, object]", raw)
