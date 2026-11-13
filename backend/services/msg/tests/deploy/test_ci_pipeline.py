"""00-2 工程化与文档基线收口 · CI 流水线结构护栏。

覆盖：流水线可解析 / 触发口径 / 必备 job / 无镜像 job / 覆盖率门禁 / 准备脚本 / 串行闸门。
"""

from pathlib import Path
from typing import cast

import pytest
import yaml

pytestmark = pytest.mark.kiwi_id(2492)
"""本文件 7 例共用 Kiwi 2492（00-2 工程化与文档基线收口）。"""

ROOT = Path(__file__).resolve().parents[5]
"""msg 仓库根（`backend/services/msg/tests/deploy/` 向上五层）。"""

CI_FILE = ROOT / ".gitlab-ci.yml"
"""流水线文件。"""


def _pipeline() -> dict[str, object]:
    """读取并解析流水线。

    Returns:
        dict[str, object]: YAML 顶层映射。
    """
    loaded = yaml.safe_load(CI_FILE.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return cast("dict[str, object]", loaded)


def _flatten(value: object) -> list[str]:
    """把 script 里的 YAML 锚点嵌套列表展平。

    Args:
        value: 任意 YAML 值。

    Returns:
        list[str]: 字符串行。
    """
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        lines: list[str] = []
        for item in cast("list[object]", value):
            lines.extend(_flatten(item))
        return lines
    return []


def _script_text(job: str) -> str:
    """取某 job 的脚本全文。

    Args:
        job: job 名。

    Returns:
        str: 换行拼接的脚本。
    """
    data = _pipeline()
    entry = cast("dict[str, object]", data[job])
    return "\n".join(_flatten(entry.get("script", [])))


def test_ci_file_exists_and_parses() -> None:
    """流水线存在且阶段固定为 lint / test。"""
    assert CI_FILE.is_file()
    data = _pipeline()
    assert data["stages"] == ["lint", "test"]


def test_trigger_only_manual() -> None:
    """触发口径：只有 web / api，推送与 MR 不建流水线。"""
    data = _pipeline()
    workflow = cast("dict[str, object]", data["workflow"])
    rules = cast("list[dict[str, str]]", workflow["rules"])
    conditions = [rule.get("if", "") for rule in rules]
    assert conditions
    assert all("web" in item or "api" in item for item in conditions)
    assert not any("push" in item or "merge_request_event" in item for item in conditions)


def test_required_jobs() -> None:
    """必备 job 全部存在，且串行闸门各一个。"""
    data = _pipeline()
    required = {
        "ci-queue-acquire",
        "ci-queue-release",
        "gate-smoke",
        "backend-lint",
        "backend-typecheck",
        "backend-test",
        "backend-modules",
        "docs-links",
        "docs-scope",
        "frontend-lint",
        "frontend-test",
        "frontend-build",
    }
    assert required <= set(data)


def test_no_image_jobs() -> None:
    """不含镜像构建 / Registry 推送（部署能力已剥离）。"""
    data = _pipeline()
    names = set(data)
    assert not any("image-build" in name or "registry" in name for name in names)
    assert "docker build" not in CI_FILE.read_text(encoding="utf-8")


def test_coverage_gate() -> None:
    """覆盖率走 engkit coverage，不走 pytest --cov。"""
    text = _script_text("backend-test")
    assert "engkit coverage" in text
    assert "--fail-under 100" in text
    assert "--cov" not in text


def test_setup_scripts_exist() -> None:
    """两仓准备脚本存在且含 ref 解析。"""
    bms = ROOT / "scripts" / "tools" / "ci" / "bms_setup.sh"
    engkit = ROOT / "scripts" / "tools" / "ci" / "engkit_setup.sh"
    assert bms.is_file()
    assert engkit.is_file()
    assert "BMS_REF" in bms.read_text(encoding="utf-8")
    assert "ENGKIT_REV" in engkit.read_text(encoding="utf-8")


def test_serial_gate() -> None:
    """串行闸门：取锁在 .pre、释放在 .post，锁名与 bms / mdm 同键。"""
    data = _pipeline()
    acquire = cast("dict[str, object]", data["ci-queue-acquire"])
    release = cast("dict[str, object]", data["ci-queue-release"])
    assert acquire["stage"] == ".pre"
    assert release["stage"] == ".post"
    text = CI_FILE.read_text(encoding="utf-8")
    assert "CI_QUEUE_LOCK: ci-queue-lock" in text
