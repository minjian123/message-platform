"""发版前检查清单（任务 07-6-2）：从场景单源渲染，禁止手写第二份。

清单是**生成物**：`checklist --check` 与仓内文件逐行比对，判据或顺序一变就报漂移。
"""

from __future__ import annotations

from pathlib import Path

from bms_core.core.concurrent import ConcurrentStableFrozenList, ConcurrentStableList

from bms_msg.core.config import DRILL_CHECKLIST_PATH
from ops.drill.env import render_scenario_rows
from ops.drill.model import ScenarioSet

__all__ = ["build_checklist", "check_checklist", "checklist_path", "normalize", "write_checklist"]

_HEADER: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (
        "# 发版前故障演练检查清单（生成物）",
        "",
        "> 本文件由 `python -m ops.drill checklist` 从 `deploy/drill/scenarios.toml` 生成，**勿手改**。",
        "> 重新生成：`cd backend && python -m ops.drill checklist`。",
        "",
        "## 1. 前置门禁",
        "",
        "- [ ] 环境不是 `prod` / `production`（硬拒绝，无逃生开关）",
        "- [ ] 已显式传 `--confirm release`（逐条执行时为 `--confirm <场景>`）",
        "- [ ] 非隔离环境已给 `MSG_DRILL_BACKUP_EVIDENCE`（隔离实例记不适用 + 理由）",
        "- [ ] `MSG_DRILL_WORK_DIR` 指向隔离临时目录，不是真实数据目录",
        "- [ ] `MSG_DRILL_REPORT_DIR` 已归档上一轮记录（本轮记录不覆盖历史证据）",
        "",
        "## 2. 执行命令",
        "",
        "```bash",
        "cd backend",
        'python -m ops.drill release --confirm release --rerun-reason "发版前复跑"',
        "```",
        "",
        "失败不中断：六类全部跑完再汇总，退出码取最差（失败 1 > 降级通过 2 > 通过 0）。",
        "",
        "## 3. 六类场景（按声明序）",
        "",
    )
)
"""清单固定头部（门禁与命令）。"""


def checklist_path(repo_root: Path) -> Path:
    """清单在仓内的落点。

    Args:
        repo_root: 仓库根。

    Returns:
        Path: 清单路径。
    """
    return repo_root / DRILL_CHECKLIST_PATH


def build_checklist(scenario_set: ScenarioSet) -> str:
    """从场景单源渲染检查清单。

    Args:
        scenario_set: 已校验场景集。

    Returns:
        str: Markdown 文本（结尾带换行）。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList(_HEADER)
    rows.add("| # | 场景 | 目标 | 命令 | 期望观察 | 记录 |")
    rows.add("| --- | --- | --- | --- | --- | --- |")
    for index, scenario in enumerate(scenario_set.scenarios, start=1):
        command = (
            f"`python -m ops.cluster_drill run {scenario.name} --confirm {scenario.name}`"
            if scenario.target == "docker"
            else f"`python -m ops.drill run {scenario.name} --confirm {scenario.name}`"
        )
        rows.add(
            f"| {index} | `{scenario.name}` | `{scenario.target}` | "
            f"{command} | "
            f"{scenario.expect} | `MSG_DRILL_REPORT_DIR/{scenario.name}.json` |"
        )
    rows.add("")
    rows.add("## 4. 判定口径")
    rows.add("")
    rows.add("- 结论三态：**通过 0 / 降级通过 2 / 失败 1**，与 07-3 / 07-4 / 07-6-1 同口径。")
    rows.add("- **降级不计通过**：可降级项无样本或未闭环时记「降级通过」，须人工复核并在签核栏写明。")
    rows.add("- `data_loss=true` 或任一硬判据非 `ok` 直接判失败，**阻断发版**。")
    rows.add("- 未通过项必须登记归口（`issues[]` / `leftovers[]` 的 owner），否则矩阵判失败。")
    rows.add("")
    rows.add("## 5. 版本与环境记录")
    rows.add("")
    rows.add("| 项 | 值 |")
    rows.add("| --- | --- |")
    for label in ("服务版本", "契约版本", "git 提交", "环境 / 目标", "配置摘要", "执行人 / 日期"):
        rows.add(f"| {label} | |")
    rows.add("")
    rows.add("## 6. 签核")
    rows.add("")
    rows.add("| 场景 | 结论 | 记录 | 复核人 |")
    rows.add("| --- | --- | --- | --- | --- |")
    for scenario in scenario_set.scenarios:
        rows.add(f"| `{scenario.name}` | | `{scenario.name}.json` | |")
    rows.add("")
    rows.add("## 7. 判据速查")
    rows.add("")
    rows.extend(render_scenario_rows(scenario_set))
    rows.add("")
    return "\n".join(rows)


def normalize(text: str) -> str:
    """归一化文本（统一换行、去行尾空白、去末尾空行），用于漂移比对。

    Args:
        text: 原始文本。

    Returns:
        str: 归一化文本。
    """
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def write_checklist(scenario_set: ScenarioSet, path: Path) -> Path:
    """生成并写入检查清单。

    Args:
        scenario_set: 已校验场景集。
        path: 目标路径。

    Returns:
        Path: 写入路径。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_checklist(scenario_set), encoding="utf-8", newline="\n")
    return path


def check_checklist(scenario_set: ScenarioSet, path: Path) -> ConcurrentStableFrozenList[str]:
    """比对仓内清单与单源渲染结果。

    Args:
        scenario_set: 已校验场景集。
        path: 仓内清单路径。

    Returns:
        ConcurrentStableFrozenList[str]: 漂移说明（空表示一致）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    try:
        actual = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        errors.add(f"检查清单不存在：{path}")
        return ConcurrentStableFrozenList(errors)
    if normalize(actual) != normalize(build_checklist(scenario_set)):
        errors.add(f"检查清单与场景单源不一致：{path}（重新生成：python -m ops.drill checklist）")
    return ConcurrentStableFrozenList(errors)
