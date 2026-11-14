#!/usr/bin/env python3
"""推送入口（薄包装）：预检是推送的内部步骤，实现在 engkit `push`。

口径与 bms 同构（见《AI开发规范》「开发环 / 推送环」）：

- 档位由**待推提交**自动选择：全为文档 → 快速档（静态 + 护栏）；含代码 → 定向档（静态 + 契约门禁
  + 定向用例 + 覆盖率门禁）；`--full` = 全量静态 + 定向用例（不含全量测试；全量测试只由 CI 执行）；
- 工作树有未提交改动即拒绝（推送前的工作树本就应当干净）；
- 预检不再是独立命令：本地手动调用 `engkit preflight` 一律拒绝（CI 环境除外）；
  **不提供「只跑预检、不推送」的出口**（`--check` 已于 2026-11-07 从 engkit 移除），预检只能由推送或 CI 触发。

msg 以路径依赖装 engkit，故本入口只做 `uv run engkit push` 的参数与退出码转发。

用法（在仓库根）::

    python3 scripts/tools/preflight/推送.py [--full]
                                           [--remote origin] [--branch <分支>]
                                           [--jobs auto|N|off] [--budget-seconds N]
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
"""仓库根。"""


def main() -> int:
    """入口。

    Returns:
        int: 退出码（与 engkit `push` 一致）。
    """
    argv = ["uv", "run", "--project", str(_ROOT / "backend"), "engkit", "push", *sys.argv[1:]]
    return subprocess.run(argv, cwd=_ROOT, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
