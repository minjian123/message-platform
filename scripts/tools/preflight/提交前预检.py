#!/usr/bin/env python3
"""提交前门禁（pre-commit）：跑本仓 `engkit.toml` 的 `[[preflight.steps]]`，范围收敛到暂存文件。

**步骤表不在本脚本维护**——直接读仓根 `engkit.toml` 的 `[[preflight.steps]]`（与推送预检同一份声明），
预检加了什么这里就跟着查什么，一项不漏；判定逻辑一律调用 `engkit` 的公开能力（`load_for` /
`expand` / `run_selected` / `coverage.check_changed`），本脚本只做「步骤裁剪 + 改动清单」两件事。
好处是将来 engkit 若长出统一的提交环命令，本脚本退化为几行转发即可，不会形成第二套判据。

裁剪口径（按暂存区文件）：

- **改动不触及代码 / 配置**（只改 `msg文档/`、`README.md` 之类）→ 只跑 `always` 类步骤（行尾检查等）；
- **`ruff` / `pyright` 步骤** → 只判本次改动的 `.py` / `.pyi`（`.toml` / `.json` 不交给 Python 静态检查器，
  否则会被当 Python 解析）；无 Python 改动则跳过并注明；
- **定向测试步骤（`scoped_tests`）** → `engkit run_selected` 选测 + 采集覆盖率，再由
  `engkit.coverage.check_changed` 判**改动行** 100%（提交环判改动行、推送环判模块级，口径见
  《AI开发规范》「覆盖率要求」节）；
- **其余命令步骤** → 无条件执行（未知类型 fail-closed，宁可多跑不可漏判）。

用法::

    python scripts/tools/preflight/提交前预检.py              # 由 .git/hooks/pre-commit 调用（判暂存区）
    python scripts/tools/preflight/提交前预检.py --dry-run     # 只打印步骤计划
    python scripts/tools/preflight/提交前预检.py --all-steps   # 不按改动面裁剪（装机自检用）
    python scripts/tools/preflight/提交前预检.py --changed-from HEAD~1   # 改判 git 区间

**逃逸口**：`git commit --no-verify` 跳过本钩子，仅限紧急回滚；推送环仍会完整复判，不构成绕过。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
"""msg 仓库根（本文件位于 `scripts/tools/preflight/`）。"""

GATED_PREFIXES = ("backend/", "scripts/", "deploy/")
"""需要门禁的代码 / 配置路径前缀。"""

GATED_SUFFIXES = (".py", ".pyi", ".toml", ".yml", ".yaml", ".json")
"""需要门禁的代码 / 配置后缀。"""

PY_SUFFIXES = (".py", ".pyi")
"""可交给 ruff / pyright 的后缀（**只收 Python**：`.toml` / `.json` 会被当源码解析而误判）。"""

MAX_INLINE_FILES = 200
"""单条命令最多内联的改动文件数（超限退化为整目录检查，避免 Windows 命令行长度上限）。"""

COVERAGE_CHANGED_REPORT = ".cache/coverage/changed.json"
"""改动行覆盖率报告落点（`.cache/` 已忽略）。"""


def _git(*args: str) -> str:
    """跑一条 git 命令并返回标准输出（失败即抛 `RuntimeError`）。

    Args:
        *args: git 子命令与参数。

    Returns:
        str: 标准输出（UTF-8 解码）。
    """
    completed = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.decode("utf-8", errors="replace").strip() or "git 执行失败")
    return completed.stdout.decode("utf-8", errors="replace")


def changed_files(changed_from: str) -> list[str]:
    """取本次提交涉及的仓根相对路径。

    Args:
        changed_from: 对比基准（空串取暂存区；给了基准则取 `<基准>...HEAD` 区间）。

    Returns:
        list[str]: 仓根相对路径（已去重保序）。
    """
    if changed_from:
        raw = _git("diff", "--name-only", "--diff-filter=ACMR", "-z", f"{changed_from}...HEAD")
    else:
        raw = _git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z")
    seen: dict[str, None] = {}
    for item in raw.split("\0"):
        if item.strip():
            seen.setdefault(item.replace("\\", "/"), None)
    return list(seen)


def is_code(path: str) -> bool:
    """是否属需要门禁的代码 / 配置。

    Args:
        path: 仓根相对路径。

    Returns:
        bool: 命中前缀或后缀即为代码 / 配置。
    """
    return path.startswith(GATED_PREFIXES) and path.endswith(GATED_SUFFIXES)


def step_scope(step) -> str:
    """判定步骤的裁剪口径。

    Args:
        step: `engkit.config.Step` 实例。

    Returns:
        str: `tests`（定向测试）/ `lint`（只判改动 Python）/ `ci`（仅 CI）/ `always`（无条件）。
    """
    if step.type == "scoped_tests":
        return "tests"
    if step.type in ("full_tests", "project_tests"):
        return "ci"
    tokens = tuple(str(token) for token in step.cmd)
    if any("ruff" in token or "pyright" in token for token in tokens):
        return "lint"
    return "always"


def step_argv(step, cwd: Path, py_files: list[str], *, scoped: bool) -> list[str] | None:
    """构造命令 argv（`lint` 步骤把检查范围收敛到改动文件）。

    Args:
        step: 步骤。
        cwd: 步骤工作目录（绝对路径）。
        py_files: 本次改动的 Python 文件（仓根相对）。
        scoped: 是否按改动面裁剪。

    Returns:
        list[str] | None: 命令；`None` 表示本次无需执行（无 Python 改动）。
    """
    from engkit.preflight import expand  # 延迟导入：只有跑步骤时才需要 engkit

    argv = expand(_cfg(), tuple(str(token) for token in step.cmd))
    if not scoped:
        return argv
    # 步骤 cwd 可能是 `backend/`，而改动清单是仓根相对——必须换算到 cwd 相对，否则 ruff 找不到文件。
    relative = [
        str((REPO_ROOT / item).relative_to(cwd)).replace("\\", "/")
        for item in py_files
        if (REPO_ROOT / item).is_relative_to(cwd)
    ]
    if not relative:
        return None
    if len(relative) > MAX_INLINE_FILES:
        print(f"[precommit] 改动 Python 文件 {len(relative)} 个 > {MAX_INLINE_FILES} ⇒ 按整目录检查")
        return argv
    if argv and argv[-1] in (".", "./"):
        return [*argv[:-1], *relative]
    return [*argv, *relative]


_CFG_CACHE: list = []
"""仓配置缓存（`load_for` 每步重读没必要）。"""


def _cfg():
    """取仓配置（首次调用时加载）。

    Returns:
        engkit.config.Config: 仓配置。
    """
    if not _CFG_CACHE:
        from engkit.config import load_for

        _CFG_CACHE.append(load_for(REPO_ROOT))
    return _CFG_CACHE[0]


def run_command(argv: list[str], cwd: Path) -> bool:
    """执行一条命令并完整透传输出。

    Args:
        argv: 命令。
        cwd: 工作目录。

    Returns:
        bool: 是否成功。
    """
    print(f"  $ {' '.join(argv)}")
    completed = subprocess.run(argv, cwd=cwd, capture_output=True, check=False)
    for stream in (completed.stdout, completed.stderr):
        text = stream.decode("utf-8", errors="replace")
        if text:
            print(text, end="" if text.endswith("\n") else "\n")
    return completed.returncode == 0


def has_coverage_data(data_file: Path) -> bool:
    """本轮是否采集到覆盖率数据（只看采集文件，不做合并）。

    Args:
        data_file: 覆盖率数据文件基址。

    Returns:
        bool: 存在 `<基址>` 或 `<基址>.<pid>` 采集文件即为 True。
    """
    from engkit import coverage

    for path in coverage.data_files(data_file):
        if path.name == data_file.name:
            return True
        # 只认 `<基址>.<pid>`（采集插件按 pid 落盘）；同目录的 `<基址>.json` 是报告，不是数据。
        if path.name.removeprefix(data_file.name).lstrip(".").isdigit():
            return True
    return False


def run_tests(code_files: list[str], jobs: str) -> bool:
    """定向测试 + 改动行覆盖率门禁。

    Args:
        code_files: 本次改动的代码 / 配置文件（仓根相对）。
        jobs: 并行档位。

    Returns:
        bool: 是否通过。
    """
    from engkit import coverage
    from engkit.preflight import run_selected

    cfg = _cfg()
    data_file = (cfg.root / cfg.coverage.data_file).resolve()
    coverage.cleanup(data_file)
    # `cov_fail_under=0`：本步只负责「跑起来 + 采集」；模块级 100% 归推送环，
    # 提交环的判据是下面的改动行门禁（口径见《AI开发规范》「覆盖率要求」节）。
    code = run_selected(cfg, changed=code_files, jobs=jobs, cov=True, cov_fail_under=0.0)
    if code != 0:
        return False
    if not has_coverage_data(data_file):
        # 与 bms 同口径：本次改动无可判模块（无受影响测试），改动行门禁交推送环，不在提交环伪造失败。
        print("[precommit] 本轮未采集到覆盖率数据（无受影响测试）⇒ 改动行门禁交推送环")
        return True
    report = (cfg.root / COVERAGE_CHANGED_REPORT).resolve()
    report.parent.mkdir(parents=True, exist_ok=True)
    return coverage.check_changed(cfg.root, data_file, report, tuple(code_files)) == 0


def main(argv: list[str] | None = None) -> int:
    """入口。

    Args:
        argv: 命令行参数（None 取 `sys.argv`）。

    Returns:
        int: 退出码（0 通过 / 1 存在失败项）。
    """
    parser = argparse.ArgumentParser(prog="提交前预检", description="msg 仓提交前门禁（按改动面裁剪）")
    parser.add_argument("--jobs", default="auto", help="选测并行档位：auto / 正整数 / off")
    parser.add_argument("--changed-from", default="", help="改为对比 git 基准（缺省只判暂存区）")
    parser.add_argument("--dry-run", action="store_true", help="只打印步骤计划，不执行")
    parser.add_argument("--all-steps", action="store_true", help="不按改动面裁剪（装机自检用）")
    args = parser.parse_args(None if argv is None else list(argv))

    try:
        files = changed_files(args.changed_from)
    except RuntimeError as error:
        print(f"[precommit] 取改动清单失败：{error}", file=sys.stderr)
        return 1

    code_files = [item for item in files if is_code(item)]
    py_files = [item for item in files if item.startswith(GATED_PREFIXES) and item.endswith(PY_SUFFIXES)]
    print(f"[precommit] 本次变更 {len(files)} 个文件（代码 / 配置 {len(code_files)} 个，Python {len(py_files)} 个）")
    if not code_files:
        print("[precommit] 改动不触及代码 / 配置：只跑 always 类步骤")

    cfg = _cfg()
    scoped = not args.all_steps
    failures: list[str] = []
    skipped: list[str] = []

    for step in cfg.preflight.steps:
        scope = step_scope(step)
        cwd = cfg.root / (step.cwd or cfg.preflight.cwd)
        if scope == "ci":
            skipped.append(f"{step.label}（仅 CI）")
            continue
        if scoped and scope == "tests" and not code_files:
            skipped.append(f"{step.label}（改动不触及代码）")
            continue
        if scope == "tests":
            print(f"\n[precommit] {step.label}")
            if args.dry_run:
                print("  (dry-run) engkit run_selected + 改动行覆盖率门禁")
                continue
            if not run_tests(code_files, args.jobs):
                failures.append(step.label)
            continue
        command = step_argv(step, cwd, py_files, scoped=scoped and scope == "lint")
        if command is None:
            skipped.append(f"{step.label}（本次无该步骤范围内的 Python 改动）")
            continue
        print(f"\n[precommit] {step.label}")
        if args.dry_run:
            print(f"  (dry-run) {' '.join(command)}")
            continue
        if not run_command(command, cwd):
            failures.append(step.label)

    print()
    for label in skipped:
        print(f"[precommit] 跳过：{label}")
    if failures:
        print(f"[precommit] 失败 {len(failures)} 项：")
        for label in failures:
            print(f"[precommit]  - {label}")
        return 1
    print("[precommit] 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
