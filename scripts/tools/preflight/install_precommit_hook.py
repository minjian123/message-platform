#!/usr/bin/env python3
"""安装 / 校验 / 卸载提交前门禁钩子（`.git/hooks/pre-commit`）。

与 bms `scripts/tools/preflight/install_precommit_hook.py` **同形**（标记块幂等替换、`--check` /
`--remove`、`.git/hooks` 不随仓库分发需换机重跑）；差别只有解释器取法——msg 仓内没有 `.venv`，
钩子用 `uv run --project backend python` 跑门禁脚本：有 venv 就用、没有就按 `backend/uv.lock` 现场建，
与步骤里既有的 `uv run ruff …` 同一条链路，不会出现「解释器缺失 ⇒ 静默放行」。

用法::

    python scripts/tools/preflight/install_precommit_hook.py            # 安装 / 覆盖
    python scripts/tools/preflight/install_precommit_hook.py --check    # 只校验（缺 / 过期退出码 1）
    python scripts/tools/preflight/install_precommit_hook.py --remove   # 卸载

钩子是 `pre-commit`，`git commit --no-verify` 可跳过（推送环仍完整复判，不构成绕过）。
"""

from __future__ import annotations

import argparse
import stat
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
"""msg 仓库根（本文件位于 `scripts/tools/preflight/`）。"""

MARKER_BEGIN = "# >>> msg precommit gate >>>"
"""钩子标记块起始行（重复安装据此替换）。"""

MARKER_END = "# <<< msg precommit gate <<<"
"""钩子标记块结束行。"""

HOOK_RELATIVE = Path(".git") / "hooks" / "pre-commit"
"""钩子文件相对路径。"""

GATE_RELATIVE = Path("scripts") / "tools" / "preflight" / "提交前预检.py"
"""门禁脚本相对路径（钩子只做转发）。"""


def hook_content(root: Path) -> str:
    """生成钩子脚本全文（含标记块）。

    Args:
        root: 仓库根。

    Returns:
        str: 钩子脚本内容（POSIX sh；Git for Windows 用其自带 sh 执行）。
    """
    gate = (root / GATE_RELATIVE).as_posix()
    return (
        "#!/bin/sh\n"
        f"{MARKER_BEGIN}\n"
        "# 提交前门禁：按暂存文件裁剪跑 engkit.toml 的 [[preflight.steps]]。\n"
        "# 由 scripts/tools/preflight/install_precommit_hook.py 生成，勿手工编辑本块。\n"
        "if ! command -v uv >/dev/null 2>&1; then\n"
        '    echo "[precommit] 未找到 uv：跳过提交前门禁（推送环仍会完整复判）" >&2\n'
        "    exit 0\n"
        "fi\n"
        "# 行尾自愈：把「已暂存且被写成 CRLF」的文件改回 LF 并重新暂存（写侧默认值不可控，\n"
        "# 这一步保证提交前工作树与暂存区一致为 LF；失败不阻断，后面 eol 步骤会复判）。\n"
        "uv run --project backend python -m engkit eol --fix-staged >/dev/null 2>&1 || true\n"
        f'exec uv run --project backend python "{gate}" "$@"\n'
        f"{MARKER_END}\n"
    )


def read_hook(target: Path) -> str:
    """读取既有钩子内容（不存在返回空串）。

    Args:
        target: 钩子文件。

    Returns:
        str: 文件内容。
    """
    return target.read_text(encoding="utf-8") if target.is_file() else ""


def merge_hook(existing: str, block: str) -> str:
    """把门禁块并入既有钩子（已有标记块则替换，否则追加）。

    Args:
        existing: 既有内容（空表示新建）。
        block: 门禁块全文。

    Returns:
        str: 合并后的内容。
    """
    if MARKER_BEGIN in existing and MARKER_END in existing:
        head, _, rest = existing.partition(MARKER_BEGIN)
        _, _, tail = rest.partition(MARKER_END)
        return f"{head}{block.rstrip(chr(10))}{tail}"
    if not existing.strip():
        return block
    separator = "" if existing.endswith("\n") else "\n"
    return f"{existing}{separator}{block}"


def make_executable(target: Path) -> None:
    """给钩子文件加可执行位（Windows 依赖 sh 关联，无实际权限位）。

    Args:
        target: 钩子文件。
    """
    mode = target.stat().st_mode
    target.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def install(root: Path) -> int:
    """安装 / 更新钩子。

    Args:
        root: 仓库根。

    Returns:
        int: 退出码（0 成功）。
    """
    target = root / HOOK_RELATIVE
    target.parent.mkdir(parents=True, exist_ok=True)
    merged = merge_hook(read_hook(target), hook_content(root))
    target.write_text(merged, encoding="utf-8", newline="\n")
    make_executable(target)
    print(f"[precommit] 已安装：{target}")
    print(f"[precommit] 门禁：uv run --project backend python {GATE_RELATIVE.as_posix()}")
    return 0


def check(root: Path) -> int:
    """校验钩子是否已安装且内容最新。

    Args:
        root: 仓库根。

    Returns:
        int: 0 已安装且最新 / 1 缺失或过期。
    """
    target = root / HOOK_RELATIVE
    existing = read_hook(target)
    if MARKER_BEGIN not in existing:
        print(f"[precommit] 未安装：{target}（跑一次安装脚本）")
        return 1
    if hook_content(root).rstrip("\n") not in existing:
        print(f"[precommit] 已安装但内容过期：{target}（重跑安装脚本）")
        return 1
    print(f"[precommit] 已安装且为最新：{target}")
    return 0


def remove(root: Path) -> int:
    """卸载门禁块（钩子无其他内容时删除文件）。

    Args:
        root: 仓库根。

    Returns:
        int: 退出码（0 成功 / 1 未安装）。
    """
    target = root / HOOK_RELATIVE
    existing = read_hook(target)
    if MARKER_BEGIN not in existing:
        print(f"[precommit] 未安装：{target}")
        return 1
    head, _, rest = existing.partition(MARKER_BEGIN)
    _, _, tail = rest.partition(MARKER_END)
    remainder = f"{head}{tail}".strip()
    if remainder in ("", "#!/bin/sh"):
        target.unlink()
        print(f"[precommit] 已删除：{target}")
        return 0
    target.write_text(f"{remainder}\n", encoding="utf-8", newline="\n")
    print(f"[precommit] 已移除门禁块（保留其余钩子内容）：{target}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """入口。

    Args:
        argv: 命令行参数（None 取 `sys.argv`）。

    Returns:
        int: 退出码。
    """
    parser = argparse.ArgumentParser(prog="install_precommit_hook", description="安装 msg 仓提交前门禁钩子")
    parser.add_argument("--root", default=str(REPO_ROOT), help="仓库根（缺省为脚本所在仓）")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="只校验安装状态")
    group.add_argument("--remove", action="store_true", help="卸载门禁块")
    args = parser.parse_args(None if argv is None else list(argv))

    root = Path(args.root).resolve()
    if args.check:
        return check(root)
    if args.remove:
        return remove(root)
    return install(root)


if __name__ == "__main__":
    sys.exit(main())
