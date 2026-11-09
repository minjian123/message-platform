"""前端契约类型生成器与零漂移门禁（msg 任务 06-1-3；Kiwi 2463）。

**单用例串行**：本文件的断言会临时改写 / 删除生成物，若拆成多个用例，
`pytest -n auto` 下不同 worker 会互相读到中间态（已实测偶发 `first != second`）。
故全部断言收敛在同一个用例内顺序执行，并用 `finally` 恢复现场。

覆盖：生成物含四条新端点、头注来源、LF 与幂等、`--check` 无漂移通过、
有漂移报差异行号、生成物缺失提示、路径解析不依赖工作目录、门禁步骤登记。
"""

import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.kiwi_id(2463)

_REPO = Path(__file__).resolve().parents[5]
_FRONTEND = _REPO / "frontend"
_SCRIPT = _FRONTEND / "packages" / "api-types" / "scripts" / "generate.mjs"
_MSG_TS = _FRONTEND / "packages" / "api-types" / "src" / "msg.ts"
_INDEX_TS = _FRONTEND / "packages" / "api-types" / "src" / "index.ts"
_CONTRACT = _REPO / "deploy" / "contracts" / "msg.json"
_ENGKIT = _REPO / "engkit.toml"

_NEW_PATHS = (
    "/api/v1/msg/admin/partitions",
    "/api/v1/msg/admin/messages",
    "/api/v1/msg/admin/overview",
    "/api/v1/msg/admin/delayed/summary",
)


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """执行生成器脚本。

    Args:
        args: 附加参数。
        cwd: 工作目录（None 为前端根）。

    Returns:
        subprocess.CompletedProcess[str]: 执行结果。
    """
    return subprocess.run(
        ["node", str(_SCRIPT), *args],
        cwd=str(cwd or _FRONTEND),
        capture_output=True,
        text=True,
        check=False,
    )


def test_api_types_generation_lifecycle() -> None:
    """生成 / 零漂移 / 漂移判定 / 缺失判定 / 门禁登记的完整生命周期。"""
    assert _CONTRACT.is_file()
    original_msg = _MSG_TS.read_bytes()
    original_index = _INDEX_TS.read_bytes()
    try:
        # 生成物内容：四条新端点 + 头注 + 汇总导出
        content = original_msg.decode("utf-8")
        assert "deploy/contracts/msg.json" in content
        assert "请勿手工修改" in content
        for path in _NEW_PATHS:
            assert f'"{path}"' in content
        assert "export type * as msg from './msg'" in original_index.decode("utf-8")

        # 连续两次生成逐字节一致；生成物 LF、末尾单换行
        assert _run().returncode == 0
        first = _MSG_TS.read_bytes()
        assert _run().returncode == 0
        second = _MSG_TS.read_bytes()
        assert first == second
        assert b"\r\n" not in second
        assert second.endswith(b"\n") and not second.endswith(b"\n\n")

        # `--check` 无漂移通过，且从仓外工作目录调用仍能解析路径
        clean = _run("--check", cwd=_REPO)
        assert clean.returncode == 0, clean.stderr
        assert "零漂移" in clean.stdout and "info.version=" in clean.stdout

        # 人为漂移：退出 2 并报首处差异行号
        _MSG_TS.write_text(
            first.decode("utf-8").replace("请勿手工修改", "被改坏了", 1),
            encoding="utf-8",
            newline="\n",
        )
        drift = _run("--check")
        assert drift.returncode == 2
        assert "漂移" in drift.stderr and "第" in drift.stderr and "行" in drift.stderr
        _MSG_TS.write_bytes(first)

        # 生成物缺失：退出 2 且提示先跑 generate，不自动补写
        _INDEX_TS.unlink()
        missing = _run("--check")
        assert missing.returncode == 2
        assert "缺少生成物" in missing.stderr
        assert not _INDEX_TS.exists()
    finally:
        _MSG_TS.write_bytes(original_msg)
        _INDEX_TS.write_bytes(original_index)

    assert _run("--check").returncode == 0

    # 门禁步骤表：命令与 cwd 均为仓内相对写法
    config = _ENGKIT.read_text(encoding="utf-8")
    assert "packages/api-types/scripts/generate.mjs" in config
    assert '"--check"' in config
    assert 'cwd = "frontend"' in config
    assert "contract_snapshot" in config
