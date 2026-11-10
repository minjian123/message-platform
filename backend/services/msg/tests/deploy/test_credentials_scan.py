"""凭据扫描与注册表门禁（Kiwi 2474；任务 07-2 §6 / §7 / §9）。

覆盖策略：

- **正例走真实仓**：`run(仓根)` 必须六项全绿，这是「本仓此刻无凭据泄漏」的可回归证据；
- **反例走构造数据**：临时仓根按需改写模板 / 编排 / 四份配置 / 忽略文件，逐项断言自检能指名违规；
- **规则正反走纯函数**：`check_text()` / `check_tracked()` 直接喂构造文本，钉住每条规则与白名单；
- 反例夹具里的「真实凭据」用拼接构造，避免用例源码自身被 C1 扫中。
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ConfigError

from ops import check_credentials as cc
from ops.check_credentials import (
    REPO_ROOT,
    check_env_discipline,
    check_key_coverage,
    check_layers,
    check_registry,
    check_runtime,
    check_text,
    check_tracked,
    git_ls_files,
    main,
    parse_compose_items,
    parse_env_items,
    registry_keys,
    run,
    scan_tree,
)

pytestmark = pytest.mark.kiwi_id(2474)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_TEMPLATE = (_REPO / "deploy" / ".env.example").read_text(encoding="utf-8")
_COMPOSE = (_REPO / "deploy" / "compose" / "msg.yml").read_text(encoding="utf-8")
_GITIGNORE = "deploy/.env\nbackend/.env\n"
_CONFIG = "[app]\nname = 'x'\n"

# 私钥头拆成两段常量：反例夹具在运行时拼出完整 PEM 头，但用例源码单行不出现该形态，
# 否则 C1 会把「用于测试扫描器的夹具」自己判成泄漏。
_PEM_HEAD = "-----BEGIN"
_PEM_TAIL = "PRIVATE KEY-----"


def _write(path: Path, text: str) -> None:
    """写临时文本文件（统一 LF）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)


def _fake_repo(
    tmp_path: Path,
    *,
    template: str = _TEMPLATE,
    compose: str = _COMPOSE,
    gitignore: str = _GITIGNORE,
    config: str = _CONFIG,
    docs: str = "# 干净文档\n",
) -> Path:
    """搭一个只含自检所需资产的最小仓根。

    Args:
        tmp_path: pytest 临时目录。
        template: 凭据模板全文。
        compose: 编排全文。
        gitignore: 忽略文件全文。
        config: 四份配置共用的最小 TOML。
        docs: 公开文档内容。

    Returns:
        Path: 临时仓根。
    """
    root = tmp_path / "repo"
    (root / "deploy" / "compose").mkdir(parents=True)
    (root / "backend").mkdir()
    (root / "frontend").mkdir()
    (root / "msg文档").mkdir()
    _write(root / ".gitignore", gitignore)
    _write(root / "deploy" / ".env.example", template)
    _write(root / "deploy" / "compose" / "msg.yml", compose)
    for name in ("config.toml", "config.dev.toml", "config.test.toml", "config.prod.toml"):
        _write(root / "backend" / name, config)
    _write(root / "msg文档" / "note.md", docs)
    return root


def _names(found: ConcurrentStableList[str]) -> str:
    """把违规明细拼成便于断言的文本。

    Args:
        found: 违规明细。

    Returns:
        str: 换行拼接的明细。
    """
    return "\n".join(found)


# ---------- 规则正反（纯函数） ----------


def test_check_text_hits_every_rule() -> None:
    """六类命中全部给出「文件:行号: 规则名」；空行不参与。"""
    private = _PEM_HEAD + " RSA " + _PEM_TAIL
    jwt = "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature"
    text = "\n".join(
        [
            "",
            private,
            "pass" + "word=hunter2",
            jwt,
            "host=192.168.1.10",
            "user=admin",
            "url=http://badhost.invalid:8080/x",
        ]
    )
    found = _names(check_text(text, "doc.md", strict=True))
    for rule in ("private-key", "nonempty-secret", "real-token", "internal-ip", "account-or-host"):
        assert "doc.md:" in found and rule in found
    assert "doc.md:2: private-key" in found
    assert "doc.md:3: nonempty-secret" in found
    assert "doc.md:4: real-token" in found
    assert "doc.md:5: internal-ip" in found


def test_check_text_whitelist_passes() -> None:
    """占位 / 回环 / Compose DNS / 权限码 / 规则描述文字均不误报。"""
    private = _PEM_HEAD + " <占位> " + _PEM_TAIL
    text = "\n".join(
        [
            private,
            "password=<v>",
            "password=${SECRET}",
            "password=",
            "password=None",
            "password=非空",
            "password=/",
            "password=localhost",
            "password=mysql",
            "host=127.0.0.1",
            "user=mysql",
            "url=http://msg-1:8000/healthz",
            "user:query",
        ]
    )
    assert not check_text(text, "doc.md", strict=True)
    assert not check_text("password=...\nBMS_TOKEN = set_tenant_context(1)", "doc.md", strict=True)


def test_check_text_strict_scope() -> None:
    """代码目录只判凭据形态；公开文档才追加内网 IP / 账号 / 主机端口红线。"""
    text = "host=192.168.1.10\nuser=admin\nurl=http://badhost.invalid:8080/x\n"
    assert not check_text(text, "backend/x.py")
    assert len(check_text(text, "msg文档/x.md", strict=True)) == 3
    assert len(check_text("pass" + "word=hunter2", "backend/x.py")) == 1


def test_skip_binary_and_generated_paths() -> None:
    """跳过目录、生成目录、二进制后缀；Dockerfile 等无后缀文件照扫。"""
    skip = cc._skip  # pyright: ignore[reportPrivateUsage]
    assert skip(Path("a/__pycache__/x.py"))
    assert skip(Path("a/x.png"))
    assert not skip(Path("a/x.py"))
    assert not skip(Path("a/Dockerfile"))


def test_scan_tree_reports_file_line_and_skips_binary(tmp_path: Path) -> None:
    """C1 全目录扫描：命中给行号；缺目录、目录项、跳过项、非 UTF-8 文件各有分支。"""
    root = _fake_repo(tmp_path, docs="host=192.168.1.10\n")
    _write(root / "deploy" / "binary.py", "placeholder")
    (root / "deploy" / "binary.py").write_bytes(b"\xff\xfe\x00")
    _write(root / "backend" / "__pycache__" / "ignored.py", "pass" + "word=hunter2")
    (root / "backend" / "nested").mkdir()
    _write(root / "deploy" / "image.png", "not text")
    found = _names(scan_tree(root))
    assert "msg文档/note.md:1: internal-ip" in found
    assert "binary.py" not in found

    (root / "frontend").rmdir()
    assert "frontend: 扫描目录缺失" in _names(scan_tree(root))


def test_check_tracked_and_git_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """两份 `.env` 不得被跟踪 / 必须被忽略；git 失败时返回空串不抛。"""
    assert not check_tracked("deploy/.env.example\n", _GITIGNORE)
    bad = _names(check_tracked("deploy/.env\nbackend/.env\n", "deploy/.env\n"))
    assert "deploy/.env: 已被 git 跟踪" in bad
    assert "backend/.env: 已被 git 跟踪" in bad
    assert "缺忽略项 backend/.env" in bad

    assert isinstance(git_ls_files(REPO_ROOT), str)

    def _failed_git(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=1, stdout="")

    monkeypatch.setattr(cc.subprocess, "run", _failed_git)
    assert git_ls_files(tmp_path) == ""


def test_parse_items_cover_comments_and_missing_section() -> None:
    """模板 / 编排解析器只收合法键行，段缺失返回空。"""
    items = parse_env_items("# 注释\nA=\nB=<v>\nnot-a-key\n")
    assert items["A"] == ("", 2)
    assert items["B"] == ("<v>", 3)
    assert "not-a-key" not in items

    assert not parse_compose_items("services:\n  msg:\n")
    parsed = parse_compose_items("x-service-env: &env\n  A_KEY: ${VAR:?}\n  # 注释\nservices:\n")
    assert parsed["A_KEY"][0] == "${VAR:?}"


# ---------- C2 ~ C6 各检查项正反 ----------


def test_env_discipline_variants(tmp_path: Path) -> None:
    """C2：模板缺失 / 缺键 / 非空值分别被指名，正例零违规。"""
    root = _fake_repo(tmp_path)
    assert not check_env_discipline(root)

    (root / "deploy" / ".env.example").unlink()
    assert "模板缺失或为空" in _names(check_env_discipline(root))

    root = _fake_repo(tmp_path / "missing", template=_TEMPLATE.replace("BMS_REDIS__URL=\n", ""))
    assert "缺键位 BMS_REDIS__URL" in _names(check_env_discipline(root))

    root = _fake_repo(
        tmp_path / "nonempty",
        template=_TEMPLATE.replace("BMS_SERVER__CERTFILE=\n", "BMS_SERVER__CERTFILE=/etc/tls/cert.pem\n"),
    )
    assert "值非空且非占位" in _names(check_env_discipline(root))


def test_layers_variants(tmp_path: Path) -> None:
    """C3：缺文件 / 非法 TOML / 生产静态禁用项分别被指名，正例零违规。"""
    root = _fake_repo(tmp_path)
    assert not check_layers(root)

    (root / "backend" / "config.test.toml").unlink()
    assert "配置文件缺失" in _names(check_layers(root))

    root = _fake_repo(tmp_path / "broken", config="[app\ninvalid\n")
    assert "TOML 解析失败" in _names(check_layers(root))

    root = _fake_repo(tmp_path / "prod")
    _write(
        root / "backend" / "config.prod.toml",
        "[app]\ndebug = true\ndocs_enabled = true\n# sqlite 兜底禁止\n",
    )
    found = _names(check_layers(root))
    for marker in ("debug = true", "docs_enabled = true", "sqlite"):
        assert marker in found


def test_registry_variants(tmp_path: Path) -> None:
    """C4：模板 / 编排 / 配置文本三处的缺键、非占位、非插值分别被指名。"""
    root = _fake_repo(tmp_path)
    assert not check_registry(root)
    assert len(registry_keys()) == 18

    root = _fake_repo(
        tmp_path / "missing",
        template=_TEMPLATE.replace("BMS_TRACER__OTLP_ENDPOINT=\n", ""),
    )
    found = _names(check_registry(root))
    assert "deploy/.env.example: 注册表缺键 BMS_TRACER__OTLP_ENDPOINT" in found
    assert "deploy/compose/msg.yml: 注册表缺键" not in found

    root = _fake_repo(
        tmp_path / "compose_missing",
        compose=_COMPOSE.replace("  BMS_REDIS__URL: ${BMS_REDIS__URL:?}\n", ""),
    )
    assert "deploy/compose/msg.yml: 注册表缺键 BMS_REDIS__URL" in _names(check_registry(root))

    root = _fake_repo(
        tmp_path / "literal",
        compose=_COMPOSE.replace("${BMS_REDIS__URL:?}", "redis://real-host:6379/0"),
    )
    assert "未经 .env 插值" in _names(check_registry(root))

    root = _fake_repo(
        tmp_path / "nonplaceholder",
        template=_TEMPLATE.replace("BMS_SERVER__KEYFILE=\n", "BMS_SERVER__KEYFILE=/etc/tls/key.pem\n"),
    )
    assert "注册表键 BMS_SERVER__KEYFILE 非占位" in _names(check_registry(root))

    root = _fake_repo(tmp_path / "configsecret", config="[app]\n" + "pass" + "word = hunter2\n")
    assert "config.toml:2: nonempty-secret" in _names(check_registry(root))


def test_runtime_check_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    """C5：当前环境通过；装配 / 校验抛 `ConfigError` 时转为违规明细。"""
    assert not check_runtime()

    def _boom() -> None:
        raise ConfigError("boom")

    monkeypatch.setattr(cc, "load_settings", _boom)
    assert "运行时生效值：boom" in _names(check_runtime())


def test_key_coverage_variants(tmp_path: Path) -> None:
    """C6：宿主插值键不进容器不判；新增模板键未被编排覆盖即报。"""
    root = _fake_repo(tmp_path)
    assert not check_key_coverage(root)

    root = _fake_repo(tmp_path / "extra", template=_TEMPLATE + "EXTRA_KEY=\n")
    assert "未覆盖模板键 EXTRA_KEY" in _names(check_key_coverage(root))


# ---------- 命令入口 ----------


def test_run_and_main_exit_codes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`run` / `main` 正例 0、反例 1；`main()` 从 `sys.argv` 取参的分支也跑到。"""
    assert run(REPO_ROOT) == 0
    bad = _fake_repo(tmp_path, docs="host=192.168.1.10\n")
    assert run(bad) == 1
    assert main(ConcurrentStableList(["--root", str(bad)])) == 1

    monkeypatch.setattr(sys, "argv", ["check_credentials"])
    assert main() == 0


def test_module_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m ops.check_credentials` 的 `__main__` 块按退出码结束。"""
    monkeypatch.setattr(sys, "argv", ["check_credentials"])
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("ops.check_credentials", run_name="__main__")
    assert excinfo.value.code == 0


def test_report_both_branches(capsys: pytest.CaptureFixture[str]) -> None:
    """报告 OK / FAIL 两种形态都给「检查项 6 项」汇总。"""
    report = cc._report  # pyright: ignore[reportPrivateUsage]
    assert report("C1", "示例", ConcurrentStableList()) == 0
    assert report("C1", "示例", ConcurrentStableList(["a.md:1: private-key"])) == 1
    assert "[OK]" in capsys.readouterr().out
