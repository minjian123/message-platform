"""配置分层与凭据管理自检（任务 07-2）：四层配置 / 六类敏感项 / 凭据形态 / 生产禁用项。

用法::

    python -m ops.check_credentials                 # 全项自检；任一违规 → 明细 + 退出码 1
    python -m ops.check_credentials --root <仓根>   # 指定仓库根（用例造临时仓）

与 `ops/check_deployment.py` 同构：离线零连库、零起应用，每项检查都是「入参即数据」的纯函数，
`run()` 只负责读文件与拼装。违规明细一律带「文件:行号: 规则名」，不回显命中值。

判据分工：

- C1 扫全仓形态（私钥头 / 非空密码 / 真实 token / 内网 IP / 账号与非白名单主机端口）；
- C2 管两份 `.env` 的忽略与跟踪纪律、模板键位与空值；
- C3 管四层配置文件齐备、可解析与生产静态禁用项；
- C4 管注册表三处齐备（模板 / 编排 / 配置文本）；
- C5 用 `load_settings()` 装配后调 `bms_msg.config_guard` 的同一纯函数，判最终生效值；
- C6 管模板键位是否被编排覆盖（宿主插值键不进容器，显式登记例外）。

白名单集中在常量与 `_is_placeholder()`，不散落进各检查函数；新增键只改注册表。
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import tomllib
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from bms_core.core.config import load_settings
from bms_core.core.exceptions import ConfigError

from bms_msg.config_guard import flatten_settings, validate_config_invariants

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
"""后端工程根（`backend/`，脚本位于 `backend/ops/`）。"""

REPO_ROOT = _BACKEND_ROOT.parent
"""仓库根。"""

CHECK_COUNT = 6
"""检查项总数。"""

TEMPLATE_RELATIVE = Path("deploy/.env.example")
COMPOSE_RELATIVE = Path("deploy/compose/msg.yml")
GITIGNORE_RELATIVE = Path(".gitignore")
PROD_CONFIG_RELATIVE = Path("backend/config.prod.toml")

ENV_FILES: ConcurrentStableFrozenList[Path] = ConcurrentStableFrozenList([Path("deploy/.env"), Path("backend/.env")])
"""两份都不入库：前者供 compose 插值，后者是本地裸跑的 dotenv 层。"""

CONFIG_FILES: ConcurrentStableFrozenList[Path] = ConcurrentStableFrozenList(
    [
        Path("backend/config.toml"),
        Path("backend/config.dev.toml"),
        Path("backend/config.test.toml"),
        PROD_CONFIG_RELATIVE,
    ]
)
"""四层链的层 1 与层 2。"""

SENSITIVE_KEYS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    [
        "BMS_DATABASE__PLATFORM__URL_TEMPLATE",
        "BMS_DATABASE__PLATFORM__PASSWORD",
        "BMS_DATABASE__TENANTS__URL_TEMPLATE",
        "BMS_DATABASE__TENANTS__PASSWORD",
        "BMS_DATABASE__ARCHIVE__URL",
        "BMS_DATABASE__ARCHIVE__PASSWORD",
        "BMS_REDIS__URL",
        "BMS_TRACER__OTLP_ENDPOINT",
        "BMS_SERVICE_TOKEN__KEYS",
        "BMS_SERVICE_TOKEN__ACTIVE_KID",
        "BMS_SECURITY__KEYS",
        "BMS_SECURITY__ACTIVE_KID",
        "BMS_SECURITY__SECRET_KEY",
        "BMS_SERVER__CERTFILE",
        "BMS_SERVER__KEYFILE",
    ]
)
"""六类敏感项的键位注册表（单一事实源）。"""

RUNTIME_KNOBS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ["MSG_STORAGE_WARN_RATIO", "MSG_STORAGE_REJECT_RATIO", "MSG_STORAGE_SAMPLE_TTL_MS"]
)
"""非敏感运行旋钮；与敏感项同批核对键位齐备。"""

HOST_ONLY_KEYS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    [
        "PLATFORM_NETWORK",
        "MSG_REGISTRY_IMAGE_PREFIX",
        "MSG_TAG",
        "MSG_DATA_HOST_DIR",
        "MSG_OFFSETS_HOST_DIR",
    ]
)
"""只做宿主侧 compose 插值、不进容器的键（C6 例外）。"""

SCAN_DIRS: ConcurrentStableFrozenList[Path] = ConcurrentStableFrozenList(
    [Path("deploy"), Path("backend"), Path("frontend"), Path("msg文档")]
)
"""扫描范围（任务书 §2.6）。"""

SKIP_PARTS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ["__pycache__", ".git", ".venv", "node_modules", ".cache", ".pytest_cache", ".ruff_cache", "build", "dist"]
)
"""跳过的目录部件。"""

TEXT_SUFFIXES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    [
        ".py",
        ".pyi",
        ".md",
        ".toml",
        ".yml",
        ".yaml",
        ".json",
        ".ts",
        ".tsx",
        ".js",
        ".jsx",
        ".mjs",
        ".vue",
        ".css",
        ".html",
        ".txt",
        ".sh",
        ".bat",
        ".example",
        ".dockerignore",
        ".gitignore",
        ".ini",
        ".cfg",
        ".sql",
        ".xml",
        ".properties",
    ]
)
"""按文本读取的后缀（其余按二进制跳过）。"""

BARE_NAMES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ["Dockerfile", "Dockerfile.dockerignore", "LICENSE", "Makefile"]
)
"""无后缀但需扫描的文件名。"""

_PRIVATE_KEY = re.compile(r"-----BEGIN[^-]*PRIVATE KEY-----")
_SECRET_ASSIGN = re.compile(
    r"(?i)\b(?:password|passwd|secret_key|api_key|access_key|private_key)\s*[=:]\s*([^\s,;'\"}]+)"
)
_REAL_TOKEN = re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.|\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")
_INTERNAL_IP = re.compile(
    r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}"
    r"|192\.168\.\d{1,3}\.\d{1,3}|169\.254\.\d{1,3}\.\d{1,3})\b"
)
_ACCOUNT_ASSIGN = re.compile(r"(?i)\b(?:user|username|account|login)\s*=\s*([^\s,;'\"}]+)")
_HOST_PORT = re.compile(r"https?://[A-Za-z0-9.$<>_:-]+:\d{2,5}")
_PLACEHOLDER_LINE = re.compile(r"<\S+>|\$\{[^}]*\}|\.\.\.|localhost|127\.0\.0\.1|example\.com", re.I)
_DNS_WHITELIST = re.compile(r"mysql|redis|otel-collector|identity|msg:8000|gateway", re.I)
_HOST_WHITELIST = re.compile(r"(?:mysql|redis|otel-collector|identity|msg(?:-\d+)?|gateway)\b", re.I)
"""URL 主机白名单：Compose DNS 服务名与单实例编号（契约级端口说明，不是服务器地址）。"""
_ENV_ITEM = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$", re.MULTILINE)
_COMPOSE_ITEM = re.compile(r"^  ([A-Z][A-Z0-9_]*):\s*(.*)$", re.MULTILINE)
_PROD_FORBIDDEN: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ["debug = true", "docs_enabled = true", "sqlite"]
)
"""生产覆盖文件的静态禁用项。"""


def registry_keys() -> ConcurrentStableList[str]:
    """取注册表全部键位（敏感项 + 运行旋钮）。

    Returns:
        ConcurrentStableList[str]: 键位清单。
    """
    keys: ConcurrentStableList[str] = ConcurrentStableList(SENSITIVE_KEYS)
    for key in RUNTIME_KNOBS:
        keys.add(key)
    return keys


def _is_placeholder(value: str) -> bool:
    """判定一个值是否为空 / 占位 / 白名单形态（不是真实凭据）。

    Args:
        value: 等号或冒号右侧的原始文本。

    Returns:
        bool: True 表示可放行。
    """
    trimmed = value.strip().strip("\"'")
    if not trimmed or trimmed.startswith(("<", "${", "...")):
        return True
    if trimmed in ("None", "null", "true", "false", "0", "1"):
        return True
    # 规则描述文字（中文、纯标点）不是凭据值：`password=` 非空 / `无 PASSWORD= / 私钥` 之类。
    if not re.fullmatch(r"[!-~]+", trimmed) or not re.search(r"[A-Za-z0-9]", trimmed):
        return True
    return bool(_PLACEHOLDER_LINE.search(trimmed)) or bool(_DNS_WHITELIST.search(trimmed))


def check_text(text: str, label: str, *, strict: bool = False) -> ConcurrentStableList[str]:
    """扫描单份文本的凭据形态（纯函数，正反用例直接喂构造文本）。

    Args:
        text: 全文。
        label: 报告里的相对路径。
        strict: 是否追加「公开文档红线」规则（内网 IP / 账号 / 非白名单主机端口）。

    Returns:
        ConcurrentStableList[str]: 「文件:行号: 规则名」违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        if _PRIVATE_KEY.search(line) and not _PLACEHOLDER_LINE.search(line):
            found.add(f"{label}:{number}: private-key")
        secret = _SECRET_ASSIGN.search(line)
        if secret and not _is_placeholder(secret.group(1)):
            found.add(f"{label}:{number}: nonempty-secret")
        if _REAL_TOKEN.search(line):
            found.add(f"{label}:{number}: real-token")
        # 只有公开文档 / 部署资产才判红线（内网 IP / 账号 / 非白名单主机端口）：
        # 代码目录里的测试夹具 IP 与 user / token 变量名不是凭据泄漏，全判会淹没真实告警。
        if not strict:
            continue
        if _INTERNAL_IP.search(line) and not _PLACEHOLDER_LINE.search(line):
            found.add(f"{label}:{number}: internal-ip")
        account = _ACCOUNT_ASSIGN.search(line)
        if account and not _is_placeholder(account.group(1)):
            found.add(f"{label}:{number}: account-or-host")
        host = _HOST_PORT.search(line)
        if host and not _HOST_WHITELIST.search(host.group(0)) and not _PLACEHOLDER_LINE.search(line):
            found.add(f"{label}:{number}: account-or-host")
    return found


def _skip(path: Path) -> bool:
    """判定路径是否不参与文本扫描。

    Args:
        path: 候选文件。

    Returns:
        bool: True 表示跳过。
    """
    for part in path.parts:
        if part in SKIP_PARTS:
            return True
    return path.suffix.lower() not in TEXT_SUFFIXES and path.name not in BARE_NAMES


def scan_tree(root: Path) -> ConcurrentStableList[str]:
    """C1：扫描四个目录的凭据形态。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    for directory in SCAN_DIRS:
        base = root / directory
        if not base.is_dir():
            found.add(f"{directory.as_posix()}: 扫描目录缺失")
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file() or _skip(path):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            strict = directory in (Path("msg文档"), Path("deploy"))
            for item in check_text(text, path.relative_to(root).as_posix(), strict=strict):
                found.add(item)
    return found


def check_tracked(ls_text: str, gitignore: str) -> ConcurrentStableList[str]:
    """C2 纯函数：两份 `.env` 不得被跟踪、且必须被忽略。

    Args:
        ls_text: `git ls-files` 输出。
        gitignore: `.gitignore` 全文。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    tracked = ConcurrentStableList(ls_text.splitlines())
    ignored = ConcurrentStableList(gitignore.splitlines())
    for rel in ENV_FILES:
        name = rel.as_posix()
        if name in tracked:
            found.add(f"{name}: 已被 git 跟踪")
        if name not in ignored:
            found.add(f".gitignore 缺忽略项 {name}")
    return found


def git_ls_files(root: Path) -> str:
    """取仓库跟踪文件清单（无 git 时返回空串，由 C2 后续检查兜住）。

    Args:
        root: 仓库根。

    Returns:
        str: `git ls-files` 标准输出。
    """
    result = subprocess.run(["git", "ls-files"], cwd=root, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return ""
    return result.stdout


def parse_env_items(text: str) -> ConcurrentStableDict[str, tuple[str, int]]:
    """解析 `KEY=VALUE` 模板项。

    Args:
        text: 模板全文。

    Returns:
        ConcurrentStableDict[str, tuple[str, int]]: 键 → (值, 行号)。
    """
    items: ConcurrentStableDict[str, tuple[str, int]] = ConcurrentStableDict()
    for number, line in enumerate(text.splitlines(), start=1):
        match = re.fullmatch(r"([A-Z][A-Z0-9_]*)=(.*)", line)
        if match:
            items[match.group(1)] = (match.group(2), number)
    return items


def parse_compose_items(text: str) -> ConcurrentStableDict[str, tuple[str, int]]:
    """解析编排 `x-service-env` 段的注入键。

    Args:
        text: 编排全文。

    Returns:
        ConcurrentStableDict[str, tuple[str, int]]: 键 → (值, 行号)。
    """
    items: ConcurrentStableDict[str, tuple[str, int]] = ConcurrentStableDict()
    if "x-service-env:" not in text:
        return items
    section = text.split("x-service-env:", 1)[1].split("\nservices:", 1)[0]
    for number, line in enumerate(section.splitlines(), start=1):
        match = _COMPOSE_ITEM.fullmatch(line)
        if match:
            items[match.group(1)] = (match.group(2), number)
    return items


def _read_or_empty(root: Path, rel: Path) -> str:
    """读文本文件；缺失返回空串（由对应检查项报文，不抛异常打断其余项）。

    Args:
        root: 仓库根。
        rel: 相对路径。

    Returns:
        str: 全文。
    """
    path = root / rel
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def check_env_discipline(root: Path) -> ConcurrentStableList[str]:
    """C2：两份 `.env` 纪律 + 模板键位与空值。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found = check_tracked(git_ls_files(root), _read_or_empty(root, GITIGNORE_RELATIVE))
    template = _read_or_empty(root, TEMPLATE_RELATIVE)
    if not template:
        found.add(f"{TEMPLATE_RELATIVE.as_posix()}: 模板缺失或为空")
        return found
    items = parse_env_items(template)
    for key in registry_keys():
        if key not in items:
            found.add(f"{TEMPLATE_RELATIVE.as_posix()}: 缺键位 {key}")
    for key in items:
        value, number = items[key]
        if not _is_placeholder(value):
            found.add(f"{TEMPLATE_RELATIVE.as_posix()}:{number}: 键 {key} 的值非空且非占位")
    return found


def check_layers(root: Path) -> ConcurrentStableList[str]:
    """C3：四层配置齐备、可解析，生产覆盖无静态禁用项。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    for rel in CONFIG_FILES:
        path = root / rel
        if not path.is_file():
            found.add(f"{rel.as_posix()}: 配置文件缺失")
            continue
        try:
            tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            found.add(f"{rel.as_posix()}: TOML 解析失败：{exc}")
    prod = _read_or_empty(root, PROD_CONFIG_RELATIVE).lower()
    for marker in _PROD_FORBIDDEN:
        if marker in prod:
            found.add(f"{PROD_CONFIG_RELATIVE.as_posix()}: 生产配置禁用项 {marker}")
    return found


def check_registry(root: Path) -> ConcurrentStableList[str]:
    """C4：注册表三处齐备（模板键 / 编排键 / 配置文本无真实值）。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    template = parse_env_items(_read_or_empty(root, TEMPLATE_RELATIVE))
    compose = parse_compose_items(_read_or_empty(root, COMPOSE_RELATIVE))
    for key in registry_keys():
        if key not in template:
            found.add(f"{TEMPLATE_RELATIVE.as_posix()}: 注册表缺键 {key}")
        elif not _is_placeholder(template[key][0]):
            found.add(f"{TEMPLATE_RELATIVE.as_posix()}:{template[key][1]}: 注册表键 {key} 非占位")
        if key not in compose:
            found.add(f"{COMPOSE_RELATIVE.as_posix()}: 注册表缺键 {key}")
        elif "${" not in compose[key][0]:
            found.add(f"{COMPOSE_RELATIVE.as_posix()}:{compose[key][1]}: 键 {key} 未经 .env 插值")
    for rel in CONFIG_FILES:
        for item in check_text(_read_or_empty(root, rel), rel.as_posix()):
            found.add(item)
    return found


def check_runtime() -> ConcurrentStableList[str]:
    """C5：装配当前环境后，用启动期同一纯函数判最终生效值。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    try:
        settings = load_settings()
        validate_config_invariants(flatten_settings(settings), os.environ.get("BMS_ENV") or settings.app.env)
    except ConfigError as exc:
        found.add(f"运行时生效值：{exc}")
    return found


def check_key_coverage(root: Path) -> ConcurrentStableList[str]:
    """C6：模板键位被编排覆盖（宿主插值键显式例外）。

    Args:
        root: 仓库根。

    Returns:
        ConcurrentStableList[str]: 违规明细。
    """
    found: ConcurrentStableList[str] = ConcurrentStableList()
    template = parse_env_items(_read_or_empty(root, TEMPLATE_RELATIVE))
    compose = parse_compose_items(_read_or_empty(root, COMPOSE_RELATIVE))
    for key in template:
        if key in HOST_ONLY_KEYS:
            continue
        if key not in compose:
            found.add(f"{COMPOSE_RELATIVE.as_posix()}: 未覆盖模板键 {key}")
    return found


def _report(code: str, title: str, violations: ConcurrentStableList[str]) -> int:
    """打印单项结果。

    Args:
        code: 检查项编号（C1~C6）。
        title: 检查项中文名。
        violations: 违规明细。

    Returns:
        int: 1 表示该项不通过，0 表示通过。
    """
    if not violations:
        print(f"[OK] {code} {title}：0 违规")
        return 0
    print(f"[FAIL] {code} {title}：{len(violations)} 项违规")
    for item in violations:
        print(f"       - {item}")
    return 1


def run(root: Path) -> int:
    """执行全项自检并打印报告。

    Args:
        root: 仓库根。

    Returns:
        int: 0 全项通过；1 存在违规。
    """
    failed = _report("C1", "凭据形态扫描", scan_tree(root))
    failed += _report("C2", "两份 .env 纪律", check_env_discipline(root))
    failed += _report("C3", "四层配置结构", check_layers(root))
    failed += _report("C4", "敏感键注册表齐备", check_registry(root))
    failed += _report("C5", "运行时生效值", check_runtime())
    failed += _report("C6", "模板与编排键位覆盖", check_key_coverage(root))
    print(f"检查项 {CHECK_COUNT} 项：通过 {CHECK_COUNT - failed}，失败 {failed}")
    return 0 if failed == 0 else 1


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码（0 通过 / 1 违规）。
    """
    parser = argparse.ArgumentParser(description="msg 配置自检：四层配置 / 敏感项 / 凭据形态 / 生产禁用项")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="仓库根（缺省自动定位）")
    args = parser.parse_args(None if argv is None else argv)
    return run(args.root)


if __name__ == "__main__":
    raise SystemExit(main())
