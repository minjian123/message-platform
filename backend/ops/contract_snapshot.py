"""公开契约快照 CLI：导出 / 校验本服务 OpenAPI（任务 03-1，与 mdm 的 `ops.contract_snapshot` 同构）。

用法::

    python -m ops.contract_snapshot export          # 导出（覆盖写入 deploy/contracts/msg.json）
    python -m ops.contract_snapshot export --print   # 导出并打印
    python -m ops.contract_snapshot check            # 零漂移校验（预检 / CI 用）
    python -m ops.contract_snapshot check --root .   # 指定仓库根

- **快照走内存构建**：导入 `bms_msg.main.ApplicationFactory`，`create(None)` 构造应用后取 `app.openapi()`
  （**不启 lifespan、不连库**）；渲染与校验复用 `bms_core.services.service_contract`。
- **单服务简化**：本产品只有一个在运服务 `msg`，包名固定 `{package_prefix}_{服务}` 即 `bms_msg`
  （`backend/config.toml` 的 `[app].package_prefix = "bms"`），故不像 mdm 那样按清单推导包名——
  服务键仍取自 `MSG_SERVICE_RECORDS`，避免「脚本里再写一份服务名」。
- **确定性**：同代码 → 同文本（缩进 2 / 键排序 / 非 ASCII 直出），供 Git 比对与零漂移校验。
- **帧协议契约（03-2）**：导出时在顶层注入 `x-msg-frame`（来自 `bms_msg.protocol.contract`）——帧是同一
  路径上的另一形态，不进运行时 `/openapi.json`（否则生成式客户端会按 JSON 解析帧端点），但必须进
  「对外承诺的全量契约」。注入在导出层完成，因此快照与实现仍只有一处事实源。
- **为什么需要它**：01-1 起契约快照一直靠人工重导，改端点忘了重导时，跨仓 `check_modules` 的
  「声明的错误码 / 能力 ↔ 契约路径」对账会漏。本脚本接进预检后，漂移在本地提交前就红。
"""

from __future__ import annotations

import argparse
import sys
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.services.module_registry import ModuleRecord, ModuleStatus
from bms_core.services.service_contract import (
    CONTRACTS_DIR,
    contract_file_name,
    render_contract_json,
    validate_contract,
)

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
"""后端工程根（`backend/`，脚本位于 `backend/ops/`）。"""

REPO_ROOT = _BACKEND_ROOT.parent
"""仓库根（`backend/` 的父目录）。"""

PACKAGE_PREFIX = "bms"
"""服务包名前缀（与 `backend/config.toml` 的 `[app].package_prefix` 同值）。"""


def _ensure_src_on_path() -> None:
    """把服务源码目录加入 `sys.path`（`python -m ops.*` 只自动带 `backend/`）。

    为什么不用 `PYTHONPATH`：预检与 CI 的步骤表只写命令，附加环境变量在各家 shell 上行为不一致；
    在脚本内定位（相对本文件）比在外部约定更稳。
    """
    src = _BACKEND_ROOT / "services" / "msg" / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def enabled_local_records() -> tuple[ModuleRecord, ...]:
    """本服务登记行（`service_key` 非空 + 状态启用）。

    Returns:
        tuple[ModuleRecord, ...]: 在运服务登记行（清单顺序）。
    """
    from services.module_registry import MSG_SERVICE_RECORDS

    return tuple(
        record
        for record in MSG_SERVICE_RECORDS
        if record.service_key is not None and record.status == ModuleStatus.ENABLED
    )


def contracts_dir(root: Path) -> Path:
    """快照目录绝对路径。

    Args:
        root: 仓库根。

    Returns:
        Path: `<root>/deploy/contracts`。
    """
    return root / CONTRACTS_DIR


def snapshot_path(root: Path, service_key: str) -> Path:
    """单个服务的快照文件路径。

    Args:
        root: 仓库根。
        service_key: 服务标识。

    Returns:
        Path: `<root>/deploy/contracts/<service_key>.json`。
    """
    return contracts_dir(root) / contract_file_name(service_key)


def service_package(service_key: str) -> str:
    """服务包名（`{package_prefix}_{service_key}`）。

    Args:
        service_key: 服务标识。

    Returns:
        str: 服务包名（如 `bms_msg`）。
    """
    return f"{PACKAGE_PREFIX}_{service_key}"


def build_openapi(service_key: str) -> ConcurrentStableDict[str, Any]:
    """内存构建服务应用并取公开契约（不启 lifespan、不连库）。

    Args:
        service_key: 服务标识。

    Returns:
        ConcurrentStableDict[str, Any]: OpenAPI 映射。

    Raises:
        RuntimeError: 服务包 / 应用工厂缺失或应用构建失败。
    """
    _ensure_src_on_path()
    package = service_package(service_key)
    try:
        module = import_module(f"{package}.main")
    except ModuleNotFoundError as exc:
        raise RuntimeError(f"服务包缺失：{package}.main（{exc}）") from exc
    factory_cls = getattr(module, "ApplicationFactory", None)
    if factory_cls is None:
        raise RuntimeError(f"服务缺少 ApplicationFactory：{package}.main")
    try:
        app = factory_cls().create(None)
    except Exception as exc:  # CLI 边界：把任何构建失败翻成可读错误
        raise RuntimeError(f"服务应用构建失败：{service_key}（{exc!r}）") from exc
    openapi = ConcurrentStableDict(app.openapi())
    # 惰性导入：本模块在 sys.path 就绪前被 import（`python -m ops.*` 只自动带 backend/）
    from bms_msg.protocol.contract import FRAME_CONTRACT_KEY, frame_contract

    openapi.set(FRAME_CONTRACT_KEY, frame_contract())
    return openapi


def _validate(record: ModuleRecord, openapi: ConcurrentStableDict[str, Any]) -> ConcurrentStableList[str]:
    """校验单个服务快照（结构 + 契约版本）。

    Args:
        record: 服务登记行。
        openapi: 公开契约映射。

    Returns:
        ConcurrentStableList[str]: 违规明细；空列表表示通过。
    """
    return ConcurrentStableList(
        validate_contract(cast("str", record.service_key), ConcurrentStableDict(openapi), record)
    )


def export(root: Path, *, to_stdout: bool = False) -> int:
    """导出各在运服务公开契约快照。

    Args:
        root: 仓库根。
        to_stdout: 是否同时打印生成内容。

    Returns:
        int: 退出码（0 成功；1 校验失败）。
    """
    target_dir = contracts_dir(root)
    target_dir.mkdir(parents=True, exist_ok=True)
    failures: ConcurrentStableList[str] = ConcurrentStableList()
    for record in enabled_local_records():
        service_key = cast("str", record.service_key)
        openapi = build_openapi(service_key)
        errors = _validate(record, openapi)
        if errors:
            failures.update(errors)
            continue
        text = render_contract_json(ConcurrentStableDict(openapi))
        path = snapshot_path(root, service_key)
        path.write_text(text, encoding="utf-8", newline="\n")
        if to_stdout:
            print(text, end="")
        else:
            print(f"[contract_snapshot] 已生成 {path}")
    if failures:
        for message in failures:
            print(f"  - {message}", file=sys.stderr)
        return 1
    return 0


def check(root: Path) -> int:
    """校验仓库内快照与本服务公开契约零漂移、契约版本一致、无缺件 / 无多余件。

    Args:
        root: 仓库根。

    Returns:
        int: 退出码（0 一致；1 缺件 / 多余件 / 漂移 / 契约版本不符）。
    """
    records = {cast("str", record.service_key): record for record in enabled_local_records()}
    expected = {contract_file_name(service_key) for service_key in records}
    directory = contracts_dir(root)
    actual: set[str] = {path.name for path in directory.glob("*.json")} if directory.is_dir() else set[str]()
    problems: ConcurrentStableList[str] = ConcurrentStableList(
        f"缺快照文件：{name}" for name in sorted(expected - actual)
    )
    problems.update(f"多余快照文件：{name}" for name in sorted(actual - expected))
    for service_key, record in records.items():
        path = snapshot_path(root, service_key)
        if not path.is_file():
            continue
        openapi = build_openapi(service_key)
        errors = _validate(record, openapi)
        if errors:
            problems.update(errors)
            continue
        if path.read_text(encoding="utf-8") != render_contract_json(ConcurrentStableDict(openapi)):
            problems.add(f"{service_key}：快照与当前公开契约漂移（重新运行 export）")
    if problems:
        print(f"[contract_snapshot] 不通过：{len(problems)} 项", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print(f"[contract_snapshot] 通过：{len(records)} 个在运服务快照与公开契约一致")
    return 0


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    parser = argparse.ArgumentParser(description="导出 / 校验本服务公开契约（OpenAPI）快照")
    parser.add_argument("command", choices=("export", "check"), help="export 导出；check 零漂移校验")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="仓库根（缺省自动定位）")
    parser.add_argument("--print", dest="to_stdout", action="store_true", help="export 时打印生成内容")
    args = parser.parse_args(argv)
    if args.command == "export":
        return export(args.root, to_stdout=args.to_stdout)
    return check(args.root)


if __name__ == "__main__":
    raise SystemExit(main())
