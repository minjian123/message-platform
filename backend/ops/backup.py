"""msg 位点快照备份（任务 07-3-1）：元数据库覆盖校验 + 位点日快照 + manifest + 保留期清理。

用法::

    python -m ops.backup                       # 正常备份；失败非零退出
    python -m ops.backup --dry-run             # 只读取证 + stdout 清单预览，不落盘
    python -m ops.backup --root <仓根>         # 指定仓库根（定位声明与配置文本）

**为什么是宿主侧离线命令**：镜像不含 `ops/`（07-2 已定），服务编排只挂 `data/` 与 `offsets/`
两个卷；备份根目录与平台库 dump 目录都在宿主，由 systemd 用户级 timer 注入环境变量后执行。
脚本不连库、不起应用、不读 `.env` 明文、不复制平台库与消息日志。

设计取舍：

1. **纯函数判据 + 可注入 IO 端口**：配置解析、空间判据、表集合对账、清单构建、过期判定都是
   纯函数；文件读写 / 目录遍历 / 时间之外的副作用收进 `BackupIO`，用例用替身注入并发写入、
   磁盘不足与中途失败，不需要真机把盘写满。
2. **失败隔离**：所有写入先落 `.staging-<备份标识>/`，校验通过才原子改名为已发布目录；
   任一步失败只删 staging，上一份快照与 `current.json` 原样不动。
3. **可核对**：manifest 与快照同目录，逐文件记 size + sha256；恢复侧（07-3-2）按同一份清单复算。
"""

from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from bms_msg import __version__ as SERVICE_VERSION
from bms_msg.core.config import (
    BACKUP_DIR_ENV,
    BACKUP_ID_FORMAT,
    BACKUP_KEEP_DAYS,
    BACKUP_KEEP_DAYS_ENV,
    BACKUP_MANIFEST_NAME,
    BACKUP_MANIFEST_SCHEMA_VERSION,
    BACKUP_PLATFORM_DUMP_DIR_ENV,
    BACKUP_POINTER_NAME,
    BACKUP_POINTER_TEMP_NAME,
    BACKUP_SNAPSHOT_RETRIES,
    BACKUP_SPACE_FACTOR,
    BACKUP_SPACE_FACTOR_ENV,
    BACKUP_SPACE_RESERVE_BYTES,
    BACKUP_STAGING_PREFIX,
    DATA_DIR_ENV,
    DATA_ROOT_SUBDIR,
    DEFAULT_DATA_DIR,
    OFFSETS_SUBDIR,
    RECOVERY_POINT_NAME,
)

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
"""后端工程根（`backend/`；脚本位于 `backend/ops/`）。"""

REPO_ROOT = _BACKEND_ROOT.parent
"""仓库根（缺省值；`--root` 可覆盖）。"""

OFFSET_SNAPSHOT_SUBDIR = "offsets"
"""快照目录内位点文件的子目录名（相对路径与源目录一致）。"""

_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`\"]?\w+[`\"]?\.)?[`\"]?(msg_[a-z0-9_]+)",
    re.IGNORECASE,
)
"""平台 dump 里的 `msg_*` 建表语句（兼容反引号、库名前缀与 IF NOT EXISTS）。"""

_SEGMENT_PATTERN = re.compile(r"^\d{20}\.log$")
"""段文件名形态（与 02-1 段枚举同规则，不导入 broker 包）。"""

_BACKUP_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")
"""已发布备份目录名形态。"""

_CONFIG_FILES = ConcurrentStableFrozenList(("config.toml", "config.dev.toml", "config.test.toml", "config.prod.toml"))
"""配置版本摘要的四份基线 / 环境覆盖（不含两份 `.env`）。"""

_RECOVERY_ORDER = ConcurrentStableFrozenList(("元数据库", "位点文件", "消息日志", "校验对齐"))
"""恢复顺序（架构 08「备份与恢复」节；恢复脚本按此顺序执行）。"""


class BackupError(RuntimeError):
    """备份失败：消息带 `backup.<类别>` 前缀与可定位对象，不含任何凭据值。"""


@dataclass(frozen=True)
class BackupConfig:
    """一次备份的生效配置（全部来自进程环境变量 / 仓内相对布局）。"""

    repo_root: Path
    """仓库根（定位声明与配置文本）。"""

    data_root: Path
    """消息数据根（`data/` 与 `offsets/` 的父目录）。"""

    backup_root: Path
    """备份根目录（宿主独立卷）。"""

    platform_dump_dir: Path
    """平台库备份产物目录（只读扫描）。"""

    keep_days: int
    """快照保留天数。"""

    space_factor: float
    """空间估算系数。"""

    dry_run: bool
    """只读取证、不落盘。"""

    now: datetime
    """本次执行的 UTC 时间（注入，便于用例复现）。"""


@dataclass(frozen=True)
class BackupIO:
    """可注入的副作用端口：默认实现走真实文件系统，用例可逐项替换。"""

    list_offset_files: Callable[[Path], ConcurrentStableList[Path]]
    file_size: Callable[[Path], int]
    read_bytes: Callable[[Path], bytes]
    write_bytes: Callable[[Path, bytes], None]
    list_tree: Callable[[Path], ConcurrentStableList[Path]]
    list_partitions: Callable[[Path], ConcurrentStableList[Path]]
    list_segments: Callable[[Path], ConcurrentStableList[str]]
    find_dump: Callable[[Path], Path | None]
    read_dump_tables: Callable[[Path], ConcurrentStableSet[str]]
    free_bytes: Callable[[Path], int]
    mkdir: Callable[[Path], None]
    replace: Callable[[Path, Path], None]
    list_backup_dirs: Callable[[Path], ConcurrentStableList[Path]]
    read_json: Callable[[Path], object]
    remove_tree: Callable[[Path], None]
    output: Callable[[str], None]
    alert: Callable[[str], None]


@dataclass(frozen=True)
class BackupPlan:
    """只读取证结果（正常备份与 dry-run 共用）。"""

    offset_entries: ConcurrentStableList[ConcurrentStableDict[str, object]]
    total_bytes: int
    required_bytes: int
    metadata: ConcurrentStableDict[str, object]
    tenants: ConcurrentStableFrozenList[str]
    segments: ConcurrentStableDict[str, object]
    versions: ConcurrentStableDict[str, str]
    config_version: str


# ---------------------------------------------------------------------------
# 默认 IO：真实文件系统
# ---------------------------------------------------------------------------


def fs_list_offset_files(root: Path) -> ConcurrentStableList[Path]:
    if not root.is_dir():
        raise BackupError(f"backup.source: 位点目录不存在（{root.name}）")
    found = (path.relative_to(root) for path in root.rglob("*.offset") if path.is_file())
    return ConcurrentStableList(sorted(found, key=str))


def fs_file_size(path: Path) -> int:
    return path.stat().st_size


def fs_read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def fs_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def fs_list_tree(root: Path) -> ConcurrentStableList[Path]:
    found = (path.relative_to(root) for path in root.rglob("*") if path.is_file())
    return ConcurrentStableList(sorted(found, key=str))


def fs_list_partitions(data_root: Path) -> ConcurrentStableList[Path]:
    root = data_root / DATA_ROOT_SUBDIR
    if not root.is_dir():
        return ConcurrentStableList()
    found = (path for path in root.glob("*/*/*/*") if path.is_dir() and path.name.isdigit())
    return ConcurrentStableList(sorted(found, key=str))


def fs_list_segments(partition_dir: Path) -> ConcurrentStableList[str]:
    found = (path.name for path in partition_dir.glob("*.log") if _SEGMENT_PATTERN.match(path.name))
    return ConcurrentStableList(sorted(found))


def fs_find_dump(dump_dir: Path) -> Path | None:
    if not dump_dir.is_dir():
        return None
    candidates = sorted(
        path
        for path in dump_dir.iterdir()
        if path.is_file() and (path.name.endswith(".sql.gz") or path.name.endswith(".sql"))
    )
    return candidates[-1] if candidates else None


def fs_read_dump_tables(path: Path) -> ConcurrentStableSet[str]:
    if path.suffix == ".gz":
        raw = gzip.decompress(path.read_bytes()).decode("utf-8", errors="replace")
    else:
        raw = path.read_text(encoding="utf-8", errors="replace")
    return ConcurrentStableSet(_TABLE_PATTERN.findall(raw))


def fs_free_bytes(path: Path) -> int:
    probe = path if path.exists() else path.parent
    return shutil.disk_usage(probe).free


def fs_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def fs_replace(source: Path, target: Path) -> None:
    source.replace(target)


def fs_list_backup_dirs(root: Path) -> ConcurrentStableList[Path]:
    if not root.is_dir():
        return ConcurrentStableList()
    found = (
        path
        for path in root.iterdir()
        if path.is_dir() and _BACKUP_ID_PATTERN.match(path.name) and (path / BACKUP_MANIFEST_NAME).is_file()
    )
    return ConcurrentStableList(sorted(found, key=str))


def fs_read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def fs_remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def fs_output(text: str) -> None:
    print(text)


def fs_alert(text: str) -> None:
    print(text, file=sys.stderr)


def filesystem_io() -> BackupIO:
    """构造默认 IO（真实文件系统 + stdout / stderr）。"""
    return BackupIO(
        list_offset_files=fs_list_offset_files,
        file_size=fs_file_size,
        read_bytes=fs_read_bytes,
        write_bytes=fs_write_bytes,
        list_tree=fs_list_tree,
        list_partitions=fs_list_partitions,
        list_segments=fs_list_segments,
        find_dump=fs_find_dump,
        read_dump_tables=fs_read_dump_tables,
        free_bytes=fs_free_bytes,
        mkdir=fs_mkdir,
        replace=fs_replace,
        list_backup_dirs=fs_list_backup_dirs,
        read_json=fs_read_json,
        remove_tree=fs_remove_tree,
        output=fs_output,
        alert=fs_alert,
    )


# ---------------------------------------------------------------------------
# 纯函数判据
# ---------------------------------------------------------------------------


def sha256_hex(data: bytes) -> str:
    """取字节串的 sha256 十六进制摘要。"""
    return hashlib.sha256(data).hexdigest()


def backup_id(now: datetime) -> str:
    """由 UTC 时间生成备份标识（字典序即时间序）。"""
    return now.strftime(BACKUP_ID_FORMAT)


def required_bytes(total: int, factor: float) -> int:
    """估算所需可用空间：位点总字节 × 系数 + 固定余量。"""
    return int(total * factor) + BACKUP_SPACE_RESERVE_BYTES


def check_space(total: int, free: int, factor: float) -> None:
    """空间不足即提前失败（正常备份与 dry-run 共用）。"""
    need = required_bytes(total, factor)
    if free < need:
        raise BackupError(f"backup.space: 可用空间不足（需要 {need} 字节，可用 {free} 字节）")


def expired(name: str, now: datetime, keep_days: int) -> bool:
    """判备份目录是否已过保留期（调用方保证目录名符合备份标识形态）。"""
    created = datetime.strptime(name, BACKUP_ID_FORMAT).replace(tzinfo=UTC)
    return now - created > timedelta(days=keep_days)


def config_from_env(
    env: ConcurrentStableDict[str, str],
    *,
    repo_root: Path,
    dry_run: bool,
    now: datetime,
) -> BackupConfig:
    """从进程环境变量解析生效配置；只消费已注入变量，不读取任何 `.env` 文件。"""
    return BackupConfig(
        repo_root=repo_root,
        data_root=Path(env.get(DATA_DIR_ENV, "") or DEFAULT_DATA_DIR),
        backup_root=required_path(env, BACKUP_DIR_ENV),
        platform_dump_dir=required_path(env, BACKUP_PLATFORM_DUMP_DIR_ENV),
        keep_days=positive_int(env, BACKUP_KEEP_DAYS_ENV, BACKUP_KEEP_DAYS),
        space_factor=factor(env, BACKUP_SPACE_FACTOR_ENV, BACKUP_SPACE_FACTOR),
        dry_run=dry_run,
        now=now,
    )


def required_path(env: ConcurrentStableDict[str, str], key: str) -> Path:
    raw = env.get(key, "")
    if not raw:
        raise BackupError(f"backup.config: 缺少必填环境变量 {key}")
    return Path(raw)


def positive_int(env: ConcurrentStableDict[str, str], key: str, default: int) -> int:
    raw = env.get(key, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise BackupError(f"backup.config: {key} 须为正整数") from exc
    if value <= 0:
        raise BackupError(f"backup.config: {key} 须为正整数")
    return value


def factor(env: ConcurrentStableDict[str, str], key: str, default: float) -> float:
    raw = env.get(key, "")
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise BackupError(f"backup.config: {key} 须为不小于 1 的有限数值") from exc
    if not 1.0 <= value < 100.0:
        raise BackupError(f"backup.config: {key} 须为不小于 1 的有限数值")
    return value


# ---------------------------------------------------------------------------
# 元数据库覆盖校验：模型清单 ↔ 平台 dump
# ---------------------------------------------------------------------------


def assigned_value(node: ast.stmt) -> ast.expr | None:
    """取 `MODEL_MODULES` 赋值语句的右值（普通赋值与带注解赋值都认）。"""
    if isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == "MODEL_MODULES" for target in node.targets
    ):
        return node.value
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "MODEL_MODULES":
        return node.value
    return None


def model_modules(tree: ast.Module) -> ConcurrentStableList[str]:
    """从模型包 `__init__.py` 的 AST 取模型模块清单（表集合的单一事实源）。"""
    found: ConcurrentStableList[str] = ConcurrentStableList()
    for node in tree.body:
        value = assigned_value(node)
        if value is None or not isinstance(value, ast.Tuple):
            continue
        for element in value.elts:
            if isinstance(element, ast.Constant) and isinstance(element.value, str):
                found.add(element.value)
    return found


def table_names(tree: ast.Module) -> ConcurrentStableList[str]:
    """从模型模块 AST 取 `__tablename__` 字面量。"""
    found: ConcurrentStableList[str] = ConcurrentStableList()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "__tablename__" for target in node.targets):
            continue
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            found.add(node.value.value)
    return found


def expected_tables(repo_root: Path, io: BackupIO) -> ConcurrentStableFrozenList[str]:
    """静态解析 `MODEL_MODULES` 指向的模型文件，导出期望的 `msg_*` 表集合。"""
    models_dir = repo_root / "backend" / "services" / "msg" / "src" / "bms_msg" / "models"
    try:
        modules = model_modules(ast.parse(io.read_bytes(models_dir / "__init__.py").decode("utf-8")))
        if not modules:
            raise BackupError("backup.metadata: 模型清单 MODEL_MODULES 缺失")
        names: ConcurrentStableSet[str] = ConcurrentStableSet()
        for module in modules:
            path = models_dir / f"{module.rsplit('.', 1)[-1]}.py"
            tables = table_names(ast.parse(io.read_bytes(path).decode("utf-8")))
            for table in tables:
                names.add(table)
    except (SyntaxError, UnicodeDecodeError) as exc:
        raise BackupError(f"backup.metadata: 模型清单不可解析：{exc}") from exc
    if not names:
        raise BackupError("backup.metadata: 模型清单未取到 msg_* 表名")
    return ConcurrentStableFrozenList(sorted(names))


def metadata_coverage(
    expected: ConcurrentStableFrozenList[str],
    found: ConcurrentStableSet[str],
    dump_name: str,
) -> ConcurrentStableDict[str, object]:
    """对账：期望表必须全在平台 dump 里；dump 里的历史多余表只登记、不判失败。"""
    missing = ConcurrentStableList(name for name in expected if name not in found)
    if missing:
        raise BackupError(f"backup.metadata: 平台备份缺表 {', '.join(missing)}")
    extra = ConcurrentStableList(name for name in sorted(found) if name not in expected)
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("source", "platform_database_backup")
    payload.set("dump", dump_name)
    payload.set("tables_expected", expected)
    payload.set("tables_in_dump", ConcurrentStableFrozenList(sorted(found)))
    payload.set("tables_extra", extra)
    payload.set("covered", True)
    return payload


# ---------------------------------------------------------------------------
# 只读取证：租户 / 分区段范围与 LEO / 版本 / 计划
# ---------------------------------------------------------------------------


def tenant_ids(
    files: ConcurrentStableList[Path],
    partitions: ConcurrentStableList[Path],
    data_root: Path,
) -> ConcurrentStableFrozenList[str]:
    """租户列表取 `offsets/` 与 `data/` 一级目录名的并集。"""
    names: ConcurrentStableSet[str] = ConcurrentStableSet()
    for relative in files:
        names.add(relative.parts[0])
    root = data_root / DATA_ROOT_SUBDIR
    for partition in partitions:
        names.add(partition.relative_to(root).parts[0])
    return ConcurrentStableFrozenList(sorted(names))


def leo_of(
    partition_dir: Path,
    bases: ConcurrentStableList[int],
    io: BackupIO,
) -> tuple[int, str]:
    """LEO 优先取恢复点；缺失 / 损坏时退化为段名最大值并标注来源。"""
    try:
        payload = io.read_json(partition_dir / RECOVERY_POINT_NAME)
        return (int(cast("dict[str, Any]", payload)["leo"]), "recovery-point")
    except OSError, ValueError, KeyError, TypeError:
        return (max(bases) + 1 if bases else 0, "segments_only")


def partition_summary(
    data_root: Path,
    partitions: ConcurrentStableList[Path],
    io: BackupIO,
) -> ConcurrentStableDict[str, object]:
    """只登记分区段范围与 LEO 摘要，不复制任何日志文件。"""
    root = data_root / DATA_ROOT_SUBDIR
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    needs: ConcurrentStableList[str] = ConcurrentStableList()
    for partition in partitions:
        relative = partition.relative_to(root)
        segments = io.list_segments(partition)
        bases = ConcurrentStableList(int(name[:20]) for name in segments if _SEGMENT_PATTERN.match(name))
        leo, source = leo_of(partition, bases, io)
        if source == "segments_only":
            needs.add(relative.as_posix())
        topic_parts = relative.parts[2].rsplit(".", 1)
        row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        row.set("tenant", relative.parts[0])
        row.set("namespace", relative.parts[1])
        row.set("topic", topic_parts[0])
        row.set("topic_id", topic_parts[-1])
        row.set("partition", int(relative.parts[3]))
        row.set("segment_bases", ConcurrentStableFrozenList(sorted(bases)))
        row.set("segment_count", len(bases))
        row.set("leo", leo)
        row.set("leo_source", source)
        rows.add(row)
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("backed_up", False)
    payload.set("partitions", rows)
    payload.set("needs_alignment", needs)
    return payload


def service_versions(repo_root: Path, io: BackupIO) -> ConcurrentStableDict[str, str]:
    """服务版本取包版本，契约版本取产品声明服务行。"""
    payload = tomllib.loads(io.read_bytes(repo_root / "deploy" / "registry" / "msg.toml").decode("utf-8"))
    services = cast("list[Any]", payload.get("services") or [])
    first = cast("dict[str, Any]", services[0]) if services else cast("dict[str, Any]", {})
    result: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    result.set("name", "msg")
    result.set("version", str(first.get("service_version") or SERVICE_VERSION))
    result.set("contract_version", str(first.get("contract_version") or ""))
    return result


def config_digest(repo_root: Path, io: BackupIO) -> str:
    """配置版本 = 四份基线 / 环境覆盖内容拼接后的 sha256（不含两份 `.env`）。"""
    digest = hashlib.sha256()
    for name in _CONFIG_FILES:
        digest.update(io.read_bytes(repo_root / "backend" / name))
    return digest.hexdigest()


def build_plan(config: BackupConfig, io: BackupIO) -> BackupPlan:
    """只读取证并组装计划：空间判据、表集合对账、租户 / 段摘要、版本与位点文件清单。"""
    offsets_root = config.data_root / OFFSETS_SUBDIR
    files = io.list_offset_files(offsets_root)
    entries: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    total = 0
    for relative in files:
        path = offsets_root / relative
        size = io.file_size(path)
        digest = sha256_hex(io.read_bytes(path))
        entry: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        entry.set("path", relative.as_posix())
        entry.set("size", size)
        entry.set("sha256", digest)
        entries.add(entry)
        total += size
    check_space(total, io.free_bytes(config.backup_root), config.space_factor)
    expected = expected_tables(config.repo_root, io)
    dump = io.find_dump(config.platform_dump_dir)
    if dump is None:
        raise BackupError("backup.metadata: 平台库备份产物不存在")
    metadata = metadata_coverage(expected, io.read_dump_tables(dump), dump.name)
    partitions = io.list_partitions(config.data_root)
    segments = partition_summary(config.data_root, partitions, io)
    return BackupPlan(
        offset_entries=entries,
        total_bytes=total,
        required_bytes=required_bytes(total, config.space_factor),
        metadata=metadata,
        tenants=tenant_ids(files, partitions, config.data_root),
        segments=segments,
        versions=service_versions(config.repo_root, io),
        config_version=config_digest(config.repo_root, io),
    )


# ---------------------------------------------------------------------------
# manifest 与快照
# ---------------------------------------------------------------------------


def plain(value: object) -> object:
    """把插入序并发集合递归转成 JSON 可序列化的普通结构（不改判据语义）。"""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, Mapping):
        mapping = cast("Mapping[Any, Any]", value)
        return {str(key): plain(item) for key, item in mapping.items()}
    if isinstance(value, Sequence):
        return [plain(item) for item in cast("Sequence[object]", value)]
    return str(value)


def render(payload: ConcurrentStableDict[str, object]) -> str:
    """渲染 JSON（键序稳定、中文不转义）。"""
    return json.dumps(plain(payload), ensure_ascii=False, sort_keys=True, indent=2)


def build_manifest(
    config: BackupConfig,
    plan: BackupPlan,
    files: ConcurrentStableList[ConcurrentStableDict[str, object]],
    dry_run: bool,
) -> ConcurrentStableDict[str, object]:
    """组装 manifest；`files` 为快照目录逐文件清单（dry-run 时为空）。"""
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("schema_version", BACKUP_MANIFEST_SCHEMA_VERSION)
    payload.set("backup_id", backup_id(config.now))
    payload.set("created_at", config.now.isoformat())
    payload.set("dry_run", dry_run)
    payload.set("service", plan.versions)
    payload.set("config_version", plan.config_version)
    payload.set("tenants", plan.tenants)
    payload.set("metadata", plan.metadata)
    offsets: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    offsets.set("files", plan.offset_entries)
    offsets.set("count", len(plan.offset_entries))
    offsets.set("bytes", plan.total_bytes)
    payload.set("offsets", offsets)
    payload.set("log_segments", plan.segments)
    payload.set("recovery_order", _RECOVERY_ORDER)
    payload.set("files", files)
    payload.set("retention_days", config.keep_days)
    return payload


def copy_offsets(
    config: BackupConfig,
    staging: Path,
    plan: BackupPlan,
    io: BackupIO,
) -> None:
    """逐文件复制并做「源文件双读 + 副本复算」一致性校验，不稳定即重试。"""
    for entry in plan.offset_entries:
        relative = Path(cast("str", entry.get("path")))
        source = config.data_root / OFFSETS_SUBDIR / relative
        target = staging / OFFSET_SNAPSHOT_SUBDIR / relative
        expected_size = cast("int", entry.get("size"))
        expected_digest = cast("str", entry.get("sha256"))
        for _ in range(BACKUP_SNAPSHOT_RETRIES):
            first = io.read_bytes(source)
            io.write_bytes(target, first)
            second = io.read_bytes(source)
            copied = io.read_bytes(target)
            if first == second == copied and len(first) == expected_size and sha256_hex(first) == expected_digest:
                break
        else:
            raise BackupError(f"backup.snapshot: 写入期间校验和未稳定：{relative.as_posix()}")


def snapshot_entries(staging: Path, io: BackupIO) -> ConcurrentStableList[ConcurrentStableDict[str, object]]:
    """逐文件复算快照目录（manifest 自身尚未写入，故清单不含清单文件）。"""
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    for relative in io.list_tree(staging):
        raw = io.read_bytes(staging / relative)
        row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        row.set("path", relative.as_posix())
        row.set("size", len(raw))
        row.set("sha256", sha256_hex(raw))
        rows.add(row)
    return rows


def verify_snapshot(
    staging: Path,
    files: ConcurrentStableList[ConcurrentStableDict[str, object]],
    io: BackupIO,
) -> None:
    """发布前逐文件复算；任一不符即整份失败（上一份不受影响）。"""
    for row in files:
        relative = Path(cast("str", row.get("path")))
        raw = io.read_bytes(staging / relative)
        if len(raw) != cast("int", row.get("size")) or sha256_hex(raw) != cast("str", row.get("sha256")):
            raise BackupError(f"backup.snapshot: 副本校验和不符：{relative.as_posix()}")


def write_pointer(config: BackupConfig, io: BackupIO, identifier: str) -> None:
    """原子写 `current.json`（同目录临时文件 + 替换，不用符号链接）。"""
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("backup_id", identifier)
    payload.set("manifest", BACKUP_MANIFEST_NAME)
    payload.set("created_at", config.now.isoformat())
    temporary = config.backup_root / BACKUP_POINTER_TEMP_NAME
    io.write_bytes(temporary, render(payload).encode("utf-8"))
    io.replace(temporary, config.backup_root / BACKUP_POINTER_NAME)


def purge(config: BackupConfig, io: BackupIO) -> None:
    """按保留期清理过期快照；最新一份与指针所指目录永不删，单份清理失败只告警。"""
    dirs = io.list_backup_dirs(config.backup_root)
    names = sorted(path.name for path in dirs)
    if not names:
        return
    newest = names[-1]
    try:
        pointer = cast("dict[str, Any]", io.read_json(config.backup_root / BACKUP_POINTER_NAME))
        pointer_id = str(pointer.get("backup_id", ""))
    except OSError, ValueError, AttributeError:
        pointer_id = ""
    for path in dirs:
        if path.name == newest or path.name == pointer_id:
            continue
        if expired(path.name, config.now, config.keep_days):
            try:
                io.remove_tree(path)
            except OSError as exc:
                io.alert(f"[ALERT] backup.purge {path.name}: {exc}")


def cleanup_staging(config: BackupConfig, io: BackupIO) -> None:
    """失败收尾：只删本次 staging，绝不触碰已发布目录与指针。"""
    io.remove_tree(config.backup_root / f"{BACKUP_STAGING_PREFIX}{backup_id(config.now)}")


# ---------------------------------------------------------------------------
# 编排与命令入口
# ---------------------------------------------------------------------------


def run(config: BackupConfig, io: BackupIO) -> int:
    """执行一次备份；返回 0 成功 / 1 失败（明细写标准错误）。"""
    try:
        plan = build_plan(config, io)
        identifier = backup_id(config.now)
        if config.dry_run:
            io.output(render(build_manifest(config, plan, ConcurrentStableList(), True)))
            return 0
        existing = io.list_backup_dirs(config.backup_root)
        if any(path.name == identifier for path in existing):
            raise BackupError(f"backup.publish: 同标识备份已存在：{identifier}")
        staging = config.backup_root / f"{BACKUP_STAGING_PREFIX}{identifier}"
        io.mkdir(staging)
        copy_offsets(config, staging, plan, io)
        files = snapshot_entries(staging, io)
        manifest = build_manifest(config, plan, files, False)
        io.write_bytes(staging / BACKUP_MANIFEST_NAME, render(manifest).encode("utf-8"))
        verify_snapshot(staging, files, io)
        io.replace(staging, config.backup_root / identifier)
        write_pointer(config, io, identifier)
        purge(config, io)
        io.output(f"[OK] backup.published {identifier} files={len(files)}")
        return 0
    except (BackupError, OSError) as exc:
        reason = str(exc) if isinstance(exc, BackupError) else f"backup.io: {exc}"
        io.alert(f"[FAIL] {reason}")
        io.alert(f"[ALERT] {reason}")
        cleanup_staging(config, io)
        return 1


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口：只消费已注入环境变量，不读取 `.env` 明文。"""
    parser = argparse.ArgumentParser(description="msg 位点快照备份：覆盖校验 + 清单 + 日快照")
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="仓库根（缺省自动定位）")
    parser.add_argument("--dry-run", action="store_true", help="只读取证并输出清单预览，不落盘")
    args = parser.parse_args(None if argv is None else argv)
    try:
        config = config_from_env(
            ConcurrentStableDict(os.environ),
            repo_root=args.root,
            dry_run=args.dry_run,
            now=datetime.now(UTC),
        )
    except BackupError as exc:
        fs_alert(f"[FAIL] {exc}")
        return 1
    return run(config, filesystem_io())


if __name__ == "__main__":
    raise SystemExit(main())
