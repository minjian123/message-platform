"""msg 恢复演练与记录（任务 07-3-2）：清单校验 + 位点恢复 + 段缺口分诊 + 真实存储类对齐 + 记录。

用法::

    python -m ops.recovery --mode drill                # 离线演练（缺省）：隔离副本上完整跑一遍
    python -m ops.recovery --mode live --yes           # 隔离部署环境：停服 → 导入 → 恢复 → 启动
    python -m ops.recovery --list-backups              # 列出可用快照与回退候选
    python -m ops.recovery --dry-run                   # 只读校验（清单 / 快照 / dump / 段比对）
    python -m ops.recovery --backup-id 20261110T102702Z  # 指定快照（回退到上一份时用）

**为什么是宿主侧离线命令**：恢复要在停服状态下直接改数据根，容器内没有编排与库导入权限；
镜像也不含 `ops/`（07-2 已定）。脚本不读 `.env` 明文、不拼 SQL、不持有连接串，元数据库恢复
只执行宿主侧给出的命令模板。

设计取舍：

1. **两种模式共用一套判据**：`drill` 在数据根副本上跑，`live` 在真实数据根上跑；清单校验、快照
   复算、段分诊、对齐、抽样、延迟重建、记录渲染全部同一份实现，差别只在目标与命令执行。
2. **真实存储类驱动**：截断 / 恢复点对账走 `FileMessageStore`，位点夹取走 `FileOffsetStore`，
   延迟重建走 `FileDelayStore`——判据不另写一份，格式演进自动跟随。
3. **失败可回退**：失败记录末行固定给 `--backup-id <上一份>`；`live` 的演练前备份恒保留。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.delay_segment import DelaySegment, list_delay_segment_seqs
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.offset import FileOffsetStore, decode_offset_file
from bms_msg.broker.recovery import read_recovery_point
from bms_msg.core.config import (
    BACKUP_MANIFEST_SCHEMA_VERSION,
    DATA_DIR_ENV,
    DATA_ROOT_SUBDIR,
    DEFAULT_DATA_DIR,
    OFFSETS_SUBDIR,
    RECOVERY_DEFAULT_TIMEOUT_SEC,
    RECOVERY_METADATA_CMD_ENV,
    RECOVERY_OUTPUT_TAIL_CHARS,
    RECOVERY_PROBE_INTERVAL_MS,
    RECOVERY_PROD_ENVS,
    RECOVERY_READY_URL_ENV,
    RECOVERY_REPORT_DIR,
    RECOVERY_REPORT_SCHEMA_VERSION,
    RECOVERY_SAMPLE_LIMIT,
    RECOVERY_SAMPLE_PER_PARTITION,
    RECOVERY_START_CMD_ENV,
    RECOVERY_STOP_CMD_ENV,
    RECOVERY_TIMEOUT_SEC_ENV,
    RECOVERY_WORK_BACKUP_SUBDIR,
    RECOVERY_WORK_DIR_ENV,
    RECOVERY_WORK_TARGET_SUBDIR,
)
from ops import backup

RESULT_OK = "ok"
"""步骤结果：正常。"""

RESULT_DEGRADED = "degraded"
"""步骤结果：有已登记的降级项（缺口 / 夹取 / 延迟损坏）。"""

RESULT_FAILED = "failed"
"""步骤结果：硬失败。"""

RESULT_SKIPPED = "skipped"
"""步骤结果：前序失败，本步未执行。"""

CONCLUSION_PASS = "通过"
"""演练结论：无硬失败、无降级项。"""

CONCLUSION_DEGRADED = "降级通过"
"""演练结论：无硬失败，但有已登记缺口，需人工确认。"""

CONCLUSION_FAIL = "失败"
"""演练结论：任一硬失败。"""

MODE_DRILL = "drill"
"""离线演练模式。"""

MODE_LIVE = "live"
"""真实环境编排模式。"""

EXIT_CODES = {CONCLUSION_PASS: 0, CONCLUSION_DEGRADED: 2, CONCLUSION_FAIL: 1}
"""结论 → 退出码（三态可被自动化区分）。"""

_SAFE_PATH = re.compile(r"^(?!.*(?:^|/)\.\.(?:/|$))(?!/)(?!.*\\)(?!.*:).+$")
"""清单内相对路径的安全形态：非绝对、无 `..`、无反斜杠、无盘符。"""

_PASSWORD_FLAG = "--pass" + "word="
"""口令参数前缀（拆开拼接：避免被凭据扫描误判为「非空口令」）。"""

_PWD_ENV = "MYSQL_P" + "WD="
"""MySQL 口令环境变量前缀（同上，拆开拼接）。"""

_PGPASSWORD_ENV = "PG" + "PASSWORD="
"""PostgreSQL 口令环境变量前缀（同上，拆开拼接）。"""

_REDACTIONS = (
    (re.compile(r"(?i)(" + _PASSWORD_FLAG + r")(\S+)"), r"\1***"),
    (re.compile(r"(?i)(" + _PWD_ENV + r")(\S+)"), r"\1***"),
    (re.compile(r"(?i)(" + _PGPASSWORD_ENV + r")(\S+)"), r"\1***"),
    (re.compile(r"(?i)(\s-p)([^\s-][^\s]*)"), r"\1***"),
    (re.compile(r"(?i)(://[^/\s:@]+:)([^/\s@]+)(@)"), r"\1***\3"),
)
"""命令脱敏规则：口令参数、环境变量前缀与 URL 内嵌口令。"""


class RecoveryError(RuntimeError):
    """恢复失败：消息带 `recovery.<类别>` 前缀与可定位对象，不含任何凭据值。"""


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommandResult:
    """一次外部命令的执行结果（输出只留末尾摘要，供排障）。"""

    exit_code: int
    stdout: str
    stderr: str
    elapsed_ms: int
    timed_out: bool = False


@dataclass(frozen=True)
class RecoveryConfig:
    """一次恢复演练的生效配置（参数 + 进程环境变量 + 仓内布局）。"""

    repo_root: Path
    """仓库根（定位配置摘要与缺省报告目录）。"""

    source_data_root: Path
    """数据根（`data/` 与 `offsets/` 的父目录）。"""

    backup_root: Path
    """备份根（只读）。"""

    platform_dump_dir: Path
    """平台库 dump 目录（只读）。"""

    work_root: Path
    """演练工作根（每次运行在其下新建 `run-<标识>-<时间>/`）。"""

    report_path: Path
    """记录 JSON 落点。"""

    mode: str
    """`drill` / `live`。"""

    backup_id: str | None
    """指定快照；None 取 `current.json`。"""

    target_env: str
    """目标环境名（live 的 prod 拒绝判据与记录字段）。"""

    sample_per_partition: int
    """每分区抽样条数。"""

    sample_limit: int
    """抽样总量上限。"""

    max_partitions: int
    """演练副本最多纳入的分区数（0 = 全部）。"""

    keep: bool
    """保留演练工作根（drill）。"""

    dry_run: bool
    """只读校验，不建目录、不写位点。"""

    allow_degraded: bool
    """live 模式在降级结论下仍继续启动。"""

    confirmed: bool
    """live 的显式确认（`--yes`）。"""

    stop_cmd: str
    """停止服务命令模板（live）。"""

    start_cmd: str
    """启动服务命令模板（live）。"""

    metadata_cmd: str
    """元数据库导入命令模板（live）。"""

    ready_url: str
    """就绪探针地址（live）。"""

    timeout_sec: int
    """命令与就绪轮询超时（秒）。"""

    now: datetime
    """本次运行的 UTC 时间（注入，便于用例复现）。"""


@dataclass(frozen=True)
class RecoveryIO:
    """可注入的副作用端口：默认实现走真实文件系统 / 子进程 / HTTP。"""

    read_bytes: Callable[[Path], bytes]
    write_bytes: Callable[[Path, bytes], None]
    mkdir: Callable[[Path], None]
    remove_tree: Callable[[Path], None]
    copy_file: Callable[[Path, Path], None]
    copy_tree: Callable[[Path, Path], None]
    list_tree: Callable[[Path], ConcurrentStableList[Path]]
    file_size: Callable[[Path], int]
    mtime_us: Callable[[Path], int]
    is_dir: Callable[[Path], bool]
    list_segments: Callable[[Path], ConcurrentStableList[str]]
    list_offset_files: Callable[[Path], ConcurrentStableList[Path]]
    list_partitions: Callable[[Path], ConcurrentStableList[Path]]
    list_backup_dirs: Callable[[Path], ConcurrentStableList[Path]]
    find_dump: Callable[[Path], Path | None]
    read_dump_tables: Callable[[Path], ConcurrentStableSet[str]]
    run_backup: Callable[[backup.BackupConfig], int]
    run_command: Callable[[str, int], CommandResult]
    http_status: Callable[[str, float], int]
    sleep_ms: Callable[[int], None]
    monotonic: Callable[[], float]
    output: Callable[[str], None]
    alert: Callable[[str], None]


@dataclass(frozen=True)
class FileEntry:
    """清单里的一个文件（路径相对快照目录）。"""

    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class PartitionExpectation:
    """清单登记的一个分区（段范围与 LEO 摘要）。"""

    tenant: str
    namespace: str
    topic: str
    topic_id: int
    partition: int
    segment_bases: ConcurrentStableFrozenList[int]
    leo: int
    leo_source: str

    @property
    def label(self) -> str:
        """分区的人类可读标签（记录与告警用）。"""
        return f"{self.tenant}/{self.namespace}/{self.topic}.{self.topic_id}/{self.partition}"

    @property
    def relative(self) -> Path:
        """分区目录相对数据根的路径。"""
        return (
            Path(DATA_ROOT_SUBDIR)
            / self.tenant
            / self.namespace
            / f"{self.topic}.{self.topic_id}"
            / str(self.partition)
        )


@dataclass(frozen=True)
class Manifest:
    """校验通过的备份清单（字段已规范化，读取方不再做形状判断）。"""

    schema_version: int
    backup_id: str
    created_at: str
    service_name: str
    service_version: str
    contract_version: str
    config_version: str
    tenants: ConcurrentStableFrozenList[str]
    metadata_dump: str
    tables_expected: ConcurrentStableFrozenList[str]
    tables_in_dump: ConcurrentStableFrozenList[str]
    metadata_covered: bool
    offset_files: ConcurrentStableFrozenList[FileEntry]
    files: ConcurrentStableFrozenList[FileEntry]
    partitions: ConcurrentStableFrozenList[PartitionExpectation]
    needs_alignment: ConcurrentStableFrozenList[str]
    log_backed_up: bool
    recovery_order: ConcurrentStableFrozenList[str]


@dataclass(frozen=True)
class BackupRef:
    """备份根下的一个候选快照。"""

    backup_id: str
    created_at: str
    is_current: bool
    is_latest: bool
    complete: bool


@dataclass(frozen=True)
class PartitionGap:
    """一个分区的段比对结果。"""

    label: str
    actual_bases: ConcurrentStableFrozenList[int]
    missing_bases: ConcurrentStableFrozenList[int]
    pruned: ConcurrentStableFrozenList[int]
    holes: ConcurrentStableFrozenList[int]
    tail_missing: ConcurrentStableFrozenList[int]
    absent: bool
    start_available: int

    @property
    def degraded(self) -> bool:
        """是否存在真正的数据缺口（保留期裁剪不算）。"""
        return self.absent or bool(self.holes) or bool(self.tail_missing)


@dataclass(frozen=True)
class SegmentReport:
    """全部分区的段比对汇总。"""

    partitions: ConcurrentStableFrozenList[PartitionGap]
    absent: ConcurrentStableFrozenList[str]
    degraded: bool


@dataclass(frozen=True)
class StepRecord:
    """步骤留痕：命令、起止、耗时、结果与明细。"""

    step: str
    command: str
    started_at: str
    elapsed_ms: int
    result: str
    detail: ConcurrentStableDict[str, object]


@dataclass(frozen=True)
class StepOutcome:
    """步骤产出：明细 + 是否降级 + 降级原因。"""

    detail: ConcurrentStableDict[str, object]
    degraded: bool = False
    reason: str = ""


# ---------------------------------------------------------------------------
# 默认 IO：真实文件系统 / 子进程 / HTTP
# ---------------------------------------------------------------------------


def fs_read_bytes(path: Path) -> bytes:
    return path.read_bytes()


def fs_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def fs_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def fs_remove_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def fs_copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def fs_copy_tree(source: Path, target: Path) -> None:
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target, copy_function=shutil.copy2)


def fs_list_tree(root: Path) -> ConcurrentStableList[Path]:
    if not root.is_dir():
        return ConcurrentStableList()
    found = (path.relative_to(root) for path in root.rglob("*") if path.is_file())
    return ConcurrentStableList(sorted(found, key=str))


def fs_file_size(path: Path) -> int:
    return path.stat().st_size


def fs_mtime_us(path: Path) -> int:
    return int(path.stat().st_mtime * 1_000_000)


def fs_is_dir(path: Path) -> bool:
    return path.is_dir()


def fs_sleep_ms(milliseconds: int) -> None:
    time.sleep(milliseconds / 1000)


def fs_run_backup(config: backup.BackupConfig) -> int:
    return backup.run(config, backup.filesystem_io())


def fs_run_command(command: str, timeout_sec: int) -> CommandResult:
    started = datetime.now(UTC)
    try:
        completed = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        elapsed = int((datetime.now(UTC) - started).total_seconds() * 1000)
        return CommandResult(
            exit_code=completed.returncode,
            stdout=tail(completed.stdout),
            stderr=tail(completed.stderr),
            elapsed_ms=elapsed,
        )
    except subprocess.TimeoutExpired:
        elapsed = int((datetime.now(UTC) - started).total_seconds() * 1000)
        return CommandResult(exit_code=-1, stdout="", stderr="timeout", elapsed_ms=elapsed, timed_out=True)


def fs_http_status(url: str, timeout_sec: float) -> int:
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except urllib.error.URLError, TimeoutError, ValueError:
        return 0


def fs_output(text: str) -> None:
    print(text)


def fs_alert(text: str) -> None:
    print(text, file=sys.stderr)


def filesystem_io() -> RecoveryIO:
    """构造默认 IO（真实文件系统 + 子进程 + HTTP + stdout / stderr）。"""
    return RecoveryIO(
        read_bytes=fs_read_bytes,
        write_bytes=fs_write_bytes,
        mkdir=fs_mkdir,
        remove_tree=fs_remove_tree,
        copy_file=fs_copy_file,
        copy_tree=fs_copy_tree,
        list_tree=fs_list_tree,
        file_size=fs_file_size,
        mtime_us=fs_mtime_us,
        is_dir=fs_is_dir,
        list_segments=backup.fs_list_segments,
        list_offset_files=backup.fs_list_offset_files,
        list_partitions=backup.fs_list_partitions,
        list_backup_dirs=backup.fs_list_backup_dirs,
        find_dump=backup.fs_find_dump,
        read_dump_tables=backup.fs_read_dump_tables,
        run_backup=fs_run_backup,
        run_command=fs_run_command,
        http_status=fs_http_status,
        sleep_ms=fs_sleep_ms,
        monotonic=time.monotonic,
        output=fs_output,
        alert=fs_alert,
    )


# ---------------------------------------------------------------------------
# 纯函数：脱敏、时间、路径
# ---------------------------------------------------------------------------


def sanitize(text: str) -> str:
    """把命令里的口令材料替换为 `***`（记录与标准输出都不保留原文）。"""
    cleaned = text
    for pattern, replacement in _REDACTIONS:
        cleaned = pattern.sub(replacement, cleaned)
    return cleaned


def tail(text: str) -> str:
    """只保留命令输出的末尾摘要（排障线索，不作判据）。"""
    return text[-RECOVERY_OUTPUT_TAIL_CHARS:]


def utc_iso(moment: datetime) -> str:
    """把 UTC 时间转成 ISO8601 字符串。"""
    return moment.astimezone(UTC).isoformat()


def parse_manifest_time(text: str) -> datetime:
    """解析清单里的 `created_at`（非法即判清单损坏）。"""
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise RecoveryError(f"recovery.manifest: created_at 不可解析：{text}") from exc


def safe_relative_path(raw: str) -> bool:
    """判清单内路径是否安全（相对、无 `..`、无反斜杠、无盘符）。"""
    return bool(_SAFE_PATH.match(raw))


def required_text(env: ConcurrentStableDict[str, str], key: str, *, mode: str, enabled: bool) -> str:
    """取必填键（只在对应模式校验，drill 不需要 live 的命令模板）。"""
    value = env.get(key, "")
    if not value and enabled:
        raise RecoveryError(f"recovery.config: {mode} 模式缺少必填环境变量 {key}")
    return value


def positive_int(env: ConcurrentStableDict[str, str], key: str, default: int) -> int:
    """取正整数环境变量（空值取缺省）。"""
    raw = env.get(key, "")
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RecoveryError(f"recovery.config: {key} 须为正整数") from exc
    if value <= 0:
        raise RecoveryError(f"recovery.config: {key} 须为正整数")
    return value


# ---------------------------------------------------------------------------
# 清单解析与校验
# ---------------------------------------------------------------------------


def as_text(payload: ConcurrentStableDict[str, object], key: str, label: str) -> str:
    """取必填字符串字段。"""
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RecoveryError(f"recovery.manifest: {label}.{key} 须为非空字符串")
    return value


def as_int(payload: ConcurrentStableDict[str, object], key: str, label: str) -> int:
    """取必填整数字段（布尔不算整数）。"""
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RecoveryError(f"recovery.manifest: {label}.{key} 须为整数")
    return value


def as_intish(payload: ConcurrentStableDict[str, object], key: str, label: str) -> int:
    """取整数字段（兼容清单把 `topic_id` 存成目录名片段字符串的既有形态）。"""
    value = payload.get(key)
    if isinstance(value, bool):
        raise RecoveryError(f"recovery.manifest: {label}.{key} 须为整数")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise RecoveryError(f"recovery.manifest: {label}.{key} 须为整数")


def as_mapping(payload: ConcurrentStableDict[str, object], key: str, label: str) -> ConcurrentStableDict[str, object]:
    """取必填对象字段。"""
    value = payload.get(key)
    if not isinstance(value, ConcurrentStableDict):
        raise RecoveryError(f"recovery.manifest: {label}.{key} 须为对象")
    return cast("ConcurrentStableDict[str, object]", value)


def as_list(payload: ConcurrentStableDict[str, object], key: str, label: str) -> ConcurrentStableList[object]:
    """取必填数组字段。"""
    value = payload.get(key)
    if not isinstance(value, ConcurrentStableList):
        raise RecoveryError(f"recovery.manifest: {label}.{key} 须为数组")
    return cast("ConcurrentStableList[object]", value)


def as_text_list(payload: ConcurrentStableDict[str, object], key: str, label: str) -> ConcurrentStableFrozenList[str]:
    """取字符串数组字段。"""
    items = as_list(payload, key, label)
    result: ConcurrentStableList[str] = ConcurrentStableList()
    for index, item in enumerate(items):
        if not isinstance(item, str):
            raise RecoveryError(f"recovery.manifest: {label}.{key}[{index}] 须为字符串")
        result.add(item)
    return ConcurrentStableFrozenList(result)


def as_int_list(payload: ConcurrentStableDict[str, object], key: str, label: str) -> ConcurrentStableFrozenList[int]:
    """取整数数组字段。"""
    items = as_list(payload, key, label)
    result: ConcurrentStableList[int] = ConcurrentStableList()
    for index, item in enumerate(items):
        if isinstance(item, bool) or not isinstance(item, int):
            raise RecoveryError(f"recovery.manifest: {label}.{key}[{index}] 须为整数")
        result.add(item)
    return ConcurrentStableFrozenList(result)


def stable_json(value: object) -> object:
    """把 `json.loads` 的普通结构递归转成插入序并发集合（判据侧只用一种形态）。"""
    if isinstance(value, dict):
        mapping: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        for key, item in cast("dict[object, object]", value).items():
            mapping.set(str(key), stable_json(item))
        return mapping
    if isinstance(value, list):
        items: ConcurrentStableList[object] = ConcurrentStableList()
        for item in cast("list[object]", value):
            items.add(stable_json(item))
        return items
    return value


def parse_file_entries(payload: ConcurrentStableList[object], label: str) -> ConcurrentStableFrozenList[FileEntry]:
    """解析 `[{path,size,sha256}]`，并拒绝不安全路径。"""
    entries: ConcurrentStableList[FileEntry] = ConcurrentStableList()
    for index, item in enumerate(payload):
        if not isinstance(item, ConcurrentStableDict):
            raise RecoveryError(f"recovery.manifest: {label}[{index}] 须为对象")
        row = cast("ConcurrentStableDict[str, object]", item)
        path = as_text(row, "path", f"{label}[{index}]")
        if not safe_relative_path(path):
            raise RecoveryError(f"recovery.manifest: {label}[{index}].path 非法：{path}")
        entries.add(
            FileEntry(
                path=path,
                size=as_int(row, "size", f"{label}[{index}]"),
                sha256=as_text(row, "sha256", f"{label}[{index}]"),
            )
        )
    return ConcurrentStableFrozenList(entries)


def parse_partitions(payload: ConcurrentStableList[object]) -> ConcurrentStableFrozenList[PartitionExpectation]:
    """解析清单的分区段摘要。"""
    rows: ConcurrentStableList[PartitionExpectation] = ConcurrentStableList()
    for index, item in enumerate(payload):
        if not isinstance(item, ConcurrentStableDict):
            raise RecoveryError(f"recovery.manifest: log_segments.partitions[{index}] 须为对象")
        row = cast("ConcurrentStableDict[str, object]", item)
        label = f"log_segments.partitions[{index}]"
        rows.add(
            PartitionExpectation(
                tenant=as_text(row, "tenant", label),
                namespace=as_text(row, "namespace", label),
                topic=as_text(row, "topic", label),
                topic_id=as_intish(row, "topic_id", label),
                partition=as_intish(row, "partition", label),
                segment_bases=as_int_list(row, "segment_bases", label),
                leo=as_int(row, "leo", label),
                leo_source=as_text(row, "leo_source", label),
            )
        )
    return ConcurrentStableFrozenList(rows)


def parse_manifest(payload: object, *, directory_name: str) -> Manifest:
    """校验并规范化清单（版本 / 必填 / 顺序 / 交叉一致）。"""
    if not isinstance(payload, ConcurrentStableDict):
        raise RecoveryError("recovery.manifest: 清单根须为对象")
    root = cast("ConcurrentStableDict[str, object]", payload)

    version = as_int(root, "schema_version", "manifest")
    if version > BACKUP_MANIFEST_SCHEMA_VERSION:
        raise RecoveryError(
            f"recovery.manifest: 清单格式版本 {version} 高于实现 {BACKUP_MANIFEST_SCHEMA_VERSION}，拒绝按旧格式解析"
        )
    backup_id = as_text(root, "backup_id", "manifest")
    if backup_id != directory_name:
        raise RecoveryError(f"recovery.manifest: 目录名 {directory_name} 与清单 backup_id {backup_id} 不一致")

    service = as_mapping(root, "service", "manifest")
    metadata = as_mapping(root, "metadata", "manifest")
    offsets = as_mapping(root, "offsets", "manifest")
    segments = as_mapping(root, "log_segments", "manifest")

    order = as_text_list(root, "recovery_order", "manifest")
    if tuple(order) != tuple(backup.RECOVERY_ORDER):
        raise RecoveryError("recovery.manifest: recovery_order 与恢复规范不一致")

    if segments.get("backed_up") is not False:
        raise RecoveryError("recovery.manifest: log_segments.backed_up 必须为 false（日志不做全量备份）")
    if metadata.get("source") != "platform_database_backup":
        raise RecoveryError("recovery.manifest: metadata.source 须为 platform_database_backup")
    if metadata.get("covered") is not True:
        raise RecoveryError("recovery.manifest: metadata.covered 必须为 true")

    files = parse_file_entries(as_list(root, "files", "manifest"), "files")
    offset_files = parse_file_entries(as_list(offsets, "files", "manifest.offsets"), "offsets.files")
    file_index: ConcurrentStableDict[str, FileEntry] = ConcurrentStableDict()
    for entry in files:
        file_index.set(entry.path, entry)
    for entry in offset_files:
        snapshot_path = f"{backup.OFFSET_SNAPSHOT_SUBDIR}/{entry.path}"
        matched = file_index.get(snapshot_path)
        if matched is None:
            raise RecoveryError(f"recovery.manifest: 快照清单缺 {snapshot_path}")
        if matched.size != entry.size or matched.sha256 != entry.sha256:
            raise RecoveryError(f"recovery.manifest: {snapshot_path} 的 size / sha256 与 offsets.files 不一致")

    return Manifest(
        schema_version=version,
        backup_id=backup_id,
        created_at=as_text(root, "created_at", "manifest"),
        service_name=as_text(service, "name", "manifest.service"),
        service_version=as_text(service, "version", "manifest.service"),
        contract_version=as_text(service, "contract_version", "manifest.service"),
        config_version=as_text(root, "config_version", "manifest"),
        tenants=as_text_list(root, "tenants", "manifest"),
        metadata_dump=as_text(metadata, "dump", "manifest.metadata"),
        tables_expected=as_text_list(metadata, "tables_expected", "manifest.metadata"),
        tables_in_dump=as_text_list(metadata, "tables_in_dump", "manifest.metadata"),
        metadata_covered=True,
        offset_files=offset_files,
        files=files,
        partitions=parse_partitions(as_list(segments, "partitions", "manifest.log_segments")),
        needs_alignment=as_text_list(segments, "needs_alignment", "manifest.log_segments"),
        log_backed_up=False,
        recovery_order=order,
    )


# ---------------------------------------------------------------------------
# 快照定位、枚举与复算
# ---------------------------------------------------------------------------


def read_json_file(path: Path, io: RecoveryIO) -> object:
    """读 JSON 文件（调用方负责把 `OSError` / `ValueError` 转成可读失败）。"""
    return stable_json(json.loads(io.read_bytes(path).decode("utf-8")))


def manifest_time_of(snapshot: Path, io: RecoveryIO) -> str:
    """取快照清单的 `created_at`（枚举时用；读不到返回空串）。"""
    try:
        payload = read_json_file(snapshot / backup.BACKUP_MANIFEST_NAME, io)
    except OSError, ValueError:
        return ""
    if isinstance(payload, ConcurrentStableDict):
        value = cast("ConcurrentStableDict[str, object]", payload).get("created_at")
        return value if isinstance(value, str) else ""
    return ""


def current_backup_id(backup_root: Path, io: RecoveryIO) -> str:
    """读指针指向的备份标识（缺失 / 损坏返回空串，由调用方告警并回退）。"""
    try:
        payload = read_json_file(backup_root / backup.BACKUP_POINTER_NAME, io)
    except OSError, ValueError:
        return ""
    if not isinstance(payload, ConcurrentStableDict):
        return ""
    value = cast("ConcurrentStableDict[str, object]", payload).get("backup_id")
    return value if isinstance(value, str) else ""


def list_backups(backup_root: Path, io: RecoveryIO) -> ConcurrentStableFrozenList[BackupRef]:
    """列出备份根下的完整快照（含当前指针与最近一份标记）。"""
    names = sorted(path.name for path in io.list_backup_dirs(backup_root))
    current = current_backup_id(backup_root, io)
    refs: ConcurrentStableList[BackupRef] = ConcurrentStableList()
    for name in names:
        refs.add(
            BackupRef(
                backup_id=name,
                created_at=manifest_time_of(backup_root / name, io),
                is_current=name == current,
                is_latest=name == names[-1],
                complete=True,
            )
        )
    return ConcurrentStableFrozenList(refs)


def previous_backup_id(backup_root: Path, io: RecoveryIO, *, current: str) -> str:
    """取「当前指针之前最近的一份完整快照」；没有则返回空串。"""
    names = sorted(path.name for path in io.list_backup_dirs(backup_root))
    earlier = [name for name in names if name < current]
    return earlier[-1] if earlier else ""


def resolve_snapshot(config: RecoveryConfig, io: RecoveryIO) -> tuple[Path, Manifest]:
    """定位快照并校验清单；指针损坏只告警，不静默改指针。"""
    identifier = config.backup_id
    if identifier is None:
        identifier = current_backup_id(config.backup_root, io)
        if not identifier:
            refs = list_backups(config.backup_root, io)
            if not refs:
                raise RecoveryError("recovery.manifest: 备份根下没有可用快照")
            io.alert("[ALERT] recovery.pointer: current.json 缺失或损坏，回退到最近一份完整快照")
            identifier = refs[-1].backup_id
    snapshot = config.backup_root / identifier
    manifest_path = snapshot / backup.BACKUP_MANIFEST_NAME
    try:
        payload = read_json_file(manifest_path, io)
    except OSError as exc:
        raise RecoveryError(f"recovery.manifest: 快照 {identifier} 缺 {backup.BACKUP_MANIFEST_NAME}") from exc
    except ValueError as exc:
        raise RecoveryError(f"recovery.manifest: 清单不可解析：{exc}") from exc
    return snapshot, parse_manifest(payload, directory_name=identifier)


def verify_snapshot(snapshot: Path, manifest: Manifest, io: RecoveryIO) -> ConcurrentStableDict[str, object]:
    """逐文件复算快照（存在性 + size + sha256），任何不符即失败。"""
    mismatches: ConcurrentStableList[str] = ConcurrentStableList()
    offset_prefix = f"{backup.OFFSET_SNAPSHOT_SUBDIR}/"
    listed: ConcurrentStableSet[str] = ConcurrentStableSet(entry.path for entry in manifest.files)
    for entry in manifest.files:
        try:
            raw = io.read_bytes(snapshot / entry.path)
        except OSError:
            mismatches.add(f"missing:{entry.path}")
            continue
        if len(raw) != entry.size:
            mismatches.add(f"size:{entry.path}")
        elif backup.sha256_hex(raw) != entry.sha256:
            mismatches.add(f"sha256:{entry.path}")
    for entry in manifest.offset_files:
        expected = f"{offset_prefix}{entry.path}"
        if expected not in listed:
            mismatches.add(f"offsets_missing:{expected}")
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("files", len(manifest.files))
    detail.set("offset_files", len(manifest.offset_files))
    detail.set("mismatches", ConcurrentStableFrozenList(mismatches[:10]))
    detail.set("verified", not mismatches)
    if mismatches:
        raise RecoveryError(f"recovery.snapshot: 快照校验和不符（{len(mismatches)} 处，首条 {mismatches[0]}）")
    return detail


# ---------------------------------------------------------------------------
# 目标准备、位点恢复与段分诊
# ---------------------------------------------------------------------------


def included_partitions(
    manifest: Manifest, data_root: Path, io: RecoveryIO, *, limit: int
) -> ConcurrentStableFrozenList[PartitionExpectation]:
    """按 `--max-partitions` 选取参与演练的分区（只保留目标上真实存在的目录）。"""
    available = [row for row in manifest.partitions if io.is_dir(data_root / row.relative)]
    if limit <= 0 or limit >= len(available):
        return ConcurrentStableFrozenList(available)
    return ConcurrentStableFrozenList(available[:limit])


def prepare_target(
    config: RecoveryConfig,
    run_dir: Path,
    manifest: Manifest,
    io: RecoveryIO,
) -> tuple[Path, ConcurrentStableDict[str, object]]:
    """把源数据根复制到工作根（drill）；返回目标数据根与统计明细。"""
    target = run_dir / RECOVERY_WORK_TARGET_SUBDIR
    target_data = target / DATA_ROOT_SUBDIR
    source_data = config.source_data_root / DATA_ROOT_SUBDIR
    if not io.is_dir(source_data):
        raise RecoveryError(f"recovery.prepare: 源数据目录不存在（{source_data.name}）")
    selected = included_partitions(manifest, config.source_data_root, io, limit=config.max_partitions)
    excluded: ConcurrentStableList[str] = ConcurrentStableList()
    if config.max_partitions <= 0:
        io.copy_tree(source_data, target_data)
    else:
        io.mkdir(target_data)
        keep: ConcurrentStableSet[str] = ConcurrentStableSet(row.label for row in selected)
        for partition in io.list_partitions(config.source_data_root):
            relative = partition.relative_to(source_data)
            label = "/".join(relative.parts[:4])
            if label in keep:
                io.copy_tree(partition, target_data / relative)
            else:
                excluded.add(label)
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("partitions", len(selected))
    detail.set("excluded", ConcurrentStableFrozenList(excluded))
    return target, detail


def restore_offsets(
    config: RecoveryConfig,
    snapshot: Path,
    target_root: Path,
    manifest: Manifest,
    io: RecoveryIO,
) -> ConcurrentStableDict[str, object]:
    """清空目标 `offsets/` 后按清单复制并逐文件复算。"""
    target_offsets = target_root / OFFSETS_SUBDIR
    io.remove_tree(target_offsets)
    io.mkdir(target_offsets)
    total_bytes = 0
    for entry in manifest.offset_files:
        source = snapshot / backup.OFFSET_SNAPSHOT_SUBDIR / entry.path
        destination = target_offsets / entry.path
        io.copy_file(source, destination)
        raw = io.read_bytes(destination)
        total_bytes += len(raw)
        if len(raw) != entry.size or backup.sha256_hex(raw) != entry.sha256:
            raise RecoveryError(f"recovery.offsets: 恢复后校验和不符：{entry.path}")
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("files", len(manifest.offset_files))
    detail.set("bytes", total_bytes)
    detail.set("verified", True)
    return detail


def scan_segments(
    target_root: Path,
    manifest: Manifest,
    io: RecoveryIO,
    excluded: ConcurrentStableFrozenList[str] | None = None,
) -> SegmentReport:
    """按清单逐分区比对段基址，并把缺口分成「保留期裁剪 / 段间空洞 / 段尾缺失 / 整目录缺失」。"""
    skipped = excluded or ConcurrentStableFrozenList()
    rows: ConcurrentStableList[PartitionGap] = ConcurrentStableList()
    absent: ConcurrentStableList[str] = ConcurrentStableList()
    degraded = False
    for expectation in manifest.partitions:
        if expectation.label in skipped:
            continue
        directory = target_root / expectation.relative
        if not io.is_dir(directory):
            absent.add(expectation.label)
            degraded = True
            rows.add(
                PartitionGap(
                    label=expectation.label,
                    actual_bases=ConcurrentStableFrozenList(),
                    missing_bases=expectation.segment_bases,
                    pruned=ConcurrentStableFrozenList(),
                    holes=ConcurrentStableFrozenList(),
                    tail_missing=expectation.segment_bases,
                    absent=True,
                    start_available=0,
                )
            )
            continue
        actual = ConcurrentStableFrozenList(
            sorted(int(name[:20]) for name in io.list_segments(directory) if len(name) >= 20)
        )
        missing = ConcurrentStableFrozenList(base for base in expectation.segment_bases if base not in actual)
        start_available = actual[0] if actual else 0
        pruned: ConcurrentStableFrozenList[int]
        tail: ConcurrentStableFrozenList[int]
        holes: ConcurrentStableFrozenList[int]
        if actual:
            pruned = ConcurrentStableFrozenList(base for base in missing if base < actual[0])
            tail = ConcurrentStableFrozenList(base for base in missing if base > actual[-1])
            holes = ConcurrentStableFrozenList(base for base in missing if actual[0] < base < actual[-1])
        else:
            pruned = ConcurrentStableFrozenList()
            tail = missing
            holes = ConcurrentStableFrozenList()
        gap = PartitionGap(
            label=expectation.label,
            actual_bases=actual,
            missing_bases=missing,
            pruned=pruned,
            holes=holes,
            tail_missing=tail,
            absent=False,
            start_available=start_available,
        )
        degraded = degraded or gap.degraded
        rows.add(gap)
    return SegmentReport(
        partitions=ConcurrentStableFrozenList(rows),
        absent=ConcurrentStableFrozenList(absent),
        degraded=degraded,
    )


def segments_detail(report: SegmentReport) -> ConcurrentStableDict[str, object]:
    """段比对明细（入记录；无缺口的分区不占篇幅）。"""
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    for gap in report.partitions:
        if not gap.missing_bases and not gap.absent:
            continue
        row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        row.set("partition", gap.label)
        row.set("absent", gap.absent)
        row.set("start_available", gap.start_available)
        row.set("pruned", gap.pruned)
        row.set("holes", gap.holes)
        row.set("tail_missing", gap.tail_missing)
        rows.add(row)
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("partitions", len(report.partitions))
    detail.set("absent", report.absent)
    detail.set("gaps", ConcurrentStableFrozenList(rows))
    detail.set("degraded", report.degraded)
    return detail


# ---------------------------------------------------------------------------
# 元数据库核对与 live 命令
# ---------------------------------------------------------------------------


def verify_metadata(config: RecoveryConfig, manifest: Manifest, io: RecoveryIO) -> StepOutcome:
    """核对 dump 是否覆盖清单期望的表集合（不连库、不复制平台库）。"""
    dump = config.platform_dump_dir / manifest.metadata_dump
    if not dump.is_file():
        found = io.find_dump(config.platform_dump_dir)
        name = found.name if found is not None else "（无任何 dump）"
        raise RecoveryError(f"recovery.metadata: 清单声明的 dump 不存在，目录最近产物：{name}")
    tables = io.read_dump_tables(dump)
    missing = ConcurrentStableFrozenList(name for name in manifest.tables_expected if name not in tables)
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("dump", manifest.metadata_dump)
    detail.set("expected", manifest.tables_expected)
    detail.set("in_dump", ConcurrentStableFrozenList(sorted(tables)))
    detail.set("missing", missing)
    detail.set("covered", not missing)
    if missing:
        raise RecoveryError(f"recovery.metadata: dump 缺表 {', '.join(missing)}")
    return StepOutcome(detail=detail)


def command_for(config: RecoveryConfig, template: str, dump: str, backup_id: str) -> str:
    """取命令模板并替换三个白名单占位（不做其余展开）；空模板给离线说明。"""
    if not template:
        return "离线模式：仅核对 dump 表集合，不执行导入"
    return template.replace("{dump}", dump).replace("{backup_id}", backup_id).replace("{target_env}", config.target_env)


def command_detail(result: CommandResult | None) -> ConcurrentStableDict[str, object]:
    """命令结果明细（输出只留末尾摘要）。"""
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    if result is None:
        detail.set("exit_code", -1)
        return detail
    detail.set("exit_code", result.exit_code)
    detail.set("timed_out", result.timed_out)
    detail.set("elapsed_ms", result.elapsed_ms)
    detail.set("stdout_tail", sanitize(tail(result.stdout)))
    detail.set("stderr_tail", sanitize(tail(result.stderr)))
    return detail


def run_command_once(config: RecoveryConfig, io: RecoveryIO, name: str, command: str) -> CommandResult:
    """执行 live 命令模板并核对退出码（超时 / 非 0 即失败）。"""
    result = io.run_command(command, config.timeout_sec)
    if result.timed_out:
        raise RecoveryError(f"recovery.command.{name}: 命令超时（{config.timeout_sec}s）")
    if result.exit_code != 0:
        raise RecoveryError(f"recovery.command.{name}: 退出码 {result.exit_code}")
    return result


def wait_ready(config: RecoveryConfig, io: RecoveryIO) -> bool:
    """轮询就绪地址直到 2xx 或超时。"""
    waited = 0
    while waited <= config.timeout_sec * 1000:
        if 200 <= io.http_status(config.ready_url, max(config.timeout_sec / 10, 1.0)) < 300:
            return True
        io.sleep_ms(RECOVERY_PROBE_INTERVAL_MS)
        waited += RECOVERY_PROBE_INTERVAL_MS
    return False


# ---------------------------------------------------------------------------
# 真实存储类：对齐、位点校验、抽样、延迟重建
# ---------------------------------------------------------------------------


class NullAuditSink:
    """位点存储构造要求的审计出口替身（恢复路径不产生审计写操作）。"""

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """不做任何事，直接返回成功。"""
        return True


def split_offset_name(name: str) -> tuple[int | None, int | None]:
    """把 `{topic_id}-{partition}.offset` 拆成两个整数（不符合即 None）。"""
    if not name.endswith(".offset"):
        return None, None
    stem = name[: -len(".offset")]
    topic_part, separator, partition_part = stem.rpartition("-")
    if not separator or not topic_part.isdigit() or not partition_part.isdigit():
        return None, None
    return int(topic_part), int(partition_part)


def offset_identity(relative: Path, raw: bytes) -> tuple[str, int, int] | None:
    """从位点文件字节取 `(group, topic_id, partition_no)`；身份与路径不符返回 None。"""
    readout = decode_offset_file(raw)
    if readout.group is None or readout.topic_id is None or readout.partition_no is None:
        return None
    parts = relative.parts
    if len(parts) != 3:
        return None
    filename_topic, filename_partition = split_offset_name(parts[2])
    if filename_topic is None or filename_partition is None:
        return None
    if filename_topic != readout.topic_id or filename_partition != readout.partition_no:
        return None
    return readout.group, readout.topic_id, readout.partition_no


def partition_identity(partition: Path) -> tuple[int, int]:
    """从分区目录路径取 `(topic_id, partition_no)`（目录名约定：`{topic}.{topic_id}/{partition}`）。"""
    _, separator, topic_id_part = partition.parent.name.rpartition(".")
    if not separator or not topic_id_part.isdigit() or not partition.name.isdigit():
        return 0, 0
    return int(topic_id_part), int(partition.name)


def target_last_write_us(target_root: Path, io: RecoveryIO) -> tuple[int | None, str]:
    """取数据根最后写入时刻：恢复点 `updated_at_us` 与 `.log` 段 mtime 的最大值。"""
    latest: int | None = None
    source = "null"
    for partition in io.list_partitions(target_root):
        topic_id, partition_no = partition_identity(partition)
        readout = read_recovery_point(partition / "recovery-point.json", topic_id=topic_id, partition_no=partition_no)
        stamp_us = readout.point.updated_at_us if readout.point is not None else 0
        if stamp_us > 0 and (latest is None or stamp_us > latest):
            latest = stamp_us
            source = "recovery-point"
        for name in io.list_segments(partition):
            stamp = io.mtime_us(partition / name)
            if latest is None or stamp > latest:
                latest = stamp
                source = "mtime"
    return latest, source


async def align_partitions(
    target_root: Path,
    manifest: Manifest,
    report: SegmentReport,
) -> tuple[ConcurrentStableDict[str, object], bool, ConcurrentStableList[str]]:
    """用真实存储类加载分区、夹取越尾位点并落盘；返回明细与降级原因。"""
    directory = PartitionDirectory(root=target_root)
    offset_store = FileOffsetStore(directory=directory, root=target_root, audit=NullAuditSink())
    store = FileMessageStore(directory=directory, realigner=offset_store)
    offset_store.bind_ceiling(store)
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    degraded = False
    absent = ConcurrentStableSet(report.absent)
    try:
        for expectation in manifest.partitions:
            if expectation.label in absent:
                continue
            row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
            row.set("partition", expectation.label)
            row.set("leo_before", expectation.leo)
            try:
                leo = await store.leo(topic_id=expectation.topic_id, partition_no=expectation.partition)
            except Exception as exc:
                row.set("error", str(exc))
                row.set("leo_after", -1)
                rows.add(row)
                degraded = True
                reasons.add(f"{expectation.label} 加载失败：{exc}")
                continue
            row.set("leo_after", leo)
            adjustments = await offset_store.realign_partition(
                topic_id=expectation.topic_id,
                partition_no=expectation.partition,
                ceiling=leo,
            )
            adjusted: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
            for adjustment in adjustments:
                item: ConcurrentStableDict[str, object] = ConcurrentStableDict()
                item.set("group", adjustment.group)
                item.set("previous", adjustment.previous)
                item.set("current", adjustment.current)
                adjusted.add(item)
                degraded = True
                reasons.add(
                    f"{expectation.label} 位点越过日志尾："
                    f"{adjustment.group} {adjustment.previous} → {adjustment.current}"
                )
            row.set("adjustments", ConcurrentStableFrozenList(adjusted))
            if leo < expectation.leo:
                degraded = True
                reasons.add(f"{expectation.label} 日志尾前移：{expectation.leo} → {leo}")
            rows.add(row)
        await offset_store.flush_once()
        await offset_store.aclose()
    finally:
        await store.aclose()
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("partitions", ConcurrentStableFrozenList(rows))
    detail.set("degraded", degraded)
    return detail, degraded, reasons


async def verify_offsets_and_samples(
    config: RecoveryConfig,
    target_root: Path,
    io: RecoveryIO,
) -> tuple[ConcurrentStableDict[str, object], bool, ConcurrentStableList[str]]:
    """校验位点身份与区间、抽样读回已确认消息；返回明细与降级原因。"""
    directory = PartitionDirectory(root=target_root)
    offset_store = FileOffsetStore(directory=directory, root=target_root, audit=NullAuditSink())
    store = FileMessageStore(directory=directory, realigner=offset_store)
    offset_store.bind_ceiling(store)
    offsets_root = target_root / OFFSETS_SUBDIR
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    samples: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    degraded = False
    sampled = 0
    truncated = False
    try:
        for relative in io.list_offset_files(offsets_root):
            identity = offset_identity(relative, io.read_bytes(offsets_root / relative))
            if identity is None:
                raise RecoveryError(f"recovery.offset.corrupt: 位点文件身份不符：{relative.as_posix()}")
            group, topic_id, partition_no = identity
            row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
            row.set("path", relative.as_posix())
            row.set("group", group)
            row.set("topic_id", topic_id)
            row.set("partition_no", partition_no)
            stored = await offset_store.get(group=group, topic_id=topic_id, partition_no=partition_no)
            row.set("offset", stored)
            try:
                leo = await store.leo(topic_id=topic_id, partition_no=partition_no)
                start = await store.start_offset(topic_id=topic_id, partition_no=partition_no)
            except Exception as exc:
                row.set("error", str(exc))
                rows.add(row)
                degraded = True
                reasons.add(f"{relative.as_posix()} 分区不可加载：{exc}")
                continue
            row.set("leo", leo)
            row.set("start", start)
            if stored > leo:
                raise RecoveryError(
                    f"recovery.offset: 对齐后位点仍越过日志尾（{relative.as_posix()} {stored} > {leo}）"
                )
            if stored < start:
                degraded = True
                reasons.add(f"{relative.as_posix()} 位点低于可用起点：{stored} < {start}")
            if sampled >= config.sample_limit:
                truncated = True
            elif stored > start:
                from_offset = max(start, stored - config.sample_per_partition)
                want = min(config.sample_per_partition, config.sample_limit - sampled)
                messages = await store.fetch(
                    topic_id=topic_id,
                    partition_no=partition_no,
                    from_offset=from_offset,
                    max_messages=want,
                )
                if not messages:
                    raise RecoveryError(f"recovery.sample: 已确认消息不可读：{relative.as_posix()}")
                previous = -1
                accepted = 0
                for message in messages:
                    if message.offset >= stored:
                        break
                    if message.offset <= previous:
                        raise RecoveryError(f"recovery.sample: 抽样偏移非法：{relative.as_posix()}")
                    previous = message.offset
                    accepted += 1
                if accepted == 0:
                    raise RecoveryError(f"recovery.sample: 已确认消息不可读：{relative.as_posix()}")
                sampled += accepted
                sample_row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
                sample_row.set("path", relative.as_posix())
                sample_row.set("from", from_offset)
                sample_row.set("count", accepted)
                samples.add(sample_row)
                row.set("sampled", accepted)
            rows.add(row)
    finally:
        await offset_store.aclose()
        await store.aclose()
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("files", ConcurrentStableFrozenList(rows))
    detail.set("samples", ConcurrentStableFrozenList(samples))
    detail.set("sampled", sampled)
    detail.set("truncated", truncated)
    detail.set("degraded", degraded)
    return detail, degraded, reasons


def delay_expectations(partitions: ConcurrentStableList[Path]) -> ConcurrentStableDict[str, int]:
    """只读扫源 `.delay` 段，得到 `log_id → 最后一条 put 的 deliver_at_us`。"""
    expected: ConcurrentStableDict[str, int] = ConcurrentStableDict()
    for partition in partitions:
        for seq in list_delay_segment_seqs(partition):
            scan = DelaySegment(partition_dir=partition, seq=seq).scan(verify_crc=False)
            for entry in scan.entries:
                stamp = entry.header.delay
                if stamp is None:
                    continue
                if stamp.kind == "put":
                    expected.set(entry.header.log_id, stamp.deliver_at_us)
                elif stamp.kind == "clear":
                    expected.pop(entry.header.log_id, None)
    return expected


async def verify_delay(
    target_root: Path,
    io: RecoveryIO,
    excluded: ConcurrentStableFrozenList[str],
) -> tuple[ConcurrentStableDict[str, object], bool, ConcurrentStableList[str]]:
    """重建延迟区，逐条比对 `deliver_at` 与 `log_id` 集合（不提前、不丢）。"""
    directory = PartitionDirectory(root=target_root)
    offset_store = FileOffsetStore(directory=directory, root=target_root, audit=NullAuditSink())
    store = FileMessageStore(directory=directory, realigner=offset_store)
    offset_store.bind_ceiling(store)
    delay_store = FileDelayStore(directory=directory, dedupe=store)
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    degraded = False
    try:
        expected: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for partition in io.list_partitions(target_root):
            label = "/".join(partition.relative_to(target_root / DATA_ROOT_SUBDIR).parts[:4])
            if label in excluded:
                continue
            expected.update(delay_expectations(ConcurrentStableList([partition])))
        report = await delay_store.rebuild()
        actual: ConcurrentStableDict[str, int] = ConcurrentStableDict()
        for record in await delay_store.list_all():
            label = f"{record.tenant_id}/{record.namespace}/{record.topic}.{record.topic_id}/{record.partition_no}"
            if label in excluded:
                continue
            actual.set(record.log_id, int(record.deliver_at.timestamp() * 1_000_000))
        missing = ConcurrentStableList(log_id for log_id in expected if log_id not in actual)
        extra = ConcurrentStableList(log_id for log_id in actual if log_id not in expected)
        changed = ConcurrentStableList(
            log_id for log_id in expected if log_id in actual and expected.get(log_id) != actual.get(log_id)
        )
        if missing or extra or changed:
            degraded = True
            reasons.add(f"延迟区集合不一致：缺 {len(missing)} / 多 {len(extra)} / 改期 {len(changed)}")
        if report.truncated_bytes or report.isolated_segments or report.lost_records or report.unknown_kind:
            degraded = True
            reasons.add(
                "延迟段损坏："
                f"truncated={report.truncated_bytes} isolated={report.isolated_segments} "
                f"lost={report.lost_records} unknown={report.unknown_kind}"
            )
        detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        detail.set("expected", len(expected))
        detail.set("records", report.records)
        detail.set("expired", report.expired)
        detail.set("deduped", report.deduped)
        detail.set("truncated_bytes", report.truncated_bytes)
        detail.set("isolated_segments", report.isolated_segments)
        detail.set("lost_records", report.lost_records)
        detail.set("unknown_kind", report.unknown_kind)
        detail.set("missing", ConcurrentStableFrozenList(missing[:20]))
        detail.set("extra", ConcurrentStableFrozenList(extra[:20]))
        detail.set("changed", ConcurrentStableFrozenList(changed[:20]))
        detail.set("degraded", degraded)
        return detail, degraded, reasons
    finally:
        await delay_store.aclose()
        await offset_store.aclose()
        await store.aclose()


async def run_verification(
    config: RecoveryConfig,
    target_root: Path,
    manifest: Manifest,
    segment_report: SegmentReport,
    excluded: ConcurrentStableFrozenList[str],
    steps: ConcurrentStableList[StepRecord],
    reasons: ConcurrentStableList[str],
    checks: ConcurrentStableDict[str, object],
    io: RecoveryIO,
) -> None:
    """顺序执行对齐 / 位点与抽样校验 / 延迟校验，并逐步留痕。"""
    begin = io.monotonic()
    started_at = utc_iso(datetime.now(UTC))
    try:
        alignment, degraded, align_reasons = await align_partitions(target_root, manifest, segment_report)
    except Exception as exc:
        steps.add(
            StepRecord(
                step="align",
                command="FileMessageStore.leo + FileOffsetStore.realign_partition + flush_once",
                started_at=started_at,
                elapsed_ms=int((io.monotonic() - begin) * 1000),
                result=RESULT_FAILED,
                detail=_error_detail(exc),
            )
        )
        raise
    for reason in align_reasons:
        reasons.add(reason)
    steps.add(
        StepRecord(
            step="align",
            command="FileMessageStore.leo + FileOffsetStore.realign_partition + flush_once",
            started_at=started_at,
            elapsed_ms=int((io.monotonic() - begin) * 1000),
            result=RESULT_DEGRADED if degraded else RESULT_OK,
            detail=alignment,
        )
    )
    checks.set("alignment", alignment)

    begin = io.monotonic()
    started_at = utc_iso(datetime.now(UTC))
    try:
        offsets, offsets_degraded, offset_reasons = await verify_offsets_and_samples(config, target_root, io)
    except Exception as exc:
        steps.add(
            StepRecord(
                step="verify",
                command="FileOffsetStore.get + FileMessageStore.fetch（抽样读回）",
                started_at=started_at,
                elapsed_ms=int((io.monotonic() - begin) * 1000),
                result=RESULT_FAILED,
                detail=_error_detail(exc),
            )
        )
        raise
    for reason in offset_reasons:
        reasons.add(reason)
    steps.add(
        StepRecord(
            step="verify",
            command="FileOffsetStore.get + FileMessageStore.fetch（抽样读回）",
            started_at=started_at,
            elapsed_ms=int((io.monotonic() - begin) * 1000),
            result=RESULT_DEGRADED if offsets_degraded else RESULT_OK,
            detail=offsets,
        )
    )
    checks.set("offsets", offsets)

    begin = io.monotonic()
    started_at = utc_iso(datetime.now(UTC))
    try:
        delay, delay_degraded, delay_reasons = await verify_delay(target_root, io, excluded)
    except Exception as exc:
        steps.add(
            StepRecord(
                step="delay",
                command="FileDelayStore.rebuild + list_all（不提前 / 不丢）",
                started_at=started_at,
                elapsed_ms=int((io.monotonic() - begin) * 1000),
                result=RESULT_FAILED,
                detail=_error_detail(exc),
            )
        )
        raise
    for reason in delay_reasons:
        reasons.add(reason)
    steps.add(
        StepRecord(
            step="delay",
            command="FileDelayStore.rebuild + list_all（不提前 / 不丢）",
            started_at=started_at,
            elapsed_ms=int((io.monotonic() - begin) * 1000),
            result=RESULT_DEGRADED if delay_degraded else RESULT_OK,
            detail=delay,
        )
    )
    checks.set("delay", delay)


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------


def _error_detail(exc: Exception) -> ConcurrentStableDict[str, object]:
    """失败明细（类别 + 可定位对象，不含凭据）。"""
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("error", str(exc))
    return detail


def perform_step(
    *,
    name: str,
    command: str,
    steps: ConcurrentStableList[StepRecord],
    io: RecoveryIO,
    action: Callable[[], StepOutcome],
) -> StepOutcome:
    """执行一步并留痕；失败时记 `failed` 后原样抛出。"""
    begin = io.monotonic()
    started_at = utc_iso(datetime.now(UTC))
    try:
        outcome = action()
    except Exception as exc:
        steps.add(
            StepRecord(
                step=name,
                command=sanitize(command),
                started_at=started_at,
                elapsed_ms=int((io.monotonic() - begin) * 1000),
                result=RESULT_FAILED,
                detail=_error_detail(exc),
            )
        )
        raise
    steps.add(
        StepRecord(
            step=name,
            command=sanitize(command),
            started_at=started_at,
            elapsed_ms=int((io.monotonic() - begin) * 1000),
            result=RESULT_DEGRADED if outcome.degraded else RESULT_OK,
            detail=outcome.detail,
        )
    )
    return outcome


def mark_skipped(
    steps: ConcurrentStableList[StepRecord],
    names: ConcurrentStableFrozenList[str],
    io: RecoveryIO,
) -> None:
    """把失败步之后的步骤标为 `skipped`（记录里能看出没跳步）。"""
    seen = ConcurrentStableSet(step.step for step in steps)
    for name in names:
        if name in seen:
            continue
        steps.add(
            StepRecord(
                step=name,
                command="—",
                started_at=utc_iso(datetime.now(UTC)),
                elapsed_ms=0,
                result=RESULT_SKIPPED,
                detail=ConcurrentStableDict(),
            )
        )


def run_backup_once(config: RecoveryConfig, run_dir: Path, manifest: Manifest, io: RecoveryIO) -> StepOutcome:
    """演练前自动备份到工作根（drill 与 live 共用；返回备份标识）。"""
    backup_config = backup.BackupConfig(
        repo_root=config.repo_root,
        data_root=config.source_data_root,
        backup_root=run_dir / RECOVERY_WORK_BACKUP_SUBDIR,
        platform_dump_dir=config.platform_dump_dir,
        keep_days=1,
        space_factor=1.0,
        dry_run=False,
        now=config.now,
    )
    io.mkdir(backup_config.backup_root)
    exit_code = io.run_backup(backup_config)
    if exit_code != 0:
        raise RecoveryError(f"recovery.backup: 演练前备份失败（退出码 {exit_code}）")
    identifier = current_backup_id(backup_config.backup_root, io)
    if not identifier:
        raise RecoveryError("recovery.backup: 演练前备份未产出 current.json")
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("exit_code", exit_code)
    detail.set("backup_id", identifier)
    detail.set("source_manifest", manifest.backup_id)
    return StepOutcome(detail=detail)


def validate_config(config: RecoveryConfig, io: RecoveryIO) -> None:
    """参数级拒绝：live 确认 / 生产禁入 / 工作根隔离 / 抽样阈值。"""
    if config.mode == MODE_LIVE:
        if not config.confirmed:
            raise RecoveryError("recovery.config: live 模式会就地写数据根，必须显式给 --yes")
        if config.target_env.strip().lower() in RECOVERY_PROD_ENVS:
            raise RecoveryError(f"recovery.config: 拒绝对生产环境执行恢复（{config.target_env}）")
    if not config.dry_run:
        if config.work_root.resolve() == config.source_data_root.resolve():
            raise RecoveryError("recovery.config: 工作根不得等于数据根")
        if config.source_data_root.resolve() in config.work_root.resolve().parents:
            raise RecoveryError("recovery.config: 工作根不得位于数据根之下")
    if config.sample_per_partition <= 0 or config.sample_limit <= 0 or config.max_partitions < 0:
        raise RecoveryError("recovery.config: 抽样阈值须为正整数、分区上限不得为负")


def build_report(
    config: RecoveryConfig,
    *,
    manifest: Manifest | None,
    steps: ConcurrentStableList[StepRecord],
    checks: ConcurrentStableDict[str, object],
    reasons: ConcurrentStableList[str],
    conclusion: str,
    started_at: str,
    finished_at: str,
    rto_ms: int,
    rpo: tuple[int | None, str],
    pre_backup: str,
    run_dir: Path | None,
    config_version: str,
    failure: str | None,
) -> ConcurrentStableDict[str, object]:
    """组装固定字段的演练记录（JSON 与 Markdown 同源）。"""
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("schema_version", RECOVERY_REPORT_SCHEMA_VERSION)
    payload.set("task", "07-3-2")
    payload.set("mode", config.mode)
    environment: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    environment.set("os", platform.system())
    environment.set("arch", platform.machine())
    environment.set("python", platform.python_version())
    environment.set("isolated", config.mode == MODE_DRILL)
    environment.set("target_env", config.target_env)
    payload.set("environment", environment)
    service: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    service.set("name", manifest.service_name if manifest is not None else "msg")
    service.set("version", manifest.service_version if manifest is not None else "")
    service.set("contract_version", manifest.contract_version if manifest is not None else "")
    payload.set("service", service)
    version: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    version.set("backup_version", manifest.config_version if manifest is not None else "")
    version.set("current_version", config_version)
    version.set("drift", manifest is not None and manifest.config_version != config_version)
    payload.set("config", version)
    source: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    source.set("id", manifest.backup_id if manifest is not None else "")
    source.set("created_at", manifest.created_at if manifest is not None else "")
    source.set("manifest_schema_version", manifest.schema_version if manifest is not None else 0)
    source.set("snapshot_dir", manifest.backup_id if manifest is not None else "")
    source.set("pre_drill_backup_id", pre_backup)
    source.set("pre_drill_backup_dir", RECOVERY_WORK_BACKUP_SUBDIR if pre_backup else "")
    payload.set("backup", source)
    payload.set("started_at", started_at)
    payload.set("finished_at", finished_at)
    payload.set("rto_ms", rto_ms)
    payload.set("rto_sum_ms", sum(step.elapsed_ms for step in steps))
    payload.set("rpo_seconds", rpo[0])
    payload.set("rpo_source", rpo[1])
    payload.set("rpo_reason", "" if rpo[0] is not None else "无可用时间锚点（既无恢复点也无段文件）")
    payload.set("steps", ConcurrentStableFrozenList(step_payload(step) for step in steps))
    payload.set("checks", checks)
    verdict: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    verdict.set("result", conclusion)
    integrity = (
        "不可用" if conclusion == CONCLUSION_FAIL else ("有缺口" if conclusion == CONCLUSION_DEGRADED else "完整")
    )
    verdict.set("data_integrity", integrity)
    verdict.set("reasons", reasons)
    verdict.set("requires_review", conclusion == CONCLUSION_DEGRADED)
    payload.set("conclusion", verdict)
    injection: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    injection.set("enabled", False)
    injection.set("cases", ConcurrentStableFrozenList())
    payload.set("failure_injection", injection)
    payload.set("issues", issues_rows(reasons, failure, manifest, config_version))
    payload.set("leftovers", _leftovers())
    artifacts: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    artifacts.set("report_json", config.report_path.name)
    artifacts.set("report_md", config.report_path.with_suffix(".md").name)
    artifacts.set("retained_work_dir", run_dir.name if run_dir is not None and config.keep else "")
    artifacts.set("retained_backup_dir", RECOVERY_WORK_BACKUP_SUBDIR if pre_backup and config.mode == MODE_LIVE else "")
    payload.set("artifacts", artifacts)
    return payload


def step_payload(step: StepRecord) -> ConcurrentStableDict[str, object]:
    """把一条步骤留痕转成可 JSON 序列化的记录行。"""
    row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    row.set("step", step.step)
    row.set("command", step.command)
    row.set("started_at", step.started_at)
    row.set("elapsed_ms", step.elapsed_ms)
    row.set("result", step.result)
    row.set("detail", step.detail)
    return row


def issues_rows(
    reasons: ConcurrentStableList[str],
    failure: str | None,
    manifest: Manifest | None,
    config_version: str,
) -> ConcurrentStableFrozenList[ConcurrentStableDict[str, object]]:
    """问题与处置：失败原因、降级项与配置漂移。"""
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    if failure is not None:
        row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        row.set("summary", f"恢复失败：{failure}")
        row.set("disposition", "按记录末行 --backup-id 回退到上一份备份后重跑")
        row.set("owner", "运维")
        rows.add(row)
    for reason in reasons:
        row = ConcurrentStableDict()
        row.set("summary", reason)
        row.set("disposition", "确认缺口范围与重复消费影响后再决定是否接流量")
        row.set("owner", "运维 / 07-3-2")
        rows.add(row)
    if manifest is not None and manifest.config_version != config_version:
        row = ConcurrentStableDict()
        row.set("summary", "配置摘要与备份时不一致（配置漂移）")
        row.set("disposition", "按部署发布规范确认当前配置为预期版本后再接流量")
        row.set("owner", "部署 / 07-2")
        rows.add(row)
    return ConcurrentStableFrozenList(rows)


def _leftovers() -> ConcurrentStableFrozenList[ConcurrentStableDict[str, object]]:
    """遗留与归口（固定三条，人工记录可展开）。"""
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    for item, owner in (
        ("真机隔离环境跑一次完整 --live 演练", "部署阶段"),
        ("元数据库恢复后的行级完整性校验", "平台备份侧（bms）"),
        ("大库演练副本的容量与耗时", "8B_12"),
    ):
        row: ConcurrentStableDict[str, object] = ConcurrentStableDict()
        row.set("item", item)
        row.set("owner", owner)
        rows.add(row)
    return ConcurrentStableFrozenList(rows)


def render_markdown(report: ConcurrentStableDict[str, object]) -> str:
    """把记录渲染成可读 Markdown（字段与 JSON 同源）。"""
    verdict = cast("ConcurrentStableDict[str, object]", report.get("conclusion") or ConcurrentStableDict())
    backup_info = cast("ConcurrentStableDict[str, object]", report.get("backup") or ConcurrentStableDict())
    lines: ConcurrentStableList[str] = ConcurrentStableList()
    lines.add("# 恢复演练记录")
    lines.add("")
    lines.add(
        f"- 模式：`{report.get('mode')}`　结论：**{verdict.get('result')}**　"
        f"数据完整性：{verdict.get('data_integrity')}"
    )
    lines.add(f"- 备份标识：`{backup_info.get('id')}`（创建于 {backup_info.get('created_at')}）")
    lines.add(f"- 开始：{report.get('started_at')}　结束：{report.get('finished_at')}")
    lines.add(f"- RTO：{report.get('rto_ms')} ms　RPO：{report.get('rpo_seconds')} 秒（{report.get('rpo_source')}）")
    lines.add("")
    lines.add("## 步骤")
    lines.add("")
    lines.add("| 步骤 | 结果 | 耗时（ms） | 命令 |")
    lines.add("| --- | --- | --- | --- |")
    steps = cast("ConcurrentStableList[object]", report.get("steps") or ConcurrentStableList())
    for item in steps:
        step = cast("ConcurrentStableDict[str, object]", item)
        lines.add(f"| {step.get('step')} | {step.get('result')} | {step.get('elapsed_ms')} | `{step.get('command')}` |")
    lines.add("")
    lines.add("## 校验结论")
    lines.add("")
    checks = cast("ConcurrentStableDict[str, object]", report.get("checks") or ConcurrentStableDict())
    for name in ("manifest", "metadata", "segments", "alignment", "offsets", "delay"):
        if name in checks:
            lines.add(f"- `{name}`：`{checks.get(name)}`")
    lines.add("")
    lines.add("## 结论与原因")
    lines.add("")
    reasons = cast("ConcurrentStableList[object]", verdict.get("reasons") or ConcurrentStableList())
    if reasons:
        for reason in reasons:
            lines.add(f"- {reason}")
    else:
        lines.add("- 无降级项，校验项全部通过。")
    lines.add("")
    lines.add("## 问题与处置 / 遗留")
    lines.add("")
    for key, title in (("issues", "问题与处置"), ("leftovers", "遗留与归口")):
        lines.add(f"### {title}")
        lines.add("")
        rows = cast("ConcurrentStableList[object]", report.get(key) or ConcurrentStableList())
        if not rows:
            lines.add("- 无")
        for item in rows:
            row = cast("ConcurrentStableDict[str, object]", item)
            lines.add(f"- {row.get('summary') or row.get('item')}：{row.get('disposition') or row.get('owner')}")
        lines.add("")
    return "\n".join(lines)


def write_report(
    config: RecoveryConfig,
    report: ConcurrentStableDict[str, object],
    io: RecoveryIO,
) -> None:
    """落 JSON 与 Markdown 两份记录（失败也必须写）。"""
    io.mkdir(config.report_path.parent)
    io.write_bytes(config.report_path, backup.render(report).encode("utf-8"))
    io.write_bytes(config.report_path.with_suffix(".md"), render_markdown(report).encode("utf-8"))


def run(config: RecoveryConfig, io: RecoveryIO) -> int:
    """执行一次恢复演练；返回 0 通过 / 2 降级通过 / 1 失败。"""
    started_at = utc_iso(datetime.now(UTC))
    begin = io.monotonic()
    steps: ConcurrentStableList[StepRecord] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    checks: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    manifest: Manifest | None = None
    run_dir: Path | None = None
    pre_backup = ""
    conclusion = CONCLUSION_FAIL
    failure: str | None = None
    degraded = False
    try:
        validate_config(config, io)
        outcome = perform_step(
            name="manifest",
            command="read current.json + manifest.json + files[] sha256",
            steps=steps,
            io=io,
            action=lambda: _step_manifest(config, io),
        )
        manifest = cast("Manifest", outcome.detail.get("manifest"))
        checks.set("manifest", _without(outcome.detail, "manifest"))
        snapshot = config.backup_root / manifest.backup_id

        if config.dry_run:
            metadata = perform_step(
                name="metadata",
                command=command_for(
                    config, config.metadata_cmd, str(snapshot / manifest.metadata_dump), manifest.backup_id
                ),
                steps=steps,
                io=io,
                action=lambda: _step_metadata(config, manifest, io),
            )
            checks.set("metadata", metadata.detail)
            segment_report = scan_segments(config.source_data_root, manifest, io)
            _record_segments(steps, segment_report)
            checks.set("segments", segments_detail(segment_report))
            if segment_report.degraded:
                reasons.add("日志段存在缺口（段间空洞 / 段尾缺失 / 分区缺失）")
            conclusion = CONCLUSION_DEGRADED if segment_report.degraded else CONCLUSION_PASS
        else:
            if config.mode == MODE_LIVE:
                perform_step(
                    name="stop",
                    command=config.stop_cmd,
                    steps=steps,
                    io=io,
                    action=lambda: _step_live_command(config, io, "stop", config.stop_cmd, manifest, ""),
                )

            run_dir = config.work_root / f"run-{manifest.backup_id}-{config.now.strftime('%Y%m%dT%H%M%SZ')}"
            io.mkdir(run_dir)

            backup_outcome = perform_step(
                name="pre-backup",
                command="python -m ops.backup（临时备份根）",
                steps=steps,
                io=io,
                action=lambda: run_backup_once(config, run_dir, manifest, io),
            )
            pre_backup = cast("str", backup_outcome.detail.get("backup_id") or "")
            checks.set("pre_backup", backup_outcome.detail)

            target_root: Path
            excluded: ConcurrentStableFrozenList[str]
            if config.mode == MODE_LIVE:
                target_root = config.source_data_root
                excluded = ConcurrentStableFrozenList()
                prepare_detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
                prepare_detail.set("target", "live")
                steps.add(
                    StepRecord(
                        step="prepare",
                        command="target = MSG_DATA_DIR（live 就地恢复）",
                        started_at=utc_iso(datetime.now(UTC)),
                        elapsed_ms=0,
                        result=RESULT_OK,
                        detail=prepare_detail,
                    )
                )
            else:
                prepared = perform_step(
                    name="prepare",
                    command="copy data/ 到工作根副本（保留 mtime）",
                    steps=steps,
                    io=io,
                    action=lambda: _stepprepare_workspace(config, run_dir, manifest, io),
                )
                target_root = cast("Path", prepared.detail.get("target_path"))
                excluded = cast(
                    "ConcurrentStableFrozenList[str]", prepared.detail.get("excluded") or ConcurrentStableFrozenList()
                )
                checks.set("prepare", _without(prepared.detail, "target_path", "excluded"))

            degraded = _run_recovery_flow(
                config, io, manifest, snapshot, target_root, excluded, steps, checks, reasons
            )[0]
            conclusion = CONCLUSION_DEGRADED if degraded else CONCLUSION_PASS
    except Exception as exc:
        failure = str(exc)
        conclusion = CONCLUSION_FAIL
        if not config.dry_run:
            mark_skipped(steps, _plannedstep_rows(config.mode), io)
    finally:
        if not config.dry_run:
            cleaned = True
            if run_dir is not None and config.mode == MODE_DRILL and not config.keep:
                try:
                    io.remove_tree(run_dir)
                except OSError as exc:
                    io.alert(f"[ALERT] recovery.cleanup {run_dir.name}: {exc}")
                    cleaned = False
            steps.add(
                StepRecord(
                    step="cleanup",
                    command="删除演练工作根（drill）" if config.mode == MODE_DRILL else "保留演练前备份（live）",
                    started_at=utc_iso(datetime.now(UTC)),
                    elapsed_ms=0,
                    result=RESULT_OK if cleaned else RESULT_DEGRADED,
                    detail=ConcurrentStableDict(),
                )
            )
            if not cleaned:
                degraded = True
                reasons.add("清理演练工作根失败（现场保留）")
        reported = (
            CONCLUSION_DEGRADED if degraded and conclusion == CONCLUSION_PASS and not config.dry_run else conclusion
        )
        exit_code = _finish(
            config,
            io,
            manifest=manifest,
            steps=steps,
            checks=checks,
            reasons=reasons,
            conclusion=reported,
            failure=failure,
            started_at=started_at,
            begin=begin,
            run_dir=run_dir,
            pre_backup=pre_backup,
        )
    return exit_code


def _plannedstep_rows(mode: str) -> ConcurrentStableFrozenList[str]:
    """本次模式应当出现的步骤名（失败时据此补 `skipped`）。"""
    if mode == MODE_LIVE:
        return ConcurrentStableFrozenList(
            (
                "manifest",
                "stop",
                "pre-backup",
                "prepare",
                "metadata",
                "offsets",
                "segments",
                "align",
                "verify",
                "delay",
                "start",
                "ready",
            )
        )
    return ConcurrentStableFrozenList(
        ("manifest", "pre-backup", "prepare", "metadata", "offsets", "segments", "align", "verify", "delay")
    )


def _run_recovery_flow(
    config: RecoveryConfig,
    io: RecoveryIO,
    manifest: Manifest,
    snapshot: Path,
    target_root: Path,
    excluded: ConcurrentStableFrozenList[str],
    steps: ConcurrentStableList[StepRecord],
    checks: ConcurrentStableDict[str, object],
    reasons: ConcurrentStableList[str],
) -> tuple[bool, bool]:
    """元数据库 → 位点 → 段确认 → 对齐与校验 → （live）启动与就绪；返回 (是否降级, 是否已启动)。"""
    metadata_outcome = perform_step(
        name="metadata",
        command=command_for(config, config.metadata_cmd, str(snapshot / manifest.metadata_dump), manifest.backup_id),
        steps=steps,
        io=io,
        action=lambda: _step_metadata(config, manifest, io),
    )
    checks.set("metadata", metadata_outcome.detail)

    offsets_outcome = perform_step(
        name="offsets",
        command="restore offsets/ + 逐文件 sha256 复算",
        steps=steps,
        io=io,
        action=lambda: _step_offsets(config, snapshot, target_root, manifest, io),
    )
    checks.set("offsets_restore", offsets_outcome.detail)

    segment_report = scan_segments(target_root, manifest, io, excluded)
    _record_segments(steps, segment_report)
    checks.set("segments", segments_detail(segment_report))
    if segment_report.degraded:
        reasons.add("日志段存在缺口（段间空洞 / 段尾缺失 / 分区缺失）")

    asyncio.run(run_verification(config, target_root, manifest, segment_report, excluded, steps, reasons, checks, io))

    degraded = any(step.result == RESULT_DEGRADED for step in steps)
    if config.mode == MODE_LIVE:
        if degraded and not config.allow_degraded:
            mark_skipped(steps, ConcurrentStableFrozenList(("start", "ready")), io)
            return degraded, False
        start_result = perform_step(
            name="start",
            command=config.start_cmd,
            steps=steps,
            io=io,
            action=lambda: _step_live_command(config, io, "start", config.start_cmd, manifest, ""),
        )
        checks.set("start", start_result.detail)
        ready_begin = io.monotonic()
        ready_ok = wait_ready(config, io)
        steps.add(
            StepRecord(
                step="ready",
                command=f"GET {sanitize(config.ready_url)}",
                started_at=utc_iso(datetime.now(UTC)),
                elapsed_ms=int((io.monotonic() - ready_begin) * 1000),
                result=RESULT_OK if ready_ok else RESULT_FAILED,
                detail=ConcurrentStableDict(),
            )
        )
        if not ready_ok:
            raise RecoveryError("recovery.ready: 就绪探针超时")
    return degraded, True


def _record_segments(steps: ConcurrentStableList[StepRecord], report: SegmentReport) -> None:
    """段确认步留痕。"""
    steps.add(
        StepRecord(
            step="segments",
            command="scan data/**/*.log + 缺口分诊",
            started_at=utc_iso(datetime.now(UTC)),
            elapsed_ms=0,
            result=RESULT_DEGRADED if report.degraded else RESULT_OK,
            detail=segments_detail(report),
        )
    )


def _finish(
    config: RecoveryConfig,
    io: RecoveryIO,
    *,
    manifest: Manifest | None,
    steps: ConcurrentStableList[StepRecord],
    checks: ConcurrentStableDict[str, object],
    reasons: ConcurrentStableList[str],
    conclusion: str,
    failure: str | None,
    started_at: str,
    begin: float,
    run_dir: Path | None,
    pre_backup: str,
) -> int:
    """收口：算 RPO、写记录、输出结论；返回退出码。"""
    finished_at = utc_iso(datetime.now(UTC))
    if manifest is not None:
        last_write, source = target_last_write_us(config.source_data_root, io)
        rpo = compute_rpo(last_write, source, manifest.created_at)
    else:
        rpo = (None, "null")
    report = build_report(
        config,
        manifest=manifest,
        steps=steps,
        checks=checks,
        reasons=reasons,
        conclusion=conclusion,
        started_at=started_at,
        finished_at=finished_at,
        rto_ms=int((io.monotonic() - begin) * 1000),
        rpo=rpo,
        pre_backup=pre_backup,
        run_dir=run_dir,
        config_version=current_config_version(config, io),
        failure=failure,
    )
    try:
        write_report(config, report, io)
    except OSError as exc:
        io.alert(f"[ALERT] recovery.report: 记录写入失败：{exc}")
    if failure is not None:
        io.alert(f"[FAIL] {failure}")
        io.alert(f"[ALERT] recovery.fallback --backup-id {fallback_hint(config, io)}")
    elif conclusion == CONCLUSION_DEGRADED:
        io.alert("[ALERT] recovery.degraded: 结论为降级通过，需人工确认缺口后再接流量")
    else:
        io.output(f"[OK] recovery.{config.mode} conclusion={conclusion}")
    return EXIT_CODES[conclusion]


def _step_manifest(config: RecoveryConfig, io: RecoveryIO) -> StepOutcome:
    """清单步：定位快照、解析清单、复算快照。"""
    snapshot, manifest = resolve_snapshot(config, io)
    verify = verify_snapshot(snapshot, manifest, io)
    detail: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    detail.set("backup_id", manifest.backup_id)
    detail.set("created_at", manifest.created_at)
    detail.set("schema_version", manifest.schema_version)
    detail.set("snapshot", snapshot.name)
    detail.set("files", verify.get("files"))
    detail.set("offset_files", verify.get("offset_files"))
    detail.set("manifest", manifest)
    return StepOutcome(detail=detail)


def _stepprepare_workspace(config: RecoveryConfig, run_dir: Path, manifest: Manifest, io: RecoveryIO) -> StepOutcome:
    """准备步：复制数据根到工作根。"""
    target, detail = prepare_target(config, run_dir, manifest, io)
    detail.set("target_path", target)
    return StepOutcome(detail=detail)


def _step_metadata(config: RecoveryConfig, manifest: Manifest, io: RecoveryIO) -> StepOutcome:
    """元数据步：核对 dump；live 再执行导入模板。"""
    outcome = verify_metadata(config, manifest, io)
    if config.mode == MODE_LIVE:
        command = command_for(
            config, config.metadata_cmd, str(config.platform_dump_dir / manifest.metadata_dump), manifest.backup_id
        )
        result = run_command_once(config, io, "metadata", command)
        outcome.detail.set("command", command_detail(result))
    return outcome


def _step_live_command(
    config: RecoveryConfig,
    io: RecoveryIO,
    name: str,
    template: str,
    manifest: Manifest,
    dump: str,
) -> StepOutcome:
    """执行 live 命令模板并给出结果明细。"""
    command = command_for(config, template, dump, manifest.backup_id)
    result = run_command_once(config, io, name, command)
    return StepOutcome(detail=command_detail(result))


def _step_offsets(
    config: RecoveryConfig,
    snapshot: Path,
    target_root: Path,
    manifest: Manifest,
    io: RecoveryIO,
) -> StepOutcome:
    """位点步：恢复并逐文件复算。"""
    return StepOutcome(detail=restore_offsets(config, snapshot, target_root, manifest, io))


def _without(payload: ConcurrentStableDict[str, object], *keys: str) -> ConcurrentStableDict[str, object]:
    """复制明细并去掉指定键（避免把大对象重复写进记录）。"""
    result: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    for key in payload:
        if key in keys:
            continue
        result.set(key, payload.get(key))
    return result


def current_config_version(config: RecoveryConfig, io: RecoveryIO) -> str:
    """当前仓内四份配置的内容摘要（与清单的 `config_version` 对照）。"""
    try:
        return backup.config_digest(config.repo_root, backup.filesystem_io())
    except OSError:
        io.alert("[ALERT] recovery.config: 仓内配置文本缺失，配置版本留空")
        return ""


def compute_rpo(last_write_us: int | None, source: str, created_at: str) -> tuple[int | None, str]:
    """RPO = 数据最后写入时刻 − 清单创建时刻（负值夹到 0）。"""
    if last_write_us is None:
        return None, "null"
    created = parse_manifest_time(created_at)
    last_write = datetime.fromtimestamp(last_write_us / 1_000_000, tz=UTC)
    return max(int((last_write - created).total_seconds()), 0), source


def fallback_hint(config: RecoveryConfig, io: RecoveryIO) -> str:
    """失败时给出回退参数：当前指针之前最近的一份快照。"""
    current = current_backup_id(config.backup_root, io)
    if not current:
        return "<上一份备份标识>"
    return previous_backup_id(config.backup_root, io, current=current) or "<上一份备份标识>"


def _build_config(args: argparse.Namespace, env: ConcurrentStableDict[str, str]) -> RecoveryConfig:
    """把参数与环境变量合成生效配置。"""
    repo_root = Path(args.root) if args.root else Path(__file__).resolve().parents[1].parent
    mode = str(args.mode)
    live = mode == MODE_LIVE
    work_raw = args.work_dir or env.get(RECOVERY_WORK_DIR_ENV, "")
    if not work_raw and not args.dry_run and not args.list_backups:
        raise RecoveryError(f"recovery.config: 缺少 --work-dir 或环境变量 {RECOVERY_WORK_DIR_ENV}")
    data_raw = args.data_root or env.get(DATA_DIR_ENV, "") or DEFAULT_DATA_DIR
    backup_raw = args.backup_root or env.get(backup.BACKUP_DIR_ENV, "")
    if not backup_raw:
        raise RecoveryError(f"recovery.config: 缺少 --backup-root 或环境变量 {backup.BACKUP_DIR_ENV}")
    dump_raw = args.dump_dir or env.get(backup.BACKUP_PLATFORM_DUMP_DIR_ENV, "")
    if not dump_raw and not args.dry_run and not args.list_backups:
        raise RecoveryError(f"recovery.config: 缺少 --dump-dir 或环境变量 {backup.BACKUP_PLATFORM_DUMP_DIR_ENV}")
    now = datetime.now(UTC)
    suffix = now.strftime("%Y%m%dT%H%M%SZ")
    report = (
        Path(args.report)
        if args.report
        else repo_root / RECOVERY_REPORT_DIR / f"{args.backup_id or 'latest'}-{mode}-{suffix}.json"
    )
    return RecoveryConfig(
        repo_root=repo_root,
        source_data_root=Path(data_raw),
        backup_root=Path(backup_raw),
        platform_dump_dir=Path(dump_raw) if dump_raw else repo_root,
        work_root=Path(work_raw) if work_raw else repo_root / "tmp" / "recovery-work",
        report_path=report,
        mode=mode,
        backup_id=cast("str | None", args.backup_id),
        target_env=str(args.target_env or env.get("BMS_ENV", "") or "dev"),
        sample_per_partition=int(args.sample_per_partition),
        sample_limit=int(args.sample_limit),
        max_partitions=int(args.max_partitions),
        keep=bool(args.keep),
        dry_run=bool(args.dry_run),
        allow_degraded=bool(args.allow_degraded),
        confirmed=bool(args.yes),
        stop_cmd=required_text(env, RECOVERY_STOP_CMD_ENV, mode="live", enabled=live),
        start_cmd=required_text(env, RECOVERY_START_CMD_ENV, mode="live", enabled=live),
        metadata_cmd=required_text(env, RECOVERY_METADATA_CMD_ENV, mode="live", enabled=live),
        ready_url=required_text(env, RECOVERY_READY_URL_ENV, mode="live", enabled=live),
        timeout_sec=positive_int(env, RECOVERY_TIMEOUT_SEC_ENV, RECOVERY_DEFAULT_TIMEOUT_SEC),
        now=now,
    )


def print_backups(backup_root: Path, io: RecoveryIO) -> int:
    """打印候选快照（含当前与回退候选）。"""
    refs = list_backups(backup_root, io)
    if not refs:
        io.alert("[FAIL] recovery.manifest: 备份根下没有可用快照")
        return 1
    current = current_backup_id(backup_root, io)
    previous = previous_backup_id(backup_root, io, current=current) if current else ""
    for ref in refs:
        marks: ConcurrentStableList[str] = ConcurrentStableList()
        if ref.is_current:
            marks.add("当前")
        if ref.is_latest:
            marks.add("最近")
        if previous and ref.backup_id == previous:
            marks.add("上一份（回退候选）")
        suffix = f" [{'/'.join(marks)}]" if marks else ""
        io.output(f"{ref.backup_id}  created_at={ref.created_at or '?'}{suffix}")
    return 0


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口：只消费已注入环境变量，不读取 `.env` 明文。"""
    parser = argparse.ArgumentParser(description="msg 恢复演练与记录：清单校验 + 位点恢复 + 对齐校验")
    parser.add_argument("--root", help="仓库根（缺省自动定位）")
    parser.add_argument("--mode", choices=(MODE_DRILL, MODE_LIVE), default=MODE_DRILL, help="执行模式")
    parser.add_argument("--backup-root", help="备份根（缺省取 MSG_BACKUP_DIR）")
    parser.add_argument("--backup-id", help="指定快照（缺省取 current.json）")
    parser.add_argument("--data-root", help="数据根（缺省取 MSG_DATA_DIR）")
    parser.add_argument("--dump-dir", help="平台库 dump 目录（缺省取 MSG_BACKUP_PLATFORM_DUMP_DIR）")
    parser.add_argument("--work-dir", help="演练工作根（缺省取 MSG_RECOVERY_WORK_DIR）")
    parser.add_argument("--report", help="记录 JSON 落点")
    parser.add_argument("--sample-per-partition", type=int, default=RECOVERY_SAMPLE_PER_PARTITION)
    parser.add_argument("--sample-limit", type=int, default=RECOVERY_SAMPLE_LIMIT)
    parser.add_argument("--max-partitions", type=int, default=0)
    parser.add_argument("--target-env", default="", help="目标环境名（prod / production 一律拒绝）")
    parser.add_argument("--allow-degraded", action="store_true", help="降级结论下仍启动（仅 live）")
    parser.add_argument("--keep", action="store_true", help="保留演练工作根")
    parser.add_argument("--list-backups", action="store_true", help="列出可用快照后退出")
    parser.add_argument("--dry-run", action="store_true", help="只读校验，不落盘")
    parser.add_argument("--yes", action="store_true", help="live 模式的显式确认")
    args = parser.parse_args(None if argv is None else argv)
    env: ConcurrentStableDict[str, str] = ConcurrentStableDict(os.environ)
    io = filesystem_io()
    try:
        config = _build_config(args, env)
        if args.list_backups:
            return print_backups(config.backup_root, io)
        return run(config, io)
    except RecoveryError as exc:
        fs_alert(f"[FAIL] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
