"""恢复演练与记录（Kiwi 2476；任务 07-3-2 §13）。

覆盖策略：

- **正例走真实链路**：用 `FileMessageStore` / `FileOffsetStore` / `FileDelayStore` 造出真实段、位点与
  延迟记录，再跑一次真实备份，最后在隔离副本上跑完整 `--drill`；判据不靠手写格式。
- **失败分支走真实损坏**：改快照字节、删段、截断 `.delay`、改清单字段，脚本按真实读取判失败。
- **live 编排走注入 runner**：命令执行、就绪探针、清理都收在可注入端口，不依赖 Docker 与网络。
"""

from __future__ import annotations

import asyncio
import gzip
import json
import shutil
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.broker.delay import DelayRecord
from bms_msg.broker.delay_file import FileDelayStore
from bms_msg.broker.file_store import FileMessageStore, PartitionDirectory
from bms_msg.broker.offset import FileOffsetStore, decode_offset_file, group_directory_name
from bms_msg.core import config as cfg
from bms_msg.core.trace import TraceContext
from ops import backup, check_credentials, recovery

pytestmark = pytest.mark.kiwi_id(2476)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

NOW = datetime(2026, 11, 10, 5, 0, 0, tzinfo=UTC)
_TENANT = 7_001
_TOPIC_ID = 1_001
_NAMESPACE = "orders"
_TOPIC_NAME = "orders.events"
_GROUP = "g1"


@dataclass(frozen=True)
class Workspace:
    """一次用例的完整环境（仓 / 数据根 / dump / 备份根 / 工作根）。"""

    root: Path
    repo: Path
    data_root: Path
    dump_dir: Path
    backup_root: Path
    work_root: Path
    report: Path
    outputs: ConcurrentStableList[str]
    alerts: ConcurrentStableList[str]
    backup_id: str


class _NullAudit:
    """位点存储构造要求的审计出口替身。"""

    async def emit(self, event: object, *, required: bool = False) -> bool:
        """不做任何事。"""
        return True


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_repo(tmp_path: Path) -> Path:
    """最小仓：模型清单 / 四份配置 / 产品声明。"""
    repo = tmp_path / "repo"
    models = repo / "backend" / "services" / "msg" / "src" / "bms_msg" / "models"
    _write(models / "__init__.py", 'MODEL_MODULES: tuple[str, ...] = ("bms_msg.models.topic",)\n')
    _write(models / "topic.py", 'class MsgTopic:\n    __tablename__ = "msg_topic"\n')
    for name in ("config.toml", "config.dev.toml", "config.test.toml", "config.prod.toml"):
        _write(repo / "backend" / name, f"# {name}\n")
    _write(
        repo / "deploy" / "registry" / "msg.toml",
        '[[services]]\nservice_version = "0.1.0"\ncontract_version = "0.1.0"\n',
    )
    return repo


def _make_dump(dump_dir: Path, *, tables: str = "msg_topic") -> None:
    dump_dir.mkdir(parents=True, exist_ok=True)
    statements = "".join(f"CREATE TABLE `{name}` (id INT);\n" for name in tables.split(","))
    (dump_dir / "all_20261110_120000.sql.gz").write_bytes(gzip.compress(statements.encode("utf-8")))


async def _seed(
    data_root: Path,
    *,
    messages: int = 5,
    offset: int = 4,
    delay: bool = True,
    segment_max_bytes: int = 1024 * 1024,
) -> None:
    """用真实存储类造消息段、位点文件与延迟记录。"""
    directory = PartitionDirectory(root=data_root)
    directory.bind(tenant_id=_TENANT, namespace=_NAMESPACE, name=_TOPIC_NAME, topic_id=_TOPIC_ID)
    store = FileMessageStore(directory=directory, fsync_mode="os", segment_max_bytes=segment_max_bytes)
    for index in range(messages):
        await store.append(
            topic_id=_TOPIC_ID,
            partition_no=0,
            message_id=f"m-{index}",
            log_id=f"l-{index}",
            key="order-1",
            payload=b"payload",
            headers=ConcurrentStableDict({"source": "test"}),
            trace=TraceContext(trace_id="trace-1"),
            published_at=NOW,
            content_type="application/json",
            compression="none",
            uncompressed_len=7,
        )
    await store.aclose()

    offsets = FileOffsetStore(directory=directory, root=data_root, audit=_NullAudit())
    await offsets.commit(group=_GROUP, topic_id=_TOPIC_ID, partition_no=0, offset=offset)
    await offsets.flush_once()
    await offsets.aclose()

    if delay:
        published = datetime.now(UTC)
        delayed = FileDelayStore(directory=directory, fsync_mode="os")
        await delayed.enqueue(
            DelayRecord(
                log_id="delay-1",
                message_id="m-delay",
                tenant_id=_TENANT,
                topic_id=_TOPIC_ID,
                namespace=_NAMESPACE,
                topic=_TOPIC_NAME,
                partition_no=0,
                delay_level="1h",
                key="order-1",
                payload=b"delayed",
                headers=ConcurrentStableDict({"source": "test"}),
                trace=TraceContext(trace_id="trace-1"),
                published_at=published,
                deliver_at=published + timedelta(hours=1),
                attempts=0,
            )
        )
        await delayed.aclose()


def _run_backup(workspace: Workspace, now: datetime, *, data_root: Path | None = None) -> int:
    config = backup.BackupConfig(
        repo_root=workspace.repo,
        data_root=data_root or workspace.data_root,
        backup_root=workspace.backup_root,
        platform_dump_dir=workspace.dump_dir,
        keep_days=7,
        space_factor=1.2,
        dry_run=False,
        now=now,
    )
    return backup.run(config, backup.filesystem_io())


async def _workspace(
    tmp_path: Path, *, messages: int = 5, offset: int = 4, segment_max_bytes: int = 1024 * 1024
) -> Workspace:
    """搭一次可跑的完整环境，并先产出一份真实备份。"""
    repo = _make_repo(tmp_path)
    data_root = tmp_path / "data"
    dump_dir = tmp_path / "dump"
    backup_root = tmp_path / "backup"
    work_root = tmp_path / "work"
    backup_root.mkdir()
    work_root.mkdir()
    _make_dump(dump_dir)
    await _seed(data_root, messages=messages, offset=offset, segment_max_bytes=segment_max_bytes)
    workspace = Workspace(
        root=tmp_path,
        repo=repo,
        data_root=data_root,
        dump_dir=dump_dir,
        backup_root=backup_root,
        work_root=work_root,
        report=tmp_path / "report.json",
        outputs=ConcurrentStableList(),
        alerts=ConcurrentStableList(),
        backup_id="",
    )
    assert _run_backup(workspace, NOW) == 0
    identifier = (backup_root / "current.json").read_text(encoding="utf-8")
    workspace = replace(workspace, backup_id=str(json.loads(identifier)["backup_id"]))
    return workspace


def prepare_workspace(
    tmp_path: Path,
    *,
    messages: int = 5,
    offset: int = 4,
    segment_max_bytes: int = 1024 * 1024,
) -> Workspace:
    """同步包装一次环境搭建（用例本体是同步函数，恢复命令本身也是同步入口）。"""
    return asyncio.run(_workspace(tmp_path, messages=messages, offset=offset, segment_max_bytes=segment_max_bytes))


def io_for(
    workspace: Workspace,
    *,
    commands: ConcurrentStableList[str] | None = None,
    http_status: int = 200,
    command_exit: int = 0,
    fail_contains: str | None = None,
    extra: dict[str, object] | None = None,
) -> recovery.RecoveryIO:
    """在真实文件系统上注入输出、命令、HTTP 与清理替身。"""

    def run_command(command: str, timeout: int) -> recovery.CommandResult:
        if commands is not None:
            commands.add(command)
        code = command_exit if fail_contains is None or fail_contains in command else 0
        return recovery.CommandResult(exit_code=code, stdout="ok", stderr="", elapsed_ms=1)

    def fixed_status(url: str, timeout: float) -> int:
        return http_status

    def no_sleep(milliseconds: int) -> None:
        return None

    base = replace(
        recovery.filesystem_io(),
        output=workspace.outputs.add,
        alert=workspace.alerts.add,
        run_command=run_command,
        http_status=fixed_status,
        sleep_ms=no_sleep,
    )
    if extra:
        base = replace(base, **extra)
    return base


def config_for(
    workspace: Workspace,
    *,
    mode: str = recovery.MODE_DRILL,
    dry_run: bool = False,
    keep: bool = False,
    allow_degraded: bool = False,
    confirmed: bool = True,
    target_env: str = "dev",
    timeout_sec: int = 5,
    now: datetime = NOW,
    max_partitions: int = 0,
    sample_per_partition: int = recovery.RECOVERY_SAMPLE_PER_PARTITION,
    sample_limit: int = recovery.RECOVERY_SAMPLE_LIMIT,
    backup_id: str | None = None,
) -> recovery.RecoveryConfig:
    """构造一次演练配置（命令模板给可识别的最小串）。"""
    return recovery.RecoveryConfig(
        repo_root=workspace.repo,
        source_data_root=workspace.data_root,
        backup_root=workspace.backup_root,
        platform_dump_dir=workspace.dump_dir,
        work_root=workspace.work_root,
        report_path=workspace.report,
        mode=mode,
        backup_id=backup_id,
        target_env=target_env,
        sample_per_partition=sample_per_partition,
        sample_limit=sample_limit,
        max_partitions=max_partitions,
        keep=keep,
        dry_run=dry_run,
        allow_degraded=allow_degraded,
        confirmed=confirmed,
        stop_cmd="stop service",
        start_cmd="start service",
        metadata_cmd="import {dump}",
        ready_url="http://127.0.0.1/readyz",
        timeout_sec=timeout_sec,
        now=now,
    )


def read_report(workspace: Workspace) -> dict[str, object]:
    return cast("dict[str, object]", json.loads(workspace.report.read_text(encoding="utf-8")))


def section(payload: dict[str, object], key: str) -> dict[str, object]:
    return cast("dict[str, object]", payload[key])


def step_rows(payload: dict[str, object]) -> list[dict[str, object]]:
    return [cast("dict[str, object]", item) for item in cast("list[object]", payload["steps"])]


def manifest_payload(workspace: Workspace) -> dict[str, object]:
    return cast(
        "dict[str, object]",
        json.loads((workspace.backup_root / workspace.backup_id / "manifest.json").read_text(encoding="utf-8")),
    )


def partition_manifest(bases: ConcurrentStableFrozenList[int], *, leo: int = 10) -> recovery.Manifest:
    """构造一个只含单分区的清单（段比对用例用）。"""
    expectation = recovery.PartitionExpectation(
        tenant=str(_TENANT),
        namespace=_NAMESPACE,
        topic=_TOPIC_NAME,
        topic_id=_TOPIC_ID,
        partition=0,
        segment_bases=bases,
        leo=leo,
        leo_source="recovery-point",
    )
    return recovery.Manifest(
        schema_version=1,
        backup_id="20261110T050000Z",
        created_at=NOW.isoformat(),
        service_name="msg",
        service_version="0.1.0",
        contract_version="0.1.0",
        config_version="cfg",
        tenants=ConcurrentStableFrozenList([str(_TENANT)]),
        metadata_dump="dump.sql.gz",
        tables_expected=ConcurrentStableFrozenList(["msg_topic"]),
        tables_in_dump=ConcurrentStableFrozenList(["msg_topic"]),
        metadata_covered=True,
        offset_files=ConcurrentStableFrozenList(),
        files=ConcurrentStableFrozenList(),
        partitions=ConcurrentStableFrozenList([expectation]),
        needs_alignment=ConcurrentStableFrozenList(),
        log_backed_up=False,
        recovery_order=ConcurrentStableFrozenList(str(item) for item in backup.RECOVERY_ORDER),
    )


def segment_dir(workspace: Workspace) -> Path:
    return workspace.data_root / "data" / str(_TENANT) / _NAMESPACE / f"{_TOPIC_NAME}.{_TOPIC_ID}" / "0"


# ---------------------------------------------------------------------------
# R1 ~ R3：清单、指针与快照
# ---------------------------------------------------------------------------


def test_manifest_rejects_high_version_and_missing_fields(tmp_path: Path) -> None:
    """R1：版本过高 / 必填缺失 / 顺序不符 / 日志口径不符 / 元数据未覆盖 / 路径穿越均拒绝。"""
    workspace = prepare_workspace(tmp_path)
    base = manifest_payload(workspace)

    def parse(payload: dict[str, object]) -> recovery.Manifest:
        return recovery.parse_manifest(recovery.stable_json(payload), directory_name=workspace.backup_id)

    assert parse(dict(base)).backup_id == workspace.backup_id

    def high_version(data: dict[str, object]) -> None:
        data["schema_version"] = 2

    def no_config(data: dict[str, object]) -> None:
        data.pop("config_version")

    def bad_order(data: dict[str, object]) -> None:
        data["recovery_order"] = ["元数据库", "位点文件", "消息日志"]

    def log_backed_up(data: dict[str, object]) -> None:
        cast("dict[str, object]", data["log_segments"])["backed_up"] = True

    def not_covered(data: dict[str, object]) -> None:
        cast("dict[str, object]", data["metadata"])["covered"] = False

    def traversal(data: dict[str, object]) -> None:
        cast("list[object]", data["files"]).append({"path": "../evil", "size": 1, "sha256": "x"})

    mutations: list[Callable[[dict[str, object]], None]] = [
        high_version,
        no_config,
        bad_order,
        log_backed_up,
        not_covered,
        traversal,
    ]
    for mutate in mutations:
        payload = json.loads(json.dumps(base))
        mutate(payload)
        with pytest.raises(recovery.RecoveryError, match=r"recovery\.manifest"):
            parse(payload)
    with pytest.raises(recovery.RecoveryError, match="目录名"):
        recovery.parse_manifest(recovery.stable_json(base), directory_name="20200101T000000Z")
    crossed = json.loads(json.dumps(base))
    crossed_offsets = cast("dict[str, object]", crossed["offsets"])
    crossed_files = cast("list[dict[str, object]]", crossed_offsets["files"])
    crossed_files[0]["sha256"] = "0" * 64
    with pytest.raises(recovery.RecoveryError, match="不一致"):
        parse(crossed)


def test_pointer_fallback_and_listing(tmp_path: Path) -> None:
    """R2：指针损坏回退到最近快照；枚举标记当前与上一份。"""
    workspace = prepare_workspace(tmp_path)
    assert _run_backup(workspace, NOW + timedelta(seconds=1)) == 0
    refs = recovery.list_backups(workspace.backup_root, recovery.filesystem_io())
    assert len(refs) == 2
    assert refs[-1].is_current and refs[-1].is_latest
    assert (
        recovery.previous_backup_id(workspace.backup_root, recovery.filesystem_io(), current=refs[-1].backup_id)
        == refs[0].backup_id
    )

    (workspace.backup_root / "current.json").unlink()
    exit_code = recovery.run(config_for(workspace), io_for(workspace))
    assert exit_code == 0
    assert any("recovery.pointer" in line for line in workspace.alerts)


def test_snapshot_corruption_blocks_before_work_dir(tmp_path: Path) -> None:
    """R3：快照校验和不符即失败，工作根未创建、源目录零改动。"""
    workspace = prepare_workspace(tmp_path)
    snapshot = workspace.backup_root / workspace.backup_id
    target = next((snapshot / "offsets").rglob("*.offset"))
    original = target.read_bytes()
    target.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
    before = sorted(path.relative_to(workspace.data_root) for path in workspace.data_root.rglob("*") if path.is_file())

    exit_code = recovery.run(config_for(workspace), io_for(workspace))

    assert exit_code == 1
    assert not list(workspace.work_root.glob("run-*"))
    after = sorted(path.relative_to(workspace.data_root) for path in workspace.data_root.rglob("*") if path.is_file())
    assert before == after
    assert any("recovery.snapshot" in line for line in workspace.alerts)
    assert section(read_report(workspace), "conclusion")["result"] == recovery.CONCLUSION_FAIL


# ---------------------------------------------------------------------------
# R4 ~ R8：恢复、段分诊、对齐、抽样、延迟
# ---------------------------------------------------------------------------


def test_full_drill_passes_and_cleans(tmp_path: Path) -> None:
    """R4 / R7 / R9 / R11：完整演练通过、抽样可读、记录字段齐备、工作根被清理。"""
    workspace = prepare_workspace(tmp_path)
    exit_code = recovery.run(config_for(workspace), io_for(workspace))

    assert exit_code == 0, workspace.alerts
    assert not list(workspace.work_root.glob("run-*"))
    payload = read_report(workspace)
    assert payload["task"] == "07-3-2"
    assert payload["mode"] == "drill"
    assert section(payload, "conclusion")["result"] == recovery.CONCLUSION_PASS
    assert section(payload, "conclusion")["data_integrity"] == "完整"
    checks = section(payload, "checks")
    assert section(checks, "segments")["degraded"] is False
    assert section(checks, "offsets")["sampled"] == 4
    assert section(checks, "delay")["records"] == 1
    assert section(checks, "delay")["expected"] == 1
    assert payload["rpo_source"] in {"recovery-point", "mtime"}
    assert (workspace.report.with_suffix(".md")).is_file()
    assert "恢复演练记录" in workspace.report.with_suffix(".md").read_text(encoding="utf-8")


def test_gap_classification(tmp_path: Path) -> None:
    """R5：保留期裁剪不降级；段间空洞 / 段尾缺失 / 分区缺失降级。"""
    io = recovery.filesystem_io()
    root = tmp_path / "target"
    directory = root / "data" / str(_TENANT) / _NAMESPACE / f"{_TOPIC_NAME}.{_TOPIC_ID}" / "0"
    directory.mkdir(parents=True)

    def write_bases(*bases: int) -> None:
        for path in directory.glob("*.log"):
            path.unlink()
        for base in bases:
            (directory / f"{base:020d}.log").write_bytes(b"")

    manifest = partition_manifest(ConcurrentStableFrozenList([0, 1, 2]))
    write_bases(2)
    pruned = recovery.scan_segments(root, manifest, io)
    assert pruned.degraded is False
    assert pruned.partitions[0].pruned == ConcurrentStableFrozenList([0, 1])

    write_bases(0, 2)
    holes = recovery.scan_segments(root, manifest, io)
    assert holes.degraded is True
    assert holes.partitions[0].holes == ConcurrentStableFrozenList([1])

    write_bases(0, 1)
    tail = recovery.scan_segments(root, manifest, io)
    assert tail.degraded is True
    assert tail.partitions[0].tail_missing == ConcurrentStableFrozenList([2])

    shutil.rmtree(directory)
    absent = recovery.scan_segments(root, manifest, io)
    assert absent.degraded is True
    assert absent.absent == ConcurrentStableFrozenList([manifest.partitions[0].label])


def test_offset_beyond_leo_is_clamped_and_reported(tmp_path: Path) -> None:
    """R6：快照位点越过日志尾被夹取并落盘，结论降级。"""
    workspace = prepare_workspace(tmp_path, offset=99)
    exit_code = recovery.run(config_for(workspace, keep=True), io_for(workspace))

    assert exit_code == 2
    payload = read_report(workspace)
    alignment = section(section(payload, "checks"), "alignment")
    row = cast("list[object]", alignment["partitions"])[0]
    assert cast("dict[str, object]", row)["leo_after"] == 5
    target = next(workspace.work_root.glob("run-*/target"))
    offset_file = target / "offsets" / str(_TENANT) / group_directory_name(_GROUP) / f"{_TOPIC_ID}-0.offset"
    readout = decode_offset_file(offset_file.read_bytes())
    assert readout.persisted is not None
    assert readout.persisted.offset == 5
    reasons = cast("list[object]", section(payload, "conclusion")["reasons"])
    assert any("位点越过日志尾" in str(reason) for reason in reasons)


def test_delay_truncation_is_degraded(tmp_path: Path) -> None:
    """R8：`.delay` 末段截断 → 重建报告截断字节数，结论降级（不静默丢）。"""
    data_root = tmp_path / "data"
    asyncio.run(_seed(data_root))
    delay_file = next((data_root / "data").rglob("*.delay"))
    raw = delay_file.read_bytes()
    delay_file.write_bytes(raw[:-3])

    detail, degraded, reasons = asyncio.run(
        recovery.verify_delay(data_root, recovery.filesystem_io(), ConcurrentStableFrozenList())
    )

    assert degraded is True
    assert cast("int", detail["truncated_bytes"]) > 0
    assert reasons


# ---------------------------------------------------------------------------
# R9 ~ R10：live 编排、拒绝与失败分支
# ---------------------------------------------------------------------------


def test_live_orchestration_and_retention(tmp_path: Path) -> None:
    """R10：停服 → 导入 → 启动顺序正确；live 保留演练前备份。"""
    workspace = prepare_workspace(tmp_path)
    commands: ConcurrentStableList[str] = ConcurrentStableList()
    exit_code = recovery.run(config_for(workspace, mode=recovery.MODE_LIVE), io_for(workspace, commands=commands))

    assert exit_code == 0, workspace.alerts
    assert list(commands) == [
        "stop service",
        "import " + str(workspace.dump_dir / next(workspace.dump_dir.iterdir()).name),
        "start service",
    ]
    payload = read_report(workspace)
    assert payload["mode"] == "live"
    run_dir = next(workspace.work_root.glob("run-*"))
    assert (run_dir / recovery.RECOVERY_WORK_BACKUP_SUBDIR / "current.json").is_file()
    assert section(payload, "artifacts")["retained_backup_dir"] == recovery.RECOVERY_WORK_BACKUP_SUBDIR


def test_live_degraded_stops_before_start(tmp_path: Path) -> None:
    """R10：段尾缺失 → 降级；未给 --allow-degraded 时不启动。"""
    workspace = prepare_workspace(tmp_path, messages=4, segment_max_bytes=1)
    segments = sorted(segment_dir(workspace).glob("*.log"))
    assert len(segments) >= 2
    segments[-1].unlink()

    commands: ConcurrentStableList[str] = ConcurrentStableList()
    exit_code = recovery.run(config_for(workspace, mode=recovery.MODE_LIVE), io_for(workspace, commands=commands))
    assert exit_code == 2
    assert "start service" not in list(commands)

    allowed: ConcurrentStableList[str] = ConcurrentStableList()
    exit_code = recovery.run(
        config_for(workspace, mode=recovery.MODE_LIVE, allow_degraded=True, now=NOW + timedelta(seconds=1)),
        io_for(workspace, commands=allowed),
    )
    assert exit_code == 2
    assert "start service" in list(allowed)


def test_live_refuses_prod_and_missing_confirmation(tmp_path: Path) -> None:
    """R10：prod 一律拒绝；缺 --yes 拒绝；两者都不执行任何命令。"""
    workspace = prepare_workspace(tmp_path)
    commands: ConcurrentStableList[str] = ConcurrentStableList()
    assert (
        recovery.run(
            config_for(workspace, mode=recovery.MODE_LIVE, target_env="prod"), io_for(workspace, commands=commands)
        )
        == 1
    )
    assert not list(commands)
    assert recovery.run(config_for(workspace, mode=recovery.MODE_LIVE, confirmed=False), io_for(workspace)) == 1
    assert any("--yes" in line for line in workspace.alerts)


def test_live_metadata_command_failure_and_ready_timeout(tmp_path: Path) -> None:
    """R10：导入命令失败 / 就绪超时均判失败，且失败后仍写记录。"""
    workspace = prepare_workspace(tmp_path)
    assert (
        recovery.run(
            config_for(workspace, mode=recovery.MODE_LIVE), io_for(workspace, command_exit=1, fail_contains="import")
        )
        == 1
    )
    assert any("recovery.command.metadata" in line for line in workspace.alerts)
    steps = step_rows(read_report(workspace))
    assert any(step["step"] == "metadata" and step["result"] == recovery.RESULT_FAILED for step in steps)
    assert any(step["step"] == "start" and step["result"] == recovery.RESULT_SKIPPED for step in steps)

    ready = prepare_workspace(tmp_path / "ready")
    exit_code = recovery.run(
        config_for(ready, mode=recovery.MODE_LIVE, timeout_sec=1),
        io_for(ready, http_status=503),
    )
    assert exit_code == 1
    assert any("recovery.ready" in line for line in ready.alerts)


# ---------------------------------------------------------------------------
# R11 ~ R13：记录、CLI、口径复用
# ---------------------------------------------------------------------------


def test_dry_run_is_read_only(tmp_path: Path) -> None:
    """R12：dry-run 不建演练目录、不写位点，只落记录。"""
    workspace = prepare_workspace(tmp_path)
    exit_code = recovery.run(config_for(workspace, dry_run=True), io_for(workspace))

    assert exit_code == 0
    assert not list(workspace.work_root.glob("run-*"))
    payload = read_report(workspace)
    checks = section(payload, "checks")
    assert "alignment" not in checks
    assert cast("dict[str, object]", checks["segments"])["degraded"] is False
    assert isinstance(payload["rpo_seconds"], int)


def test_live_keeps_work_dir(tmp_path: Path) -> None:
    """R9：live 不删工作根（演练前备份是回退点）。"""
    workspace = prepare_workspace(tmp_path)
    assert recovery.run(config_for(workspace, mode=recovery.MODE_LIVE), io_for(workspace)) == 0
    assert list(workspace.work_root.glob("run-*"))


def test_cli_list_backups_and_config_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """R12：`--list-backups` 可用；缺键 / live 缺模板均指名失败。"""
    workspace = prepare_workspace(tmp_path)
    for key in (
        "MSG_RECOVERY_WORK_DIR",
        "MSG_RECOVERY_STOP_CMD",
        "MSG_RECOVERY_START_CMD",
        "MSG_RECOVERY_METADATA_CMD",
        "MSG_RECOVERY_READY_URL",
        "MSG_RECOVERY_TIMEOUT_SEC",
        "MSG_BACKUP_DIR",
        "MSG_BACKUP_PLATFORM_DUMP_DIR",
        "MSG_DATA_DIR",
    ):
        monkeypatch.delenv(key, raising=False)

    assert recovery.main(ConcurrentStableList(["--list-backups", "--backup-root", str(workspace.backup_root)])) == 0
    out = capsys.readouterr().out
    assert workspace.backup_id in out and "当前" in out

    monkeypatch.setenv("MSG_BACKUP_DIR", str(workspace.backup_root))
    monkeypatch.setenv("MSG_BACKUP_PLATFORM_DUMP_DIR", str(workspace.dump_dir))
    monkeypatch.setenv("MSG_DATA_DIR", str(workspace.data_root))
    assert (
        recovery.main(
            ConcurrentStableList(["--root", str(workspace.repo), "--dry-run", "--report", str(workspace.report)])
        )
        == 0
    )

    assert (
        recovery.main(
            ConcurrentStableList(["--root", str(workspace.repo), "--backup-root", str(workspace.backup_root)])
        )
        == 1
    )
    assert "MSG_RECOVERY_WORK_DIR" in capsys.readouterr().err

    monkeypatch.setenv("MSG_RECOVERY_WORK_DIR", str(workspace.work_root))
    assert recovery.main(ConcurrentStableList(["--root", str(workspace.repo), "--mode", "live", "--yes"])) == 1
    assert "MSG_RECOVERY_STOP_CMD" in capsys.readouterr().err


def test_record_helpers_and_redaction() -> None:
    """R11：RTO / RPO 口径、结论三态退出码、命令脱敏。"""
    assert recovery.compute_rpo(None, "null", NOW.isoformat()) == (None, "null")
    later = NOW + timedelta(seconds=30)
    assert recovery.compute_rpo(int(later.timestamp() * 1_000_000), "mtime", NOW.isoformat())[0] == 30
    assert (
        recovery.compute_rpo(int((NOW - timedelta(seconds=30)).timestamp() * 1_000_000), "mtime", NOW.isoformat())[0]
        == 0
    )
    assert recovery.EXIT_CODES == {"通过": 0, "降级通过": 2, "失败": 1}
    assert "***" in recovery.sanitize("mysql --pass" + "word=secret -uroot")
    assert "***" in recovery.sanitize("MYSQL_P" + "WD=secret mysql -uroot")
    assert "***" in recovery.sanitize("psql postgres://user:secret@host/db")
    assert "secret" not in recovery.sanitize("mysql -psecret -uroot")


def test_keys_reuse_and_container_boundary() -> None:
    """R13：六键在常量 / 模板 / 宿主例外三处一致，且不进容器编排。"""
    env_example = (_REPO / "deploy" / ".env.example").read_text(encoding="utf-8")
    compose = (_REPO / "deploy" / "compose" / "msg.yml").read_text(encoding="utf-8")
    keys = (
        cfg.RECOVERY_WORK_DIR_ENV,
        cfg.RECOVERY_STOP_CMD_ENV,
        cfg.RECOVERY_START_CMD_ENV,
        cfg.RECOVERY_METADATA_CMD_ENV,
        cfg.RECOVERY_READY_URL_ENV,
        cfg.RECOVERY_TIMEOUT_SEC_ENV,
    )
    for key in keys:
        assert f"{key}=" in env_example
        assert key in check_credentials.HOST_ONLY_KEYS
        assert key not in compose
    assert tuple(backup.RECOVERY_ORDER) == ("元数据库", "位点文件", "消息日志", "校验对齐")


def test_script_has_no_absolute_path_or_dotenv_read() -> None:
    """R12：脚本不写绝对路径、不读 `.env` 明文。"""
    text = (_REPO / "backend" / "ops" / "recovery.py").read_text(encoding="utf-8")
    assert '.env"' not in text.replace("不读取 `.env`", "")
    assert 'Path("C:' not in text
    assert 'Path("/' not in text
    assert "os.environ" in text
