"""位点快照备份脚本（Kiwi 2475；任务 07-3-1 §9）。

覆盖策略：

- **正例走真实临时目录**：默认 IO 就是真实文件系统，临时仓根 / 位点 / dump / 备份卷都落在
  `tmp_path`，正常备份跑完整链路并逐文件复算清单；
- **反例走可注入端口**：`dataclasses.replace(io, ...)` 逐项替换读写 / 空间 / 改名，注入并发写入、
  磁盘不足、中途失败与清理失败，不靠真机把盘写满；
- **纯函数直接喂构造数据**：配置解析、AST 清单、表集合对账、过期判定、序列化逐分支断言；
- **契约走真实仓文本**：键位三处齐备、宿主例外、无硬编码路径、调度模板形态都在仓内文件上核对。
"""

from __future__ import annotations

import ast
import gzip
import json
import runpy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from ops import backup
from ops import check_credentials as cc

pytestmark = pytest.mark.kiwi_id(2475)

_REPO = Path(__file__).resolve().parents[5]
"""仓根（`backend/services/msg/tests/deploy/<本文件>` 上溯五级）。"""

_NOW = datetime(2026, 11, 10, 5, 0, 0, tzinfo=UTC)
_BACKUP_KEYS = ("MSG_BACKUP_DIR", "MSG_BACKUP_PLATFORM_DUMP_DIR", "MSG_BACKUP_KEEP_DAYS", "MSG_BACKUP_SPACE_FACTOR")
_STABLE = b"offset-slot-v1"


@dataclass(frozen=True)
class Workspace:
    """一次用例的完整临时环境。"""

    root: Path
    repo: Path
    data_root: Path
    backup_root: Path
    dump_dir: Path
    config: backup.BackupConfig
    io: backup.BackupIO
    outputs: ConcurrentStableList[str]
    alerts: ConcurrentStableList[str]


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _workspace(
    tmp_path: Path,
    *,
    with_recovery: bool = True,
    now: datetime = _NOW,
    keep_days: int = 7,
    factor: float = 1.2,
    dry_run: bool = False,
) -> Workspace:
    """搭最小可跑仓：模型清单 / 四份配置 / 产品声明 / 两个位点 / 一个分区 / 一份 gzip dump。"""
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

    data_root = tmp_path / "data"
    for relative in ("7001/group-abcdef12/1001-0.offset", "7002/group-12345678/1001-1.offset"):
        target = data_root / backup.OFFSETS_SUBDIR / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_STABLE)
    partition = data_root / backup.DATA_ROOT_SUBDIR / "7001" / "ns" / "orders.1001" / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000000.log").write_bytes(b"segment")
    if with_recovery:
        _write(partition / backup.RECOVERY_POINT_NAME, '{"leo": 42, "segments": []}')

    dump_dir = tmp_path / "dump"
    dump_dir.mkdir()
    (dump_dir / "all_20261110_120000.sql.gz").write_bytes(
        gzip.compress(b"CREATE TABLE `msg_topic` (id INT);\nCREATE TABLE `msg_topic_acl` (id INT);\n")
    )
    (dump_dir / "notes.txt").write_text("not a dump", encoding="utf-8")

    backup_root = tmp_path / "backup"
    backup_root.mkdir()
    outputs: ConcurrentStableList[str] = ConcurrentStableList()
    alerts: ConcurrentStableList[str] = ConcurrentStableList()
    io = replace(backup.filesystem_io(), output=outputs.add, alert=alerts.add)
    config = backup.BackupConfig(
        repo_root=repo,
        data_root=data_root,
        backup_root=backup_root,
        platform_dump_dir=dump_dir,
        keep_days=keep_days,
        space_factor=factor,
        dry_run=dry_run,
        now=now,
    )
    return Workspace(
        root=tmp_path,
        repo=repo,
        data_root=data_root,
        backup_root=backup_root,
        dump_dir=dump_dir,
        config=config,
        io=io,
        outputs=outputs,
        alerts=alerts,
    )


def _published(ws: Workspace, identifier: str) -> Path:
    return ws.backup_root / identifier


def _manifest(ws: Workspace, identifier: str) -> Any:
    return json.loads((_published(ws, identifier) / backup.BACKUP_MANIFEST_NAME).read_text("utf-8"))


# ---------------------------------------------------------------------------
# T1 配置解析与纯函数判据
# ---------------------------------------------------------------------------


def test_pure_helpers() -> None:
    assert backup.sha256_hex(_STABLE) == backup.sha256_hex(_STABLE)
    assert len(backup.sha256_hex(_STABLE)) == 64
    assert backup.backup_id(_NOW) == "20261110T050000Z"
    assert backup.required_bytes(100, 1.5) == 150 + backup.BACKUP_SPACE_RESERVE_BYTES
    assert backup.expired("20261101T050000Z", _NOW, 7) is True
    assert backup.expired("20261109T050000Z", _NOW, 7) is False
    backup.check_space(0, backup.BACKUP_SPACE_RESERVE_BYTES, 1.2)
    with pytest.raises(backup.BackupError, match=r"backup\.space"):
        backup.check_space(0, 1, 1.2)


def test_config_from_env_defaults_and_overrides() -> None:
    env = ConcurrentStableDict[str, str]({"MSG_BACKUP_DIR": "/backup", "MSG_BACKUP_PLATFORM_DUMP_DIR": "/dump"})
    config = backup.config_from_env(env, repo_root=Path("/repo"), dry_run=True, now=_NOW)
    assert config.backup_root == Path("/backup")
    assert config.platform_dump_dir == Path("/dump")
    assert config.data_root == Path(backup.DEFAULT_DATA_DIR)
    assert config.keep_days == backup.BACKUP_KEEP_DAYS
    assert config.space_factor == backup.BACKUP_SPACE_FACTOR
    assert config.dry_run is True

    env.set("MSG_DATA_DIR", "/elsewhere/data")
    env.set("MSG_BACKUP_KEEP_DAYS", "14")
    env.set("MSG_BACKUP_SPACE_FACTOR", "2.5")
    config = backup.config_from_env(env, repo_root=Path("/repo"), dry_run=False, now=_NOW)
    assert config.data_root == Path("/elsewhere/data")
    assert config.keep_days == 14
    assert config.space_factor == 2.5


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"MSG_BACKUP_DIR": ""}, "MSG_BACKUP_DIR"),
        ({"MSG_BACKUP_PLATFORM_DUMP_DIR": ""}, "MSG_BACKUP_PLATFORM_DUMP_DIR"),
        ({"MSG_BACKUP_KEEP_DAYS": "abc"}, "正整数"),
        ({"MSG_BACKUP_KEEP_DAYS": "0"}, "正整数"),
        ({"MSG_BACKUP_SPACE_FACTOR": "abc"}, "有限数值"),
        ({"MSG_BACKUP_SPACE_FACTOR": "0.5"}, "有限数值"),
        ({"MSG_BACKUP_SPACE_FACTOR": "200"}, "有限数值"),
        ({"MSG_BACKUP_SPACE_FACTOR": "nan"}, "有限数值"),
    ],
)
def test_config_from_env_invalid(overrides: Any, reason: str) -> None:
    env = ConcurrentStableDict[str, str](
        {"MSG_BACKUP_DIR": "/backup", "MSG_BACKUP_PLATFORM_DUMP_DIR": "/dump", **overrides}
    )
    with pytest.raises(backup.BackupError, match=reason):
        backup.config_from_env(env, repo_root=Path("/repo"), dry_run=False, now=_NOW)


# ---------------------------------------------------------------------------
# T2 覆盖校验：模型清单 AST / dump 表名 / 对账
# ---------------------------------------------------------------------------


def test_ast_helpers_branches() -> None:
    assert backup.assigned_value(ast.parse("MODEL_MODULES = ('a',)").body[0]) is not None
    assert backup.assigned_value(ast.parse("MODEL_MODULES: tuple[str, ...] = ('a',)").body[0]) is not None
    assert backup.assigned_value(ast.parse("OTHER = ('a',)").body[0]) is None
    assert backup.assigned_value(ast.parse("OTHER: int = 1").body[0]) is None
    assert list(backup.model_modules(ast.parse("MODEL_MODULES = ('a', 'b')"))) == ["a", "b"]
    assert len(backup.model_modules(ast.parse("MODEL_MODULES = 'a'"))) == 0
    assert len(backup.model_modules(ast.parse("OTHER = ('a',)"))) == 0
    assert list(backup.model_modules(ast.parse("MODEL_MODULES = (1, 'a')"))) == ["a"]
    assert list(backup.table_names(ast.parse('class T:\n    __tablename__ = "msg_topic"'))) == ["msg_topic"]
    assert len(backup.table_names(ast.parse("OTHER = 1"))) == 0
    assert len(backup.table_names(ast.parse("__tablename__ = 1"))) == 0
    assert len(backup.table_names(ast.parse("class T:\n    pass"))) == 0


def test_expected_tables_variants(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    assert backup.expected_tables(ws.repo, ws.io) == ConcurrentStableFrozenList(["msg_topic"])

    def read_undecodable(_path: Path) -> bytes:
        return b"\xff\xfe"

    bad = replace(ws.io, read_bytes=read_undecodable)
    with pytest.raises(backup.BackupError, match="模型清单不可解析"):
        backup.expected_tables(ws.repo, bad)

    _write(ws.repo / "backend/services/msg/src/bms_msg/models/__init__.py", "OTHER = 1\n")
    with pytest.raises(backup.BackupError, match="MODEL_MODULES 缺失"):
        backup.expected_tables(ws.repo, ws.io)

    _write(
        ws.repo / "backend/services/msg/src/bms_msg/models/__init__.py", 'MODEL_MODULES = ("bms_msg.models.topic",)\n'
    )
    _write(ws.repo / "backend/services/msg/src/bms_msg/models/topic.py", "class T:\n    pass\n")
    with pytest.raises(backup.BackupError, match="未取到"):
        backup.expected_tables(ws.repo, ws.io)

    _write(ws.repo / "backend/services/msg/src/bms_msg/models/topic.py", "class T:\n    __tablename__ = (")
    with pytest.raises(backup.BackupError, match="模型清单不可解析"):
        backup.expected_tables(ws.repo, ws.io)


def test_metadata_coverage_branches() -> None:
    expected = ConcurrentStableFrozenList(["msg_topic"])
    payload = backup.metadata_coverage(expected, ConcurrentStableSet(["msg_topic"]), "all.sql.gz")
    assert payload.get("covered") is True
    assert payload.get("dump") == "all.sql.gz"
    assert list(cast("ConcurrentStableList[str]", payload.get("tables_extra"))) == []

    with_extra = ConcurrentStableSet(["msg_topic", "msg_legacy"])
    extra = backup.metadata_coverage(expected, with_extra, "all.sql.gz").get("tables_extra")
    assert list(cast("ConcurrentStableList[str]", extra)) == ["msg_legacy"]
    with pytest.raises(backup.BackupError, match="平台备份缺表 msg_topic"):
        backup.metadata_coverage(expected, ConcurrentStableSet(["msg_other"]), "all.sql.gz")


def test_dump_table_patterns(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    dump = ws.dump_dir / "all_20261110_120000.sql.gz"
    assert "msg_topic" in backup.fs_read_dump_tables(dump)
    plain = ws.dump_dir / "plain.sql"
    plain.write_text(
        "CREATE TABLE IF NOT EXISTS `bms_dev`.`msg_topic` (id INT);\ncreate table msg_quota (id INT);\n",
        encoding="utf-8",
    )
    assert sorted(backup.fs_read_dump_tables(plain)) == ["msg_quota", "msg_topic"]


# ---------------------------------------------------------------------------
# T3 dry-run 与默认 IO 边界
# ---------------------------------------------------------------------------


def test_default_io_edges(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ws = _workspace(tmp_path)
    with pytest.raises(backup.BackupError, match="位点目录不存在"):
        backup.fs_list_offset_files(tmp_path / "missing")
    assert len(backup.fs_list_partitions(tmp_path / "no-data")) == 0
    assert backup.fs_find_dump(tmp_path / "no-dump") is None
    empty = tmp_path / "empty-dump"
    empty.mkdir()
    assert backup.fs_find_dump(empty) is None
    assert backup.fs_find_dump(ws.dump_dir) is not None
    assert backup.fs_free_bytes(tmp_path / "not-created") > 0
    assert len(backup.fs_list_backup_dirs(tmp_path / "no-backup-root")) == 0
    ignored = ws.backup_root / "20261110T050000Z"
    ignored.mkdir()
    assert len(backup.fs_list_backup_dirs(ws.backup_root)) == 0  # 无 manifest 的目录不算可用快照
    backup.fs_remove_tree(tmp_path / "already-gone")
    backup.fs_output("stdout-line")
    backup.fs_alert("stderr-line")
    captured = capsys.readouterr()
    assert "stdout-line" in captured.out
    assert "stderr-line" in captured.err
    assert len(backup.fs_list_tree(ws.data_root / backup.OFFSETS_SUBDIR)) == 2
    assert list(backup.fs_list_segments(ws.data_root / backup.DATA_ROOT_SUBDIR / "7001/ns/orders.1001/0")) == [
        "00000000000000000000.log"
    ]
    assert backup.fs_file_size(ws.data_root / backup.OFFSETS_SUBDIR / "7001/group-abcdef12/1001-0.offset") == len(
        _STABLE
    )
    assert backup.fs_read_json(
        ws.data_root / backup.DATA_ROOT_SUBDIR / "7001/ns/orders.1001/0/recovery-point.json"
    ) == {
        "leo": 42,
        "segments": [],
    }


def test_dry_run_never_writes(tmp_path: Path) -> None:
    ws = _workspace(tmp_path, dry_run=True)
    writes: ConcurrentStableList[str] = ConcurrentStableList()
    mkdirs: ConcurrentStableList[str] = ConcurrentStableList()

    def record_write(path: Path, _data: bytes) -> None:
        writes.add(path.as_posix())

    def record_mkdir(path: Path) -> None:
        mkdirs.add(path.as_posix())

    io = replace(ws.io, write_bytes=record_write, mkdir=record_mkdir)
    assert backup.run(ws.config, io) == 0
    assert len(writes) == 0
    assert len(mkdirs) == 0
    preview = cast("dict[str, Any]", json.loads(ws.outputs[0]))
    assert preview["dry_run"] is True
    assert preview["backup_id"] == "20261110T050000Z"
    assert preview["files"] == []
    assert preview["offsets"]["count"] == 2
    assert preview["metadata"]["covered"] is True
    assert preview["log_segments"]["backed_up"] is False
    assert sorted(ws.backup_root.iterdir()) == []  # 指针 / staging / 快照都不落盘


# ---------------------------------------------------------------------------
# T4 正常备份与 manifest
# ---------------------------------------------------------------------------


def test_normal_backup_manifest_and_pointer(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    assert backup.run(ws.config, ws.io) == 0
    identifier = backup.backup_id(_NOW)
    target = _published(ws, identifier)
    assert target.is_dir()
    assert (ws.backup_root / f"{backup.BACKUP_STAGING_PREFIX}{identifier}").exists() is False
    manifest = _manifest(ws, identifier)
    assert manifest["schema_version"] == backup.BACKUP_MANIFEST_SCHEMA_VERSION
    assert manifest["backup_id"] == identifier
    assert manifest["dry_run"] is False
    assert manifest["service"]["version"] == "0.1.0"
    assert manifest["service"]["contract_version"] == "0.1.0"
    assert len(manifest["config_version"]) == 64
    assert manifest["tenants"] == ["7001", "7002"]
    assert manifest["metadata"]["covered"] is True
    assert manifest["metadata"]["tables_expected"] == ["msg_topic"]
    assert manifest["offsets"]["count"] == 2
    assert manifest["offsets"]["bytes"] == 2 * len(_STABLE)
    assert manifest["log_segments"]["backed_up"] is False
    assert manifest["log_segments"]["partitions"][0]["leo"] == 42
    assert manifest["log_segments"]["partitions"][0]["leo_source"] == "recovery-point"
    assert manifest["log_segments"]["needs_alignment"] == []
    assert len(manifest["recovery_order"]) == 4
    assert manifest["retention_days"] == 7
    assert len(manifest["files"]) == 2
    for row in manifest["files"]:
        raw = (target / row["path"]).read_bytes()
        assert len(raw) == row["size"]
        assert backup.sha256_hex(raw) == row["sha256"]
    for entry in manifest["offsets"]["files"]:
        assert (target / "offsets" / entry["path"]).read_bytes() == _STABLE
        assert entry["sha256"] == backup.sha256_hex(_STABLE)
    pointer = cast("dict[str, Any]", json.loads((ws.backup_root / backup.BACKUP_POINTER_NAME).read_text("utf-8")))
    assert pointer["backup_id"] == identifier
    assert pointer["manifest"] == backup.BACKUP_MANIFEST_NAME
    assert ws.outputs[0].startswith("[OK] backup.published")
    assert not ws.alerts


def test_log_summary_without_recovery_point(tmp_path: Path) -> None:
    ws = _workspace(tmp_path, with_recovery=False)
    plan = backup.build_plan(ws.config, ws.io)
    segments = cast("dict[str, Any]", plan.segments)
    row = cast("dict[str, Any]", segments["partitions"][0])
    assert row["leo_source"] == "segments_only"
    assert row["leo"] == 1
    assert segments["needs_alignment"] == ["7001/ns/orders.1001/0"]
    assert segments["backed_up"] is False


def test_tenant_ids_and_leo_branches(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    files = ConcurrentStableList([Path("7001/g/1-0.offset")])
    partitions = ConcurrentStableList([ws.data_root / backup.DATA_ROOT_SUBDIR / "7002/ns/t.1/0"])
    assert list(backup.tenant_ids(files, partitions, ws.data_root)) == ["7001", "7002"]
    assert list(backup.tenant_ids(ConcurrentStableList(), ConcurrentStableList(), ws.data_root)) == []
    partition = ws.data_root / backup.DATA_ROOT_SUBDIR / "7001/ns/orders.1001/0"
    assert backup.leo_of(partition, ConcurrentStableList([3, 7]), ws.io) == (42, "recovery-point")

    def read_text(_path: Path) -> object:
        return "not-json"

    def read_without_key(_path: Path) -> object:
        return {"other": 1}

    def read_bad_leo(_path: Path) -> object:
        return {"leo": "abc"}

    def read_list(_path: Path) -> object:
        return [1]

    bad_json = replace(ws.io, read_json=read_text)
    assert backup.leo_of(partition, ConcurrentStableList([3, 7]), bad_json) == (8, "segments_only")
    no_key = replace(ws.io, read_json=read_without_key)
    assert backup.leo_of(partition, ConcurrentStableList(), no_key) == (0, "segments_only")
    wrong_type = replace(ws.io, read_json=read_bad_leo)
    assert backup.leo_of(partition, ConcurrentStableList([1]), wrong_type) == (2, "segments_only")
    not_mapping = replace(ws.io, read_json=read_list)
    assert backup.leo_of(partition, ConcurrentStableList(), not_mapping) == (0, "segments_only")


def test_service_versions_and_digest(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    versions = backup.service_versions(ws.repo, ws.io)
    assert versions.get("name") == "msg"
    assert versions.get("contract_version") == "0.1.0"
    _write(ws.repo / "deploy/registry/msg.toml", 'product_key = "msg"\n')
    fallback = backup.service_versions(ws.repo, ws.io)
    assert fallback.get("contract_version") == ""
    assert len(backup.config_digest(ws.repo, ws.io)) == 64


def test_plain_and_render() -> None:
    assert backup.plain(None) is None
    assert backup.plain("文本") == "文本"
    assert backup.plain(3) == 3
    assert backup.plain(True) is True
    assert backup.plain(Path("a/b")) == "a/b"
    mapping = ConcurrentStableDict[str, object]()
    mapping.set("items", ConcurrentStableList([1, "二"]))
    assert backup.plain(mapping) == {"items": [1, "二"]}
    assert backup.plain(ConcurrentStableList([Path("x")])) == ["x"]
    assert isinstance(backup.plain(object()), str)
    payload = ConcurrentStableDict[str, object]()
    payload.set("a", 1)
    assert json.loads(backup.render(payload)) == {"a": 1}


# ---------------------------------------------------------------------------
# T5 一致性快照：并发写入重试 / 校验和不符
# ---------------------------------------------------------------------------


def test_copy_offsets_retry_then_stable(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    plan = backup.build_plan(ws.config, ws.io)
    source = ws.data_root / backup.OFFSETS_SUBDIR / "7001/group-abcdef12/1001-0.offset"
    staging = ws.backup_root / "staging"
    memory: ConcurrentStableDict[Path, bytes] = ConcurrentStableDict()
    sequence = [b"changed", b"changed", _STABLE, _STABLE]

    def read_bytes(path: Path) -> bytes:
        if "staging" in path.parts:
            return memory.get(path, b"")
        if path == source:
            return sequence.pop(0)
        return _STABLE

    def write_bytes(path: Path, data: bytes) -> None:
        memory.set(path, data)

    io = replace(ws.io, read_bytes=read_bytes, write_bytes=write_bytes)
    backup.copy_offsets(ws.config, staging, plan, io)
    assert memory.get(staging / "offsets/7001/group-abcdef12/1001-0.offset") == _STABLE


def test_copy_offsets_retry_exhausted(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    plan = backup.build_plan(ws.config, ws.io)
    staging = ws.backup_root / "staging"

    def always_changing(_path: Path) -> bytes:
        return b"always-changing"

    def no_write(_path: Path, _data: bytes) -> None:
        return None

    io = replace(ws.io, read_bytes=always_changing, write_bytes=no_write)
    with pytest.raises(backup.BackupError, match="校验和未稳定"):
        backup.copy_offsets(ws.config, staging, plan, io)


def test_verify_snapshot_mismatch(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    plan = backup.build_plan(ws.config, ws.io)
    staging = ws.backup_root / "staging"
    backup.copy_offsets(ws.config, staging, plan, ws.io)
    rows = backup.snapshot_entries(staging, ws.io)
    backup.verify_snapshot(staging, rows, ws.io)

    def tampered(_path: Path) -> bytes:
        return b"tampered"

    def same_size(_path: Path) -> bytes:
        return b"X" * len(_STABLE)

    with pytest.raises(backup.BackupError, match="副本校验和不符"):
        backup.verify_snapshot(staging, rows, replace(ws.io, read_bytes=tampered))
    with pytest.raises(backup.BackupError, match="副本校验和不符"):
        backup.verify_snapshot(staging, rows, replace(ws.io, read_bytes=same_size))


# ---------------------------------------------------------------------------
# T7/T8 空间不足、失败保护、同标识冲突、IO 异常
# ---------------------------------------------------------------------------


def test_space_shortage_fails_early(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)

    def no_space(_path: Path) -> int:
        return 0

    def forbidden_mkdir(_path: Path) -> None:
        pytest.fail("空间不足不得建 staging")

    io = replace(ws.io, free_bytes=no_space, mkdir=forbidden_mkdir)
    assert backup.run(ws.config, io) == 1
    assert any("backup.space" in line for line in ws.alerts)
    assert any(line.startswith("[FAIL]") for line in ws.alerts)
    assert any(line.startswith("[ALERT]") for line in ws.alerts)
    assert sorted(ws.backup_root.iterdir()) == []


def test_mid_failure_preserves_previous(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    assert backup.run(ws.config, ws.io) == 0
    first_manifest = _manifest(ws, backup.backup_id(_NOW))
    first_pointer = (ws.backup_root / backup.BACKUP_POINTER_NAME).read_bytes()
    second = replace(ws.config, now=_NOW + timedelta(days=1))

    def failing_write(path: Path, data: bytes) -> None:
        if path.name.endswith(".offset"):
            raise OSError("injected write failure")
        backup.fs_write_bytes(path, data)

    io = replace(ws.io, write_bytes=failing_write)
    assert backup.run(second, io) == 1
    assert _manifest(ws, backup.backup_id(_NOW)) == first_manifest
    assert (ws.backup_root / backup.BACKUP_POINTER_NAME).read_bytes() == first_pointer
    assert (ws.backup_root / f"{backup.BACKUP_STAGING_PREFIX}{backup.backup_id(second.now)}").exists() is False
    assert any("backup.io" in line for line in ws.alerts)


def test_replace_failure_and_same_id_conflict(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    assert backup.run(ws.config, ws.io) == 0
    first_manifest = _manifest(ws, backup.backup_id(_NOW))
    second = replace(ws.config, now=_NOW + timedelta(days=1))

    def failing_replace(_source: Path, _target: Path) -> None:
        raise OSError("injected rename failure")

    assert backup.run(second, replace(ws.io, replace=failing_replace)) == 1
    assert _manifest(ws, backup.backup_id(_NOW)) == first_manifest
    assert backup.run(ws.config, ws.io) == 1
    assert any("backup.publish" in line for line in ws.alerts)


def test_run_oserror_is_reported(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)

    def failing_list(_root: Path) -> ConcurrentStableList[Path]:
        raise OSError("injected source failure")

    assert backup.run(ws.config, replace(ws.io, list_offset_files=failing_list)) == 1
    assert any("backup.io" in line for line in ws.alerts)


def test_build_plan_without_dump(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)

    def no_dump(_path: Path) -> Path | None:
        return None

    assert backup.run(ws.config, replace(ws.io, find_dump=no_dump)) == 1
    assert any("平台库备份产物不存在" in line for line in ws.alerts)


def test_cleanup_staging_only(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    removed: ConcurrentStableList[str] = ConcurrentStableList()

    def record_remove(path: Path) -> None:
        removed.add(path.name)

    backup.cleanup_staging(ws.config, replace(ws.io, remove_tree=record_remove))
    assert list(removed) == [f"{backup.BACKUP_STAGING_PREFIX}{backup.backup_id(_NOW)}"]


# ---------------------------------------------------------------------------
# T9 保留期清理
# ---------------------------------------------------------------------------


def _seed_backup_dir(ws: Workspace, days_ago: int) -> str:
    identifier = backup.backup_id(_NOW - timedelta(days=days_ago))
    path = ws.backup_root / identifier
    path.mkdir()
    _write(path / backup.BACKUP_MANIFEST_NAME, "{}")
    return identifier


def test_purge_keeps_newest_pointer_and_unexpired(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    newest = _seed_backup_dir(ws, 0)
    pointed = _seed_backup_dir(ws, 10)
    expired = _seed_backup_dir(ws, 20)
    fresh = _seed_backup_dir(ws, 3)
    _write(ws.backup_root / backup.BACKUP_POINTER_NAME, json.dumps({"backup_id": pointed}))
    backup.purge(ws.config, ws.io)
    assert _published(ws, newest).is_dir()
    assert _published(ws, pointed).is_dir()
    assert _published(ws, fresh).is_dir()
    assert _published(ws, expired).exists() is False


def test_purge_empty_and_pointer_failures(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    backup.purge(ws.config, ws.io)  # 无可用快照目录
    _seed_backup_dir(ws, 0)
    old = _seed_backup_dir(ws, 20)
    assert _published(ws, old).is_dir()

    def bad_json(_path: Path) -> object:
        raise ValueError("bad json")

    def not_mapping(_path: Path) -> object:
        return [1, 2]

    for reader in (bad_json, not_mapping):
        backup.purge(ws.config, replace(ws.io, read_json=reader))
    assert _published(ws, old).exists() is False  # 指针读失败时按过期判据照常清理
    backup.purge(ws.config, ws.io)  # 无 current.json → OSError 分支


def test_purge_single_failure_only_alerts(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    _seed_backup_dir(ws, 0)
    _seed_backup_dir(ws, 20)

    def failing_remove(_path: Path) -> None:
        raise OSError("injected purge failure")

    backup.purge(ws.config, replace(ws.io, remove_tree=failing_remove))
    assert any("backup.purge" in line for line in ws.alerts)
    assert len(ws.outputs) == 0


# ---------------------------------------------------------------------------
# T10/T11 调度模板、键位三处齐备、无硬编码
# ---------------------------------------------------------------------------


def test_template_and_key_contract() -> None:
    config_source = (_REPO / "backend/services/msg/src/bms_msg/core/config.py").read_text("utf-8")
    template = (_REPO / "deploy/.env.example").read_text("utf-8")
    script = (_REPO / "backend/ops/backup.py").read_text("utf-8")
    compose = (_REPO / "deploy/compose/msg.yml").read_text("utf-8")
    ignored = (_REPO / ".gitignore").read_text("utf-8")
    service = (_REPO / "deploy/backup/msg-backup.service").read_text("utf-8")
    timer = (_REPO / "deploy/backup/msg-backup.timer").read_text("utf-8")
    compose_env = compose.split("x-service-env:", 1)[1].split("\nservices:", 1)[0]

    for key in _BACKUP_KEYS:
        assert f'"{key}"' in config_source
        assert f"{key}=" in template
        assert key in cc.HOST_ONLY_KEYS
        assert key not in compose_env
    for key in _BACKUP_KEYS:
        line = next(line for line in template.splitlines() if line.startswith(f"{key}="))
        assert line.endswith("=")
    assert "var/backup/" in ignored
    assert "deploy/backup/*.env" in ignored
    assert "Type=oneshot" in service
    assert "EnvironmentFile=" in service
    assert "-m ops.backup" in service
    for placeholder in ("<检出根>", "<仓内解释器>", "<备份日志>"):
        assert placeholder in service
    assert "OnCalendar=*-*-* 13:00:00" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer
    for forbidden in ("/mnt/data", "/var/lib/msg", "load_dotenv", 'Path(".env")', "192.168."):
        assert forbidden not in script
        assert forbidden not in service
        assert forbidden not in timer


# ---------------------------------------------------------------------------
# T12 命令入口与退出码
# ---------------------------------------------------------------------------


def test_main_dry_run_and_missing_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    monkeypatch.setenv("MSG_BACKUP_DIR", str(ws.backup_root))
    monkeypatch.setenv("MSG_BACKUP_PLATFORM_DUMP_DIR", str(ws.dump_dir))
    monkeypatch.setenv("MSG_DATA_DIR", str(ws.data_root))
    assert backup.main(ConcurrentStableList(["--root", str(ws.repo), "--dry-run"])) == 0
    assert sorted(ws.backup_root.iterdir()) == []

    monkeypatch.delenv("MSG_BACKUP_DIR")
    assert backup.main(ConcurrentStableList(["--root", str(ws.repo)])) == 1


def test_main_without_argv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    monkeypatch.setenv("MSG_BACKUP_DIR", str(ws.backup_root))
    monkeypatch.setenv("MSG_BACKUP_PLATFORM_DUMP_DIR", str(ws.dump_dir))
    monkeypatch.setenv("MSG_DATA_DIR", str(ws.data_root))
    monkeypatch.setattr("sys.argv", ["prog", "--root", str(ws.repo), "--dry-run"])
    assert backup.main() == 0


def test_module_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    monkeypatch.setenv("MSG_BACKUP_DIR", str(ws.backup_root))
    monkeypatch.setenv("MSG_BACKUP_PLATFORM_DUMP_DIR", str(ws.dump_dir))
    monkeypatch.setenv("MSG_DATA_DIR", str(ws.data_root))
    monkeypatch.setattr("sys.argv", ["prog", "--root", str(ws.repo), "--dry-run"])
    with pytest.raises(SystemExit) as caught:
        runpy.run_path(str(Path(backup.__file__)), run_name="__main__")
    assert caught.value.code == 0
