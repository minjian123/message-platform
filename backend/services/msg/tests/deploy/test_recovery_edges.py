"""恢复演练的边界与失败分支（Kiwi 2476；任务 07-3-2 §13 补充）。

主链路见 `test_recovery.py`；本文件只补默认 IO、校验助手、失败分支与清理兜底，
用真实文件系统 + 可注入端口，不依赖网络与 Docker。
"""

from __future__ import annotations

import asyncio
import gzip
import http.client
import runpy
import shutil
import sys
import urllib.error
import urllib.request
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.broker.offset import OffsetSlot, encode_offset_file, group_directory_name
from ops import backup, recovery
from services.msg.tests.deploy.test_recovery import (
    NOW,
    Workspace,
    config_for,
    io_for,
    manifest_payload,
    partition_manifest,
    prepare_workspace,
    read_report,
    section,
    segment_dir,
    step_rows,
)

pytestmark = pytest.mark.kiwi_id(2476)


def _snapshot_manifest(workspace: Workspace) -> recovery.Manifest:
    """取一次真实备份的已解析清单。"""
    _, manifest = recovery.resolve_snapshot(config_for(workspace), recovery.filesystem_io())
    return manifest


def _offset_bytes(offset: int, *, group: str = "g1", topic_id: int = 1_001, partition_no: int = 0) -> bytes:
    """造一个合法的位点文件字节。"""
    return encode_offset_file(
        group=group,
        topic_id=topic_id,
        partition_no=partition_no,
        slots=(OffsetSlot(seq=1, offset=offset, updated_at_us=1), OffsetSlot(seq=0, offset=0, updated_at_us=0)),
    )


def _second_backup(workspace: Workspace) -> int:
    """在同一备份根再产出一份快照（用于回退候选标记）。"""
    config = backup.BackupConfig(
        repo_root=workspace.repo,
        data_root=workspace.data_root,
        backup_root=workspace.backup_root,
        platform_dump_dir=workspace.dump_dir,
        keep_days=7,
        space_factor=1.2,
        dry_run=False,
        now=NOW + timedelta(seconds=1),
    )
    return backup.run(config, backup.filesystem_io())


# ---------------------------------------------------------------------------
# 默认 IO
# ---------------------------------------------------------------------------


def test_default_io_helpers(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """默认 IO：复制 / 遍历 / 大小 / 休眠 / 输出都走真实实现。"""
    io = recovery.filesystem_io()
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    target = tmp_path / "target"
    target.mkdir()
    (target / "stale.txt").write_text("stale", encoding="utf-8")
    io.copy_tree(source, target)
    assert (target / "a.txt").is_file() and not (target / "stale.txt").exists()
    assert io.list_tree(target) == ConcurrentStableList([Path("a.txt")])
    assert io.list_tree(tmp_path / "missing") == ConcurrentStableList()
    assert io.file_size(target / "a.txt") == 1
    io.sleep_ms(0)
    io.output("out")
    io.alert("alert")
    captured = capsys.readouterr()
    assert captured.out.strip() == "out"
    assert captured.err.strip() == "alert"
    io.remove_tree(target)
    io.remove_tree(target)


def test_default_command_and_http(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """默认命令 / HTTP：成功、非零退出、超时、2xx、HTTPError、URLError。"""
    ok = recovery.fs_run_command(f'"{sys.executable}" -c "print(123)"', 20)
    assert ok.exit_code == 0 and "123" in ok.stdout and not ok.timed_out
    failed = recovery.fs_run_command(f'"{sys.executable}" -c "import sys; sys.exit(3)"', 20)
    assert failed.exit_code == 3
    timed_out = recovery.fs_run_command(f'"{sys.executable}" -c "import time; time.sleep(2)"', 1)
    assert timed_out.timed_out is True and timed_out.exit_code == -1

    class _Response:
        status = 204

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *args: object) -> bool:
            return False

    def raise_http(url: str, timeout: float) -> _Response:
        raise urllib.error.HTTPError(url, 404, "missing", http.client.HTTPMessage(), None)

    def raise_url(url: str, timeout: float) -> _Response:
        raise urllib.error.URLError("down")

    def ok_urlopen(url: str, timeout: float) -> _Response:
        return _Response()

    monkeypatch.setattr(urllib.request, "urlopen", ok_urlopen)
    assert recovery.fs_http_status("http://example", 1.0) == 204
    monkeypatch.setattr(urllib.request, "urlopen", raise_http)
    assert recovery.fs_http_status("http://example", 1.0) == 404
    monkeypatch.setattr(urllib.request, "urlopen", raise_url)
    assert recovery.fs_http_status("http://example", 1.0) == 0


def test_default_backup_runner(tmp_path: Path) -> None:
    """默认 IO 的 `run_backup` 直接调 `ops.backup`。"""
    workspace = prepare_workspace(tmp_path)
    config = backup.BackupConfig(
        repo_root=workspace.repo,
        data_root=workspace.data_root,
        backup_root=workspace.backup_root,
        platform_dump_dir=workspace.dump_dir,
        keep_days=7,
        space_factor=1.2,
        dry_run=True,
        now=NOW,
    )
    assert recovery.fs_run_backup(config) == 0


# ---------------------------------------------------------------------------
# 纯助手
# ---------------------------------------------------------------------------


def test_validation_helpers_reject_bad_shapes() -> None:
    """清单 / 配置助手的所有非法形状都指名失败。"""
    with pytest.raises(recovery.RecoveryError, match="created_at"):
        recovery.parse_manifest_time("not-a-time")
    env = ConcurrentStableDict({"K": "bad"})
    with pytest.raises(recovery.RecoveryError, match="须为正整数"):
        recovery.positive_int(env, "K", 1)
    env.set("K", "0")
    with pytest.raises(recovery.RecoveryError, match="须为正整数"):
        recovery.positive_int(env, "K", 1)
    env.set("K", "7")
    assert recovery.positive_int(env, "K", 1) == 7
    env.set("K", "")
    assert recovery.positive_int(env, "K", 1) == 1

    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    payload.set("flag", True)
    payload.set("text", 1)
    payload.set("list", "not-list")
    payload.set("texts", ConcurrentStableList([1]))
    payload.set("ints", ConcurrentStableList([True]))
    with pytest.raises(recovery.RecoveryError):
        recovery.as_int(payload, "flag", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_text(payload, "text", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_intish(payload, "flag", "x")
    payload.set("bad_text", "abc")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_intish(payload, "bad_text", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_mapping(payload, "text", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_list(payload, "list", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_text_list(payload, "texts", "x")
    with pytest.raises(recovery.RecoveryError):
        recovery.as_int_list(payload, "ints", "x")
    payload.set("num", 5)
    assert recovery.as_intish(payload, "num", "x") == 5
    digits: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    digits.set("topic_id", "1001")
    assert recovery.as_intish(digits, "topic_id", "x") == 1_001
    with pytest.raises(recovery.RecoveryError):
        recovery.parse_file_entries(ConcurrentStableList([1]), "files")
    with pytest.raises(recovery.RecoveryError):
        recovery.parse_partitions(ConcurrentStableList([1]))


def test_manifest_time_and_pointer_readers(tmp_path: Path) -> None:
    """枚举读取在 JSON 损坏 / 形状不符时返回空串，不抛错。"""
    io = recovery.filesystem_io()
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "manifest.json").write_text("{", encoding="utf-8")
    assert recovery.manifest_time_of(bad, io) == ""
    (bad / "manifest.json").write_text("[]", encoding="utf-8")
    assert recovery.manifest_time_of(bad, io) == ""
    (tmp_path / "current.json").write_text("[]", encoding="utf-8")
    assert recovery.current_backup_id(tmp_path, io) == ""
    (tmp_path / "current.json").write_text("{", encoding="utf-8")
    assert recovery.current_backup_id(tmp_path, io) == ""


def test_resolve_snapshot_failures(tmp_path: Path) -> None:
    """无候选 / 缺清单 / 清单不可解析都判失败。"""
    workspace = prepare_workspace(tmp_path)
    empty = tmp_path / "empty"
    empty.mkdir()
    config = replace(config_for(workspace), backup_root=empty)
    with pytest.raises(recovery.RecoveryError, match="没有可用快照"):
        recovery.resolve_snapshot(config, recovery.filesystem_io())
    with pytest.raises(recovery.RecoveryError, match=r"manifest\.json"):
        recovery.resolve_snapshot(replace(config, backup_id="20200101T000000Z"), recovery.filesystem_io())

    broken = empty / "20200101T000000Z"
    broken.mkdir()
    (broken / "manifest.json").write_text("{", encoding="utf-8")
    with pytest.raises(recovery.RecoveryError, match="清单不可解析"):
        recovery.resolve_snapshot(config, recovery.filesystem_io())


def test_parse_manifest_root_and_metadata_source(tmp_path: Path) -> None:
    """清单根不是对象 / metadata.source 不符 / offsets 未进 files 都拒绝。"""
    workspace = prepare_workspace(tmp_path)
    with pytest.raises(recovery.RecoveryError, match="清单根"):
        recovery.parse_manifest(recovery.stable_json([]), directory_name="x")
    payload = manifest_payload(workspace)
    cast("dict[str, object]", payload["metadata"])["source"] = "other"
    with pytest.raises(recovery.RecoveryError, match=r"metadata\.source"):
        recovery.parse_manifest(recovery.stable_json(payload), directory_name=workspace.backup_id)

    missing = manifest_payload(workspace)
    files = cast("list[object]", missing["files"])
    files[:] = [
        item for item in files if not cast("str", cast("dict[str, object]", item)["path"]).startswith("offsets/")
    ]
    with pytest.raises(recovery.RecoveryError, match="快照清单缺"):
        recovery.parse_manifest(recovery.stable_json(missing), directory_name=workspace.backup_id)


def test_snapshot_verify_branches(tmp_path: Path) -> None:
    """快照复算：缺文件 / 大小不符 / offsets 未登记进 files。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)
    io = recovery.filesystem_io()
    snapshot = workspace.backup_root / workspace.backup_id
    first = manifest.files[0]
    target = snapshot / first.path
    original = target.read_bytes()
    target.write_bytes(original + b"x")
    with pytest.raises(recovery.RecoveryError, match=r"size|sha256"):
        recovery.verify_snapshot(snapshot, manifest, io)
    target.unlink()
    with pytest.raises(recovery.RecoveryError, match="missing"):
        recovery.verify_snapshot(snapshot, manifest, io)
    target.write_bytes(original)
    trimmed = replace(manifest, files=ConcurrentStableFrozenList(manifest.files[1:]))
    with pytest.raises(recovery.RecoveryError, match="offsets_missing"):
        recovery.verify_snapshot(snapshot, trimmed, io)


def test_scan_segments_empty_and_excluded(tmp_path: Path) -> None:
    """空分区目录按段尾缺失处理；显式排除的分区不参与。"""
    io = recovery.filesystem_io()
    root = tmp_path / "target"
    partition = root / "data" / "7001" / "orders" / "orders.events.1001" / "0"
    partition.mkdir(parents=True)
    manifest = partition_manifest(ConcurrentStableFrozenList([0, 1]))
    empty = recovery.scan_segments(root, manifest, io)
    assert empty.degraded is True
    assert empty.partitions[0].tail_missing == ConcurrentStableFrozenList([0, 1])
    skipped = recovery.scan_segments(root, manifest, io, ConcurrentStableFrozenList([manifest.partitions[0].label]))
    assert not skipped.partitions and skipped.degraded is False


def test_metadata_and_command_branches(tmp_path: Path) -> None:
    """dump 缺失 / 缺表、命令结果为空与超时。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)
    empty = tmp_path / "empty-dump"
    empty.mkdir()
    with pytest.raises(recovery.RecoveryError, match="最近产物"):
        recovery.verify_metadata(
            replace(config_for(workspace), platform_dump_dir=empty), manifest, recovery.filesystem_io()
        )
    with pytest.raises(recovery.RecoveryError, match="最近产物"):
        recovery.verify_metadata(
            replace(config_for(workspace), platform_dump_dir=tmp_path / "nope"), manifest, recovery.filesystem_io()
        )
    missing_table = tmp_path / "missing-table"
    missing_table.mkdir()
    (missing_table / manifest.metadata_dump).write_bytes(gzip.compress(b"CREATE TABLE `other` (id INT);\n"))
    with pytest.raises(recovery.RecoveryError, match="dump 缺表"):
        recovery.verify_metadata(
            replace(config_for(workspace), platform_dump_dir=missing_table), manifest, recovery.filesystem_io()
        )
    assert cast("int", recovery.command_detail(None)["exit_code"]) == -1

    def timeout_command(command: str, seconds: int) -> recovery.CommandResult:
        return recovery.CommandResult(0, "", "", 1, timed_out=True)

    timeout_io = replace(
        recovery.filesystem_io(),
        run_command=timeout_command,
    )
    with pytest.raises(recovery.RecoveryError, match="超时"):
        recovery.run_command_once(config_for(workspace), timeout_io, "stop", "stop")


def test_offset_and_partition_identity_branches(tmp_path: Path) -> None:
    """位点文件身份：后缀 / 数字 / 头字段 / 路径长度不符都返回 None。"""
    assert recovery.split_offset_name("x") == (None, None)
    assert recovery.split_offset_name("a-b.offset") == (None, None)
    assert recovery.offset_identity(Path("a.offset"), b"junk") is None
    assert recovery.offset_identity(Path("7001/g/1001-0.offset"), b"junk") is None
    good = _offset_bytes(3)
    assert recovery.offset_identity(Path("1001-0.offset"), good) is None
    assert recovery.offset_identity(Path("7001/g/1001.offset"), good) is None
    assert recovery.offset_identity(Path("7001/g/1002-0.offset"), good) is None
    assert recovery.offset_identity(Path("7001/g/1001-0.offset"), good) == ("g1", 1_001, 0)
    assert recovery.partition_identity(Path("stuff/0")) == (0, 0)
    assert recovery.partition_identity(Path("stuff/orders.1001/0")) == (1_001, 0)


def test_target_last_write_uses_mtime(tmp_path: Path) -> None:
    """没有恢复点时退化为段文件 mtime。"""
    root = tmp_path / "target"
    partition = root / "data" / "7001" / "orders" / "orders.events.1001" / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000000.log").write_bytes(b"")
    stamp, source = recovery.target_last_write_us(root, recovery.filesystem_io())
    assert stamp is not None and source == "mtime"


# ---------------------------------------------------------------------------
# 对齐 / 位点 / 抽样 / 延迟
# ---------------------------------------------------------------------------


def test_align_absent_and_load_failure(tmp_path: Path) -> None:
    """缺席分区跳过；分区无法加载时降级并继续。"""
    manifest = partition_manifest(ConcurrentStableFrozenList([0]))
    absent = recovery.SegmentReport(
        partitions=ConcurrentStableFrozenList(),
        absent=ConcurrentStableFrozenList([manifest.partitions[0].label]),
        degraded=True,
    )
    detail, degraded, reasons = asyncio.run(recovery.align_partitions(tmp_path, manifest, absent))
    assert detail["partitions"] == ConcurrentStableFrozenList()
    assert not degraded and not reasons

    empty = recovery.SegmentReport(
        partitions=ConcurrentStableFrozenList(), absent=ConcurrentStableFrozenList(), degraded=False
    )
    detail, degraded, reasons = asyncio.run(recovery.align_partitions(tmp_path, manifest, empty))
    assert degraded is True and reasons
    assert cast("list[object]", detail["partitions"])


def test_verify_offsets_failure_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """位点身份坏 / 分区不可加载 / 越过日志尾 / 低于起点 / 抽样截断与空读。"""
    workspace = prepare_workspace(tmp_path)
    root = workspace.data_root
    proper = root / "offsets" / "7001" / group_directory_name("g1") / "1001-0.offset"
    proper.parent.mkdir(parents=True, exist_ok=True)
    proper.write_bytes(b"junk")
    with pytest.raises(recovery.RecoveryError, match=r"recovery\.offset\.corrupt"):
        asyncio.run(recovery.verify_offsets_and_samples(config_for(workspace), root, recovery.filesystem_io()))

    proper.write_bytes(_offset_bytes(99))
    with pytest.raises(recovery.RecoveryError, match="越过日志尾"):
        asyncio.run(recovery.verify_offsets_and_samples(config_for(workspace), root, recovery.filesystem_io()))

    proper.write_bytes(_offset_bytes(0))
    untouched, untouched_degraded, _ = asyncio.run(
        recovery.verify_offsets_and_samples(config_for(workspace), root, recovery.filesystem_io())
    )
    assert untouched_degraded is False
    assert cast("list[object]", untouched["files"])

    empty_root = tmp_path / "empty-root"
    (empty_root / "offsets" / "7001" / group_directory_name("g1")).mkdir(parents=True)
    (empty_root / "offsets" / "7001" / group_directory_name("g1") / "1001-0.offset").write_bytes(_offset_bytes(0))
    detail, degraded, reasons = asyncio.run(
        recovery.verify_offsets_and_samples(config_for(workspace), empty_root, recovery.filesystem_io())
    )
    assert degraded is True and reasons and cast("list[object]", detail["files"])

    below = tmp_path / "below"
    partition = below / "data" / "7001" / "orders" / "orders.events.1001" / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000010.log").write_bytes(b"")
    (below / "offsets" / "7001" / group_directory_name("g1")).mkdir(parents=True)
    (below / "offsets" / "7001" / group_directory_name("g1") / "1001-0.offset").write_bytes(_offset_bytes(5))
    detail, degraded, reasons = asyncio.run(
        recovery.verify_offsets_and_samples(
            replace(config_for(workspace), sample_limit=0), below, recovery.filesystem_io()
        )
    )
    assert degraded is True
    assert any("低于可用起点" in reason for reason in reasons)
    assert cast("bool", detail["truncated"]) is True

    truncated = prepare_workspace(tmp_path / "small-sample")
    small = replace(config_for(truncated), sample_per_partition=0)
    with pytest.raises(recovery.RecoveryError, match="已确认消息不可读"):
        asyncio.run(recovery.verify_offsets_and_samples(small, truncated.data_root, recovery.filesystem_io()))

    async def duplicate_fetch(self: object, **kwargs: object) -> ConcurrentStableList[object]:
        return ConcurrentStableList([SimpleNamespace(offset=2), SimpleNamespace(offset=2)])

    monkeypatch.setattr(recovery.FileMessageStore, "fetch", duplicate_fetch)
    with pytest.raises(recovery.RecoveryError, match="抽样偏移非法"):
        asyncio.run(
            recovery.verify_offsets_and_samples(config_for(truncated), truncated.data_root, recovery.filesystem_io())
        )

    async def late_fetch(self: object, **kwargs: object) -> ConcurrentStableList[object]:
        return ConcurrentStableList([SimpleNamespace(offset=99)])

    monkeypatch.setattr(recovery.FileMessageStore, "fetch", late_fetch)
    with pytest.raises(recovery.RecoveryError, match="已确认消息不可读"):
        asyncio.run(
            recovery.verify_offsets_and_samples(config_for(truncated), truncated.data_root, recovery.filesystem_io())
        )
    monkeypatch.undo()

    two = prepare_workspace(tmp_path / "two-groups")
    second = two.data_root / "offsets" / "7001" / group_directory_name("g2") / "1001-0.offset"
    second.parent.mkdir(parents=True)
    second.write_bytes(_offset_bytes(4, group="g2"))
    detail, _, _ = asyncio.run(
        recovery.verify_offsets_and_samples(
            replace(config_for(two), sample_limit=1), two.data_root, recovery.filesystem_io()
        )
    )
    assert cast("bool", detail["truncated"]) is True


def test_delay_expectations_unknown_and_clear(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """延迟期望集合：未知 kind 跳过、clear 覆盖 put。"""
    data_root = tmp_path / "data"
    partition = data_root / "data" / "7001" / "orders" / "orders.events.1001" / "0"
    partition.mkdir(parents=True)
    (partition / "00000000000000000000.delay").write_bytes(b"")
    fake_entry = SimpleNamespace(
        header=SimpleNamespace(log_id="x", delay=SimpleNamespace(kind="weird", deliver_at_us=1))
    )
    no_stamp = SimpleNamespace(header=SimpleNamespace(log_id="y", delay=None))
    put = SimpleNamespace(header=SimpleNamespace(log_id="z", delay=SimpleNamespace(kind="put", deliver_at_us=9)))
    clear = SimpleNamespace(header=SimpleNamespace(log_id="z", delay=SimpleNamespace(kind="clear", deliver_at_us=0)))

    def fake_scan(self: object, verify_crc: bool = True) -> SimpleNamespace:
        return SimpleNamespace(entries=(fake_entry, no_stamp, put, clear))

    monkeypatch.setattr(recovery.DelaySegment, "scan", fake_scan)
    assert recovery.delay_expectations(ConcurrentStableList([partition])) == ConcurrentStableDict()
    monkeypatch.undo()

    seeded = prepare_workspace(tmp_path / "cleared")
    directory = recovery.PartitionDirectory(root=seeded.data_root)
    delay_store = recovery.FileDelayStore(directory=directory, fsync_mode="os")
    asyncio.run(delay_store.rebuild())
    asyncio.run(delay_store.remove(log_id="delay-1"))
    asyncio.run(delay_store.aclose())
    expected = recovery.delay_expectations(recovery.filesystem_io().list_partitions(seeded.data_root))
    assert "delay-1" not in expected


def test_verify_delay_missing_extra_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """延迟集合：缺 / 多 / 改期都判降级；被排除的分区不参与。"""
    workspace = prepare_workspace(tmp_path)
    excluded_label = "/".join(segment_dir(workspace).relative_to(workspace.data_root / "data").parts[:4])
    detail, degraded, reasons = asyncio.run(
        recovery.verify_delay(
            workspace.data_root, recovery.filesystem_io(), ConcurrentStableFrozenList([excluded_label])
        )
    )
    assert degraded is False and cast("int", detail["expected"]) == 0

    def fake_expectations(partitions: ConcurrentStableList[Path]) -> ConcurrentStableDict[str, int]:
        return ConcurrentStableDict({"missing": 1, "changed": 2})

    monkeypatch.setattr(recovery, "delay_expectations", fake_expectations)
    detail, degraded, reasons = asyncio.run(
        recovery.verify_delay(workspace.data_root, recovery.filesystem_io(), ConcurrentStableFrozenList())
    )
    assert degraded is True
    assert any("集合不一致" in reason for reason in reasons)


def test_run_verification_failure_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """对齐 / 位点 / 延迟任一步抛错都记录失败步后上抛；降级原因逐条汇总。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)
    report = recovery.scan_segments(workspace.data_root, manifest, recovery.filesystem_io())

    async def degraded_offsets(
        *args: object, **kwargs: object
    ) -> tuple[ConcurrentStableDict[str, object], bool, ConcurrentStableList[str]]:
        return ConcurrentStableDict(), True, ConcurrentStableList(["offset-reason"])

    async def degraded_delay(
        *args: object, **kwargs: object
    ) -> tuple[ConcurrentStableDict[str, object], bool, ConcurrentStableList[str]]:
        return ConcurrentStableDict(), True, ConcurrentStableList(["delay-reason"])

    monkeypatch.setattr(recovery, "verify_offsets_and_samples", degraded_offsets)
    monkeypatch.setattr(recovery, "verify_delay", degraded_delay)
    steps: ConcurrentStableList[recovery.StepRecord] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    checks: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    asyncio.run(
        recovery.run_verification(
            config_for(workspace),
            workspace.data_root,
            manifest,
            report,
            ConcurrentStableFrozenList(),
            steps,
            reasons,
            checks,
            recovery.filesystem_io(),
        )
    )
    assert "offset-reason" in list(reasons) and "delay-reason" in list(reasons)
    monkeypatch.undo()

    def boom(*args: object, **kwargs: object) -> object:
        raise recovery.RecoveryError("boom")

    for name in ("align_partitions", "verify_offsets_and_samples", "verify_delay"):
        monkeypatch.setattr(recovery, name, boom)
        steps = ConcurrentStableList()
        reasons = ConcurrentStableList()
        checks = ConcurrentStableDict()
        with pytest.raises(recovery.RecoveryError, match="boom"):
            asyncio.run(
                recovery.run_verification(
                    config_for(workspace),
                    workspace.data_root,
                    manifest,
                    report,
                    ConcurrentStableFrozenList(),
                    steps,
                    reasons,
                    checks,
                    recovery.filesystem_io(),
                )
            )
        assert any(step.result == recovery.RESULT_FAILED for step in steps)
        monkeypatch.undo()


def test_prepare_copy_subset_and_missing_source(tmp_path: Path) -> None:
    """`--max-partitions` 只复制选中分区并登记排除项；源目录缺失即失败。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)
    second = replace(manifest.partitions[0], partition=1)
    (workspace.data_root / "data" / "7001" / "orders" / "orders.events.1001" / "1").mkdir()
    extended = replace(manifest, partitions=ConcurrentStableFrozenList([manifest.partitions[0], second]))
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    target, detail = recovery.prepare_target(
        replace(config_for(workspace), max_partitions=1), run_dir, extended, recovery.filesystem_io()
    )
    assert (target / "data").is_dir()
    assert detail["excluded"] == ConcurrentStableFrozenList(["7001/orders/orders.events.1001/1"])
    missing = replace(config_for(workspace), source_data_root=tmp_path / "nope")
    with pytest.raises(recovery.RecoveryError, match="源数据目录不存在"):
        recovery.prepare_target(missing, run_dir, manifest, recovery.filesystem_io())


def test_included_partitions_limit(tmp_path: Path) -> None:
    """分区上限只保留前 N 个真实存在的分区。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)
    second = manifest.partitions[0]
    extended = replace(
        manifest,
        partitions=ConcurrentStableFrozenList([second, replace(second, partition=1)]),
    )
    (workspace.data_root / "data" / "7001" / "orders" / "orders.events.1001" / "1").mkdir()
    selected = recovery.included_partitions(extended, workspace.data_root, recovery.filesystem_io(), limit=1)
    assert len(selected) == 1


def test_restore_offsets_detects_corruption(tmp_path: Path) -> None:
    """复制后复算不符即失败。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)

    def bad_copy(source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"bad")

    io = replace(recovery.filesystem_io(), copy_file=bad_copy)
    with pytest.raises(recovery.RecoveryError, match="恢复后校验和不符"):
        recovery.restore_offsets(
            config_for(workspace), workspace.backup_root / workspace.backup_id, tmp_path / "t", manifest, io
        )


def test_run_backup_once_failures(tmp_path: Path) -> None:
    """演练前备份失败 / 未产出指针都判失败。"""
    workspace = prepare_workspace(tmp_path)
    manifest = _snapshot_manifest(workspace)

    def failed_backup(config: backup.BackupConfig) -> int:
        return 1

    failed = replace(recovery.filesystem_io(), run_backup=failed_backup)
    with pytest.raises(recovery.RecoveryError, match="演练前备份失败"):
        recovery.run_backup_once(config_for(workspace), tmp_path / "run", manifest, failed)

    def silent_backup(config: backup.BackupConfig) -> int:
        return 0

    silent = replace(recovery.filesystem_io(), run_backup=silent_backup)
    with pytest.raises(recovery.RecoveryError, match=r"未产出 current\.json"):
        recovery.run_backup_once(config_for(workspace), tmp_path / "run2", manifest, silent)


def test_validate_config_guards(tmp_path: Path) -> None:
    """工作根与数据根重叠、抽样阈值非法都拒绝。"""
    workspace = prepare_workspace(tmp_path)
    io = recovery.filesystem_io()
    with pytest.raises(recovery.RecoveryError, match="不得等于数据根"):
        recovery.validate_config(replace(config_for(workspace), work_root=workspace.data_root), io)
    inside = workspace.data_root / "work"
    with pytest.raises(recovery.RecoveryError, match="不得位于数据根之下"):
        recovery.validate_config(replace(config_for(workspace), work_root=inside), io)
    with pytest.raises(recovery.RecoveryError, match="抽样阈值"):
        recovery.validate_config(replace(config_for(workspace), sample_limit=0), io)


def test_null_audit_andfallback_hint(tmp_path: Path) -> None:
    """空审计出口与回退提示。"""
    assert asyncio.run(recovery.NullAuditSink().emit(None)) is True
    workspace = prepare_workspace(tmp_path)
    assert recovery.fallback_hint(config_for(workspace), recovery.filesystem_io()) == "<上一份备份标识>"
    assert recovery.fallback_hint(
        replace(config_for(workspace), backup_root=tmp_path / "none"), recovery.filesystem_io()
    ) == ("<上一份备份标识>")


def test_config_version_and_issues_drift(tmp_path: Path) -> None:
    """配置文本缺失时留空并告警；配置漂移进入问题与处置。"""
    workspace = prepare_workspace(tmp_path)
    io = io_for(workspace)
    assert recovery.current_config_version(replace(config_for(workspace), repo_root=tmp_path / "nope"), io) == ""
    assert any("recovery.config" in line for line in workspace.alerts)
    manifest = _snapshot_manifest(workspace)
    issues = recovery.issues_rows(ConcurrentStableList(), None, manifest, "other-digest")
    assert any("配置漂移" in cast("str", cast("dict[str, object]", row)["summary"]) for row in issues)


def test_dry_run_gap_and_cleanup_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """dry-run 遇到缺口标降级；清理失败告警并保留现场。"""
    workspace = prepare_workspace(tmp_path)
    degraded_report = recovery.SegmentReport(
        partitions=ConcurrentStableFrozenList(), absent=ConcurrentStableFrozenList(["x"]), degraded=True
    )

    def fake_segments(*args: object, **kwargs: object) -> recovery.SegmentReport:
        return degraded_report

    monkeypatch.setattr(recovery, "scan_segments", fake_segments)
    assert recovery.run(config_for(workspace, dry_run=True), io_for(workspace)) == 2
    assert any(
        "日志段存在缺口" in str(reason)
        for reason in cast("list[object]", section(read_report(workspace), "conclusion")["reasons"])
    )
    monkeypatch.undo()

    def failing_remove(path: Path) -> None:
        if path.name.startswith("run-"):
            raise OSError("locked")
        if path.exists():
            shutil.rmtree(path)

    io = replace(io_for(workspace), remove_tree=failing_remove)
    assert recovery.run(replace(config_for(workspace), report_path=tmp_path / "cleanup.json"), io) == 2
    assert any("recovery.cleanup" in line for line in workspace.alerts)


def test_print_backups_empty_and_previous(tmp_path: Path) -> None:
    """枚举为空即失败；两份备份时标记回退候选。"""
    workspace = prepare_workspace(tmp_path)
    io = io_for(workspace)
    assert recovery.print_backups(tmp_path / "empty", io) == 1
    assert any("没有可用快照" in line for line in workspace.alerts)
    assert _second_backup(workspace) == 0
    assert recovery.print_backups(workspace.backup_root, io) == 0
    assert any("上一份" in line for line in workspace.outputs)


def test_build_config_missing_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """缺备份根 / 缺 dump 目录都指名键名。"""
    for key in ("MSG_BACKUP_DIR", "MSG_BACKUP_PLATFORM_DUMP_DIR", "MSG_RECOVERY_WORK_DIR", "MSG_DATA_DIR"):
        monkeypatch.delenv(key, raising=False)
    assert recovery.main(ConcurrentStableList(["--root", str(tmp_path), "--work-dir", str(tmp_path / "work")])) == 1
    assert "MSG_BACKUP_DIR" in capsys.readouterr().err
    monkeypatch.setenv("MSG_BACKUP_DIR", str(tmp_path / "backup"))
    assert recovery.main(ConcurrentStableList(["--root", str(tmp_path), "--work-dir", str(tmp_path / "work")])) == 1
    assert "MSG_BACKUP_PLATFORM_DUMP_DIR" in capsys.readouterr().err


def test_dry_run_failure_does_not_mark_skipped(tmp_path: Path) -> None:
    """dry-run 在清单步失败时不补 skipped 步骤（未进入完整流程）。"""
    workspace = prepare_workspace(tmp_path)
    assert recovery.run(config_for(workspace, dry_run=True, backup_id="20200101T000000Z"), io_for(workspace)) == 1
    steps = step_rows(read_report(workspace))
    assert any(step["step"] == "manifest" and step["result"] == recovery.RESULT_FAILED for step in steps)
    assert not any(step["result"] == recovery.RESULT_SKIPPED for step in steps)


def test_report_write_failure_alerts(tmp_path: Path) -> None:
    """记录写入失败只告警，不改变演练结论。"""
    workspace = prepare_workspace(tmp_path)

    def failing_write(path: Path, data: bytes) -> None:
        raise OSError("disk full")

    io = replace(io_for(workspace), write_bytes=failing_write)
    assert recovery.run(replace(config_for(workspace), report_path=tmp_path / "report.json"), io) == 0
    assert any("recovery.report" in line for line in workspace.alerts)


def test_module_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`python -m ops.recovery --list-backups` 走 `__main__` 分支。"""
    workspace = prepare_workspace(tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        ["recovery", "--list-backups", "--backup-root", str(workspace.backup_root)],
    )
    with pytest.raises(SystemExit) as raised:
        runpy.run_module("ops.recovery", run_name="__main__")
    assert raised.value.code == 0
