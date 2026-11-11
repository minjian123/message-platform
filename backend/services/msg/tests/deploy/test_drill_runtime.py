# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false
# 回放式替身（FakeHttp / FakeInstance / lambda 回调）故意不回放内部类型：本文件只证明判据取值
# 与调用序列，类型精确性由被测模块自身（ops/drill，strict 全绿）保证。
"""六类故障注入脚本 · 实例生命周期 / 代理 / 编排 / 命令行（Kiwi 2479；任务 07-6-1）。

与 `test_drill.py` 同一用例编号：那份覆盖键位 / 场景单源 / 记录契约 / 观测解析，本文件覆盖
代理四档、实例起停、水位收紧、场景注册与编排、命令行退出码。进程、HTTP、socket 全部替身
或本地回环，不启动真实实例、不连外部网络。
"""

from __future__ import annotations

import socket
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
)

from ops.drill import __main__ as drill_cli
from ops.drill import engine, proxy, runtime
from ops.drill.context import ScenarioOutcome, check
from ops.drill.env import DrillConfig, DrillError
from ops.drill.model import (
    RESULT_FAILED,
    RESULT_OK,
    Criterion,
    DrillReport,
    DrillScenario,
    PhaseSample,
)

pytestmark = pytest.mark.kiwi_id(2479)

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根（用于定位场景单源）。"""


def _config(tmp_path: Path, **overrides: object) -> DrillConfig:
    """构造演练配置（工作根落在 tmp_path）。

    Args:
        tmp_path: 临时目录。
        overrides: 覆盖字段。

    Returns:
        DrillConfig: 配置。
    """
    base = DrillConfig(
        work_dir=tmp_path,
        env="test",
        base_port=18500,
        report_dir=tmp_path / "reports",
        recovery_budget_ms=240_000,
        fill_mb=8,
        slow_write_mb=4,
        reject_ratio=0.0001,
        repo_root=REPO_ROOT,
    )
    if not overrides:
        return base
    merged = cast("Any", {**base.__dict__, **overrides})
    return DrillConfig(**merged)


class _FakeProcess:
    """假子进程（覆盖正常退出 / 强杀 / 超时三条路径）。"""

    def __init__(self, *, exit_after: int = 0) -> None:
        self.pid = 4321
        self.returncode: int | None = None
        self.killed = False
        self.terminated = False
        self._wait_calls = 0
        self._exit_after = exit_after

    def poll(self) -> int | None:
        """返回退出码。"""
        return self.returncode

    def kill(self) -> None:
        """强杀。"""
        self.killed = True
        self.returncode = -9

    def terminate(self) -> None:
        """优雅终止。"""
        self.terminated = True
        self.returncode = -15

    def wait(self, timeout: float | None = None) -> int:
        """等待退出（超过 ``_exit_after`` 次调用后自然退出）。"""
        self._wait_calls += 1
        if self.returncode is None and self._wait_calls > self._exit_after:
            self.returncode = 0
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake", timeout or 1.0)
        return self.returncode


# ---------------------------------------------------------------------------
# 客户端侧代理：四档
# ---------------------------------------------------------------------------


def _echo_server() -> tuple[socket.socket, int]:
    """起一个本地回显服务（用例内唯一的真实 socket 依赖）。

    Returns:
        tuple[socket.socket, int]: 监听套接字与端口。
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(16)

    def serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                data = conn.recv(1024)
                if data:
                    conn.sendall(data)

    threading.Thread(target=serve, daemon=True).start()
    return server, int(server.getsockname()[1])


def test_tcp_proxy_four_modes() -> None:
    """代理：直通可用、delay 仍通、loss=1 丢包、drop 直接断连。"""
    server, target_port = _echo_server()
    proxied = proxy.TcpProxy(target_port, delay_ms=1, loss=1.0)
    port = proxied.start()
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
            conn.sendall(b"ping")
            assert conn.recv(16) == b"ping"

        proxied.set_mode(proxy.MODE_DELAY)
        with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
            conn.sendall(b"slow")
            assert conn.recv(16) == b"slow"

        proxied.set_mode(proxy.MODE_LOSS)
        with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
            conn.sendall(b"lost")
            conn.settimeout(0.5)
            with pytest.raises(TimeoutError):
                conn.recv(16)

        proxied.set_mode(proxy.MODE_DROP)
        with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
            conn.settimeout(0.5)
            assert conn.recv(16) == b""
    finally:
        proxied.close()
        server.close()

    assert proxied.mode == proxy.MODE_DROP


def test_tcp_proxy_target_down_closes_client() -> None:
    """代理：目标端口不可达时关闭客户端连接（不泄漏）。"""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    dead_port = int(probe.getsockname()[1])
    probe.close()

    proxied = proxy.TcpProxy(dead_port)
    port = proxied.start()
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as conn:
            conn.settimeout(3.0)
            assert conn.recv(16) == b""
    finally:
        proxied.close()


def test_tcp_proxy_close_is_idempotent() -> None:
    """代理：关闭幂等（重复关闭不再抛错）。"""
    proxied = proxy.TcpProxy(1)
    proxied.start()
    proxied.close()
    proxied.close()


# ---------------------------------------------------------------------------
# 实例生命周期（进程与 HTTP 替身）
# ---------------------------------------------------------------------------


def test_instance_start_ready_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：启动、就绪、两档信号。"""
    instance = runtime.DrillInstance(_config(tmp_path), port=18501)
    process = _FakeProcess()
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: process)
    monkeypatch.setattr(runtime.httpx, "get", lambda *a, **k: httpx.Response(200, json={}))

    instance.start()
    instance.wait_ready()
    instance.signal("SIGKILL")
    instance.signal("SIGTERM")

    assert instance.starts == 1
    assert process.killed and process.terminated


def test_instance_reports_early_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：进程提前退出时报错并带日志尾部。"""
    instance = runtime.DrillInstance(_config(tmp_path), port=18502)
    instance.process = cast(
        "subprocess.Popen[bytes]",
        SimpleNamespace(
            poll=lambda: 3,
            returncode=3,
            pid=1,
            kill=lambda: None,
            terminate=lambda: None,
            wait=lambda timeout=None: 3,
        ),
    )
    instance.log_path.write_text("fatal-boot", encoding="utf-8")

    with pytest.raises(DrillError, match="提前退出"):
        instance.wait_ready()


def test_instance_ready_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：探针始终不就绪时按超时报错。"""
    instance = runtime.DrillInstance(_config(tmp_path), port=18503)
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: _FakeProcess())
    monkeypatch.setattr(runtime.httpx, "get", lambda *a, **k: httpx.Response(503, json={}))
    monkeypatch.setattr(runtime, "DRILL_READY_TIMEOUT_SEC", 0.01)
    monkeypatch.setattr(runtime.time, "sleep", lambda _s: None)

    with pytest.raises(DrillError, match="未在"):
        instance.wait_ready()


def test_instance_ready_tolerates_connection_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：连接错误按「未就绪」继续轮询，最终就绪即返回。"""
    instance = runtime.DrillInstance(_config(tmp_path), port=18504)
    attempts: list[int] = []

    def flaky(*_a: object, **_k: object) -> httpx.Response:
        attempts.append(1)
        if len(attempts) == 1:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, json={})

    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: _FakeProcess())
    monkeypatch.setattr(runtime.httpx, "get", flaky)
    monkeypatch.setattr(runtime.time, "sleep", lambda _s: None)

    instance.start()
    instance.wait_ready()

    assert len(attempts) == 2


def test_instance_start_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：进程拉不起来时报错并指名。"""

    def boom(*_a: object, **_k: object) -> None:
        raise OSError("no exec")

    monkeypatch.setattr(runtime.subprocess, "Popen", boom)

    with pytest.raises(DrillError, match="无法拉起"):
        runtime.DrillInstance(_config(tmp_path), port=18505).start()


def test_instance_signal_before_start(tmp_path: Path) -> None:
    """实例：未启动时发信号报错。"""
    with pytest.raises(DrillError, match="未启动"):
        runtime.DrillInstance(_config(tmp_path), port=18506).signal("SIGKILL")


def test_instance_stop_kills_and_is_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：停止幂等；不退出时兜底强杀；未启动时停止无副作用。"""
    instance = runtime.DrillInstance(_config(tmp_path), port=18507)
    process = _FakeProcess(exit_after=99)
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: process)
    monkeypatch.setattr(runtime.httpx, "get", lambda *a, **k: httpx.Response(200, json={}))
    instance.start()
    instance.wait_ready()

    instance.stop()
    instance.stop()

    assert process.terminated
    assert runtime.DrillInstance(_config(tmp_path), port=18508).wait_exit() is None


def test_instance_log_tail_includes_text(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：日志尾部作为排障线索附在错误里。"""
    config = _config(tmp_path)
    instance = runtime.DrillInstance(config, port=18509)
    instance.log_path.write_text("boom-line", encoding="utf-8")

    assert "boom-line" in instance.log_tail()


def test_free_port_reports_exhaustion(monkeypatch: pytest.MonkeyPatch) -> None:
    """端口探测：整段被占时报错。"""

    class _Busy:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def __enter__(self) -> _Busy:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def setsockopt(self, *a: object) -> None:
            return None

        def bind(self, *a: object) -> None:
            raise OSError("busy")

    monkeypatch.setattr(runtime.socket, "socket", _Busy)

    with pytest.raises(DrillError, match="全被占用"):
        runtime.free_port(20000)


def test_tight_watermark_brackets_baseline(tmp_path: Path) -> None:
    """水位：warn 夹在基线与拒绝之间（恢复后能自动解除），拒绝值本身是四位小数。"""
    warn, reject, need_mb = runtime.tight_watermark(tmp_path, 8)

    assert warn < reject < 1
    assert need_mb >= 8
    assert round(reject, 4) == reject


def test_tight_watermark_rejects_full_volume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """水位：卷接近满时拒绝（无法构造跨阈）。"""
    monkeypatch.setattr(runtime.shutil, "disk_usage", lambda _p: SimpleNamespace(total=1000, used=999, free=1))

    with pytest.raises(DrillError, match="无法构造滞回区间"):
        runtime.tight_watermark(tmp_path, 1)


# ---------------------------------------------------------------------------
# 场景注册与编排
# ---------------------------------------------------------------------------


def test_scenario_registry_matches_source() -> None:
    """编排：注册的六类与场景单源一致。"""
    assert list(engine.scenario_names()) == [
        "process_crash",
        "disk_full",
        "slow_disk",
        "network_partition",
        "stuck_consumer",
        "poison_message",
    ]


def test_load_module_unknown() -> None:
    """编排：未注册场景报错并列出可选值。"""
    with pytest.raises(DrillError, match="未注册的场景"):
        engine.load_module("nope")


def test_reset_work_dir_removes_runtime_artifacts(tmp_path: Path) -> None:
    """编排：运行前清掉数据根与运行时产物（保证 LEO / 位点从零开始）。"""
    config = _config(tmp_path)
    (tmp_path / "var" / "data").mkdir(parents=True)
    (tmp_path / "platform.db").write_text("x", encoding="utf-8")
    (tmp_path / "instance.log").write_text("x", encoding="utf-8")

    engine.reset_work_dir(config)

    assert not (tmp_path / "var").exists()
    assert not (tmp_path / "platform.db").exists()
    assert not (tmp_path / "instance.log").exists()


def test_load_scenario_hits_and_misses(tmp_path: Path) -> None:
    """编排：从单源取场景；未知名报错。"""
    config = _config(tmp_path)

    scenario = engine.load_scenario(config, "disk_full")

    assert scenario.name == "disk_full"
    with pytest.raises(DrillError, match="没有 'nope'"):
        engine.load_scenario(config, "nope")


def test_load_scenario_rejects_broken_source(tmp_path: Path) -> None:
    """编排：单源非法时拒绝（错误一次列全）。"""
    (tmp_path / "deploy" / "drill").mkdir(parents=True)
    (tmp_path / "deploy" / "drill" / "scenarios.toml").write_text("schema_version = 1\n", encoding="utf-8")

    with pytest.raises(DrillError, match="场景单源非法"):
        engine.load_scenario(_config(tmp_path, repo_root=tmp_path), "process_crash")


def test_outcome_from_failure_marks_failed() -> None:
    """编排：前置失败装成一条失败判据（失败也留痕）。"""
    outcome = engine._outcome_from_failure(  # pyright: ignore[reportPrivateUsage]
        DrillError("drill.ready 实例未就绪"), ConcurrentStableFrozenList()
    )

    assert outcome.criteria[0].key == "execution"
    assert outcome.criteria[0].result == RESULT_FAILED
    assert outcome.injection["method"] == "aborted"


def _outcome(criteria: list[Criterion]) -> ScenarioOutcome:
    """构造场景产物。

    Args:
        criteria: 判据。

    Returns:
        ScenarioOutcome: 场景产物。
    """
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict({"method": "fake", "detail": "d", "artifact": ""}),
        samples=ConcurrentStableFrozenList(),
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=1,
        data_loss=False,
    )


def test_execute_returns_report_and_cleans_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """编排：跑完返回报告，实例与客户端都被关掉。"""
    closed: list[str] = []

    class _Instance:
        def stop(self) -> None:
            closed.append("instance")

    class _Http:
        def close(self) -> None:
            closed.append("http")

    monkeypatch.setattr(
        engine,
        "_prepare",
        lambda config, module: engine._Prepared(  # pyright: ignore[reportPrivateUsage]
            instance=cast("runtime.DrillInstance", _Instance()),
            http=cast("runtime.DrillHttp", _Http()),
            fill_mb=0,
            reject_ratio=0.0,
        ),
    )
    module = engine.load_module("process_crash")
    monkeypatch.setattr(module, "run", lambda ctx: _outcome([check("k", "e", "a", RESULT_OK)]))

    report = engine.execute(_config(tmp_path), engine.load_scenario(_config(tmp_path), "process_crash"))

    assert report.conclusion == "通过"
    assert closed == ["http", "instance"]


def test_execute_turns_drill_error_into_failed_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """编排：场景执行抛错时仍产出失败报告（不吞错）。"""

    class _Stub:
        def stop(self) -> None:
            pass

    class _Http:
        def close(self) -> None:
            pass

    monkeypatch.setattr(
        engine,
        "_prepare",
        lambda config, module: engine._Prepared(  # pyright: ignore[reportPrivateUsage]
            instance=cast("runtime.DrillInstance", _Stub()),
            http=cast("runtime.DrillHttp", _Http()),
            fill_mb=0,
            reject_ratio=0.0,
        ),
    )
    module = engine.load_module("disk_full")
    monkeypatch.setattr(module, "run", lambda ctx: (_ for _ in ()).throw(DrillError("drill.scenario 注入失败")))

    report = engine.execute(_config(tmp_path), engine.load_scenario(_config(tmp_path), "disk_full"))

    assert report.conclusion == "失败"
    assert report.exit_code == 1


def test_execute_prepare_failure_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """编排：实例起不来时报错也被装成失败报告。"""

    def boom(config: object, module: object) -> None:
        raise DrillError("drill.ready 实例未在 60s 内就绪")

    monkeypatch.setattr(engine, "_prepare", boom)

    report = engine.execute(_config(tmp_path), engine.load_scenario(_config(tmp_path), "process_crash"))

    assert report.conclusion == "失败"
    assert next(iter(report.reasons)).startswith("execution")


def test_plan_lines_renders_actions(tmp_path: Path) -> None:
    """编排：dry-run 只渲染动作，不启动实例。"""
    config = _config(tmp_path)

    lines = engine.plan_lines(config, engine.load_scenario(config, "process_crash"))

    assert lines and lines[0].startswith("1.")
    assert not (tmp_path / "var").exists()


# ---------------------------------------------------------------------------
# 命令行退出码
# ---------------------------------------------------------------------------


def test_cli_plan_renders_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：plan 校验单源并打印表，退出 0。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))

    assert drill_cli.main(["plan"]) == 0


def test_cli_required_work_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：缺必填键时退出 2 并指名键。"""
    monkeypatch.delenv("MSG_DRILL_WORK_DIR", raising=False)

    assert drill_cli.main(["plan"]) == 2


def test_cli_rejects_production(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：生产环境硬拒绝（退出 2，零动作）。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("MSG_DRILL_ENV", "production")

    assert drill_cli.main(["run", "process_crash", "--confirm", "process_crash"]) == 2


def test_cli_requires_confirm(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：缺二次确认时退出 2。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))

    assert drill_cli.main(["run", "process_crash"]) == 2


def test_cli_unknown_scenario(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：未注册场景退出 2。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))

    assert drill_cli.main(["run", "nope", "--confirm", "nope"]) == 2


def test_cli_dry_run_prints_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """命令行：dry-run 打印动作并退出 0（零写盘）。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))

    code = drill_cli.main(["run", "disk_full", "--confirm", "disk_full", "--dry-run"])

    assert code == 0
    assert "[PLAN] disk_full" in capsys.readouterr().out


def test_cli_cleanup_is_idempotent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：cleanup 清填充与压测目录，重复执行结果一致。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    for name in ("fill", "slowdisk"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "x").write_text("x", encoding="utf-8")

    assert drill_cli.main(["cleanup"]) == 0
    assert drill_cli.main(["cleanup"]) == 0
    assert not (tmp_path / "fill").exists()


def test_cli_backup_requires_evidence_outside_isolation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：非隔离环境无备份证据时拒绝执行。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("MSG_DRILL_ENV", "staging")
    monkeypatch.delenv("MSG_DRILL_BACKUP_EVIDENCE", raising=False)

    assert drill_cli.main(["run", "process_crash", "--confirm", "process_crash"]) == 2


def test_cli_run_writes_reports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """命令行：run 落两份记录并回显结论与退出码。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("MSG_DRILL_REPORT_DIR", str(tmp_path / "reports"))
    seen: dict[str, object] = {}

    def fake_execute(config: DrillConfig, scenario: DrillScenario) -> DrillReport:
        seen["scenario"] = scenario.name
        return _report()

    monkeypatch.setattr(drill_cli, "execute", fake_execute)

    code = drill_cli.main(["run", "process_crash", "--confirm", "process_crash"])

    assert code == 0
    assert seen["scenario"] == "process_crash"
    assert (tmp_path / "reports" / "process_crash.json").is_file()


def _report(criteria: Criterion | None = None) -> DrillReport:
    """构造最小报告（命令行用例用）。

    Args:
        criteria: 单条判据。

    Returns:
        DrillReport: 报告。
    """
    from datetime import UTC, datetime

    now = datetime(2026, 11, 11, tzinfo=UTC)
    sample = PhaseSample(
        phase="before",
        captured_at=now.isoformat(),
        readyz_status=200,
        readyz_ok=True,
        healthz_ok=True,
        metrics=ConcurrentStableFrozenDict({"msg_messages_in_total": 1.0}),
        detail="{}",
    )
    return DrillReport(
        scenario="process_crash",
        mode="run",
        env="test",
        target="local",
        started_at=now,
        finished_at=now,
        injection=ConcurrentStableFrozenDict({"method": "fake", "detail": "d", "artifact": ""}),
        samples=ConcurrentStableFrozenList([sample]),
        criteria=ConcurrentStableFrozenList([criteria or check("k", "e", "a", RESULT_OK)]),
        rto_ms=1,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
    )
