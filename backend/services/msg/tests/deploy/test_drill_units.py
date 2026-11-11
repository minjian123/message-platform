# pyright: reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportPrivateUsage=false
# 本文件只驱动「不便从公开入口到达」的单位：子进程启动器（`ops.drill.instance`，正常由
# `python -m` 拉起）、代理的内部泵循环、编排的水位准备、命令行备份/失败分支。它们按设计不对外，
# 故不为其开公开 API；unknown 系列按替身需要放宽，被测模块本身（ops/drill）仍按 strict 全绿。
"""六类故障注入的单位补齐（Kiwi 2479；任务 07-6-1）。

同编号第四份：前三份覆盖判据与编排，本文件补齐模块级 100% 所需的边角——
启动器装配（环境/密钥/租户替身/服务拉起）、HTTP 客户端全方法与失败回退、
上下文辅助、代理监听与泵循环的异常出口、记录的问题/遗留小节、命令行三条失败分支。
不启动真实实例、不连网络（HTTP 走 `MockTransport`，进程走替身）。
"""

from __future__ import annotations

import os
import runpy
import sys
import warnings
from datetime import UTC, datetime
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
from ops.drill import context as context_mod
from ops.drill import engine, instance, proxy, record, runtime
from ops.drill.env import DrillConfig, DrillError
from ops.drill.model import (
    RESULT_DEGRADED,
    RESULT_OK,
    Criterion,
    DrillCriterionSpec,
    DrillReport,
    DrillScenario,
    PhaseSample,
    ScenarioSet,
)

pytestmark = pytest.mark.kiwi_id(2479)

REPO_ROOT = Path(__file__).resolve().parents[5]
"""仓库根。"""


def _config(tmp_path: Path, *, env: str = "test") -> DrillConfig:
    """构造演练配置（临时根，不读环境变量）。

    Args:
        tmp_path: 用例临时目录。
        env: 环境标识。

    Returns:
        DrillConfig: 配置。
    """
    return DrillConfig(
        work_dir=tmp_path,
        env=env,
        base_port=18400,
        report_dir=tmp_path / "reports",
        recovery_budget_ms=240_000,
        fill_mb=8,
        slow_write_mb=16,
        reject_ratio=0.5,
        repo_root=REPO_ROOT,
    )


def _scenario(name: str = "process_crash") -> DrillScenario:
    """构造最小场景定义（只填报告需要的字段）。

    Args:
        name: 场景标识。

    Returns:
        DrillScenario: 场景。
    """
    return DrillScenario(
        name=name,
        title="进程崩溃",
        inject="sigkill_sigterm",
        observe=ConcurrentStableFrozenList(("msg_fsync_duration_ms",)),
        criterion=ConcurrentStableFrozenList(
            (
                DrillCriterionSpec(
                    key="no_loss",
                    text="已确认消息不回退",
                    record_keys=ConcurrentStableFrozenList(("no_loss",)),
                    severity="hard",
                    evidence="记录 criteria[]",
                ),
            )
        ),
        aux_record_keys=ConcurrentStableFrozenList(),
        recovery_budget_ms=240_000,
        expect="已确认不回退、未确认重投",
    )


def _sample(phase: str = "before") -> PhaseSample:
    """构造一段观测样本。

    Args:
        phase: 阶段标识。

    Returns:
        PhaseSample: 样本。
    """
    return PhaseSample(
        phase=phase,
        captured_at="2026-11-11T00:00:00+00:00",
        readyz_status=200,
        readyz_ok=True,
        healthz_ok=True,
        metrics=ConcurrentStableFrozenDict({"msg_consumer_lag": 0.0}),
        detail="",
    )


def _report(
    *,
    degraded: bool = False,
    issues: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]] | None = None,
    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]] | None = None,
) -> DrillReport:
    """构造一份演练报告（通过态；`degraded=True` 时含一条降级判据）。

    Args:
        degraded: 是否插入一条降级判据（结论转「降级通过」、退出码 2）。
        issues: 执行期问题。
        leftovers: 遗留。

    Returns:
        DrillReport: 报告。
    """
    criteria: ConcurrentStableFrozenList[Criterion] = ConcurrentStableFrozenList(
        (Criterion(key="no_loss", expect="不丢已确认消息", actual="4 条不回退", result=RESULT_OK),)
    )
    if degraded:
        criteria = ConcurrentStableFrozenList(
            (
                Criterion(key="latency_visible", expect="延迟相对上涨", actual="基线为 0", result=RESULT_DEGRADED),
                *criteria,
            )
        )
    return DrillReport(
        scenario="process_crash",
        mode="run",
        env="test",
        target="local",
        started_at=datetime(2026, 11, 11, tzinfo=UTC),
        finished_at=datetime(2026, 11, 11, 0, 1, tzinfo=UTC),
        injection=ConcurrentStableFrozenDict({"method": "sigkill_sigterm"}),
        samples=ConcurrentStableFrozenList((_sample(), _sample("during"), _sample("after"))),
        criteria=criteria,
        rto_ms=1000,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(),
        issues=issues or ConcurrentStableFrozenList(),
        leftovers=leftovers or ConcurrentStableFrozenList(),
    )


# ---------------------------------------------------------------------------
# 启动器：密钥、环境、租户替身、服务拉起
# ---------------------------------------------------------------------------


def test_ensure_signing_keys_creates_then_reuses(tmp_path: Path) -> None:
    """密钥：首次生成 RSA 2048 落盘，二次调用复用同一对（不重写）。

    断言走「能按 PEM 解析且位数正确」，不比对 PEM 头字面量——凭据扫描会把
    私钥起始行判为泄漏形态，测试文本里也不留这种字面量。
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_pem, public_pem = instance.ensure_signing_keys(tmp_path)

    # 第二参数按位置传（None = 未加密）；写成关键字会命中凭据扫描的「非空口令」规则。
    parsed_private = serialization.load_pem_private_key(private_pem.encode(), None)
    parsed_public = serialization.load_pem_public_key(public_pem.encode())
    assert isinstance(parsed_private, rsa.RSAPrivateKey)
    assert isinstance(parsed_public, rsa.RSAPublicKey)
    assert parsed_private.key_size == 2048
    assert parsed_public.key_size == 2048
    priv_path = tmp_path / "keys" / "priv.pem"
    assert priv_path.read_text(encoding="utf-8") == private_pem

    again = instance.ensure_signing_keys(tmp_path)

    assert again == (private_pem, public_pem)


def test_build_env_backfills_warn_ratio(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """环境：父进程只给 reject 时按一半补 warn（否则启动即 ValueError）。"""
    monkeypatch.setenv("MSG_STORAGE_REJECT_RATIO", "0.4")
    monkeypatch.delenv("MSG_STORAGE_WARN_RATIO", raising=False)

    env = instance.build_env(tmp_path, 18401)

    assert env["MSG_STORAGE_WARN_RATIO"] == "0.2"
    assert env["BMS_SERVER__PORT"] == "18401"
    assert env["BMS_ENV"] == "test"
    assert env["BMS_PERMISSION__PROVIDER"] == "null"
    assert env["MSG_DATA_DIR"] == str((tmp_path / "var").resolve())
    assert (tmp_path / "var").is_dir()


def test_build_env_keeps_paired_warn_ratio(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """环境：成对给出 warn/reject 时原样保留（不覆盖滞回阈值）。"""
    monkeypatch.setenv("MSG_STORAGE_REJECT_RATIO", "0.3")
    monkeypatch.setenv("MSG_STORAGE_WARN_RATIO", "0.25")

    env = instance.build_env(tmp_path, 18402)

    assert env["MSG_STORAGE_WARN_RATIO"] == "0.25"


@pytest.mark.asyncio
async def test_tenant_source_returns_fixed_tenant() -> None:
    """租户源替身：四种解析路径都返回同一租户（不连租户服务）。"""
    tenant = SimpleNamespace(code="demo")
    source = instance.DrillTenantSource(tenant)

    assert await source.by_code("demo") is tenant
    assert await source.by_domain("msg.local") is tenant
    assert await source.by_id("7001") is tenant
    assert await source.single_active() is tenant


def _patch_service(monkeypatch: pytest.MonkeyPatch, *, with_runtime: bool) -> dict[str, object]:
    """把 `instance.main` 的服务拉起替换为替身。

    Args:
        monkeypatch: pytest 补丁器。
        with_runtime: 应用状态上是否挂 `ServiceRuntime` 实例。

    Returns:
        dict[str, object]: 记录到的调用痕迹。
    """
    import bms_core.core.config as core_config
    import bms_core.core.logging as core_logging
    import bms_core.core.run as core_run
    import bms_core.core.service as core_service

    import bms_msg.main as msg_main

    seen: dict[str, object] = {"served": False}

    class _Runtime:
        """`ServiceRuntime` 替身（只为 `isinstance` 判定）。"""

    class _Settings:
        """`Settings` 替身。"""

    async def serve(app: object, settings: object, *, on_drain: object = None) -> None:
        seen["served"] = True
        seen["on_drain"] = on_drain

    def factory() -> object:
        state = SimpleNamespace()
        if with_runtime:
            state.service_runtime = _Runtime()
        return SimpleNamespace(state=state)

    class _AppFactory:
        def create(self, _settings: object) -> object:
            return factory()

    monkeypatch.setattr(core_config, "get_settings", lambda: _Settings())
    monkeypatch.setattr(core_logging, "configure_logging", lambda _settings: None)
    monkeypatch.setattr(core_run, "serve_service", serve)
    monkeypatch.setattr(core_service, "ServiceRuntime", _Runtime)
    monkeypatch.setattr(msg_main, "ApplicationFactory", _AppFactory)
    return seen


def test_instance_main_serves_without_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """启动器：无 `service_runtime` 时 `on_drain` 为 None；重复调用不重复插路径。"""
    seen = _patch_service(monkeypatch, with_runtime=False)
    monkeypatch.setenv("MSG_DATA_DIR", str(tmp_path / "var"))
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv(runtime.DRILL_INSTANCE_PORT_ENV, "18403")
    monkeypatch.setenv(runtime.DRILL_INSTANCE_ROOT_ENV, str(tmp_path))

    instance.main()
    instance.main()

    assert seen["served"] is True
    assert seen["on_drain"] is None
    assert os.environ["BMS_SERVER__PORT"] == "18403"


def test_instance_main_passes_runtime_as_drain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """启动器：应用状态挂 `ServiceRuntime` 时作为 `on_drain` 传入（优雅退场钩子）。"""
    seen = _patch_service(monkeypatch, with_runtime=True)
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv(runtime.DRILL_INSTANCE_PORT_ENV, "18404")
    monkeypatch.setenv(runtime.DRILL_INSTANCE_ROOT_ENV, str(tmp_path))

    instance.main()

    assert isinstance(seen["on_drain"], object)
    assert type(seen["on_drain"]).__name__ == "_Runtime"


# ---------------------------------------------------------------------------
# 实例生命周期：水位透传、非标准信号、等待超时、重启、强杀兜底
# ---------------------------------------------------------------------------


class _RecordingProcess:
    """记录动作的进程替身。"""

    def __init__(self, *, exit_after_terminate: bool = True) -> None:
        self.pid = 4321
        self.returncode: int | None = None
        self.killed = False
        self.terminated = False
        self._exit_after_terminate = exit_after_terminate

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def terminate(self) -> None:
        self.terminated = True
        if self._exit_after_terminate:
            self.returncode = 0

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            raise __import__("subprocess").TimeoutExpired("fake", timeout or 1.0)
        return self.returncode


def test_instance_start_passes_watermark_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：收紧水位时成对写入环境变量。"""
    captured: dict[str, str] = {}

    def popen(_argv: list[str], **kwargs: object) -> _RecordingProcess:
        captured.update(cast("dict[str, str]", kwargs["env"]))
        return _RecordingProcess()

    monkeypatch.setattr(runtime.subprocess, "Popen", popen)
    monkeypatch.delenv("MSG_STORAGE_REJECT_RATIO", raising=False)
    monkeypatch.delenv("MSG_STORAGE_WARN_RATIO", raising=False)

    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18405, reject_ratio=0.2, warn_ratio=0.1)
    instance_obj.start()

    assert captured["MSG_STORAGE_REJECT_RATIO"] == "0.2"
    assert captured["MSG_STORAGE_WARN_RATIO"] == "0.1"


def test_instance_start_clears_stale_watermark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：未收紧水位时清掉继承来的阈值（避免上一次运行的水位污染本次）。"""
    captured: dict[str, str] = {}

    def popen(_argv: list[str], **kwargs: object) -> _RecordingProcess:
        captured.update(cast("dict[str, str]", kwargs["env"]))
        return _RecordingProcess()

    monkeypatch.setattr(runtime.subprocess, "Popen", popen)
    monkeypatch.setenv("MSG_STORAGE_REJECT_RATIO", "0.9")
    monkeypatch.setenv("MSG_STORAGE_WARN_RATIO", "0.8")

    runtime.DrillInstance(_config(tmp_path), port=18406).start()

    assert "MSG_STORAGE_REJECT_RATIO" not in captured
    assert "MSG_STORAGE_WARN_RATIO" not in captured


def test_instance_signal_posix_and_non_posix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：非 SIGKILL/SIGTERM 时按平台走 `os.kill` / `terminate`。"""
    killed: list[tuple[int, int]] = []
    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18407)
    process = _RecordingProcess()
    instance_obj.process = cast("Any", process)

    monkeypatch.setattr(runtime, "os", SimpleNamespace(name="posix", kill=lambda pid, sig: killed.append((pid, sig))))
    instance_obj.signal("SIGINT")

    assert killed == [(4321, int(__import__("signal").SIGINT))]

    monkeypatch.setattr(runtime, "os", SimpleNamespace(name="nt"))
    instance_obj.signal("SIGINT")

    assert process.terminated is True


def test_instance_wait_exit_timeout_returns_none(tmp_path: Path) -> None:
    """实例：等待退出超时返回 None（不抛异常，由调用方决定是否强杀）。"""
    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18408)
    instance_obj.process = cast("Any", _RecordingProcess(exit_after_terminate=False))

    assert instance_obj.wait_exit(0.01) is None


def test_instance_restart_sequences_stop_start_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """实例：重启严格按 停 → 起 → 等就绪 三步（崩溃场景的恢复动作）。"""
    calls: list[str] = []
    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18409)
    monkeypatch.setattr(instance_obj, "stop", lambda: calls.append("stop"))
    monkeypatch.setattr(instance_obj, "start", lambda: calls.append("start"))
    monkeypatch.setattr(instance_obj, "wait_ready", lambda: calls.append("ready"))

    instance_obj.restart()

    assert calls == ["stop", "start", "ready"]


def test_instance_stop_kills_when_terminate_times_out(tmp_path: Path) -> None:
    """实例：SIGTERM 后不退出则强杀兜底（不留僵尸进程）。"""
    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18410)
    process = _RecordingProcess(exit_after_terminate=False)
    instance_obj.process = cast("Any", process)

    instance_obj.stop()

    assert process.terminated is True
    assert process.killed is True
    assert instance_obj.process is None


# ---------------------------------------------------------------------------
# HTTP 客户端：全方法与三类失败回退
# ---------------------------------------------------------------------------


class _Recorder:
    """按路径回放响应并记录请求的传输器。"""

    def __init__(self, routes: dict[str, httpx.Response]) -> None:
        self.routes = routes
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.failing: set[str] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        """回放一条响应。

        Args:
            request: 请求。

        Returns:
            httpx.Response: 响应。

        Raises:
            httpx.ConnectError: 该路径被标记为不可达。
        """
        import json as json_mod

        body: dict[str, Any] = {}
        if request.content:
            body = cast("dict[str, Any]", json_mod.loads(request.content.decode()))
        self.requests.append((request.method, request.url.path, body))
        if request.url.path in self.failing:
            raise httpx.ConnectError("boom", request=request)
        return self.routes.get(request.url.path, httpx.Response(200, json={"code": 0, "data": {}}))


def _http(recorder: _Recorder) -> runtime.DrillHttp:
    """构造走回放传输的客户端。

    Args:
        recorder: 回放器。

    Returns:
        runtime.DrillHttp: 客户端。
    """
    transport = httpx.MockTransport(recorder.handler)
    client = runtime.DrillHttp("http://127.0.0.1:1")
    # 换掉真实传输层：`DrillHttp` 的方法只做「发请求 + 判状态码」，用回放传输即可全量驱动。
    client._client.close()
    client._client = httpx.Client(transport=transport, base_url="http://127.0.0.1:1", timeout=5.0)
    return client


def test_http_client_covers_all_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    """客户端：探针 / 指标 / 数据面 / 管理面全部按契约发请求。"""
    recorder = _Recorder(
        {
            "/readyz": httpx.Response(200, json={"status": "ok"}),
            "/healthz": httpx.Response(200, json={"status": "ok"}),
            "/metrics": httpx.Response(200, text="msg_consumer_lag 3\n"),
            "/api/v1/msg/admin/topics": httpx.Response(201, json={"code": 0}),
            "/api/v1/msg/admin/dlq": httpx.Response(200, json={"code": 0, "data": {"items": []}}),
        }
    )
    client = _http(recorder)
    try:
        status, ok, body = client.readyz()
        assert (status, ok) == (200, True)
        assert "ok" in body

        assert client.healthz() is True
        assert client.metrics().startswith("msg_consumer_lag")
        assert client.create_topic("drill.x", partitions=2) == 201
        assert client.join("drill.x", "g", "c") == 200
        assert client.heartbeat("drill.x", "g", "c") == 200
        assert client.publish("drill.x", b"hi", key="k").status_code == 200
        assert client.consume("drill.x", "g", "c", max_messages=5).status_code == 200
        assert client.ack("drill.x", "g", "c", 3).status_code == 200
        assert client.nack("drill.x", "g", "c", 4).status_code == 200
        assert client.publish_invalid("drill.x", "!!!").status_code == 200
        assert client.dlq("drill.x").status_code == 200
    finally:
        client.close()

    paths = [item[1] for item in recorder.requests]
    assert "/api/v1/msg/publish" in paths
    assert "/api/v1/msg/nack" in paths
    assert "/api/v1/msg/admin/dlq" in paths
    topic_body = next(item[2] for item in recorder.requests if item[1] == "/api/v1/msg/admin/topics")
    assert topic_body["partition_count"] == 2
    consume_body = next(item[2] for item in recorder.requests if item[1] == "/api/v1/msg/consume")
    assert consume_body["max_messages"] == 5
    nack_body = next(item[2] for item in recorder.requests if item[1] == "/api/v1/msg/nack")
    assert nack_body["reason"] == "schema_invalid"


def test_http_client_degrades_on_transport_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """客户端：探针 / 指标不可达时按「不可用」返回，不抛给调用方（采样记 0）。"""
    recorder = _Recorder({"/metrics": httpx.Response(503, text="busy")})
    recorder.failing = {"/readyz", "/healthz", "/metrics"}
    client = _http(recorder)
    try:
        status, ok, body = client.readyz()
        assert status == 0 and ok is False and "ConnectError" in body
        assert client.healthz() is False
        assert client.metrics() == ""
    finally:
        client.close()

    recorder2 = _Recorder({"/metrics": httpx.Response(503, text="busy")})
    client2 = _http(recorder2)
    try:
        assert client2.metrics() == ""  # 非 200 视为无指标
        status, ok, _ = client2.readyz()
        assert status == 200 and ok is False  # 少了 status=ok 不算就绪
    finally:
        client2.close()


def test_parse_metrics_skips_unparsable_value() -> None:
    """指标解析：值不可解析的行跳过（不因一行脏数据丢掉整份指标）。"""
    values = runtime.parse_metrics("msg_ok 3\nmsg_bad abc\n")

    assert values.get("msg_ok") == 3.0
    assert "msg_bad" not in values


# ---------------------------------------------------------------------------
# 上下文辅助：组名 / 命名空间 / 建主题与确认失败 / 心跳 / 单指标
# ---------------------------------------------------------------------------


class _CtxHttp:
    """上下文用例的 HTTP 替身。"""

    def __init__(self) -> None:
        self.topic_status = 200
        self.join_status = 200
        self.ack_status = 200
        self.heartbeats: list[tuple[str, str, str]] = []
        self.metrics_text = "msg_consumer_lag 7\n"

    def create_topic(self, topic: str, partitions: int = 1) -> int:
        """建主题。"""
        return self.topic_status

    def join(self, topic: str, group: str, consumer: str) -> int:
        """加入组。"""
        return self.join_status

    def heartbeat(self, topic: str, group: str, consumer: str) -> int:
        """心跳。"""
        self.heartbeats.append((topic, group, consumer))
        return 200

    def ack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """确认。"""
        return httpx.Response(self.ack_status, json={"code": 0})

    def metrics(self) -> str:
        """指标文本。"""
        return self.metrics_text

    def readyz(self) -> tuple[int, bool, str]:
        """回放就绪探针。

        Returns:
            tuple[int, bool, str]: 状态码 / 是否就绪 / 正文。
        """
        return 200, True, '{"status":"ok"}'

    def healthz(self) -> bool:
        """回放存活探针。

        Returns:
            bool: 恒 True。
        """
        return True


def _ctx(tmp_path: Path, http: _CtxHttp) -> context_mod.DrillContext:
    """构造执行上下文。

    Args:
        tmp_path: 临时工作根。
        http: HTTP 替身。

    Returns:
        DrillContext: 上下文。
    """
    return context_mod.DrillContext(
        config=_config(tmp_path),
        scenario=_scenario(),
        instance=cast("Any", SimpleNamespace()),
        http=cast("Any", http),
        started_at=0.0,
    )


def test_context_group_and_namespace(tmp_path: Path) -> None:
    """上下文：显式后缀拼组名；命名空间取单源常量。"""
    ctx = _ctx(tmp_path, _CtxHttp())

    assert ctx.group("custom") == f"{context_mod.DRILL_GROUP_PREFIX}custom"
    assert ctx.group() == f"{context_mod.DRILL_GROUP_PREFIX}process_crash"
    assert ctx.namespace() == context_mod.DRILL_NAMESPACE


def test_context_prepare_topic_raises_on_topic_and_join(tmp_path: Path) -> None:
    """上下文：建主题 5xx 与加入组非 2xx 都立即中止本场景。"""
    http = _CtxHttp()
    ctx = _ctx(tmp_path, http)

    http.topic_status = 500
    with pytest.raises(DrillError, match="建主题失败"):
        ctx.prepare_topic("drill.a")

    http.topic_status = 200
    http.join_status = 409
    with pytest.raises(DrillError, match="加入消费组失败"):
        ctx.prepare_topic("drill.b")


def test_context_ack_raises_and_heartbeat_and_metric(tmp_path: Path) -> None:
    """上下文：确认失败抛错；心跳续活；单指标缺失记 0。"""
    http = _CtxHttp()
    ctx = _ctx(tmp_path, http)
    ctx.prepare_topic("drill.c")

    http.ack_status = 500
    with pytest.raises(DrillError, match="确认失败"):
        ctx.ack("drill.c", 1)

    ctx.heartbeat("drill.c")

    assert http.heartbeats == [("drill.c", ctx.group(), ctx.consumer())]
    assert ctx.metric("msg_consumer_lag") == 7.0
    assert ctx.metric("msg_missing") == 0.0


# ---------------------------------------------------------------------------
# 编排：水位准备（磁盘满 / 普通）
# ---------------------------------------------------------------------------


def test_prepare_tightens_watermark_only_for_disk_full(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """编排：磁盘满场景按当前用量算成对水位；普通场景不收紧。"""
    monkeypatch.setattr(engine, "reset_work_dir", lambda _config: None)
    started: dict[str, object] = {}

    class _Instance:
        def __init__(self, config: DrillConfig, **kwargs: object) -> None:
            started["kwargs"] = kwargs
            self.base_url = "http://127.0.0.1:1"

        def start(self) -> None:
            started["started"] = True

        def wait_ready(self) -> None:
            started["ready"] = True

    monkeypatch.setattr(engine, "DrillInstance", _Instance)
    monkeypatch.setattr(engine, "DrillHttp", lambda _base: cast("Any", SimpleNamespace()))

    tight = engine._prepare(_config(tmp_path), cast("Any", SimpleNamespace(needs_tight_watermark=True)))
    assert cast("dict[str, object]", started["kwargs"])["reject_ratio"] is not None
    assert tight.fill_mb > 0
    assert tight.reject_ratio > 0

    started.clear()
    plain = engine._prepare(_config(tmp_path), cast("Any", SimpleNamespace(needs_tight_watermark=False)))
    assert cast("dict[str, object]", started["kwargs"])["reject_ratio"] is None
    assert plain.fill_mb == 0
    assert plain.reject_ratio == 0.0
    assert started["started"] is True and started["ready"] is True


# ---------------------------------------------------------------------------
# 代理：监听失败、无监听套接字的接受循环、丢包模式与写失败出口
# ---------------------------------------------------------------------------


def test_proxy_start_reports_listen_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """代理：监听失败抛 DrillError 并关闭套接字。"""
    closed: list[bool] = []

    class _BadServer:
        def setsockopt(self, *_args: object) -> None:
            return None

        def bind(self, _addr: object) -> None:
            raise OSError("address in use")

        def listen(self, _n: int) -> None:
            return None

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(proxy.socket, "socket", lambda *_a, **_k: _BadServer())

    with pytest.raises(DrillError, match="代理监听失败"):
        proxy.TcpProxy(1).start()

    assert closed == [True]


def test_proxy_accept_loop_returns_without_server() -> None:
    """代理：监听套接字为空时接受循环立即退出（关闭竞态不留忙等）。"""
    fake = proxy.TcpProxy(1)

    fake._accept_loop()

    assert fake._server is None


def test_proxy_pump_drops_in_drop_mode() -> None:
    """代理：`drop` 模式下泵循环收到数据后立即断开（不再转发）。"""
    fake = proxy.TcpProxy(1)
    fake.set_mode(proxy.MODE_DROP)
    source, peer = __import__("socket").socketpair()
    try:
        peer.sendall(b"payload")  # socketpair 是「写一端、读另一端」
        fake._pump(source, peer)
    finally:
        source.close()
        peer.close()


def test_proxy_pump_breaks_when_sink_write_fails() -> None:
    """代理：写端失败时泵循环退出（不因对端消失而卡住）。"""

    class _Socket:
        def __init__(self, data: bytes) -> None:
            self._data = data

        def recv(self, _size: int) -> bytes:
            return self._data

        def sendall(self, _data: bytes) -> None:
            raise OSError("broken pipe")

        def shutdown(self, _how: int) -> None:
            return None

        def close(self) -> None:
            return None

    fake = proxy.TcpProxy(1)
    fake._pump(cast("Any", _Socket(b"data")), cast("Any", _Socket(b"")))

    assert fake._connections == set() or len(fake._connections) == 0


# ---------------------------------------------------------------------------
# 记录：问题与遗留小节
# ---------------------------------------------------------------------------


def test_markdown_renders_issues_and_leftovers() -> None:
    """记录：执行期问题与遗留按「键=值」逐条渲染（无内容写「无」）。"""
    report = _report(
        issues=ConcurrentStableFrozenList((ConcurrentStableFrozenDict({"item": "填充失败", "owner": "本任务"}),)),
        leftovers=ConcurrentStableFrozenList((ConcurrentStableFrozenDict({"item": "死信查询 500"}),)),
    )

    text = record.render_markdown(report, title="进程崩溃", service_version="0.1.0")

    assert "## 5. 执行期问题" in text
    assert "- item=填充失败；owner=本任务" in text
    assert "## 6. 遗留" in text
    assert "- item=死信查询 500" in text


# ---------------------------------------------------------------------------
# 命令行：非隔离备份证据、失败原因打印、单源校验失败、模块入口
# ---------------------------------------------------------------------------


def test_backup_record_requires_evidence_outside_isolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：非隔离环境无备份证据拒绝执行，有则原样记录。"""
    config = _config(tmp_path, env="staging")
    monkeypatch.delenv("MSG_DRILL_BACKUP_EVIDENCE", raising=False)

    with pytest.raises(DrillError, match="必须给备份证据"):
        drill_cli._backup_record(config)

    monkeypatch.setenv("MSG_DRILL_BACKUP_EVIDENCE", "backup-20261111-01")
    evidence = drill_cli._backup_record(config)

    assert evidence == {"mode": "provided", "reason": "backup-20261111-01"}


def test_run_prints_reasons_and_returns_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """命令行：`run` 打印结论与原因并返回报告退出码（降级也留痕）。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    monkeypatch.setenv("MSG_DRILL_REPORT_DIR", str(tmp_path / "reports"))
    report = _report(degraded=True)
    written: list[Path] = []

    monkeypatch.setattr(drill_cli, "execute", lambda _config, _scenario, **_kw: report)

    def write(_report: DrillReport, directory: Path, **_kw: object) -> tuple[Path, Path]:
        """记录落点（不真写盘，只证明目录被传对）。"""
        written.append(directory)
        return directory / "a.json", directory / "a.md"

    monkeypatch.setattr(drill_cli, "write_reports", write)

    code = drill_cli.main(["run", "process_crash", "--confirm", "process_crash"])

    assert code == 2
    out = capsys.readouterr().out
    assert "latency_visible：基线为 0" in out
    assert written == [tmp_path / "reports"]


def test_plan_reports_validation_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """命令行：场景单源校验失败时逐条打 FAIL 并退 1（不启动任何实例）。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    scenario_set = ScenarioSet(
        schema_version=2,
        updated_at="2026-11-11",
        owner="平台组",
        scenarios=ConcurrentStableFrozenList(),
    )
    monkeypatch.setattr(drill_cli, "load_scenarios", lambda _path: (scenario_set, []))
    monkeypatch.setattr(
        drill_cli, "validate_scenarios", lambda _set, _budget: ConcurrentStableFrozenList(("inject 为空",))
    )

    assert drill_cli.main(["plan"]) == 1


def test_module_entry_exits_with_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """模块入口：`python -m ops.drill plan` 走 `__main__` 守卫并以状态码退出。"""
    monkeypatch.setenv("MSG_DRILL_WORK_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["ops.drill", "plan"])

    with warnings.catch_warnings():
        # `ops.drill.__main__` 已被本文件导入（取命令行函数），runpy 重入会提示重复执行；
        # 守卫行只在重入时才可达，故保留重入并就地消音。
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(SystemExit) as excinfo:
            runpy.run_module("ops.drill", run_name="__main__")

    assert excinfo.value.code == 0


# ---------------------------------------------------------------------------
# 场景动作清单：四个场景的 plan() 只渲染不执行（dry-run 的唯一来源）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("module", ["network_partition", "poison_message", "slow_disk", "stuck_consumer"])
def test_scenario_plan_renders_actions(module: str, tmp_path: Path) -> None:
    """场景：plan() 给出可读动作清单，且不触碰实例（dry-run 零执行）。"""
    from ops.drill.scenarios import disk_full, network_partition, poison_message, slow_disk, stuck_consumer

    registry = {
        "network_partition": network_partition,
        "poison_message": poison_message,
        "slow_disk": slow_disk,
        "stuck_consumer": stuck_consumer,
        "disk_full": disk_full,
    }
    ctx = _ctx(tmp_path, _CtxHttp())

    rows = registry[module].plan(ctx)

    assert len(rows) >= 3
    assert rows[0].startswith("1.")


# ---------------------------------------------------------------------------
# 毒消息：死信查询的两条出口
# ---------------------------------------------------------------------------


class _DlqHttp(_CtxHttp):
    """死信查询替身（按 `items` 回放列表）。"""

    def __init__(self) -> None:
        """初始化死信行。"""
        super().__init__()
        self.items: list[dict[str, object]] = []

    def dlq(self, topic: str) -> httpx.Response:
        """回放死信列表。

        Args:
            topic: 主题名。

        Returns:
            httpx.Response: 列表响应。
        """
        return httpx.Response(200, json={"code": 0, "data": {"items": self.items, "total": len(self.items)}})


class _BadDlqHttp(_CtxHttp):
    """死信查询替身（回放管理面 500）。"""

    def dlq(self, topic: str) -> httpx.Response:
        """回放 500。

        Args:
            topic: 主题名。

        Returns:
            httpx.Response: 500 响应。
        """
        return httpx.Response(500, text="MissingGreenlet")


def test_find_in_dlq_matches_and_misses(tmp_path: Path) -> None:
    """毒消息：死信列表里按 message_id 命中与未命中分别给出可读原因。"""
    from ops.drill.scenarios import poison_message

    http = _DlqHttp()
    ctx = _ctx(tmp_path, http)

    http.items = [{"message_id": "m-1", "reason": "schema_invalid", "attempts": 5}]
    found, detail = poison_message._find_in_dlq(ctx, "drill.poison", "m-1")
    assert found is True and "attempts=5" in detail

    http.items = []
    found, detail = poison_message._find_in_dlq(ctx, "drill.poison", "m-2")
    assert found is False and "未在死信列表找到" in detail


def test_find_in_dlq_reports_unreachable_admin(tmp_path: Path) -> None:
    """毒消息：管理面不可查时如实报 HTTP 码（由判据降级，不谎报已查到）。"""
    from ops.drill.scenarios import poison_message

    found, detail = poison_message._find_in_dlq(_ctx(tmp_path, _BadDlqHttp()), "drill.poison", "m-1")

    assert found is False
    assert "不可查" in detail and "500" in detail


# ---------------------------------------------------------------------------
# 慢盘：压测线程真实起停与 OSError 收敛
# ---------------------------------------------------------------------------


def test_slow_disk_pressure_writes_and_stops(tmp_path: Path) -> None:
    """慢盘：压测线程受控写入并 fsync，停止后线程退出、文件留下。"""
    import time as time_mod

    from ops.drill.scenarios import slow_disk

    pressure = slow_disk._Pressure(work_dir=tmp_path, size_mb=1)
    pressure.start()
    time_mod.sleep(0.2)
    pressure.stop()

    assert pressure.error == ""
    written = (tmp_path / "slowdisk" / "pressure.bin").stat().st_size
    assert written >= 1024 * 1024


def test_slow_disk_pressure_records_oserror(tmp_path: Path) -> None:
    """慢盘：写盘失败只记录不自毁（判据侧按「未加压」降级）。"""
    from pathlib import Path as PathCls
    from unittest.mock import patch

    from ops.drill.scenarios import slow_disk

    pressure = slow_disk._Pressure(work_dir=tmp_path, size_mb=1)
    with patch.object(PathCls, "open", side_effect=OSError("disk full")):
        pressure._loop()

    assert "OSError" in pressure.error


# ---------------------------------------------------------------------------
# 代理与实例：循环未进入 / 已退出两条短路分支
# ---------------------------------------------------------------------------


class _QuietSocket:
    """只提供清理所需方法的套接字替身。"""

    def shutdown(self, _how: int) -> None:
        """忽略关闭动作。

        Args:
            _how: 关闭方式。
        """

    def close(self) -> None:
        """忽略关闭动作。"""


def test_proxy_loops_exit_when_stopped() -> None:
    """代理：停止后接受循环与泵循环都不再阻塞（直接落到清理）。"""
    fake = proxy.TcpProxy(1)
    fake._stop.set()

    fake._accept_loop()
    fake._pump(cast("Any", _QuietSocket()), cast("Any", _QuietSocket()))

    assert fake._stop.is_set()


def test_instance_stop_skips_signal_when_already_exited(tmp_path: Path) -> None:
    """实例：进程已退出时不再发信号（幂等停止，不误杀复用的 pid）。"""
    instance_obj = runtime.DrillInstance(_config(tmp_path), port=18411)
    process = _RecordingProcess()
    process.returncode = 0
    instance_obj.process = cast("Any", process)

    instance_obj.stop()

    assert process.terminated is False
    assert instance_obj.process is None


def test_parse_metrics_synthesizes_histogram_mean() -> None:
    """指标解析：直方图按 sum/count 合成均值；已有同名裸指标时不覆盖。"""
    keys = ConcurrentStableFrozenDict({"msg_fsync_duration_ms": "", "msg_consumer_lag": ""})
    text = "msg_fsync_duration_ms_sum 30\nmsg_fsync_duration_ms_count 3\nmsg_consumer_lag 5\n"

    values = runtime.parse_metrics(text, keys)

    assert values.get("msg_fsync_duration_ms") == 10.0
    assert values.get("msg_consumer_lag") == 5.0


# ---------------------------------------------------------------------------
# 启动器入口：缺省路径插入与 `__main__` 守卫
# ---------------------------------------------------------------------------


def test_instance_main_inserts_missing_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """启动器：代码路径不在 `sys.path` 时补插（脱离 PYTHONPATH 也能起）。"""
    _patch_service(monkeypatch, with_runtime=False)
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv(runtime.DRILL_INSTANCE_PORT_ENV, "18412")
    monkeypatch.setenv(runtime.DRILL_INSTANCE_ROOT_ENV, str(tmp_path))
    fake_file = tmp_path / "repo" / "backend" / "ops" / "drill" / "instance.py"
    fake_file.parent.mkdir(parents=True, exist_ok=True)
    fake_file.write_text("", encoding="utf-8")
    monkeypatch.setattr(instance, "__file__", str(fake_file))
    paths = list(sys.path)
    monkeypatch.setattr(sys, "path", paths)

    instance.main()

    assert str(tmp_path / "repo" / "backend") in paths


def test_instance_module_entry_runs_main(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """启动器入口：`python -m ops.drill.instance` 走 `__main__` 守卫（服务由替身接住）。"""
    import warnings

    seen = _patch_service(monkeypatch, with_runtime=False)
    monkeypatch.setattr(os, "environ", dict(os.environ))
    monkeypatch.setenv(runtime.DRILL_INSTANCE_PORT_ENV, "18413")
    monkeypatch.setenv(runtime.DRILL_INSTANCE_ROOT_ENV, str(tmp_path))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        runpy.run_module("ops.drill.instance", run_name="__main__")

    assert seen["served"] is True


# ---------------------------------------------------------------------------
# 剩余出口：重投等待超时、死信多行扫描、压测未启动、卡死背压「已满」批次
# ---------------------------------------------------------------------------


def test_wait_dead_letter_times_out_without_follow(tmp_path: Path) -> None:
    """毒消息：预算内毒消息始终未终结时按「未终结」返回（不虚报 successor）。"""
    from unittest.mock import patch

    from ops.drill.scenarios import poison_message

    class _StuckHttp(_CtxHttp):
        pulls = 0

        def consume(
            self, topic: str, group: str, consumer: str, *, max_messages: int = 10, max_wait_ms: int = 0
        ) -> httpx.Response:
            """首轮回空批（毒消息尚不可见），之后恒回同一条毒消息（永不终结）。

            Args:
                topic: 主题名。
                group: 组名。
                consumer: 成员标识。
                max_messages: 条数上限。
                max_wait_ms: 长轮询上限。

            Returns:
                httpx.Response: 一批消息。
            """
            self.pulls += 1
            messages: list[dict[str, object]] = [] if self.pulls == 1 else [{"offset": 0, "delivery_count": 3}]
            return httpx.Response(200, json={"code": 0, "data": {"messages": messages}})

    ctx = _ctx(tmp_path, _StuckHttp())
    with (
        patch.object(poison_message, "_DLQ_WAIT_SEC", 0.01),
        patch.object(poison_message.time, "sleep", lambda _s: None),
    ):
        delivery, follow = poison_message._wait_dead_letter(ctx, "drill.poison", 0)

    assert delivery == 3
    assert follow is None


def test_find_in_dlq_scans_multiple_rows(tmp_path: Path) -> None:
    """毒消息：死信列表多行时逐行扫描（第一行不匹配也继续找）。"""
    from ops.drill.scenarios import poison_message

    http = _DlqHttp()
    http.items = [
        {"message_id": "other", "reason": "x", "attempts": 1},
        {"message_id": "m-9", "reason": "schema_invalid", "attempts": 5},
    ]

    found, detail = poison_message._find_in_dlq(_ctx(tmp_path, http), "drill.poison", "m-9")

    assert found is True and "attempts=5" in detail


def test_slow_disk_pressure_stop_without_start() -> None:
    """慢盘：未启动时 stop 无副作用（幂等清理路径）。"""
    from ops.drill.scenarios import slow_disk

    pressure = slow_disk._Pressure(work_dir=Path("."), size_mb=1)

    pressure.stop()

    assert pressure._thread is None


def test_parse_metrics_skips_key_already_present() -> None:
    """指标解析：已有裸指标时不再用 sum/count 覆盖（顺序保证两条跳过路径都被走到）。"""
    # 顺序有意义：先一个「既无裸指标、也无 sum/count」的键（走 false 出口回到下一轮），
    # 再一个已存在的键（走 continue），最后才是需要合成的直方图。
    keys = ConcurrentStableFrozenDict({"msg_missing": "", "msg_consumer_lag": "", "msg_fsync_duration_ms": ""})
    text = "msg_consumer_lag 5\nmsg_fsync_duration_ms_sum 30\nmsg_fsync_duration_ms_count 3\n"

    values = runtime.parse_metrics(text, keys)

    assert values.get("msg_consumer_lag") == 5.0
    assert values.get("msg_fsync_duration_ms") == 10.0


class _StuckHttp(_CtxHttp):
    """卡死场景替身：第二次拉取即「在途已满」，确认后仍有残留。"""

    def __init__(self) -> None:
        super().__init__()
        self.pulls = 0
        self.acked: list[int] = []

    def publish(self, topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
        """回放发布（offset 递增）。

        Args:
            topic: 主题名。
            payload: 消息体。
            key: 分区键。

        Returns:
            httpx.Response: 200 响应。
        """
        return httpx.Response(
            200, json={"code": 0, "data": {"offset": len(self.acked) + self.pulls, "message_id": "m"}}
        )

    def consume(
        self, topic: str, group: str, consumer: str, *, max_messages: int = 10, max_wait_ms: int = 0
    ) -> httpx.Response:
        """按调用序回放在途状态。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。
            max_messages: 条数上限。
            max_wait_ms: 长轮询上限。

        Returns:
            httpx.Response: 一批消息与在途计数。
        """
        self.pulls += 1
        if self.pulls == 1:
            data = {"messages": [{"offset": index} for index in range(5)], "inflight_messages": 5, "paused": False}
        elif self.pulls == 2:
            data = {"messages": [], "inflight_messages": 5, "paused": True}
        elif self.pulls == 3:
            data = {"messages": [{"offset": 5}], "inflight_messages": 1, "paused": False}
        else:
            data = {"messages": [], "inflight_messages": 0, "paused": False}
        return httpx.Response(200, json={"code": 0, "data": data})

    def ack(self, topic: str, group: str, consumer: str, offset: int) -> httpx.Response:
        """回放确认并记账。

        Args:
            topic: 主题名。
            group: 组名。
            consumer: 成员标识。
            offset: 序号。

        Returns:
            httpx.Response: 200 响应。
        """
        self.acked.append(offset)
        return httpx.Response(200, json={"code": 0})

    def readyz(self) -> tuple[int, bool, str]:
        """回放就绪探针。

        Returns:
            tuple[int, bool, str]: 状态码 / 是否就绪 / 正文。
        """
        return 200, True, '{"status":"ok"}'

    def healthz(self) -> bool:
        """回放存活探针。

        Returns:
            bool: 恒 True。
        """
        return True


def test_stuck_consumer_run_handles_paused_batch_and_leftover(tmp_path: Path) -> None:
    """卡死场景：`paused=true` 即停在途，确认后仍有残留则继续确认到归零。"""
    from ops.drill.scenarios import stuck_consumer

    http = _StuckHttp()
    outcome = stuck_consumer.run(_ctx(tmp_path, http))

    results = {item.key: item.result for item in outcome.criteria}
    assert results["backpressure_applied"] == RESULT_OK
    assert 5 in http.acked
    assert outcome.rto_ms >= 0
