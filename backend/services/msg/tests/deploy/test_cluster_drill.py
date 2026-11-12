"""集群演练包（02-8）：配置 / 命令构造 / 注入步骤 / 探针解析 / 数据核对 / 客户端刷新 / 记录。"""

from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ops.cluster_drill import __main__ as cluster_main
from ops.cluster_drill import chain as chain_mod
from ops.cluster_drill import inject as cluster_inject
from ops.cluster_drill import remote as remote_mod
from ops.cluster_drill.client import MetadataView, PublishOutcome, refresh_and_retry
from ops.cluster_drill.config import (
    ClusterDrillConfig,
    ClusterDrillError,
    load_config,
    parse_env_file,
    reject_production,
    require_confirmation,
)
from ops.cluster_drill.data import compare_committed
from ops.cluster_drill.inject import (
    apply_steps,
    connect_command,
    disconnect_command,
    kill_command,
    network_name,
    start_command,
    steps_for,
    stop_command,
)
from ops.cluster_drill.probe import MetricSample, ProbeClient, metric_value, parse_metrics, parse_readyz
from ops.cluster_drill.remote import (
    RemoteResult,
    RemoteRunner,
    SshTunnel,
    build_image_script,
    compose_down_script,
    compose_up_script,
    image_reference,
    ssh_argv,
    sync_source_script,
    tunnel_argv,
)
from ops.cluster_drill.scenarios import build_criteria, degraded_note, missing_keys
from ops.drill.model import DrillCriterionSpec, DrillReport, DrillScenario, PhaseSample

pytestmark = pytest.mark.kiwi_id(2487)


def _config(tmp_path: Path) -> ClusterDrillConfig:
    """构造演练配置。"""

    return ClusterDrillConfig(
        env="test",
        ssh_target="user@host",
        ssh_port=2222,
        work_dir="/srv/msg-drill",
        project="msg-drill",
        container_prefix="msg-drill",
        db_prefix="msgdrill",
        ports=ConcurrentStableList((18800, 18801, 18802)),
        report_dir=tmp_path,
        variables=ConcurrentStableFrozenDict({"PLATFORM_NETWORK": "msg-drill-net"}),
    )


def test_config_parse_and_validate(tmp_path: Path) -> None:
    """配置：环境文件解析、必填键、正整数与生产拒绝。"""
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "MSG_CLUSTER_DRILL_SSH_TARGET=user@host",
                "MSG_CLUSTER_DRILL_WORK_DIR=/srv/work",
                "MSG_CLUSTER_DRILL_PORT_1=19000",
                "BAD_LINE",
            ]
        ),
        encoding="utf-8",
    )
    values = parse_env_file(env_file)
    assert values["MSG_CLUSTER_DRILL_SSH_TARGET"] == "user@host"

    config = load_config(repo_root=tmp_path, env_path=env_file, environ=ConcurrentStableFrozenDict())
    assert config.ssh_target == "user@host"
    assert list(config.ports) == [19000, 18801, 18802]

    with pytest.raises(ClusterDrillError, match="不存在"):
        parse_env_file(tmp_path / "missing.env")
    empty = tmp_path / "empty.env"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ClusterDrillError, match="SSH_TARGET"):
        load_config(repo_root=tmp_path, env_path=empty, environ=ConcurrentStableFrozenDict())
    with pytest.raises(ClusterDrillError):
        reject_production("prod")
    with pytest.raises(ClusterDrillError, match="二次确认"):
        require_confirmation("replica_crash", None)


def test_remote_scripts_and_argv(tmp_path: Path) -> None:
    """远程命令：SSH argv、compose 起停、镜像构建、源码接收脚本。"""
    config = _config(tmp_path)

    assert list(ssh_argv(config, "echo hi"))[:2] == ["ssh", "-o"]

    up = compose_up_script(config, commit="abc123")
    assert "msg-1 msg-2 msg-3" in up
    assert "cd /srv/msg-drill/msg" in up
    assert "MSG_TAG=drill-abc123" in up
    assert "MSG_REGISTRY_IMAGE_PREFIX=drill" in up
    assert "-v" in compose_down_script(config)
    assert "down --remove-orphans" in compose_down_script(config, volumes=False)
    assert "MSG_TAG=drill-cleanup" in compose_down_script(config)

    assert image_reference(config, commit="abc123") == "drill/msg:drill-abc123"
    build = build_image_script(config, commit="abc123")
    assert "--provenance=false" in build
    assert "-f msg/deploy/Dockerfile" in build
    assert "drill/msg:drill-abc123" in build
    assert "tar -xf -" in sync_source_script(config, local_root=tmp_path)

    pinned = ClusterDrillConfig(
        env="test",
        ssh_target="user@host",
        work_dir="/srv/msg-drill/",
        image_tag="pinned-1",
        ports=ConcurrentStableList((18800, 18801, 18802)),
        report_dir=tmp_path,
    )
    assert image_reference(pinned, commit="abc123") == "drill/msg:pinned-1"
    assert "MSG_TAG=pinned-1" in compose_up_script(pinned, commit="abc123")


def test_tunnel_argv_and_lifecycle(tmp_path: Path) -> None:
    """隧道：三端口转发参数；打开 / 就绪 / 关闭（含进程退出与超时强杀分支）。"""
    config = _config(tmp_path)
    argv = tunnel_argv(config)
    assert argv[0] == "ssh" and "-N" in argv
    assert [item for item in argv if item.startswith("127.0.0.1:")] == [
        "127.0.0.1:18800:127.0.0.1:18800",
        "127.0.0.1:18801:127.0.0.1:18801",
        "127.0.0.1:18802:127.0.0.1:18802",
    ]
    assert argv[-1] == "user@host"

    class FakeProcess:
        def __init__(self, *, exit_code: int | None = None, wait_error: bool = False) -> None:
            self.exit_code = exit_code
            self.wait_error = wait_error
            self.terminated = False
            self.killed = False

        def poll(self) -> int | None:
            return self.exit_code

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.exit_code = -9

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            if self.wait_error:
                self.wait_error = False
                raise subprocess.TimeoutExpired("ssh", 5)
            return self.exit_code if self.exit_code is not None else 0

    class FakeConn:
        def close(self) -> None:
            return None

    def failing_connector(address: tuple[str, int], timeout: float = 1) -> FakeConn:
        del address, timeout
        raise OSError("refused")

    def ok_connector(address: tuple[str, int], timeout: float = 1) -> FakeConn:
        del address, timeout
        return FakeConn()

    not_opened = SshTunnel(config)
    assert not_opened.wait_ready() is False
    not_opened.close()

    dead = SshTunnel(config, popen=lambda *_a, **_k: FakeProcess(exit_code=255), connector=ok_connector)
    dead.open()
    assert dead.wait_ready() is False
    dead.close()

    delayed = SshTunnel(
        config,
        popen=lambda *_a, **_k: FakeProcess(),
        connector=failing_connector,
        attempts=2,
        sleeper=lambda _s: None,
    )
    delayed.open()
    assert delayed.wait_ready() is False
    delayed.close()

    healthy = SshTunnel(
        config,
        popen=lambda *_a, **_k: FakeProcess(),
        connector=ok_connector,
        sleeper=lambda _s: None,
    )
    healthy.open()
    assert healthy.wait_ready() is True
    healthy.close()

    stubborn_process = FakeProcess(wait_error=True)
    stubborn = SshTunnel(
        config,
        popen=lambda *_a, **_k: stubborn_process,
        connector=ok_connector,
        sleeper=lambda _s: None,
    )
    stubborn.open()
    stubborn.close()
    assert stubborn_process.killed is True


def test_sync_source_packs_workspace_layout(tmp_path: Path) -> None:
    """源码同步：按工作区根布局打包 msg 仓 + 基座源码子目录；缺基座直接拒。"""
    workspace = tmp_path / "ws"
    (workspace / "msg").mkdir(parents=True)
    (workspace / "bms" / "backend" / "libs" / "bms_core").mkdir(parents=True)
    captured: ConcurrentStableList[ConcurrentStableList[str]] = ConcurrentStableList()

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        captured.add(ConcurrentStableList(argv))
        return subprocess.CompletedProcess(argv, 0, stdout=b"ok", stderr=b"")

    result = RemoteRunner(_config(tmp_path), runner=runner).sync_source(local_root=workspace / "msg")
    assert result.ok is True
    assert result.stdout == "ok"
    tar_argv = captured[0]
    assert "msg" in tar_argv
    assert "bms/backend/libs/bms_core" in tar_argv
    assert "mkdir -p /srv/msg-drill" in captured[1][-1]

    empty = tmp_path / "no-base"
    (empty / "msg").mkdir(parents=True)
    with pytest.raises(ClusterDrillError, match="基座源码"):
        RemoteRunner(_config(tmp_path), runner=runner).sync_source(local_root=empty / "msg")

    assert remote_mod._text(None) == ""  # pyright: ignore[reportPrivateUsage]
    assert remote_mod._text("x") == "x"  # pyright: ignore[reportPrivateUsage]


def test_inject_commands_and_steps(tmp_path: Path) -> None:
    """注入：信号 / 停止 / 启停 / 断网命令与三类场景步骤。"""
    config = _config(tmp_path)

    assert "SIGKILL" in kill_command("c")
    assert stop_command("c").startswith("docker stop")
    assert start_command("c").startswith("docker start")
    assert "network disconnect" in disconnect_command("c", "net")
    assert "network connect" in connect_command("c", "net")
    assert network_name(config) == "msg-drill-net"
    assert "docker network create msg-drill-net" in cluster_inject.ensure_network_script(config)
    assert "docker network rm msg-drill-net" in cluster_inject.remove_network_script(config)

    assert len(steps_for("replica_crash", config)) == 2
    partition_steps = steps_for("network_partition_multi", config, leader="msg-drill-2")
    assert len(partition_steps) == 4
    disconnected = [step.command for step in partition_steps if step.phase == "inject"]
    assert all("msg-drill-2" not in command.split()[-1] for command in disconnected[1:])
    assert len(steps_for("split_brain_multi", config)) == 2
    with pytest.raises(KeyError):
        steps_for("unknown", config)

    attach = cluster_inject.ensure_attached_script(config)
    assert attach.count("docker network connect --alias msg-") == 3
    assert cluster_inject.node_alias(config, "msg-drill-2") == "msg-2"
    rejoin = steps_for("split_brain_multi", config, leader="msg-drill-2")[-1]
    assert "--alias msg-2" in rejoin.command


def test_apply_steps_failure_raises(tmp_path: Path) -> None:
    """注入执行：任一步失败即抛（不静默继续）。"""
    config = _config(tmp_path)

    class FailingRunner(RemoteRunner):
        def run(self, script: str) -> RemoteResult:
            del script
            return RemoteResult(code=1, stderr="boom")

    with pytest.raises(ClusterDrillError, match="失败"):
        apply_steps(FailingRunner(config), steps_for("replica_crash", config), phase="inject")


def test_probe_parsers() -> None:
    """探针：readyz 判定、检查项、指标解析与同名求和。"""
    reading = parse_readyz(
        200,
        {"status": "ok", "checks": {"msg_storage": {"ok": True}, "msg_recovery": {"ok": False}}},
    )
    assert reading.ok is True
    assert reading.checks["msg_storage"] == "ok"
    assert reading.checks["msg_recovery"] == "failed"
    assert parse_readyz(503, {"status": "degraded"}).ok is False
    assert parse_readyz(200, "text").ok is True

    samples = parse_metrics('# help\nmsg_isr_size{topic_id="9"} 2\nmsg_isr_size{topic_id="10"} 3\nmsg_bad x\n')
    assert metric_value(samples, "msg_isr_size") == 5


async def test_probe_client() -> None:
    """探针客户端：三实例全就绪、不可达返回 0。"""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/readyz":
            return httpx.Response(200, json={"status": "ok", "checks": {}})
        return httpx.Response(200, text="msg_isr_size 1\n")

    client = ProbeClient(
        ConcurrentStableList(("http://a", "http://b", "http://c")),
        transport=httpx.MockTransport(handler),
    )
    assert await client.all_ready() is True
    assert metric_value(await client.metrics(0), "msg_isr_size") == 1
    await client.aclose()

    failing = ProbeClient(
        ConcurrentStableList(("http://a",)),
        transport=httpx.MockTransport(lambda _request: (_ for _ in ()).throw(httpx.ConnectError("x"))),
    )
    assert (await failing.readyz(0)).status == 0
    assert await failing.metrics(0) == []
    await failing.aclose()


def test_data_compare_distinguishes_loss_and_tail() -> None:
    """数据核对：HW 前缺席算丢；HW 后缺席只算未提交尾部。"""
    verdict = compare_committed(
        committed=ConcurrentStableDict({"a": 1, "b": 2, "c": 3}),
        consumed=ConcurrentStableFrozenList(("a",)),
        high_watermark=3,
    )
    assert list(verdict.lost) == ["b"]
    assert list(verdict.retained_tail) == ["c"]
    assert verdict.data_loss is True

    clean = compare_committed(
        committed=ConcurrentStableDict({"a": 1}),
        consumed=ConcurrentStableFrozenList(("a",)),
        high_watermark=2,
    )
    assert clean.ok is True


async def test_client_refresh_flow() -> None:
    """客户端刷新：350033 → 元数据 → 改投成功；非 350033 直接记失败。"""

    async def publish(base: str) -> PublishOutcome:
        if base.endswith("18800"):
            return PublishOutcome(ok=False, code=350033, reason="not_leader", leader="msg-2", epoch=2)
        return PublishOutcome(ok=True, code=0, offset=7)

    async def metadata() -> MetadataView:
        return MetadataView(metadata_version=5, leader="msg-2", epoch=2)

    outcome = await refresh_and_retry(
        publish=publish,
        fetch_metadata=metadata,
        old_base="http://127.0.0.1:18800",
        metadata_version_before=4,
    )
    assert outcome.refreshed is True
    assert outcome.version_advanced is True
    assert outcome.second is not None and outcome.second.ok is True
    assert outcome.reasons == []

    async def denied(_base: str) -> PublishOutcome:
        return PublishOutcome(ok=False, code=350022, reason="watermark")

    rejected = await refresh_and_retry(
        publish=denied,
        fetch_metadata=metadata,
        old_base="http://127.0.0.1:18800",
        metadata_version_before=4,
    )
    assert rejected.refreshed is False
    assert rejected.reasons


def test_scenarios_build_criteria_and_degraded_note() -> None:
    """判据组装：缺失记 skipped；isr_verified=false 记降级。"""
    spec = DrillCriterionSpec(
        key="committed_not_lost",
        text="不丢",
        record_keys=ConcurrentStableFrozenList(("committed_not_lost",)),
        severity="hard",
        evidence="记录",
    )
    scenario = DrillScenario(
        name="replica_crash",
        title="t",
        inject="container_kill",
        observe=ConcurrentStableFrozenList(("msg_isr_size",)),
        criterion=ConcurrentStableFrozenList((spec,)),
        aux_record_keys=ConcurrentStableFrozenList(),
        recovery_budget_ms=1000,
        expect="e",
        target="docker",
    )
    criteria = build_criteria(scenario, ConcurrentStableDict())
    assert criteria[0].result == "skipped"
    assert missing_keys(scenario, ConcurrentStableDict()) == ["committed_not_lost"]
    assert degraded_note(False)[0] == "degraded"
    assert degraded_note(True)[0] == "ok"


def test_cleanup_success_and_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """清理：先删项目与卷、再删演练网络；compose down 失败即报失败。"""
    config = _config(tmp_path)
    scripts: ConcurrentStableList[str] = ConcurrentStableList()

    def ok_run(_runner: RemoteRunner, script: str) -> RemoteResult:
        scripts.add(script)
        return RemoteResult(code=0)

    monkeypatch.setattr(cluster_main.RemoteRunner, "run", ok_run)
    assert cluster_main.run_cleanup(config) == 0
    assert "--profile cluster down --remove-orphans -v" in scripts[0]
    assert "docker network rm msg-drill-net" in scripts[1]

    def fail_run(_runner: RemoteRunner, script: str) -> RemoteResult:
        del script
        return RemoteResult(code=1, stderr="boom")

    monkeypatch.setattr(cluster_main.RemoteRunner, "run", fail_run)
    assert cluster_main.run_cleanup(config) == 1


def test_plan_and_dry_run(tmp_path: Path) -> None:
    """命令行：plan 渲染三类；run --dry-run 零执行；未知场景拒绝。"""
    config = _config(tmp_path)
    assert cluster_main.run_plan(config) == 0

    scenario_set = cluster_main._scenario_set(config)  # pyright: ignore[reportPrivateUsage]
    scenario = next(item for item in scenario_set.scenarios if item.name == "replica_crash")
    import asyncio

    assert (
        asyncio.run(
            cluster_main.run_scenario(
                config,
                scenario,
                confirm="replica_crash",
                dry_run=True,
                skip_provision=True,
                topic_id=None,
                rerun_reason="",
            )
        )
        == 0
    )
    with pytest.raises(ClusterDrillError, match="二次确认"):
        asyncio.run(
            cluster_main.run_scenario(
                config,
                scenario,
                confirm=None,
                dry_run=True,
                skip_provision=True,
                topic_id=None,
                rerun_reason="",
            )
        )


def test_report_contract_has_docker_target(tmp_path: Path) -> None:
    """记录：target=docker 且字段齐备（复用 07-6 契约）。"""
    report = DrillReport(
        scenario="replica_crash",
        mode="run",
        env="test",
        target="docker",
        started_at=datetime(2026, 11, 12, tzinfo=UTC),
        finished_at=datetime(2026, 11, 12, tzinfo=UTC),
        injection=ConcurrentStableFrozenDict({"method": "container_kill"}),
        samples=ConcurrentStableFrozenList(),
        criteria=ConcurrentStableFrozenList(),
        rto_ms=0,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(),
        issues=ConcurrentStableFrozenList(),
        leftovers=ConcurrentStableFrozenList(),
    )
    from ops.cluster_drill.report import write_cluster_report

    json_path, markdown_path = write_cluster_report(report, tmp_path, title="副本崩溃")
    assert json_path.exists()
    assert markdown_path.exists()


# ---------------------------------------------------------------------------
# T8 采集链（chain）
# ---------------------------------------------------------------------------


def _topic_status_json(*partitions: tuple[int, str, int]) -> str:
    rows = [
        {
            "topic_id": 1,
            "partition_no": no,
            "leader": leader,
            "epoch": epoch,
            "switch_state": "idle",
            "replicas": ["msg-1", "msg-2", "msg-3"],
        }
        for no, leader, epoch in partitions
    ]
    return json.dumps({"topic_id": 1, "partitions": rows}, ensure_ascii=False)


def test_chain_parse_state_and_switch() -> None:
    """采集链解析：状态快照 / 切主结果；形态非法即抛。"""
    state = chain_mod.parse_topic_state(json.loads(_topic_status_json((0, "msg-1", 1), (1, "msg-2", 2))))
    assert [item.partition_no for item in state.partitions] == [0, 1]
    assert state.leader_of(1) == "msg-2"
    assert state.leader_of(9) == ""
    assert state.epoch_of(9) == -1

    items = chain_mod.parse_switch_result(
        {
            "items": [
                {
                    "partition_no": 0,
                    "from_leader": "msg-1",
                    "to_leader": "msg-2",
                    "from_epoch": 1,
                    "to_epoch": 2,
                    "isr_verified": True,
                    "retained_tail": 0,
                }
            ],
            "failures": [],
        }
    )
    assert len(items) == 1 and items[0].isr_verified is True

    with pytest.raises(ClusterDrillError):
        chain_mod.parse_topic_state([])  # pyright: ignore[reportArgumentType]
    with pytest.raises(ClusterDrillError):
        chain_mod.parse_topic_state({"partitions": "x"})
    with pytest.raises(ClusterDrillError):
        chain_mod.parse_topic_state({"partitions": [1]})
    with pytest.raises(ClusterDrillError):
        chain_mod.parse_switch_result({"items": [1]})


def test_chain_derive_facts_ok_and_failed() -> None:
    """判据推导：正常（不丢 / 不重 / 切主 / ISR 回升）与位点倒退。"""
    records = ConcurrentStableList(
        [
            chain_mod.PublishRecord(partition=0, offset=1, message_id="a", attempts=2),
            chain_mod.PublishRecord(partition=0, offset=2, message_id="b"),
            chain_mod.PublishRecord(partition=1, offset=1, message_id="c"),
        ]
    )
    markers = ConcurrentStableList(
        [
            chain_mod.PublishRecord(partition=0, offset=3, message_id="m0"),
            chain_mod.PublishRecord(partition=1, offset=2, message_id="m1"),
        ]
    )
    before = chain_mod.parse_topic_state(json.loads(_topic_status_json((0, "msg-1", 1), (1, "msg-1", 1))))
    after = chain_mod.parse_topic_state(json.loads(_topic_status_json((0, "msg-2", 2), (1, "msg-2", 2))))
    switch_items = ConcurrentStableFrozenList(
        [
            chain_mod.SwitchItem(
                partition_no=0, from_leader="msg-1", to_leader="msg-2", from_epoch=1, to_epoch=2, isr_verified=True
            )
        ]
    )
    facts = chain_mod.derive_facts(
        records=records,
        markers=markers,
        before=before,
        after=after,
        switch_items=switch_items,
        isr_before=6,
        isr_during=3,
        isr_after=6,
        probe_failed_during=True,
        recovery_ok=True,
    )
    assert facts["committed_not_lost"][0] == "ok"
    assert facts["no_double_write"][0] == "ok"
    assert facts["client_refresh"][0] == "ok"
    assert facts["leader_switched"][0] == "ok"
    assert facts["isr_verified"][0] == "ok"
    assert facts["isr_shrink_rejoin"][0] == "ok"
    assert facts["leader_lease_expired"][0] == "ok"
    assert facts["stale_leader_rejected"][0] == "ok"
    assert facts["rejoin_demotes"][0] == "ok"
    assert facts["rto_within_budget"][0] == "ok"

    broken = chain_mod.derive_facts(
        records=ConcurrentStableList(
            [
                chain_mod.PublishRecord(partition=0, offset=1, message_id="a"),
                chain_mod.PublishRecord(partition=0, offset=2, message_id="b"),
                chain_mod.PublishRecord(partition=1, offset=1, message_id="c"),
            ]
        ),
        markers=ConcurrentStableList([chain_mod.PublishRecord(partition=0, offset=2, message_id="m")]),
        before=before,
        after=after,
        switch_items=ConcurrentStableFrozenList(),
        isr_before=3,
        isr_during=3,
        isr_after=3,
        probe_failed_during=False,
        recovery_ok=False,
    )
    assert broken["committed_not_lost"][0] == "failed"
    assert broken["client_refresh"][0] == "degraded"
    assert broken["leader_switched"][0] == "ok"
    assert broken["isr_shrink_rejoin"][0] == "degraded"
    assert broken["rto_within_budget"][0] == "failed"

    duplicated = chain_mod.derive_facts(
        records=ConcurrentStableList(
            [
                chain_mod.PublishRecord(partition=0, offset=1, message_id="a"),
                chain_mod.PublishRecord(partition=0, offset=1, message_id="b"),
            ]
        ),
        markers=ConcurrentStableList([chain_mod.PublishRecord(partition=0, offset=2, message_id="m")]),
        before=before,
        after=after,
        switch_items=ConcurrentStableFrozenList(),
        isr_before=0,
        isr_during=0,
        isr_after=0,
        probe_failed_during=False,
        recovery_ok=True,
    )
    assert duplicated["no_double_write"][0] == "failed"
    assert duplicated["isr_shrink_rejoin"][0] == "degraded"


async def test_chain_client_publish_and_topic() -> None:
    """采集客户端：建主题（成功 / 已存在 / 失败）、发布（350033 改投、350032 等待、硬失败）。"""
    calls: ConcurrentStableList[str] = ConcurrentStableList()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.add(f"{request.url.path}")
        body = json.loads(request.content or b"{}")
        if request.url.path.endswith("/admin/topics"):
            if body.get("name") == "exists":
                return httpx.Response(409, json={"code": chain_mod.TOPIC_EXISTS})
            if body.get("name") == "boom":
                return httpx.Response(500, json={"code": 10000})
            return httpx.Response(200, json={"code": 0, "data": {"id": "42"}})
        leader = body.get("key")
        if leader == "notleader" and request.url.host == "a":
            return httpx.Response(503, json={"code": chain_mod.NOT_LEADER, "data": {"partition": 0, "leader": "msg-2"}})
        if leader == "insufficient":
            return httpx.Response(503, json={"code": chain_mod.INSUFFICIENT_ISR})
        if leader == "hard":
            return httpx.Response(400, json={"code": 10001})
        return httpx.Response(200, json={"code": 0, "data": {"partition": 1, "offset": 7, "message_id": "mid"}})

    transport = httpx.MockTransport(handler)
    client = chain_mod.DrillClient(
        ConcurrentStableList(("http://a", "http://b")),
        tenant="demo",
        namespace="msg",
        topic="ok",
        transport=transport,
        sleeper=_no_sleep,
    )
    try:
        assert await client.ensure_topic(3) == 42
        record = await client.publish("k1", b"x")
        assert record.offset == 7
        refreshed = await client.publish("notleader", b"x")
        assert refreshed.attempts >= 1
        batch = await client.publish_batch(2)
        assert len(batch) == 2
        leaders = await client.probe_partition_leaders("notleader")
        assert leaders[0] == "msg-2"
        with pytest.raises(chain_mod.ChainError, match="发布失败（重试耗尽）"):
            await client.publish("insufficient", b"x")
        with pytest.raises(chain_mod.ChainError, match="发布失败"):
            await client.publish("hard", b"x")
    finally:
        await client.aclose()

    missing = chain_mod.DrillClient(
        ConcurrentStableList(("http://a",)),
        tenant="demo",
        namespace="msg",
        topic="exists",
        transport=transport,
        sleeper=_no_sleep,
    )
    try:
        with pytest.raises(chain_mod.ChainError, match="已存在"):
            await missing.ensure_topic(3)
    finally:
        await missing.aclose()

    broken = chain_mod.DrillClient(
        ConcurrentStableList(("http://a",)),
        tenant="demo",
        namespace="msg",
        topic="boom",
        transport=transport,
        sleeper=_no_sleep,
    )
    try:
        with pytest.raises(chain_mod.ChainError, match="建主题失败"):
            await broken.ensure_topic(3)
    finally:
        await broken.aclose()

    empty = chain_mod.DrillClient(
        ConcurrentStableList(("http://a",)),
        tenant="demo",
        namespace="msg",
        topic="ok",
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, text="not-json")),
        sleeper=_no_sleep,
    )
    try:
        with pytest.raises(chain_mod.ChainError, match="建主题失败"):
            await empty.ensure_topic(3)
    finally:
        await empty.aclose()


def test_topic_state_helpers(tmp_path: Path) -> None:
    """状态读取 / 写入探针 / ISR 合计：库直读、缺模板、坏连接均按失败口径。"""
    dsn = f"sqlite+aiosqlite:///{(tmp_path / 'drill.db').as_posix()}"
    asyncio.run(_prepare_sqlite(dsn))
    state = asyncio.run(cluster_main._topic_state(dsn, 1))  # pyright: ignore[reportPrivateUsage]
    assert state.leader_of(0) == "msg-1"
    assert state.epoch_of(0) == 2

    bad = f"sqlite+aiosqlite:///{(tmp_path / 'none.db').as_posix()}"
    with pytest.raises(ClusterDrillError, match="查询失败"):
        asyncio.run(cluster_main._topic_state(bad, 1))  # pyright: ignore[reportPrivateUsage]

    variables = ConcurrentStableFrozenDict(
        {
            "BMS_DATABASE__PLATFORM__URL_TEMPLATE": "mysql+aiomysql://app@db:3306/{database}",
            "BMS_DATABASE__PLATFORM__PASSWORD": "p@ss",
        }
    )
    assert chain_mod.platform_dsn(variables, db_prefix="msgdrill") == (
        "mysql+aiomysql://app:p%40ss@db:3306/msgdrill_msg"
    )
    assert chain_mod.platform_dsn(variables, db_prefix="msg") == "mysql+aiomysql://app:p%40ss@db:3306/msg"
    plain = ConcurrentStableFrozenDict({"BMS_DATABASE__PLATFORM__URL_TEMPLATE": "sqlite+aiosqlite:///x"})
    assert chain_mod.platform_dsn(plain, db_prefix="msgdrill") == "sqlite+aiosqlite:///x"
    with pytest.raises(ClusterDrillError, match="缺少"):
        chain_mod.platform_dsn(ConcurrentStableFrozenDict(), db_prefix="msgdrill")
    with pytest.raises(ClusterDrillError, match="形态非法"):
        chain_mod.platform_dsn(
            ConcurrentStableFrozenDict(
                {
                    "BMS_DATABASE__PLATFORM__URL_TEMPLATE": "mysql://bad",
                    "BMS_DATABASE__PLATFORM__PASSWORD": "x",
                }
            ),
            db_prefix="msgdrill",
        )

    samples = ConcurrentStableList(
        [
            PhaseSample(
                phase="before",
                captured_at=_NOW,
                readyz_status=200,
                readyz_ok=True,
                healthz_ok=True,
                detail="",
                metrics=ConcurrentStableFrozenDict({"msg_isr_size": 3}),
            ),
            PhaseSample(
                phase="before",
                captured_at=_NOW,
                readyz_status=0,
                readyz_ok=False,
                healthz_ok=False,
                detail="",
                metrics=ConcurrentStableFrozenDict({"msg_isr_size": 1}),
            ),
        ]
    )
    assert cluster_main._isr_total(samples) == 3  # pyright: ignore[reportPrivateUsage]
    assert cluster_main._isr_total(ConcurrentStableList()) == 0  # pyright: ignore[reportPrivateUsage]

    class _RejectingClient:
        async def publish_at(self, base: str, key: str) -> ConcurrentStableFrozenDict[str, object]:
            del base, key
            return ConcurrentStableFrozenDict({"status": 503, "leader": "msg-2"})

    class _AcceptingClient:
        async def publish_at(self, base: str, key: str) -> ConcurrentStableFrozenDict[str, object]:
            del base, key
            return ConcurrentStableFrozenDict({"status": 200, "leader": ""})

    assert (
        asyncio.run(cluster_main._probe_publish_rejected(_RejectingClient(), "http://a"))  # pyright: ignore[reportPrivateUsage, reportArgumentType]
        is True
    )
    assert (
        asyncio.run(cluster_main._probe_publish_rejected(_AcceptingClient(), "http://a"))  # pyright: ignore[reportPrivateUsage, reportArgumentType]
        is False
    )


async def _prepare_sqlite(dsn: str) -> None:
    """建一张最小 `msg_partition` 表并插一行（供直读状态用例）。"""
    engine = create_async_engine(dsn)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "create table msg_partition (topic_id int, partition_no int, leader text, epoch int, "
                    "replicas text, switch_state text)"
                )
            )
            await connection.execute(
                text("insert into msg_partition values (1, 0, 'msg-1', 2, '[\"msg-2\", \"msg-1\"]', 'idle')")
            )
    finally:
        await engine.dispose()


_NOW = "2026-11-12T00:00:00+00:00"


async def _no_sleep(_seconds: float) -> None:
    """测试用空等待（跳过 `350032` 重试间隔）。"""
    return None


async def test_chain_client_transport_error() -> None:
    """传输异常按发布失败处理（不影响后续改投判定）。"""
    offline = chain_mod.DrillClient(
        ConcurrentStableList(("http://a",)),
        tenant="demo",
        namespace="msg",
        topic="ok",
        transport=httpx.MockTransport(lambda _r: (_ for _ in ()).throw(httpx.ConnectError("x"))),
        sleeper=_no_sleep,
    )
    try:
        with pytest.raises(chain_mod.ChainError, match="发布失败"):
            await offline.publish("k", b"x")
    finally:
        await offline.aclose()


def test_container_for_and_wait_isr(tmp_path: Path) -> None:
    """节点→容器映射；ISR 等待在达标后立即返回。"""
    config = _config(tmp_path)
    assert cluster_main._container_for(config, "msg-2") == "msg-drill-2"  # pyright: ignore[reportPrivateUsage]
    assert cluster_main._container_for(config, "") == "msg-drill-1"  # pyright: ignore[reportPrivateUsage]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, text='msg_isr_size{topic_id="1",partition="0"} 3\nmsg_isr_size{topic_id="1",partition="1"} 3\n'
        )

    client = ProbeClient(
        ConcurrentStableList(("http://a", "http://b", "http://c")),
        transport=httpx.MockTransport(handler),
    )
    try:
        assert asyncio.run(cluster_main._wait_isr(client, 6, topic_id=1, timeout_sec=1)) is True  # pyright: ignore[reportPrivateUsage]
    finally:
        asyncio.run(client.aclose())


def test_chain_old_leader_demoted_facts() -> None:
    """旧 leader 自报 follower 时，切主与围栏按真实现象记 ok（无 DB 换主证据也可判）。"""
    same = chain_mod.parse_topic_state(json.loads(_topic_status_json((0, "msg-1", 0), (1, "msg-1", 0))))
    facts = chain_mod.derive_facts(
        records=ConcurrentStableList([chain_mod.PublishRecord(partition=0, offset=1, message_id="a")]),
        markers=ConcurrentStableList([chain_mod.PublishRecord(partition=0, offset=2, message_id="m")]),
        before=same,
        after=same,
        switch_items=ConcurrentStableFrozenList(),
        isr_before=3,
        isr_during=2,
        isr_after=3,
        probe_failed_during=True,
        recovery_ok=True,
        old_leader_demoted=True,
    )
    assert facts["leader_switched"][0] == "ok"
    assert facts["old_leader_fenced"][0] == "ok"


def test_leader_changed_probe(tmp_path: Path) -> None:
    """注入期换主探测：自报 leader / 响应者本身两种证据都能判出换主。"""
    bases: ConcurrentStableList[str] = ConcurrentStableList(("http://a", "http://b", "http://c"))

    class _Client:
        def __init__(self, replies: ConcurrentStableList[ConcurrentStableFrozenDict[str, object]]) -> None:
            self._replies = replies

        async def publish_at(self, base: str, key: str) -> ConcurrentStableFrozenDict[str, object]:
            del base, key
            if not self._replies:
                return ConcurrentStableFrozenDict({"status": 200, "partition": 1, "leader": ""})
            return self._replies.pop(0)

    changed = asyncio.run(
        cluster_main._leader_changed(  # pyright: ignore[reportPrivateUsage]
            _Client(
                ConcurrentStableList((ConcurrentStableFrozenDict({"status": 503, "partition": 0, "leader": "msg-2"}),))
            ),
            bases,
            "msg-1",
            0,
        )
    )
    assert changed is True

    unchanged = asyncio.run(
        cluster_main._leader_changed(  # pyright: ignore[reportPrivateUsage]
            _Client(
                ConcurrentStableList((ConcurrentStableFrozenDict({"status": 503, "partition": 0, "leader": "msg-1"}),))
            ),
            bases,
            "msg-1",
            0,
        )
    )
    assert unchanged is False


def test_last_json_object_skips_log_lines() -> None:
    """stdout 混有 structlog 日志时取最后一个 JSON 对象；无 JSON 即抛。"""
    noisy = (
        "2026-11-12 04:08:34 [info ] msg_leader_switch_begin partition=1\n"
        "2026-11-12 04:08:39 [info ] msg_leader_switch_finished {not json}\n"
        '{"items": [], "failures": []}\n'
    )
    parsed = chain_mod.last_json_object(noisy)
    assert isinstance(parsed, dict) and "items" in parsed
    assert chain_mod.last_json_object('{"a": 1}') == {"a": 1}
    with pytest.raises(ClusterDrillError, match="未找到 JSON"):
        chain_mod.last_json_object("纯日志，没有对象")


def test_restart_script_covers_all_instances(tmp_path: Path) -> None:
    """前置重置脚本按容器名逐实例重启（派生态不硬编码）。"""
    config = _config(tmp_path)
    script = remote_mod.restart_script(config)
    assert script.startswith("docker restart ")
    for name in ("msg-drill-1", "msg-drill-2", "msg-drill-3"):
        assert name in script


def test_isr_total_of_takes_per_partition_max() -> None:
    """ISR 合计按分区取各节点最大值：降主节点残留的旧行不得把总数拉低或拉高。

    实测：恢复后各 leader 都报 ISR=3，但降主节点的旧行仍在（值 1），
    直接三实例求和在 5 附近徘徊，`_wait_isr` 永远等不到 9。
    """

    def sample(topic: str, partition: str, value: float) -> MetricSample:
        return MetricSample(
            name="msg_isr_size",
            labels=ConcurrentStableFrozenDict({"topic_id": topic, "partition": partition}),
            value=value,
        )

    payloads: ConcurrentStableList[ConcurrentStableList[MetricSample]] = ConcurrentStableList(
        (
            ConcurrentStableList((sample("1", "0", 1.0), sample("1", "1", 3.0), sample("1", "2", 1.0))),
            ConcurrentStableList((sample("1", "0", 1.0), sample("1", "1", 1.0), sample("1", "2", 3.0))),
            ConcurrentStableList((sample("1", "0", 3.0), sample("1", "1", 1.0), sample("1", "2", 1.0))),
            ConcurrentStableList((sample("2", "0", 3.0),)),
        )
    )
    assert cluster_main._isr_total_of(payloads, topic_id=1) == 9  # pyright: ignore[reportPrivateUsage]
    assert cluster_main._isr_total_of(payloads, topic_id=2) == 3  # pyright: ignore[reportPrivateUsage]
    assert cluster_main._isr_total_of(payloads, topic_id=3) == 0  # pyright: ignore[reportPrivateUsage]
