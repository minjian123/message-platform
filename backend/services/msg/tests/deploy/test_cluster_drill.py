"""集群演练包（02-8）：配置 / 命令构造 / 注入步骤 / 探针解析 / 数据核对 / 客户端刷新 / 记录。"""

from __future__ import annotations

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

from ops.cluster_drill import __main__ as cluster_main
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
from ops.cluster_drill.probe import ProbeClient, metric_value, parse_metrics, parse_readyz
from ops.cluster_drill.remote import (
    RemoteResult,
    RemoteRunner,
    build_image_script,
    compose_down_script,
    compose_up_script,
    ssh_argv,
    sync_source_script,
)
from ops.cluster_drill.scenarios import build_criteria, degraded_note, missing_keys
from ops.drill.model import DrillCriterionSpec, DrillReport, DrillScenario


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
    assert "msg-1 msg-2 msg-3" in compose_up_script(config)
    assert "-v" in compose_down_script(config)
    assert "docker build" in build_image_script(config, commit="abc123")
    assert "tar -xf -" in sync_source_script(config, local_root=tmp_path)


def test_inject_commands_and_steps(tmp_path: Path) -> None:
    """注入：信号 / 停止 / 启停 / 断网命令与三类场景步骤。"""
    config = _config(tmp_path)

    assert "SIGKILL" in kill_command("c")
    assert stop_command("c").startswith("docker stop")
    assert start_command("c").startswith("docker start")
    assert "network disconnect" in disconnect_command("c", "net")
    assert "network connect" in connect_command("c", "net")
    assert network_name(config) == "msg-drill-net"

    assert len(steps_for("replica_crash", config)) == 2
    assert len(steps_for("network_partition_multi", config)) == 4
    assert len(steps_for("split_brain_multi", config)) == 2
    with pytest.raises(KeyError):
        steps_for("unknown", config)


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
