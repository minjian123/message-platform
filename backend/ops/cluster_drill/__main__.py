"""多实例故障切换演练编排（02-8）：`plan` / `run` / `verify` / `cleanup`。

**为什么单独入口**：07-6 的 `ops.drill` 只跑本机隔离实例；多实例场景要经 SSH 调 Docker、开隧道、
注入容器故障，放在同一命令里会让「本机演练」也背上远程依赖。

退出码与 07-6 同口径：`0` 通过、`1` 失败、`2` 降级（含 `force` / 可降级项）。
"""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from ops.cluster_drill import chain, inject, scenarios
from ops.cluster_drill import cleanup as cluster_cleanup
from ops.cluster_drill.config import (
    ClusterDrillConfig,
    ClusterDrillError,
    load_config,
    reject_production,
    require_confirmation,
)
from ops.cluster_drill.probe import MetricSample, ProbeClient, metric_value
from ops.cluster_drill.remote import (
    RemoteRunner,
    SshTunnel,
    build_image_script,
    compose_up_script,
    restart_script,
)
from ops.cluster_drill.report import backup_evidence, cluster_config_digest, write_cluster_report
from ops.drill.env import TARGET_DOCKER, load_scenarios, validate_scenarios
from ops.drill.model import (
    Criterion,
    DrillReport,
    DrillScenario,
    PhaseSample,
    ScenarioSet,
)

__all__ = ["main", "run_plan", "run_scenario"]

_EXIT_PARAM = 2
"""参数 / 生产拒绝的退出码（与「降级通过」同码）。"""

_PROBE_INTERVAL_MS = 2_000
"""就绪轮询间隔（毫秒）。"""

_INJECTION_SETTLE_SEC = 12.0
"""注入后等待（秒）：ISR 收缩判据依赖 `lag_time_max_ms`（缺省 10s）内的拉取中断。"""

_EXPECTED_ISR_PER_PARTITION = 3
"""每分区的期望 ISR（三副本全同步）。"""


class _Outcome:
    """一次场景执行的中间结果（记录组装用）。"""

    def __init__(self) -> None:
        self.criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
        self.commands: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
        self.issues: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
        self.started_at = datetime.now(UTC)
        self.finished_at = datetime.now(UTC)
        self.rto_ms = 0
        self.data_loss = False


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.cluster_drill", description="msg 多实例故障演练（02-8）")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("plan", help="渲染三类多实例场景与注入步骤（不执行）")

    run = sub.add_parser("run", help="执行一个多实例场景")
    run.add_argument("scenario", help="场景标识（replica_crash / network_partition_multi / split_brain_multi）")
    run.add_argument("--confirm", dest="confirm", default=None, help="二次确认：必须等于场景标识")
    run.add_argument("--dry-run", action="store_true", help="只渲染注入与恢复动作，零执行")
    run.add_argument(
        "--skip-provision", action="store_true", dest="skip_provision", help="跳过源码同步与镜像构建（复用已起实例）"
    )
    run.add_argument("--topic-id", type=int, dest="topic_id", default=None, help="演练主题主键（省略则按参数新建）")
    run.add_argument("--namespace", default="msg", help="命名空间（须为已启用 module_key）")
    run.add_argument("--topic-name", dest="topic_name", default="drill.replica.probe", help="演练主题名")
    run.add_argument("--tenant", default="demo", help="租户编码（数据面 dev 覆盖 / 管理面头）")
    run.add_argument("--partition-count", type=int, dest="partition_count", default=3, help="新建主题的分区数")
    run.add_argument("--publish-count", type=int, dest="publish_count", default=30, help="注入前 acks=all 发布条数")
    run.add_argument("--rerun-reason", dest="rerun_reason", default="", help="复跑原因")

    verify = sub.add_parser("verify", help="对账多实例记录并生成矩阵")
    verify.add_argument("--records", action="append", required=True, help="记录目录（可重复）")

    clean = sub.add_parser("cleanup", help="停止演练容器并清理卷 / 网络 / 演练库 / Redis / 工作区 / 镜像（幂等）")
    clean.add_argument(
        "--keep-data",
        action="store_true",
        dest="keep_data",
        help="保留演练库与 Redis 数据（只清容器 / 卷 / 目录 / 镜像）",
    )
    return parser


def _scenario_set(config: ClusterDrillConfig) -> ScenarioSet:
    """读取并校验场景单源（只保留多实例场景）。

    Args:
        config: 演练配置。

    Returns:
        ScenarioSet: 多实例场景集。

    Raises:
        ClusterDrillError: 单源非法。
    """
    root = Path(__file__).resolve().parents[3]
    scenario_set, errors = load_scenarios(root / "deploy" / "drill" / "scenarios.toml")
    problems: ConcurrentStableList[str] = ConcurrentStableList(errors)
    problems.extend(validate_scenarios(scenario_set, config.recovery_budget_ms))
    if problems:
        raise ClusterDrillError("cluster-drill.scenario 场景单源非法：" + "；".join(problems))
    return ScenarioSet(
        schema_version=scenario_set.schema_version,
        updated_at=scenario_set.updated_at,
        owner=scenario_set.owner,
        scenarios=ConcurrentStableFrozenList(item for item in scenario_set.scenarios if item.target == TARGET_DOCKER),
    )


def run_plan(config: ClusterDrillConfig) -> int:
    """渲染三类场景与注入步骤。

    Args:
        config: 演练配置。

    Returns:
        int: 退出码（恒 0）。
    """
    scenario_set = _scenario_set(config)
    print(f"多实例场景：{len(scenario_set.scenarios)} 类（env={config.env}、project={config.project}）")
    for scenario in scenario_set.scenarios:
        print(f"[{scenario.name}] {scenario.title}")
        for step in inject.steps_for(scenario.name, config):
            print(f"  [{step.phase}] {step.name}: {step.command}")
    return 0


async def _wait_ready(client: ProbeClient, timeout_sec: int) -> bool:
    """轮询三实例就绪（异步等待，不嵌套事件循环）。

    Args:
        client: 探针。
        timeout_sec: 超时（秒）。

    Returns:
        bool: 超时前全部就绪为 True。
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if await client.all_ready():
            return True
        await asyncio.sleep(_PROBE_INTERVAL_MS / 1000)
    return False


async def _samples(client: ProbeClient, phase: str, *, topic_id: int) -> ConcurrentStableList[PhaseSample]:
    """采集一段观测（三实例各一条；ISR 取「按分区各节点最大值的合计」）。

    Args:
        client: 探针。
        phase: 阶段（`before` / `during` / `after`）。
        topic_id: 主题主键（ISR 只统计本主题的分区）。

    Returns:
        ConcurrentStableList[PhaseSample]: 三实例样本。
    """
    readings: ConcurrentStableList[tuple[bool, int, ConcurrentStableList[MetricSample]]] = ConcurrentStableList()
    payloads: ConcurrentStableList[ConcurrentStableList[MetricSample]] = ConcurrentStableList()
    for index in range(3):
        reading = await client.readyz(index)
        metrics = await client.metrics(index)
        payloads.add(metrics)
        readings.add((reading.ok, reading.status, metrics))
    isr_total = _isr_total_of(payloads, topic_id=topic_id)
    result: ConcurrentStableList[PhaseSample] = ConcurrentStableList()
    for ready, status, metrics in readings:
        result.add(
            PhaseSample(
                phase=phase,
                captured_at=datetime.now(UTC).isoformat(),
                readyz_status=status,
                readyz_ok=ready,
                healthz_ok=status == 200,
                metrics=ConcurrentStableFrozenDict(
                    {
                        "msg_leader_switch_total": metric_value(metrics, "msg_leader_switch_total"),
                        "msg_isr_size": float(isr_total),
                        "msg_publish_errors_total": metric_value(metrics, "msg_publish_errors_total"),
                    }
                ),
                detail="",
            )
        )
    return result


def _isr_total_of(payloads: ConcurrentStableList[ConcurrentStableList[MetricSample]], *, topic_id: int) -> int:
    """按分区取各节点自报的 ISR 最大值再求和（同主题的多节点读数去重）。

    节点降主后其旧分区的 `msg_isr_size` 行仍留在指标里且不再更新，直接把三实例所有行
    相加会在 3~27 之间乱跳，无法判断「ISR 是否回升」；只有当前 leader 的行是最新的。

    Args:
        payloads: 三实例的原始指标读数。
        topic_id: 主题主键。

    Returns:
        int: ISR 合计（分区数 × 每分区最大 ISR）。
    """
    best: ConcurrentStableDict[tuple[str, str], float] = ConcurrentStableDict()
    for samples in payloads:
        for sample in samples:
            if sample.name != "msg_isr_size":
                continue
            if sample.labels.get("topic_id") != str(topic_id):
                continue
            key = (sample.labels.get("topic_id", ""), sample.labels.get("partition", ""))
            best.set(key, max(best.get(key, 0.0), sample.value))
    return int(sum(best.values()))


def _isr_total(samples: ConcurrentStableList[PhaseSample]) -> int:
    """某一段样本里的 ISR 合计（取三实例最大值，避免不可达实例拉低）。

    Args:
        samples: 同一阶段的样本（每实例一条）。

    Returns:
        int: ISR 合计。
    """
    return max((int(sample.metrics.get("msg_isr_size", 0)) for sample in samples), default=0)


async def _topic_state(dsn: str, topic_id: int) -> chain.TopicState:
    """取分区状态快照（直读平台库 `msg_partition`；不依赖容器内 CLI 插件引导）。

    Args:
        dsn: 平台库连接串。
        topic_id: 主题主键。

    Returns:
        chain.TopicState: 分区状态。

    Raises:
        ClusterDrillError: 查询失败。
    """
    try:
        return await chain.load_topic_state(dsn, topic_id=topic_id)
    except chain.ChainError as exc:
        raise ClusterDrillError(str(exc)) from exc


async def _ensure_isr(
    client: ProbeClient,
    drill: chain.DrillClient,
    target: int,
    *,
    topic_id: int,
    rounds: int = 3,
    round_timeout_sec: float = 60,
) -> bool:
    """预热 + 等 ISR 收敛到期望合计（落后副本需被写请求「碰一次」才建拉取态）。

    Args:
        client: 探针。
        drill: 采集客户端（提供预热）。
        target: 期望 ISR 合计（3 实例 × 分区数）。
        topic_id: 主题主键。
        rounds: 预热轮数上限。
        round_timeout_sec: 每轮等待上限（秒）。

    Returns:
        bool: 收敛 True。
    """
    for _ in range(rounds):
        await drill.warmup()
        try:
            await drill.publish("warm-commit", b"warmup-commit")
        except chain.ChainError:
            continue
        if await _wait_isr(client, target, topic_id=topic_id, timeout_sec=round_timeout_sec):
            return True
    return False


async def _wait_isr(client: ProbeClient, target: int, *, topic_id: int, timeout_sec: float = 180) -> bool:
    """等 ISR 回升到目标值（副本重入判据依赖 `lag_time_max_ms` 窗口）。

    Args:
        client: 探针。
        target: 目标 ISR 合计（3 实例 × 分区数）。
        topic_id: 主题主键。
        timeout_sec: 超时（秒）。

    Returns:
        bool: 达标 True。
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        payloads: ConcurrentStableList[ConcurrentStableList[MetricSample]] = ConcurrentStableList()
        for index in range(3):
            payloads.add(await client.metrics(index))
        if _isr_total_of(payloads, topic_id=topic_id) >= target:
            return True
        await asyncio.sleep(3)
    return False


def _container_for(config: ClusterDrillConfig, node: str) -> str:
    """把节点标识（`msg-1`）映射到演练容器名（`msg-drill-1`）。

    Args:
        config: 演练配置。
        node: 节点标识（空 = 首个实例）。

    Returns:
        str: 容器名。
    """
    suffix = node.rsplit("-", 1)[-1] if node else ""
    return f"{config.container_prefix}-{suffix}" if suffix else config.container_names[0]


async def _leader_changed(
    drill: chain.SingleShotPublisher,
    bases: ConcurrentStableList[str],
    old_leader: str,
    partition_no: int,
) -> bool:
    """注入期探测：同一分区是否已由别的节点持写权。

    Args:
        drill: 采集客户端。
        bases: 三实例地址（与容器序号对齐）。
        old_leader: 注入前该分区 leader。
        partition_no: 分区号。

    Returns:
        bool: 已换主 True。
    """
    for offset, base in enumerate(bases):
        info = await drill.publish_at(base, "k0")
        if int(cast("int", info.get("partition", -1))) != int(partition_no):
            continue
        node = str(info.get("leader") or "") or f"msg-{offset + 1}"
        if node != old_leader:
            return True
    return False


async def _probe_publish_rejected(drill: chain.SingleShotPublisher, base: str) -> bool:
    """注入期探针：**只写被隔离的那个实例**，被拒（非 leader / 租约到期）即 True。

    不能用「集群任意节点能写」来判：隔离旧 leader 后新 leader 仍可写，集群级发布
    一定成功，永远观测不到拒绝（真机踩坑）。只有定向打被隔离实例才能验证租约围栏。

    Args:
        drill: 采集客户端。
        base: 被隔离实例的地址。

    Returns:
        bool: 写入是否被拒。
    """
    result = await drill.publish_at(base, "probe")
    return int(cast("int", result.get("status", 0) or 0)) != 200


async def run_scenario(
    config: ClusterDrillConfig,
    scenario: DrillScenario,
    *,
    confirm: str | None,
    dry_run: bool,
    skip_provision: bool,
    topic_id: int | None,
    rerun_reason: str,
    namespace: str = "msg",
    topic_name: str = "drill.replica.probe",
    tenant: str = "demo",
    partition_count: int = 3,
    publish_count: int = 30,
) -> int:
    """执行一个多实例场景。

    Args:
        config: 演练配置。
        scenario: 场景定义（`target=docker`）。
        confirm: 二次确认值。
        dry_run: 只渲染动作。
        skip_provision: 跳过同步 / 构建。
        topic_id: 演练主题主键（None = 按命名空间 / 名称新建）。
        rerun_reason: 复跑原因。
        namespace: 命名空间（已启用 module_key）。
        topic_name: 演练主题名。
        tenant: 租户编码。
        partition_count: 新建主题的分区数。
        publish_count: 注入前 `acks=all` 发布条数。

    Returns:
        int: 退出码。
    """
    require_confirmation(scenario.name, confirm)
    steps = inject.steps_for(scenario.name, config)
    if dry_run:
        print(f"[PLAN] {scenario.name} · {scenario.title}（dry-run，零执行）")
        for step in steps:
            print(f"  [{step.phase}] {step.name}: {step.command}")
        return 0
    dsn = chain.platform_dsn(config.variables, db_prefix=config.db_prefix)
    run_topic = topic_name if topic_id is not None else f"{topic_name}.{scenario.name}.{int(time.time())}"
    outcome = _Outcome()
    runner = RemoteRunner(config)
    root = Path(__file__).resolve().parents[3]
    started = time.monotonic()
    if not skip_provision:
        sync = runner.sync_source(local_root=root)
        if not sync.ok:
            raise ClusterDrillError(f"cluster-drill.remote 源码同步失败：{sync.stderr.strip()[:200]}")
        commit = _commit(root)
        build = runner.run(build_image_script(config, commit=commit))
        outcome.commands.add(ConcurrentStableDict({"step": "build", "result": "ok" if build.ok else "failed"}))
        if not build.ok:
            raise ClusterDrillError(f"cluster-drill.remote 镜像构建失败：{build.stderr.strip()[:200]}")
        network = runner.run(inject.ensure_network_script(config))
        outcome.commands.add(
            ConcurrentStableDict({"step": "network_create", "result": "ok" if network.ok else "failed"})
        )
        if not network.ok:
            raise ClusterDrillError(f"cluster-drill.remote 演练网络创建失败：{network.stderr.strip()[:200]}")
        up = runner.run(compose_up_script(config, commit=commit))
        outcome.commands.add(ConcurrentStableDict({"step": "compose_up", "result": "ok" if up.ok else "failed"}))
        if not up.ok:
            raise ClusterDrillError(f"cluster-drill.remote 起三实例失败：{up.stderr.strip()[:200]}")
    bases: ConcurrentStableList[str] = ConcurrentStableList(f"http://127.0.0.1:{port}" for port in config.ports)
    client = ProbeClient(bases)
    drill = chain.DrillClient(bases, tenant=tenant, namespace=namespace, topic=run_topic)
    tunnel = SshTunnel(config)
    tunnel.open()
    records: ConcurrentStableList[chain.PublishRecord] = ConcurrentStableList()
    markers: ConcurrentStableList[chain.PublishRecord] = ConcurrentStableList()
    switch_items: ConcurrentStableFrozenList[chain.SwitchItem] = ConcurrentStableFrozenList()
    before_state: chain.TopicState | None = None
    after_state: chain.TopicState | None = None
    probe_failed_during = False
    recovery_ok = False
    try:
        if not tunnel.wait_ready():
            raise ClusterDrillError("cluster-drill.guard 本机 SSH 隧道未在预算内就绪")
        # 每轮前置重置：上一轮结束后副本拉取态 / epoch 会残留（产品缺陷，见遗留），
        # 不重置会让 ISR 永久停在 1，`acks=all` 直接 350032；重启可确定性回到健康基线。
        reset = runner.run(restart_script(config))
        outcome.commands.add(ConcurrentStableDict({"step": "reset_cluster", "result": "ok" if reset.ok else "failed"}))
        if not reset.ok:
            raise ClusterDrillError(f"cluster-drill.remote 重置三实例失败：{reset.stderr.strip()[:200]}")
        attached = runner.run(inject.ensure_attached_script(config))
        outcome.commands.add(
            ConcurrentStableDict({"step": "network_attach", "result": "ok" if attached.ok else "failed"})
        )
        if not await _wait_ready(client, config.ready_timeout_sec):
            raise ClusterDrillError("cluster-drill.guard 三实例未在预算内就绪")
        if topic_id is None:
            topic_id = await drill.ensure_topic(int(partition_count))
            outcome.commands.add(
                ConcurrentStableDict(
                    {"step": "create_topic", "result": "ok", "topic": run_topic, "topic_id": str(topic_id)}
                )
            )
        state = await _topic_state(dsn, topic_id)
        expected_isr = _EXPECTED_ISR_PER_PARTITION * len(state.partitions)
        if not await _ensure_isr(client, drill, expected_isr, topic_id=topic_id):
            raise ClusterDrillError(f"cluster-drill.guard ISR 未在预算内收敛到 {expected_isr}")
        records = await drill.publish_batch(int(publish_count), warm=False)
        before = await _samples(client, "before", topic_id=topic_id)
        before_state = await _topic_state(dsn, topic_id)
        isr_before = _isr_total(before)
        target = _container_for(config, before_state.leader_of(0))
        target_index = next(
            (index for index, name in enumerate(config.container_names) if name == target),
            0,
        )
        steps = inject.steps_for(scenario.name, config, leader=target)
        switch = runner.docker_exec(
            config.container_names[0],
            f"PATH=/opt/msg/backend/.venv/bin:$PATH BMS_APP__SERVICE=msg "
            f"python -m ops.failover switch --topic-id {int(topic_id)} --all-partitions "
            f"--operator system:drill --reason {scenario.name} --json",
        )
        outcome.commands.add(ConcurrentStableDict({"step": "failover", "result": "ok" if switch.ok else "failed"}))
        if switch.ok:
            switch_items = chain.parse_switch_result(chain.last_json_object(switch.stdout or ""))
        inject.apply_steps(runner, steps, phase="inject")
        await asyncio.sleep(_INJECTION_SETTLE_SEC)
        during = await _samples(client, "during", topic_id=topic_id)
        probe_failed_during = await _probe_publish_rejected(drill, bases[target_index])
        old_leader = before_state.leader_of(0)
        switched_during = await _leader_changed(drill, bases, old_leader, before_state.partitions[0].partition_no)
        inject.apply_steps(runner, steps, phase="recover")
        recovery_ok = await _wait_ready(client, config.ready_timeout_sec)
        if recovery_ok:
            await drill.warmup()
            recovery_ok = await _wait_isr(client, isr_before, topic_id=topic_id)
        if not recovery_ok:
            outcome.issues.add(
                ConcurrentStableDict(
                    {"step": "recover", "reason": "恢复后未在预算内全部就绪（含 ISR 回升）", "owner": "02-8"}
                )
            )
        after = await _samples(client, "after", topic_id=topic_id)
        after_state = await _topic_state(dsn, topic_id)
        demoted = False
        if old_leader:
            suffix = old_leader.rsplit("-", 1)[-1]
            index = int(suffix) - 1 if suffix.isdigit() else 0
            if 0 <= index < len(bases):
                reported = await drill.probe_leader(bases[index])
                demoted = bool(reported) and reported != old_leader
        markers = await drill.publish_batch(chain.MARKER_ROUNDS)
    finally:
        await client.aclose()
        await drill.aclose()
        tunnel.close()
    outcome.rto_ms = int((time.monotonic() - started) * 1000)
    samples = ConcurrentStableList([*before, *during, *after])
    facts = chain.derive_facts(
        records=records,
        markers=markers,
        before=before_state,
        after=after_state,
        switch_items=switch_items,
        isr_before=_isr_total(before),
        isr_during=_isr_total(during),
        isr_after=_isr_total(after),
        probe_failed_during=probe_failed_during,
        recovery_ok=recovery_ok,
        old_leader_demoted=demoted,
        switched_during=switched_during,
    )
    outcome.criteria.extend(scenarios.build_criteria(scenario, facts))
    report = _report(config, scenario, outcome, samples, rerun_reason=rerun_reason)
    json_path, markdown_path = write_cluster_report(report, config.report_dir, title=scenario.title)
    print(f"[{report.conclusion}] {scenario.title}（退出码 {report.exit_code}，恢复 {report.rto_ms} ms）")
    for reason in report.reasons:
        print(f"  - {reason}")
    print(f"  记录：{json_path} ｜ {markdown_path}")
    return report.exit_code


def _report(
    config: ClusterDrillConfig,
    scenario: DrillScenario,
    outcome: _Outcome,
    samples: ConcurrentStableList[PhaseSample],
    *,
    rerun_reason: str,
) -> DrillReport:
    """组装演练报告（`target=docker`）。

    Args:
        config: 演练配置。
        scenario: 场景定义。
        outcome: 中间结果。
        samples: 三段样本。
        rerun_reason: 复跑原因。

    Returns:
        DrillReport: 报告。
    """
    return DrillReport(
        scenario=scenario.name,
        mode="run",
        env=config.env,
        target=TARGET_DOCKER,
        started_at=outcome.started_at,
        finished_at=datetime.now(UTC),
        injection=ConcurrentStableFrozenDict(
            {"method": scenario.inject, "detail": scenario.expect, "artifact": config.project}
        ),
        samples=ConcurrentStableFrozenList(samples),
        criteria=ConcurrentStableFrozenList(outcome.criteria),
        rto_ms=outcome.rto_ms,
        data_loss=outcome.data_loss,
        deviations=ConcurrentStableFrozenList(
            (
                "演练使用独立项目 / 容器名 / 派生库前缀，不等同生产链路",
                "未启用网关别名，客户端经本机隧道直连实例",
                "未做真盘故障注入（归 07-8）",
            )
        ),
        issues=ConcurrentStableFrozenList(ConcurrentStableFrozenDict(item) for item in outcome.issues),
        leftovers=ConcurrentStableFrozenList(),
        commands=ConcurrentStableFrozenList(ConcurrentStableFrozenDict(item) for item in outcome.commands),
        rerun=ConcurrentStableFrozenDict({"reason": rerun_reason} if rerun_reason else {}),
        config_digest=cluster_config_digest(config, scenario.name),
        backup=backup_evidence(config),
    )


def _commit(root: Path) -> str:
    """取本地提交短号（失败回 `unknown`，不因 git 缺失阻断）。

    Args:
        root: 仓库根。

    Returns:
        str: 提交短号。
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except OSError, subprocess.TimeoutExpired:
        return "unknown"
    return (result.stdout or "").strip() or "unknown"


def run_cleanup(config: ClusterDrillConfig, *, keep_data: bool = False) -> int:
    """清理演练环境（幂等）：容器 / 卷 → 网络 → 演练库与 Redis → 宿主目录 → 镜像。

    Args:
        config: 演练配置。
        keep_data: 保留演练库与 Redis 数据（只清容器 / 卷 / 目录 / 镜像）。

    Returns:
        int: 退出码（0 成功；1 远程失败；2 数据清理前置不满足）。
    """
    if not keep_data:
        try:
            cluster_cleanup.assert_safe_data_prefix(config)
        except ClusterDrillError as exc:
            print(f"[FAIL] {exc}")
            return _EXIT_PARAM
    runner = RemoteRunner(config)
    failures: ConcurrentStableList[str] = ConcurrentStableList()
    down = runner.run(cluster_cleanup.cleanup_down_script(config))
    if not down.ok:
        failures.add(f"停容器 / 删卷：{down.stderr.strip()[:200]}")
    runner.run(inject.remove_network_script(config))
    if not keep_data:
        purge = runner.run(cluster_cleanup.purge_data_script(config, network=inject.network_name(config)))
        if purge.code == cluster_cleanup.PURGE_SKIPPED_EXIT:
            print(f"[SKIP] 数据清理跳过：{purge.stderr.strip()[:200]}")
        elif not purge.ok:
            failures.add(f"数据库 / Redis：{purge.stderr.strip()[:200]}")
    paths = runner.run(cluster_cleanup.remove_paths_script(config))
    if not paths.ok:
        failures.add(f"宿主目录：{paths.stderr.strip()[:200]}")
    images = runner.run(cluster_cleanup.remove_images_script())
    if not images.ok:
        failures.add(f"演练镜像：{images.stderr.strip()[:200]}")
    if failures:
        print(f"[FAIL] cluster-drill.cleanup 未完全清理（{len(failures)} 项）：")
        for reason in failures:
            print(f"  - {reason}")
        return 1
    print(f"[OK] 已清理演练项目 {config.project}")
    return 0


def run_verify(scenario_set: ScenarioSet, records: ConcurrentStableList[Path]) -> int:
    """对账多实例记录并写矩阵。

    Args:
        scenario_set: 多实例场景集。
        records: 记录目录。

    Returns:
        int: 退出码。
    """
    from ops.drill import evidence

    report = evidence.verify(scenario_set, ConcurrentStableFrozenList(records), ConcurrentStableFrozenList())
    print(f"[{report.conclusion}] 多实例场景 × 判据 × 证据（退出码 {report.exit_code}）")
    for reason in report.reasons:
        print(f"  - {reason}")
    return report.exit_code


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    args = _parser().parse_args(sys.argv[1:] if argv is None else list(argv))
    try:
        config = load_config()
        reject_production(config.env)
        if args.command == "plan":
            return run_plan(config)
        if args.command == "cleanup":
            return run_cleanup(config, keep_data=bool(args.keep_data))
        if args.command == "verify":
            scenario_set = _scenario_set(config)
            return run_verify(
                scenario_set,
                ConcurrentStableList(Path(item).expanduser().resolve() for item in args.records),
            )
        scenario_set = _scenario_set(config)
        target = next((item for item in scenario_set.scenarios if item.name == args.scenario), None)
        if target is None:
            raise ClusterDrillError(f"cluster-drill.scenario 未注册的多实例场景 {args.scenario!r}")
        return asyncio.run(
            run_scenario(
                config,
                target,
                confirm=args.confirm,
                dry_run=bool(args.dry_run),
                skip_provision=bool(args.skip_provision),
                topic_id=args.topic_id,
                rerun_reason=str(args.rerun_reason),
                namespace=str(args.namespace),
                topic_name=str(args.topic_name),
                tenant=str(args.tenant),
                partition_count=int(args.partition_count),
                publish_count=int(args.publish_count),
            )
        )
    except ClusterDrillError as exc:
        print(f"[FAIL] {exc}")
        return _EXIT_PARAM


if __name__ == "__main__":  # pragma: no cover - 命令行入口
    raise SystemExit(main())
