"""回滚演练与记录（任务 07-4-2）：配置层回滚闭环 + 三类故障注入 + 四类一致性校验 + 记录。

用法::

    python -m ops.rollback check --env test          # 清单 + 矩阵 + 场景静态闸门
    python -m ops.rollback matrix                    # 渲染场景表（设计文档由此粘贴）
    python -m ops.rollback drill --msg-url http://msg:8000 --original-readonly-url http://bms:8000 --env test

**为什么单独一个命令**：07-4-1 回答「能不能切、先切哪个域」；本命令回答「切坏了怎么退、
退回后是否真的恢复」。判据只有一份（`run_rollback_drill`），用例与命令行共用，避免
「用例一套判据、脚本另一套」。

设计取舍：

1. **回滚只改配置**：`switch_to_msg()` / `switch_to_original()` 只改 `provider`，不发布业务
   代码、不重建镜像、不做数据库降级；`config_version` 与端口签名前后必须一致。
2. **失败必须可观测**：注入不可达时发布必须抛 `EventPublishError`，被吞即判失败；未确认
   事件留在 outbox，由既有投递器在回滚后补发一次。
3. **判据可追溯**：场景的 `criterion` 必须逐字出现在切换矩阵的回滚判据里，改判据先改矩阵。
4. **无法确认的残留不默认通过**：原链路事件侧为记录型替身（broker 未部署）时标 `degraded`
   并在遗留里写明归口，结论降级而不是通过。
5. **记录先于结论**：失败也写 JSON + Markdown，失败步与原因进记录。
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import platform as platform_module
import sys
import time
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import httpx
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.config import Settings
from bms_core.core.resources import ResourceManager
from bms_core.db.engine import PLATFORM_DB_KEY
from bms_core.db.registry import EngineRegistry
from bms_core.db.session import DbSession
from bms_core.events.base import EventEnvelope, EventPublisher, EventPublishError
from bms_core.events.rocketmq_publisher import RocketMqEventPublisher
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher
from bms_core.msgclient.transport import RpcTransport
from bms_core.outbox.dispatcher import PollOutboxDispatcher
from bms_core.outbox.store import SqlOutboxStore
from fastapi import FastAPI

from bms_msg import CONTRACT_VERSION, SERVICE_NAME, __version__
from bms_msg.core.config import (
    MIGRATION_DEFAULT_CONSUMER_ID,
    MIGRATION_DEFAULT_ENV,
    MIGRATION_DEFAULT_EVENT_TYPE,
    MIGRATION_DEFAULT_GROUP,
    MIGRATION_DEFAULT_READ_ONLY_METHOD,
    MIGRATION_DEFAULT_READ_ONLY_SERVICE,
    MIGRATION_DEFAULT_TENANT,
    MIGRATION_MATRIX_PATH,
    MIGRATION_PRECONDITIONS_PATH,
    ROLLBACK_RECORD_SCHEMA_VERSION,
    ROLLBACK_RECOVERY_BUDGET_MS,
    ROLLBACK_REPORT_DIR,
    ROLLBACK_SCENARIOS_PATH,
)
from ops.migration import (
    CONCLUSION_DEGRADED,
    CONCLUSION_FAIL,
    CONCLUSION_PASS,
    EXIT_CODES,
    RESULT_DEGRADED,
    RESULT_FAILED,
    RESULT_OK,
    RESULT_SKIPPED,
    DrillMessage,
    PreconditionSet,
    SwitchMatrix,
    SwitchReport,
    WriteAttemptResult,
    business_zero_change,
    digest_bytes,
    digest_json,
    evaluate_gate,
    event_chain_errors,
    load_matrix,
    load_preconditions,
    normalize_env,
    resolve_switch,
    validate_matrix,
    validate_preconditions,
)

__all__ = [
    "CONCLUSION_DEGRADED",
    "CONCLUSION_FAIL",
    "CONCLUSION_PASS",
    "DATA_INTEGRITY_COMPLETE",
    "DATA_INTEGRITY_GAP",
    "DATA_INTEGRITY_UNAVAILABLE",
    "EXIT_CODES",
    "INJECTION_ERROR_RATE",
    "INJECTION_LATENCY",
    "INJECTION_MODES",
    "INJECTION_UNAVAILABLE",
    "LATENCY_SOURCE_MEASURED",
    "LATENCY_SOURCE_SIMULATED",
    "RESULT_DEGRADED",
    "RESULT_FAILED",
    "RESULT_OK",
    "RESULT_SKIPPED",
    "BackupEvidence",
    "CycleRecord",
    "HttpRollbackTargets",
    "InjectionScenario",
    "IntegrityReport",
    "OutboxRound",
    "OutboxState",
    "ProbeResult",
    "RollbackError",
    "RollbackReport",
    "RollbackTargets",
    "ScenarioSet",
    "StateSnapshot",
    "build_parser",
    "build_targets",
    "criterion_matched",
    "error_rate_percent",
    "integrity_report",
    "load_scenarios",
    "main",
    "record_dict",
    "render_markdown",
    "render_scenario_rows",
    "report_signature",
    "run_check",
    "run_rollback_drill",
    "stable_config_version",
    "switch_errors",
    "validate_scenarios",
    "write_record",
]

_REPO_ROOT = Path(__file__).resolve().parents[2]
"""仓根（`backend/ops/rollback.py` 上溯两级）。"""

INJECTION_UNAVAILABLE = "unavailable"
"""注入：msg 不可达。"""

INJECTION_ERROR_RATE = "error_rate"
"""注入：发布错误率超阈值。"""

INJECTION_LATENCY = "latency"
"""注入：只读调用延迟超阈值。"""

INJECTION_MODES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    (INJECTION_UNAVAILABLE, INJECTION_ERROR_RATE, INJECTION_LATENCY)
)
"""注入模式闭集（三类必须齐备）。"""

_INJECTION_TARGETS: ConcurrentStableFrozenDict[str, str] = ConcurrentStableFrozenDict(
    {
        INJECTION_UNAVAILABLE: "both",
        INJECTION_ERROR_RATE: "event",
        INJECTION_LATENCY: "readonly",
    }
)
"""模式 → 观测目标（一一对应，写错即场景非法）。"""

_TARGET_DOMAINS: ConcurrentStableFrozenDict[str, ConcurrentStableFrozenList[str]] = ConcurrentStableFrozenDict(
    {
        "both": ConcurrentStableFrozenList(("low_risk_event", "readonly_rpc")),
        "event": ConcurrentStableFrozenList(("low_risk_event",)),
        "readonly": ConcurrentStableFrozenList(("readonly_rpc",)),
    }
)
"""观测目标 → 矩阵域（判据逐字追溯的范围）。"""

DATA_INTEGRITY_COMPLETE = "完整"
"""数据完整性：四类一致性全过。"""

DATA_INTEGRITY_GAP = "有缺口"
"""数据完整性：有降级项但无硬失败。"""

DATA_INTEGRITY_UNAVAILABLE = "不可用"
"""数据完整性：任一硬失败。"""

LATENCY_SOURCE_SIMULATED = "simulated"
"""延迟来源：进程内按观测值注入（不真实等待）。"""

LATENCY_SOURCE_MEASURED = "measured"
"""延迟来源：网络实测。"""

_MSG_PROVIDER = "msg"
"""切到消息平台的实现名。"""

_ORIGINAL_PROVIDER = "original"
"""原实现模式名。"""

_ORIGINAL_EVENT_PROVIDER = "rocketmq"
"""事件端口原实现名。"""

_ORIGINAL_CLIENT_PROVIDER = "http"
"""服务调用端口原实现名。"""

_RECORDING_TRANSPORT = "recording"
"""原链路事件侧替身载体名（broker 未部署）。"""

_CHECK_OK = 0
"""`check` 通过退出码。"""

_CHECK_FAIL = 1
"""`check` 失败退出码。"""

_CHECK_USAGE = 2
"""`check` 参数错误退出码。"""

_ENV_PROD = "prod"
"""归一化后的生产环境标识。"""

_MODE_CHECK = "check"
"""记录模式：静态检查。"""

_MODE_DRILL = "drill"
"""记录模式：演练。"""


class RollbackError(RuntimeError):
    """回滚演练失败：消息带 `rollback.<类别>` 前缀，便于定位。"""


def _now() -> datetime:
    """当前 UTC 时间（记录用）。

    Returns:
        datetime: 带时区的当前时刻。
    """
    return datetime.now(UTC)


def _monotonic() -> float:
    """单调时钟（步骤耗时；不用于记录时刻）。

    Returns:
        float: 单调秒数。
    """
    return time.monotonic()


# ---------------------------------------------------------------------------
# 场景单源
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InjectionScenario:
    """一条回滚演练场景。"""

    name: str
    """场景标识（小写下划线）。"""

    mode: str
    """`unavailable` / `error_rate` / `latency`。"""

    target: str
    """`both` / `event` / `readonly`（与 mode 一一对应）。"""

    criterion: str
    """矩阵回滚判据原文（逐字匹配）。"""

    threshold: float
    """错误率百分比或延迟毫秒；不可达写 0。"""

    samples: int
    """采样次数。"""

    expected: str
    """期望观察（记录用）。"""

    note: str
    """备注（阈值占位说明等）。"""


@dataclass(frozen=True)
class ScenarioSet:
    """回滚场景集（一份文件 = 一套可复跑场景）。"""

    schema_version: int
    """场景格式版本。"""

    updated_at: str
    """最近核对时间。"""

    owner: str
    """责任人。"""

    recovery_budget_ms: int
    """单场景恢复时间预算（毫秒）。"""

    scenarios: ConcurrentStableFrozenList[InjectionScenario]
    """场景列表（声明序即执行序）。"""


def _table(value: object) -> dict[str, object]:
    """把 TOML 取值折成字符串键的表（非表返回空表，交给校验器报错）。

    Args:
        value: TOML 取值。

    Returns:
        dict[str, object]: 表（非表时为空）。
    """
    if not isinstance(value, dict):
        return {}
    return cast("dict[str, object]", value)


def _empty_scenarios() -> ScenarioSet:
    """构造空场景集（解析失败时的占位；不冒充「有三类场景」）。

    Returns:
        ScenarioSet: 空场景集。
    """
    return ScenarioSet(
        schema_version=0,
        updated_at="",
        owner="",
        recovery_budget_ms=ROLLBACK_RECOVERY_BUDGET_MS,
        scenarios=ConcurrentStableFrozenList(),
    )


def load_scenarios(path: Path) -> tuple[ScenarioSet, ConcurrentStableFrozenList[str]]:
    """读取并解析回滚场景（解析错误也返回，便于一次列全）。

    Args:
        path: 场景文件。

    Returns:
        tuple: 场景集（解析失败时为空集）+ 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    try:
        raw = cast("dict[str, object]", tomllib.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        errors.add(f"回滚场景不存在：{path}")
        return _empty_scenarios(), ConcurrentStableFrozenList(errors)
    except tomllib.TOMLDecodeError as exc:
        errors.add(f"回滚场景解析失败：{path}（{exc}）")
        return _empty_scenarios(), ConcurrentStableFrozenList(errors)

    scenarios: ConcurrentStableList[InjectionScenario] = ConcurrentStableList()
    raw_scenarios = raw.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        errors.add("回滚场景缺少 [[scenarios]]（至少登记一条）")
    else:
        for index, raw_scenario in enumerate(cast("list[object]", raw_scenarios)):
            item = _table(raw_scenario)
            if not item:
                errors.add(f"scenarios[{index}] 不是表")
                continue
            scenarios.add(
                InjectionScenario(
                    name=str(item.get("name") or ""),
                    mode=str(item.get("mode") or ""),
                    target=str(item.get("target") or ""),
                    criterion=str(item.get("criterion") or ""),
                    threshold=float(cast("float", item.get("threshold") or 0.0)),
                    samples=int(cast("int", item.get("samples") or 0)),
                    expected=str(item.get("expected") or ""),
                    note=str(item.get("note") or ""),
                )
            )
    meta = _table(raw.get("meta"))
    scenario_set = ScenarioSet(
        schema_version=int(cast("int", raw.get("schema_version") or 0)),
        updated_at=str(meta.get("updated_at") or ""),
        owner=str(meta.get("owner") or ""),
        recovery_budget_ms=int(cast("int", meta.get("recovery_budget_ms") or ROLLBACK_RECOVERY_BUDGET_MS)),
        scenarios=ConcurrentStableFrozenList(scenarios),
    )
    return scenario_set, ConcurrentStableFrozenList(errors)


def validate_scenarios(scenarios: ScenarioSet, matrix: SwitchMatrix) -> ConcurrentStableFrozenList[str]:
    """校验场景字段、模式闭集、判据可追溯性与恢复预算。

    Args:
        scenarios: 已解析场景集。
        matrix: 已解析切换矩阵（判据原文来源）。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    if scenarios.schema_version != 1:
        errors.add(f"场景 schema_version 非法：{scenarios.schema_version!r}（期望 1）")
    if scenarios.recovery_budget_ms <= 0:
        errors.add(f"恢复预算非法：{scenarios.recovery_budget_ms}（须为正整数毫秒）")
    if not scenarios.scenarios:
        errors.add("场景为空：至少登记三类注入")
    criteria: ConcurrentStableDict[str, ConcurrentStableFrozenList[str]] = ConcurrentStableDict()
    for domain in matrix.domains:
        criteria.set(domain.key, domain.rollback_criteria)
    seen: ConcurrentStableSet[str] = ConcurrentStableSet()
    modes: ConcurrentStableSet[str] = ConcurrentStableSet()
    for scenario in scenarios.scenarios:
        where = scenario.name or "(缺少 name)"
        if not scenario.name:
            errors.add("存在缺少 name 的场景")
        elif scenario.name in seen:
            errors.add(f"场景 name 重复：{scenario.name}")
        else:
            seen.add(scenario.name)
        if scenario.mode not in INJECTION_MODES:
            errors.add(f"{where}：mode = {scenario.mode!r} 非法（可选 {' / '.join(INJECTION_MODES)}）")
        else:
            modes.add(scenario.mode)
            expected_target = _INJECTION_TARGETS.get(scenario.mode)
            if scenario.target != expected_target:
                errors.add(f"{where}：target = {scenario.target!r} 与 mode = {scenario.mode!r} 不匹配")
        if not scenario.criterion.strip():
            errors.add(f"{where}：criterion 为空（须从矩阵回滚判据原文复制）")
        else:
            domains = _TARGET_DOMAINS.get(scenario.target, ConcurrentStableFrozenList())
            matched = False
            for domain_key in domains:
                if scenario.criterion in criteria.get(domain_key, ConcurrentStableFrozenList()):
                    matched = True
                    break
            if not matched:
                errors.add(f"{where}：criterion 未出现在矩阵回滚判据中：{scenario.criterion!r}")
        if scenario.threshold < 0:
            errors.add(f"{where}：threshold 不能为负（当前 {scenario.threshold}）")
        if scenario.samples < 1:
            errors.add(f"{where}：samples 须 >= 1（当前 {scenario.samples}）")
        if not scenario.expected.strip():
            errors.add(f"{where}：expected 为空（须写期望观察）")
    for mode in INJECTION_MODES:
        if mode not in modes:
            errors.add(f"缺少注入模式 {mode!r}（三类注入必须齐备）")
    return ConcurrentStableFrozenList(errors)


def render_scenario_rows(scenarios: ScenarioSet) -> ConcurrentStableFrozenList[str]:
    """把场景集渲染成 Markdown 表行（设计文档的表由此粘贴，用例断言两处一致）。

    Args:
        scenarios: 已校验场景集。

    Returns:
        ConcurrentStableFrozenList[str]: 表头 + 分隔行 + 每个场景一行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("| 场景 | 模式 | 目标 | 判据（矩阵原文） | 阈值 | 采样 | 期望观察 |")
    rows.add("| --- | --- | --- | --- | --- | --- | --- |")
    for scenario in scenarios.scenarios:
        if scenario.mode == INJECTION_UNAVAILABLE:
            threshold = "0（不适用）"
        elif scenario.mode == INJECTION_ERROR_RATE:
            threshold = f"{scenario.threshold:g}%"
        else:
            threshold = f"{scenario.threshold:g}ms"
        rows.add(
            f"| `{scenario.name}` | `{scenario.mode}` | `{scenario.target}` | {scenario.criterion} "
            f"| {threshold} | {scenario.samples} | {scenario.expected} |"
        )
    return ConcurrentStableFrozenList(rows)


# ---------------------------------------------------------------------------
# 观测值与判据
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeResult:
    """一次通道探测（只读调用）。"""

    target: str
    """调用目标。"""

    digest: str
    """结果摘要。"""

    count: int
    """结果条数（单值响应记 1）。"""

    latency_ms: int
    """本次调用耗时（毫秒；进程内可注入观测值）。"""


@dataclass(frozen=True)
class OutboxRound:
    """一轮发件箱投递的计数。"""

    published: int
    """成功投递条数。"""

    failed: int
    """失败保行条数。"""

    dead: int
    """转死信条数。"""

    backlog: int
    """投递后积压。"""


@dataclass(frozen=True)
class OutboxState:
    """发件箱状态快照。"""

    pending: int
    """待投递行数。"""

    delivered: int
    """已投递行数。"""

    dead: int
    """死信行数。"""


@dataclass(frozen=True)
class StateSnapshot:
    """一致性快照（位点 / 死信 / 重放 / 审计 / outbox / 效果账本）。"""

    available: bool
    """快照是否可用（网络形态只验通道时为 False）。"""

    offsets: ConcurrentStableFrozenDict[str, int]
    """已提交位点（键：`group|topic|partition`）。"""

    dead_letters: int
    """死信条数。"""

    replay_tasks: ConcurrentStableFrozenDict[str, str]
    """重放任务（任务 ID → 状态）。"""

    audit_count: int
    """审计事件条数。"""

    audit_digest: str
    """审计链摘要（可复算）。"""

    audit_chain_ok: bool
    """审计链是否完整。"""

    outbox: OutboxState
    """发件箱状态。"""

    effects: ConcurrentStableFrozenDict[str, int]
    """业务效果账本（事件 ID → 执行次数；幂等消费替身按此判「效果一次」）。"""


@dataclass(frozen=True)
class BackupEvidence:
    """演练前备份证据。"""

    mode: str
    """`backup`（已备份）/ `not_applicable`（不适用）。"""

    backup_id: str
    """备份标识（不适用时为空）。"""

    note: str
    """说明（为什么不需要备份 / 备份到哪）。"""


@dataclass(frozen=True)
class CycleRecord:
    """一个场景的切换 / 回滚 / 恢复时间线。"""

    scenario: str
    """场景名。"""

    switch_ms: int
    """切到 msg 的配置耗时。"""

    rollback_started_at: str
    """回滚开始（UTC ISO8601）。"""

    rollback_finished_at: str
    """回滚结束（配置改完，UTC ISO8601）。"""

    config_change_ms: int
    """回滚改配置耗时。"""

    recovery_ms: int
    """回滚开始到双链路恢复的耗时。"""


@dataclass(frozen=True)
class IntegrityReport:
    """四类一致性结论。"""

    effect_once: bool
    """效果一次（无重复副作用）。"""

    no_loss: bool
    """无丢失（发布 / 消费 / outbox 三方对账）。"""

    state: bool
    """位点 / 死信 / 重放未被回滚带乱。"""

    audit_chain: bool
    """审计链完整且条数不减。"""

    detail: ConcurrentStableFrozenList[str]
    """明细（失败原因）。"""

    @property
    def ok(self) -> bool:
        """四类是否全过。

        Returns:
            bool: 全过为 True。
        """
        return self.effect_once and self.no_loss and self.state and self.audit_chain


@dataclass(frozen=True)
class StepResult:
    """一个演练步骤的结果。"""

    step: str
    """步骤名。"""

    result: str
    """`ok` / `degraded` / `failed` / `skipped`。"""

    elapsed_ms: int
    """耗时（毫秒）。"""

    detail: str
    """明细（失败原因 / 计数摘要）。"""


@dataclass(frozen=True)
class RollbackReport:
    """回滚演练报告（渲染 JSON / Markdown 的唯一输入）。"""

    mode: str
    """`check` / `drill`。"""

    env: str
    """归一化环境。"""

    started_at: datetime
    """开始时刻（UTC）。"""

    finished_at: datetime
    """结束时刻（UTC）。"""

    config_version: str
    """回滚前后一致的配置版本。"""

    switch_domain: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """切换域（批次 1 的两类端口）。"""

    baseline: ConcurrentStableFrozenDict[str, object]
    """原链路基线与备份证据。"""

    switch: ConcurrentStableFrozenDict[str, object]
    """切到 msg 后的两端口判定。"""

    injection: ConcurrentStableFrozenDict[str, object]
    """注入场景与观测结果。"""

    observe_metrics: ConcurrentStableFrozenDict[str, float]
    """矩阵指标名 → 本次本地观测值。"""

    rollback: ConcurrentStableFrozenDict[str, object]
    """回滚时间线（周期 / 恢复时间 / 预算）。"""

    gate: ConcurrentStableFrozenDict[str, object]
    """生产闸门结果。"""

    steps: ConcurrentStableFrozenList[StepResult]
    """步骤结果。"""

    counts: ConcurrentStableFrozenDict[str, int]
    """计数（发布 / 消费 / 投递 / 积压 / 重复 / 执行 / 周期）。"""

    integrity: IntegrityReport
    """四类一致性结论。"""

    backup: BackupEvidence
    """演练前备份证据。"""

    conclusion: str
    """`通过` / `降级通过` / `失败`。"""

    data_integrity: str
    """`完整` / `有缺口` / `不可用`。"""

    reasons: ConcurrentStableFrozenList[str]
    """结论原因（降级 / 失败项）。"""

    issues: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """问题与处置。"""

    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """遗留与归口。"""

    artifacts: ConcurrentStableFrozenDict[str, str]
    """记录产物路径。"""


def stable_config_version(settings: Settings) -> str:
    """计算部署配置版本（排除 `provider` 开关值，回滚前后必须一致）。

    回滚只改 `provider`，因此「配置版本」不能把 provider 算进去；命名空间、实例地址、
    基址模板与策略仍纳入，用于发现「回滚顺手改了别的东西」。

    Args:
        settings: bms 配置。

    Returns:
        str: sha256 前 16 位。
    """
    instances = settings.service_client.instances.get(_MSG_PROVIDER)
    payload = {
        "app.service": settings.app.service,
        "app.env": settings.app.env,
        "event.namespace": str(settings.event.options.get("namespace") or settings.app.service or ""),
        "service_client.base_url_template": str(settings.service_client.options.get("base_url_template") or ""),
        "service_client.endpoints.msg": str(settings.service_client.endpoints.get(_MSG_PROVIDER) or ""),
        "service_client.instances.msg": list(instances) if instances is not None else [],
        "service_client.instance_strategy": str(settings.service_client.options.get("instance_strategy") or ""),
    }
    return digest_json(payload)


def error_rate_percent(failures: int, attempts: int) -> float:
    """计算错误率百分比（零次尝试记 0，不除零）。

    Args:
        failures: 失败次数。
        attempts: 尝试次数。

    Returns:
        float: 百分比（0~100）。
    """
    if attempts <= 0:
        return 0.0
    return round(failures * 100.0 / attempts, 4)


def criterion_matched(scenario: InjectionScenario, *, failures: int, attempts: int, latency_ms: int) -> bool:
    """按场景模式判定注入是否命中判据。

    Args:
        scenario: 注入场景。
        failures: 失败次数（错误率场景）。
        attempts: 尝试次数（错误率场景）。
        latency_ms: 只读调用延迟（延迟场景）。

    Returns:
        bool: 命中为 True。
    """
    if scenario.mode == INJECTION_ERROR_RATE:
        return error_rate_percent(failures, attempts) >= scenario.threshold
    if scenario.mode == INJECTION_LATENCY:
        return float(latency_ms) >= scenario.threshold
    return failures >= 1


def switch_errors(report: SwitchReport, expected: str) -> ConcurrentStableFrozenList[str]:
    """校验两个端口解析到期望模式（`msg` / `original`）。

    Args:
        report: 切换判定。
        expected: 期望模式。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    for decision in (report.event, report.service_client):
        if decision.error:
            errors.add(f"{decision.plugin_key}：{decision.error_kind or '解析'}失败 —— {decision.error}")
        elif decision.mode != expected:
            errors.add(
                f"{decision.plugin_key}：当前解析到 {decision.implementation!r}（{decision.mode}），期望 {expected}"
            )
    return ConcurrentStableFrozenList(errors)


def _offset_errors(before: StateSnapshot, after: StateSnapshot) -> ConcurrentStableFrozenList[str]:
    """位点只允许前进、键集合只允许变大。

    Args:
        before: 前一次快照。
        after: 后一次快照。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    for key, value in before.offsets.items():
        current = after.offsets.get(key)
        if current is None:
            errors.add(f"位点键丢失：{key}")
        elif current < value:
            errors.add(f"位点回退：{key} {value} → {current}")
    return ConcurrentStableFrozenList(errors)


def integrity_report(
    snapshots: ConcurrentStableFrozenList[StateSnapshot],
    *,
    counts: ConcurrentStableFrozenDict[str, int],
) -> IntegrityReport:
    """四类一致性判定（效果一次 / 无丢失 / 位点死信重放 / 审计链）。

    Args:
        snapshots: 快照序列（首个为切换前基线，末个为演练结束）。
        counts: 步骤计数（`outbox_enqueued` / `delivered_original` / `outbox_pending` /
            `published_msg` / `consumed_msg`）。

    Returns:
        IntegrityReport: 四类结论与明细。
    """
    detail: ConcurrentStableList[str] = ConcurrentStableList()
    if not snapshots:
        detail.add("没有任何一致性快照")
        return IntegrityReport(
            effect_once=False, no_loss=False, state=False, audit_chain=False, detail=ConcurrentStableFrozenList(detail)
        )
    first = snapshots[0]
    last = snapshots[-1]
    effect_once = True
    for event_id, executions in last.effects.items():
        if executions != 1:
            effect_once = False
            detail.add(f"效果重复：{event_id} 执行 {executions} 次")
    outbox_enqueued = counts.get("outbox_enqueued", 0)
    delivered = counts.get("delivered_original", 0)
    pending = counts.get("outbox_pending", 0)
    no_loss = outbox_enqueued == delivered + pending
    if not no_loss:
        detail.add(f"发件箱对账不平：入队 {outbox_enqueued} ≠ 投递 {delivered} + 积压 {pending}")
    if counts.get("published_msg", 0) != counts.get("consumed_msg", 0):
        no_loss = False
        detail.add(f"消息对账不平：发布 {counts.get('published_msg', 0)} ≠ 消费 {counts.get('consumed_msg', 0)}")
    if not last.available or not first.available:
        state = False
        audit_chain = False
        detail.add("一致性快照不可用（网络形态只验通道）")
    else:
        offset_errors = _offset_errors(first, last)
        state = not offset_errors
        detail.extend(offset_errors)
        if last.dead_letters != first.dead_letters:
            state = False
            detail.add(f"死信条数变化：{first.dead_letters} → {last.dead_letters}")
        if tuple(last.replay_tasks.items()) != tuple(first.replay_tasks.items()):
            state = False
            detail.add("重放任务快照在回滚后不一致")
        audit_chain = last.audit_chain_ok and last.audit_count >= first.audit_count
        if not audit_chain:
            detail.add("审计链不完整或条数减少")
    return IntegrityReport(
        effect_once=effect_once,
        no_loss=no_loss,
        state=state,
        audit_chain=audit_chain,
        detail=ConcurrentStableFrozenList(detail),
    )


# ---------------------------------------------------------------------------
# 演练端口
# ---------------------------------------------------------------------------


class RollbackTargets(Protocol):
    """回滚演练目标（进程内真应用与网络实现共用同一份判据）。"""

    def original_config(self) -> SwitchReport:
        """回滚前的配置解析（两个端口须为原实现）。"""
        ...

    def switch_to_msg(self) -> SwitchReport:
        """只改配置：两个 `provider` 切到 msg。"""
        ...

    def switch_to_original(self) -> SwitchReport:
        """只改配置：两个 `provider` 回原实现。"""
        ...

    def config_version(self) -> str:
        """配置版本（回滚前后必须相同）。"""
        ...

    def original_event_transport(self) -> str:
        """原链路事件侧载体（`recording` / `rocketmq`）。"""
        ...

    def latency_source(self) -> str:
        """延迟观测来源（`simulated` / `measured`）。"""
        ...

    def supports_outbox(self) -> bool:
        """是否提供真发件箱（未提供时 outbox 相关步骤记 `skipped`）。"""
        ...

    async def prepare(self) -> None:
        """建主题并加入消费组（幂等）。"""
        ...

    async def publish(self, event: EventEnvelope) -> None:
        """经当前 provider 发布（失败抛 `EventPublishError`）。"""
        ...

    async def consume(self) -> ConcurrentStableFrozenList[DrillMessage]:
        """经 msg 数据面拉取并确认。"""
        ...

    async def read_only_msg(self) -> ProbeResult:
        """msg 通道只读调用。"""
        ...

    async def read_only_original(self) -> ProbeResult:
        """原链路只读调用。"""
        ...

    async def inject(self, mode: str, *, threshold: float) -> None:
        """注入失败（`unavailable` / `error_rate` / `latency`）。"""
        ...

    async def clear_injection(self) -> None:
        """解除注入。"""
        ...

    async def inflight_attempt(self, key: str) -> WriteAttemptResult | None:
        """在途：同幂等键重试（无法注入时返回 None）。"""
        ...

    async def enqueue_outbox(self, event: EventEnvelope) -> str:
        """写发件箱（真 bms 存储）；未提供时抛 `RollbackError`。"""
        ...

    async def dispatch_outbox(self) -> OutboxRound:
        """投递一轮（当前 provider）；未提供时抛 `RollbackError`。"""
        ...

    async def snapshot(self) -> StateSnapshot:
        """一致性快照。"""
        ...

    async def cleanup(self) -> None:
        """清理隔离环境。"""
        ...

    async def aclose(self) -> None:
        """释放连接。"""
        ...


# ---------------------------------------------------------------------------
# 演练编排
# ---------------------------------------------------------------------------


def _event(
    event_type: str,
    index: int,
    now: datetime,
    *,
    tenant: str | None = MIGRATION_DEFAULT_TENANT,
    nonce: str = "",
) -> EventEnvelope:
    """构造一条演练事件（ID 由运行标识 + 序号组成，同一次演练内稳定且跨次唯一）。

    Args:
        event_type: 事件类型。
        index: 第几条（同一次演练内递增）。
        now: 事件发生时刻。
        tenant: 租户编码（发件箱侧为空，避免演示租户编码与雪花主键混淆）。
        nonce: 运行标识（同一发件箱库重复演练靠它避免 `event_id` 唯一冲突）。

    Returns:
        EventEnvelope: 事件信封。
    """
    prefix = f"rollback-{nonce}-" if nonce else "rollback-"
    return EventEnvelope(
        event_type=event_type,
        payload=ConcurrentStableDict({"rollback_index": index, "run": "07-4-2"}),
        tenant_id=tenant,
        trace_id=f"rollback-{index}",
        event_id=f"{prefix}{index:04d}",
        occurred_at=now,
        aggregate_key=f"rollback-{index % 4}",
    )


async def run_rollback_drill(
    *,
    matrix: SwitchMatrix,
    preconditions: PreconditionSet,
    scenarios: ScenarioSet,
    targets: RollbackTargets,
    env: str,
    backup: BackupEvidence,
    event_type: str = MIGRATION_DEFAULT_EVENT_TYPE,
    run_nonce: str = "",
    now: Callable[[], datetime] = _now,
    clock: Callable[[], float] = _monotonic,
) -> RollbackReport:
    """执行隔离环境回滚演练（判据唯一实现；用例与 CLI 共用）。

    Args:
        matrix: 已解析切换矩阵。
        preconditions: 已解析前置清单。
        scenarios: 已解析场景集。
        targets: 演练目标（真 msg 应用或网络实现）。
        env: 归一化环境（不得为 prod）。
        backup: 演练前备份证据（不适用时写明理由）。
        event_type: 演练事件类型。
        run_nonce: 运行标识（同一发件箱库重复演练时避免事件 ID 冲突；空取开始时刻）。
        now: 时间源（用例注入固定时刻）。
        clock: 单调时钟（用例注入假时钟）。

    Returns:
        RollbackReport: 回滚演练报告。

    Raises:
        ValueError: `env` 为生产环境（生产回滚归 bms 8B_08）。
    """
    if env == _ENV_PROD:
        raise ValueError("生产环境禁止执行回滚演练（生产回滚归 bms 8B_08）")
    started_at = now()
    nonce = run_nonce or started_at.strftime("%Y%m%dT%H%M%S%f")

    def _evt(index: int, *, tenant: str | None = MIGRATION_DEFAULT_TENANT) -> EventEnvelope:
        """按本次运行标识构造事件。"""
        return _event(event_type, index, now(), tenant=tenant, nonce=nonce)

    steps: ConcurrentStableList[StepResult] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    issues: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    snapshots: ConcurrentStableList[StateSnapshot] = ConcurrentStableList()
    cycles: ConcurrentStableList[CycleRecord] = ConcurrentStableList()
    injection_rows: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    counts: ConcurrentStableDict[str, int] = ConcurrentStableDict(
        {
            "published_msg": 0,
            "consumed_msg": 0,
            "delivered_original": 0,
            "outbox_enqueued": 0,
            "outbox_pending": 0,
            "duplicates": 0,
            "executions": 0,
            "cycles": 0,
        }
    )
    latency_ms = 0
    error_failures = 0
    failed_steps: ConcurrentStableList[str] = ConcurrentStableList()

    def _step(name: str, result: str, tick: float, detail: str) -> None:
        """登记一步（统一取整毫秒）。"""
        steps.add(StepResult(step=name, result=result, elapsed_ms=max(int((clock() - tick) * 1000), 0), detail=detail))
        if result == RESULT_FAILED:
            failed_steps.add(name)
            reasons.add(f"{name}：{detail}")

    def _failed(name: str, tick: float, detail: str) -> None:
        """登记失败步。"""
        _step(name, RESULT_FAILED, tick, detail)

    preconditions_errors = validate_preconditions(preconditions)
    gate = evaluate_gate(preconditions, env)
    tick = clock()
    if preconditions_errors:
        _failed("preconditions", tick, "；".join(preconditions_errors))
    else:
        detail = "全部满足" if not gate.blocked_by else f"生产阻断项 {len(gate.blocked_by)} 条（本环境不阻断）"
        _step("preconditions", RESULT_OK, tick, detail)

    tick = clock()
    matrix_errors = validate_matrix(
        matrix, known_preconditions=ConcurrentStableFrozenList(item.id for item in preconditions.items)
    )
    switch_domain: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    for domain in matrix.domains:
        if domain.phase != 1:
            continue
        switch_domain.add(
            ConcurrentStableFrozenDict({"key": domain.key, "port": domain.port, "switch_key": domain.switch_key})
        )
    if matrix_errors:
        _failed("matrix", tick, "；".join(matrix_errors))
    else:
        keys = "、".join(item["key"] for item in switch_domain)
        _step("matrix", RESULT_OK, tick, f"切换域 {len(switch_domain)} 个：{keys}")

    tick = clock()
    scenario_errors = validate_scenarios(scenarios, matrix)
    if scenario_errors:
        _failed("scenarios", tick, "；".join(scenario_errors))
    else:
        modes = "、".join(INJECTION_MODES)
        _step(
            "scenarios",
            RESULT_OK,
            tick,
            f"{len(scenarios.scenarios)} 个场景（{modes}），预算 {scenarios.recovery_budget_ms}ms",
        )

    tick = clock()
    baseline = targets.original_config()
    baseline_errors = switch_errors(baseline, _ORIGINAL_PROVIDER)
    if baseline_errors:
        _failed("baseline", tick, "；".join(baseline_errors))
    else:
        _step(
            "baseline",
            RESULT_OK,
            tick,
            f"event={baseline.event.implementation} service_client={baseline.service_client.implementation} "
            f"fingerprint={baseline.fingerprint}；备份 {backup.mode}"
            + (f"（{backup.backup_id}）" if backup.backup_id else ""),
        )

    baseline_readonly: ProbeResult | None = None
    tick = clock()
    try:
        baseline_readonly = await targets.read_only_original()
    except Exception as exc:
        _failed("baseline_readonly", tick, f"{type(exc).__name__}: {exc}")
    else:
        if baseline_readonly.count <= 0:
            _failed("baseline_readonly", tick, f"{baseline_readonly.target} 返回条数 {baseline_readonly.count}")
        else:
            _step(
                "baseline_readonly",
                RESULT_OK,
                tick,
                f"{baseline_readonly.target} digest={baseline_readonly.digest} count={baseline_readonly.count}",
            )

    tick = clock()
    if not targets.supports_outbox():
        _step("baseline_event", RESULT_SKIPPED, tick, "未提供发件箱，原链路事件侧未验（降级）")
        reasons.add("baseline_event：未提供发件箱，原链路事件侧未验（降级）")
    else:
        try:
            baseline_id = await targets.enqueue_outbox(_evt(1, tenant=None))
            baseline_round = await targets.dispatch_outbox()
            counts["outbox_enqueued"] = counts.get("outbox_enqueued", 0) + 1
            counts["delivered_original"] = counts.get("delivered_original", 0) + baseline_round.published
        except Exception as exc:
            _failed("baseline_event", tick, f"{type(exc).__name__}: {exc}")
        else:
            if baseline_round.published != 1:
                _failed(
                    "baseline_event",
                    tick,
                    f"原链路应恰好投递 1 条，实际 {baseline_round.published}（id={baseline_id}）",
                )
            else:
                _step("baseline_event", RESULT_OK, tick, "原链路投递 1 条")

    tick = clock()
    before_switch_version = targets.config_version()
    switch = targets.switch_to_msg()
    switch_problems = switch_errors(switch, _MSG_PROVIDER)
    switch_ms = max(int((clock() - tick) * 1000), 0)
    if switch_problems:
        _failed("switch_to_msg", tick, "；".join(switch_problems))
    elif targets.config_version() != before_switch_version:
        _failed("switch_to_msg", tick, "切换改动了配置版本（只允许改 provider）")
    else:
        _step(
            "switch_to_msg",
            RESULT_OK,
            tick,
            f"event={switch.event.implementation} service_client={switch.service_client.implementation} "
            f"fingerprint={switch.fingerprint} 配置耗时 {switch_ms}ms",
        )

    tick = clock()
    try:
        await targets.prepare()
    except Exception as exc:
        _failed("prepare", tick, f"{type(exc).__name__}: {exc}")
    else:
        _step("prepare", RESULT_OK, tick, "主题与消费组就绪（幂等）")

    msg_event = _evt(2)
    tick = clock()
    try:
        await targets.publish(msg_event)
        messages = await targets.consume()
        counts["published_msg"] = counts.get("published_msg", 0) + 1
        chain_errors = event_chain_errors(msg_event, messages)
        if chain_errors:
            _failed("event_chain_msg", tick, "；".join(chain_errors))
        else:
            counts["consumed_msg"] = counts.get("consumed_msg", 0) + len(messages)
            _step("event_chain_msg", RESULT_OK, tick, f"发布 1 → 消费 {len(messages)}（载荷 / 属性一致）")
    except Exception as exc:
        _failed("event_chain_msg", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        msg_readonly = await targets.read_only_msg()
    except Exception as exc:
        _failed("readonly_msg", tick, f"{type(exc).__name__}: {exc}")
    else:
        if msg_readonly.count <= 0:
            _failed("readonly_msg", tick, f"{msg_readonly.target} 返回条数 {msg_readonly.count}")
        else:
            _step(
                "readonly_msg",
                RESULT_OK,
                tick,
                f"{msg_readonly.target} digest={msg_readonly.digest} count={msg_readonly.count}",
            )

    tick = clock()
    try:
        baseline_snapshot = await targets.snapshot()
        snapshots.add(baseline_snapshot)
        if baseline_snapshot.available:
            _step("snapshot_msg", RESULT_OK, tick, "一致性基线快照可用")
        else:
            _step("snapshot_msg", RESULT_DEGRADED, tick, "快照不可用（网络形态只验通道）")
            reasons.add("snapshot_msg：网络形态无位点 / 死信 / 重放 / 审计快照，一致性降级")
    except Exception as exc:
        _failed("snapshot_msg", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        await targets.publish(_evt(3))
        await targets.publish(_evt(4))
        dual_messages = await targets.consume()
        counts["published_msg"] = counts.get("published_msg", 0) + 2
        counts["consumed_msg"] = counts.get("consumed_msg", 0) + len(dual_messages)
        if len(dual_messages) != 2:
            _failed("dual_write_msg", tick, f"两次发布应产生 2 条消息，实际 {len(dual_messages)} 条")
        else:
            _step("dual_write_msg", RESULT_OK, tick, "两次发布 = 恰好两条 msg 消息（原链路零条）")
    except Exception as exc:
        _failed("dual_write_msg", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        inflight = await targets.inflight_attempt("rollback-inflight")
    except Exception as exc:
        _failed("inflight_idempotency", tick, f"{type(exc).__name__}: {exc}")
    else:
        if inflight is None:
            _step("inflight_idempotency", RESULT_SKIPPED, tick, "目标无法注入传输失败（网络演练不验在途）")
            reasons.add("inflight_idempotency：网络演练无法注入传输失败，在途判据由进程内演练覆盖")
        elif inflight.executions != 1:
            _failed("inflight_idempotency", tick, f"服务端执行 {inflight.executions} 次（期望 1 次）")
        elif len(set(inflight.keys)) != 1:
            _failed("inflight_idempotency", tick, f"两跳幂等键不一致：{list(inflight.keys)}")
        else:
            counts["executions"] = inflight.executions
            _step(
                "inflight_idempotency",
                RESULT_OK,
                tick,
                f"attempts={len(inflight.keys)} executions={inflight.executions}",
            )

    scenario_index = 10
    for order, scenario in enumerate(scenarios.scenarios):
        if failed_steps:
            steps.add(
                StepResult(step=f"inject.{scenario.name}", result=RESULT_SKIPPED, elapsed_ms=0, detail="前序失败")
            )
            continue
        await targets.clear_injection()
        tick = clock()
        failures = 0
        attempts = 0
        observed_latency = 0
        message = ""
        try:
            await targets.inject(scenario.mode, threshold=scenario.threshold)
            if scenario.mode == INJECTION_UNAVAILABLE:
                attempts = 1
                unavailable_event = _evt(scenario_index)
                scenario_index += 1
                try:
                    await targets.publish(unavailable_event)
                except EventPublishError:
                    failures = 1
                except Exception as exc:  # 非 EventPublishError 不算命中
                    message = f"不可达发布抛出 {type(exc).__name__}（须为 EventPublishError）"
                try:
                    await targets.read_only_msg()
                except Exception as exc:
                    message = message or f"只读调用失败：{type(exc).__name__}"
                else:
                    failures = 0
                    message = message or "只读调用未失败（不可达注入未生效）"
            elif scenario.mode == INJECTION_ERROR_RATE:
                attempts = scenario.samples
                successes = 0
                for _sample in range(scenario.samples):
                    try:
                        await targets.publish(_evt(scenario_index))
                    except EventPublishError:
                        failures += 1
                    except Exception as exc:
                        message = f"错误率采样抛出 {type(exc).__name__}（须为 EventPublishError）"
                        break
                    else:
                        successes += 1
                    finally:
                        scenario_index += 1
                if successes:
                    drained = await targets.consume()
                    counts["published_msg"] = counts.get("published_msg", 0) + successes
                    counts["consumed_msg"] = counts.get("consumed_msg", 0) + len(drained)
                error_failures = failures
            else:
                attempts = 1
                probe = await targets.read_only_msg()
                observed_latency = probe.latency_ms
                latency_ms = probe.latency_ms
        except Exception as exc:
            _failed(f"inject.{scenario.name}", tick, f"{type(exc).__name__}: {exc}")
            continue

        matched = criterion_matched(scenario, failures=failures, attempts=attempts, latency_ms=observed_latency)
        injection_rows.add(
            ConcurrentStableFrozenDict(
                {
                    "name": scenario.name,
                    "mode": scenario.mode,
                    "target": scenario.target,
                    "criterion": scenario.criterion,
                    "threshold": f"{scenario.threshold}",
                    "attempts": str(attempts),
                    "failures": str(failures),
                    "error_rate": f"{error_rate_percent(failures, attempts)}",
                    "latency_ms": str(observed_latency),
                    "matched": "true" if matched else "false",
                    "expected": scenario.expected,
                }
            )
        )
        if not matched:
            _failed(
                f"inject.{scenario.name}",
                tick,
                message or f"判据未命中（尝试 {attempts} / 失败 {failures} / 延迟 {observed_latency}ms）",
            )
            continue
        if scenario.mode == INJECTION_UNAVAILABLE:
            detail = "不可达：发布抛 EventPublishError、只读调用失败"
        else:
            detail = f"{scenario.mode} 命中判据（尝试 {attempts} / 失败 {failures} / 延迟 {observed_latency}ms）"
        _step(f"inject.{scenario.name}", RESULT_OK, tick, detail)

        unconfirmed_id = ""
        if scenario.mode == INJECTION_UNAVAILABLE and targets.supports_outbox():
            tick = clock()
            try:
                unconfirmed_id = await targets.enqueue_outbox(_evt(scenario_index, tenant=None))
                scenario_index += 1
                counts["outbox_enqueued"] = counts.get("outbox_enqueued", 0) + 1
                retained = await targets.dispatch_outbox()
            except Exception as exc:
                _failed(f"unconfirmed.{scenario.name}", tick, f"{type(exc).__name__}: {exc}")
            else:
                if retained.published != 0 or retained.failed < 1:
                    _failed(
                        f"unconfirmed.{scenario.name}",
                        tick,
                        f"未确认事件应保行：投递 {retained.published} / 失败 {retained.failed}",
                    )
                else:
                    _step(
                        f"unconfirmed.{scenario.name}",
                        RESULT_OK,
                        tick,
                        f"未确认事件保行（failed={retained.failed} backlog={retained.backlog}）",
                    )

        rollback_started = now()
        rollback_tick = clock()
        tick = rollback_tick
        rollback_report = targets.switch_to_original()
        rollback_errors = switch_errors(rollback_report, _ORIGINAL_PROVIDER)
        config_change_ms = max(int((clock() - rollback_tick) * 1000), 0)
        if rollback_errors:
            _failed(f"rollback.{scenario.name}", tick, "；".join(rollback_errors))
            continue
        if targets.config_version() != before_switch_version:
            _failed(f"rollback.{scenario.name}", tick, "回滚改动了配置版本（只允许改 provider）")
            continue
        rollback_finished = now()
        _step(
            f"rollback.{scenario.name}",
            RESULT_OK,
            tick,
            f"两端口回原实现（event={rollback_report.event.implementation} "
            f"service_client={rollback_report.service_client.implementation}）配置耗时 {config_change_ms}ms",
        )

        tick = clock()
        try:
            await targets.clear_injection()
            if unconfirmed_id:
                first_round = await targets.dispatch_outbox()
                second_round = await targets.dispatch_outbox()
            else:
                first_round = OutboxRound(0, 0, 0, 0)
                second_round = OutboxRound(0, 0, 0, 0)
            counts["delivered_original"] = counts.get("delivered_original", 0) + first_round.published
            recovered_readonly = await targets.read_only_original()
        except Exception as exc:
            _failed(f"recover.{scenario.name}", tick, f"{type(exc).__name__}: {exc}")
            continue
        recover_errors: ConcurrentStableList[str] = ConcurrentStableList()
        if unconfirmed_id and first_round.published != 1:
            recover_errors.add(f"未确认事件应补发 1 条，实际 {first_round.published}")
        if unconfirmed_id and second_round.published != 0:
            recover_errors.add(f"再投应 0 条，实际 {second_round.published}（重投）")
        if recovered_readonly.count <= 0:
            recover_errors.add(f"{recovered_readonly.target} 返回条数 {recovered_readonly.count}")
        elif baseline_readonly is not None and recovered_readonly.digest != baseline_readonly.digest:
            recover_errors.add("原链路只读结果与基线不同构")
        recovery_ms = max(int((clock() - rollback_tick) * 1000), 0)
        if recovery_ms > scenarios.recovery_budget_ms:
            recover_errors.add(f"恢复超预算：{recovery_ms}ms > {scenarios.recovery_budget_ms}ms")
        cycles.add(
            CycleRecord(
                scenario=scenario.name,
                switch_ms=switch_ms,
                rollback_started_at=rollback_started.isoformat(),
                rollback_finished_at=rollback_finished.isoformat(),
                config_change_ms=config_change_ms,
                recovery_ms=recovery_ms,
            )
        )
        counts["cycles"] = counts.get("cycles", 0) + 1
        if recover_errors:
            _failed(f"recover.{scenario.name}", tick, "；".join(recover_errors))
            continue
        transport = targets.original_event_transport()
        detail = f"原链路恢复：只读 digest={recovered_readonly.digest}，恢复耗时 {recovery_ms}ms（载体 {transport}）"
        if transport != _ORIGINAL_EVENT_PROVIDER:
            _step(f"recover.{scenario.name}", RESULT_DEGRADED, tick, detail + "；原链路事件侧为记录型替身")
            reasons.add(f"recover.{scenario.name}：原链路事件侧为记录型替身（broker 未部署）")
        else:
            _step(f"recover.{scenario.name}", RESULT_OK, tick, detail)

        try:
            snapshots.add(await targets.snapshot())
        except Exception as exc:
            _failed(f"snapshot.{scenario.name}", tick, f"{type(exc).__name__}: {exc}")

        if order != len(scenarios.scenarios) - 1 and not failed_steps:
            tick = clock()
            again = targets.switch_to_msg()
            again_errors = switch_errors(again, _MSG_PROVIDER)
            if again_errors:
                _failed(f"switch_back.{scenario.name}", tick, "；".join(again_errors))
                continue
            try:
                await targets.publish(_evt(scenario_index))
                scenario_index += 1
                again_messages = await targets.consume()
                counts["published_msg"] = counts.get("published_msg", 0) + 1
                counts["consumed_msg"] = counts.get("consumed_msg", 0) + len(again_messages)
            except Exception as exc:
                _failed(f"switch_back.{scenario.name}", tick, f"{type(exc).__name__}: {exc}")
            else:
                if len(again_messages) != 1:
                    _failed(f"switch_back.{scenario.name}", tick, f"重切后应消费 1 条，实际 {len(again_messages)}")
                else:
                    _step(f"switch_back.{scenario.name}", RESULT_OK, tick, "重新切到 msg 并冒烟一条事件链")

    if not failed_steps:
        tick = clock()
        try:
            await targets.enqueue_outbox(_evt(scenario_index, tenant=None))
            scenario_index += 1
            await targets.enqueue_outbox(_evt(scenario_index, tenant=None))
            scenario_index += 1
            counts["outbox_enqueued"] = counts.get("outbox_enqueued", 0) + 2
            dual_round = await targets.dispatch_outbox()
            counts["delivered_original"] = counts.get("delivered_original", 0) + dual_round.published
            final_messages = await targets.consume()
        except Exception as exc:
            _failed("dual_write_original", tick, f"{type(exc).__name__}: {exc}")
        else:
            if len(final_messages) != 0 or dual_round.published != 2:
                _failed(
                    "dual_write_original",
                    tick,
                    f"原链路应 2 条 / msg 应 0 条，实际 {dual_round.published} / {len(final_messages)}",
                )
            else:
                _step(
                    "dual_write_original",
                    RESULT_OK,
                    tick,
                    "原链路两次投递 = 两条，msg 零条（无第二条通道）",
                )

    tick = clock()
    final_snapshot: StateSnapshot | None = None
    try:
        final_snapshot = await targets.snapshot()
        snapshots.add(final_snapshot)
        counts["outbox_pending"] = final_snapshot.outbox.pending
    except Exception as exc:
        _failed("snapshot_final", tick, f"{type(exc).__name__}: {exc}")
    else:
        _step("snapshot_final", RESULT_OK, tick, "演练结束快照已采集")

    integrity = integrity_report(ConcurrentStableFrozenList(snapshots), counts=ConcurrentStableFrozenDict(counts))
    tick = clock()
    if final_snapshot is not None and not final_snapshot.available:
        _step("consistency", RESULT_DEGRADED, tick, "快照不可用，位点 / 死信 / 重放 / 审计链未验（降级）")
        reasons.add("consistency：网络形态无一致性快照，四类校验未全验")
    elif integrity.ok:
        _step("consistency", RESULT_OK, tick, "效果一次 / 无丢失 / 位点死信重放 / 审计链 四类全过")
    else:
        detail = "；".join(integrity.detail) or "四类一致性未全过"
        _step("consistency", RESULT_FAILED, tick, detail)
        reasons.add(f"consistency：{detail}")

    tick = clock()
    zero_errors, _evidence = business_zero_change()
    if zero_errors:
        _failed("business_zero_change", tick, "；".join(zero_errors))
    else:
        _step("business_zero_change", RESULT_OK, tick, "端口签名不变、回滚未改业务代码")

    try:
        await targets.cleanup()
    except Exception as exc:
        issues.add(
            ConcurrentStableFrozenDict(
                {"summary": f"清理隔离环境失败：{type(exc).__name__}", "disposition": "手工清理临时目录"}
            )
        )
    await targets.aclose()
    finished_at = now()

    failed = any(step.result == RESULT_FAILED for step in steps)
    degraded = any(step.result in (RESULT_DEGRADED, RESULT_SKIPPED) for step in steps)
    if failed:
        conclusion = CONCLUSION_FAIL
        data_integrity = DATA_INTEGRITY_UNAVAILABLE
    elif degraded:
        conclusion = CONCLUSION_DEGRADED
        data_integrity = DATA_INTEGRITY_GAP
    else:
        conclusion = CONCLUSION_PASS
        data_integrity = DATA_INTEGRITY_COMPLETE
    if gate.blocked_by:
        issues.add(
            ConcurrentStableFrozenDict(
                {
                    "summary": f"生产迁移被前置清单阻断：{'、'.join(gate.blocked_by)}",
                    "disposition": "8B_05 交付后更新清单状态；本任务不进入生产迁移",
                }
            )
        )
    if targets.original_event_transport() != _ORIGINAL_EVENT_PROVIDER:
        leftovers.add(
            ConcurrentStableFrozenDict(
                {"item": "原链路事件侧真实 RocketMQ 集群联调（当前为记录型替身）", "owner": "bms 8B_08 + 部署阶段"}
            )
        )
    leftovers.add(
        ConcurrentStableFrozenDict(
            {
                "item": "域级白名单 / 灰度键（当前只有全局 provider）",
                "owner": "bms 8B_08",
            }
        )
    )
    leftovers.add(ConcurrentStableFrozenDict({"item": "判据阈值真实数值（当前为占位形状）", "owner": "8B_12"}))
    leftovers.add(ConcurrentStableFrozenDict({"item": "真实集群回滚与生产窗口", "owner": "bms 8B_08"}))

    return RollbackReport(
        mode=_MODE_DRILL,
        env=env,
        started_at=started_at,
        finished_at=finished_at,
        config_version=before_switch_version,
        switch_domain=ConcurrentStableFrozenList(switch_domain),
        baseline=ConcurrentStableFrozenDict(
            {
                "event_implementation": baseline.event.implementation,
                "service_client_implementation": baseline.service_client.implementation,
                "config_fingerprint": baseline.fingerprint,
                "readonly_digest": baseline_readonly.digest if baseline_readonly is not None else "",
                "backup_mode": backup.mode,
                "backup_id": backup.backup_id,
                "backup_note": backup.note,
            }
        ),
        switch=ConcurrentStableFrozenDict(
            {
                "event_implementation": switch.event.implementation,
                "service_client_implementation": switch.service_client.implementation,
                "config_fingerprint": switch.fingerprint,
            }
        ),
        injection=ConcurrentStableFrozenDict(
            {
                "total": len(scenarios.scenarios),
                "scenarios": ConcurrentStableFrozenList(injection_rows),
                "threshold_source": "placeholder_8b12",
                "latency_source": targets.latency_source(),
            }
        ),
        observe_metrics=ConcurrentStableFrozenDict(
            {
                "msg_publish_errors_total": float(error_failures),
                "msg_rpc_duration_ms": float(latency_ms),
                "msg_messages_in_total": float(counts.get("published_msg", 0)),
                "msg_rpc_calls_total": float(counts.get("cycles", 0) + 2),
            }
        ),
        rollback=ConcurrentStableFrozenDict(
            {
                "cycles": ConcurrentStableFrozenList(cycles),
                "recovery_ms": cycles[-1].recovery_ms if cycles else 0,
                "config_change_ms": cycles[-1].config_change_ms if cycles else 0,
                "recovery_budget_ms": scenarios.recovery_budget_ms,
            }
        ),
        gate=ConcurrentStableFrozenDict(
            {
                "allowed": gate.allowed,
                "blocked_by": ConcurrentStableFrozenList(gate.blocked_by),
                "checked_env": gate.env,
            }
        ),
        steps=ConcurrentStableFrozenList(steps),
        counts=ConcurrentStableFrozenDict(counts),
        integrity=integrity,
        backup=backup,
        conclusion=conclusion,
        data_integrity=data_integrity,
        reasons=ConcurrentStableFrozenList(reasons),
        issues=ConcurrentStableFrozenList(issues),
        leftovers=ConcurrentStableFrozenList(leftovers),
        artifacts=ConcurrentStableFrozenDict(),
    )


# ---------------------------------------------------------------------------
# 记录渲染
# ---------------------------------------------------------------------------


def _plain(value: object) -> object:
    """把并发集合递归折成普通结构（JSON 序列化用）。

    Args:
        value: 任意值。

    Returns:
        object: 普通 dict / list / 标量。
    """
    if isinstance(value, (ConcurrentStableFrozenDict, ConcurrentStableDict)):
        return {str(key): _plain(item) for key, item in cast("dict[str, object]", value.items())}
    if isinstance(value, (ConcurrentStableFrozenList, ConcurrentStableList)):
        return [_plain(item) for item in cast("list[object]", value)]
    if isinstance(value, CycleRecord):
        return {
            "scenario": value.scenario,
            "switch_ms": value.switch_ms,
            "rollback_started_at": value.rollback_started_at,
            "rollback_finished_at": value.rollback_finished_at,
            "config_change_ms": value.config_change_ms,
            "recovery_ms": value.recovery_ms,
        }
    return value


def record_dict(report: RollbackReport) -> ConcurrentStableDict[str, object]:
    """把报告折成 JSON 记录（字段与设计 §10.1 一一对应）。

    Args:
        report: 回滚演练报告。

    Returns:
        ConcurrentStableDict[str, object]: 可 JSON 序列化的记录。
    """
    record: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    record.set("schema_version", ROLLBACK_RECORD_SCHEMA_VERSION)
    record.set("task", "07-4-2")
    record.set("mode", report.mode)
    record.set(
        "environment",
        {
            "os": platform_module.system().lower(),
            "arch": platform_module.machine(),
            "python": platform_module.python_version(),
            "env": report.env,
        },
    )
    record.set("service", {"name": SERVICE_NAME, "version": __version__, "contract_version": CONTRACT_VERSION})
    record.set("config_version", report.config_version)
    record.set("switch_domain", [_plain(item) for item in report.switch_domain])
    record.set("baseline", _plain(report.baseline))
    record.set("switch", _plain(report.switch))
    record.set("injection", _plain(report.injection))
    record.set("observe_metrics", _plain(report.observe_metrics))
    record.set("rollback", _plain(report.rollback))
    record.set("production_gate", _plain(report.gate))
    record.set("started_at", report.started_at.isoformat())
    record.set("finished_at", report.finished_at.isoformat())
    record.set(
        "steps",
        [
            {"step": step.step, "result": step.result, "elapsed_ms": step.elapsed_ms, "detail": step.detail}
            for step in report.steps
        ],
    )
    record.set("counts", dict(report.counts.items()))
    record.set(
        "integrity",
        {
            "effect_once": report.integrity.effect_once,
            "no_loss": report.integrity.no_loss,
            "state": report.integrity.state,
            "audit_chain": report.integrity.audit_chain,
            "detail": list(report.integrity.detail),
        },
    )
    record.set("backup", {"mode": report.backup.mode, "backup_id": report.backup.backup_id, "note": report.backup.note})
    record.set(
        "conclusion",
        {
            "result": report.conclusion,
            "data_integrity": report.data_integrity,
            "reasons": list(report.reasons),
            "requires_review": report.conclusion != CONCLUSION_PASS,
        },
    )
    record.set("issues", [_plain(item) for item in report.issues])
    record.set("leftovers", [_plain(item) for item in report.leftovers])
    record.set("artifacts", dict(report.artifacts.items()))
    return record


def render_markdown(record: ConcurrentStableDict[str, object]) -> str:
    """按 JSON 记录渲染 Markdown（同源，不另取数）。

    Args:
        record: `record_dict()` 产物。

    Returns:
        str: Markdown 文本。
    """
    conclusion = cast("dict[str, object]", record["conclusion"])
    environment = cast("dict[str, object]", record["environment"])
    lines: ConcurrentStableList[str] = ConcurrentStableList()
    lines.add("# 回滚演练记录（07-4-2）")
    lines.add("")
    lines.add(f"- 结论：**{conclusion['result']}**（数据完整性：{conclusion['data_integrity']}）")
    lines.add(
        f"- 环境：`{environment['env']}`（os={environment['os']} "
        f"arch={environment['arch']} python={environment['python']}）"
    )
    lines.add(f"- 开始：{record['started_at']}　结束：{record['finished_at']}")
    lines.add(f"- 配置版本：`{record['config_version']}`")
    lines.add("")
    lines.add("## 1. 切换域与闸门")
    lines.add("")
    lines.add("| 域 | 端口 | 开关键 |")
    lines.add("| --- | --- | --- |")
    for domain in cast("list[dict[str, str]]", record["switch_domain"]):
        lines.add(f"| `{domain['key']}` | `{domain['port']}` | `{domain['switch_key']}` |")
    gate = cast("dict[str, object]", record["production_gate"])
    blocked = "、".join(cast("list[str]", gate["blocked_by"])) or "无"
    lines.add(f"| `production_gate.allowed` | {gate['allowed']} | `blocked_by`：{blocked} |")
    lines.add("")
    lines.add("## 2. 注入与判据")
    lines.add("")
    lines.add("| 场景 | 模式 | 阈值 | 观测 | 命中 |")
    lines.add("| --- | --- | --- | --- | --- |")
    injection = cast("dict[str, object]", record["injection"])
    for row in cast("list[dict[str, str]]", injection["scenarios"]):
        lines.add(
            f"| `{row['name']}` | `{row['mode']}` | {row['threshold']} "
            f"| 尝试 {row['attempts']} / 失败 {row['failures']} / 延迟 {row['latency_ms']}ms | {row['matched']} |"
        )
    lines.add("")
    lines.add("## 3. 回滚与恢复")
    lines.add("")
    lines.add("| 场景 | 配置改完(ms) | 恢复(ms) | 回滚开始 | 回滚结束 |")
    lines.add("| --- | --- | --- | --- | --- |")
    rollback = cast("dict[str, object]", record["rollback"])
    for cycle in cast("list[dict[str, object]]", rollback["cycles"]):
        lines.add(
            f"| {cycle['scenario']} | {cycle['config_change_ms']} | {cycle['recovery_ms']} "
            f"| {cycle['rollback_started_at']} | {cycle['rollback_finished_at']} |"
        )
    lines.add(f"| 预算 | — | {rollback['recovery_budget_ms']} | — | — |")
    lines.add("")
    lines.add("## 4. 步骤")
    lines.add("")
    lines.add("| 步骤 | 结果 | 耗时(ms) | 明细 |")
    lines.add("| --- | --- | --- | --- |")
    for step in cast("list[dict[str, object]]", record["steps"]):
        lines.add(f"| {step['step']} | {step['result']} | {step['elapsed_ms']} | {step['detail']} |")
    lines.add("")
    lines.add("## 5. 计数与一致性")
    lines.add("")
    lines.add("| 项 | 值 |")
    lines.add("| --- | --- |")
    for key, value in cast("dict[str, int]", record["counts"]).items():
        lines.add(f"| `{key}` | {value} |")
    integrity = cast("dict[str, object]", record["integrity"])
    lines.add(
        f"| `integrity` | 效果一次 {integrity['effect_once']} / 无丢失 {integrity['no_loss']} "
        f"/ 状态 {integrity['state']} / 审计链 {integrity['audit_chain']} |"
    )
    lines.add("")
    lines.add("## 6. 原因 / 问题 / 遗留")
    lines.add("")
    reasons = cast("list[str]", conclusion["reasons"])
    lines.add("**原因**：" + ("；".join(reasons) if reasons else "无"))
    lines.add("")
    lines.add("| 问题 | 处置 |")
    lines.add("| --- | --- |")
    for issue in cast("list[dict[str, str]]", record["issues"]):
        lines.add(f"| {issue['summary']} | {issue['disposition']} |")
    lines.add("")
    lines.add("| 遗留 | 归口 |")
    lines.add("| --- | --- |")
    for leftover in cast("list[dict[str, str]]", record["leftovers"]):
        lines.add(f"| {leftover['item']} | {leftover['owner']} |")
    lines.add("")
    return "\n".join(lines)


def write_record(report: RollbackReport, report_path: Path) -> tuple[Path, Path]:
    """写 JSON + Markdown 记录（同一份数据，两个载体）。

    Args:
        report: 回滚演练报告。
        report_path: JSON 落点（同名 `.md` 一并写）。

    Returns:
        tuple: JSON 路径 + Markdown 路径。
    """
    json_path = report_path.with_suffix(".json")
    markdown_path = report_path.with_suffix(".md")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    artifacts = ConcurrentStableDict(report.artifacts)
    artifacts.set("report_json", str(json_path))
    artifacts.set("report_md", str(markdown_path))
    finalized = RollbackReport(
        mode=report.mode,
        env=report.env,
        started_at=report.started_at,
        finished_at=report.finished_at,
        config_version=report.config_version,
        switch_domain=report.switch_domain,
        baseline=report.baseline,
        switch=report.switch,
        injection=report.injection,
        observe_metrics=report.observe_metrics,
        rollback=report.rollback,
        gate=report.gate,
        steps=report.steps,
        counts=report.counts,
        integrity=report.integrity,
        backup=report.backup,
        conclusion=report.conclusion,
        data_integrity=report.data_integrity,
        reasons=report.reasons,
        issues=report.issues,
        leftovers=report.leftovers,
        artifacts=ConcurrentStableFrozenDict(artifacts),
    )
    record = record_dict(finalized)
    payload = dict(record.items())
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8", newline="\n"
    )
    markdown_path.write_text(render_markdown(record), encoding="utf-8", newline="\n")
    return json_path, markdown_path


def report_signature(report: RollbackReport) -> str:
    """计算可复跑签名（步骤结果 + 计数 + 结论 + 一致性，不含时间戳）。

    Args:
        report: 回滚演练报告。

    Returns:
        str: sha256 前 16 位。
    """
    payload = {
        "steps": [{"step": step.step, "result": step.result, "detail": step.detail} for step in report.steps],
        "counts": dict(report.counts.items()),
        "conclusion": report.conclusion,
        "data_integrity": report.data_integrity,
        "integrity": {
            "effect_once": report.integrity.effect_once,
            "no_loss": report.integrity.no_loss,
            "state": report.integrity.state,
            "audit_chain": report.integrity.audit_chain,
        },
    }
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"))
    return digest.hexdigest()[:16]


# ---------------------------------------------------------------------------
# 网络演练目标（CLI 用；用例用进程内实现）
# ---------------------------------------------------------------------------


class _SwitchablePublisher(EventPublisher):
    """可切换的发布端口（回滚演练改配置后换实现，不改调用点）。"""

    def __init__(self, publisher: EventPublisher) -> None:
        """初始化。

        Args:
            publisher: 当前实现。
        """
        self.publisher = publisher

    @property
    def event_type(self) -> str:
        """事件类型（跟随当前实现）。"""
        return self.publisher.event_type

    async def publish(self, event: EventEnvelope) -> None:
        """转发发布。

        Args:
            event: 事件信封。
        """
        await self.publisher.publish(event)

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """转发事务发布。

        Args:
            event: 事件信封。
        """
        await self.publisher.publish_transactional(event)

    async def aclose(self) -> None:
        """释放当前实现。"""
        await self.publisher.aclose()


class _RecordingPublisher(EventPublisher):
    """记录型发布器（RocketMQ broker 未部署时的原链路替身；记录每条投递）。"""

    def __init__(self) -> None:
        """初始化（空记录）。"""
        self.delivered: ConcurrentStableList[str] = ConcurrentStableList()

    @property
    def event_type(self) -> str:
        """事件类型（固定 `rocketmq`）。"""
        return _ORIGINAL_EVENT_PROVIDER

    async def publish(self, event: EventEnvelope) -> None:
        """记录一条投递。

        Args:
            event: 事件信封。
        """
        self.delivered.add(event.event_id or "")

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。

        Args:
            event: 事件信封。
        """
        await self.publish(event)


class _UnavailablePublisher(EventPublisher):
    """不可达发布器（注入 `unavailable`）。"""

    @property
    def event_type(self) -> str:
        """事件类型（固定 `msg`）。"""
        return _MSG_PROVIDER

    async def publish(self, event: EventEnvelope) -> None:
        """恒抛投递失败。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 恒定失败。
        """
        del event
        raise EventPublishError("msg 实例不可达（注入）")

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（恒失败）。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 恒定失败。
        """
        await self.publish(event)


class _FlakyPublisher(EventPublisher):
    """按固定频率失败的发布器（注入 `error_rate`）。"""

    def __init__(self, inner: EventPublisher, *, fail_every: int) -> None:
        """初始化。

        Args:
            inner: 正常实现。
            fail_every: 每第几次失败（1 = 全失败）。
        """
        self._inner = inner
        self._fail_every = max(fail_every, 1)
        self._attempts = 0

    @property
    def event_type(self) -> str:
        """事件类型（跟随内层）。"""
        return self._inner.event_type

    async def publish(self, event: EventEnvelope) -> None:
        """按频率失败后再转发。

        Args:
            event: 事件信封。

        Raises:
            EventPublishError: 命中失败频率。
        """
        self._attempts += 1
        if self._attempts % self._fail_every == 0:
            raise EventPublishError("msg 发布错误率超阈值（注入）")
        await self._inner.publish(event)

    async def publish_transactional(self, event: EventEnvelope) -> None:
        """事务发布（同 `publish`）。

        Args:
            event: 事件信封。
        """
        await self.publish(event)


class _RegistryAdapter:
    """最小引擎注册表适配（投递器只需要 `get` / `is_sync_only`）。"""

    def __init__(self, engine: object) -> None:
        """初始化。

        Args:
            engine: 异步引擎。
        """
        self._engine = engine

    def is_sync_only(self, db_key: str) -> bool:
        """是否同步方言（网络演练只接异步方言）。

        Args:
            db_key: 数据源键。

        Returns:
            bool: 恒 False。
        """
        del db_key
        return False

    async def get(self, db_key: str, *, read_only: bool = False) -> object:
        """取引擎。

        Args:
            db_key: 数据源键。
            read_only: 是否只读（忽略）。

        Returns:
            object: 异步引擎。
        """
        del db_key, read_only
        return self._engine


class HttpRollbackTargets:
    """网络演练目标：真 HTTP 管理面 / 数据面 + 真发件箱（可选）+ 原链路只读。"""

    def __init__(
        self,
        *,
        settings: Settings,
        msg_url: str,
        unavailable_url: str,
        namespace: str,
        event_type: str,
        group: str,
        consumer_id: str,
        tenant: str,
        read_only_service: str,
        read_only_method: str,
        original_readonly_url: str,
        outbox_url: str = "",
        http_transport: httpx.AsyncBaseTransport | None = None,
        original_transport: httpx.AsyncBaseTransport | None = None,
        outbox_engine: object | None = None,
        rpc_transport: RpcTransport | None = None,
    ) -> None:
        """初始化（构造连接对象，不发起调用）。

        Args:
            settings: bms 配置（切换判定与 RocketMQ 端点来源）。
            msg_url: msg 实例基址。
            unavailable_url: 不可达实例基址（注入 `unavailable`）。
            namespace: 事件命名空间。
            event_type: 演练事件类型。
            group: 消费组。
            consumer_id: 成员标识。
            tenant: 租户编码。
            read_only_service: 只读调用目标服务。
            read_only_method: 只读调用目标方法。
            original_readonly_url: 原链路只读地址。
            outbox_url: 平台库连接串（空 = 不验发件箱）。
            http_transport: 注入的 httpx 传输。
            original_transport: 原链路传输（缺省同 `http_transport`）。
            outbox_engine: 注入的异步引擎（用例给内存库；生产按 `outbox_url` 新建）。
            rpc_transport: 注入的 RPC 传输（用例走 ASGI；生产为 None → h2）。
        """
        from ops.migration import build_http_targets

        self._settings = settings
        self._msg_url = msg_url.rstrip("/")
        self._namespace = namespace
        self._event_type = event_type
        self._tenant = tenant
        self._msg_targets = build_http_targets(
            settings,
            base_url=self._msg_url,
            unavailable_url=unavailable_url,
            namespace=namespace,
            event_type=event_type,
            group=group,
            consumer_id=consumer_id,
            tenant=tenant,
            read_only_service=read_only_service,
            read_only_method=read_only_method,
            http_transport=http_transport,
            rpc_transport=rpc_transport,
        )
        self._msg_publisher = MsgEventPublisher(
            base_url=self._msg_url, namespace=namespace, tenant=tenant, transport=http_transport
        )
        self._recording = _RecordingPublisher()
        self._rocket: RocketMqEventPublisher | None = (
            RocketMqEventPublisher.from_settings(settings) if settings.rocketmq.endpoints else None
        )
        self._switchable = _SwitchablePublisher(self._msg_publisher)
        self._original_http = httpx.AsyncClient(
            base_url=original_readonly_url.rstrip("/"),
            transport=original_transport or http_transport,
            timeout=10.0,
        )
        self._outbox_url = outbox_url
        self._outbox_engine: object | None = outbox_engine
        self._owns_engine = False
        self._outbox_store: SqlOutboxStore | None = None
        self._dispatcher: PollOutboxDispatcher | None = None
        self._session_factory: object | None = None
        self._closed = False
        self._injected = ""
        self._base_publisher: EventPublisher = self._msg_publisher

    def original_config(self) -> SwitchReport:
        """回滚前的配置解析（须为原实现）。

        Returns:
            SwitchReport: 两个端口的判定。
        """
        self._settings.event.provider = _ORIGINAL_EVENT_PROVIDER
        self._settings.service_client.provider = _ORIGINAL_CLIENT_PROVIDER
        return resolve_switch(self._settings)

    def switch_to_msg(self) -> SwitchReport:
        """只改配置切到 msg。

        Returns:
            SwitchReport: 两个端口的判定。
        """
        self._settings.event.provider = _MSG_PROVIDER
        self._settings.service_client.provider = _MSG_PROVIDER
        self._base_publisher = self._msg_publisher
        self._switchable.publisher = self._msg_publisher
        self._injected = ""
        return resolve_switch(self._settings)

    def switch_to_original(self) -> SwitchReport:
        """只改配置回原实现。

        Returns:
            SwitchReport: 两个端口的判定。
        """
        self._settings.event.provider = _ORIGINAL_EVENT_PROVIDER
        self._settings.service_client.provider = _ORIGINAL_CLIENT_PROVIDER
        self._base_publisher = self._rocket if self._rocket is not None else self._recording
        self._switchable.publisher = self._base_publisher
        self._injected = ""
        return resolve_switch(self._settings)

    def config_version(self) -> str:
        """配置版本（只含 provider 之外的部署配置，不含敏感值）。

        Returns:
            str: sha256 前 16 位。
        """
        return stable_config_version(self._settings)

    def original_event_transport(self) -> str:
        """原链路事件侧载体。

        Returns:
            str: `rocketmq` 或 `recording`。
        """
        return _ORIGINAL_EVENT_PROVIDER if self._rocket is not None else _RECORDING_TRANSPORT

    def latency_source(self) -> str:
        """延迟观测来源（网络形态实测）。

        Returns:
            str: `measured`。
        """
        return LATENCY_SOURCE_MEASURED

    def supports_outbox(self) -> bool:
        """是否提供真发件箱。

        Returns:
            bool: 配置了平台库（或注入引擎）为 True。
        """
        return bool(self._outbox_url) or self._outbox_engine is not None

    async def prepare(self) -> None:
        """建主题并加入消费组。"""
        await self._msg_targets.prepare_event()

    async def publish(self, event: EventEnvelope) -> None:
        """经当前 provider 发布。

        Args:
            event: 事件信封。
        """
        await self._switchable.publish(event)

    async def consume(self) -> ConcurrentStableFrozenList[DrillMessage]:
        """拉取并确认当前批。

        Returns:
            ConcurrentStableFrozenList[DrillMessage]: 本次新消息。
        """
        return await self._msg_targets.consume_events()

    async def read_only_msg(self) -> ProbeResult:
        """msg 通道只读调用（含实测耗时）。

        Returns:
            ProbeResult: 结果与耗时。

        Raises:
            RollbackError: 注入不可达时。
        """
        if self._injected == INJECTION_UNAVAILABLE:
            raise RollbackError("rollback.inject: msg 实例不可达（注入）")
        started = _monotonic()
        result = await self._msg_targets.read_only()
        elapsed = max(int((_monotonic() - started) * 1000), 0)
        return ProbeResult(target=result.target, digest=result.digest, count=result.count, latency_ms=elapsed)

    async def read_only_original(self) -> ProbeResult:
        """原链路只读调用（含实测耗时）。

        Returns:
            ProbeResult: 结果与耗时。
        """
        started = _monotonic()
        response = await self._original_http.get("/api/v1/modules/snapshot")
        response.raise_for_status()
        elapsed = max(int((_monotonic() - started) * 1000), 0)
        return ProbeResult(target="http:snapshot", digest=digest_bytes(response.content), count=1, latency_ms=elapsed)

    async def inject(self, mode: str, *, threshold: float) -> None:
        """注入失败（网络形态：不可达 / 错误率 / 延迟）。

        Args:
            mode: 注入模式。
            threshold: 错误率百分比或延迟毫秒。
        """
        self._injected = mode
        if mode == INJECTION_UNAVAILABLE:
            self._switchable.publisher = _UnavailablePublisher()
        elif mode == INJECTION_ERROR_RATE:
            fail_every = max(round(100.0 / max(threshold, 0.0001)), 1)
            self._switchable.publisher = _FlakyPublisher(self._base_publisher, fail_every=fail_every)
        elif mode == INJECTION_LATENCY:
            await asyncio.sleep(max(threshold, 0.0) / 1000.0)

    async def clear_injection(self) -> None:
        """解除注入（恢复当前模式的正常实现）。"""
        self._injected = ""
        self._switchable.publisher = self._base_publisher

    async def inflight_attempt(self, key: str) -> WriteAttemptResult | None:
        """网络演练不注入传输失败（在途判据由进程内演练覆盖）。

        Args:
            key: 幂等键（未使用）。

        Returns:
            None: 恒定不支持。
        """
        del key
        return None

    async def _ensure_outbox(self) -> None:
        """按需建发件箱连接（首次使用）。

        Raises:
            RollbackError: 未配置平台库。
        """
        if self._dispatcher is not None:
            return
        if self._outbox_engine is None:
            if not self._outbox_url:
                raise RollbackError("rollback.outbox: 未配置平台库，无法验证未确认事件与 outbox 重投")
            from sqlalchemy.ext.asyncio import create_async_engine

            self._outbox_engine = create_async_engine(self._outbox_url)
            self._owns_engine = True
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(cast("object", self._outbox_engine), expire_on_commit=False)  # type: ignore[arg-type]
        self._session_factory = factory
        store = SqlOutboxStore()
        registry = _RegistryAdapter(self._outbox_engine)
        self._outbox_store = store
        self._dispatcher = PollOutboxDispatcher(
            store=store,
            publisher=self._switchable,
            engine_registry=cast("EngineRegistry", registry),
            session_factory=None,
            batch_size=100,
            max_retries=3,
            backoff=0.0,
            db_keys=ConcurrentStableList((PLATFORM_DB_KEY,)),
        )

    async def enqueue_outbox(self, event: EventEnvelope) -> str:
        """写发件箱（真 bms 存储）。

        Args:
            event: 事件信封。

        Returns:
            str: 事件 ID。

        Raises:
            RollbackError: 未配置平台库。
        """
        await self._ensure_outbox()
        store = self._outbox_store
        factory = self._session_factory
        if store is None or factory is None:  # pragma: no cover - `_ensure_outbox` 已保证
            raise RollbackError("rollback.outbox: 存储未就绪")
        session_maker = cast("Callable[[], object]", factory)
        async with cast("object", session_maker()) as raw_session:  # type: ignore[attr-defined]
            session = cast("DbSession", raw_session)
            async with session.begin():
                return await store.enqueue(session, event)

    async def dispatch_outbox(self) -> OutboxRound:
        """投递一轮（当前 provider）。

        Returns:
            OutboxRound: 计数。

        Raises:
            RollbackError: 未配置平台库。
        """
        await self._ensure_outbox()
        dispatcher = self._dispatcher
        if dispatcher is None:  # pragma: no cover - `_ensure_outbox` 已保证
            raise RollbackError("rollback.outbox: 投递器未就绪")
        result = await dispatcher.dispatch_once(db_key=PLATFORM_DB_KEY)
        return OutboxRound(published=result.published, failed=result.failed, dead=result.dead, backlog=result.backlog)

    async def snapshot(self) -> StateSnapshot:
        """一致性快照（网络形态只给出发件箱积压，位点 / 死信 / 重放 / 审计不可读）。

        Returns:
            StateSnapshot: 快照（`available=False`）。
        """
        outbox = OutboxState(pending=0, delivered=0, dead=0)
        store = self._outbox_store
        factory = self._session_factory
        if store is not None and factory is not None:
            session_maker = cast("Callable[[], object]", factory)
            async with cast("object", session_maker()) as raw_session:  # type: ignore[attr-defined]
                pending = await store.backlog(cast("DbSession", raw_session))
            outbox = OutboxState(pending=pending, delivered=0, dead=0)
        return StateSnapshot(
            available=False,
            offsets=ConcurrentStableFrozenDict(),
            dead_letters=0,
            replay_tasks=ConcurrentStableFrozenDict(),
            audit_count=0,
            audit_digest="",
            audit_chain_ok=True,
            outbox=outbox,
            effects=ConcurrentStableFrozenDict(),
        )

    async def cleanup(self) -> None:
        """清理隔离环境（关闭本次创建的发件箱连接）。"""
        engine = self._outbox_engine
        self._outbox_engine = None
        self._outbox_store = None
        self._dispatcher = None
        self._session_factory = None
        if engine is not None and self._owns_engine:
            await cast("object", engine).dispose()  # type: ignore[attr-defined]
        self._owns_engine = False

    async def aclose(self) -> None:
        """释放全部连接（幂等）。"""
        if self._closed:
            return
        self._closed = True
        await self._msg_targets.aclose()
        await self._msg_publisher.aclose()
        if self._rocket is not None:
            await self._rocket.aclose()
        await self._original_http.aclose()


def build_targets(settings: Settings, args: argparse.Namespace) -> RollbackTargets:
    """缺省演练目标工厂（用例 monkeypatch 成进程内实现，CLI 代码路径因此可覆盖）。

    Args:
        settings: bms 配置。
        args: 命令行参数。

    Returns:
        RollbackTargets: 网络演练目标。

    Raises:
        RollbackError: 未提供原链路只读地址（网络演练无法验证恢复）。
    """
    if not cast("str", args.original_readonly_url):
        raise RollbackError("rollback.config: 缺少 --original-readonly-url，无法验证原链路恢复")
    return HttpRollbackTargets(
        settings=settings,
        msg_url=cast("str", args.msg_url),
        unavailable_url=cast("str", args.unavailable_url),
        namespace=cast("str", args.namespace),
        event_type=cast("str", args.event_type),
        group=cast("str", args.group),
        consumer_id=cast("str", args.consumer_id),
        tenant=cast("str", args.tenant),
        read_only_service=cast("str", args.read_only_service),
        read_only_method=cast("str", args.read_only_method),
        original_readonly_url=cast("str", args.original_readonly_url),
        outbox_url=cast("str", args.outbox_url),
    )


_TARGETS_FACTORY: Callable[[Settings, argparse.Namespace], RollbackTargets] = build_targets
"""演练目标工厂（模块属性；用例 monkeypatch 该属性即可离线跑 CLI）。"""


def _settings_for(args: argparse.Namespace) -> Settings:
    """按命令行参数构造 bms 配置（只覆盖开关与实例地址，不改其他项）。

    Args:
        args: 命令行参数。

    Returns:
        Settings: 应用配置。
    """
    settings = Settings()
    settings.app.service = cast("str", args.namespace)
    settings.event.provider = _ORIGINAL_EVENT_PROVIDER
    settings.event.options.set("namespace", cast("str", args.namespace))
    settings.service_client.provider = _ORIGINAL_CLIENT_PROVIDER
    settings.service_client.instances.set(_MSG_PROVIDER, ConcurrentStableList([cast("str", args.msg_url)]))
    return settings


def _default_path(raw: str, fallback: str) -> Path:
    """把命令行路径折成绝对路径（相对路径按仓根解析）。

    Args:
        raw: 命令行取值。
        fallback: 缺省相对路径。

    Returns:
        Path: 绝对路径。
    """
    value = Path(raw or fallback)
    return value if value.is_absolute() else _REPO_ROOT / value


def run_check(
    *,
    preconditions_path: Path,
    matrix_path: Path,
    scenarios_path: Path,
    env: str,
) -> tuple[int, ConcurrentStableFrozenList[str]]:
    """执行 `check`：清单 + 闸门 + 矩阵 + 场景。

    Args:
        preconditions_path: 前置清单。
        matrix_path: 切换矩阵。
        scenarios_path: 回滚场景。
        env: 归一化环境。

    Returns:
        tuple: 退出码 + 输出行。
    """
    lines: ConcurrentStableList[str] = ConcurrentStableList()
    preconditions, load_errors = load_preconditions(preconditions_path)
    errors: ConcurrentStableList[str] = ConcurrentStableList(load_errors)
    errors.extend(validate_preconditions(preconditions))
    matrix, matrix_errors = load_matrix(matrix_path)
    errors.extend(matrix_errors)
    errors.extend(
        validate_matrix(matrix, known_preconditions=ConcurrentStableFrozenList(i.id for i in preconditions.items))
    )
    scenarios, scenario_errors = load_scenarios(scenarios_path)
    errors.extend(scenario_errors)
    errors.extend(validate_scenarios(scenarios, matrix))
    if errors:
        lines.add(f"校验失败（{len(errors)} 项）：")
        lines.extend(errors)
        return _CHECK_FAIL, ConcurrentStableFrozenList(lines)
    gate = evaluate_gate(preconditions, env)
    lines.add(f"前置清单：{len(preconditions.items)} 项，全部合法")
    lines.add(f"切换矩阵：{len(matrix.domains)} 域 / {len(matrix.phases)} 批次，全部合法")
    lines.add(f"回滚场景：{len(scenarios.scenarios)} 个（三类注入齐备），预算 {scenarios.recovery_budget_ms}ms")
    lines.add(f"环境：{env}；生产闸门：{'允许' if gate.allowed else '阻断'}")
    if gate.blocked_by:
        lines.add(f"阻断项（{len(gate.blocked_by)}）：")
        lines.extend(gate.detail)
    if env == _ENV_PROD and not gate.allowed:
        return _CHECK_FAIL, ConcurrentStableFrozenList(lines)
    return _CHECK_OK, ConcurrentStableFrozenList(lines)


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="python -m ops.rollback", description="bms 回滚演练与记录（07-4-2）")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="前置清单 + 生产闸门 + 矩阵 + 场景校验")
    check.add_argument("--preconditions", default=MIGRATION_PRECONDITIONS_PATH)
    check.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)
    check.add_argument("--scenarios", default=ROLLBACK_SCENARIOS_PATH)
    check.add_argument("--env", default=MIGRATION_DEFAULT_ENV)

    matrix = subparsers.add_parser("matrix", help="渲染回滚场景表（文档表由此粘贴）")
    matrix.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)
    matrix.add_argument("--scenarios", default=ROLLBACK_SCENARIOS_PATH)

    drill = subparsers.add_parser("drill", help="隔离环境回滚演练并写记录")
    drill.add_argument("--preconditions", default=MIGRATION_PRECONDITIONS_PATH)
    drill.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)
    drill.add_argument("--scenarios", default=ROLLBACK_SCENARIOS_PATH)
    drill.add_argument("--env", default=MIGRATION_DEFAULT_ENV)
    drill.add_argument("--msg-url", required=True)
    drill.add_argument("--unavailable-url", default="http://127.0.0.1:1")
    drill.add_argument("--namespace", default=SERVICE_NAME)
    drill.add_argument("--event-type", default=MIGRATION_DEFAULT_EVENT_TYPE)
    drill.add_argument("--group", default=MIGRATION_DEFAULT_GROUP)
    drill.add_argument("--consumer-id", default=MIGRATION_DEFAULT_CONSUMER_ID)
    drill.add_argument("--tenant", default=MIGRATION_DEFAULT_TENANT)
    drill.add_argument("--read-only-service", default=MIGRATION_DEFAULT_READ_ONLY_SERVICE)
    drill.add_argument("--read-only-method", default=MIGRATION_DEFAULT_READ_ONLY_METHOD)
    drill.add_argument("--original-readonly-url", default="")
    drill.add_argument("--outbox-url", default="")
    drill.add_argument("--backup", action="store_true", help="演练前按 07-3-1 入口自动备份")
    drill.add_argument("--report", default="")
    drill.add_argument("--repeat", action="store_true", help="复跑一次并比对签名")
    return parser


def _cmd_drill(args: argparse.Namespace) -> int:
    """执行 `drill` 子命令。

    Args:
        args: 命令行参数。

    Returns:
        int: 退出码。
    """
    from bms_core.core.assembly import register_platform_plugins as _register

    from ops import backup as backup_module

    try:
        env = normalize_env(cast("str", args.env))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return _CHECK_USAGE
    if env == _ENV_PROD:
        print("生产环境禁止执行回滚演练（生产回滚归 bms 8B_08）", file=sys.stderr)
        return _CHECK_USAGE
    preconditions, load_errors = load_preconditions(
        _default_path(cast("str", args.preconditions), MIGRATION_PRECONDITIONS_PATH)
    )
    matrix, matrix_errors = load_matrix(_default_path(cast("str", args.matrix), MIGRATION_MATRIX_PATH))
    scenarios, scenario_errors = load_scenarios(_default_path(cast("str", args.scenarios), ROLLBACK_SCENARIOS_PATH))
    errors: ConcurrentStableList[str] = ConcurrentStableList(load_errors)
    errors.extend(matrix_errors)
    errors.extend(scenario_errors)
    errors.extend(validate_scenarios(scenarios, matrix))
    if errors:
        for line in errors:
            print(line, file=sys.stderr)
        return _CHECK_FAIL
    evidence = BackupEvidence(mode="not_applicable", backup_id="", note="未要求演练前备份（--backup 未给）")
    if bool(args.backup):
        try:
            config = backup_module.config_from_env(
                ConcurrentStableDict(dict(os.environ)), repo_root=_REPO_ROOT, dry_run=False, now=_now()
            )
            code = backup_module.run(config, backup_module.filesystem_io())
        except Exception as exc:
            print(f"演练前备份失败：{type(exc).__name__}: {exc}", file=sys.stderr)
            return _CHECK_FAIL
        if code != 0:
            print(f"演练前备份失败：退出码 {code}", file=sys.stderr)
            return _CHECK_FAIL
        evidence = BackupEvidence(
            mode="backup", backup_id=config.now.strftime("%Y%m%dT%H%M%SZ"), note="复用 07-3-1 入口"
        )
    settings = _settings_for(args)
    _register(settings, FastAPI(), ResourceManager())
    try:
        report = asyncio.run(
            run_rollback_drill(
                matrix=matrix,
                preconditions=preconditions,
                scenarios=scenarios,
                targets=_TARGETS_FACTORY(settings, args),
                env=env,
                backup=evidence,
                event_type=cast("str", args.event_type),
            )
        )
        if bool(args.repeat):
            again = asyncio.run(
                run_rollback_drill(
                    matrix=matrix,
                    preconditions=preconditions,
                    scenarios=scenarios,
                    targets=_TARGETS_FACTORY(settings, args),
                    env=env,
                    backup=evidence,
                    event_type=cast("str", args.event_type),
                )
            )
            if report_signature(report) != report_signature(again):
                print("复跑签名不一致（同一场景两次结果不同）", file=sys.stderr)
                return _CHECK_FAIL
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return _CHECK_USAGE
    except RollbackError as exc:
        print(str(exc), file=sys.stderr)
        return _CHECK_FAIL
    report_path = _default_path(
        cast("str", args.report), f"{ROLLBACK_REPORT_DIR}/drill-{report.started_at:%Y%m%dT%H%M%SZ}"
    )
    json_path, markdown_path = write_record(report, report_path)
    print(f"结论：{report.conclusion}（退出码 {EXIT_CODES[report.conclusion]}）")
    for step in report.steps:
        print(f"  {step.step}: {step.result}（{step.elapsed_ms}ms）{step.detail}")
    blocked = "、".join(cast("list[str]", record_dict(report)["production_gate"]["blocked_by"]))  # type: ignore[index]
    if report.gate["blocked_by"]:
        print(f"生产闸门阻断项：{blocked}（本环境不阻断）")
    print(f"记录：{json_path}")
    print(f"记录：{markdown_path}")
    return EXIT_CODES[report.conclusion]


def main(argv: list[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（None 取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "matrix":
        matrix, errors = load_matrix(_default_path(cast("str", args.matrix), MIGRATION_MATRIX_PATH))
        scenarios, scenario_errors = load_scenarios(_default_path(cast("str", args.scenarios), ROLLBACK_SCENARIOS_PATH))
        if errors or scenario_errors:
            for line in list(errors) + list(scenario_errors):
                print(line, file=sys.stderr)
            return _CHECK_FAIL
        scenario_problems = validate_scenarios(scenarios, matrix)
        if scenario_problems:
            for line in scenario_problems:
                print(line, file=sys.stderr)
            return _CHECK_FAIL
        for row in render_scenario_rows(scenarios):
            print(row)
        return _CHECK_OK
    if args.command == "check":
        try:
            env = normalize_env(cast("str", args.env))
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return _CHECK_USAGE
        code, lines = run_check(
            preconditions_path=_default_path(cast("str", args.preconditions), MIGRATION_PRECONDITIONS_PATH),
            matrix_path=_default_path(cast("str", args.matrix), MIGRATION_MATRIX_PATH),
            scenarios_path=_default_path(cast("str", args.scenarios), ROLLBACK_SCENARIOS_PATH),
            env=env,
        )
        stream = sys.stdout if code == _CHECK_OK else sys.stderr
        for line in lines:
            print(line, file=stream)
        return code
    return _cmd_drill(args)


if __name__ == "__main__":
    raise SystemExit(main())
