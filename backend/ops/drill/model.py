"""故障演练数据模型（任务 07-6-1）：场景定义 / 三段样本 / 判据 / 报告。

为什么单独一个模块：判据函数（各场景文件）、编排（`engine`）与记录渲染（`record`）共用同一组
不可变数据类，避免「脚本一套字段、记录另一套」。所有集合字段一律用插入序并发集合类。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
)

__all__ = [
    "CONCLUSION_DEGRADED",
    "CONCLUSION_FAIL",
    "CONCLUSION_PASS",
    "EXIT_CODES",
    "RESULT_DEGRADED",
    "RESULT_FAILED",
    "RESULT_OK",
    "RESULT_SKIPPED",
    "SEVERITIES",
    "SEVERITY_DEGRADABLE",
    "SEVERITY_HARD",
    "Criterion",
    "DrillCriterionSpec",
    "DrillReport",
    "DrillScenario",
    "PhaseSample",
    "ScenarioSet",
]

CONCLUSION_PASS = "通过"
"""结论：全部判据通过。"""

CONCLUSION_DEGRADED = "降级通过"
"""结论：无失败，但有 skipped / degraded 判据（退出 2，需人工复核）。"""

CONCLUSION_FAIL = "失败"
"""结论：任一判据失败或执行异常。"""

EXIT_CODES: ConcurrentStableFrozenDict[str, int] = ConcurrentStableFrozenDict(
    {CONCLUSION_PASS: 0, CONCLUSION_DEGRADED: 2, CONCLUSION_FAIL: 1}
)
"""结论 → 进程退出码（与 07-3-2 / 07-4-1 / 07-4-2 同口径）。"""

RESULT_OK = "ok"
"""判据结果：通过。"""

RESULT_DEGRADED = "degraded"
"""判据结果：无法完全确认（不判失败，但结论降级）。"""

RESULT_FAILED = "failed"
"""判据结果：失败。"""

RESULT_SKIPPED = "skipped"
"""判据结果：前置不满足未执行（结论降级）。"""

SEVERITY_HARD = "hard"
"""判据级别：硬判据（任一记录键非 `ok` 即场景不通过）。"""

SEVERITY_DEGRADABLE = "degradable"
"""判据级别：可降级判据（采不到样本时记降级，不判失败）。"""

SEVERITIES: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList((SEVERITY_HARD, SEVERITY_DEGRADABLE))
"""判据级别闭集。"""


@dataclass(frozen=True)
class DrillCriterionSpec:
    """一条判据的声明（`scenarios.toml` 的 `[[scenarios.criterion]]`）。"""

    key: str
    """判据标识（场景内唯一，小写下划线）。"""

    text: str
    """人读判据原文（`plan` 的「恢复判据」列即它）。"""

    record_keys: ConcurrentStableFrozenList[str]
    """该判据在记录 `criteria[]` 中的键（至少一条；双向对账的锚点）。"""

    severity: str
    """`hard` / `degradable`（闭集见 `SEVERITIES`）。"""

    evidence: str
    """证据要求（看哪段样本 / 哪个字段 / 哪份记录）。"""


@dataclass(frozen=True)
class DrillScenario:
    """一条场景登记（`deploy/drill/scenarios.toml` 的一行）。"""

    name: str
    """场景标识（小写下划线，与 `scenarios/` 模块名一致）。"""

    title: str
    """中文标题（渲染表用）。"""

    inject: str
    """注入方式标识（闭集见 `env.INJECT_METHODS`）。"""

    observe: ConcurrentStableFrozenList[str]
    """观测指标名（`msg_*`；缺失时记 0，不判失败）。"""

    criterion: ConcurrentStableFrozenList[DrillCriterionSpec]
    """判据声明（逐条可读；记录里按 `record_keys` 对账）。"""

    aux_record_keys: ConcurrentStableFrozenList[str]
    """注入前置类记录键（参与孤儿键检查，不构成通过判据）。"""

    recovery_budget_ms: int
    """恢复预算（毫秒；超出判失败）。"""

    expect: str
    """期望观察（记录与文档用）。"""

    target: str = "local"
    """执行目标（local = 本机隔离实例；docker = 多实例容器演练）。"""


@dataclass(frozen=True)
class ScenarioSet:
    """六类场景集合（场景单源的解析结果）。"""

    schema_version: int
    """格式版本（读到更高版本即拒）。"""

    updated_at: str
    """最近核对日期。"""

    owner: str
    """责任人。"""

    scenarios: ConcurrentStableFrozenList[DrillScenario]
    """场景列表（声明序即执行序）。"""


@dataclass(frozen=True)
class PhaseSample:
    """一段观测快照（注入前 / 注入中 / 恢复后）。"""

    phase: str
    """`before` / `during` / `after`。"""

    captured_at: str
    """采集时刻（UTC ISO8601）。"""

    readyz_status: int
    """`/readyz` HTTP 状态码（0 = 未取到）。"""

    readyz_ok: bool
    """`/readyz` 是否通过（状态码 200 且 `status=ok`）。"""

    healthz_ok: bool
    """`/healthz` 是否 200。"""

    metrics: ConcurrentStableFrozenDict[str, float]
    """指标子集（场景 `observe` 列出的键；未采集到记 0）。"""

    detail: str
    """明细（失败原因 / 探针正文摘要）。"""


@dataclass(frozen=True)
class Criterion:
    """一条判据的判定结果。"""

    key: str
    """判据键（稳定标识，07-6-2 按此组表）。"""

    expect: str
    """期望（场景单源原文）。"""

    actual: str
    """实测（可读摘要）。"""

    result: str
    """`ok` / `degraded` / `failed` / `skipped`。"""


@dataclass(frozen=True)
class DrillReport:
    """一次演练的完整报告（JSON / Markdown 的唯一输入）。"""

    scenario: str
    """场景标识。"""

    mode: str
    """`run` / `dry-run`。"""

    env: str
    """归一化环境。"""

    target: str
    """`local` / `docker`。"""

    started_at: datetime
    """开始时刻（UTC）。"""

    finished_at: datetime
    """结束时刻（UTC）。"""

    injection: ConcurrentStableFrozenDict[str, str]
    """注入方式 / 明细 / 产物。"""

    samples: ConcurrentStableFrozenList[PhaseSample]
    """三段样本。"""

    criteria: ConcurrentStableFrozenList[Criterion]
    """判据结果（声明序）。"""

    rto_ms: int
    """恢复耗时（注入开始 → 恢复判据通过，毫秒）。"""

    data_loss: bool
    """是否丢失已确认消息（唯一判 `true` 的硬事实）。"""

    deviations: ConcurrentStableFrozenList[str]
    """偏离项（隔离实例的替代实现；不隐瞒）。"""

    issues: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """执行期出现的问题与处置。"""

    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """留在本任务之外的遗留与归口。"""

    backup: ConcurrentStableFrozenDict[str, str] = field(default_factory=ConcurrentStableFrozenDict[str, str])
    """演练前备份证据：`mode=not_applicable|provided` + 理由（隔离实例不适用，非隔离必须给）。"""

    commands: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]] = field(
        default_factory=ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    )
    """动作轨迹（方法 + 路径 + 关键参数；不含头部 / 载荷 / 凭据）。"""

    artifacts: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]] = field(
        default_factory=ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]
    )
    """原始产物清单（相对工作根；文件给大小与 sha256）。"""

    logs: ConcurrentStableFrozenDict[str, object] = field(default_factory=ConcurrentStableFrozenDict[str, object])
    """实例日志摘要（总行数 / 字节 / sha256 + 尾部文本）。"""

    config_digest: str = ""
    """生效配置摘要（跨版本比较用）。"""

    rerun: ConcurrentStableFrozenDict[str, str] = field(default_factory=ConcurrentStableFrozenDict[str, str])
    """复跑标识（`attempt` / `previous_started_at` / `previous_conclusion` / `reason`）；空表示首跑。"""

    @property
    def conclusion(self) -> str:
        """按判据结果汇总结论。

        Returns:
            str: `通过` / `降级通过` / `失败`。
        """
        if any(item.result == RESULT_FAILED for item in self.criteria):
            return CONCLUSION_FAIL
        if any(item.result in (RESULT_DEGRADED, RESULT_SKIPPED) for item in self.criteria):
            return CONCLUSION_DEGRADED
        return CONCLUSION_PASS

    @property
    def exit_code(self) -> int:
        """结论对应的进程退出码。

        Returns:
            int: 0 / 2 / 1。
        """
        return EXIT_CODES[self.conclusion]

    @property
    def reasons(self) -> ConcurrentStableFrozenList[str]:
        """未通过判据的原因（结论为通过时为空）。

        Returns:
            ConcurrentStableFrozenList[str]: 原因列表。
        """
        if self.conclusion == CONCLUSION_PASS:
            return ConcurrentStableFrozenList()
        return ConcurrentStableFrozenList(
            f"{item.key}：{item.actual}" for item in self.criteria if item.result != RESULT_OK
        )
