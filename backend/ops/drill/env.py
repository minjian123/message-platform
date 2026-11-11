"""演练配置、生产防护与场景单源解析（任务 07-6-1）。

三件事只有一份实现：宿主侧键位解析（缺键 / 非法值立刻失败）、生产拒绝与二次确认、
`deploy/drill/scenarios.toml` 的解析与校验。判据与注入在 `ops.drill.scenarios.*`。
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
    ConcurrentStableSet,
)

from bms_msg.core.config import (
    DRILL_BASE_PORT,
    DRILL_BASE_PORT_ENV,
    DRILL_DEFAULT_ENV,
    DRILL_FILL_MB,
    DRILL_FILL_MB_ENV,
    DRILL_PROD_ENVS,
    DRILL_RECOVERY_BUDGET_MS,
    DRILL_REJECT_RATIO,
    DRILL_REJECT_RATIO_ENV,
    DRILL_REPORT_DIR,
    DRILL_REPORT_DIR_ENV,
    DRILL_SCENARIO_SCHEMA_VERSION,
    DRILL_SLOW_WRITE_MB,
    DRILL_SLOW_WRITE_MB_ENV,
    DRILL_WORK_DIR_ENV,
)
from ops.drill.model import SEVERITIES, SEVERITY_HARD, DrillCriterionSpec, DrillScenario, ScenarioSet

__all__ = [
    "INJECT_METHODS",
    "DrillConfig",
    "DrillError",
    "backup_evidence",
    "load_config",
    "load_scenarios",
    "normalize_env",
    "render_criteria_rows",
    "render_scenario_rows",
    "validate_scenarios",
]

INJECT_METHODS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(
    ("sigkill_sigterm", "fill_volume", "controlled_write", "tcp_proxy", "unacked_inflight", "invalid_and_exhausted")
)
"""注入方式闭集：六类场景各一，写错即场景非法（防止「场景名换了、注入没换」）。"""

_ISOLATED_ENVS: ConcurrentStableFrozenList[str] = ConcurrentStableFrozenList(("test", "dev", "local"))
"""视为隔离环境的标识（演练前备份记 `not_applicable`）。"""


class DrillError(RuntimeError):
    """演练失败（参数 / 前置 / 执行）：消息带 `drill.<类别>` 前缀，不含任何凭据值。"""


@dataclass(frozen=True)
class DrillConfig:
    """一次演练的生效配置（全部来自进程环境变量）。"""

    work_dir: Path
    """演练工作根（临时数据根 / 记录 / 填充文件都在其下）。"""

    env: str
    """归一化环境标识。"""

    base_port: int
    """隔离实例起始端口。"""

    report_dir: Path
    """记录落点。"""

    recovery_budget_ms: int
    """单场景恢复预算（毫秒）。"""

    fill_mb: int
    """磁盘满场景的受控填充量（MiB）。"""

    slow_write_mb: int
    """慢盘场景的受控写入量（MiB）。"""

    reject_ratio: float
    """磁盘满场景的拒绝水位（0 < 值 < 1）。"""

    repo_root: Path
    """仓库根（定位场景单源）。"""


def normalize_env(value: str) -> str:
    """归一化环境标识（去空白 + 小写）。

    Args:
        value: 原始环境标识。

    Returns:
        str: 归一化结果。
    """
    return value.strip().lower()


def _positive_int(raw: str | None, key: str, default: int) -> int:
    """解析正整数环境变量（空取缺省，非法即失败并指名键）。

    Args:
        raw: 环境变量原值。
        key: 键名（报错用）。
        default: 缺省值。

    Returns:
        int: 生效值。

    Raises:
        DrillError: 取值非法。
    """
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise DrillError(f"drill.config {key} 须为正整数（当前 {raw!r}）") from exc
    if value <= 0:
        raise DrillError(f"drill.config {key} 须为正整数（当前 {value}）")
    return value


def load_config(env: dict[str, str] | None = None, *, repo_root: Path | None = None) -> DrillConfig:
    """从进程环境变量解析配置（唯一配置入口）。

    Args:
        env: 环境变量映射（缺省取 `os.environ`；用例注入替身）。
        repo_root: 仓库根（缺省由脚本位置自定位）。

    Returns:
        DrillConfig: 生效配置。

    Raises:
        DrillError: 必填键缺失或数值非法。
    """
    source = env if env is not None else dict(__import__("os").environ)
    root = repo_root if repo_root is not None else Path(__file__).resolve().parents[3]
    raw_work = (source.get(DRILL_WORK_DIR_ENV) or "").strip()
    if not raw_work:
        raise DrillError(f"drill.config 缺少必填键 {DRILL_WORK_DIR_ENV}（演练工作根）")
    raw_ratio = (source.get(DRILL_REJECT_RATIO_ENV) or "").strip()
    ratio = DRILL_REJECT_RATIO
    if raw_ratio:
        try:
            ratio = float(raw_ratio)
        except ValueError as exc:
            raise DrillError(f"drill.config {DRILL_REJECT_RATIO_ENV} 须为 0~1 的小数（当前 {raw_ratio!r}）") from exc
        if not 0 < ratio < 1:
            raise DrillError(f"drill.config {DRILL_REJECT_RATIO_ENV} 须在 0~1 之间（当前 {ratio}）")
    return DrillConfig(
        work_dir=Path(raw_work).expanduser().resolve(),
        env=normalize_env(source.get("MSG_DRILL_ENV") or DRILL_DEFAULT_ENV),
        base_port=_positive_int(source.get(DRILL_BASE_PORT_ENV), DRILL_BASE_PORT_ENV, DRILL_BASE_PORT),
        report_dir=(root / (source.get(DRILL_REPORT_DIR_ENV) or DRILL_REPORT_DIR)).resolve(),
        recovery_budget_ms=_positive_int(
            source.get("MSG_DRILL_RECOVERY_BUDGET_MS"), "MSG_DRILL_RECOVERY_BUDGET_MS", DRILL_RECOVERY_BUDGET_MS
        ),
        fill_mb=_positive_int(source.get(DRILL_FILL_MB_ENV), DRILL_FILL_MB_ENV, DRILL_FILL_MB),
        slow_write_mb=_positive_int(source.get(DRILL_SLOW_WRITE_MB_ENV), DRILL_SLOW_WRITE_MB_ENV, DRILL_SLOW_WRITE_MB),
        reject_ratio=ratio,
        repo_root=root,
    )


def reject_production(env: str) -> None:
    """生产环境一律拒绝（无逃生开关）。

    Args:
        env: 归一化环境标识。

    Raises:
        DrillError: 环境为生产。
    """
    if env in DRILL_PROD_ENVS:
        raise DrillError(f"drill.guard 生产环境禁止注入故障（env={env}；生产演练走人工流程）")


def require_confirmation(scenario: str, confirm: str | None) -> None:
    """二次确认：`--confirm <场景名>` 必须与本次场景一致。

    Args:
        scenario: 本次场景标识。
        confirm: 命令行给出的确认值。

    Raises:
        DrillError: 未确认或确认值与场景不一致。
    """
    if (confirm or "").strip() != scenario:
        raise DrillError(f"drill.guard 缺少二次确认：请显式传 --confirm {scenario}")


def backup_evidence(config: DrillConfig) -> ConcurrentStableFrozenDict[str, str]:
    """演练前备份的证据口径：隔离实例记不适用，非隔离必须给证据。

    Args:
        config: 演练配置。

    Returns:
        ConcurrentStableFrozenDict[str, str]: 备份证据（`mode` / `reason`）。

    Raises:
        DrillError: 非隔离环境且未给备份证据。
    """
    evidence = (os.environ.get("MSG_DRILL_BACKUP_EVIDENCE") or "").strip()
    if config.env in _ISOLATED_ENVS:
        return ConcurrentStableFrozenDict(
            {
                "mode": "not_applicable",
                "reason": "隔离演练实例为临时数据根与 SQLite 元数据库，不含真实业务数据",
            }
        )
    if not evidence:
        raise DrillError(f"drill.guard 非隔离环境（{config.env}）必须给备份证据：MSG_DRILL_BACKUP_EVIDENCE")
    return ConcurrentStableFrozenDict({"mode": "provided", "reason": evidence})


def _frozen(value: object) -> ConcurrentStableFrozenList[str]:
    """把 TOML 字符串数组折成冻结列表（非数组记空，交给校验器报错）。

    Args:
        value: TOML 取值。

    Returns:
        ConcurrentStableFrozenList[str]: 字符串列表。
    """
    if not isinstance(value, list):
        return ConcurrentStableFrozenList()
    items: ConcurrentStableList[str] = ConcurrentStableList()
    for item in cast("list[object]", value):
        items.add(str(item))
    return ConcurrentStableFrozenList(items)


def _criterion_specs(
    value: object, where: str, errors: ConcurrentStableList[str]
) -> ConcurrentStableFrozenList[DrillCriterionSpec]:
    """把 TOML 判据数组解析成结构化条目（类型错误即登记，交给校验器一次列全）。

    Args:
        value: `criterion` 取值（schema 2 起为表数组）。
        where: 场景位置（报错用）。
        errors: 错误收集器。

    Returns:
        ConcurrentStableFrozenList[DrillCriterionSpec]: 判据声明（非法条目也保留，便于校验器逐项报）。
    """
    if not isinstance(value, list):
        errors.add(f"{where}：criterion 须为 [[scenarios.criterion]] 数组（schema 2 起结构化）")
        return ConcurrentStableFrozenList()
    items: ConcurrentStableList[DrillCriterionSpec] = ConcurrentStableList()
    for index, entry in enumerate(cast("list[object]", value)):
        if not isinstance(entry, dict):
            errors.add(f"{where}：criterion[{index}] 不是表")
            continue
        table = cast("dict[str, object]", entry)
        items.add(
            DrillCriterionSpec(
                key=str(table.get("key") or ""),
                text=str(table.get("text") or ""),
                record_keys=_frozen(table.get("record_keys")),
                severity=str(table.get("severity") or ""),
                evidence=str(table.get("evidence") or ""),
            )
        )
    return ConcurrentStableFrozenList(items)


def load_scenarios(path: Path) -> tuple[ScenarioSet, ConcurrentStableFrozenList[str]]:
    """读取并解析场景单源（解析错误也返回，便于一次列全）。

    Args:
        path: 场景文件路径。

    Returns:
        tuple: 场景集（解析失败时为空集）+ 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    empty = ScenarioSet(schema_version=0, updated_at="", owner="", scenarios=ConcurrentStableFrozenList())
    try:
        raw = cast("dict[str, object]", tomllib.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        errors.add(f"场景单源不存在：{path}")
        return empty, ConcurrentStableFrozenList(errors)
    except tomllib.TOMLDecodeError as exc:
        errors.add(f"场景单源解析失败：{path}（{exc}）")
        return empty, ConcurrentStableFrozenList(errors)

    items: ConcurrentStableList[DrillScenario] = ConcurrentStableList()
    raw_scenarios = raw.get("scenarios")
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        errors.add("场景单源缺少 [[scenarios]]（六类必须齐备）")
    else:
        for index, entry in enumerate(cast("list[object]", raw_scenarios)):
            if not isinstance(entry, dict):
                errors.add(f"scenarios[{index}] 不是表")
                continue
            table = cast("dict[str, object]", entry)
            name = str(table.get("name") or "")
            items.add(
                DrillScenario(
                    name=name,
                    title=str(table.get("title") or ""),
                    inject=str(table.get("inject") or ""),
                    observe=_frozen(table.get("observe")),
                    criterion=_criterion_specs(table.get("criterion"), name or f"scenarios[{index}]", errors),
                    aux_record_keys=_frozen(table.get("aux_record_keys")),
                    recovery_budget_ms=int(cast("int", table.get("recovery_budget_ms") or 0)),
                    expect=str(table.get("expect") or ""),
                )
            )
    raw_meta = cast("dict[str, object]", raw.get("meta")) if isinstance(raw.get("meta"), dict) else None
    scenario_set = ScenarioSet(
        schema_version=int(cast("int", raw.get("schema_version") or 0)),
        updated_at=str(raw_meta.get("updated_at") or "") if raw_meta else "",
        owner=str(raw_meta.get("owner") or "") if raw_meta else "",
        scenarios=ConcurrentStableFrozenList(items),
    )
    return scenario_set, ConcurrentStableFrozenList(errors)


def validate_scenarios(scenario_set: ScenarioSet, budget_ms: int) -> ConcurrentStableFrozenList[str]:
    """校验场景单源：六类齐备、注入方式闭集、观测与判据非空、预算合法。

    Args:
        scenario_set: 已解析场景集。
        budget_ms: 配置的恢复预算（场景未写时按它兜底）。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    if scenario_set.schema_version != DRILL_SCENARIO_SCHEMA_VERSION:
        errors.add(
            f"场景单源 schema_version 非法：{scenario_set.schema_version}（期望 {DRILL_SCENARIO_SCHEMA_VERSION}）"
        )
    if not scenario_set.updated_at:
        errors.add("场景单源缺少 meta.updated_at")
    if not scenario_set.owner:
        errors.add("场景单源缺少 meta.owner")
    seen: ConcurrentStableSet[str] = ConcurrentStableSet()
    methods: ConcurrentStableSet[str] = ConcurrentStableSet()
    for scenario in scenario_set.scenarios:
        where = scenario.name or "(缺少 name)"
        if not scenario.name:
            errors.add("存在缺少 name 的场景")
        elif scenario.name in seen:
            errors.add(f"场景 name 重复：{scenario.name}")
        else:
            seen.add(scenario.name)
        if not scenario.title:
            errors.add(f"{where}：title 为空")
        if scenario.inject not in INJECT_METHODS:
            errors.add(f"{where}：inject = {scenario.inject!r} 非法（可选 {' / '.join(INJECT_METHODS)}）")
        else:
            methods.add(scenario.inject)
        if not scenario.observe:
            errors.add(f"{where}：observe 为空（至少一个指标）")
        if not scenario.criterion:
            errors.add(f"{where}：criterion 为空（至少一条判据）")
        errors.extend(_validate_criteria(scenario, where))
        if not scenario.expect:
            errors.add(f"{where}：expect 为空（写期望观察）")
        if scenario.recovery_budget_ms <= 0:
            errors.add(f"{where}：recovery_budget_ms 须为正整数（当前 {scenario.recovery_budget_ms}）")
        elif scenario.recovery_budget_ms > budget_ms * 10:
            errors.add(
                f"{where}：recovery_budget_ms 超出配置预算一个量级（{scenario.recovery_budget_ms} > {budget_ms}×10）"
            )
    for method in INJECT_METHODS:
        if method not in methods:
            errors.add(f"缺少注入方式 {method!r}（六类必须齐备）")
    return ConcurrentStableFrozenList(errors)


def _validate_criteria(scenario: DrillScenario, where: str) -> ConcurrentStableFrozenList[str]:
    """校验单个场景的判据声明（键唯一、级别闭集、记录键非空且不与前置键相交）。

    Args:
        scenario: 场景定义。
        where: 场景位置（报错用）。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    keys: ConcurrentStableSet[str] = ConcurrentStableSet()
    record_keys: ConcurrentStableSet[str] = ConcurrentStableSet()
    for index, spec in enumerate(scenario.criterion, start=1):
        label = f"{where}：criterion[{index}]"
        if not spec.key:
            errors.add(f"{label} 缺少 key")
        elif spec.key in keys:
            errors.add(f"{label} key 重复：{spec.key}")
        else:
            keys.add(spec.key)
        if not spec.text:
            errors.add(f"{label}（{spec.key or '?'}）缺少 text")
        if spec.severity not in SEVERITIES:
            allowed = " / ".join(SEVERITIES)
            errors.add(f"{label}（{spec.key or '?'}）severity = {spec.severity!r} 非法（可选 {allowed}）")
        if not spec.record_keys:
            errors.add(f"{label}（{spec.key or '?'}）record_keys 为空")
        for key in spec.record_keys:
            if key in record_keys:
                errors.add(f"{label}（{spec.key or '?'}）record_keys 重复：{key}")
            else:
                record_keys.add(key)
        if not spec.evidence:
            errors.add(f"{label}（{spec.key or '?'}）evidence 为空")
    for key in scenario.aux_record_keys:
        if key in record_keys:
            errors.add(f"{where}：aux_record_keys 与判据记录键重复：{key}")
    return ConcurrentStableFrozenList(errors)


def render_scenario_rows(scenario_set: ScenarioSet) -> ConcurrentStableFrozenList[str]:
    """把场景集渲染成 Markdown 表行（设计文档的表由此粘贴，用例断言两处一致）。

    Args:
        scenario_set: 已校验场景集。

    Returns:
        ConcurrentStableFrozenList[str]: 表头 + 分隔行 + 每场景一行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("| 场景 | 注入方式 | 观测指标 | 恢复判据 |")
    rows.add("| --- | --- | --- | --- |")
    for scenario in scenario_set.scenarios:
        observe = "、".join(f"`{item}`" for item in scenario.observe)
        criterion = "；".join(spec.text for spec in scenario.criterion)
        rows.add(f"| `{scenario.name}` | `{scenario.inject}` | {observe} | {criterion} |")
    return ConcurrentStableFrozenList(rows)


def render_criteria_rows(scenario_set: ScenarioSet) -> ConcurrentStableFrozenList[str]:
    """把判据声明渲染成 Markdown 表行（详细设计 §5 由此粘贴，用例断言两处一致）。

    Args:
        scenario_set: 已校验场景集。

    Returns:
        ConcurrentStableFrozenList[str]: 表头 + 分隔行 + 每条判据一行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("| 场景 | # | 判据 | 级别 | 记录键 | 证据要求 |")
    rows.add("| --- | --- | --- | --- | --- | --- |")
    for scenario in scenario_set.scenarios:
        for index, spec in enumerate(scenario.criterion, start=1):
            level = "硬" if spec.severity == SEVERITY_HARD else "可降级"
            keys = "、".join(f"`{key}`" for key in spec.record_keys)
            rows.add(f"| `{scenario.name}` | {index} | {spec.text} | {level} | {keys} | {spec.evidence} |")
    return ConcurrentStableFrozenList(rows)
