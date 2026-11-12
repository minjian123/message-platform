"""bms 迁移编排（任务 07-4-1）：前置清单闸门 + 切换矩阵 + 开关契约 + 隔离环境演练与记录。

用法::

    python -m ops.migration check --env prod          # 前置清单 + 生产闸门 + 矩阵校验
    python -m ops.migration check --env test          # 隔离环境：阻断项只记录、不阻断
    python -m ops.migration matrix                    # 渲染切换矩阵（文档表由它粘贴）
    python -m ops.migration drill --msg-url http://msg:8000   # 切到 msg 的隔离环境演练

**为什么是仓内命令而不是只写用例**：装换前要能回答三个问题——「现在能不能迁」「迁哪个域、看什么指标」
「配错了会怎样」。前两个是清单与矩阵（运维可改、代码不发布），第三个是演练（判据必须只有一份）。
用例与命令共用同一份 `run_drill()`，避免「用例一套判据、脚本另一套」。

设计取舍：

1. **前置状态是事实、不是探测**：8B_05 是否交付只能由人 + 计划表确认；代码符号探测会在「有骨架没能力」
   时误放行。清单文件因此是唯一状态源，闸门只读它。
2. **非法配置不静默回落**：未配置 = 原实现是缺省语义；非法值 / `msg` 配置不全 = 装配期拒启。静默回落会
   把「以为切了 msg、实际还在 http」的假切换带进观察窗口。
3. **演练与检查分离**：`check` 不跑流量；`drill` 只允许隔离环境（`prod` 一律拒绝，无覆盖开关）。
4. **记录先于结论**：失败也写 JSON + Markdown，失败步与原因进记录，不出现「失败了没有产物」。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import inspect
import json
import platform as platform_module
import re
import sys
import time
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import httpx
from bms_core.catalog.base import fetch_catalog_snapshot
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableFrozenSet,
    ConcurrentStableList,
    ConcurrentStableSet,
)
from bms_core.core.config import Settings
from bms_core.core.exceptions import ConfigError
from bms_core.core.plugin import PluginError, resolve_plugin
from bms_core.core.resources import ResourceManager
from bms_core.events.base import EventEnvelope, EventPublisher, EventPublishError
from bms_core.events.topic import encode_event, event_attributes, topic_for
from bms_core.msgclient.adapter.event_publisher import MsgEventPublisher
from bms_core.msgclient.adapter.policy import default_circuit
from bms_core.msgclient.adapter.service_client import MsgServiceClient
from bms_core.msgclient.rpc import RpcClient, StaticInstanceSelector
from bms_core.msgclient.transport import RpcTransport
from bms_core.servicecall.base import BaseServiceClient
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
    MIGRATION_OBSERVE_WINDOW_MINUTES,
    MIGRATION_PRECONDITIONS_PATH,
    MIGRATION_RECORD_SCHEMA_VERSION,
    MIGRATION_REPORT_DIR,
)
from bms_msg.metrics.names import COUNTER_NAMES, GAUGE_NAMES, HISTOGRAM_NAMES

__all__ = [
    "CONCLUSION_DEGRADED",
    "CONCLUSION_FAIL",
    "CONCLUSION_PASS",
    "DrillReport",
    "DrillTargets",
    "GateResult",
    "HttpTargets",
    "MatrixDomain",
    "Precondition",
    "SwitchMatrix",
    "build_http_targets",
    "build_parser",
    "build_targets",
    "config_fingerprint",
    "digest_bytes",
    "digest_json",
    "event_chain_errors",
    "load_matrix",
    "load_preconditions",
    "main",
    "resolve_switch",
    "run_check",
    "run_drill",
    "validate_matrix",
    "validate_preconditions",
]

_REPO_ROOT = Path(__file__).resolve().parents[2]
"""仓根（`backend/ops/migration.py` 上溯两级）。"""

_DATA_PLANE_PREFIX = "/api/v1/msg"
"""数据面 / 管理面统一前缀（内部路径口径）。"""

_STATUS_SATISFIED = "satisfied"
"""前置状态：已满足。"""

_STATUS_NOT_READY = "not_ready"
"""前置状态：未满足（生产闸门阻断）。"""

_PRECONDITION_STATUSES = (_STATUS_SATISFIED, _STATUS_NOT_READY)
"""前置状态闭集（其它取值一律判非法）。"""

_ENV_PROD = "prod"
"""归一化后的生产环境标识。"""

_ENV_TEST = "test"
"""归一化后的测试环境标识。"""

_ENV_DEV = "dev"
"""归一化后的开发环境标识。"""

_ENV_ALIASES: ConcurrentStableFrozenDict[str, str] = ConcurrentStableFrozenDict(
    {
        "prod": _ENV_PROD,
        "production": _ENV_PROD,
        "test": _ENV_TEST,
        "testing": _ENV_TEST,
        "dev": _ENV_DEV,
        "development": _ENV_DEV,
    }
)
"""环境别名 → 归一化标识（未登记的取值按参数错误处理，不猜语义）。"""

_MSG_PROVIDER = "msg"
"""切到消息平台的实现名（与 bms `assembly.MSG_PROVIDER_NAME` 同值）。"""

_DEFAULT_ORIGINALS: ConcurrentStableFrozenDict[str, str] = ConcurrentStableFrozenDict(
    {"event": "rocketmq", "service_client": "http"}
)
"""两类端口的原实现名（矩阵默认值文案的口径来源）。"""

_ORIGINAL_CATEGORIES: ConcurrentStableFrozenDict[str, ConcurrentStableFrozenList[str]] = ConcurrentStableFrozenDict(
    {
        "event": ConcurrentStableFrozenList(["low_risk", "audit", "business"]),
        "service_client": ConcurrentStableFrozenList(["readonly", "write"]),
    }
)
"""端口允许的域类别（事件端口不得声明只读调用类别，反之亦然）。"""

_DOMAIN_KEY_RE = re.compile(r"^\[(event|service_client)\]\.domains\.[a-z][a-z0-9_]*$")
"""域级键形（8B_08 待交付；这里只校验形态，不假装已生效）。"""

_SWITCH_KEY_RE = re.compile(r"^\[(event|service_client)\]\.provider$")
"""全局开关键形。"""

_READ_ONLY_STATUSES = ("pending_8b08",)
"""域级键状态闭集（交付后改 active 时须同步改校验器与用例）。"""

_MATRIX_PORTS = ("event", "service_client")
"""两类端口（矩阵必须都覆盖）。"""

_MATRIX_CATEGORIES = ("low_risk", "audit", "business", "readonly", "write")
"""域类别闭集。"""

_METRIC_NAMES: ConcurrentStableFrozenSet[str] = ConcurrentStableFrozenSet(
    tuple(COUNTER_NAMES) + tuple(GAUGE_NAMES) + tuple(HISTOGRAM_NAMES)
)
"""已登记指标名（矩阵不得引用未登记指标）。"""

RESULT_OK = "ok"
"""步骤结果：正常。"""

RESULT_DEGRADED = "degraded"
"""步骤结果：有已登记降级项（如 fallback 未提供）。"""

RESULT_FAILED = "failed"
"""步骤结果：硬失败。"""

RESULT_SKIPPED = "skipped"
"""步骤结果：当前环境不支持该步（如网络演练无法注入失败）。"""

CONCLUSION_PASS = "通过"
"""结论：无硬失败、无降级项。"""

CONCLUSION_DEGRADED = "降级通过"
"""结论：无硬失败，但存在降级 / 跳过项，需人工确认。"""

CONCLUSION_FAIL = "失败"
"""结论：任一硬失败。"""

EXIT_CODES: ConcurrentStableFrozenDict[str, int] = ConcurrentStableFrozenDict(
    {CONCLUSION_PASS: 0, CONCLUSION_DEGRADED: 2, CONCLUSION_FAIL: 1}
)
"""结论 → 退出码（三态可被自动化区分）。"""

_CHECK_OK = 0
"""`check` 通过退出码。"""

_CHECK_FAIL = 1
"""`check` 失败（清单 / 矩阵 / 生产闸门）退出码。"""

_CHECK_USAGE = 2
"""`check` 参数错误（未登记环境 / 文件缺失以外的用法问题）退出码。"""


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


def digest_bytes(data: bytes) -> str:
    """计算字节摘要（16 位十六进制，够对账且不占记录体积）。

    Args:
        data: 原始字节。

    Returns:
        str: sha256 前 16 位。
    """
    return hashlib.sha256(data).hexdigest()[:16]


def digest_json(value: object) -> str:
    """计算 JSON 规范化后的摘要（键排序，保证同值同摘要）。

    Args:
        value: 任意可 JSON 序列化的值。

    Returns:
        str: sha256 前 16 位。
    """
    return digest_bytes(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"))


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


def _string_list(raw: object) -> ConcurrentStableFrozenList[str]:
    """把 TOML 数组折成不可变字符串列表（非法元素原样保留，交给校验器报错）。

    Args:
        raw: TOML 取值（应为数组）。

    Returns:
        ConcurrentStableFrozenList[str]: 字符串列表（非数组时为空）。
    """
    if not isinstance(raw, list):
        return ConcurrentStableFrozenList()
    return ConcurrentStableFrozenList([str(item) for item in cast("list[object]", raw)])


def normalize_env(raw: str) -> str:
    """归一化环境名。

    Args:
        raw: 命令行 / 环境变量里的环境名。

    Returns:
        str: `prod` / `test` / `dev`。

    Raises:
        ValueError: 未登记的环境名（不猜语义）。
    """
    key = raw.strip().lower()
    normalized = _ENV_ALIASES.get(key)
    if normalized is None:
        allowed = " / ".join(sorted(_ENV_ALIASES))
        raise ValueError(f"未登记的环境名：{raw!r}（可选 {allowed}）")
    return normalized


# ---------------------------------------------------------------------------
# 前置清单
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Precondition:
    """一条迁移前置。"""

    id: str
    """前置标识。"""

    label: str
    """中文名。"""

    status: str
    """`satisfied` / `not_ready`。"""

    required_in: ConcurrentStableFrozenList[str]
    """必须满足的环境集合（`prod` 为硬闸门）。"""

    evidence: str
    """证据锚点（文档 / 任务 / 演练记录）。"""

    note: str
    """备注（为什么阻断 / 交接口径）。"""


@dataclass(frozen=True)
class PreconditionSet:
    """前置清单（一份文件 = 一份状态）。"""

    schema_version: int
    """清单格式版本。"""

    updated_at: str
    """最近核对时间（人工填写）。"""

    owner: str
    """清单责任人。"""

    items: ConcurrentStableFrozenList[Precondition]
    """全部前置（声明序即展示序）。"""


@dataclass(frozen=True)
class GateResult:
    """生产闸门结果。"""

    env: str
    """归一化环境。"""

    allowed: bool
    """是否允许进入该环境的迁移。"""

    blocked_by: ConcurrentStableFrozenList[str]
    """阻断项 id（按清单顺序）。"""

    detail: ConcurrentStableFrozenList[str]
    """阻断项明细（`id：label（evidence）`）。"""


def load_preconditions(path: Path) -> tuple[PreconditionSet, ConcurrentStableFrozenList[str]]:
    """读取并解析前置清单（解析错误也返回，便于一次列全）。

    Args:
        path: 清单文件。

    Returns:
        tuple: 清单对象（解析失败时为空清单）+ 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    try:
        raw = cast("dict[str, object]", tomllib.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        errors.add(f"前置清单不存在：{path}")
        return _empty_preconditions(), ConcurrentStableFrozenList(errors)
    except tomllib.TOMLDecodeError as exc:
        errors.add(f"前置清单解析失败：{path}（{exc}）")
        return _empty_preconditions(), ConcurrentStableFrozenList(errors)

    items: ConcurrentStableList[Precondition] = ConcurrentStableList()
    raw_items = raw.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        errors.add("前置清单缺少 [[items]]（至少登记一条）")
    else:
        for index, raw_item in enumerate(cast("list[object]", raw_items)):
            item = _table(raw_item)
            if not item:
                errors.add(f"items[{index}] 不是表")
                continue
            items.add(
                Precondition(
                    id=str(item.get("id") or ""),
                    label=str(item.get("label") or ""),
                    status=str(item.get("status") or ""),
                    required_in=_string_list(item.get("required_in")),
                    evidence=str(item.get("evidence") or ""),
                    note=str(item.get("note") or ""),
                )
            )
    meta = _table(raw.get("meta"))
    preconditions = PreconditionSet(
        schema_version=int(cast("int", raw.get("schema_version") or 0)),
        updated_at=str(meta.get("updated_at") or ""),
        owner=str(meta.get("owner") or ""),
        items=ConcurrentStableFrozenList(items),
    )
    return preconditions, ConcurrentStableFrozenList(errors)


def _empty_preconditions() -> PreconditionSet:
    """构造空前置清单（解析失败时的占位；不冒充「全绿」）。

    Returns:
        PreconditionSet: 空清单。
    """
    return PreconditionSet(
        schema_version=0,
        updated_at="",
        owner="",
        items=ConcurrentStableFrozenList(),
    )


def validate_preconditions(preconditions: PreconditionSet) -> ConcurrentStableFrozenList[str]:
    """校验前置清单字段与状态闭集。

    Args:
        preconditions: 已解析清单。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    if preconditions.schema_version != 1:
        errors.add(f"清单 schema_version 非法：{preconditions.schema_version!r}（期望 1）")
    if not preconditions.items:
        errors.add("清单为空：至少登记一条前置")
    seen: ConcurrentStableSet[str] = ConcurrentStableSet()
    for item in preconditions.items:
        where = item.id or "(缺少 id)"
        if not item.id:
            errors.add("存在缺少 id 的前置条目")
        elif item.id in seen:
            errors.add(f"前置 id 重复：{item.id}")
        else:
            seen.add(item.id)
        if not item.label:
            errors.add(f"{where}：缺少 label")
        if item.status not in _PRECONDITION_STATUSES:
            allowed = " / ".join(_PRECONDITION_STATUSES)
            errors.add(f"{where}：status = {item.status!r} 非法（可选 {allowed}）")
        if not item.required_in:
            errors.add(f"{where}：required_in 为空（至少写 prod / test / dev 之一）")
        for env in item.required_in:
            if env not in (_ENV_PROD, _ENV_TEST, _ENV_DEV):
                errors.add(f"{where}：required_in 含未登记环境 {env!r}")
        if not item.evidence:
            errors.add(f"{where}：evidence 为空（空证据等于没核对）")
    return ConcurrentStableFrozenList(errors)


def evaluate_gate(preconditions: PreconditionSet, env: str) -> GateResult:
    """按环境判定迁移闸门。

    Args:
        preconditions: 已校验清单。
        env: 归一化环境（`prod` / `test` / `dev`）。

    Returns:
        GateResult: 闸门结果（非生产环境只记录阻断项，不判失败）。
    """
    blocked: ConcurrentStableList[str] = ConcurrentStableList()
    detail: ConcurrentStableList[str] = ConcurrentStableList()
    env_blockers = 0
    for item in preconditions.items:
        unsatisfied = item.status != _STATUS_SATISFIED
        if not unsatisfied:
            continue
        if _ENV_PROD in item.required_in or env in item.required_in:
            blocked.add(item.id)
            detail.add(f"{item.id}：{item.label}（{item.evidence}）")
        if env in item.required_in:
            env_blockers += 1
    allowed = env_blockers == 0
    return GateResult(
        env=env,
        allowed=allowed,
        blocked_by=ConcurrentStableFrozenList(blocked),
        detail=ConcurrentStableFrozenList(detail),
    )


# ---------------------------------------------------------------------------
# 切换矩阵
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatrixPhase:
    """一个迁移批次。"""

    id: int
    """批次号（1~3）。"""

    name: str
    """中文名。"""

    scope: str
    """覆盖范围。"""

    gate: str
    """进入该批次的条件。"""


@dataclass(frozen=True)
class MatrixDomain:
    """一个可切换域。"""

    key: str
    """域标识。"""

    label: str
    """中文名。"""

    port: str
    """`event` / `service_client`。"""

    category: str
    """`low_risk` / `audit` / `business` / `readonly` / `write`。"""

    phase: int
    """批次号。"""

    switch_key: str
    """当前生效的开关键（全局 `provider`）。"""

    domain_level_key: str
    """域级键（8B_08 待交付）。"""

    domain_level_status: str
    """域级键状态（当前只允许 `pending_8b08`）。"""

    default_value: str
    """缺省值 = 原实现。"""

    target_value: str
    """切换目标（只允许 `msg`）。"""

    prerequisites: ConcurrentStableFrozenList[str]
    """依赖的前置清单 id。"""

    observe_metrics: ConcurrentStableFrozenList[str]
    """观察指标名（必须已登记）。"""

    rollback_criteria: ConcurrentStableFrozenList[str]
    """回滚判据（形状；阈值数值归 8B_12）。"""

    owner: str
    """责任人。"""

    note: str
    """备注。"""


@dataclass(frozen=True)
class SwitchMatrix:
    """切换矩阵（TOML 单源）。"""

    schema_version: int
    """矩阵格式版本。"""

    updated_at: str
    """最近更新时间。"""

    owner: str
    """矩阵责任人。"""

    observe_window_minutes: int
    """观察窗口缺省时长（分钟）。"""

    phases: ConcurrentStableFrozenList[MatrixPhase]
    """批次定义。"""

    domains: ConcurrentStableFrozenList[MatrixDomain]
    """域定义。"""


def _empty_matrix() -> SwitchMatrix:
    """构造空矩阵（解析失败时的占位）。

    Returns:
        SwitchMatrix: 空矩阵。
    """
    return SwitchMatrix(
        schema_version=0,
        updated_at="",
        owner="",
        observe_window_minutes=MIGRATION_OBSERVE_WINDOW_MINUTES,
        phases=ConcurrentStableFrozenList(),
        domains=ConcurrentStableFrozenList(),
    )


def load_matrix(path: Path) -> tuple[SwitchMatrix, ConcurrentStableFrozenList[str]]:
    """读取并解析切换矩阵。

    Args:
        path: 矩阵文件。

    Returns:
        tuple: 矩阵对象（解析失败时为空矩阵）+ 错误列表。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    try:
        raw = cast("dict[str, object]", tomllib.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        errors.add(f"切换矩阵不存在：{path}")
        return _empty_matrix(), ConcurrentStableFrozenList(errors)
    except tomllib.TOMLDecodeError as exc:
        errors.add(f"切换矩阵解析失败：{path}（{exc}）")
        return _empty_matrix(), ConcurrentStableFrozenList(errors)

    phases: ConcurrentStableList[MatrixPhase] = ConcurrentStableList()
    raw_phases = raw.get("phases")
    for index, raw_phase in enumerate(cast("list[object]", raw_phases) if isinstance(raw_phases, list) else []):
        item = _table(raw_phase)
        if not item:
            errors.add(f"phases[{index}] 不是表")
            continue
        phases.add(
            MatrixPhase(
                id=int(cast("int", item.get("id") or 0)),
                name=str(item.get("name") or ""),
                scope=str(item.get("scope") or ""),
                gate=str(item.get("gate") or ""),
            )
        )
    domains: ConcurrentStableList[MatrixDomain] = ConcurrentStableList()
    raw_domains = raw.get("domains")
    if not isinstance(raw_domains, list) or not raw_domains:
        errors.add("矩阵缺少 [[domains]]（至少覆盖两类端口）")
    else:
        for index, raw_domain in enumerate(cast("list[object]", raw_domains)):
            item = _table(raw_domain)
            if not item:
                errors.add(f"domains[{index}] 不是表")
                continue
            domains.add(
                MatrixDomain(
                    key=str(item.get("key") or ""),
                    label=str(item.get("label") or ""),
                    port=str(item.get("port") or ""),
                    category=str(item.get("category") or ""),
                    phase=int(cast("int", item.get("phase") or 0)),
                    switch_key=str(item.get("switch_key") or ""),
                    domain_level_key=str(item.get("domain_level_key") or ""),
                    domain_level_status=str(item.get("domain_level_status") or ""),
                    default_value=str(item.get("default_value") or ""),
                    target_value=str(item.get("target_value") or ""),
                    prerequisites=_string_list(item.get("prerequisites")),
                    observe_metrics=_string_list(item.get("observe_metrics")),
                    rollback_criteria=_string_list(item.get("rollback_criteria")),
                    owner=str(item.get("owner") or ""),
                    note=str(item.get("note") or ""),
                )
            )
    meta = _table(raw.get("meta"))
    matrix = SwitchMatrix(
        schema_version=int(cast("int", raw.get("schema_version") or 0)),
        updated_at=str(meta.get("updated_at") or ""),
        owner=str(meta.get("owner") or ""),
        observe_window_minutes=int(cast("int", meta.get("observe_window_minutes") or MIGRATION_OBSERVE_WINDOW_MINUTES)),
        phases=ConcurrentStableFrozenList(phases),
        domains=ConcurrentStableFrozenList(domains),
    )
    return matrix, ConcurrentStableFrozenList(errors)


def validate_matrix(
    matrix: SwitchMatrix, *, known_preconditions: ConcurrentStableFrozenList[str] | None = None
) -> ConcurrentStableFrozenList[str]:
    """校验矩阵结构、字段与闭集（不判断域级键是否已实现）。

    Args:
        matrix: 已解析矩阵。
        known_preconditions: 前置清单 id 集合（提供时交叉核对 prerequisites）。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    if matrix.schema_version != 1:
        errors.add(f"矩阵 schema_version 非法：{matrix.schema_version!r}（期望 1）")
    if matrix.observe_window_minutes <= 0:
        errors.add(f"观察窗口非法：{matrix.observe_window_minutes}（须为正整数分钟）")
    phase_ids = ConcurrentStableSet(item.id for item in matrix.phases)
    if not phase_ids:
        errors.add("矩阵缺少 [[phases]]（批次定义）")
    if not matrix.domains:
        errors.add("矩阵没有域")
    seen: ConcurrentStableSet[str] = ConcurrentStableSet()
    ports: ConcurrentStableSet[str] = ConcurrentStableSet()
    phase_one_ports: ConcurrentStableSet[str] = ConcurrentStableSet()
    for domain in matrix.domains:
        where = domain.key or "(缺少 key)"
        if not domain.key:
            errors.add("存在缺少 key 的域")
        elif domain.key in seen:
            errors.add(f"域 key 重复：{domain.key}")
        else:
            seen.add(domain.key)
        if domain.port not in _MATRIX_PORTS:
            errors.add(f"{where}：port = {domain.port!r} 非法（可选 {' / '.join(_MATRIX_PORTS)}）")
        else:
            ports.add(domain.port)
        if domain.category not in _MATRIX_CATEGORIES:
            errors.add(f"{where}：category = {domain.category!r} 非法")
        else:
            allowed_categories = _ORIGINAL_CATEGORIES.get(domain.port)
            if allowed_categories is not None and domain.category not in allowed_categories:
                errors.add(
                    f"{where}：category = {domain.category!r} 与 port = {domain.port!r} 不匹配"
                    f"（允许 {' / '.join(allowed_categories)}）"
                )
        if domain.phase not in phase_ids:
            errors.add(f"{where}：phase = {domain.phase} 未在 [[phases]] 中声明")
        if domain.phase == 1 and domain.port in _MATRIX_PORTS:
            phase_one_ports.add(domain.port)
        switch_match = _SWITCH_KEY_RE.match(domain.switch_key)
        if switch_match is None:
            errors.add(f"{where}：switch_key = {domain.switch_key!r} 非法（须为 [端口].provider）")
        elif switch_match.group(1) != domain.port:
            errors.add(f"{where}：switch_key 与 port 不一致（{domain.switch_key!r} / {domain.port!r}）")
        domain_match = _DOMAIN_KEY_RE.match(domain.domain_level_key)
        if domain_match is None:
            errors.add(f"{where}：domain_level_key = {domain.domain_level_key!r} 非法（域段须小写下划线）")
        elif domain_match.group(1) != domain.port:
            errors.add(f"{where}：domain_level_key 与 port 不一致")
        if domain.domain_level_status not in _READ_ONLY_STATUSES:
            errors.add(f"{where}：domain_level_status = {domain.domain_level_status!r} 非法（当前只允许 pending_8b08）")
        if not domain.default_value:
            errors.add(f"{where}：default_value 为空（须写原实现）")
        else:
            original = _DEFAULT_ORIGINALS.get(domain.port)
            if original is not None and not domain.default_value.strip().startswith(original):
                errors.add(f"{where}：default_value 须以原实现 {original!r} 开头（当前 {domain.default_value!r}）")
        if domain.target_value != _MSG_PROVIDER:
            errors.add(f"{where}：target_value = {domain.target_value!r} 非法（只允许 msg）")
        if not domain.prerequisites:
            errors.add(f"{where}：prerequisites 为空")
        elif known_preconditions is not None:
            for prerequisite in domain.prerequisites:
                if prerequisite not in known_preconditions:
                    errors.add(f"{where}：prerequisites 引用了未登记前置 {prerequisite!r}")
        if not domain.observe_metrics:
            errors.add(f"{where}：observe_metrics 为空")
        for metric in domain.observe_metrics:
            if metric not in _METRIC_NAMES:
                errors.add(f"{where}：指标未登记：{metric!r}")
        if not domain.rollback_criteria:
            errors.add(f"{where}：rollback_criteria 为空")
        for criterion in domain.rollback_criteria:
            if not criterion.strip():
                errors.add(f"{where}：rollback_criteria 含空判据")
        if not domain.owner:
            errors.add(f"{where}：owner 为空")
    for port in _MATRIX_PORTS:
        if port not in ports:
            errors.add(f"矩阵未覆盖端口 {port!r}")
        if port not in phase_one_ports:
            errors.add(f"批次 1 未包含端口 {port!r}（首批必须同时有事件与只读调用）")
    return ConcurrentStableFrozenList(errors)


def render_matrix_rows(matrix: SwitchMatrix) -> ConcurrentStableFrozenList[str]:
    """把矩阵渲染成 Markdown 表行（设计文档的表由此粘贴，用例断言两处一致）。

    Args:
        matrix: 已校验矩阵。

    Returns:
        ConcurrentStableFrozenList[str]: 表头 + 分隔行 + 每个域一行。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.add("| 域键 | 端口 | 类别 | 批次 | 开关键 | 域级键（8B_08） | 默认值 | 观察指标 | 回滚判据 | 责任人 |")
    rows.add("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for domain in matrix.domains:
        metrics = " / ".join(f"`{item}`" for item in domain.observe_metrics)
        criteria = "；".join(domain.rollback_criteria)
        rows.add(
            f"| `{domain.key}` | `{domain.port}` | `{domain.category}` | {domain.phase} | `{domain.switch_key}` "
            f"| `{domain.domain_level_key}`（`{domain.domain_level_status}`） | {domain.default_value} "
            f"| {metrics} | {criteria} | {domain.owner} |"
        )
    return ConcurrentStableFrozenList(rows)


# ---------------------------------------------------------------------------
# 开关契约
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SwitchDecision:
    """一个端口的切换判定。"""

    plugin_key: str
    """`event` / `service_client`。"""

    configured: str
    """配置值（空串表示未配置）。"""

    implementation: str
    """解析出的实现名（失败时为空）。"""

    mode: str
    """`msg` / `original` / `invalid`。"""

    error_kind: str
    """空串 / `plugin`（未注册）/ `config`（配置不全）。"""

    error: str
    """错误文案（空串表示解析成功）。"""


@dataclass(frozen=True)
class SwitchReport:
    """两个端口的切换判定。"""

    event: SwitchDecision
    """事件发布端口。"""

    service_client: SwitchDecision
    """服务调用端口。"""

    fingerprint: str
    """配置指纹（切换点对账用）。"""


def _resolve_one(plugin_key: str, configured: str, expected_version: str) -> SwitchDecision:
    """解析一个端口的实现并分类（不吞异常，把异常折成判定结果）。

    Args:
        plugin_key: 能力域键。
        configured: 配置值。
        expected_version: 端口契约版本。

    Returns:
        SwitchDecision: 判定结果。
    """
    try:
        implementation = resolve_plugin(plugin_key, configured, expected_version=expected_version)
    except PluginError as exc:
        return SwitchDecision(
            plugin_key=plugin_key,
            configured=configured,
            implementation="",
            mode="invalid",
            error_kind="plugin",
            error=str(exc),
        )
    except ConfigError as exc:
        return SwitchDecision(
            plugin_key=plugin_key,
            configured=configured,
            implementation="",
            mode="invalid",
            error_kind="config",
            error=str(exc),
        )
    name = str(getattr(implementation, "plugin_name", "") or type(implementation).__name__)
    mode = _MSG_PROVIDER if name == _MSG_PROVIDER else "original"
    return SwitchDecision(
        plugin_key=plugin_key,
        configured=configured,
        implementation=name,
        mode=mode,
        error_kind="",
        error="",
    )


def resolve_switch(settings: Settings) -> SwitchReport:
    """按配置解析两个端口的实现（装配期口径）。

    Args:
        settings: bms 应用配置。

    Returns:
        SwitchReport: 两个端口的判定 + 配置指纹。
    """
    event = _resolve_one("event", str(settings.event.provider or ""), EventPublisher.contract_version)
    client = _resolve_one(
        "service_client", str(settings.service_client.provider or ""), BaseServiceClient.contract_version
    )
    return SwitchReport(event=event, service_client=client, fingerprint=config_fingerprint(settings))


def config_fingerprint(settings: Settings) -> str:
    """计算配置指纹（切换点对账；不含敏感值）。

    Args:
        settings: bms 应用配置。

    Returns:
        str: sha256 前 16 位。
    """
    instances = settings.service_client.instances.get(_MSG_PROVIDER)
    payload = {
        "event.provider": str(settings.event.provider or ""),
        "event.namespace": str(settings.event.options.get("namespace") or settings.app.service or ""),
        "service_client.provider": str(settings.service_client.provider or ""),
        "service_client.base_url_template": str(settings.service_client.options.get("base_url_template") or ""),
        "service_client.endpoints.msg": str(settings.service_client.endpoints.get(_MSG_PROVIDER) or ""),
        "service_client.instances.msg": list(instances) if instances is not None else [],
        "service_client.instance_strategy": str(settings.service_client.options.get("instance_strategy") or ""),
    }
    return digest_json(payload)


def validate_switch(report: SwitchReport) -> ConcurrentStableFrozenList[str]:
    """校验两个端口都已切到 msg 且无解析错误（演练前提）。

    Args:
        report: 切换判定。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示可演练）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    for decision in (report.event, report.service_client):
        if decision.error:
            errors.add(f"{decision.plugin_key}：{decision.error_kind or '解析'}失败 —— {decision.error}")
        elif decision.mode != _MSG_PROVIDER:
            errors.add(
                f"{decision.plugin_key}：当前解析到 {decision.implementation!r}（{decision.mode}），演练要求 msg"
            )
    return ConcurrentStableFrozenList(errors)


# ---------------------------------------------------------------------------
# 演练端口与步骤
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DrillMessage:
    """演练用一条消息（已由端口实现解析成结构，判据不碰原始 JSON）。"""

    message_id: str
    """消息 ID。"""

    offset: int
    """分区内序号。"""

    payload: bytes
    """原始载荷。"""

    headers: ConcurrentStableFrozenDict[str, str]
    """结构化头。"""

    key: str | None
    """分区键。"""


@dataclass(frozen=True)
class ReadOnlyResult:
    """只读调用结果（摘要 + 条数；不把整个响应搬进记录）。"""

    target: str
    """调用目标（如 `platform.modules.snapshot`）。"""

    digest: str
    """结果摘要。"""

    count: int
    """结果条数（单值响应记 1）。"""


@dataclass(frozen=True)
class WriteAttemptResult:
    """非幂等写调用的重试结果（在途请求判据）。"""

    executions: int
    """服务端实际执行次数（去重后）。"""

    keys: ConcurrentStableFrozenList[str]
    """每一跳的幂等键（须全部相同）。"""

    digest: str
    """结果摘要。"""


class DrillTargets(Protocol):
    """演练目标（进程内真应用与网络实现同一份判据）。"""

    async def prepare_event(self) -> None:
        """建主题并加入消费组。"""
        ...

    async def publish(self, event: EventEnvelope) -> None:
        """经 msg 通道发布（失败抛 `EventPublishError`）。"""
        ...

    async def publish_unavailable(self, event: EventEnvelope) -> None:
        """经不可达实例发布（必须抛 `EventPublishError`）。"""
        ...

    async def consume_events(self) -> ConcurrentStableFrozenList[DrillMessage]:
        """拉取并确认当前可投递批（返回本次新消息）。"""
        ...

    async def read_only(self) -> ReadOnlyResult:
        """只读远程调用。"""
        ...

    async def fallback_read_only(self) -> ReadOnlyResult | None:
        """原链路只读调用（未提供时返回 None）。"""
        ...

    async def write_attempt(self, key: str) -> WriteAttemptResult | None:
        """非幂等写调用重试（无法注入失败时返回 None）。"""
        ...

    def switch(self) -> SwitchReport:
        """两个端口的切换判定。"""
        ...

    async def aclose(self) -> None:
        """释放连接。"""
        ...


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
class DrillReport:
    """演练报告（渲染 JSON / Markdown 的唯一输入）。"""

    mode: str
    """`check` / `drill`。"""

    env: str
    """归一化环境。"""

    started_at: datetime
    """开始时刻（UTC）。"""

    finished_at: datetime
    """结束时刻（UTC）。"""

    switch: ConcurrentStableFrozenDict[str, str]
    """切换结果（扁平化，便于记录与对账）。"""

    gate: GateResult
    """生产闸门结果。"""

    matrix: ConcurrentStableFrozenDict[str, object]
    """矩阵摘要（域数 / 批次数 / 指标校验数）。"""

    steps: ConcurrentStableFrozenList[StepResult]
    """步骤结果。"""

    counts: ConcurrentStableFrozenDict[str, int]
    """计数（发布 / 消费 / 执行 / 消息 / 重复）。"""

    conclusion: str
    """`通过` / `降级通过` / `失败`。"""

    reasons: ConcurrentStableFrozenList[str]
    """结论原因（降级 / 失败项）。"""

    issues: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """问题与处置。"""

    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    """遗留与归口。"""

    artifacts: ConcurrentStableFrozenDict[str, str]
    """记录产物路径（相对仓根 / 绝对路径按调用方给）。"""


def _event(event_type: str, index: int, now: datetime, tenant: str) -> EventEnvelope:
    """构造一条演练事件（ID 稳定，便于记录对账）。

    Args:
        event_type: 事件类型。
        index: 第几条（同一次演练内递增）。
        now: 事件发生时刻。
        tenant: 租户编码。

    Returns:
        EventEnvelope: 事件信封。
    """
    return EventEnvelope(
        event_type=event_type,
        payload=ConcurrentStableDict({"drill_index": index, "run": "07-4-1"}),
        tenant_id=tenant,
        trace_id=f"drill-{index}",
        event_id=f"drill-{index:04d}",
        occurred_at=now,
        aggregate_key=f"drill-{index % 4}",
    )


def event_chain_errors(
    event: EventEnvelope,
    messages: ConcurrentStableFrozenList[DrillMessage],
) -> ConcurrentStableFrozenList[str]:
    """校验事件链：恰好一条、载荷与属性一致。

    Args:
        event: 已发布事件。
        messages: 本次消费到的消息。

    Returns:
        ConcurrentStableFrozenList[str]: 错误列表（空表示通过）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    if len(messages) != 1:
        errors.add(f"事件链应消费到 1 条，实际 {len(messages)} 条")
        return ConcurrentStableFrozenList(errors)
    message = messages[0]
    expected = encode_event(event)
    if message.payload != expected:
        errors.add(f"载荷不一致：期望 {len(expected)} 字节，实际 {len(message.payload)} 字节")
    for name, value in event_attributes(event).items():
        actual = message.headers.get(name)
        if actual != value:
            errors.add(f"属性不一致：{name} 期望 {value!r}，实际 {actual!r}")
    if event.aggregate_key is not None and message.key != event.aggregate_key:
        errors.add(f"分区键不一致：期望 {event.aggregate_key!r}，实际 {message.key!r}")
    return ConcurrentStableFrozenList(errors)


def _signature_shape(func: Callable[..., object]) -> ConcurrentStableFrozenList[str]:
    """取可调用对象的参数形状（名字 + 种类，不含注解，避免模块前缀干扰）。

    Args:
        func: 可调用对象。

    Returns:
        ConcurrentStableFrozenList[str]: `name:kind` 列表。
    """
    shape: ConcurrentStableList[str] = ConcurrentStableList()
    for parameter in inspect.signature(func).parameters.values():
        shape.add(f"{parameter.name}:{parameter.kind.name}")
    return ConcurrentStableFrozenList(shape)


def business_zero_change() -> tuple[ConcurrentStableFrozenList[str], ConcurrentStableFrozenDict[str, str]]:
    """校验「业务代码零改动」的端口契约面（签名形状不变）。

    Returns:
        tuple: 错误列表 + 取证（函数名 → 形状）。
    """
    errors: ConcurrentStableList[str] = ConcurrentStableList()
    evidence: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    read_only_shape = _signature_shape(fetch_catalog_snapshot)
    evidence.set("fetch_catalog_snapshot", ",".join(read_only_shape))
    required = [
        parameter.name
        for parameter in inspect.signature(fetch_catalog_snapshot).parameters.values()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    ]
    if required != ["client"]:
        errors.add(f"真实调用点新增了必填参数：{required}（业务代码应保持不变）")
    for label, implementation, port in (
        ("MsgEventPublisher.publish", MsgEventPublisher.publish, EventPublisher.publish),
        ("MsgServiceClient.call", MsgServiceClient.call, BaseServiceClient.call),
    ):
        impl_shape = _signature_shape(implementation)
        port_shape = _signature_shape(port)
        evidence.set(label, ",".join(impl_shape))
        if tuple(impl_shape) != tuple(port_shape):
            errors.add(f"{label} 与端口签名不一致：{list(impl_shape)} vs {list(port_shape)}")
    return ConcurrentStableFrozenList(errors), ConcurrentStableFrozenDict(evidence)


async def run_drill(
    *,
    matrix: SwitchMatrix,
    preconditions: PreconditionSet,
    targets: DrillTargets,
    env: str,
    event_type: str = MIGRATION_DEFAULT_EVENT_TYPE,
    observe_window_minutes: int = MIGRATION_OBSERVE_WINDOW_MINUTES,
    now: Callable[[], datetime] = _now,
    clock: Callable[[], float] = _monotonic,
) -> DrillReport:
    """执行隔离环境演练（判据唯一实现；用例与 CLI 共用）。

    Args:
        matrix: 已解析矩阵。
        preconditions: 已解析前置清单。
        targets: 演练目标（真 msg 应用或网络实现）。
        env: 归一化环境（不得为 prod）。
        event_type: 演练事件类型。
        observe_window_minutes: 观察窗口（记录用；演练不真的等待）。
        now: 时间源（用例注入固定时刻）。
        clock: 单调时钟（用例注入假时钟）。

    Returns:
        DrillReport: 演练报告。

    Raises:
        ValueError: `env` 为生产环境（生产切换归 bms 8B_08）。
    """
    if env == _ENV_PROD:
        raise ValueError("生产环境禁止执行迁移演练（生产切换归 bms 8B_08）")
    started_at = now()
    steps: ConcurrentStableList[StepResult] = ConcurrentStableList()
    reasons: ConcurrentStableList[str] = ConcurrentStableList()
    counts: ConcurrentStableDict[str, int] = ConcurrentStableDict(
        {"published": 0, "consumed": 0, "executions": 0, "messages": 0, "duplicates": 0}
    )
    issues: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    seen_ids: ConcurrentStableSet[str] = ConcurrentStableSet()

    def _step(name: str, result: str, elapsed: float, detail: str) -> None:
        """登记一步（统一取整毫秒）。"""
        cost = max(int((clock() - elapsed) * 1000), 0)
        steps.add(StepResult(step=name, result=result, elapsed_ms=cost, detail=detail))

    def _failed(name: str, elapsed: float, detail: str) -> None:
        """登记失败步并记录原因。"""
        _step(name, RESULT_FAILED, elapsed, detail)
        reasons.add(f"{name}：{detail}")

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
    if matrix_errors:
        _failed("matrix", tick, "；".join(matrix_errors))
    else:
        _step(
            "matrix",
            RESULT_OK,
            tick,
            f"{len(matrix.domains)} 域 / {len(matrix.phases)} 批次 / 观察窗口 {observe_window_minutes} 分钟",
        )

    tick = clock()
    switch = targets.switch()
    switch_errors = validate_switch(switch)
    if switch_errors:
        _failed("switch_semantics", tick, "；".join(switch_errors))
    else:
        _step(
            "switch_semantics",
            RESULT_OK,
            tick,
            f"event={switch.event.implementation} service_client={switch.service_client.implementation} "
            f"fingerprint={switch.fingerprint}",
        )

    event = _event(event_type, 1, now(), MIGRATION_DEFAULT_TENANT)
    tick = clock()
    try:
        await targets.prepare_event()
        await targets.publish(event)
        messages = await targets.consume_events()
        counts["published"] = counts.get("published", 0) + 1
        counts["consumed"] = counts.get("consumed", 0) + len(messages)
        for message in messages:
            if message.message_id in seen_ids:
                counts["duplicates"] = counts.get("duplicates", 0) + 1
            seen_ids.add(message.message_id)
        chain_errors = event_chain_errors(event, messages)
        if chain_errors:
            _failed("event_chain", tick, "；".join(chain_errors))
        else:
            digest = digest_bytes(encode_event(event))
            _step("event_chain", RESULT_OK, tick, f"topic={topic_for(event_type)} payload_digest={digest}")
    except Exception as exc:
        _failed("event_chain", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        read_only = await targets.read_only()
        if read_only.count <= 0:
            _failed("readonly_call", tick, f"{read_only.target} 返回条数 {read_only.count}")
        else:
            detail = f"{read_only.target} digest={read_only.digest} count={read_only.count}"
            _step("readonly_call", RESULT_OK, tick, detail)
    except Exception as exc:
        _failed("readonly_call", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        write = await targets.write_attempt("drill-inflight")
    except Exception as exc:
        write = None
        _failed("inflight_idempotency", tick, f"{type(exc).__name__}: {exc}")
    else:
        if write is None:
            _step("inflight_idempotency", RESULT_SKIPPED, tick, "目标无法注入失败（网络演练不验在途）")
            reasons.add("inflight_idempotency：网络演练无法注入传输失败，在途判据由进程内演练覆盖")
        elif write.executions != 1:
            _failed("inflight_idempotency", tick, f"服务端执行 {write.executions} 次（期望 1 次）")
        elif len(set(write.keys)) != 1:
            _failed("inflight_idempotency", tick, f"两跳幂等键不一致：{list(write.keys)}")
        else:
            counts["executions"] = write.executions
            _step("inflight_idempotency", RESULT_OK, tick, f"attempts={len(write.keys)} executions={write.executions}")

    tick = clock()
    unavailable = _event(event_type, 2, now(), MIGRATION_DEFAULT_TENANT)
    try:
        await targets.publish_unavailable(unavailable)
    except EventPublishError:
        try:
            await targets.publish(_event(event_type, 3, now(), MIGRATION_DEFAULT_TENANT))
            delivered = await targets.consume_events()
            counts["published"] = counts.get("published", 0) + 1
            counts["consumed"] = counts.get("consumed", 0) + len(delivered)
            for message in delivered:
                if message.message_id in seen_ids:
                    counts["duplicates"] = counts.get("duplicates", 0) + 1
                seen_ids.add(message.message_id)
            if len(delivered) != 1:
                _failed("unconfirmed_event", tick, f"恢复后应只投递 1 条，实际 {len(delivered)} 条")
            else:
                _step("unconfirmed_event", RESULT_OK, tick, "不可达抛错；恢复后恰好投递 1 条")
        except Exception as exc:
            _failed("unconfirmed_event", tick, f"{type(exc).__name__}: {exc}")
    except Exception as exc:
        _failed("unconfirmed_event", tick, f"不可达发布未抛 EventPublishError：{type(exc).__name__}: {exc}")
    else:
        _failed("unconfirmed_event", tick, "不可达发布未抛 EventPublishError（失败被吞）")

    tick = clock()
    try:
        await targets.publish(_event(event_type, 4, now(), MIGRATION_DEFAULT_TENANT))
        await targets.publish(_event(event_type, 5, now(), MIGRATION_DEFAULT_TENANT))
        messages = await targets.consume_events()
        counts["published"] = counts.get("published", 0) + 2
        counts["consumed"] = counts.get("consumed", 0) + len(messages)
        for message in messages:
            if message.message_id in seen_ids:
                counts["duplicates"] = counts.get("duplicates", 0) + 1
            seen_ids.add(message.message_id)
        counts["messages"] = counts.get("messages", 0) + len(messages)
        if len(messages) != 2:
            _failed("dual_write", tick, f"两次调用应产生 2 条消息，实际 {len(messages)} 条")
        else:
            _step("dual_write", RESULT_OK, tick, "两次调用 = 恰好两条消息（无第二通道）")
    except Exception as exc:
        _failed("dual_write", tick, f"{type(exc).__name__}: {exc}")

    tick = clock()
    try:
        fallback = await targets.fallback_read_only()
    except Exception as exc:
        _failed("fallback", tick, f"{type(exc).__name__}: {exc}")
    else:
        if fallback is None:
            _step("fallback", RESULT_DEGRADED, tick, "未提供原链路地址，fallback 未验")
            reasons.add("fallback：未提供原链路，回滚可用性未验")
        else:
            _step("fallback", RESULT_OK, tick, f"{fallback.target} digest={fallback.digest} count={fallback.count}")

    tick = clock()
    zero_errors, evidence = business_zero_change()
    if zero_errors:
        _failed("business_zero_change", tick, "；".join(zero_errors))
    else:
        _step("business_zero_change", RESULT_OK, tick, f"端口签名不变：{len(evidence)} 项")

    await targets.aclose()
    finished_at = now()
    failed = any(item.result == RESULT_FAILED for item in steps)
    degraded = any(item.result in (RESULT_DEGRADED, RESULT_SKIPPED) for item in steps)
    if failed:
        conclusion = CONCLUSION_FAIL
    elif degraded:
        conclusion = CONCLUSION_DEGRADED
    else:
        conclusion = CONCLUSION_PASS
    if gate.blocked_by:
        issues.add(
            ConcurrentStableFrozenDict(
                {
                    "summary": f"生产迁移被前置清单阻断：{'、'.join(gate.blocked_by)}",
                    "disposition": "8B_05 / 07-4-2 交付后更新清单状态，本任务不进入生产迁移",
                }
            )
        )
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    leftovers.add(
        ConcurrentStableFrozenDict(
            {
                "item": "生产集群联调与观察窗口（PromQL 取值 / 阈值裁定）",
                "owner": "bms 8B_08 + 8B_12",
            }
        )
    )
    leftovers.add(
        ConcurrentStableFrozenDict({"item": "域级白名单 / 灰度键（当前只有全局 provider）", "owner": "bms 8B_08"})
    )
    leftovers.add(ConcurrentStableFrozenDict({"item": "完整回滚闭环（注入失败 → 回滚 → 恢复）", "owner": "07-4-2"}))
    return DrillReport(
        mode="drill",
        env=env,
        started_at=started_at,
        finished_at=finished_at,
        switch=ConcurrentStableFrozenDict(
            {
                "event_provider": switch.event.configured,
                "event_implementation": switch.event.implementation,
                "event_mode": switch.event.mode,
                "service_client_provider": switch.service_client.configured,
                "service_client_implementation": switch.service_client.implementation,
                "service_client_mode": switch.service_client.mode,
                "config_fingerprint": switch.fingerprint,
            }
        ),
        gate=gate,
        matrix=ConcurrentStableFrozenDict(
            {
                "domains": len(matrix.domains),
                "phases": len(matrix.phases),
                "observe_window_minutes": observe_window_minutes,
                "metrics_checked": sorted({metric for domain in matrix.domains for metric in domain.observe_metrics}),
            }
        ),
        steps=ConcurrentStableFrozenList(steps),
        counts=ConcurrentStableFrozenDict(counts),
        conclusion=conclusion,
        reasons=ConcurrentStableFrozenList(reasons),
        issues=ConcurrentStableFrozenList(issues),
        leftovers=ConcurrentStableFrozenList(leftovers),
        artifacts=ConcurrentStableFrozenDict(),
    )


# ---------------------------------------------------------------------------
# 记录渲染
# ---------------------------------------------------------------------------


def record_dict(report: DrillReport) -> ConcurrentStableDict[str, object]:
    """把报告折成 JSON 记录（字段与设计 §10.1 一一对应）。

    Args:
        report: 演练报告。

    Returns:
        ConcurrentStableDict[str, object]: 可 JSON 序列化的记录。
    """
    record: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    record.set("schema_version", MIGRATION_RECORD_SCHEMA_VERSION)
    record.set("task", "07-4-1")
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
    record.set("switch", dict(report.switch.items()))
    record.set(
        "production_gate",
        {
            "allowed": report.gate.allowed,
            "blocked_by": list(report.gate.blocked_by),
            "checked_env": report.gate.env,
        },
    )
    record.set("matrix", {key: value for key, value in report.matrix.items()})
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
        "conclusion",
        {
            "result": report.conclusion,
            "reasons": list(report.reasons),
            "requires_review": report.conclusion != CONCLUSION_PASS,
        },
    )
    record.set("issues", [dict(item.items()) for item in report.issues])
    record.set("leftovers", [dict(item.items()) for item in report.leftovers])
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
    lines.add("# 迁移演练记录（07-4-1）")
    lines.add("")
    lines.add(f"- 结论：**{conclusion['result']}**")
    env_line = (
        f"- 环境：`{environment['env']}`（os={environment['os']} "
        f"arch={environment['arch']} python={environment['python']}）"
    )
    lines.add(env_line)
    lines.add(f"- 开始：{record['started_at']}　结束：{record['finished_at']}")
    lines.add("")
    lines.add("## 1. 切换与闸门")
    lines.add("")
    lines.add("| 项 | 值 |")
    lines.add("| --- | --- |")
    for key, value in cast("dict[str, object]", record["switch"]).items():
        lines.add(f"| `{key}` | {value} |")
    gate = cast("dict[str, object]", record["production_gate"])
    blocked = "、".join(cast("list[str]", gate["blocked_by"])) or "无"
    lines.add(f"| `production_gate.allowed` | {gate['allowed']} |")
    lines.add(f"| `production_gate.blocked_by` | {blocked} |")
    lines.add("")
    lines.add("## 2. 步骤")
    lines.add("")
    lines.add("| 步骤 | 结果 | 耗时(ms) | 明细 |")
    lines.add("| --- | --- | --- | --- |")
    for step in cast("list[dict[str, object]]", record["steps"]):
        lines.add(f"| {step['step']} | {step['result']} | {step['elapsed_ms']} | {step['detail']} |")
    lines.add("")
    lines.add("## 3. 计数")
    lines.add("")
    counts = cast("dict[str, int]", record["counts"])
    lines.add("| 项 | 值 |")
    lines.add("| --- | --- |")
    for key, value in counts.items():
        lines.add(f"| `{key}` | {value} |")
    lines.add("")
    lines.add("## 4. 原因 / 问题 / 遗留")
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


def write_record(report: DrillReport, report_path: Path) -> tuple[Path, Path]:
    """写 JSON + Markdown 记录（同一份数据，两个载体）。

    Args:
        report: 演练报告。
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
    finalized = DrillReport(
        mode=report.mode,
        env=report.env,
        started_at=report.started_at,
        finished_at=report.finished_at,
        switch=report.switch,
        gate=report.gate,
        matrix=report.matrix,
        steps=report.steps,
        counts=report.counts,
        conclusion=report.conclusion,
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


# ---------------------------------------------------------------------------
# 网络演练目标（CLI 用；用例用进程内实现）
# ---------------------------------------------------------------------------


def _headers(tenant: str) -> ConcurrentStableDict[str, str]:
    """构造数据面 / 管理面请求头。

    Args:
        tenant: 租户编码。

    Returns:
        ConcurrentStableDict[str, str]: 请求头。
    """
    headers: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    headers.set("X-Tenant-ID", tenant)
    return headers


class HttpTargets:
    """网络演练目标：真 HTTP 管理面 / 数据面 + 真 RPC 传输。"""

    def __init__(
        self,
        *,
        settings: Settings,
        base_url: str,
        unavailable_url: str,
        namespace: str,
        event_type: str,
        group: str,
        consumer_id: str,
        tenant: str,
        read_only_service: str,
        read_only_method: str,
        fallback_base_url: str | None,
        fallback_path: str,
        http_transport: httpx.AsyncBaseTransport | None = None,
        unavailable_transport: httpx.AsyncBaseTransport | None = None,
        fallback_transport: httpx.AsyncBaseTransport | None = None,
        rpc_transport: RpcTransport | None = None,
    ) -> None:
        """初始化（构造连接对象，不发起调用）。

        Args:
            settings: bms 配置（用于解析切换判定）。
            base_url: msg 实例基址。
            unavailable_url: 不可达实例基址（用于「未确认事件」注入）。
            namespace: 事件命名空间。
            event_type: 演练事件类型。
            group: 消费组。
            consumer_id: 成员标识。
            tenant: 租户编码。
            read_only_service: 只读调用目标服务。
            read_only_method: 只读调用目标方法。
            fallback_base_url: 原链路基址（None 表示未提供）。
            fallback_path: 原链路只读路径。
            http_transport: 注入的 httpx 传输（用例走 ASGI）。
            unavailable_transport: 不可达实例的 httpx 传输（缺省同 `http_transport`）。
            fallback_transport: 原链路的 httpx 传输（缺省同 `http_transport`）。
            rpc_transport: 注入的 RPC 传输（用例走 ASGI；生产为 None → h2）。
        """
        self._settings = settings
        self._base_url = base_url.rstrip("/")
        self._namespace = namespace
        self._event_type = event_type
        self._group = group
        self._consumer_id = consumer_id
        self._tenant = tenant
        self._read_only_service = read_only_service
        self._read_only_method = read_only_method
        self._fallback_base_url = fallback_base_url.rstrip("/") if fallback_base_url else None
        self._fallback_path = fallback_path
        self._http: httpx.AsyncClient = httpx.AsyncClient(
            base_url=self._base_url, transport=http_transport, timeout=10.0
        )
        self._publisher = MsgEventPublisher(
            base_url=self._base_url, namespace=namespace, tenant=tenant, transport=http_transport
        )
        self._unavailable = MsgEventPublisher(
            base_url=unavailable_url.rstrip("/"),
            namespace=namespace,
            tenant=tenant,
            transport=unavailable_transport or http_transport,
        )
        self._fallback_http: httpx.AsyncClient | None = (
            httpx.AsyncClient(
                base_url=self._fallback_base_url,
                transport=fallback_transport or http_transport,
                timeout=10.0,
            )
            if self._fallback_base_url is not None
            else None
        )
        self._rpc = RpcClient(
            StaticInstanceSelector([self._base_url]),
            tenant=tenant,
            transport=rpc_transport,
            circuit=default_circuit(),
        )
        self._closed = False

    async def prepare_event(self) -> None:
        """建主题并加入消费组（已存在时按幂等处理）。"""
        topic = topic_for(self._event_type)
        headers = _headers(self._tenant)
        created = await self._http.post(
            f"{_DATA_PLANE_PREFIX}/admin/topics",
            json={"namespace": self._namespace, "name": topic, "partition_count": 1},
            headers=dict(headers.items()),
        )
        if created.status_code not in (200, 409):
            created.raise_for_status()
        joined = await self._http.post(
            f"{_DATA_PLANE_PREFIX}/join",
            json={
                "group": self._group,
                "namespace": self._namespace,
                "topic": topic,
                "consumer_id": self._consumer_id,
            },
            headers=dict(headers.items()),
        )
        joined.raise_for_status()

    async def publish(self, event: EventEnvelope) -> None:
        """经 msg 通道发布事件。

        Args:
            event: 事件信封。
        """
        await self._publisher.publish(event)

    async def publish_unavailable(self, event: EventEnvelope) -> None:
        """经不可达实例发布（必须抛 `EventPublishError`）。

        Args:
            event: 事件信封。
        """
        await self._unavailable.publish(event)

    async def consume_events(self) -> ConcurrentStableFrozenList[DrillMessage]:
        """拉取并确认当前可投递批。

        Returns:
            ConcurrentStableFrozenList[DrillMessage]: 本次新消息。
        """
        topic = topic_for(self._event_type)
        headers = dict(_headers(self._tenant).items())
        response = await self._http.post(
            f"{_DATA_PLANE_PREFIX}/consume",
            json={
                "group": self._group,
                "consumer_id": self._consumer_id,
                "namespace": self._namespace,
                "topic": topic,
                "partition": 0,
                "max_messages": 100,
                "max_wait_ms": 0,
            },
            headers=headers,
        )
        response.raise_for_status()
        body = cast("dict[str, object]", response.json())
        data = cast("dict[str, object]", body.get("data", {}))
        raw_items = cast("list[object]", data.get("messages", []))
        messages: ConcurrentStableList[DrillMessage] = ConcurrentStableList()
        for raw in raw_items:
            item = cast("dict[str, object]", raw)
            parsed_headers = cast("dict[str, str]", item.get("headers", {}))
            messages.add(
                DrillMessage(
                    message_id=str(item.get("message_id") or ""),
                    offset=int(cast("int", item.get("offset") or 0)),
                    payload=base64.b64decode(str(item.get("payload") or "")),
                    headers=ConcurrentStableFrozenDict(parsed_headers),
                    key=cast("str | None", item.get("key")),
                )
            )
            await self._ack(topic, int(cast("int", item.get("offset") or 0)))
        return ConcurrentStableFrozenList(messages)

    async def _ack(self, topic: str, offset: int) -> None:
        """确认一条消息（让位点前进，下一步只拿到新消息）。

        Args:
            topic: 主题名。
            offset: 分区内序号。
        """
        response = await self._http.post(
            f"{_DATA_PLANE_PREFIX}/ack",
            json={
                "group": self._group,
                "consumer_id": self._consumer_id,
                "namespace": self._namespace,
                "topic": topic,
                "partition": 0,
                "offset": offset,
            },
            headers=dict(_headers(self._tenant).items()),
        )
        response.raise_for_status()

    async def read_only(self) -> ReadOnlyResult:
        """只读 RPC 调用（`service.method`；网络演练用内置方法验通道可用）。

        业务调用点（`fetch_catalog_snapshot`）的适配器映射由进程内演练验证——那里走
        `ServiceRequest` → 公开路径 → 方法归一化；这里只为部署窗口提供一个「任何实例都有」的目标。

        Returns:
            ReadOnlyResult: 目标 / 摘要 / 条数。
        """
        reply = await self._rpc.call(self._read_only_service, self._read_only_method, b"drill")
        return ReadOnlyResult(
            target=f"{self._read_only_service}.{self._read_only_method}",
            digest=digest_bytes(reply.payload),
            count=1,
        )

    async def fallback_read_only(self) -> ReadOnlyResult | None:
        """原链路只读调用（未提供地址时返回 None）。

        Returns:
            ReadOnlyResult | None: 结果或 None。
        """
        if self._fallback_base_url is None:
            return None
        client = self._fallback_http
        if client is None:  # pragma: no cover - 构造期已保证：有基址就必有客户端
            return None
        response = await client.get(self._fallback_path)
        response.raise_for_status()
        return ReadOnlyResult(
            target=f"http:{self._fallback_path}",
            digest=digest_bytes(response.content),
            count=1,
        )

    async def write_attempt(self, key: str) -> WriteAttemptResult | None:
        """网络演练不注入传输失败（在途判据由进程内演练覆盖）。

        Args:
            key: 幂等键（未使用）。

        Returns:
            None: 恒定不支持。
        """
        del key
        return None

    def switch(self) -> SwitchReport:
        """两个端口的切换判定。

        Returns:
            SwitchReport: 判定结果。
        """
        return resolve_switch(self._settings)

    async def aclose(self) -> None:
        """释放全部连接（幂等）。"""
        if self._closed:
            return
        self._closed = True
        await self._http.aclose()
        await self._publisher.aclose()
        await self._unavailable.aclose()
        if self._fallback_http is not None:
            await self._fallback_http.aclose()
        await self._rpc.aclose()


def build_http_targets(
    settings: Settings,
    *,
    base_url: str,
    unavailable_url: str = "http://127.0.0.1:1",
    namespace: str,
    event_type: str = MIGRATION_DEFAULT_EVENT_TYPE,
    group: str = MIGRATION_DEFAULT_GROUP,
    consumer_id: str = MIGRATION_DEFAULT_CONSUMER_ID,
    tenant: str = MIGRATION_DEFAULT_TENANT,
    read_only_service: str = MIGRATION_DEFAULT_READ_ONLY_SERVICE,
    read_only_method: str = MIGRATION_DEFAULT_READ_ONLY_METHOD,
    fallback_base_url: str | None = None,
    fallback_path: str = "/api/v1/modules/snapshot",
    http_transport: httpx.AsyncBaseTransport | None = None,
    unavailable_transport: httpx.AsyncBaseTransport | None = None,
    fallback_transport: httpx.AsyncBaseTransport | None = None,
    rpc_transport: RpcTransport | None = None,
) -> HttpTargets:
    """构造网络演练目标（CLI 与用例共用；用例注入 ASGI 传输）。

    Args:
        settings: bms 配置。
        base_url: msg 实例基址。
        unavailable_url: 不可达实例基址。
        namespace: 事件命名空间。
        event_type: 演练事件类型。
        group: 消费组。
        consumer_id: 成员标识。
        tenant: 租户编码。
        read_only_service: 只读调用目标服务。
        read_only_method: 只读调用目标方法。
        fallback_base_url: 原链路基址。
        fallback_path: 原链路只读路径。
        http_transport: 注入的 httpx 传输。
        unavailable_transport: 不可达实例的 httpx 传输。
        fallback_transport: 原链路的 httpx 传输。
        rpc_transport: 注入的 RPC 传输。

    Returns:
        HttpTargets: 网络演练目标。
    """
    return HttpTargets(
        settings=settings,
        base_url=base_url,
        unavailable_url=unavailable_url,
        namespace=namespace,
        event_type=event_type,
        group=group,
        consumer_id=consumer_id,
        tenant=tenant,
        read_only_service=read_only_service,
        read_only_method=read_only_method,
        fallback_base_url=fallback_base_url,
        fallback_path=fallback_path,
        http_transport=http_transport,
        unavailable_transport=unavailable_transport,
        fallback_transport=fallback_transport,
        rpc_transport=rpc_transport,
    )


# ---------------------------------------------------------------------------
# 命令
# ---------------------------------------------------------------------------


def run_check(
    *,
    preconditions_path: Path,
    matrix_path: Path,
    env: str,
) -> tuple[int, ConcurrentStableFrozenList[str]]:
    """执行 `check`：清单 + 闸门 + 矩阵。

    Args:
        preconditions_path: 前置清单。
        matrix_path: 切换矩阵。
        env: 归一化环境。

    Returns:
        tuple: 退出码 + 输出行。
    """
    lines: ConcurrentStableList[str] = ConcurrentStableList()
    preconditions, load_errors = load_preconditions(preconditions_path)
    errors: ConcurrentStableList[str] = ConcurrentStableList(load_errors)
    errors.extend(validate_preconditions(preconditions))
    matrix, matrix_load_errors = load_matrix(matrix_path)
    errors.extend(matrix_load_errors)
    errors.extend(
        validate_matrix(matrix, known_preconditions=ConcurrentStableFrozenList(i.id for i in preconditions.items))
    )
    if errors:
        lines.add(f"校验失败（{len(errors)} 项）：")
        lines.extend(errors)
        return _CHECK_FAIL, ConcurrentStableFrozenList(lines)
    gate = evaluate_gate(preconditions, env)
    lines.add(f"前置清单：{len(preconditions.items)} 项，全部合法")
    lines.add(f"切换矩阵：{len(matrix.domains)} 域 / {len(matrix.phases)} 批次，全部合法")
    lines.add(f"环境：{env}；生产闸门：{'允许' if gate.allowed else '阻断'}")
    if gate.blocked_by:
        lines.add(f"阻断项（{len(gate.blocked_by)}）：")
        lines.extend(gate.detail)
    if env == _ENV_PROD and not gate.allowed:
        return _CHECK_FAIL, ConcurrentStableFrozenList(lines)
    return _CHECK_OK, ConcurrentStableFrozenList(lines)


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


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="python -m ops.migration", description="bms 迁移编排（07-4-1）")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="前置清单 + 生产闸门 + 矩阵校验")
    check.add_argument("--preconditions", default=MIGRATION_PRECONDITIONS_PATH)
    check.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)
    check.add_argument("--env", default=MIGRATION_DEFAULT_ENV)

    matrix = subparsers.add_parser("matrix", help="渲染切换矩阵（文档表由此粘贴）")
    matrix.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)

    drill = subparsers.add_parser("drill", help="隔离环境迁移演练并写记录")
    drill.add_argument("--preconditions", default=MIGRATION_PRECONDITIONS_PATH)
    drill.add_argument("--matrix", default=MIGRATION_MATRIX_PATH)
    drill.add_argument("--env", default=MIGRATION_DEFAULT_ENV)
    drill.add_argument("--msg-url", required=True)
    drill.add_argument("--unavailable-url", default="http://127.0.0.1:1")
    drill.add_argument("--namespace", default=SERVICE_NAME)
    drill.add_argument("--event-type", default=MIGRATION_DEFAULT_EVENT_TYPE)
    drill.add_argument("--read-only-service", default=MIGRATION_DEFAULT_READ_ONLY_SERVICE)
    drill.add_argument("--read-only-method", default=MIGRATION_DEFAULT_READ_ONLY_METHOD)
    drill.add_argument("--fallback-base-url", default="")
    drill.add_argument("--fallback-path", default="/api/v1/modules/snapshot")
    drill.add_argument("--report", default="")
    drill.add_argument("--event-provider", default=_MSG_PROVIDER)
    drill.add_argument("--service-client-provider", default=_MSG_PROVIDER)
    return parser


def build_targets(settings: Settings, args: argparse.Namespace) -> DrillTargets:
    """缺省演练目标工厂（用例 monkeypatch 成进程内实现，CLI 代码路径因此可覆盖）。

    Args:
        settings: bms 配置。
        args: 命令行参数。

    Returns:
        DrillTargets: 网络演练目标。
    """
    return build_http_targets(
        settings,
        base_url=args.msg_url,
        unavailable_url=args.unavailable_url,
        namespace=args.namespace,
        event_type=args.event_type,
        read_only_service=args.read_only_service,
        read_only_method=args.read_only_method,
        fallback_base_url=args.fallback_base_url or None,
        fallback_path=args.fallback_path,
    )


_TARGETS_FACTORY: Callable[[Settings, argparse.Namespace], DrillTargets] = build_targets
"""演练目标工厂（模块属性；用例 monkeypatch 该属性即可离线跑 CLI）。"""


def _settings_for(args: argparse.Namespace) -> Settings:
    """按命令行参数构造 bms 配置（只覆盖开关与实例地址，不改其他项）。

    Args:
        args: 命令行参数。

    Returns:
        Settings: 应用配置。
    """
    settings = Settings()
    settings.app.service = args.namespace
    settings.event.provider = args.event_provider
    settings.event.options.set("namespace", args.namespace)
    settings.service_client.provider = args.service_client_provider
    settings.service_client.instances.set(_MSG_PROVIDER, ConcurrentStableList([args.msg_url]))
    return settings


def _cmd_drill(args: argparse.Namespace) -> int:
    """执行 `drill` 子命令。

    Args:
        args: 命令行参数。

    Returns:
        int: 退出码。
    """
    try:
        env = normalize_env(args.env)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return _CHECK_USAGE
    if env == _ENV_PROD:
        print("生产环境禁止执行迁移演练（生产切换归 bms 8B_08）", file=sys.stderr)
        return _CHECK_USAGE
    preconditions, load_errors = load_preconditions(_default_path(args.preconditions, MIGRATION_PRECONDITIONS_PATH))
    matrix, matrix_errors = load_matrix(_default_path(args.matrix, MIGRATION_MATRIX_PATH))
    errors: ConcurrentStableList[str] = ConcurrentStableList(load_errors)
    errors.extend(matrix_errors)
    if errors:
        for line in errors:
            print(line, file=sys.stderr)
        return _CHECK_FAIL
    settings = _settings_for(args)
    register_platform_plugins(settings)
    targets = _TARGETS_FACTORY(settings, args)
    report = asyncio.run(
        run_drill(
            matrix=matrix,
            preconditions=preconditions,
            targets=targets,
            env=env,
            event_type=args.event_type,
        )
    )
    report_path = _default_path(args.report, f"{MIGRATION_REPORT_DIR}/drill-{report.started_at:%Y%m%dT%H%M%SZ}")
    json_path, markdown_path = write_record(report, report_path)
    print(f"结论：{report.conclusion}（退出码 {EXIT_CODES[report.conclusion]}）")
    for step in report.steps:
        print(f"  {step.step}: {step.result}（{step.elapsed_ms}ms）{step.detail}")
    if report.gate.blocked_by:
        print(f"生产闸门阻断项：{'、'.join(report.gate.blocked_by)}（本环境不阻断）")
    print(f"记录：{json_path}")
    print(f"记录：{markdown_path}")
    return EXIT_CODES[report.conclusion]


def register_platform_plugins(settings: Settings) -> None:
    """登记平台内建实现（`resolve_plugin` 能解析 `event` / `service_client` 的前提）。

    Args:
        settings: bms 配置。
    """
    from bms_core.core.assembly import register_platform_plugins as _register

    _register(settings, FastAPI(), ResourceManager())


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
        matrix_errors = validate_matrix(matrix)
        if errors or matrix_errors:
            for line in list(errors) + list(matrix_errors):
                print(line, file=sys.stderr)
            return _CHECK_FAIL
        for row in render_matrix_rows(matrix):
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
            env=env,
        )
        for line in lines:
            print(line)
        return code
    return _cmd_drill(args)


if __name__ == "__main__":
    raise SystemExit(main())
