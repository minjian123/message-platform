"""场景执行上下文与统一接口（任务 07-6-1）。

六个场景模块共用一组操作（采样 / 建主题 / 发布 / 拉取 / 确认 / 判据累积），差别只在注入与判据；
把「怎么算通过」留在各场景文件里，把「怎么连、怎么采」收在这里，避免六份重复实现漂移。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

import httpx
from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import (
    DRILL_CONSUMER_PREFIX,
    DRILL_GROUP_PREFIX,
    DRILL_NAMESPACE,
    DRILL_TOPIC_PREFIX,
)
from ops.drill.env import DrillConfig, DrillError
from ops.drill.model import (
    RESULT_FAILED,
    RESULT_OK,
    Criterion,
    DrillScenario,
    PhaseSample,
)
from ops.drill.runtime import DrillHttp, DrillInstance, payload_of, sample_phase

__all__ = [
    "DrillContext",
    "ScenarioModule",
    "ScenarioOutcome",
    "as_mapping",
    "check",
    "items_of",
    "offset_of",
    "require_ok",
    "result_of",
]


def as_mapping(value: object) -> ConcurrentStableFrozenDict[str, object]:
    """把响应体里的嵌套映射收窄成冻结映射（非映射视为空，调用方按缺失处理）。

    Args:
        value: 任意值。

    Returns:
        ConcurrentStableFrozenDict[str, object]: 冻结映射。
    """
    if not isinstance(value, Mapping):
        return ConcurrentStableFrozenDict()
    return ConcurrentStableFrozenDict(dict(cast("Mapping[str, object]", value)))


def items_of(
    body: ConcurrentStableFrozenDict[str, object], key: str = "messages"
) -> ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]:
    """取响应体里的对象数组（`messages` 等）并逐条收窄。

    Args:
        body: 响应体。
        key: 数组字段名。

    Returns:
        ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, object]]: 条目列表。
    """
    raw = body.get(key)
    rows: ConcurrentStableList[ConcurrentStableFrozenDict[str, object]] = ConcurrentStableList()
    if isinstance(raw, list):
        for item in cast("list[object]", raw):
            rows.append(as_mapping(item))
    return ConcurrentStableFrozenList(rows)


def offset_of(body: ConcurrentStableFrozenDict[str, object]) -> int:
    """取响应体里的 `offset`（缺失即 0，由判据显式暴露差异）。

    Args:
        body: 响应体。

    Returns:
        int: 分区内序号。
    """
    return int(cast("int", body.get("offset") or 0))


def check(key: str, expect: str, actual: str, result: str) -> Criterion:
    """构造一条判据结果（唯一入口，保证 key / expect 口径一致）。

    Args:
        key: 判据键。
        expect: 期望（场景单源原文）。
        actual: 实测摘要。
        result: `ok` / `degraded` / `failed` / `skipped`。

    Returns:
        Criterion: 判据结果。
    """
    return Criterion(key=key, expect=expect, actual=actual, result=result)


def require_ok(response: httpx.Response, what: str) -> ConcurrentStableFrozenDict[str, object]:
    """校验状态码与业务码，返回响应体里的 `data`（统一包装 `{code, message, data}`）。

    Args:
        response: 响应。
        what: 动作描述（报错用）。

    Returns:
        ConcurrentStableFrozenDict[str, object]: `data` 子结构。

    Raises:
        DrillError: 状态码非 2xx 或业务码非 0。
    """
    if not 200 <= response.status_code < 300:
        raise DrillError(f"drill.scenario {what} 失败：HTTP {response.status_code} {response.text[:300]}")
    body = payload_of(response)
    code = int(cast("int", body.get("code") or 0))
    if code != 0:
        raise DrillError(f"drill.scenario {what} 失败：业务码 {code} {str(body.get('message'))[:200]}")
    return as_mapping(body.get("data"))


@dataclass(frozen=True)
class ScenarioOutcome:
    """一次场景执行的产物（判据 + 三段样本 + 注入明细）。"""

    injection: ConcurrentStableFrozenDict[str, str]
    """注入方式 / 明细 / 产物。"""

    samples: ConcurrentStableFrozenList[PhaseSample]
    """三段观测（注入前 / 注入中 / 恢复后）。"""

    criteria: ConcurrentStableFrozenList[Criterion]
    """判据结果（声明序）。"""

    rto_ms: int
    """恢复耗时（注入开始 → 恢复判据通过）。"""

    data_loss: bool
    """是否丢失已确认消息。"""

    deviations: ConcurrentStableFrozenList[str] = field(default_factory=ConcurrentStableFrozenList[str])
    """偏离项。"""

    issues: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]] = field(
        default_factory=ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    )
    """执行期问题。"""

    leftovers: ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]] = field(
        default_factory=ConcurrentStableFrozenList[ConcurrentStableFrozenDict[str, str]]
    )
    """遗留。"""


class ScenarioModule(Protocol):
    """场景模块接口（`ops.drill.scenarios.*` 各自实现）。"""

    needs_tight_watermark: bool
    """是否需要在启动前收紧存储水位（只有磁盘满场景）。"""

    def plan(self, ctx: DrillContext) -> ConcurrentStableFrozenList[str]:
        """渲染注入与恢复动作（`--dry-run`，零执行）。

        Args:
            ctx: 执行上下文。

        Returns:
            ConcurrentStableFrozenList[str]: 可读动作清单。
        """
        ...

    def run(self, ctx: DrillContext) -> ScenarioOutcome:
        """执行注入、恢复与判据。

        Args:
            ctx: 执行上下文。

        Returns:
            ScenarioOutcome: 判据与观测。
        """
        ...


@dataclass
class DrillContext:
    """场景执行上下文（实例 + 客户端 + 场景定义 + 配置）。"""

    config: DrillConfig
    """演练配置。"""

    scenario: DrillScenario
    """场景定义（场景单源的一行）。"""

    instance: DrillInstance
    """隔离实例。"""

    http: DrillHttp
    """面向实例的 HTTP 客户端。"""

    started_at: float
    """场景开始的单调时钟（秒；算 `rto_ms` 用）。"""

    fill_mb: int = 0
    """本次实际填充量（MiB；只有磁盘满场景非 0，由编排按盘容量算出）。"""

    reject_ratio: float = 0.0
    """本次实例实际使用的拒绝水位（只有磁盘满场景收紧）。"""

    _samples: ConcurrentStableList[PhaseSample] = field(default_factory=ConcurrentStableList[PhaseSample])

    _active_group: str = ""
    """当前活动消费组（`prepare_topic` 按主题切换；空表示用场景名兜底）。"""

    _active_consumer: str = ""
    """当前活动成员标识（按主题唯一：同一进程内多主题演练不会互相判死）。"""

    @property
    def work_dir(self) -> Path:
        """演练工作根。

        Returns:
            Path: 工作根。
        """
        return self.config.work_dir

    def topic(self, suffix: str = "") -> str:
        """构造演练主题名（带统一前缀，便于清理识别）。

        Args:
            suffix: 主题后缀（缺省用场景名）。

        Returns:
            str: 主题名。
        """
        return f"{DRILL_TOPIC_PREFIX}{suffix or self.scenario.name}"

    def group(self, suffix: str = "") -> str:
        """取当前活动消费组名（`prepare_topic` 之后即为该主题的组）。

        Args:
            suffix: 组后缀（缺省用当前活动组；显式给出则按该后缀构造）。

        Returns:
            str: 组名。
        """
        if suffix:
            return f"{DRILL_GROUP_PREFIX}{suffix}"
        return self._active_group or f"{DRILL_GROUP_PREFIX}{self.scenario.name}"

    def consumer(self) -> str:
        """取当前活动成员标识。

        Returns:
            str: 成员标识。
        """
        return self._active_consumer or f"{DRILL_CONSUMER_PREFIX}{self.scenario.name}"

    def namespace(self) -> str:
        """演练命名空间。

        Returns:
            str: 命名空间。
        """
        return DRILL_NAMESPACE

    def sample(self, phase: str) -> PhaseSample:
        """采集一段观测并记入上下文（三段样本的唯一入口）。

        Args:
            phase: `before` / `during` / `after`。

        Returns:
            PhaseSample: 快照。
        """
        sample = sample_phase(phase, self.http, tuple(self.scenario.observe))
        self._samples.append(sample)
        return sample

    @property
    def samples(self) -> ConcurrentStableFrozenList[PhaseSample]:
        """已采集的三段样本（按采集顺序）。

        Returns:
            ConcurrentStableFrozenList[PhaseSample]: 样本。
        """
        return ConcurrentStableFrozenList(self._samples)

    def prepare_topic(self, topic: str) -> None:
        """建主题 + 加入消费组（幂等；已存在视为成功）。

        Args:
            topic: 主题名。

        Raises:
            DrillError: 建主题或加入失败。
        """
        response = self.http.create_topic(topic)
        if response not in (200, 201, 409):
            raise DrillError(f"drill.scenario 建主题失败：HTTP {response}")
        group = f"{DRILL_GROUP_PREFIX}{topic}"
        consumer = f"{DRILL_CONSUMER_PREFIX}{topic}"
        joined = self.http.join(topic, group, consumer)
        if joined not in (200, 201):
            raise DrillError(f"drill.scenario 加入消费组失败：topic={topic} group={group} HTTP {joined}")
        self._active_group = group
        self._active_consumer = consumer

    def publish(self, topic: str, payload: bytes, key: str | None = None) -> httpx.Response:
        """发布一条消息。

        Args:
            topic: 主题名。
            payload: 消息体。
            key: 分区键。

        Returns:
            httpx.Response: 响应。
        """
        return self.http.publish(topic, payload, key)

    def consume(
        self, topic: str, *, max_messages: int = 10, max_wait_ms: int = 0
    ) -> ConcurrentStableFrozenDict[str, object]:
        """拉取消息（2xx 校验）。

        Args:
            topic: 主题名。
            max_messages: 最多条数。
            max_wait_ms: 长轮询上限（毫秒）。

        Returns:
            ConcurrentStableFrozenDict[str, object]: 响应体。
        """
        response = self.http.consume(
            topic, self.group(), self.consumer(), max_messages=max_messages, max_wait_ms=max_wait_ms
        )
        return require_ok(response, "拉取消息")

    def ack(self, topic: str, offset: int) -> None:
        """确认一条消息（非 2xx 即失败）。

        Args:
            topic: 主题名。
            offset: 分区内序号。

        Raises:
            DrillError: 确认失败。
        """
        response = self.http.ack(topic, self.group(), self.consumer(), offset)
        if not 200 <= response.status_code < 300:
            raise DrillError(f"drill.scenario 确认失败：offset={offset} HTTP {response.status_code}")

    def heartbeat(self, topic: str) -> None:
        """给当前成员续活（长等待场景每轮调用一次）。

        Args:
            topic: 主题名。
        """
        self.http.heartbeat(topic, self.group(), self.consumer())

    def metric(self, name: str) -> float:
        """读单个指标当前值（缺失记 0）。

        Args:
            name: 指标名。

        Returns:
            float: 取值。
        """
        from ops.drill.runtime import parse_metrics

        parsed = parse_metrics(self.http.metrics())
        return parsed.get(name, 0.0)

    def metric_sum(self, name: str) -> float:
        """读指标的同名多标签合计值（死信存量等多主题指标必须求和）。

        Args:
            name: 指标名。

        Returns:
            float: 合计值（无样本为 0）。
        """
        from ops.drill.runtime import parse_metric_sum

        return parse_metric_sum(self.http.metrics(), name)


def result_of(passed: bool) -> str:
    """把布尔判定转成判据结果。

    Args:
        passed: 是否通过。

    Returns:
        str: `ok` / `failed`。
    """
    return RESULT_OK if passed else RESULT_FAILED
