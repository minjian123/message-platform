"""演练探针（02-8）：`/readyz`、`/metrics` 与分区快照的解析。

**解析与取数分离**：解析是纯函数（用例喂真实报文片段）；取数走 `ProbeClient`，可用
`httpx.MockTransport` 覆盖失败分支。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import cast

import httpx
from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableFrozenDict, ConcurrentStableList
from bms_core.core.objects import BaseFrameworkObject, BaseValueObject

__all__ = ["MetricSample", "ProbeClient", "ReadyReading", "parse_metrics", "parse_readyz"]

_METRIC_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?"
    r"\s+(?P<value>[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)$"
)
"""Prometheus 文本行（本任务只认简单数值行，忽略 histogram 分位。"""


@dataclass(frozen=True)
class ReadyReading(BaseValueObject):
    """一次 `/readyz` 读数。"""

    status: int
    """HTTP 状态码（0 = 不可达）。"""

    ok: bool
    """状态码 200 且 `status=ok`。"""

    checks: ConcurrentStableFrozenDict[str, str] = field(default_factory=lambda: ConcurrentStableFrozenDict[str, str]())
    """检查项 → 结果（`ok` / `failed`）。"""


@dataclass(frozen=True)
class MetricSample(BaseValueObject):
    """一条指标读数。"""

    name: str
    labels: ConcurrentStableFrozenDict[str, str]
    value: float


def parse_readyz(status: int, body: object) -> ReadyReading:
    """解析 `/readyz` 响应。

    Args:
        status: HTTP 状态码。
        body: 响应体（JSON 映射；非映射按空）。

    Returns:
        ReadyReading: 读数。
    """
    checks: ConcurrentStableDict[str, str] = ConcurrentStableDict()
    if isinstance(body, dict):
        payload = cast("dict[str, object]", body)
        raw_checks = payload.get("checks")
        if isinstance(raw_checks, dict):
            for name, value in cast("dict[str, object]", raw_checks).items():
                if isinstance(value, dict):
                    mapped = cast("dict[str, object]", value).get("ok")
                    checks.set(str(name), "ok" if mapped is True else "failed")
                else:
                    checks.set(str(name), "ok" if value is True else "failed")
        ok = status == 200 and payload.get("status") == "ok"
    else:
        ok = status == 200
    return ReadyReading(status=status, ok=ok, checks=ConcurrentStableFrozenDict(checks))


def parse_metrics(text: str) -> ConcurrentStableList[MetricSample]:
    """解析 Prometheus 文本里的简单数值行。

    Args:
        text: `/metrics` 响应文本。

    Returns:
        ConcurrentStableList[MetricSample]: 读数（声明序）。
    """
    samples: ConcurrentStableList[MetricSample] = ConcurrentStableList()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        matched = _METRIC_LINE.match(line)
        if matched is None:
            continue
        labels: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        raw_labels = matched.group("labels") or ""
        for pair in raw_labels.split(","):
            name, _, value = pair.partition("=")
            if name.strip():
                labels.set(name.strip(), value.strip().strip('"'))
        samples.add(
            MetricSample(
                name=matched.group("name"),
                labels=ConcurrentStableFrozenDict(labels),
                value=float(matched.group("value")),
            )
        )
    return samples


def metric_value(samples: ConcurrentStableList[MetricSample], name: str) -> float:
    """取指标总量（同名的各行求和；缺失为 0）。

    Args:
        samples: 读数。
        name: 指标名。

    Returns:
        float: 合计值。
    """
    return sum(sample.value for sample in samples if sample.name == name)


class ProbeClient(BaseFrameworkObject):
    """实例探针（本机隧道端口；用例注入 `transport` 覆盖网络）。"""

    def __init__(
        self,
        bases: ConcurrentStableList[str],
        *,
        timeout_sec: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """初始化。

        Args:
            bases: 三实例基地址（`http://127.0.0.1:<端口>`）。
            timeout_sec: 请求超时（秒）。
            transport: 传输替身（用例注入）。
        """
        self._bases = bases
        self._client = httpx.AsyncClient(timeout=timeout_sec, transport=transport)

    async def aclose(self) -> None:
        """关闭连接池。"""
        await self._client.aclose()

    async def readyz(self, index: int) -> ReadyReading:
        """读某实例 `/readyz`。

        Args:
            index: 实例下标（0 起）。

        Returns:
            ReadyReading: 读数（不可达 status=0）。
        """
        try:
            response = await self._client.get(f"{self._bases[index]}/readyz")
        except httpx.HTTPError:
            return ReadyReading(status=0, ok=False)
        try:
            body: object = response.json()
        except ValueError:
            body = ConcurrentStableDict[str, object]()
        return parse_readyz(response.status_code, body)

    async def metrics(self, index: int) -> ConcurrentStableList[MetricSample]:
        """读某实例 `/metrics`。

        Args:
            index: 实例下标。

        Returns:
            ConcurrentStableList[MetricSample]: 读数（不可达为空）。
        """
        try:
            response = await self._client.get(f"{self._bases[index]}/metrics")
        except httpx.HTTPError:
            return ConcurrentStableList()
        return parse_metrics(response.text)

    async def all_ready(self) -> bool:
        """三实例是否全部就绪。

        Returns:
            bool: 全部 200 且 `status=ok`。
        """
        for index in range(len(self._bases)):
            reading = await self.readyz(index)
            if not reading.ok:
                return False
        return True
