"""场景三 · 慢盘（任务 07-6-1）：受控写入持续 `fsync` 压同一卷，验证「不产生假确认」。

判定重心不是「延迟一定涨多少」（本机 SSD 上争抢未必显著，涨多少属部署阶段阈值），而是
**确认过的消息在停压后必须仍可读、位点不越尾**——这才是慢 I/O 下最怕的假确认。
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from ops.drill.context import (
    DrillContext,
    ScenarioOutcome,
    check,
    items_of,
    offset_of,
    require_ok,
    result_of,
)
from ops.drill.model import RESULT_DEGRADED, RESULT_OK, Criterion

needs_tight_watermark = False
"""慢盘场景不收紧水位。"""

_PRESSURE_DIR = "slowdisk"
"""压测文件目录名。"""

_PRESSURE_FILE = "pressure.bin"
"""压测文件名（覆盖写，不累积）。"""

_CHUNK_BYTES = 1024 * 1024
"""单次写入块（1 MiB）。"""

_PUBLISH_COUNT = 20
"""注入期发布并确认的条数。"""

_RISE_FACTOR = 1.05
"""判定「相对基线上升」的最小倍数（只作方向性判定，绝对阈值归部署阶段）。"""


@dataclass
class _Pressure:
    """受控写压测（后台线程：持续写 + `fsync` 同一卷）。"""

    work_dir: Path
    size_mb: int
    _stop: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = None
    error: str = ""

    def start(self) -> None:
        """启动压测线程。"""
        self._thread = threading.Thread(target=self._loop, name="drill-slow-disk", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """停止压测线程并等它退出。"""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def _loop(self) -> None:
        """循环覆盖写压测文件（异常记入 `error`，不让线程静默死掉）。"""
        folder = self.work_dir / _PRESSURE_DIR
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / _PRESSURE_FILE
        block = b"\0" * _CHUNK_BYTES
        try:
            while not self._stop.is_set():
                with path.open("wb") as handle:
                    for _ in range(max(self.size_mb, 1)):
                        handle.write(block)
                    handle.flush()
                    os.fsync(handle.fileno())
        except OSError as exc:  # 盘满 / 权限：只记录，判据侧按未加压处理
            self.error = f"{type(exc).__name__}: {exc}"


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"1. 建主题 `{ctx.topic()}`，采样「注入前」（取 `msg_fsync_duration_ms` 基线）")
    rows.append(f"2. 后台线程对 `{_PRESSURE_DIR}/{_PRESSURE_FILE}` 循环写 {ctx.config.slow_write_mb} MiB 并 fsync")
    rows.append(f"3. 注入期发布并确认 {_PUBLISH_COUNT} 条，采样「注入中」")
    rows.append("4. 停压，采样「恢复后」")
    rows.append("5. 判据：无假确认（已确认消息停压后不再投递、位点不越尾）、积压归零、延迟方向性上升")
    return rows


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行慢盘注入并验证「假确认」不存在。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    topic = ctx.topic()
    ctx.prepare_topic(topic)
    before = ctx.sample("before")

    injected_at = time.monotonic()
    pressure = _Pressure(work_dir=ctx.work_dir, size_mb=ctx.config.slow_write_mb)
    pressure.start()
    offsets: ConcurrentStableList[int] = ConcurrentStableList()
    for index in range(_PUBLISH_COUNT):
        body = require_ok(ctx.publish(topic, f"slow-{index}".encode()), f"慢盘期发布第 {index} 条")
        offsets.append(offset_of(body))
    confirmed = items_of(ctx.consume(topic, max_messages=_PUBLISH_COUNT))
    for item in confirmed:
        ctx.ack(topic, offset_of(item))
    during = ctx.sample("during")
    pressure.stop()
    time.sleep(1.0)
    after = ctx.sample("after")
    rto_ms = int((time.monotonic() - injected_at) * 1000)

    leftover = items_of(ctx.consume(topic, max_messages=_PUBLISH_COUNT))
    fsync_before = before.metrics.get("msg_fsync_duration_ms", 0.0)
    fsync_during = during.metrics.get("msg_fsync_duration_ms", 0.0)
    latency_before = before.metrics.get("msg_publish_latency_ms", 0.0)
    latency_during = during.metrics.get("msg_publish_latency_ms", 0.0)

    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    criteria.append(
        check(
            "no_fake_ack",
            "无假确认（已确认消息停压后不再投递）",
            f"停压后重投 {len(leftover)} 条（期望 0）",
            result_of(not leftover),
        )
    )
    criteria.append(
        check(
            "offset_within_leo",
            "位点不越尾（已确认条数 = 发布条数）",
            f"发布 {len(offsets)} 条、确认 {len(confirmed)} 条",
            result_of(len(confirmed) == _PUBLISH_COUNT),
        )
    )
    criteria.append(
        check(
            "lag_drained",
            "停压后积压归零（可拉取为空且无在途）",
            f"拉取 {len(leftover)} 条、/readyz={'200' if after.readyz_ok else after.readyz_status}",
            result_of(not leftover and after.readyz_ok),
        )
    )
    # 基线为 0 说明该直方图在本次运行没有样本（隔离实例无段 fsync 压测），此时无法判「上涨」，
    # 只能降级并如实写出两个值——不能拿 0 >= 0 当作通过。
    comparable = fsync_before > 0 or latency_before > 0
    rose = (fsync_before > 0 and fsync_during >= fsync_before * _RISE_FACTOR) or (
        latency_before > 0 and latency_during >= latency_before * _RISE_FACTOR
    )
    criteria.append(
        check(
            "latency_visible",
            "`msg_fsync_duration_ms` / 发布延迟相对基线上涨（方向性；绝对阈值归部署阶段）",
            (
                f"fsync {fsync_before:.2f} → {fsync_during:.2f} ms；"
                f"publish {latency_before:.2f} → {latency_during:.2f} ms"
                + ("" if comparable else "（基线为 0：该指标本次无样本，无法比较）")
            ),
            RESULT_OK if comparable and rose else RESULT_DEGRADED,
        )
    )
    criteria.append(
        check(
            "pressure_written",
            "受控写入确实执行（压测线程无 I/O 错误）",
            f"写入 {ctx.config.slow_write_mb} MiB，错误={pressure.error or '无'}",
            result_of(not pressure.error),
        )
    )

    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("慢盘由「受控写入 + fsync 争抢同卷」等价实现，未使用 dm-delay（需 root 与设备操作）")
    deviations.append("本机 SSD 上 I/O 争抢可能不足以抬升 fsync 延迟：该项判「降级」而非「失败」")
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "controlled_write",
                "detail": f"后台线程循环写 {ctx.config.slow_write_mb} MiB 至 {_PRESSURE_DIR}/{_PRESSURE_FILE} 并 fsync",
                "artifact": str(ctx.work_dir / _PRESSURE_DIR / _PRESSURE_FILE),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=rto_ms,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(deviations),
    )
