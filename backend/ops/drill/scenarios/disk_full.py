"""场景二 · 磁盘满（任务 07-6-1）：受控填充数据卷 + 收紧拒绝水位。

本机没有可配额的隔离卷，所以「满」由两件事合成：把拒绝水位压到「当前用量 + 填充占比的一半」，
再用受控填充分块写满到越过阈值。生产容器通道改用 `tmpfs` 限额卷，判据完全相同。
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import STORAGE_SAMPLE_TTL_MS
from ops.drill.context import (
    DrillContext,
    ScenarioOutcome,
    check,
    require_ok,
    result_of,
)
from ops.drill.model import Criterion
from ops.drill.runtime import payload_of

needs_tight_watermark = True
"""磁盘满场景必须在启动前收紧水位。"""

_FILL_DIR = "fill"
"""填充文件目录名（落在演练工作根下，与数据根同卷）。"""

_FILL_FILE = "volume.bin"
"""填充文件名。"""

_CHUNK_BYTES = 1024 * 1024
"""单次写入块（1 MiB）。"""

_DISK_FULL_CODE = 350022
"""存储水位拒绝的业务码（02-6）。"""

_DISK_FULL_HTTP = 507
"""存储水位拒绝的 HTTP 状态（02-6）。"""


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append("1. 实例以收紧水位启动（拒绝水位按当时用量 + 填充占比的一半）")
    rows.append(f"2. 建主题 `{ctx.topic()}` 并发布一条基线消息，采样「注入前」")
    rows.append(f"3. 向 `{_FILL_DIR}/{_FILL_FILE}` 写入 {ctx.fill_mb} MiB 并 fsync（受控填充）")
    rows.append("4. 采样「注入中」：/readyz 应 503、`msg_disk_used_ratio` 上升")
    rows.append("5. 注入中发布 → 期望 HTTP 507 + 业务码 350022")
    rows.append("6. 删除填充文件、等水位采样 TTL 过期，采样「恢复后」")
    rows.append("7. 恢复后发布 → 期望成功、/readyz 回 200")
    return rows


def _fill(work_dir: Path, size_mb: int) -> Path:
    """受控填充：写入指定 MiB 并逐块 `fsync`（不占系统盘，只占演练工作根）。

    Args:
        work_dir: 演练工作根。
        size_mb: 填充量（MiB）。

    Returns:
        Path: 填充文件路径。
    """
    folder = work_dir / _FILL_DIR
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / _FILL_FILE
    block = b"\0" * _CHUNK_BYTES
    with path.open("wb") as handle:
        for _ in range(max(size_mb, 1)):
            handle.write(block)
            handle.flush()
            os.fsync(handle.fileno())
    return path


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行磁盘满注入并验证「拒绝 → 可观测 → 清理后恢复」。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    topic = ctx.topic()
    ctx.prepare_topic(topic)
    require_ok(ctx.publish(topic, b"disk-before"), "注入前发布基线消息")
    before = ctx.sample("before")

    injected_at = time.monotonic()
    fill_path = _fill(ctx.work_dir, ctx.fill_mb)
    time.sleep(STORAGE_SAMPLE_TTL_MS / 1000 + 0.2)
    during = ctx.sample("during")
    rejected = ctx.publish(topic, b"disk-during")
    rejected_body = payload_of(rejected)
    rejected_code = int(float(str(rejected_body.get("code") or 0)))

    fill_path.unlink(missing_ok=True)
    time.sleep(STORAGE_SAMPLE_TTL_MS / 1000 + 0.2)
    after = ctx.sample("after")
    recovered = ctx.publish(topic, b"disk-after")
    rto_ms = int((time.monotonic() - injected_at) * 1000)

    ratio_before = before.metrics.get("msg_disk_used_ratio", 0.0)
    ratio_during = during.metrics.get("msg_disk_used_ratio", 0.0)
    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    criteria.append(
        check(
            "publish_rejected",
            f"满盘期发布被拒（HTTP {_DISK_FULL_HTTP} + 业务码 {_DISK_FULL_CODE}）",
            f"HTTP {rejected.status_code}、业务码 {rejected_code}",
            result_of(rejected.status_code == _DISK_FULL_HTTP and rejected_code == _DISK_FULL_CODE),
        )
    )
    criteria.append(
        check(
            "readyz_degraded",
            "满盘期 /readyz 转 503（不摘实例但不再接写）",
            f"/readyz={during.readyz_status}",
            result_of(during.readyz_status == 503),
        )
    )
    criteria.append(
        check(
            "ratio_risen",
            "`msg_disk_used_ratio` 随填充上升",
            f"{ratio_before:.4f} → {ratio_during:.4f}",
            result_of(ratio_during > ratio_before),
        )
    )
    criteria.append(
        check(
            "process_alive",
            "进程存活（/healthz 仍 200，磁盘满不致死）",
            f"/healthz={'200' if during.healthz_ok else '不可达'}",
            result_of(during.healthz_ok),
        )
    )
    criteria.append(
        check(
            "recovered",
            "清理填充后恢复可写、/readyz 回 200",
            f"发布 HTTP {recovered.status_code}、/readyz={after.readyz_status}",
            result_of(recovered.status_code == 200 and after.readyz_ok),
        )
    )
    criteria.append(
        check(
            "rto_within_budget",
            "恢复耗时在预算内",
            f"{rto_ms} ms（预算 {ctx.config.recovery_budget_ms} ms）",
            result_of(rto_ms <= ctx.config.recovery_budget_ms),
        )
    )

    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("本机无可配额卷：用「收紧拒绝水位 + 受控填充」等价「临时卷被填满」，容器通道为 tmpfs 限额卷")
    deviations.append("填充文件落在演练工作根（与数据根同卷），清理即删除，不触碰其他目录")
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "fill_volume",
                "detail": f"填充 {ctx.fill_mb} MiB 至 {_FILL_DIR}/{_FILL_FILE}，拒绝水位收紧至 {ctx.reject_ratio}",
                "artifact": str(fill_path),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=rto_ms,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(deviations),
    )
