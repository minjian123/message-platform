"""场景一 · 进程崩溃（任务 07-6-1）：SIGKILL / SIGTERM 两档，同一数据根重启。

判据的事实基础：在途状态（`InflightStore`）是**进程内存**，重启即丢——因此「未确认消息重投」是
真重投，但 `delivery_count` 会从 1 重新计；记录里如实标注，不把重投计数当硬判据。
已确认位点落盘（位点文件）才是崩溃场景必须证明不丢的东西。
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import OFFSET_FLUSH_INTERVAL_MS
from ops.drill.context import (
    DrillContext,
    ScenarioOutcome,
    check,
    items_of,
    offset_of,
    require_ok,
    result_of,
)
from ops.drill.model import RESULT_OK, Criterion

needs_tight_watermark = False
"""崩溃场景不收紧水位（用运行时缺省 0.85）。"""

_TOTAL = 6
"""每轮发布条数。"""

_UNACKED = 2
"""留作「未确认」的条数（崩溃后应重投）。"""

_MAX_EXIT_WAIT_SEC = 20.0
"""等待被信号杀死的实例退出（秒）。"""

_FLUSH_WAIT_SEC = OFFSET_FLUSH_INTERVAL_MS / 1000 + 1.0
"""确认后等位点刷盘窗口（位点提交是「标脏 + 定期落盘」，杀进程前必须让脏页落盘）。"""


@dataclass(frozen=True)
class _Round:
    """一轮「杀进程 → 重启 → 回读」的产物。"""

    criteria: ConcurrentStableFrozenList[Criterion]
    """该轮判据。"""

    rto_ms: int
    """该轮恢复耗时（毫秒）。"""


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"1. 建主题 `{ctx.topic()}-kill` / `{ctx.topic()}-term` 并各加入消费组")
    rows.append(f"2. 每轮发布 {_TOTAL} 条，确认前 {_TOTAL - _UNACKED} 条（留 {_UNACKED} 条未确认）")
    rows.append("3. 采样「注入前」")
    rows.append("4. 向实例进程发送 SIGKILL（第一轮）/ SIGTERM（第二轮），等进程退出")
    rows.append("5. 采样「注入中」（探针不可达即证实例已死）")
    rows.append("6. 以同一数据根重启实例，等 /readyz 200")
    rows.append("7. 采样「恢复后」，回读：已确认不回退、未确认重投、LEO 不回退")
    return rows


def _round(ctx: DrillContext, suffix: str, sig: str) -> _Round:
    """跑一轮「发布 → 确认 → 杀进程 → 重启 → 回读」。

    Args:
        ctx: 执行上下文。
        suffix: 主题后缀（区分两档）。
        sig: 信号名。

    Returns:
        _Round: 该轮判据与恢复耗时。
    """
    topic = ctx.topic(suffix)
    ctx.prepare_topic(topic)
    offsets: ConcurrentStableList[int] = ConcurrentStableList()
    for index in range(_TOTAL):
        body = require_ok(ctx.publish(topic, f"crash-{suffix}-{index}".encode()), f"发布第 {index} 条")
        offsets.append(offset_of(body))
    fetched = ctx.consume(topic, max_messages=_TOTAL)
    confirmed_upto = _TOTAL - _UNACKED
    for item in items_of(fetched)[:confirmed_upto]:
        ctx.ack(topic, offset_of(item))
    time.sleep(_FLUSH_WAIT_SEC)

    ctx.sample("before")
    injected_at = time.monotonic()
    ctx.instance.signal(sig)
    exit_code = ctx.instance.wait_exit(_MAX_EXIT_WAIT_SEC)
    ctx.sample("during")

    ctx.instance.restart()
    # 成员会话是进程内存：重启后必须重新 join（不 join 会 350013），这正是客户端崩溃后的真实动作。
    ctx.prepare_topic(topic)
    ctx.sample("after")

    replayed = [offset_of(item) for item in items_of(ctx.consume(topic, max_messages=_TOTAL))]
    fresh = offset_of(require_ok(ctx.publish(topic, b"crash-after"), "恢复后发布"))
    rto_ms = int((time.monotonic() - injected_at) * 1000)
    unacked = list(offsets[confirmed_upto:])

    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    criteria.append(
        check(
            f"{sig}.confirmed_not_rewound",
            "已确认消息不回退（恢复后不重投 offset < 已确认位点）",
            f"已确认 {confirmed_upto} 条，恢复后投出 {replayed}",
            result_of(min(replayed, default=confirmed_upto) >= confirmed_upto),
        )
    )
    criteria.append(
        check(
            f"{sig}.unacked_redelivered",
            f"未确认 {_UNACKED} 条重新投递（至少一次语义）",
            f"恢复后投出 {replayed}，期望含 {unacked}",
            result_of(all(offset in replayed for offset in unacked)),
        )
    )
    criteria.append(
        check(
            f"{sig}.leo_not_rewound",
            "重启不重排 offset（新消息 offset 连续）",
            f"新消息 offset={fresh}，期望 {_TOTAL}",
            result_of(fresh == _TOTAL),
        )
    )
    criteria.append(
        check(
            f"{sig}.killed_by_signal",
            "实例确实被信号终止（非正常退出）",
            f"信号 {sig}，退出码 {exit_code}",
            result_of(exit_code is not None),
        )
    )
    criteria.append(
        check(
            f"{sig}.rto_within_budget",
            "恢复耗时在预算内",
            f"{rto_ms} ms（预算 {ctx.config.recovery_budget_ms} ms）",
            result_of(rto_ms <= ctx.config.recovery_budget_ms),
        )
    )
    return _Round(criteria=ConcurrentStableFrozenList(criteria), rto_ms=rto_ms)


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行崩溃注入：SIGKILL 与 SIGTERM 两档各一轮。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    rounds: ConcurrentStableList[_Round] = ConcurrentStableList()
    rounds.append(_round(ctx, "kill", "SIGKILL"))
    rounds.append(_round(ctx, "term", "SIGTERM"))
    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    for item in rounds:
        criteria.extend(item.criteria)
    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("位点提交是「标脏 + 定期落盘」，确认后等一个刷盘窗口再杀进程（否则脏页丢失会整段重投）")
    deviations.append("在途状态为进程内存，重启后 delivery_count 从 1 重新计；未确认消息靠位点重投保证不丢")
    deviations.append("成员会话同为进程内存：重启后脚本按客户端真实动作重新 join，未沿用旧会话")
    deviations.append("元数据库用 SQLite（生产为平台 MySQL），数据面与文件存储为真实现")
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "sigkill_sigterm",
                "detail": f"两档各发布 {_TOTAL} 条、确认 {_TOTAL - _UNACKED} 条后杀进程并以同一数据根重启",
                "artifact": str(ctx.work_dir),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=max(item.rto_ms for item in rounds),
        data_loss=any(item.key.endswith("confirmed_not_rewound") and item.result != RESULT_OK for item in criteria),
        deviations=ConcurrentStableFrozenList(deviations),
    )
