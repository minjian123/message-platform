"""场景五 · 消费者卡死（任务 07-6-1）：成员只拉不确认，填满 inflight 窗口。

要证明两件事：积压对消费端**可见**（`paused=true` + 用量字段），背压让内存**有界**
（在途条数不越过上限）；恢复确认后积压归零且不重复消费。
"""

from __future__ import annotations

from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import INFLIGHT_MAX_MESSAGES
from ops.drill.context import (
    DrillContext,
    ScenarioOutcome,
    check,
    items_of,
    offset_of,
    require_ok,
    result_of,
)
from ops.drill.model import Criterion

needs_tight_watermark = False
"""卡死场景不收紧水位。"""

_PUBLISH_COUNT = INFLIGHT_MAX_MESSAGES + 5
"""发布条数：略多于在途上限，才可能触发 `paused`。"""


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"1. 建主题 `{ctx.topic()}` 并加入消费组，采样「注入前」")
    rows.append(f"2. 发布 {_PUBLISH_COUNT} 条（略多于在途上限 {INFLIGHT_MAX_MESSAGES}）")
    rows.append("3. 消费端只拉取不确认，直到 `paused=true`（在途填满）")
    rows.append("4. 采样「注入中」：积压可见、在途不超过上限")
    rows.append("5. 逐条确认全部在途消息，采样「恢复后」")
    rows.append("6. 判据：背压生效、恢复后可拉取为空（积压归零）")
    return rows


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行卡死注入并验证背压与恢复。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    topic = ctx.topic()
    ctx.prepare_topic(topic)
    before = ctx.sample("before")

    for index in range(_PUBLISH_COUNT):
        require_ok(ctx.publish(topic, f"stuck-{index}".encode()), f"发布第 {index} 条")
    pulled: ConcurrentStableList[int] = ConcurrentStableList()
    paused = False
    inflight = 0
    for _ in range(3):
        result = ctx.consume(topic, max_messages=INFLIGHT_MAX_MESSAGES)
        for item in items_of(result):
            pulled.append(offset_of(item))
        inflight = max(inflight, cast("int", result.get("inflight_messages") or 0))
        if bool(result.get("paused")):
            paused = True
            break
    during = ctx.sample("during")

    for offset in pulled:
        ctx.ack(topic, offset)
    leftover = items_of(ctx.consume(topic, max_messages=INFLIGHT_MAX_MESSAGES))
    for item in leftover:
        ctx.ack(topic, offset_of(item))
    ctx.sample("after")
    drained = items_of(ctx.consume(topic, max_messages=INFLIGHT_MAX_MESSAGES))

    lag_before = before.metrics.get("msg_consumer_lag", 0.0)
    lag_during = during.metrics.get("msg_consumer_lag", 0.0)
    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    criteria.append(
        check(
            "backpressure_applied",
            "在途填满后 consume 返回 `paused=true`（背压生效）",
            f"paused={paused}、inflight_messages={inflight}（上限 {INFLIGHT_MAX_MESSAGES}）",
            result_of(paused and 0 < inflight <= INFLIGHT_MAX_MESSAGES),
        )
    )
    criteria.append(
        check(
            "inflight_bounded",
            "在途条数不超过上限（内存有界）",
            f"inflight_messages={inflight} ≤ {INFLIGHT_MAX_MESSAGES}",
            result_of(inflight <= INFLIGHT_MAX_MESSAGES),
        )
    )
    criteria.append(
        check(
            "backlog_visible",
            "积压可观测（在途条数 > 0 或 `msg_consumer_lag` 上升）",
            f"在途 {inflight} 条、lag {lag_before:.0f} → {lag_during:.0f}",
            result_of(inflight > 0 or lag_during > lag_before),
        )
    )
    criteria.append(
        check(
            "resumed_after_ack",
            "确认后恢复消费且积压归零（不重复消费）",
            f"确认 {len(pulled) + len(leftover)} 条，残留 {len(drained)} 条",
            result_of(not drained),
        )
    )
    criteria.append(
        check(
            "no_duplicate",
            "恢复期投递不重复（offset 唯一且单调）",
            f"投出 {len(pulled)} 条，唯一 {len(set(pulled))} 条",
            result_of(len(pulled) == len(set(pulled))),
        )
    )

    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("单分区单消费者（多成员分配与再平衡不在本类判据内）")
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "unacked_inflight",
                "detail": f"发布 {_PUBLISH_COUNT} 条后只拉不确认，填满在途上限 {INFLIGHT_MAX_MESSAGES}",
                "artifact": str(ctx.work_dir),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=0,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(deviations),
    )
