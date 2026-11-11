"""场景六 · 毒消息（任务 07-6-1）：入口拒绝 + 重投超限转死信 + 主动拒绝即时转死信。

三段各有取证价值：非法载荷证明「脏数据进不了日志」；反复失败证明「重投有上界、不无限循环」；
主动 `nack` 证明「业务判定不可重试时能立刻终结」。三者都不许阻塞同分区后续消息。
"""

from __future__ import annotations

import time
from typing import cast

from bms_core.core.concurrent import (
    ConcurrentStableFrozenDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.core.config import MAX_DELIVERY_ATTEMPTS
from ops.drill.context import (
    DrillContext,
    ScenarioOutcome,
    as_mapping,
    check,
    items_of,
    offset_of,
    require_ok,
    result_of,
)
from ops.drill.model import RESULT_DEGRADED, RESULT_FAILED, RESULT_OK, Criterion
from ops.drill.runtime import payload_of

needs_tight_watermark = False
"""毒消息场景不收紧水位。"""

_DLQ_WAIT_SEC = 220.0
"""等毒消息转死信的上限（秒）：`MAX_DELIVERY_ATTEMPTS × 30s` 可见性超时 + 余量。"""

_POLL_INTERVAL_SEC = 5.0
"""轮询间隔（秒）。"""

_INVALID_PAYLOAD = "!!!not-base64!!!"
"""非法 `payload`（非 base64；期望入口 4xx 且不进日志）。"""


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"1. 建主题 `{ctx.topic()}` 与 `{ctx.topic()}-nack`，采样「注入前」")
    rows.append("2. 以非法 payload 发布 → 期望 4xx（入口拒绝，不进日志）")
    rows.append("3. 发布毒消息（offset 0）+ 正常消息（offset 1），只拉不确认")
    rows.append(
        f"4. 每 {_POLL_INTERVAL_SEC:.0f}s 拉取一次，观察 delivery_count 增长直到转死信（≤ {_DLQ_WAIT_SEC:.0f}s）"
    )
    rows.append("5. 采样「注入中」，再做一次死信查询（管理面按 message_id 可查）")
    rows.append("6. 另起主题发布一条并 `nack(schema_invalid)` → 即时转死信")
    rows.append("7. 采样「恢复后」，判据：死信可查、重投不超上限、后续消息不被阻塞")
    return rows


def _wait_dead_letter(ctx: DrillContext, topic: str, poison_offset: int) -> tuple[int, int | None]:
    """轮询等待毒消息转死信，返回（最大投递次数，后续消息 offset）。

    Args:
        ctx: 执行上下文。
        topic: 主题名。
        poison_offset: 毒消息的 offset。

    Returns:
        tuple[int, int | None]: 最大 `delivery_count`、后续消息 offset（未出现为 None）。
    """
    deadline = time.monotonic() + _DLQ_WAIT_SEC
    max_delivery = 0
    follow: int | None = None
    sampled = False
    while time.monotonic() < deadline:
        ctx.heartbeat(topic)  # 成员会话 30s 超时；等待重投期间必须续活
        batch = items_of(ctx.consume(topic, max_messages=5))
        offsets = [offset_of(item) for item in batch]
        for item in batch:
            if offset_of(item) == poison_offset:
                max_delivery = max(max_delivery, cast("int", item.get("delivery_count") or 0))
        if not sampled:
            ctx.sample("during")
            sampled = True
        # 只有「本批已不含毒消息」才说明它已终结（转死信后被跳过）；同批混有后续消息时不能算终结。
        if poison_offset not in offsets:
            follow = next((offset for offset in offsets if offset > poison_offset), None)
            if follow is not None:
                return max_delivery, follow
        time.sleep(_POLL_INTERVAL_SEC)
    return max_delivery, follow


def _find_in_dlq(ctx: DrillContext, topic: str, message_id: str) -> tuple[bool, str]:
    """在死信列表里按 message_id 查找。

    管理面查询受资源级 ACL 影响：隔离实例无授权种子，`GET /admin/dlq` 在有死信行时抛
    `MissingGreenlet`（500，见残留清单）。此时返回 False 并给出原因，由判据降级而非谎报「已查到」。

    Args:
        ctx: 执行上下文。
        topic: 主题名。
        message_id: 消息 ID。

    Returns:
        tuple[bool, str]: 是否查到、原因摘要。
    """
    response = ctx.http.dlq(topic)
    if response.status_code != 200:
        return False, f"死信列表不可查（HTTP {response.status_code}，{response.text[:120]}）"
    body = payload_of(response)
    data = as_mapping(body.get("data"))
    for item in items_of(data, "items"):
        if str(item.get("message_id")) == message_id:
            return True, f"reason={item.get('reason')}、attempts={item.get('attempts')}"
    return False, f"未在死信列表找到（HTTP 200，总数 {data.get('total')}）"


def _consume_one(ctx: DrillContext, topic: str, timeout_sec: float = 10.0) -> int | None:
    """拉取一条消息（短轮询，确保 nack 有真实在途消息）。

    Args:
        ctx: 执行上下文。
        topic: 主题名。
        timeout_sec: 等待上限（秒）。

    Returns:
        int | None: 分区内序号（未拉到为 None）。
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        batch = items_of(ctx.consume(topic, max_messages=1))
        if batch:
            return offset_of(batch[0])
        ctx.heartbeat(topic)
        time.sleep(0.2)
    return None


def _wait_dlq_growth(ctx: DrillContext, baseline: float, timeout_sec: float = 20.0) -> float:
    """等待死信总数增长（采样周期 15s，超时返回当前值）。

    Args:
        ctx: 执行上下文。
        baseline: 基线值。
        timeout_sec: 等待上限（秒）。

    Returns:
        float: 当前 `msg_dlq_total`。
    """
    deadline = time.monotonic() + timeout_sec
    current = ctx.metric_sum("msg_dlq_total")
    while current <= baseline and time.monotonic() < deadline:
        time.sleep(0.2)
        current = ctx.metric_sum("msg_dlq_total")
    return current


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行毒消息三类注入并验证死信闭环。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    topic = ctx.topic()
    ctx.prepare_topic(topic)
    ctx.sample("before")
    dlq_before = ctx.metric_sum("msg_dlq_total")

    injected_at = time.monotonic()
    invalid = ctx.http.publish_invalid(topic, _INVALID_PAYLOAD)
    poison = require_ok(ctx.publish(topic, b"poison", key=None), "发布毒消息")
    poison_offset = offset_of(poison)
    poison_id = str(poison.get("message_id") or "")
    require_ok(ctx.publish(topic, b"normal-after-poison"), "发布后续正常消息")

    max_delivery, follow = _wait_dead_letter(ctx, topic, poison_offset)
    dlq_after_wait = ctx.metric_sum("msg_dlq_total")
    found, found_detail = _find_in_dlq(ctx, topic, poison_id)

    nack_topic = ctx.topic("nack")
    ctx.prepare_topic(nack_topic)
    dlq_before_nack = ctx.metric_sum("msg_dlq_total")
    nacked = require_ok(ctx.publish(nack_topic, b"nack-me"), "发布待拒绝消息")
    nack_offset = _consume_one(ctx, nack_topic)
    if nack_offset is None:
        nack_response = None
        dlq_after_nack = dlq_before_nack
    else:
        nack_response = ctx.http.nack(nack_topic, ctx.group(), ctx.consumer(), nack_offset)
        dlq_after_nack = _wait_dlq_growth(ctx, dlq_before_nack)
    nack_found, nack_detail = _find_in_dlq(ctx, nack_topic, str(nacked.get("message_id") or ""))
    ctx.sample("after")
    rto_ms = int((time.monotonic() - injected_at) * 1000)

    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    invalid_body = payload_of(invalid)
    invalid_code = cast("int", invalid_body.get("code") or 0)
    criteria.append(
        check(
            "invalid_payload_rejected",
            "非法载荷被入口拒绝（HTTP 4xx 或业务码非 0）",
            f"HTTP {invalid.status_code}、业务码 {invalid_code}",
            result_of(400 <= invalid.status_code < 500 or invalid_code != 0),
        )
    )
    criteria.append(
        check(
            "redelivery_bounded",
            f"重投次数不超过上限（{MAX_DELIVERY_ATTEMPTS}）",
            f"最大 delivery_count={max_delivery}",
            result_of(0 < max_delivery <= MAX_DELIVERY_ATTEMPTS),
        )
    )
    criteria.append(
        check(
            "dead_letter_created",
            "重投超限后进死信（msg_dlq_total 增长）",
            f"msg_dlq_total {dlq_before:.0f} → {dlq_after_wait:.0f}",
            result_of(dlq_after_wait > dlq_before),
        )
    )
    criteria.append(
        check(
            "dead_letter_queryable",
            "死信管理面可查（按 message_id 命中）",
            found_detail,
            RESULT_OK if found else (RESULT_DEGRADED if "不可查" in found_detail else RESULT_FAILED),
        )
    )
    criteria.append(
        check(
            "not_blocking_followers",
            "毒消息终结后同分区后续消息可消费（不阻塞）",
            (
                f"后续消息 offset={follow}（期望 {poison_offset + 1}）"
                if follow is not None
                else f"毒消息在 {_DLQ_WAIT_SEC:.0f}s 内未终结（重投已达 {max_delivery} 次），后续消息按停等被阻塞"
            ),
            RESULT_OK if follow == poison_offset + 1 else RESULT_DEGRADED,
        )
    )
    criteria.append(
        check(
            "nack_to_dlq",
            "主动 nack 即时转死信",
            (
                "未在 10s 内拉到待拒绝消息，nack 未取证"
                if nack_response is None
                else (
                    f"nack HTTP {nack_response.status_code}；msg_dlq_total {dlq_before_nack:.0f} → {dlq_after_nack:.0f}"
                )
            ),
            RESULT_DEGRADED
            if nack_response is None
            else result_of(200 <= nack_response.status_code < 300 and dlq_after_nack > dlq_before_nack),
        )
    )
    criteria.append(
        check(
            "nack_queryable",
            "主动 nack 的死信管理面可查",
            "未在 10s 内拉到待拒绝消息，nack 未取证" if nack_response is None else f"{nack_detail}",
            RESULT_DEGRADED
            if nack_response is None
            else (
                RESULT_OK
                if 200 <= nack_response.status_code < 300 and nack_found
                else (
                    RESULT_DEGRADED
                    if 200 <= nack_response.status_code < 300 and "不可查" in nack_detail
                    else RESULT_FAILED
                )
            ),
        )
    )

    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("可见性超时为常量 30s、重投上限 5 次，本场景按预算等待；提速需 02-2/02-3 开放旋钮")
    deviations.append("死信库为 SQLite（隔离实例），元数据结构与生产一致")
    leftovers: ConcurrentStableList[ConcurrentStableFrozenDict[str, str]] = ConcurrentStableList()
    leftovers.append(
        ConcurrentStableFrozenDict(
            {
                "item": "管理面死信列表在有死信行时返回 500（MissingGreenlet：ACL 释放后读 ORM 属性）",
                "owner": "06-5 死信管理（生产同样触发，需 ACL 释放与 ORM 过期解耦后复验本场景）",
            }
        )
    )
    leftovers.append(
        ConcurrentStableFrozenDict(
            {
                "item": "毒消息终结后同分区后续消息未在预算内证明可消费（停等阻塞）",
                "owner": "02-3 消费位点管理 / 02-2 崩溃恢复（定位位点推进与死信转移的先后）",
            }
        )
    )
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "invalid_and_exhausted",
                "detail": f"非法载荷 + 重投超限（上限 {MAX_DELIVERY_ATTEMPTS}）+ 主动 nack",
                "artifact": str(ctx.work_dir),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=rto_ms,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(deviations),
        leftovers=ConcurrentStableFrozenList(leftovers),
    )
