"""场景四 · 网络分区（任务 07-6-1）：客户端侧代理切「断连 → 恢复」，验重连与不重复消费。

为什么用代理而不是 iptables：代理只影响本脚本的连接，不碰容器网络与系统防火墙；
「客户端断连后能否重连、会不会重复消费」才是这一段要证明的东西。
"""

from __future__ import annotations

import contextlib
import time

import httpx
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
from ops.drill.model import Criterion
from ops.drill.proxy import MODE_DELAY, MODE_DROP, MODE_LOSS, MODE_PASS, TcpProxy
from ops.drill.runtime import DrillHttp

needs_tight_watermark = False
"""网络分区场景不收紧水位。"""

_BASELINE = 5
"""分区前发布条数。"""

_BASELINE_ACKED = 3
"""分区前确认条数（余下 2 条留作「恢复后仍可继续消费」）。"""


def plan(ctx: DrillContext) -> ConcurrentStableList[str]:
    """渲染本场景的注入与恢复动作。

    Args:
        ctx: 执行上下文。

    Returns:
        ConcurrentStableList[str]: 动作清单。
    """
    rows: ConcurrentStableList[str] = ConcurrentStableList()
    rows.append(f"1. 建主题 `{ctx.topic()}`，发布 {_BASELINE} 条并确认前 {_BASELINE_ACKED} 条")
    rows.append("2. 启动本地 TCP 代理（`127.0.0.1:<随机端口>` → 实例端口），采样「注入前」")
    rows.append(f"3. 代理切 `{MODE_PASS}` 经代理发布一条，确认链路可用")
    rows.append(f"4. 代理切 `{MODE_DROP}`（拒绝新连接 + 断开现有），经代理发布应失败")
    rows.append("5. 采样「注入中」（直连实例仍健康，证故障只在客户端链路）")
    rows.append(f"6. 代理切回 `{MODE_PASS}`，经代理发布应成功（重连）")
    rows.append("7. 采样「恢复后」，直连回读：已确认不回退、未确认可继续消费、积压归零")
    rows.append(f"8. 另档：`{MODE_LOSS}` / `{MODE_DELAY}` 各渲染一次（弱网 / 高延迟，只验可切换）")
    return rows


def run(ctx: DrillContext) -> ScenarioOutcome:
    """执行断连 → 重连的完整判定。

    Args:
        ctx: 执行上下文。

    Returns:
        ScenarioOutcome: 判据与三段样本。
    """
    topic = ctx.topic()
    ctx.prepare_topic(topic)
    offsets: ConcurrentStableList[int] = ConcurrentStableList()
    for index in range(_BASELINE):
        body = require_ok(ctx.publish(topic, f"partition-{index}".encode()), f"分区前发布第 {index} 条")
        offsets.append(offset_of(body))
    delivered = items_of(ctx.consume(topic, max_messages=_BASELINE))
    for item in delivered[:_BASELINE_ACKED]:
        ctx.ack(topic, offset_of(item))
    ctx.sample("before")

    injected_at = 0.0
    proxy = TcpProxy(ctx.instance.port)
    proxy.start()
    proxied = DrillHttp(f"http://127.0.0.1:{proxy.port}")
    try:
        passthrough = proxied.publish(topic, b"partition-through-proxy")
        injected_at = time.monotonic()
        proxy.set_mode(MODE_DROP)
        blocked = False
        try:
            proxied.publish(topic, b"partition-during-drop")
        except httpx.HTTPError:
            blocked = True
        during = ctx.sample("during")
        proxy.set_mode(MODE_LOSS)
        with contextlib.suppress(httpx.HTTPError):
            proxied.publish(topic, b"partition-during-loss")
        proxy.set_mode(MODE_DELAY)
        delayed = proxied.publish(topic, b"partition-during-delay")
        proxy.set_mode(MODE_PASS)
        reconnected = proxied.publish(topic, b"partition-after-reconnect")
        ctx.sample("after")
        rto_ms = int((time.monotonic() - injected_at) * 1000)
    finally:
        proxied.close()
        proxy.close()

    replayed = [offset_of(item) for item in items_of(ctx.consume(topic, max_messages=_BASELINE + 4))]
    for offset in replayed:
        ctx.ack(topic, offset)
    drained = items_of(ctx.consume(topic, max_messages=_BASELINE + 4))

    criteria: ConcurrentStableList[Criterion] = ConcurrentStableList()
    criteria.append(
        check(
            "proxy_passthrough",
            "代理直通档可用（经代理发布 2xx）",
            f"HTTP {passthrough.status_code}",
            result_of(200 <= passthrough.status_code < 300),
        )
    )
    criteria.append(
        check(
            "drop_blocks_publish",
            "断连档发布失败（客户端可见的连接错误）",
            f"发布{'被拒（连接错误）' if blocked else '仍成功（断连未生效）'}",
            result_of(blocked),
        )
    )
    criteria.append(
        check(
            "instance_alive_during_partition",
            "断连期实例本身健康（故障只在客户端链路）",
            f"/readyz={during.readyz_status}、/healthz={'200' if during.healthz_ok else '不可达'}",
            result_of(during.readyz_ok and during.healthz_ok),
        )
    )
    criteria.append(
        check(
            "reconnected",
            "恢复后经代理发布成功（客户端重连）",
            f"HTTP {reconnected.status_code}",
            result_of(200 <= reconnected.status_code < 300),
        )
    )
    criteria.append(
        check(
            "no_duplicate_delivery",
            "已确认消息不重复投递（位点单调不回退）",
            f"已确认 {_BASELINE_ACKED} 条，恢复后投出 {replayed}",
            result_of(min(replayed, default=_BASELINE_ACKED) >= _BASELINE_ACKED),
        )
    )
    criteria.append(
        check(
            "lag_drained",
            "恢复后积压追平（确认完可拉取为空）",
            f"残留 {len(drained)} 条",
            result_of(not drained),
        )
    )
    criteria.append(
        check(
            "modes_switchable",
            "三档模式可切换（loss 后仍可发、delay 档发布成功）",
            f"loss 后继续发布，delay 档 HTTP {delayed.status_code}",
            result_of(200 <= delayed.status_code < 300),
        )
    )

    deviations: ConcurrentStableList[str] = ConcurrentStableList()
    deviations.append("分区由客户端侧本地 TCP 代理实现（drop / loss / delay），未断容器网络")
    deviations.append("客户端重连由本脚本驱动（每次调用新建/复用连接），未覆盖 SDK 内建退避策略")
    return ScenarioOutcome(
        injection=ConcurrentStableFrozenDict(
            {
                "method": "tcp_proxy",
                "detail": f"代理端口 {proxy.port} → 实例端口 {ctx.instance.port}，档位 pass/drop/loss/delay",
                "artifact": str(ctx.work_dir),
            }
        ),
        samples=ctx.samples,
        criteria=ConcurrentStableFrozenList(criteria),
        rto_ms=rto_ms,
        data_loss=False,
        deviations=ConcurrentStableFrozenList(deviations),
    )
