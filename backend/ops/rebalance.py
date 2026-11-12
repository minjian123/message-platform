"""分区重平衡运维入口（02-8）：`plan` / `apply` / `status`。

**为什么是 CLI 不是管理 API**：迁移是低频、需人工确认的敏感操作；首期不扩管理面契约
（管理列表 / 二次确认归 06-2 后续），CLI 足以覆盖受控加入 / 下线与演练。

退出码：`0` 成功 / 无动作；`2` 存在阻断、追平中或降级；`1` 存在失败。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Iterable
from typing import cast

from bms_core.core.collections import BaseCollection
from bms_core.core.concurrent import (
    ConcurrentStableDict,
    ConcurrentStableFrozenList,
    ConcurrentStableList,
)

from bms_msg.models.partition_migration import (
    PLAN_STATUS_CATCHING_UP,
    PLAN_STATUS_DEGRADED,
)
from bms_msg.services.partition_migration import (
    MigrationApplyResult,
    MigrationPlanResult,
    MigrationStatusRow,
    MigrationStepError,
    PartitionMigrationService,
)
from ops.cluster_runtime import build_runtime

__all__ = ["main", "run_apply", "run_plan", "run_status"]

EXIT_OK = 0
"""成功 / 无动作。"""

EXIT_FAILED = 1
"""存在失败项。"""

EXIT_DEGRADED = 2
"""存在阻断 / 追平中 / 降级项（不计通过）。"""


def _item_payload(item: object) -> ConcurrentStableDict[str, object]:
    """把计划项 / 台账行转成 JSON 载荷。

    Args:
        item: 计划项或台账行。

    Returns:
        ConcurrentStableDict[str, object]: JSON 对象。
    """
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict()
    for name in (
        "plan_id",
        "topic_id",
        "partition_no",
        "action",
        "step",
        "from_replicas",
        "to_replicas",
        "from_leader",
        "to_leader",
        "status",
        "result",
        "isr_verified",
        "retained_tail",
        "progress_leo",
        "progress_hw",
        "remaining_records",
        "operator",
        "reason",
        "error",
        "attempt",
    ):
        if hasattr(item, name):
            value = getattr(item, name)
            if isinstance(value, ConcurrentStableFrozenList):
                payload.set(name, list(cast("ConcurrentStableFrozenList[str]", value)))
            else:
                payload.set(name, cast("object", value))
    return payload


def _plain(value: object) -> object:
    """把并发集合递归转成内置结构（JSON 可序列化）。

    Args:
        value: 任意值。

    Returns:
        object: 内置结构。
    """
    if isinstance(value, ConcurrentStableDict | dict):
        return {key: _plain(item) for key, item in value.items()}  # type: ignore[union-attr]
    if isinstance(value, BaseCollection):
        return [_plain(item) for item in cast("Iterable[object]", value)]
    if isinstance(value, list | tuple):
        return [_plain(item) for item in cast("Iterable[object]", value)]
    return value


def _print_json(payload: ConcurrentStableDict[str, object]) -> None:
    """输出 JSON（UTF-8 友好、键排序）。

    Args:
        payload: JSON 对象。
    """
    print(json.dumps(_plain(payload), ensure_ascii=False, sort_keys=True, default=str))


async def run_plan(
    service: PartitionMigrationService,
    *,
    nodes: ConcurrentStableFrozenList[str],
    factor: int,
    min_insync: int,
    topic_ids: ConcurrentStableFrozenList[int] | None,
    as_json: bool,
) -> int:
    """执行 `plan`。

    Args:
        service: 迁移编排。
        nodes: 候选节点。
        factor: 副本数。
        min_insync: 最小同步副本数。
        topic_ids: 主题过滤。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码。
    """
    try:
        result = await service.plan(nodes=nodes, factor=factor, min_insync=min_insync, topic_ids=topic_ids)
    except MigrationStepError as exc:
        print(f"plan 失败：{exc.reason}（{exc.detail}）", file=sys.stderr)
        return EXIT_DEGRADED
    if as_json:
        _print_json(
            ConcurrentStableDict(
                {
                    "plan_id": result.plan_id,
                    "items": [_item_payload(item) for item in result.items],
                    "blocks": [
                        ConcurrentStableDict(
                            {
                                "topic_id": block.topic_id,
                                "partition_no": block.partition_no,
                                "reason": block.reason,
                                "detail": block.detail,
                            }
                        )
                        for block in result.blocks
                    ],
                }
            )
        )
    else:
        _print_plan(result)
    return EXIT_DEGRADED if result.blocks else EXIT_OK


def _print_plan(result: MigrationPlanResult) -> None:
    """打印计划动作表与阻断项。

    Args:
        result: 计划结果。
    """
    print(f"plan_id: {result.plan_id}")
    if not result.items and not result.blocks:
        print("无动作（副本集与 leader 已符合目标）")
        return
    if result.items:
        print("| 主题 | 分区 | 动作 | 原副本集 | 目标副本集 | 原 leader | 目标 leader |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
        for item in result.items:
            print(
                f"| {item.topic_id} | {item.partition_no} | {item.action} | "
                f"{','.join(item.from_replicas)} | {','.join(item.to_replicas)} | "
                f"{item.from_leader} | {item.to_leader} |"
            )
    if result.blocks:
        print("阻断项：")
        for block in result.blocks:
            print(f"- {block.topic_id}/{block.partition_no} {block.reason}：{block.detail}")


async def run_apply(
    service: PartitionMigrationService,
    *,
    plan_id: str,
    operator: str,
    reason: str,
    wait_ms: int | None,
    allow_force: bool,
    allow_degraded: bool,
    resume: bool,
    as_json: bool,
) -> int:
    """执行 `apply`。

    Args:
        service: 迁移编排。
        plan_id: 计划标识。
        operator: 执行人。
        reason: 原因。
        wait_ms: 追平预算。
        allow_force: 是否允许 force 切主。
        allow_degraded: 是否继续执行降级项。
        resume: 是否续跑中断项。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码。
    """
    result = await service.apply(
        plan_id=plan_id,
        operator=operator,
        reason=reason,
        wait_ms=wait_ms,
        allow_force=allow_force,
        allow_degraded=allow_degraded,
        resume=resume,
    )
    if as_json:
        _print_json(
            ConcurrentStableDict(
                {
                    "plan_id": result.plan_id,
                    "succeeded": result.succeeded,
                    "degraded": result.degraded,
                    "failed": result.failed,
                    "catching_up": result.catching_up,
                    "items": [_item_payload(item) for item in result.items],
                }
            )
        )
    else:
        _print_apply(result)
    return _exit_code(result)


def _print_apply(result: MigrationApplyResult) -> None:
    """打印执行结果。

    Args:
        result: 执行结果。
    """
    print(
        f"plan_id={result.plan_id} 成功={result.succeeded} 降级={result.degraded} "
        f"失败={result.failed} 追平中={result.catching_up}"
    )
    for item in result.items:
        print(
            f"- {item.topic_id}/{item.partition_no} {item.action} {item.status} "
            f"{item.error or item.result or ''}".rstrip()
        )


def _exit_code(result: MigrationApplyResult) -> int:
    """按台账终态汇总退出码。

    Args:
        result: 执行结果。

    Returns:
        int: 退出码。
    """
    if result.failed:
        return EXIT_FAILED
    relevant = [item for item in result.items if item.status != "planned"]
    if any(item.status in (PLAN_STATUS_CATCHING_UP, PLAN_STATUS_DEGRADED) for item in relevant):
        return EXIT_DEGRADED
    return EXIT_OK


async def run_status(service: PartitionMigrationService, *, plan_id: str | None, as_json: bool) -> int:
    """执行 `status`。

    Args:
        service: 迁移编排。
        plan_id: 计划标识（None = 最近一个）。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码（恒 0）。
    """
    rows = await service.status(plan_id=plan_id)
    if as_json:
        _print_json(
            ConcurrentStableDict(
                {
                    "plan_id": plan_id or "",
                    "rows": [
                        ConcurrentStableDict(
                            {
                                **_item_payload(row.item),
                                "leader_leo": row.leader_leo,
                                "leader_hw": row.leader_hw,
                                "remaining": row.remaining,
                                "caught_up": row.caught_up,
                            }
                        )
                        for row in rows
                    ],
                }
            )
        )
    else:
        _print_status(rows)
    return EXIT_OK


def _print_status(rows: ConcurrentStableList[MigrationStatusRow]) -> None:
    """打印状态表。

    Args:
        rows: 状态行。
    """
    if not rows:
        print("无台账记录")
        return
    print("| 计划 | 主题 | 分区 | 动作 | 状态 | 剩余条数 | 是否追平 | 错误 |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        print(
            f"| {row.item.plan_id} | {row.item.topic_id} | {row.item.partition_no} | {row.item.action} | "
            f"{row.item.status} | {row.remaining} | {'是' if row.caught_up else '否'} | {row.item.error} |"
        )


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.rebalance", description="msg 分区重平衡（02-8）")
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="生成迁移计划（只读运行态）")
    plan.add_argument("--nodes", required=True, help="候选节点（逗号分隔）")
    plan.add_argument("--factor", type=int, required=True, help="副本数")
    plan.add_argument("--min-insync", type=int, required=True, dest="min_insync", help="最小同步副本数")
    plan.add_argument("--topic-id", action="append", type=int, dest="topic_ids", help="主题主键（可重复）")
    plan.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")

    apply_parser = sub.add_parser("apply", help="执行计划（逐分区串行）")
    apply_parser.add_argument("--plan-id", required=True, dest="plan_id", help="计划标识")
    apply_parser.add_argument("--operator", required=True, help="执行人")
    apply_parser.add_argument("--reason", required=True, help="执行原因")
    apply_parser.add_argument("--wait-ms", type=int, dest="wait_ms", default=None, help="追平预算（毫秒）")
    apply_parser.add_argument(
        "--allow-force", action="store_true", dest="allow_force", help="允许 force 切主（记降级）"
    )
    apply_parser.add_argument(
        "--allow-degraded", action="store_true", dest="allow_degraded", help="继续执行其余分区（降级仍不计通过）"
    )
    apply_parser.add_argument("--resume", action="store_true", help="续跑中断项")
    apply_parser.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")

    status = sub.add_parser("status", help="查台账并刷新追平进度")
    status.add_argument("--plan-id", dest="plan_id", default=None, help="计划标识（缺省最近一个）")
    status.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")
    return parser


async def _run(args: argparse.Namespace) -> int:
    """按子命令执行（装配运行时）。

    Args:
        args: 命令行参数。

    Returns:
        int: 退出码。
    """
    runtime = await build_runtime()
    try:
        if args.command == "plan":
            nodes = ConcurrentStableFrozenList(item.strip() for item in str(args.nodes).split(",") if item.strip())
            topic_ids = ConcurrentStableFrozenList(int(item) for item in args.topic_ids) if args.topic_ids else None
            return await run_plan(
                runtime.migration,
                nodes=nodes,
                factor=int(args.factor),
                min_insync=int(args.min_insync),
                topic_ids=topic_ids,
                as_json=bool(args.as_json),
            )
        if args.command == "apply":
            return await run_apply(
                runtime.migration,
                plan_id=str(args.plan_id),
                operator=str(args.operator),
                reason=str(args.reason),
                wait_ms=cast("int | None", args.wait_ms),
                allow_force=bool(args.allow_force),
                allow_degraded=bool(args.allow_degraded),
                resume=bool(args.resume),
                as_json=bool(args.as_json),
            )
        return await run_status(runtime.migration, plan_id=cast("str | None", args.plan_id), as_json=bool(args.as_json))
    finally:
        await runtime.aclose()


def main(argv: ConcurrentStableList[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（None 取 `sys.argv[1:]`）。

    Returns:
        int: 退出码。
    """
    parser = _parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else list(argv))
    return asyncio.run(_run(args))


if __name__ == "__main__":  # pragma: no cover - 命令行入口
    raise SystemExit(main())
