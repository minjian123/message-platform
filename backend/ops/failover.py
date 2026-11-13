"""分区切主运维入口（02-8）：`switch` / `status`。

**定位**：只做「调用 02-5 切换服务 + 打印结果」，不新增管理端点；集群演练工具
（原 `ops.cluster_drill`，已随部署域剥离）曾复用同一入口触发切主，避免演练脚本另写一套切换逻辑。

退出码：`0` 成功；`1` 失败；`2` 降级（`isr_verified=false`，不计通过）。
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

from bms_msg.broker.election import (
    LeaderSwitchError,
    LeaderSwitchService,
    SwitchBatchResult,
    SwitchPartitionRequest,
    SwitchTopicRequest,
)
from bms_msg.services.partition_migration import MigrationTopology
from ops.cluster_runtime import build_runtime

__all__ = ["main", "run_status", "run_switch"]

EXIT_OK = 0
"""成功。"""

EXIT_FAILED = 1
"""失败。"""

EXIT_DEGRADED = 2
"""降级（`isr_verified=false`）。"""


async def run_switch(
    switch: LeaderSwitchService,
    *,
    topic_id: int,
    partition_no: int | None,
    target: str | None,
    operator: str,
    reason: str,
    force: bool,
    as_json: bool,
) -> int:
    """执行切主（单分区或整主题）。

    Args:
        switch: 02-5 切换服务。
        topic_id: 主题主键。
        partition_no: 分区号（None = 整主题）。
        target: 目标节点（None = 自动选）。
        operator: 执行人。
        reason: 原因。
        force: 是否允许 force（无法验证 ISR 时降级继续）。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码。
    """
    if partition_no is None:
        batch = await switch.switch_topic(
            SwitchTopicRequest(topic_id=topic_id, force=force, operator=operator, reason=reason)
        )
        return _print_batch(batch, as_json=as_json)
    try:
        result = await switch.switch_partition(
            SwitchPartitionRequest(
                topic_id=topic_id,
                partition_no=partition_no,
                target=target,
                force=force,
                operator=operator,
                reason=reason,
            )
        )
    except LeaderSwitchError as exc:
        print(f"切主失败：{exc.reason}（{exc.detail}）", file=sys.stderr)
        return EXIT_FAILED
    payload: ConcurrentStableDict[str, object] = ConcurrentStableDict(
        {
            "topic_id": result.topic_id,
            "partition_no": result.partition_no,
            "from_leader": result.from_leader,
            "to_leader": result.to_leader,
            "from_epoch": result.from_epoch,
            "to_epoch": result.to_epoch,
            "applied": result.applied,
            "watermark": result.watermark,
            "truncated_to": result.truncated_to,
            "retained_tail": result.retained_tail,
            "isr_verified": result.isr_verified,
            "duration_ms": result.duration_ms,
        }
    )
    if as_json:
        _print_json(payload)
    else:
        print(
            f"{result.topic_id}/{result.partition_no} {result.from_leader} → {result.to_leader} "
            f"epoch {result.from_epoch}→{result.to_epoch} 结果="
            f"{'truncated' if result.truncated_to is not None else ('retained' if result.retained_tail else 'noop')} "
            f"isr_verified={result.isr_verified} 耗时={result.duration_ms}ms"
        )
    return EXIT_OK if result.isr_verified else EXIT_DEGRADED


def _print_batch(batch: SwitchBatchResult, *, as_json: bool) -> int:
    """输出整主题切换结果并汇总退出码。

    Args:
        batch: 批量结果。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码。
    """
    if as_json:
        _print_json(
            ConcurrentStableDict(
                {
                    "items": [
                        ConcurrentStableDict(
                            {
                                "topic_id": item.topic_id,
                                "partition_no": item.partition_no,
                                "from_leader": item.from_leader,
                                "to_leader": item.to_leader,
                                "from_epoch": item.from_epoch,
                                "to_epoch": item.to_epoch,
                                "isr_verified": item.isr_verified,
                                "retained_tail": item.retained_tail,
                            }
                        )
                        for item in batch.items
                    ],
                    "failures": [
                        ConcurrentStableDict(
                            {
                                "topic_id": item.topic_id,
                                "partition_no": item.partition_no,
                                "reason": item.reason,
                                "detail": item.detail,
                            }
                        )
                        for item in batch.failures
                    ],
                }
            )
        )
    else:
        for item in batch.items:
            print(
                f"{item.topic_id}/{item.partition_no} {item.from_leader} → {item.to_leader} "
                f"isr_verified={item.isr_verified}"
            )
        for failure in batch.failures:
            print(f"失败 {failure.topic_id}/{failure.partition_no}：{failure.reason}（{failure.detail}）")
    if batch.failures:
        return EXIT_FAILED
    if any(not item.isr_verified for item in batch.items):
        return EXIT_DEGRADED
    return EXIT_OK


async def run_status(topology: MigrationTopology, *, topic_id: int, as_json: bool) -> int:
    """打印主题各分区的 leader / epoch / 切换状态。

    Args:
        topology: 分区事实端口。
        topic_id: 主题主键。
        as_json: 是否输出 JSON。

    Returns:
        int: 退出码（恒 0）。
    """
    partitions = await topology.list_partitions(topic_ids=ConcurrentStableFrozenList((topic_id,)))
    rows: ConcurrentStableList[ConcurrentStableDict[str, object]] = ConcurrentStableList()
    for partition in partitions:
        rows.add(
            ConcurrentStableDict(
                {
                    "topic_id": partition.topic_id,
                    "partition_no": partition.partition_no,
                    "leader": partition.leader,
                    "epoch": partition.epoch,
                    "switch_state": partition.switch_state,
                    "replicas": list(partition.replicas),
                }
            )
        )
    if as_json:
        _print_json(ConcurrentStableDict({"topic_id": topic_id, "partitions": rows}))
    elif not rows:
        print(f"主题 {topic_id} 无分区")
    else:
        print("| 分区 | leader | epoch | 切换状态 | 副本集 |")
        print("| --- | --- | --- | --- | --- |")
        for row in rows:
            replicas = cast("list[str]", row["replicas"])
            print(
                f"| {row['partition_no']} | {row['leader']} | {row['epoch']} | "
                f"{row['switch_state']} | {','.join(replicas)} |"
            )
    return EXIT_OK


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


def _parser() -> argparse.ArgumentParser:
    """构造参数解析器。

    Returns:
        argparse.ArgumentParser: 解析器。
    """
    parser = argparse.ArgumentParser(prog="ops.failover", description="msg 分区切主（02-8）")
    sub = parser.add_subparsers(dest="command", required=True)

    switch = sub.add_parser("switch", help="手动切主（单分区或整主题）")
    switch.add_argument("--topic-id", type=int, required=True, dest="topic_id", help="主题主键")
    group = switch.add_mutually_exclusive_group(required=True)
    group.add_argument("--partition", type=int, dest="partition_no", help="分区号")
    group.add_argument("--all-partitions", action="store_true", dest="all_partitions", help="整主题逐分区切换")
    switch.add_argument("--target", default=None, help="目标节点（缺省自动选 ISR 内 LEO 最大）")
    switch.add_argument("--operator", required=True, help="执行人（演练用 system:drill）")
    switch.add_argument("--reason", required=True, help="切换原因")
    switch.add_argument("--force", action="store_true", help="允许 force（无法验证 ISR 时记降级）")
    switch.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")

    status = sub.add_parser("status", help="查主题各分区 leader / epoch / 切换状态")
    status.add_argument("--topic-id", type=int, required=True, dest="topic_id", help="主题主键")
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
        if args.command == "switch":
            return await run_switch(
                runtime.switch,
                topic_id=int(args.topic_id),
                partition_no=cast("int | None", args.partition_no),
                target=cast("str | None", args.target),
                operator=str(args.operator),
                reason=str(args.reason),
                force=bool(args.force),
                as_json=bool(args.as_json),
            )
        return await run_status(runtime.topology, topic_id=int(args.topic_id), as_json=bool(args.as_json))
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
