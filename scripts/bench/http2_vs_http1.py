#!/usr/bin/env python3
"""HTTP/2 传输基准：同机、同数据量、同并发、同预热下比较「HTTP/1.1 + JSON」与「h2c + JSON」。

任务 03-1 的判据（8B_01 go/no-go）：**吞吐 ≥ 基线 2 倍，且 p99 ≤ 50ms**。

用法::

    # 服务已在 8000 起（msg 服务：python -m bms_msg）
    python scripts/bench/http2_vs_http1.py --base-url http://127.0.0.1:8000 --ensure-topic
    python scripts/bench/http2_vs_http1.py --total 20000 --concurrency 64 --payload-bytes 512
    python scripts/bench/http2_vs_http1.py --json .cache/bench/http2.json

口径与边界：

- **两侧同一个客户端库**（httpx）：换客户端会让「相对比值」失去意义——比的必须是传输层，不是压测器。
  Python 客户端在高并发下可能先成为瓶颈，此时比值会被压平；脚本会打印客户端侧吞吐，便于判断是否打满。
- **h2c 必须 prior knowledge**：只有 `http2=True, http1=False` 才会发 HTTP/2 preface（仅 `http2=True`
  仍协商 HTTP/1.1，bms 02 篇实测）。两侧分别是 `/api/v1/msg/publish`（基线）与 `/msg/publish`（帧入口）。
- **只测发布**：发布是数据面唯一有写语义的路径，且两侧语义与错误码完全一致（同一 handler），
  吞吐与延迟差异只可能来自传输层。消费链路涉及成员制与停等，不适合做吞吐对比。
- **失败必带归因**：每次失败记「HTTP 状态码 / 业务码 / 异常类型」并留少量原文（响应体前 160 字符或异常
  `repr`），预热失败单独计数。只记数字无法定性「真缺陷 vs 装置问题」——03-1 收口的 9 个失败即因此悬空。
- **单连接按预算分轮**：服务端 hypercorn 默认 `keep_alive_max_requests=1000`，单条 h2 连接跑满 1000 条
  即 GOAWAY 退休；客户端对在途流不重试，会记成传输失败。脚本按 `--connection-requests`（缺省 800）
  在**轮边界**换连接，把连接退休挡在测量范围外（详见 `_run_phase`）。
- **不达标不调参**：结论如实打印；不达标按规划 §7 触发刹车评审（备选方案须另拍板）。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

DEFAULT_TOPIC = "bench.http2"
"""基准主题（`--ensure-topic` 时自动创建）。"""

DEFAULT_NAMESPACE = "msg"
"""命名空间（须为已启用 module_key）。"""

THROUGHPUT_RATIO_TARGET = 2.0
"""吞吐判据：对比档 ≥ 基线的 2 倍。"""

P99_TARGET_MS = 50.0
"""延迟判据：p99 ≤ 50ms。"""

CONNECTION_REQUEST_BUDGET = 800
"""单条连接承载的请求上限（**必须小于服务端退休阈值**）。

hypercorn 的 `keep_alive_max_requests` 默认 1000：单条 h2 连接跑满 1000 条即 GOAWAY 退休，
httpx 对在途 stream 不重试（`WriteError` / `RemoteProtocolError(<ConnectionTerminated>)`）。
留 200 条余量，把「连接退休」挡在测量范围之外（详见 `_run_phase`）。HTTP/1.1 档同口径分轮，
两侧只差协议，装置行为一致。
"""


FAILURE_SAMPLES_MAX = 5
"""每档保留的失败样本条数（只作排障上下文，不参与统计）。"""


@dataclass
class Result:
    """一个档位的测量结果。

    失败**不只计数**：`failure_reasons` 按「HTTP 状态码 / 业务码 / 异常类型」归类计数，
    `failure_samples` 留少量原文（含响应体前若干字符），供定性「真缺陷 vs 装置问题」。
    03-1 收口的 9 个失败在首版报告里只有数字、无法归因，因此把归因能力做进脚本。
    """

    label: str
    path: str
    http2: bool
    total: int = 0
    elapsed_s: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)
    failures: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)
    failure_samples: list[str] = field(default_factory=list)
    warmup_failures: int = 0

    def record_failure(self, reason: str, detail: str) -> None:
        """记一次失败：归类计数 + 限量留样。

        Args:
            reason: 归类键（`HTTP 502 / code None`、`HTTPError ReadTimeout` 之类）。
            detail: 原文（状态码 + 响应体前若干字符，或异常 `repr`）。
        """
        self.failures += 1
        self.failure_reasons[reason] = self.failure_reasons.get(reason, 0) + 1
        if len(self.failure_samples) < FAILURE_SAMPLES_MAX:
            self.failure_samples.append(detail)

    @property
    def throughput(self) -> float:
        """吞吐（条/秒）。

        Returns:
            float: 成功条数 / 用时；用时为 0 时返回 0。
        """
        return self.total / self.elapsed_s if self.elapsed_s > 0 else 0.0

    def percentile(self, ratio: float) -> float:
        """延迟分位值（毫秒）。

        Args:
            ratio: 分位（0~1），如 0.99。

        Returns:
            float: 分位延迟；无样本时返回 0。
        """
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        index = min(len(ordered) - 1, max(0, round(ratio * (len(ordered) - 1))))
        return ordered[index]


async def ensure_topic(client: httpx.AsyncClient, *, namespace: str, topic: str, tenant: str) -> None:
    """确保主题存在（已存在不算失败）。

    Args:
        client: 客户端（HTTP/1.1 即可）。
        namespace: 命名空间。
        topic: 主题名。
        tenant: 租户编码（`X-Tenant-ID`）。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": namespace, "name": topic, "partition_count": 1},
        headers={"X-Tenant-ID": tenant},
    )
    code = response.json().get("code")
    if response.status_code == 200:
        print(f"[bench] 已创建主题 {namespace}/{topic}")
        return
    if code == 350008:
        print(f"[bench] 主题已存在：{namespace}/{topic}")
        return
    raise SystemExit(f"[bench] 建主题失败：HTTP {response.status_code} {response.text[:200]}")


def _body(*, namespace: str, topic: str, payload: str, index: int) -> dict[str, object]:
    """构造一次发布请求体（key 变化以分散分区路由开销）。

    Args:
        namespace: 命名空间。
        topic: 主题名。
        payload: base64 载荷。
        index: 序号（用于 key）。

    Returns:
        dict[str, object]: 请求体。
    """
    return {"namespace": namespace, "topic": topic, "key": f"k-{index}", "payload": payload}


def _business_code(response: httpx.Response) -> int | str:
    """取响应里的业务码（非 JSON / 缺字段不抛错，否则归因时反而丢掉失败样本）。

    Args:
        response: 响应。

    Returns:
        int | str: 业务码；无法解析时给 `"<non-json>"` / `"<no-code>"`。
    """
    try:
        body = response.json()
    except ValueError:
        return "<non-json>"
    if not isinstance(body, dict):
        return "<non-object>"
    code = body.get("code")
    return code if isinstance(code, int) else "<no-code>"


async def run_mode(
    *,
    base_url: str,
    path: str,
    http2: bool,
    tenant: str,
    namespace: str,
    topic: str,
    total: int,
    concurrency: int,
    payload: str,
    warmup: int,
    connection_requests: int,
) -> Result:
    """跑一个档位（先预热再测量）。

    Args:
        base_url: 服务地址。
        path: 发布路径（基线或帧入口）。
        http2: 是否强制 HTTP/2（h2c prior knowledge）。
        tenant: 租户编码。
        namespace: 命名空间。
        topic: 主题名。
        total: 测量条数。
        concurrency: 并发在途数。
        payload: base64 载荷。
        warmup: 预热条数。
        connection_requests: 单条连接承载的请求上限（分轮换连接）。

    Returns:
        Result: 测量结果。
    """
    label = "h2c + JSON" if http2 else "HTTP/1.1 + JSON"
    result = Result(label=label, path=path, http2=http2)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(30.0)

    def _client() -> httpx.AsyncClient:
        """构造客户端（h2 档必须禁用 HTTP/1.1 才会走 h2c prior knowledge）。

        Returns:
            httpx.AsyncClient: 客户端。
        """
        if http2:
            return httpx.AsyncClient(base_url=base_url, http2=True, http1=False, limits=limits, timeout=timeout)
        return httpx.AsyncClient(base_url=base_url, http2=False, limits=limits, timeout=timeout)

    async def _worker(client: httpx.AsyncClient, count: int, offset: int, *, record: bool) -> None:
        """单个并发体：串行发 `count` 条（并发由多个 worker 提供）。

        Args:
            client: 客户端（同一连接池，h2 下即同一条连接）。
            count: 本 worker 的条数。
            offset: 起始序号（让 key 不重复）。
            record: 是否记录延迟（预热只计失败、不计延迟）。
        """
        headers = {"X-Tenant-ID": tenant}
        for step in range(count):
            index = offset + step
            started = time.perf_counter()
            try:
                response = await client.post(
                    path, json=_body(namespace=namespace, topic=topic, payload=payload, index=index), headers=headers
                )
                code = _business_code(response)
                ok = response.status_code == 200 and code == 0
                reason = f"HTTP {response.status_code} / code {code}"
                detail = f"{reason} :: {response.text[:160]}"
            except httpx.HTTPError as exc:
                ok = False
                reason = f"HTTPError {type(exc).__name__}"
                detail = f"{reason} :: {exc!r}"
            if record:
                if ok:
                    result.latencies_ms.append((time.perf_counter() - started) * 1000)
                else:
                    result.record_failure(reason, detail)
            elif not ok:
                result.warmup_failures += 1

    print(f"[bench] {label}：预热 {warmup} 条 → 测量 {total} 条（并发 {concurrency}）")
    await _run_phase(
        make_client=_client,
        worker=_worker,
        total=warmup,
        concurrency=concurrency,
        budget=connection_requests,
        offset_base=0,
        record=False,
    )
    started = time.perf_counter()
    await _run_phase(
        make_client=_client,
        worker=_worker,
        total=total,
        concurrency=concurrency,
        budget=connection_requests,
        offset_base=warmup,
        record=True,
    )
    result.elapsed_s = time.perf_counter() - started
    result.total = len(result.latencies_ms)
    return result


async def _run_phase(
    *,
    make_client: Callable[[], httpx.AsyncClient],
    worker: Callable[..., Any],
    total: int,
    concurrency: int,
    budget: int,
    offset_base: int,
    record: bool,
) -> None:
    """按「单连接请求预算」分轮跑 `total` 条（每轮一条新连接）。

    为什么分轮：hypercorn 的 `keep_alive_max_requests` 默认 **1000**，单条 h2 连接跑满 1000 条即
    `close_connection()`（GOAWAY）主动退休，httpx 对**在途** stream 不重试，写失败即
    `WriteError` / `RemoteProtocolError(<ConnectionTerminated …>)`。那不是传输失败，是服务端按设计的
    连接生命周期——不分轮就会把它记成「传输失败」（03-1 收口的 9 个失败即此形态，实证
    `last_stream_id:2001` = 第 1001 条请求）。分轮在**轮边界**换连接，无在途请求、无须重试，
    因此既不改写失败语义，也不掩盖真实失败。

    Args:
        make_client: 客户端工厂（每轮新建一条连接，轮结束即关）。
        worker: 单 worker 协程。
        total: 本轮次阶段的总条数。
        concurrency: 并发在途数。
        budget: 单条连接承载的请求上限（须小于服务端退休阈值）。
        offset_base: 起始序号偏移（让 key 跨阶段不重复）。
        record: 是否记录延迟（预热不记）。
    """
    done = 0
    while done < total:
        chunk = min(budget, total - done)
        async with make_client() as client:
            await _distribute(worker, client, chunk, concurrency, record=record, offset=offset_base + done)
        done += chunk


async def _distribute(
    worker: Callable[..., Any],
    client: httpx.AsyncClient,
    total: int,
    concurrency: int,
    *,
    record: bool,
    offset: int = 0,
) -> None:
    """把 `total` 条均分给 `concurrency` 个 worker 并等待全部完成。

    Args:
        worker: 单 worker 协程。
        client: 客户端。
        total: 总条数。
        concurrency: 并发数。
        record: 是否记录延迟。
        offset: 序号起点（跨轮 / 跨阶段累加，避免 key 重复）。
    """
    base, extra = divmod(total, max(1, concurrency))
    tasks = []
    cursor = offset
    for worker_index in range(concurrency):
        count = base + (1 if worker_index < extra else 0)
        if count:
            tasks.append(worker(client, count, cursor, record=record))
            cursor += count
    if tasks:
        await asyncio.gather(*tasks)


def _print_report(baseline: Result, candidate: Result) -> None:
    """打印 Markdown 表与结论。

    Args:
        baseline: 基线档结果。
        candidate: 对比档结果。
    """
    ratio = candidate.throughput / baseline.throughput if baseline.throughput else 0.0
    rows = [
        ("档位", "路径", "条数", "用时(s)", "吞吐(条/s)", "p50(ms)", "p95(ms)", "p99(ms)", "失败"),
        ("---", "---", "---", "---", "---", "---", "---", "---", "---"),
    ]
    for item in (baseline, candidate):
        rows.append(
            (
                item.label,
                item.path,
                str(item.total),
                f"{item.elapsed_s:.2f}",
                f"{item.throughput:.1f}",
                f"{item.percentile(0.50):.2f}",
                f"{item.percentile(0.95):.2f}",
                f"{item.percentile(0.99):.2f}",
                str(item.failures),
            )
        )
    print()
    for row in rows:
        print("| " + " | ".join(row) + " |")
    print()
    _print_failures(baseline, candidate)
    print(f"吞吐比（对比 / 基线）= **{ratio:.2f}x**（判据 ≥ {THROUGHPUT_RATIO_TARGET:.1f}x）")
    print(f"对比档 p99 = **{candidate.percentile(0.99):.2f}ms**（判据 ≤ {P99_TARGET_MS:.0f}ms）")
    passed = ratio >= THROUGHPUT_RATIO_TARGET and candidate.percentile(0.99) <= P99_TARGET_MS
    print(f"结论：{'达标' if passed else '未达标（按规划 §7 触发刹车评审，不调参硬凑）'}")


def _print_failures(baseline: Result, candidate: Result) -> None:
    """打印失败归因（无失败则跳过）。

    Args:
        baseline: 基线档结果。
        candidate: 对比档结果。
    """
    for item in (baseline, candidate):
        if not item.failures and not item.warmup_failures:
            continue
        print(f"**{item.label} 失败归因**（测量 {item.failures} / 预热 {item.warmup_failures}）：")
        print()
        for reason, count in sorted(item.failure_reasons.items(), key=lambda pair: -pair[1]):
            print(f"- `{reason}` × {count}")
        if item.failure_samples:
            print()
            print("样本：")
            for sample in item.failure_samples:
                print(f"- {sample}")
        print()


def _write_json(path: Path, baseline: Result, candidate: Result, params: dict[str, object]) -> None:
    """落一份原始结果 JSON（供复现比对）。

    Args:
        path: 落点。
        baseline: 基线档结果。
        candidate: 对比档结果。
        params: 本次参数（条数 / 并发 / 预热 / 载荷 / 单连接预算），复跑须逐项对齐。
    """
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "params": params,
        "modes": [
            {
                "label": item.label,
                "path": item.path,
                "http2": item.http2,
                "total": item.total,
                "elapsed_s": round(item.elapsed_s, 4),
                "throughput": round(item.throughput, 2),
                "p50_ms": round(item.percentile(0.50), 3),
                "p95_ms": round(item.percentile(0.95), 3),
                "p99_ms": round(item.percentile(0.99), 3),
                "failures": item.failures,
                "warmup_failures": item.warmup_failures,
                "failure_reasons": dict(item.failure_reasons),
                "failure_samples": list(item.failure_samples),
            }
            for item in (baseline, candidate)
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[bench] 原始结果：{path}")


def main(argv: list[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数（缺省取 `sys.argv[1:]`）。

    Returns:
        int: 退出码（0 已产出报告；1 连不上服务）。
    """
    parser = argparse.ArgumentParser(description="HTTP/2 传输基准（HTTP/1.1+JSON vs h2c+JSON）")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="服务地址（PoC 直连 8000）")
    parser.add_argument("--tenant", default="demo", help="租户编码（X-Tenant-ID）")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help="命名空间")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="主题名")
    parser.add_argument("--total", type=int, default=10_000, help="每档测量条数")
    parser.add_argument("--concurrency", type=int, default=32, help="并发在途请求数")
    parser.add_argument("--payload-bytes", type=int, default=1024, help="载荷字节数（base64 后传输）")
    parser.add_argument("--warmup", type=int, default=1_000, help="每档预热条数（不计入结果）")
    parser.add_argument(
        "--connection-requests",
        type=int,
        default=CONNECTION_REQUEST_BUDGET,
        help=f"单条连接承载的请求上限（缺省 {CONNECTION_REQUEST_BUDGET}，须小于服务端退休阈值 1000）",
    )
    parser.add_argument("--json", type=Path, default=None, help="原始结果 JSON 落点")
    parser.add_argument("--ensure-topic", action="store_true", help="先建基准主题（已存在不算失败）")
    args = parser.parse_args(argv)

    payload = base64.b64encode(b"x" * args.payload_bytes).decode("ascii")

    async def _run() -> int:
        """执行两档测量。

        Returns:
            int: 退出码。
        """
        async with httpx.AsyncClient(base_url=args.base_url, timeout=10.0) as probe:
            try:
                await probe.get("/healthz")
            except httpx.HTTPError as exc:
                print(f"[bench] 连不上服务 {args.base_url}：{exc!r}", file=sys.stderr)
                print("[bench] 先起服务：cd msg/backend && python -m bms_msg")
                print("[bench] 或：bms 本地全套 up --services platform,msg")
                return 1
            if args.ensure_topic:
                await ensure_topic(probe, namespace=args.namespace, topic=args.topic, tenant=args.tenant)

        baseline = await run_mode(
            base_url=args.base_url,
            path="/api/v1/msg/publish",
            http2=False,
            tenant=args.tenant,
            namespace=args.namespace,
            topic=args.topic,
            total=args.total,
            concurrency=args.concurrency,
            payload=payload,
            warmup=args.warmup,
            connection_requests=args.connection_requests,
        )
        candidate = await run_mode(
            base_url=args.base_url,
            path="/msg/publish",
            http2=True,
            tenant=args.tenant,
            namespace=args.namespace,
            topic=args.topic,
            total=args.total,
            concurrency=args.concurrency,
            payload=payload,
            warmup=args.warmup,
            connection_requests=args.connection_requests,
        )
        _print_report(baseline, candidate)
        if args.json is not None:
            _write_json(
                args.json,
                baseline,
                candidate,
                {
                    "total": args.total,
                    "concurrency": args.concurrency,
                    "warmup": args.warmup,
                    "payload_bytes": args.payload_bytes,
                    "connection_requests": args.connection_requests,
                    "base_url": args.base_url,
                    "tenant": args.tenant,
                    "namespace": args.namespace,
                    "topic": args.topic,
                },
            )
        return 0

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
