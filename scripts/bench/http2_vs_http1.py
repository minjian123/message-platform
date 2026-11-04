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
- **不达标不调参**：结论如实打印；不达标按规划 §7 触发刹车评审（备选方案须另拍板）。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

DEFAULT_TOPIC = "bench.http2"
"""基准主题（`--ensure-topic` 时自动创建）。"""

DEFAULT_NAMESPACE = "msg"
"""命名空间（须为已启用 module_key）。"""

THROUGHPUT_RATIO_TARGET = 2.0
"""吞吐判据：对比档 ≥ 基线的 2 倍。"""

P99_TARGET_MS = 50.0
"""延迟判据：p99 ≤ 50ms。"""


@dataclass
class Result:
    """一个档位的测量结果。"""

    label: str
    path: str
    http2: bool
    total: int = 0
    elapsed_s: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)
    failures: int = 0

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
            record: 是否记录延迟（预热不记）。
        """
        headers = {"X-Tenant-ID": tenant}
        for step in range(count):
            index = offset + step
            started = time.perf_counter()
            try:
                response = await client.post(
                    path, json=_body(namespace=namespace, topic=topic, payload=payload, index=index), headers=headers
                )
                ok = response.status_code == 200 and response.json().get("code") == 0
            except httpx.HTTPError:
                ok = False
            if record:
                if ok:
                    result.latencies_ms.append((time.perf_counter() - started) * 1000)
                else:
                    result.failures += 1

    async with _client() as client:
        print(f"[bench] {label}：预热 {warmup} 条 → 测量 {total} 条（并发 {concurrency}）")
        await _distribute(_worker, client, warmup, concurrency, record=False)
        started = time.perf_counter()
        await _distribute(_worker, client, total, concurrency, record=True)
        result.elapsed_s = time.perf_counter() - started
        result.total = len(result.latencies_ms)
    return result


async def _distribute(worker, client: httpx.AsyncClient, total: int, concurrency: int, *, record: bool) -> None:
    """把 `total` 条均分给 `concurrency` 个 worker 并等待全部完成。

    Args:
        worker: 单 worker 协程。
        client: 客户端。
        total: 总条数。
        concurrency: 并发数。
        record: 是否记录延迟。
    """
    base, extra = divmod(total, max(1, concurrency))
    tasks = []
    offset = 0
    for worker_index in range(concurrency):
        count = base + (1 if worker_index < extra else 0)
        if count:
            tasks.append(worker(client, count, offset, record=record))
            offset += count
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
    print(f"吞吐比（对比 / 基线）= **{ratio:.2f}x**（判据 ≥ {THROUGHPUT_RATIO_TARGET:.1f}x）")
    print(f"对比档 p99 = **{candidate.percentile(0.99):.2f}ms**（判据 ≤ {P99_TARGET_MS:.0f}ms）")
    passed = ratio >= THROUGHPUT_RATIO_TARGET and candidate.percentile(0.99) <= P99_TARGET_MS
    print(f"结论：{'达标' if passed else '未达标（按规划 §7 触发刹车评审，不调参硬凑）'}")


def _write_json(path: Path, baseline: Result, candidate: Result) -> None:
    """落一份原始结果 JSON（供复现比对）。

    Args:
        path: 落点。
        baseline: 基线档结果。
        candidate: 对比档结果。
    """
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
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
        )
        _print_report(baseline, candidate)
        if args.json is not None:
            _write_json(args.json, baseline, candidate)
        return 0

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
