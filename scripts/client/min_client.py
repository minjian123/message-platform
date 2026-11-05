"""最小测试客户端（任务 03-4）：连接 → 心跳 → 订阅 → 断线退避重连。

**用途**：验收「滚动重启不丢在途、客户端自动重连、重连不形成脉冲」——它不是 SDK（正式 Producer /
Consumer / RPC 客户端归 8B_03），只做三件事：按服务端自报参数发心跳、读长驻流、按指数退避 + 抖动重连，
并把每一轮的时间线打印出来供人工判定。

**怎么跑**（先起服务，建好主题并 join 消费组）：

```bash
uv run --project backend python scripts/client/min_client.py --attempts 3
```

**验收口径**：中途 `SIGTERM` 服务进程 → 客户端应打印 `drain` 或 `disconnect` 原因并按退避重连；
重启服务后恢复推送。抖动区间在退出汇总里打印（判定「不形成脉冲」的直接证据）。

**为什么退避算在包里**：`next_backoff_ms` 在 `bms_msg/broker/connection.py`——纯函数才好被单测钉住
上下界；写在本脚本里等于没有门禁（本脚本不在覆盖率统计包内）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

import httpx

from bms_msg.broker.connection import next_backoff_ms
from bms_msg.protocol.frame import FRAME_CONTENT_TYPE, FrameFlags, FrameType, decode_frame, encode_frame

__all__ = ["Attempt", "main", "run"]

_POLL_SECONDS = 0.2
"""轮询间隔（秒）：足够及时地发现 draining / 到期，又不空转。"""

_DEFAULT_TOPIC = "audit.operation"
"""缺省订阅主题（与既有用例 / 基准同一主题，便于直接联调）。"""


@dataclass
class Attempt:
    """一轮连接的记录（打印时间线与退出汇总都用它）。"""

    index: int
    """第几轮（从 1 起）。"""

    reason: str
    """本轮结束原因（`close:deadline` / `drain` / `disconnect` / `duration`）。"""

    delay_ms: int
    """本轮结束后等待的毫秒数（最后一轮为 0）。"""

    messages: int = 0
    """本轮收到的消息条数。"""


@dataclass
class Options:
    """命令行参数。"""

    base_url: str
    tenant: str
    namespace: str
    topic: str
    group: str
    consumer_id: str
    partition: int
    protocol: str
    attempts: int
    duration: float
    heartbeat_interval: float | None


@dataclass
class State:
    """跨任务的共享状态（心跳任务写、主循环读）。"""

    draining: bool = field(default=False)
    disconnecting: bool = field(default=False)
    heartbeat_errors: int = field(default=0)


def _parse_args(argv: list[str] | None = None) -> Options:
    """解析命令行参数。

    Args:
        argv: 参数列表（None 取 `sys.argv`）。

    Returns:
        Options: 解析结果。
    """
    parser = argparse.ArgumentParser(description="msg 最小测试客户端（心跳 + 订阅 + 退避重连）")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--tenant", default="demo")
    parser.add_argument("--namespace", default="msg")
    parser.add_argument("--topic", default=_DEFAULT_TOPIC)
    parser.add_argument("--group", default="audit")
    parser.add_argument("--consumer-id", default="min-client")
    parser.add_argument("--partition", type=int, default=0)
    parser.add_argument("--protocol", choices=["h2c", "h1"], default="h2c")
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--heartbeat-interval", type=float, default=None)
    args = parser.parse_args(argv)
    return Options(
        base_url=str(args.base_url),
        tenant=str(args.tenant),
        namespace=str(args.namespace),
        topic=str(args.topic),
        group=str(args.group),
        consumer_id=str(args.consumer_id),
        partition=int(args.partition),
        protocol=str(args.protocol),
        attempts=int(args.attempts),
        duration=float(args.duration),
        heartbeat_interval=args.heartbeat_interval,
    )


def _client(options: Options) -> httpx.AsyncClient:
    """按协议档构造客户端（h2c 必须 `http2=True, http1=False`，否则仍协商 HTTP/1.1）。

    Args:
        options: 命令行参数。

    Returns:
        httpx.AsyncClient: 客户端。
    """
    headers = {"X-Tenant-ID": options.tenant}
    if options.protocol == "h2c":
        return httpx.AsyncClient(base_url=options.base_url, headers=headers, http2=True, http1=False)
    return httpx.AsyncClient(base_url=options.base_url, headers=headers)


def _heartbeat_frame() -> bytes:
    """构造一帧心跳请求（类型 17，header 仅 `request_id`）。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(
        frame_type=int(FrameType.HEARTBEAT),
        flags=FrameFlags.NEED_RESPONSE,
        header={"request_id": f"hb-{time.monotonic_ns()}"},
    )


def _subscribe_params(options: Options) -> dict[str, str | int]:
    """长驻流查询参数。

    Args:
        options: 命令行参数。

    Returns:
        dict[str, str | int]: 查询参数。
    """
    return {
        "namespace": options.namespace,
        "topic": options.topic,
        "group": options.group,
        "consumer_id": options.consumer_id,
        "partition": options.partition,
    }


def _member_body(options: Options) -> dict[str, str]:
    """成员请求体（`join` / `heartbeat` / `leave` 共用，01-4）。

    Args:
        options: 命令行参数。

    Returns:
        dict[str, str]: 成员请求体。
    """
    return {
        "namespace": options.namespace,
        "topic": options.topic,
        "group": options.group,
        "consumer_id": options.consumer_id,
    }


async def _join(client: httpx.AsyncClient, options: Options, stream: Any) -> None:
    """加入消费组（同 `consumer_id` 重复加入视为重连，幂等）。

    **为什么客户端要自己加入**：成员制是 01-4 的前提——不 join 就没有分区分配，长驻流会被 350014 拒绝；
    只在排练脚本里由人工 join，会让「客户端能自愈」这件事不成立。

    Args:
        client: 客户端。
        options: 命令行参数。
        stream: 输出流。
    """
    response = await client.post("/api/v1/msg/join", json=_member_body(options))
    if response.status_code == 200:
        assignment = dict(response.json()["data"])
        print(f"已加入消费组：分区={assignment.get('partitions')} 世代={assignment.get('generation')}", file=stream)
        return
    print(f"加入消费组失败（{response.status_code}）：{response.text}（请先建主题）", file=stream)


async def _read_connection(client: httpx.AsyncClient) -> dict[str, Any]:
    """读连接自报（拿服务端的心跳间隔 / 超时 / 上限，绝不硬编码）。

    Args:
        client: 客户端。

    Returns:
        dict[str, Any]: `ConnectionInfo` 主体。

    Raises:
        httpx.HTTPError: 请求失败。
        KeyError: 响应结构不符（服务端版本不匹配）。
    """
    response = await client.get("/msg/connection")
    response.raise_for_status()
    body = response.json()
    return dict(body["data"])


async def _heartbeat_loop(
    client: httpx.AsyncClient, *, options: Options, interval_s: float, state: State, deadline: float
) -> None:
    """按间隔发两类心跳，把 `draining` 与错误记进共享状态。

    两类心跳各有归属，缺一不可：
    - **连接心跳**（帧 17，03-4）：判连接存活、拿服务端参数与 drain 信号；
    - **成员心跳**（JSON，01-4）：判成员存活，不发则 30s 后成员失效、长驻流被 350014 拒绝（演练实测）。

    Args:
        client: 客户端。
        options: 命令行参数。
        interval_s: 心跳间隔（秒；取自服务端自报）。
        state: 共享状态。
        deadline: 本轮截止时刻（`loop.time()`）。
    """
    member_body = _member_body(options)
    while asyncio.get_running_loop().time() < deadline:
        try:
            response = await client.post(
                "/msg/connection/heartbeat",
                content=_heartbeat_frame(),
                headers={"Content-Type": FRAME_CONTENT_TYPE},
            )
            if response.status_code == 200:
                state.draining = state.draining or _heartbeat_draining(response.content)
            else:
                state.heartbeat_errors += 1
            member = await client.post("/api/v1/msg/heartbeat", json=member_body)
            if member.status_code != 200:
                state.heartbeat_errors += 1
        except httpx.HTTPError:
            state.heartbeat_errors += 1
            state.disconnecting = True
            return
        await asyncio.sleep(interval_s)


def _heartbeat_draining(body: bytes) -> bool:
    """从心跳响应帧里读 `draining`（帧头已由服务端平铺 `ConnectionInfo` 字段）。

    只做最小解析：用协议自己的解码器，避免在脚本里再写一套 MessagePack 解析。

    Args:
        body: 响应帧字节。

    Returns:
        bool: 是否在摘流。
    """
    return bool(decode_frame(body).header.get("draining"))


async def _consume_once(client: httpx.AsyncClient, options: Options, state: State, deadline: float) -> Attempt:
    """跑一轮订阅流，直到到期 / 摘流 / 断线 / 总时长用尽。

    **为什么用「读任务 + 轮询」而不是在 SSE 循环里判到期**：空闲期服务端只发注释行，而注释行不进事件流
    ——挂在事件循环里的到期判断会一直不被求值，一轮会拖到流自己的时长上限（演练实测 45s）。把「读」放进
    独立任务，由外层按固定间隔看三个条件（完成 / draining / 到期），三种退出都及时。

    Args:
        client: 客户端。
        options: 命令行参数。
        state: 共享状态。
        deadline: 本轮截止时刻。

    Returns:
        Attempt: 本轮记录（`delay_ms` 由调用方填）。
    """
    counter: dict[str, int] = {"messages": 0}
    reader = asyncio.create_task(_drain_stream(client, options, counter))
    reason = "duration"
    try:
        while True:
            done, _ = await asyncio.wait({reader}, timeout=_POLL_SECONDS)
            if done:
                messages, reason = reader.result()
                return Attempt(index=0, reason=reason, delay_ms=0, messages=messages)
            if state.draining:
                reason = "drain"
                break
            if asyncio.get_running_loop().time() >= deadline:
                reason = "duration"
                break
    finally:
        if not reader.done():
            reader.cancel()
            with suppress(asyncio.CancelledError, TimeoutError):
                await reader
    return Attempt(index=0, reason=reason, delay_ms=0, messages=counter["messages"])


async def _drain_stream(
    client: httpx.AsyncClient, options: Options, counter: dict[str, int]
) -> tuple[int, str]:
    """读完一条长驻流，返回（消息条数, 结束原因）；被取消时把已收条数留在 `counter`。

    Args:
        client: 客户端。
        options: 命令行参数。
        counter: 共享计数字典（被取消后外层仍能取到已收条数）。

    Returns:
        tuple[int, str]: 消息条数与结束原因（`close:<reason>` / `disconnect` / `http:<code>` / `stream_end`）。
    """
    messages = 0
    reason = "stream_end"
    try:
        async with client.stream("GET", "/msg/subscribe", params=_subscribe_params(options)) as stream:
            if stream.status_code != 200:
                await stream.aread()
                return 0, f"http:{stream.status_code}"
            async for name, data in _sse_events(stream):
                if name == "message":
                    messages += 1
                    counter["messages"] = messages
                elif name == "close":
                    reason = f"close:{data.get('reason', 'unknown')}"
                    return messages, reason
    except httpx.HTTPError:
        return messages, "disconnect"
    return messages, reason


async def _sse_events(stream: httpx.Response) -> AsyncIterator[tuple[str, dict[str, Any]]]:
    """把 SSE 行流解析成（事件名, 数据）二元组（跳过注释行与空行）。

    Args:
        stream: 已打开的流式响应。

    Yields:
        tuple[str, dict[str, Any]]: 事件名与已解析的 JSON 负载（无负载给空 dict）。
    """
    name = ""
    data: dict[str, Any] = {}
    async for line in stream.aiter_lines():
        if line.startswith("event: "):
            name = line[len("event: ") :]
        elif line.startswith("data: "):
            parsed = json.loads(line[len("data: ") :])
            data = dict(parsed) if isinstance(parsed, dict) else {}
        elif line.strip() == "" and name:
            yield name, data
            name = ""
            data = {}


async def run(options: Options, *, out: Any = None) -> int:
    """跑完整验收流程（多轮重连 + 汇总）。

    Args:
        options: 命令行参数。
        out: 输出流（缺省 `sys.stdout`）。

    Returns:
        int: 进程退出码（0 表示跑完，2 表示服务端不可达）。
    """
    stream = out if out is not None else sys.stdout
    attempts: list[Attempt] = []
    started = asyncio.get_running_loop().time()
    async with _client(options) as client:
        try:
            info = await _read_connection(client)
        except (httpx.HTTPError, KeyError) as exc:
            print(f"服务端不可达或响应异常：{exc}", file=stream)
            return 2
        await _join(client, options, stream)
        interval = options.heartbeat_interval or float(info["heartbeat_interval_ms"]) / 1000
        print(
            f"连接参数：http={info['http_version']} 心跳={interval}s 超时={info['heartbeat_timeout_ms']}ms "
            f"空闲={info['idle_timeout_ms']}ms 连接上限={info['max_connections']} "
            f"单连接流上限={info['max_streams_per_connection']}",
            file=stream,
        )
        for index in range(1, options.attempts + 1):
            round_end = min(started + options.duration, asyncio.get_running_loop().time() + 60.0)
            state = State()
            heartbeat = asyncio.create_task(
                _heartbeat_loop(client, options=options, interval_s=interval, state=state, deadline=round_end)
            )
            attempt = await _consume_once(client, options, state, round_end)
            heartbeat.cancel()
            attempt.index = index
            attempts.append(attempt)
            if index >= options.attempts or asyncio.get_running_loop().time() >= started + options.duration:
                break
            attempt.delay_ms = next_backoff_ms(index - 1)
            print(
                f"第 {index} 轮：原因={attempt.reason} 消息={attempt.messages} "
                f"心跳错误={state.heartbeat_errors} 退避={attempt.delay_ms}ms",
                file=stream,
            )
            await asyncio.sleep(attempt.delay_ms / 1000)
    _print_summary(attempts, stream)
    return 0


def _print_summary(attempts: list[Attempt], stream: Any) -> None:
    """打印退出汇总（重连次数 / 退避区间 / 是否形成脉冲）。

    Args:
        attempts: 各轮记录。
        stream: 输出流。
    """
    delays = [item.delay_ms for item in attempts if item.delay_ms > 0]
    print("—— 汇总 ——", file=stream)
    print(f"轮次={len(attempts)} 原因={' / '.join(item.reason for item in attempts)}", file=stream)
    if delays:
        print(
            f"退避序列={delays} 最小={min(delays)}ms 最大={max(delays)}ms "
            f"（抖动来自 ±20%，相邻轮不相等即未形成脉冲）",
            file=stream,
        )
    else:
        print("未发生重连（服务端未断开，或首轮即达 --attempts）", file=stream)


def main(argv: list[str] | None = None) -> int:
    """命令行入口。

    Args:
        argv: 参数列表（None 取 `sys.argv`）。

    Returns:
        int: 退出码。
    """
    return asyncio.run(run(_parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
