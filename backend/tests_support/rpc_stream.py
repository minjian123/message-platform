"""测试辅助：RPC 帧流的读取与「可控 send / receive」的 ASGI 直调（04-3-1）。

为什么需要两个层次：

- `read_stream()`：把 `StreamingResponse` 的字节还原成帧序列（累积缓冲 → `decode_frames`），
  用例据此断言 `seq` / 末帧 `end_of_stream` / payload。`httpx.ASGITransport` 会把整个响应当一次
  读完，用它拿到的就是完整字节，足够覆盖「帧序 / 结束三态 / 错误帧」。
- `call_stream()`：**自己当服务器**调 ASGI 应用，`send` / `receive` 都由用例掌控。只有这样才验得动
  两件事：① 慢消费者时「已产出未写出」是否被攒批上界挡住（背压）；② `http.disconnect` 之后服务端
  是否停止产出并归还名额（取消）。`httpx` 的 ASGITransport 既不能慢读也不能造断开。

本模块在 `pythonpath` 的 `.` 下可导入，且不属覆盖率统计包（`coverage_packages = ["bms_msg",
"services", "ops"]`），不会把测试便利代码算进覆盖率分母。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from bms_core.msgclient.frame import Frame, FrameFlags, FrameType, decode_frames

__all__ = [
    "FRAME_TYPE",
    "AsgiStreamResult",
    "FrameEntry",
    "call_stream",
    "read_stream",
]

FRAME_TYPE = "application/x-msg-frame"
"""帧媒体类型（与 `tests_support/frame_client.py` 同值；此处单独一份避免跨文件耦合）。"""


@dataclass(frozen=True)
class FrameEntry:
    """一帧的「可断言视图」（`seq` / 结束位只对 `48` 帧有意义）。"""

    frame: Frame
    """解码结果。"""

    seq: int | None
    """帧头 `seq`（ERROR 帧没有，为 None）。"""

    end: bool
    """是否置了 `flags.end_of_stream`。"""

    @property
    def code(self) -> int | None:
        """ERROR 帧的错误码（非错误帧为 None）。

        Returns:
            int | None: 错误码。
        """
        if self.frame.frame_type != int(FrameType.ERROR):
            return None
        value = self.frame.header.get("code")
        return int(value) if isinstance(value, int) else None

    @property
    def is_result(self) -> bool:
        """是否 `48` + response 位的结果帧。

        Returns:
            bool: 结果帧为 True。
        """
        return self.frame.frame_type == int(FrameType.RPC) and bool(self.frame.flags & FrameFlags.RESPONSE)


def read_stream(data: bytes) -> list[FrameEntry]:
    """把一段帧流字节还原成帧序列（不完整尾包视为协议错，直接断言失败）。

    Args:
        data: 响应体字节。

    Returns:
        list[FrameEntry]: 帧序列（按出现顺序）。

    Raises:
        AssertionError: 尾部有不完整帧（服务端切帧与 `header_len` / `payload_len` 不一致）。
    """
    batch = decode_frames(data)
    assert batch.consumed == len(data), f"帧流尾部有 {len(data) - batch.consumed} 字节不完整"
    entries: list[FrameEntry] = []
    for frame in batch.frames:
        raw_seq = frame.header.get("seq")
        entries.append(
            FrameEntry(
                frame=frame,
                seq=int(raw_seq) if isinstance(raw_seq, int) else None,
                end=bool(frame.flags & FrameFlags.END_OF_STREAM),
            )
        )
    return entries


@dataclass
class AsgiStreamResult:
    """一次 ASGI 直调的结果（状态行 + 逐块响应体 + 未捕获异常）。"""

    status: int = 0
    """响应状态码（未发出响应头时为 0）。"""

    headers: dict[str, str] = field(default_factory=dict[str, str])
    """响应头（小写键）。"""

    chunks: list[bytes] = field(default_factory=list[bytes])
    """逐块响应体（与 `send` 的调用一一对应；空块不计）。"""

    error: BaseException | None = None
    """应用抛出的未捕获异常（用于「无收尾帧即异常中断」这类断言）。"""

    @property
    def body(self) -> bytes:
        """全部响应体拼起来。

        Returns:
            bytes: 响应体。
        """
        return b"".join(self.chunks)


ChunkHook = Callable[[bytes, int], Awaitable[None]]
"""每写出一块时的回调（`(chunk, 已完整写出的块数)`；慢消费者用它 `sleep`）。"""


async def call_stream(
    app: Any,
    *,
    path: str,
    body: bytes,
    tenant: str = "demo",
    on_chunk: ChunkHook | None = None,
    disconnect: asyncio.Event | None = None,
) -> AsgiStreamResult:
    """直接调 ASGI 应用跑一条帧流（`send` / `receive` 都由本函数掌控）。

    Args:
        app: 应用实例（`build_frame_app()` 的产物）。
        path: 请求路径。
        body: 请求体（一帧）。
        tenant: 租户头（`X-Tenant-ID`）。
        on_chunk: 每写出一块的回调（慢消费者；在 `send` 里 `await`，因此真的会阻塞生产）。
        disconnect: 置位后 `receive` 返回 `http.disconnect`（模拟客户端取消 / 断连）。

    Returns:
        AsgiStreamResult: 状态码、响应头、逐块响应体与应用抛出的异常。
    """
    result = AsgiStreamResult()
    headers = [
        (b"content-type", FRAME_TYPE.encode()),
        (b"content-length", str(len(body)).encode()),
        (b"x-tenant-id", tenant.encode()),
    ]
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 1234),
        "server": ("127.0.0.1", 80),
        "state": {},
    }
    body_sent = False
    idle = asyncio.Event()

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        if disconnect is not None and disconnect.is_set():
            # **必须同步返回**：Starlette 的 `is_disconnected()` 把 receive 放进一个「立刻取消」的
            # 作用域里探一次，凡是需要 `await` 才能就绪的消息它都看不见（这是它「不阻塞」的实现方式）。
            return {"type": "http.disconnect"}
        # 其余情况永不返回：既不让 `is_disconnected()` 命中，也不让监听任务提前结束。
        await idle.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            result.status = int(message["status"])
            result.headers = {key.decode().lower(): value.decode() for key, value in message.get("headers", [])}
            return
        chunk = bytes(message.get("body", b""))
        if chunk:
            result.chunks.append(chunk)
            if on_chunk is not None:
                await on_chunk(chunk, len(result.chunks))

    try:
        await app(scope, receive, send)
    except BaseException as exc:  # 用例要拿到「无收尾帧就断流」的那次异常（含取消）
        result.error = exc
    finally:
        idle.set()
    return result


def seqs(entries: Sequence[FrameEntry]) -> list[int]:
    """取结果帧的 `seq` 序列（跳过 ERROR 帧）。

    Args:
        entries: 帧序列。

    Returns:
        list[int]: `seq` 列表。
    """
    return [entry.seq for entry in entries if entry.is_result and entry.seq is not None]
