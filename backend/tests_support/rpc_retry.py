"""04-4 测试辅助：确定性时钟 / 抖动源 / 按尝试应答的脚本传输。

`tests_support` 不属覆盖率分母（`MSG_COVERAGE_SOURCE = "bms_msg,services,ops"`），但三份用例
都从这里取替身，保证「fake clock + 固定随机源」在三个域里口径一致。
"""

from __future__ import annotations

from collections.abc import Sequence

from bms_core.msgclient.frame import FrameFlags, FrameType, encode_frame
from bms_core.msgclient.transport import RpcHttpResponse

from tests_support.rpc_transport import ScriptedRpcTransport

__all__ = ["FakeClock", "FixedJitter", "error_response", "result_response", "scripted"]


class FakeClock:
    """单调时钟替身：`sleep_ms` 只推进虚拟时刻、记录等待序列，不真的睡。"""

    def __init__(self, now_ms: int = 0) -> None:
        """初始化。

        Args:
            now_ms: 起始时刻（毫秒）。
        """
        self.current = now_ms
        self.sleeps: list[int] = []

    def now_ms(self) -> int:
        """当前虚拟时刻。

        Returns:
            int: 毫秒。
        """
        return self.current

    async def sleep_ms(self, ms: int) -> None:
        """推进虚拟时刻并记录等待。

        Args:
            ms: 等待毫秒。
        """
        self.sleeps.append(ms)
        self.current += ms

    def advance(self, ms: int) -> None:
        """不经过 `sleep_ms` 地推进时刻（模拟两次调用之间自然流逝的时间）。

        Args:
            ms: 推进毫秒。
        """
        self.current += ms


class FixedJitter:
    """确定性抖动源：按给定序列依次返回；序列用尽后重复最后一个值。"""

    def __init__(self, values: Sequence[int]) -> None:
        """初始化。

        Args:
            values: 依次返回的整数（至少一个）。
        """
        self.values = tuple(values)
        self.calls: list[tuple[int, int]] = []

    def uniform(self, low: int, high: int) -> int:
        """返回序列里的下一个值（记录入参范围供断言）。

        Args:
            low: 下界。
            high: 上界。

        Returns:
            int: 固定值。
        """
        self.calls.append((low, high))
        index = min(len(self.calls) - 1, len(self.values) - 1)
        return self.values[index]


def result_response(payload: bytes = b"ok", *, request_id: str = "r1") -> RpcHttpResponse:
    """构造一个成功结果帧响应。

    Args:
        payload: 结果字节。
        request_id: 请求标识。

    Returns:
        RpcHttpResponse: 响应。
    """
    body = encode_frame(
        frame_type=int(FrameType.RPC),
        flags=FrameFlags.RESPONSE,
        header={"request_id": request_id},
        payload=payload,
    )
    return RpcHttpResponse(status=200, headers=(), body=body, stream_id=1)


def error_response(
    code: int = 350005,
    *,
    category: str | None = "transient",
    retryable: bool = True,
    retry_after_ms: int | None = None,
    layer: str = "business",
) -> RpcHttpResponse:
    """构造一个错误帧响应（可省略 `category` 模拟老服务端）。

    Args:
        code: 产品码。
        category: 错误类别（None 表示帧头不带该字段）。
        retryable: 可重试标记。
        retry_after_ms: 建议等待毫秒。
        layer: 错误层。

    Returns:
        RpcHttpResponse: 响应。
    """
    header: dict[str, object] = {
        "code": code,
        "message": "injected",
        "retryable": retryable,
        "layer": layer,
        "detail": None,
        "retry_after_ms": retry_after_ms,
    }
    if category is not None:
        header["category"] = category
    body = encode_frame(frame_type=int(FrameType.ERROR), flags=FrameFlags.RESPONSE, header=header)
    return RpcHttpResponse(status=200, headers=(), body=body, stream_id=1)


def scripted(*responses: RpcHttpResponse | BaseException) -> ScriptedRpcTransport:
    """按尝试序号返回响应或抛异常的脚本传输。

    Args:
        responses: 依次使用的响应（或要抛出的异常）。

    Returns:
        ScriptedRpcTransport: 传输替身。
    """
    queue = list(responses)

    async def handler(request: object) -> RpcHttpResponse:
        del request
        item = queue.pop(0) if queue else result_response()
        if isinstance(item, BaseException):
            raise item
        return item

    return ScriptedRpcTransport(handler=handler)
