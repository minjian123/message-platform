"""写响应前的取消复核（任务 04-2）。

**为什么需要复核**：正常取消（客户端 `RST_STREAM` / 连接断开）会让 handler 协程收到
`CancelledError` 并一路抛出——那次调用根本走不到「构造响应」。但 handler 若用
`contextlib.suppress(CancelledError)` 吞掉取消并正常返回，端点就会写出一个本该不存在的 200。
`Task.cancelling()`（3.11+）给出「还有未完成的取消请求」计数，据此在写响应前再判一次。

**为什么不包在 `invoke_rpc` 里**：判定必须紧贴「写响应」那一刻；`invoke_rpc` 返回后到写响应之间
还要组装响应头 / 结果帧，包在里面就漏了。所以两形态各调一次。
"""

from __future__ import annotations

import asyncio

__all__ = ["ensure_not_cancelled"]


def ensure_not_cancelled() -> None:
    """当前请求若已被取消，则抛出 `CancelledError`（不构造任何响应）。

    Raises:
        CancelledError: 当前任务仍有未完成的取消请求（handler 吞掉取消并正常返回的情形）。
    """
    task = asyncio.current_task()
    if task is None:  # pragma: no cover - 只可能在非任务上下文调用；端点与闸门恒在请求任务内
        return
    if task.cancelling():
        raise asyncio.CancelledError
