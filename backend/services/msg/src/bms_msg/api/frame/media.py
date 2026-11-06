"""帧形态判定（任务 03-4 从 `gate.py` 提升为独立模块）。

为什么单独一层：连接守卫（`api/frame/guard.py`）要在**闸门之前**判「这是不是帧请求」——帧请求的
拒绝必须回 200 + `error` 帧（03-2 §6.5），JSON 请求回 429 统一错误体。若守卫直接 import `gate.py`，
而 `gate.py` 又 import 数据面模块（`api/data/messages.py`），数据面模块再 import 守卫就会成环；
把这条纯判定（只看 `Content-Type`）落在最底层，环就断了。

边界：这里只回答「是不是帧形态」，不解析帧、不判业务。
"""

from bms_core.msgclient.frame import FRAME_CONTENT_TYPE
from fastapi import Request

__all__ = ["is_frame_request"]


def is_frame_request(request: Request) -> bool:
    """判断是否为帧形态请求（只看媒体类型；缺省 / JSON 一律不是）。

    Args:
        request: 当前请求。

    Returns:
        bool: `Content-Type` 主类型为 `application/x-msg-frame` 时 True。
    """
    raw = request.headers.get("content-type", "")
    return raw.split(";")[0].strip().lower() == FRAME_CONTENT_TYPE
