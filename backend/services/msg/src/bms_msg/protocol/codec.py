"""header 的 MessagePack 编解码（任务 03-2）。

**强约束**：header 必须解成 **Map 且键全为字符串**。理由：

- 键为字符串才能跨语言稳定映射（数字键在 JSON / 各语言 Map 里的排序与表示各不相同）；
- 键类型不在解码期判，就分不清「格式坏了」与「键类型非法」，错误码只能含糊成 `350018` 一句话。

`raw=False` 让 `str` 直出（而不是 `bytes`）；`strict_map_key=False` 是**故意**的——先解出来再判键类型，
比解不出来时猜原因更可靠。`use_bin_type=True` 让二进制与字符串分开编码，跨语言不歧义。
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any, cast

import msgpack
from bms_core.core.concurrent import ConcurrentStableDict
from bms_core.core.serialization import normalize_collections

from bms_msg.errors import MsgFrameInvalidError

__all__ = ["decode_header", "encode_header"]

_UNPACK_ERRORS = (msgpack.exceptions.UnpackException, ValueError, TypeError)
"""解码可能抛出的异常（msgpack 的各类格式错误 + 非字节入参）。"""

_UNPACK = cast("Callable[..., object]", msgpack.unpackb)  # pyright: ignore[reportUnknownMemberType]
"""`unpackb` 的类型化视图（官方包未随发布类型标注，直接取属性在 strict 模式下是 Unknown）。"""

_PACK = cast("Callable[..., bytes]", msgpack.packb)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
"""`packb` 的类型化视图（同上）。"""


def decode_header(raw: bytes | memoryview) -> ConcurrentStableDict[str, Any]:
    """把 header 字节解成 Map（键必须为字符串）。

    Args:
        raw: header 原始字节。

    Returns:
        ConcurrentStableDict[str, Any]: 头（插入序与线上一致）。

    Raises:
        MsgFrameInvalidError: 非法 MessagePack / 非 Map / 键非字符串（`350018`）。
    """
    try:
        value = _UNPACK(bytes(raw), raw=False, strict_map_key=False, use_list=True)
    except _UNPACK_ERRORS as exc:
        raise MsgFrameInvalidError(f"header 不是合法的 MessagePack：{exc}") from exc
    if not isinstance(value, dict):
        raise MsgFrameInvalidError(f"header 必须是 MessagePack Map（实际 {type(value).__name__}）")
    header: ConcurrentStableDict[str, Any] = ConcurrentStableDict()
    for key, item in cast("dict[object, object]", value).items():
        if not isinstance(key, str):
            raise MsgFrameInvalidError(f"header 键必须为字符串（实际 {type(key).__name__}）")
        header.set(key, item)
    return header


def encode_header(header: Mapping[str, Any]) -> bytes:
    """把 Map 编码为 header 字节。

    Args:
        header: 待编码头（键须为字符串）。

    Returns:
        bytes: MessagePack 字节。

    Raises:
        MsgFrameInvalidError: 键非字符串（`350018`）。
    """
    for key in cast("Iterable[object]", header):
        if not isinstance(key, str):
            raise MsgFrameInvalidError(f"header 键必须为字符串（实际 {type(key).__name__}）")
    return _PACK(normalize_collections(dict(header.items())), use_bin_type=True)
