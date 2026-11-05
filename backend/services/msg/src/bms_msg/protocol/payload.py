"""消息体口径（任务 03-3 详细设计 §4）：内容类型取值与压缩标记。

三件事，各只有一处判定：

1. **`content_type` 是媒体类型字符串**（`type/subtype`），缺省 `application/msgpack`。未登记但语法合法的取值
   **一律透传**——broker 不解 payload，本来就无从判断「支不支持」；白名单拒绝会把「新增格式」变成
   「先改 broker 再上线」，与透明转发原则冲突。
2. **压缩标记两段式**：`flags.compressed` 只表「是否压缩」，算法名在 header 的 `compression` 键。算法名放
   flags 高位会不可读（抓包看不到）且新增算法要抢位；放 header 则新增算法只是加一个白名单值。
3. **标记必须自洽**：置位却不给算法、给了算法却未置位，都在入口以 `350018` 拒绝——放过去会变成消费端
   解压失败且无法定位是谁写错。

边界：本模块**不压缩、不解压**（broker 零拷贝，压缩由生产者做、消费者解）；也不解析媒体类型参数
（`;charset=…` 只按分隔符截断，参数不参与判定）。取值形态非法是契约层 `10001`（键在、值形态错），
与 03-2「帧结构问题 `350018`」的分工一致。
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from bms_core.core.concurrent import ConcurrentStableDict, ConcurrentStableList
from bms_core.core.exceptions import ParamError

from bms_msg.errors import MsgFrameInvalidError

__all__ = [
    "COMPRESSION_GZIP",
    "COMPRESSION_LZ4",
    "COMPRESSION_NONE",
    "COMPRESSION_SNAPPY",
    "COMPRESSION_ZSTD",
    "CONTENT_TYPE_JSON",
    "CONTENT_TYPE_MSGPACK",
    "CONTENT_TYPE_OCTET_STREAM",
    "CONTENT_TYPE_PROTOBUF",
    "CONTENT_TYPE_TEXT",
    "DEFAULT_CONTENT_TYPE",
    "KNOWN_COMPRESSIONS",
    "KNOWN_CONTENT_TYPES",
    "ensure_compression_flag",
    "normalize_compression",
    "normalize_content_type",
    "payload_contract",
    "split_content_type",
]

CONTENT_TYPE_MSGPACK = "application/msgpack"
"""缺省内容类型：MessagePack（与「默认序列化 MessagePack」一致）。"""

CONTENT_TYPE_JSON = "application/json"
"""文本 JSON（便于调试与跨语言；生产端可选）。"""

CONTENT_TYPE_TEXT = "text/plain"
"""纯文本。"""

CONTENT_TYPE_OCTET_STREAM = "application/octet-stream"
"""不承诺格式的原始字节。"""

CONTENT_TYPE_PROTOBUF = "application/x-protobuf"
"""预留：Protobuf（登记在册、当前无生产 / 消费方；实现归 8B_03 SDK）。"""

DEFAULT_CONTENT_TYPE = CONTENT_TYPE_MSGPACK
"""生产者不传时生效的内容类型（响应帧回显该值，消费端据此自解释）。"""

STATUS_DEFAULT = "default"
"""取值状态：不传时的生效值。"""

STATUS_KNOWN = "known"
"""取值状态：已登记、broker 透传。"""

STATUS_RESERVED = "reserved"
"""取值状态：预留位（契约可见、当前无实现）。"""

KNOWN_CONTENT_TYPES: Mapping[str, str] = MappingProxyType(
    {
        CONTENT_TYPE_MSGPACK: STATUS_DEFAULT,
        CONTENT_TYPE_JSON: STATUS_KNOWN,
        CONTENT_TYPE_TEXT: STATUS_KNOWN,
        CONTENT_TYPE_OCTET_STREAM: STATUS_KNOWN,
        CONTENT_TYPE_PROTOBUF: STATUS_RESERVED,
    }
)
"""已登记的内容类型（**未登记不代表拒收**：语法合法即透传，见模块文档）。"""

COMPRESSION_NONE = "none"
"""未压缩（缺省）。"""

COMPRESSION_GZIP = "gzip"
"""gzip（标准库即可实现；验收用例用它扮演生产 / 消费两端）。"""

COMPRESSION_ZSTD = "zstd"
"""zstd（新系统首选，可调级别；实现归 SDK）。"""

COMPRESSION_LZ4 = "lz4"
"""lz4（极快，低延迟场景；实现归 SDK）。"""

COMPRESSION_SNAPPY = "snappy"
"""snappy（消息批量默认；实现归 SDK）。"""

KNOWN_COMPRESSIONS: tuple[str, ...] = (
    COMPRESSION_NONE,
    COMPRESSION_GZIP,
    COMPRESSION_ZSTD,
    COMPRESSION_LZ4,
    COMPRESSION_SNAPPY,
)
"""压缩算法白名单（本任务只登记与校验，**不引入压缩库**）。"""

_MEDIA_TYPE_TOKEN = r"[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,127}"
"""RFC 6838 的 token 形态（主类型 / 子类型各一段）。"""

_MEDIA_TYPE_PATTERN = re.compile(rf"^{_MEDIA_TYPE_TOKEN}/{_MEDIA_TYPE_TOKEN}$")
"""媒体类型主形态（不含参数；参数段在校验前截掉）。"""

_MAX_CONTENT_TYPE_LENGTH = 255
"""内容类型字符串上限（两个 token 各 128 位 + 分隔符，留余量）。"""


def split_content_type(raw: str) -> str:
    """取媒体类型的主体（丢弃 `;` 之后的参数，统一小写）。

    媒体类型大小写不敏感（RFC 6838），统一小写后才能比较与聚合；参数（`charset` / `boundary` 等）
    broker 不解释，保留反而让「同一格式两种写法」在统计与回显里分散。

    Args:
        raw: 原始内容类型字符串。

    Returns:
        str: 小写主体（如 `application/json`）。
    """
    return raw.split(";", 1)[0].strip().lower()


def normalize_content_type(raw: str | None) -> str:
    """规范化内容类型（缺省补默认值、校验语法、小写化）。

    只校验**语法**，不校验是否已登记：broker 不解 payload，无从判断格式「支不支持」；未登记取值透传，
    由入口打点提示（详细设计 §4.1）。

    Args:
        raw: 帧 header 或 JSON 请求体给出的内容类型（可空）。

    Returns:
        str: 规范化的媒体类型（空值给 ``DEFAULT_CONTENT_TYPE``）。

    Raises:
        ParamError: 形态非法（不是 `type/subtype`，或过长）——契约层 `10001`。
    """
    if raw is None:
        return DEFAULT_CONTENT_TYPE
    value = split_content_type(raw)
    if not value:
        return DEFAULT_CONTENT_TYPE
    if len(value) > _MAX_CONTENT_TYPE_LENGTH or _MEDIA_TYPE_PATTERN.match(value) is None:
        raise ParamError(f"content_type 须为 type/subtype 形式的媒体类型：{raw!r}")
    return value


def normalize_compression(raw: str | None) -> str:
    """规范化压缩算法名（缺省 `none`，白名单外即契约层拒绝）。

    Args:
        raw: header `compression` 或 JSON 请求体给出的算法名（可空）。

    Returns:
        str: 规范化后的算法名（空值给 ``COMPRESSION_NONE``）。

    Raises:
        ParamError: 不在白名单（`10001`）。
    """
    if raw is None:
        return COMPRESSION_NONE
    value = raw.strip().lower()
    if not value:
        return COMPRESSION_NONE
    if value not in KNOWN_COMPRESSIONS:
        raise ParamError(f"compression 不支持（须为 {' / '.join(KNOWN_COMPRESSIONS)} 之一）：{raw!r}")
    return value


def ensure_compression_flag(*, compressed: bool, compression: str) -> None:
    """校验「是否压缩」与「用什么算法」自洽（协议层 `350018`）。

    两个方向都拒绝：置位却不给算法（说压了但没说怎么解）、给了算法却没置位（说没压却带了算法）。

    Args:
        compressed: 帧 `flags.compressed` 是否置位。
        compression: 已规范化的算法名（``normalize_compression`` 的结果）。

    Raises:
        MsgFrameInvalidError: 两者矛盾（`350018`）。
    """
    if compressed and compression == COMPRESSION_NONE:
        raise MsgFrameInvalidError("flags.compressed 已置位但 header 未给 compression 算法")
    if not compressed and compression != COMPRESSION_NONE:
        raise MsgFrameInvalidError(f"未置位 flags.compressed 却声明了 compression={compression}")


def payload_contract() -> ConcurrentStableDict[str, Any]:
    """构造消息体契约片段（供 ``protocol.contract.frame_contract()`` 组装）。

    Returns:
        ConcurrentStableDict[str, Any]: 内容类型取值 / 压缩白名单 / 消息体规则三组。
    """
    content_types: ConcurrentStableList[ConcurrentStableDict[str, str]] = ConcurrentStableList()
    for value, status in KNOWN_CONTENT_TYPES.items():
        entry: ConcurrentStableDict[str, str] = ConcurrentStableDict()
        entry.set("value", value)
        entry.set("status", status)
        content_types.add(entry)
    return ConcurrentStableDict(
        {
            "payload_content_types": content_types,
            "compressions": ConcurrentStableList(KNOWN_COMPRESSIONS),
            "payload_rules": ConcurrentStableDict(
                {
                    "top_level": "map<str, any>（payload 解出的顶层必须是 Map，才谈得上只增字段）",
                    "default_content_type": DEFAULT_CONTENT_TYPE,
                    "unknown_content_type": "语法合法即透传（broker 不解 payload，不做白名单拒绝）",
                    "compression_flag": "flags.compressed 表是否压缩；header.compression 表算法；两者必须自洽",
                    "limit": "上限判压缩后字节（≤ 1 MiB）；broker 不解压",
                    "uncompressed_len": "可选声明，仅形态校验与透传，不参与上限判定",
                }
            ),
        }
    )
