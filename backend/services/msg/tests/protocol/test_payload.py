"""消息体口径（任务 03-3；Kiwi 2423）：内容类型取值 / 压缩标记 / 契约片段。

三条不可动摇的口径（详细设计 §4）：

1. **未登记不代表拒收**：broker 不解 payload，无从判断格式「支不支持」；语法合法即透传，只有形态非法
   才是 `10001`。
2. **压缩两段式**：`flags.compressed` 表是否压缩、header `compression` 表算法，两者必须自洽（矛盾
   `350018`）。
3. **算法只是登记**：白名单校验到位，但本任务不引压缩库（压缩 / 解压归生产者与消费者）。

> 说明：`application/json` 这类媒体类型大小写不敏感，规范化统一小写、丢弃 `;` 之后的参数——
> broker 不解释参数，保留只会让「同一格式两种写法」在统计与回显里分散。
"""

from __future__ import annotations

import pytest
from bms_core.core.exceptions import ParamError

from bms_msg.errors import MsgFrameInvalidError
from bms_msg.protocol.payload import (
    COMPRESSION_GZIP,
    COMPRESSION_NONE,
    COMPRESSION_SNAPPY,
    COMPRESSION_ZSTD,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_MSGPACK,
    CONTENT_TYPE_OCTET_STREAM,
    CONTENT_TYPE_PROTOBUF,
    CONTENT_TYPE_TEXT,
    DEFAULT_CONTENT_TYPE,
    KNOWN_COMPRESSIONS,
    KNOWN_CONTENT_TYPES,
    ensure_compression_flag,
    normalize_compression,
    normalize_content_type,
    payload_contract,
    split_content_type,
)

pytestmark = pytest.mark.kiwi_id(2423)

_STATUS_DEFAULT = "default"
_STATUS_KNOWN = "known"
_STATUS_RESERVED = "reserved"


def test_default_content_type_is_msgpack() -> None:
    """不传即 MessagePack（与「默认序列化 MessagePack」一致）。"""
    assert normalize_content_type(None) == CONTENT_TYPE_MSGPACK
    assert normalize_content_type("") == CONTENT_TYPE_MSGPACK
    assert normalize_content_type("   ") == CONTENT_TYPE_MSGPACK
    assert DEFAULT_CONTENT_TYPE == CONTENT_TYPE_MSGPACK


def test_known_content_types_normalized() -> None:
    """已登记取值原样通过；大小写不敏感、参数丢弃。"""
    assert normalize_content_type(CONTENT_TYPE_JSON) == CONTENT_TYPE_JSON
    assert normalize_content_type("Application/JSON") == CONTENT_TYPE_JSON
    assert normalize_content_type("application/json; charset=utf-8") == CONTENT_TYPE_JSON
    assert normalize_content_type(CONTENT_TYPE_TEXT) == CONTENT_TYPE_TEXT
    assert normalize_content_type(CONTENT_TYPE_OCTET_STREAM) == CONTENT_TYPE_OCTET_STREAM
    assert normalize_content_type(CONTENT_TYPE_PROTOBUF) == CONTENT_TYPE_PROTOBUF


def test_unknown_content_type_passes_through() -> None:
    """未登记但语法合法的取值透传（broker 不做白名单拒绝）。"""
    assert normalize_content_type("application/x-vendor-custom") == "application/x-vendor-custom"


@pytest.mark.parametrize(
    "raw",
    ["json", "/json", "application/", "application/json/x", "application json", "application/js*n", "a/b" + "c" * 200],
)
def test_invalid_content_type_rejected(raw: str) -> None:
    """形态非法即契约层 `10001`（键在、值形态错）。"""
    with pytest.raises(ParamError):
        normalize_content_type(raw)


def test_content_type_status_table() -> None:
    """取值状态：msgpack 缺省、protobuf 预留、其余已知。"""
    assert KNOWN_CONTENT_TYPES[CONTENT_TYPE_MSGPACK] == _STATUS_DEFAULT
    assert KNOWN_CONTENT_TYPES[CONTENT_TYPE_PROTOBUF] == _STATUS_RESERVED
    assert KNOWN_CONTENT_TYPES[CONTENT_TYPE_JSON] == _STATUS_KNOWN
    assert KNOWN_CONTENT_TYPES[CONTENT_TYPE_TEXT] == _STATUS_KNOWN
    assert KNOWN_CONTENT_TYPES[CONTENT_TYPE_OCTET_STREAM] == _STATUS_KNOWN


def test_split_content_type() -> None:
    """主体截取：去参数、去空白、小写。"""
    assert split_content_type(" Text/Plain ; charset=UTF-8") == CONTENT_TYPE_TEXT
    assert split_content_type("APPLICATION/MSGPACK") == CONTENT_TYPE_MSGPACK


def test_compression_default_is_none() -> None:
    """不传即未压缩。"""
    assert normalize_compression(None) == COMPRESSION_NONE
    assert normalize_compression("") == COMPRESSION_NONE
    assert normalize_compression("  ") == COMPRESSION_NONE


def test_known_compressions_accepted() -> None:
    """白名单内取值通过（大小写不敏感）。"""
    for name in KNOWN_COMPRESSIONS:
        assert normalize_compression(name) == name
    assert normalize_compression("GZIP") == COMPRESSION_GZIP
    assert normalize_compression(COMPRESSION_ZSTD) == COMPRESSION_ZSTD
    assert normalize_compression(COMPRESSION_SNAPPY) == COMPRESSION_SNAPPY


def test_unknown_compression_rejected() -> None:
    """白名单外即 `10001`（如 brotli）。"""
    with pytest.raises(ParamError):
        normalize_compression("br")


def test_flag_and_algorithm_must_agree() -> None:
    """四种组合：一致通过两种，矛盾拒绝两种。"""
    ensure_compression_flag(compressed=False, compression=COMPRESSION_NONE)
    ensure_compression_flag(compressed=True, compression=COMPRESSION_GZIP)

    with pytest.raises(MsgFrameInvalidError):
        ensure_compression_flag(compressed=True, compression=COMPRESSION_NONE)
    with pytest.raises(MsgFrameInvalidError):
        ensure_compression_flag(compressed=False, compression=COMPRESSION_GZIP)


def test_payload_contract_matches_constants() -> None:
    """契约片段与常量同值（导出到 `x-msg-frame` 后仍是同一处事实源）。"""
    contract = payload_contract()

    assert [entry["value"] for entry in contract["payload_content_types"]] == list(KNOWN_CONTENT_TYPES)
    assert [entry["status"] for entry in contract["payload_content_types"]] == list(KNOWN_CONTENT_TYPES.values())
    assert list(contract["compressions"]) == list(KNOWN_COMPRESSIONS)
    rules = contract["payload_rules"]
    assert rules["default_content_type"] == DEFAULT_CONTENT_TYPE
    assert rules["top_level"].startswith("map<str, any>")
    assert "limit" in rules
    assert "compression_flag" in rules
    assert "uncompressed_len" in rules
