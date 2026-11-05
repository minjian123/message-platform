"""帧协议契约（任务 03-2；Kiwi 2421）：常量同值 / 类型与位表齐全 / 必填集与模型一致。

契约是「对外承诺」的机器可读描述（导出为 `x-msg-frame`），所以它必须**与实现同值**——
本文件把三处容易漂移的地方钉成断言：上限常量、类型编号、各端点必填键。
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from bms_msg.protocol.contract import FRAME_CONTRACT_KEY, frame_contract
from bms_msg.protocol.frame import (
    FRAME_CONTENT_TYPE,
    FRAME_HEADER_BYTES,
    MAX_FRAME_HEADER_BYTES,
    MAX_FRAME_PAYLOAD_BYTES,
    PROTOCOL_VERSION,
    FrameType,
)
from bms_msg.protocol.payload import DEFAULT_CONTENT_TYPE, KNOWN_COMPRESSIONS, KNOWN_CONTENT_TYPES
from bms_msg.schemas.message import AckRequest, ConsumeRequest, NackRequest, PublishRequest

pytestmark = pytest.mark.kiwi_id(2421)


def test_contract_key_is_openapi_extension() -> None:
    """扩展键必须以 `x-` 开头（OpenAPI 只允许扩展键非标准字段）。"""
    assert FRAME_CONTRACT_KEY.startswith("x-")


def test_contract_matches_constants() -> None:
    """版本 / 帧头宽度 / 上限与实现常量同值。"""
    contract = frame_contract()

    assert contract["protocol_version"] == PROTOCOL_VERSION
    assert contract["frame_header_bytes"] == FRAME_HEADER_BYTES
    assert contract["limits"] == {"header_bytes": MAX_FRAME_HEADER_BYTES, "payload_bytes": MAX_FRAME_PAYLOAD_BYTES}


def test_contract_lists_all_frame_types_and_reserved_ranges() -> None:
    """类型表覆盖全部已登记类型，预留区间含 admin / rpc 两段且不与已用编号重叠。"""
    contract = frame_contract()

    assert contract["frame_types"] == {str(int(member)): member.name.lower() for member in FrameType}
    ranges = {item["name"] for item in contract["reserved_types"]}
    assert {"admin", "rpc"} <= ranges
    # 03-3 新增 nack=5：数据面预留必须从 6 起，否则契约自相矛盾。
    assert contract["frame_types"]["5"] == "nack"
    data_plane = next(item for item in contract["reserved_types"] if item["name"] == "data-plane-reserved")
    assert data_plane["range"] == "6-15"


def test_contract_declares_payload_rules() -> None:
    """消息体口径进契约：内容类型取值 / 压缩白名单 / 消息体规则，且与常量同值。"""
    contract = frame_contract()

    assert contract["frame_media_type"] == FRAME_CONTENT_TYPE
    assert "content_type" not in contract, "帧媒体类型已改名 frame_media_type，与消息体 content_type 消歧"
    values = [entry["value"] for entry in contract["payload_content_types"]]
    statuses = [entry["status"] for entry in contract["payload_content_types"]]
    assert values == list(KNOWN_CONTENT_TYPES)
    assert statuses == list(KNOWN_CONTENT_TYPES.values())
    assert list(contract["compressions"]) == list(KNOWN_COMPRESSIONS)
    rules = contract["payload_rules"]
    assert rules["default_content_type"] == DEFAULT_CONTENT_TYPE
    assert rules["top_level"].startswith("map<str, any>")


def test_contract_envelope_carries_payload_marks() -> None:
    """信封可选字段含内容类型与压缩标记（只增可选字段：老端忽略即可）。"""
    optional = frame_contract()["envelope"]["optional"]

    assert {"content_type", "compression", "uncompressed_len"} <= set(optional)


def test_contract_flags_cover_all_bits() -> None:
    """标志位表按位号列出（0~4），不多不少。"""
    assert frame_contract()["flags"] == {
        "0": "compressed",
        "1": "end_of_stream",
        "2": "need_response",
        "3": "batch",
        "4": "response",
    }


@pytest.mark.parametrize(
    ("endpoint", "model"),
    [
        ("publish", PublishRequest),
        ("consume", ConsumeRequest),
        ("ack", AckRequest),
        ("nack", NackRequest),
    ],
)
def test_endpoint_required_fields_match_models(endpoint: str, model: type[BaseModel]) -> None:
    """契约里的必填键 = 请求模型的必填字段（不另维护一份字段表，杜绝两处漂移）。

    Args:
        endpoint: 契约里的端点名。
        model: 对应请求模型。
    """
    declared = frame_contract()["endpoint_fields"][endpoint]["required"]
    expected = [name for name, field in model.model_fields.items() if field.is_required()]

    assert sorted(declared) == sorted(expected)


def test_known_unsupported_declares_owner() -> None:
    """「登记但未服务」的类型必须说明归口（客户端据此判断该等后续任务）。

    `heartbeat` 已由 03-4 落地（移出本清单，语义见 `connection.heartbeat`）；当前只剩帧式订阅。
    """
    unsupported = frame_contract()["known_unsupported"]

    assert set(unsupported) == {"subscribe"}
    assert all("03-" in note for note in unsupported.values())
