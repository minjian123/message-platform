"""测试向量回放（任务 03-2；Kiwi 2421）：六组向量逐条对齐「实现 ↔ 冻结字节」。

向量入库 `services/msg/tests/data/frames/*.json`：`kind=hex` 的用**冻结十六进制**回放（防「用实现验证
实现」），`kind=limit` 的只给尺寸（1 MiB 的 hex 体积过大），由用例按尺寸构造后立即解码，验证上限
**含端点**可通过。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from bms_core.msgclient.codec import encode_header
from bms_core.msgclient.frame import (
    MAX_FRAME_HEADER_BYTES,
    Frame,
    decode_frame,
    encode_frame,
)

from bms_msg.errors import MsgError

pytestmark = pytest.mark.kiwi_id(2421)

_VECTOR_DIR = Path(__file__).resolve().parents[1] / "data" / "frames"
"""向量目录（相对本文件：`tests/protocol` → `tests/data/frames`）。"""

_VECTORS = sorted(_VECTOR_DIR.glob("*.json"))
"""全部向量（文件名序，便于失败时定位）。"""


def _load(path: Path) -> dict[str, Any]:
    """读一个向量文件。

    Args:
        path: 向量文件路径。

    Returns:
        dict[str, Any]: 向量内容。
    """
    return json.loads(path.read_text(encoding="utf-8"))


def _assert_frame(frame: Frame, expected: dict[str, Any]) -> None:
    """逐项核对解码结果。

    Args:
        frame: 解码结果。
        expected: 向量里的期望（`type` / `header` / `payload_utf8` / `payload_len` / `version`）。
    """
    if "type" in expected:
        assert frame.frame_type == expected["type"]
    if "version" in expected:
        assert list(frame.version) == expected["version"]
    for key, value in expected.get("header", {}).items():
        assert frame.header[key] == value
    if "payload_utf8" in expected:
        assert bytes(frame.payload).decode() == expected["payload_utf8"]
    if "payload_len" in expected:
        assert len(frame.payload) == expected["payload_len"]


@pytest.mark.parametrize("path", _VECTORS, ids=[item.stem for item in _VECTORS])
def test_vector_matches_implementation(path: Path) -> None:
    """按 `expect` 回放：`ok` 解出并核对字段，数字则核对错误码。

    Args:
        path: 向量文件。
    """
    case = _load(path)
    if case["kind"] == "hex":
        data = bytes.fromhex(case["hex"])
        if case["expect"] == "ok":
            _assert_frame(decode_frame(data), case.get("assert", {}))
            return
        with pytest.raises(MsgError) as excinfo:
            decode_frame(data)
        assert excinfo.value.code == int(case["expect"])
        return
    _assert_limit_vector(case)


def _assert_limit_vector(case: dict[str, Any]) -> None:
    """上限边界向量：按尺寸构造后解码（含端点应通过）。

    Args:
        case: 向量内容（`header_bytes` / `payload_bytes`）。
    """
    header = {"request_id": "r1"}
    if case["header_bytes"] >= MAX_FRAME_HEADER_BYTES:
        header["pad"] = _pad_header(header, MAX_FRAME_HEADER_BYTES)
    payload = b"\x00" * case["payload_bytes"]

    frame = decode_frame(encode_frame(frame_type=1, header=header, payload=payload))

    assert len(frame.payload) == case["payload_bytes"]


def _pad_header(header: dict[str, Any], target: int) -> str:
    """把 header 填充到**恰好** target 字节（用未知键 `pad`，顺便验证「未知键忽略」）。

    msgpack 字符串的长度前缀宽度会随长度变化（1 / 2 / 3 / 5 字节），所以要按实测差值迭代逼近，
    不能「一次算出长度差直接补」。

    Args:
        header: 基础头。
        target: 目标编码长度（字节）。

    Returns:
        str: 使编码长度恰为 target 的填充串。

    Raises:
        AssertionError: 迭代未能收敛（长度前缀档位变化导致，理论上不会发生）。
    """
    size = 0
    for _ in range(8):
        current = len(encode_header({**header, "pad": "x" * size}))
        if current == target:
            return "x" * size
        size = max(0, size + (target - current))
    raise AssertionError(f"header 未能填充到 {target} 字节（最后一次 {size}）")


def test_vector_directory_covers_six_groups() -> None:
    """六组向量齐备（合法 / 边界 / 非法 / 未知类型 / 未知字段 / 超大），防止有人删样例。"""
    names = {item.stem for item in _VECTORS}

    assert {"legal_publish", "legal_ack_min"} <= names
    assert {"boundary_max_header", "boundary_max_payload"} <= names
    assert {"invalid_short", "invalid_magic", "invalid_length_mismatch", "version_major_2"} <= names
    assert "unknown_type" in names
    assert "unknown_header_key" in names
    assert {"oversize_header", "oversize_payload"} <= names
