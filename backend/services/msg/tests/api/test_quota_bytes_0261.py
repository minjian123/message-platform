"""B18 存储配额 payload 字节（02-6-1 / Kiwi 2498）。

覆盖：base64 解码一次并缓存、真实字节数、配额边缘不因 padding 误判 507。
"""

# pyright: reportPrivateUsage=false
from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any, cast

import pytest
from bms_core.msgclient.frame import MAX_FRAME_PAYLOAD_BYTES

from bms_msg.api.data import messages as messages_module
from bms_msg.api.data.messages import _ensure_storage_quota, _payload_bytes
from bms_msg.errors import MsgStorageWatermarkExceededError
from bms_msg.schemas.message import PublishRequest

pytestmark = pytest.mark.kiwi_id(2498)

NAMESPACE = "msg"
TENANT_ID = 7001


class _Quota:
    """配额替身（固定硬限）。"""

    def __init__(self, limit: int) -> None:
        self._limit = limit

    async def effective(self, tenant_id: int, namespace: str, key: str) -> SimpleNamespace:
        del tenant_id, namespace, key
        return SimpleNamespace(limit=self._limit)


class _Usage:
    """用量替身（固定存储字节）。"""

    def __init__(self, stored: int) -> None:
        self._stored = stored

    async def storage_bytes(self, tenant_id: int) -> int:
        del tenant_id
        return self._stored


def _request(payload: str) -> PublishRequest:
    return PublishRequest(namespace=NAMESPACE, topic="t", payload=payload)


def test_payload_bytes_are_exact() -> None:
    """padding 按真实解码字节计，不再按 `len*3//4` 估算。"""
    assert _payload_bytes(_request("")) == 0
    assert _payload_bytes(_request("YQ==")) == 1
    assert _payload_bytes(_request("YWI=")) == 2


def test_payload_bytes_decode_once_and_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """首次解码后写回私有通道；后续调用与投递复用，不再二次解码。"""
    calls = 0
    original = messages_module.resolve_payload

    def _wrapped(request: PublishRequest) -> bytes:
        nonlocal calls
        calls += 1
        return original(request)

    monkeypatch.setattr(messages_module, "resolve_payload", _wrapped)
    request = _request("YWI=")
    assert _payload_bytes(request) == 2
    assert request.raw_payload == b"ab"
    assert _payload_bytes(request) == 2
    assert calls == 1


async def test_storage_quota_uses_exact_bytes() -> None:
    """配额 2 字节时 `YWI=` 通过（旧估算会误判 3 字节而拒绝）。"""
    await _ensure_storage_quota(
        quota=cast(Any, _Quota(2)),
        usage=cast(Any, _Usage(0)),
        tenant_id=TENANT_ID,
        namespace=NAMESPACE,
        incoming=_payload_bytes(_request("YWI=")),
    )
    with pytest.raises(MsgStorageWatermarkExceededError):
        await _ensure_storage_quota(
            quota=cast(Any, _Quota(1)),
            usage=cast(Any, _Usage(0)),
            tenant_id=TENANT_ID,
            namespace=NAMESPACE,
            incoming=_payload_bytes(_request("YWI=")),
        )


def test_one_mib_payload_is_exact() -> None:
    """1 MiB 边界按真实字节计（base64 长度 1 398 104，但字节数仍是 1 048 576）。"""
    text = base64.b64encode(b"\x00" * MAX_FRAME_PAYLOAD_BYTES).decode("ascii")
    assert _payload_bytes(_request(text)) == MAX_FRAME_PAYLOAD_BYTES
