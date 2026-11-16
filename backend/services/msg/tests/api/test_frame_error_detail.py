"""帧错误帧随附业务数据（03-2 补口；Kiwi 2512）。

修复前：`api/frame/gate.py` 三处 `error_frame(...)` 只写码与文案，不传异常自带的 `data`。
后果是 `350022` 的水位读数、`350033` 的 `leader` / `epoch` 只在 JSON 路径可见，帧客户端
拿不到行动依据（`350022` 的 `_payload` 与本模块注释都写明「JSON 与帧 detail 共用一份结构」）。

本文件断言：同一次失败，帧路径 `detail` 与 JSON 路径 `data` 逐字段一致；无业务数据时
`detail` 为空但键存在（客户端不必区分「无键」与「空值」两种缺省）。
"""

from __future__ import annotations

import importlib
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.exceptions import ResourceAccessDeniedError
from bms_core.core.plugin import default_plugin_registry
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame, encode_frame
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response

from bms_msg.api.deps import get_topic_acl_service
from bms_msg.api.frame.responses import error_frame
from bms_msg.errors import MsgErrorCode, MsgNotLeaderError, MsgStorageWatermarkExceededError
from tests_support.frame_app import build_frame_app

pytestmark = pytest.mark.kiwi_id(2512)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"

NAMESPACE = "msg"
TOPIC_NAME = "audit.operation"
DEMO_HEADERS = {"X-Tenant-ID": "demo"}
FRAME_TYPE = "application/x-msg-frame"

WATERMARK_DATA: dict[str, object] = {
    "used_ratio": 0.97,
    "threshold": 0.95,
    "volume": "/var/lib/msg",
    "free_bytes": 1024,
    "reason": "ratio",
}
"""`350022` 的随附数据（字段取自 `broker/watermark.py::_payload`）。"""


class _RejectGate:
    """恒拒的存储水位闸门（`StorageGate` 协议只要求 `ensure_writable`）。"""

    def ensure_writable(self) -> None:
        """抛出带随附数据的 `350022`。

        Raises:
            MsgStorageWatermarkExceededError: 恒抛。
        """
        raise MsgStorageWatermarkExceededError("存储水位超拒绝阈值", data=dict(WATERMARK_DATA))


class _NotLeaderReplication:
    """恒抛非主错误的复制视图替身（只覆盖发布路径会调的那一个方法）。"""

    async def ensure_writable(self, *, topic_id: int, partition_no: int, acks: str) -> None:
        """抛出带 `leader` / `epoch` 的 `350033`。

        Args:
            topic_id: 主题主键。
            partition_no: 分区号。
            acks: 确认档位。

        Raises:
            MsgNotLeaderError: 恒抛。
        """
        del acks
        raise MsgNotLeaderError(
            reason="not_leader",
            topic_id=topic_id,
            partition=partition_no,
            leader="msg-2",
            epoch=7,
        )


class _DenyAcl:
    """ACL 守卫替身：取任何动作都抛基座 `82013`（带随附数据，用于验证映射后的透传）。"""

    async def authorize(self, **kwargs: object) -> None:
        """拒绝。

        Args:
            kwargs: 守卫入参（不参与判定）。

        Raises:
            ResourceAccessDeniedError: 恒抛。
        """
        del kwargs
        raise ResourceAccessDeniedError("主体对该主题无授权", data={"principal": "u-1"})


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记权限 / 租户模块门禁（与帧数据面用例同口径）。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明（统一渲染据此解析文案键与 HTTP 状态）。

    Yields:
        None: 用例运行期。
    """
    reset_error_code_records()
    manifest = load_product_manifest(_MANIFEST)
    register_error_code_records(ConcurrentStableList(manifest.error_codes))
    yield
    reset_error_code_records()


@pytest.fixture
def api_app(db_session: Any, tenant_source: object) -> FastAPI:
    """最小应用（`/api/v1` 业务路由 + 根路径 `/msg/*` 帧聚合）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    return build_frame_app(session=db_session, tenant_source=tenant_source)


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncIterator[AsyncClient]:
    """应用内客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as opened:
        yield opened


async def _create_topic(client: AsyncClient) -> None:
    """建主题（两条路径共同的元数据前置）。

    Args:
        client: 客户端。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": TOPIC_NAME, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


def _frame(frame_type: int, header: dict[str, Any], payload: bytes = b"") -> bytes:
    """编码一帧（缺省置 need_response 位）。

    Args:
        frame_type: 帧类型。
        header: 帧头。
        payload: 消息体。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(frame_type=frame_type, header=header, payload=payload, flags=int(FrameFlags.NEED_RESPONSE))


async def _post_frame(client: AsyncClient, path: str, body: bytes) -> Response:
    """发一个帧请求。

    Args:
        client: 客户端。
        path: 路径。
        body: 帧字节。

    Returns:
        Response: 响应。
    """
    return await client.post(path, content=body, headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE})


def _publish_frame() -> bytes:
    """构造发布帧。

    Returns:
        bytes: 帧字节。
    """
    return _frame(
        int(FrameType.PUBLISH),
        {"request_id": "r1", "namespace": NAMESPACE, "topic": TOPIC_NAME},
        b"payload",
    )


@pytest.mark.asyncio
async def test_watermark_detail_matches_json_data(client: AsyncClient, api_app: FastAPI) -> None:
    """`350022`：帧路径 `detail` 与 JSON 路径 `data` 逐字段一致（同一份结构）。"""
    await _create_topic(client)
    api_app.state.storage_watermark = _RejectGate()

    framed = await _post_frame(client, "/msg/publish", _publish_frame())
    frame = decode_frame(framed.content)
    assert framed.status_code == 200
    assert frame.header["code"] == int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED)
    assert dict(cast("dict[str, object]", frame.header["detail"])) == WATERMARK_DATA

    plain = await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": ""},
        headers=DEMO_HEADERS,
    )
    assert plain.status_code == 507
    body = cast("dict[str, object]", plain.json())
    assert body["code"] == int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED)
    assert body["data"] == WATERMARK_DATA


@pytest.mark.asyncio
async def test_not_leader_detail_carries_leader_and_epoch(client: AsyncClient, api_app: FastAPI) -> None:
    """`350033`：帧路径带 `reason` / `leader` / `epoch`，客户端据此改投（此前这五个字段被丢掉）。"""
    await _create_topic(client)
    api_app.state.replication = _NotLeaderReplication()

    response = await _post_frame(client, "/msg/publish", _publish_frame())
    frame = decode_frame(response.content)

    assert frame.header["code"] == int(MsgErrorCode.NOT_LEADER)
    detail = cast("dict[str, object]", frame.header["detail"])
    assert detail["reason"] == "not_leader"
    assert detail["leader"] == "msg-2"
    assert detail["epoch"] == 7
    assert detail["partition"] == 0
    # 雪花 ID 越过 msgpack 的 int64 上限：以十进制字符串下发（与 JSON 的雪花 ID 口径一致），
    # 否则整个错误帧装配会 OverflowError（比丢字段更糟：500）。
    assert isinstance(detail["topic_id"], str)
    assert int(detail["topic_id"]) != 0


@pytest.mark.asyncio
async def test_mapped_biz_error_carries_detail(client: AsyncClient, api_app: FastAPI) -> None:
    """基座码映射分支：`mapped.data` 同样进 `detail`（82013 → 350027 不丢随附数据）。"""
    await _create_topic(client)
    api_app.dependency_overrides[get_topic_acl_service] = lambda: _DenyAcl()

    response = await _post_frame(client, "/msg/publish", _publish_frame())
    frame = decode_frame(response.content)

    assert frame.header["code"] == int(MsgErrorCode.ACL_DENIED)
    assert dict(cast("dict[str, object]", frame.header["detail"])) == {"principal": "u-1"}


@pytest.mark.asyncio
async def test_version_unsupported_detail_is_empty(client: AsyncClient) -> None:
    """帧主版本不支持：解码前就失败，`detail` 为空但键存在（不因缺数据而崩）。"""
    body = encode_frame(
        frame_type=int(FrameType.PUBLISH),
        header={"request_id": "r1", "namespace": NAMESPACE, "topic": TOPIC_NAME},
        flags=int(FrameFlags.NEED_RESPONSE),
        version=(2, 0),
    )

    response = await _post_frame(client, "/msg/publish", body)
    frame = decode_frame(response.content)

    assert frame.header["code"] == int(MsgErrorCode.VERSION_UNSUPPORTED)
    assert "detail" in frame.header
    assert frame.header["detail"] is None


def test_error_frame_detail_normalizes_snowflake_id() -> None:
    """`error_frame` 把超 `uint64` 的整数转字符串：126 位雪花 ID 不再让帧装配溢出。"""
    snowflake = 2**126 - 1
    body = error_frame(code=int(MsgErrorCode.NOT_LEADER), message="x", detail={"topic_id": snowflake})
    detail = cast("dict[str, object]", decode_frame(body).header["detail"])
    assert detail["topic_id"] == str(snowflake)


def test_error_frame_detail_recurses_and_keeps_scalars() -> None:
    """嵌套映射 / 序列递归转换；布尔、浮点、字符串、空值、字节原样保留。"""
    snowflake = 2**100 + 1
    body = error_frame(
        code=int(MsgErrorCode.STORAGE_WATERMARK_EXCEEDED),
        message="x",
        detail={
            "ratio": 0.5,
            "flag": True,
            "empty": None,
            "name": "n",
            "big": snowflake,
            "items": [snowflake, "x"],
            "tuple": (snowflake,),
            "raw": b"b",
        },
    )
    detail = cast("dict[str, object]", decode_frame(body).header["detail"])
    assert (detail["ratio"], detail["flag"], detail["empty"], detail["name"]) == (0.5, True, None, "n")
    assert detail["big"] == str(snowflake)
    assert detail["items"] == [str(snowflake), "x"]
    assert detail["tuple"] == [str(snowflake)]
    assert detail["raw"] == b"b"


def test_error_frame_detail_keeps_uint64_range() -> None:
    """边界锁：`int64` 与 `uint64` 两段内的整数原样保留，越过 `uint64` 才转字符串。

    MessagePack 的整数是 `uint64` + `int64` 两段（正数可用满 `2**64-1`），不是只有 `int64`；
    这条用例防止有人把上界误改成 `2**63-1`（会把一批合法值无谓地变成字符串）。
    """
    body = error_frame(
        code=int(MsgErrorCode.OFFSET_INVALID),
        message="x",
        detail={"int64_max": 2**63 - 1, "uint64_max": 2**64 - 1, "over": 2**64, "negative": -(2**63)},
    )
    detail = cast("dict[str, object]", decode_frame(body).header["detail"])
    assert detail["int64_max"] == 2**63 - 1
    assert detail["uint64_max"] == 2**64 - 1
    assert detail["negative"] == -(2**63)
    assert detail["over"] == str(2**64)
