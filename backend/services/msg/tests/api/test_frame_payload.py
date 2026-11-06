"""帧路径的内容类型与压缩（任务 03-3；Kiwi 2423）。

三条链路各验一次：

- **透传**：header 的 `content_type` / `compression` / `uncompressed_len` 落进消息记录，并在 consume 响应回显；
- **自洽**：`flags.compressed` 与 `compression` 矛盾的四种组合；
- **直通**：consume 的 payload 段就是记录里的原始字节（不再 base64 往返），`length` 与切片一致。

压缩在本任务只做「标记 + 透明转发」：broker 不解压，生产者用标准库 `gzip` 压、消费者自己解——
这也是详细设计 §4.5「算法只登记、不引压缩库」的验收形态。
"""

from __future__ import annotations

import gzip
import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.core.plugin import default_plugin_registry
from bms_core.msgclient.frame import FrameFlags, FrameType, decode_frame
from bms_core.msgclient.payload import (
    COMPRESSION_GZIP,
    COMPRESSION_NONE,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_MSGPACK,
)
from bms_core.registry.products import load_product_manifest
from bms_core.services.module_registry import register_error_code_records, reset_error_code_records
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from bms_msg.errors import register_msg_error_mapper
from tests_support.frame_app import build_frame_app
from tests_support.frame_client import (
    CONSUMER,
    DEMO_HEADERS,
    GROUP,
    NAMESPACE,
    TOPIC_NAME,
    create_topic,
    frame_bytes,
    header,
    join_group,
    post_frame,
)

pytestmark = pytest.mark.kiwi_id(2423)

_MANIFEST = Path(__file__).resolve().parents[5] / "deploy" / "registry" / "msg.toml"
"""产品自持声明（错误码文案键与 HTTP 状态的唯一事实源）。"""


@pytest.fixture(autouse=True)
def _plugins() -> Iterator[None]:
    """登记产品码覆盖并确保占位模块门禁已导入。

    Yields:
        None: 用例运行期。
    """
    registry = default_plugin_registry()
    registry.reset()
    register_msg_error_mapper()
    importlib.import_module("bms_core.permission.null")
    importlib.import_module("bms_core.tenant_module.null")
    yield
    registry.reset()


@pytest.fixture(autouse=True)
def _declared_codes() -> Iterator[None]:
    """装载产品清单的错误码声明。

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
    """最小应用（业务路由 + `/msg/*` 聚合 + 帧闸门）。

    Args:
        db_session: 内存库会话。
        tenant_source: 租户源替身。

    Returns:
        FastAPI: 应用实例。
    """
    return build_frame_app(session=db_session, tenant_source=tenant_source)


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncClient:
    """ASGI 异步客户端。

    Args:
        api_app: 应用实例。

    Returns:
        AsyncClient: 客户端。
    """
    return AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test")


def _publish(client: AsyncClient, payload: bytes, **extra: Any) -> Any:
    """发一个 publish 帧（header 追加给定字段）。

    Args:
        client: 客户端。
        payload: 消息体。
        extra: 追加的帧头字段。

    Returns:
        httpx.Response: 响应。
    """
    request_header = header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, **extra})
    flags = int(FrameFlags.NEED_RESPONSE)
    if extra.get("compression") not in (None, COMPRESSION_NONE):
        flags |= int(FrameFlags.COMPRESSED)
    return post_frame(
        client,
        "/msg/publish",
        frame_bytes(int(FrameType.PUBLISH), request_header, payload, flags=flags),
    )


def _consume_header() -> dict[str, Any]:
    """consume 帧头（成员制必填项）。

    Returns:
        dict[str, Any]: 帧头。
    """
    return header(
        extra={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "group": GROUP,
            "consumer_id": CONSUMER,
            "partition": 0,
            "max_wait_ms": 0,
        }
    )


async def test_content_type_and_compression_roundtrip(client: AsyncClient) -> None:
    """压缩消息：生产者压、broker 原样转、消费者解出原文，三个标记全程回显。"""
    await create_topic(client)
    await join_group(client)
    raw = b'{"order_id": "A-1", "amount": 100}'
    packed = gzip.compress(raw)

    published = await _publish(
        client,
        packed,
        content_type=CONTENT_TYPE_JSON,
        compression=COMPRESSION_GZIP,
        uncompressed_len=len(raw),
    )
    assert published.status_code == 200
    assert decode_frame(published.content).header["offset"] == 0

    consumed = await post_frame(client, "/msg/consume", frame_bytes(int(FrameType.CONSUME), _consume_header()))
    frame = decode_frame(consumed.content)
    meta = frame.header["messages"][0]
    assert meta["content_type"] == CONTENT_TYPE_JSON
    assert meta["compression"] == COMPRESSION_GZIP
    assert meta["uncompressed_len"] == len(raw)
    assert meta["length"] == len(packed)

    forwarded = bytes(frame.payload)
    assert forwarded == packed
    assert gzip.decompress(forwarded) == raw


async def test_default_marks_are_msgpack_and_none(client: AsyncClient) -> None:
    """不传三字段：记录与回显都按缺省（msgpack / none / null），消费端据此自解释。"""
    await create_topic(client)
    await join_group(client)

    assert (await _publish(client, b"plain")).status_code == 200

    consumed = await post_frame(client, "/msg/consume", frame_bytes(int(FrameType.CONSUME), _consume_header()))
    meta = decode_frame(consumed.content).header["messages"][0]
    assert meta["content_type"] == CONTENT_TYPE_MSGPACK
    assert meta["compression"] == COMPRESSION_NONE
    assert meta["uncompressed_len"] is None


async def test_unknown_content_type_passes_through(client: AsyncClient) -> None:
    """未登记但语法合法的取值透传（broker 不做白名单拒绝，只打点）。"""
    await create_topic(client)
    await join_group(client)

    assert (await _publish(client, b"vendor", content_type="application/x-vendor-custom")).status_code == 200

    consumed = await post_frame(client, "/msg/consume", frame_bytes(int(FrameType.CONSUME), _consume_header()))
    meta = decode_frame(consumed.content).header["messages"][0]
    assert meta["content_type"] == "application/x-vendor-custom"


async def test_content_type_syntax_invalid(client: AsyncClient) -> None:
    """媒体类型形态非法 → 契约层 `10001`（不是 `350018`：键在、值形态错）。"""
    await create_topic(client)

    response = await _publish(client, b"x", content_type="json")

    frame = decode_frame(response.content)
    assert response.status_code == 200
    assert frame.header["code"] == 10001


async def test_compression_not_in_whitelist(client: AsyncClient) -> None:
    """算法不在白名单 → `10001`。"""
    await create_topic(client)

    response = await _publish(client, b"x", compression="br")

    frame = decode_frame(response.content)
    assert frame.header["code"] == 10001


async def test_flag_without_algorithm_rejected(client: AsyncClient) -> None:
    """`flags.compressed` 置位却没给算法 → `350018`（说压了但没说怎么解）。"""
    await create_topic(client)

    response = await post_frame(
        client,
        "/msg/publish",
        frame_bytes(
            int(FrameType.PUBLISH),
            header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME}),
            b"x",
            flags=int(FrameFlags.NEED_RESPONSE) | int(FrameFlags.COMPRESSED),
        ),
    )

    frame = decode_frame(response.content)
    assert frame.header["code"] == 350018
    assert frame.header["layer"] == "protocol"


async def test_flag_with_none_algorithm_rejected(client: AsyncClient) -> None:
    """置位却声明 `none` → `350018`（两个字段互相打脸）。"""
    await create_topic(client)

    response = await post_frame(
        client,
        "/msg/publish",
        frame_bytes(
            int(FrameType.PUBLISH),
            header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "compression": COMPRESSION_NONE}),
            b"x",
            flags=int(FrameFlags.NEED_RESPONSE) | int(FrameFlags.COMPRESSED),
        ),
    )

    assert decode_frame(response.content).header["code"] == 350018


async def test_algorithm_without_flag_rejected(client: AsyncClient) -> None:
    """给了算法却没置位 → `350018`（说没压却带了算法）。"""
    await create_topic(client)

    response = await post_frame(
        client,
        "/msg/publish",
        frame_bytes(
            int(FrameType.PUBLISH),
            header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME, "compression": COMPRESSION_GZIP}),
            b"x",
            flags=int(FrameFlags.NEED_RESPONSE),
        ),
    )

    assert decode_frame(response.content).header["code"] == 350018


async def test_consume_payload_is_raw_bytes(client: AsyncClient) -> None:
    """直通：consume 的 payload 段就是原始字节（含不可打印字节），`length` 可切片还原。"""
    await create_topic(client)
    await join_group(client)
    raw = b"\x00\x01\xfe\xffbinary"

    assert (await _publish(client, raw)).status_code == 200
    consumed = await post_frame(client, "/msg/consume", frame_bytes(int(FrameType.CONSUME), _consume_header()))
    frame = decode_frame(consumed.content)
    meta = frame.header["messages"][0]

    assert meta["length"] == len(raw)
    assert bytes(frame.payload) == raw


async def test_nack_frame_on_publish_path_rejected(client: AsyncClient) -> None:
    """nack 帧发到 publish 路径：`350018` 且提示正确路径（与未知类型区分开）。"""
    response = await post_frame(
        client,
        "/msg/publish",
        frame_bytes(
            int(FrameType.NACK),
            header(extra={"namespace": NAMESPACE, "topic": TOPIC_NAME}),
        ),
    )

    frame = decode_frame(response.content)
    assert frame.header["code"] == 350018
    assert "/msg/nack" in frame.header["message"]


async def test_json_path_carries_content_type(client: AsyncClient) -> None:
    """JSON 路径同口径：请求体声明内容类型，响应明细回显（两条入口一份语义）。"""
    await create_topic(client)
    await join_group(client)

    published = await client.post(
        "/api/v1/msg/publish",
        json={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "payload": "",
            "content_type": CONTENT_TYPE_JSON,
            "compression": COMPRESSION_NONE,
            "uncompressed_len": 3,
        },
        headers=DEMO_HEADERS,
    )
    assert published.status_code == 200

    consumed = await client.post(
        "/api/v1/msg/consume",
        json={
            "namespace": NAMESPACE,
            "topic": TOPIC_NAME,
            "group": GROUP,
            "consumer_id": CONSUMER,
            "partition": 0,
            "max_wait_ms": 0,
        },
        headers=DEMO_HEADERS,
    )
    assert consumed.status_code == 200
    messages = consumed.json()["data"]["messages"]
    assert messages[0]["content_type"] == CONTENT_TYPE_JSON
    assert messages[0]["uncompressed_len"] == 3


async def test_json_path_invalid_content_type(client: AsyncClient) -> None:
    """JSON 路径的形态非法同样是 `10001`（与帧路径一个判据）。"""
    await create_topic(client)

    response = await client.post(
        "/api/v1/msg/publish",
        json={"namespace": NAMESPACE, "topic": TOPIC_NAME, "payload": "", "content_type": "json"},
        headers=DEMO_HEADERS,
    )

    body = response.json()
    assert body["code"] == 10001, body
