"""测试辅助：帧客户端的最小操作面（03-3 起的内容类型 / 压缩 / nack 用例共用）。

为什么单独一份而不是复用 `tests/api/test_frame_data_plane.py` 里的同名辅助：`services/msg/tests` 不是包，
跨文件 import 不可靠（见 `tests/conftest.py` 开头）；本模块在 `pythonpath` 的 `.` 下可导入，且不属覆盖率
统计包（`coverage_packages = ["bms_msg", "services", "ops"]`），不会把测试便利代码算进覆盖率分母。
"""

from __future__ import annotations

from typing import Any

from httpx import AsyncClient, Response

from bms_msg.protocol.frame import FrameFlags, encode_frame

__all__ = [
    "CONSUMER",
    "DEMO_HEADERS",
    "FRAME_TYPE",
    "GROUP",
    "NAMESPACE",
    "TOPIC_NAME",
    "create_topic",
    "frame_bytes",
    "header",
    "join_group",
    "post_frame",
]

NAMESPACE = "msg"
"""命名空间（用例统一样式）。"""

TOPIC_NAME = "audit.operation"
"""主题名（与既有帧用例一致，便于对照）。"""

GROUP = "audit"
"""消费组名。"""

CONSUMER = "c1"
"""成员标识。"""

FRAME_TYPE = "application/x-msg-frame"
"""帧媒体类型（请求头 `Content-Type`）。"""

DEMO_HEADERS = {"X-Tenant-ID": "demo"}
"""租户头（中间件据此解析租户）。"""


def header(*, request_id: str | None = "r1", extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """构造帧头（缺省含 `request_id`）。

    Args:
        request_id: 请求标识；None 表示故意不带。
        extra: 追加字段。

    Returns:
        dict[str, Any]: 帧头。
    """
    value: dict[str, Any] = {} if request_id is None else {"request_id": request_id}
    value.update(extra or {})
    return value


def frame_bytes(
    frame_type: int,
    frame_header: dict[str, Any],
    payload: bytes = b"",
    *,
    flags: int = int(FrameFlags.NEED_RESPONSE),
) -> bytes:
    """编码一帧（缺省置 need_response 位）。

    Args:
        frame_type: 帧类型。
        frame_header: 帧头。
        payload: 消息体。
        flags: 标志位。

    Returns:
        bytes: 帧字节。
    """
    return encode_frame(frame_type=frame_type, header=frame_header, payload=payload, flags=flags)


async def post_frame(client: AsyncClient, path: str, body: bytes) -> Response:
    """发一个帧请求。

    Args:
        client: 客户端。
        path: 路径。
        body: 帧字节。

    Returns:
        Response: 响应。
    """
    return await client.post(path, content=body, headers={**DEMO_HEADERS, "Content-Type": FRAME_TYPE})


async def create_topic(client: AsyncClient, *, name: str = TOPIC_NAME) -> None:
    """建主题（数据面元数据前置）。

    Args:
        client: 客户端。
        name: 主题名。
    """
    response = await client.post(
        "/api/v1/msg/admin/topics",
        json={"namespace": NAMESPACE, "name": name, "partition_count": 1},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200


async def join_group(client: AsyncClient, *, topic: str = TOPIC_NAME) -> None:
    """加入消费组（成员制前置）。

    Args:
        client: 客户端。
        topic: 主题名。
    """
    response = await client.post(
        "/api/v1/msg/join",
        json={"group": GROUP, "namespace": NAMESPACE, "topic": topic, "consumer_id": CONSUMER},
        headers=DEMO_HEADERS,
    )
    assert response.status_code == 200
