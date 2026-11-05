"""连接心跳端点（任务 03-4）：`POST /msg/connection/heartbeat`。

**为什么单独一条路径而不是 `/msg/heartbeat`**：`/api/v1/msg/heartbeat` 是 01-4 的**成员心跳**（判「成员还
活着」，影响分区分配）。两者同名不同义，路径必须分开——成员心跳留在成员路由，连接心跳在连接路由上。

**双形态**（与数据面同一哲学：同路径同语义、两种承载）：

- **帧**：媒体类型 `application/x-msg-frame`；帧类型 17；`header.request_id` 必填；payload 必须为空。
  响应为 200 + 类型 17 响应帧（header 平铺自报字段）。
- **JSON**：空体；响应为 200 + `ApiResponse[ConnectionInfo]`。

**心跳不做的事**：不解析租户（可缺，记 `anonymous`）、不要权限码、不触库、不取工作单元——心跳必须比业务
快，慢业务不能把心跳拖成「超时」（需求 03-4 完成标准）。超限拒绝由连接守卫（`api/frame/guard.py`）给出。
"""

from bms_core.api.base import BaseRouter
from bms_core.core.concurrent import ConcurrentStableList
from bms_core.schemas.common import ApiResponse
from fastapi import Depends, Request, Response
from fastapi.responses import JSONResponse

from bms_msg.api.deps import ConnectionTuningDep
from bms_msg.api.frame.guard import HEARTBEAT_GUARD, HeartbeatRecordDep, connection_info
from bms_msg.api.frame.media import is_frame_request
from bms_msg.api.frame.responses import FRAME_MEDIA_TYPE, error_frame, result_frame
from bms_msg.errors import MsgError, MsgErrorCode, MsgFrameInvalidError
from bms_msg.protocol.frame import PROTOCOL_VERSION, FrameType, decode_frame

__all__ = ["router"]

router = BaseRouter(
    key="msg_heartbeat",
    prefix="/msg",
    tags=["msg-frame"],
    dependencies=[Depends(HEARTBEAT_GUARD)],
)
"""连接心跳路由（`/msg/connection/heartbeat`；守卫含连接数上限判定与 `last_heartbeat_at` 刷新）。"""

_HEARTBEAT_ERROR_CODES = ConcurrentStableList(
    [int(MsgErrorCode.FRAME_INVALID), int(MsgErrorCode.CONNECTION_LIMIT_EXCEEDED)]
)
"""心跳端点的错误码声明：`350018`（帧不合法 / 缺必填头）、`350020`（连接数超限）。

不声明 `350019`：主版本不支持由 `decode_frame` 抛出、同样以 error 帧返回，但它不是「心跳能不能发」的
独立分支（与数据面四端点的声明口径一致，避免码表噪音）。
"""


def _frame_error(exc: MsgError) -> Response:
    """把帧解析 / 校验失败渲染成 error 帧（HTTP 200）。

    Args:
        exc: 业务异常（携带产品码）。

    Returns:
        Response: 200 + error 帧。
    """
    return Response(
        content=error_frame(code=exc.code, message=exc.message or str(exc)),
        media_type=FRAME_MEDIA_TYPE,
        status_code=200,
    )


def _request_id_of(body: bytes) -> str:
    """解码并校验心跳帧，返回 `request_id`。

    校验顺序与数据面闸门一致：结构（`decode_frame`）→ 类型 → payload 必须为空 → 必填头。

    Args:
        body: 请求体字节。

    Returns:
        str: 帧头里的 `request_id`。

    Raises:
        MsgFrameInvalidError: 类型不是 17 / 带 payload / 缺 `request_id`（`350018`）。
        MsgVersionUnsupportedError: 主版本不支持（`350019`）。
        MsgPayloadTooLargeError: 帧超限（`350006`）。
    """
    frame = decode_frame(body)
    if frame.frame_type != int(FrameType.HEARTBEAT):
        raise MsgFrameInvalidError(f"该端点只接受心跳帧（17），收到 {frame.frame_type}")
    if bytes(frame.payload):
        raise MsgFrameInvalidError("心跳帧不得携带 payload")
    request_id = frame.header.get("request_id")
    if not isinstance(request_id, str) or not request_id:
        raise MsgFrameInvalidError("缺少必填 frame header 字段：request_id")
    return request_id


@router.post("/connection/heartbeat", openapi_extra={"x-error-codes": list(_HEARTBEAT_ERROR_CODES)})
async def heartbeat(
    request: Request,
    record: HeartbeatRecordDep,
    tuning: ConnectionTuningDep,
) -> Response:
    """心跳：刷新连接保活并回服务端连接参数（帧 17 或 JSON，同一份 `ConnectionInfo`）。

    无权限码：连接探活先于身份（05 域认证落地后再评估收紧）。
    帧形态：类型必须是 17、payload 必须为空、`request_id` 必填（缺失 `350018`）；主版本不符 `350019`。
    两种失败都以 200 + error 帧返回（帧可解析即协议内失败）。
    JSON 形态：空体，返回 `ApiResponse[ConnectionInfo]`；连接数超限时回 429（`350020`）。
    """
    if not is_frame_request(request):
        body = ApiResponse.ok(connection_info(request, record, tuning))
        return JSONResponse(content=body.model_dump(mode="json"))
    try:
        request_id = _request_id_of(await request.body())
    except MsgError as exc:
        return _frame_error(exc)
    result = result_frame(
        frame_type=int(FrameType.HEARTBEAT),
        request_id=request_id,
        result=connection_info(request, record, tuning, protocol_version=PROTOCOL_VERSION),
    )
    return Response(content=result, media_type=FRAME_MEDIA_TYPE, status_code=200)
